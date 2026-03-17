using WireMock.Server;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;
using Polars.CSharp;
using static Polars.CSharp.Polars;
using Polars.Integration.Tests.Fixtures;
using Polars.NET.Core; // 假设你的 Polars.NET 命名空间

namespace Polars.Integration.Tests;

// [Collection("MinioCollection")] // 假设你已经配置了 CollectionFixture
public class CatalogIntegrationTests(MinioFixture _minio) : IAsyncLifetime, IClassFixture<MinioFixture>
{
    private WireMockServer _catalogMockServer = null!;

    public Task InitializeAsync()
    {
        // 1. 启动本地的 HTTP Mock 服务器，充当 Unity Catalog
        _catalogMockServer = WireMockServer.Start();
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        _catalogMockServer?.Stop();
        _catalogMockServer?.Dispose();
        return Task.CompletedTask;
    }

    /// <summary>
    /// 为指定的表配置 WireMock 以模拟 Databricks Unity Catalog 响应。
    /// 包含 TableInfo (表信息) 和 TemporaryCredentials (读/写临时凭证) 两个端点。
    /// </summary>
    private void SetupUnityCatalogMock(
        string catalog, 
        string schema, 
        string table, 
        string s3StorageLocation, 
        string expectedToken, 
        string accessKey, 
        string secretKey)
    {
        var dummyTableId = Guid.NewGuid().ToString();

        // ==========================================
        // Mock Endpoint 1: 获取表元数据 (Table Info)
        // ==========================================
        _catalogMockServer
            .Given(
                Request.Create()
                    .WithPath($"/api/2.1/unity-catalog/tables/{catalog}.{schema}.{table}")
                    .UsingGet()
                    .WithHeader("Authorization", $"Bearer {expectedToken}")
            )
            .RespondWith(
                Response.Create()
                    .WithStatusCode(200)
                    .WithBodyAsJson(new 
                    {
                        name = table,
                        catalog_name = catalog,
                        schema_name = schema,
                        table_type = "EXTERNAL",
                        data_source_format = "DELTA",
                        storage_location = s3StorageLocation,
                        table_id = dummyTableId,
                        
                        created_at = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        created_by = "test_admin",
                        updated_at = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        updated_by = "test_admin"
                    })
            );

        // ==========================================
        // Mock Endpoint 2: 获取临时凭证 (Temporary Credentials)
        // 读写操作底层都会调用这个接口拿 Token
        // ==========================================
        _catalogMockServer
            .Given(
                Request.Create()
                    .WithPath("/api/2.1/unity-catalog/temporary-table-credentials")
                    .UsingPost() // 注意这里是 POST 请求
                    .WithHeader("Authorization", $"Bearer {expectedToken}")
            )
            .RespondWith(
                Response.Create()
                    .WithStatusCode(200)
                    .WithBodyAsJson(new 
                    {
                        aws_temp_credentials = new 
                        {
                            access_key_id = accessKey,
                            secret_access_key = secretKey,
                            session_token = (string?)null, // 剔除 dummy token 以兼容原生 MinIO
                            access_point = (string?)null
                        },
                        azure_user_delegation_sas = (object?)null,
                        gcp_oauth_token = (object?)null,
                        
                        expiration_time = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeMilliseconds()
                    })
            );
    }

    [Fact]
    [Trait("Catalog","Scan")]
    public void Test_ScanDeltaCatalog_With_Minio_And_WireMock()
    {
        // ==========================================
        // 1. 准备物理路径和 Endpoint (针对 MinIO)
        // ==========================================
        var catalog = "main";
        var schema = "default";
        var table = $"delta_catalog_test_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-super-secret-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        // ==========================================
        // 2. 真实写入阶段：使用已有验证通过的 WriteDelta
        // ==========================================
        // 写入用的 Options，需要直接包含 AccessKey 和 SecretKey
        var writeOptions = CloudOptions.Aws(
            region: _minio.Region,
            accessKey: _minio.AccessKey,
            secretKey: _minio.SecretKey,
            endpoint: polarsEndpoint
        );
        writeOptions.Credentials!["AWS_ALLOW_HTTP"] = "true";
        writeOptions.Credentials!["aws_allow_http"] = "true";
        writeOptions.Credentials!["AWS_S3_FORCE_PATH_STYLE"] = "true";
        writeOptions.Credentials!["aws_s3_force_path_style"] = "true";
        writeOptions.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        Console.WriteLine("Step 1: Real Write (Create Table) directly to MinIO...");
        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3 },
            Msg = new[] { "Alice", "Bob", "Charlie" }
        }))
        {
            // 写入真实的 Parquet 和 _delta_log 文件夹
            df.WriteDelta(s3StorageLocation, mode: DeltaSaveMode.Append, cloudOptions: writeOptions);
        }

        // ==========================================
        // 3. 配置 WireMock 扮演 Databricks Unity Catalog
        // ==========================================
        Console.WriteLine("Step 2: Setup WireMock to act as Unity Catalog...");
        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        // ==========================================
        // 4. Catalog 读取阶段：使用 ScanDeltaCatalog
        // ==========================================
        Console.WriteLine("Step 3: Scan data using Catalog mechanism...");
        
        // 读取用的 Options，不需要（也不应该）包含长期 Key，它们会由 WireMock 动态下发！
        // 我们只保留 MinIO 需要的 Endpoint 和基础 HTTP 设置
        var readOptions = CloudOptions.Aws(
            region: _minio.Region,
            endpoint: polarsEndpoint
        );
        readOptions.Credentials!["aws_allow_http"] = "true";
        readOptions.Credentials!["aws_s3_force_path_style"] = "true";

        // 发起请求：
        // A. 实例化咱们新设计的 UnityCatalog 对象
        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);
        
        // B. 使用 Catalog 对象的上下文去扫描表！
        using var lf = uc.ScanCatalogTable(
            catalogName: catalog,
            schemaName: schema,
            tableName: table,
            cloudOptions: readOptions
            // 注意：咱们刚加的 version 和 datetime 参数如果不传就是 null，默认读最新版！
        );

        using var resultDf = lf.Collect();
        resultDf.Show();

        // ==========================================
        // 5. 验证闭环结果
        // ==========================================
        Assert.Equal(3, resultDf.Height);
        Assert.Contains("Alice", resultDf["Msg"].ToArray<string>());
        Assert.Contains("Charlie", resultDf["Msg"].ToArray<string>());

        Console.WriteLine("Success! Data successfully retrieved via Unity Catalog Mock.");
    }
    [Fact]
    [Trait("Catalog", "Sink")]
    public void Test_SinkCatalogTable_Basic_Overwrite_And_Append()
    {
        // ==========================================
        // 1. 环境准备与 Mock 部署
        // ==========================================
        var catalog = "main";
        var schema = "default";
        var table = $"delta_catalog_sink_basic_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-sink-secret-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        // 一行代码部署 WireMock 拦截网！
        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        // 写入和读取用的 Options：剥离所有身份信息，只留网络路由
        var cloudOptions = CloudOptions.Aws(region: _minio.Region, endpoint: polarsEndpoint);
        cloudOptions.Credentials!["aws_allow_http"] = "true";
        cloudOptions.Credentials!["aws_s3_force_path_style"] = "true";

        // ==========================================
        // 2. 第一次写入：Overwrite (无中生有建表)
        // ==========================================
        Console.WriteLine("Step 1: Overwrite via Catalog (Initialize Table)...");
        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);

        using (var df1 = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2 },
            Name = new[] { "Alice", "Bob" }
        }))
        {
            // 调用咱们刚写的绝赞扩展方法！
            // 注意：不传 partitionBy，测试默认推断逻辑
            df1.WriteCatalogTable(
                uc, catalog, schema, table, 
                mode: DeltaSaveMode.Overwrite, 
                cloudOptions: cloudOptions
            );
        }

        // ==========================================
        // 3. 第二次写入：Append (追加数据)
        // ==========================================
        Console.WriteLine("Step 2: Append via Catalog (Add Data)...");
        using (var df2 = DataFrame.FromColumns(new { 
            Id = new[] { 3, 4 },
            Name = new[] { "Charlie", "David" }
        }))
        {
            df2.WriteCatalogTable(
                uc, catalog, schema, table, 
                mode: DeltaSaveMode.Append, 
                cloudOptions: cloudOptions
            );
        }

        // ==========================================
        // 4. 闭环验证：使用 Catalog 机制读取并核对
        // ==========================================
        Console.WriteLine("Step 3: Scan and Verify...");
        
        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect().Sort(["Id"]);
        
        resultDf.Show();

        // 验证：2行 Overwrite + 2行 Append = 4行
        Assert.Equal(4, resultDf.Height);
        Assert.Equal(1, resultDf["Id"].ToArray<int>()[0]);
        Assert.Equal("David", resultDf["Name"].ToArray<string>()[3]);

        Console.WriteLine("Success! Data successfully Overwritten and Appended via Unity Catalog Mock.");
    }
    [Fact]
    [Trait("Catalog", "SinkAdvanced")]
    public void Test_SinkCatalogTable_Advanced_Modes_And_AutoPartition()
    {
        // ==========================================
        // 1. 环境准备与 Mock 部署
        // ==========================================
        var catalog = "main";
        var schema = "default";
        var table = $"delta_catalog_sink_adv_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-adv-secret-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        var cloudOptions = CloudOptions.Aws(region: _minio.Region, endpoint: polarsEndpoint);
        cloudOptions.Credentials!["aws_allow_http"] = "true";
        cloudOptions.Credentials!["aws_s3_force_path_style"] = "true";

        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);

        // ==========================================
        // 2. 初始建表：显式指定分区策略
        // ==========================================
        Console.WriteLine("Step 1: Overwrite with explicit partition (Initialize Table)...");
        using (var df1 = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2 },
            Region = new[] { "North", "South" }
        }))
        {
            df1.WriteCatalogTable(
                uc, catalog, schema, table, 
                mode: DeltaSaveMode.Overwrite, 
                // 【核心】：第一次建表时，告诉引擎我们要按 Region 分区
                partitionBy: Selector.Col("Region"), 
                cloudOptions: cloudOptions
            );
        }

        // ==========================================
        // 3. 测试魔法 1：自动推断分区
        // ==========================================
        Console.WriteLine("Step 2: Append with Auto-Partition Inference...");
        using (var df2 = DataFrame.FromColumns(new { 
            Id = new[] { 3, 4 },
            Region = new[] { "East", "North" }
        }))
        {
            df2.WriteCatalogTable(
                uc, catalog, schema, table, 
                mode: DeltaSaveMode.Append, 
                // 【魔法生效】：我不传 partitionBy，Rust 底层必须自己去查现有的 Delta 元数据并按 Region 切分文件！
                partitionBy: null, 
                cloudOptions: cloudOptions
            );
        }

        // ==========================================
        // 4. 测试魔法 2：ErrorIfExists 拦截
        // ==========================================
        Console.WriteLine("Step 3: SaveMode.ErrorIfExists (Should Throw)...");
        using (var dfError = DataFrame.FromColumns(new { Id = new[] { 5 }, Region = new[] { "West" } }))
        {
            var ex = Assert.Throws<PolarsException>(() => 
            {
                dfError.WriteCatalogTable(
                    uc, catalog, schema, table, 
                    mode: DeltaSaveMode.ErrorIfExists, 
                    cloudOptions: cloudOptions
                );
            });
            // 验证报错信息正是咱们在 Rust 抛出的
            Assert.Contains("already exists", ex.Message); 
            Console.WriteLine(" -> Exception caught correctly!");
        }

        // ==========================================
        // 5. 测试魔法 3：Ignore 幂等跳过
        // ==========================================
        Console.WriteLine("Step 4: SaveMode.Ignore (Should Do Nothing)...");
        using (var dfIgnore = DataFrame.FromColumns(new { Id = new[] { 6 }, Region = new[] { "West" } }))
        {
            dfIgnore.WriteCatalogTable(
                uc, catalog, schema, table, 
                mode: DeltaSaveMode.Ignore, 
                cloudOptions: cloudOptions
            );
        }

        // ==========================================
        // 6. 终极闭环验证
        // ==========================================
        Console.WriteLine("Step 5: Scan and Verify Data Integrity...");
        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect().Sort(["Id"]);
        resultDf.Show();

        // 验证：
        // 1. 只有 Step 1 和 2 的 4 条数据
        // 2. Ignore 写入的 Id=6 的数据绝对不能出现
        Assert.Equal(4, resultDf.Height);
        
        var idArray = resultDf["Id"].ToArray<int>();
        Assert.Contains(1, idArray);
        Assert.Contains(3, idArray);
        Assert.DoesNotContain(6, idArray);

        Console.WriteLine("Success! All advanced SaveModes and Auto-Partitioning validated perfectly!");
    }
    [Fact]
    [Trait("Catalog", "Concurrent")]
    public async Task Test_Concurrent_SinkCatalogTable_Append_Stress_TestAsync()
    {
        // ==========================================
        // 1. 环境准备与 Mock 部署
        // ==========================================
        var catalog = "main";
        var schema = "default";
        var table = $"delta_catalog_concurrent_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-concurrent-secret-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        // 部署拦截网：所有并发线程都会疯狂打这个 Mock 接口拿凭证
        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        // 干干净净的 Options，全靠 Catalog 下发 STS Token
        var cloudOptions = CloudOptions.Aws(region: _minio.Region, endpoint: polarsEndpoint);
        cloudOptions.Credentials!["aws_allow_http"] = "true";
        cloudOptions.Credentials!["aws_s3_force_path_style"] = "true";
        // 【防坑必带】：S3 本地并发重命名必备
        cloudOptions.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"; 

        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);

        // ==========================================
        // 2. 初始写入 (打好 Version 0 的地基)
        // ==========================================
        Console.WriteLine("Step 1: Initializing Table via Catalog (Version 0)...");
        using (var dfInit = DataFrame.FromColumns(new { WorkerId = new[] { 0 }, RowId = new[] { 0 } }))
        {
            // 通过 Catalog 写入第一条记录
            dfInit.WriteCatalogTable(
                uc, catalog, schema, table, 
                mode: DeltaSaveMode.Overwrite, 
                cloudOptions: cloudOptions
            );
        }

        // ==========================================
        // 3. 并发写入风暴 (Stress Test)
        // ==========================================
        Console.WriteLine("Step 2: Starting Concurrent Appends via Catalog...");
        int concurrency = 10;
        int rowsPerWorker = 100;
        var tasks = new List<Task>();

        for (int i = 0; i < concurrency; i++)
        {
            int workerId = i + 1;
            tasks.Add(Task.Run(() =>
            {
                try 
                {
                    // 构造该 Worker 的专属数据
                    var workerIds = Enumerable.Repeat(workerId, rowsPerWorker).ToArray();
                    var rowIds = Enumerable.Range(1, rowsPerWorker).ToArray();

                    using var sourceDf = DataFrame.FromColumns(new { WorkerId = workerIds, RowId = rowIds });

                    // 10 个线程同时向 Catalog 开火！
                    sourceDf.WriteCatalogTable(
                        uc, catalog, schema, table, 
                        mode: DeltaSaveMode.Append, 
                        cloudOptions: cloudOptions
                    );
                    
                    Console.WriteLine($"[Worker {workerId}] Success!");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Worker {workerId}] FAILED: {ex.Message}");
                    throw; // 抛出异常让测试失败
                }
            }));
        }

        // 等待所有厮杀结束
        await Task.WhenAll(tasks);

        // ==========================================
        // 4. 终极闭环验证
        // ==========================================
        Console.WriteLine("Step 3: Scan and Verify Data Integrity...");
        
        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect();

        // 验证 A: 总行数 = 初始 1 行 + (10 个 worker * 100 行) = 1001 行
        long expectedHeight = 1 + (concurrency * rowsPerWorker);
        Assert.Equal(expectedHeight, resultDf.Height);

        // 验证 B: 绝对没有数据丢失！每个 Worker 的 100 行都在
        for (int i = 1; i <= concurrency; i++)
        {
            var workerRowCount = resultDf.Filter(Col("WorkerId") == i).Height;
            Assert.Equal(rowsPerWorker, workerRowCount);
        }

        Console.WriteLine("Catalog Concurrent Write Stress Test Passed Perfectly! 🚀");
    }
    [Fact]
    [Trait("Catalog", "Roundtrip")]
    public void Test_CatalogTable_Full_Roundtrip_DDL_and_DML()
    {
        // ==========================================
        // 1. 环境准备 (完全剥离长期 AWS 凭证)
        // ==========================================
        var catalog = "main";
        var schema = "default";
        var table = $"delta_catalog_roundtrip_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-roundtrip-secret-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        // 核心亮点：不管是读、写还是建表，都只用这一个不包含 AccessKey 的 options！
        var cloudOptions = CloudOptions.Aws(region: _minio.Region, endpoint: polarsEndpoint);
        cloudOptions.Credentials!["aws_allow_http"] = "true";
        cloudOptions.Credentials!["aws_s3_force_path_style"] = "true";
        cloudOptions.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        // ==========================================
        // 2. 部署 WireMock (读/写/建/删 全家桶)
        // ==========================================
        Console.WriteLine("Step 1: Setup WireMock DDL & DML endpoints...");
        
        // A. 基础的读写拦截网 (Get Table Info & Get Credentials)
        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        // B. 补充 Mock：建表接口 (POST /tables)
        _catalogMockServer
            .Given(Request.Create().WithPath("/api/2.1/unity-catalog/tables").UsingPost().WithHeader("Authorization", $"Bearer {expectedToken}"))
            .RespondWith(Response.Create().WithStatusCode(200).WithBodyAsJson(new {
                name = table, 
                catalog_name = catalog, 
                schema_name = schema,
                table_type = "EXTERNAL", 
                data_source_format = "DELTA",
                storage_location = s3StorageLocation, 
                table_id = Guid.NewGuid().ToString(),
                
                // 【填坑】：这四个字段在创建表成功后的返回体里也必须存在！
                created_at = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                created_by = "test_admin",
                updated_at = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                updated_by = "test_admin"
            }));

        // C. 补充 Mock：删表接口 (DELETE /tables/{full_name})
        _catalogMockServer
            .Given(Request.Create().WithPath($"/api/2.1/unity-catalog/tables/{catalog}.{schema}.{table}").UsingDelete().WithHeader("Authorization", $"Bearer {expectedToken}"))
            .RespondWith(Response.Create().WithStatusCode(200));

        // ==========================================
        // 3. 实例化 UnityCatalog
        // ==========================================
        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);

        // ==========================================
        // 4. [DDL] 显式创建 Catalog 表
        // ==========================================
        Console.WriteLine("Step 2: Create Table (DDL) via Catalog...");
        using (var tableSchema = PolarsSchema.From(new Dictionary<string, DataType>
        {
            { "Id", DataType.Int32 },
            { "Msg", DataType.String }
        }))
        {
            uc.CreateCatalogTable(
                catalog, schema, table, 
                tableSchema, 
                CatalogTableType.External, 
                s3StorageLocation
            );
        }

        // ==========================================
        // 5. [DML] 写入数据 (Catalog Sink)
        // ==========================================
        Console.WriteLine("Step 3: Write Data (DML) via Catalog...");
        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3 },
            Msg = new[] { "Alice", "Bob", "Charlie" }
        }))
        {
            // 底层会去拿 STS Token 并写往 S3
            df.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Overwrite, cloudOptions: cloudOptions);
        }

        // ==========================================
        // 6. [DQL] 读取并验证数据 (Catalog Scan)
        // ==========================================
        Console.WriteLine("Step 4: Scan and Verify (DQL) via Catalog...");
        using (var lf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions))
        using (var resultDf = lf.Collect().Sort(["Id"]))
        {
            resultDf.Show();

            Assert.Equal(3, resultDf.Height);
            Assert.Equal(1, resultDf["Id"].ToArray<int>()[0]);
            Assert.Contains("Charlie", resultDf["Msg"].ToArray<string>());
        }

        // ==========================================
        // 7. [DDL] 清理：删除 Catalog 表
        // ==========================================
        Console.WriteLine("Step 5: Drop Table (DDL) via Catalog...");
        uc.DeleteCatalogTable(catalog, schema, table);
        
        // 如果删表接口被成功触发，测试就不会抛错

        Console.WriteLine("Success! Unity Catalog Full Roundtrip (Create -> Write -> Scan -> Delete) completed perfectly!");
    }
    [Fact]
    [Trait("Catalog", "Delete")]
    public void Test_Catalog_Delete_Full_Cycle_Logic()
    {
        // ==========================================
        // 1. 环境准备与 Mock 部署
        // ==========================================
        var catalog = "main";
        var schema = "default";
        var table = $"delta_catalog_delete_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-delete-secret-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        // 部署读写拦截网
        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        // 纯净版 Options
        var cloudOptions = CloudOptions.Aws(region: _minio.Region, endpoint: polarsEndpoint);
        cloudOptions.Credentials!["aws_allow_http"] = "true";
        cloudOptions.Credentials!["aws_s3_force_path_style"] = "true";
        cloudOptions.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);

        // ==========================================
        // 2. 初始化写入 (Version 0) - 带有分区
        // ==========================================
        Console.WriteLine("Step 1: Initial Write (Partitioned by Year)...");
        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3, 4, 5 }, 
            Msg = new[] { "A", "B", "C", "D", "E" },
            Year = new[] { "2023", "2023", "2024", "2024", "2024" }
        }))
        {
            // 注意这里测试一下咱们上一轮加的 partitionBy
            df.WriteCatalogTable(
                uc, catalog, schema, table, 
                partitionBy: Selector.Col("Year"), 
                mode: DeltaSaveMode.Overwrite, 
                cloudOptions: cloudOptions
            );
        }

        // 验证 V0 (5 行数据)
        using var dfV0 = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions).Collect();
        Assert.Equal(5, dfV0.Height);

        // ==========================================
        // 3. 执行删除：部分删除 (Rewrite Test) -> 产生 Version 1
        // ==========================================
        Console.WriteLine("Step 2: Delete Row (Id=4) - Rewrite Partition 2024...");
        
        // Predicate: (Year == '2024') & (Id == 4)
        var predicateRewrite = (Col("Year") == Lit("2024")) & (Col("Id") == 4);
        
        // 呼叫全新的 DeleteCatalogRecords API！
        uc.DeleteCatalogRecords(catalog, schema, table, predicateRewrite, cloudOptions: cloudOptions);

        // 验证 V1 (应该剩 4 行: 1,2,3,5)
        using var dfV1 = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions).Collect().Sort("Id");
        Assert.Equal(4, dfV1.Height);
        Assert.DoesNotContain(4, dfV1["Id"].ToArray<int>());
        Assert.Contains(3, dfV1["Id"].ToArray<int>());
        Assert.Contains(5, dfV1["Id"].ToArray<int>());

        // ==========================================
        // 4. 执行删除：整分区删除 (Drop Partition) -> 产生 Version 2
        // ==========================================
        Console.WriteLine("Step 3: Delete Partition (Year='2023') - Drop Files...");
        
        var predicateDrop = Col("Year") == Lit("2023");
        
        uc.DeleteCatalogRecords(catalog, schema, table, predicateDrop, cloudOptions: cloudOptions);

        // 验证 V2 (应该剩 2 行: 3, 5)
        using var dfV2 = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions).Collect().Sort("Id");
        Assert.Equal(2, dfV2.Height);
        Assert.DoesNotContain(1, dfV2["Id"].ToArray<int>());
        Assert.DoesNotContain(2, dfV2["Id"].ToArray<int>());
        Assert.Equal("2024", dfV2["Year"].ToArray<string>()[0]); // 只剩 2024 的数据了

        // ==========================================
        // 5. 执行删除：无匹配 (No-op Test)
        // ==========================================
        Console.WriteLine("Step 4: Delete Non-existent (Id=999) - No-op...");
        
        var predicateNoOp = Col("Id") == 999;
        uc.DeleteCatalogRecords(catalog, schema, table, predicateNoOp, cloudOptions: cloudOptions);

        // 验证数据无变化
        using var dfV3 = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions).Collect();
        Assert.Equal(2, dfV3.Height);

        // ==========================================
        // 6. Time Travel 验证
        // ==========================================
        Console.WriteLine("Step 5: Time Travel Check...");
        
        // 回溯到 V1 (包含全部 5 行)
        using var dfBackV0 = uc.ScanCatalogTable(catalog, schema, table, version: 1, cloudOptions: cloudOptions).Collect();
        Assert.Equal(5, dfBackV0.Height);
        
        // 回溯到 V2 (删除 Id=4 后，剩 4 行)
        using var dfBackV1 = uc.ScanCatalogTable(catalog, schema, table, version: 2, cloudOptions: cloudOptions).Collect();
        Assert.Equal(4, dfBackV1.Height);
        Assert.DoesNotContain(4, dfBackV1["Id"].ToArray<int>());

        Console.WriteLine("Catalog Delete Full Cycle Passed Perfectly! ");
    }
    [Fact]
    [Trait("Catalog", "DeleteConcurrent")]
    public async Task Test_Concurrent_Catalog_Delete_Conflict_Stress_TestAsync()
    {
        // ==========================================
        // 1. 环境准备与 Mock 部署
        // ==========================================
        var catalog = "main";
        var schema = "default";
        var table = $"delta_catalog_concurrent_del_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-concurrent-del-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        var cloudOptions = CloudOptions.Aws(region: _minio.Region, endpoint: polarsEndpoint);
        cloudOptions.Credentials!["aws_allow_http"] = "true";
        cloudOptions.Credentials!["aws_s3_force_path_style"] = "true";
        cloudOptions.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);

        // ==========================================
        // 2. 初始写入：10 行数据，全塞在一个文件里（不设分区）
        // ==========================================
        Console.WriteLine("Step 1: Initializing Table with 10 rows in a single file...");
        using (var dfInit = DataFrame.FromColumns(new { 
            Id = Enumerable.Range(1, 10).ToArray(),
            Msg = Enumerable.Repeat("Target", 10).ToArray()
        }))
        {
            dfInit.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Overwrite, cloudOptions: cloudOptions);
        }

        // ==========================================
        // 3. 并发删除风暴 (大逃杀开始)
        // ==========================================
        Console.WriteLine("Step 2: Starting Concurrent Deletes on the SAME file...");
        
        int concurrency = 5;
        var tasks = new List<Task>();
        
        // 用来统计战况
        int successCount = 0;
        int conflictCount = 0;

        for (int i = 0; i < concurrency; i++)
        {
            int targetId = i + 1; // 每个 worker 试图删除不同的 Id (1 到 5)
            tasks.Add(Task.Run(() =>
            {
                try 
                {
                    var predicate = Col("Id") == targetId;
                    uc.DeleteCatalogRecords(catalog, schema, table, predicate, cloudOptions: cloudOptions);
                    
                    Console.WriteLine($"[Worker {targetId}] WINNER! Successfully deleted Id={targetId}.");
                    Interlocked.Increment(ref successCount);
                }
                catch (Exception ex)
                {
                    // 期待捕获到底层的 Commit 冲突异常
                    Console.WriteLine($"[Worker {targetId}] BLOCKED by OCC lock: {ex.Message}");
                    Interlocked.Increment(ref conflictCount);
                }
            }));
        }

        await Task.WhenAll(tasks);

        // ==========================================
        // 4. 战报核验 (Data Integrity Check)
        // ==========================================
        Console.WriteLine("\nStep 3: Verifying Battlefield...");
        Console.WriteLine($"Total Success: {successCount}, Total Conflicts: {conflictCount}");

        // 由于时间差，可能不止1个成功（如果某个 worker 启动慢，在别人 commit 之后才 load，那它就会成功），
        // 但肯定会有失败的，且 成功数 + 失败数 必须 = 5
        Assert.True(successCount >= 1, "At least one deletion should succeed.");
        Assert.Equal(concurrency, successCount + conflictCount);

        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect();
        
        resultDf.Show();

        // 终极一致性断言：剩余的数据行数 = 初始 10 行 - 成功的删除次数
        // 如果这里通过了，说明引擎的原子性和锁机制完美无瑕，绝对没有脏覆盖！
        Assert.Equal(10 - successCount, resultDf.Height);

        Console.WriteLine("Catalog Concurrent Delete Stress Test Passed! OCC is working perfectly. ");
    }
    [Fact]
    [Trait("Catalog", "Chaos")]
    public async Task Test_Concurrent_Chaos_Mixed_Append_Delete_Merge_Async()
    {
        // ==========================================
        // 0. 注入“鸡血”：调高引擎重试上限，允许 20 次大乱斗
        // ==========================================
        PolarsConfig.SetEnvVar("POLARS_DELTA_MAX_RETRIES", "20");

        // ==========================================
        // 1. 环境准备与 Mock 部署
        // ==========================================
        var catalog = "main";
        var schema = "default";
        var table = $"delta_chaos_ultimate_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-chaos-ultimate-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        var cloudOptions = CloudOptions.Aws(region: _minio.Region, endpoint: polarsEndpoint);
        cloudOptions.Credentials!["aws_allow_http"] = "true";
        cloudOptions.Credentials!["aws_s3_force_path_style"] = "true";
        cloudOptions.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);

        // ==========================================
        // 2. 初始阵地：写入 50 行基础数据 (ID: 1 ~ 50)
        // ==========================================
        Console.WriteLine("Step 1: Setting up the battlefield (50 initial rows)...");
        using (var dfInit = DataFrame.FromColumns(new { 
            Id = Enumerable.Range(1, 50).ToArray(),
            Team = Enumerable.Repeat("Init", 50).ToArray()
        }))
        {
            dfInit.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Overwrite, cloudOptions: cloudOptions);
        }

        // ==========================================
        // 3. 混沌大逃杀：5个Append vs 5个Delete vs 5个Merge
        // ==========================================
        Console.WriteLine("Step 2: Unleashing TRUE Chaos (Append vs Delete vs Merge)...");
        
        var tasks = new List<Task>();
        int concurrency = 5;

        // 【🔥 Team Fire：疯狂写入】 (101~150)
        for (int i = 0; i < concurrency; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(() =>
            {
                var startId = 100 + (workerId * 10) + 1;
                using var dfAppend = DataFrame.FromColumns(new { 
                    Id = Enumerable.Range(startId, 10).ToArray(), 
                    Team = Enumerable.Repeat($"Writer_{workerId}", 10).ToArray() 
                });
                
                dfAppend.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Append, cloudOptions: cloudOptions);
                Console.WriteLine($"[Team Fire] Writer {workerId} appended IDs {startId} to {startId+9}.");
            }));
        }

        // 【🧊 Team Ice：疯狂删除】 (1~50)
        for (int i = 0; i < concurrency; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(() =>
            {
                var minId = (workerId * 10) + 1;
                var maxId = minId + 9;
                
                var predicate = (Col("Id") >= minId) & (Col("Id") <= maxId);
                uc.DeleteCatalogRecords(catalog, schema, table, predicate, cloudOptions: cloudOptions);
                Console.WriteLine($"[Team Ice] Deleter {workerId} deleted IDs {minId} to {maxId}.");
            }));
        }

        // 【⚡ Team Lightning：极限合并】 (201~250)
        for (int i = 0; i < concurrency; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(() =>
            {
                var startId = 200 + (workerId * 10) + 1;
                using var dfMerge = DataFrame.FromColumns(new { 
                    Id = Enumerable.Range(startId, 10).ToArray(), 
                    Team = Enumerable.Repeat($"Merger_{workerId}", 10).ToArray() 
                });

                uc.MergeCatalogRecords(catalog, schema, table, dfMerge, ["Id"], cloudOptions: cloudOptions)
                  .WhenMatchedUpdate()
                  .WhenNotMatchedInsert()
                  .Execute();
                  
                Console.WriteLine($"[Team Lightning] Merger {workerId} upserted IDs {startId} to {startId+9}.");
            }));
        }

        // 观战，直到全部 15 个操作完成
        await Task.WhenAll(tasks);

        // ==========================================
        // 4. 打扫战场：终极一致性校验
        // ==========================================
        Console.WriteLine("\nStep 3: Checking Battlefield Integrity...");
        
        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect().Sort("Id");
        
        // 极致的数学题对账：
        // 初始 50 - 被刺客删掉 50 + 狂战士加的 50 + 闪电军团Upsert的 50 = 最终 100 行！
        Assert.Equal(100, resultDf.Height);

        var remainingIds = resultDf["Id"].ToArray<int>();

        // 校验 1：1~50 必须死透了 (被 Team Ice 干掉)
        Assert.DoesNotContain(1, remainingIds);
        Assert.DoesNotContain(50, remainingIds);

        // 校验 2：101~150 必须全都在 (Team Fire 追加)
        Assert.Contains(101, remainingIds);
        Assert.Contains(150, remainingIds);

        // 校验 3：201~250 必须全都在 (Team Lightning 插入)
        Assert.Contains(201, remainingIds);
        Assert.Contains(250, remainingIds);

        Console.WriteLine("ULTIMATE 3-WAY CHAOS TEST PASSED! Append, Delete, and Merge survived together!");
        
        // 测试完把环境变量重置
        Environment.SetEnvironmentVariable("POLARS_DELTA_MAX_RETRIES", null);
    }
    [Fact]
    [Trait("Catalog", "FourWayChaos")]
    public async Task Test_Ultimate_Chaos_DV_Append_Delete_Merge_Optimize_Async()
    {
        // ==========================================
        // 0. 注入“鸡血”：调高引擎重试上限，允许 20 次大乱斗
        // ==========================================
        PolarsConfig.SetEnvVar("POLARS_DELTA_MAX_RETRIES", "30");

        // ==========================================
        // 1. 环境准备与 Mock 部署
        // ==========================================
        var catalog = "main";
        var schema = "default";
        var table = $"delta_chaos_dv_ultimate_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-chaos-dv-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        var cloudOptions = CloudOptions.Aws(region: _minio.Region, endpoint: polarsEndpoint);
        cloudOptions.Credentials!["aws_allow_http"] = "true";
        cloudOptions.Credentials!["aws_s3_force_path_style"] = "true";
        cloudOptions.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);

        // ==========================================
        // 2. 初始阵地与 DV 激活
        // ==========================================
        Console.WriteLine("Step 1: Setting up the battlefield (50 initial rows)...");
        using (var dfInit = DataFrame.FromColumns(new { 
            Id = Enumerable.Range(1, 50).ToArray(),
            Team = Enumerable.Repeat("Init", 50).ToArray(),
            Value = Enumerable.Repeat(1.0, 50).ToArray()
        }))
        {
            dfInit.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Overwrite, cloudOptions: cloudOptions);
        }

        // 【核心操作】：直接使用物理路径调用原生 API，为该表开启 Deletion Vectors！
        Console.WriteLine("Step 1.5: Activating Deletion Vectors (DV)...");
        Delta.AddFeature(
            s3StorageLocation, // 使用物理路径强制开启底层 Feature
            DeltaTableFeatures.DeletionVectors, 
            allowProtocolIncrease: true, 
            cloudOptions: cloudOptions
        );

        // ==========================================
        // 3. 混沌机制：模拟网络延迟与假死重试
        // ==========================================
        // 我们需要保证最终数据一致性，所以这里模拟的是“带有重试机制的脆弱 Worker”
        static async Task ExecuteWithChaosAsync(string workerName, Action action)
        {
            var rnd = new Random(Guid.NewGuid().GetHashCode());
            int maxWorkerRetries = 100;

            for (int attempt = 1; attempt <= maxWorkerRetries; attempt++)
            {
                // 1. 模拟网络抖动，随机延迟 10ms ~ 300ms 进场
                await Task.Delay(rnd.Next(10, 300));

                try
                {
                    // 2. 模拟 20% 概率的 Worker 进程崩溃/网络断开 (直接抛错)
                    if (rnd.NextDouble() < 0.20)
                    {
                        throw new Exception("Simulated Transient Network Failure!");
                    }

                    // 3. 执行真正的业务动作 (交由底层的 Rust Jitter 扛并发)
                    action();
                    return; // 成功则退出重试
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Chaos Simulator] {workerName} failed on attempt {attempt}: {ex.Message}");
                    if (attempt == maxWorkerRetries)
                        throw; // 彻底挂了，抛出让测试 Fail
                }
            }
        }

        // ==========================================
        // 4. 终极大乱斗：四军混战
        // ==========================================
        Console.WriteLine("Step 2: Unleashing DV + Optimize TRUE Chaos...");
        
        var tasks = new List<Task>();
        int concurrency = 5;

        // 【🔥 Team Fire：疯狂写入】 (101~150)
        for (int i = 0; i < concurrency; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(() => ExecuteWithChaosAsync($"Writer_{workerId}", () =>
            {
                var startId = 100 + (workerId * 10) + 1;
                using var dfAppend = DataFrame.FromColumns(new { 
                    Id = Enumerable.Range(startId, 10).ToArray(), 
                    Team = Enumerable.Repeat($"Writer_{workerId}", 10).ToArray(),
                    Value = Enumerable.Repeat(1.0, 10).ToArray() 
                });
                dfAppend.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Append, cloudOptions: cloudOptions);
                Console.WriteLine($"[Team Fire] Writer {workerId} appended IDs {startId} to {startId+9}.");
            })));
        }

        // 【🧊 Team Ice：疯狂 DV 删除】 (1~50)
        for (int i = 0; i < concurrency; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(() => ExecuteWithChaosAsync($"Deleter_{workerId}", () =>
            {
                var minId = (workerId * 10) + 1;
                var maxId = minId + 9;
                var predicate = (Col("Id") >= minId) & (Col("Id") <= maxId);
                uc.DeleteCatalogRecords(catalog, schema, table, predicate, cloudOptions: cloudOptions);
                Console.WriteLine($"[Team Ice] Deleter {workerId} deleted IDs {minId} to {maxId} (using DV!).");
            })));
        }

        // 【⚡ Team Lightning：极限合并】 (201~250)
        for (int i = 0; i < concurrency; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(() => ExecuteWithChaosAsync($"Merger_{workerId}", () =>
            {
                var startId = 200 + (workerId * 10) + 1;
                using var dfMerge = DataFrame.FromColumns(new { 
                    Id = Enumerable.Range(startId, 10).ToArray(), 
                    Team = Enumerable.Repeat($"Merger_{workerId}", 10).ToArray(),
                    Value = Enumerable.Repeat(99.9, 10).ToArray() 
                });

                uc.MergeCatalogRecords(catalog, schema, table, dfMerge, ["Id"], cloudOptions: cloudOptions)
                  .WhenMatchedUpdate()
                  .WhenNotMatchedInsert()
                  .Execute();
                Console.WriteLine($"[Team Lightning] Merger {workerId} upserted IDs {startId} to {startId+9}.");
            })));
        }

        // 【🛠️ Team Architect：并发 Optimize】 (3 个 Worker，负责把刚生成的 DV 和小碎片疯狂压实)
        for (int i = 0; i < 3; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(() => ExecuteWithChaosAsync($"Optimizer_{workerId}", () =>
            {
                long filesOptimized = uc.OptimizeCatalogTable(
                    catalog, schema, table, 
                    targetSizeMb: 128, 
                    zOrderColumns: ["Id"], 
                    cloudOptions: cloudOptions
                );
                Console.WriteLine($"[Team Architect] Optimizer {workerId} successfully compacted {filesOptimized} files.");
            })));
        }

        // 观战，直到全部 18 个操作完成
        await Task.WhenAll(tasks);

        // ==========================================
        // 5. 打扫战场：终极一致性校验
        // ==========================================
        Console.WriteLine("\nStep 3: Checking Battlefield Integrity...");
        
        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect().Sort("Id");

        // 极致对账：初始 50 - 删 50 + 写 50 + 合并 50 = 最终 100 行！
        Assert.Equal(100, resultDf.Height);

        var remainingIds = resultDf["Id"].ToArray<int>();

        // 校验 1：1~50 必须死透了 (Team Ice 利用 DV 删掉，可能被 Optimize 清理了物理行)
        Assert.DoesNotContain(1, remainingIds);
        Assert.DoesNotContain(50, remainingIds);

        // 校验 2：101~150 必须全都在 (Team Fire 追加)
        Assert.Contains(101, remainingIds);
        Assert.Contains(150, remainingIds);

        // 校验 3：201~250 必须全都在 (Team Lightning 插入)
        Assert.Contains(201, remainingIds);
        Assert.Contains(250, remainingIds);

        long newVersion = uc.DeltaRestore(
            catalog, schema, table, 
            version: 1, 
            cloudOptions: cloudOptions
        );

        Console.WriteLine($"--- New Version is {newVersion} ---");

        // 校验回滚后的状态：只剩最初的 50 条英雄
        using var restoredLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var restoredDf = restoredLf.Collect().Sort("Id");
        
        Console.WriteLine("--- Restored Data ---");
        restoredDf.Show();
        Assert.Equal(50, restoredDf.Height);

        using var historyDf = uc.DeltaHistory(catalog, schema, table, cloudOptions: cloudOptions);
        
        Console.WriteLine("--- Table History ---");
        historyDf.Show();

        long deletedFiles = uc.DeltaVacuum(
            catalog, schema, table,
            retentionHours: 0,          
            enforceRetention: false,    
            cloudOptions: cloudOptions
        );

        Console.WriteLine($"--- Vacuum deleted {deletedFiles} orphaned files ---");

        Console.WriteLine("ULTIMATE 4-WAY CHAOS TEST (DV + OPTIMIZE + JITTER + VACUUM + RESTORE + HISTORY) PASSED! ");
        
        // 测试完把环境变量重置
        Environment.SetEnvironmentVariable("POLARS_DELTA_MAX_RETRIES", null);
    }
    [Fact]
    [Trait("Catalog", "DeleteDV")]
    public async Task Test_Isolation_Pure_Delete_With_DV_Async()
    {
        // ==========================================
        // 1. 环境准备与 Mock 部署
        // ==========================================
        var catalog = "main";
        var schema = "default";
        var table = $"delta_iso_delete_dv_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-iso-delete-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        var cloudOptions = CloudOptions.Aws(region: _minio.Region, endpoint: polarsEndpoint);
        cloudOptions.Credentials!["aws_allow_http"] = "true";
        cloudOptions.Credentials!["aws_s3_force_path_style"] = "true";
        cloudOptions.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);

        // ==========================================
        // 2. 初始阵地：写入 50 行基础数据
        // ==========================================
        Console.WriteLine("Step 1: Writing initial 50 rows...");
        using (var dfInit = DataFrame.FromColumns(new { 
            Id = Enumerable.Range(1, 50).ToArray(),
            Team = Enumerable.Repeat("Init", 50).ToArray(),
            Value = Enumerable.Repeat(1.0, 50).ToArray()
        }))
        {
            dfInit.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Overwrite, cloudOptions: cloudOptions);
        }

        // ==========================================
        // 3. 开启 Deletion Vectors 特性 (MoR 模式)
        // ==========================================
        Console.WriteLine("Step 2: Enabling Deletion Vectors...");
        Delta.AddFeature(
            s3StorageLocation, // 使用物理路径强制开启底层 Feature
            "deletionVectors", 
            allowProtocolIncrease: true, 
            cloudOptions: cloudOptions
        );

        // ==========================================
        // 4. 执行纯 Delete 操作 (利用 Catalog API)
        // ==========================================
        Console.WriteLine("Step 3: Executing Delete (Id <= 10)...");
        // 我们期望删除 10 行，底层应该生成一个包含 10 个标记的 DV 文件
        var deletePredicate = Col("Id") <= 10;
        
        // 记录一下执行前的版本
        // var historyBefore = Delta.History(s3StorageLocation, cloudOptions: cloudOptions);
        
        uc.DeleteCatalogRecords(catalog, schema, table, deletePredicate, cloudOptions: cloudOptions);

        // ==========================================
        // 5. 验证结果
        // ==========================================
        Console.WriteLine("Step 4: Verifying Results...");
        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect().Sort("Id");
        
        // 预期：50 - 10 = 40 行
        Assert.Equal(40, resultDf.Height);

        // 验证被删的数据彻底消失
        var remainingIds = resultDf["Id"].ToArray<int>();
        Assert.DoesNotContain(1, remainingIds);
        Assert.DoesNotContain(10, remainingIds);
        
        // 验证保留的数据原封不动
        Assert.Contains(11, remainingIds);
        Assert.Contains(50, remainingIds);

        Console.WriteLine("Pure Delete with DV under Catalog works perfectly!");
    }
    [Fact]
    [Trait("Catalog", "MergeDV")]
    public async Task Test_Isolation_Pure_Merge_With_DV_Async()
    {
        // ==========================================
        // 1. 环境准备与 Mock
        // ==========================================
        var catalog = "main";
        var schema = "default";
        var table = $"delta_iso_merge_dv_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-iso-merge-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        var cloudOptions = CloudOptions.Aws(region: _minio.Region, endpoint: polarsEndpoint);
        cloudOptions.Credentials!["aws_allow_http"] = "true";
        cloudOptions.Credentials!["aws_s3_force_path_style"] = "true";
        cloudOptions.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);

        // ==========================================
        // 2. 初始阵地：写入 5 行基础数据 (Id: 1~5)
        // ==========================================
        Console.WriteLine("Step 1: Writing initial 5 rows...");
        using (var dfInit = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3, 4, 5 },
            Team = new[] { "A", "A", "B", "B", "C" },
            Value = new[] { 10.0, 20.0, 30.0, 40.0, 50.0 }
        }))
        {
            dfInit.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Overwrite, cloudOptions: cloudOptions);
        }

        // ==========================================
        // 3. 开启 Deletion Vectors 特性 (激活 MoR)
        // ==========================================
        Console.WriteLine("Step 2: Enabling Deletion Vectors...");
        Delta.AddFeature(
            s3StorageLocation, 
            "deletionVectors", 
            allowProtocolIncrease: true, 
            cloudOptions: cloudOptions
        );

        // ==========================================
        // 4. 执行纯 Merge 操作 (Update 1~3, Insert 6~7)
        // ==========================================
        Console.WriteLine("Step 3: Executing Merge...");
        using (var dfMerge = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3, 6, 7 },
            Team = new[] { "A_Upd", "A_Upd", "B_Upd", "New", "New" },
            Value = new[] { 99.0, 99.0, 99.0, 100.0, 100.0 }
        }))
        {
            uc.MergeCatalogRecords(catalog, schema, table, dfMerge, ["Id"], cloudOptions: cloudOptions)
              .WhenMatchedUpdate() // 触发目标表旧数据的 Delete (写 DV)
              .WhenNotMatchedInsert() // 触发新数据写入 (写新 Parquet)
              .Execute();
        }

        // ==========================================
        // 5. 验证结果
        // ==========================================
        Console.WriteLine("Step 4: Verifying Results...");
        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect().Sort("Id");
        
        // 预期行数：原来 5 行 + 新增 2 行 (6, 7) = 7 行
        Assert.Equal(7, resultDf.Height);

        // 验证 Update 是否成功 (旧的 1~3 应该被 DV 隐藏了，读出来的是新的)
        var row1 = resultDf.Filter(Col("Id") == 1);
        Assert.Equal("A_Upd", row1["Team"].ToArray<string>()[0]);
        Assert.Equal(99.0, row1["Value"].ToArray<double>()[0]);

        // 验证没被碰过的行 (4, 5) 原封不动
        var row4 = resultDf.Filter(Col("Id") == 4);
        Assert.Equal("B", row4["Team"].ToArray<string>()[0]);

        Console.WriteLine("Pure Merge with DV under Catalog works perfectly!");
    }
    [Fact]
    [Trait("Catalog", "MergeOptimizeDV")]
    public async Task Test_Concurrent_Chaos_Merge_And_Optimize_With_DV_Async()
    {
        PolarsConfig.SetEnvVar("POLARS_DELTA_MAX_RETRIES", "20");

        var catalog = "main";
        var schema = "default";
        var table = $"delta_chaos_merge_opt_dv_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-chaos-merge-opt-dv-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        var cloudOptions = CloudOptions.Aws(region: _minio.Region, endpoint: polarsEndpoint);
        cloudOptions.Credentials!["aws_allow_http"] = "true";
        cloudOptions.Credentials!["aws_s3_force_path_style"] = "true";
        cloudOptions.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);

        // 1. 制造碎片阵地：写入 10 个小文件 (共 100 行)
        Console.WriteLine("Step 1: Creating 10 fragmented files (IDs 1-100)...");
        for (int i = 0; i < 10; i++)
        {
            int startId = (i * 10) + 1;
            using var dfInit = DataFrame.FromColumns(new { 
                Id = Enumerable.Range(startId, 10).ToArray(),
                Team = Enumerable.Repeat("Init", 10).ToArray(),
                Value = Enumerable.Repeat(1.0, 10).ToArray()
            });
            var mode = i == 0 ? DeltaSaveMode.Overwrite : DeltaSaveMode.Append;
            dfInit.WriteCatalogTable(uc, catalog, schema, table, mode: mode, cloudOptions: cloudOptions);
        }

        // 2. 开启 Deletion Vectors 特性 (MoR)
        Console.WriteLine("Step 2: Enabling Deletion Vectors...");
        Delta.AddFeature(s3StorageLocation, "deletionVectors", allowProtocolIncrease: true, cloudOptions: cloudOptions);

        // 3. 混沌大逃杀
        Console.WriteLine("Step 3: Unleashing Chaos (5 Mergers vs 3 Optimizers)...");
        var tasks = new List<Task>();

        // 【⚡ 闪电军团：Merge 5个 Worker】(各自负责 Update 10行旧数据，Insert 10行新数据)
        for (int i = 0; i < 5; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(() => 
            {
                var startId = 51 + (workerId * 10); // Update: 51~100
                var insertId = 101 + (workerId * 10); // Insert: 101~150
                using var dfMerge = DataFrame.FromColumns(new { 
                    Id = Enumerable.Range(startId, 10).Concat(Enumerable.Range(insertId, 10)).ToArray(), 
                    Team = Enumerable.Repeat($"Merger_{workerId}", 20).ToArray(),
                    Value = Enumerable.Repeat(99.9, 20).ToArray() 
                });
                
                uc.MergeCatalogRecords(catalog, schema, table, dfMerge, ["Id"], cloudOptions: cloudOptions)
                  .WhenMatchedUpdate()
                  .WhenNotMatchedInsert()
                  .Execute();
                  
                Console.WriteLine($"[Team Lightning] Merger {workerId} Finished.");
            }));
        }

        // 【🛠️ 空间架构师：Optimize 3个 Worker】(负责吸收 DV 并进行 Z-Order)
        for (int i = 0; i < 3; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(async () => 
            {
                // 稍微延迟进场，让 Merge 先打出一点 DV 碎片
                await Task.Delay(new Random().Next(100, 500));
                try 
                {
                    long filesOptimized = uc.OptimizeCatalogTable(
                        catalog, schema, table, 
                        targetSizeMb: 128, 
                        zOrderColumns: ["Id"], 
                        cloudOptions: cloudOptions
                    );
                    Console.WriteLine($"[Team Architect] Optimizer {workerId} compacted {filesOptimized} files.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Team Architect] Optimizer {workerId} exited gracefully: {ex.Message}");
                }
            }));
        }

        await Task.WhenAll(tasks);

        // 4. 终极校验
        Console.WriteLine("\nStep 4: Checking Battlefield Integrity...");
        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect().Sort("Id");
        
        // 初始 100 行。Update 50 行不增加总数，Insert 50 行。最终必须是 150 行！
        Assert.Equal(150, resultDf.Height);

        var remainingIds = resultDf["Id"].ToArray<int>();
        var remainingValues = resultDf["Value"].ToArray<double>();

        // Id 1 是没被碰过的
        Assert.Equal(1, remainingIds[0]);
        Assert.Equal(1.0, remainingValues[0]);

        // Id 51 是被 Update 过的
        Assert.Equal(51, remainingIds[50]);
        Assert.Equal(99.9, remainingValues[50]);

        // Id 150 是被 Insert 进去的
        Assert.Equal(150, remainingIds[149]);
        Assert.Equal(99.9, remainingValues[149]);

        Console.WriteLine("ULTIMATE MERGE vs OPTIMIZE WITH DV PASSED! ");
        Environment.SetEnvironmentVariable("POLARS_DELTA_MAX_RETRIES", null);
    }
    [Fact]
    [Trait("Catalog", "LogicalChaos")]
    public async Task Test_Ultimate_Chaos_Logical_Overlap_Async()
    {
        PolarsConfig.SetEnvVar("POLARS_DELTA_MAX_RETRIES", "30");

        var catalog = "main";
        var schema = "default";
        var table = $"delta_overlap_chaos_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-overlap-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        var cloudOptions = CloudOptions.Aws(region: _minio.Region, endpoint: polarsEndpoint);
        cloudOptions.Credentials!["aws_allow_http"] = "true";
        cloudOptions.Credentials!["aws_s3_force_path_style"] = "true";
        cloudOptions.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);

        // 1. 初始阵地: 1 ~ 50
        Console.WriteLine("Step 1: Setting up the battlefield (50 initial rows)...");
        using (var dfInit = DataFrame.FromColumns(new { 
            Id = Enumerable.Range(1, 50).ToArray(),
            Team = Enumerable.Repeat("Init", 50).ToArray(),
            Value = Enumerable.Repeat(1.0, 50).ToArray()
        }))
        {
            dfInit.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Overwrite, cloudOptions: cloudOptions);
        }

        // 1.5 开启 DV
        Delta.AddFeature(s3StorageLocation, DeltaTableFeatures.DeletionVectors,allowProtocolIncrease: true, cloudOptions: cloudOptions);
        // Delta.AddFeature(s3StorageLocation, DeltaTableFeatures.ChangeDataFeed, allowProtocolIncrease: true, cloudOptions: cloudOptions);
        // var properties = new Dictionary<string, string>
        // {
        //     { "delta.enableChangeDataFeed", "true" }
        // };
        // Delta.SetTableProperties(s3StorageLocation, properties,true,cloudOptions:cloudOptions);

        static async Task ExecuteWithChaosAsync(string workerName, Action action)
        {
            var rnd = new Random(Guid.NewGuid().GetHashCode());
            for (int attempt = 1; attempt <= 3; attempt++)
            {
                await Task.Delay(rnd.Next(10, 300));
                try
                {
                    if (rnd.NextDouble() < 0.20) throw new Exception("Simulated Transient Network Failure!");
                    action();
                    return; 
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Chaos Simulator] {workerName} failed on attempt {attempt}: {ex.Message}");
                    if (attempt == 3) throw;
                }
            }
        }

        Console.WriteLine("Step 2: Unleashing Logical Overlap Chaos...");
        var tasks = new List<Task>();

        // 【🔥 Team Fire：单纯追加】 (101~150，共 50 行) - 5个 Worker
        for (int i = 0; i < 5; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(() => ExecuteWithChaosAsync($"Writer_{workerId}", () =>
            {
                var startId = 100 + (workerId * 10) + 1;
                using var dfAppend = DataFrame.FromColumns(new { 
                    Id = Enumerable.Range(startId, 10).ToArray(), 
                    Team = Enumerable.Repeat($"Writer_{workerId}", 10).ToArray(),
                    Value = Enumerable.Repeat(1.0, 10).ToArray() 
                });
                dfAppend.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Append, cloudOptions: cloudOptions);
            })));
        }

        // 【🧊 Team Ice：删除 1 ~ 30】 - 3个 Worker
        // (涵盖纯删除区 1~20，以及死亡交叉区 21~30)
        for (int i = 0; i < 3; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(() => ExecuteWithChaosAsync($"Deleter_{workerId}", () =>
            {
                var minId = (workerId * 10) + 1;
                var maxId = minId + 9;
                var predicate = (Col("Id") >= minId) & (Col("Id") <= maxId);
                uc.DeleteCatalogRecords(catalog, schema, table, predicate, cloudOptions: cloudOptions);
            })));
        }

        // 【⚡ Team Lightning：纯 Update 21 ~ 50】 - 3个 Worker
        // (涵盖死亡交叉区 21~30，以及纯更新区 31~50)
        for (int i = 0; i < 3; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(() => ExecuteWithChaosAsync($"Merger_{workerId}", () =>
            {
                var startId = 20 + (workerId * 10) + 1; // 21, 31, 41
                using var dfMerge = DataFrame.FromColumns(new { 
                    Id = Enumerable.Range(startId, 10).ToArray(), 
                    Team = Enumerable.Repeat($"Merger_{workerId}", 10).ToArray(),
                    Value = Enumerable.Repeat(99.9, 10).ToArray() 
                });

                uc.MergeCatalogRecords(catalog, schema, table, dfMerge, ["Id"], cloudOptions: cloudOptions)
                  .WhenMatchedUpdate() // ！！！注意：没有 Insert 分支了！！！
                  .Execute();
            })));
        }

        // 【🛠️ Team Architect：并发 Optimize】
        for (int i = 0; i < 3; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(() => ExecuteWithChaosAsync($"Optimizer_{workerId}", () =>
            {
                uc.OptimizeCatalogTable(catalog, schema, table, targetSizeMb: 128, zOrderColumns: ["Id"], cloudOptions: cloudOptions);
            })));
        }

        await Task.WhenAll(tasks);

        Console.WriteLine("\nStep 3: Checking Battlefield Integrity...");
        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect().Sort("Id");
        
        // 【终极数学题账本】
        // 初始: 50
        // 删除 1~30: -30
        // Update 21~50: (21~30 已重叠死亡), 31~50 存活被更新 (共20行)
        // 追加 101~150: +50
        // 最终存活: 31~50 (20行) + 101~150 (50行) = 70 行！
        Assert.Equal(70, resultDf.Height);
        var yesterday = DateTime.UtcNow.AddDays(-1);

        var cdcLf = Delta.ReadChangeDataFeed(
            path: s3StorageLocation, 
            startTimestamp: yesterday,
            cloudOptions:cloudOptions
        );

        cdcLf.Collect().Show();

        var remainingIds = resultDf["Id"].ToArray<int>();

        // 校验 1：1~30 必须死透了 (不管是被先删还是被先更新)
        Assert.DoesNotContain(1, remainingIds);
        Assert.DoesNotContain(30, remainingIds);

        // 校验 2：31~50 必须存活，且被 Update 过了
        var row31 = resultDf.Filter(Col("Id") == 31);
        Assert.Equal(31, row31["Id"].ToArray<int>()[0]);
        Assert.Equal(99.9, row31["Value"].ToArray<double>()[0]); // 必须是 99.9

        // 校验 3：101~150 必须全都在
        Assert.Contains(101, remainingIds);
        Assert.Contains(150, remainingIds);

        Console.WriteLine("ULTIMATE LOGICAL OVERLAP CHAOS PASSED! ROW LEVEL CONFLICTS RESOLVED!");
        Environment.SetEnvironmentVariable("POLARS_DELTA_MAX_RETRIES", null);
    }
    [Fact]
    [Trait("Catalog", "Maintenance")]
    public void Test_Catalog_History_Restore_Vacuum_Lifecycle()
    {
        // ==========================================
        // 0. 环境与 Mock 初始化
        // ==========================================
        var catalog = "main";
        var schema = "default";
        var table = $"delta_maintenance_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-maintenance-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        var cloudOptions = CloudOptions.Aws(region: _minio.Region, endpoint: polarsEndpoint);
        cloudOptions.Credentials!["aws_allow_http"] = "true";
        cloudOptions.Credentials!["aws_s3_force_path_style"] = "true";
        cloudOptions.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);

        // ==========================================
        // 1. 创世 (V1)
        // ==========================================
        Console.WriteLine("Step 1: Creating initial table (V1)...");
        using var dfV1 = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3 },
            Hero = new[] { "Iron Man", "Captain America", "Thor" }
        });
        
        dfV1.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Overwrite, cloudOptions: cloudOptions);

        // ==========================================
        // 2. 变异 (V2) - 混入反派
        // ==========================================
        Console.WriteLine("Step 2: Appending bad data (V2)...");
        using var dfV2 = DataFrame.FromColumns(new { 
            Id = new[] { 4, 5 },
            Hero = new[] { "Thanos", "Ultron" } 
        });

        dfV2.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Append, cloudOptions: cloudOptions);

        // 校验 V2 状态
        using var currentLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var currentDf = currentLf.Collect();
        Assert.Equal(5, currentDf.Height);

        // ==========================================
        // 3. 审计 (History)
        // ==========================================
        Console.WriteLine("\nStep 3: Checking Table History...");
        using var historyDf = uc.DeltaHistory(catalog, schema, table, cloudOptions: cloudOptions);
        
        Console.WriteLine("--- Table History ---");
        historyDf.Show();
        
        // 历史记录至少应该有 2 条 (V0: WRITE, V1: APPEND)
        Assert.True(historyDf.Height >= 2);

        // ==========================================
        // 4. 时光倒流 (Restore)
        // ==========================================
        Console.WriteLine("\nStep 4: Restoring to V1...");
        long newVersion = uc.DeltaRestore(
            catalog, schema, table, 
            version: 1, 
            cloudOptions: cloudOptions
        );

        // Restore 会产生一个新的 Commit (V3)
        Assert.Equal(3, newVersion);

        // 校验回滚后的状态：只剩最初的 3 条英雄
        using var restoredLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var restoredDf = restoredLf.Collect().Sort("Id");
        
        Console.WriteLine("--- Restored Data ---");
        restoredDf.Show();
        Assert.Equal(3, restoredDf.Height);

        // ==========================================
        // 5. 毁灭 (Vacuum)
        // ==========================================
        Console.WriteLine("\nStep 5: Vacuuming orphaned files...");
        // 虽然回滚到了 V1，但 V1 (Thanos/Ultron) 的 Parquet 文件仍在 S3 中。
        // 强行关闭保留期保护，清理孤儿文件。
        long deletedFiles = uc.DeltaVacuum(
            catalog, schema, table,
            retentionHours: 0,          
            enforceRetention: false,    
            cloudOptions: cloudOptions
        );

        Console.WriteLine($"--- Vacuum deleted {deletedFiles} orphaned files ---");
        // 至少删除了 1 个文件 (V1 的 Append 产生的文件)
        Assert.True(deletedFiles > 0);
    }

}