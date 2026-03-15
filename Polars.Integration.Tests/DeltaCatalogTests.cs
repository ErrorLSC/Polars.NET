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

        Console.WriteLine("Catalog Delete Full Cycle Passed Perfectly! 🗑️✨");
    }
}