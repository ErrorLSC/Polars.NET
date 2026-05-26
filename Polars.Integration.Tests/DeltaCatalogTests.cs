using WireMock.Server;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;
using Polars.CSharp;
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;
using Polars.Integration.Tests.Fixtures;
using Polars.NET.Core; 

namespace Polars.Integration.Tests;

public class CatalogIntegrationTests(MinioFixture _minio) : IAsyncLifetime, IClassFixture<MinioFixture>
{
    private WireMockServer _catalogMockServer = null!;

    public Task InitializeAsync()
    {
        _catalogMockServer = WireMockServer.Start();
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        _catalogMockServer?.Stop();
        _catalogMockServer?.Dispose();
        return Task.CompletedTask;
    }

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

        _catalogMockServer
            .Given(
                Request.Create()
                    .WithPath("/api/2.1/unity-catalog/temporary-table-credentials")
                    .UsingPost() 
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
                            session_token = (string?)null, 
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
        var catalog = "main";
        var schema = "default";
        var table = $"delta_catalog_test_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-super-secret-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

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

        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3 },
            Msg = new[] { "Alice", "Bob", "Charlie" }
        }))
        {
            df.WriteDelta(s3StorageLocation, mode: DeltaSaveMode.Append, cloudOptions: writeOptions);
        }

        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        // ==========================================
        // ScanDeltaCatalog
        // ==========================================
        
        var readOptions = CloudOptions.Aws(
            region: _minio.Region,
            endpoint: polarsEndpoint
        );
        readOptions.Credentials!["aws_allow_http"] = "true";
        readOptions.Credentials!["aws_s3_force_path_style"] = "true";

        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);
        
        using var lf = uc.ScanCatalogTable(
            catalogName: catalog,
            schemaName: schema,
            tableName: table,
            cloudOptions: readOptions
        );

        using var resultDf = lf.Collect();
        // shape: (3, 2)
        // ┌─────┬─────────┐
        // │ Id  ┆ Msg     │
        // │ --- ┆ ---     │
        // │ i32 ┆ str     │
        // ╞═════╪═════════╡
        // │ 1   ┆ Alice   │
        // │ 2   ┆ Bob     │
        // │ 3   ┆ Charlie │
        // └─────┴─────────┘

        Assert.Equal(3, resultDf.Height);
        Assert.Contains("Alice", resultDf["Msg"].ToArray<string>());
        Assert.Contains("Charlie", resultDf["Msg"].ToArray<string>());

    }
    [Fact]
    [Trait("Catalog", "Sink")]
    public void Test_SinkCatalogTable_Basic_Overwrite_And_Append()
    {
        var catalog = "main";
        var schema = "default";
        var table = $"delta_catalog_sink_basic_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-sink-secret-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        var cloudOptions = CloudOptions.Aws(region: _minio.Region, endpoint: polarsEndpoint);
        cloudOptions.Credentials!["aws_allow_http"] = "true";
        cloudOptions.Credentials!["aws_s3_force_path_style"] = "true";

        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);

        using (var df1 = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2 },
            Name = new[] { "Alice", "Bob" }
        }))
        {
            df1.WriteCatalogTable(
                uc, catalog, schema, table, 
                mode: DeltaSaveMode.Overwrite, 
                cloudOptions: cloudOptions
            );
        }

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

        
        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect().Sort(["Id"]);
        
        // shape: (4, 2)
        // ┌─────┬─────────┐
        // │ Id  ┆ Name    │
        // │ --- ┆ ---     │
        // │ i32 ┆ str     │
        // ╞═════╪═════════╡
        // │ 1   ┆ Alice   │
        // │ 2   ┆ Bob     │
        // │ 3   ┆ Charlie │
        // │ 4   ┆ David   │
        // └─────┴─────────┘

        Assert.Equal(4, resultDf.Height);
        Assert.Equal(1, resultDf["Id"].ToArray<int>()[0]);
        Assert.Equal("David", resultDf["Name"].ToArray<string>()[3]);

    }
    [Fact]
    [Trait("Catalog", "SinkAdvanced")]
    public void Test_SinkCatalogTable_Advanced_Modes_And_AutoPartition()
    {
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

        using (var df1 = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2 },
            Region = new[] { "North", "South" }
        }))
        {
            df1.WriteCatalogTable(
                uc, catalog, schema, table, 
                mode: DeltaSaveMode.Overwrite, 
                partitionBy: Cs.ByName("Region"), 
                cloudOptions: cloudOptions
            );
        }

        using (var df2 = DataFrame.FromColumns(new { 
            Id = new[] { 3, 4 },
            Region = new[] { "East", "North" }
        }))
        {
            df2.WriteCatalogTable(
                uc, catalog, schema, table, 
                mode: DeltaSaveMode.Append, 
                partitionBy: null, 
                cloudOptions: cloudOptions
            );
        }

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
            Assert.Contains("already exists", ex.Message); 
        }

        using (var dfIgnore = DataFrame.FromColumns(new { Id = new[] { 6 }, Region = new[] { "West" } }))
        {
            dfIgnore.WriteCatalogTable(
                uc, catalog, schema, table, 
                mode: DeltaSaveMode.Ignore, 
                cloudOptions: cloudOptions
            );
        }

        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect().Sort(["Id"]);
        // shape: (4, 2)
        // ┌─────┬────────┐
        // │ Id  ┆ Region │
        // │ --- ┆ ---    │
        // │ i32 ┆ str    │
        // ╞═════╪════════╡
        // │ 1   ┆ North  │
        // │ 2   ┆ South  │
        // │ 3   ┆ East   │
        // │ 4   ┆ North  │
        // └─────┴────────┘

        Assert.Equal(4, resultDf.Height);
        
        var idArray = resultDf["Id"].ToArray<int>();
        Assert.Contains(1, idArray);
        Assert.Contains(3, idArray);
        Assert.DoesNotContain(6, idArray);

    }
    [Fact]
    [Trait("Catalog", "Concurrent")]
    public async Task Test_Concurrent_SinkCatalogTable_Append_Stress_TestAsync()
    {
        var catalog = "main";
        var schema = "default";
        var table = $"delta_catalog_concurrent_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-concurrent-secret-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        var cloudOptions = CloudOptions.Aws(region: _minio.Region, endpoint: polarsEndpoint);
        cloudOptions.Credentials!["aws_allow_http"] = "true";
        cloudOptions.Credentials!["aws_s3_force_path_style"] = "true";
        cloudOptions.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"; 

        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);


        using (var dfInit = DataFrame.FromColumns(new { WorkerId = new[] { 0 }, RowId = new[] { 0 } }))
        {
            dfInit.WriteCatalogTable(
                uc, catalog, schema, table, 
                mode: DeltaSaveMode.Overwrite, 
                cloudOptions: cloudOptions
            );
        }

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
                    var workerIds = Enumerable.Repeat(workerId, rowsPerWorker).ToArray();
                    var rowIds = Enumerable.Range(1, rowsPerWorker).ToArray();

                    using var sourceDf = DataFrame.FromColumns(new { WorkerId = workerIds, RowId = rowIds });

                    sourceDf.WriteCatalogTable(
                        uc, catalog, schema, table, 
                        mode: DeltaSaveMode.Append, 
                        cloudOptions: cloudOptions
                    );
                    
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Worker {workerId}] FAILED: {ex.Message}");
                    throw; 
                }
            }));
        }

        await Task.WhenAll(tasks);
        
        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect();

        long expectedHeight = 1 + (concurrency * rowsPerWorker);
        Assert.Equal(expectedHeight, resultDf.Height);
        for (int i = 1; i <= concurrency; i++)
        {
            var workerRowCount = resultDf.Filter(Pl.Col("WorkerId") == i).Height;
            Assert.Equal(rowsPerWorker, workerRowCount);
        }

    }
    [Fact]
    [Trait("Catalog", "Roundtrip")]
    public void Test_CatalogTable_Full_Roundtrip_DDL_and_DML()
    {

        var catalog = "main";
        var schema = "default";
        var table = $"delta_catalog_roundtrip_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-roundtrip-secret-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        var cloudOptions = CloudOptions.Aws(region: _minio.Region, endpoint: polarsEndpoint);
        cloudOptions.Credentials!["aws_allow_http"] = "true";
        cloudOptions.Credentials!["aws_s3_force_path_style"] = "true";
        cloudOptions.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

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
                
                created_at = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                created_by = "test_admin",
                updated_at = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                updated_by = "test_admin"
            }));

        _catalogMockServer
            .Given(Request.Create().WithPath($"/api/2.1/unity-catalog/tables/{catalog}.{schema}.{table}").UsingDelete().WithHeader("Authorization", $"Bearer {expectedToken}"))
            .RespondWith(Response.Create().WithStatusCode(200));


        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);

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

        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3 },
            Msg = new[] { "Alice", "Bob", "Charlie" }
        }))
        {
            df.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Overwrite, cloudOptions: cloudOptions);
        }

        using (var lf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions))
        using (var resultDf = lf.Collect().Sort(["Id"]))
        {
            Assert.Equal(3, resultDf.Height);
            Assert.Equal(1, resultDf["Id"].ToArray<int>()[0]);
            Assert.Contains("Charlie", resultDf["Msg"].ToArray<string>());
        }


        uc.DeleteCatalogTable(catalog, schema, table);

    }
    [Fact]
    [Trait("Catalog", "Delete")]
    public void Test_Catalog_Delete_Full_Cycle_Logic()
    {
        var catalog = "main";
        var schema = "default";
        var table = $"delta_catalog_delete_{Guid.NewGuid():N}";
        var s3StorageLocation = $"s3://{_minio.BucketName}/{table}";
        var expectedToken = "dapi-delete-secret-token";

        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";

        SetupUnityCatalogMock(catalog, schema, table, s3StorageLocation, expectedToken, _minio.AccessKey, _minio.SecretKey);

        var cloudOptions = CloudOptions.Aws(region: _minio.Region, endpoint: polarsEndpoint);
        cloudOptions.Credentials!["aws_allow_http"] = "true";
        cloudOptions.Credentials!["aws_s3_force_path_style"] = "true";
        cloudOptions.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        using var uc = new UnityCatalog(_catalogMockServer.Urls[0], expectedToken);


        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3, 4, 5 }, 
            Msg = new[] { "A", "B", "C", "D", "E" },
            Year = new[] { "2023", "2023", "2024", "2024", "2024" }
        }))
        {
            df.WriteCatalogTable(
                uc, catalog, schema, table, 
                partitionBy: Cs.ByName("Year"), 
                mode: DeltaSaveMode.Overwrite, 
                cloudOptions: cloudOptions
            );
        }

        using var dfV1 = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions).Collect();
        Assert.Equal(5, dfV1.Height);

        
        // Predicate: (Year == '2024') & (Id == 4)
        var predicateRewrite = (Pl.Col("Year") == Pl.Lit("2024")) & (Pl.Col("Id") == 4);

        uc.DeleteCatalogRecords(catalog, schema, table, predicateRewrite, cloudOptions: cloudOptions);

        using var dfV2 = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions).Collect().Sort("Id");
        Assert.Equal(4, dfV2.Height);
        Assert.DoesNotContain(4, dfV2["Id"].ToArray<int>());
        Assert.Contains(3, dfV2["Id"].ToArray<int>());
        Assert.Contains(5, dfV2["Id"].ToArray<int>());
        
        var predicateDrop = Pl.Col("Year") == Pl.Lit("2023");
        
        uc.DeleteCatalogRecords(catalog, schema, table, predicateDrop, cloudOptions: cloudOptions);

        using var dfV3 = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions).Collect().Sort("Id");
        Assert.Equal(2, dfV3.Height);
        Assert.DoesNotContain(1, dfV3["Id"].ToArray<int>());
        Assert.DoesNotContain(2, dfV3["Id"].ToArray<int>());
        Assert.Equal("2024", dfV3["Year"].ToArray<string>()[0]);
        
        var predicateNoOp = Pl.Col("Id") == 999;
        uc.DeleteCatalogRecords(catalog, schema, table, predicateNoOp, cloudOptions: cloudOptions);

        using var dfV4 = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions).Collect();
        Assert.Equal(2, dfV4.Height);

        
        // V1
        using var dfBackV1 = uc.ScanCatalogTable(catalog, schema, table, version: 1, cloudOptions: cloudOptions).Collect();
        Assert.Equal(5, dfBackV1.Height);
        
        // V2 
        using var dfBackV2 = uc.ScanCatalogTable(catalog, schema, table, version: 2, cloudOptions: cloudOptions).Collect();
        Assert.Equal(4, dfBackV2.Height);
        Assert.DoesNotContain(4, dfBackV2["Id"].ToArray<int>());
    }
    [Fact]
    [Trait("Catalog", "DeleteConcurrent")]
    public async Task Test_Concurrent_Catalog_Delete_Conflict_Stress_TestAsync()
    {
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

        using (var dfInit = DataFrame.FromColumns(new { 
            Id = Enumerable.Range(1, 10).ToArray(),
            Msg = Enumerable.Repeat("Target", 10).ToArray()
        }))
        {
            dfInit.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Overwrite, cloudOptions: cloudOptions);
        }

        
        int concurrency = 5;
        var tasks = new List<Task>();
        
        int successCount = 0;
        int conflictCount = 0;

        for (int i = 0; i < concurrency; i++)
        {
            int targetId = i + 1; 
            tasks.Add(Task.Run(() =>
            {
                try 
                {
                    var predicate = Pl.Col("Id") == targetId;
                    uc.DeleteCatalogRecords(catalog, schema, table, predicate, cloudOptions: cloudOptions);
                    
                    Interlocked.Increment(ref successCount);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Worker {targetId}] BLOCKED by OCC lock: {ex.Message}");
                    Interlocked.Increment(ref conflictCount);
                }
            }));
        }

        await Task.WhenAll(tasks);

        Assert.True(successCount >= 1, "At least one deletion should succeed.");
        Assert.Equal(concurrency, successCount + conflictCount);

        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect();
        // shape: (5, 2)
        // ┌─────┬────────┐
        // │ Id  ┆ Msg    │
        // │ --- ┆ ---    │
        // │ i32 ┆ str    │
        // ╞═════╪════════╡
        // │ 6   ┆ Target │
        // │ 7   ┆ Target │
        // │ 8   ┆ Target │
        // │ 9   ┆ Target │
        // │ 10  ┆ Target │
        // └─────┴────────┘

        Assert.Equal(10 - successCount, resultDf.Height);

    }
    [Fact]
    [Trait("Catalog", "Chaos")]
    public async Task Test_Concurrent_Chaos_Mixed_Append_Delete_Merge_Async()
    {
        PolarsConfig.SetEnvVar("POLARS_DELTA_MAX_RETRIES", "20");

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

        using (var dfInit = DataFrame.FromColumns(new { 
            Id = Enumerable.Range(1, 50).ToArray(),
            Team = Enumerable.Repeat("Init", 50).ToArray()
        }))
        {
            dfInit.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Overwrite, cloudOptions: cloudOptions);
        }


        
        var tasks = new List<Task>();
        int concurrency = 5;

        // Append
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

        // Delete
        for (int i = 0; i < concurrency; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(() =>
            {
                var minId = (workerId * 10) + 1;
                var maxId = minId + 9;
                
                var predicate = (Pl.Col("Id") >= minId) & (Pl.Col("Id") <= maxId);
                uc.DeleteCatalogRecords(catalog, schema, table, predicate, cloudOptions: cloudOptions);
                Console.WriteLine($"[Team Ice] Deleter {workerId} deleted IDs {minId} to {maxId}.");
            }));
        }

        // Merge
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

                dfMerge.MergeCatalogRecords(uc,catalog, schema, table, ["Id"], cloudOptions: cloudOptions)
                  .WhenMatchedUpdate()
                  .WhenNotMatchedInsert()
                  .Execute();
                  
                Console.WriteLine($"[Team Lightning] Merger {workerId} upserted IDs {startId} to {startId+9}.");
            }));
        }

        await Task.WhenAll(tasks);
        
        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect().Sort("Id");
        
        Assert.Equal(100, resultDf.Height);

        var remainingIds = resultDf["Id"].ToArray<int>();

        Assert.DoesNotContain(1, remainingIds);
        Assert.DoesNotContain(50, remainingIds);

        Assert.Contains(101, remainingIds);
        Assert.Contains(150, remainingIds);

        Assert.Contains(201, remainingIds);
        Assert.Contains(250, remainingIds);
        
        Environment.SetEnvironmentVariable("POLARS_DELTA_MAX_RETRIES", null);
    }
    [Fact]
    [Trait("Catalog", "FourWayChaos")]
    public async Task Test_Ultimate_Chaos_DV_Append_Delete_Merge_Optimize_Async()
    {
        PolarsConfig.SetEnvVar("POLARS_DELTA_MAX_RETRIES", "30");

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


        using (var dfInit = DataFrame.FromColumns(new { 
            Id = Enumerable.Range(1, 50).ToArray(),
            Team = Enumerable.Repeat("Init", 50).ToArray(),
            Value = Enumerable.Repeat(1.0, 50).ToArray()
        }))
        {
            dfInit.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Overwrite, cloudOptions: cloudOptions);
        }

        Delta.AddFeature(
            s3StorageLocation, 
            DeltaTableFeatures.DeletionVectors, 
            allowProtocolIncrease: true, 
            cloudOptions: cloudOptions
        );


        static async Task ExecuteWithChaosAsync(string workerName, Action action)
        {
            var rnd = new Random(Guid.NewGuid().GetHashCode());
            int maxWorkerRetries = 100;

            for (int attempt = 1; attempt <= maxWorkerRetries; attempt++)
            {
                await Task.Delay(rnd.Next(10, 300));

                try
                {
                    if (rnd.NextDouble() < 0.20)
                    {
                        throw new Exception("Simulated Transient Network Failure!");
                    }
                    action();
                    return; 
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Chaos Simulator] {workerName} failed on attempt {attempt}: {ex.Message}");
                    if (attempt == maxWorkerRetries)
                        throw; 
                }
            }
        }
        
        var tasks = new List<Task>();
        int concurrency = 5;

        // Append Team
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

        // Delete Team
        for (int i = 0; i < concurrency; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(() => ExecuteWithChaosAsync($"Deleter_{workerId}", () =>
            {
                var minId = (workerId * 10) + 1;
                var maxId = minId + 9;
                var predicate = (Pl.Col("Id") >= minId) & (Pl.Col("Id") <= maxId);
                uc.DeleteCatalogRecords(catalog, schema, table, predicate, cloudOptions: cloudOptions);
                Console.WriteLine($"[Team Ice] Deleter {workerId} deleted IDs {minId} to {maxId} (using DV!).");
            })));
        }

        // Merge Team
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

                dfMerge.MergeCatalogRecords(uc,catalog, schema, table, ["Id"], cloudOptions: cloudOptions)
                  .WhenMatchedUpdate()
                  .WhenNotMatchedInsert()
                  .Execute();
                Console.WriteLine($"[Team Lightning] Merger {workerId} upserted IDs {startId} to {startId+9}.");
            })));
        }

        // Optimization Team
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

        await Task.WhenAll(tasks);
        
        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect().Sort("Id");

        Assert.Equal(100, resultDf.Height);

        var remainingIds = resultDf["Id"].ToArray<int>();

        Assert.DoesNotContain(1, remainingIds);
        Assert.DoesNotContain(50, remainingIds);

        Assert.Contains(101, remainingIds);
        Assert.Contains(150, remainingIds);

        Assert.Contains(201, remainingIds);
        Assert.Contains(250, remainingIds);

        ulong newVersion = uc.DeltaRestore(
            catalog, schema, table, 
            version: 1, 
            cloudOptions: cloudOptions
        );

        using var restoredLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var restoredDf = restoredLf.Collect().Sort("Id");

        Assert.Equal(50, restoredDf.Height);

        using var historyDf = uc.DeltaHistory(catalog, schema, table, cloudOptions: cloudOptions);

        long deletedFiles = uc.DeltaVacuum(
            catalog, schema, table,
            retentionHours: 0,          
            enforceRetention: false,    
            cloudOptions: cloudOptions
        );
        // --- Table History ---
        // shape: (20, 21)
        // ┌─────────┬────────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬───────────┐
        // │ version ┆ timestamp  ┆ operation ┆ mode      ┆ … ┆ numRemove ┆ numRestor ┆ engineMak ┆ engine    │
        // │ ---     ┆ ---        ┆ ---       ┆ ---       ┆   ┆ dFile     ┆ edFile    ┆ er        ┆ ---       │
        // │ str     ┆ datetime[m ┆ str       ┆ str       ┆   ┆ ---       ┆ ---       ┆ ---       ┆ str       │
        // │         ┆ s, UCT]    ┆           ┆           ┆   ┆ i64       ┆ i64       ┆ str       ┆           │
        // ╞═════════╪════════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪═══════════╡
        // │ null    ┆ 2026-03-20 ┆ DELETE    ┆ null      ┆ … ┆ null      ┆ null      ┆ ErrorLSC  ┆ Polars.NE │
        // │         ┆ 07:16:17.3 ┆           ┆           ┆   ┆           ┆           ┆           ┆ T         │
        // │         ┆ 11 UTC     ┆           ┆           ┆   ┆           ┆           ┆           ┆           │
        // │ null    ┆ 2026-03-20 ┆ DELETE    ┆ null      ┆ … ┆ null      ┆ null      ┆ ErrorLSC  ┆ Polars.NE │
        // │         ┆ 07:16:16.5 ┆           ┆           ┆   ┆           ┆           ┆           ┆ T         │
        // │         ┆ 07 UTC     ┆           ┆           ┆   ┆           ┆           ┆           ┆           │
        // │ null    ┆ 2026-03-20 ┆ DELETE    ┆ null      ┆ … ┆ null      ┆ null      ┆ ErrorLSC  ┆ Polars.NE │
        // │         ┆ 07:16:16.0 ┆           ┆           ┆   ┆           ┆           ┆           ┆ T         │
        // │         ┆ 66 UTC     ┆           ┆           ┆   ┆           ┆           ┆           ┆           │
        // │ null    ┆ 2026-03-20 ┆ OPTIMIZE  ┆ null      ┆ … ┆ null      ┆ null      ┆ ErrorLSC  ┆ Polars.NE │
        // │         ┆ 07:16:15.8 ┆           ┆           ┆   ┆           ┆           ┆           ┆ T         │
        // │         ┆ 89 UTC     ┆           ┆           ┆   ┆           ┆           ┆           ┆           │
        // │ null    ┆ 2026-03-20 ┆ MERGE     ┆ null      ┆ … ┆ null      ┆ null      ┆ ErrorLSC  ┆ Polars.NE │
        // │         ┆ 07:16:15.7 ┆           ┆           ┆   ┆           ┆           ┆           ┆ T         │
        // │         ┆ 15 UTC     ┆           ┆           ┆   ┆           ┆           ┆           ┆           │
        // │ …       ┆ …          ┆ …         ┆ …         ┆ … ┆ …         ┆ …         ┆ …         ┆ …         │
        // │ null    ┆ 2026-03-20 ┆ WRITE     ┆ Append    ┆ … ┆ null      ┆ null      ┆ ErrorLSC  ┆ Polars.NE │
        // │         ┆ 07:16:14.5 ┆           ┆           ┆   ┆           ┆           ┆           ┆ T         │
        // │         ┆ 07 UTC     ┆           ┆           ┆   ┆           ┆           ┆           ┆           │
        // │ null    ┆ 2026-03-20 ┆ ADD       ┆ null      ┆ … ┆ null      ┆ null      ┆ null      ┆ null      │
        // │         ┆ 07:16:14.2 ┆ FEATURE   ┆           ┆   ┆           ┆           ┆           ┆           │
        // │         ┆ 19 UTC     ┆           ┆           ┆   ┆           ┆           ┆           ┆           │
        // │ null    ┆ 2026-03-20 ┆ WRITE     ┆ Overwrite ┆ … ┆ null      ┆ null      ┆ ErrorLSC  ┆ Polars.NE │
        // │         ┆ 07:16:14.0 ┆           ┆           ┆   ┆           ┆           ┆           ┆ T         │
        // │         ┆ 32 UTC     ┆           ┆           ┆   ┆           ┆           ┆           ┆           │
        // │ null    ┆ 2026-03-20 ┆ CREATE    ┆ ErrorIfEx ┆ … ┆ null      ┆ null      ┆ null      ┆ null      │
        // │         ┆ 07:16:14.0 ┆ TABLE     ┆ ists      ┆   ┆           ┆           ┆           ┆           │
        // │         ┆ 04 UTC     ┆           ┆           ┆   ┆           ┆           ┆           ┆           │
        // │ 1       ┆ 2026-03-20 ┆ RESTORE   ┆ null      ┆ … ┆ 1         ┆ 1         ┆ null      ┆ null      │
        // │         ┆ 07:16:17.8 ┆           ┆           ┆   ┆           ┆           ┆           ┆           │
        // │         ┆ 29 UTC     ┆           ┆           ┆   ┆           ┆           ┆           ┆           │
        // └─────────┴────────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴───────────┘
        // --- Vacuum deleted 14 orphaned files ---
        
        Environment.SetEnvironmentVariable("POLARS_DELTA_MAX_RETRIES", null);
    }
    [Fact]
    [Trait("Catalog", "DeleteDV")]
    public async Task Test_Isolation_Pure_Delete_With_DV_Async()
    {

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

        using (var dfInit = DataFrame.FromColumns(new { 
            Id = Enumerable.Range(1, 50).ToArray(),
            Team = Enumerable.Repeat("Init", 50).ToArray(),
            Value = Enumerable.Repeat(1.0, 50).ToArray()
        }))
        {
            dfInit.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Overwrite, cloudOptions: cloudOptions);
        }

        Delta.AddFeature(
            s3StorageLocation, 
            "deletionVectors", 
            allowProtocolIncrease: true, 
            cloudOptions: cloudOptions
        );

        var deletePredicate = Pl.Col("Id") <= 10;
        
        uc.DeleteCatalogRecords(catalog, schema, table, deletePredicate, cloudOptions: cloudOptions);

        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect().Sort("Id");

        Assert.Equal(40, resultDf.Height);

        var remainingIds = resultDf["Id"].ToArray<int>();
        Assert.DoesNotContain(1, remainingIds);
        Assert.DoesNotContain(10, remainingIds);

        Assert.Contains(11, remainingIds);
        Assert.Contains(50, remainingIds);

    }
    [Fact]
    [Trait("Catalog", "MergeDV")]
    public async Task Test_Isolation_Pure_Merge_With_DV_Async()
    {
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

        using (var dfInit = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3, 4, 5 },
            Team = new[] { "A", "A", "B", "B", "C" },
            Value = new[] { 10.0, 20.0, 30.0, 40.0, 50.0 }
        }))
        {
            dfInit.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Overwrite, cloudOptions: cloudOptions);
        }

        Delta.AddFeature(
            s3StorageLocation, 
            "deletionVectors", 
            allowProtocolIncrease: true, 
            cloudOptions: cloudOptions
        );

        // ==========================================
        // Merge(Update 1~3, Insert 6~7)
        // ==========================================
        using (var dfMerge = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3, 6, 7 },
            Team = new[] { "A_Upd", "A_Upd", "B_Upd", "New", "New" },
            Value = new[] { 99.0, 99.0, 99.0, 100.0, 100.0 }
        }))
        {
            dfMerge.MergeCatalogRecords(uc,catalog, schema, table, ["Id"], cloudOptions: cloudOptions)
              .WhenMatchedUpdate()
              .WhenNotMatchedInsert() 
              .Execute();
        }

        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect().Sort("Id");

        Assert.Equal(7, resultDf.Height);

        var row1 = resultDf.Filter(Pl.Col("Id") == 1);
        Assert.Equal("A_Upd", row1["Team"].ToArray<string>()[0]);
        Assert.Equal(99.0, row1["Value"].ToArray<double>()[0]);

        var row4 = resultDf.Filter(Pl.Col("Id") == 4);
        Assert.Equal("B", row4["Team"].ToArray<string>()[0]);

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

        Delta.AddFeature(s3StorageLocation, "deletionVectors", allowProtocolIncrease: true, cloudOptions: cloudOptions);

        var tasks = new List<Task>();

        // Merge
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

        // Optimize
        for (int i = 0; i < 3; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(async () => 
            {
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

        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect().Sort("Id");

        Assert.Equal(150, resultDf.Height);

        var remainingIds = resultDf["Id"].ToArray<int>();
        var remainingValues = resultDf["Value"].ToArray<double>();

        Assert.Equal(1, remainingIds[0]);
        Assert.Equal(1.0, remainingValues[0]);

        Assert.Equal(51, remainingIds[50]);
        Assert.Equal(99.9, remainingValues[50]);

        Assert.Equal(150, remainingIds[149]);
        Assert.Equal(99.9, remainingValues[149]);

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

        using (var dfInit = DataFrame.FromColumns(new { 
            Id = Enumerable.Range(1, 50).ToArray(),
            Team = Enumerable.Repeat("Init", 50).ToArray(),
            Value = Enumerable.Repeat(1.0, 50).ToArray()
        }))
        {
            dfInit.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Overwrite, cloudOptions: cloudOptions);
        }

        Delta.AddFeature(s3StorageLocation, DeltaTableFeatures.DeletionVectors,allowProtocolIncrease: true, cloudOptions: cloudOptions);

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

        var tasks = new List<Task>();

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

        for (int i = 0; i < 3; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(() => ExecuteWithChaosAsync($"Deleter_{workerId}", () =>
            {
                var minId = (workerId * 10) + 1;
                var maxId = minId + 9;
                var predicate = (Pl.Col("Id") >= minId) & (Pl.Col("Id") <= maxId);
                uc.DeleteCatalogRecords(catalog, schema, table, predicate, cloudOptions: cloudOptions);
            })));
        }

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
                  .WhenMatchedUpdate() 
                  .Execute();
            })));
        }

        for (int i = 0; i < 3; i++)
        {
            int workerId = i;
            tasks.Add(Task.Run(() => ExecuteWithChaosAsync($"Optimizer_{workerId}", () =>
            {
                uc.OptimizeCatalogTable(catalog, schema, table, targetSizeMb: 128, zOrderColumns: ["Id"], cloudOptions: cloudOptions);
            })));
        }

        await Task.WhenAll(tasks);

        using var resultLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var resultDf = resultLf.Collect().Sort("Id");
        
        Assert.Equal(70, resultDf.Height);
        var yesterday = DateTime.UtcNow.AddDays(-1);

        var cdcLf = Delta.ReadChangeDataFeed(
            path: s3StorageLocation, 
            startTimestamp: yesterday,
            cloudOptions:cloudOptions
        );

        cdcLf.Collect();
        // shape: (1_330, 6)
        // ┌─────┬──────┬───────┬──────────────┬─────────────────┬─────────────────────────┐
        // │ Id  ┆ Team ┆ Value ┆ _change_type ┆ _commit_version ┆ _commit_timestamp       │
        // │ --- ┆ ---  ┆ ---   ┆ ---          ┆ ---             ┆ ---                     │
        // │ i32 ┆ str  ┆ f64   ┆ str          ┆ i32             ┆ datetime[ms]            │
        // ╞═════╪══════╪═══════╪══════════════╪═════════════════╪═════════════════════════╡
        // │ 1   ┆ Init ┆ 1.0   ┆ insert       ┆ 1               ┆ 2026-03-20 07:20:13.178 │
        // │ 2   ┆ Init ┆ 1.0   ┆ insert       ┆ 1               ┆ 2026-03-20 07:20:13.178 │
        // │ 3   ┆ Init ┆ 1.0   ┆ insert       ┆ 1               ┆ 2026-03-20 07:20:13.178 │
        // │ 4   ┆ Init ┆ 1.0   ┆ insert       ┆ 1               ┆ 2026-03-20 07:20:13.178 │
        // │ 5   ┆ Init ┆ 1.0   ┆ insert       ┆ 1               ┆ 2026-03-20 07:20:13.178 │
        // │ …   ┆ …    ┆ …     ┆ …            ┆ …               ┆ …                       │
        // │ 46  ┆ Init ┆ 1.0   ┆ delete       ┆ 15              ┆ 2026-03-20 07:20:15.270 │
        // │ 47  ┆ Init ┆ 1.0   ┆ delete       ┆ 15              ┆ 2026-03-20 07:20:15.270 │
        // │ 48  ┆ Init ┆ 1.0   ┆ delete       ┆ 15              ┆ 2026-03-20 07:20:15.270 │
        // │ 49  ┆ Init ┆ 1.0   ┆ delete       ┆ 15              ┆ 2026-03-20 07:20:15.270 │
        // │ 50  ┆ Init ┆ 1.0   ┆ delete       ┆ 15              ┆ 2026-03-20 07:20:15.270 │
        // └─────┴──────┴───────┴──────────────┴─────────────────┴─────────────────────────┘

        var remainingIds = resultDf["Id"].ToArray<int>();

        Assert.DoesNotContain(1, remainingIds);
        Assert.DoesNotContain(30, remainingIds);

        var row31 = resultDf.Filter(Pl.Col("Id") == 31);
        Assert.Equal(31, row31["Id"].ToArray<int>()[0]);
        Assert.Equal(99.9, row31["Value"].ToArray<double>()[0]);

        Assert.Contains(101, remainingIds);
        Assert.Contains(150, remainingIds);

        Environment.SetEnvironmentVariable("POLARS_DELTA_MAX_RETRIES", null);
    }
    [Fact]
    [Trait("Catalog", "Maintenance")]
    public void Test_Catalog_History_Restore_Vacuum_Lifecycle()
    {
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

        using var dfV1 = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3 },
            Hero = new[] { "Iron Man", "Captain America", "Thor" }
        });
        
        dfV1.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Overwrite, cloudOptions: cloudOptions);


        using var dfV2 = DataFrame.FromColumns(new { 
            Id = new[] { 4, 5 },
            Hero = new[] { "Thanos", "Ultron" } 
        });

        dfV2.WriteCatalogTable(uc, catalog, schema, table, mode: DeltaSaveMode.Append, cloudOptions: cloudOptions);

        using var currentLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var currentDf = currentLf.Collect();
        Assert.Equal(5, currentDf.Height);

        using var historyDf = uc.DeltaHistory(catalog, schema, table, cloudOptions: cloudOptions);
        
        // --- Table History ---
        // shape: (3, 10)
        // ┌───────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬──────────┐
        // │ timestamp ┆ operation ┆ mode      ┆ protocol  ┆ … ┆ engineInf ┆ clientVer ┆ engineMak ┆ engine   │
        // │ ---       ┆ ---       ┆ ---       ┆ ---       ┆   ┆ o         ┆ sion      ┆ er        ┆ ---      │
        // │ datetime[ ┆ str       ┆ str       ┆ str       ┆   ┆ ---       ┆ ---       ┆ ---       ┆ str      │
        // │ ms, UCT]  ┆           ┆           ┆           ┆   ┆ str       ┆ str       ┆ str       ┆          │
        // ╞═══════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪══════════╡
        // │ 2026-03-2 ┆ WRITE     ┆ Append    ┆ null      ┆ … ┆ delta-rs: ┆ delta-rs. ┆ ErrorLSC  ┆ Polars.N │
        // │ 0 07:21:4 ┆           ┆           ┆           ┆   ┆ 0.31.1    ┆ 0.31.1    ┆           ┆ ET       │
        // │ 8.343 UTC ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
        // │ 2026-03-2 ┆ WRITE     ┆ Overwrite ┆ null      ┆ … ┆ delta-rs: ┆ delta-rs. ┆ ErrorLSC  ┆ Polars.N │
        // │ 0 07:21:4 ┆           ┆           ┆           ┆   ┆ 0.31.1    ┆ 0.31.1    ┆           ┆ ET       │
        // │ 8.123 UTC ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
        // │ 2026-03-2 ┆ CREATE    ┆ ErrorIfEx ┆ {"minRead ┆ … ┆ delta-rs: ┆ delta-rs. ┆ null      ┆ null     │
        // │ 0 07:21:4 ┆ TABLE     ┆ ists      ┆ erVersion ┆   ┆ 0.31.1    ┆ 0.31.1    ┆           ┆          │
        // │ 8.095 UTC ┆           ┆           ┆ ":1,"minW ┆   ┆           ┆           ┆           ┆          │
        // │           ┆           ┆           ┆ rit…      ┆   ┆           ┆           ┆           ┆          │
        // └───────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴──────────┘
        
        Assert.True(historyDf.Height >= 2);

        ulong newVersion = uc.DeltaRestore(
            catalog, schema, table, 
            version: 1, 
            cloudOptions: cloudOptions
        );

        Assert.Equal(3UL, newVersion);

        using var restoredLf = uc.ScanCatalogTable(catalog, schema, table, cloudOptions: cloudOptions);
        using var restoredDf = restoredLf.Collect().Sort("Id");
        
        Assert.Equal(3, restoredDf.Height);

        long deletedFiles = uc.DeltaVacuum(
            catalog, schema, table,
            retentionHours: 0,          
            enforceRetention: false,    
            cloudOptions: cloudOptions
        );

        Assert.True(deletedFiles > 0);
    }
}