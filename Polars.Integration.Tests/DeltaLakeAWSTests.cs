using Polars.Integration.Tests.Fixtures;
using Polars.CSharp;
using Pl = Polars.CSharp.Polars;
using System.Text;
using Minio;
using Minio.DataModel.Args;
using Polars.Integration.Tests.Utils;
using Polars.NET.Core;
using Polars.NET.Linq.CSharpExtensions;

namespace Polars.Integration.Tests;

public class DeltaLakeTests(MinioFixture minio) : IClassFixture<MinioFixture>
{
    [Fact]
    [Trait("DeltaLake","Scan")]
    public async Task Test_Scan_Delta_AWS_Minio()
    {
        var tableName = $"delta_table_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        var parquetFileName = "part-0000.parquet";
        var parquetUrl = $"{rootUrl}/{parquetFileName}";

        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');

        var polarsEndpoint = $"http://{rawEndpoint}";

        var minioSdkEndpoint = rawEndpoint;

        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint 
        );
        options.Credentials!["aws_allow_http"] = "true"; 
        options.Credentials!["aws_s3_force_path_style"] = "true"; 

        using var df = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 3 },
            Name = new[] { "Polars", "Delta", "Rust" },
            Value = new[] { 10.5, 20.0, 30.5 }
        });

        df.WriteParquet(parquetUrl, cloudOptions: options);

        var schemaObj = new
        {
            type = "struct",
            fields = new object[]
            {
                new { name = "Id", type = "integer", nullable = true, metadata = new { } },
                new { name = "Name", type = "string", nullable = true, metadata = new { } },
                new { name = "Value", type = "double", nullable = true, metadata = new { } }
            }
        };
        var schemaString = System.Text.Json.JsonSerializer.Serialize(schemaObj);

        var sb = new StringBuilder();

        // Line 1: Protocol
        // {"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
        sb.AppendLine("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}");

        // Line 2: Metadata 
        var metadataInner = new
        {
            id = Guid.NewGuid().ToString(),
            format = new { provider = "parquet", options = new { } },
            schemaString,
            partitionColumns = new string[0],
            configuration = new { },
            createdTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };
        sb.AppendLine($"{{\"metaData\":{System.Text.Json.JsonSerializer.Serialize(metadataInner)}}}");

        // Line 3: Add
        var addInner = new
        {
            path = parquetFileName,
            partitionValues = new { },
            size = 1000L,
            modificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            dataChange = true,
            stats = "{\"numRecords\":3}"
        };
        sb.AppendLine($"{{\"add\":{System.Text.Json.JsonSerializer.Serialize(addInner)}}}");

        var logContent = sb.ToString();

        var logPath = $"{tableName}/_delta_log/00000000000000000000.json";

        var minioClient = new MinioClient()
            .WithEndpoint(minioSdkEndpoint)
            .WithCredentials(minio.AccessKey, minio.SecretKey)
            .WithRegion(minio.Region)
            .Build();

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(logContent));
        await minioClient.PutObjectAsync(new PutObjectArgs()
            .WithBucket(minio.BucketName)
            .WithObject(logPath)
            .WithStreamData(stream)
            .WithObjectSize(stream.Length)
            .WithContentType("application/json"));

        // pl_scan_delta -> deltalake::open_table -> Polars Scan
        using var lfRead = LazyFrame.ScanDelta(rootUrl, cloudOptions: options);
        using var dfRead = lfRead.Collect();

        // shape: (3, 3)
        // ┌─────┬────────┬───────┐
        // │ Id  ┆ Name   ┆ Value │
        // │ --- ┆ ---    ┆ ---   │
        // │ i32 ┆ str    ┆ f64   │
        // ╞═════╪════════╪═══════╡
        // │ 1   ┆ Polars ┆ 10.5  │
        // │ 2   ┆ Delta  ┆ 20.0  │
        // │ 3   ┆ Rust   ┆ 30.5  │
        // └─────┴────────┴───────┘
        Assert.Equal(df.Height, dfRead.Height);
        
        var originalNames = df["Name"].ToArray<string>();
        var readNames = dfRead["Name"].ToArray<string>();
        
        Assert.Equal(originalNames, readNames);
        
        Assert.Contains("Value", dfRead.ColumnNames);

    }
    [Fact]
    [Trait("DeltaLake","TimeTravel")]
    public async Task Test_Scan_Delta_TimeTravel()
    {

        var tableName = $"delta_time_travel_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        
        // Endpoint
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}"; 
        
        // Polars Cloud Options
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );
        options.Credentials!["aws_allow_http"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";

        // MinIO Client
        var minioClient = new MinioClient()
            .WithEndpoint(rawEndpoint)
            .WithCredentials(minio.AccessKey, minio.SecretKey)
            .WithRegion(minio.Region)
            .Build();

        var timeV0 = DateTimeOffset.UtcNow.AddHours(-1); 

        var timeV1 = DateTimeOffset.UtcNow;


        var fileV0 = "part-v0.parquet";
        
        using (var df0 = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, Name = new[] { "V0" }, Value = new[] { 10.0 } 
        }))
        {
            df0.WriteParquet($"{rootUrl}/{fileV0}", cloudOptions: options);
        }

        // Log V0 (Protocol + Metadata + Add)
        var sb0 = new StringBuilder();
        sb0.AppendLine(DeltaLakeTestHelper.ActionProtocol());
        sb0.AppendLine(DeltaLakeTestHelper.ActionMetadata(
            DeltaLakeTestHelper.GenerateSchemaString(), 
            timeV0.ToUnixTimeMilliseconds() // 手动指定时间
        ));
        sb0.AppendLine(DeltaLakeTestHelper.ActionAdd(fileV0, modTime: timeV0.ToUnixTimeMilliseconds()));
        
        await DeltaLakeTestHelper.UploadLogAsync(minioClient, minio.BucketName, tableName, 0, sb0.ToString());

        // ==========================================
        // Version 1 (Append Commit)
        // ==========================================
        var fileV1 = "part-v1.parquet";

        using (var df1 = DataFrame.FromColumns(new { 
            Id = new[] { 2 }, Name = new[] { "V1" }, Value = new[] { 20.0 } 
        }))
        {
            df1.WriteParquet($"{rootUrl}/{fileV1}", cloudOptions: options);
        }

        var sb1 = new StringBuilder();
        sb1.AppendLine(DeltaLakeTestHelper.ActionAdd(fileV1, modTime: timeV1.ToUnixTimeMilliseconds()));

        await DeltaLakeTestHelper.UploadLogAsync(minioClient, minio.BucketName, tableName, 1, sb1.ToString());

        using var lfLatest = LazyFrame.ScanDelta(rootUrl, cloudOptions: options);
        var dfLatest = lfLatest.Collect();
        
        Assert.Equal(2, dfLatest.Height); 
        Assert.Contains("V1", dfLatest["Name"].ToArray<string>());

        using var lfV0 = LazyFrame.ScanDelta(rootUrl, version: 0, cloudOptions: options);
        var dfV0 = lfV0.Collect();

        Assert.Equal(1, dfV0.Height); 
        Assert.Equal("V0", dfV0["Name"].ToArray<string>()[0]);

        var queryTime = timeV0.AddMinutes(1).ToString("yyyy-MM-ddTHH:mm:ssZ");
        
        using var lfTime = LazyFrame.ScanDelta(rootUrl, datetime: queryTime, cloudOptions: options);
        var dfTime = lfTime.Collect();

        Assert.Equal(1, dfTime.Height);
        Assert.Equal("V0", dfTime["Name"].ToArray<string>()[0]);
    }
    [Fact]
    [Trait("DeltaLake", "FullCycle")]
    public void Test_Sink_And_Scan_Delta_Full_Cycle_Modes()
    {
        var tableName = $"delta_modes_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );

        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_allow_http"] = "true";
        options.Credentials!["AWS_S3_FORCE_PATH_STYLE"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";
        
        // ==========================================
        // Mode: Append Version 0 + 1
        // ==========================================

        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2 }, 
            Msg = new[] { "V1_A", "V1_B" } 
        }))
        {
            df.WriteDelta(rootUrl, mode: DeltaSaveMode.Append, cloudOptions: options);
        }

        using var dfV1 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(2, dfV1.Height);

        // ==========================================
        // Mode: Append Version 2
        // ==========================================

        using (var df2 = DataFrame.FromColumns(new { 
            Id = new[] { 3 }, 
            Msg = new[] { "V2_C" } 
        }))
        {
            df2.WriteDelta(rootUrl, mode: DeltaSaveMode.Append, cloudOptions: options);
        }

        using var dfV2 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(3, dfV2.Height);
        Assert.Contains("V1_A", dfV2["Msg"].ToArray<string>());
        Assert.Contains("V2_C", dfV2["Msg"].ToArray<string>());

        // ==========================================
        // Mode: Overwrite Version 3
        // ==========================================

        using (var dfOverwrite = DataFrame.FromColumns(new { 
            Id = new[] { 999 }, 
            Msg = new[] { "Overwrite_New" } 
        }))
        {
            dfOverwrite.WriteDelta(rootUrl, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
        }

        using var dfV3 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(1, dfV3.Height);
        Assert.Equal("Overwrite_New", dfV3["Msg"].ToArray<string>()[0]);

        using var dfBackToV2 = LazyFrame.ScanDelta(rootUrl, version: 2, cloudOptions: options).Collect();
        Assert.Equal(3, dfBackToV2.Height);

        // ==========================================
        // Mode: ErrorIfExists
        // ==========================================

        using (var dfError = DataFrame.FromColumns(new { Id = new[] { 0 } }))
        {
            var ex = Assert.Throws<PolarsException>(() => 
            {
                dfError.WriteDelta(rootUrl, mode: DeltaSaveMode.ErrorIfExists, cloudOptions: options);
            });
            
            Assert.Contains("Table already exists", ex.Message);
        }

        // ==========================================
        // Mode: Ignore
        // ==========================================
        using (var dfIgnore = DataFrame.FromColumns(new { 
            Id = new[] { 888 }, 
            Msg = new[] { "Should_Not_Exist" } 
        }))
        {
            dfIgnore.WriteDelta(rootUrl, mode: DeltaSaveMode.Ignore, cloudOptions: options);
        }

        using var dfFinal = DataFrame.ReadDelta(rootUrl, cloudOptions: options);
        Assert.Equal(1, dfFinal.Height);
        Assert.DoesNotContain("Should_Not_Exist", dfFinal["Msg"].ToArray<string>());

    }
    [Fact]
    [Trait("DeltaLake", "Partitioned")]
    public void Test_Sink_And_Scan_Delta_Partitioned_Full_Cycle()
    {
        var tableName = $"delta_partitioned_{Guid.NewGuid()}";

        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );

        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";
        
        // ==========================================
        // Mode: Append (Version 0 + 1)
        // ==========================================

        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3 }, 
            Msg = new[] { "V1_2023_A", "V1_2023_B", "V1_2024_A" },
            Year = new[] { "2023", "2023", "2024" } 
        }))
        {
            df.WriteDelta(
                rootUrl, 
                partitionBy: "Year", 
                mode: DeltaSaveMode.Append, 
                cloudOptions: options
            );
        }

        using var dfV1 = DataFrame.ReadDelta(rootUrl, cloudOptions: options).Sort("Id");
        Assert.Equal(3, dfV1.Height);
        
        var yearsV0 = dfV1["Year"].ToArray<string>();
        Assert.Equal("2023", yearsV0[0]);
        Assert.Equal("2024", yearsV0[2]);

        // ==========================================
        // Mode: Append Version 2
        // ==========================================
        using (var df2 = DataFrame.FromColumns(new { 
            Id = new[] { 4, 5 }, 
            Msg = new[] { "V2_2024_B", "V2_2025_A" },
            Year = new[] { "2024", "2025" } 
        }))
        {
            df2.WriteDelta(
                rootUrl, 
                partitionBy: "Year", 
                mode: DeltaSaveMode.Append, 
                cloudOptions: options
            );
        }

        using var dfV2 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect().Sort("Id");
        Assert.Equal(5, dfV2.Height);
        
        Assert.Contains("2025", dfV2["Year"].ToArray<string>());

        // ==========================================
        // Mode: Overwrite Version 3
        // ==========================================

        using (var dfOverwrite = DataFrame.FromColumns(new { 
            Id = new[] { 999 }, 
            Msg = new[] { "Overwrite_All" },
            Year = new[] { "2099" } 
        }))
        {
            dfOverwrite.WriteDelta(
                rootUrl, 
                partitionBy: "Year", 
                mode: DeltaSaveMode.Overwrite, 
                cloudOptions: options
            );
        }

        using var dfV3 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(1, dfV3.Height);
        Assert.Equal("2099", dfV3["Year"].ToArray<string>()[0]);
        
        using var dfBackToV1 = LazyFrame.ScanDelta(rootUrl, version: 2, cloudOptions: options).Collect();
        Assert.Equal(5, dfBackToV1.Height);

        // ==========================================
        // Mode: ErrorIfExists
        // ==========================================
        using (var dfError = DataFrame.FromColumns(new { Id = new[] { 0 }, Year = new[] { "0000" } }))
        {
            var ex = Assert.Throws<PolarsException>(() => 
            {
                dfError.WriteDelta(
                    rootUrl, 
                    partitionBy: "Year",
                    mode: DeltaSaveMode.ErrorIfExists, 
                    cloudOptions: options
                );
            });
            Assert.Contains("Table already exists", ex.Message);
        }

        // ==========================================
        // Mode: Ignore
        // ==========================================
        using (var dfIgnore = DataFrame.FromColumns(new { 
            Id = new[] { 888 }, 
            Msg = new[] { "Should_Not_Exist" },
            Year = new[] { "2099" }
        }))
        {
            dfIgnore.WriteDelta(
                rootUrl, 
                partitionBy: "Year",
                mode: DeltaSaveMode.Ignore, 
                cloudOptions: options
            );
        }

        using var dfFinal = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(1, dfFinal.Height);
        Assert.DoesNotContain("Should_Not_Exist", dfFinal["Msg"].ToArray<string>());

    }
    [Fact]
    [Trait("DeltaLake", "SchemaEvolution")]
    public void Test_Sink_Delta_Schema_Evolution_Cycle()
    {
        var tableName = $"delta_evolve_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: $"http://{minio.Endpoint.Replace("http://", "")}"
        );
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        // ==========================================
        // Phase 1: Schema A: Id, Val
        // ==========================================
        using (var dfInit = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, 
            Val = new[] { "OldRow" } 
        }))
        {
            dfInit.WriteDelta(
                rootUrl, 
                partitionBy: "Id", 
                mode: DeltaSaveMode.Overwrite, 
                cloudOptions: options
            );
        }

        // ==========================================
        // Phase 2: Schema B: Id, Val, NewCol
        // can_evolve = false => Exception
        // ==========================================
        using var dfNew = DataFrame.FromColumns(new { 
            Id = new[] { 2 }, 
            Val = new[] { "NewRow" }, 
            NewCol = new[] { 999 } 
        });

        var ex = Assert.Throws<PolarsException>(() => 
        {
            dfNew.WriteDelta(
                rootUrl, 
                partitionBy: "Id", 
                mode: DeltaSaveMode.Append, 
                canEvolve: false, 
                cloudOptions: options
            );
        });

        Assert.Contains("Schema mismatch", ex.Message);
        Assert.Contains("can_evolve", ex.Message);

        // ==========================================
        // Phase 3: Schema Evolution
        // ==========================================
        dfNew.WriteDelta(
            rootUrl, 
            partitionBy: "Id",
            mode: DeltaSaveMode.Append, 
            canEvolve: true,
            cloudOptions: options
        );

        // ==========================================
        // Phase 4: Read Validation
        // ==========================================
        using var resultDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Sort("Id", false) 
            .Collect();

        var columns = resultDf.ColumnNames;
        Assert.Contains("NewCol", columns);

        Assert.Equal(2, resultDf.Height);

        var row1 = resultDf.Row(0);
        Assert.Equal(1, row1[0]);
        Assert.Null(row1[2]);     

        var row2 = resultDf.Row(1); 
        Assert.Equal(2, row2[0]); 
        Assert.Equal(999, row2[2]); 

    }
    [Fact]
    [Trait("DeltaLake", "Delete")]
    public void Test_Sink_And_Delete_Delta_Full_Cycle()
    {
        var tableName = $"delta_delete_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );

        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";
        
        // ==========================================
        // Version 1
        // ==========================================
        // 2023: ID 1, 2
        // 2024: ID 3, 4, 5
        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3, 4, 5 }, 
            Msg = new[] { "A", "B", "C", "D", "E" },
            Year = new[] { "2023", "2023", "2024", "2024", "2024" }
        }))
        {
            df.WriteDelta(
                rootUrl, 
                partitionBy: "Year", 
                mode: DeltaSaveMode.Append, 
                cloudOptions: options
            );
        }

        // V1 (5 rows)
        using var dfV1 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(5, dfV1.Height);

        // ==========================================
        // Rewrite Test
        // ==========================================
        
        // Predicate: (Year == '2024') & (Id == 4)
        var predicateRewrite = (Pl.Col("Year") == Pl.Lit("2024")) & (Pl.Col("Id")==4);
        
        Delta.Delete(rootUrl, predicateRewrite, cloudOptions: options);

        using var dfV2 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect().Sort("Id");
        Assert.Equal(4, dfV2.Height);
        Assert.DoesNotContain(4, dfV2["Id"].ToArray<int>());
        Assert.Contains(3, dfV2["Id"].ToArray<int>());
        Assert.Contains(5, dfV2["Id"].ToArray<int>());

        // ==========================================
        // Drop Partition
        // ==========================================
        
        var predicateDrop = Pl.Col("Year") == Pl.Lit("2023");
        
        Delta.Delete(rootUrl, predicateDrop, cloudOptions: options);

        using var dfV3 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect().Sort("Id");
        Assert.Equal(2, dfV3.Height);
        Assert.DoesNotContain(1, dfV3["Id"].ToArray<int>());
        Assert.DoesNotContain(2, dfV3["Id"].ToArray<int>());
        Assert.Equal("2024", dfV3["Year"].ToArray<string>()[0]); 

        // ==========================================
        // No-op Test
        // ==========================================

        
        var predicateNoOp = Pl.Col("Id") == 999;
        
        Delta.Delete(rootUrl, predicateNoOp, cloudOptions: options);

        using var dfV4 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();

        Assert.Equal(2, dfV4.Height);

        // ==========================================
        // Time Travel 
        // ==========================================

        // V1
        using var dfBackV1 = LazyFrame.ScanDelta(rootUrl, version: 1, cloudOptions: options).Collect();
        
        // V2 
        using var dfBackV2 = LazyFrame.ScanDelta(rootUrl, version: 2, cloudOptions: options).Collect();
        Assert.Equal(4, dfBackV2.Height);
        Assert.DoesNotContain(4, dfBackV2["Id"].ToArray<int>());

    }
    [Fact]
    [Trait("DeltaLake", "DeleteDV")]
    public void Test_Sink_And_Delete_Delta_With_Deletion_Vectors()
    {
        var tableName = $"delta_dv_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        // ==========================================
        // V1 - CoW Ready
        // ==========================================
        using (var df = DataFrame.FromColumns(new { 
            Id = Enumerable.Range(0, 10).ToArray(), // 0..9
            Data = Enumerable.Repeat("A", 10).ToArray()
        }))
        {
            df.WriteDelta(rootUrl, mode: DeltaSaveMode.Append, cloudOptions: options);
        }

        // ==========================================
        // Deletion Vectors (Upgrade to v7)
        // ==========================================
        
        // Protocol => Reader v3 / Writer v7
        Delta.AddFeature(
            rootUrl, 
            DeltaTableFeatures.DeletionVectors, 
            allowProtocolIncrease: true, 
            cloudOptions: options
        );

        int[] validIds = [0,2,4];
        
        var predicate = Pl.Col("Id").IsIn(Pl.Lit(validIds).Implode());
        
        Delta.Delete(rootUrl, predicate, cloudOptions: options);

        using var dfV2 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect().Sort("Id");

        Assert.Equal(7, dfV2.Height);
        
        var ids = dfV2["Id"].ToArray<int>();
        Assert.DoesNotContain(0, ids);
        Assert.DoesNotContain(2, ids);
        Assert.DoesNotContain(4, ids);
        Assert.Contains(1, ids);
        Assert.Contains(9, ids);
        
        using var dfV1 = LazyFrame.ScanDelta(rootUrl, version: 1, cloudOptions: options).Collect();
        Assert.Equal(10, dfV1.Height);
        
    }
    [Fact]
    [Trait("DeltaLake", "Upsert")]
    public void Test_Upsert_Delta_Full_Cycle_Upsert()
    {
        var tableName = $"delta_merge_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );

        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";
        
        // ==========================================
        // [Target] Version 1
        // ==========================================

        // 2024-01-01: Order 1 (Pending), Order 2 (Pending)
        // 2024-01-02: Order 3 (Pending)
        using (var df = DataFrame.FromColumns(new { 
            OrderId = new[] { 1, 2, 3 }, 
            Status = new[] { "Pending", "Pending", "Pending" },
            Amount = new[] { 100.0, 200.0, 300.0 },
            Date = new[] { "2024-01-01", "2024-01-01", "2024-01-02" } 
        }))
        {
            df.WriteDelta(
                rootUrl, 
                partitionBy: "Date", 
                mode: DeltaSaveMode.Append, 
                cloudOptions: options
            );
        }

        using var dfV1 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect().Sort("OrderId");
        Assert.Equal(3, dfV1.Height);

        // ==========================================
        // [Source]
        // ==========================================
        
        // Source DataFrame
        // 1. Update: Order 1 -> Status="Shipped" 
        // 2. Insert (Backfill): Order 4 
        // 3. Insert (New Partition): Order 5
        // 4. Unchanged: Order 2, Order 3 
        
        using var sourceDf = DataFrame.FromColumns(new { 
            OrderId = new[] { 1, 4, 5 }, 
            Status = new[] { "Shipped", "Paid", "New" },
            Amount = new[] { 100.0, 400.0, 500.0 }, 
            Date = new[] { "2024-01-01", "2024-01-01", "2024-01-03" } 
        });
        // ==========================================
        // MERGE (Upsert)
        // ==========================================
        
        sourceDf.MergeDeltaOrdered(rootUrl, mergeKeys: ["OrderId"], cloudOptions: options).Execute();

        // ==========================================
        // Result (Version 2)
        // ==========================================

        using var dfMerged = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect().Sort("OrderId");

        Assert.Equal(5, dfMerged.Height);

        var row1 = dfMerged.Filter(Pl.Col("OrderId") == 1);
        Assert.Equal("Shipped", row1["Status"].ToArray<string>()[0]);
        Assert.Equal("2024-01-01", row1["Date"].ToArray<string>()[0]); 

        var row4 = dfMerged.Filter(Pl.Col("OrderId") == 4);
        Assert.Equal("Paid", row4["Status"].ToArray<string>()[0]);
        Assert.Equal("2024-01-01", row4["Date"].ToArray<string>()[0]);

        var row5 = dfMerged.Filter(Pl.Col("OrderId") == 5);
        Assert.Equal("New", row5["Status"].ToArray<string>()[0]);
        Assert.Equal("2024-01-03", row5["Date"].ToArray<string>()[0]);

        var row2 = dfMerged.Filter(Pl.Col("OrderId") ==2 );
        Assert.Equal("Pending", row2["Status"].ToArray<string>()[0]);
        
        var row3 = dfMerged.Filter(Pl.Col("OrderId")==3);
        Assert.Equal("Pending", row3["Status"].ToArray<string>()[0]);

        // ==========================================
        // Partition Pruning Check
        // ==========================================
        // Source Partition: 2024-01-01, 2024-01-03
        // Target Partition: 2024-01-01, 2024-01-02
        
        using var dfHistory = LazyFrame.ScanDelta(rootUrl, version: 1, cloudOptions: options).Collect().Sort("OrderId");
        
        var row1Old = dfHistory.Filter(Pl.Col("OrderId")==1);
        Assert.Equal("Pending", row1Old["Status"].ToArray<string>()[0]);

        Assert.Equal(0, dfHistory.Filter(Pl.Col("OrderId")==4).Height);

    }
    [Fact]
    [Trait("DeltaLake", "MergeSchemaEvolution")]
    public void Test_Merge_Delta_Schema_Evolution_Full_Cycle()
    {
        var tableName = $"delta_merge_evolve_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: $"http://{minio.Endpoint.Replace("http://", "")}"
        );
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        // ==========================================
        // Phase 1: Schema V1
        // ==========================================
        // Schema: [Id: Int, Val: String]
        // Data:   (1, "Old_A")
        using (var dfV1 = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, 
            Val = new[] { "Old_A" } 
        }))
        {
            dfV1.WriteDelta(rootUrl, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
        }

        // ==========================================
        // Phase 2: Source Schema V2
        // ==========================================
        // Schema: [Id: Int, Val: String, NewCol: String] 
        // Data:
        //   (1, "Updated_A", "Filled")  <-- Matched Update
        //   (2, "New_B",     "Filled")  <-- Not Matched Insert
        using var sourceDf = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2 }, 
            Val = new[] { "Updated_A", "New_B" }, 
            NewCol = new[] { "Filled", "Filled" } 
        });

        // ==========================================
        // Phase 3: Negative Test
        // ==========================================
        var ex = Assert.Throws<PolarsException>(() => 
        {
            sourceDf.MergeDeltaOrdered(
                rootUrl, 
                mergeKeys: ["Id"], 
                cloudOptions: options
            ).Execute();
        });

        Assert.Contains("Schema mismatch", ex.Message);
        Assert.Contains("NewCol", ex.Message);
        Assert.Contains("can_evolve", ex.Message); 

        // ==========================================
        // Phase 4: Evolution Test
        // ==========================================
        sourceDf.MergeDeltaOrdered(
            rootUrl, 
            mergeKeys: ["Id"], 
            cloudOptions: options,
            canEvolve: true 
        ).Execute();

        // ==========================================
        // Phase 5:Read & Verify
        // ==========================================
        using var resultDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Sort("Id", false)
            .Collect();

        var cols = resultDf.ColumnNames;
        Assert.Contains("NewCol", cols);
        Assert.Equal(3, cols.Length); 

        var row1 = resultDf.Row(0);
        Assert.Equal(1, row1[0]);           
        Assert.Equal("Updated_A", row1[1]); 
        Assert.Equal("Filled", row1[2]);   

        var row2 = resultDf.Row(1);
        Assert.Equal(2, row2[0]);
        Assert.Equal("New_B", row2[1]);
        Assert.Equal("Filled", row2[2]);

        // ==========================================
        // Phase 6: Backfill Null Test
        // ==========================================
        
        using (var dfV1_Late = DataFrame.FromColumns(new { 
            Id = new[] { 3 }, 
            Val = new[] { "Late_Arrival_V1" } 
        }))
        {
            dfV1_Late.MergeDeltaOrdered(
                rootUrl, 
                mergeKeys: ["Id"], 
                cloudOptions: options
            ).Execute();
        }

        // ==========================================
        // Phase 7: Verification
        // ==========================================
        using var finalDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Sort("Id", false)
            .Collect();

        var row3 = finalDf.Row(2); 
        Assert.Equal(3, row3[0]);
        Assert.Equal("Late_Arrival_V1", row3[1]);
        Assert.Null(row3[2]); 

    }
    [Fact]
    [Trait("DeltaLake", "Merge")]
    public void Test_Merge_Delta_Full_Features_Complex_Logic()
    {
        var tableName = $"delta_merge_full_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        // ==========================================
        // [Target] Version 1
        // ==========================================
        // ID 1: Stock=10, Status=Active  
        // ID 2: Stock=20, Status=Active   
        // ID 3: Stock=0,  Status=Recall  
        // ID 4: Stock=0,  Status=Obsolete 
        // ID 5: Stock=50, Status=Active  
        
        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3, 4, 5 }, 
            Stock = new[] { 10, 20, 0, 0, 50 },
            Status = new[] { "Active", "Active", "Recall", "Obsolete", "Active" },
            Category = new[] { "A", "A", "B", "B", "A" } // Partition
        }))
        {
            df.WriteDelta(
                rootUrl, 
                partitionBy: "Category", 
                mode: DeltaSaveMode.Append, 
                cloudOptions: options
            );
        }

        // ==========================================
        // Merge Source
        // ==========================================
        
        // ID 1: Stock=100 (New) -> Update (New > Old)
        // ID 2: Stock=15  (New) -> Not Update (15 < 20)，Keep 20
        // ID 3: Status=DeleteMe -> Matched Delete 
        // ID 6: Stock=60  (New) -> Insert (Stock > 0)
        // ID 7: Stock=0   (New) -> Not Insert (Stock > 0),Discard
        // ID 4 & 5: Not Matched By Source
        
        using var sourceDf = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3, 6, 7 }, 
            Stock = new[] { 100, 15, 0, 60, 0 },
            Status = new[] { "Active", "Active", "DeleteMe", "New", "Bad" },
            Category = new[] { "A", "A", "B", "C", "C" }
        });

        // ==========================================
        // MERGE Expression
        // ==========================================
        // A. Update Condition: Source Stock > Target Stock
        static Expr updateCond(MergeContext m) => m.Source("Stock") > m.Target("Stock");

        // B. Matched Delete Condition: Source Status set as 'DeleteMe'
        static Expr matchDeleteCond(MergeContext m) => m.Source("Status") == "DeleteMe";

        // C. Insert Condition: Source Stock > 0 
        static Expr insertCond(MergeContext m) => m.Source("Stock") > 0;

        // D. Source Delete (Target Only) Condition: Target Status is 'Obsolete'
        static Expr srcDeleteCond(MergeContext m) => m.Target("Status") == "Obsolete";

        // ==========================================
        // Full Merge
        // ==========================================

        sourceDf.MergeDeltaOrdered(
            rootUrl,
            mergeKeys: ["Id"],
            cloudOptions: options
        )
        .WhenMatchedDelete(matchDeleteCond)
        .WhenMatchedUpdate(updateCond)
        .WhenNotMatchedBySourceDelete(srcDeleteCond)
        .WhenNotMatchedInsert(insertCond)
        .Execute();
        
        using var dfRes = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect().Sort("Id");
        // shape: (4, 4)
        // ┌─────┬───────┬────────┬──────────┐
        // │ Id  ┆ Stock ┆ Status ┆ Category │
        // │ --- ┆ ---   ┆ ---    ┆ ---      │
        // │ i32 ┆ i32   ┆ str    ┆ str      │
        // ╞═════╪═══════╪════════╪══════════╡
        // │ 1   ┆ 100   ┆ Active ┆ A        │
        // │ 2   ┆ 20    ┆ Active ┆ A        │
        // │ 5   ┆ 50    ┆ Active ┆ A        │
        // │ 6   ┆ 60    ┆ New    ┆ C        │
        // └─────┴───────┴────────┴──────────┘

        // ID 1: Updated to 100 (100 > 10)
        // ID 2: Kept at 20 (15 < 20, Update Condition Failed)
        // ID 3: Deleted (Matched Delete)
        // ID 4: Deleted (Target Only & Obsolete)
        // ID 5: Kept at 50 (Target Only & Active)
        // ID 6: Inserted (60 > 0)
        // ID 7: Ignored (0 !> 0)

        Assert.Equal(4, dfRes.Height);

        // Case 1: Conditional Update
        var row1 = dfRes.Filter(Pl.Col("Id") == 1);
        Assert.Equal(100, row1["Stock"].ToArray<int>()[0]);

        // Case 2: Conditional Update Skip (Keep Target)
        var row2 = dfRes.Filter(Pl.Col("Id") == 2);
        Assert.Equal(20, row2["Stock"].ToArray<int>()[0]); 

        // Case 3: Matched Delete
        Assert.Equal(0, dfRes.Filter(Pl.Col("Id") == 3).Height);

        // Case 4: Not Matched By Source Delete (Pruning)
        Assert.Equal(0, dfRes.Filter(Pl.Col("Id") == 4).Height);

        // Case 5: Not Matched By Source Keep
        var row5 = dfRes.Filter(Pl.Col("Id") == 5);
        Assert.Equal(50, row5["Stock"].ToArray<int>()[0]);

        // Case 6: Conditional Insert
        var row6 = dfRes.Filter(Pl.Col("Id") == 6);
        Assert.Equal(60, row6["Stock"].ToArray<int>()[0]);
        Assert.Equal("C", row6["Category"].ToArray<string>()[0]); 

        // Case 7: Conditional Insert Skip
        Assert.Equal(0, dfRes.Filter(Pl.Col("Id") == 7).Height);
        Delta.History(path:rootUrl,cloudOptions:options);
        // shape: (3, 16)
        // ┌───────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬──────────┐
        // │ timestamp ┆ operation ┆ mode      ┆ predicate ┆ … ┆ engineInf ┆ engine    ┆ clientVer ┆ engineMa │
        // │ ---       ┆ ---       ┆ ---       ┆ ---       ┆   ┆ o         ┆ ---       ┆ sion      ┆ ker      │
        // │ datetime[ ┆ str       ┆ str       ┆ str       ┆   ┆ ---       ┆ str       ┆ ---       ┆ ---      │
        // │ ms, UCT]  ┆           ┆           ┆           ┆   ┆ str       ┆           ┆ str       ┆ str      │
        // ╞═══════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪══════════╡
        // │ 2026-03-2 ┆ MERGE     ┆ null      ┆ source.Id ┆ … ┆ delta-rs: ┆ Polars.NE ┆ delta-rs. ┆ ErrorLSC │
        // │ 0 06:04:5 ┆           ┆           ┆ =         ┆   ┆ 0.31.1    ┆ T         ┆ 0.31.1    ┆          │
        // │ 9.813 UTC ┆           ┆           ┆ target.Id ┆   ┆           ┆           ┆           ┆          │
        // │ 2026-03-2 ┆ WRITE     ┆ Append    ┆ null      ┆ … ┆ delta-rs: ┆ Polars.NE ┆ delta-rs. ┆ ErrorLSC │
        // │ 0 06:04:5 ┆           ┆           ┆           ┆   ┆ 0.31.1    ┆ T         ┆ 0.31.1    ┆          │
        // │ 9.552 UTC ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
        // │ 2026-03-2 ┆ CREATE    ┆ ErrorIfEx ┆ null      ┆ … ┆ delta-rs: ┆ null      ┆ delta-rs. ┆ null     │
        // │ 0 06:04:5 ┆ TABLE     ┆ ists      ┆           ┆   ┆ 0.31.1    ┆           ┆ 0.31.1    ┆          │
        // │ 9.529 UTC ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
        // └───────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴──────────┘
    }
    [Fact]
    [Trait("DeltaLake", "MergeDV")]
    public void Test_Merge_Delta_With_Deletion_Vectors_Complex_Logic()
    {
        var tableName = $"delta_merge_dv_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        // ==========================================
        // [Target] (Version 1)
        // ==========================================
        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3, 4, 5 }, 
            Stock = new[] { 10, 20, 0, 0, 50 },
            Status = new[] { "Active", "Active", "Recall", "Obsolete", "Active" },
            Category = new[] { "A", "A", "B", "B", "A" } // Partition
        }))
        {
            df.WriteDelta(
                rootUrl, 
                partitionBy: "Category", 
                mode: DeltaSaveMode.Append, 
                cloudOptions: options
            );
        }

        // ==========================================
        // Deletion Vectors (Version 2)
        // ==========================================
        Delta.AddFeature(
            rootUrl, 
            DeltaTableFeatures.DeletionVectors,
            allowProtocolIncrease: true, 
            cloudOptions: options
        );

        // ==========================================
        // Merge Source
        // ==========================================
        
        using var sourceDf = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3, 6, 7 }, 
            Stock = new[] { 100, 15, 0, 60, 0 },
            Status = new[] { "Active", "Active", "DeleteMe", "New", "Bad" },
            Category = new[] { "A", "A", "B", "C", "C" }
        });

        // ==========================================
        // MERGE Expressions
        // ==========================================
        static Expr updateCond(MergeContext m) => m.Source("Stock") > m.Target("Stock");
        static Expr matchDeleteCond(MergeContext m) => m.Source("Status") == "DeleteMe";
        static Expr insertCond(MergeContext m) => m.Source("Stock") > 0;
        static Expr srcDeleteCond(MergeContext m) => m.Target("Status") == "Obsolete";


        sourceDf.MergeDeltaOrdered(
            rootUrl,
            mergeKeys: ["Id"],
            cloudOptions: options
        )
        .WhenMatchedUpdate(updateCond)
        .WhenMatchedDelete(matchDeleteCond)
        .WhenNotMatchedInsert(insertCond)
        .WhenNotMatchedBySourceDelete(srcDeleteCond)
        .Execute();

        // ==========================================
        // Version 3
        // ==========================================
        
        using var dfRes = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect().Sort("Id");

        Assert.Equal(4, dfRes.Height);

        // Case 1: Conditional Update
        var row1 = dfRes.Filter(Pl.Col("Id") == 1);
        Assert.Equal(100, row1["Stock"].ToArray<int>()[0]);

        // Case 2: Conditional Update Skip (Keep Target)
        var row2 = dfRes.Filter(Pl.Col("Id") == 2);
        Assert.Equal(20, row2["Stock"].ToArray<int>()[0]);

        // Case 3: Matched Delete
        Assert.Equal(0, dfRes.Filter(Pl.Col("Id") == 3).Height);

        // Case 4: Not Matched By Source Delete
        Assert.Equal(0, dfRes.Filter(Pl.Col("Id") == 4).Height);

        // Case 5: Not Matched By Source Keep
        var row5 = dfRes.Filter(Pl.Col("Id") == 5);
        Assert.Equal(50, row5["Stock"].ToArray<int>()[0]);

        // Case 6: Conditional Insert
        var row6 = dfRes.Filter(Pl.Col("Id") == 6);
        Assert.Equal(60, row6["Stock"].ToArray<int>()[0]);
        Assert.Equal("C", row6["Category"].ToArray<string>()[0]);

        // Case 7: Conditional Insert Skip
        Assert.Equal(0, dfRes.Filter(Pl.Col("Id") == 7).Height);

        using var dfV1 = LazyFrame.ScanDelta(rootUrl, version: 1, cloudOptions: options).Collect();
        Assert.Equal(5, dfV1.Height);

        // Delta.History(path:rootUrl, cloudOptions:options).Show();
        // shape: (4, 17)
        // ┌───────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬──────────┐
        // │ timestamp ┆ operation ┆ mode      ┆ predicate ┆ … ┆ engineInf ┆ engine    ┆ engineMak ┆ clientVe │
        // │ ---       ┆ ---       ┆ ---       ┆ ---       ┆   ┆ o         ┆ ---       ┆ er        ┆ rsion    │
        // │ datetime[ ┆ str       ┆ str       ┆ str       ┆   ┆ ---       ┆ str       ┆ ---       ┆ ---      │
        // │ ms, UCT]  ┆           ┆           ┆           ┆   ┆ str       ┆           ┆ str       ┆ str      │
        // ╞═══════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪══════════╡
        // │ 2026-03-2 ┆ MERGE     ┆ null      ┆ source.Id ┆ … ┆ delta-rs: ┆ Polars.NE ┆ ErrorLSC  ┆ delta-rs │
        // │ 0 06:15:5 ┆           ┆           ┆ =         ┆   ┆ 0.31.1    ┆ T         ┆           ┆ .0.31.1  │
        // │ 5.319 UTC ┆           ┆           ┆ target.Id ┆   ┆           ┆           ┆           ┆          │
        // │ 2026-03-2 ┆ ADD       ┆ null      ┆ null      ┆ … ┆ delta-rs: ┆ null      ┆ null      ┆ delta-rs │
        // │ 0 06:15:5 ┆ FEATURE   ┆           ┆           ┆   ┆ 0.31.1    ┆           ┆           ┆ .0.31.1  │
        // │ 5.048 UTC ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
        // │ 2026-03-2 ┆ WRITE     ┆ Append    ┆ null      ┆ … ┆ delta-rs: ┆ Polars.NE ┆ ErrorLSC  ┆ delta-rs │
        // │ 0 06:15:5 ┆           ┆           ┆           ┆   ┆ 0.31.1    ┆ T         ┆           ┆ .0.31.1  │
        // │ 4.865 UTC ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
        // │ 2026-03-2 ┆ CREATE    ┆ ErrorIfEx ┆ null      ┆ … ┆ delta-rs: ┆ null      ┆ null      ┆ delta-rs │
        // │ 0 06:15:5 ┆ TABLE     ┆ ists      ┆           ┆   ┆ 0.31.1    ┆           ┆           ┆ .0.31.1  │
        // │ 4.842 UTC ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆          │
        // └───────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴──────────┘
    }
    [Fact]
    [Trait("DeltaLake", "MergeCompositeKeys")]
    [Obsolete("This test is to test old API")]
    public void Test_Merge_Delta_Composite_Keys_Full_Logic()
    {
        var tableName = $"delta_merge_composite_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: $"http://{minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/')}"
        );
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        // ==========================================
        // [Target] Version 1
        // ==========================================
        // [Region, StoreId]
        
        // Data Scenarios:
        // 1. [North, 101]: Stock 10  -> Update
        // 2. [North, 102]: Stock 20  -> Update Skip
        // 3. [South, 101]: Stock 5   -> Matched Delete
        // 4. [South, 999]: Obsolete  -> Source Delete
        // 5. [East,  555]: Active    -> Target Only Keep

        using (var df = DataFrame.FromColumns(new { 
            Region = new[]  { "North", "North", "South", "South", "East" },
            StoreId = new[] { 101,     102,     101,     999,     555 },
            Stock = new[]   { 10,      20,      5,       0,       50 },
            Status = new[]  { "Active","Active","Recall","Obsolete","Active" }
        }))
        {
            df.WriteDelta(
                rootUrl, 
                partitionBy: "Region", 
                mode: DeltaSaveMode.Append, 
                cloudOptions: options
            );
        }

        // ==========================================
        // Merge Source
        // ==========================================

        // Source Logic:
        // 1. [North, 101]: Stock 100 (New > Old) -> Update
        // 2. [North, 102]: Stock 15  (New < Old) -> Skip Update
        // 3. [South, 101]: Status "DeleteMe"     -> Matched Delete
        // 4. [West,  888]: Stock 60              -> Insert (New Region)
        // 5. [West,  999]: Stock 0               -> Skip Insert (Condition Fail)

        using var sourceDf = DataFrame.FromColumns(new { 
            Region = new[]  { "North", "North", "South", "West", "West" },
            StoreId = new[] { 101,     102,     101,     888,     999 },
            Stock = new[]   { 100,     15,      0,       60,      0 },
            Status = new[]  { "Active","Active","DeleteMe","New",   "Bad" }
        });

        // ==========================================
        // Merge Expressions
        // ==========================================
        
        var updateCond = Delta.Source("Stock") > Delta.Target("Stock");

        var matchDeleteCond = Delta.Source("Status") == "DeleteMe";

        var insertCond = Delta.Source("Stock") > 0;

        var srcDeleteCond = Delta.Target("Status") == "Obsolete";

        sourceDf.MergeDelta(
            rootUrl,
            mergeKeys:  ["Region", "StoreId"], 
            matchedUpdateCond: updateCond,
            matchedDeleteCond: matchDeleteCond,
            notMatchedInsertCond: insertCond,
            notMatchedBySourceDeleteCond: srcDeleteCond,
            cloudOptions: options
        );

        using var dfRes = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Collect()
            .Sort(["Region", "StoreId"]); // Sort for deterministic assert

        // 1. North/101: Updated (100)
        // 2. North/102: Kept (20)
        // 3. South/101: Deleted
        // 4. South/999: Deleted (Obsolete)
        // 5. East/555:  Kept (50)
        // 6. West/888:  Inserted (60)
        // 7. West/999:  Ignored

        Assert.Equal(4, dfRes.Height);

        // Case 1: Composite Match Update
        var row1 = dfRes.Filter(Pl.Col("Region") == "North" & Pl.Col("StoreId") == 101);
        Assert.Equal(100, row1["Stock"].ToArray<int>()[0]);

        // Case 2: Composite Match Skip
        var row2 = dfRes.Filter(Pl.Col("Region") == "North" & Pl.Col("StoreId") == 102);
        Assert.Equal(20, row2["Stock"].ToArray<int>()[0]);

        // Case 3: Composite Match Delete (South/101)
        Assert.Equal(0, dfRes.Filter(Pl.Col("Region") == "South" & Pl.Col("StoreId") == 101).Height);

        // Case 4: Not Matched By Source Delete
        Assert.Equal(0, dfRes.Filter(Pl.Col("Region") == "South" & Pl.Col("StoreId") == 999).Height);

        // Case 5: Target Only Keep
        var rowEast = dfRes.Filter(Pl.Col("Region") == "East");
        Assert.Equal(50, rowEast["Stock"].ToArray<int>()[0]);

        // Case 6: Insert
        var rowWest = dfRes.Filter(Pl.Col("Region") == "West" & Pl.Col("StoreId") == 888);
        Assert.Equal(60, rowWest["Stock"].ToArray<int>()[0]);

    }
    [Fact]
    [Trait("DeltaLake", "MergePartialUpdate")]
    public void Test_Merge_Delta_Partial_Update_Allowed()
    {
        var tableName = $"delta_merge_partial_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        // Target: Id, ColA, ColB
        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, 
            ColA = new[] { 10 }, 
            ColB = new[] { 20 } 
        }))
        {
            df.WriteDelta(rootUrl, mode: DeltaSaveMode.Append, cloudOptions: options);
        }

        // Source: Id, ColA (Missing ColB)
        using var sourceDf = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, 
            ColA = new[] { 99 }
        });

        sourceDf.MergeDeltaOrdered(rootUrl, mergeKeys: ["Id"], cloudOptions: options).Execute();
        using var res = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();

        // ColA (10 -> 99)
        Assert.Equal(99, res["ColA"][0]);
        // ColB (20 -> 20)
        Assert.Equal(20, res["ColB"][0]);
    }
    [Fact]
    [Trait("DeltaLake", "MergeDuplicateSource")]
    public void Test_Merge_Delta_With_Duplicate_Source_Keys()
    {
        var tableName = $"delta_merge_dup_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        // Prepare Target Data (Id=1, Val=10)
        using (var dfTarget = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, 
            Time = new[] {"2025"},
            Val = new[] { 10 } 
        }))
        {
            dfTarget.WriteDelta(rootUrl, mode: DeltaSaveMode.Append, cloudOptions: options);
        }

        using var dfSource = DataFrame.FromColumns(new { 
            Id = new[] { 1, 1,1 }, 
            Time = new[] {"2025","2024","2025"},
            Val = new[] { 88, 99,100 }
        });
        var ex = Assert.Throws<PolarsException>(() => 
        {
            dfSource.MergeDeltaOrdered(rootUrl, mergeKeys: ["Id","Time"], cloudOptions: options).Execute();
        });

    }
    [Fact]
    [Trait("DeltaLake", "MergeExplicitNull")]
    public void Test_Merge_Delta_Explicit_Null_Overwrites_Value()
    {
        var tableName = $"delta_merge_null_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        // Target: Id=1, Val=100
        using (var dfTarget = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, 
            Val = new int?[] { 100 } 
        }))
        {
            dfTarget.WriteDelta(rootUrl, mode: DeltaSaveMode.Append, cloudOptions: options);
        }

        // Source: Id=1, Val=null
        using (var dfSource = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, 
            Val = new int?[] { null }
        }))
        {
            // Merge
            dfSource.MergeDeltaOrdered(rootUrl, mergeKeys: ["Id"], cloudOptions: options).Execute();
        }

        using var res = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        
        Assert.Null(res["Val"][0]);
        Assert.Equal(1, res.Height);
    }
    [Fact]
    [Trait("DeltaLake", "MergeSchemaEvolution")]
    public void Test_Merge_Delta_Schema_Evolution_Add_New_Column()
    {
        var tableName = $"delta_merge_evolution_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        // Target: Id, OldCol
        using (var dfTarget = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2 }, 
            OldCol = new[] { "Existing1", "Existing2" } 
        }))
        {
            dfTarget.WriteDelta(rootUrl, mode: DeltaSaveMode.Append, cloudOptions: options);
        }

        // Source: Id, NewCol
        using (var dfSource = DataFrame.FromColumns(new { 
            Id = new[] { 1, 3 }, 
            NewCol = new[] { "NewValue", "FreshValue" }
        }))
        {
            // Merge 
            dfSource.MergeDeltaOrdered(rootUrl, mergeKeys: ["Id"], cloudOptions: options,canEvolve:true).Execute();
        }

        using var res = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Sort("Id", false) 
            .Collect();

        Assert.Contains("OldCol", res.ColumnNames);
        Assert.Contains("NewCol", res.ColumnNames); 

        // Id=1 (Matched): OldCol=Existing1 (Kept), NewCol=NewValue (Added)
        var row1 = res.Row(0); // Id=1
        Assert.Equal(1, row1[0]);
        Assert.Equal("Existing1", row1[1]);
        Assert.Equal("NewValue", row1[2]);

        // Id=2 (Target Only): OldCol=Existing2, NewCol=null (Backfilled)
        var row2 = res.Row(1); // Id=2
        Assert.Equal(2, row2[0]);
        Assert.Equal("Existing2", row2[1]); 
        Assert.Null(row2[2]); 

        // Id=3 (Insert): OldCol=null, NewCol=FreshValue
        var row3 = res.Row(2); // Id=3
        Assert.Equal(3, row3[0]);
        Assert.Null(row3[1]);
        Assert.Equal("FreshValue", row3[2]);
    }
    [Fact]
    [Trait("DeltaLake", "Concurrent")]
    public async Task Test_Concurrent_Merge_Stress_TestAsync()
    {
        var tableName = $"delta_concurrent_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        using (var dfInit = DataFrame.FromColumns(new { Id = new[] { 1 }, Value = new[] { 0 } }))
        {
            dfInit.WriteDelta(rootUrl, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
        }

        int concurrency = 5;
        var tasks = new List<Task>();

        for (int i = 0; i < concurrency; i++)
        {
            int workerId = i + 1;
            tasks.Add(Task.Run(() =>
            {
                try 
                {

                    using var sourceDf = DataFrame.FromColumns(new 
                    { 
                        Id = new[] { 1, 100 + workerId }, 
                        Value = new[] { workerId, workerId } 
                    });
                    
                    sourceDf.MergeDeltaOrdered(
                        rootUrl, 
                        mergeKeys: ["Id"], 
                        cloudOptions: options
                    ).Execute();
                    
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Worker {workerId}] FAILED: {ex.Message}");
                    throw;
                }
            }));
        }

        await Task.WhenAll(tasks);

    using var result = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();

    int finalValue = (int)result.Filter(Pl.Col("Id") ==1)["Value"][0]!;
    Assert.True(finalValue > 0, "Id=1 should be updated by someone");

    var countNewRows = result.Filter(Pl.Col("Id") > 100).Height;
    Assert.Equal(concurrency, countNewRows);
    }
    [Fact]
    [Trait("DeltaLake", "ConcurrentWrite")]
    public async Task Test_Concurrent_Write_Append_Stress_TestAsync()
    {

        var tableName = $"delta_concurrent_write_{Guid.NewGuid():N}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        using (var dfInit = DataFrame.FromColumns(new { WorkerId = new[] { 0 }, RowId = new[] { 0 } }))
        {
            dfInit.WriteDelta(rootUrl, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
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

                    using var sourceDf = DataFrame.FromColumns(new 
                    { 
                        WorkerId = workerIds, 
                        RowId = rowIds 
                    });
                    
                    sourceDf.WriteDelta(
                        rootUrl, 
                        mode: DeltaSaveMode.Append, 
                        cloudOptions: options
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

        using var result = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        
        result.Show();

        long expectedHeight = 1 + (concurrency * rowsPerWorker);
        Assert.Equal(expectedHeight, result.Height);

        for (int i = 1; i <= concurrency; i++)
        {
            var workerRowCount = result.Filter(Pl.Col("WorkerId") == i).Height;
            Assert.Equal(rowsPerWorker, workerRowCount);
        }

    }
    [Fact]
    [Trait("DeltaLake", "Vacuum")]
    public void Test_Sink_Overwrite_And_Vacuum_Full_Cycle()
    {

        var tableName = $"delta_vacuum_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );

        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3 }, 
            Value = new[] { 10, 20, 30 }
        }))
        {
            df.WriteDelta(
                rootUrl, 
                mode: DeltaSaveMode.Overwrite, 
                cloudOptions: options
            );
        }

        using (var df2 = DataFrame.FromColumns(new { 
            Id = new[] { 4, 5 }, 
            Value = new[] { 40, 50 }
        }))
        {
            df2.WriteDelta(
                rootUrl, 
                mode: DeltaSaveMode.Overwrite, 
                cloudOptions: options
            );
        }

        using var currentDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(2, currentDf.Height); // Id: 4, 5

        using var oldDf = LazyFrame.ScanDelta(rootUrl, version: 1, cloudOptions: options).Collect();
        Assert.Equal(3, oldDf.Height); // Id: 1, 2, 3

        // DryRun
        long filesToDelete = Delta.Vacuum(
            rootUrl, 
            retentionHours: 0, 
            enforceRetention: false, 
            dryRun: true,
            cloudOptions: options
        );

        Assert.True(filesToDelete > 0, "Dry run should find stale files from Version 0");

        using var oldDfCheck = LazyFrame.ScanDelta(rootUrl, version: 1, cloudOptions: options).Collect();
        Assert.Equal(3, oldDfCheck.Height);

        // RealRun
        long deletedCount = Delta.Vacuum(
            rootUrl, 
            retentionHours: 0, 
            enforceRetention: false, 
            dryRun: false, 
            cloudOptions: options
        );

        Assert.Equal(filesToDelete, deletedCount);

        var ex = Assert.ThrowsAny<Exception>(() => 
        {
            LazyFrame.ScanDelta(rootUrl, version: 1, cloudOptions: options).Collect();
        });

        Assert.Contains("404 Not Found", ex.Message); 

        using var aliveDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(2, aliveDf.Height);
        Assert.Equal(40, aliveDf["Value"][0]);

    }
    [Fact]
    [Trait("DeltaLake", "Restore")]
    public void Test_Restore_To_Version()
    {
        var tableName = $"delta_restore_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );

        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        using (var df = DataFrame.FromColumns(new { Id = new[] { 1, 2 }, Val = new[] { "Good", "Good" } }))
        {
            df.WriteDelta(rootUrl, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
        }

        using (var dfBad = DataFrame.FromColumns(new { Id = new[] { 1, 2 }, Val = new[] { "Bad", "Bad" } }))
        {
            dfBad.WriteDelta(rootUrl, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
        }

        using var currentDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal("Bad", currentDf["Val"][0]);

        ulong newVersion = Delta.Restore(
            rootUrl, 
            version: 1, 
            cloudOptions: options
        );

        // V1 (Good) -> V2 (Bad) -> V3 (Restore to V1 content)
        Assert.Equal(3UL, newVersion);

        using var restoredDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal("Good", restoredDf["Val"][0]);
    }
    [Fact]
    [Trait("DeltaLake", "Optimize")]
    public void Test_Optimize_ZOrder_Scale_100()
    {
        var tableName = $"delta_opt_100_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );

        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        for (int i = 0; i < 4; i++)
        {
            int startId = i * 25;

            var ids = Enumerable.Range(startId, 25).ToArray();

            var cats = ids.Select(x => x % 2 == 0 ? "Even" : "Odd").ToArray();
            var vals = ids.Select(x => x * 1.5).ToArray();

            using var df = DataFrame.FromColumns(new
            {
                Id = ids,
                Category = cats,
                Val = vals
            });
            
            df.WriteDelta(rootUrl, mode: DeltaSaveMode.Append,partitionBy: "Category", cloudOptions: options);
        }

        using var beforeDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(100, beforeDf.Height);
        
        var zCols = new[] { "Id" };
        
        long numFiles = Delta.Optimize(
            rootUrl,
            targetSizeMb: 128, 
            zOrderColumns: zCols,
            cloudOptions: options
        );

        using var afterDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Sort("Id",  false )
            .Collect();

        Assert.Equal(100, afterDf.Height);
        // shape: (100, 3)
        // ┌─────┬──────────┬───────┐
        // │ Id  ┆ Category ┆ Val   │
        // │ --- ┆ ---      ┆ ---   │
        // │ i32 ┆ str      ┆ f64   │
        // ╞═════╪══════════╪═══════╡
        // │ 0   ┆ Even     ┆ 0.0   │
        // │ 1   ┆ Odd      ┆ 1.5   │
        // │ 2   ┆ Even     ┆ 3.0   │
        // │ 3   ┆ Odd      ┆ 4.5   │
        // │ 4   ┆ Even     ┆ 6.0   │
        // │ …   ┆ …        ┆ …     │
        // │ 95  ┆ Odd      ┆ 142.5 │
        // │ 96  ┆ Even     ┆ 144.0 │
        // │ 97  ┆ Odd      ┆ 145.5 │
        // │ 98  ┆ Even     ┆ 147.0 │
        // │ 99  ┆ Odd      ┆ 148.5 │
        // └─────┴──────────┴───────┘
        // Row 0
        Assert.Equal(0, afterDf["Id"][0]);
        Assert.Equal("Even", afterDf["Category"][0]);
        
        // Row 49 (Middle)
        Assert.Equal(49, afterDf["Id"][49]);
        Assert.Equal("Odd", afterDf["Category"][49]); // 49 is Odd

        // Row 99 (Last)
        Assert.Equal(99, afterDf["Id"][99]);
        Assert.Equal("Odd", afterDf["Category"][99]);
        Assert.Equal(148.5, (double)afterDf["Val"][99]!); // 99 * 1.5

    }
    [Fact]
    [Trait("DeltaLake", "OptimizeDV")]
    public void Test_Optimize_ZOrder_With_Deletion_Vectors_Scale_100()
    {
        PolarsConfig.SetEnvVar("POLARS_DELTA_MAX_RETRIES", "30");
        var tableName = $"delta_opt_dv_100_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );

        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        for (int i = 0; i < 4; i++)
        {
            int startId = i * 25;
            var ids = Enumerable.Range(startId, 25).ToArray();
            var cats = ids.Select(x => x % 2 == 0 ? "Even" : "Odd").ToArray();
            var vals = ids.Select(x => x * 1.5).ToArray();

            using var df = DataFrame.FromColumns(new
            {
                Id = ids,
                Category = cats,
                Val = vals
            });
            
            df.WriteDelta(rootUrl, mode: DeltaSaveMode.Append, partitionBy: "Category", cloudOptions: options);
        }

        using var beforeDf = DataFrame.ReadDelta(rootUrl, cloudOptions: options);
        Assert.Equal(100, beforeDf.Height);
        // ==========================================
        // Add DV Feature
        // ==========================================
        Delta.AddFeature(
            rootUrl, 
            DeltaTableFeatures.DeletionVectors, 
            allowProtocolIncrease: true, 
            cloudOptions: options
        );

        using (var delDf = DataFrame.FromColumns(new { Id = new[] { 10, 99 }, Action = new[] { "DeleteMe", "DeleteMe" } }))
        {
            delDf.MergeDeltaOrdered(
                rootUrl,
                mergeKeys: ["Id"],
                canEvolve:true,
                
                cloudOptions: options
            )
            .WhenMatchedDelete(m => m.Source("Action") == "DeleteMe")
            .Execute();
        }

        using var midDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(98, midDf.Height); 

        // ==========================================
        // Optimize (Z-Order by Id + DV Purging)
        // ==========================================
        
        var zCols = new[] { "Id" };
        
        long numFiles = Delta.Optimize(
            rootUrl,
            targetSizeMb: 128, 
            zOrderColumns: zCols,
            cloudOptions: options
        );

        Assert.True(numFiles > 0, "Should have optimized at least 1 file");

        // ==========================================
        // Data Integrity & Purge Check
        // ==========================================
        using var afterDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Sort("Id", false) 
            .Collect();

        afterDf.Show();

        Assert.Equal(98, afterDf.Height);
        var validIds = new[] {10,99};
        
        var predicate = Pl.Col("Id").IsIn(Pl.Lit(validIds).Implode());
        var deletedRows = afterDf.Filter(predicate);
        Assert.Equal(0, deletedRows.Height); 

        // Row 0
        Assert.Equal(0, afterDf["Id"][0]);
        Assert.Equal("Even", afterDf["Category"][0]);
        
        // Row 49 (Id=49 is actually at index 48 now because Id=10 is gone)
        var row49 = afterDf.Filter(Pl.Col("Id") == 49);
        Assert.Equal(49, row49["Id"][0]);
        Assert.Equal("Odd", row49["Category"][0]); 

        // Row 98 (Last element, since 99 was deleted)
        var lastRow = afterDf.Filter(Pl.Col("Id") == 98);
        Assert.Equal(98, lastRow["Id"][0]);
        Assert.Equal("Even", lastRow["Category"][0]);
        Assert.Equal(147.0, (double)lastRow["Val"][0]!); // 98 * 1.5

    }
    [Fact]
    [Trait("DeltaLake", "Properties")]
    public void Test_Set_Table_Retention()
    {
        var tableName = $"delta_props_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );
        
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";
        
        using (var df = DataFrame.FromColumns(new { Id = new[] { 1 } }))
        {
            df.WriteDelta(rootUrl, cloudOptions: options);
        }

        
        var props = new Dictionary<string, string>
        {
            { DeltaTableProperties.DeletedFileRetentionDuration, "interval 1 hour" },
            { "my.custom.metadata", "polars-driver-v1" } 
        };

        Delta.SetTableProperties(rootUrl, props, cloudOptions: options);
        
        using (var df2 = DataFrame.FromColumns(new { Id = new[] { 2 } }))
        {
            df2.WriteDelta(rootUrl, mode: DeltaSaveMode.Append, cloudOptions: options);
        }
        
        using var dfRead = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(2, dfRead.Height);
    }
    [Fact]
    [Trait("DeltaLake", "MergeCompositeKeysOrdered")]
    public void Test_Merge_Delta_Composite_Keys_Ordered_Logic()
    {
        var tableName = $"delta_merge_composite_ordered_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: $"http://{minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/')}"
        );
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";

        // ==========================================
        // Target] Version 1
        // ==========================================
        // [Region, StoreId]
        // Data Scenarios:
        // 1. [North, 101]: Stock 10  -> Update
        // 2. [North, 102]: Stock 20  -> Update Skip
        // 3. [South, 101]: Stock 5   -> Matched Delete
        // 4. [South, 999]: Obsolete  -> Source Delete
        // 5. [East,  555]: Active    -> Target Only Keep

        using (var df = DataFrame.FromColumns(new { 
            Region = new[]  { "North", "North", "South", "South", "East" },
            StoreId = new[] { 101,     102,     101,     999,     555 },
            Stock = new[]   { 10,      20,      5,       0,       50 },
            Status = new[]  { "Active","Active","Recall","Obsolete","Active" }
        }))
        {
            df.WriteDelta(
                rootUrl, 
                partitionBy: "Region", 
                mode: DeltaSaveMode.Append, 
                cloudOptions: options
            );
        }

        // ==========================================
        // Merge Source
        // ==========================================
        // Source Logic:
        // 1. [North, 101]: Stock 100 (New > Old) -> Update
        // 2. [North, 102]: Stock 15  (New < Old) -> Skip Update
        // 3. [South, 101]: Status "DeleteMe"     -> Matched Delete
        // 4. [West,  888]: Stock 60              -> Insert (New Region)
        // 5. [West,  999]: Stock 0               -> Skip Insert (Condition Fail)

        using var sourceDf = DataFrame.FromColumns(new { 
            Region = new[]  { "North", "North", "South", "West", "West" },
            StoreId = new[] { 101,     102,     101,     888,     999 },
            Stock = new[]   { 100,     15,      0,       60,      0 },
            Status = new[]  { "Active","Active","DeleteMe","New",   "Bad" }
        });

        static Expr updateCond(MergeContext m) => m.Source("Stock") > m.Target("Stock");
        static Expr matchDeleteCond(MergeContext m) => m.Source("Status") == "DeleteMe";
        static Expr insertCond(MergeContext m) => m.Source("Stock") > 0;
        static Expr srcDeleteCond(MergeContext m) => m.Target("Status") == "Obsolete";

        sourceDf.MergeDeltaOrdered(
                rootUrl,
                mergeKeys: ["Region", "StoreId"], 
                cloudOptions: options
            )
            .WhenMatchedDelete(matchDeleteCond)          
            .WhenMatchedUpdate(updateCond)             
            .WhenNotMatchedInsert(insertCond)             
            .WhenNotMatchedBySourceDelete(srcDeleteCond) 
            .Execute();                                  

        
        using var dfRes = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Collect()
            .Sort(["Region", "StoreId"]); // Sort for deterministic assert

        // shape: (4, 4)
        // ┌────────┬─────────┬───────┬────────┐
        // │ Region ┆ StoreId ┆ Stock ┆ Status │
        // │ ---    ┆ ---     ┆ ---   ┆ ---    │
        // │ str    ┆ i32     ┆ i32   ┆ str    │
        // ╞════════╪═════════╪═══════╪════════╡
        // │ East   ┆ 555     ┆ 50    ┆ Active │
        // │ North  ┆ 101     ┆ 100   ┆ Active │
        // │ North  ┆ 102     ┆ 20    ┆ Active │
        // │ West   ┆ 888     ┆ 60    ┆ New    │
        // └────────┴─────────┴───────┴────────┘
        
        // 1. North/101: Updated (100)
        // 2. North/102: Kept (20)
        // 3. South/101: Deleted
        // 4. South/999: Deleted (Obsolete)
        // 5. East/555:  Kept (50)
        // 6. West/888:  Inserted (60)
        // 7. West/999:  Ignored

        Assert.Equal(4, dfRes.Height);

        // Case 1: Composite Match Update
        var row1 = dfRes.Filter(Pl.Col("Region") == "North" & Pl.Col("StoreId") == 101);
        Assert.Equal(100, row1["Stock"].ToArray<int>()[0]);

        // Case 2: Composite Match Skip
        var row2 = dfRes.Filter(Pl.Col("Region") == "North" & Pl.Col("StoreId") == 102);
        Assert.Equal(20, row2["Stock"].ToArray<int>()[0]);

        // Case 3: Composite Match Delete (South/101)
        Assert.Equal(0, dfRes.Filter(Pl.Col("Region") == "South" & Pl.Col("StoreId") == 101).Height);

        // Case 4: Not Matched By Source Delete
        Assert.Equal(0, dfRes.Filter(Pl.Col("Region") == "South" & Pl.Col("StoreId") == 999).Height);

        // Case 5: Target Only Keep
        var rowEast = dfRes.Filter(Pl.Col("Region") == "East");
        Assert.Equal(50, rowEast["Stock"].ToArray<int>()[0]);

        // Case 6: Insert
        var rowWest = dfRes.Filter(Pl.Col("Region") == "West" & Pl.Col("StoreId") == 888);
        Assert.Equal(60, rowWest["Stock"].ToArray<int>()[0]);

    }
    [Fact]
    [Trait("DeltaLake", "MergeOrderSemantics")]
    [Obsolete("Test old api")]
    public void Test_Merge_Delta_Action_Order_Matters()
    {

        var tableDeleteWins = $"delta_merge_order_del_{Guid.NewGuid()}";
        var urlDeleteWins = $"s3://{minio.BucketName}/{tableDeleteWins}";

        var tableUpdateWins = $"delta_merge_order_upd_{Guid.NewGuid()}";
        var urlUpdateWins = $"s3://{minio.BucketName}/{tableUpdateWins}";

        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: $"http://{minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/')}"
        );
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";

        using (var dfTarget = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, 
            Stock = new[] { 10 }, 
            Status = new[] { "Active" } 
        }))
        {
            dfTarget.WriteDelta(urlDeleteWins, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
            dfTarget.WriteDelta(urlUpdateWins, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
        }

        // This record meets "Stock > Target" (100 > 10)
        // and "Status == DeleteMe"
        using var sourceDf = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, 
            Stock = new[] { 100 }, 
            Status = new[] { "DeleteMe" } 
        });
        var m = MergeContext.Delta;
        var updateCond = m.Source("Stock") > m.Target("Stock");
        var deleteCond = m.Source("Status") == "DeleteMe";

        // ==========================================
        // Delete First
        // ==========================================
        sourceDf.MergeDeltaOrdered(urlDeleteWins, mergeKeys: ["Id"], cloudOptions: options)
            .WhenMatchedDelete(deleteCond)  
            .WhenMatchedUpdate(updateCond)  
            .Execute();

        // ==========================================
        // Update First
        // ==========================================
        sourceDf.MergeDeltaOrdered(urlUpdateWins, mergeKeys: ["Id"], cloudOptions: options)
            .WhenMatchedUpdate(updateCond)  
            .WhenMatchedDelete(deleteCond)
            .Execute();

        using var resDeleteWins = LazyFrame.ScanDelta(urlDeleteWins, cloudOptions: options).Collect();
        using var resUpdateWins = LazyFrame.ScanDelta(urlUpdateWins, cloudOptions: options).Collect();

        Assert.Equal(0, resDeleteWins.Height); 

        Assert.Equal(1, resUpdateWins.Height);
        Assert.Equal(100, resUpdateWins["Stock"].ToArray<int>()[0]);
        Assert.Equal("DeleteMe", resUpdateWins["Status"].ToArray<string>()[0]);

    }

    public record StoreRecord(string Region, int StoreId, int Stock, string Status);

    [Fact]
    [Trait("DeltaLake", "LINQ")]
    public void Test_Merge_Delta_Composite_Keys_Ordered_Logic_With_LINQ()
    {

        var tableName = $"delta_merge_composite_ordered_{Guid.NewGuid():N}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: $"http://{minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/')}"
        );
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";


        using (var df = DataFrame.FromColumns(new { 
            Region = new[]  { "North", "North", "South", "South", "East" },
            StoreId = new[] { 101,     102,     101,     999,     555 },
            Stock = new[]   { 10,      20,      5,       0,       50 },
            Status = new[]  { "Active","Active","Recall","Obsolete","Active" }
        }))
        {
            df.WriteDelta(
                rootUrl, 
                partitionBy: "Region", 
                mode: DeltaSaveMode.Append, 
                cloudOptions: options
            );
        }

        using var targetReadDf = DataFrame.ReadDelta(rootUrl, cloudOptions: options);

        // LINQ to Polars
        var sourceQuery = targetReadDf.AsQueryable<StoreRecord>()
            .Where(x => x.Region == "North" || (x.Region == "South" && x.StoreId == 101))
            .Select(x => new StoreRecord(
                x.Region,
                x.StoreId,
                (x.Region == "North" && x.StoreId == 101) ? 100 : 
                (x.Region == "North" && x.StoreId == 102) ? 15 : 0, 
                (x.Region == "South" && x.StoreId == 101) ? "DeleteMe" : x.Status
            ))
            .Concat([
                new StoreRecord("West", 888, 60, "New"),
                new StoreRecord("West", 999, 0, "Bad")
            ]);
        using var sourceDf = sourceQuery.ToDataFrame();

        // Console.WriteLine("--- Source DataFrame Generated from LINQ ---");
        // shape: (5, 4)
        // ┌────────┬─────────┬───────┬──────────┐
        // │ Region ┆ StoreId ┆ Stock ┆ Status   │
        // │ ---    ┆ ---     ┆ ---   ┆ ---      │
        // │ str    ┆ i32     ┆ i32   ┆ str      │
        // ╞════════╪═════════╪═══════╪══════════╡
        // │ North  ┆ 101     ┆ 100   ┆ Active   │
        // │ North  ┆ 102     ┆ 15    ┆ Active   │
        // │ South  ┆ 101     ┆ 0     ┆ DeleteMe │
        // │ West   ┆ 888     ┆ 60    ┆ New      │
        // │ West   ┆ 999     ┆ 0     ┆ Bad      │
        // └────────┴─────────┴───────┴──────────┘

        static Expr updateCond(MergeContext m) => m.Source("Stock") > m.Target("Stock");
        static Expr matchDeleteCond(MergeContext m) => m.Source("Status") == "DeleteMe";
        static Expr insertCond(MergeContext m) => m.Source("Stock") > 0;
        static Expr srcDeleteCond(MergeContext m) => m.Target("Status") == "Obsolete";

        // ==========================================
        // Ordered Full Merge
        // ==========================================

        sourceDf.MergeDeltaOrdered(
                rootUrl,
                mergeKeys: ["Region", "StoreId"], 
                cloudOptions: options
            )
            .WhenMatchedDelete(matchDeleteCond)           
            .WhenMatchedUpdate(updateCond)                
            .WhenNotMatchedInsert(insertCond)             
            .WhenNotMatchedBySourceDelete(srcDeleteCond)  
            .Execute();                                   

        
        using var dfRes = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Collect()
            .Sort(["Region", "StoreId"]);

        // shape: (4, 4)
        // ┌────────┬─────────┬───────┬────────┐
        // │ Region ┆ StoreId ┆ Stock ┆ Status │
        // │ ---    ┆ ---     ┆ ---   ┆ ---    │
        // │ str    ┆ i32     ┆ i32   ┆ str    │
        // ╞════════╪═════════╪═══════╪════════╡
        // │ East   ┆ 555     ┆ 50    ┆ Active │
        // │ North  ┆ 101     ┆ 100   ┆ Active │
        // │ North  ┆ 102     ┆ 20    ┆ Active │
        // │ West   ┆ 888     ┆ 60    ┆ New    │
        // └────────┴─────────┴───────┴────────┘
        
        Assert.Equal(4, dfRes.Height);

        // Case 1: Composite Match Update
        var row1 = dfRes.Filter(Pl.Col("Region") == "North" & Pl.Col("StoreId") == 101);
        Assert.Equal(100, row1["Stock"].ToArray<int>()[0]);

        // Case 2: Composite Match Skip
        var row2 = dfRes.Filter(Pl.Col("Region") == "North" & Pl.Col("StoreId") == 102);
        Assert.Equal(20, row2["Stock"].ToArray<int>()[0]);

        // Case 3: Composite Match Delete
        Assert.Equal(0, dfRes.Filter(Pl.Col("Region") == "South" & Pl.Col("StoreId") == 101).Height);

        // Case 4: Not Matched By Source Delete
        Assert.Equal(0, dfRes.Filter(Pl.Col("Region") == "South" & Pl.Col("StoreId") == 999).Height);

        // Case 5: Target Only Keep
        var rowEast = dfRes.Filter(Pl.Col("Region") == "East");
        Assert.Equal(50, rowEast["Stock"].ToArray<int>()[0]);

        // Case 6: Insert
        var rowWest = dfRes.Filter(Pl.Col("Region") == "West" & Pl.Col("StoreId") == 888);
        Assert.Equal(60, rowWest["Stock"].ToArray<int>()[0]);

    }
}
