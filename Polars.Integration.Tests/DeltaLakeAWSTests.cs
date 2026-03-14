using Polars.Integration.Tests.Fixtures;
using Polars.CSharp;
using static Polars.CSharp.Polars;
using System.Text;
using Minio;
using Minio.DataModel.Args;
using Polars.Integration.Tests.Utils;
using Polars.NET.Core;
using Polars.NET.Linq.CSharpExtensions;
using Polars.NET.Linq;

namespace Polars.Integration.Tests;

public class DeltaLakeTests(MinioFixture minio) : IClassFixture<MinioFixture>
{
    [Fact]
    public async Task Test_Scan_Delta_AWS_Minio()
    {
        // ==========================================
        // 1. 准备环境 & 配置
        // ==========================================
        var tableName = $"delta_table_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        var parquetFileName = "part-0000.parquet";
        var parquetUrl = $"{rootUrl}/{parquetFileName}";

        // ==========================================
        // 关键修复：处理 Endpoint 格式
        // ==========================================
        // 1. MinIO 的 Endpoint 通常是 "127.0.0.1:32899" 这种格式
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');

        // 2. Polars (Rust) 必须要有协议头，否则 Url::parse 失败 -> 回退到真实 AWS
        var polarsEndpoint = $"http://{rawEndpoint}";

        // 3. MinIO .NET SDK 必须没有协议头，否则报错 InvalidEndpoint
        var minioSdkEndpoint = rawEndpoint;

        // 构造 CloudOptions 给 Polars
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint // <--- 传入带 http:// 的地址
        );
        options.Credentials!["aws_allow_http"] = "true"; // 允许 HTTP
        options.Credentials!["aws_s3_force_path_style"] = "true"; // 强制 Path Style

        // ==========================================
        // 2. 第一步：使用 Polars 写入 Parquet 数据文件
        //    (假装它是 Delta Table 的一部分)
        // ==========================================
        using var df = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 3 },
            Name = new[] { "Polars", "Delta", "Rust" },
            Value = new[] { 10.5, 20.0, 30.5 }
        });

        df.WriteParquet(parquetUrl, cloudOptions: options);

        // ==========================================
        // 3. 第二步：手动构造 Delta Log (修正字段名大小写)
        // ==========================================
        
        // 1. 准备 Schema 字符串 (这部分保持不变)
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

        // 2. Line 1: Protocol (全小写)
        // 格式: {"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
        sb.AppendLine("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}");

        // 3. Line 2: Metadata (关键修正: metaData，D大写！)
        // 我们手动构建外层 JSON，内层用 Serialize 处理复杂结构
        var metadataInner = new
        {
            id = Guid.NewGuid().ToString(),
            format = new { provider = "parquet", options = new { } },
            schemaString = schemaString,
            partitionColumns = new string[0],
            configuration = new { },
            createdTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };
        // 修正点：这里手动拼接 "metaData": ...
        sb.AppendLine($"{{\"metaData\":{System.Text.Json.JsonSerializer.Serialize(metadataInner)}}}");

        // 4. Line 3: Add (全小写)
        var addInner = new
        {
            path = parquetFileName,
            partitionValues = new { },
            size = 1000L,
            modificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            dataChange = true,
            stats = "{\"numRecords\":3}"
        };
        // 修正点：这里手动拼接 "add": ...
        sb.AppendLine($"{{\"add\":{System.Text.Json.JsonSerializer.Serialize(addInner)}}}");

        var logContent = sb.ToString();
        
        // 调试用：打印生成的 JSON 看看长啥样
        // Console.WriteLine("Generated Delta Log:\n" + logContent);

        var logPath = $"{tableName}/_delta_log/00000000000000000000.json";

        // 使用 MinioClient 直接上传 Log 文件
        // 注意：这里需要依赖 Minio NuGet 包
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

        // ==========================================
        // 4. 第三步：Scan Delta (测试核心目标)
        // ==========================================
        // 这一步会触发 Rust 端的 pl_scan_delta -> deltalake::open_table -> Polars Scan
        using var lfRead = LazyFrame.ScanDelta(rootUrl, cloudOptions: options);
        using var dfRead = lfRead.Collect();
        Console.WriteLine("DataFrame From Delta Table Here:");
        df.Show();
        // ==========================================
        // 5. 验证
        // ==========================================
        Assert.Equal(df.Height, dfRead.Height);
        
        var originalNames = df["Name"].ToArray<string>();
        var readNames = dfRead["Name"].ToArray<string>();
        
        Assert.Equal(originalNames, readNames);
        
        // 验证是否正确使用了 Delta Log 中的 Schema (例如列名)
        Assert.Contains("Value", dfRead.ColumnNames);
        
        Console.WriteLine($"Delta Lake Scan (S3/MinIO) Passed! Table: {tableName}");
    }
    [Fact]
    [Trait("DeltaLake","TimeTravel")]
    public async Task Test_Scan_Delta_TimeTravel()
    {
        // ==========================================
        // 1. 环境准备
        // ==========================================
        var tableName = $"delta_time_travel_{Guid.NewGuid()}";
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        
        // 构造 Endpoint
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}"; // Polars 需要 http://
        
        // Polars Cloud Options
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );
        options.Credentials!["aws_allow_http"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";

        // MinIO Client (用于上传 Log)
        var minioClient = new MinioClient()
            .WithEndpoint(rawEndpoint)
            .WithCredentials(minio.AccessKey, minio.SecretKey)
            .WithRegion(minio.Region)
            .Build();

        // 定义时间点 (用于 Datetime Travel 测试)
        // V0 创建于 1小时前
        var timeV0 = DateTimeOffset.UtcNow.AddHours(-1); 
        // V1 创建于 现在
        var timeV1 = DateTimeOffset.UtcNow;

        // ==========================================
        // 2. 构造 Version 0 (Initial Commit)
        // ==========================================
        var fileV0 = "part-v0.parquet";
        
        // 2.1 写入 V0 数据 Parquet
        using (var df0 = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, Name = new[] { "V0" }, Value = new[] { 10.0 } 
        }))
        {
            df0.Lazy().SinkParquet($"{rootUrl}/{fileV0}", cloudOptions: options);
        }

        // 2.2 生成 Log V0 (Protocol + Metadata + Add)
        var sb0 = new StringBuilder();
        sb0.AppendLine(DeltaLakeTestHelper.ActionProtocol());
        sb0.AppendLine(DeltaLakeTestHelper.ActionMetadata(
            DeltaLakeTestHelper.GenerateSchemaString(), 
            timeV0.ToUnixTimeMilliseconds() // 手动指定时间
        ));
        sb0.AppendLine(DeltaLakeTestHelper.ActionAdd(fileV0, modTime: timeV0.ToUnixTimeMilliseconds()));
        
        await DeltaLakeTestHelper.UploadLogAsync(minioClient, minio.BucketName, tableName, 0, sb0.ToString());

        // ==========================================
        // 3. 构造 Version 1 (Append Commit)
        // ==========================================
        var fileV1 = "part-v1.parquet";

        // 3.1 写入 V1 数据 Parquet
        using (var df1 = DataFrame.FromColumns(new { 
            Id = new[] { 2 }, Name = new[] { "V1" }, Value = new[] { 20.0 } 
        }))
        {
            df1.Lazy().SinkParquet($"{rootUrl}/{fileV1}", cloudOptions: options);
        }

        // 3.2 生成 Log V1 (只有 Add，继承之前的 Protocol/Metadata)
        // 注意：标准的 Delta Log，后续版本通常只包含 Add/Remove 动作，
        // 除非 Metadata 变了，否则不需要再写 Protocol/Metadata
        var sb1 = new StringBuilder();
        sb1.AppendLine(DeltaLakeTestHelper.ActionAdd(fileV1, modTime: timeV1.ToUnixTimeMilliseconds()));

        await DeltaLakeTestHelper.UploadLogAsync(minioClient, minio.BucketName, tableName, 1, sb1.ToString());

        // ==========================================
        // 4. 测试 A: 读取最新版 (Snapshot / Version 1)
        // ==========================================
        using var lfLatest = LazyFrame.ScanDelta(rootUrl, cloudOptions: options);
        var dfLatest = lfLatest.Collect();
        
        Assert.Equal(2, dfLatest.Height); // 应该有 1 和 2 两行
        Assert.Contains("V1", dfLatest["Name"].ToArray<string>());
        Console.WriteLine("Test A (Latest) Passed");

        // ==========================================
        // 5. 测试 B: Time Travel via Version (回到 V0)
        // ==========================================
        using var lfV0 = LazyFrame.ScanDelta(rootUrl, version: 0, cloudOptions: options);
        var dfV0 = lfV0.Collect();

        Assert.Equal(1, dfV0.Height); // 只有 V0 那一行
        Assert.Equal("V0", dfV0["Name"].ToArray<string>()[0]);
        Console.WriteLine("Test B (Version Travel) Passed");

        // ==========================================
        // 6. 测试 C: Time Travel via Datetime
        // ==========================================
        // 我们查询 timeV0 之后 1 分钟的时间点 (那时 V1 还没发生)
        var queryTime = timeV0.AddMinutes(1).ToString("yyyy-MM-ddTHH:mm:ssZ");
        
        using var lfTime = LazyFrame.ScanDelta(rootUrl, datetime: queryTime, cloudOptions: options);
        var dfTime = lfTime.Collect();

        Assert.Equal(1, dfTime.Height); // 应该还是 V0
        Assert.Equal("V0", dfTime["Name"].ToArray<string>()[0]);
        Console.WriteLine($"Test C (Time Travel to {queryTime}) Passed");
    }
    [Fact]
    [Trait("DeltaLake", "FullCycle")]
    public void Test_Sink_And_Scan_Delta_Full_Cycle_Modes()
    {
        // ==========================================
        // 1. 环境与鉴权准备
        // ==========================================
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

        // 核心配置：MinIO 兼容性 + 原子提交支持
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_allow_http"] = "true";
        options.Credentials!["AWS_S3_FORCE_PATH_STYLE"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";
        
        // ==========================================
        // 2. [Mode: Append] 初始化写入 (Version 0 + 1)
        // ==========================================
        Console.WriteLine("Step 1: Initial Write (Append)...");
        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2 }, 
            Msg = new[] { "V1_A", "V1_B" } 
        }))
        {
            // 此时表不存在，Append 会触发 Create Table
            df.WriteDelta(rootUrl, mode: DeltaSaveMode.Append, cloudOptions: options);
        }

        // 验证 V1
        using var dfV1 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(2, dfV1.Height);

        // ==========================================
        // 3. [Mode: Append] 追加写入 (Version 2)
        // ==========================================
        Console.WriteLine("Step 2: Appending Data (Append)...");
        using (var df2 = DataFrame.FromColumns(new { 
            Id = new[] { 3 }, 
            Msg = new[] { "V2_C" } 
        }))
        {
            df2.WriteDelta(rootUrl, mode: DeltaSaveMode.Append, cloudOptions: options);
        }

        // 验证 V2 (总共 3 行)
        using var dfV2 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(3, dfV2.Height);
        Assert.Contains("V1_A", dfV2["Msg"].ToArray<string>());
        Assert.Contains("V2_C", dfV2["Msg"].ToArray<string>());

        // ==========================================
        // 4. [Mode: Overwrite] 覆盖写入 (Version 3)
        // ==========================================
        // 预期：之前的 3 行数据逻辑删除，只保留新的 1 行数据
        Console.WriteLine("Step 3: Overwriting Data (Overwrite)...");
        using (var dfOverwrite = DataFrame.FromColumns(new { 
            Id = new[] { 999 }, 
            Msg = new[] { "Overwrite_New" } 
        }))
        {
            dfOverwrite.Lazy().SinkDelta(rootUrl, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
        }

        // 验证 V3 (应该只有 1 行)
        using var dfV3 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(1, dfV3.Height);
        Assert.Equal("Overwrite_New", dfV3["Msg"].ToArray<string>()[0]);
        
        // 验证 V3 的历史 (Time Travel Check)
        // 我们可以回溯到 V2 确认旧数据还在历史里
        using var dfBackToV2 = LazyFrame.ScanDelta(rootUrl, version: 2, cloudOptions: options).Collect();
        Assert.Equal(3, dfBackToV2.Height);

        // ==========================================
        // 5. [Mode: ErrorIfExists] 冲突检测
        // ==========================================
        // 预期：表已存在，应抛出异常
        Console.WriteLine("Step 4: Testing ErrorIfExists...");
        using (var dfError = DataFrame.FromColumns(new { Id = new[] { 0 } }))
        {
            var ex = Assert.Throws<PolarsException>(() => 
            {
                dfError.Lazy().SinkDelta(rootUrl, mode: DeltaSaveMode.ErrorIfExists, cloudOptions: options);
            });
            
            // 验证 Rust 返回的错误信息
            Assert.Contains("Table already exists", ex.Message);
        }

        // ==========================================
        // 6. [Mode: Ignore] 忽略写入
        // ==========================================
        // 预期：表已存在，不报错，但不写入任何数据 (Height 保持为 1)
        Console.WriteLine("Step 5: Testing Ignore...");
        using (var dfIgnore = DataFrame.FromColumns(new { 
            Id = new[] { 888 }, 
            Msg = new[] { "Should_Not_Exist" } 
        }))
        {
            dfIgnore.Lazy().SinkDelta(rootUrl, mode: DeltaSaveMode.Ignore, cloudOptions: options);
        }

        // 验证数据未变 (依然是 Overwrite 后的那 1 行)
        using var dfFinal = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(1, dfFinal.Height);
        Assert.DoesNotContain("Should_Not_Exist", dfFinal["Msg"].ToArray<string>());

        Console.WriteLine("Delta Streaming Full Cycle (Append/Overwrite/Error/Ignore) Passed!");
    }
    [Fact]
    [Trait("DeltaLake", "Partitioned")]
    public void Test_Sink_And_Scan_Delta_Partitioned_Full_Cycle()
    {
        // ==========================================
        // 1. 环境与鉴权准备 (复用你的 MinIO 配置)
        // ==========================================
        var tableName = $"delta_partitioned_{Guid.NewGuid()}";
        // 假设 _minio 是你测试基类中配置好的 MinioFixture
        var rootUrl = $"s3://{minio.BucketName}/{tableName}";
        
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: polarsEndpoint
        );

        // 核心配置：MinIO 兼容性 + 原子提交支持
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";
        
        // ==========================================
        // 2. [Mode: Append] 初始化分区写入 (Version 0)
        // ==========================================
        // 数据包含 'Year' 列作为分区键
        Console.WriteLine("Step 1: Initial Partitioned Write (Append)...");
        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3 }, 
            Msg = new[] { "V1_2023_A", "V1_2023_B", "V1_2024_A" },
            Year = new[] { "2023", "2023", "2024" } // 分区列
        }))
        {
            // 按 Year 列分区写入
            df.Lazy().SinkDelta(
                rootUrl, 
                partitionBy: "Year", 
                mode: DeltaSaveMode.Append, 
                cloudOptions: options
            );
        }

        // 验证 V0
        // Delta Lake 会自动识别分区列，并将其作为普通列 'Year' 加载回来
        using var dfV0 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect().Sort("Id");
        Assert.Equal(3, dfV0.Height);
        
        // 验证分区列内容
        var yearsV0 = dfV0["Year"].ToArray<string>();
        Assert.Equal("2023", yearsV0[0]);
        Assert.Equal("2024", yearsV0[2]);

        // ==========================================
        // 3. [Mode: Append] 追加更多分区数据 (Version 1)
        // ==========================================
        Console.WriteLine("Step 2: Appending Data to New & Existing Partitions...");
        using (var df2 = DataFrame.FromColumns(new { 
            Id = new[] { 4, 5 }, 
            Msg = new[] { "V2_2024_B", "V2_2025_A" },
            Year = new[] { "2024", "2025" } // 2024是现有分区，2025是新分区
        }))
        {
            df2.Lazy().SinkDelta(
                rootUrl, 
                partitionBy: "Year", 
                mode: DeltaSaveMode.Append, 
                cloudOptions: options
            );
        }

        // 验证 V1 (总共 5 行)
        using var dfV1 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect().Sort("Id");
        // dfV1.Show();
        Assert.Equal(5, dfV1.Height);
        
        // 检查是否包含新分区 2025
        Assert.Contains("2025", dfV1["Year"].ToArray<string>());

        // ==========================================
        // 4. [Mode: Overwrite] 覆盖分区表 (Version 2)
        // ==========================================
        // 预期：之前的 5 行数据全部逻辑删除，只保留新的数据
        // 注意：Overwrite 模式下，通常要保持分区架构一致，或者开启 schema evolution
        Console.WriteLine("Step 3: Overwriting Partitioned Table...");
        using (var dfOverwrite = DataFrame.FromColumns(new { 
            Id = new[] { 999 }, 
            Msg = new[] { "Overwrite_All" },
            Year = new[] { "2099" } // 全新的分区
        }))
        {
            dfOverwrite.Lazy().SinkDelta(
                rootUrl, 
                partitionBy: "Year", 
                mode: DeltaSaveMode.Overwrite, 
                cloudOptions: options
            );
        }

        // 验证 V2 (应该只有 1 行)
        using var dfV2 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(1, dfV2.Height);
        Assert.Equal("2099", dfV2["Year"].ToArray<string>()[0]);
        
        // 验证 V2 的历史 (Time Travel Check) - 回溯到 V1
        using var dfBackToV1 = LazyFrame.ScanDelta(rootUrl, version: 2, cloudOptions: options).Collect();
        Assert.Equal(5, dfBackToV1.Height);

        // ==========================================
        // 5. [Mode: ErrorIfExists] 冲突检测
        // ==========================================
        Console.WriteLine("Step 4: Testing ErrorIfExists...");
        using (var dfError = DataFrame.FromColumns(new { Id = new[] { 0 }, Year = new[] { "0000" } }))
        {
            var ex = Assert.Throws<PolarsException>(() => 
            {
                dfError.Lazy().SinkDelta(
                    rootUrl, 
                    partitionBy: "Year",
                    mode: DeltaSaveMode.ErrorIfExists, 
                    cloudOptions: options
                );
            });
            Assert.Contains("Table already exists", ex.Message);
        }

        // ==========================================
        // 6. [Mode: Ignore] 忽略写入
        // ==========================================
        Console.WriteLine("Step 5: Testing Ignore...");
        using (var dfIgnore = DataFrame.FromColumns(new { 
            Id = new[] { 888 }, 
            Msg = new[] { "Should_Not_Exist" },
            Year = new[] { "2099" }
        }))
        {
            dfIgnore.Lazy().SinkDelta(
                rootUrl, 
                partitionBy: "Year",
                mode: DeltaSaveMode.Ignore, 
                cloudOptions: options
            );
        }

        // 验证数据未变
        using var dfFinal = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(1, dfFinal.Height);
        Assert.DoesNotContain("Should_Not_Exist", dfFinal["Msg"].ToArray<string>());

        Console.WriteLine("Delta Partitioned Full Cycle Test Passed!");
    }
    [Fact]
    [Trait("DeltaLake", "SchemaEvolution")]
    public void Test_Sink_Delta_Schema_Evolution_Cycle()
    {
        // 1. 准备环境
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
        // Phase 1: 初始化表 (Schema A: Id, Val)
        // ==========================================
        using (var dfInit = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, 
            Val = new[] { "OldRow" } 
        }))
        {
            // 初始写入，模式为 Overwrite 或 Append 均可
            dfInit.Lazy().SinkDelta(
                rootUrl, 
                partitionBy: "Id", 
                mode: DeltaSaveMode.Overwrite, 
                cloudOptions: options
            );
        }

        // ==========================================
        // Phase 2: 尝试写入新列 (Schema B: Id, Val, NewCol)
        // 预期：can_evolve = false (默认)，应该报错
        // ==========================================
        using var dfNew = DataFrame.FromColumns(new { 
            Id = new[] { 2 }, 
            Val = new[] { "NewRow" }, 
            NewCol = new[] { 999 } // <--- 新增列
        });

        var ex = Assert.Throws<PolarsException>(() => 
        {
            dfNew.Lazy().SinkDelta(
                rootUrl, 
                partitionBy: "Id", 
                mode: DeltaSaveMode.Append, 
                canEvolve: false, // 显式禁止演变
                cloudOptions: options
            );
        });

        // 验证报错信息包含 Schema Mismatch 提示
        Assert.Contains("Schema mismatch", ex.Message);
        Assert.Contains("can_evolve", ex.Message);

        // ==========================================
        // Phase 3: 开启演变写入 (Schema Evolution)
        // 预期：can_evolve = true，应该成功
        // ==========================================
        dfNew.Lazy().SinkDelta(
            rootUrl, 
            partitionBy: "Id",
            mode: DeltaSaveMode.Append, 
            canEvolve: true, // <--- 开启演变！
            cloudOptions: options
        );

        // ==========================================
        // Phase 4: 验证读取 (Read Validation)
        // ==========================================
        using var resultDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Sort("Id", false) // 按 Id 排序方便断言
            .Collect();

        // 1. 验证列是否存在
        var columns = resultDf.ColumnNames;
        Assert.Contains("NewCol", columns); // 新列必须存在

        // 2. 验证数据行数
        Assert.Equal(2, resultDf.Height);

        // 3. 验证数据内容
        // Row 1 (Id=1): 旧数据，NewCol 应该是 null
        var row1 = resultDf.Row(0); // Id=1
        Assert.Equal(1, row1[0]); // Id
        Assert.Null(row1[2]);     // NewCol (自动补 Null)

        // Row 2 (Id=2): 新数据，NewCol 应该是 999
        var row2 = resultDf.Row(1); // Id=2
        Assert.Equal(2, row2[0]); // Id
        Assert.Equal(999, row2[2]); // NewCol

        Console.WriteLine("Schema Evolution Test Passed: Column 'NewCol' added successfully!");
    }
    [Fact]
    [Trait("DeltaLake", "Delete")]
    public void Test_Sink_And_Delete_Delta_Full_Cycle()
    {
        // ==========================================
        // 1. 环境与鉴权准备
        // ==========================================
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

        // 核心配置：MinIO 兼容性 + 原子提交支持
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";
        
        // ==========================================
        // 2. 初始化写入 (Version 1) - 带分区
        // ==========================================
        Console.WriteLine("Step 1: Initial Write (Partitioned by Year)...");
        // 数据结构：
        // 2023: ID 1, 2
        // 2024: ID 3, 4, 5
        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3, 4, 5 }, 
            Msg = new[] { "A", "B", "C", "D", "E" },
            Year = new[] { "2023", "2023", "2024", "2024", "2024" }
        }))
        {
            df.Lazy().SinkDelta(
                rootUrl, 
                partitionBy: "Year", 
                mode: DeltaSaveMode.Append, 
                cloudOptions: options
            );
        }

        // 验证 V0 (5 rows)
        using var dfV1 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        // dfV1.Show();
        Assert.Equal(5, dfV1.Height);

        // ==========================================
        // 3. 执行删除：部分删除 (Rewrite Test)
        // ==========================================
        // 目标：删除 Year='2024' 分区中 Id=4 的行。
        // 预期：2024 分区文件被重写，Id 3 和 5 保留。
        Console.WriteLine("Step 2: Delete Row (Id=4) - Rewrite Partition 2024...");
        
        // Predicate: (Year == '2024') & (Id == 4)
        // 注意：我们必须构造一个 Expr 传给 DeleteDelta
        var predicateRewrite = (Col("Year") == Lit("2024")) & (Col("Id")==4);
        
        Delta.Delete(rootUrl, predicateRewrite, cloudOptions: options);

        // 验证 V2 (应该剩 4 行: 1,2,3,5)
        using var dfV2 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect().Sort("Id");
        // dfV2.Show();
        Assert.Equal(4, dfV2.Height);
        Assert.DoesNotContain(4, dfV2["Id"].ToArray<int>());
        Assert.Contains(3, dfV2["Id"].ToArray<int>());
        Assert.Contains(5, dfV2["Id"].ToArray<int>());

        // ==========================================
        // 4. 执行删除：完全删除 (Drop Partition Test)
        // ==========================================
        // 目标：删除 Year='2023' 的所有行。
        // 预期：2023 分区文件被直接 Remove，不生成新文件。
        Console.WriteLine("Step 3: Delete Partition (Year='2023') - Drop Files...");
        
        var predicateDrop = Col("Year") == Lit("2023");
        
        Delta.Delete(rootUrl, predicateDrop, cloudOptions: options);

        // 验证 V3 (应该剩 2 行: 3, 5)
        using var dfV3 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect().Sort("Id");
        // dfV3.Show();
        Assert.Equal(2, dfV3.Height);
        Assert.DoesNotContain(1, dfV3["Id"].ToArray<int>());
        Assert.DoesNotContain(2, dfV3["Id"].ToArray<int>());
        Assert.Equal("2024", dfV3["Year"].ToArray<string>()[0]); // 只剩 2024

        // ==========================================
        // 5. 执行删除：无匹配 (No-op Test)
        // ==========================================
        // 目标：删除 Id=999 (不存在)。
        // 预期：不产生新版本 (或者产生空提交)，数据不变。
        Console.WriteLine("Step 4: Delete Non-existent (Id=999) - No-op...");
        
        var predicateNoOp = Col("Id") == 999;
        
        Delta.Delete(rootUrl, predicateNoOp, cloudOptions: options);

        // 验证 V4 (数据应与 V3 一致)
        using var dfV4 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        // dfV4.Show();
        Assert.Equal(2, dfV4.Height);

        // ==========================================
        // 6. Time Travel 验证
        // ==========================================
        Console.WriteLine("Step 5: Time Travel Check...");
        
        // 回溯到 V1 (5 行)
        using var dfBackV0 = LazyFrame.ScanDelta(rootUrl, version: 1, cloudOptions: options).Collect();
        // dfBackV0.Show();
        // Assert.Equal(5, dfBackV0.Height);
        
        // 回溯到 V2 (删除 Id=4 后，剩 4 行)
        using var dfBackV1 = LazyFrame.ScanDelta(rootUrl, version: 2, cloudOptions: options).Collect();
        // dfBackV1.Show();
        Assert.Equal(4, dfBackV1.Height);
        Assert.DoesNotContain(4, dfBackV1["Id"].ToArray<int>());

        Console.WriteLine("Delta Delete Full Cycle Passed!");
    }
    [Fact]
    [Trait("DeltaLake", "DeleteDV")]
    public void Test_Sink_And_Delete_Delta_With_Deletion_Vectors()
    {
        // ==========================================
        // 1. 环境准备
        // ==========================================
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
        // 2. 初始化写入 (V0 - CoW Ready)
        // ==========================================
        Console.WriteLine("Step 1: Initial Write (10 rows)...");
        using (var df = DataFrame.FromColumns(new { 
            Id = Enumerable.Range(0, 10).ToArray(), // 0..9
            Data = Enumerable.Repeat("A", 10).ToArray()
        }))
        {
            df.WriteDelta(rootUrl, mode: DeltaSaveMode.Append, cloudOptions: options);
        }

        // ==========================================
        // 3. 开启 Deletion Vectors 特性 (Upgrade to V1)
        // ==========================================
        Console.WriteLine("Step 2: Enabling Deletion Vectors...");
        
        // 这会把 Protocol 升级到 Reader v3 / Writer v7
        Delta.AddFeature(
            rootUrl, 
            DeltaTableFeatures.DeletionVectors, // 确保你有这个常量，或传字符串 "deletionVectors"
            allowProtocolIncrease: true, 
            cloudOptions: options
        );

        // ==========================================
        // 4. 执行删除 (Should use MoR/DV)
        // ==========================================
        // 删除 Id 为 0, 2, 4 的偶数行
        Console.WriteLine("Step 3: Delete Even numbers (0, 2, 4)...");
        var validIds = new[] {0,2,4};
        
        var predicate = Col("Id").IsIn(Lit(validIds).Implode());
        
        // 此时 Rust 端的 DeleteStrategy.determine 会发现 WriterVersion=7
        // 从而自动选择 MergeOnRead 策略
        Delta.Delete(rootUrl, predicate, cloudOptions: options);

        // ==========================================
        // 5. 验证数据
        // ==========================================
        using var dfV2 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect().Sort("Id");
        
        // 预期：剩下 7 行 (1, 3, 5, 6, 7, 8, 9)
        Assert.Equal(7, dfV2.Height);
        
        var ids = dfV2["Id"].ToArray<int>();
        Assert.DoesNotContain(0, ids);
        Assert.DoesNotContain(2, ids);
        Assert.DoesNotContain(4, ids);
        Assert.Contains(1, ids);
        Assert.Contains(9, ids);

        // ==========================================
        // 6. 验证 Time Travel (确保旧数据还在)
        // ==========================================
        Console.WriteLine("Step 4: Time Travel Check...");
        
        // Version 0: 原始 10 行
        // 注意：AddFeature 产生 Version 1，Delete 产生 Version 2
        using var dfV1 = LazyFrame.ScanDelta(rootUrl, version: 1, cloudOptions: options).Collect();
        Assert.Equal(10, dfV1.Height);
        
        Console.WriteLine("Delta Deletion Vector Test Passed!");
    }
    [Fact]
    [Trait("DeltaLake", "Upsert")]
    public void Test_Upsert_Delta_Full_Cycle_Upsert()
    {
        // ==========================================
        // 1. 环境与鉴权准备 (S3/MinIO)
        // ==========================================
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
        // 2. [Target] 初始化历史数据 (Version 1)
        // ==========================================
        Console.WriteLine("Step 1: Initial Write (Target Table)...");
        // 场景：订单表，按 Date 分区
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

        // 验证 V0
        using var dfV0 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect().Sort("OrderId");
        Assert.Equal(3, dfV0.Height);

        // ==========================================
        // 3. [Source] 构建 Upsert 数据 (Mixed Scenarios)
        // ==========================================
        Console.WriteLine("Step 2: Preparing Source Data for Merge...");
        
        // 我们构建一个 Source DataFrame，包含：
        // 1. Update: Order 1 -> Status="Shipped" (同一分区 2024-01-01)
        // 2. Insert (Backfill): Order 4 -> (同一分区 2024-01-01，漏录单)
        // 3. Insert (New Partition): Order 5 -> (新分区 2024-01-03)
        // 4. Unchanged: Order 2 和 Order 3 不在 Source 中，应保持原样
        
        using var sourceDf = DataFrame.FromColumns(new { 
            OrderId = new[] { 1, 4, 5 }, 
            Status = new[] { "Shipped", "Paid", "New" },
            Amount = new[] { 100.0, 400.0, 500.0 }, // Amount 这里没变或有变，逻辑一样
            Date = new[] { "2024-01-01", "2024-01-01", "2024-01-03" } 
        });
        // sourceDf.Show();
        // ==========================================
        // 4. 执行 MERGE (Upsert)
        // ==========================================
        Console.WriteLine("Step 3: Executing Merge (Upsert)...");
        
        // 调用我们封装的 MergeDelta
        // Merge Key: "OrderId"
        // 预期行为:
        // - 匹配 OrderId=1 -> 更新 Status
        // - 不匹配 (Target中无 OrderId 4, 5) -> 插入
        sourceDf.MergeDeltaOrdered(rootUrl, mergeKeys: ["OrderId"], cloudOptions: options).Execute();

        // ==========================================
        // 5. 验证结果 (Version 1)
        // ==========================================
        Console.WriteLine("Step 4: Verifying Results...");
        
        // 强制读取最新版本
        using var dfMerged = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect().Sort("OrderId");
        dfMerged.Show();
        // A. 验证总行数 (3 原有 + 2 新增 = 5 行)
        Assert.Equal(5, dfMerged.Height);

        // B. 验证 Case A (Update): Order 1 应该是 "Shipped"
        var row1 = dfMerged.Filter(Col("OrderId") == 1);
        Assert.Equal("Shipped", row1["Status"].ToArray<string>()[0]);
        Assert.Equal("2024-01-01", row1["Date"].ToArray<string>()[0]); // 分区保持不变

        // C. 验证 Case B (Insert Backfill): Order 4 应该存在且正确
        var row4 = dfMerged.Filter(Col("OrderId") == 4);
        Assert.Equal("Paid", row4["Status"].ToArray<string>()[0]);
        Assert.Equal("2024-01-01", row4["Date"].ToArray<string>()[0]);

        // D. 验证 Case C (Insert New Partition): Order 5 应该存在
        var row5 = dfMerged.Filter(Col("OrderId") == 5);
        Assert.Equal("New", row5["Status"].ToArray<string>()[0]);
        Assert.Equal("2024-01-03", row5["Date"].ToArray<string>()[0]);

        // E. 验证 Case D (Unchanged): Order 2 和 3 应该保持原样
        var row2 = dfMerged.Filter(Col("OrderId") ==2 );
        Assert.Equal("Pending", row2["Status"].ToArray<string>()[0]);
        
        var row3 = dfMerged.Filter(Col("OrderId")==3);
        Assert.Equal("Pending", row3["Status"].ToArray<string>()[0]);

        // ==========================================
        // 6. [高级验证] 分区剪枝验证 (Partition Pruning Check)
        // ==========================================
        // 理论上：
        // Source 涉及的分区是: 2024-01-01, 2024-01-03
        // Target 涉及的分区是: 2024-01-01, 2024-01-02
        //
        // 预期行为：
        // - 2024-01-01 分区文件被重写 (包含 Order 1 的更新和 Order 4 的插入)。
        // - 2024-01-02 分区 (Order 3) **不应该被读取或重写**。它的文件修改时间应保持在 T0。
        // - 2024-01-03 分区是新增文件。
        
        // 我们可以通过 Time Travel 检查历史版本，或者检查 Log，
        // 这里我们简单做一个 Time Travel 检查，确保旧数据还在，证明 CoW 机制生效。
        
        Console.WriteLine("Step 5: Time Travel Check...");
        using var dfHistory = LazyFrame.ScanDelta(rootUrl, version: 1, cloudOptions: options).Collect().Sort("OrderId");
        
        // 历史版本中 Order 1 还是 Pending
        var row1Old = dfHistory.Filter(Col("OrderId")==1);
        Assert.Equal("Pending", row1Old["Status"].ToArray<string>()[0]);
        
        // 历史版本中没有 Order 4
        Assert.Equal(0, dfHistory.Filter(Col("OrderId")==4).Height);

        Console.WriteLine("Delta Merge Full Cycle Passed! ");
    }

    [Fact]
    [Trait("DeltaLake", "MergeSchemaEvolution")]
    public void Test_Merge_Delta_Schema_Evolution_Full_Cycle()
    {
        // ==========================================
        // 0. 环境准备
        // ==========================================
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
        // Phase 1: 初始化 Target 表 (Schema V1)
        // ==========================================
        // Schema: [Id: Int, Val: String]
        // Data:   (1, "Old_A")
        using (var dfV1 = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, 
            Val = new[] { "Old_A" } 
        }))
        {
            dfV1.Lazy().SinkDelta(rootUrl, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
        }

        // ==========================================
        // Phase 2: 准备 Source 数据 (Schema V2)
        // ==========================================
        // Schema: [Id: Int, Val: String, NewCol: String] <--- 多了一列
        // Data:
        //   (1, "Updated_A", "Filled")  <-- 更新旧行 (Matched Update)
        //   (2, "New_B",     "Filled")  <-- 插入新行 (Not Matched Insert)
        using var sourceDf = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2 }, 
            Val = new[] { "Updated_A", "New_B" }, 
            NewCol = new[] { "Filled", "Filled" } 
        });

        // ==========================================
        // Phase 3: 默认保护测试 (Negative Test)
        // ==========================================
        // 预期：不传 canEvolve=true，必须报错，防止 Schema 意外污染
        var ex = Assert.Throws<PolarsException>(() => 
        {
            sourceDf.MergeDeltaOrdered(
                rootUrl, 
                mergeKeys: ["Id"], 
                cloudOptions: options
                // canEvolve 默认为 false
            ).Execute();
        });

        Assert.Contains("Schema mismatch", ex.Message);
        Assert.Contains("NewCol", ex.Message);
        Assert.Contains("can_evolve", ex.Message); // 确保提示语友好
        
        Console.WriteLine("[Pass] Schema Protection worked.");

        // ==========================================
        // Phase 4: 究极进化 (Evolution Test)
        // ==========================================
        // 预期：传入 canEvolve=true，自动修改 Metadata 并写入数据
        sourceDf.MergeDeltaOrdered(
            rootUrl, 
            mergeKeys: ["Id"], 
            cloudOptions: options,
            canEvolve: true // <--- The Magic Switch
        ).Execute();
        
        Console.WriteLine("[Pass] Merge with Schema Evolution executed.");

        // ==========================================
        // Phase 5: 数据验证 (Read & Verify)
        // ==========================================
        using var resultDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Sort("Id", false)
            .Collect();
        // resultDf.Show();
        // 1. 验证新列是否存在
        var cols = resultDf.ColumnNames;
        Assert.Contains("NewCol", cols);
        Assert.Equal(3, cols.Length); // Id, Val, NewCol

        // 2. 验证数据正确性
        // Row 1 (Id=1): 这是一个 Update 操作
        // 它应该同时更新 Val 和 NewCol
        var row1 = resultDf.Row(0);
        Assert.Equal(1, row1[0]);           // Id
        Assert.Equal("Updated_A", row1[1]); // Val (Updated)
        Assert.Equal("Filled", row1[2]);    // NewCol (Filled by Source)

        // Row 2 (Id=2): 这是一个 Insert 操作
        var row2 = resultDf.Row(1);
        Assert.Equal(2, row2[0]);
        Assert.Equal("New_B", row2[1]);
        Assert.Equal("Filled", row2[2]);

        // ==========================================
        // Phase 6: 验证旧 Schema 兼容性 (Backfill Null Test)
        // ==========================================
        // 场景：模拟一个旧的业务系统（还没升级代码），它只知道 Id 和 Val，不知道有 NewCol。
        // 它试图插入一条新数据 (Id=3)。
        // 预期：
        // 1. 不报错（因为 Target 有 NewCol，Source 没有，这是允许的，属于 Partial Update/Insert）。
        // 2. 插入后，Id=3 的 NewCol 自动为 Null。
        
        using (var dfV1_Late = DataFrame.FromColumns(new { 
            Id = new[] { 3 }, 
            Val = new[] { "Late_Arrival_V1" } 
            // 注意：这里没有 NewCol
        }))
        {
            // 即使 canEvolve = false 也可以，因为我们没有“新增”列，只是“少”列
            // 你的 Rust 代码逻辑会自动处理少列的情况 (fallback to NULL)
            dfV1_Late.MergeDeltaOrdered(
                rootUrl, 
                mergeKeys: ["Id"], 
                cloudOptions: options
            ).Execute();
        }

        // ==========================================
        // Phase 7: 最终全量验证
        // ==========================================
        using var finalDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Sort("Id", false)
            .Collect();

        // 我们现在应该有 3 行数据，3 种状态：
        // Id=1: 经历了 Evolution，NewCol = "Filled"
        // Id=2: 是 Evolution 时的新增行，NewCol = "Filled"
        // Id=3: 是 Evolution 后的 V1 数据，NewCol = null (关键验证点！)

        var row3 = finalDf.Row(2); // Id=3
        Assert.Equal(3, row3[0]);
        Assert.Equal("Late_Arrival_V1", row3[1]);
        Assert.Null(row3[2]); // <--- 必须是 Null

        Console.WriteLine("[Pass] V1 Data Merged into V2 Table successfully (NewCol is Null).");

    }
    [Fact]
    [Trait("DeltaLake", "Merge")]
    public void Test_Merge_Delta_Full_Features_Complex_Logic()
    {
        // ==========================================
        // 1. 环境准备
        // ==========================================
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
        // 2. [Target] 初始化数据 (Version 1)
        // ==========================================
        // 场景：库存表
        // ID 1: Stock=10, Status=Active   (待更新)
        // ID 2: Stock=20, Status=Active   (待忽略更新 - 条件不足)
        // ID 3: Stock=0,  Status=Recall   (待匹配删除 - 召回品)
        // ID 4: Stock=0,  Status=Obsolete (待源端缺失删除 - 过期品)
        // ID 5: Stock=50, Status=Active   (待保留 - 源端缺失但不过期)
        
        Console.WriteLine("Step 1: Initial Write (Target)...");
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
        // 3. [Source] 准备 Merge 数据
        // ==========================================
        Console.WriteLine("Step 2: Preparing Source Data...");
        
        // ID 1: Stock=100 (New) -> 满足 Update 条件 (New > Old)
        // ID 2: Stock=15  (New) -> 不满足 Update 条件 (15 < 20)，应保持原值 20
        // ID 3: Status=DeleteMe -> 满足 Matched Delete 条件
        // ID 6: Stock=60  (New) -> 满足 Insert 条件 (Stock > 0)
        // ID 7: Stock=0   (New) -> 不满足 Insert 条件 (Stock > 0)，应被丢弃
        // ID 4 & 5: Source 中不存在 (Not Matched By Source)
        
        using var sourceDf = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3, 6, 7 }, 
            Stock = new[] { 100, 15, 0, 60, 0 },
            Status = new[] { "Active", "Active", "DeleteMe", "New", "Bad" },
            Category = new[] { "A", "A", "B", "C", "C" }
        });

        // ==========================================
        // 4. 定义 MERGE 逻辑表达式
        // ==========================================
        // A. Update 条件: 只有当 Source 库存 > Target 库存时才更新
        // 原写法: Col("Stock_src_tmp") > Col("Stock")
        // 新写法:
        var updateCond = Delta.Source("Stock") > Delta.Target("Stock");

        // B. Matched Delete 条件: Source Status 标记为 'DeleteMe'
        var matchDeleteCond = Delta.Source("Status") == "DeleteMe";

        // C. Insert 条件: 只有库存 > 0 才插入
        var insertCond = Delta.Source("Stock") > 0;

        // D. Source Delete (Target Only) 条件: Target Status 是 'Obsolete'
        // 这里只需要引用 Target，因为 Source 那行不存在
        var srcDeleteCond = Delta.Target("Status") == "Obsolete";

        // ==========================================
        // 5. 执行 Full Merge
        // ==========================================
        Console.WriteLine("Step 3: Executing Full Merge...");

        sourceDf.MergeDeltaOrdered(
            rootUrl,
            mergeKeys: ["Id"],
            // matchedUpdateCond: updateCond,
            // matchedDeleteCond: matchDeleteCond,
            // notMatchedInsertCond: insertCond,
            // notMatchedBySourceDeleteCond: srcDeleteCond,
            cloudOptions: options
        )
        .WhenMatchedDelete(matchDeleteCond)
        .WhenMatchedUpdate(updateCond)
        .WhenNotMatchedBySourceDelete(srcDeleteCond)
        .WhenNotMatchedInsert(insertCond)
        .Execute();

        // ==========================================
        // 6. 验证结果 (Version 2)
        // ==========================================
        Console.WriteLine("Step 4: Verifying Results...");
        
        using var dfRes = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect().Sort("Id");
        dfRes.Show();

        // 预期结果分析：
        // ID 1: Updated to 100 (100 > 10)
        // ID 2: Kept at 20 (15 < 20, Update Condition Failed)
        // ID 3: Deleted (Matched Delete)
        // ID 4: Deleted (Target Only & Obsolete)
        // ID 5: Kept at 50 (Target Only & Active)
        // ID 6: Inserted (60 > 0)
        // ID 7: Ignored (0 !> 0)

        // 验证行数: 1, 2, 5, 6 共 4 行
        Assert.Equal(4, dfRes.Height);

        // Case 1: Conditional Update
        var row1 = dfRes.Filter(Col("Id") == 1);
        Assert.Equal(100, row1["Stock"].ToArray<int>()[0]);

        // Case 2: Conditional Update Skip (Keep Target)
        var row2 = dfRes.Filter(Col("Id") == 2);
        Assert.Equal(20, row2["Stock"].ToArray<int>()[0]); // 还是旧值 20

        // Case 3: Matched Delete
        Assert.Equal(0, dfRes.Filter(Col("Id") == 3).Height);

        // Case 4: Not Matched By Source Delete (Pruning)
        Assert.Equal(0, dfRes.Filter(Col("Id") == 4).Height);

        // Case 5: Not Matched By Source Keep
        var row5 = dfRes.Filter(Col("Id") == 5);
        Assert.Equal(50, row5["Stock"].ToArray<int>()[0]);

        // Case 6: Conditional Insert
        var row6 = dfRes.Filter(Col("Id") == 6);
        Assert.Equal(60, row6["Stock"].ToArray<int>()[0]);
        Assert.Equal("C", row6["Category"].ToArray<string>()[0]); // 新分区

        // Case 7: Conditional Insert Skip
        Assert.Equal(0, dfRes.Filter(Col("Id") == 7).Height);
        Delta.History(path:rootUrl,cloudOptions:options).Show();
        Console.WriteLine("Full Feature Merge Passed! 🚀");
    }
    [Fact]
    [Trait("DeltaLake", "MergeDV")]
    public void Test_Merge_Delta_With_Deletion_Vectors_Complex_Logic()
    {
        // ==========================================
        // 1. 环境准备
        // ==========================================
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
        // 2. [Target] 初始化数据 (Version 0)
        // ==========================================
        Console.WriteLine("Step 1: Initial Write (Target)...");
        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3, 4, 5 }, 
            Stock = new[] { 10, 20, 0, 0, 50 },
            Status = new[] { "Active", "Active", "Recall", "Obsolete", "Active" },
            Category = new[] { "A", "A", "B", "B", "A" } // Partition
        }))
        {
            df.Lazy().SinkDelta(
                rootUrl, 
                partitionBy: "Category", 
                mode: DeltaSaveMode.Append, 
                cloudOptions: options
            );
        }

        // ==========================================
        // 2.5 开启 Deletion Vectors 特性 (Version 1)
        // ==========================================
        // 这是 MoR 测试的核心！升级表协议，触发 Rust 后端的 MoR 分支。
        Console.WriteLine("Step 1.5: Enabling Deletion Vectors (MoR Mode)...");
        Delta.AddFeature(
            rootUrl, 
            "deletionVectors", // 或者使用常量 DeltaTableFeatures.DeletionVectors
            allowProtocolIncrease: true, 
            cloudOptions: options
        );

        // ==========================================
        // 3. [Source] 准备 Merge 数据
        // ==========================================
        Console.WriteLine("Step 2: Preparing Source Data...");
        
        using var sourceDf = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3, 6, 7 }, 
            Stock = new[] { 100, 15, 0, 60, 0 },
            Status = new[] { "Active", "Active", "DeleteMe", "New", "Bad" },
            Category = new[] { "A", "A", "B", "C", "C" }
        });

        // ==========================================
        // 4. 定义 MERGE 逻辑表达式
        // ==========================================
        var updateCond = Delta.Source("Stock") > Delta.Target("Stock");
        var matchDeleteCond = Delta.Source("Status") == "DeleteMe";
        var insertCond = Delta.Source("Stock") > 0;
        var srcDeleteCond = Delta.Target("Status") == "Obsolete";

        // ==========================================
        // 5. 执行 Full Merge (Version 2 - 将产生 DV 文件)
        // ==========================================
        Console.WriteLine("Step 3: Executing Full Merge (via Deletion Vectors)...");

        sourceDf.MergeDeltaOrdered(
            rootUrl,
            mergeKeys: ["Id"],
            // matchedUpdateCond: updateCond,
            // matchedDeleteCond: matchDeleteCond,
            // notMatchedInsertCond: insertCond,
            // notMatchedBySourceDeleteCond: srcDeleteCond,
            cloudOptions: options
        )
        .WhenMatchedUpdate(updateCond)
        .WhenMatchedDelete(matchDeleteCond)
        .WhenNotMatchedInsert(insertCond)
        .WhenNotMatchedBySourceDelete(srcDeleteCond)
        .Execute();

        // ==========================================
        // 6. 验证结果 (Version 2)
        // ==========================================
        Console.WriteLine("Step 4: Verifying Results...");
        
        using var dfRes = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect().Sort("Id");
        dfRes.Show();

        // 验证行数: 1, 2, 5, 6 共 4 行 (数据逻辑结果必须与 CoW 完全一致)
        Assert.Equal(4, dfRes.Height);

        // Case 1: Conditional Update
        var row1 = dfRes.Filter(Col("Id") == 1);
        Assert.Equal(100, row1["Stock"].ToArray<int>()[0]);

        // Case 2: Conditional Update Skip (Keep Target)
        var row2 = dfRes.Filter(Col("Id") == 2);
        Assert.Equal(20, row2["Stock"].ToArray<int>()[0]);

        // Case 3: Matched Delete
        Assert.Equal(0, dfRes.Filter(Col("Id") == 3).Height);

        // Case 4: Not Matched By Source Delete
        Assert.Equal(0, dfRes.Filter(Col("Id") == 4).Height);

        // Case 5: Not Matched By Source Keep
        var row5 = dfRes.Filter(Col("Id") == 5);
        Assert.Equal(50, row5["Stock"].ToArray<int>()[0]);

        // Case 6: Conditional Insert
        var row6 = dfRes.Filter(Col("Id") == 6);
        Assert.Equal(60, row6["Stock"].ToArray<int>()[0]);
        Assert.Equal("C", row6["Category"].ToArray<string>()[0]);

        // Case 7: Conditional Insert Skip
        Assert.Equal(0, dfRes.Filter(Col("Id") == 7).Height);

        // ==========================================
        // 7. Time Travel 验证 (确保旧文件没被物理重写)
        // ==========================================
        Console.WriteLine("Step 5: Time Travel Check...");
        using var dfV1 = LazyFrame.ScanDelta(rootUrl, version: 1, cloudOptions: options).Collect();
        Assert.Equal(5, dfV1.Height); // 原始数据完整保留

        Delta.History(path:rootUrl, cloudOptions:options).Show();
        Console.WriteLine("Full Feature DV Merge Passed! ");
    }
    [Fact]
    [Trait("DeltaLake", "MergeCompositeKeys")]
    [Obsolete("This test is to test old API")]
    public void Test_Merge_Delta_Composite_Keys_Full_Logic()
    {
        // ==========================================
        // 1. 环境准备
        // ==========================================
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
        // 2. [Target] 初始化数据 (Version 1)
        // ==========================================
        // 复合主键: [Region, StoreId]
        Console.WriteLine("Step 1: Initial Write (Target)...");
        
        // Data Scenarios:
        // 1. [North, 101]: Stock 10  -> 待更新 (Update)
        // 2. [North, 102]: Stock 20  -> 待忽略 (Update Skip)
        // 3. [South, 101]: Stock 5   -> 待删除 (Matched Delete) - 注意：StoreId 也是 101，测试复合键区分能力
        // 4. [South, 999]: Obsolete  -> 待清理 (Source Delete)
        // 5. [East,  555]: Active    -> 待保留 (Target Only Keep)

        using (var df = DataFrame.FromColumns(new { 
            Region = new[]  { "North", "North", "South", "South", "East" },
            StoreId = new[] { 101,     102,     101,     999,     555 },
            Stock = new[]   { 10,      20,      5,       0,       50 },
            Status = new[]  { "Active","Active","Recall","Obsolete","Active" }
        }))
        {
            // Region 既是主键一部分，也是分区键
            df.WriteDelta(
                rootUrl, 
                partitionBy: "Region", 
                mode: DeltaSaveMode.Append, 
                cloudOptions: options
            );
        }

        // ==========================================
        // 3. [Source] 准备 Merge 数据
        // ==========================================
        Console.WriteLine("Step 2: Preparing Source Data...");

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
        // 4. 定义表达式 (使用语法糖)
        // ==========================================
        
        // A. Update: Source 库存 > Target 库存
        var updateCond = Delta.Source("Stock") > Delta.Target("Stock");

        // B. Matched Delete: Source 状态标记
        var matchDeleteCond = Delta.Source("Status") == "DeleteMe";

        // C. Insert: 库存 > 0
        var insertCond = Delta.Source("Stock") > 0;

        // D. Source Delete: Target 状态为过期
        var srcDeleteCond = Delta.Target("Status") == "Obsolete";

        // ==========================================
        // 5. 执行 Full Merge (Composite Keys)
        // ==========================================
        Console.WriteLine("Step 3: Executing Composite Key Merge...");

        sourceDf.MergeDelta(
            rootUrl,
            // 关键点：传递多列作为复合主键
            mergeKeys:  ["Region", "StoreId"], 
            matchedUpdateCond: updateCond,
            matchedDeleteCond: matchDeleteCond,
            notMatchedInsertCond: insertCond,
            notMatchedBySourceDeleteCond: srcDeleteCond,
            cloudOptions: options
        );

        // ==========================================
        // 6. 验证结果
        // ==========================================
        Console.WriteLine("Step 4: Verifying Results...");
        
        using var dfRes = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Collect()
            .Sort(["Region", "StoreId"]); // Sort for deterministic assert

        dfRes.Show();
        // 预期结果行数分析：
        // 1. North/101: Updated (100)
        // 2. North/102: Kept (20)
        // 3. South/101: Deleted
        // 4. South/999: Deleted (Obsolete)
        // 5. East/555:  Kept (50)
        // 6. West/888:  Inserted (60)
        // 7. West/999:  Ignored
        // 总计：4 行

        Assert.Equal(4, dfRes.Height);

        // Case 1: Composite Match Update
        var row1 = dfRes.Filter(Col("Region") == "North" & Col("StoreId") == 101);
        Assert.Equal(100, row1["Stock"].ToArray<int>()[0]);

        // Case 2: Composite Match Skip
        var row2 = dfRes.Filter(Col("Region") == "North" & Col("StoreId") == 102);
        Assert.Equal(20, row2["Stock"].ToArray<int>()[0]);

        // Case 3: Composite Match Delete (South/101)
        // 关键验证：North/101 还在，但 South/101 没了，证明复合键生效
        Assert.Equal(0, dfRes.Filter(Col("Region") == "South" & Col("StoreId") == 101).Height);

        // Case 4: Not Matched By Source Delete
        Assert.Equal(0, dfRes.Filter(Col("Region") == "South" & Col("StoreId") == 999).Height);

        // Case 5: Target Only Keep
        var rowEast = dfRes.Filter(Col("Region") == "East");
        Assert.Equal(50, rowEast["Stock"].ToArray<int>()[0]);

        // Case 6: Insert
        var rowWest = dfRes.Filter(Col("Region") == "West" & Col("StoreId") == 888);
        Assert.Equal(60, rowWest["Stock"].ToArray<int>()[0]);

        Console.WriteLine("Composite Key Merge Passed! 🚀");
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
        // 意图：只更新 ColA，ColB 保持原样
        using var sourceDf = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, 
            ColA = new[] { 99 }
        });

        // 应该成功，不抛异常
        sourceDf.MergeDeltaOrdered(rootUrl, mergeKeys: ["Id"], cloudOptions: options).Execute();

        // 验证
        using var res = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        res.Show();
        // ColA 变了 (10 -> 99)
        Assert.Equal(99, res["ColA"][0]);
        // ColB 没变 (20 -> 20)
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

        // 1. 准备 Target 数据 (Id=1, Val=10)
        // 这是一个干净的表，主键唯一
        using (var dfTarget = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, 
            Time = new[] {"2025"},
            Val = new[] { 10 } 
        }))
        {
            dfTarget.Lazy().SinkDelta(rootUrl, mode: DeltaSaveMode.Append, cloudOptions: options);
        }

        // 2. 准备 Source 数据 (脏数据)
        // 注意：Id=1 出现了两次！这就好比你发了两个更新请求：
        // 请求A: 把 1 改成 88
        // 请求B: 把 1 改成 99
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

        // 1. Target: Id=1, Val=100 (有值)
        using (var dfTarget = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, 
            Val = new int?[] { 100 } // 使用 int? 允许后续变 null
        }))
        {
            dfTarget.WriteDelta(rootUrl, mode: DeltaSaveMode.Append, cloudOptions: options);
        }

        // 2. Source: Id=1, Val=null (显式 Null)
        // 意图：把 Id=1 的 Val 清空
        // 注意：这里必须显式构造含有 "Val" 列的 DataFrame，哪怕全是 null
        using (var dfSource = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, 
            Val = new int?[] { null } // <--- 关键点：列存在，值为 null
        }))
        {
            // 3. Merge
            // 预期逻辑：
            // Rust 检查 Schema -> 发现 Source 有 "Val" 列 -> has_source_col = true
            // update_expr = col("Val_src_tmp")
            // 执行 Update -> 写入 null
            dfSource.MergeDeltaOrdered(rootUrl, mergeKeys: ["Id"], cloudOptions: options).Execute();
        }

        // 4. 验证
        using var res = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        
        Console.WriteLine("=== Explicit Null Test Result ===");
        res.Show();

        // 断言：
        // 如果逻辑错判为“缺失列”，这里会是 100 (Keep Old Value) -> FAIL
        // 如果逻辑正确，这里必须是 null (Overwrite with Null) -> PASS
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

        // 1. Target: Id, OldCol
        using (var dfTarget = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2 }, 
            OldCol = new[] { "Existing1", "Existing2" } 
        }))
        {
            dfTarget.WriteDelta(rootUrl, mode: DeltaSaveMode.Append, cloudOptions: options);
        }

        // 2. Source: Id, NewCol (注意：没有 OldCol，这既是 Schema Evolution 也是 Partial Update)
        // Id=1 更新 -> OldCol 保持, NewCol 写入 "NewValue"
        // Id=3 插入 -> OldCol 为 Null, NewCol 写入 "FreshValue"
        using (var dfSource = DataFrame.FromColumns(new { 
            Id = new[] { 1, 3 }, 
            NewCol = new[] { "NewValue", "FreshValue" }
        }))
        {
            // 3. Merge (开启 Schema Evolution 通常是 Delta 引擎层的参数，
            // 但 Polars Merge 只要生成了这一列，写入时由 Delta Lake 决定是否接受。
            // 我们的目标是确保 Polars 输出了这一列。)
            dfSource.MergeDeltaOrdered(rootUrl, mergeKeys: ["Id"], cloudOptions: options,canEvolve:true).Execute();
        }

        // 4. 验证
        using var res = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Sort("Id", false) // 按 Id 排序: 1, 2, 3
            .Collect();
        
        Console.WriteLine("=== Schema Evolution Result ===");
        res.Show();

        // 断言列是否存在
        Assert.Contains("OldCol", res.ColumnNames);
        Assert.Contains("NewCol", res.ColumnNames); // <--- 如果 Rust 没改，这里必挂

        // 验证数据行
        // Id=1 (Matched): OldCol=Existing1 (Kept), NewCol=NewValue (Added)
        var row1 = res.Row(0); // Id=1
        Assert.Equal(1, row1[0]);
        Assert.Equal("Existing1", row1[1]);
        Assert.Equal("NewValue", row1[2]);

        // Id=2 (Target Only): OldCol=Existing2, NewCol=null (Backfilled)
        var row2 = res.Row(1); // Id=2
        Assert.Equal(2, row2[0]);
        Assert.Equal("Existing2", row2[1]); 
        Assert.Null(row2[2]); // <--- 关键：旧行自动补 Null

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
        // 1. Setup: 初始化一张表，有一行基础数据
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

        // 初始数据: Id=1, Value=0
        using (var dfInit = DataFrame.FromColumns(new { Id = new[] { 1 }, Value = new[] { 0 } }))
        {
            dfInit.WriteDelta(rootUrl, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
        }

        // 2. 准备并发任务
        // 我们启动 5 个 Task，每个 Task 试图把 Value 更新为自己的 TaskId
        // 这一定会触发 Conflict，因为大家都在争抢 Version 1
        int concurrency = 5;
        var tasks = new List<Task>();

        for (int i = 0; i < concurrency; i++)
        {
            int workerId = i + 1;
            tasks.Add(Task.Run(() =>
            {
                try 
                {
                    // 每个 Worker 都有自己的 Source
                    // 逻辑：把 Id=1 的 Value 更新为 workerId
                    // 同时插入一条新数据 Id = 100 + workerId
                    using var sourceDf = DataFrame.FromColumns(new 
                    { 
                        Id = new[] { 1, 100 + workerId }, 
                        Value = new[] { workerId, workerId } 
                    });

                    Console.WriteLine($"[Worker {workerId}] Starting Merge...");
                    
                    // 执行 Merge
                    sourceDf.MergeDeltaOrdered(
                        rootUrl, 
                        mergeKeys: ["Id"], 
                        cloudOptions: options
                    ).Execute();
                    
                    Console.WriteLine($"[Worker {workerId}] Success!");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Worker {workerId}] FAILED: {ex.Message}");
                    throw; // 测试应该失败
                }
            }));
        }

        // 3. 等待所有任务完成
        // 如果你的 Retry 逻辑有效，这里应该全部 Pass。
        // 如果没有 Retry，大概率只有 1 个成功，4 个抛出 "DeltaProtocolError: Transaction conflict"
        await Task.WhenAll(tasks);

    using var result = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
    // 3. 修复 "取消装箱可能为 null 的值"
    // GetColumn 返回的是 object，直接强转 int 在数据为空时会炸
    result.Show();

    // 验证 A: Id=1 的行，Value 应该是某一个 workerId (最后胜出的那个)
    int finalValue = (int)result.Filter(Col("Id") ==1)["Value"][0]!;
    Assert.True(finalValue > 0, "Id=1 should be updated by someone");

    // 验证 B: 所有的新增行 (Id > 100) 都应该存在
    // 因为 Insert 通常不会冲突 (除非 Id 重复)，但在全链路重试中，
    // 我们要确保 Retry 之后，Insert 逻辑也被正确重新执行了。
    var countNewRows = result.Filter(Col("Id") > 100).Height;
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

                    Console.WriteLine($"[Worker {workerId}] Starting Append...");
                    
                    sourceDf.WriteDelta(
                        rootUrl, 
                        mode: DeltaSaveMode.Append, 
                        cloudOptions: options
                    );
                    
                    Console.WriteLine($"[Worker {workerId}] Success!");
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
            var workerRowCount = result.Filter(Col("WorkerId") == i).Height;
            Assert.Equal(rowsPerWorker, workerRowCount);
        }

    }
    [Fact]
    [Trait("DeltaLake", "Vacuum")]
    public void Test_Sink_Overwrite_And_Vacuum_Full_Cycle()
    {
        // ==========================================
        // 1. 环境与鉴权准备
        // ==========================================
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

        // ==========================================
        // 2. 初始化写入 (Version 0) - 制造第一批文件
        // ==========================================
        Console.WriteLine("Step 1: Initial Write (Version 0)...");
        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2, 3 }, 
            Value = new[] { 10, 20, 30 }
        }))
        {
            df.Lazy().SinkDelta(
                rootUrl, 
                mode: DeltaSaveMode.Overwrite, 
                cloudOptions: options
            );
        }

        // ==========================================
        // 3. 覆盖写入 (Version 1) - 制造垃圾文件
        // ==========================================
        // 我们使用 Overwrite 模式。这意味着 Version 0 的 Parquet 文件虽然还在磁盘上，
        // 但在逻辑上已经过时了（Stale）。
        Console.WriteLine("Step 2: Overwrite (Version 1) - Creating stale files...");
        using (var df2 = DataFrame.FromColumns(new { 
            Id = new[] { 4, 5 }, 
            Value = new[] { 40, 50 }
        }))
        {
            df2.Lazy().SinkDelta(
                rootUrl, 
                mode: DeltaSaveMode.Overwrite, 
                cloudOptions: options
            );
        }

        // 验证当前数据 (V1)
        using var currentDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(2, currentDf.Height); // Id: 4, 5

        // 验证 Time Travel (V0) - 在 Vacuum 之前应该能读到
        Console.WriteLine("Step 3: Verify Time Travel BEFORE Vacuum...");
        using var oldDf = LazyFrame.ScanDelta(rootUrl, version: 1, cloudOptions: options).Collect();
        Assert.Equal(3, oldDf.Height); // Id: 1, 2, 3

        // ==========================================
        // 4. 执行 Vacuum (Dry Run)
        // ==========================================
        Console.WriteLine("Step 4: Vacuum Dry Run...");
        
        // retentionHours: 0 (立即过期，否则默认要等 7 天)
        // enforceRetention: false (必须设为 false，否则 Delta 会阻止删除最近的文件)
        // dryRun: true (只看不删)
        long filesToDelete = Delta.Vacuum(
            rootUrl, 
            retentionHours: 0, 
            enforceRetention: false, 
            dryRun: true,
            cloudOptions: options
        );

        Console.WriteLine($"Dry Run found {filesToDelete} files to delete.");
        Assert.True(filesToDelete > 0, "Dry run should find stale files from Version 0");

        // 再次验证 Time Travel (V0) - Dry Run 不应该删除物理文件
        using var oldDfCheck = LazyFrame.ScanDelta(rootUrl, version: 1, cloudOptions: options).Collect();
        Assert.Equal(3, oldDfCheck.Height);

        // ==========================================
        // 5. 执行 Vacuum (Real Run)
        // ==========================================
        Console.WriteLine("Step 5: Vacuum Real Run (Delete physical files)...");

        long deletedCount = Delta.Vacuum(
            rootUrl, 
            retentionHours: 0, 
            enforceRetention: false, 
            dryRun: false, 
            cloudOptions: options
        );

        Console.WriteLine($"Vacuum deleted {deletedCount} files.");
        Assert.Equal(filesToDelete, deletedCount);

        // ==========================================
        // 6. 验证毁灭性打击 (Time Travel Should Fail)
        // ==========================================
        Console.WriteLine("Step 6: Verify Time Travel FAILURE after Vacuum...");

        // 此时 Version 1 的 Parquet 文件已经被删除了。
        // 尝试读取 Version 1 应该抛出异常 (通常是 IO Error 或 File Not Found)
        var ex = Assert.ThrowsAny<Exception>(() => 
        {
            // 注意：ScanDelta 本身可能只是读取 Log，只有 Collect 真正读数据时才会炸
            LazyFrame.ScanDelta(rootUrl, version: 1, cloudOptions: options).Collect();
        });

        Console.WriteLine($"Caught expected exception: {ex.Message}");
        // 错误信息通常包含 "No such file" 或 "Objcet not found" 或 "IO error"
        // Assert.Contains("No such file", ex.Message); // 具体消息取决于 S3/MinIO 返回的错误文本

        // ==========================================
        // 7. 验证当前版本 (V1) 依然健康
        // ==========================================
        Console.WriteLine("Step 7: Verify Current Version is still alive...");
        using var aliveDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(2, aliveDf.Height);
        Assert.Equal(40, aliveDf["Value"][0]);

        Console.WriteLine("Delta Vacuum Full Cycle Passed!");
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

        // Step 1: 写入 Version 1 (正确的数据)
        Console.WriteLine("Step 1: Write Version 0 and 1 (Correct Data)");
        using (var df = DataFrame.FromColumns(new { Id = new[] { 1, 2 }, Val = new[] { "Good", "Good" } }))
        {
            df.Lazy().SinkDelta(rootUrl, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
        }

        // Step 2: 写入 Version 2 (错误的数据 - Overwrite)
        Console.WriteLine("Step 2: Write Version 2 (Bad Data - Overwrite)");
        using (var dfBad = DataFrame.FromColumns(new { Id = new[] { 1, 2 }, Val = new[] { "Bad", "Bad" } }))
        {
            dfBad.Lazy().SinkDelta(rootUrl, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
        }

        // 验证当前是坏数据
        using var currentDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal("Bad", currentDf["Val"][0]);

        // Step 3: 执行 Restore 回到 Version 0
        Console.WriteLine("Step 3: Restore to Version 1...");
        long newVersion = Delta.Restore(
            rootUrl, 
            version: 1, 
            cloudOptions: options
        );

        Console.WriteLine($"Restored! New Version is: {newVersion}");
        // 注意：Restore 会产生一个新的 Commit。
        // V1 (Good) -> V2 (Bad) -> V3 (Restore to V1 content)
        Assert.Equal(3, newVersion);

        // Step 4: 验证数据变回了好数据
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

        // Step 1: 制造碎片数据 (分 4 批写入，共 100 行)
        // 100 行对于 Z-Order 算法来说很重要，因为 100 % 8 != 0 (余 4)
        // 这将同时测试 Rust 端的 "SIMD Batch Loop" 和 "Remainder Loop"
        Console.WriteLine("Step 1: Write Fragmented Data (4 Appends, 100 Rows total)");

        for (int i = 0; i < 4; i++)
        {
            int startId = i * 25;
            // 生成 25 行数据
            var ids = Enumerable.Range(startId, 25).ToArray();
            // Category: "Even" 或 "Odd"，用于测试 Z-Order 的聚类效果
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

        // 验证当前有 4 个文件，100 行数据
        using var beforeDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(100, beforeDf.Height);
        Console.WriteLine($"Before Optimization: {beforeDf.Height} rows verified.");

        // Step 2: 执行 Optimize (Z-Order by Category, Id)
        // 我们期望它把 4 个小文件合并成 1 个大文件，并且数据按 Z-Order 排序
        Console.WriteLine("Step 2: Executing Optimize with Z-Order...");
        
        var zCols = new[] { "Id" };
        
        long numFiles = Delta.Optimize(
            rootUrl,
            targetSizeMb: 128, 
            zOrderColumns: zCols,
            cloudOptions: options
        );

        Console.WriteLine($"Optimized! Numbers of optimized file: {numFiles}");
        // Assert.True(numFiles > 0, "Should have optimized at least 1 file");

        // Step 3: 验证数据完整性 (Data Integrity)
        // 这一步最关键：经过复杂的位交织操作后，数据绝对不能乱、不能丢
        using var afterDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Sort("Id",  false ) // 拉回来按 ID 排序对比
            .Collect();

        Assert.Equal(100, afterDf.Height);
        afterDf.Show();
        // 验证首尾和中间的数据，确保没有 Bit 移位错误
        // Row 0
        Assert.Equal(0, afterDf["Id"][0]);
        Assert.Equal("Even", afterDf["Category"][0]);
        
        // Row 49 (Middle)
        Assert.Equal(49, afterDf["Id"][49]);
        Assert.Equal("Odd", afterDf["Category"][49]); // 49 is Odd

        // Row 99 (Last - 落在 Remainder Loop 处理范围内)
        Assert.Equal(99, afterDf["Id"][99]);
        Assert.Equal("Odd", afterDf["Category"][99]);
        Assert.Equal(148.5, (double)afterDf["Val"][99]!); // 99 * 1.5

        Console.WriteLine("Data Integrity Check Passed!");
    }
    [Fact]
    [Trait("DeltaLake", "OptimizeDV")]
    public void Test_Optimize_ZOrder_With_Deletion_Vectors_Scale_100()
    {
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

        // ==========================================
        // Step 1: 制造碎片数据 (分 4 批写入，共 100 行)
        // ==========================================
        Console.WriteLine("Step 1: Write Fragmented Data (4 Appends, 100 Rows total)...");

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
        Console.WriteLine($"Before Optimization: {beforeDf.Height} rows verified.");

        // ==========================================
        // Step 1.5: 开启 Deletion Vectors 特性 (MoR 模式)
        // ==========================================
        Console.WriteLine("Step 1.5: Enabling Deletion Vectors (MoR Mode)...");
        Delta.AddFeature(
            rootUrl, 
            DeltaTableFeatures.DeletionVectors, 
            allowProtocolIncrease: true, 
            cloudOptions: options
        );

        // ==========================================
        // Step 2: 制造 DV 僵尸行 (删除 Id = 10 和 Id = 99)
        // ==========================================
        Console.WriteLine("Step 2: Creating Deletion Vectors (Soft Deleting Id 10 and 99)...");
        using (var delDf = DataFrame.FromColumns(new { Id = new[] { 10, 99 }, Action = new[] { "DeleteMe", "DeleteMe" } }))
        {
            // 利用我们刚刚写好的 MoR Merge 来打 DV 补丁
            delDf.MergeDeltaOrdered(
                rootUrl,
                mergeKeys: ["Id"],
                // matchedDeleteCond: Delta.Source("Action") == "DeleteMe",
                canEvolve:true,
                
                cloudOptions: options
            )
            .WhenMatchedDelete(Delta.Source("Action") == "DeleteMe")
            .Execute();
        }

        // 此时，逻辑上只剩 98 行，但在底层物理文件上，10 和 99 依然存在，只是被标记在 DV (.bin) 文件里
        using var midDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(98, midDf.Height); 
        Console.WriteLine($"After DV Delete (Logical): {midDf.Height} rows verified.");
        // midDf.Show();
        // ==========================================
        // Step 3: 执行 Optimize (Z-Order by Id + DV Purging)
        // ==========================================
        Console.WriteLine("Step 3: Executing Optimize with Z-Order and DV Purging...");
        
        var zCols = new[] { "Id" };
        
        long numFiles = Delta.Optimize(
            rootUrl,
            targetSizeMb: 128, 
            zOrderColumns: zCols,
            cloudOptions: options
        );

        Console.WriteLine($"Optimized! Numbers of original files compacted/purged: {numFiles}");
        Assert.True(numFiles > 0, "Should have optimized at least 1 file");

        // ==========================================
        // Step 4: 验证数据完整性 (Data Integrity & Purge Check)
        // ==========================================
        using var afterDf = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Sort("Id", false) 
            .Collect();

        afterDf.Show();

        // 关键断言 1：经过复杂的 DV 过滤和 Z-Order 位交织后，行数必须依然是 98
        Assert.Equal(98, afterDf.Height);
        var validIds = new[] {10,99};
        
        var predicate = Col("Id").IsIn(Lit(validIds).Implode());
        // 关键断言 2：被软删除的行必须已经被物理剔除
        var deletedRows = afterDf.Filter(predicate);
        Assert.Equal(0, deletedRows.Height); 

        // 关键断言 3：没被删除的位交织数据必须正确
        // Row 0
        Assert.Equal(0, afterDf["Id"][0]);
        Assert.Equal("Even", afterDf["Category"][0]);
        
        // Row 49 (Id=49 is actually at index 48 now because Id=10 is gone)
        var row49 = afterDf.Filter(Col("Id") == 49);
        Assert.Equal(49, row49["Id"][0]);
        Assert.Equal("Odd", row49["Category"][0]); 

        // Row 98 (Last element, since 99 was deleted)
        var lastRow = afterDf.Filter(Col("Id") == 98);
        Assert.Equal(98, lastRow["Id"][0]);
        Assert.Equal("Even", lastRow["Category"][0]);
        Assert.Equal(147.0, (double)lastRow["Val"][0]!); // 98 * 1.5

        Console.WriteLine("Data Integrity & DV Purge Check Passed! 🚀");
    }
    [Fact]
    [Trait("DeltaLake", "Features")]
    public void Test_Add_DeletionVector_Feature()
    {
        var tableName = $"delta_feat_{Guid.NewGuid()}";
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
        
        // 1. 初始化一张普通的表 (Protocol 默认通常是较低的，比如 Reader v1/Writer v2)
        using (var df = DataFrame.FromColumns(new { Id = new[] { 1, 2, 3 } }))
        {
            df.WriteDelta(rootUrl, cloudOptions: options);
        }

        // 2. 尝试添加 DeletionVectors 特性
        // 这通常需要 Reader v3 / Writer v7
        Console.WriteLine("Enabling DeletionVectors...");
        Delta.AddFeature(
            rootUrl, 
            DeltaTableFeatures.DeletionVectors, 
            allowProtocolIncrease: true, 
            cloudOptions: options
        );

        // 3. 验证 (通过再次写入或读取来确保存储层没崩)
        // 既然开启了 DV，下次 Delta Lake 内部做 Delete 时就会用 DV 了
        // 这里我们要确保 Polars 依然能读它 (兼容性检查)
        using var dfRead = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(3, dfRead.Height);
        
        // 注意：目前我们没有暴露 GetProtocolVersion 的 API，
        // 但如果 AddFeature 没抛异常，说明 Protocol 升级成功了。
        // 如果想更严谨，可以去 S3 查看 _delta_log/0000000000000001.json 里的 protocol 字段
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
        
        // 1. 创建表
        using (var df = DataFrame.FromColumns(new { Id = new[] { 1 } }))
        {
            df.Lazy().SinkDelta(rootUrl, cloudOptions: options);
        }

        // 2. 设置属性：修改删除文件保留时间为 1 小时 (默认是 7 天)
        // 这在测试 Vacuum 功能时非常有用
        Console.WriteLine("Setting retention to 1 hour...");
        
        var props = new Dictionary<string, string>
        {
            { DeltaTableProperties.DeletedFileRetentionDuration, "interval 1 hour" },
            { "my.custom.metadata", "polars-driver-v1" } // 也可以设自定义 Tag
        };

        Delta.SetTableProperties(rootUrl, props, cloudOptions: options);

        // 3. 验证 (通过 GetProtocol 或查看 S3 上的 Metadata Action)
        // 目前我们还没做 GetProperties 的 API，但只要上面不报错，
        // 说明 Commit 已经成功写入了 Delta Log。
        
        // 补充验证：尝试再次写入，确保表没坏
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
        // ==========================================
        // 1. 环境准备
        // ==========================================
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
        // 2. [Target] 初始化数据 (Version 1)
        // ==========================================
        // 复合主键: [Region, StoreId]
        Console.WriteLine("Step 1: Initial Write (Target)...");
        
        // Data Scenarios:
        // 1. [North, 101]: Stock 10  -> 待更新 (Update)
        // 2. [North, 102]: Stock 20  -> 待忽略 (Update Skip)
        // 3. [South, 101]: Stock 5   -> 待删除 (Matched Delete) - 注意：StoreId 也是 101，测试复合键区分能力
        // 4. [South, 999]: Obsolete  -> 待清理 (Source Delete)
        // 5. [East,  555]: Active    -> 待保留 (Target Only Keep)

        using (var df = DataFrame.FromColumns(new { 
            Region = new[]  { "North", "North", "South", "South", "East" },
            StoreId = new[] { 101,     102,     101,     999,     555 },
            Stock = new[]   { 10,      20,      5,       0,       50 },
            Status = new[]  { "Active","Active","Recall","Obsolete","Active" }
        }))
        {
            // Region 既是主键一部分，也是分区键
            df.WriteDelta(
                rootUrl, 
                partitionBy: "Region", 
                mode: DeltaSaveMode.Append, 
                cloudOptions: options
            );
        }

        // ==========================================
        // 3. [Source] 准备 Merge 数据
        // ==========================================
        Console.WriteLine("Step 2: Preparing Source Data...");

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
        // 4. 定义表达式 (使用语法糖)
        // ==========================================
        
        // A. Update: Source 库存 > Target 库存
        var updateCond = Delta.Source("Stock") > Delta.Target("Stock");

        // B. Matched Delete: Source 状态标记
        var matchDeleteCond = Delta.Source("Status") == "DeleteMe";

        // C. Insert: 库存 > 0
        var insertCond = Delta.Source("Stock") > 0;

        // D. Source Delete: Target 状态为过期
        var srcDeleteCond = Delta.Target("Status") == "Obsolete";

        // ==========================================
        // 5. 执行 Ordered Full Merge (全新 Builder API)
        // ==========================================
        Console.WriteLine("Step 3: Executing Composite Key Ordered Merge...");

        // 关键点：使用全新的链式调用，底层会严格按照此处书写的顺序构建执行树
        sourceDf.MergeDeltaOrdered(
                rootUrl,
                mergeKeys: ["Region", "StoreId"], 
                cloudOptions: options
            )
            .WhenMatchedDelete(matchDeleteCond)           // 顺位 1: 先检查是否需要删除
            .WhenMatchedUpdate(updateCond)                // 顺位 2: 再检查是否需要更新
            .WhenNotMatchedInsert(insertCond)             // 顺位 3: 处理新增数据
            .WhenNotMatchedBySourceDelete(srcDeleteCond)  // 顺位 4: 处理源端不存在但需要删除的目标数据
            .Execute();                                   // 终极执行：下发 FFI 平行数组

        // ==========================================
        // 6. 验证结果
        // ==========================================
        Console.WriteLine("Step 4: Verifying Results...");
        
        using var dfRes = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Collect()
            .Sort(["Region", "StoreId"]); // Sort for deterministic assert

        dfRes.Show();
        
        // 预期结果行数分析：
        // 1. North/101: Updated (100)
        // 2. North/102: Kept (20)
        // 3. South/101: Deleted
        // 4. South/999: Deleted (Obsolete)
        // 5. East/555:  Kept (50)
        // 6. West/888:  Inserted (60)
        // 7. West/999:  Ignored
        // 总计：4 行

        Assert.Equal(4, dfRes.Height);

        // Case 1: Composite Match Update
        var row1 = dfRes.Filter(Col("Region") == "North" & Col("StoreId") == 101);
        Assert.Equal(100, row1["Stock"].ToArray<int>()[0]);

        // Case 2: Composite Match Skip
        var row2 = dfRes.Filter(Col("Region") == "North" & Col("StoreId") == 102);
        Assert.Equal(20, row2["Stock"].ToArray<int>()[0]);

        // Case 3: Composite Match Delete (South/101)
        // 关键验证：North/101 还在，但 South/101 没了，证明复合键生效
        Assert.Equal(0, dfRes.Filter(Col("Region") == "South" & Col("StoreId") == 101).Height);

        // Case 4: Not Matched By Source Delete
        Assert.Equal(0, dfRes.Filter(Col("Region") == "South" & Col("StoreId") == 999).Height);

        // Case 5: Target Only Keep
        var rowEast = dfRes.Filter(Col("Region") == "East");
        Assert.Equal(50, rowEast["Stock"].ToArray<int>()[0]);

        // Case 6: Insert
        var rowWest = dfRes.Filter(Col("Region") == "West" & Col("StoreId") == 888);
        Assert.Equal(60, rowWest["Stock"].ToArray<int>()[0]);

        Console.WriteLine("Ordered Composite Key Merge Passed! 🚀");
    }
    [Fact]
    [Trait("DeltaLake", "MergeOrderSemantics")]
    public void Test_Merge_Delta_Action_Order_Matters()
    {
        // ==========================================
        // 1. 环境准备：建两个完全相同的表
        // ==========================================
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

        // ==========================================
        // 2. 初始化 Target 数据：只有一条可怜的记录
        // ==========================================
        using (var dfTarget = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, 
            Stock = new[] { 10 }, 
            Status = new[] { "Active" } 
        }))
        {
            dfTarget.WriteDelta(urlDeleteWins, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
            dfTarget.WriteDelta(urlUpdateWins, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
        }

        // ==========================================
        // 3. 初始化 Source 数据：制造致命冲突！
        // ==========================================
        // 这条新数据既满足 "Stock > Target" (100 > 10)
        // 又满足 "Status == DeleteMe"
        using var sourceDf = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, 
            Stock = new[] { 100 }, 
            Status = new[] { "DeleteMe" } 
        });

        var updateCond = Delta.Source("Stock") > Delta.Target("Stock");
        var deleteCond = Delta.Source("Status") == "DeleteMe";

        // ==========================================
        // 4. 测试 A：Delete 优先 (Delete 截胡)
        // ==========================================
        Console.WriteLine("Executing Merge A: Delete -> Update");
        sourceDf.Lazy().MergeDeltaOrdered(urlDeleteWins, mergeKeys: ["Id"], cloudOptions: options)
            .WhenMatchedDelete(deleteCond)  // 顺位 1：先看能不能删
            .WhenMatchedUpdate(updateCond)  // 顺位 2：再看能不能更
            .Execute();

        // ==========================================
        // 5. 测试 B：Update 优先 (Update 截胡)
        // ==========================================
        Console.WriteLine("Executing Merge B: Update -> Delete");
        sourceDf.Lazy().MergeDeltaOrdered(urlUpdateWins, mergeKeys: ["Id"], cloudOptions: options)
            .WhenMatchedUpdate(updateCond)  // 顺位 1：先看能不能更
            .WhenMatchedDelete(deleteCond)  // 顺位 2：再看能不能删
            .Execute();

        // ==========================================
        // 6. 见证奇迹的时刻
        // ==========================================
        using var resDeleteWins = LazyFrame.ScanDelta(urlDeleteWins, cloudOptions: options).Collect();
        using var resUpdateWins = LazyFrame.ScanDelta(urlUpdateWins, cloudOptions: options).Collect();

        Console.WriteLine("--- Result A (Delete First) ---");
        resDeleteWins.Show();
        
        Console.WriteLine("--- Result B (Update First) ---");
        resUpdateWins.Show();

        // 断言 A：数据被成功删除了，因为 Delete 排在前面！
        Assert.Equal(0, resDeleteWins.Height); 

        // 断言 B：数据被更新了，因为 Update 抢先一步命中了！
        Assert.Equal(1, resUpdateWins.Height);
        Assert.Equal(100, resUpdateWins["Stock"].ToArray<int>()[0]);
        Assert.Equal("DeleteMe", resUpdateWins["Status"].ToArray<string>()[0]);

        Console.WriteLine("Order Semantics Test Passed!");
    }

    public record StoreRecord(string Region, int StoreId, int Stock, string Status);

    [Fact]
    [Trait("DeltaLake", "LINQ")]
    public void Test_Merge_Delta_Composite_Keys_Ordered_Logic_With_LINQ()
    {
        // ==========================================
        // 1. 环境准备
        // ==========================================
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

        // ==========================================
        // 2. [Target] 初始化数据 (Version 1)
        // ==========================================
        Console.WriteLine("Step 1: Initial Write (Target)...");

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
        // 3. [Source] 使用 LINQ 和 Record 准备 Merge 数据
        // ==========================================
        Console.WriteLine("Step 2: Preparing Source Data via LINQ...");

        // 直接从刚才写入的 Target 中读取出 DataFrame
        using var targetReadDf = DataFrame.ReadDelta(rootUrl, cloudOptions: options);

        // 神奇的 LINQ to Polars 转换开始
        var sourceQuery = targetReadDf.AsQueryable<StoreRecord>()
            // A. 过滤掉不需要去 Merge 更新的数据 (原 Target 中的 South/999 和 East/555)
            .Where(x => x.Region == "North" || (x.Region == "South" && x.StoreId == 101))
            // B. 改变现有数据的值，制造 Update 和 Delete 条件
            .Select(x => new StoreRecord(
                x.Region,
                x.StoreId,
                // Stock 突变逻辑
                (x.Region == "North" && x.StoreId == 101) ? 100 : 
                (x.Region == "North" && x.StoreId == 102) ? 15 : 0, 
                // Status 突变逻辑
                (x.Region == "South" && x.StoreId == 101) ? "DeleteMe" : x.Status
            ))
            // C. 模拟外部系统新产生的数据，追加进流中
            .Concat([
                new StoreRecord("West", 888, 60, "New"),
                new StoreRecord("West", 999, 0, "Bad")
            ]);
        // 终极转换：流式查询退化为物理 DataFrame，准备扔给 Rust
        using var sourceDf = sourceQuery.ToDataFrame();

        // 打印一下看看到底造出了什么数据，心里更有底
        Console.WriteLine("--- Source DataFrame Generated from LINQ ---");
        sourceDf.Show();

        // ==========================================
        // 4. 定义表达式 (使用语法糖)
        // ==========================================
        var updateCond = Delta.Source("Stock") > Delta.Target("Stock");
        var matchDeleteCond = Delta.Source("Status") == "DeleteMe";
        var insertCond = Delta.Source("Stock") > 0;
        var srcDeleteCond = Delta.Target("Status") == "Obsolete";

        // ==========================================
        // 5. 执行 Ordered Full Merge
        // ==========================================
        Console.WriteLine("Step 3: Executing Composite Key Ordered Merge...");

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

        // ==========================================
        // 6. 验证结果
        // ==========================================
        Console.WriteLine("Step 4: Verifying Results...");
        
        using var dfRes = LazyFrame.ScanDelta(rootUrl, cloudOptions: options)
            .Collect()
            .Sort(["Region", "StoreId"]);

        dfRes.Show();
        
        Assert.Equal(4, dfRes.Height);

        // Case 1: Composite Match Update
        var row1 = dfRes.Filter(Col("Region") == "North" & Col("StoreId") == 101);
        Assert.Equal(100, row1["Stock"].ToArray<int>()[0]);

        // Case 2: Composite Match Skip
        var row2 = dfRes.Filter(Col("Region") == "North" & Col("StoreId") == 102);
        Assert.Equal(20, row2["Stock"].ToArray<int>()[0]);

        // Case 3: Composite Match Delete
        Assert.Equal(0, dfRes.Filter(Col("Region") == "South" & Col("StoreId") == 101).Height);

        // Case 4: Not Matched By Source Delete
        Assert.Equal(0, dfRes.Filter(Col("Region") == "South" & Col("StoreId") == 999).Height);

        // Case 5: Target Only Keep
        var rowEast = dfRes.Filter(Col("Region") == "East");
        Assert.Equal(50, rowEast["Stock"].ToArray<int>()[0]);

        // Case 6: Insert
        var rowWest = dfRes.Filter(Col("Region") == "West" & Col("StoreId") == 888);
        Assert.Equal(60, rowWest["Stock"].ToArray<int>()[0]);

        Console.WriteLine("Ordered Composite Key Merge with LINQ Passed!");
    }
}
