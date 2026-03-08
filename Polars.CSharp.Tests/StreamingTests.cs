using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;
using static Polars.CSharp.Polars;
using Polars.NET.Core.Data;
using Xunit.Abstractions;

namespace Polars.CSharp.Tests;
public class StreamingTests(ITestOutputHelper output)
{
    private readonly ITestOutputHelper _output = output;

    private class BigDataPoco
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public double Value { get; set; }
    }

    // 生成器：惰性生成数据，模拟从数据库或文件读取
    private static IEnumerable<BigDataPoco> GenerateData_1(int count)
    {
        for (int i = 0; i < count; i++)
        {
            yield return new BigDataPoco
            {
                Id = i,
                Name = $"Row_{i}",
                Value = i * 0.1
            };
        }
    }

    [Fact]
    public void Test_FromArrowStream_Integration()
    {
        int totalRows = 500_000; // 50万行
        int batchSize = 100_000; // 10万行一个 Batch

        // 1. 启动流式导入
        // 这一步应该非常快，且内存占用平稳
        using var df = DataFrame.FromEnumerable(GenerateData_1(totalRows), batchSize);

        // 2. 验证行数
        Assert.Equal(totalRows, df.Height);

        // 3. 验证头部数据
        Assert.Equal(0, df.GetValue<int>(0, "Id"));
        Assert.Equal("Row_0", df.GetValue<string>(0, "Name"));

        // 4. 验证中间/尾部数据 (跨 Batch)
        // 第 250,000 行应该在第 3 个 Batch 里
        long midIndex = 250_000;
        Assert.Equal((int)midIndex, df.GetValue<int>(midIndex, "Id"));
        Assert.Equal($"Row_{midIndex}", df.GetValue<string>(midIndex, "Name"));

        // 验证最后一行
        long lastIndex = totalRows - 1;
        Assert.Equal((int)lastIndex, df.GetValue<int>(lastIndex, "Id"));
    }

    private class StreamPoco
    {
        public int Id { get; set; }
        public string Group { get; set; }
        public double Value { get; set; }
    }

    // 模拟无限数据流 / 数据库读取 / CSV 行读取
    private static IEnumerable<StreamPoco> GenerateData_2(int count)
    {
        for (int i = 0; i < count; i++)
        {
            // 可以在这里打断点，观察是否是 lazy 执行的
            yield return new StreamPoco
            {
                Id = i,
                Group = i % 2 == 0 ? "Even" : "Odd",
                Value = i * 1.5
            };
        }
    }
    [Fact]
    public void Test_Lazy_ScanArrowStream_EndToEnd()
    {
        int totalRows = 50000;
        int batchSize = 10000; // 5 个 Batch

        // 1. 定义 LazyFrame (此时 C# 还没有开始遍历 GenerateData)
        var lf = LazyFrame.ScanEnumerable(GenerateData_2(totalRows),null, batchSize);

        // 2. 构建查询计划 (Filter -> Select -> Alias)
        // 我们只保留偶数行，并且把 Value 翻倍
        var q = lf
            .Filter(Col("Group") == Lit("Even"))
            .Select(
                Col("Id"),
                (Col("Value") * 2).Alias("DoubleValue")
            );

        // 3. 第一次执行 (Trigger!)
        // 此时 Rust 才会通过回调，驱动 C# 的 Enumerator
        using var df1 = q.Clone().Collect();

        // --- 验证 1: 数据完整性 (验证 PrependEnumerator 是否工作) ---
        // 过滤后应该剩 25000 行
        Assert.Equal(totalRows / 2, df1.Height);

        // 检查第一行 (Id=0)。如果 Prepend 逻辑坏了，Id=0 这一批次可能会丢失。
        Assert.Equal(0, df1.GetValue<int>(0, "Id"));
        Assert.Equal(0 * 1.5 * 2, df1.GetValue<double>(0, "DoubleValue")); // 0

        // 检查最后一行 (Id=4998)
        var lastIdx = df1.Height - 1;
        Assert.Equal(49998, df1.GetValue<int>(lastIdx, "Id"));
        
        // --- 验证 2: 可重入性 (Re-entrant) ---
        // LazyFrame 应该可以被多次 Collect。
        // 这验证了我们的 ScanContext.Factory 是否正确创建了新的 Enumerator。
        
        using var df2 = q.Collect();
        Assert.Equal(df1.Height, df2.Height);
        Assert.Equal(df1.GetValue<int>(0, "Id"), df2.GetValue<int>(0, "Id"));
    }
    
    [Fact]
    public void Test_Lazy_Stream_Empty()
    {
        // 验证空流处理：不应该崩溃，应该返回空 DataFrame
        var lf = LazyFrame.ScanEnumerable(new List<StreamPoco>(), batchSize: 100);
        using var df = lf.Collect();

        Assert.Equal(0, df.Height);
        // Schema 应该依然存在 (Id, Group, Value)
        Assert.Contains("Id", df.ColumnNames);
    }
    [Fact]
    public void Test_EndToEnd_Streaming_Invincible()
    {
        // 1. 模拟超大规模数据 (1亿行)
        // 实际上 C# 只是生成器，不占内存
        static IEnumerable<BigDataPoco> InfiniteStream()
        {
            // 假设我们要处理 1亿行，这里为了测试速度用 100万行演示逻辑
            // 如果你把这个数字改成 100_000_000，只要你内存大于 BatchSize，它依然能跑通！
            int limit = 1_000_000; 
            for (int i = 0; i < limit; i++)
            {
                yield return new BigDataPoco 
                { 
                    Id = i, 
                    Name = "IgnoreMe", // 大部分数据会被过滤掉
                    Value = i 
                };
            }
        }

        int batchSize = 50_000;

        // 2. 建立管道
        // Input: Streaming (C# -> Rust Chunk by Chunk)
        var lf = LazyFrame.ScanEnumerable(InfiniteStream(),null, batchSize);

        // 3. 定义计算图
        // 过滤条件非常苛刻，只有最后一行满足
        var q = lf
            .Filter(Col("Id") > 999_998) 
            .Select(Col("Id"), Col("Value"));

        // 4. 执行: Streaming Collect
        // Rust 引擎会：
        //   a. 拉取 C# 5万行
        //   b. 在内存中 Filter，扔掉 49999 行
        //   c. 释放这 5万行内存
        //   d. 重复...
        // 整个过程内存占用极低，哪怕处理 1TB 数据也不会崩
        using var df = q.Collect();

        // 5. 验证
        Assert.Equal(1, df.Height);
        Assert.Equal(999999, df.GetValue<int>(0, "Id"));
        
        Console.WriteLine("Streaming execution completed without OOM!");
    }
    private class BenchPoco
    {
        public int Id { get; set; }
        public string Category { get; set; } // 测试 StringView
        public double Value { get; set; }
    }

    // 1. 无限弹药库：惰性生成器
    // 只有当 Rust 端通过 FFI 拉取时，这里才会执行
    private IEnumerable<BenchPoco> GenerateMassiveData(int count)
    {
        // 预分配常用字符串，避免 C# 端生成 1亿个 string 对象的开销
        // 模拟真实场景中有限的分类
        var catA = "Category_A"; 
        var catB = "Category_B"; 

        for (int i = 0; i < count; i++)
        {
            yield return new BenchPoco
            {
                Id = i,
                // 交替生成，方便后续 Filter 测试
                Category = (i % 2 == 0) ? catA : catB, 
                Value = 1.0 // 设为 1.0 方便验证 Sum = Count
            };
        }
    }
    [Fact(Skip ="Too long")]
    [Trait("Stream", "StressTest")] 
    public async Task Test_100_Million_Rows_StreamingAsync()
    {
        // ====================================================
        // 配置区
        // ====================================================
        int totalRows = 100_000_000; 
        int batchSize = 500_000;     
        
        // 预热 GC，确保基准线准确
        GC.Collect();
        GC.WaitForPendingFinalizers();

        // ====================================================
        // 内存监控启动
        // ====================================================
        using var cts = new CancellationTokenSource();
        long peakPhysicalMemory = 0;
        long peakManagedMemory = 0;

        var proc = Process.GetCurrentProcess();

        // 启动后台任务监控内存
        var monitorTask = Task.Run(async () => 
        {
            while (!cts.IsCancellationRequested)
            {
                proc.Refresh();
                // 1. 物理内存 : 包含 C# 堆 + Rust 堆 + 所有非托管资源 (最重要!)
                long currentPhysical = proc.PrivateMemorySize64;
                
                // 2. 托管内存 (GC Heap): 仅 C# 对象
                long currentManaged = GC.GetTotalMemory(false);

                // 更新峰值
                if (currentPhysical > peakPhysicalMemory) peakPhysicalMemory = currentPhysical;
                if (currentManaged > peakManagedMemory) peakManagedMemory = currentManaged;

                // 实时打印 (可选，根据测试运行器可能看不到实时输出)
                // Console.WriteLine($"[Monitor] Phys: {currentPhysical/1024/1024} MB | Man: {currentManaged/1024/1024} MB");

                try { await Task.Delay(100, cts.Token); } catch { break; }
            }
        });

        _output.WriteLine($"[Start] Baseline Physical: {proc.PrivateMemorySize64 / 1024 / 1024} MB");

        var sw = Stopwatch.StartNew();

        // ====================================================
        // 核心逻辑
        // ====================================================
        try 
        {
            // 1. 建立管道
            using var lf = LazyFrame.ScanEnumerable(
                    GenerateMassiveData(totalRows), 
                    batchSize: batchSize, 
                    useBuffered: true
                );
            // 2. 构建查询
            var q = lf
                .Filter(Col("Category") == Lit("Category_A"))
                .Select(
                    Col("Id").Sum().Alias("SumId"),
                    (Col("Value") * 2).Sum().Alias("SumValue"),
                    Col("Id").Count().Alias("Count")
                );

            // 3. 执行：CollectStreaming
            // 关键点：use 语句会触发 Dispose，释放 Rust 端资源
            using var df = q.Collect(useStreaming:true);
            
            // 停止计时
            sw.Stop();
            
            // ====================================================
            // 验证结果
            // ====================================================
            Assert.Equal(1, df.Height);
            long expectedCount = totalRows / 2;
            Assert.Equal(expectedCount, df.GetValue<long>(0, "Count"));
            double expectedSumValue = expectedCount * 2.0;
            Assert.Equal(expectedSumValue, df.GetValue<double>(0, "SumValue"));
        }
        finally
        {
            // 停止监控
            cts.Cancel();
            // 等待监控任务结束（吞掉 Cancel 异常）
            try {await monitorTask;} catch {} 
        }

        // ====================================================
        // 最终报告
        // ====================================================
        proc.Refresh();
        long endPhysical = proc.PrivateMemorySize64;
        if (endPhysical > peakPhysicalMemory) peakPhysicalMemory = endPhysical;
        
        Console.WriteLine("--------------------------------------------------");
        Console.WriteLine($"[Result] Processed {totalRows:N0} rows");
        Console.WriteLine($"[Time]   Total Time: {sw.Elapsed.TotalSeconds:F2} s");
        Console.WriteLine($"[Speed]  Throughput: {totalRows / sw.Elapsed.TotalSeconds:N0} rows/sec");
        Console.WriteLine("--------------------------------------------------");
        Console.WriteLine($"[Memory] Peak Physical (Total): {peakPhysicalMemory / 1024 / 1024} MB");
        Console.WriteLine($"[Memory] Peak Managed  (C#):    {peakManagedMemory / 1024 / 1024} MB");
        Console.WriteLine($"[Memory] End Physical:          {endPhysical / 1024 / 1024} MB");
        Console.WriteLine("--------------------------------------------------");

        // ====================================================
        // 内存断言 
        // ====================================================
        // 这里给一个宽松的阈值 4GB，如果超过说明绝对有问题。
        Assert.True(peakPhysicalMemory < 4L * 1024 * 1024 * 1024, 
            $"Memory Leak Detected! Peak memory usage ({peakPhysicalMemory/1024/1024} MB) exceeded 4GB limit.");
    }
    [Fact]
    public void Test_ArrowToDbStream_EndToEnd()
    {
        // 1. 构造一个模拟的 Arrow RecordBatch 流
        // [核心修改] 使用 DateOnly，因为我们已经决定 Date32 映射到 DateOnly
        var today = DateOnly.FromDateTime(DateTime.Now); 

        var schema = new Schema.Builder()
            .Field(new Field("Id", Int32Type.Default, true))
            .Field(new Field("Name", StringViewType.Default, true))
            .Field(new Field("Date", Date32Type.Default, true)) // Date32
            .Build();

        IEnumerable<RecordBatch> MockArrowStream()
        {
            // Batch 1: [1, "Alice", today]
            // Arrow 的 Date32Builder 通常接受 DateTimeOffset 或 int (days)
            // 这里我们把 DateOnly 转回午夜的 DateTimeOffset 传进去
            var dtOffset = new DateTimeOffset(today.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);
            
            yield return new RecordBatch(schema, [
                new Int32Array.Builder().Append(1).Build(),
                new StringViewArray.Builder().Append("Alice").Build(),
                new Date32Array.Builder().Append(dtOffset).Build() 
            ], 1);

            // Batch 2: [2, "Bob", null]
            yield return new RecordBatch(schema, [
                new Int32Array.Builder().Append(2).Build(),
                new StringViewArray.Builder().Append("Bob").Build(),
                new Date32Array.Builder().AppendNull().Build()
            ], 1);
        }

        // 2. 核心测试
        using var dbReader = new ArrowToDbStream(MockArrowStream());

        // 3. 模拟 SqlBulkCopy
        var targetTable = new System.Data.DataTable();
        targetTable.Load(dbReader);

        // 4. 验证结果
        Assert.Equal(2, targetTable.Rows.Count);
        
        // Row 1
        Assert.Equal(1, targetTable.Rows[0]["Id"]);
        Assert.Equal("Alice", targetTable.Rows[0]["Name"]);
        
        // [核心修改] 验证类型是 DateOnly，且值相等
        var actualDate = targetTable.Rows[0]["Date"];
        Assert.IsType<DateOnly>(actualDate);
        Assert.Equal(today, (DateOnly)actualDate);

        // Row 2
        Assert.Equal(2, targetTable.Rows[1]["Id"]);
        Assert.Equal(DBNull.Value, targetTable.Rows[1]["Date"]);
    }
    [Fact]
    public void Test_SinkTo_Generic_EndToEnd()
    {
        // =========================================================
        // 场景：全链路流式写入 (Rust -> C# SinkTo -> ArrowToDbStream -> Mock DB)
        // 验证：通过 SinkTo 接口是否能正确把流式数据灌入 DataTable (模拟 SqlBulkCopy)
        // =========================================================

        int totalRows = 50_000;
        
        // 1. 准备数据源
        var df = DataFrame.FromColumns(new 
        {
            Id = Enumerable.Range(0, totalRows).ToArray(),
            Value = Enumerable.Repeat("test_val", totalRows).ToArray()
        });

        // 2. 准备 "伪数据库"
        var targetTable = new System.Data.DataTable();

        // 3. 调用通用 SinkTo 接口
        // 这一步会阻塞直到 Polars 计算完成且 Mock DB 写入完成
        df.Lazy().SinkTo(reader => 
        {
            Console.WriteLine("[MockDB] Start Bulk Insert...");
            
            // 模拟 SqlBulkCopy.WriteToServer(reader)
            // DataTable.Load 内部会遍历 reader 直到结束
            targetTable.Load(reader);
            
            Console.WriteLine($"[MockDB] Inserted {targetTable.Rows.Count} rows.");
        });

        // 4. 验证结果
        Assert.Equal(totalRows, targetTable.Rows.Count);
        
        // 验证首尾数据
        Assert.Equal(0, targetTable.Rows[0]["Id"]);
        Assert.Equal("test_val", targetTable.Rows[0]["Value"]);
        Assert.Equal(totalRows - 1, targetTable.Rows[totalRows - 1]["Id"]);
    }
    [Fact]
    [Trait("Stream","E2E")]
    public void Test_ETL_Stream_EndToEnd()
    {
        int totalRows = 100_000;
        
        // ---------------------------------------------------------
        // [Extract] 
        // ---------------------------------------------------------
        var sourceTable = new System.Data.DataTable();
        sourceTable.Columns.Add("OrderId", typeof(int));
        sourceTable.Columns.Add("Region", typeof(string));
        sourceTable.Columns.Add("Amount", typeof(double));
        sourceTable.Columns.Add("OrderDate", typeof(DateTime));

        // 生成模拟数据
        // 偶数行是 "US" 地区，奇数行是 "EU" 地区
        // 金额随 ID 增加
        // 日期固定为今天中午（避开时区坑）
        var baseDate = DateTime.Now.Date.AddHours(12);
        for (int i = 0; i < totalRows; i++)
        {
            string region = (i % 2 == 0) ? "US" : "EU";
            sourceTable.Rows.Add(i, region, i * 1.5, baseDate.AddDays(i % 10)); // 日期循环
        }

        // 创建源 DataReader (模拟 SqlDataReader)
        // 注意：IDataReader 是 forward-only 的，读过就没了，所以我们要小心处理 Schema
        using var sourceReader = sourceTable.CreateDataReader();

        // ---------------------------------------------------------
        // 2. [Transform] 构建 Polars 流式管道
        // ---------------------------------------------------------

        var lf = LazyFrame.ScanDatabase(sourceReader,50000);

        // Step C: 定义转换逻辑 (Transform)
        // 业务需求：
        // 1. 只保留 "US" 地区的订单
        // 2. 计算税后金额 (Amount * 1.08)
        // 3. 选取需要的列
        var pipeline = lf
            .Filter(Col("Region") == Lit("US"))
            .WithColumns((Col("Amount") * 1.08).Alias("TaxedAmount"))
            .Select(Col("OrderId"),Col("TaxedAmount"),
                    Col("OrderDate"));

        // ---------------------------------------------------------
        // 3. [Load] 准备目标数据库 & 执行 Sink
        // ---------------------------------------------------------
        
        var targetTable = new System.Data.DataTable(); // 模拟目标表

        var schemaContract = new Dictionary<string, Type>
        {
            { "OrderDate", typeof(DateTime) }
        };
        Console.WriteLine("[ETL] Starting Pipeline...");
        var sw = Stopwatch.StartNew();
        // 模拟 SqlBulkCopy.WriteToServer(reader)
        // 这一步会疯狂调用 reader.Read()，从而反向拉动整个链条

        // 执行流式写入！
        // 这一步会驱动：
        // sourceReader -> Arrow转换 -> Rust引擎(Filter/Calc) -> Callback -> Buffer -> ArrowToDbStream -> targetTable.Load
        pipeline.SinkTo(reader => 
        {
            // 验证点 1: Reader 敢不敢对外宣称它是 DateTime?
            // (如果没有 Override，它只能宣称是 Int64)
            int dateColIndex = reader.GetOrdinal("OrderDate");
            Assert.Equal(typeof(DateTime), reader.GetFieldType(dateColIndex));
            
            // 模拟加载
            targetTable.Load(reader);

        }, bufferSize: 5, typeOverrides: schemaContract);

        sw.Stop();
        Console.WriteLine($"[ETL] Completed in {sw.Elapsed.TotalSeconds:F3}s. Rows written: {targetTable.Rows.Count}");

        // ---------------------------------------------------------
        // 4. [Verify] 验证结果
        // ---------------------------------------------------------
        
        // 验证行数：只保留了 US (偶数行)，应该是 50,000 行
        Assert.Equal(totalRows / 2, targetTable.Rows.Count);
        // Console.WriteLine(targetTable.Rows[0]["OrderDate"]);
        // 验证第一行 (OrderId 0)
        // 0 * 1.5 * 1.08 = 0
        Assert.Equal(0, targetTable.Rows[0]["OrderId"]);
        Assert.Equal(0.0, (double)targetTable.Rows[0]["TaxedAmount"], 4);
        Assert.Equal(baseDate,targetTable.Rows[0]["OrderDate"]);     

        // 验证最后一行 (OrderId 99998) -> 它是最后一个偶数
        // 99998 * 1.5 * 1.08 = 161996.76
        int lastId = 99998;
        Assert.Equal(lastId, targetTable.Rows[^1]["OrderId"]);
        
        double expectedAmount = lastId * 1.5 * 1.08;
        double actualAmount = (double)targetTable.Rows[^1]["TaxedAmount"];
        Assert.Equal(expectedAmount, actualAmount, 0.001); // 允许浮点微小误差

        // 验证列名 (确保 Select 生效)
        Assert.True(targetTable.Columns.Contains("TaxedAmount"));
        Assert.False(targetTable.Columns.Contains("Amount")); // 原列被排除了
        Assert.False(targetTable.Columns.Contains("Region")); // 原列被排除了
    }
    [Fact]
    public void Test_ScanDatabase_Factory_Reusability()
    {
        // 1. 准备源数据
        var table = new System.Data.DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Rows.Add(1);
        table.Rows.Add(2);

        // 2. 定义工厂
        // 每次调用都会生成一个新的 DataTableReader (状态重置)
        System.Data.IDataReader factory() => table.CreateDataReader();

        // 3. 创建 LazyFrame (Factory 模式)
        var lf1 = LazyFrame.ScanDatabase(factory);
        var lf2 = lf1.Clone();

        // 4. [第一次执行]
        // 这一步会调用 factory() -> 读完 -> dispose reader
        var df1 = lf1.Collect();
        Assert.Equal(2, df1.Height);
        Assert.Equal(1, df1[0, "Id"]);
        
        // 5. [第二次执行] - 关键点！
        // 如果我们传的是普通 reader，这里就会崩 (ObjectDisposedException 或 Empty)
        // 但因为是 factory，这里会再次调用 factory() -> 全新 reader
        var df2 = lf2.Collect();
        Assert.Equal(2, df2.Height);
        Assert.Equal(2, df2[1, "Id"]);
    }
    [Fact]
    public void Test_1_Extract_CSharpToPolars_NewTypes()
    {
        // ==========================================
        // 目标：验证 C# DataTable/DataReader 到 Polars 的 "读" 链路
        // 核心检查：Schema 生成是否正确，C# 装箱到 Arrow 是否崩溃
        // ==========================================

        int totalRows = 10; 
        var sourceTable = new System.Data.DataTable();
        sourceTable.Columns.Add("Id", typeof(int));
        sourceTable.Columns.Add("TimeVal", typeof(TimeOnly));  
        sourceTable.Columns.Add("TinyInt", typeof(sbyte));     
        sourceTable.Columns.Add("UnsignedBig", typeof(ulong)); 

        var startInfo = new TimeOnly(8, 0, 0); 

        for (int i = 0; i < totalRows; i++)
        {
            if (i == 5) 
            {
                sourceTable.Rows.Add(i, DBNull.Value, DBNull.Value, DBNull.Value);
                continue;
            }
            var time = startInfo.Add(TimeSpan.FromMinutes(i)).Add(TimeSpan.FromMilliseconds(i)); 
            sbyte tiny = (sbyte)(i * 10 - 50); 
            ulong ubig = (ulong)long.MaxValue + (ulong)i; 
            
            sourceTable.Rows.Add(i, time, tiny, ubig);
        }

        using var sourceReader = sourceTable.CreateDataReader();
        
        // 执行操作：直接扫描并立刻 Collect() 触发数据拉取！
        using var lf = LazyFrame.ScanDatabase(sourceReader, batchSize: 2);
        using var df = lf.Collect(); // 👑 如果 FFI 或者 Schema 出错，这里会直接炸！

        // 验证点：Polars 是否成功接收了 10 行数据
        Assert.Equal(10, df.Height);
        
    }
    [Fact]
    [Trait("Stream","NewType")]
    public void Test_2_Load_PolarsToCSharp_NewTypes()
    {
        // ==========================================
        // 目标：验证 Polars 到 C# 的 "写" 链路 (SinkTo)
        // 核心检查：多类型混合场景下的内存偏移、零装箱转换是否精准
        // ==========================================

        var sourceTable = new System.Data.DataTable();
        sourceTable.Columns.Add("Id", typeof(int));
        sourceTable.Columns.Add("TimeVal", typeof(TimeOnly));
        sourceTable.Columns.Add("UnsignedBig", typeof(ulong));
        // 👑 新增三员大将
        sourceTable.Columns.Add("TimeSpanVal", typeof(TimeSpan));
        sourceTable.Columns.Add("DecimalVal", typeof(decimal));
        sourceTable.Columns.Add("DateOnlyVal", typeof(DateOnly));
        
        // 准备测试数据
        var sampleTimeOnly = new TimeOnly(8, 0, 0);
        var sampleTimeSpan = new TimeSpan(1, 30, 45); // 1小时30分45秒
        var sampleDecimal = 123456.789m;
        var sampleDateOnly = new DateOnly(2024, 5, 20); // 2024-05-20

        sourceTable.Rows.Add(
            1, 
            sampleTimeOnly, 
            (ulong)long.MaxValue,
            sampleTimeSpan,
            sampleDecimal,
            sampleDateOnly
        );
        
        // 测试第二行的全 Null 兼容性
        sourceTable.Rows.Add(2, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);
        // sourceTable.Rows.Add(2, DBNull.Value,DBNull.Value,DBNull.Value);

        using var sourceReader = sourceTable.CreateDataReader();
        using var sourceReader2 = sourceTable.CreateDataReader();
        var df = DataFrame.ReadDatabase(sourceReader2);
        df.Show();
        using var lf = LazyFrame.ScanDatabase(sourceReader, batchSize: 2);

        // 2. 准备 Sink
        var targetTable = new System.Data.DataTable();

        // 👑 精确提供 Override，确保 Arrow 数据映射回 C# 时不走样
        var typeOverrides = new Dictionary<string, Type>
        {
            { "TimeVal", typeof(TimeOnly) }, 
            { "UnsignedBig", typeof(ulong) },
            { "TimeSpanVal", typeof(TimeSpan) },
            { "DecimalVal", typeof(decimal) },
            { "DateOnlyVal", typeof(DateOnly) }
        };

        // 执行操作
        lf.SinkTo(targetTable.Load, typeOverrides: typeOverrides); 
        // df.WriteTo(targetTable.Load,typeOverrides:typeOverrides);

        // 3. 硬核验证
        Assert.Equal(2, targetTable.Rows.Count);

        var row0 = targetTable.Rows[0];
        
        // 验证原有类型
        Assert.Equal(sampleTimeOnly, Assert.IsType<TimeOnly>(row0["TimeVal"]));
        Assert.Equal((ulong)long.MaxValue, Assert.IsType<ulong>(row0["UnsignedBig"]));

        // 👑 验证新增的三大类型是否被零拷贝/无损还原
        Assert.Equal(sampleTimeSpan, Assert.IsType<TimeSpan>(row0["TimeSpanVal"]));
        Assert.Equal(sampleDecimal, Assert.IsType<decimal>(row0["DecimalVal"]));
        Assert.Equal(sampleDateOnly, Assert.IsType<DateOnly>(row0["DateOnlyVal"]));

        // 简单验证 Null 行
        var row1 = targetTable.Rows[1];
        Assert.Equal(DBNull.Value, row1["DateOnlyVal"]);

        // Console.WriteLine("Test 2 (Write): Polars Arrow -> C# 强类型解析完全成功 (包含 TimeSpan/Decimal/DateOnly)！");
    }
    [Fact]
    [Trait("Stream", "Binary")]
    public void Test_3_BinaryType_Roundtrip()
    {
        // ==========================================
        // 目标：验证 byte[] (BLOB) 类型在 Polars 管道中的保真度
        // 核心检查：Polars 底层无论是 Binary, LargeBinary 还是 BinaryView，
        // 我们的 BinaryColumnBuilder 和 BinaryAccessor 能否完美抗住！
        // ==========================================

        var sourceTable = new System.Data.DataTable();
        sourceTable.Columns.Add("Id", typeof(int));
        sourceTable.Columns.Add("BlobVal", typeof(byte[]));
        
        // 准备测试数据
        // 1. 常规二进制流 (带 0x00 截断符和高位字节)
        byte[] sampleBytes = [0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0xFF];
        // 2. 极端情况：长度为 0 的空数组
        byte[] emptyBytes = [];

        sourceTable.Rows.Add(1, sampleBytes);
        sourceTable.Rows.Add(2, emptyBytes);
        // 3. 极端情况：数据库 Null
        sourceTable.Rows.Add(3, DBNull.Value);

        using var sourceReader = sourceTable.CreateDataReader();
        using var lf = LazyFrame.ScanDatabase(sourceReader, batchSize: 2);

        // 2. 准备 Sink
        var targetTable = new System.Data.DataTable();

        // 👑 显式告知期望反序列化成 byte[]
        // var typeOverrides = new Dictionary<string, Type>
        // {
        //     { "BlobVal", typeof(byte[]) }
        // };

        // 执行操作
        lf.SinkTo(targetTable.Load, typeOverrides: null); 

        // 3. 硬核验证
        Assert.Equal(3, targetTable.Rows.Count);

        // 验证第一行：常规数据
        var row0 = targetTable.Rows[0];
        var actualBytes = Assert.IsType<byte[]>(row0["BlobVal"]);
        Assert.Equal(sampleBytes, actualBytes); // xUnit 会自动按字节逐个比较数组内容

        // 验证第二行：空数组
        var row1 = targetTable.Rows[1];
        var actualEmpty = Assert.IsType<byte[]>(row1["BlobVal"]);
        Assert.Empty(actualEmpty);

        // 验证第三行：Null 值
        var row2 = targetTable.Rows[2];
        Assert.Equal(DBNull.Value, row2["BlobVal"]);

    }
    [Fact]
    [Trait("Category", "Debug")]
    public void Test_4_Guid_FixedSizeBinary_Roundtrip()
    {
        // ==========================================
        // 目标：验证 Guid (FixedSizeBinary(16)) 类型的端到端零拷贝传输
        // 核心检查：stackalloc 写入和 Span 原位读取是否无损且精准
        // ==========================================

        var sourceTable = new System.Data.DataTable();
        sourceTable.Columns.Add("Id", typeof(int));
        sourceTable.Columns.Add("GuidVal", typeof(Guid));
        
        // 准备测试数据
        var sampleGuid = Guid.NewGuid(); // 随机生成一个标准的 Guid
        var emptyGuid = Guid.Empty;      // 全 0 的 Guid (00000000-0000-0000-0000-000000000000)

        sourceTable.Rows.Add(1, sampleGuid);
        sourceTable.Rows.Add(2, emptyGuid);
        // 极端情况：Null
        sourceTable.Rows.Add(3, DBNull.Value);

        using var sourceReader = sourceTable.CreateDataReader();
        
        // 发射到 Polars
        using var lf = LazyFrame.ScanDatabase(sourceReader, batchSize: 2);
        // var df = DataFrame.ReadDatabase(sourceReader);
        // df.Show();
        // 准备 Sink
        var targetTable = new System.Data.DataTable();

        // 👑 显式告知 C# 期望反序列化成 Guid，触发 FixedSizeBinaryAccessor 里的 GetGuidFast 通道
        var typeOverrides = new Dictionary<string, Type>
        {
            { "GuidVal", typeof(Guid) }
        };

        // 执行操作
        lf.SinkTo(targetTable.Load, typeOverrides: typeOverrides); 

        // 👑 硬核验证
        Assert.Equal(3, targetTable.Rows.Count);

        // 验证第一行：常规 Guid
        var row0 = targetTable.Rows[0];
        var actualGuid = Assert.IsType<Guid>(row0["GuidVal"]);
        Assert.Equal(sampleGuid, actualGuid);

        // 验证第二行：Empty Guid
        var row1 = targetTable.Rows[1];
        var actualEmpty = Assert.IsType<Guid>(row1["GuidVal"]);
        Assert.Equal(emptyGuid, actualEmpty);

        // 验证第三行：Null 值
        var row2 = targetTable.Rows[2];
        Assert.Equal(DBNull.Value, row2["GuidVal"]);

    }
}
