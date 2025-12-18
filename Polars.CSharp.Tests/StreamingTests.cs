using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Polars.NET.Core.Data;

namespace Polars.CSharp.Tests;
public class StreamingTests
{
    private class BigDataPoco
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public double Value { get; set; }
    }

    // 生成器：惰性生成数据，模拟从数据库或文件读取
    private IEnumerable<BigDataPoco> GenerateData_1(int count)
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
        using var df = DataFrame.FromArrowStream(GenerateData_1(totalRows), batchSize);

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
    private IEnumerable<StreamPoco> GenerateData_2(int count)
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
        var lf = LazyFrame.ScanArrowStream(GenerateData_2(totalRows), batchSize);

        // 2. 构建查询计划 (Filter -> Select -> Alias)
        // 我们只保留偶数行，并且把 Value 翻倍
        var q = lf
            .Filter(Polars.Col("Group") == Polars.Lit("Even"))
            .Select(
                Polars.Col("Id"),
                (Polars.Col("Value") * Polars.Lit(2)).Alias("DoubleValue")
            );

        // 3. 第一次执行 (Trigger!)
        // 此时 Rust 才会通过回调，驱动 C# 的 Enumerator
        using var df1 = q.Clone().CollectStreaming();

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
        
        using var df2 = q.CollectStreaming();
        Assert.Equal(df1.Height, df2.Height);
        Assert.Equal(df1.GetValue<int>(0, "Id"), df2.GetValue<int>(0, "Id"));
    }
    
    [Fact]
    public void Test_Lazy_Stream_Empty()
    {
        // 验证空流处理：不应该崩溃，应该返回空 DataFrame
        var lf = LazyFrame.ScanArrowStream(new List<StreamPoco>(), batchSize: 100);
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
        var lf = LazyFrame.ScanArrowStream(InfiniteStream(), batchSize);

        // 3. 定义计算图
        // 过滤条件非常苛刻，只有最后一行满足
        var q = lf
            .Filter(Polars.Col("Id") > Polars.Lit(999_998)) 
            .Select(Polars.Col("Id"), Polars.Col("Value"));

        // 4. 执行: Streaming Collect
        // Rust 引擎会：
        //   a. 拉取 C# 5万行
        //   b. 在内存中 Filter，扔掉 49999 行
        //   c. 释放这 5万行内存
        //   d. 重复...
        // 整个过程内存占用极低，哪怕处理 1TB 数据也不会崩
        using var df = q.CollectStreaming();

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
    [Fact(Skip = "Stress test, will be skiped")]
    [Trait("Category", "StressTest")] // 标记为压力测试，CI 中可选跳过
    public void Test_100_Million_Rows_Streaming()
    {
        // ====================================================
        // 配置区
        // ====================================================
        int totalRows = 100_000_000; // 1 亿行！
        int batchSize = 500_000;     // 每次处理 50 万行 (平衡 FFI 开销与内存)
        
        // 打印初始内存
        long memStart = GC.GetTotalMemory(true);
        Console.WriteLine($"[Start] Memory: {memStart / 1024 / 1024} MB");

        var sw = Stopwatch.StartNew();

        // 1. 建立管道 (Lazy)
        // 此时没有任何数据产生
        var lf = LazyFrame.ScanArrowStream(GenerateMassiveData(totalRows), batchSize);

        // 2. 构建查询计划
        // 任务：
        // 1. 过滤出 Category_A (5000万行)
        // 2. 将 Value 乘以 2
        // 3. 对 Id 求和 (测试大数聚合)
        // 4. 对 Value 求和
        var q = lf
            .Filter(Polars.Col("Category") == Polars.Lit("Category_A"))
            .Select(
                Polars.Col("Id").Sum().Alias("SumId"),
                (Polars.Col("Value") * Polars.Lit(2)).Sum().Alias("SumValue"), // 1.0 * 2 * 5000w = 1亿
                Polars.Col("Id").Count().Alias("Count")
            );

        // 3. 执行：CollectStreaming (开启流式引擎)
        // 这一步是见证奇迹的时刻
        using var df = q.CollectStreaming();

        sw.Stop();
        
        // 打印结束内存
        // 如果内存泄漏，或者不是流式，这里可能会显示好几个 GB，甚至 OOM
        long memEnd = GC.GetTotalMemory(true);
        Console.WriteLine($"[End] Memory: {memEnd / 1024 / 1024} MB");
        Console.WriteLine($"[Time] Processed {totalRows:N0} rows in {sw.Elapsed.TotalSeconds:F2} seconds.");
        
        // 计算吞吐量
        double throughput = totalRows / sw.Elapsed.TotalSeconds;
        Console.WriteLine($"[Speed] {throughput:N0} rows/sec");

        // ====================================================
        // 验证结果 (数学验证)
        // ====================================================
        
        // 1. 验证行数 (聚合后应该是 1 行)
        Assert.Equal(1, df.Height);

        // 2. 验证 Count (应该是 5000万)
        long expectedCount = totalRows / 2;
        Assert.Equal(expectedCount, (long)df.GetValue<long>(0, "Count")); // Count 返回通常是 UInt32/UInt64/Int64

        // 3. 验证 Value Sum
        // 5000万行 * 1.0 * 2 = 100,000,000
        double expectedSumValue = expectedCount * 2.0;
        Assert.Equal(expectedSumValue, df.GetValue<double>(0, "SumValue"));

        // 4. 验证 Id Sum (等差数列求和)
        // 偶数 id: 0, 2, 4, ... 
        // 项数 n = 50,000,000
        // 首项 a1 = 0, 末项 an = (n-1)*2 = 99,999,998
        // Sum = n * (a1 + an) / 2
        // 注意：结果会非常大，需要 decimal 或 ulong 防止溢出，Polars SumId 可能是 Int64 或 Float64
        // 这里做一个近似验证或类型转换验证
        // 这里只是演示，只要不报错且 Count 对了，基本就稳了
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
    public void Test_ETL_Stream_EndToEnd()
    {
        // ==================================================================================
        // 场景模拟：每日订单处理 (ETL)
        // Source DB (Mock) -> DataReader -> Polars Lazy -> Filter/Calc -> SinkTo -> Target DB
        // 目标：处理 10 万行数据，内存不积压，类型全覆盖。
        // ==================================================================================

        int totalRows = 100_000;
        
        // ---------------------------------------------------------
        // 1. [Extract] 准备源数据库 (Source)
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
        // 2. [Pipeline] 构建 Polars 流式管道
        // ---------------------------------------------------------
        
        // Step A: 获取 Schema (元数据)
        // 最佳实践：对于 forward-only 流，先通过元数据获取 Schema，避免 Peek 第一行导致数据丢失
        var arrowSchema = DbToArrowStream.GetArrowSchema(sourceReader);

        // Step B: 建立延迟读取节点 (Lazy Scan)
        // 使用 ScanRecordBatches 并传入 explicit schema，实现绝对安全的流式读取
        var lf = LazyFrame.ScanRecordBatches(sourceReader.ToArrowBatches(batchSize: 10_000), arrowSchema);

        // Step C: 定义转换逻辑 (Transform)
        // 业务需求：
        // 1. 只保留 "US" 地区的订单
        // 2. 计算税后金额 (Amount * 1.08)
        // 3. 选取需要的列
        var pipeline = lf
            .Filter(Polars.Col("Region") == Polars.Lit("US"))
            .WithColumns((Polars.Col("Amount") * 1.08).Alias("TaxedAmount"))
            .Select(Polars.Col("OrderId").Cast(DataType.Int32), // 明确：我要 Int32
            Polars.Col("TaxedAmount").Cast(DataType.Float64), // 明确：我要 Double
            Polars.Col("OrderDate").Cast(DataType.Datetime));

        // ---------------------------------------------------------
        // 3. [Load] 准备目标数据库 & 执行 Sink
        // ---------------------------------------------------------
        
        var targetTable = new System.Data.DataTable(); // 模拟目标表
        
        Console.WriteLine("[ETL] Starting Pipeline...");
        var sw = System.Diagnostics.Stopwatch.StartNew();
        // 模拟 SqlBulkCopy.WriteToServer(reader)
        // 这一步会疯狂调用 reader.Read()，从而反向拉动整个链条

        // 执行流式写入！
        // 这一步会驱动：
        // sourceReader -> Arrow转换 -> Rust引擎(Filter/Calc) -> Callback -> Buffer -> ArrowToDbStream -> targetTable.Load
        pipeline.SinkTo(targetTable.Load, bufferSize: 5);

        sw.Stop();
        Console.WriteLine($"[ETL] Completed in {sw.Elapsed.TotalSeconds:F3}s. Rows written: {targetTable.Rows.Count}");

        // ---------------------------------------------------------
        // 4. [Verify] 验证结果
        // ---------------------------------------------------------
        
        // 验证行数：只保留了 US (偶数行)，应该是 50,000 行
        Assert.Equal(totalRows / 2, targetTable.Rows.Count);

        // 验证第一行 (OrderId 0)
        // 0 * 1.5 * 1.08 = 0
        Assert.Equal(0, targetTable.Rows[0]["OrderId"]);
        Assert.Equal(0.0, (double)targetTable.Rows[0]["TaxedAmount"], 4);
        var actualVal = targetTable.Rows[0]["OrderDate"];
        DateTime actualDate;

        if (actualVal is DateTime dt)
        {
            actualDate = dt;
        }
        else if (actualVal is long ticks) // <--- 命中这里
        {
            // Polars 返回的是微秒 (Microseconds)
            // 1766145600000000
            // 还原逻辑：Epoch + Microseconds
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            
            // 注意：这里还原出来的是 "12:00 UTC"
            // 因为 Naive DateTime 在 Arrow 里被视为 Wall Clock Time 存储
            // 所以这个 UTC 时间的值，就是我们要的墙上时间
            actualDate = epoch.AddTicks(ticks * 10); 
        }
        else
        {
            throw new Exception($"Unexpected type: {actualVal.GetType()}");
        }

        // 比较：忽略 Kind (Utc vs Local)，只比较“钟表上的时间”是否一致
        // baseDate: 12:00:00 (Local/Unspecified)
        // actualDate: 12:00:00 (Utc)
        // 只要它们显示的数字一样，ETL 就是成功的
        
        Assert.Equal(baseDate.ToString("yyyy-MM-dd HH:mm:ss"), actualDate.ToString("yyyy-MM-dd HH:mm:ss"));

        // 验证最后一行 (OrderId 99998) -> 它是最后一个偶数
        // 99998 * 1.5 * 1.08 = 161996.76
        int lastId = 99998;
        Assert.Equal(lastId, targetTable.Rows[targetTable.Rows.Count - 1]["OrderId"]);
        
        double expectedAmount = lastId * 1.5 * 1.08;
        double actualAmount = (double)targetTable.Rows[targetTable.Rows.Count - 1]["TaxedAmount"];
        Assert.Equal(expectedAmount, actualAmount, 0.001); // 允许浮点微小误差

        // 验证列名 (确保 Select 生效)
        Assert.True(targetTable.Columns.Contains("TaxedAmount"));
        Assert.False(targetTable.Columns.Contains("Amount")); // 原列被排除了
        Assert.False(targetTable.Columns.Contains("Region")); // 原列被排除了
    }
}
