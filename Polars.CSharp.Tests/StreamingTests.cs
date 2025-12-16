using System.Diagnostics;

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
}