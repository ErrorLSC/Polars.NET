using System.Reflection.Metadata.Ecma335;

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
        int totalRows = 50_000;
        int batchSize = 10_000; // 5 个 Batch

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
        using var df1 = q.Clone().Collect();

        // --- 验证 1: 数据完整性 (验证 PrependEnumerator 是否工作) ---
        // 过滤后应该剩 25000 行
        Assert.Equal(totalRows / 2, df1.Height);

        // 检查第一行 (Id=0)。如果 Prepend 逻辑坏了，Id=0 这一批次可能会丢失。
        Assert.Equal(0, df1.GetValue<int>(0, "Id"));
        Assert.Equal(0 * 1.5 * 2, df1.GetValue<double>(0, "DoubleValue")); // 0

        // 检查最后一行 (Id=49998)
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
        var lf = LazyFrame.ScanArrowStream(new List<StreamPoco>(), batchSize: 100);
        using var df = lf.Collect();
        
        Assert.Equal(0, df.Height);
        // Schema 应该依然存在 (Id, Group, Value)
        Assert.Contains("Id", df.ColumnNames);
    }
}