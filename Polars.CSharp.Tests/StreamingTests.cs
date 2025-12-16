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
    private IEnumerable<BigDataPoco> GenerateData(int count)
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
        using var df = DataFrame.FromArrowStream(GenerateData(totalRows), batchSize);

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
}