namespace Polars.CSharp.Tests;

public class MetadataTests
{
    [Fact]
    public void Test_Series_DataTypeName()
    {
        // 1. Int64
        using var sInt = new Series("a", new long[] { 1, 2 });
        Assert.Equal("i64", sInt.DataTypeName);

        // 2. Decimal (自动推断 Scale)
        using var sDec = new Series("b", new Decimal[]{1.5m, 2.345m}); 
        // 1.5 -> 1.500, Scale=3. Polars 的 decimal 显示格式通常是 "decimal(precision, scale)"
        // 注意：具体字符串取决于 Polars 版本，通常包含 "decimal"
        Assert.Contains("decimal", sDec.DataTypeName); 
        Assert.Contains("3", sDec.DataTypeName); // 应该包含 scale 3

        // 3. String
        using var sStr = new Series("c", ["x", "y"]);
        // Polars 0.50+ 默认可能是 String (StringView)
        Assert.True(sStr.DataTypeName == "str" || sStr.DataTypeName == "String");
    }

    [Fact]
    public void Test_PrintSchema()
    {
        // 1. 准备数据
        var data = new[]
        {
            new { Id = 1, Name = "Alice", IsActive = true }
        };
        using var df = DataFrame.From(data);

        // 2. 捕获控制台输出
        using var sw = new System.IO.StringWriter();
        var originalOut = System.Console.Out;
        System.Console.SetOut(sw);

        try
        {
            // 3. 调用 PrintSchema
            df.PrintSchema();
        }
        finally
        {
            // 恢复控制台
            System.Console.SetOut(originalOut);
        }

        // 4. 验证输出内容
        var output = sw.ToString();
        
        // 应该包含 root 和列信息
        Assert.Contains("root", output);
        Assert.Contains("|-- Id: Int32", output);
        Assert.Contains("|-- Name: String", output);     // 或者 String
        Assert.Contains("|-- IsActive: Boolean", output); // 或者 Boolean
        
        System.Console.WriteLine(output);
    }
}