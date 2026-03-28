namespace Polars.CSharp.Tests;

public class MetadataTests
{
    [Fact]
    public void Test_Series_DataTypeName()
    {
        // Int64
        using var sInt = new Series("a", new long[] { 1, 2 });
        Assert.Equal("i64", sInt.DataTypeName);

        // Decimal 
        using var sDec = new Series("b", [1.5m, 2.345m]); 
        Assert.Contains("decimal", sDec.DataTypeName); 
        Assert.Contains("3", sDec.DataTypeName); 

        // String
        using var sStr = new Series("c", ["x", "y"]);
        Assert.True(sStr.DataTypeName == "str" || sStr.DataTypeName == "String");
    }

    [Fact]
    [Trait("Schema","Print")]
    public void Test_PrintSchema()
    {
        var data = new[]
        {
            new { Id = 1, Name = "Alice", IsActive = true }
        };
        using var df = DataFrame.From(data);

        using var sw = new StringWriter();
        var originalOut = Console.Out;
        Console.SetOut(sw);
        Console.WriteLine();
        try
        {
            df.PrintSchema();
        }
        finally
        {
            Console.SetOut(originalOut);
        }

        var output = sw.ToString();
        Console.WriteLine(output);
    
    }
}