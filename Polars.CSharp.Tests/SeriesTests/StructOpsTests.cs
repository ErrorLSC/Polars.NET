using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class SeriesStructOpsTests
{
    [Fact]
    [Trait("Series", "StructFunctions")]
    public void Test_Series_Struct_WithFields()
    {
        // Row 0: [1, 2] -> { "a": 1, "b": 2 }
        // Row 1: [3, 4] -> { "a": 3, "b": 4 }
        int[,] data = { { 1, 2 }, { 3, 4 } };
        using Series arrSeries = Pl.CreateSeries("array", data);
        using Series structSeries = arrSeries.Array.ToStruct(["a", "b"]);

        using Series newStructSeries = structSeries.Struct.WithFields(
            Pl.Col("array").Struct.Field("a") * 10,           
            Pl.Lit(99).Alias("c")       
        );
        newStructSeries.Show();
        using DataFrame unnestedDf = newStructSeries.Struct.Unnest();

        Assert.Equal(3, unnestedDf.Width);
        Assert.True(unnestedDf.Columns.Contains("a"));
        Assert.True(unnestedDf.Columns.Contains("b"));
        Assert.True(unnestedDf.Columns.Contains("c"));

        Assert.Equal([10, 30], unnestedDf["a"].ToArray<int>());

        Assert.Equal([2, 4], unnestedDf["b"].ToArray<int>());

        Assert.Equal([99, 99], unnestedDf["c"].ToArray<int>());
    }
    [Fact]
    [Trait("Series", "StructFunctions")]
    public void Test_Series_Struct_Properties_And_Indexers()
    {
        // Row 0: { "a": 10, "b": 20, "c": 30 }
        // Row 1: { "a": 40, "b": 50, "c": 60 }
        int[,] data = { { 10, 20, 30 }, { 40, 50, 60 } };
        using Series arrSeries = Pl.CreateSeries("array", data);
        using Series structSeries = arrSeries.Array.ToStruct(["a", "b", "c"]);

        string[] fields = structSeries.Struct.Fields;
        Assert.Equal(["a", "b", "c"], fields);
        var schema = structSeries.Struct.Schema;
        Assert.True(schema.ContainsKey("a"));
        Assert.True(schema.ContainsKey("b"));
        Assert.True(schema.ContainsKey("c"));
        Assert.Equal(Pl.Int32, schema["a"]);

        using Series colA = structSeries.Struct["a"];
        Assert.Equal("a", colA.Name);
        Assert.Equal([10, 40], colA.ToArray<int>());

        using Series colC = structSeries.Struct[2]; 
        Assert.Equal("c", colC.Name);
        Assert.Equal([30, 60], colC.ToArray<int>());

        using Series subStruct = structSeries.Struct[["c", "a"]];

        Assert.Equal(["c", "a"], subStruct.Struct.Fields);
        
        using DataFrame unnested = subStruct.Struct.Unnest();
        Assert.Equal(2, unnested.Width);
        Assert.Equal([30, 60], unnested["c"].ToArray<int>());
        Assert.Equal([10, 40], unnested["a"].ToArray<int>());
    }
    [Fact]
    [Trait("Series", "StructDrop")]
    public void Test_Series_Struct_Drop()
    {
        // Row 0: { "a": 10, "b": 20, "c": 30 }
        // Row 1: { "a": 40, "b": 50, "c": 60 }
        int[,] data = { { 10, 20, 30 }, { 40, 50, 60 } };
        using Series arrSeries = Pl.CreateSeries("array", data);
        using Series structSeries = arrSeries.Array.ToStruct(["a", "b", "c"]);

        Series dropped = structSeries.Struct.Drop(["a","c"]);
        Assert.Equal(1,dropped.Unnest().Width);
    }
}