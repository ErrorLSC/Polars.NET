using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class SeriesArrayOpsTests
{
    [Fact]
    [Trait("Series","ArrayToStruct")]
    public void Test_Series_Struct_Unnest()
    {
        // Row 0: [1, 2]
        // Row 1: [3, 4]
        int[,] data = { {1, 2}, {3, 4} };
        Series arrSeries = Pl.Series("array",data);
        Series lenSeries = arrSeries.Array.Len();

        Assert.Equal([2u,2u],lenSeries.ToArray<uint>());
        // ToStruct() -> Unnest()
        using Series structSeries = arrSeries.Array.ToStruct(["nihao","chifan"]);
        using Series nihao = structSeries.Struct.Field("nihao");
        Assert.Equal("nihao", nihao.Name);
        Assert.Equal([1,3],nihao.ToArray<int>());
        using DataFrame unnestedDf = structSeries.Struct.Unnest();

        Assert.Equal(2, unnestedDf.Width);
        Assert.True(unnestedDf.Columns.Contains("nihao"));
        Assert.True(unnestedDf.Columns.Contains("chifan"));
    }
    [Fact]
    [Trait("Series", "ArrayToStruct")]
    public void Test_Series_Struct_Unnest_With_NameGenerator()
    {
        // Row 0: [10, 20, 30]
        // Row 1: [40, 50, 60]
        int[,] data = { { 10, 20, 30 }, { 40, 50, 60 } };
        Series arrSeries = Pl.Series("array", data);

        // "field_index_0", "field_index_1", "field_index_2"
        using Series structSeries = arrSeries.Array.ToStruct(i => $"field_index_{i}", 3);
        using DataFrame unnestedDf = structSeries.Struct.Unnest();

        Assert.Equal(3, unnestedDf.Width);
        
        Assert.True(unnestedDf.Columns.Contains("field_index_0"));
        Assert.True(unnestedDf.Columns.Contains("field_index_1"));
        Assert.True(unnestedDf.Columns.Contains("field_index_2"));

        using Series col1 = unnestedDf["field_index_1"];
        Assert.Equal([20, 50], col1.ToArray<int>());
    }
    [Fact]
    [Trait("Series", "ArrayFunctions")]
    public void Test_Series_Array_Extended_APIs()
    {
        // Row 0: [1, 1, 2]
        // Row 1: [3, 4, 4]
        int[,] data = { { 1, 1, 2 }, { 3, 4, 4 } };
        using Series arrSeries = Pl.Series("array", data);

        using Series nUnique = arrSeries.Array.NUnique();

        Assert.Equal([2u, 2u], nUnique.ToArray<uint>()); 

        using Series matchCounts = arrSeries.Array.CountMatches(1);
        // Row 0: -> 2
        // Row 1: -> 0
        Assert.Equal([2u, 0u], matchCounts.ToArray<uint>());

        using Series aggSeries = arrSeries.Array.Agg(Pl.Element().Sum());
        // Row 0: 1 + 1 + 2 = 4
        // Row 1: 3 + 4 + 4 = 11
        Assert.Equal([4, 11], aggSeries.ToArray<int>());

        // [[10, 10, 20], [30, 40, 40]]
        using Series evalSeries = arrSeries.Array.Eval(Pl.Element() * 10);
 
        using Series firstElements = evalSeries.Array.First();
        using Series lastElements = evalSeries.Array.Last();
        
        // First() : [10, 30]
        Assert.Equal("array", firstElements.Name); 
        Assert.Equal([10, 30], firstElements.ToArray<int>());
        
        // Last() : [20, 40]
        Assert.Equal([20, 40], lastElements.ToArray<int>());
    }
}