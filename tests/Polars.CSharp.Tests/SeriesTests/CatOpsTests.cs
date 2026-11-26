using System.Runtime.InteropServices.Marshalling;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class SeriesCategoricalOpsTests
{
    [Fact]
    [Trait("Series", "Categorical")]
    public void Test_Series_Cat_Ops()
    {
        string[] data = [
            "apple",
            "banana",
            "波拉熊", 
            "apple",   
            null
        ];

        using Series sStr = Pl.CreateSeries("cat_data", data);
        using Series s = sStr.Cast(DataType.Categorical("name"));

        using Series categories = s.Cat.GetCategories();
        Assert.Equal(DataType.String, categories.DataType);
        Assert.Equal(["apple", "banana", "波拉熊"], categories.ToArray<string>());

        using Series lenChars = s.Cat.LenChars();

        Assert.Equal([5u, 6u, 3u, 5u, null], lenChars.ToArray<uint?>());

        using Series lenBytes = s.Cat.LenBytes();
        Assert.Equal([5u, 6u, 9u, 5u, null], lenBytes.ToArray<uint?>());

        using Series startsWith = s.Cat.StartsWith("app");
        Assert.Equal([true, false, false, true, null], startsWith.ToArray<bool?>());

        using Series endsWith = s.Cat.EndsWith("熊");
        Assert.Equal([false, false, true, false, null], endsWith.ToArray<bool?>());
    }
    [Fact]
    [Trait("Series", "Categorical")]
    public void Cat_GetCategories_Gives_The_Categories_Once_On_The_Streaming_Engine()
    {
        // Many morsels of one column: the categories come once, not once per morsel.
        string[] names = ["apple", "banana", "波拉熊"];
        using Series text = Pl.CreateSeries("c", Enumerable.Range(0, 400_000).Select(i => names[i % 3]).ToArray());
        using Series categorical = text.Cast(DataType.Categorical("fruit"));
        using var df = DataFrame.FromSeries(categorical);
        using var categories = df.Lazy().Select(Pl.Col("c").Cat.GetCategories()).Collect();
        Assert.Equal(names, categories["c"].ToArray<string>());
    }

    [Fact]
    [Trait("Series", "Categorical")]
    public void Test_Series_Cat_To_Physical()
    {
        string[] data = [
            "apple",
            "banana",
            "波拉熊", 
            "apple",   
            null
        ];

        using Series sStr = Pl.CreateSeries("cat_data", data);
        using Series s = sStr.Cast(DataType.Categorical("name"));

        using var resPhyical = s.Cat.Physical();
        Assert.Equal([0u,1u,2u,0u,null],resPhyical.ToArray<uint?>());

        Assert.Equal(s,resPhyical.Cat.To(DataType.Categorical("name")));

    }
}