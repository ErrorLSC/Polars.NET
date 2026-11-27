using Apache.Arrow.Types;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp.Tests;

/// <summary>
/// The Map type itself (Polars 2.2 <c>map[key, value]</c>): construction, introspection, display, Arrow and .NET type
/// mapping, selectors, and Map columns as Python Polars 2.2 writes them (TestData/map, make_map_fixtures.py).
/// </summary>
public class MapTypeTests
{
    private static string Fixture(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "map", name);

    [Fact]
    public void Map_Type_Has_Its_Kind_Parts_And_Display()
    {
        using var map = DataType.Map(DataType.String, DataType.Int64);

        Assert.Equal(DataTypeKind.Map, map.Kind);
        Assert.Equal("map[str, i64]", map.ToString());
        Assert.Equal(DataType.String, map.MapKeyType);
        Assert.Equal(DataType.Int64, map.MapValueType);
        Assert.True(map.IsNested);
        Assert.Equal(DataType.Map(DataType.String, DataType.Int64), map);
        Assert.NotEqual(DataType.Map(DataType.String, DataType.Int32), map);
        Assert.NotEqual(DataType.List(DataType.Struct(("key", DataType.String), ("value", DataType.Int64))), map);

        Assert.Null(DataType.Int64.MapKeyType);
        Assert.Null(DataType.List(DataType.Int64).MapValueType);
    }

    [Fact]
    public void Nested_Map_Types_Display_Like_Polars()
    {
        var entries = DataType.Map(DataType.String, DataType.Int64);
        Assert.Equal("list[map[str, i64]]", DataType.List(entries).ToString());
        Assert.Equal("map[str, map[str, i64]]", DataType.Map(DataType.String, entries).ToString());
        Assert.Equal("map[i32, struct[u: str, v: f64]]",
            DataType.Map(DataType.Int32, DataType.Struct(("u", DataType.String), ("v", DataType.Float64))).ToString());
        Assert.Equal(DataType.Map(DataType.String, entries).MapValueType, entries);
    }

    [Fact]
    public void A_Null_Key_Type_Is_Refused()
    {
        var error = Assert.ThrowsAny<Exception>(() => DataType.Map(DataType.Null, DataType.Int64));
        Assert.Contains("Map key", error.Message);
        // A null value type is fine (a map of nulls).
        Assert.Equal("map[str, null]", DataType.Map(DataType.String, DataType.Null).ToString());
    }

    [Theory]
    [InlineData("basic.parquet")]
    [InlineData("basic.arrow")]
    public void Python_Map_Columns_Read_As_Map(string file)
    {
        using var df = file.EndsWith(".parquet") ? DataFrame.ReadParquet(Fixture(file)) : DataFrame.ReadIpc(Fixture(file));

        DataType m = df.Schema["m"];
        Assert.Equal(DataTypeKind.Map, m.Kind);
        Assert.Equal(DataType.Map(DataType.String, DataType.Int64), m);
        Assert.Equal("map[str, i64]", df["m"].DataType.ToString());
    }

    [Fact]
    public void Nested_Map_Shapes_Keep_Their_Types()
    {
        using var df = DataFrame.ReadIpc(Fixture("nested.arrow"));
        using var entries = DataType.Map(DataType.String, DataType.Int64);

        Assert.Equal(DataType.List(entries), df.Schema["lm"]);
        Assert.Equal(DataType.Struct(("name", DataType.String), ("m", entries)), df.Schema["sm"]);
        Assert.Equal(DataType.Map(DataType.String, DataType.List(DataType.Int64)), df.Schema["ml"]);
        // The struct above left `entries` usable.
        Assert.Equal(DataType.Map(DataType.String, entries), df.Schema["mm"]);
        Assert.Equal(DataType.Map(DataType.Int32, DataType.Struct(("u", DataType.String), ("v", DataType.Float64))), df.Schema["ms"]);

        Assert.Equal(DataType.Int64, df.Schema["mm"].MapValueType!.MapValueType);
        Assert.Equal(DataTypeKind.Map, df.Schema["lm"].InnerType!.Kind);
        Assert.Equal(DataTypeKind.Map, df.Schema["sm"].StructFields![1].DataType.Kind);
    }

    [Fact]
    public void Selectors_Pick_Map_Columns()
    {
        using var df = DataFrame.ReadIpc(Fixture("nested.arrow"));

        Assert.Equal(["ml", "mm", "ms"], df.Select(Cs.Map()).Columns);
        Assert.Equal(["ml", "mm", "ms"], df.Select(Cs.ByDtype(DataType.Map(DataType.String, DataType.Int64))).Columns);
        Assert.Equal(["lm", "sm", "ml", "mm", "ms"], df.Select(Cs.Nested()).Columns);
    }

    [Fact]
    public void Arrow_And_Net_Map_Types_Become_Map()
    {
        var arrow = new MapType(StringType.Default, Int64Type.Default);
        Assert.Equal(DataType.Map(DataType.String, DataType.Int64), DataType.FromArrowType(arrow));
        Assert.Equal(DataType.Map(DataType.String, DataType.Int64), DataType.FromNetType<Dictionary<string, long>>());
        Assert.Equal(DataType.Map(DataType.Int32, DataType.List(DataType.String)), DataType.FromNetType<IReadOnlyDictionary<int, string[]>>());

        using var schema = PolarsSchema.From<Tagged>();
        Assert.Equal(DataType.Map(DataType.String, DataType.Int64), schema["Tags"]);
        Assert.Equal(DataType.Map(DataType.String, DataType.Map(DataType.Int32, DataType.Float64)), schema["Nested"]);
    }

    private sealed record Tagged(int Id, Dictionary<string, long> Tags, Dictionary<string, Dictionary<int, double>> Nested);
}
