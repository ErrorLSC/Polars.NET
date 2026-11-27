#nullable enable
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// The Map namespace (<c>MapOps</c>) and <c>List.ToMap</c>, against what Python Polars 2.2 gives on the same data
/// (TestData/map, make_map_fixtures.py).
/// </summary>
public class MapOpsTests
{
    private static string Fixture(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "map", name);

    // basic.arrow: {a: 1, b: 2}, null, {}, {c: null}
    [Fact]
    public void Entries_Keys_Values_And_Len()
    {
        using var df = DataFrame.ReadIpc(Fixture("basic.arrow"));
        using var res = df.Select(
            Col("m").MapOps.Entries().Alias("entries"),
            Col("m").MapOps.Keys().Alias("keys"),
            Col("m").MapOps.Values().Alias("values"),
            Col("m").MapOps.Len().Alias("len"));

        Assert.Equal(DataType.List(DataType.Struct(("key", DataType.String), ("value", DataType.Int64))), res.Schema["entries"]);
        Assert.Equal(DataType.List(DataType.String), res.Schema["keys"]);
        Assert.Equal(DataType.List(DataType.Int64), res.Schema["values"]);
        Assert.Equal(DataType.UInt32, res.Schema["len"]);

        Assert.Equal([["a", "b"], null, [], ["c"]], res["keys"].ToArray<string[]?>());
        Assert.Equal([[1L, 2L], null, [], [null]], res["values"].ToArray<long?[]?>());
        Assert.Equal([2u, null, 0u, 1u], res["len"].ToArray<uint?>());
        Assert.Equal([2, 0, 0, 1], res["entries"].ToArray<KeyValue[]?>().Select(e => e?.Length ?? 0));
        Assert.Equal(new KeyValue("b", 2), res["entries"].ToArray<KeyValue[]?>()[0]![1]);
    }

    [Fact]
    public void ContainsKey_And_Get_With_A_Literal_Key()
    {
        using var df = DataFrame.ReadIpc(Fixture("basic.arrow"));
        using var res = df.Select(
            Col("m").MapOps.ContainsKey("a").Alias("has_a"),
            Col("m").MapOps.Get("a").Alias("a"),
            Col("m").MapOps.Get("c").Alias("c"));

        Assert.Equal([true, null, false, false], res["has_a"].ToArray<bool?>());
        Assert.Equal([1L, null, null, null], res["a"].ToArray<long?>());
        // A key present with a null value, a missing key and a null map all read null.
        Assert.Equal([null, null, null, null], res["c"].ToArray<long?>());
    }

    [Fact]
    public void Keys_From_A_Column_Are_Looked_Up_Row_By_Row()
    {
        using var df = DataFrame.ReadIpc(Fixture("basic.arrow"))
            .WithColumns(Lit(Series.From("k", ["b", "x", "a", "c"])).Alias("k"));
        using var res = df.Select(Col("m").MapOps.Get(Col("k")).Alias("g"), Col("m").MapOps.ContainsKey(Col("k")).Alias("h"));

        Assert.Equal([2L, null, null, null], res["g"].ToArray<long?>());
        Assert.Equal([true, null, false, true], res["h"].ToArray<bool?>());
    }

    [Fact]
    public void A_Repeated_Key_Is_Found_At_Its_First_Entry()
    {
        // duplicates.arrow: [x: 1, y: 2, x: 5], [z: 9]
        using var df = DataFrame.ReadIpc(Fixture("duplicates.arrow"));
        using var res = df.Select(Col("m").MapOps.Get("x").Alias("x"), Col("m").MapOps.Len().Alias("len"), Col("m").MapOps.Keys().Alias("keys"));

        Assert.Equal([1L, null], res["x"].ToArray<long?>());
        Assert.Equal([3u, 1u], res["len"].ToArray<uint>());
        Assert.Equal(["x", "y", "x"], res["keys"].GetValue<string[]>(0)!);
    }

    [Fact]
    public void Nested_Values_Come_Out_With_Their_Types()
    {
        using var df = DataFrame.ReadIpc(Fixture("nested.arrow"));
        using var res = df.Select(
            Col("mm").MapOps.Get("outer").Alias("o"),
            Col("ms").MapOps.Get(1).Alias("one"),
            Col("ml").MapOps.Get("p").Alias("p"));

        Assert.Equal(DataType.Map(DataType.String, DataType.Int64), res.Schema["o"]);
        Assert.Equal(new Dictionary<string, long> { ["inner"] = 7 }, res["o"].GetValue<Dictionary<string, long>>(0));
        Assert.Null(res["o"].GetValue<Dictionary<string, long>>(1));
        Assert.Equal(new UV("one", 1.5), res["one"].GetValue<UV>(0));
        Assert.Equal([1L, 2L], res["p"].GetValue<long[]>(0)!);
    }

    [Fact]
    public void An_Integer_Keyed_Map_Takes_Integer_Literals()
    {
        using var df = DataFrame.FromRows([new Numbered(new() { [1] = "one", [2] = "two" })]);
        using var res = df.Select(Col("M").MapOps.Get(2).Alias("two"), Col("M").MapOps.ContainsKey(3L).Alias("three"));

        Assert.Equal("two", res["two"].GetValue<string>(0));
        Assert.False(res["three"].GetValue<bool>(0));
    }

    [Fact]
    public void List_ToMap_Keeps_The_First_Position_And_The_Last_Value()
    {
        using var s = Series.From("l", new KeyValue[]?[] { [new("x", 1), new("y", 2), new("x", 5)], null, [] })
            .Cast(DataType.List(DataType.Struct(("key", DataType.String), ("value", DataType.Int64))));
        using var map = s.List.ToMap();

        Assert.Equal(DataType.Map(DataType.String, DataType.Int64), map.DataType);
        Assert.Equal(["x", "y"], map.MapOps.Keys().GetValue<string[]>(0)!);
        Assert.Equal(new Dictionary<string, long> { ["x"] = 5, ["y"] = 2 }, map.GetValue<Dictionary<string, long>>(0));
        Assert.Null(map.GetValue<Dictionary<string, long>>(1));
        Assert.Empty(map.GetValue<Dictionary<string, long>>(2)!);
    }

    [Fact]
    public void List_ToMap_Refuses_Other_Field_Names_And_Null_Keys()
    {
        using var named = Series.From("l", new Named[][] { [new("a", 1)] });
        Assert.Contains("named `key` and `value`", Assert.ThrowsAny<Exception>(() => named.List.ToMap()).Message);

        using var nullKey = Series.From("l", new KeyValue[][] { [new(null!, 1)] })
            .Cast(DataType.List(DataType.Struct(("key", DataType.String), ("value", DataType.Int64))));
        Assert.Contains("cannot be null", Assert.ThrowsAny<Exception>(() => nullKey.List.ToMap()).Message);
    }

    [Fact]
    public void Map_Operations_On_Other_Types_Are_Refused()
    {
        using var df = DataFrame.FromColumns(new { a = new[] { 1L } });
        Assert.Contains("requires a Map dtype", Assert.ThrowsAny<Exception>(() => df.Select(Col("a").MapOps.Len())).Message);
    }

    [Fact]
    public void Series_Namespace_And_Streaming_Engine_Agree()
    {
        using var df = DataFrame.ReadIpc(Fixture("basic.arrow"));
        using var series = df["m"];

        Assert.Equal([1L, null, null, null], series.MapOps.Get("a").ToArray<long?>());
        Assert.Equal([2u, null, 0u, 1u], series.MapOps.Len().ToArray<uint?>());
        Assert.Equal([true, null, false, false], series.MapOps.ContainsKey("b").ToArray<bool?>());
        Assert.Equal([["a", "b"], null, [], ["c"]], series.MapOps.Keys().ToArray<string[]?>());

        using var streamed = df.Lazy().Select(Col("m").MapOps.Get("a")).Collect(Engine.Streaming);
        Assert.Equal([1L, null, null, null], streamed["m"].ToArray<long?>());
    }

    public sealed record KeyValue(string key, long? value);
    public sealed record Named(string k, long v);
    public sealed record UV(string? u, double? v);
    public sealed record Numbered(Dictionary<long, string> M);
}
