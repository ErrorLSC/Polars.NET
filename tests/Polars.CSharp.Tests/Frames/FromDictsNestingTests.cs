#nullable enable

namespace Polars.CSharp.Tests;

/// <summary>
/// Nested dictionaries in <see cref="DataFrame.FromDicts"/> rows, as Python Polars 2.2's <c>from_dicts</c> takes them:
/// a dictionary of object values is a record (a struct of its keys), a typed dictionary is a map, and a schema that
/// makes a column a map builds the records as maps of their values' type.
/// </summary>
public class FromDictsNestingTests
{
    private static List<IDictionary<string, object?>> Rows(params object?[] values) =>
        [.. values.Select((v, i) => (IDictionary<string, object?>)new Dictionary<string, object?> { ["id"] = (long)i + 1, ["m"] = v })];

    private static Dictionary<string, object?> Record(params (string Key, object? Value)[] entries) =>
        entries.ToDictionary(static e => e.Key, static e => e.Value);

    [Fact]
    public void Records_Become_Structs_Of_Their_Keys()
    {
        using var df = DataFrame.FromDicts(Rows(Record(("a", 1L), ("b", 2L)), Record(("c", 3L)), null));

        Assert.Equal(DataType.Struct(("a", DataType.Int64), ("b", DataType.Int64), ("c", DataType.Int64)), df.Schema["m"]);
        Assert.Equal(1, df["m"].NullCount);
        Assert.Equal([1L, null, null], df["m"].Struct.Field("a").ToArray<long?>());
        Assert.Equal([null, 3L, null], df["m"].Struct.Field("c").ToArray<long?>());
    }

    [Fact]
    public void Record_Fields_Are_Promoted_As_Columns_Are()
    {
        using var numbers = DataFrame.FromDicts(Rows(Record(("a", 1L)), Record(("a", 2.5))));
        Assert.Equal(DataType.Struct(("a", DataType.Float64)), numbers.Schema["m"]);
        Assert.Equal([1.0, 2.5], numbers["m"].Struct.Field("a").ToArray<double>());

        using var mixed = DataFrame.FromDicts(Rows(Record(("a", 1L)), Record(("a", "x"))));
        Assert.Equal(DataType.Struct(("a", DataType.String)), mixed.Schema["m"]);
        Assert.Equal(["1", "x"], mixed["m"].Struct.Field("a").ToArray<string>());
    }

    [Fact]
    public void Records_Nest()
    {
        using var df = DataFrame.FromDicts(Rows(Record(("a", Record(("b", 1L)))), Record(("a", null))));

        Assert.Equal(DataType.Struct(("a", DataType.Struct(("b", DataType.Int64)))), df.Schema["m"]);
        Assert.Equal([1L, null], df["m"].Struct.Field("a").Struct.Field("b").ToArray<long?>());
        Assert.Equal(1, df["m"].Struct.Field("a").NullCount);
    }

    [Fact]
    public void Records_Without_Keys_Are_A_Struct_Of_No_Fields()
    {
        using var df = DataFrame.FromDicts(Rows(Record(), null));
        Assert.Equal(DataTypeKind.Struct, df.Schema["m"].Kind);
        Assert.Empty(df.Schema["m"].StructFields!);
        Assert.Equal(2, df.Height);
    }

    [Fact]
    public void Typed_Dictionaries_Stay_Maps()
    {
        using var df = DataFrame.FromDicts(Rows(new Dictionary<string, long> { ["a"] = 1 }, null));
        Assert.Equal(DataType.Map(DataType.String, DataType.Int64), df.Schema["m"]);
        Assert.Equal(new Dictionary<string, long> { ["a"] = 1 }, df["m"].GetValue<Dictionary<string, long>>(0));
    }

    [Fact]
    public void A_Map_In_The_Schema_Builds_The_Records_As_Maps_Of_Their_Values_Type()
    {
        using var schema = PolarsSchema.From([("id", DataType.Int64), ("m", DataType.Map(DataType.String, DataType.Int64))]);
        using var df = DataFrame.FromDicts(Rows(Record(("a", 1L), ("b", 2L)), Record(("c", 3L)), null), schema: schema);
        Assert.Equal(DataType.Map(DataType.String, DataType.Int64), df.Schema["m"]);
        Assert.Equal([new Dictionary<string, long> { ["a"] = 1, ["b"] = 2 }, new() { ["c"] = 3 }, null], df["m"].ToArray<Dictionary<string, long>?>());

        // Dates stay dates (no text in between), numbers are promoted, as columns are.
        using var dates = DataFrame.FromDicts(Rows(Record(("a", new DateOnly(2024, 1, 2)))),
            schemaOverrides: PolarsSchema.From([("m", DataType.Map(DataType.String, DataType.Date))]));
        Assert.Equal(new Dictionary<string, DateOnly> { ["a"] = new(2024, 1, 2) }, dates["m"].GetValue<Dictionary<string, DateOnly>>(0));
        using var mixed = DataFrame.FromDicts(Rows(Record(("a", 1L)), Record(("b", 2.5))),
            schemaOverrides: PolarsSchema.From([("m", DataType.Map(DataType.String, DataType.Float64))]));
        Assert.Equal([new Dictionary<string, double> { ["a"] = 1 }, new() { ["b"] = 2.5 }], mixed["m"].ToArray<Dictionary<string, double>>());
    }

    [Fact]
    public void A_Map_Of_Records_Asks_For_Typed_Dictionaries()
    {
        var error = Assert.Throws<NotSupportedException>(() => DataFrame.FromDicts(Rows(Record(("a", Record(("x", 1L))))),
            schemaOverrides: PolarsSchema.From([("m", DataType.Map(DataType.String, DataType.Struct(("x", DataType.Int64))))])));
        Assert.Contains("'m'", error.Message);
        Assert.Contains("typed dictionaries", error.Message);
    }

    [Fact]
    public void Maps_Round_Trip_Through_ToDicts()
    {
        using var source = DataFrame.FromRows([new Tagged(1, new() { ["a"] = 1 }), new Tagged(2, null)]);
        using var back = DataFrame.FromDicts(source.ToDicts().Select(static d => (IDictionary<string, object?>)d));

        Assert.Equal(source.Schema["Tags"], back.Schema["Tags"]);
        Assert.True(back.Equals(source));
    }

    public sealed record Tagged(int Id, Dictionary<string, long>? Tags);
}
