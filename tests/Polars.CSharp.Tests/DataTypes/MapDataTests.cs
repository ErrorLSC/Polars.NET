#nullable enable
using System.Collections;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// .NET dictionaries in and out of Map columns: rows, series, literals, value reads, duplicates (first wins), refused
/// keys, and casts between Map and its entries.
/// </summary>
public class MapDataTests
{
    private static string Fixture(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "map", name);

    [Fact]
    public void Rows_With_Dictionaries_Make_Map_Columns_And_Read_Back()
    {
        Tagged[] rows = [new(1, new() { ["a"] = 1, ["b"] = 2 }), new(2, null), new(3, [])];
        using var df = DataFrame.FromRows(rows);

        Assert.Equal(DataType.Map(DataType.String, DataType.Int64), df.Schema["Tags"]);
        Assert.Equal(1, df["Tags"].NullCount);

        List<Tagged> back = [];
        foreach (var row in df.Rows<Tagged>()) back.Add(row);
        Assert.Equal(new Dictionary<string, long> { ["a"] = 1, ["b"] = 2 }, back[0].Tags);
        Assert.Null(back[1].Tags);
        Assert.Empty(back[2].Tags!);

        List<int?> counts = [];
        foreach (var row in df.Rows<TaggedReadOnly>()) counts.Add(row.Tags?.Count);
        Assert.Equal([2, null, 0], counts);
    }

    [Fact]
    public void Schema_From_Type_Matches_The_Data_Even_Without_Rows()
    {
        using var schema = PolarsSchema.From<Tagged>();
        using var df = DataFrame.FromRows(new List<Tagged>());
        using var one = DataFrame.FromRows([new Tagged(1, new() { ["x"] = 1 })]);

        Assert.Equal(schema["Tags"], df.Schema["Tags"]);
        Assert.Equal(schema["Tags"], one.Schema["Tags"]);
    }

    [Fact]
    public void Series_Of_Dictionaries_Read_As_Dictionaries()
    {
        using var s = Series.From("m", new Dictionary<string, long>?[] { new() { ["x"] = 1, ["y"] = 2 }, null, [] });

        Assert.Equal("map[str, i64]", s.DataType.ToString());
        Assert.Equal(new Dictionary<string, long> { ["x"] = 1, ["y"] = 2 }, s.GetValue<Dictionary<string, long>>(0));
        Assert.Null(s.GetValue<Dictionary<string, long>>(1));
        Assert.Empty(s.GetValue<IReadOnlyDictionary<string, long>>(2)!);

        var all = s.ToArray<IReadOnlyDictionary<string, long>?>();
        Assert.Equal(3, all.Length);
        Assert.Equal(2, all[0]!.Count);
        Assert.Null(all[1]);

        // The boxed indexer reads a map as a dictionary.
        Assert.Equal(new Dictionary<string, long> { ["x"] = 1, ["y"] = 2 }, Assert.IsType<Dictionary<string, long>>(s[0]));
    }

    [Fact]
    public void Keys_And_Values_Of_Other_Types()
    {
        using var df = DataFrame.FromRows([new Keyed(1, new() { [1] = "a", [2] = null }, new() { [Color.Green] = 1.5 })]);

        Assert.Equal(DataType.Map(DataType.Int32, DataType.String), df.Schema["Names"]);
        Assert.Equal(DataTypeKind.Enum, df.Schema["Weights"].MapKeyType!.Kind);

        foreach (var row in df.Rows<Keyed>())
        {
            Assert.Equal("a", row.Names[1]);
            Assert.Null(row.Names[2]);
            Assert.Equal(1.5, row.Weights[Color.Green]);
        }
    }

    [Fact]
    public void Enum_Keyed_Maps_Survive_A_Spooled_Stream()
    {
        // A sink over a .NET stream spools it to a temporary IPC file with categoricals as strings, then restores the
        // dtypes: here the key of a map.
        // One row per batch, each with other keys: every batch brings its own dictionary.
        Keyed[] rows = [new(1, new() { [1] = "a" }, new() { [Color.Green] = 1.5 }), new(2, [], [ ]), new(3, [], new() { [Color.Red] = 2 })];
        string dir = Directory.CreateTempSubdirectory("map-spool").FullName;
        try
        {
            // Collected without a sink, the stream is read in memory (its maps converted, not reinterpreted).
            using (var collected = LazyFrame.ScanEnumerable(rows).Collect())
                Assert.Equal(rows[0].Names, collected["Names"].GetValue<Dictionary<int, string?>>(0));

            string path = Path.Combine(dir, "keyed.arrow");
            LazyFrame.ScanEnumerable(rows, batchSize: 1).SinkIpc(path);
            using var df = DataFrame.ReadIpc(path);

            Assert.Equal(DataTypeKind.Enum, df.Schema["Weights"].MapKeyType!.Kind);
            List<Keyed> back = [];
            foreach (var row in df.Rows<Keyed>()) back.Add(row);
            Assert.Equal(rows[0].Weights, back[0].Weights);
            Assert.Empty(back[1].Weights);
            Assert.Equal(rows[2].Weights, back[2].Weights);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Categorical_Keyed_Maps_Survive_A_Spooled_Stream()
    {
        // Two batches, each with its own categorical dictionary for the map keys (an IPC file holds one per field):
        // the spool writes the maps as entries with string keys and the scan casts them back.
        static DataFrame Batch(string key, long value)
        {
            using var df = DataFrame.FromRows([new Tagged(1, new() { [key] = value })]);
            return df.Select(Col("Tags").MapOps.Entries()
                .Cast(DataType.List(DataType.Struct(("key", DataType.Categorical()), ("value", DataType.Int64))))
                .List.ToMap().Alias("Tags"));
        }
        using var first = Batch("a", 1);
        using var second = Batch("b", 2);
        Assert.Equal(DataTypeKind.Categorical, first.Schema["Tags"].MapKeyType!.Kind);
        using var one = first.ToArrow();
        using var two = second.ToArrow();

        string dir = Directory.CreateTempSubdirectory("map-spool").FullName;
        try
        {
            string path = Path.Combine(dir, "categorical.arrow");
            LazyFrame.ScanRecordBatches([one, two], one.Schema).SinkIpc(path);
            using var df = DataFrame.ReadIpc(path);

            Assert.Equal(DataTypeKind.Categorical, df.Schema["Tags"].MapKeyType!.Kind);
            Assert.Equal([new Dictionary<string, long> { ["a"] = 1 }, new() { ["b"] = 2 }], df["Tags"].ToArray<Dictionary<string, long>>());
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void A_Repeated_Key_Reads_As_Its_First_Value()
    {
        // pyarrow wrote [x: 1, y: 2, x: 5]; Arrow IPC keeps every entry, a .NET dictionary takes the first, as a lookup does.
        using var df = DataFrame.ReadIpc(Fixture("duplicates.arrow"));

        Assert.Equal(1, df["m"].GetValue<Dictionary<string, long>>(0)!["x"]);
        foreach (var row in df.Rows<Duplicated>())
        {
            Assert.Equal(row.id == 1 ? new Dictionary<string, long> { ["x"] = 1, ["y"] = 2 } : new() { ["z"] = 9 }, row.m);
            break;
        }
    }

    [Fact]
    public void The_Engines_Parquet_Reader_Keeps_One_Entry_Per_Key()
    {
        // Polars' own Parquet reader de-duplicates maps: the first key's position, the last value.
        using var df = DataFrame.ReadParquet(Fixture("duplicates.parquet"));
        Assert.Equal(new Dictionary<string, long> { ["x"] = 5, ["y"] = 2 }, df["m"].GetValue<Dictionary<string, long>>(0));
    }

    [Fact]
    public void Nested_Keys_Are_Refused_As_Dictionary_Keys()
    {
        using var s = Series.From("m", new[] { new Dictionary<Point, int> { [new Point(1, 2)] = 3 } });
        Assert.Equal("map[struct[X: i32, Y: i32], i32]", s.DataType.ToString());

        var error = Assert.Throws<NotSupportedException>(() => s.GetValue<Dictionary<Point, int>>(0));
        Assert.Contains("nested keys", error.Message);
        Assert.Contains("Entries()", error.Message);
    }

    [Fact]
    public void A_Null_Key_Is_Refused_When_Writing()
    {
        var error = Assert.Throws<ArgumentException>(() => Series.From("m", new[] { new NullKeyed() }));
        Assert.Contains("null key", error.Message);
    }

    [Fact]
    public void A_Dictionary_Literal_Is_One_Map_Value()
    {
        using var df = DataFrame.FromColumns(new { Id = new[] { 1, 2 } });
        using var res = df.Select(Col("Id"), Lit(new Dictionary<string, long> { ["k"] = 5 }).Alias("m"));

        Assert.Equal(DataType.Map(DataType.String, DataType.Int64), res.Schema["m"]);
        Assert.All(res["m"].ToArray<Dictionary<string, long>>(), m => Assert.Equal(new Dictionary<string, long> { ["k"] = 5 }, m));
    }

    [Fact]
    public void Maps_Cast_To_Their_Entries_And_Back()
    {
        using var df = DataFrame.FromRows([new Tagged(1, new() { ["a"] = 1 }), new Tagged(2, null)]);
        static DataType Entries() => DataType.List(DataType.Struct(("key", DataType.String), ("value", DataType.Int64)));

        using var entries = df.Select(Col("Tags").Cast(Entries()));
        Assert.Equal(Entries(), entries.Schema["Tags"]);

        using var back = entries.Select(Col("Tags").Cast(DataType.Map(DataType.String, DataType.Int64)));
        Assert.Equal(DataType.Map(DataType.String, DataType.Int64), back.Schema["Tags"]);
        Assert.Equal(new Dictionary<string, long> { ["a"] = 1 }, back["Tags"].GetValue<Dictionary<string, long>>(0));
        Assert.Null(back["Tags"].GetValue<Dictionary<string, long>>(1));

        // Values cast within a map; keys do not change type.
        using var wider = df.Select(Col("Tags").Cast(DataType.Map(DataType.String, DataType.Float64)));
        Assert.Equal(1.0, wider["Tags"].GetValue<Dictionary<string, double>>(0)!["a"]);
        Assert.ThrowsAny<Exception>(() => df.Select(Col("Tags").Cast(DataType.Map(DataType.Int64, DataType.Int64))));
    }

    public enum Color { Red, Green }
    public sealed record Tagged(int Id, Dictionary<string, long>? Tags);
    public sealed record TaggedReadOnly(int Id, IReadOnlyDictionary<string, long>? Tags);
    public sealed record Keyed(int Id, Dictionary<int, string?> Names, Dictionary<Color, double> Weights);
    public sealed record Duplicated(long id, Dictionary<string, long> m);
    public sealed record Point(int X, int Y);

    /// <summary>A dictionary that hands out a null key (a .NET Dictionary cannot hold one; a custom one can).</summary>
    private sealed class NullKeyed : IReadOnlyDictionary<string, int>
    {
        private readonly KeyValuePair<string, int>[] _entries = [new(null!, 1)];
        public int this[string key] => throw new NotSupportedException();
        public IEnumerable<string> Keys => _entries.Select(e => e.Key);
        public IEnumerable<int> Values => _entries.Select(e => e.Value);
        public int Count => _entries.Length;
        public bool ContainsKey(string key) => false;
        public bool TryGetValue(string key, out int value) { value = 0; return false; }
        public IEnumerator<KeyValuePair<string, int>> GetEnumerator() => ((IEnumerable<KeyValuePair<string, int>>)_entries).GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
