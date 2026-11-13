using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using static Seseragi.Files.Tests.Parquet.HandBuiltParquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>Nested columns: projection by path, variants, and levels no writer produces (hand-built files).</summary>
public class ParquetNestedTests
{
    private static List<string> Rows(byte[] file, ParquetReadOptions? options = null)
    {
        using var parquet = ParquetFile.Open(new MemoryStream(file));
        using var reader = parquet.OpenReader(options);
        return Render(reader.ReadBatches());
    }

    private static List<string> Render(IEnumerable<RecordBatch> batches)
    {
        List<string> rows = [];
        foreach (RecordBatch b in batches)
        {
            for (int i = 0; i < b.Length; i++) rows.Add(string.Join(" | ", Enumerable.Range(0, b.ColumnCount).Select(c => CanonicalRender.Value(b.Column(c), i))));
            b.Dispose();
        }
        return rows;
    }

    [Fact]
    public void DefinitionLevelAboveTheMaximum_IsRefused()
    {
        SchemaElement[] schema = [Group("schema", 1, null), Group("s", 1), Leaf("x", ParquetType.Int32, FieldRepetition.Optional)];
        byte[] good = Build(schema, 2, new HandBuiltParquet.Column(["s", "x"], ParquetType.Int32, 2, 0, [2, 1], [], Int32s(7)));
        Assert.Equal(["{x:7}", "{x:null}"], Rows(good));
        byte[] bad = Build(schema, 2, new HandBuiltParquet.Column(["s", "x"], ParquetType.Int32, 2, 0, [2, 3], [], Int32s(7)));
        Assert.Contains("definition level 3 exceeds the maximum 2", Assert.Throws<ParquetFormatException>(() => Rows(bad)).Message);
    }

    [Fact]
    public void BitPackedDefinitionLevels_AreRead()
    {
        SchemaElement[] schema = [Group("schema", 1, null), Leaf("x", ParquetType.Int32, FieldRepetition.Optional)];
        byte[] file = Build(schema, 9, new HandBuiltParquet.Column(["x"], ParquetType.Int32, 1, 0, [1, 0, 1, 1, 0, 1, 1, 1, 1], [], Int32s(1, 2, 3, 4, 5, 6, 7),
            ParquetEncoding.BitPacked));
        Assert.Equal(["1", "null", "2", "3", "null", "4", "5", "6", "7"], Rows(file));
    }

    private static SchemaElement[] MapWithOptionalKeys =>
    [
        Group("schema", 1, null), Group("m", 1, FieldRepetition.Optional, ConvertedType.Map), Group("key_value", 2, FieldRepetition.Repeated),
        Leaf("key", ParquetType.ByteArray, FieldRepetition.Optional, ConvertedType.Utf8), Leaf("value", ParquetType.Int32, FieldRepetition.Optional),
    ];

    [Fact]
    public void MapKeys_ThatAreNull_AreRefused()
    {
        // m (1) → key_value (2) → key (3); a map of one entry, its value null.
        byte[] good = Build(MapWithOptionalKeys, 2,
            new HandBuiltParquet.Column(["m", "key_value", "key"], ParquetType.ByteArray, 3, 1, [3, 1], [0, 0], Strings("k")),
            new HandBuiltParquet.Column(["m", "key_value", "value"], ParquetType.Int32, 3, 1, [2, 1], [0, 0], []));
        Assert.Equal(["M{u:6b=>null}", "M{}"], Rows(good));
        byte[] bad = Build(MapWithOptionalKeys, 1,
            new HandBuiltParquet.Column(["m", "key_value", "key"], ParquetType.ByteArray, 3, 1, [2], [0], []),
            new HandBuiltParquet.Column(["m", "key_value", "value"], ParquetType.Int32, 3, 1, [3], [0], Int32s(5)));
        Assert.Contains("a map key is null", Assert.Throws<ParquetFormatException>(() => Rows(bad)).Message);
    }

    [Fact]
    public void LeavesThatDisagree_AreRefused()
    {
        // l (1) → list (2, repeated) → element (3) → a, b (4): a holds two elements of the first record, b one.
        SchemaElement[] schema =
        [
            Group("schema", 1, null), Group("l", 1, FieldRepetition.Optional, ConvertedType.List), Group("list", 1, FieldRepetition.Repeated),
            Group("element", 2), Leaf("a", ParquetType.Int32, FieldRepetition.Optional), Leaf("b", ParquetType.Int32, FieldRepetition.Optional),
        ];
        byte[] good = Build(schema, 1,
            new HandBuiltParquet.Column(["l", "list", "element", "a"], ParquetType.Int32, 4, 1, [4, 4], [0, 1], Int32s(1, 2)),
            new HandBuiltParquet.Column(["l", "list", "element", "b"], ParquetType.Int32, 4, 1, [4, 3], [0, 1], Int32s(3)));
        Assert.Equal(["[{a:1,b:3},{a:2,b:null}]"], Rows(good));
        byte[] bad = Build(schema, 1,
            new HandBuiltParquet.Column(["l", "list", "element", "a"], ParquetType.Int32, 4, 1, [4, 4], [0, 1], Int32s(1, 2)),
            new HandBuiltParquet.Column(["l", "list", "element", "b"], ParquetType.Int32, 4, 1, [4], [0], Int32s(3)));
        Assert.Contains("disagree", Assert.Throws<ParquetFormatException>(() => Rows(bad)).Message);
    }

    [Fact]
    public void FixedSizeLists_HoldPlaceholders_AndRefuseOtherLengths()
    {
        var list = new ParquetList("v", Repetition.Optional, new ParquetLeaf("element", Repetition.Optional, PhysicalType.Int32));
        Apache.Arrow.Schema stored = new([new Field("v", new FixedSizeListType(new Field("element", Int32Type.Default, true), 2), true)], null);
        byte[] Write(params object?[] rows) => ParquetArrowSchemaTests.WithArrowSchema(
            MinimalParquetWriter.Write([list], [.. rows.Select(r => (IReadOnlyDictionary<string, object?>)new Dictionary<string, object?> { ["v"] = r })]), stored);

        byte[] good = Write((List<object?>)[1, 2], null, (List<object?>)[null, 4]);
        using (var file = ParquetFile.Open(new MemoryStream(good)))
        {
            using var reader = file.OpenReader();
            RecordBatch batch = Assert.Single(reader.ReadBatches());
            var array = Assert.IsType<FixedSizeListArray>(batch.Column(0));
            Assert.Equal(6, array.Values.Length); // the null list holds two placeholders
            Assert.Equal(["[1,2]", "null", "[null,4]"], Enumerable.Range(0, 3).Select(i => CanonicalRender.Value(array, i)));
        }
        Assert.Contains("fixed-size list of 2", Assert.Throws<ParquetFormatException>(() => Rows(Write((List<object?>)[1, 2, 3]))).Message);
        Assert.Contains("fixed-size list of 2", Assert.Throws<ParquetFormatException>(() => Rows(Write((List<object?>)[]))).Message);
    }

    /// <summary>
    /// LIST backward-compatibility rules 4 and 5 (LogicalTypes.md): a repeated group of one field named <c>array</c>
    /// or <c>&lt;list&gt;_tuple</c> is itself the element (a struct); any other name is the three-level layout.
    /// </summary>
    [Theory]
    [InlineData("array", "list<array: struct<str: string not null> not null>", "[{str:u:61},{str:u:62}]")]
    [InlineData("my_list_tuple", "list<my_list_tuple: struct<str: string not null> not null>", "[{str:u:61},{str:u:62}]")]
    [InlineData("element", "list<str: string not null>", "[u:61,u:62]")]
    public void LegacyLists_FollowRules4And5(string repeatedName, string type, string row)
    {
        SchemaElement[] schema =
        [
            Group("schema", 1, null), Group("my_list", 1, FieldRepetition.Optional, ConvertedType.List),
            Group(repeatedName, 1, FieldRepetition.Repeated), Leaf("str", ParquetType.ByteArray, FieldRepetition.Required, ConvertedType.Utf8),
        ];
        byte[] file = Build(schema, 2, new HandBuiltParquet.Column(["my_list", repeatedName, "str"], ParquetType.ByteArray, 2, 1, [2, 2, 0], [0, 1, 0], Strings("a", "b")));
        using var parquet = ParquetFile.Open(new MemoryStream(file));
        Assert.Equal(type, ParquetValueTests.TypeName(parquet.Schema.FieldsList[0].DataType));
        Assert.Equal([row, "null"], Rows(file));
    }

    [Fact]
    public void EmptyGroups_AreRefused()
    {
        SchemaElement[] schema = [Group("schema", 2, null), Group("g", 0), Leaf("x", ParquetType.Int32, FieldRepetition.Required)];
        byte[] file = Build(schema, 1, new HandBuiltParquet.Column(["x"], ParquetType.Int32, 0, 0, [], [], Int32s(1)));
        using var parquet = ParquetFile.Open(new MemoryStream(file));
        Assert.Contains("has no fields", Assert.Throws<ParquetFormatException>(() => parquet.Schema).Message);
    }

    [Fact]
    public void WideFixedSizeLists_WithManyNulls_ReadWithinMemory()
    {
        // 1536 items per entry, every other entry null: placeholders for the nulls, scratch only for the slots.
        var list = new ParquetList("v", Repetition.Optional, new ParquetLeaf("element", Repetition.Required, PhysicalType.Float));
        Apache.Arrow.Schema stored = new([new Field("v", new FixedSizeListType(new Field("element", FloatType.Default, false), 1536), true)], null);
        List<object?> vector = [.. Enumerable.Range(0, 1536).Select(i => (object?)(float)i)];
        byte[] file = ParquetArrowSchemaTests.WithArrowSchema(MinimalParquetWriter.Write([list],
            [.. Enumerable.Range(0, 200).Select(r => (IReadOnlyDictionary<string, object?>)new Dictionary<string, object?> { ["v"] = r % 2 == 0 ? vector : null })]), stored);
        using var parquet = ParquetFile.Open(new MemoryStream(file));
        using var reader = parquet.OpenReader();
        RecordBatch batch = Assert.Single(reader.ReadBatches());
        var array = Assert.IsType<FixedSizeListArray>(batch.Column(0));
        Assert.Equal(200 * 1536, array.Values.Length);
        Assert.Equal(100, array.NullCount);
        Assert.Equal(1535f, ((FloatArray)array.Values).GetValue(2 * 1536 + 1535));
    }

    /// <summary>The checkpoint writer's own output (structs, lists, maps, nested lists) reads back as written.</summary>
    [Fact]
    public void CheckpointWriterOutput_ReadsBack()
    {
        List<string> rows = Rows(ParquetMinimalWriterTests.WriteSample());
        Assert.Equal(3, rows.Count);
        string[] first = rows[0].Split(" | ");
        Assert.Equal("true", first[0]);
        Assert.Equal("7", first[1]);
        Assert.Equal("u:68c3a96c6c6f", first[5]); // héllo
        Assert.Equal("{x:1,tags:[u:61,null,u:63],conf:M{u:6b=>u:76,u:6e=>null}}", first[15]);
        Assert.Equal("[[1,2],null,[]]", first[16]);
        Assert.Equal("{x:null,tags:[],conf:null}", rows[1].Split(" | ")[15]);
        Assert.Equal("null", rows[2].Split(" | ")[15]);
    }

    // ------------------------------------------------------------------
    // Projection by path
    // ------------------------------------------------------------------

    [Fact]
    public void ColumnPaths_PruneStructs_InCheckpoints()
    {
        string path = ParquetFixtures.FullPath("nested/delta_checkpoint_classic.parquet");
        using var file = ParquetFile.Open(path);
        using var full = file.OpenReader(new() { Columns = ["add", "protocol"] });
        List<RecordBatch> fullBatches = [.. full.ReadBatches()];
        using var pruned = file.OpenReader(new() { ColumnPaths = [["add", "path"], ["add", "deletionVector", "cardinality"]], Columns = ["protocol"] });
        Assert.Equal(["add", "protocol"], pruned.Schema.FieldsList.Select(f => f.Name));
        var add = (StructType)pruned.Schema.FieldsList[0].DataType;
        Assert.Equal(["path", "deletionVector"], add.Fields.Select(f => f.Name));
        Assert.Equal(["cardinality"], ((StructType)add.Fields[1].DataType).Fields.Select(f => f.Name));
        Assert.True(pruned.LeafCount < full.LeafCount);

        List<RecordBatch> prunedBatches = [.. pruned.ReadBatches()];
        List<string> expected = [], actual = [];
        foreach (RecordBatch b in fullBatches)
        {
            var a = (StructArray)b.Column(0);
            var type = (StructType)a.Data.DataType;
            var dv = (StructArray)a.Fields[type.GetFieldIndex("deletionVector")];
            var dvType = (StructType)dv.Data.DataType;
            for (int i = 0; i < b.Length; i++)
                expected.Add($"{a.IsNull(i)} {CanonicalRender.Value(a.Fields[type.GetFieldIndex("path")], i)} " +
                    $"{(dv.IsNull(i) ? "null" : CanonicalRender.Value(dv.Fields[dvType.GetFieldIndex("cardinality")], i))} {CanonicalRender.Value(b.Column(1), i)}");
        }
        foreach (RecordBatch b in prunedBatches)
        {
            var a = (StructArray)b.Column(0);
            var dv = (StructArray)a.Fields[1];
            for (int i = 0; i < b.Length; i++)
                actual.Add($"{a.IsNull(i)} {CanonicalRender.Value(a.Fields[0], i)} {(dv.IsNull(i) ? "null" : CanonicalRender.Value(dv.Fields[0], i))} {CanonicalRender.Value(b.Column(1), i)}");
        }
        Assert.Equal(expected, actual);
        Assert.Contains(actual, r => r.Contains("u:") && !r.StartsWith("True", StringComparison.Ordinal));
    }

    [Fact]
    public void ColumnPaths_GoThroughListsAndMaps()
    {
        using var file = ParquetFile.Open(ParquetFixtures.FullPath("nested/pa_nested_v1.parquet"));
        using var reader = file.OpenReader(new() { ColumnPaths = [["points", "x"], ["deep", "m", "value", "v"], ["map", "value"]] });
        Assert.Equal("list<element: struct<x: int32>>", ParquetValueTests.TypeName(reader.Schema.FieldsList[0].DataType));
        Assert.StartsWith("list<element: struct<m: map<string, list<element: struct<v: list<element: int16>>>>>>",
            ParquetValueTests.TypeName(reader.Schema.FieldsList.Single(f => f.Name == "deep").DataType), StringComparison.Ordinal);
        List<string> rows = Render(reader.ReadBatches());
        Assert.Equal(400, rows.Count);

        using var full = file.OpenReader(new() { Columns = ["points"] });
        List<string> points = Render(full.ReadBatches());
        string Strip(string row) => System.Text.RegularExpressions.Regex.Replace(row, @",y:[^,}]*,label:[^}]*", "");
        Assert.Equal(points.Select(Strip), rows.Select(r => r.Split(" | ")[0]));

        Assert.Throws<ArgumentException>(() => file.OpenReader(new() { ColumnPaths = [["points", "nope"]] }));
        Assert.Throws<ArgumentException>(() => file.OpenReader(new() { ColumnPaths = [["id", "below"]] }));
        Assert.Throws<ArgumentException>(() => file.OpenReader(new() { ColumnPaths = [["map", "entries"]] }));
        Assert.Throws<ArgumentException>(() => file.OpenReader(new() { ColumnPaths = [["nope"]] }));
    }

    [Fact]
    public void Variants_AreStructsMarkedAsVariant_ShreddedAsStored()
    {
        using var file = ParquetFile.Open(ParquetFixtures.FullPath("nested/spark_variant.parquet"));
        Field v = file.Schema.FieldsList.Single(f => f.Name == "v");
        Assert.Contains(v.Metadata, kv => kv is { Key: "ARROW:extension:name", Value: "arrow.parquet.variant" });
        Assert.Equal(["metadata", "value", "typed_value"], ((StructType)v.DataType).Fields.Select(f => f.Name));
    }
}
