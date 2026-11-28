using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// Datasets read by field id (<see cref="ParquetFieldMatching.ByFieldId"/>), as Iceberg tables are: columns renamed,
/// reordered, dropped and added again under an old name, promoted; struct fields renamed at any depth (in lists and
/// maps too); filters and statistics through the schema's names; missing ids; types only the stored Arrow schema
/// restores; files without ids read through a name mapping; the schema and options checked.
/// </summary>
public sealed class ParquetFieldIdTests
{
    private readonly MemoryFileStore _store = new();

    private static Field F(string name, IArrowType type, int? id) => ArrowFieldIds.With(new Field(name, type, true), id);

    private void Put(string path, Field[] fields, IArrowArray[] columns, int rowGroupRows = 1000)
    {
        var schema = new Apache.Arrow.Schema(fields, null);
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, new ParquetWriteOptions { RowGroupRows = rowGroupRows }))
        {
            writer.Write(new RecordBatch(schema, columns, columns[0].Length));
            writer.Complete();
        }
        _store.Add(path, stream.ToArray());
    }

    private ParquetDataset Open(Field[] schema, bool allowMissing = false, NameMapping? mapping = null, string path = "d/") =>
        ParquetDataset.Open(path, new ParquetDatasetOptions
        {
            Store = _store,
            Schema = new Apache.Arrow.Schema(schema, null),
            FieldMatching = ParquetFieldMatching.ByFieldId,
            AllowMissingColumns = allowMissing,
            NameMapping = mapping,
        });

    private static List<string> Read(ParquetDataset dataset, ParquetReadOptions? options = null)
    {
        using var reader = dataset.OpenReader(options);
        List<string> rows = [];
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch) rows.AddRange(ArrowRender.Rows(batch));
        }
        return rows;
    }

    private static Int32Array Ints(params int?[] values)
    {
        var b = new Int32Array.Builder();
        foreach (int? v in values) b.Append(v);
        return b.Build();
    }

    private static Int64Array Longs(params long?[] values)
    {
        var b = new Int64Array.Builder();
        foreach (long? v in values) b.Append(v);
        return b.Build();
    }

    private static StringArray Strings(params string?[] values)
    {
        var b = new StringArray.Builder();
        foreach (string? v in values) b.Append(v);
        return b.Build();
    }

    private static ScalarExpr Compare(BinaryOp op, string column, LiteralValue value) =>
        ScalarExpr.NewBinary(op, ScalarExpr.NewColumn(column), ScalarExpr.NewLiteral(value));

    [Fact]
    public void RenamedReorderedAndDroppedColumns_AreReadByTheirIds()
    {
        // Version 1: id (1), name (2), old (3). Version 2: key (1, now 64-bit), label (2, renamed), then "old" dropped
        // and a new "old" (4) added; written in another order.
        Put("d/a.parquet", [F("id", Int32Type.Default, 1), F("name", StringType.Default, 2), F("old", Int64Type.Default, 3)],
            [Ints(1, 2), Strings("a", "b"), Longs(100, 200)]);
        Put("d/b.parquet", [F("old", Int64Type.Default, 4), F("label", StringType.Default, 2), F("key", Int64Type.Default, 1)],
            [Longs(7), Strings("c"), Longs(3)]);
        Field[] schema = [F("key", Int64Type.Default, 1), F("label", StringType.Default, 2), F("old", Int64Type.Default, 4)];
        using ParquetDataset dataset = Open(schema, allowMissing: true);
        // The first file's "old" has another id: never the new column's values.
        Assert.Equal(["1 | \"a\" | null", "2 | \"b\" | null", "3 | \"c\" | 7"], Read(dataset));
        Assert.Equal(["\"a\"", "\"b\"", "\"c\""], Read(dataset, new ParquetReadOptions { Columns = ["label"] }));
    }

    [Fact]
    public void AnIdAFileLacks_IsAnError_UnlessMissingColumnsAreAllowed()
    {
        Put("d/a.parquet", [F("id", Int32Type.Default, 1)], [Ints(1)]);
        Field[] schema = [F("id", Int32Type.Default, 1), F("extra", StringType.Default, 9)];
        using ParquetDataset strict = Open(schema);
        var e = Assert.ThrowsAny<Exception>(() => Read(strict));
        Assert.Contains("d/a.parquet", e.Message);
        Assert.Contains("extra", e.Message);
        using ParquetDataset lenient = Open(schema, allowMissing: true);
        Assert.Equal(["1 | null"], Read(lenient));
    }

    [Fact]
    public void StructFields_AreMatchedByIdAtEveryDepth()
    {
        // pt (5): struct { x (6), y (7) }; items (8): list of struct { a (9) }; m (10): map string → struct { v (12) }.
        StructType Point(string x, string y) => new([F(x, Int32Type.Default, 6), F(y, Int32Type.Default, 7)]);
        StructType Item(string a) => new([F(a, Int32Type.Default, 9)]);
        StructType Value(string v) => new([F(v, Int32Type.Default, 12)]);
        Field[] Fields(string x, string y, string a, string v, string pt, string items, string m) =>
        [
            F(pt, Point(x, y), 5),
            F(items, new ListType(F("element", Item(a), 13)), 8),
            F(m, new MapType(F("key", StringType.Default, 14) is var k ? new Field(k.Name, k.DataType, false, k.Metadata) : null!, F("value", Value(v), 11)), 10),
        ];
        IArrowArray[] Columns(Field[] fields, int seed)
        {
            var pt = (StructType)fields[0].DataType;
            var point = new StructArray(pt, 1, [Ints(seed), Ints(seed + 1)], ArrowBuffer.Empty, 0);
            var itemType = (StructType)((ListType)fields[1].DataType).ValueDataType;
            var items = new ListArray(fields[1].DataType, 1, new ArrowBuffer.Builder<int>().Append(0).Append(2).Build(),
                new StructArray(itemType, 2, [Ints(seed + 2, seed + 3)], ArrowBuffer.Empty, 0), ArrowBuffer.Empty, 0);
            var mapType = (MapType)fields[2].DataType;
            var entries = new StructArray(mapType.KeyValueType, 1,
                [Strings("k"), new StructArray((StructType)mapType.ValueField.DataType, 1, [Ints(seed + 4)], ArrowBuffer.Empty, 0)], ArrowBuffer.Empty, 0);
            var map = new MapArray(mapType, 1, new ArrowBuffer.Builder<int>().Append(0).Append(1).Build(), entries, ArrowBuffer.Empty, 0);
            return [point, items, map];
        }
        Field[] v1 = Fields("x", "y", "a", "v", "pt", "items", "m"), v2 = Fields("lat", "lon", "b", "w", "point", "things", "props");
        Put("d/a.parquet", v1, Columns(v1, 10));
        Put("d/b.parquet", v2, Columns(v2, 20));
        Field[] schema = Fields("X", "Y", "A", "V", "P", "I", "M");
        using ParquetDataset dataset = Open(schema);
        Assert.Equal(
        [
            "{X: 10, Y: 11} | [{A: 12}, {A: 13}] | {\"k\": {V: 14}}",
            "{X: 20, Y: 21} | [{A: 22}, {A: 23}] | {\"k\": {V: 24}}",
        ], Read(dataset));
    }

    [Fact]
    public void AFilterOnARenamedColumn_PrunesByItsStatistics()
    {
        // Three row groups of 100 rows: 0..99, 100..199, 200..299.
        Put("d/a.parquet", [F("a", Int64Type.Default, 1)], [Longs([.. Enumerable.Range(0, 300).Select(i => (long?)i)])], rowGroupRows: 100);
        using ParquetDataset dataset = Open([F("b", Int64Type.Default, 1)]);
        ScalarExpr filter = Compare(BinaryOp.GtEq, "b", LiteralValue.NewInt64(250));
        // Only the last row group is read (its rows all delivered without row filtering), then the rows filtered.
        Assert.Equal(100, Read(dataset, new ParquetReadOptions { Filter = filter }).Count);
        Assert.Equal([.. Enumerable.Range(250, 50).Select(i => i.ToString())], Read(dataset, new ParquetReadOptions { Filter = filter, FilterRows = true }));
    }

    [Fact]
    public void TypesOnlyTheStoredArrowSchemaKnows_SurviveARename()
    {
        // A duration is an INT64 to Parquet: only the stored Arrow schema says it is one.
        var durations = new DurationArray.Builder(DurationType.Millisecond).Append(TimeSpan.FromSeconds(3)).Build();
        Put("d/a.parquet", [F("d", DurationType.Millisecond, 1)], [durations]);
        using ParquetDataset dataset = Open([F("wait", DurationType.Millisecond, 1)]);
        using var reader = dataset.OpenReader();
        using RecordBatch batch = reader.ReadBatches().Single();
        Assert.IsType<DurationType>(batch.Schema.FieldsList[0].DataType);
        Assert.Equal("wait", batch.Schema.FieldsList[0].Name);
        Assert.Equal(["3000"], ArrowRender.Rows(batch));
    }

    [Fact]
    public void FilesWithoutIds_TakeThemFromTheNameMapping()
    {
        Put("d/a.parquet", [F("id", Int32Type.Default, null), F("nm", StringType.Default, null), F("gone", StringType.Default, null),
            F("s", new StructType([F("q", Int32Type.Default, null)]), null)],
            [Ints(1), Strings("a"), Strings("z"), new StructArray(new StructType([F("q", Int32Type.Default, null)]), 1, [Ints(5)], ArrowBuffer.Empty, 0)]);
        NameMapping mapping = NameMapping.Parse("""
            [
              { "field-id": 1, "names": ["id"] },
              { "field-id": 2, "names": ["name", "nm"] },
              { "names": ["gone"] },
              { "field-id": 3, "names": ["s"], "fields": [ { "field-id": 4, "names": ["q"] } ] }
            ]
            """);
        Field[] schema = [F("key", Int32Type.Default, 1), F("label", StringType.Default, 2), F("st", new StructType([F("Q", Int32Type.Default, 4)]), 3)];
        using ParquetDataset dataset = Open(schema, mapping: mapping);
        Assert.Equal(["1 | \"a\" | {Q: 5}"], Read(dataset));

        using ParquetDataset unmapped = Open(schema);
        var e = Assert.Throws<ParquetFormatException>(() => Read(unmapped));
        Assert.Contains("d/a.parquet", e.Message);
        Assert.Contains("NameMapping", e.Message);
    }

    [Fact]
    public void PartitionColumns_NeedNoId()
    {
        Put("d/k=1/a.parquet", [F("a", Int32Type.Default, 1)], [Ints(5)]);
        using ParquetDataset dataset = Open([F("k", Int64Type.Default, null), F("b", Int32Type.Default, 1)]);
        Assert.Equal(["1 | 5"], Read(dataset));
    }

    [Fact]
    public void TheSchemaAndOptions_AreChecked()
    {
        Put("d/a.parquet", [F("a", Int32Type.Default, 1)], [Ints(5)]);
        var noId = Assert.Throws<ArgumentException>(() => Open([F("a", Int32Type.Default, 1), F("s", new StructType([F("x", Int32Type.Default, null)]), 2)]));
        Assert.Contains("'s.x'", noId.Message);
        var twice = Assert.Throws<ArgumentException>(() => Open([F("a", Int32Type.Default, 1), F("b", Int32Type.Default, 1)]));
        Assert.Contains("field id 1", twice.Message);
        Assert.Throws<ArgumentException>(() => ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = _store, FieldMatching = ParquetFieldMatching.ByFieldId }));
        Assert.Throws<ArgumentException>(() => ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = _store, NameMapping = new NameMapping([]) }));
    }

    [Fact]
    public void ANameMapping_ParsesIcebergJson_AndRefusesOther()
    {
        NameMapping mapping = NameMapping.Parse("""[{"field-id":1,"names":["a","b"],"fields":[{"field-id":2,"names":["c"]}]},{"field-id":null,"names":["d"]}]""");
        Assert.Equal(2, mapping.Fields.Count);
        Assert.Equal(["a", "b"], mapping.Fields[0].Names);
        Assert.Equal(2, mapping.Fields[0].Fields![0].FieldId);
        Assert.Null(mapping.Fields[1].FieldId);
        Assert.Throws<FormatException>(() => NameMapping.Parse("{}"));
        Assert.Throws<FormatException>(() => NameMapping.Parse("""[{"field-id":"x","names":[]}]"""));
        Assert.Throws<FormatException>(() => NameMapping.Parse("""[{"field-id":1}]"""));
        Assert.Throws<FormatException>(() => NameMapping.Parse("[1,"));
    }
}
