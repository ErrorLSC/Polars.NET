using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Schema;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// <see cref="FooterInterner"/> and <see cref="SchemaShape"/>: the files of a dataset whose footers hold the same schema
/// bytes share their schema elements, and with the same stored Arrow schema one shape (mapped, selected and planned
/// once); files that differ in anything the mapping reads do not.
/// </summary>
public sealed class ParquetSchemaShapeTests
{
    private static byte[] File(Field[] fields, IArrowArray[] columns, ParquetWriteOptions? options = null)
    {
        var schema = new Apache.Arrow.Schema(fields, null);
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, options ?? new ParquetWriteOptions()))
        {
            writer.Write(new RecordBatch(schema, columns, columns[0].Length));
            writer.Complete();
        }
        return stream.ToArray();
    }

    private static byte[] Footer(byte[] file)
    {
        var (offset, length) = ParquetMetadata.FooterRange(file, file.Length, "file");
        return file.AsSpan((int)offset, length).ToArray();
    }

    private static Int64Array Longs(params long[] values) => new Int64Array.Builder().AppendRange(values).Build();

    private static StringArray Strings(params string[] values) => new StringArray.Builder().AppendRange(values).Build();

    private static byte[] XS(long x, string s, bool required = false) =>
        File([new Field("x", Int64Type.Default, !required), new Field("s", StringType.Default, true)], [Longs(x), Strings(s)]);

    [Fact]
    public void FootersOfOneSchema_ShareTheirElementsAndShape_AndReadAsWithoutSharing()
    {
        var interner = new FooterInterner();
        byte[] a = Footer(XS(1, "a")), b = Footer(XS(2, "bb"));
        FileMetaData first = FileMetaData.Parse(a, interner), second = FileMetaData.Parse(b, interner);
        Assert.Same(first.Schema, second.Schema);
        Assert.Same(first.KeyValueMetadata, second.KeyValueMetadata);
        Assert.Same(first.Shape, second.Shape);
        Assert.Equal((1, 1, 1), interner.Counts);

        // What is read is what a reader of its own reads.
        FileMetaData alone = FileMetaData.Parse(b);
        Assert.Null(alone.Shape);
        Assert.Equal(alone.Schema.Select(e => (e.Name, e.Type, e.Repetition, e.NumChildren)), second.Schema.Select(e => (e.Name, e.Type, e.Repetition, e.NumChildren)));
        Assert.Equal(alone.KeyValueMetadata!, second.KeyValueMetadata!);
        Assert.Equal(alone.RowGroups.Single().NumRows, second.RowGroups.Single().NumRows);
        Assert.Equal(SchemaShape.Of(alone).Mapping(new ParquetReadOptions()).Schema.ToString(), second.Shape!.Mapping(new ParquetReadOptions()).Schema.ToString());
        Assert.Same(second.Shape.PublicSchema, ParquetMetadata.ParseFooter(a, interner).Schema);
    }

    [Fact]
    public void FootersOfOtherSchemas_DoNotShare()
    {
        var interner = new FooterInterner();
        FileMetaData Parse(byte[] file) => FileMetaData.Parse(Footer(file), interner);
        FileMetaData xs = Parse(XS(1, "a"));
        FileMetaData sx = Parse(File([new Field("s", StringType.Default, true), new Field("x", Int64Type.Default, true)], [Strings("b"), Longs(2)]));
        FileMetaData required = Parse(XS(3, "c", required: true));
        FileMetaData xs2 = Parse(XS(4, "d"));
        Assert.Same(xs.Shape, xs2.Shape);
        Assert.NotSame(xs.Schema, sx.Schema);
        Assert.NotSame(xs.Schema, required.Schema);
        Assert.NotSame(xs.Shape, sx.Shape);
        Assert.NotSame(xs.Shape, required.Shape);
        Assert.Equal(3, interner.Counts.Schemas);
        Assert.Equal(3, interner.Counts.Shapes);
    }

    /// <summary>Footers read with an interner share their column paths and writer name; their public views read as before.</summary>
    [Fact]
    public void ColumnPathsAndWriters_AreShared_AndViewsMadeOnFirstUse()
    {
        var interner = new FooterInterner();
        byte[] a = Footer(XS(1, "a")), b = Footer(XS(2, "bb"));
        FileMetaData first = FileMetaData.Parse(a, interner), second = FileMetaData.Parse(b, interner);
        for (int c = 0; c < 2; c++)
            Assert.Same(first.RowGroups[0].Columns[c].MetaData!.PathInSchema, second.RowGroups[0].Columns[c].MetaData!.PathInSchema);
        Assert.Equal(["x"], second.RowGroups[0].Columns[0].MetaData!.PathInSchema);
        Assert.NotNull(first.CreatedBy);
        Assert.Same(first.CreatedBy, second.CreatedBy);
        Assert.Equal((2, 1), interner.TextCounts);

        // The public views, made when first asked for, are what a footer read alone gives.
        ParquetMetadata shared = ParquetMetadata.ParseFooter(b, interner), alone = ParquetMetadata.Parse(b);
        Assert.Equal(alone.CreatedBy, shared.CreatedBy);
        Assert.Equal(alone.KeyValueMetadata, shared.KeyValueMetadata);
        Assert.Equal(alone.RowGroups.Select(g => (g.RowCount, string.Join(';', g.Columns.Select(c => $"{string.Join('.', c.Path)}:{c.ValueCount}:{c.Statistics}")))),
            shared.RowGroups.Select(g => (g.RowCount, string.Join(';', g.Columns.Select(c => $"{string.Join('.', c.Path)}:{c.ValueCount}:{c.Statistics}")))));
        Assert.Same(shared.RowGroups, shared.RowGroups);
        Assert.Empty(new ParquetMetadata { Schema = [], RowCount = 0 }.RowGroups);
    }

    /// <summary>Two files of the same Parquet schema but other stored Arrow schemas share their elements, not their shape.</summary>
    [Fact]
    public void FootersDifferingOnlyInTheStoredArrowSchema_HaveShapesOfTheirOwn()
    {
        static byte[] Times(string zone)
        {
            var type = new TimestampType(TimeUnit.Microsecond, zone);
            var values = new TimestampArray.Builder(type).Append(DateTimeOffset.UnixEpoch).Build();
            return Footer(File([new Field("t", type, true)], [values]));
        }
        var interner = new FooterInterner();
        FileMetaData utc = FileMetaData.Parse(Times("UTC"), interner), tokyo = FileMetaData.Parse(Times("Asia/Tokyo"), interner);
        Assert.Same(utc.Schema, tokyo.Schema);
        Assert.NotSame(utc.KeyValueMetadata, tokyo.KeyValueMetadata);
        Assert.NotSame(utc.Shape, tokyo.Shape);
        Assert.Equal("UTC", ((TimestampType)utc.Shape!.Mapping(new ParquetReadOptions()).Schema.FieldsList[0].DataType).Timezone);
        Assert.Equal("Asia/Tokyo", ((TimestampType)tokyo.Shape!.Mapping(new ParquetReadOptions()).Schema.FieldsList[0].DataType).Timezone);
    }

    /// <summary>Beyond the limit, footers read their own lists (nothing more is kept), as they would alone.</summary>
    [Fact]
    public void BeyondTheLimit_FootersReadTheirOwn()
    {
        var interner = new FooterInterner();
        for (int i = 0; i <= FooterInterner.MaxEntries; i++)
        {
            byte[] footer = Footer(File([new Field($"c{i}", Int64Type.Default, true)], [Longs(i)], new ParquetWriteOptions { StoreArrowSchema = false }));
            FileMetaData meta = FileMetaData.Parse(footer, interner);
            Assert.Equal($"c{i}", meta.Schema[1].Name);
            Assert.Equal($"c{i}", meta.Shape!.Mapping(new ParquetReadOptions()).Schema.FieldsList[0].Name);
        }
        Assert.Equal((FooterInterner.MaxEntries, 0, FooterInterner.MaxEntries), interner.Counts);
    }

    /// <summary>A damaged list fails as it fails without sharing (skipping it first checks less than reading it).</summary>
    [Fact]
    public void ADamagedSchemaList_FailsAsWithoutSharing()
    {
        // FileMetaData { 2: list<i32> [1] } — schema elements are structs.
        byte[] footer = [0x29, 0x15, 0x02, 0x00];
        string alone = Assert.Throws<ParquetFormatException>(() => FileMetaData.Parse(footer)).Message;
        Assert.Equal(alone, Assert.Throws<ParquetFormatException>(() => FileMetaData.Parse(footer, new FooterInterner())).Message);
    }

    /// <summary>Readers of one file share its shape and select for their own options (types, row index).</summary>
    [Fact]
    public void ReadersOfOneFile_SelectForTheirOwnOptions()
    {
        using ParquetFile file = ParquetFile.Open(XS(7, "q"));
        using (var views = file.OpenReader()) Assert.IsType<StringViewType>(views.Schema.FieldsList[1].DataType);
        using (var plain = file.OpenReader(new ParquetReadOptions { ViewTypes = false }))
        {
            Assert.IsType<StringType>(plain.Schema.FieldsList[1].DataType);
            using RecordBatch batch = plain.ReadBatches().Single();
            Assert.Equal("q", ((StringArray)batch.Column(1)).GetString(0));
        }
        using (var numbered = file.OpenReader(new ParquetReadOptions { RowIndexColumn = "i" }))
        {
            Assert.Equal(["i", "x", "s"], numbered.Schema.FieldsList.Select(f => f.Name));
            using RecordBatch batch = numbered.ReadBatches().Single();
            Assert.Equal([0L, 7L], [((Int64Array)batch.Column(0)).GetValue(0)!.Value, ((Int64Array)batch.Column(1)).GetValue(0)!.Value]);
        }
    }

    /// <summary>A column chunk without metadata fails when the footer is read, though its views are made later.</summary>
    [Fact]
    public void AChunkWithoutMetadata_FailsWhenTheFooterIsRead()
    {
        FileMetaData meta = FileMetaData.Parse(Footer(XS(1, "a")));
        RowGroup group = meta.RowGroups[0];
        var damaged = new FileMetaData
        {
            Version = meta.Version,
            Schema = meta.Schema,
            NumRows = meta.NumRows,
            RowGroups = [new RowGroup { Columns = [group.Columns[0], new ColumnChunk { FileOffset = group.Columns[1].FileOffset }], TotalByteSize = group.TotalByteSize, NumRows = group.NumRows }],
        };
        byte[] footer = ParquetFooterTests.Encode(damaged);
        Assert.Contains("has no metadata", Assert.Throws<ParquetFormatException>(() => ParquetMetadata.Parse(footer)).Message);
        Assert.Contains("has no metadata", Assert.Throws<ParquetFormatException>(() => ParquetMetadata.ParseFooter(footer, new FooterInterner())).Message);
    }

    /// <summary>A dataset of files of one schema maps and plans them once; files of another schema get their own.</summary>
    [Fact]
    public void ADataset_SharesOneShapePerSchema_AndReadsEveryFile()
    {
        var store = new MemoryFileStore();
        for (int i = 0; i < 6; i++) store.Add($"d/f{i}.parquet", XS(i, $"v{i}"));
        store.Add("d/g.parquet", File([new Field("s", StringType.Default, true), new Field("x", Int64Type.Default, true)], [Strings("w"), Longs(9)]));
        using var dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = store });
        List<string> rows = [];
        foreach (var options in (ParquetReadOptions[])[new(), new() { MaxDegreeOfParallelism = 4 }, new() { Columns = ["s"] }])
        {
            using var reader = dataset.OpenReader(options);
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch)
                {
                    for (int r = 0; r < batch.Length; r++)
                        rows.Add(string.Join('|', Enumerable.Range(0, batch.ColumnCount).Select(c => batch.Column(c) switch
                        {
                            Int64Array l => l.GetValue(r)!.Value.ToString(),
                            StringViewArray v => v.GetString(r),
                            _ => "?",
                        })));
                }
            }
        }
        string[] all = ["0|v0", "1|v1", "2|v2", "3|v3", "4|v4", "5|v5", "9|w"];
        Assert.Equal([.. all, .. all, .. all.Select(r => r[(r.IndexOf('|') + 1)..])], rows);
        Assert.Equal(2, dataset.Interner.Counts.Shapes);
    }
}
