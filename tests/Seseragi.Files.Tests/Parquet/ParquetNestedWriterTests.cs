using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using static Seseragi.Files.Tests.TestBatches;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// Nested columns written by <see cref="ParquetWriter"/> against our reader: random structs, lists of every layout,
/// maps, run-end encoded and dictionary-encoded nested values, nulls at every level, sliced batches; the Parquet schema
/// and levels as pyarrow and Spark write them; field ids; refusals. Other readers: ParquetWriterInteropTests.
/// </summary>
public sealed class ParquetNestedWriterTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("seseragi-parquet-nested").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private string Path(string name) => System.IO.Path.Combine(_dir, name);

    /// <summary>The rows of every column of a file, through our reader (checksums verified), as canonical text.</summary>
    private static string[][] ReadBack(string path)
    {
        using var file = ParquetFile.Open(path);
        using var reader = file.OpenReader(new ParquetReadOptions { VerifyChecksums = true });
        List<RecordBatch> batches = [.. reader.ReadBatches()];
        try
        {
            return [.. Enumerable.Range(0, file.Schema.FieldsList.Count).Select(c => batches.SelectMany(b => ParquetNestedData.Canonical(b.Column(c))).ToArray())];
        }
        finally
        {
            foreach (RecordBatch b in batches) b.Dispose();
        }
    }

    public static TheoryData<ParquetPageVersion, string, bool> Layouts()
    {
        TheoryData<ParquetPageVersion, string, bool> data = [];
        data.Add(ParquetPageVersion.V1, "snappy", true);
        data.Add(ParquetPageVersion.V1, "none", false);
        data.Add(ParquetPageVersion.V2, "zstd", true);
        data.Add(ParquetPageVersion.V2, "lz4", false);
        data.Add(ParquetPageVersion.V1, "gzip", true);
        data.Add(ParquetPageVersion.V2, "none", true);
        return data;
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public void NestedColumns_ComeBack_InEveryLayout(ParquetPageVersion version, string codec, bool dictionary)
    {
        for (int seed = 0; seed < 12; seed++)
        {
            int s = HashCode.Combine(version, codec, dictionary, seed) & 0xFFFF;
            var (batch, expected) = ParquetNestedData.Batch(s, 900);
            var options = new ParquetWriteOptions
            {
                DataPageVersion = version, Compression = ParquetWriterTests.Codec(codec), Dictionary = dictionary,
                DataPageRows = 1 + s % 60, DataPageBytes = 4000, RowGroupRows = 350, DictionaryPageBytes = 300,
            };
            // A slice of the batch (offsets in every nested array), an empty one, the rest.
            string path = Path($"nested{seed}.parquet");
            ParquetWriter.Write(path, batch.Schema, [batch.Slice(17, 300), batch.Slice(317, 0), batch.Slice(317, 583)], options);
            string[][] read = ReadBack(path);
            for (int c = 0; c < batch.ColumnCount; c++)
            {
                string[] want = expected[c][17..];
                if (!want.SequenceEqual(read[c]))
                {
                    int row = want.Zip(read[c]).TakeWhile(p => p.First == p.Second).Count();
                    Assert.Fail($"seed {s}: column {batch.Schema.FieldsList[c].Name} ({batch.Schema.FieldsList[c].DataType.Name}) row {row}: " +
                        $"expected {want.ElementAtOrDefault(row)}, read {read[c].ElementAtOrDefault(row)}");
                }
            }
        }
    }

    /// <summary>The example pyarrow and Spark were probed with: the same schema elements, value and null counts.</summary>
    [Fact]
    public void TheSchemaAndLevels_AreThoseOfPyArrowAndSpark()
    {
        var li = new ListArray.Builder(Int32Type.Default);
        var liValues = (Int32Array.Builder)li.ValueBuilder;
        li.Append(); liValues.Append(1); liValues.AppendNull();
        li.Append();
        li.AppendNull();
        li.Append(); liValues.Append(3);
        var stType = new StructType([new Field("a", Int32Type.Default, true), new Field("b", StringType.Default, true)]);
        var st = Struct(stType, [Fixed(Int32Type.Default, [(int?)1, 0, null, 4]), Strings(["x", "", "y", null])], [true, false, true, true]);
        var mpType = new MapType(new Field("key", StringType.Default, false), new Field("value", Int32Type.Default, true));
        var mp = new MapArray.Builder(mpType);
        mp.Append(); ((StringArray.Builder)mp.KeyBuilder).Append("k"); ((Int32Array.Builder)mp.ValueBuilder).Append(1);
        mp.Append();
        mp.AppendNull();
        mp.Append(); ((StringArray.Builder)mp.KeyBuilder).Append("z"); ((Int32Array.Builder)mp.ValueBuilder).AppendNull();
        RecordBatch batch = Batch(("li", li.Build(), true), ("st", st, true), ("mp", mp.Build(), true));
        string path = Path("probe.parquet");
        ParquetWriter.Write(path, batch.Schema, [batch]);
        using var file = ParquetFile.Open(path);
        FileMetaData footer = file.Metadata.Thrift!;
        Assert.Equal(
            ["schema", "li", "list", "element", "st", "a", "b", "mp", "key_value", "key", "value"],
            footer.Schema.Select(e => e.Name));
        Assert.Equal<FieldRepetition?>(
            [null, FieldRepetition.Optional, FieldRepetition.Repeated, FieldRepetition.Optional, FieldRepetition.Optional, FieldRepetition.Optional,
             FieldRepetition.Optional, FieldRepetition.Optional, FieldRepetition.Repeated, FieldRepetition.Required, FieldRepetition.Optional],
            footer.Schema.Select(e => e.Repetition));
        Assert.Equal((LogicalTypeKind.List, ConvertedType.List), (footer.Schema[1].LogicalType!.Kind, footer.Schema[1].ConvertedType!.Value));
        Assert.Equal((LogicalTypeKind.Map, ConvertedType.Map), (footer.Schema[7].LogicalType!.Kind, footer.Schema[7].ConvertedType!.Value));
        Assert.Null(footer.Schema[2].LogicalType);
        Assert.Null(footer.Schema[8].ConvertedType);
        ColumnMetaData[] chunks = [.. footer.RowGroups[0].Columns.Select(c => c.MetaData!)];
        Assert.Equal(["li.list.element", "st.a", "st.b", "mp.key_value.key", "mp.key_value.value"], chunks.Select(c => string.Join('.', c.PathInSchema)));
        Assert.Equal([5L, 4, 4, 4, 4], chunks.Select(c => c.NumValues));
        Assert.Equal([3L, 2, 2, 2, 3], chunks.Select(c => c.Statistics!.NullCount!.Value));
        Assert.Equal(["[1,null]", "[]", "null", "[3]"], ReadBack(path)[0]);
        Assert.Equal(["{k=>1}", "{}", "null", "{z=>null}"], ReadBack(path)[2].Select(v => v.Replace("s:", "")));
    }

    /// <summary>Format 2 page headers count what their pages hold: rows (pages break between rows), levels, nulls.</summary>
    [Fact]
    public void V2PageHeaders_CountTheirRows_LevelsAndNulls()
    {
        var (batch, _) = ParquetNestedData.Batch(77, 2000);
        string path = Path("v2.parquet");
        ParquetWriter.Write(path, batch.Schema, [batch], new ParquetWriteOptions { DataPageVersion = ParquetPageVersion.V2, DataPageRows = 37, RowGroupRows = 700 });
        byte[] bytes = File.ReadAllBytes(path);
        using var file = ParquetFile.Open(path);
        int pages = 0;
        foreach (RowGroup group in file.Metadata.Thrift!.RowGroups)
        {
            foreach (ColumnChunk chunk in group.Columns)
            {
                ColumnMetaData meta = chunk.MetaData!;
                long at = meta.DictionaryPageOffset ?? meta.DataPageOffset, end = at + meta.TotalCompressedSize;
                long rows = 0, values = 0, nulls = 0;
                while (at < end)
                {
                    PageHeader header = PageHeader.Parse(bytes.AsSpan((int)at), out int length);
                    if (header.DataPageHeaderV2 is { } v2)
                    {
                        rows += v2.NumRows;
                        values += v2.NumValues;
                        nulls += v2.NumNulls;
                        Assert.InRange(v2.NumRows, 1, 37);
                        pages++;
                    }
                    at += length + header.CompressedPageSize;
                }
                Assert.Equal(group.NumRows, rows);
                Assert.Equal(meta.NumValues, values);
                Assert.Equal(meta.Statistics!.NullCount, nulls);
            }
        }
        Assert.True(pages > 100);
    }

    /// <summary>Pages of long lists close at DataPageBytes too (between rows), not only at DataPageRows.</summary>
    [Fact]
    public void NestedPages_CloseAtTheirByteSize()
    {
        int rows = 3000, width = 512;
        var type = new ListType(new Field("item", FloatType.Default, true));
        var items = new FloatArray(new ArrowBuffer(new byte[rows * width * 4]), ArrowBuffer.Empty, rows * width, 0, 0);
        int[] offsets = [.. Enumerable.Range(0, rows + 1).Select(i => i * width)];
        var list = new ListArray(type, rows, new ArrowBuffer(System.Runtime.InteropServices.MemoryMarshal.AsBytes(offsets.AsSpan()).ToArray()), items, ArrowBuffer.Empty, 0);
        RecordBatch batch = Batch(("emb", list, true));
        string path = Path("pages.parquet");
        ParquetWriter.Write(path, batch.Schema, [batch], new ParquetWriteOptions { DataPageBytes = 64 << 10, Dictionary = false });
        byte[] bytes = File.ReadAllBytes(path);
        using var file = ParquetFile.Open(path);
        ColumnMetaData meta = file.Metadata.Thrift!.RowGroups[0].Columns[0].MetaData!;
        long at = meta.DataPageOffset, end = at + meta.TotalCompressedSize;
        List<int> sizes = [];
        while (at < end)
        {
            PageHeader header = PageHeader.Parse(bytes.AsSpan((int)at), out int length);
            sizes.Add(header.UncompressedPageSize);
            at += length + header.CompressedPageSize;
        }
        // At most the budget plus one step of rows (4096 levels of 4-byte values) and the levels.
        Assert.True(sizes.Max() < (64 << 10) + 4096 * 4 + 4096, $"largest page {sizes.Max()} bytes");
        Assert.True(sizes.Count > 80, $"{sizes.Count} pages"); // 6 MB in pages of 64 KiB
        Assert.Equal(rows, ReadBack(path)[0].Length);
    }

    [Fact]
    public void FieldIds_AreWritten_OnEveryNamedField()
    {
        static Field Id(Field f, int id) => ArrowFieldIds.With(f, id);
        var element = Id(new Field("item", Int32Type.Default, true), 3);
        var listType = new ListType(element);
        var stType = new StructType([Id(new Field("a", Int64Type.Default, true), 5)]);
        var mapType = new MapType(Id(new Field("key", StringType.Default, false), 7), Id(new Field("value", Int32Type.Default, true), 8), false);
        var schema = new Apache.Arrow.Schema(
        [
            Id(new Field("li", listType, true), 2), Id(new Field("st", stType, true), 4), Id(new Field("mp", mapType, true), 6), Id(new Field("x", Int64Type.Default, true), 9),
        ], null);
        var li = new ListArray.Builder(element).Append().Build();
        var st = Struct(stType, [Fixed(Int64Type.Default, [(long?)1])], [true]);
        var mp = new MapArray.Builder(mapType);
        mp.Append();
        var batch = new RecordBatch(schema, [new ListArray(listType, 1, li.ValueOffsetsBuffer, li.Values, ArrowBuffer.Empty, 0), st, mp.Build(), Fixed(Int64Type.Default, [(long?)1])], 1);
        string path = Path("ids.parquet");
        ParquetWriter.Write(path, schema, [batch]);
        using var file = ParquetFile.Open(path);
        Assert.Equal<int?>([null, 2, null, 3, 4, 5, 6, null, 7, 8, 9], file.Metadata.Thrift!.Schema.Select(e => e.FieldId));
        // Our reader gives them back on the Arrow fields.
        Field readList = file.Schema.GetFieldByName("li");
        Assert.Equal(2, ArrowFieldIds.Of(readList));
        Assert.Equal(3, ArrowFieldIds.Of(((ListType)readList.DataType).ValueField));
        Assert.Equal(7, ArrowFieldIds.Of(((MapType)file.Schema.GetFieldByName("mp").DataType).KeyField));
        Assert.Equal(9, ArrowFieldIds.Of(file.Schema.GetFieldByName("x")));
    }

    [Fact]
    public void LongLists_AndOneRowPages_ComeBack()
    {
        // One row of thousands of items in pages of one row; rows of nothing around it.
        var builder = new ListArray.Builder(Int64Type.Default);
        var values = (Int64Array.Builder)builder.ValueBuilder;
        builder.AppendNull();
        builder.Append();
        for (int i = 0; i < 50_000; i++) values.Append(i);
        builder.Append();
        var batch = new RecordBatch(new Apache.Arrow.Schema([new Field("big", new ListType(new Field("item", Int64Type.Default, true)), true)], null), [builder.Build()], 3);
        string path = Path("long.parquet");
        ParquetWriter.Write(path, batch.Schema, [batch], new ParquetWriteOptions { DataPageRows = 1, DataPageBytes = 100 });
        string[] read = ReadBack(path)[0];
        Assert.Equal("null", read[0]);
        Assert.Equal(50_000, read[1].Count(ch => ch == ',') + 1);
        Assert.Equal("[]", read[2]);
    }

    [Fact]
    public void Writes_AreRefused_WithReasons()
    {
        // Unions have no Parquet form. (Nor have structs without fields, but Apache.Arrow refuses to make one.)
        var union = new Apache.Arrow.Schema([new Field("u", new UnionType([new Field("a", Int32Type.Default, true)], [0], UnionMode.Sparse), true)], null);
        Assert.Contains("union", Assert.Throws<NotSupportedException>(() => ParquetWriter.Create(Path("u.parquet"), union)).Message);

        // Arrays whose children do not have the types the nested type declares (Apache.Arrow builds them unchecked).
        var declared = new StructType([new Field("x", DoubleType.Default, true)]);
        var wrongChild = new StructArray(declared, 1, [Strings(["not a double"])], ArrowBuffer.Empty, 0);
        RecordBatch inconsistent = Batch(("s", wrongChild, true));
        using (var writer = ParquetWriter.Create(new MemoryStream(), inconsistent.Schema))
            Assert.Contains("'s.x' is utf8, not double", Assert.Throws<ArgumentException>(() => writer.Write(inconsistent)).Message);

        // A null under a present parent where the schema says non-nullable, at any depth: found before anything is written.
        var stType = new StructType([new Field("a", Int64Type.Default, false)]);
        var st = Struct(stType, [Fixed(Int64Type.Default, [(long?)1, null, null])], [true, false, true]);
        var listType = new ListType(new Field("item", Int32Type.Default, false));
        var list = List(listType, [0, 1, 2], Fixed(Int32Type.Default, [(int?)1, null]), [true, true]);
        foreach (var (batch, where) in (ValueTuple<RecordBatch, string>[])[(Batch(("s", st, true)), "s.a"), (Batch(("l", list, true)), "l[]")])
        {
            using var stream = new MemoryStream();
            using var writer = ParquetWriter.Create(stream, batch.Schema, new ParquetWriteOptions { LeaveOpen = true });
            Assert.Contains($"'{where}' is not nullable", Assert.Throws<ArgumentException>(() => writer.Write(batch)).Message);
            Assert.Equal(0, stream.Length);
            writer.Write(batch.Slice(0, 1)); // the writer goes on: the first row is fine
        }
    }
}
