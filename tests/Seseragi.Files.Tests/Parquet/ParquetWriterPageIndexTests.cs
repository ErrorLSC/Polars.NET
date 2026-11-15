using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using static Seseragi.Files.Tests.TestBatches;
using Statistics = Kaminari.Seseragi.Files.Parquet.Metadata.Statistics;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// The page index and size statistics <see cref="ParquetWriter"/> writes. Each page's entry is checked against the
/// rows of that page written alone (whose chunk statistics are checked elsewhere): bounds, null counts, histograms
/// and value bytes must be what the page alone would get; the offset index against the page headers it points at.
/// Then the rules (which chunks get no column index, boundary orders, which histograms), and reads that use the index.
/// </summary>
public sealed class ParquetWriterPageIndexTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("seseragi-parquet-pageindex").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private string Path(string name) => System.IO.Path.Combine(_dir, name);

    /// <summary>A chunk's indexes, read from the file's bytes (null when absent).</summary>
    private static (ColumnIndex? Columns, OffsetIndex? Offsets) Indexes(byte[] bytes, ColumnChunk chunk) =>
        (chunk.ColumnIndexOffset is long c ? ColumnIndex.Parse(bytes.AsSpan((int)c, chunk.ColumnIndexLength!.Value)) : null,
         chunk.OffsetIndexOffset is long o ? OffsetIndex.Parse(bytes.AsSpan((int)o, chunk.OffsetIndexLength!.Value)) : null);

    /// <summary>
    /// Writes <paramref name="batch"/> and checks every chunk's page index against its page headers and against the
    /// rows of each page written alone; returns how many column indexes and pages were checked.
    /// </summary>
    private (int ColumnIndexes, int Pages) CheckAgainstPagesAlone(RecordBatch batch, ParquetWriteOptions options, string name)
    {
        string path = Path(name);
        ParquetWriter.Write(path, batch.Schema, [batch], options);
        byte[] bytes = File.ReadAllBytes(path);
        using var file = ParquetFile.Open(path);
        FileMetaData footer = file.Metadata.Thrift!;
        Dictionary<(long, long), FileMetaData> alone = [];
        int indexes = 0, pages = 0;
        long groupStart = 0;
        foreach (RowGroup group in footer.RowGroups)
        {
            for (int c = 0; c < group.Columns.Length; c++)
            {
                ColumnChunk chunk = group.Columns[c];
                ColumnMetaData meta = chunk.MetaData!;
                string column = string.Join('.', meta.PathInSchema);
                var (columns, offsets) = Indexes(bytes, chunk);
                Assert.NotNull(offsets);
                PageLocation[] locations = offsets.PageLocations;

                // The offset index: the data pages back to back, from the first to the chunk's end, rows from 0 up.
                long at = meta.DataPageOffset, end = (meta.DictionaryPageOffset ?? meta.DataPageOffset) + meta.TotalCompressedSize;
                long[] rows = new long[locations.Length];
                for (int k = 0; k < locations.Length; k++)
                {
                    Assert.Equal(at, locations[k].Offset);
                    PageHeader header = PageHeader.Parse(bytes.AsSpan((int)at), out int length);
                    Assert.True(header.Type is PageType.DataPage or PageType.DataPageV2, $"{column}: page {k} is {header.Type}");
                    Assert.Equal(length + header.CompressedPageSize, locations[k].CompressedPageSize);
                    rows[k] = (k + 1 < locations.Length ? locations[k + 1].FirstRowIndex : group.NumRows) - locations[k].FirstRowIndex;
                    Assert.True(rows[k] > 0, $"{column}: page {k} starts at row {locations[k].FirstRowIndex} after the next");
                    if (header.DataPageHeaderV2 is { } v2) Assert.Equal(v2.NumRows, rows[k]);
                    else if (meta.PathInSchema.Length == 1) Assert.Equal(header.DataPageHeader!.NumValues, rows[k]); // flat: one level a row
                    at += locations[k].CompressedPageSize;
                }
                Assert.Equal(0, locations[0].FirstRowIndex);
                Assert.Equal(end, at);

                // Each page against its rows written alone.
                bool anyUnbounded = false;
                for (int k = 0; k < locations.Length; k++)
                {
                    long first = groupStart + locations[k].FirstRowIndex;
                    if (!alone.TryGetValue((first, rows[k]), out FileMetaData? single))
                    {
                        string slicePath = Path($"{name}-{first}-{rows[k]}.parquet");
                        ParquetWriter.Write(slicePath, batch.Schema, [batch.Slice((int)first, (int)rows[k])],
                            options with { RowGroupRows = int.MaxValue, RowGroupBytes = long.MaxValue, PageIndex = false });
                        using var sliceFile = ParquetFile.Open(slicePath);
                        alone[(first, rows[k])] = single = sliceFile.Metadata.Thrift!;
                    }
                    ColumnMetaData pageAlone = single.RowGroups[0].Columns[c].MetaData!;
                    Statistics? s = pageAlone.Statistics;
                    long values = pageAlone.NumValues - (s?.NullCount ?? 0);
                    if (options.Statistics && values > 0 && s?.MaxValue is null) anyUnbounded = true;
                    if (columns is not null)
                    {
                        Assert.Equal(values == 0, columns.NullPages[k]);
                        Assert.Equal(s!.NullCount, columns.NullCounts![k]);
                        Assert.Equal(values == 0 ? [] : s.MinValue, columns.MinValues[k]);
                        Assert.Equal(values == 0 ? [] : s.MaxValue, columns.MaxValues[k]);
                    }
                    SizeStatistics? sizes = pageAlone.SizeStatistics;
                    if (offsets.UnencodedByteArrayDataBytes is { } unencoded) Assert.Equal(sizes!.UnencodedByteArrayDataBytes, unencoded[k]);
                    if (columns?.DefinitionLevelHistograms is { } def)
                    {
                        long[] page = sizes!.DefinitionLevelHistogram!;
                        Assert.Equal(page, def.AsSpan(k * page.Length, page.Length).ToArray());
                    }
                    if (columns?.RepetitionLevelHistograms is { } rep)
                    {
                        long[] page = sizes!.RepetitionLevelHistogram!;
                        Assert.Equal(page, rep.AsSpan(k * page.Length, page.Length).ToArray());
                        Assert.Equal(rows[k], page[0]); // a row starts at each repetition level 0
                    }
                    pages++;
                }
                // A column index exactly when the statistics allow every page one.
                Assert.True((options.Statistics && !anyUnbounded) == (columns is not null), $"{column}: column index {(columns is null ? "missing" : "written")}");
                if (columns is not null) indexes++;

                // The chunk's size statistics sum its pages'.
                if (meta.SizeStatistics is { } chunkSizes)
                {
                    if (chunkSizes.UnencodedByteArrayDataBytes is { } total) Assert.Equal(total, offsets.UnencodedByteArrayDataBytes!.Sum());
                    if (chunkSizes.DefinitionLevelHistogram is { } def) Assert.Equal(meta.NumValues, def.Sum());
                    if (chunkSizes.RepetitionLevelHistogram is { } rep) Assert.Equal(group.NumRows, rep[0]);
                }
            }
            groupStart += group.NumRows;
        }
        return (indexes, pages);
    }

    [Theory]
    [InlineData(ParquetPageVersion.V1, 1)]
    [InlineData(ParquetPageVersion.V2, 2)]
    [InlineData(ParquetPageVersion.V1, 3)]
    public void FlatColumns_PageIndexIsEachPageAlone(ParquetPageVersion version, int seed)
    {
        RecordBatch batch = ParquetWriteData.Wide(seed, 900);
        var (indexes, pages) = CheckAgainstPagesAlone(batch, new ParquetWriteOptions { DataPageVersion = version, DataPageRows = 97, RowGroupRows = 400 }, $"flat{seed}");
        Assert.True(indexes > 60, $"only {indexes} column indexes");
        Assert.True(pages > 400, $"only {pages} pages");
    }

    [Theory]
    [InlineData(ParquetPageVersion.V1, 11)]
    [InlineData(ParquetPageVersion.V2, 12)]
    [InlineData(ParquetPageVersion.V2, 13)]
    public void NestedColumns_PageIndexIsEachPageAlone(ParquetPageVersion version, int seed)
    {
        var (batch, _) = ParquetNestedData.Batch(seed, 700);
        var (indexes, pages) = CheckAgainstPagesAlone(batch, new ParquetWriteOptions { DataPageVersion = version, DataPageRows = 53, RowGroupRows = 300 }, $"nested{seed}");
        Assert.True(indexes > 5, $"only {indexes} column indexes");
        Assert.True(pages > 50, $"only {pages} pages");
    }

    [Fact]
    public void WithoutStatistics_OnlyOffsetIndexes_AndDefinitionHistogramsCarryNulls()
    {
        RecordBatch batch = ParquetWriteData.Wide(5, 300);
        var options = new ParquetWriteOptions { Statistics = false, DataPageRows = 50 };
        CheckAgainstPagesAlone(batch, options, "nostats");
        using var file = ParquetFile.Open(Path("nostats"));
        foreach (ColumnChunk chunk in file.Metadata.Thrift!.RowGroups[0].Columns)
        {
            Assert.Null(chunk.ColumnIndexOffset);
            Assert.NotNull(chunk.OffsetIndexOffset);
            ColumnMetaData meta = chunk.MetaData!;
            bool optional = meta.PathInSchema[0] is not ("req_i64" or "req_str");
            // With no null count to say it, a nullable column's definition histogram does.
            Assert.Equal(optional, meta.SizeStatistics?.DefinitionLevelHistogram is not null);
        }
    }

    [Fact]
    public void PageIndexAndSizeStatisticsOff_WriteNeither()
    {
        var (batch, _) = ParquetNestedData.Batch(3, 200);
        string path = Path("off.parquet");
        ParquetWriter.Write(path, batch.Schema, [batch], new ParquetWriteOptions { PageIndex = false, SizeStatistics = false });
        using var file = ParquetFile.Open(path);
        Assert.All(file.Metadata.Thrift!.RowGroups[0].Columns, c =>
        {
            Assert.Null(c.ColumnIndexOffset);
            Assert.Null(c.OffsetIndexOffset);
            Assert.Null(c.MetaData!.SizeStatistics);
        });
    }

    /// <summary>Which histograms are written: repetition levels below lists, definition levels deeper than one (or without statistics).</summary>
    [Fact]
    public void SizeStatistics_SayOnlyWhatOtherFieldsDoNot()
    {
        var listType = new ListType(new Field("item", Int32Type.Default, true));
        var lists = new ListArray.Builder(Int32Type.Default);
        foreach (int?[]? row in (int?[]?[])[[1, null], null, [], [4]])
        {
            if (row is null)
            {
                lists.AppendNull();
                continue;
            }
            lists.Append();
            foreach (int? v in row)
            {
                if (v is { } x) ((Int32Array.Builder)lists.ValueBuilder).Append(x);
                else ((Int32Array.Builder)lists.ValueBuilder).AppendNull();
            }
        }
        RecordBatch batch = Batch(
            ("req", Fixed(Int64Type.Default, [(long?)1, 2, 3, 4]), false),
            ("opt", Fixed(Int64Type.Default, [(long?)1, null, 3, 4]), true),
            ("s", Strings(["ab", null, "", "xyz"]), true),
            ("fsb", Fixed(new FixedSizeBinaryType(2), [(ushort?)0x0201, 0x0403, 0x0605, 0x0807]), true),
            ("l", lists.Build(), true));
        string path = Path("sizes.parquet");
        ParquetWriter.Write(path, batch.Schema, [batch]);
        byte[] bytes = File.ReadAllBytes(path);
        using var file = ParquetFile.Open(path);
        ColumnChunk[] chunks = file.Metadata.Thrift!.RowGroups[0].Columns;
        Assert.Null(chunks[0].MetaData!.SizeStatistics); // required: nothing to say
        Assert.Null(chunks[1].MetaData!.SizeStatistics); // one level: the null count says it
        Assert.Equal(5, chunks[2].MetaData!.SizeStatistics!.UnencodedByteArrayDataBytes); // value bytes, without lengths
        Assert.Null(chunks[2].MetaData!.SizeStatistics!.DefinitionLevelHistogram);
        Assert.Null(chunks[3].MetaData!.SizeStatistics); // fixed-length bytes are not counted
        SizeStatistics list = chunks[4].MetaData!.SizeStatistics!;
        Assert.Equal([1L, 1, 1, 2], list.DefinitionLevelHistogram!); // null list, empty list, null item, items
        Assert.Equal([4L, 1], list.RepetitionLevelHistogram!);
        Assert.Null(list.UnencodedByteArrayDataBytes);
        var (columns, offsets) = Indexes(bytes, chunks[4]);
        Assert.Equal([1L, 1, 1, 2], columns!.DefinitionLevelHistograms!);
        Assert.Equal([4L, 1], columns.RepetitionLevelHistograms!);
        Assert.Null(offsets!.UnencodedByteArrayDataBytes);
        var (stringColumns, stringOffsets) = Indexes(bytes, chunks[2]);
        Assert.Null(stringColumns!.DefinitionLevelHistograms);
        Assert.Equal([5L], stringOffsets!.UnencodedByteArrayDataBytes!);
        Assert.Null(Indexes(bytes, chunks[1]).Columns!.DefinitionLevelHistograms);
    }

    // ------------------------------------------------------------------
    // Column index rules
    // ------------------------------------------------------------------

    private ColumnIndex?[] ColumnIndexes(RecordBatch batch, ParquetWriteOptions? options = null, string name = "ci.parquet")
    {
        string path = Path(name);
        ParquetWriter.Write(path, batch.Schema, [batch], options ?? new ParquetWriteOptions { DataPageRows = 10 });
        byte[] bytes = File.ReadAllBytes(path);
        using var file = ParquetFile.Open(path);
        return [.. file.Metadata.Thrift!.RowGroups.SelectMany(g => g.Columns).Select(c => Indexes(bytes, c).Columns)];
    }

    [Fact]
    public void BoundaryOrder_FollowsThePagesBounds()
    {
        long?[] ascending = [.. Enumerable.Range(0, 50).Select(i => (long?)i)];
        long?[] withNullPage = [.. Enumerable.Range(0, 50).Select(i => i is >= 20 and < 30 ? null : (long?)i)];
        RecordBatch batch = Batch(
            ("asc", Fixed(Int64Type.Default, ascending), true),
            ("desc", Fixed(Int64Type.Default, [.. ascending.Reverse()]), true),
            ("same", Fixed(Int64Type.Default, [.. ascending.Select(_ => (long?)7)]), true),
            ("unordered", Fixed(Int64Type.Default, [.. ascending.Select(i => (long?)(i * 37 % 50))]), true),
            ("nullpage", Fixed(Int64Type.Default, withNullPage), true),
            ("str", Strings([.. ascending.Select(i => $"k{i:D3}")]), true),
            ("u64", Fixed(UInt64Type.Default, [.. ascending.Select(i => (ulong?)(i < 25 ? (ulong)i!.Value : ulong.MaxValue - 50 + (ulong)i!.Value))]), true),
            ("allnull", Fixed(Int64Type.Default, [.. ascending.Select(_ => (long?)null)]), true),
            // Lower bounds rising while upper bounds fall: neither order.
            ("narrowing", Fixed(Int64Type.Default, [.. ascending.Select(i => (long?)(i % 10 == 0 ? 100 - i : i))]), true));
        ColumnIndex?[] ci = ColumnIndexes(batch);
        Assert.Equal(
            [BoundaryOrder.Ascending, BoundaryOrder.Descending, BoundaryOrder.Ascending, BoundaryOrder.Unordered, BoundaryOrder.Ascending,
             BoundaryOrder.Ascending, BoundaryOrder.Ascending, BoundaryOrder.Ascending, BoundaryOrder.Unordered],
            ci.Select(c => c!.BoundaryOrder));
        Assert.Equal([false, false, true, false, false], ci[4]!.NullPages); // a page of nulls only: empty bounds
        Assert.Equal([0L, 0, 10, 0, 0], ci[4]!.NullCounts!);
        Assert.Empty(ci[4]!.MinValues[2]);
        Assert.All(ci[7]!.NullPages, Assert.True);
    }

    [Fact]
    public void NoColumnIndex_WhereAPageHasNoBounds()
    {
        double?[] doubles = [.. Enumerable.Range(0, 30).Select(i => (double?)i)];
        RecordBatch batch = Batch(
            ("nan", Fixed(DoubleType.Default, [.. doubles.Select((v, i) => i == 25 ? double.NaN : v)]), true), // NaN in the last page only
            ("plain", Fixed(DoubleType.Default, doubles), true),
            ("f16nan", Fixed(HalfFloatType.Default, [.. doubles.Select((v, i) => (Half?)(i == 3 ? Half.NaN : (Half)v!.Value))]), true),
            ("interval", Fixed(new IntervalType(IntervalUnit.YearMonth), [.. Enumerable.Range(0, 30).Select(i => (int?)i)]), true),
            ("unbounded", Binaries(30, Enumerable.Repeat((byte)0xFF, 80).ToArray()), true),
            ("nul", new NullArray(30), true));
        ColumnIndex?[] ci = ColumnIndexes(batch);
        Assert.Null(ci[0]); // as Spark writes: NaN makes no page orderable for parquet-mr
        Assert.NotNull(ci[1]);
        Assert.Null(ci[2]);
        Assert.Null(ci[3]); // INTERVAL has no order
        Assert.Null(ci[4]); // a 0xFF… bound cut to 64 bytes has no upper bound
        Assert.All(ci[5]!.NullPages, Assert.True); // the null type: null pages only

        // INT96 has no order either; another row group of the same column keeps its index when only one holds a NaN.
        RecordBatch ts = Batch(("ts", Fixed(new TimestampType(TimeUnit.Microsecond, "UTC"), [.. Enumerable.Range(0, 30).Select(i => (long?)i)]), true));
        Assert.Null(ColumnIndexes(ts, new ParquetWriteOptions { Int96Timestamps = true, DataPageRows = 10 }, "int96.parquet")[0]);
        ColumnIndex?[] groups = ColumnIndexes(batch, new ParquetWriteOptions { DataPageRows = 10, RowGroupRows = 20 }, "groups.parquet");
        Assert.NotNull(groups[0]); // rows 0..19 of "nan"
        Assert.Null(groups[6]);    // rows 20..29
        RecordBatch nanFirst = Batch(("x", Fixed(DoubleType.Default, [.. doubles.Select((v, i) => i == 0 ? double.NaN : v)]), true));
        ColumnIndex?[] first = ColumnIndexes(nanFirst, new ParquetWriteOptions { DataPageRows = 10, RowGroupRows = 20 }, "nanfirst.parquet");
        Assert.Null(first[0]);
        Assert.NotNull(first[1]); // the next row group starts afresh
    }

    [Fact]
    public void LongStringBounds_AreCutAsChunkStatisticsAre()
    {
        string[] words = [.. Enumerable.Range(0, 20).Select(i => new string('w', 70) + i.ToString("D2"))];
        RecordBatch batch = Batch(("s", Strings(words), true));
        ColumnIndex ci = ColumnIndexes(batch)[0]!;
        Assert.All(ci.MinValues, v => Assert.Equal(64, v.Length));
        Assert.Equal(Encoding.UTF8.GetBytes(new string('w', 63) + "x"), ci.MaxValues[0]);
        ColumnIndex whole = ColumnIndexes(batch, new ParquetWriteOptions { DataPageRows = 10, StatisticsTruncateLength = null }, "whole.parquet")[0]!;
        Assert.Equal(Encoding.UTF8.GetBytes(words[9]), whole.MaxValues[0]);
    }

    // ------------------------------------------------------------------
    // Readers use the index
    // ------------------------------------------------------------------

    [Fact]
    public void FilteredReads_SkipPages_AndKeepEveryMatchingRow()
    {
        var random = new Random(31);
        RecordBatch batch = ParquetWriteData.Sorted(3000, random);
        string with = Path("pi.parquet"), without = Path("nopi.parquet");
        var options = new ParquetWriteOptions { RowGroupRows = 1500, DataPageRows = 100 };
        ParquetWriter.Write(with, batch.Schema, [batch], options);
        ParquetWriter.Write(without, batch.Schema, [batch], options with { PageIndex = false });
        var i32 = (Int32Array)batch.Column(0);
        int narrowed = 0;
        foreach (int row in (int[])[0, 99, 100, 1499, 1500, 2999, random.Next(3000), random.Next(3000)])
        {
            if (i32.IsNull(row)) continue;
            foreach (BinaryOp op in (BinaryOp[])[BinaryOp.Eq, BinaryOp.Lt, BinaryOp.GtEq])
            {
                ScalarExpr filter = ScalarExpr.NewBinary(op, ScalarExpr.NewColumn("i32"), ScalarExpr.NewLiteral(LiteralValue.NewInt32(i32.Values[row])));
                var (expected, pagesWithout) = RowsKept(without, filter);
                var (actual, pagesWith) = RowsKept(with, filter);
                Assert.Equal(expected, actual);
                if (pagesWith < pagesWithout) narrowed++;
            }
        }
        Assert.True(narrowed >= 8, $"the page index narrowed only {narrowed} reads");
    }

    private static IArrowArray Binaries(int count, byte[] value)
    {
        var b = new BinaryArray.Builder();
        for (int i = 0; i < count; i++) b.Append((ReadOnlySpan<byte>)value);
        return b.Build();
    }

    /// <summary>The rows a filtered read keeps and the data pages it decoded.</summary>
    private static (long[] Rows, int PagesRead) RowsKept(string path, ScalarExpr filter)
    {
        using var file = ParquetFile.Open(path);
        using var reader = file.OpenReader(new ParquetReadOptions { Filter = filter, FilterRows = true, RowIndexColumn = "row", Columns = [] });
        List<long> rows = [];
        foreach (RecordBatch b in reader.ReadBatches())
        {
            using (b) rows.AddRange(((Int64Array)b.Column(b.ColumnCount - 1)).Values.ToArray());
        }
        return ([.. rows], reader.PagesRead);
    }
}
