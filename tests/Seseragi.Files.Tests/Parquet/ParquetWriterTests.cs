using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using static Seseragi.Files.Tests.Parquet.ParquetWriteData;
using static Seseragi.Files.Tests.TestBatches;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// <see cref="ParquetWriter"/> against our reader: every type, page layout, codec and dictionary setting gives back
/// the values written (checksums verified); the shapes batches come in; the file's structure; errors.
/// Statistics and the other readers are checked in ParquetWriterStatisticsTests and ParquetWriterInteropTests.
/// </summary>
public sealed class ParquetWriterTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("seseragi-parquet-write").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private string Path(string name) => System.IO.Path.Combine(_dir, name);

    internal static IParquetCompression Codec(string name) => name switch
    {
        "none" => NoCompression.Instance,
        "snappy" => SnappyCompression.Instance,
        "zstd" => new ZstandardCompression(5),
        "gzip" => new GzipCompression(),
        "brotli" => new BrotliCompression(3),
        _ => Lz4RawCompression.Instance,
    };

    /// <summary>The rows of a file through our reader (checksums verified), as canonical text per column.</summary>
    internal static List<string>[] ReadBack(string path, int columns, out Apache.Arrow.Schema schema)
    {
        using var file = ParquetFile.Open(path);
        schema = file.Schema;
        using var reader = file.OpenReader(new ParquetReadOptions { VerifyChecksums = true });
        List<RecordBatch> batches = [.. reader.ReadBatches()];
        try
        {
            return Columns(batches, columns);
        }
        finally
        {
            foreach (RecordBatch b in batches) b.Dispose();
        }
    }

    public static TheoryData<ParquetPageVersion, string, bool> Layouts()
    {
        TheoryData<ParquetPageVersion, string, bool> data = [];
        foreach (ParquetPageVersion version in (ParquetPageVersion[])[ParquetPageVersion.V1, ParquetPageVersion.V2])
        {
            foreach (string codec in (string[])["none", "snappy", "zstd", "gzip", "brotli", "lz4"])
            {
                data.Add(version, codec, true);
                data.Add(version, codec, false);
            }
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public void EveryType_ComesBack_InEveryLayout(ParquetPageVersion version, string codec, bool dictionary)
    {
        int seed = HashCode.Combine(version, codec, dictionary) & 0xFFFF;
        RecordBatch batch = Wide(seed, 2500);
        string path = Path("wide.parquet");
        var options = new ParquetWriteOptions
        {
            DataPageVersion = version, Compression = Codec(codec), Dictionary = dictionary,
            DataPageRows = 97, DataPageBytes = 3000, RowGroupRows = 1000, DictionaryPageBytes = 400,
        };
        // Slices start mid-bitmap; an empty batch and a one-row batch on the way.
        ParquetWriter.Write(path, batch.Schema, [batch.Slice(0, 701), batch.Slice(701, 0), batch.Slice(701, 1), batch.Slice(702, 1798)], options);
        List<string>[] read = ReadBack(path, batch.ColumnCount, out var schema);
        List<string>[] written = Columns([batch], batch.ColumnCount);
        for (int c = 0; c < batch.ColumnCount; c++)
            Assert.True(written[c].SequenceEqual(read[c]), $"seed {seed}: column {batch.Schema.FieldsList[c].Name} differs at row {written[c].Zip(read[c]).TakeWhile(p => p.First == p.Second).Count()}");
        Assert.Equal(batch.Schema.FieldsList.Select(f => f.Name), schema.FieldsList.Select(f => f.Name));
        // What the stored Arrow schema restores: zone names, seconds, durations, string dictionaries.
        Assert.Equal("Asia/Tokyo", ((TimestampType)schema.GetFieldByName("ts_us_tokyo").DataType).Timezone);
        Assert.Equal(TimeUnit.Second, ((TimestampType)schema.GetFieldByName("ts_s").DataType).Unit);
        Assert.IsType<DurationType>(schema.GetFieldByName("dur_s").DataType);
        Assert.IsType<DictionaryType>(schema.GetFieldByName("dict_str").DataType);
        Assert.False(schema.GetFieldByName("req_i64").IsNullable);
        using var file = ParquetFile.Open(path);
        Assert.Equal(3, file.Metadata.RowGroups.Count);
    }

    [Fact]
    public void AWriterHoldingAFewRows_KeepsLittleScratch()
    {
        // A partition's writer holds its open page long, many writers at once: no buffer is taken before it is needed
        // (the dictionary indices only once a page is encoded).
        var batch = Batch(("a", Fixed(Int64Type.Default, [(long?)1, 2, 3]), true), ("s", Strings(["x", "y", null]), true));
        using var stream = new MemoryStream();
        using var writer = ParquetWriter.Create(stream, batch.Schema, new ParquetWriteOptions { LeaveOpen = true });
        writer.Write(batch);
        Assert.InRange(writer.ScratchBytes, 0, 128 * 1024); // some 110 KB: the strings' first 64 KiB, the open pages' buffers
    }

    [Fact]
    public void LevelsBeyond255_AreRefused_255Written()
    {
        // Optional structs nested around an optional leaf: the leaf's definition level is the number of fields.
        static Schema Nested(int fields)
        {
            Field field = new("v", Int32Type.Default, true);
            for (int f = 1; f < fields; f++) field = new Field("s", new StructType([field]), true);
            return new Schema([field], null);
        }

        using (var stream = new MemoryStream()) ParquetWriter.Write(stream, Nested(255), []);
        using var refused = new MemoryStream();
        Assert.Contains("deeper than 255", Assert.Throws<NotSupportedException>(() => ParquetWriter.Write(refused, Nested(256), [])).Message);
    }

    [Fact]
    public void Int96_AndCoarserTimestamps_ComeBack()
    {
        var micros = new TimestampType(TimeUnit.Microsecond, "UTC");
        var nanos = new TimestampType(TimeUnit.Nanosecond, (string?)null);
        RecordBatch batch = Batch(
            ("us", Fixed(micros, [(long?)0, -1, 1_700_000_000_123_000, null, -3_000_000_000_000_000]), true),
            ("ns", Fixed(nanos, [(long?)0, -1_000, 1_700_000_000_123_456_000, null, long.MinValue / 1000 * 1000]), true));
        string int96 = Path("int96.parquet");
        ParquetWriter.Write(int96, batch.Schema, [batch], new ParquetWriteOptions { Int96Timestamps = true });
        Assert.Equal(Columns([batch], 2), ReadBack(int96, 2, out var schema));
        using (var file = ParquetFile.Open(int96))
        {
            Assert.All(file.Metadata.Thrift!.Schema.Skip(1), e => Assert.Equal(ParquetType.Int96, e.Type));
            Assert.All(file.Metadata.RowGroups[0].Columns, c => Assert.Null(c.Statistics?.Min)); // no bounds for INT96
        }
        string millis = Path("millis.parquet");
        var whole = Batch(("us", Fixed(micros, [(long?)0, -1000, 1_700_000_000_123_000, null]), true));
        ParquetWriter.Write(millis, whole.Schema, [whole], new ParquetWriteOptions { TimestampUnit = TimeUnit.Millisecond });
        Assert.Equal(Columns([whole], 1), ReadBack(millis, 1, out _));
        using (var file = ParquetFile.Open(millis))
            Assert.Equal(ParquetTimeUnit.Millis, file.Metadata.Thrift!.Schema[1].LogicalType!.Unit);
        Assert.Contains("cannot hold exactly", Assert.Throws<NotSupportedException>(() =>
            ParquetWriter.Write(Path("inexact.parquet"), batch.Schema, [batch], new ParquetWriteOptions { TimestampUnit = TimeUnit.Millisecond })).Message);
        Assert.False(File.Exists(Path("inexact.parquet")));
    }

    [Fact]
    public void Int96_ReadsInTheGivenZone_UnlessTheFileNamesOne()
    {
        var naive = new TimestampType(TimeUnit.Microsecond, (string?)null);
        RecordBatch batch = Batch(("ts", Fixed(naive, [(long?)1_700_000_000_123_456, null]), true));
        string bare = Path("int96_bare.parquet"), described = Path("int96_described.parquet");
        // As Spark writes them: no Arrow schema to say what the values are.
        ParquetWriter.Write(bare, batch.Schema, [batch], new ParquetWriteOptions { Int96Timestamps = true, StoreArrowSchema = false });
        ParquetWriter.Write(described, batch.Schema, [batch], new ParquetWriteOptions { Int96Timestamps = true });

        static (TimestampType Type, long? First) Read(string path, string? zone)
        {
            using var file = ParquetFile.Open(path);
            using var reader = file.OpenReader(new ParquetReadOptions { Int96Timezone = zone });
            using RecordBatch read = reader.ReadBatches().Single();
            return ((TimestampType)reader.Schema.FieldsList[0].DataType, ((TimestampArray)read.Column(0)).GetValue(0));
        }

        Assert.Equal((null, 1_700_000_000_123_456), ((string?)Read(bare, null).Type.Timezone, Read(bare, null).First));
        var utc = Read(bare, "UTC");
        Assert.Equal(("UTC", TimeUnit.Microsecond, 1_700_000_000_123_456L), (utc.Type.Timezone, utc.Type.Unit, utc.First));
        // The file's Arrow schema says wall-clock values: it wins over the option.
        Assert.Null(Read(described, "UTC").Type.Timezone);
    }

    [Fact]
    public void NoBatches_MakeAFileWithoutRowGroups()
    {
        RecordBatch batch = Wide(1, 3);
        string path = Path("empty.parquet");
        using (var writer = ParquetWriter.Create(path, batch.Schema)) { }
        using var file = ParquetFile.Open(path);
        Assert.Equal(0, file.Metadata.RowCount);
        Assert.Empty(file.Metadata.RowGroups);
        Assert.Equal(batch.Schema.FieldsList.Select(f => f.Name), file.Schema.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public void RowGroups_CloseOnRows_OnBytes_AndOnRequest()
    {
        RecordBatch batch = Wide(2, 1000);
        string path = Path("groups.parquet");
        ParquetMetadata written;
        long bytes, length;
        using (var stream = File.Create(path))
        {
            using (var writer = ParquetWriter.Create(stream, batch.Schema, new ParquetWriteOptions { RowGroupRows = 400, LeaveOpen = true }))
            {
                writer.Write(batch.Slice(0, 300));
                Assert.Equal(0, writer.BytesWritten); // nothing before a row group closes
                Assert.True(writer.PendingBytes > 0);
                writer.EndRowGroup();
                writer.EndRowGroup(); // nothing open: nothing happens
                Assert.True(writer.BytesWritten > 0);
                writer.Write(batch.Slice(300, 700)); // 400 + 300 rows
                Assert.Throws<InvalidOperationException>(() => writer.Metadata);
                writer.Complete();
                written = writer.Metadata;
                bytes = writer.BytesWritten;
            }
            length = stream.Length;
        }
        Assert.Equal(bytes, length);
        Assert.Equal([300L, 400, 300], written.RowGroups.Select(g => g.RowCount));
        using var file = ParquetFile.Open(path);
        Assert.Equal(written.RowGroups.Select(g => g.RowCount), file.Metadata.RowGroups.Select(g => g.RowCount));
        Assert.Equal(written.RowGroups.SelectMany(g => g.Columns.Select(c => c.Statistics?.NullCount)),
            file.Metadata.RowGroups.SelectMany(g => g.Columns.Select(c => c.Statistics?.NullCount)));

        // A byte budget closes groups before the row count does, checked after each segment the columns take at once
        // (a batch, or 16 384 rows of it): batches of 100 rows, groups of a few batches; one batch of 1000, one group.
        string small = Path("small.parquet");
        ParquetWriter.Write(small, batch.Schema, [.. Enumerable.Range(0, 10).Select(i => batch.Slice(i * 100, 100))],
            new ParquetWriteOptions { RowGroupBytes = 20_000, DataPageRows = 50 });
        using var smallFile = ParquetFile.Open(small);
        Assert.True(smallFile.Metadata.RowGroups.Count > 2, $"{smallFile.Metadata.RowGroups.Count} row groups");
        Assert.All(smallFile.Metadata.RowGroups, g => Assert.Equal(0, g.RowCount % 100));
        Assert.Equal(Columns([batch], batch.ColumnCount), ReadBack(small, batch.ColumnCount, out _));
        string whole = Path("whole.parquet");
        ParquetWriter.Write(whole, batch.Schema, [batch], new ParquetWriteOptions { RowGroupBytes = 20_000, DataPageRows = 50 });
        using var wholeFile = ParquetFile.Open(whole);
        Assert.Single(wholeFile.Metadata.RowGroups);
    }

    /// <summary>The dictionary is judged on the first page holding values, not on pages of nulls before it.</summary>
    [Fact]
    public void Dictionary_IsJudgedOnTheFirstPageWithValues()
    {
        string?[] values = [.. Enumerable.Range(0, 5000).Select(i => i < 2500 ? null : "v" + i % 7)];
        RecordBatch batch = Batch(("s", Strings(values), true), ("n", Fixed(Int64Type.Default, [.. values.Select((v, i) => v is null ? null : (long?)(i % 5))]), true));
        string path = Path("nullfirst.parquet");
        ParquetWriter.Write(path, batch.Schema, [batch], new ParquetWriteOptions { DataPageRows = 1000 });
        using var file = ParquetFile.Open(path);
        foreach (ColumnChunk chunk in file.Metadata.Thrift!.RowGroups[0].Columns)
        {
            PageEncodingStats[] stats = chunk.MetaData!.EncodingStats!;
            Assert.NotNull(chunk.MetaData.DictionaryPageOffset);
            Assert.Contains(stats, s => s.Encoding == ParquetEncoding.RleDictionary && s.Count == 3); // the pages with values
            Assert.Contains(stats, s => s.PageType != PageType.DictionaryPage && s.Encoding == ParquetEncoding.Plain && s.Count == 2); // the null pages before
        }
        Assert.Equal(Columns([batch], 2), ReadBack(path, 2, out _));
    }

    [Fact]
    public void TheFooter_DescribesTheChunks()
    {
        int n = 5000;
        RecordBatch batch = Batch(
            ("few", Fixed(Int64Type.Default, [.. Enumerable.Range(0, n).Select(i => (long?)(i % 10))]), false),
            ("unique", Fixed(Int64Type.Default, [.. Enumerable.Range(0, n).Select(i => (long?)i * 7919)]), false),
            ("grows", Strings([.. Enumerable.Range(0, n).Select(i => i < 500 ? $"v{i % 3}" : $"distinct-{i}")]), true));
        string path = Path("footer.parquet");
        ParquetWriter.Write(path, batch.Schema, [batch], new ParquetWriteOptions
        {
            DataPageRows = 500, DictionaryPageBytes = 4000, Metadata = new Dictionary<string, string> { ["owner"] = "test" },
        });
        using var file = ParquetFile.Open(path);
        FileMetaData footer = file.Metadata.Thrift!;
        Assert.StartsWith("seseragi version ", footer.CreatedBy);
        Assert.Equal(2, footer.Version);
        Assert.Equal(3, footer.ColumnOrders!.Length);
        Assert.All(footer.ColumnOrders, o => Assert.Equal(ColumnOrder.TypeDefinedOrder, o));
        Assert.Contains(footer.KeyValueMetadata!, kv => kv is { Key: "owner", Value: "test" });
        Assert.Contains(footer.KeyValueMetadata!, kv => kv.Key == "ARROW:schema");
        ColumnMetaData[] chunks = [.. footer.RowGroups[0].Columns.Select(c => c.MetaData!)];

        // Few distinct values: a dictionary page first, then dictionary-encoded pages only.
        Assert.Equal([ParquetEncoding.Plain, ParquetEncoding.Rle, ParquetEncoding.RleDictionary], chunks[0].Encodings);
        Assert.Equal(chunks[0].DictionaryPageOffset, footer.RowGroups[0].Columns[0].FileOffset);
        Assert.True(chunks[0].DataPageOffset > chunks[0].DictionaryPageOffset);
        Assert.Equal([new PageEncodingStats(PageType.DictionaryPage, ParquetEncoding.Plain, 1), new PageEncodingStats(PageType.DataPage, ParquetEncoding.RleDictionary, 10)],
            chunks[0].EncodingStats!);
        // Unique values: the first page shows that a dictionary does not pay; no dictionary page.
        Assert.Null(chunks[1].DictionaryPageOffset);
        Assert.Equal([ParquetEncoding.Plain, ParquetEncoding.Rle], chunks[1].Encodings);
        // A dictionary outgrowing its budget: dictionary pages, then plain ones.
        Assert.NotNull(chunks[2].DictionaryPageOffset);
        Assert.Contains(chunks[2].EncodingStats!, s => s.Encoding == ParquetEncoding.Plain && s.PageType == PageType.DataPage);
        Assert.Contains(chunks[2].EncodingStats!, s => s.Encoding == ParquetEncoding.RleDictionary);
        Assert.All(chunks, c => Assert.Equal(n, c.NumValues));
        Assert.Equal(chunks.Sum(c => c.TotalCompressedSize), footer.RowGroups[0].TotalCompressedSize);
        Assert.Equal(Columns([batch], 3), ReadBack(path, 3, out _));

        // Without dictionaries: plain pages only.
        string plain = Path("plain.parquet");
        ParquetWriter.Write(plain, batch.Schema, [batch], new ParquetWriteOptions { Dictionary = false });
        using var plainFile = ParquetFile.Open(plain);
        Assert.All(plainFile.Metadata.Thrift!.RowGroups[0].Columns, c => Assert.Null(c.MetaData!.DictionaryPageOffset));
    }

    [Fact]
    public async Task TheAsyncApi_WritesTheSameFile()
    {
        RecordBatch batch = Wide(3, 600);
        string sync = Path("sync.parquet"), async = Path("async.parquet");
        var options = new ParquetWriteOptions { RowGroupRows = 250 };
        ParquetWriter.Write(sync, batch.Schema, [batch.Slice(0, 100), batch.Slice(100, 500)], options);
        await ParquetWriter.WriteAsync(async, batch.Schema, ((RecordBatch[])[batch.Slice(0, 100), batch.Slice(100, 500)]).ToAsyncEnumerable(), options);
        Assert.Equal(File.ReadAllBytes(sync), File.ReadAllBytes(async));

        string instance = Path("instance.parquet");
        await using (var writer = ParquetWriter.Create(instance, batch.Schema, options))
        {
            await writer.WriteAsync(batch.Slice(0, 100));
            await writer.WriteAsync(batch.Slice(100, 500));
        }
        Assert.Equal(File.ReadAllBytes(sync), File.ReadAllBytes(instance));
    }

    [Fact]
    public void Writes_AreRefused_WithReasons()
    {
        RecordBatch batch = Wide(4, 10);
        // A null in a required column is found before anything is written; the writer goes on.
        var required = new Apache.Arrow.Schema([new Field("x", Int64Type.Default, false)], null);
        using (var writer = ParquetWriter.Create(Path("req.parquet"), required))
        {
            var withNull = Batch(("x", Fixed(Int64Type.Default, [(long?)1, null]), true));
            Assert.Contains("not nullable", Assert.Throws<ArgumentException>(() => writer.Write(withNull)).Message);
            writer.Write(Batch(("x", Fixed(Int64Type.Default, [(long?)1, 2]), false)));
        }
        Assert.Equal([["1", "2"]], ReadBack(Path("req.parquet"), 1, out _));
        // Columns shorter or longer than the batch.
        using (var writer = ParquetWriter.Create(Path("rows.parquet"), required))
        {
            var uneven = new RecordBatch(required, [Fixed(Int64Type.Default, [(long?)1, 2])], 3);
            Assert.Contains("has 2 rows; the batch has 3", Assert.Throws<ArgumentException>(() => writer.Write(uneven)).Message);
        }
        // A batch of other types.
        using (var writer = ParquetWriter.Create(Path("types.parquet"), required))
            Assert.Contains("in the writer schema", Assert.Throws<ArgumentException>(() => writer.Write(Batch(("x", Fixed(Int32Type.Default, [(int?)1]), false)))).Message);
        // Values Parquet cannot hold exactly; the failed file is deleted.
        var dates = Batch(("d", Fixed(Date64Type.Default, [(long?)86_400_000, 1]), true));
        Assert.Contains("whole day", Assert.Throws<NotSupportedException>(() => ParquetWriter.Write(Path("d.parquet"), dates.Schema, [dates])).Message);
        Assert.False(File.Exists(Path("d.parquet")));
        var wide = Batch(("dec", Fixed(new Decimal128Type(5, 0), [(Int128?)100_000]), true));
        Assert.Contains("precision", Assert.Throws<NotSupportedException>(() => ParquetWriter.Write(Path("dec.parquet"), wide.Schema, [wide])).Message);
        var interval = Batch(("iv", Fixed(new IntervalType(IntervalUnit.MonthDayNanosecond), [(MonthDayNano?)new MonthDayNano(1, 1, 5)]), true));
        Assert.Contains("INTERVAL", Assert.Throws<NotSupportedException>(() => ParquetWriter.Write(Path("iv.parquet"), interval.Schema, [interval])).Message);
        // A failed writer refuses to go on and leaves no footer.
        using (var stream = new MemoryStream())
        {
            var writer = ParquetWriter.Create(stream, dates.Schema, new ParquetWriteOptions { LeaveOpen = true });
            Assert.Throws<NotSupportedException>(() => writer.Write(dates));
            Assert.Throws<InvalidOperationException>(() => writer.Write(dates));
            writer.Dispose();
            Assert.Equal(0, stream.Length);
        }
        // Options.
        Assert.Throws<ArgumentException>(() => ParquetWriter.Create(new MemoryStream(), batch.Schema,
            new ParquetWriteOptions { Metadata = new Dictionary<string, string> { ["ARROW:schema"] = "x" } }));
        Assert.Throws<ArgumentException>(() => ParquetWriter.Create(new MemoryStream(), batch.Schema, new ParquetWriteOptions { TimestampUnit = TimeUnit.Second }));
        Assert.Throws<ArgumentOutOfRangeException>(() => ParquetWriter.Create(new MemoryStream(), batch.Schema, new ParquetWriteOptions { DataPageRows = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => ParquetWriter.Create(new MemoryStream(), batch.Schema, new ParquetWriteOptions { StatisticsTruncateLength = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new GzipCompression(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new BrotliCompression(12));
    }
}
