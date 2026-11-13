using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Codecs;
using Kaminari.Seseragi.Files.Parquet.Encodings;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>The reader's options, internals (row ranges, codecs, decoders) and behaviour on damaged pages.</summary>
public class ParquetReaderTests
{
    private const string TinyPages = "apache/data/alltypes_tiny_pages.parquet";

    private static List<string> Rows(IEnumerable<RecordBatch> batches)
    {
        List<string> rows = [];
        foreach (RecordBatch b in batches)
        {
            for (int i = 0; i < b.Length; i++) rows.Add(string.Join(" | ", Enumerable.Range(0, b.ColumnCount).Select(c => CanonicalRender.Value(b.Column(c), i))));
            b.Dispose();
        }
        return rows;
    }

    [Theory]
    [InlineData(TinyPages)]
    [InlineData("matrix/pa_pages_rowgroups.parquet")]
    [InlineData("matrix/pa_dict_v2_zstd.parquet")]
    [InlineData("apache/data/rle_boolean_encoding.parquet")]
    public async Task RowRanges_ReadWhatAFullReadSlices(string name)
    {
        using var file = ParquetFile.Open(ParquetFixtures.FullPath(name));
        using var reader = file.OpenReader(new() { Columns = ParquetValueTests.FlatColumns(file.Schema), BatchSize = 13 });
        for (int g = 0; g < file.Thrift.RowGroups.Length; g++)
        {
            long rows = file.Thrift.RowGroups[g].NumRows;
            List<string> full;
            using (var chunks = await reader.Fetch(g, useAsync: true, default)) full = Rows(reader.Decode(g, chunks));
            Assert.Equal(rows, full.Count);
            (long Start, long Count)[] ranges = [(1, 3), (rows / 3, 17), (rows / 2 + 5, 1), (rows - 2, 2)];
            ranges = [.. ranges.Where(r => r.Start + r.Count <= rows).DistinctBy(r => r.Start).OrderBy(r => r.Start)];
            ranges = [.. ranges.Where((r, i) => i == 0 || r.Start >= ranges[i - 1].Start + ranges[i - 1].Count)];
            List<string> sliced;
            using (var chunks = await reader.Fetch(g, useAsync: false, default)) sliced = Rows(reader.Decode(g, chunks, ranges));
            Assert.Equal([.. ranges.SelectMany(r => full.Skip((int)r.Start).Take((int)r.Count))], sliced);
        }
    }

    [Fact]
    public async Task AsyncRead_EqualsSyncRead()
    {
        foreach (string name in (string[])[TinyPages, "matrix/pa_plain_v2_gzip.parquet", "writers/spark_lz4.parquet"])
        {
            await using var file = await ParquetFile.OpenAsync(ParquetFixtures.FullPath(name));
            var options = new ParquetReadOptions { Columns = ParquetValueTests.FlatColumns(file.Schema), BatchSize = 100 };
            List<string> sync;
            using (var reader = file.OpenReader(options)) sync = Rows(reader.ReadBatches());
            List<RecordBatch> batches = [];
            await using (var reader = file.OpenReader(options))
            {
                await foreach (RecordBatch b in reader.ReadBatchesAsync()) batches.Add(b);
            }
            Assert.Equal(sync, Rows(batches));
        }
    }

    [Theory]
    [InlineData("apache/data/datapage_v1-corrupt-checksum.parquet")]
    [InlineData("apache/data/rle-dict-uncompressed-corrupt-checksum.parquet")]
    public void CorruptChecksums_FailOnlyWhenVerified(string name)
    {
        using var file = ParquetFile.Open(ParquetFixtures.FullPath(name));
        using (var reader = file.OpenReader()) Assert.NotEmpty(Rows(reader.ReadBatches()));
        using var verifying = file.OpenReader(new() { VerifyChecksums = true });
        Assert.Contains("CRC", Assert.Throws<ParquetFormatException>(() => Rows(verifying.ReadBatches())).Message);
    }

    [Theory]
    [InlineData("apache/data/datapage_v1-uncompressed-checksum.parquet")]
    [InlineData("apache/data/datapage_v1-snappy-compressed-checksum.parquet")]
    [InlineData("apache/data/plain-dict-uncompressed-checksum.parquet")]
    [InlineData("apache/data/rle-dict-snappy-checksum.parquet")]
    [InlineData("matrix/pa_pages_rowgroups.parquet")]
    public void ValidChecksums_Verify(string name)
    {
        using var file = ParquetFile.Open(ParquetFixtures.FullPath(name));
        using var reader = file.OpenReader(new() { VerifyChecksums = true, Columns = ParquetValueTests.FlatColumns(file.Schema) });
        Assert.NotEmpty(Rows(reader.ReadBatches()));
    }

    [Fact]
    public void Options_ProjectSelectAndValidate()
    {
        using var file = ParquetFile.Open(ParquetFixtures.FullPath("matrix/pa_pages_rowgroups.parquet"));
        using (var reader = file.OpenReader(new() { Columns = ["str", "i8", "bool"] }))
        {
            Assert.Equal(["bool", "i8", "str"], reader.Schema.FieldsList.Select(f => f.Name)); // file order
            Assert.Equal(300, reader.ReadBatches().Sum(b => { int n = b.Length; b.Dispose(); return n; }));
        }
        using (var reader = file.OpenReader(new() { Columns = ["i32"], RowGroups = [3, 0] }))
        {
            int[] lengths = [.. reader.ReadBatches().Select(b => { int n = b.Length; b.Dispose(); return n; })];
            Assert.Equal([file.Thrift.RowGroups[3].NumRows, file.Thrift.RowGroups[0].NumRows], lengths.Select(n => (long)n));
        }
        using (var reader = file.OpenReader(new() { Columns = [] }))
        {
            RecordBatch[] batches = [.. reader.ReadBatches()];
            Assert.Equal(300, batches.Sum(b => b.Length));
            Assert.All(batches, b => Assert.Equal(0, b.ColumnCount));
        }
        Assert.Throws<ArgumentException>(() => file.OpenReader(new() { Columns = ["nope"] }));
        Assert.Throws<ArgumentOutOfRangeException>(() => file.OpenReader(new() { RowGroups = [99] }));
        Assert.Throws<ArgumentOutOfRangeException>(() => file.OpenReader(new() { BatchSize = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => file.OpenReader(new() { Int96Unit = TimeUnit.Millisecond }));

        using var nested = ParquetFile.Open(ParquetFixtures.FullPath("writers/pyarrow.parquet"));
        Assert.Contains("'list_i'", Assert.Throws<NotSupportedException>(() => nested.OpenReader()).Message);
        using var flatOnly = nested.OpenReader(new() { Columns = ["ts_utc", "dict_str"] });
        Assert.Equal(3, flatOnly.ReadBatches().Sum(b => { int n = b.Length; b.Dispose(); return n; }));
    }

    [Fact]
    public void Schema_MapsTypesAnnotationsAndFieldIds()
    {
        using var file = ParquetFile.Open(ParquetFixtures.FullPath("writers/pyarrow.parquet"));
        Field Of(string name) => file.Schema.FieldsList.Single(f => f.Name == name);
        Assert.Equal("Asia/Tokyo", ((TimestampType)Of("ts_tokyo").DataType).Timezone);
        Assert.IsType<DurationType>(Of("dur_us").DataType);
        Assert.IsType<StringType>(Of("large_str").DataType);
        Assert.IsType<DictionaryType>(Of("dict_str").DataType);
        Assert.Equal(2, Assert.IsType<FixedSizeListType>(Of("fsl").DataType).ListSize);
        Assert.IsType<MapType>(Of("map").DataType);
        Assert.Equal(["x", "y"], Assert.IsType<StructType>(Of("struct").DataType).Fields.Select(f => f.Name));

        using var polars = ParquetFile.Open(ParquetFixtures.FullPath("writers/polars.parquet"));
        Assert.Contains(polars.Schema.FieldsList.Single(f => f.Name == "enum_str").Metadata, kv => kv.Key == "_PL_ENUM_VALUES2");

        using var spark = ParquetFile.Open(ParquetFixtures.FullPath("writers/spark_default.parquet"));
        Assert.Equal(new TimestampType(TimeUnit.Microsecond, (string?)null).Name, spark.Schema.FieldsList[0].DataType.Name);
        Assert.Null(((TimestampType)spark.Schema.FieldsList[0].DataType).Timezone);
        var arr = Assert.IsType<ListType>(spark.Schema.FieldsList.Single(f => f.Name == "arr").DataType);
        Assert.Equal("element", arr.ValueField.Name);
    }

    // ------------------------------------------------------------------
    // Codecs and decoders
    // ------------------------------------------------------------------

    /// <summary>The codecs written (by their Thrift values: the enum is internal).</summary>
    public static TheoryData<int> WritableCodecs =>
        [.. ((CompressionCodec[])[CompressionCodec.Uncompressed, CompressionCodec.Snappy, CompressionCodec.Gzip, CompressionCodec.Brotli, CompressionCodec.Zstd, CompressionCodec.Lz4Raw]).Select(c => (int)c)];

    [Theory]
    [MemberData(nameof(WritableCodecs))]
    public void Codecs_RoundTrip(int codecValue)
    {
        var codec = (CompressionCodec)codecValue;
        var random = new Random(7);
        byte[][] inputs =
        [
            [],
            [42],
            [.. Enumerable.Range(0, 100_000).Select(i => (byte)(i % 7))],
            [.. Enumerable.Range(0, 70_000).Select(_ => (byte)random.Next(256))],
            [.. Enumerable.Range(0, 50_000).Select(i => (byte)(i / 1000 + (i % 13 == 0 ? random.Next(3) : 0)))],
        ];
        foreach (byte[] input in inputs)
        {
            byte[] compressed = new byte[PageCodecs.MaxCompressedLength(codec, input.Length)];
            int n = PageCodecs.Compress(codec, input, compressed);
            byte[] output = new byte[input.Length];
            PageCodecs.Decompress(codec, compressed.AsSpan(0, n), output);
            Assert.Equal(input, output);
            Assert.Throws<ParquetFormatException>(() => PageCodecs.Decompress(codec, compressed.AsSpan(0, n), new byte[input.Length + 1]));
        }
    }

    [Fact]
    public void Lz4Block_RefusesDamagedBlocks()
    {
        byte[] input = [.. Enumerable.Range(0, 5000).Select(i => (byte)(i % 11))];
        byte[] compressed = new byte[Lz4Block.MaxCompressedLength(input.Length)];
        int n = Lz4Block.Compress(input, compressed);
        var random = new Random(3);
        for (int i = 0; i < 3000; i++)
        {
            byte[] damaged = compressed[..n];
            damaged[random.Next(n)] = (byte)random.Next(256);
            Exception? e = Record.Exception(() => Lz4Block.Decompress(damaged.AsSpan(0, random.Next(n + 1)), new byte[input.Length]));
            if (e is not null) Assert.IsType<ParquetFormatException>(e);
        }
    }

    [Fact]
    public void RleBitPacked_ReadsAndSkipsRunsOfBothKinds()
    {
        // RLE run of 10 × 5, bit-packed run of 8 values 0..7 (width 3), RLE run of 3 × 1.
        byte[] data = [10 << 1, 5, (1 << 1) | 1, 0b10001000, 0b11000110, 0b11111010, 3 << 1, 1];
        var decoder = new RleBitPackedDecoder(data, 3);
        Span<int> all = stackalloc int[30];
        Assert.Equal(21, decoder.Read(all));
        Assert.Equal([5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 0, 1, 2, 3, 4, 5, 6, 7, 1, 1, 1], all[..21].ToArray());

        var skipping = new RleBitPackedDecoder(data, 3);
        Assert.Equal(12, skipping.Skip(12));
        Span<int> rest = stackalloc int[4];
        Assert.Equal(4, skipping.Read(rest));
        Assert.Equal([2, 3, 4, 5], rest.ToArray());

        // A last bit-packed run cut short: 1 group (8 values) declared, 2 bytes there: 5 values of 3 bits.
        var cut = new RleBitPackedDecoder([(1 << 1) | 1, 0b10001000, 0b11000110], 3);
        Span<int> some = stackalloc int[8];
        Assert.Equal(5, cut.Read(some));
        Assert.Equal([0, 1, 2, 3, 4], some[..5].ToArray());

        Assert.Throws<ParquetFormatException>(() => new RleBitPackedDecoder(data, 33));
        Assert.Throws<ParquetFormatException>(() => new RleBitPackedDecoder([2 << 1, 9], 3).Read(stackalloc int[2])); // 9 is wider than 3 bits
        Assert.Throws<ParquetFormatException>(() => new RleBitPackedDecoder([0x80], 3).Read(stackalloc int[1])); // header cut short
    }

    [Fact]
    public void LegacyBitPacked_IsMostSignificantBitFirst()
    {
        // Encodings.md: 0..7 at width 3 is 00000101 00111001 01110111.
        Span<byte> values = stackalloc byte[8];
        BitPacking.UnpackMsbFirst([0b00000101, 0b00111001, 0b01110111], 0, 3, values);
        Assert.Equal([0, 1, 2, 3, 4, 5, 6, 7], values.ToArray());
    }

    // ------------------------------------------------------------------
    // Damaged data
    // ------------------------------------------------------------------

    /// <summary>Every bad_data file reads (flat columns) or fails with ParquetFormatException; nothing else escapes.</summary>
    [Fact]
    public void BadData_FailsOnlyWithParquetFormatException()
    {
        foreach (string name in ParquetFixtures.All.Where(n => n.StartsWith("apache/bad_data/", StringComparison.Ordinal)))
        {
            Exception? e = Record.Exception(() =>
            {
                using var file = ParquetFile.Open(ParquetFixtures.FullPath(name));
                string[] flat = ParquetValueTests.FlatColumns(file.Schema);
                using var reader = file.OpenReader(new() { Columns = flat, VerifyChecksums = true });
                Rows(reader.ReadBatches());
            });
            if (e is not null) Assert.True(e is ParquetFormatException, $"{name}: {e}");
        }
    }

    /// <summary>Bytes flipped in the data pages (the footer intact) fail with ParquetFormatException or read; nothing else.</summary>
    [Theory]
    [InlineData(TinyPages, 300)]
    [InlineData("matrix/pa_dict_v1_snappy.parquet", 300)]
    [InlineData("matrix/pa_plain_v2_gzip.parquet", 200)]
    [InlineData("matrix/pa_plain_v1.parquet", 300)]
    [InlineData("matrix/duckdb_zstd.parquet", 200)]
    [InlineData("matrix/pa_plain_v2_lz4raw.parquet", 200)]
    [InlineData("writers/spark_lz4.parquet", 200)]
    [InlineData("apache/data/hadoop_lz4_compressed.parquet", 200)]
    public void DamagedPages_FailCleanly(string name, int rounds)
    {
        byte[] original = File.ReadAllBytes(ParquetFixtures.FullPath(name));
        var (footerStart, _) = ParquetMetadata.FooterRange(original, original.Length, name);
        var random = new Random(name.Length * 31 + rounds);
        for (int i = 0; i < rounds; i++)
        {
            byte[] damaged = (byte[])original.Clone();
            for (int f = random.Next(1, 4); f > 0; f--) damaged[random.Next(4, (int)footerStart)] = (byte)random.Next(256);
            Exception? e = Record.Exception(() =>
            {
                using var file = ParquetFile.Open(new MemoryStream(damaged));
                using var reader = file.OpenReader(new() { Columns = ParquetValueTests.FlatColumns(file.Schema), BatchSize = 97 });
                Rows(reader.ReadBatches());
            });
            if (e is not null) Assert.True(e is ParquetFormatException, $"{name} round {i}: {e}");
        }
    }
}
