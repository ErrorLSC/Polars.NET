using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Codecs;
using Kaminari.Seseragi.Files.Parquet.Encodings;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Reading;

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
    [InlineData("nested/pa_nested_small_pages.parquet")]
    [InlineData("nested/pa_nested_v2_dict.parquet")]
    [InlineData("nested/delta_checkpoint_classic.parquet")]
    [InlineData("matrix/enc_dba_small_pages.parquet")]
    [InlineData("matrix/enc_dlba_small_pages.parquet")]
    [InlineData("matrix/enc_dbp_small_pages.parquet")]
    [InlineData("matrix/enc_bss_small_pages.parquet")]
    [InlineData("matrix/enc_spark_v2.parquet")]
    public async Task RowRanges_ReadWhatAFullReadSlices(string name)
    {
        using var file = ParquetFile.Open(ParquetFixtures.FullPath(name));
        using var reader = file.OpenReader(new() { BatchSize = 13 });
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
            var options = new ParquetReadOptions { BatchSize = 100 };
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
        foreach (ChunkReading reading in (ChunkReading[])[ChunkReading.Auto, ChunkReading.Fetch, ChunkReading.Stream])
        {
            using (var reader = file.OpenReader(new() { Reading = reading })) Assert.NotEmpty(Rows(reader.ReadBatches()));
            using var verifying = file.OpenReader(new() { VerifyChecksums = true, Reading = reading });
            Assert.Contains("CRC", Assert.Throws<ParquetFormatException>(() => Rows(verifying.ReadBatches())).Message);
        }
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
        foreach (ChunkReading reading in (ChunkReading[])[ChunkReading.Auto, ChunkReading.Fetch, ChunkReading.Stream])
        {
            using var reader = file.OpenReader(new() { VerifyChecksums = true, Reading = reading });
            Assert.NotEmpty(Rows(reader.ReadBatches()));
        }
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
        Assert.IsType<StringViewType>(Of("large_str").DataType); // views by default (large types are not kept)
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
                using var reader = file.OpenReader(new() { VerifyChecksums = true });
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
    [InlineData("nested/pa_nested_v1.parquet", 300)]
    [InlineData("nested/pa_nested_v2_dict.parquet", 300)]
    [InlineData("nested/delta_checkpoint_classic.parquet", 200)]
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
                using var reader = file.OpenReader(new() { BatchSize = 97 });
                Rows(reader.ReadBatches());
            });
            if (e is not null) Assert.True(e is ParquetFormatException, $"{name} round {i}: {e}");
        }
    }

    [Fact]
    public async Task Parallel_StoppingEarly_FreesTheRest_AndReportsErrorsInOrder()
    {
        // A file of many row groups, read with many threads.
        string path = ParquetFixtures.FullPath("matrix/pa_pages_rowgroups.parquet");
        using var file = ParquetFile.Open(path);
        Assert.True(file.Metadata.RowGroups.Count > 2);
        var options = new ParquetReadOptions { MaxDegreeOfParallelism = 4, BatchSize = 100 };

        using (var reader = file.OpenReader(options))
        {
            using RecordBatch first = reader.ReadBatches().First(); // the rest are cancelled and freed
            Assert.True(first.Length > 0);
        }
        using (var reader = file.OpenReader(options))
        {
            using var cancel = new CancellationTokenSource();
            int seen = 0;
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                await foreach (RecordBatch batch in reader.ReadBatchesAsync(cancel.Token))
                {
                    batch.Dispose();
                    if (++seen == 2) cancel.Cancel();
                }
            });
        }

        // A damaged later row group: the batches before it come first, then its error, as one thread reports it.
        byte[] bytes = File.ReadAllBytes(path);
        var meta = ParquetFile.Open(path).Thrift;
        var lastChunk = meta.RowGroups[^1].Columns[0].MetaData!;
        long at = lastChunk.DictionaryPageOffset is long d and >= 4 ? d : lastChunk.DataPageOffset;
        for (long k = at; k < at + 40 && k < bytes.Length; k++) bytes[k] ^= 0x5A;
        string Error(int parallelism, out int rowsBefore)
        {
            int rows = 0;
            using var damaged = ParquetFile.Open(new MemoryStream(bytes));
            using var reader = damaged.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = parallelism, BatchSize = 100 });
            try
            {
                foreach (RecordBatch batch in reader.ReadBatches())
                {
                    rows += batch.Length;
                    batch.Dispose();
                }
            }
            catch (ParquetFormatException e)
            {
                rowsBefore = rows;
                return e.Message;
            }
            rowsBefore = rows;
            return "";
        }
        string sequential = Error(1, out int sequentialRows);
        string parallel = Error(4, out int parallelRows);
        Assert.NotEqual("", sequential);
        Assert.Equal(sequential, parallel);
        Assert.Equal(sequentialRows, parallelRows);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AbsurdFooterSizes_AreRefusedCleanly_WithoutHugeAllocations(bool viewTypes)
    {
        // The footer's value count and uncompressed size size the buffers in advance; absurd ones must not allocate
        // gigabytes, and the mismatch with the pages is a format error.
        SchemaElement[] schema =
        [
            HandBuiltParquet.Group("schema", 2, repetition: null),
            HandBuiltParquet.Group("l", 1, converted: ConvertedType.List),
            HandBuiltParquet.Group("list", 1, FieldRepetition.Repeated),
            HandBuiltParquet.Leaf("element", ParquetType.Int32, FieldRepetition.Optional),
            HandBuiltParquet.Leaf("s", ParquetType.ByteArray, FieldRepetition.Optional, ConvertedType.Utf8),
        ];
        byte[] file = HandBuiltParquet.Build(schema, 2,
            // Large enough to ask for terabytes (not so large that the arithmetic wraps).
            new HandBuiltParquet.Column(["l", "list", "element"], ParquetType.Int32, 3, 1, [3, 3], [0, 0], HandBuiltParquet.Int32s(1, 2),
                FooterValues: 1L << 40, FooterUncompressedSize: 1L << 50),
            new HandBuiltParquet.Column(["s"], ParquetType.ByteArray, 1, 0, [1, 1], [], HandBuiltParquet.Strings("a", "b"),
                FooterUncompressedSize: 1L << 50));
        using var parquet = ParquetFile.Open(new MemoryStream(file));
        foreach (int parallelism in (int[])[1, 4])
        {
            using var reader = parquet.OpenReader(new ParquetReadOptions { ViewTypes = viewTypes, MaxDegreeOfParallelism = parallelism });
            var e = Assert.ThrowsAny<ParquetFormatException>(() =>
            {
                foreach (RecordBatch batch in reader.ReadBatches()) batch.Dispose();
            });
            Assert.Contains("more values than its row group has rows", e.Message);
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public void DictionaryPages_WithNulls_PlaceEveryValue(int parallelism)
    {
        // 70 rows (a whole word of the null mask and part of the next): every third row null, and rows 40–49.
        bool[] present = [.. Enumerable.Range(0, 70).Select(i => i % 3 != 0 && i is not (>= 40 and < 50))];
        byte[] Defs(Range rows) => [.. present[rows].Select(p => p ? (byte)1 : (byte)0)];
        int[] Picks(Range rows, int salt) => [.. Enumerable.Range(0, present[rows].Count(p => p)).Select(i => (i * 7 + salt) % 3)];

        bool[] flags = [true, false, true];
        decimal[] amounts = [123.45m, -0.01m, 1000.00m];
        string[] texts = ["the first long dictionary entry", "the second long dictionary entry", "the third long dictionary entry"];
        string[] plain = ["a long plain value number one", "a long plain value number two"];
        int[] flagPicks = Picks(.., 1), amountPicks = Picks(.., 2), textPicks = Picks(3.., 0);

        SchemaElement[] schema =
        [
            HandBuiltParquet.Group("schema", 3, repetition: null),
            HandBuiltParquet.Leaf("flag", ParquetType.Boolean, FieldRepetition.Optional),
            new SchemaElement { Name = "amount", Type = ParquetType.ByteArray, Repetition = FieldRepetition.Optional, ConvertedType = ConvertedType.Decimal, Precision = 10, Scale = 2 },
            HandBuiltParquet.Leaf("text", ParquetType.ByteArray, FieldRepetition.Optional, ConvertedType.Utf8),
        ];
        byte[] file = HandBuiltParquet.Build(schema, present.Length,
            new HandBuiltParquet.Column(["flag"], ParquetType.Boolean, 1, 0, Defs(..), [], HandBuiltParquet.Indices(2, flagPicks),
                Encoding: ParquetEncoding.RleDictionary, Dictionary: new(3, [0b101])),
            // Big-endian two's complement: 12345, -1, 100000 (hundredths).
            new HandBuiltParquet.Column(["amount"], ParquetType.ByteArray, 1, 0, Defs(..), [], HandBuiltParquet.Indices(2, amountPicks),
                Encoding: ParquetEncoding.RleDictionary, Dictionary: new(3, [2, 0, 0, 0, 0x30, 0x39, 1, 0, 0, 0, 0xFF, 3, 0, 0, 0, 0x01, 0x86, 0xA0])),
            // A PLAIN page of long values first (the batch's own data buffer), then a dictionary-encoded page whose
            // views must point at the dictionary's buffer, the batch's second.
            new HandBuiltParquet.Column(["text"], ParquetType.ByteArray, 1, 0, Defs(..3), [], HandBuiltParquet.Strings(plain),
                Dictionary: new(3, HandBuiltParquet.Strings(texts)),
                MorePages: [new(Defs(3..), [], HandBuiltParquet.Indices(2, textPicks), ParquetEncoding.RleDictionary)]));

        using var parquet = ParquetFile.Open(new MemoryStream(file));
        using var reader = parquet.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = parallelism });
        using RecordBatch batch = Assert.Single(reader.ReadBatches());
        var flag = (BooleanArray)batch.Column(0);
        var amount = (Decimal128Array)batch.Column(1);
        var text = (StringViewArray)batch.Column(2);
        for (int i = 0, f = 0, a = 0, t = 0; i < present.Length; i++)
        {
            if (!present[i])
            {
                Assert.True(flag.IsNull(i) && amount.IsNull(i) && text.IsNull(i), $"row {i}");
                continue;
            }
            Assert.Equal(flags[flagPicks[f++]], flag.GetValue(i));
            Assert.Equal(amounts[amountPicks[a++]], amount.GetValue(i));
            Assert.Equal(i < 3 ? plain[i - 1] : texts[textPicks[t++]], text.GetString(i));
        }
    }

    /// <summary>
    /// A column's page buffers go back to the pool once the column is read, not when its row group is done: columns
    /// read one after another hold one window at a time (not one per column), and none is out after a read, in
    /// sequence or in parallel, uncompressed or compressed (window and scratch).
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public void PageBuffers_GoBackWhenTheirColumnIsRead(int parallelism)
    {
        string[] names = ["a", "b", "c", "d"];
        // More than 64 KiB, so that the file is streamed (smaller ones are read into memory at once).
        byte[] file = MinimalParquetWriter.Write([.. names.Select(n => new ParquetLeaf(n, Repetition.Required, PhysicalType.Int32))],
            [.. Enumerable.Range(0, 20_000).Select(i => names.ToDictionary(n => n, object? (_) => i))]);
        using (var parquet = ParquetFile.Open(new MemoryStream(file)))
        {
            using var reader = parquet.OpenReader(new() { MaxDegreeOfParallelism = parallelism, Reading = ChunkReading.Stream });
            Assert.Equal(20_000, RowCount(reader.ReadBatches()));
            if (parallelism == 1) Assert.Equal(1, reader.Buffers.PeakRented);
            Assert.Equal(0, reader.Buffers.Rented);
        }

        // Snappy pages of 38 columns: a window and a scratch buffer per column being read.
        using var snappy = ParquetFile.OpenOnSource(Path.Combine(AppContext.BaseDirectory, "TestData", "parquet", "matrix", "pa_dict_v1_snappy.parquet"));
        using var compressed = snappy.OpenReader(new() { MaxDegreeOfParallelism = parallelism, Reading = ChunkReading.Stream });
        Assert.True(RowCount(compressed.ReadBatches()) > 0);
        if (parallelism == 1) Assert.Equal(2, compressed.Buffers.PeakRented);
        Assert.Equal(0, compressed.Buffers.Rented);

        static long RowCount(IEnumerable<RecordBatch> batches)
        {
            long rows = 0;
            foreach (RecordBatch batch in batches)
            {
                rows += batch.Length;
                batch.Dispose();
            }
            return rows;
        }
    }

    /// <summary>
    /// A flat optional leaf with BIT_PACKED definition levels (the deprecated encoding): each page's own count of
    /// present values places its values, also when the pages' null counts differ (the second page dictionary-encoded:
    /// its indices are decoded by that count).
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void BitPackedDefinitionLevels_OfAFlatLeaf_PlaceEachPagesValues(bool levelsAsBits)
    {
        byte[] first = [1, 0, 1, 1, 0], second = [0, 0, 1, 1, 1, 1, 1, 0];
        SchemaElement[] schema =
        [
            HandBuiltParquet.Group("schema", 1, repetition: null),
            HandBuiltParquet.Leaf("n", ParquetType.Int32, FieldRepetition.Optional),
        ];
        byte[] file = HandBuiltParquet.Build(schema, first.Length + second.Length,
            new HandBuiltParquet.Column(["n"], ParquetType.Int32, 1, 0, first, [], HandBuiltParquet.Int32s(10, 11, 12), ParquetEncoding.BitPacked,
                Dictionary: new(3, HandBuiltParquet.Int32s(20, 21, 22)),
                MorePages: [new(second, [], HandBuiltParquet.Indices(2, 0, 1, 2, 2, 0), ParquetEncoding.RleDictionary)]));
        using var parquet = ParquetFile.Open(new MemoryStream(file));
        using var reader = parquet.OpenReader(new ParquetReadOptions { LevelsAsBits = levelsAsBits });
        using RecordBatch batch = Assert.Single(reader.ReadBatches());
        var n = (Int32Array)batch.Column(0);
        Assert.Equal((int?[])[10, null, 11, 12, null, null, null, 20, 21, 22, 22, 20, null], [.. Enumerable.Range(0, n.Length).Select(n.GetValue)]);
    }

    /// <summary>
    /// STRING dictionary entries are UTF-8: a page that is all ASCII is checked at once, one that is not entry by entry
    /// (valid text reads as written), and an entry that is not UTF-8 is refused, in both string layouts.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void DictionaryPages_ThatAreNotUtf8_AreRefused(bool viewTypes)
    {
        SchemaElement[] schema =
        [
            HandBuiltParquet.Group("schema", 1, repetition: null),
            HandBuiltParquet.Leaf("s", ParquetType.ByteArray, FieldRepetition.Optional, ConvertedType.Utf8),
        ];
        byte[] File(byte[] second) => HandBuiltParquet.Build(schema, 3,
            new HandBuiltParquet.Column(["s"], ParquetType.ByteArray, 1, 0, [1, 1, 1], [], HandBuiltParquet.Indices(1, 0, 1, 0),
                Encoding: ParquetEncoding.RleDictionary, Dictionary: new(2, [.. HandBuiltParquet.Strings("plain"), .. BitConverter.GetBytes(second.Length), .. second])));
        string?[] Read(byte[] file)
        {
            using var parquet = ParquetFile.Open(new MemoryStream(file));
            using var reader = parquet.OpenReader(new ParquetReadOptions { ViewTypes = viewTypes });
            using RecordBatch batch = Assert.Single(reader.ReadBatches());
            return batch.Column(0) switch
            {
                StringViewArray views => [.. Enumerable.Range(0, views.Length).Select(i => views.GetString(i))],
                StringArray strings => [.. Enumerable.Range(0, strings.Length).Select(i => strings.GetString(i))],
                var other => throw new InvalidOperationException(other.GetType().Name),
            };
        }

        Assert.Equal((string?[])["plain", "héllo", "plain"], Read(File("héllo"u8.ToArray())));
        Assert.Equal((string?[])["plain", "a long entry, not inline", "plain"], Read(File("a long entry, not inline"u8.ToArray())));
        Assert.Contains("UTF-8", Assert.Throws<ParquetFormatException>(() => Read(File([0x68, 0xC3]))).Message);
        Assert.Contains("UTF-8", Assert.Throws<ParquetFormatException>(() => Read(File([0x68, 0xFF, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68]))).Message);
    }

    /// <summary>
    /// Read page by page from a file, a page header larger than the first bytes looked at (statistics of 200 KB) is
    /// parsed from more; a source that ends inside a column chunk is a format error in every way of reading.
    /// </summary>
    [Fact]
    public void Streaming_LargePageHeaders_AndSourcesCutShort()
    {
        // The value page is larger than what is read ahead at once.
        string big = new('x', PageReader.ReadAhead + 200_000);
        string stats = new('x', 200_000);
        SchemaElement[] schema =
        [
            HandBuiltParquet.Group("schema", 1, repetition: null),
            HandBuiltParquet.Leaf("s", ParquetType.ByteArray, FieldRepetition.Required, ConvertedType.Utf8),
        ];
        string[] values = ["first", big, "last"];
        byte[] file = HandBuiltParquet.Build(schema, values.Length,
            new HandBuiltParquet.Column(["s"], ParquetType.ByteArray, 0, 0, [], [], HandBuiltParquet.Strings(values),
                PageStatistics: new Statistics { MinValue = System.Text.Encoding.UTF8.GetBytes(stats), MaxValue = System.Text.Encoding.UTF8.GetBytes(stats) }));

        // Also from a stream that starts 100 bytes into its buffer (memory is sliced from there).
        byte[] prefixed = [.. new byte[100], .. file];
        foreach (ChunkReading reading in (ChunkReading[])[ChunkReading.Stream, ChunkReading.Fetch, ChunkReading.Slice])
        {
            foreach (bool prefix in (bool[])[false, true])
            {
                var stream = prefix ? new MemoryStream(prefixed, 0, prefixed.Length, false, publiclyVisible: true) { Position = 100 }
                    : new MemoryStream(file, 0, file.Length, false, publiclyVisible: true);
                using var parquet = ParquetFile.Open(stream);
                using var reader = parquet.OpenReader(new() { Reading = reading });
                Assert.Equal(reading, reader.EffectiveReading);
                using RecordBatch batch = Assert.Single(reader.ReadBatches());
                Assert.Equal(values, Enumerable.Range(0, 3).Select(i => ((StringViewArray)batch.Column(0)).GetString(i)));
            }
        }

        // The footer is intact; the data stops 1000 bytes into the column chunk.
        foreach (ChunkReading reading in (ChunkReading[])[ChunkReading.Stream, ChunkReading.Fetch])
        {
            using var parquet = ParquetFile.Open(new CutShort(file, 1004));
            using var reader = parquet.OpenReader(new() { Reading = reading });
            Assert.ThrowsAny<ParquetFormatException>(() => Rows(reader.ReadBatches()));
        }
    }

    /// <summary>A source that reads nothing between <paramref name="cut"/> and its last 64 KiB (where opening reads the footer).</summary>
    private sealed class CutShort(byte[] bytes, int cut) : IRandomAccess
    {
        public long Length => bytes.Length;

        public int ReadAt(long offset, Span<byte> destination)
        {
            if (offset >= bytes.Length - 65_536) return Copy(offset, destination);
            if (offset >= cut) return 0;
            return Copy(offset, destination[..(int)Math.Min(destination.Length, cut - offset)]);
        }

        private int Copy(long offset, Span<byte> destination)
        {
            int count = (int)Math.Clamp(bytes.Length - offset, 0, destination.Length);
            bytes.AsSpan((int)offset, count).CopyTo(destination);
            return count;
        }

        public ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ReadAt(offset, destination.Span));
    }

    /// <summary>Row groups larger than the in-flight budget are still read, one at a time, in order.</summary>
    [Fact]
    public void RowGroupsOverTheInFlightBudget_ReadOneAtATime()
    {
        using var file = ParquetFile.Open(ParquetFixtures.FullPath("matrix/pa_pages_rowgroups.parquet"));
        Assert.True(file.Metadata.RowGroups.Count > 1);
        List<string> Read(long budget)
        {
            using var reader = file.OpenReader(new() { MaxDegreeOfParallelism = 8, InFlightBytes = budget, BatchSize = 100 });
            return Rows(reader.ReadBatches());
        }
        Assert.Equal(Read(512L << 20), Read(1));
    }

    /// <summary>Without a forced way, chunks are sliced from memory, read page by page from a file, and fetched from other sources.</summary>
    [Fact]
    public void ChunkReading_FollowsTheSource()
    {
        string path = ParquetFixtures.FullPath("matrix/pa_pages_rowgroups.parquet");
        byte[] bytes = File.ReadAllBytes(path);
        Assert.True(bytes.Length > 65_536, "the file must be larger than what opening reads at once");
        using (var file = ParquetFile.Open(path))
        using (var reader = file.OpenReader())
            Assert.Equal(ChunkReading.Stream, reader.EffectiveReading);
        using (var file = ParquetFile.Open(new MemoryStream(bytes, 0, bytes.Length, false, publiclyVisible: true)))
        using (var reader = file.OpenReader())
            Assert.Equal(ChunkReading.Slice, reader.EffectiveReading);
        using (var file = ParquetFile.Open(new CutShort(bytes, bytes.Length)))
        using (var reader = file.OpenReader())
            Assert.Equal(ChunkReading.Fetch, reader.EffectiveReading);
    }

    /// <summary>
    /// Off reads no chunk from a mapping, On every one, Auto the uncompressed fixed-width ones; sources that are not
    /// local files ignore the setting; an unknown mode is refused.
    /// </summary>
    [Fact]
    public void MemoryMap_ReadsTheChunksItSays()
    {
        // pa_plain_v1: uncompressed, fixed-width and string columns; pa_dict_v1_snappy: compressed.
        foreach (string name in (string[])["matrix/pa_plain_v1.parquet", "matrix/pa_dict_v1_snappy.parquet"])
        {
            using var file = ParquetFile.OpenOnSource(ParquetFixtures.FullPath(name));
            int chunks = file.Thrift.RowGroups.Sum(g => g.Columns.Length);
            int fixedUncompressed = file.Thrift.RowGroups.Sum(g => g.Columns.Count(c => c.MetaData!.Codec == CompressionCodec.Uncompressed
                && c.MetaData.Type is not (ParquetType.ByteArray)));
            List<string>? reference = null;
            foreach (ParquetMemoryMap map in (ParquetMemoryMap[])[ParquetMemoryMap.Off, ParquetMemoryMap.On, ParquetMemoryMap.Auto])
            {
                using var reader = file.OpenReader(new() { MemoryMap = map });
                List<string> rows = Rows(reader.ReadBatches());
                reference ??= rows;
                Assert.Equal(reference, rows);
                Assert.Equal(map switch { ParquetMemoryMap.Off => 0, ParquetMemoryMap.On => chunks, _ => fixedUncompressed }, reader.MappedChunks);
            }
            Assert.True(((Kaminari.Seseragi.Files.IO.RandomAccessSource)file.Source).IsMapped);
        }

        // A file none of whose chunks is read from a mapping (compressed, automatic) is not mapped at all.
        using (var compressed = ParquetFile.OpenOnSource(ParquetFixtures.FullPath("matrix/pa_dict_v1_snappy.parquet")))
        using (var reader = compressed.OpenReader(new() { MemoryMap = ParquetMemoryMap.Auto }))
        {
            Assert.NotEmpty(Rows(reader.ReadBatches()));
            Assert.False(((Kaminari.Seseragi.Files.IO.RandomAccessSource)compressed.Source).IsMapped);
        }

        // A file stream opened at an offset (the Parquet data after 100 other bytes) maps from there.
        string path = Path.Combine(Path.GetTempPath(), $"seseragi-offset-{Guid.NewGuid():N}.parquet");
        try
        {
            File.WriteAllBytes(path, [.. new byte[100], .. File.ReadAllBytes(ParquetFixtures.FullPath("matrix/pa_plain_v1.parquet"))]);
            using var plain = ParquetFile.OpenOnSource(ParquetFixtures.FullPath("matrix/pa_plain_v1.parquet"));
            using var expected = plain.OpenReader();
            var stream = new FileStream(path, FileMode.Open, FileAccess.Read) { Position = 100 };
            using var shifted = ParquetFile.Open(stream);
            using var mapped = shifted.OpenReader(new() { MemoryMap = ParquetMemoryMap.On });
            Assert.Equal(Rows(expected.ReadBatches()), Rows(mapped.ReadBatches()));
            Assert.True(mapped.MappedChunks > 0);
        }
        finally
        {
            File.Delete(path);
        }
        using (var plain = ParquetFile.OpenOnSource(ParquetFixtures.FullPath("matrix/pa_plain_v1.parquet")))
            Assert.Contains(plain.Thrift.RowGroups, g => g.Columns.Any(c => c.MetaData!.Type == ParquetType.ByteArray)); // Auto leaves some out

        byte[] bytes = File.ReadAllBytes(ParquetFixtures.FullPath("matrix/pa_plain_v1.parquet"));
        using (var memory = ParquetFile.Open(new MemoryStream(bytes)))
        using (var reader = memory.OpenReader(new() { MemoryMap = ParquetMemoryMap.On }))
        {
            Assert.NotEmpty(Rows(reader.ReadBatches()));
            Assert.Equal(0, reader.MappedChunks);
        }
        using var any = ParquetFile.Open(ParquetFixtures.FullPath("matrix/pa_plain_v1.parquet"));
        Assert.Throws<ArgumentOutOfRangeException>(() => any.OpenReader(new() { MemoryMap = (ParquetMemoryMap)7 }));
    }

    /// <summary>A file larger than 2 GiB (sparse: its data starts 2.5 GiB in) is mapped and read.</summary>
    [Fact]
    public void MemoryMap_FilesBeyondTwoGigabytes()
    {
        const long gap = 5L << 29; // 2.5 GiB
        SchemaElement[] schema =
        [
            HandBuiltParquet.Group("schema", 1, repetition: null),
            HandBuiltParquet.Leaf("v", ParquetType.Int32, FieldRepetition.Required),
        ];
        byte[] bytes = HandBuiltParquet.Build(schema, 4, gap, new HandBuiltParquet.Column(["v"], ParquetType.Int32, 0, 0, [], [], HandBuiltParquet.Int32s(1, 2, 3, 4)));
        string path = Path.Combine(Path.GetTempPath(), $"seseragi-sparse-{Guid.NewGuid():N}.parquet");
        try
        {
            using (var stream = new FileStream(path, FileMode.CreateNew))
            {
                stream.Write(bytes.AsSpan(0, 4));
                stream.Position = 4 + gap;
                stream.Write(bytes.AsSpan(4));
            }
            using var file = ParquetFile.Open(path);
            foreach (ParquetMemoryMap map in (ParquetMemoryMap[])[ParquetMemoryMap.On, ParquetMemoryMap.Off])
            {
                using var reader = file.OpenReader(new() { MemoryMap = map });
                using RecordBatch batch = Assert.Single(reader.ReadBatches());
                Assert.Equal([1, 2, 3, 4], ((Int32Array)batch.Column(0)).Values.ToArray());
                Assert.Equal(map == ParquetMemoryMap.On ? 1 : 0, reader.MappedChunks);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>
    /// A file disposed while its mapping is being read: the row groups being decoded keep it, the later ones fail
    /// cleanly (no access to unmapped memory).
    /// </summary>
    [Fact]
    public void MemoryMap_FileDisposedWhileReading_FailsCleanly()
    {
        var file = ParquetFile.OpenOnSource(ParquetFixtures.FullPath("matrix/pa_pages_rowgroups.parquet"));
        Assert.True(file.Thrift.RowGroups.Length > 1);
        using var reader = file.OpenReader(new() { MemoryMap = ParquetMemoryMap.On, MaxDegreeOfParallelism = 1 });
        using IEnumerator<RecordBatch> batches = reader.ReadBatches().GetEnumerator();
        Assert.True(batches.MoveNext());
        batches.Current.Dispose();
        file.Dispose();
        Exception? error = Record.Exception(() =>
        {
            while (batches.MoveNext()) batches.Current.Dispose();
        });
        Assert.True(error is null or ObjectDisposedException, $"{error}");
    }

    /// <summary>A disposed source makes no mapping (it would never be released), and a mapping's ranges stay inside the file.</summary>
    [Fact]
    public void MemoryMap_NoMappingAfterDispose_AndRangesInsideTheFile()
    {
        using var stream = new FileStream(ParquetFixtures.FullPath("matrix/pa_plain_v1.parquet"), FileMode.Open, FileAccess.Read);
        var file = ParquetFile.Open(stream, leaveOpen: true);
        var source = (Kaminari.Seseragi.Files.IO.RandomAccessSource)file.Source;
        file.Dispose();
        Assert.Null(source.Map(out _));
        Assert.False(source.IsMapped);

        var mapped = Kaminari.Sazanami.Helpers.MappedFile.Open(stream)!;
        try
        {
            Assert.Equal(stream.Length, mapped.Length);
            Assert.True(mapped.Range(0, 4).Span.SequenceEqual("PAR1"u8));
            Assert.Throws<ArgumentOutOfRangeException>(() => mapped.Range(stream.Length - 1, 2));
            Assert.Throws<ArgumentOutOfRangeException>(() => mapped.Range(-1, 1));
            Assert.True(mapped.TryAcquire());
            mapped.Release();
        }
        finally
        {
            mapped.Release();
        }
        Assert.False(mapped.TryAcquire());
    }
}
