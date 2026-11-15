using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Filters;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using static Seseragi.Files.Tests.TestBatches;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// The Bloom filters <see cref="ParquetWriter"/> writes: every value of a chunk passes its filter, whichever way its
/// pages were encoded (dictionary pages, a dictionary dropped after the first page, one full halfway, none); filters
/// sized for the chunk's distinct values or the count given, cut to the largest size; where they lie; which columns
/// get one; and reads that skip row groups by them.
/// </summary>
public sealed class ParquetWriterBloomTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("seseragi-parquet-bloom").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private string Path(string name) => System.IO.Path.Combine(_dir, name);

    private static readonly ParquetColumnOptions Bloom = new() { BloomFilter = new ParquetBloomFilterOptions() };

    /// <summary>The filter of a chunk, read from the file's bytes (null when it has none).</summary>
    private static SplitBlockBloomFilter? Filter(byte[] file, ColumnChunk chunk)
    {
        ColumnMetaData meta = chunk.MetaData!;
        if (meta.BloomFilterOffset is not long offset) return null;
        BloomFilterHeader header = BloomFilterHeader.Parse(file.AsSpan((int)offset), out int length);
        Assert.Equal((1, 1, 1), (header.Algorithm, header.Hash, header.Compression)); // split blocks, XXH64, uncompressed
        Assert.Equal(length + header.NumBytes, meta.BloomFilterLength);
        return new SplitBlockBloomFilter(file.AsSpan((int)offset + length, header.NumBytes).ToArray());
    }

    private static ulong Hash(ReadOnlySpan<byte> plain) => XxHash64.HashToUInt64(plain);

    private static byte[] Le<T>(T value) where T : unmanaged
    {
        byte[] bytes = new byte[System.Runtime.CompilerServices.Unsafe.SizeOf<T>()];
        System.Runtime.InteropServices.MemoryMarshal.Write(bytes, in value);
        return bytes;
    }

    /// <summary>The PLAIN bytes (lengths left out) of each non-null leaf value of rows <paramref name="from"/>.. +<paramref name="count"/>.</summary>
    private static IEnumerable<byte[]> PlainValues(IArrowArray array, int from, int count)
    {
        for (int i = from; i < from + count; i++)
        {
            if (array.IsNull(i)) continue;
            switch (array)
            {
                case ListArray list:
                    foreach (byte[] v in PlainValues(list.Values, list.ValueOffsets[i], list.GetValueLength(i))) yield return v;
                    break;
                case StructArray st:
                    foreach (IArrowArray field in st.Fields)
                        foreach (byte[] v in PlainValues(field, i, 1)) yield return v;
                    break;
                case DictionaryArray d:
                    foreach (byte[] v in PlainValues(d.Dictionary, ((Int32Array)d.Indices).Values[i], 1)) yield return v;
                    break;
                case Int32Array a: yield return Le(a.Values[i]); break;
                case Int64Array a: yield return Le(a.Values[i]); break;
                case DoubleArray a: yield return Le(a.Values[i]); break;
                case Date32Array a: yield return Le(a.Values[i]); break;
                case TimestampArray a: yield return Le(a.Values[i]); break;
                case StringArray a: yield return a.GetBytes(i).ToArray(); break;
                case Decimal128Array a: yield return Le(BinaryPrimitives.ReadInt32LittleEndian(a.ValueBuffer.Span[(i * 16)..])); break; // precision ≤ 9: INT32
                case FixedSizeBinaryArray a: yield return a.GetBytes(i).ToArray(); break;
                default: throw new NotSupportedException(array.GetType().Name);
            }
        }
    }

    /// <summary>Columns of every kind a filter must cover, encoded every way: many distinct values and few, dictionaries, nested values.</summary>
    private static RecordBatch Mixed(int rows, int seed)
    {
        var r = new Random(seed);
        int?[] Ints(int distinct) => [.. Enumerable.Range(0, rows).Select(_ => r.Next(8) == 0 ? null : (int?)r.Next(distinct))];
        var listBuilder = new ListArray.Builder(Int64Type.Default);
        for (int i = 0; i < rows; i++)
        {
            if (r.Next(6) == 0)
            {
                listBuilder.AppendNull();
                continue;
            }
            listBuilder.Append();
            for (int k = r.Next(4); k > 0; k--) ((Int64Array.Builder)listBuilder.ValueBuilder).Append(r.NextInt64());
        }
        IArrowArray structA = Strings([.. Enumerable.Range(0, rows).Select(_ => r.Next(5) == 0 ? null : "a" + r.Next(50))]);
        IArrowArray structB = Fixed(Int32Type.Default, Ints(1_000_000));
        var structType = new StructType([new Field("a", StringType.Default, true), new Field("b", Int32Type.Default, true)]);
        var dictType = new DictionaryType(Int32Type.Default, StringType.Default, false);
        return Batch(
            ("many", Fixed(Int32Type.Default, Ints(1 << 30)), true),       // the dictionary is dropped after the first page
            ("few", Fixed(Int64Type.Default, [.. Ints(20).Select(v => (long?)v)]), true), // dictionary pages throughout
            ("half", Strings([.. Ints(400).Select(v => v is null ? null : "value-" + v)]), true), // the dictionary fills halfway
            ("dbl", Fixed(DoubleType.Default, [.. Ints(1000).Select(v => v is null ? null : (double?)(v * 0.25 - 50))]), true),
            ("zero", Fixed(DoubleType.Default, [.. Ints(3).Select(v => v is null ? null : (double?)(v == 0 ? -0d : v))]), true),
            ("fsb", Fixed(new FixedSizeBinaryType(4), Ints(1 << 20)), true),
            ("dec", Fixed(new Decimal128Type(9, 2), [.. Ints(100_000).Select(v => (Int128?)v)]), true),
            ("day", Fixed(Date32Type.Default, Ints(30_000)), true),
            ("ts", Fixed(new TimestampType(TimeUnit.Microsecond, "UTC"), [.. Ints(1 << 30).Select(v => (long?)v * 1000)]), true),
            ("dict", new DictionaryArray(dictType, Fixed(Int32Type.Default, [.. Ints(3)]), Strings(["red", "green", "blue"])), true),
            ("l", listBuilder.Build(), true),
            ("s", Struct(structType, [structA, structB], [.. Enumerable.Range(0, rows).Select(_ => r.Next(7) != 0)]), true));
    }

    private static Dictionary<string, ParquetColumnOptions> AllColumns(ParquetColumnOptions settings) =>
        new() { ["many"] = settings, ["few"] = settings, ["half"] = settings, ["dbl"] = settings, ["zero"] = settings, ["fsb"] = settings,
                ["dec"] = settings, ["day"] = settings, ["ts"] = settings, ["dict"] = settings, ["l"] = settings, ["s"] = settings };

    [Theory]
    [InlineData(true, ParquetPageVersion.V1)]
    [InlineData(false, ParquetPageVersion.V2)]
    public void EveryValue_PassesItsChunksFilter_SizedForItsDistinctValues(bool dictionary, ParquetPageVersion version)
    {
        RecordBatch batch = Mixed(3000, 7);
        string path = Path("bloom.parquet");
        ParquetWriter.Write(path, batch.Schema, [batch.Slice(0, 1700), batch.Slice(1700, 1300)], new ParquetWriteOptions
        {
            RowGroupRows = 1000, DataPageRows = 100, DictionaryPageBytes = 1500, Dictionary = dictionary, DataPageVersion = version,
            Columns = AllColumns(Bloom),
        });
        byte[] bytes = File.ReadAllBytes(path);
        using var file = ParquetFile.Open(path);
        FileMetaData footer = file.Metadata.Thrift!;
        // Leaves in schema order: the list's items and the struct's two fields are leaves of their own.
        (int Column, Func<IArrowArray, IArrowArray> Leaf)[] leaves =
        [
            .. Enumerable.Range(0, 11).Select(c => (c, (Func<IArrowArray, IArrowArray>)(a => a))),
            (11, a => new StructArray(new StructType([new Field("a", StringType.Default, true)]), a.Length, [((StructArray)a).Fields[0]], ((StructArray)a).NullBitmapBuffer, ((StructArray)a).NullCount)),
            (11, a => new StructArray(new StructType([new Field("b", Int32Type.Default, true)]), a.Length, [((StructArray)a).Fields[1]], ((StructArray)a).NullBitmapBuffer, ((StructArray)a).NullCount)),
        ];
        bool sawDictionaryPages = false, sawPlainPages = false, sawFallback = false;
        long groupStart = 0;
        foreach (RowGroup group in footer.RowGroups)
        {
            Assert.Equal(leaves.Length, group.Columns.Length);
            for (int k = 0; k < leaves.Length; k++)
            {
                ColumnChunk chunk = group.Columns[k];
                SplitBlockBloomFilter filter = Filter(bytes, chunk) ?? throw new Xunit.Sdk.XunitException($"leaf {k} has no Bloom filter");
                IArrowArray leaf = leaves[k].Leaf(batch.Column(leaves[k].Column));
                HashSet<ulong> hashes = [.. PlainValues(leaf, (int)groupStart, (int)group.NumRows).Select(v => Hash(v))];
                foreach (ulong h in hashes) Assert.True(filter.MightContain(h), $"leaf {k} row group {group.Ordinal}: a value the chunk holds is ruled out");
                // Sized for the distinct values (at least one block).
                Assert.Equal(SplitBlockBloomFilter.BlocksFor(Math.Max(1, hashes.Count), 0.01) * 32, filter.Bitset.Length);
                PageEncodingStats[] stats = chunk.MetaData!.EncodingStats!;
                bool dict = stats.Any(s => s.Encoding == ParquetEncoding.RleDictionary), plain = stats.Any(s => s.PageType != PageType.DictionaryPage && s.Encoding == ParquetEncoding.Plain);
                sawDictionaryPages |= dict && !plain;
                sawPlainPages |= plain && !dict;
                sawFallback |= dict && plain;
            }
            groupStart += group.NumRows;
        }
        Assert.True(sawPlainPages);
        Assert.Equal(dictionary, sawDictionaryPages);
        Assert.Equal(dictionary, sawFallback);
    }

    [Theory]
    [InlineData(0.01)]
    [InlineData(0.05)]
    [InlineData(0.001)]
    public void FalsePositives_StayNearTheRateAskedFor(double rate)
    {
        long?[] values = [.. Enumerable.Range(0, 20_000).Select(i => (long?)i * 2)];
        RecordBatch batch = Batch(("x", Fixed(Int64Type.Default, values), true));
        string path = Path("rate.parquet");
        ParquetWriter.Write(path, batch.Schema, [batch], new ParquetWriteOptions
        {
            Columns = new Dictionary<string, ParquetColumnOptions> { ["x"] = new() { BloomFilter = new() { FalsePositiveRate = rate, MaxBytes = 1 << 24 } } },
        });
        byte[] bytes = File.ReadAllBytes(path);
        using var file = ParquetFile.Open(path);
        SplitBlockBloomFilter filter = Filter(bytes, file.Metadata.Thrift!.RowGroups[0].Columns[0])!;
        Assert.All(values, v => Assert.True(filter.MightContain(Hash(Le(v!.Value)))));
        int probes = 400_000, passed = 0;
        for (long i = 0; i < probes; i++)
        {
            if (filter.MightContain(Hash(Le(i * 2 + 1)))) passed++; // odd numbers: none was written
        }
        double observed = (double)passed / probes;
        Assert.True(observed <= rate * 1.5, $"{observed:P3} of absent values pass a filter asked for {rate:P3}");
    }

    [Fact]
    public void Filters_AreSizedForTheDistinctValues_AtMostDistinctValues_CutToMaxBytes()
    {
        long?[] values = [.. Enumerable.Range(0, 200_000).Select(i => (long?)(i % 50_000))]; // 50 000 distinct
        RecordBatch batch = Batch(("a", Fixed(Int64Type.Default, values), true), ("b", Fixed(Int64Type.Default, values), true),
            ("c", Fixed(Int64Type.Default, values), true), ("d", Fixed(Int64Type.Default, values), true), ("e", Fixed(Int64Type.Default, values), true));
        string path = Path("sizes.parquet");
        ParquetWriter.Write(path, batch.Schema, [batch], new ParquetWriteOptions
        {
            Columns = new Dictionary<string, ParquetColumnOptions>
            {
                ["a"] = new() { BloomFilter = new() },                                        // the 50 000 counted
                ["b"] = new() { BloomFilter = new() { DistinctValues = 10_000_000 } },       // a limit above the count: the count
                ["c"] = new() { BloomFilter = new() { DistinctValues = 1000 } },             // a limit below it: the limit
                ["d"] = new() { BloomFilter = new() { MaxBytes = 5000 } },                   // cut to the power of two below
                ["e"] = new() { BloomFilter = new() { MaxBytes = 32 } },
            },
        });
        byte[] bytes = File.ReadAllBytes(path);
        using var file = ParquetFile.Open(path);
        int[] sizes = [.. file.Metadata.Thrift!.RowGroups[0].Columns.Select(c => Filter(bytes, c)!.Bitset.Length)];
        int counted = SplitBlockBloomFilter.BlocksFor(50_000, 0.01) * 32;
        Assert.Equal([counted, counted, SplitBlockBloomFilter.BlocksFor(1000, 0.01) * 32, 4096, 32], sizes);
        foreach (ColumnChunk chunk in file.Metadata.Thrift!.RowGroups[0].Columns)
        {
            SplitBlockBloomFilter filter = Filter(bytes, chunk)!;
            Assert.All(Enumerable.Range(0, 50_000), v => Assert.True(filter.MightContain(Hash(Le((long)v)))));
        }
    }

    [Fact]
    public void Filters_LieAfterTheRowGroups_BeforeThePageIndexes()
    {
        RecordBatch batch = Mixed(2000, 3);
        string path = Path("place.parquet");
        ParquetWriter.Write(path, batch.Schema, [batch], new ParquetWriteOptions { RowGroupRows = 700, Columns = AllColumns(Bloom) });
        using var file = ParquetFile.Open(path);
        ColumnChunk[] chunks = [.. file.Metadata.Thrift!.RowGroups.SelectMany(g => g.Columns)];
        long dataEnd = chunks.Max(c => (c.MetaData!.DictionaryPageOffset ?? c.MetaData.DataPageOffset) + c.MetaData.TotalCompressedSize);
        long[] filters = [.. chunks.Select(c => c.MetaData!.BloomFilterOffset!.Value)];
        Assert.True(filters.Min() == dataEnd, "the filters follow the last row group");
        Assert.True(filters.Max() < chunks.Min(c => c.ColumnIndexOffset ?? c.OffsetIndexOffset!.Value), "the page indexes follow the filters");
        Assert.Equal(filters.Order(), filters); // in row group order, column by column
    }

    [Fact]
    public void ColumnSettings_ApplyByPath_AndAreRefusedWhereTheyCannot()
    {
        RecordBatch batch = Mixed(200, 5);
        string path = Path("paths.parquet");
        ParquetWriter.Write(path, batch.Schema, [batch], new ParquetWriteOptions
        {
            // The longer path wins, whichever comes first.
            Columns = new Dictionary<string, ParquetColumnOptions> { ["s.a"] = new(), ["s"] = Bloom, ["l.list.element"] = Bloom },
        });
        byte[] bytes = File.ReadAllBytes(path);
        using (var file = ParquetFile.Open(path))
        {
            ColumnChunk[] chunks = file.Metadata.Thrift!.RowGroups[0].Columns;
            Assert.Equal([false, false, false, false, false, false, false, false, false, false, true, false, true],
                chunks.Select(c => Filter(bytes, c) is not null));
        }

        void Refused(string why, Dictionary<string, ParquetColumnOptions> columns, RecordBatch? other = null)
        {
            RecordBatch b = other ?? batch;
            var e = Assert.ThrowsAny<ArgumentException>(() => ParquetWriter.Create(new MemoryStream(), b.Schema, new ParquetWriteOptions { Columns = columns }));
            Assert.Contains(why, e.Message, StringComparison.Ordinal);
        }
        Refused("'nope', which is no column", new() { ["nope"] = Bloom });
        Refused("'l.element', which is no column", new() { ["l.element"] = Bloom });
        Refused("'s.', which is no column", new() { ["s."] = Bloom });
        Refused("'man', which is no column", new() { ["man"] = Bloom }); // a prefix of 'many' is no ancestor
        Refused("BOOLEAN, which takes no Bloom filter", new() { ["flag"] = Bloom }, Batch(("flag", Fixed(BooleanType.Default, [(byte?)1]), true)));
        Refused("between 0 and 1", new() { ["many"] = new() { BloomFilter = new() { FalsePositiveRate = 1 } } });
        Refused("at least 32 bytes", new() { ["many"] = new() { BloomFilter = new() { MaxBytes = 16 } } });
        Refused("positive number of distinct values", new() { ["many"] = new() { BloomFilter = new() { DistinctValues = 0 } } });
    }

    [Fact]
    public void FilteredReads_SkipRowGroupsTheFiltersRuleOut_AndKeepEveryMatchingRow()
    {
        var r = new Random(41);
        string[] uids = [.. Enumerable.Range(0, 4000).Select(_ => Convert.ToHexString(BitConverter.GetBytes(r.NextInt64())))];
        RecordBatch batch = Batch(("uid", Strings(uids), true), ("k", Fixed(Int64Type.Default, [.. Enumerable.Range(0, 4000).Select(_ => (long?)r.Next(1_000_000))]), true));
        string with = Path("with.parquet"), without = Path("without.parquet");
        var options = new ParquetWriteOptions { RowGroupRows = 500 };
        ParquetWriter.Write(with, batch.Schema, [batch], options with { Columns = new Dictionary<string, ParquetColumnOptions> { ["uid"] = Bloom, ["k"] = Bloom } });
        ParquetWriter.Write(without, batch.Schema, [batch], options);
        int skipped = 0;
        List<(string, LiteralValue)> probes = [.. uids.Where((_, i) => i % 397 == 0).Select(u => ("uid", LiteralValue.NewString(u))),
            ("uid", LiteralValue.NewString("8000000000000000")), ("uid", LiteralValue.NewString(uids[5] + "0")),
            .. Enumerable.Range(0, 10).Select(i => ("k", LiteralValue.NewInt64(((Int64Array)batch.Column(1)).Values[i * 333]))), ("k", LiteralValue.NewInt64(1_000_001 / 2))];
        foreach (var (column, literal) in probes)
        {
            ScalarExpr filter = ScalarExpr.NewBinary(BinaryOp.Eq, ScalarExpr.NewColumn(column), ScalarExpr.NewLiteral(literal));
            var (expected, groupsWithout) = RowsKept(without, filter);
            var (actual, groupsWith) = RowsKept(with, filter);
            Assert.Equal(expected, actual);
            if (groupsWith < groupsWithout) skipped++;
        }
        Assert.True(skipped >= probes.Count - 2, $"the filters skipped row groups in only {skipped} of {probes.Count} reads");
    }

    private static (long[] Rows, int Groups) RowsKept(string path, ScalarExpr filter)
    {
        using var file = ParquetFile.Open(path);
        using var reader = file.OpenReader(new ParquetReadOptions { Filter = filter, FilterRows = true, RowIndexColumn = "row", Columns = [] });
        List<long> rows = [];
        foreach (RecordBatch b in reader.ReadBatches())
        {
            using (b) rows.AddRange(((Int64Array)b.Column(0)).Values.ToArray());
        }
        return ([.. rows], reader.RowGroupsRead.Count);
    }
}
