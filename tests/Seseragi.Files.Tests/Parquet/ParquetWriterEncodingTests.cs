using System.Buffers;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Encodings;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Writing;
using static Seseragi.Files.Tests.Parquet.ParquetWriteData;
using static Seseragi.Files.Tests.TestBatches;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// The encodings <see cref="ParquetWriter"/> writes besides PLAIN and the dictionary (DELTA_BINARY_PACKED,
/// DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT, RLE booleans): every type that takes one comes back
/// through our reader in both page versions, alone or after the dictionary gives up; the footer says what the pages
/// hold; the encoders against our decoders at their edges; settings a type cannot take are refused.
/// </summary>
public sealed class ParquetWriterEncodingTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("seseragi-parquet-encodings").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private string Path(string name) => System.IO.Path.Combine(_dir, name);

    private static ParquetEncoding Thrift(ParquetValueEncoding e) => e switch
    {
        ParquetValueEncoding.DeltaBinaryPacked => ParquetEncoding.DeltaBinaryPacked,
        ParquetValueEncoding.DeltaLengthByteArray => ParquetEncoding.DeltaLengthByteArray,
        ParquetValueEncoding.DeltaByteArray => ParquetEncoding.DeltaByteArray,
        ParquetValueEncoding.ByteStreamSplit => ParquetEncoding.ByteStreamSplit,
        ParquetValueEncoding.Rle => ParquetEncoding.Rle,
        _ => ParquetEncoding.Plain,
    };

    /// <summary>The leaf paths and physical types of a schema, as the writer maps it.</summary>
    private (string Path, ParquetType Type)[] Leaves(RecordBatch batch)
    {
        string path = Path($"probe-{Guid.NewGuid():N}.parquet");
        ParquetWriter.Write(path, batch.Schema, [batch.Slice(0, 1)]);
        using var file = ParquetFile.Open(path);
        return [.. file.Metadata.Thrift!.RowGroups[0].Columns.Select(c => (string.Join('.', c.MetaData!.PathInSchema), c.MetaData.Type))];
    }

    public static TheoryData<ParquetValueEncoding, ParquetPageVersion, bool> Cases()
    {
        TheoryData<ParquetValueEncoding, ParquetPageVersion, bool> data = [];
        foreach (ParquetValueEncoding e in Enum.GetValues<ParquetValueEncoding>().Where(e => e != ParquetValueEncoding.Plain))
        {
            foreach (ParquetPageVersion v in (ParquetPageVersion[])[ParquetPageVersion.V1, ParquetPageVersion.V2])
            {
                data.Add(e, v, false);
                data.Add(e, v, true);
            }
        }
        return data;
    }

    /// <summary>
    /// Every column whose type takes <paramref name="encoding"/> written in it (with the dictionary first, when on,
    /// small enough to give up halfway): the values come back, and the pages say they are so encoded.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cases))]
    public void EveryTypeTakingAnEncoding_ComesBack(ParquetValueEncoding encoding, ParquetPageVersion version, bool dictionary)
    {
        int seed = HashCode.Combine(encoding, version, dictionary) & 0xFFFF;
        RecordBatch batch = Wide(seed, 2500);
        var leaves = Leaves(batch);
        Dictionary<string, ParquetColumnOptions> columns = [];
        foreach (var (leaf, type) in leaves.Where(l => ColumnSettings.Takes(l.Type, encoding)))
            columns[leaf] = new ParquetColumnOptions { Encoding = encoding, Dictionary = dictionary };
        // The types the specification lets each encoding hold (no more is refused, no fewer written).
        string[] expected = encoding switch
        {
            ParquetValueEncoding.DeltaBinaryPacked => ["i8", "i32", "u64", "d32", "t64ns", "ts_ms_utc", "dur_s", "dec32", "dec64", "req_i64", "dict_i64", "nul"],
            ParquetValueEncoding.DeltaLengthByteArray => ["str", "vstr", "bin", "lbin", "dict_str", "req_str"],
            ParquetValueEncoding.DeltaByteArray => ["str", "bin", "fsb", "f16", "dec30", "dec60", "iv_ym", "dict_str"],
            ParquetValueEncoding.ByteStreamSplit => ["f32", "f64", "f16", "fsb", "i32", "i64", "dec30", "iv_mdn"],
            _ => ["b"],
        };
        Assert.All(expected, leaf => Assert.Contains(leaf, columns.Keys));
        string path = Path("enc.parquet");
        ParquetWriter.Write(path, batch.Schema, [batch.Slice(0, 900), batch.Slice(900, 1600)], new ParquetWriteOptions
        {
            DataPageVersion = version, DataPageRows = 97, RowGroupRows = 1000, DictionaryPageBytes = 300, Columns = columns,
        });
        List<string>[] read = ParquetWriterTests.ReadBack(path, batch.ColumnCount, out _);
        List<string>[] written = Columns([batch], batch.ColumnCount);
        for (int c = 0; c < batch.ColumnCount; c++)
            Assert.True(written[c].SequenceEqual(read[c]), $"seed {seed}: column {batch.Schema.FieldsList[c].Name} differs at row {written[c].Zip(read[c]).TakeWhile(p => p.First == p.Second).Count()}");

        using var file = ParquetFile.Open(path);
        bool fallback = false;
        foreach (RowGroup group in file.Metadata.Thrift!.RowGroups)
        {
            foreach (ColumnChunk chunk in group.Columns)
            {
                ColumnMetaData meta = chunk.MetaData!;
                string leaf = string.Join('.', meta.PathInSchema);
                PageEncodingStats[] data = [.. meta.EncodingStats!.Where(s => s.PageType != PageType.DictionaryPage)];
                if (!columns.ContainsKey(leaf))
                {
                    Assert.DoesNotContain(data, s => s.Encoding is not (ParquetEncoding.Plain or ParquetEncoding.RleDictionary));
                    continue;
                }
                // Data pages of the encoding (or of the dictionary, while it lasts) and nothing else.
                Assert.All(data, s => Assert.True(s.Encoding == Thrift(encoding) || (dictionary && s.Encoding == ParquetEncoding.RleDictionary), $"{leaf}: {s.Encoding} pages"));
                Assert.Contains(data, s => s.Encoding == Thrift(encoding) || dictionary);
                Assert.Equal(meta.Encodings.Distinct().Count(), meta.Encodings.Length);
                foreach (PageEncodingStats s in data) Assert.Contains(s.Encoding, meta.Encodings);
                fallback |= data.Length == 2;
            }
        }
        if (dictionary && encoding != ParquetValueEncoding.Rle) Assert.True(fallback, "no chunk went on in the encoding after its dictionary");
    }

    /// <summary>Nested leaves (lists, structs, maps) take encodings too, a whole column's leaves by its own path.</summary>
    [Theory]
    [InlineData(ParquetPageVersion.V1, 61)]
    [InlineData(ParquetPageVersion.V2, 62)]
    public void NestedLeaves_TakeEncodings(ParquetPageVersion version, int seed)
    {
        var (batch, expected) = ParquetNestedData.Batch(seed, 1500);
        Dictionary<string, ParquetColumnOptions> columns = [];
        foreach (var ((leaf, type), index) in Leaves(batch).Select((l, i) => (l, i)))
        {
            ParquetValueEncoding encoding = type switch
            {
                ParquetType.Int32 or ParquetType.Int64 => ParquetValueEncoding.DeltaBinaryPacked,
                ParquetType.ByteArray => index % 2 == 0 ? ParquetValueEncoding.DeltaByteArray : ParquetValueEncoding.DeltaLengthByteArray,
                ParquetType.Double or ParquetType.FixedLenByteArray => ParquetValueEncoding.ByteStreamSplit,
                ParquetType.Boolean => ParquetValueEncoding.Rle,
                _ => ParquetValueEncoding.Plain,
            };
            columns[leaf] = new ParquetColumnOptions { Encoding = encoding, Dictionary = false };
        }
        string path = Path("nested.parquet");
        ParquetWriter.Write(path, batch.Schema, [batch], new ParquetWriteOptions { DataPageVersion = version, DataPageRows = 41, RowGroupRows = 600, Columns = columns });
        using var file = ParquetFile.Open(path);
        using var reader = file.OpenReader(new ParquetReadOptions { VerifyChecksums = true });
        List<RecordBatch> read = [.. reader.ReadBatches()];
        for (int c = 0; c < expected.Length; c++)
            Assert.Equal(expected[c], read.SelectMany(b => ParquetNestedData.Canonical(b.Column(c), batch.Schema.FieldsList[c].DataType)));
        foreach (RecordBatch b in read) b.Dispose();
    }

    // ------------------------------------------------------------------
    // The encoders at their edges, against our decoders
    // ------------------------------------------------------------------

    [Fact]
    public void DeltaBinaryPacked_RoundTrips_AndInt32MiniblocksStayWithin32Bits()
    {
        var r = new Random(5);
        int[][] int32Cases =
        [
            [], [7], [int.MinValue, int.MaxValue, int.MinValue, int.MaxValue, 0, -1], // deltas of ±(2^32 - 1): 32 bits once wrapped
            [.. Enumerable.Range(0, 128)], [.. Enumerable.Range(0, 129)], [.. Enumerable.Range(0, 33).Select(i => -i * 1000)],
            [.. Enumerable.Range(0, 1000).Select(_ => r.Next(int.MinValue, int.MaxValue))], [.. Enumerable.Repeat(42, 300)],
        ];
        foreach (int[] values in int32Cases)
        {
            var output = new ArrayBufferWriter<byte>();
            DeltaBinaryPackedEncoder.Encode<int>(values, output);
            int[] decoded = new int[values.Length];
            DeltaBinaryPacked.Decode<int>(output.WrittenSpan, decoded, new ulong[DeltaBinaryPacked.Chunk], toEnd: true);
            Assert.Equal(values, decoded);
            Assert.All(MiniblockWidths(output.WrittenSpan.ToArray()), w => Assert.InRange(w, 0, 32));
        }
        long[][] int64Cases =
        [
            [], [long.MinValue], [long.MinValue, long.MaxValue, long.MinValue, 0, long.MaxValue],
            [.. Enumerable.Range(0, 1000).Select(_ => r.NextInt64(long.MinValue, long.MaxValue))], [.. Enumerable.Range(0, 257).Select(i => (long)i * i)],
        ];
        foreach (long[] values in int64Cases)
        {
            var output = new ArrayBufferWriter<byte>();
            DeltaBinaryPackedEncoder.Encode<long>(values, output);
            long[] decoded = new long[values.Length];
            DeltaBinaryPacked.Decode<long>(output.WrittenSpan, decoded, new ulong[DeltaBinaryPacked.Chunk], toEnd: true);
            Assert.Equal(values, decoded);
        }
        // Deltas of +10 and -10: 5 bits above the minimum (-10), not the 32 or 64 a minimum taken from the first delta needs.
        var zigzag = new ArrayBufferWriter<byte>();
        DeltaBinaryPackedEncoder.Encode<int>([.. Enumerable.Range(0, 200).Select(i => i % 2 * 10)], zigzag);
        Assert.All(MiniblockWidths(zigzag.WrittenSpan.ToArray()), w => Assert.Equal(5, w));
        // Consecutive integers: every delta is the minimum, so the miniblocks take no bits.
        var steps = new ArrayBufferWriter<byte>();
        DeltaBinaryPackedEncoder.Encode<long>([.. Enumerable.Range(0, 129).Select(i => (long)i)], steps);
        Assert.Equal([0, 0, 0, 0], MiniblockWidths(steps.WrittenSpan.ToArray()));
    }

    /// <summary>The bit widths of every miniblock of a DELTA_BINARY_PACKED stream.</summary>
    private static List<int> MiniblockWidths(byte[] data)
    {
        int p = 0;
        ulong Uleb()
        {
            ulong v = 0;
            for (int shift = 0; ; shift += 7)
            {
                byte b = data[p++];
                v |= (ulong)(b & 0x7F) << shift;
                if (b < 0x80) return v;
            }
        }
        long block = (long)Uleb(), miniblocks = (long)Uleb(), total = (long)Uleb();
        Uleb();
        List<int> widths = [];
        for (long done = 1; done < total; done += block)
        {
            Uleb();
            int[] w = [.. data.AsSpan(p, (int)miniblocks).ToArray().Select(b => (int)b)];
            p += (int)miniblocks;
            long left = total - done;
            for (int m = 0; m < miniblocks && m * (block / miniblocks) < left; m++)
            {
                widths.Add(w[m]);
                p += (int)(block / miniblocks * w[m] / 8);
            }
        }
        return widths;
    }

    [Fact]
    public void DeltaByteArrays_RoundTrip_SharedPrefixesAndEmptyValues()
    {
        string[] words = ["", "", "apple", "applesauce", "apply", "b", "", "雷雨", "雷", new string('x', 300), new string('x', 301) + "y", "z"];
        byte[][] values = [.. words.Select(System.Text.Encoding.UTF8.GetBytes)];
        byte[] bytes = [.. values.SelectMany(v => v)];
        int[] offsets = [0, .. values.Select((v, i) => values.Take(i + 1).Sum(x => x.Length))];
        foreach (bool prefixed in (bool[])[false, true])
        {
            var output = new ArrayBufferWriter<byte>();
            if (prefixed) DeltaByteArrayEncoder.EncodeIncremental(bytes, offsets, output);
            else DeltaByteArrayEncoder.EncodeLengths(bytes, offsets, output);
            int[] prefixes = new int[values.Length], lengths = new int[values.Length];
            var sections = DeltaByteArrays.Read(output.WrittenSpan, prefixed, prefixed ? prefixes : [], lengths, new ulong[DeltaBinaryPacked.Chunk]);
            // Rebuild the values from the sections.
            List<byte[]> rebuilt = [];
            int at = sections.DataStart;
            byte[] previous = [];
            for (int i = 0; i < values.Length; i++)
            {
                byte[] suffix = output.WrittenSpan.Slice(at, lengths[i]).ToArray();
                at += lengths[i];
                previous = [.. previous.Take(prefixes[i]), .. suffix];
                rebuilt.Add(previous);
            }
            Assert.Equal(values, rebuilt);
            if (prefixed) Assert.Equal([0, 0, 0, 5, 4, 0, 0, 0, 3, 0, 300, 0], prefixes);
        }
    }

    [Fact]
    public void Booleans_InRle_ComeBack_WithLongRuns()
    {
        bool?[] values = [.. Enumerable.Range(0, 5000).Select(i => i % 11 == 0 ? null : (bool?)(i < 2000 || i % 3 == 0))];
        RecordBatch batch = Batch(("b", BooleanColumn(values), true), ("req", BooleanColumn([.. values.Select(v => (bool?)(v ?? false))]), false));
        foreach (ParquetPageVersion version in (ParquetPageVersion[])[ParquetPageVersion.V1, ParquetPageVersion.V2])
        {
            string path = Path($"bool-{version}.parquet");
            ParquetWriter.Write(path, batch.Schema, [batch], new ParquetWriteOptions
            {
                DataPageVersion = version, DataPageRows = 700,
                Columns = new Dictionary<string, ParquetColumnOptions> { ["b"] = new() { Encoding = ParquetValueEncoding.Rle }, ["req"] = new() { Encoding = ParquetValueEncoding.Rle } },
            });
            Assert.Equal(Columns([batch], 2), ParquetWriterTests.ReadBack(path, 2, out _));
            using var file = ParquetFile.Open(path);
            Assert.All(file.Metadata.Thrift!.RowGroups[0].Columns, c => Assert.Equal([new PageEncodingStats(version == ParquetPageVersion.V1 ? PageType.DataPage : PageType.DataPageV2, ParquetEncoding.Rle, 8)], c.MetaData!.EncodingStats!));
        }

        static IArrowArray BooleanColumn(bool?[] values)
        {
            var b = new BooleanArray.Builder();
            foreach (bool? v in values)
            {
                if (v is { } x) b.Append(x);
                else b.AppendNull();
            }
            return b.Build();
        }
    }

    [Fact]
    public void EncodingsATypeCannotTake_AreRefused()
    {
        RecordBatch batch = Batch(("s", Strings(["a"]), true), ("i", Fixed(Int32Type.Default, [(int?)1]), true), ("f", Fixed(new FixedSizeBinaryType(2), [(short?)1]), true),
            ("ts", Fixed(new TimestampType(TimeUnit.Microsecond, "UTC"), [(long?)1]), true), ("d", Fixed(DoubleType.Default, [(double?)1]), true));
        void Refused(string column, ParquetValueEncoding encoding, string why, ParquetWriteOptions? options = null)
        {
            var e = Assert.Throws<ArgumentException>(() => ParquetWriter.Create(new MemoryStream(), batch.Schema, (options ?? new()) with
            {
                Columns = new Dictionary<string, ParquetColumnOptions> { [column] = new() { Encoding = encoding } },
            }));
            Assert.Contains(why, e.Message, StringComparison.Ordinal);
        }
        Refused("s", ParquetValueEncoding.DeltaBinaryPacked, "Column 's' is ByteArray, which DeltaBinaryPacked cannot encode; it takes Plain, DeltaLengthByteArray, DeltaByteArray");
        Refused("i", ParquetValueEncoding.DeltaLengthByteArray, "Column 'i' is Int32, which DeltaLengthByteArray cannot encode");
        Refused("f", ParquetValueEncoding.DeltaLengthByteArray, "Column 'f' is FixedLenByteArray");
        Refused("i", ParquetValueEncoding.Rle, "Int32, which Rle cannot encode");
        Refused("d", ParquetValueEncoding.DeltaBinaryPacked, "Double, which DeltaBinaryPacked cannot encode; it takes Plain, ByteStreamSplit");
        Refused("ts", ParquetValueEncoding.ByteStreamSplit, "Column 'ts' is Int96, which ByteStreamSplit cannot encode; it takes Plain", new ParquetWriteOptions { Int96Timestamps = true });
        Assert.Throws<ArgumentOutOfRangeException>(() => ParquetWriter.Create(new MemoryStream(), batch.Schema, new ParquetWriteOptions
        {
            Columns = new Dictionary<string, ParquetColumnOptions> { ["s"] = new() { Encoding = (ParquetValueEncoding)42 } },
        }));
    }
}
