using System.Numerics;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Encodings;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// The decoders of the non-PLAIN value encodings against reference encoders written from Encodings.md, over
/// layouts writers produce and the edge cases the spec allows, and the damage they refuse.
/// </summary>
public class ParquetEncodingTests
{
    // ------------------------------------------------------------------
    // DELTA_BINARY_PACKED
    // ------------------------------------------------------------------

    [Fact]
    public void DeltaBinaryPacked_DecodesWhatTheReferenceEncodes()
    {
        var random = new Random(31);
        foreach (var (blockSize, miniblocks) in ((int, int)[])[(128, 4), (128, 1), (256, 8), (256, 4), (512, 16)])
        {
            foreach (int count in (int[])[0, 1, 2, 31, 32, 33, 127, 128, 129, 300, 1000])
            {
                foreach (string shape in (string[])["sorted", "random", "extremes", "constant", "small"])
                {
                    long[] values = [.. Enumerable.Range(0, count).Select(i => shape switch
                    {
                        "sorted" => 1_000_000L + i * 37L,
                        "extremes" => i % 2 == 0 ? long.MinValue + i : long.MaxValue - i, // deltas wrap
                        "constant" => 42L,
                        "small" => random.Next(-8, 8),
                        _ => random.NextInt64(long.MinValue, long.MaxValue),
                    })];
                    foreach (bool int32 in (bool[])[true, false])
                    {
                        long[] expected = int32 ? [.. values.Select(v => (long)(int)v)] : values;
                        foreach (bool garbage in (bool[])[false, true])
                        {
                            byte[] encoded = Dbp.Encode(expected, blockSize, miniblocks, int32, garbage ? random : null, trimLast: garbage);
                            string what = $"{shape}, {count} values, block {blockSize}/{miniblocks}, {(int32 ? "INT32" : "INT64")}, garbage {garbage}";
                            if (int32)
                            {
                                Check<int>(encoded, [.. expected.Select(v => (int)v)], what);
                            }
                            else Check<long>(encoded, expected, what);
                        }
                    }
                }
            }
        }

        static void Check<T>(byte[] encoded, T[] expected, string what) where T : unmanaged, IBinaryInteger<T>
        {
            var scratch = new ulong[DeltaBinaryPacked.Chunk];
            var all = new T[expected.Length];
            int end = DeltaBinaryPacked.Decode(encoded, all.AsSpan(), scratch, toEnd: true);
            Assert.True(all.AsSpan().SequenceEqual(expected), what);
            Assert.Equal(encoded.Length, end);
            // A prefix (a page with fewer present values than the stream counts), without and with the walk to the end.
            int some = expected.Length / 3;
            var prefix = new T[some];
            DeltaBinaryPacked.Decode(encoded, prefix.AsSpan(), scratch, toEnd: false);
            Assert.True(prefix.AsSpan().SequenceEqual(expected.AsSpan(0, some)), $"prefix: {what}");
            Assert.Equal(encoded.Length, DeltaBinaryPacked.Decode(encoded, prefix.AsSpan(), scratch, toEnd: true));
        }
    }

    [Fact]
    public void DeltaBinaryPacked_RefusesDamage()
    {
        var scratch = new ulong[DeltaBinaryPacked.Chunk];
        long[] values = [.. Enumerable.Range(0, 300).Select(i => (long)i * i)];
        byte[] good = Dbp.Encode(values, 128, 4, int32: false, null, trimLast: false);
        Assert.Equal(good.Length, DeltaBinaryPacked.Decode(good, new long[300].AsSpan(), scratch, toEnd: true));

        Refused(Dbp.Header(100, 4, 1, 0), "multiple of 128");          // block size
        Refused(Dbp.Header(0, 4, 1, 0), "multiple of 128");
        Refused(Dbp.Header(128, 0, 1, 0), "miniblocks");                // miniblock count
        Refused(Dbp.Header(128, 3, 1, 0), "miniblocks");                // does not divide
        Refused(Dbp.Header(1152, 35, 1, 0), "miniblocks");              // does not divide, though 1152 / 35 rounds to 32
        Refused(Dbp.Header(128, 8, 1, 0), "miniblocks");                // 16 values each: not a multiple of 32
        Refused([0x80], "cut short");                                   // a header cut short
        Refused([.. Enumerable.Repeat((byte)0xFF, 11)], "too long");    // a varint too long
        Refused(good, "fewer than the 301", wanted: 301);               // fewer values than present
        // The last miniblock may stop at its last value's byte (padding left out); one byte less is cut short.
        byte[] trimmed = Dbp.Encode(values, 128, 4, int32: false, null, trimLast: true);
        Assert.Equal(trimmed.Length, DeltaBinaryPacked.Decode(trimmed, new long[300].AsSpan(), scratch, toEnd: true));
        Assert.True(trimmed.Length < good.Length);
        Refused(trimmed[..^1], "cut short");
        Refused(good[..10], "cut short");

        // INT32 deltas 33 bits wide, as DuckDB writes them (taken in 64 bits, not wrapped to 32): read in 64 bits, truncated.
        int[] full = [int.MaxValue, int.MinValue, int.MaxValue, 0];
        byte[] duck = Dbp.Encode([.. full.Select(v => (long)v)], 128, 4, int32: false, null, trimLast: false);
        var read = new int[4];
        DeltaBinaryPacked.Decode(duck, read.AsSpan(), scratch, toEnd: true);
        Assert.Equal(full, read);
        // A miniblock wider than 64 bits.
        byte[] wide = [.. Dbp.Header(128, 4, 2, 0), 0, 65, 0, 0, 0, .. new byte[65 * 32 / 8]];
        Assert.Contains("65 bits", Assert.Throws<ParquetFormatException>(() => DeltaBinaryPacked.Decode(wide, new long[2].AsSpan(), scratch, toEnd: true)).Message);

        // A header counting more values than the stream holds: read as far as wanted, refused when walked to the end.
        byte[] more = [.. Dbp.Header(128, 4, 1000, 0), .. good[Dbp.Header(128, 4, 300, 0).Length..]];
        var some = new long[300];
        DeltaBinaryPacked.Decode(more, some.AsSpan(), scratch, toEnd: false);
        Assert.Equal(values, some);
        Assert.Throws<ParquetFormatException>(() => DeltaBinaryPacked.Decode(more, some.AsSpan(), scratch, toEnd: true));

        void Refused(byte[] data, string message, int wanted = 1) =>
            Assert.Contains(message, Assert.Throws<ParquetFormatException>(() => DeltaBinaryPacked.Decode(data, new long[wanted].AsSpan(), scratch, toEnd: true)).Message);
    }

    /// <summary>
    /// DELTA_BINARY_PACKED pages through the reader: with nulls (the stream holds the present values only), over
    /// two pages, a header counting more values than the page holds, INT32 and INT64; on another physical type, refused.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void DeltaBinaryPackedPages_ReadWithNulls_AndOnlyForIntegers(bool levelsAsBits)
    {
        SchemaElement[] schema =
        [
            HandBuiltParquet.Group("schema", 2, repetition: null),
            HandBuiltParquet.Leaf("i", ParquetType.Int32, FieldRepetition.Optional),
            HandBuiltParquet.Leaf("l", ParquetType.Int64, FieldRepetition.Required),
        ];
        long[] ints = [5, -7, int.MaxValue, int.MinValue], longs = [.. Enumerable.Range(0, 7).Select(i => long.MaxValue - 3L * i * i)];
        byte[] Ints(long[] values) => Dbp.Encode(values, 128, 4, int32: true, null, trimLast: false);
        byte[] more = Dbp.Encode([.. longs[3..], 1, 2, 3], 128, 4, int32: false, null, trimLast: false); // three values more than the page holds
        // l has no levels: the hand-built file counts a page's values by its (unwritten) repetition levels.
        byte[] file = HandBuiltParquet.Build(schema, 7,
            new HandBuiltParquet.Column(["i"], ParquetType.Int32, 1, 0, [1, 0, 1, 1, 0, 0, 1], [], Ints(ints), Encoding: ParquetEncoding.DeltaBinaryPacked),
            new HandBuiltParquet.Column(["l"], ParquetType.Int64, 0, 0, [], [0, 0, 0], Dbp.Encode(longs[..3], 128, 4, int32: false, null, trimLast: false),
                Encoding: ParquetEncoding.DeltaBinaryPacked,
                MorePages: [new([], [0, 0, 0, 0], more, ParquetEncoding.DeltaBinaryPacked)]));
        using (var parquet = ParquetFile.Open(new MemoryStream(file)))
        using (var reader = parquet.OpenReader(new() { LevelsAsBits = levelsAsBits, BatchSize = 3 }))
        {
            List<int?> i = [];
            List<long> l = [];
            foreach (Apache.Arrow.RecordBatch batch in reader.ReadBatches())
            {
                using (batch)
                {
                    var a = (Apache.Arrow.Int32Array)batch.Column(0);
                    var b = (Apache.Arrow.Int64Array)batch.Column(1);
                    for (int k = 0; k < batch.Length; k++)
                    {
                        i.Add(a.GetValue(k));
                        l.Add(b.GetValue(k)!.Value);
                    }
                }
            }
            Assert.Equal((int?[])[5, null, -7, int.MaxValue, null, null, int.MinValue], i);
            Assert.Equal(longs, l);
        }

        SchemaElement[] doubles = [HandBuiltParquet.Group("schema", 1, repetition: null), HandBuiltParquet.Leaf("d", ParquetType.Double, FieldRepetition.Required)];
        byte[] wrong = HandBuiltParquet.Build(doubles, 2, new HandBuiltParquet.Column(["d"], ParquetType.Double, 0, 0, [], [], Dbp.Encode([1, 2], 128, 4, false, null, false), Encoding: ParquetEncoding.DeltaBinaryPacked));
        using var bad = ParquetFile.Open(new MemoryStream(wrong));
        using var refusing = bad.OpenReader(new());
        Assert.Contains("Double values cannot be encoded with DeltaBinaryPacked", Assert.Throws<ParquetFormatException>(() =>
        {
            foreach (Apache.Arrow.RecordBatch batch in refusing.ReadBatches()) batch.Dispose();
        }).Message);
    }

    // ------------------------------------------------------------------
    // DELTA_LENGTH_BYTE_ARRAY and DELTA_BYTE_ARRAY
    // ------------------------------------------------------------------

    [Fact]
    public void DeltaByteArrays_DecodeWhatTheReferenceEncodes()
    {
        var random = new Random(37);
        var scratch = new ulong[DeltaBinaryPacked.Chunk];
        foreach (int count in (int[])[0, 1, 2, 5, 129, 700])
        {
            foreach (string shape in (string[])["shared", "random", "empty", "long", "fixed", "medium"])
            {
                byte[][] values = [.. Enumerable.Range(0, count).Select(i => shape switch
                {
                    "shared" => System.Text.Encoding.UTF8.GetBytes($"customer_{i / 7:D6}_{i % 7}"), // long common prefixes
                    "empty" => i % 3 == 0 ? [] : [(byte)i],
                    "long" => Bytes(random, random.Next(200, 3000)),
                    "fixed" => Bytes(random, 4),
                    "medium" => i % 2 == 0 && i > 0 ? [.. Bytes(random, 0), .. System.Text.Encoding.UTF8.GetBytes(new string('m', random.Next(17, 60)))] : Bytes(random, random.Next(14, 50)), // around the 16- and 32-byte copies
                    _ => Bytes(random, random.Next(0, 20)),
                })];
                if (shape == "shared") Array.Sort(values, (a, b) => a.AsSpan().SequenceCompareTo(b));
                int fixedLength = shape == "fixed" ? 4 : 0;
                byte[] expected = fixedLength > 0 ? [.. values.SelectMany(v => v)] : [.. values.SelectMany(v => (byte[])[.. BitConverter.GetBytes(v.Length), .. v])];
                foreach (bool prefixed in (bool[])[false, true])
                {
                    if (fixedLength > 0 && !prefixed) continue; // DELTA_LENGTH_BYTE_ARRAY is for BYTE_ARRAY only
                    byte[] encoded = prefixed ? Delta.ByteArray(values) : Delta.LengthByteArray(values);
                    var lengths = new int[count];
                    var prefixes = new int[prefixed ? count : 0];
                    var sections = DeltaByteArrays.Read(encoded, prefixed, prefixes, lengths, scratch);
                    var plain = new byte[DeltaByteArrays.PlainLength(prefixes, lengths, fixedLength)];
                    DeltaByteArrays.WritePlain(encoded.AsSpan(sections.DataStart, (int)sections.DataBytes), prefixes, lengths, plain, fixedLength);
                    Assert.True(plain.AsSpan().SequenceEqual(expected), $"{shape}, {count} values, {(prefixed ? "DELTA_BYTE_ARRAY" : "DELTA_LENGTH_BYTE_ARRAY")}");
                    Assert.Equal(encoded.Length, sections.DataStart + sections.DataBytes);
                    if (fixedLength > 0) continue;

                    // The same values as data, starts and lengths (the readers that take them so).
                    var starts = new int[count];
                    ReadOnlySpan<byte> data = encoded.AsSpan(sections.DataStart, (int)sections.DataBytes);
                    byte[] direct;
                    if (prefixed)
                    {
                        direct = new byte[DeltaByteArrays.ValuesLength(prefixes, lengths) + DeltaByteArrays.Slack];
                        DeltaByteArrays.WriteValues(data, prefixes, lengths, direct, starts);
                    }
                    else
                    {
                        direct = data.ToArray();
                        DeltaByteArrays.Starts(lengths, starts);
                    }
                    for (int i = 0; i < count; i++)
                        Assert.True(direct.AsSpan(starts[i], lengths[i]).SequenceEqual(values[i]), $"direct: {shape}, value {i} of {count}");
                }
            }
        }

        static byte[] Bytes(Random random, int length)
        {
            var bytes = new byte[length];
            random.NextBytes(bytes);
            return bytes;
        }
    }

    [Fact]
    public void DeltaByteArrays_RefuseDamage()
    {
        var scratch = new ulong[DeltaBinaryPacked.Chunk];
        byte[] Lengths(params long[] lengths) => Dbp.Encode(lengths, 128, 4, int32: true, null, trimLast: false);

        Refused([.. Lengths(3, -1), .. "abc"u8], prefixed: false, 2, "length is -1");
        Refused([.. Lengths(3, 4), .. "abcdef"u8], prefixed: false, 2, "run past the page");
        Refused([.. Lengths(1, 2), .. Lengths(2, 1), .. "abc"u8], prefixed: true, 2, "prefix of 1 bytes is longer than the previous value (0 bytes)"); // the first prefix
        Refused([.. Lengths(0, 3), .. Lengths(2, 1), .. "abc"u8], prefixed: true, 2, "prefix of 3 bytes is longer than the previous value (2 bytes)");
        Refused([.. Lengths(0, -2), .. Lengths(2, 1), .. "abc"u8], prefixed: true, 2, "prefix length is -2");
        Refused([.. Lengths(0, 0), .. Lengths(2), .. "ab"u8], prefixed: true, 2, "fewer than the 2 present"); // fewer suffixes than prefixes

        // FIXED_LEN_BYTE_ARRAY values of another length.
        byte[] fixedWrong = [.. Lengths(0, 1), .. Lengths(2, 2), .. "abcd"u8];
        var lengths = new int[2];
        var prefixes = new int[2];
        var sections = DeltaByteArrays.Read(fixedWrong, true, prefixes, lengths, scratch);
        Assert.Contains("FIXED_LEN_BYTE_ARRAY(2) value is 3 bytes", Assert.Throws<ParquetFormatException>(() =>
            DeltaByteArrays.WritePlain(fixedWrong.AsSpan(sections.DataStart), prefixes, lengths, new byte[4], 2)).Message);

        // Writing checks prefixes too (a second guard: Read checks them before the output is sized).
        Assert.Contains("prefix of 3 bytes is longer than the previous value (1 bytes)", Assert.Throws<ParquetFormatException>(() =>
            DeltaByteArrays.WritePlain("ab"u8, (int[])[0, 3], (int[])[1, 1], new byte[16], 0)).Message);

        void Refused(byte[] encoded, bool prefixed, int count, string message) =>
            Assert.Contains(message, Assert.Throws<ParquetFormatException>(() =>
                DeltaByteArrays.Read(encoded, prefixed, new int[prefixed ? count : 0], new int[count], scratch)).Message);
    }

    /// <summary>
    /// Delta byte array pages through the reader: strings with nulls in both layouts (UTF-8 checked as for PLAIN),
    /// FIXED_LEN_BYTE_ARRAY decimals; DELTA_LENGTH_BYTE_ARRAY on a fixed-length column refused.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void DeltaByteArrayPages_ReadLikePlain(bool viewTypes)
    {
        SchemaElement[] schema =
        [
            HandBuiltParquet.Group("schema", 2, repetition: null),
            HandBuiltParquet.Leaf("s", ParquetType.ByteArray, FieldRepetition.Optional, ConvertedType.Utf8),
            new SchemaElement { Name = "d", Type = ParquetType.FixedLenByteArray, TypeLength = 4, Repetition = FieldRepetition.Optional, ConvertedType = ConvertedType.Decimal, Precision = 9, Scale = 2 },
        ];
        byte[][] strings = [.. ((string[])["apple", "applesauce", "apricot", "a long value that is not inline in a view"]).Select(System.Text.Encoding.UTF8.GetBytes)];
        byte[][] decimals = [[0, 0, 0x30, 0x39], [0, 0, 0x30, 0x3A], [0xFF, 0xFF, 0xFF, 0xFF]]; // 123.45, 123.46, -0.01
        byte[] defs = [1, 0, 1, 1, 0, 1];
        string?[] expectedStrings = ["apple", null, "applesauce", "apricot", null, "a long value that is not inline in a view"];
        decimal?[] expectedDecimals = [123.45m, null, 123.46m, -0.01m, null, null];
        byte[] file = HandBuiltParquet.Build(schema, 6,
            new HandBuiltParquet.Column(["s"], ParquetType.ByteArray, 1, 0, defs, [], Delta.ByteArray(strings), Encoding: ParquetEncoding.DeltaByteArray),
            new HandBuiltParquet.Column(["d"], ParquetType.FixedLenByteArray, 1, 0, [1, 0, 1, 1, 0, 0], [], Delta.ByteArray(decimals), Encoding: ParquetEncoding.DeltaByteArray));
        using (var parquet = ParquetFile.Open(new MemoryStream(file)))
        using (var reader = parquet.OpenReader(new() { ViewTypes = viewTypes, BatchSize = 4 }))
        {
            List<string?> s = [];
            List<decimal?> d = [];
            foreach (Apache.Arrow.RecordBatch batch in reader.ReadBatches())
            {
                using (batch)
                {
                    var column = batch.Column(0);
                    var decimalColumn = (Apache.Arrow.Decimal128Array)batch.Column(1);
                    for (int k = 0; k < batch.Length; k++)
                    {
                        s.Add(column switch
                        {
                            Apache.Arrow.StringViewArray views => views.GetString(k),
                            Apache.Arrow.StringArray classic => classic.GetString(k),
                            _ => throw new InvalidOperationException(),
                        });
                        d.Add(decimalColumn.GetValue(k));
                    }
                }
            }
            Assert.Equal(expectedStrings, s);
            Assert.Equal(expectedDecimals, d);
        }

        // Not UTF-8, and DELTA_LENGTH_BYTE_ARRAY for a FIXED_LEN_BYTE_ARRAY column.
        byte[] notUtf8 = HandBuiltParquet.Build(schema, 1,
            new HandBuiltParquet.Column(["s"], ParquetType.ByteArray, 1, 0, [1], [], Delta.ByteArray([[0x68, 0xC3]]), Encoding: ParquetEncoding.DeltaByteArray),
            new HandBuiltParquet.Column(["d"], ParquetType.FixedLenByteArray, 1, 0, [0], [], [], Encoding: ParquetEncoding.Plain));
        Assert.Contains("UTF-8", ReadError(notUtf8));
        byte[] wrongType = HandBuiltParquet.Build(schema, 1,
            new HandBuiltParquet.Column(["s"], ParquetType.ByteArray, 1, 0, [0], [], [], Encoding: ParquetEncoding.Plain),
            new HandBuiltParquet.Column(["d"], ParquetType.FixedLenByteArray, 1, 0, [1], [], Delta.LengthByteArray([[0, 0, 0, 1]]), Encoding: ParquetEncoding.DeltaLengthByteArray));
        Assert.Contains("FixedLenByteArray values cannot be encoded with DeltaLengthByteArray", ReadError(wrongType));

        string ReadError(byte[] bytes)
        {
            using var parquet = ParquetFile.Open(new MemoryStream(bytes));
            using var reader = parquet.OpenReader(new() { ViewTypes = viewTypes });
            return Assert.Throws<ParquetFormatException>(() =>
            {
                foreach (Apache.Arrow.RecordBatch batch in reader.ReadBatches()) batch.Dispose();
            }).Message;
        }
    }

    // ------------------------------------------------------------------
    // BYTE_STREAM_SPLIT
    // ------------------------------------------------------------------

    [Fact]
    public void ByteStreamSplit_JoinsTheStreams_ForEveryWidth()
    {
        var random = new Random(41);
        for (int width = 1; width <= 16; width++)
        {
            foreach (int count in (int[])[0, 1, 15, 16, 17, 31, 32, 33, 100, 257])
            {
                foreach (int extra in (int[])[0, 1, 9])
                {
                    int stride = count + extra; // streams may hold more values than are taken
                    var streams = new byte[stride * width];
                    random.NextBytes(streams);
                    var expected = new byte[count * width];
                    for (int i = 0; i < count; i++)
                    {
                        for (int j = 0; j < width; j++) expected[i * width + j] = streams[j * stride + i];
                    }
                    var joined = new byte[count * width + 3];
                    Kaminari.Sazanami.Encodings.ByteStreamSplit.Decode(streams, width, count, joined);
                    Assert.True(joined.AsSpan(0, count * width).SequenceEqual(expected), $"width {width}, {count} values of {stride}");
                    Assert.True(joined.AsSpan(count * width).SequenceEqual(new byte[3]), "past the values");
                }
            }
        }
        Assert.Throws<ArgumentException>(() => Kaminari.Sazanami.Encodings.ByteStreamSplit.Decode(new byte[10], 4, 1, new byte[4]));
        Assert.Throws<ArgumentOutOfRangeException>(() => Kaminari.Sazanami.Encodings.ByteStreamSplit.Decode(new byte[8], 4, 3, new byte[12]));
        Assert.Throws<ArgumentException>(() => Kaminari.Sazanami.Encodings.ByteStreamSplit.Decode(new byte[8], 4, 2, new byte[7]));
    }

    /// <summary>
    /// BYTE_STREAM_SPLIT pages through the reader: doubles with nulls, a FIXED_LEN_BYTE_ARRAY(2) Float16 column whose
    /// streams hold more values than the page's present ones; damaged lengths and other physical types refused.
    /// </summary>
    [Fact]
    public void ByteStreamSplitPages_ReadLikePlain()
    {
        SchemaElement[] schema =
        [
            HandBuiltParquet.Group("schema", 2, repetition: null),
            HandBuiltParquet.Leaf("x", ParquetType.Double, FieldRepetition.Optional),
            new SchemaElement { Name = "h", Type = ParquetType.FixedLenByteArray, TypeLength = 2, Repetition = FieldRepetition.Optional, LogicalType = new LogicalType(LogicalTypeKind.Float16) },
        ];
        double[] doubles = [1.5, -0.0, double.NaN, 1e300];
        Half[] halves = [(Half)1.5, (Half)(-2), (Half)0.25];
        byte[] file = HandBuiltParquet.Build(schema, 6,
            new HandBuiltParquet.Column(["x"], ParquetType.Double, 1, 0, [1, 0, 1, 1, 0, 1], [], Split([.. doubles.SelectMany(BitConverter.GetBytes)], 8), Encoding: ParquetEncoding.ByteStreamSplit),
            new HandBuiltParquet.Column(["h"], ParquetType.FixedLenByteArray, 1, 0, [0, 1, 1, 0, 1, 0], [], Split([.. halves.SelectMany(BitConverter.GetBytes), 0x11, 0x22], 2), Encoding: ParquetEncoding.ByteStreamSplit));
        using (var parquet = ParquetFile.Open(new MemoryStream(file)))
        using (var reader = parquet.OpenReader(new()))
        {
            using Apache.Arrow.RecordBatch batch = Assert.Single(reader.ReadBatches());
            var x = (Apache.Arrow.DoubleArray)batch.Column(0);
            var h = (Apache.Arrow.HalfFloatArray)batch.Column(1);
            Assert.Equal((double?[])[1.5, null, -0.0, double.NaN, null, 1e300], [.. Enumerable.Range(0, 6).Select(x.GetValue)]);
            Assert.True(BitConverter.DoubleToInt64Bits(x.GetValue(2)!.Value) == BitConverter.DoubleToInt64Bits(-0.0));
            Assert.Equal((Half?[])[null, (Half)1.5, (Half)(-2), null, (Half)0.25, null], [.. Enumerable.Range(0, 6).Select(h.GetValue)]);
        }

        SchemaElement[] one = [HandBuiltParquet.Group("schema", 1, repetition: null), HandBuiltParquet.Leaf("x", ParquetType.Double, FieldRepetition.Required)];
        Assert.Contains("does not split into streams of 8-byte values", ReadError(HandBuiltParquet.Build(one, 1, new HandBuiltParquet.Column(["x"], ParquetType.Double, 0, 0, [], [], new byte[12], Encoding: ParquetEncoding.ByteStreamSplit))));
        Assert.Contains("holds 1 values, fewer than the 2 present", ReadError(HandBuiltParquet.Build(one, 2, new HandBuiltParquet.Column(["x"], ParquetType.Double, 0, 0, [], [], new byte[8], Encoding: ParquetEncoding.ByteStreamSplit))));
        SchemaElement[] strings = [HandBuiltParquet.Group("schema", 1, repetition: null), HandBuiltParquet.Leaf("s", ParquetType.ByteArray, FieldRepetition.Required)];
        Assert.Contains("ByteArray values cannot be encoded with ByteStreamSplit", ReadError(HandBuiltParquet.Build(strings, 1, new HandBuiltParquet.Column(["s"], ParquetType.ByteArray, 0, 0, [], [], new byte[8], Encoding: ParquetEncoding.ByteStreamSplit))));

        static byte[] Split(byte[] plain, int width)
        {
            int n = plain.Length / width;
            var streams = new byte[plain.Length];
            for (int i = 0; i < n; i++)
            {
                for (int j = 0; j < width; j++) streams[j * n + i] = plain[i * width + j];
            }
            return streams;
        }

        static string ReadError(byte[] bytes)
        {
            using var parquet = ParquetFile.Open(new MemoryStream(bytes));
            using var reader = parquet.OpenReader(new());
            return Assert.Throws<ParquetFormatException>(() =>
            {
                foreach (Apache.Arrow.RecordBatch batch in reader.ReadBatches()) batch.Dispose();
            }).Message;
        }
    }

    /// <summary>DELTA_LENGTH_BYTE_ARRAY and DELTA_BYTE_ARRAY encoders written from Encodings.md.</summary>
    internal static class Delta
    {
        public static byte[] LengthByteArray(byte[][] values) =>
            [.. Dbp.Encode([.. values.Select(v => (long)v.Length)], 128, 4, int32: true, null, trimLast: false), .. values.SelectMany(v => v)];

        public static byte[] ByteArray(byte[][] values)
        {
            var prefixes = new long[values.Length];
            byte[][] suffixes = new byte[values.Length][];
            for (int i = 0; i < values.Length; i++)
            {
                int prefix = 0;
                if (i > 0)
                {
                    while (prefix < values[i].Length && prefix < values[i - 1].Length && values[i][prefix] == values[i - 1][prefix]) prefix++;
                }
                prefixes[i] = prefix;
                suffixes[i] = values[i][prefix..];
            }
            return [.. Dbp.Encode(prefixes, 128, 4, int32: true, null, trimLast: false), .. LengthByteArray(suffixes)];
        }
    }

    /// <summary>A DELTA_BINARY_PACKED encoder written from Encodings.md.</summary>
    internal static class Dbp
    {
        public static byte[] Header(long blockSize, long miniblocks, long count, long first)
        {
            List<byte> data = [];
            Uleb(data, (ulong)blockSize);
            Uleb(data, (ulong)miniblocks);
            Uleb(data, (ulong)count);
            Uleb(data, Zigzag(first));
            return [.. data];
        }

        /// <summary>
        /// Encodes <paramref name="values"/> (INT32 ones as 32-bit deltas); with <paramref name="garbage"/>, the bit
        /// widths of the last block's unused miniblocks are random; with <paramref name="trimLast"/>, the last
        /// miniblock stops at its last value's byte (no padding).
        /// </summary>
        public static byte[] Encode(long[] values, int blockSize, int miniblocks, bool int32, Random? garbage, bool trimLast)
        {
            List<byte> data = [.. Header(blockSize, miniblocks, values.Length, values.Length == 0 ? 0 : values[0])];
            int per = blockSize / miniblocks;
            long[] deltas = [.. Enumerable.Range(1, Math.Max(0, values.Length - 1)).Select(i => int32 ? (long)(int)((uint)values[i] - (uint)values[i - 1]) : unchecked(values[i] - values[i - 1]))];
            for (int start = 0; start < deltas.Length; start += blockSize)
            {
                long[] block = deltas[start..Math.Min(deltas.Length, start + blockSize)];
                long min = block.Min();
                Uleb(data, Zigzag(min));
                ulong[] relative = [.. block.Select(d => int32 ? (ulong)(uint)((int)d - (int)min) : unchecked((ulong)(d - min)))];
                int used = (relative.Length + per - 1) / per;
                int[] widths = [.. Enumerable.Range(0, miniblocks).Select(m => m < used
                    ? 64 - BitOperations.LeadingZeroCount(relative.Skip(m * per).Take(per).Aggregate(0UL, (a, v) => a | v))
                    : garbage?.Next(256) ?? 0)];
                data.AddRange(widths.Select(w => (byte)w));
                for (int m = 0; m < used; m++)
                {
                    ulong[] mini = relative.Skip(m * per).Take(per).ToArray();
                    bool last = start + blockSize >= deltas.Length && m == used - 1;
                    int bytes = trimLast && last ? (mini.Length * widths[m] + 7) / 8 : per * widths[m] / 8;
                    var packed = new byte[per * widths[m] / 8];
                    for (int i = 0; i < mini.Length; i++)
                    {
                        for (int k = 0; k < widths[m]; k++)
                        {
                            long bit = (long)i * widths[m] + k;
                            if ((mini[i] >> k & 1) != 0) packed[bit >> 3] |= (byte)(1 << (int)(bit & 7));
                        }
                    }
                    data.AddRange(packed.Take(bytes));
                }
            }
            return [.. data];
        }

        private static ulong Zigzag(long value) => (ulong)((value << 1) ^ (value >> 63));

        private static void Uleb(List<byte> data, ulong value)
        {
            for (; value >= 0x80; value >>= 7) data.Add((byte)(value | 0x80));
            data.Add((byte)value);
        }
    }
}
