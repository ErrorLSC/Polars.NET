using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Compression;
using Kaminari.Sazanami.Rows;
using Kaminari.Sazanami.Helpers;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Codecs;
using Kaminari.Seseragi.Files.Parquet.Encodings;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Seseragi.Files.Fuzz;

/// <summary>What a case did: read to the end, refused as expected, or a finding (anything else).</summary>
internal readonly record struct Outcome(bool Finding, string Signature, string Detail)
{
    public static readonly Outcome Read = new(false, "read", "");
    public static Outcome Refused(Exception e) => new(false, "refused", e.Message);

    public static Outcome From(Exception e) =>
        new(true, $"{e.GetType().Name} at {TopFrame(e)}", e.ToString());

    /// <summary>The innermost frame in this repository's code (the place to look).</summary>
    private static string TopFrame(Exception e)
    {
        foreach (string line in (e.StackTrace ?? "").Split('\n'))
        {
            string frame = line.Trim();
            if (frame.StartsWith("at Kaminari.", StringComparison.Ordinal))
            {
                int paren = frame.IndexOf('(');
                return paren > 0 ? frame[3..paren] : frame[3..];
            }
        }
        return "?";
    }
}

/// <summary>
/// The cases: case <c>n</c> of seed <c>s</c> is the same everywhere (reproducible from the two numbers). Three in four
/// are a corpus file with a few mutations, read with random options; the fourth feeds a codec or the RLE decoder
/// directly, its input and output ending at guard pages.
/// </summary>
internal sealed class Cases(IReadOnlyList<string> corpus)
{
    private readonly Dictionary<string, (byte[] Bytes, long[] Hot, int Footer)> _files = [];
    private static readonly int[] BatchSizes = [1, 3, 64, 1000, 65_536];
    private static readonly int[] SplitWidths = [2, 4, 8];

    public static Random RandomFor(long seed, long n)
    {
        // SplitMix64 of the pair: independent streams per case, stable across processes (unlike HashCode).
        ulong z = (ulong)seed * 0x9E3779B97F4A7C15UL + (ulong)n * 0xD1B54A32D192ED03UL + 0x632BE59BD9B4E019UL;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return new Random((int)((z ^ (z >> 31)) & 0x7FFFFFFF));
    }

    public static bool IsFileCase(long n) => n % 4 != 3;

    public Outcome Run(long seed, long n)
    {
        Random random = RandomFor(seed, n);
        try
        {
            return IsFileCase(n) ? ReadFile(Mutate(random, out _), random) : Kernel(random);
        }
        catch (Exception e)
        {
            return Outcome.From(e);
        }
    }

    /// <summary>The mutated file of a file case (for saving a finding).</summary>
    public byte[] FileOf(long seed, long n, out string source) => Mutate(RandomFor(seed, n), out source);

    // ------------------------------------------------------------------
    // Files
    // ------------------------------------------------------------------

    private byte[] Mutate(Random random, out string source)
    {
        source = corpus[random.Next(corpus.Count)];
        var (original, hot, footer) = Load(source);
        List<byte> bytes = [.. original];
        int mutations = 1 + random.Next(random.Next(2) == 0 ? 2 : 8);
        for (int m = 0; m < mutations && bytes.Count > 0; m++)
        {
            int at = Position(random, bytes.Count, hot, footer);
            switch (random.Next(100))
            {
                case < 35:
                    bytes[at] ^= (byte)(1 << random.Next(8));
                    break;
                case < 55:
                    bytes[at] = Interesting8[random.Next(Interesting8.Length)];
                    break;
                case < 70:
                {
                    // A little-endian 32-bit value: a length, an offset, a count.
                    int value = Interesting32[random.Next(Interesting32.Length)];
                    for (int k = 0; k < 4 && at + k < bytes.Count; k++) bytes[at + k] = (byte)(value >> (8 * k));
                    break;
                }
                case < 78:
                    bytes[at] = (byte)(bytes[at] + random.Next(-3, 4)); // varints and small counts, nudged
                    break;
                case < 84:
                {
                    int length = Math.Min(random.Next(1, 64), bytes.Count - at);
                    bytes.RemoveRange(at, length);
                    break;
                }
                case < 90:
                {
                    int length = Math.Min(random.Next(1, 64), bytes.Count - at);
                    bytes.InsertRange(at, bytes.GetRange(at, length));
                    break;
                }
                case < 96:
                {
                    // Bytes of another file over these (pages and headers of other writers).
                    byte[] other = Load(corpus[random.Next(corpus.Count)]).Bytes;
                    int from = random.Next(other.Length), length = Math.Min(Math.Min(random.Next(1, 512), other.Length - from), bytes.Count - at);
                    for (int k = 0; k < length; k++) bytes[at + k] = other[from + k];
                    break;
                }
                default:
                    // Cut short, the footer kept: what a torn upload looks like to the page reader.
                    if (random.Next(2) == 0) bytes.RemoveRange(at, bytes.Count - at);
                    else
                    {
                        int keep = Math.Min(bytes.Count - at, footer + 8);
                        bytes.RemoveRange(at, Math.Max(0, bytes.Count - at - keep));
                    }
                    break;
            }
        }
        return [.. bytes];
    }

    private static readonly byte[] Interesting8 = [0, 1, 2, 0x7F, 0x80, 0xFF, 0xFE, 0x40, 0x10, 8, 9, 0x15, 0x19, 0x1C, 0x26];
    private static readonly int[] Interesting32 = [0, 1, -1, 2, 7, 8, 9, 63, 64, 65, 255, 256, 4096, 65535, 65536, 1 << 20, int.MaxValue, int.MinValue, int.MaxValue - 7];

    /// <summary>Anywhere, near a column chunk's start (page headers, level lengths, run headers), or in the footer.</summary>
    private static int Position(Random random, int length, long[] hot, int footer)
    {
        int roll = random.Next(100);
        if (roll < 40 && hot.Length > 0)
        {
            long at = hot[random.Next(hot.Length)] + random.Next(random.Next(2) == 0 ? 48 : 512);
            if (at < length) return (int)at;
        }
        if (roll is >= 40 and < 55 && length > 8 && footer > 0)
        {
            int at = length - 8 - 1 - random.Next(Math.Min(footer, length - 8));
            if (at >= 0) return at;
        }
        return random.Next(length);
    }

    private (byte[] Bytes, long[] Hot, int Footer) Load(string path)
    {
        if (_files.TryGetValue(path, out var known)) return known;
        byte[] bytes = File.ReadAllBytes(path);
        int footer = bytes.Length >= 12 ? Math.Clamp(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8)), 0, bytes.Length - 12) : 0;
        List<long> hot = [];
        try
        {
            using var file = ParquetFile.Open(new MemoryStream(bytes));
            foreach (RowGroup group in file.Thrift.RowGroups)
            {
                foreach (ColumnChunk chunk in group.Columns)
                {
                    if (chunk.MetaData is not { } meta) continue;
                    hot.Add(meta.DataPageOffset);
                    if (meta.DictionaryPageOffset is long dictionary) hot.Add(dictionary);
                    // Page indexes: read when rows are skipped or a filter prunes pages.
                    if (chunk.ColumnIndexOffset is long columnIndex) hot.Add(columnIndex);
                    if (chunk.OffsetIndexOffset is long offsetIndex) hot.Add(offsetIndex);
                }
            }
        }
        catch (ParquetFormatException)
        {
            // Files the reader refuses (encrypted, unsupported) still serve as raw material.
        }
        return _files[path] = (bytes, [.. hot], footer);
    }

    private static Outcome ReadFile(byte[] bytes, Random random)
    {
        try
        {
            using var file = ParquetFile.Open(new MemoryStream(bytes));
            string[] names = [.. file.Schema.FieldsList.Select(f => f.Name)];
            var options = new ParquetReadOptions
            {
                MaxDegreeOfParallelism = 1 + random.Next(2),
                ViewTypes = random.Next(2) == 0,
                BatchSize = BatchSizes[random.Next(BatchSizes.Length)],
                VerifyChecksums = random.Next(4) == 0,
                Reading = (ChunkReading)random.Next(4),
                LevelsAsBits = random.Next(4) != 0,
                Columns = random.Next(3) == 0 && names.Length > 1 ? [.. names.Where(_ => random.Next(2) == 0)] : null,
                // Row selections and filters read page indexes and skip pages by them.
                // Inside the row groups (a damaged footer's num_rows may say otherwise; rows past them are an argument error).
                Rows = random.Next(4) == 0 ? RandomRows(random, file.Metadata.RowGroups.Sum(g => Math.Max(0, g.RowCount))) : null,
                Filter = random.Next(4) == 0 ? RandomFilter(random, file.Schema) : null,
                MaxRows = random.Next(8) == 0 ? random.Next(0, 5000) : null,
                FetchGap = random.Next(2) == 0 ? 0 : 1 << 20,
            };
            using var reader = file.OpenReader(options);
            foreach (RecordBatch batch in reader.ReadBatches()) batch.Dispose();
            return Outcome.Read;
        }
        catch (ParquetFormatException e)
        {
            return Outcome.Refused(e);
        }
    }

    /// <summary>A few ranges of rows inside the file (none when its footer gives no rows).</summary>
    private static RowRanges? RandomRows(Random random, long rows)
    {
        if (rows <= 0) return null;
        int n = 1 + random.Next(4);
        var ranges = new (long, long)[n];
        for (int i = 0; i < n; i++)
        {
            long start = random.NextInt64(rows);
            ranges[i] = (start, random.NextInt64(1, Math.Max(2, rows - start + 1)));
        }
        return RowRanges.Of(ranges).Intersect(RowRanges.All(rows));
    }

    /// <summary>A comparison of a random top-level column with a literal of its kind, or a conjunction of two.</summary>
    private static ScalarExpr? RandomFilter(Random random, Schema schema)
    {
        if (schema.FieldsList.Count == 0) return null;
        ScalarExpr Comparison()
        {
            Field field = schema.FieldsList[random.Next(schema.FieldsList.Count)];
            LiteralValue literal = field.DataType switch
            {
                StringType or StringViewType => LiteralValue.NewString(((char)('a' + random.Next(26))).ToString()),
                FloatType or DoubleType or HalfFloatType => LiteralValue.NewFloat64(random.NextDouble() * 2000 - 1000),
                BooleanType => LiteralValue.NewBool(random.Next(2) == 0),
                Date32Type => LiteralValue.NewDate(DateOnly.FromDayNumber(random.Next(700_000, 760_000))),
                _ => LiteralValue.NewInt64(random.NextInt64(-5000, 5000)),
            };
            BinaryOp op = random.Next(6) switch
            {
                0 => BinaryOp.Eq, 1 => BinaryOp.NotEq, 2 => BinaryOp.Lt, 3 => BinaryOp.LtEq, 4 => BinaryOp.Gt, _ => BinaryOp.GtEq,
            };
            return ScalarExpr.NewBinary(op, ScalarExpr.NewColumn(field.Name), ScalarExpr.NewLiteral(literal));
        }
        return random.Next(3) == 0 ? ScalarExpr.NewBinary(BinaryOp.And, Comparison(), Comparison()) : Comparison();
    }

    // ------------------------------------------------------------------
    // Kernels
    // ------------------------------------------------------------------

    private static Outcome Kernel(Random random) => random.Next(6) switch
    {
        0 => Codec(random, snappy: true),
        1 => Codec(random, snappy: false),
        2 => Rle(random),
        3 => Delta(random),
        4 => DeltaArrays(random),
        _ => Split(random),
    };

    /// <summary>A DELTA_BINARY_PACKED stream: a plausible header (or not), then random blocks; decoded as INT32 and INT64, to the end and not.</summary>
    private static byte[] DeltaStream(Random random)
    {
        List<byte> data = [];
        if (random.Next(4) != 0)
        {
            int[] blocks = [128, 256, 1024];
            int block = blocks[random.Next(blocks.Length)], miniblocks = random.Next(2) == 0 ? 4 : block / 32;
            Uleb(data, (ulong)block);
            Uleb(data, (ulong)miniblocks);
            Uleb(data, (ulong)random.Next(random.Next(2) == 0 ? 40 : 3000));
            Uleb(data, (ulong)random.NextInt64());
        }
        var rest = new byte[random.Next(random.Next(2) == 0 ? 64 : 4000)];
        random.NextBytes(rest);
        // Mostly narrow miniblocks, as writers make them.
        for (int at = 0; at < rest.Length && random.Next(4) != 0; at += random.Next(1, 300)) rest[at] = (byte)random.Next(random.Next(3) == 0 ? 256 : 33);
        data.AddRange(rest);
        return [.. data];

        static void Uleb(List<byte> data, ulong value)
        {
            for (; value >= 0x80; value >>= 7) data.Add((byte)(value | 0x80));
            data.Add((byte)value);
        }
    }

    private static Outcome Delta(Random random)
    {
        using var input = GuardedMemory.Block.Copy(DeltaStream(random));
        int count = random.Next(random.Next(2) == 0 ? 50 : 3500);
        var scratch = new ulong[DeltaBinaryPacked.Chunk];
        try
        {
            using var longs = new GuardedMemory.Block(count * 8);
            DeltaBinaryPacked.Decode(input.GetSpan(), MemoryMarshal.Cast<byte, long>(longs.GetSpan()), scratch, toEnd: random.Next(2) == 0);
            using var ints = new GuardedMemory.Block(count * 4);
            DeltaBinaryPacked.Decode(input.GetSpan(), MemoryMarshal.Cast<byte, int>(ints.GetSpan()), scratch, toEnd: random.Next(2) == 0);
            for (int i = 0; i < count; i++)
            {
                if ((int)MemoryMarshal.Cast<byte, long>(longs.GetSpan())[i] != MemoryMarshal.Cast<byte, int>(ints.GetSpan())[i])
                    return new Outcome(true, "DELTA_BINARY_PACKED INT32 differs from truncated INT64", $"value {i}");
            }
            return Outcome.Read;
        }
        catch (ParquetFormatException e)
        {
            return Outcome.Refused(e);
        }
    }

    /// <summary>DELTA_LENGTH_BYTE_ARRAY / DELTA_BYTE_ARRAY sections from random streams, written as PLAIN into a guarded buffer of exactly their size.</summary>
    private static Outcome DeltaArrays(Random random)
    {
        bool prefixed = random.Next(2) == 0;
        byte[] encoded = prefixed ? [.. DeltaStream(random), .. DeltaStream(random)] : DeltaStream(random);
        int count = random.Next(random.Next(2) == 0 ? 30 : 2000);
        int fixedLength = prefixed && random.Next(3) == 0 ? random.Next(1, 20) : 0;
        using var input = GuardedMemory.Block.Copy(encoded);
        var scratch = new ulong[DeltaBinaryPacked.Chunk];
        try
        {
            var lengths = new int[count];
            var prefixes = new int[prefixed ? count : 0];
            var sections = DeltaByteArrays.Read(input.GetSpan(), prefixed, prefixes, lengths, scratch);
            long size = DeltaByteArrays.PlainLength(prefixes, lengths, fixedLength);
            if (size > 64 << 20) return Outcome.Read; // a legitimately huge expansion: not this target's business
            using var plain = new GuardedMemory.Block((int)size);
            ReadOnlySpan<byte> data = input.GetSpan().Slice(sections.DataStart, (int)sections.DataBytes);
            DeltaByteArrays.WritePlain(data, prefixes, lengths, plain.GetSpan(), fixedLength);
            if (prefixed && fixedLength == 0)
            {
                // The direct form, into a buffer of exactly its size plus the slack it may write.
                using var values = new GuardedMemory.Block((int)(DeltaByteArrays.ValuesLength(prefixes, lengths) + DeltaByteArrays.Slack));
                DeltaByteArrays.WriteValues(data, prefixes, lengths, values.GetSpan(), new int[count]);
            }
            return Outcome.Read;
        }
        catch (ParquetFormatException e)
        {
            return Outcome.Refused(e);
        }
    }

    /// <summary>Byte stream split of random widths and lengths, joined into a guarded buffer of exactly the values' size.</summary>
    private static Outcome Split(Random random)
    {
        int width = random.Next(4) == 0 ? random.Next(1, 40) : SplitWidths[random.Next(SplitWidths.Length)];
        int stride = random.Next(random.Next(2) == 0 ? 40 : 3000);
        int count = random.Next(stride + 1);
        var streams = new byte[stride * width];
        random.NextBytes(streams);
        using var input = GuardedMemory.Block.Copy(streams);
        using var output = new GuardedMemory.Block(count * width);
        Kaminari.Sazanami.Encodings.ByteStreamSplit.Decode(input.GetSpan(), width, count, output.GetSpan());
        for (int i = 0; i < count; i += Math.Max(1, count / 7))
        {
            for (int j = 0; j < width; j++)
            {
                if (output.GetSpan()[i * width + j] != streams[j * stride + i]) return new Outcome(true, "BYTE_STREAM_SPLIT joined wrong", $"width {width}, value {i}");
            }
        }
        return Outcome.Read;
    }

    /// <summary>A compressed block of data with repeats (copies), mutated a little, into a guarded destination.</summary>
    private static Outcome Codec(Random random, bool snappy)
    {
        byte[] plain = Plain(random);
        byte[] compressed = new byte[snappy ? PageCodecs.MaxCompressedLength(CompressionCodec.Snappy, plain.Length) : Lz4Block.MaxCompressedLength(plain.Length)];
        int length = snappy ? PageCodecs.Compress(CompressionCodec.Snappy, plain, compressed) : Lz4Block.Compress(plain, compressed);
        compressed = compressed[..length];
        int mutations = random.Next(4);
        for (int m = 0; m < mutations && compressed.Length > 0; m++)
        {
            int at = random.Next(compressed.Length);
            compressed[at] = random.Next(2) == 0 ? (byte)(compressed[at] ^ (1 << random.Next(8))) : (byte)random.Next(256);
        }
        if (random.Next(8) == 0) compressed = compressed[..random.Next(compressed.Length + 1)];
        int declared = random.Next(4) == 0 ? Math.Max(0, plain.Length + random.Next(-16, 17)) : plain.Length;
        using var source = GuardedMemory.Block.Copy(compressed);
        using var destination = new GuardedMemory.Block(declared);
        try
        {
            int written = snappy ? SnappyBlock.Decompress(source.GetSpan(), destination.GetSpan()) : Lz4Block.Decompress(source.GetSpan(), destination.GetSpan());
            if (mutations == 0 && declared == plain.Length && compressed.Length == length && (written != plain.Length || !destination.GetSpan().SequenceEqual(plain)))
                return new Outcome(true, $"{(snappy ? "Snappy" : "Lz4")} round trip differs", $"{plain.Length} bytes");
            return Outcome.Read;
        }
        catch (Exception e) when (e is InvalidDataException or ParquetFormatException)
        {
            return Outcome.Refused(e);
        }
    }

    private static byte[] Plain(Random random)
    {
        var plain = new List<byte>();
        int target = random.Next(4) == 0 ? random.Next(70_000) : random.Next(600);
        while (plain.Count < target)
        {
            if (plain.Count > 4 && random.Next(3) != 0)
            {
                int back = random.Next(1, Math.Min(plain.Count, random.Next(2) == 0 ? 16 : 70_000) + 1), copy = random.Next(1, 80);
                for (int k = 0; k < copy; k++) plain.Add(plain[plain.Count - back]);
            }
            else for (int k = random.Next(1, 40); k > 0; k--) plain.Add((byte)random.Next(random.Next(2) == 0 ? 4 : 256));
        }
        return [.. plain];
    }

    /// <summary>Random RLE / bit-packed data read every way (values, bits, all bits at once, skips), all of it guarded.</summary>
    private static Outcome Rle(Random random)
    {
        int width = random.Next(4) == 0 ? 1 : random.Next(33);
        byte[] data = new byte[random.Next(random.Next(2) == 0 ? 16 : 600)];
        random.NextBytes(data);
        // Mostly plausible headers: short runs, small bit-packed group counts.
        for (int at = 0; at < data.Length && random.Next(3) != 0; at += random.Next(1, 12)) data[at] = (byte)(random.Next(2) == 0 ? random.Next(64) << 1 : (random.Next(8) << 1) | 1);
        int count = random.Next(random.Next(2) == 0 ? 64 : 3000);
        using var input = GuardedMemory.Block.Copy(data);
        using var values = new GuardedMemory.Block(count * 4);
        try
        {
            var decoder = new RleBitPackedDecoder(input.GetSpan(), width);
            int read = decoder.Read(MemoryMarshal.Cast<byte, int>(values.GetSpan()));
            var skipping = new RleBitPackedDecoder(input.GetSpan(), width);
            if (skipping.Skip(count) != read) return new Outcome(true, "Rle Skip counts differently from Read", $"width {width}");
            if (width != 1) return Outcome.Read;

            using var bits = new GuardedMemory.Block((count + 7) / 8);
            var bitDecoder = new RleBitPackedDecoder(input.GetSpan(), 1);
            int readBits = bitDecoder.ReadBits(bits.GetSpan(), 0, count);
            using var all = new GuardedMemory.Block((count + 7) / 8);
            int readAll = RleBitPackedDecoder.ReadAllBits(input.GetSpan(), all.GetSpan(), count, out long ones);
            if (readBits != read || readAll != read) return new Outcome(true, "Rle bit readers count differently", $"{read} {readBits} {readAll}");
            ReadOnlySpan<int> v = MemoryMarshal.Cast<byte, int>(values.GetSpan())[..read];
            for (int i = 0; i < read; i++)
            {
                bool bit = (all.GetSpan()[i >> 3] >> (i & 7) & 1) != 0;
                if (bit != (v[i] != 0) || bit != ((bits.GetSpan()[i >> 3] >> (i & 7) & 1) != 0))
                    return new Outcome(true, "Rle bit readers differ from Read", $"value {i}");
            }
            if (ones != v.Count(1)) return new Outcome(true, "ReadAllBits miscounts ones", $"{ones} vs {v.Count(1)}");
            return Outcome.Read;
        }
        catch (ParquetFormatException e)
        {
            return Outcome.Refused(e);
        }
    }
}
