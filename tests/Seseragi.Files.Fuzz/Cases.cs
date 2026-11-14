using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Kaminari.Sazanami.Compression;
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

    // ------------------------------------------------------------------
    // Kernels
    // ------------------------------------------------------------------

    private static Outcome Kernel(Random random) => random.Next(3) switch
    {
        0 => Codec(random, snappy: true),
        1 => Codec(random, snappy: false),
        _ => Rle(random),
    };

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
