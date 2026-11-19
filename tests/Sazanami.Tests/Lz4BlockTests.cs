using Kaminari.Sazanami.Arrow.Ipc;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Compression;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="Lz4Block"/> and <see cref="Lz4Frame"/> against the straightforward codec they replaced
/// (<see cref="Lz4Reference"/>): blocks of every shape decode alike (copies in 16-byte steps where the window leaves
/// room, exact ones near its end), damaged blocks fail or decode alike, nothing outside the window is touched.
/// </summary>
public class Lz4BlockTests
{
    /// <summary>Inputs of the shapes Arrow buffers have: random bytes, runs, short periods, small integers, text.</summary>
    internal static IEnumerable<byte[]> Inputs(int seed, int count)
    {
        var random = new Random(seed);
        for (int n = 0; n < count; n++)
        {
            int length = random.Next(8) switch
            {
                0 => random.Next(0, 20),
                1 => random.Next(20, 100),
                2 => 65_536,
                3 => random.Next(65_000, 200_000),
                _ => random.Next(100, 20_000),
            };
            byte[] data = new byte[length];
            switch (random.Next(6))
            {
                case 0:
                    random.NextBytes(data);
                    break;
                case 1:
                    break; // zeros
                case 2:
                {
                    int period = random.Next(1, 40);
                    byte[] unit = new byte[period];
                    random.NextBytes(unit);
                    for (int i = 0; i < length; i++) data[i] = unit[i % period];
                    break;
                }
                case 3:
                    for (int i = 0; i + 4 <= length; i += 4) BitConverter.TryWriteBytes(data.AsSpan(i), random.Next(0, 1000));
                    break;
                case 4:
                {
                    string[] words = ["alpha", "beta", "gamma", "delta", "lz4", "arrow", " ", "category_07", "x"];
                    for (int i = 0; i < length;)
                    {
                        byte[] w = System.Text.Encoding.UTF8.GetBytes(words[random.Next(words.Length)]);
                        int k = Math.Min(w.Length, length - i);
                        w.AsSpan(0, k).CopyTo(data.AsSpan(i));
                        i += k;
                    }
                    break;
                }
                default:
                    // Mixed: random stretches between repeats of earlier bytes.
                    random.NextBytes(data);
                    for (int i = 0; i < length; i += random.Next(1, 300))
                    {
                        int back = random.Next(1, Math.Max(2, Math.Min(i, 70_000)));
                        int run = random.Next(4, 200);
                        for (int k = 0; k < run && i + k < length && i - back >= 0; k++) data[i + k] = data[i + k - back];
                    }
                    break;
            }
            yield return data;
        }
    }

    private static byte[] Decode(ReadOnlySpan<byte> block, int length, int room)
    {
        // Guard bytes around the window: nothing outside it may change.
        byte[] buffer = new byte[length + room + 64];
        buffer.AsSpan().Fill(0xA5);
        int written = Lz4Block.Decompress(block, buffer.AsSpan(32, length + room));
        Assert.Equal(length, written);
        Assert.All(buffer[..32], b => Assert.Equal(0xA5, b));
        Assert.All(buffer[(32 + length + room)..], b => Assert.Equal(0xA5, b));
        return buffer[32..(32 + length)];
    }

    [Fact]
    public void Blocks_DecodeAsTheReferenceDecodes()
    {
        foreach (byte[] data in Inputs(1, 400))
        {
            byte[] block = new byte[Lz4Reference.MaxCompressedLength(data.Length)];
            int n = Lz4Reference.Compress(data, block);
            foreach (int room in (int[])[0, 1, 7, 15, 16, 100])
                Assert.Equal(data, Decode(block.AsSpan(0, n), data.Length, room));
        }
    }

    /// <summary>
    /// Hand-built sequences: every match offset from 1 to 70 and lengths from 4 to 300, at the start, the middle and
    /// the end of the window (where 16-byte copies have no room).
    /// </summary>
    [Fact]
    public void EveryOffsetAndLength_DecodesAsTheReferenceDecodes()
    {
        var random = new Random(2);
        for (int offset = 1; offset <= 70; offset++)
        {
            foreach (int length in (int[])[4, 5, 7, 8, 9, 15, 16, 17, 18, 19, 31, 32, 33, 64, 300])
            {
                foreach (int tail in (int[])[5, 12, 20, 40])
                {
                    byte[] prefix = new byte[offset + random.Next(0, 30)];
                    random.NextBytes(prefix);
                    byte[] suffix = new byte[tail];
                    random.NextBytes(suffix);
                    byte[] block = Sequence(prefix, offset, length, suffix);
                    int total = prefix.Length + length + suffix.Length;
                    byte[] expected = new byte[total];
                    Assert.Equal(total, Lz4Reference.Decompress(block, expected));
                    foreach (int room in (int[])[0, 3, 16, 40]) Assert.Equal(expected, Decode(block, total, room));
                }
            }
        }

        // Literals then a match of `length` bytes `offset` back, then literals only.
        static byte[] Sequence(byte[] literals, int offset, int length, byte[] last)
        {
            List<byte> b = [];
            int lit = literals.Length, extra = length - 4;
            b.Add((byte)((Math.Min(lit, 15) << 4) | Math.Min(extra, 15)));
            if (lit >= 15) Length(b, lit - 15);
            b.AddRange(literals);
            b.Add((byte)offset);
            b.Add((byte)(offset >> 8));
            if (extra >= 15) Length(b, extra - 15);
            b.Add((byte)(Math.Min(last.Length, 15) << 4));
            if (last.Length >= 15) Length(b, last.Length - 15);
            b.AddRange(last);
            return [.. b];
        }

        static void Length(List<byte> b, int n)
        {
            for (; n >= 255; n -= 255) b.Add(255);
            b.Add((byte)n);
        }
    }

    /// <summary>Damaged blocks fail with InvalidDataException or decode to what the reference decodes; never touch outside the window.</summary>
    [Fact]
    public void DamagedBlocks_FailOrDecodeAsTheReference()
    {
        var random = new Random(3);
        int failed = 0;
        foreach (byte[] data in Inputs(4, 300))
        {
            if (data.Length == 0) continue;
            byte[] block = new byte[Lz4Reference.MaxCompressedLength(data.Length)];
            int n = Lz4Reference.Compress(data, block);
            for (int m = 0; m < 4; m++)
            {
                byte[] damaged = block[..n];
                for (int k = random.Next(1, 4); k > 0; k--) damaged[random.Next(damaged.Length)] = (byte)random.Next(256);
                if (random.Next(4) == 0) damaged = damaged[..random.Next(damaged.Length)];
                int length = data.Length + random.Next(-10, 10);
                if (length < 0) continue;
                byte[] reference = new byte[length];
                int expected;
                try
                {
                    expected = Lz4Reference.Decompress(damaged, reference);
                }
                catch (InvalidDataException)
                {
                    expected = -1;
                }
                byte[] buffer = new byte[length + 64];
                buffer.AsSpan().Fill(0x5A);
                int got;
                try
                {
                    got = Lz4Block.Decompress(damaged, buffer.AsSpan(32, length));
                }
                catch (InvalidDataException)
                {
                    got = -1;
                }
                Assert.All(buffer[..32], b => Assert.Equal(0x5A, b));
                Assert.All(buffer[(32 + length)..], b => Assert.Equal(0x5A, b));
                Assert.Equal(expected, got);
                if (got < 0) failed++;
                else Assert.Equal(reference[..got], buffer[32..(32 + got)]);
            }
        }
        Assert.True(failed > 100, $"only {failed} damaged blocks failed");
    }

    /// <summary>A block decoded after earlier output (linked blocks): matches reach back into it.</summary>
    [Fact]
    public void LinkedBlocks_ReachBackIntoTheWindow()
    {
        foreach (byte[] data in Inputs(5, 200))
        {
            if (data.Length < 40) continue;
            // The second half compressed with the first as history: the reference decodes it with that history.
            int half = data.Length / 2;
            byte[] block = new byte[Lz4Reference.MaxCompressedLength(data.Length - half)];
            int n = Lz4Reference.Compress(data.AsSpan(half), block);
            byte[] window = new byte[data.Length + 16];
            data.AsSpan(0, half).CopyTo(window);
            Assert.Equal(data.Length - half, Lz4Block.Decompress(block.AsSpan(0, n), window, half));
            Assert.Equal(data, window[..data.Length]);
        }
        // A match reaching before the window's start is damage.
        byte[] reaching = [0x10, 0xAB, 0x05, 0x00, 0x50, 1, 2, 3, 4, 5];
        Assert.Throws<InvalidDataException>(() => Lz4Block.Decompress(reaching, new byte[40], 0));
        Assert.Equal(10, Lz4Block.Decompress(reaching, new byte[40], 4)); // 4 bytes of history make it valid
    }

    /// <summary>The encoder's blocks decode with the reference decoder and this one, within the bound, and compress about as well.</summary>
    [Fact]
    public void EncodedBlocks_DecodeWithBothDecoders()
    {
        long ours = 0, reference = 0;
        foreach (byte[] data in Inputs(7, 500))
        {
            byte[] block = new byte[Lz4Block.MaxCompressedLength(data.Length)];
            int n = Lz4Block.Compress(data, block);
            Assert.InRange(n, 1, block.Length);
            byte[] back = new byte[data.Length];
            Assert.Equal(data.Length, Lz4Reference.Decompress(block.AsSpan(0, n), back));
            Assert.Equal(data, back);
            Assert.Equal(data, Decode(block.AsSpan(0, n), data.Length, 16));
            ours += n;
            reference += Lz4Reference.Compress(data, new byte[Lz4Reference.MaxCompressedLength(data.Length)]);
        }
        Assert.True(ours <= reference * 1.03, $"{ours} bytes against the reference's {reference}");
    }

    /// <summary>The output depends on the input alone, not on what the thread compressed before (the table is reused uncleared).</summary>
    [Fact]
    public void EncodedBlocks_DependOnTheInputAlone()
    {
        byte[][] inputs = [.. Inputs(8, 60)];
        byte[][] first = [.. inputs.Select(Compress)];
        // Again in reverse order, and after other inputs: the same bytes.
        foreach (byte[] other in Inputs(9, 20)) Compress(other);
        for (int i = inputs.Length - 1; i >= 0; i--) Assert.Equal(first[i], Compress(inputs[i]));

        static byte[] Compress(byte[] data)
        {
            byte[] block = new byte[Lz4Block.MaxCompressedLength(data.Length)];
            return block[..Lz4Block.Compress(data, block)];
        }
    }

    /// <summary>
    /// IPC buffers compressed in units of 16 blocks on several threads: each frame is the one compressing the whole
    /// buffer at once gives, whatever the threads; incompressible buffers stored as they are, empty ones empty.
    /// </summary>
    [Fact]
    public unsafe void SplitBuffers_GiveTheFramesOfWholeOnes()
    {
        var random = new Random(10);
        byte[][] data =
        [
            [.. Inputs(11, 1).First().Concat(new byte[5 << 20])], // over several units, mostly zeros
            [.. Enumerable.Range(0, (3 << 20) + 12_345).Select(i => (byte)(i % 251 ^ (i >> 11)))],
            new byte[(2 << 20) + 1], // incompressible: stored
            [],
            [.. Enumerable.Range(0, 300).Select(i => (byte)i)],
            new byte[4 * 65_536],
            new byte[4 * 65_536 + 1],
        ];
        random.NextBytes(data[2]);
        random.NextBytes(data[5]);
        foreach (int parallelism in (int[])[1, 3, 16])
        {
            using var body = new IpcEncodedBody();
            foreach (byte[] d in data)
            {
                byte* p = body.Make(Math.Max(1, d.Length));
                d.CopyTo(new Span<byte>(p, d.Length));
                body.Buffers.Add(new IpcPiece((nint)p, d.Length));
            }
            IpcBatchEncoder.Compress(body, IpcCompression.Lz4Frame, 0, parallelism);
            for (int i = 0; i < data.Length; i++)
            {
                IpcPiece piece = body.Buffers[i];
                if (data[i].Length == 0)
                {
                    Assert.Equal(0, piece.Length);
                    continue;
                }
                byte[] whole = new byte[Lz4Frame.MaxCompressedLength(data[i].Length)];
                int n = Lz4Frame.Compress(data[i], whole);
                if (n < data[i].Length)
                {
                    Assert.Equal(data[i].Length, piece.Prefix);
                    Assert.Equal(whole[..n], piece.Span.ToArray());
                }
                else
                {
                    Assert.Equal(-1, piece.Prefix);
                    Assert.Equal(data[i], piece.Span.ToArray());
                }
            }
        }
    }

    /// <summary>Zstandard buffers over the frame size are several frames: they decode whole, the same bytes for any thread count.</summary>
    [Fact]
    public unsafe void SplitZstdBuffers_DecodeWhole_AndDoNotDependOnTheThreads()
    {
        byte[][] data = [[.. Enumerable.Range(0, (1 << 20) + 777).Select(i => (byte)(i % 97 ^ (i >> 9)))], new byte[70_000], [1, 2, 3]];
        byte[][]? first = null;
        foreach (int parallelism in (int[])[1, 4, 16])
        {
            using var body = new IpcEncodedBody();
            foreach (byte[] d in data)
            {
                byte* p = body.Make(d.Length);
                d.CopyTo(new Span<byte>(p, d.Length));
                body.Buffers.Add(new IpcPiece((nint)p, d.Length));
            }
            IpcBatchEncoder.Compress(body, IpcCompression.Zstd, 3, parallelism, zstdFrameBytes: 64 << 10);
            byte[][] outputs = [.. body.Buffers.Select(b => b.Span.ToArray())];
            first ??= outputs;
            for (int i = 0; i < data.Length; i++)
            {
                Assert.Equal(first[i], outputs[i]);
                if (body.Buffers[i].Prefix == -1) continue;
                byte[] back = new byte[data[i].Length];
                Assert.Equal(data[i].Length, ZstdBlock.Decompress(outputs[i], back));
                Assert.Equal(data[i], back);
            }
        }
        Assert.True(first![0].Length < data[0].Length);
    }

    /// <summary>
    /// Blocks decoded from input and into output that both end at a guard page, and encoded likewise: no read or write
    /// past either end (an over-read or over-write of the wide copies would fault and end the test process).
    /// </summary>
    [Fact]
    public void Decoding_StaysInsideItsSpans()
    {
        foreach (byte[] data in Inputs(12, 300))
        {
            byte[] block = new byte[Lz4Block.MaxCompressedLength(data.Length)];
            int n = Lz4Block.Compress(data, block);
            using var source = GuardedMemory.Block.Copy(block.AsSpan(0, n));
            using var target = new GuardedMemory.Block(Math.Max(1, data.Length));
            Span<byte> output = target.GetSpan()[..data.Length];
            Assert.Equal(data.Length, Lz4Block.Decompress(source.GetSpan(), output));
            Assert.Equal(data, output.ToArray());
            // The encoder reads inside its input and writes inside a destination of exactly the bound (its literal copies
            // run 8 bytes at a time without a check: at least 8 more output bytes always follow them).
            using var copy = GuardedMemory.Block.Copy(data);
            using var bound = new GuardedMemory.Block(Lz4Block.MaxCompressedLength(data.Length));
            Assert.Equal(n, Lz4Block.Compress(copy.GetSpan(), bound.GetSpan()));
            Assert.Equal(block[..n], bound.GetSpan()[..n].ToArray());
        }
    }

    /// <summary>
    /// Inputs either side of 64 KiB (16-bit positions up to it, 32-bit beyond) round-trip; beyond it a repeat more than
    /// 65535 bytes back is not taken as a match (its offset would not fit).
    /// </summary>
    [Fact]
    public void BothTablePaths_RoundTrip_AndFarRepeatsAreNotMatched()
    {
        var random = new Random(14);
        foreach (int length in (int[])[13, 65_535, 65_536, 65_537, 131_072, 300_000])
        {
            byte[] data = new byte[length];
            random.NextBytes(data);
            for (int i = 0; i < length; i += 4096) data.AsSpan(i, Math.Min(64, length - i)).Fill((byte)i); // some matches
            AssertRoundTrip(data);
        }
        // Random bytes, then their first 4 KiB again 70,000 bytes later: too far to reference.
        byte[] far = new byte[74_096];
        random.NextBytes(far.AsSpan(0, 70_000));
        far.AsSpan(0, 4096).CopyTo(far.AsSpan(70_000));
        AssertRoundTrip(far);
        byte[] block = new byte[Lz4Block.MaxCompressedLength(far.Length)];
        Assert.True(Lz4Block.Compress(far, block) > far.Length - 100, "a repeat out of reach was matched");

        static void AssertRoundTrip(byte[] data)
        {
            byte[] block = new byte[Lz4Block.MaxCompressedLength(data.Length)];
            int n = Lz4Block.Compress(data, block);
            byte[] back = new byte[data.Length];
            Assert.Equal(data.Length, Lz4Reference.Decompress(block.AsSpan(0, n), back));
            Assert.Equal(data, back);
        }
    }

    [Fact]
    public void Frames_RoundTrip()
    {
        foreach (byte[] data in Inputs(6, 150))
        {
            byte[] frame = new byte[Lz4Frame.MaxCompressedLength(data.Length)];
            int n = Lz4Frame.Compress(data, frame);
            byte[] back = new byte[data.Length];
            Assert.Equal(data.Length, Lz4Frame.Decompress(frame.AsSpan(0, n), back));
            Assert.Equal(data, back);
        }
    }
}
