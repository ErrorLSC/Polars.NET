using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Compression;
using ZstdSharp;

namespace Sazanami.Tests;

/// <summary>
/// Zstandard into a growing buffer: frames that declare their size and frames that do not (streaming writers, as Java
/// Avro's), one after another, come back whole; damaged, cut or oversized data is refused.
/// </summary>
public class ZstdBlockTests
{
    private static byte[] Data(int length, int seed)
    {
        var random = new Random(seed);
        return [.. Enumerable.Range(0, length).Select(i => (byte)(i % 13 == 0 ? random.Next() : i / 97))];
    }

    /// <summary>A frame that declares no content size (a streaming compressor's).</summary>
    private static byte[] Streamed(byte[] data)
    {
        using var memory = new MemoryStream();
        using (var stream = new CompressionStream(memory, 3)) stream.Write(data);
        return memory.ToArray();
    }

    private static byte[] OneShot(byte[] data)
    {
        byte[] frame = new byte[ZstdBlock.MaxCompressedLength(data.Length)];
        return frame[..ZstdBlock.Compress(data, frame)];
    }

    [Theory]
    [InlineData(0)]
    [InlineData(100)]
    [InlineData(1_000_000)]
    public void Frames_WithAndWithoutTheirSize_ComeBackWhole(int length)
    {
        byte[] a = Data(length, 1), b = Data(length / 2 + 1, 2);
        Assert.Equal(-1, ZstdBlock.DeclaredLength(Streamed(a)));
        foreach (byte[][] frames in (byte[][][])[[Streamed(a)], [OneShot(a)], [Streamed(a), Streamed(b)], [Streamed(a), OneShot(b)]])
        {
            byte[] expected = [.. frames.Length == 1 ? a : [.. a, .. b]];
            using var output = new NativeByteBuffer(16);
            int n = ZstdBlock.Decompress([.. frames.SelectMany(f => f)], output);
            Assert.Equal(expected, output.Span[..n].ToArray());
            n = ZstdBlock.Decompress(OneShot(b), output); // the thread's context is ready for one-shot frames again
            Assert.Equal(b, output.Span[..n].ToArray());
        }
    }

    [Fact]
    public void DamagedCutOrOversizedData_IsRefused()
    {
        byte[] data = Data(200_000, 3);
        byte[] streamed = Streamed(data), oneShot = OneShot(data);
        using var output = new NativeByteBuffer();
        Assert.Throws<InvalidDataException>(() => ZstdBlock.Decompress(streamed[..(streamed.Length / 2)], output));
        byte[] damaged = [.. streamed];
        for (int i = 20; i < damaged.Length - 8; i += 7) damaged[i] ^= 0x5A;
        Assert.Throws<InvalidDataException>(() => ZstdBlock.Decompress(damaged, output));
        Assert.Throws<InvalidDataException>(() => ZstdBlock.Decompress(streamed, output, maxLength: 100_000));
        Assert.Throws<InvalidDataException>(() => ZstdBlock.Decompress(oneShot, output, maxLength: 100_000));
        int n = ZstdBlock.Decompress(streamed, output, maxLength: data.Length);
        Assert.Equal(data, output.Span[..n].ToArray());
    }
}
