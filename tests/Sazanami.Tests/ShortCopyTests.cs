using Kaminari.Sazanami.Kernels;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="ShortCopy"/> against a plain copy: every length up to past the inline limit, at unaligned offsets in
/// both buffers; the bytes after the copy are left alone, and a destination too short is refused as the span copy
/// refuses it.
/// </summary>
public class ShortCopyTests
{
    [Fact]
    public void EveryLength_CopiesExactlyItsBytes()
    {
        var r = new Random(4);
        byte[] source = new byte[ShortCopy.MaxInline + 40];
        r.NextBytes(source);
        for (int length = 0; length <= ShortCopy.MaxInline + 8; length++)
        {
            foreach (int from in (int[])[0, 1, 7, 13])
            {
                foreach (int to in (int[])[0, 3, 9])
                {
                    byte[] destination = new byte[to + length + 24];
                    Array.Fill(destination, (byte)0xA5);
                    ShortCopy.Copy(source.AsSpan(from, length), destination.AsSpan(to));
                    Assert.True(destination.AsSpan(to, length).SequenceEqual(source.AsSpan(from, length)), $"length {length} from {from} to {to}");
                    Assert.All(destination[..to], b => Assert.Equal(0xA5, b));
                    Assert.All(destination[(to + length)..], b => Assert.Equal(0xA5, b));
                }
            }
        }
    }

    [Fact]
    public void ADestinationTooShort_IsRefused()
    {
        byte[] source = new byte[20];
        Assert.Throws<ArgumentException>(() => ShortCopy.Copy(source, new byte[19]));
    }
}
