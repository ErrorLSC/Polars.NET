using System.IO.Hashing;
using Kaminari.Sazanami.Compression;

namespace Sazanami.Tests;

/// <summary><see cref="XxHash32Stream"/> equals System.IO.Hashing's XXH32 for any data cut into any pieces.</summary>
public sealed class XxHash32StreamTests
{
    [Fact]
    public void AnyPieces_HashAsTheWhole()
    {
        var random = new Random(20261119);
        foreach (int length in (int[])[0, 1, 3, 4, 15, 16, 17, 31, 32, 33, 64, 100, 65_536, 65_536 * 3 + 7, .. Enumerable.Range(0, 200).Select(_ => random.Next(0, 5000))])
        {
            byte[] data = new byte[length];
            random.NextBytes(data);
            uint expected = XxHash32.HashToUInt32(data);
            foreach (int pieces in (int[])[1, 2, 5, 40])
            {
                var stream = new XxHash32Stream();
                int at = 0;
                for (int p = 0; p < pieces - 1 && at < length; p++)
                {
                    int take = random.Next(0, Math.Min(length - at, random.Next(2) == 0 ? 20 : 70_000) + 1);
                    stream.Append(data.AsSpan(at, take));
                    at += take;
                }
                stream.Append(data.AsSpan(at));
                Assert.Equal(expected, stream.Hash());
            }
        }
    }

    [Fact]
    public void TheHash_CanBeTakenMidway()
    {
        byte[] data = [.. Enumerable.Range(0, 100).Select(i => (byte)(i * 37))];
        var stream = new XxHash32Stream();
        stream.Append(data.AsSpan(0, 21));
        Assert.Equal(XxHash32.HashToUInt32(data.AsSpan(0, 21)), stream.Hash());
        stream.Append(data.AsSpan(21));
        Assert.Equal(XxHash32.HashToUInt32(data), stream.Hash());
    }
}
