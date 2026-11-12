using Kaminari.Seseragi.Files.Avro;

namespace Seseragi.Files.Tests;

/// <summary>The SWAR varint paths against the plain definition, at every length boundary.</summary>
public class AvroVarintTests
{
    /// <summary>Reference zigzag varint encoding, byte by byte.</summary>
    private static byte[] Reference(long value)
    {
        ulong n = (ulong)((value << 1) ^ (value >> 63));
        List<byte> bytes = [];
        while (n >= 0x80)
        {
            bytes.Add((byte)(n | 0x80));
            n >>= 7;
        }
        bytes.Add((byte)n);
        return [.. bytes];
    }

    public static IEnumerable<long> Values()
    {
        yield return 0;
        yield return long.MinValue;
        yield return long.MaxValue;
        for (int bits = 1; bits < 64; bits++)
        {
            long p = 1L << bits;
            foreach (long v in (long[])[p - 1, p, p + 1, -p, -p - 1, -p + 1]) yield return v;
            // boundaries of the zigzag value at every 7-bit group
            if (bits % 7 == 0) foreach (long v in (long[])[(p >> 1) - 1, p >> 1, -(p >> 1), -(p >> 1) - 1]) yield return v;
        }
        var random = new Random(7);
        for (int i = 0; i < 20_000; i++) yield return random.NextInt64(long.MinValue, long.MaxValue) >> random.Next(64);
    }

    [Fact]
    public void Writer_matches_the_reference_encoding()
    {
        using var w = new AvroBufferWriter(16);
        foreach (long v in Values())
        {
            w.Clear();
            w.WriteLong(v);
            Assert.Equal(Reference(v), w.WrittenSpan.ToArray());
        }
    }

    [Fact]
    public void Reader_decodes_with_and_without_eight_bytes_of_lookahead()
    {
        foreach (long v in Values())
        {
            var encoded = Reference(v);
            // exactly the varint (lookahead path only when it is 8+ bytes long) ...
            var tight = new AvroSpanReader(encoded);
            Assert.Equal(v, tight.ReadLong());
            Assert.Equal(encoded.Length, tight.Position);
            // ... and followed by padding (8-byte load path for every length up to 8)
            byte[] padded = [.. encoded, .. new byte[9]];
            var loose = new AvroSpanReader(padded);
            Assert.Equal(v, loose.ReadLong());
            Assert.Equal(encoded.Length, loose.Position);
        }
    }

    [Fact]
    public void Overlong_varint_is_refused()
    {
        byte[] eleven = [.. Enumerable.Repeat((byte)0xFF, 10), 0x01];
        Assert.Throws<AvroFormatException>(() => new AvroSpanReader(eleven).ReadLong());
        byte[] truncated = [0x80, 0x80];
        Assert.Throws<AvroFormatException>(() => new AvroSpanReader(truncated).ReadLong());
    }
}
