using Kaminari.Sazanami.Kernels;

namespace Sazanami.Tests;

/// <summary><see cref="MaskCompact"/> against a model: slot by slot, the kept valid slots' values in order.</summary>
public class MaskCompactTests
{
    /// <summary>
    /// A mask of n slots, word by word: all set, all clear, or runs of ones and zeros of random lengths (so words start
    /// inside runs); bits past the last slot clear.
    /// </summary>
    private static ulong[] RandomMask(Random random, int n)
    {
        var mask = new ulong[(n + 63) / 64 + random.Next(2)];
        bool on = random.Next(2) == 0;
        for (int w = 0; w << 6 < n; w++)
        {
            int kind = random.Next(4), density = random.Next(1, 5);
            for (int i = w << 6; i < Math.Min(n, (w + 1) << 6); i++)
            {
                if (kind == 2 && random.Next(density * 3) == 0) on = !on;
                if (kind switch { 0 => true, 1 => false, 2 => on, _ => random.Next(2) == 0 }) mask[i >> 6] |= 1UL << (i & 63);
            }
        }
        return mask;
    }

    private static bool Bit(ulong[] mask, int i) => (mask[i >> 6] >> (i & 63) & 1) == 1;

    [Fact]
    public void Values_AreTheKeptValidSlotsValues_InOrder()
    {
        var random = new Random(17);
        for (int round = 0; round < 3000; round++)
        {
            int n = random.Next(4) == 0 ? random.Next(1, 4) * 64 : random.Next(300);
            ulong[] keep = RandomMask(random, n);
            ulong[]? valid = random.Next(3) == 0 ? null : RandomMask(random, n);
            int present = Enumerable.Range(0, n).Count(i => valid is null || Bit(valid, i));
            long[] values = [.. Enumerable.Range(0, present).Select(_ => random.NextInt64())];
            List<long> expected = [];
            for (int i = 0, rank = 0; i < n; i++)
            {
                if (valid is not null && !Bit(valid, i)) continue;
                if (Bit(keep, i)) expected.Add(values[rank]);
                rank++;
            }

            var target = new long[expected.Count + 2];
            int count = MaskCompact.Values<long>(values, keep, valid, n, target);
            Assert.Equal(expected, target[..count]);

            // Any width, by bytes: the typed paths (1, 2, 4, 8, 16) and the byte-wise one (3, 12).
            foreach (int width in (int[])[1, 2, 3, 4, 8, 12, 16])
            {
                byte[] bytes = new byte[present * width];
                random.NextBytes(bytes);
                var into = new byte[(expected.Count + 1) * width];
                Assert.Equal(expected.Count, MaskCompact.Values(bytes, width, keep, valid, n, into));
                List<byte> want = [];
                for (int i = 0, rank = 0; i < n; i++)
                {
                    if (valid is not null && !Bit(valid, i)) continue;
                    if (Bit(keep, i)) want.AddRange(bytes.AsSpan(rank * width, width).ToArray());
                    rank++;
                }
                Assert.Equal(want, into[..(expected.Count * width)]);
            }
        }
    }

    [Fact]
    public void Bits_AreTheKeptSlotsBits_Packed()
    {
        var random = new Random(23);
        for (int round = 0; round < 3000; round++)
        {
            int n = random.Next(4) == 0 ? random.Next(1, 5) * 64 : random.Next(400);
            ulong[] keep = RandomMask(random, n), bits = RandomMask(random, n);
            List<bool> expected = [.. Enumerable.Range(0, n).Where(i => Bit(keep, i)).Select(i => Bit(bits, i))];
            var target = new ulong[(expected.Count + 63) / 64 + 1];
            Array.Fill(target, ulong.MaxValue); // bits past the kept ones must come out clear
            int set = MaskCompact.Bits(bits, keep, n, target);
            Assert.Equal(expected.Count(b => b), set);
            Assert.Equal(expected, Enumerable.Range(0, expected.Count).Select(j => Bit(target, j)));
            for (int j = expected.Count; j < (expected.Count + 63) / 64 * 64; j++) Assert.False(Bit(target, j));
        }
    }

    [Fact]
    public void ShortInputs_AreRefused()
    {
        ulong[] keep = [ulong.MaxValue, 1];
        Assert.Throws<ArgumentException>(() => MaskCompact.Values<int>(new int[64], keep, default, 65, new int[65]));   // a value short
        Assert.Throws<ArgumentException>(() => MaskCompact.Values<int>(new int[65], keep, default, 65, new int[64]));   // the target short
        Assert.Throws<ArgumentException>(() => MaskCompact.Values<int>(new int[65], [ulong.MaxValue], default, 65, new int[65])); // the mask short
        Assert.Throws<ArgumentException>(() => MaskCompact.Values<int>(new int[65], keep, [ulong.MaxValue], 65, new int[65]));    // the validity short
        Assert.Throws<ArgumentException>(() => MaskCompact.Values(new byte[64 * 3], 3, keep, default, 65, new byte[65 * 3]));
        Assert.Throws<ArgumentException>(() => MaskCompact.Bits([ulong.MaxValue, 1], keep, 65, [0UL]));
        Assert.Equal(0, MaskCompact.Values<int>([], [], default, 0, []));
    }
}
