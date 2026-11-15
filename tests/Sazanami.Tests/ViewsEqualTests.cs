using System.Buffers.Binary;
using Kaminari.Sazanami.Filters;

namespace Sazanami.Tests;

/// <summary><see cref="BinaryKernels.ViewsEqual"/> against the view-by-view test it replaces for equality.</summary>
public class ViewsEqualTests
{
    [Fact]
    public void ViewsEqual_IsTheViewByViewEquality()
    {
        var random = new Random(31);
        for (int round = 0; round < 2000; round++)
        {
            // Values sharing the literal's length and first bytes often; inline padding left as garbage.
            byte[] literal = Bytes(random, random.Next(21));
            int length = random.Next(1, 200);
            var views = new byte[length * 16];
            random.NextBytes(views);
            List<byte> data = [];
            for (int i = 0; i < length; i++)
            {
                byte[] value = random.Next(4) switch
                {
                    0 => literal,
                    1 => [.. literal.Take(Math.Min(4, literal.Length)), .. Bytes(random, Math.Max(0, literal.Length - 4))],
                    2 => [.. literal.Take(Math.Max(0, literal.Length - 1)), (byte)random.Next(256)],
                    _ => Bytes(random, random.Next(21)),
                };
                Span<byte> view = views.AsSpan(i * 16, 16);
                BinaryPrimitives.WriteInt32LittleEndian(view, value.Length);
                if (value.Length <= 12)
                {
                    value.CopyTo(view[4..]); // the bytes after the value stay random
                    continue;
                }
                value.AsSpan(0, 4).CopyTo(view[4..]);
                BinaryPrimitives.WriteInt32LittleEndian(view[8..], 0);
                BinaryPrimitives.WriteInt32LittleEndian(view[12..], data.Count);
                data.AddRange(value);
            }
            ReadOnlyMemory<byte>[] buffers = [data.ToArray()];
            int words = (length + 63) / 64;
            ulong[] valid = [.. Enumerable.Range(0, words).Select(_ => (ulong)random.NextInt64() | (ulong)random.NextInt64() << 1)];
            valid[^1] &= length % 64 == 0 ? ulong.MaxValue : (1UL << (length % 64)) - 1;
            foreach (ulong[]? slots in (ulong[]?[])[null, valid])
            {
                // Null slots may hold anything, buffer numbers included: they must not be read.
                if (slots is not null)
                {
                    for (int i = 0; i < length; i++)
                    {
                        if ((slots[i >> 6] >> (i & 63) & 1) == 0) BinaryPrimitives.WriteInt32LittleEndian(views.AsSpan(i * 16 + 8), 7);
                    }
                }
                var expected = new ulong[words];
                var actual = new ulong[words];
                BinaryKernels.Views(new BytesTests.Equal(literal), views, buffers, length, slots, expected);
                BinaryKernels.ViewsEqual(literal, views, buffers, length, slots, actual);
                Assert.Equal(expected, actual);
            }
        }

        static byte[] Bytes(Random random, int n)
        {
            var bytes = new byte[n];
            random.NextBytes(bytes);
            return bytes;
        }
    }
}
