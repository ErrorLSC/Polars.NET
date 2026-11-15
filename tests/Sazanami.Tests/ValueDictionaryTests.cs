using System.Text;
using Kaminari.Sazanami.Encodings;

namespace Sazanami.Tests;

/// <summary>
/// The dictionaries of dictionary encoding against a model (first appearance order, an index per distinct value, by
/// bytes): enough distinct strings that 32-bit hashes collide; NaN payloads and signed zeros distinct; reuse after
/// Clear. And <see cref="BitPacker"/> against <see cref="BitUnpacking"/> at every width.
/// </summary>
public class ValueDictionaryTests
{
    [Fact]
    public void ByteStrings_GetOneIndexEach_InFirstAppearanceOrder()
    {
        var random = new Random(7);
        var dictionary = new ByteArrayDictionary();
        for (int round = 0; round < 2; round++)
        {
            Dictionary<string, int> model = new(StringComparer.Ordinal);
            for (int i = 0; i < 300_000; i++)
            {
                // Mostly new values (hash collisions are likely among 300 000), some repeats, the empty string.
                string value = random.Next(10) == 0 && model.Count > 0 ? $"v{random.Next(model.Count)}" : i == 5 ? "" : $"v{model.Count}";
                int expected = model.TryGetValue(value, out int known) ? known : model[value] = model.Count;
                Assert.Equal(expected, dictionary.GetOrAdd(Encoding.UTF8.GetBytes(value)));
            }
            Assert.Equal(model.Count, dictionary.Count);
            Assert.Equal(model.Keys.Sum(k => (long)Encoding.UTF8.GetByteCount(k)), dictionary.ByteSize);
            foreach (var (value, index) in model) Assert.Equal(value, Encoding.UTF8.GetString(dictionary[index]));
            dictionary.Clear();
            Assert.Equal(0, dictionary.Count);
        }
    }

    [Fact]
    public void FixedValues_AreEqualByTheirBytes()
    {
        var doubles = new FixedValueDictionary<double>();
        double otherNaN = BitConverter.Int64BitsToDouble(0x7FF8_0000_0000_0001);
        double[] values = [1.5, double.NaN, otherNaN, 0d, -0d, 1.5, double.NaN, -0d];
        Assert.Equal([0, 1, 2, 3, 4, 0, 1, 4], values.Select(doubles.GetOrAdd));
        Assert.Equal(5, doubles.Count);

        var random = new Random(3);
        var longs = new FixedValueDictionary<long>();
        var twelve = new FixedValueDictionary<(long, int)>();
        Dictionary<long, int> model = [];
        for (int i = 0; i < 200_000; i++)
        {
            long value = random.Next(4) == 0 ? random.Next(100) : random.NextInt64();
            int expected = model.TryGetValue(value, out int known) ? known : model[value] = model.Count;
            Assert.Equal(expected, longs.GetOrAdd(value));
            Assert.Equal(expected, twelve.GetOrAdd((value, (int)(value >> 7))));
        }
        Assert.Equal(model.Keys.OrderBy(k => model[k]), longs.Values.ToArray());
    }

    [Fact]
    public void PackedValues_UnpackToTheSame_AtEveryWidth()
    {
        var random = new Random(32);
        for (int width = 0; width <= 32; width++)
        {
            uint max = width == 32 ? uint.MaxValue : (1u << width) - 1;
            uint[] values = [.. Enumerable.Range(0, random.Next(1, 300)).Select(_ => (uint)random.NextInt64(0, (long)max + 1))];
            byte[] packed = new byte[BitPacker.Bytes(values.Length, width) + 8];
            BitPacker.Pack(values, width, packed);
            uint[] unpacked = new uint[values.Length];
            BitUnpacking.Unpack<uint>(packed, 0, width, unpacked);
            Assert.Equal(values, unpacked);
        }
    }
}
