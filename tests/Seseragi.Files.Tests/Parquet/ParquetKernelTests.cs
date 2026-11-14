using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Compression;
using Kaminari.Sazanami.Encodings;
using Kaminari.Sazanami.Helpers;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Encodings;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>The fast decoding kernels against their bounds-checked reference paths, over every width, offset and length.</summary>
public class ParquetKernelTests
{
    [Fact]
    public void BitUnpack_EqualsTheCheckedPath_ForEveryWidthOffsetAndLength()
    {
        // Values wider than the destination are refused (above 32 bits: 64-bit integers only).
        Assert.Throws<ArgumentOutOfRangeException>(() => BitUnpacking.Unpack(new byte[64], 0, 33, new int[8].AsSpan()));
        Assert.Throws<ArgumentOutOfRangeException>(() => BitUnpacking.Unpack(new byte[64], 0, 65, new long[8].AsSpan()));
        var random = new Random(7);
        for (int width = 0; width <= 64; width++)
        {
            foreach (int bytes in (int[])[0, 1, 7, 9, 64, 300, 700])
            {
                byte[] packed = new byte[bytes];
                random.NextBytes(packed);
                long capacity = bytes * 8L / Math.Max(1, width);
                foreach (int first in (int[])[0, 1, 5, 8, 13, 16])
                {
                    for (int length = 0; length <= 70 && first + length <= Math.Max(capacity, width == 0 ? 100 : 0); length += length < 20 ? 1 : 7)
                    {
                        if (width <= 32)
                        {
                            Check<int>(packed, first, width, length);
                            Check<uint>(packed, first, width, length);
                        }
                        if (width <= 8) Check<byte>(packed, first, width, length);
                        Check<long>(packed, first, width, length);
                        Check<ulong>(packed, first, width, length);
                    }
                }
            }
        }

        static void Check<T>(byte[] packed, int first, int width, int length) where T : unmanaged, System.Numerics.IBinaryInteger<T>
        {
            var fast = new T[length];
            var reference = new T[length];
            BitUnpacking.Unpack(packed, first, width, fast.AsSpan());
            BitUnpacking.UnpackChecked(packed, first, width, reference.AsSpan());
            Assert.True(fast.AsSpan().SequenceEqual(reference), $"width {width}, first {first}, length {length}, {packed.Length} bytes, {typeof(T).Name}");
            // Both against values assembled bit by bit (bits past the end read as zeros).
            for (int i = 0; i < length; i++)
            {
                ulong value = 0;
                for (int b = 0; b < width; b++)
                {
                    long bit = (long)(first + i) * width + b;
                    if (bit >> 3 < packed.Length && (packed[bit >> 3] >> (int)(bit & 7) & 1) != 0) value |= 1UL << b;
                }
                Assert.True(reference[i] == T.CreateTruncating(value), $"bit by bit: width {width}, value {first + i}, {typeof(T).Name}");
            }
        }
    }

    [Fact]
    public unsafe void Bitmap_AppendMaskAndAppendBits_EqualBitByBitAppends()
    {
        var random = new Random(11);
        for (int before = 0; before < 10; before++)
        {
            foreach (int length in (int[])[0, 1, 7, 8, 15, 16, 17, 31, 32, 33, 64, 100, 257])
            {
                byte[] mask = new byte[length];
                for (int i = 0; i < length; i++) mask[i] = (byte)(random.Next(3) == 0 ? 0 : random.Next(1, 256));
                byte[] packed = new byte[(length + 16) / 8 + 2];
                random.NextBytes(packed);
                foreach (int firstBit in (int[])[0, 3, 8, 13])
                {
                    if (firstBit + length > packed.Length * 8) continue;
                    var fast = new NativeBitmap();
                    var slow = new NativeBitmap();
                    try
                    {
                        for (int i = 0; i < before; i++)
                        {
                            fast.Append(i % 3 == 0);
                            slow.Append(i % 3 == 0);
                        }
                        fast.AppendMask(mask);
                        foreach (byte m in mask) slow.Append(m != 0);
                        fast.AppendBits(packed, firstBit, length);
                        for (int i = 0; i < length; i++) slow.Append((packed[(firstBit + i) >> 3] >> ((firstBit + i) & 7) & 1) != 0);
                        Assert.Equal(slow.Length, fast.Length);
                        Assert.Equal(slow.UnsetCount, fast.UnsetCount);
                        long bytes = (slow.Length + 7) / 8;
                        byte* f = fast.Detach(out long fb);
                        byte* s = slow.Detach(out long sb);
                        Assert.True(new ReadOnlySpan<byte>(f, (int)bytes).SequenceEqual(new ReadOnlySpan<byte>(s, (int)bytes)), $"before {before}, length {length}, first {firstBit}");
                        NativeMemoryPool.Free(f);
                        NativeMemoryPool.Free(s);
                    }
                    finally
                    {
                        fast.Free();
                        slow.Free();
                    }
                }
            }
        }
    }

    [Fact]
    public void MaskExpand_EqualsTheSlotBySlotSpread_ForEveryLengthPatternAndWidth()
    {
        var random = new Random(13);
        foreach (int length in (int[])[0, 1, 7, 63, 64, 65, 127, 128, 129, 191, 200, 1000])
        {
            foreach (string pattern in (string[])["valid", "null", "random", "sparse", "dense", "leading", "trailing", "blocks", "lastnull", "firstnull"])
            {
                byte[] mask = new byte[length];
                for (int i = 0; i < length; i++)
                {
                    bool valid = pattern switch
                    {
                        "valid" => true,
                        "null" => false,
                        "sparse" => random.Next(10) == 0,
                        "dense" => random.Next(10) != 0,
                        "leading" => i >= length / 3,          // nulls first, then valid
                        "trailing" => i < length - length / 3, // valid, then nulls
                        "blocks" => i / 64 % 3 != 1,           // whole words valid and whole words null
                        "lastnull" => i != length - 1,         // a null slot reads the value after the last: there is none
                        "firstnull" => i != 0,
                        _ => random.Next(2) == 0,
                    };
                    mask[i] = valid ? (byte)random.Next(1, 256) : (byte)0;
                }
                Check<byte>(mask, random);
                Check<short>(mask, random);
                Check<int>(mask, random);
                Check<long>(mask, random);
                Check<Int128>(mask, random);
                Check<Triple>(mask, random);
                CheckBits(mask, random);
            }
        }

        // Bits laid over the slots: against the slot-by-slot reference, from any source bit, the mask as bytes and as
        // bits, and through the boolean builder after a few slots already in it (any bit position).
        static void CheckBits(byte[] mask, Random random)
        {
            int valid = mask.Count(m => m != 0);
            int first = random.Next(0, 70);
            byte[] source = new byte[(first + valid + 7) / 8];
            random.NextBytes(source);
            bool Source(int k) => (source[(first + k) >> 3] >> ((first + k) & 7) & 1) != 0;
            var expected = new bool[mask.Length];
            for (int i = 0, j = 0; i < mask.Length; i++) expected[i] = mask[i] != 0 && Source(j++);
            string what = $"bits: {mask.Length} slots, {valid} valid, from bit {first}";

            int words = (mask.Length + 63) / 64;
            ulong[] slots = new ulong[words + 1];
            random.NextBytes(System.Runtime.InteropServices.MemoryMarshal.AsBytes(slots.AsSpan()));
            ulong past = slots[^1];
            MaskExpand.Bits(source, first, slots, mask);
            for (int i = 0; i < words * 64; i++)
                Assert.True((slots[i >> 6] >> (i & 63) & 1) == (i < mask.Length && expected[i] ? 1UL : 0), $"{what}, slot {i}");
            Assert.Equal(past, slots[^1]);

            int offset = random.Next(0, 13);
            byte[] bitmap = new byte[(offset + mask.Length + 7) / 8 + 1];
            for (int i = 0; i < mask.Length; i++)
            {
                if (mask[i] != 0) bitmap[(offset + i) >> 3] |= (byte)(1 << ((offset + i) & 7));
            }
            ulong[] fromBits = new ulong[words];
            MaskExpand.Bits(source, first, fromBits, SlotMask.FromBits(bitmap, offset, mask.Length));
            Assert.True(fromBits.AsSpan().SequenceEqual(slots.AsSpan(0, words)), $"{what}, mask as bits");

            if (valid > 0) Assert.Throws<ArgumentOutOfRangeException>(() => MaskExpand.Bits(source.AsSpan(0, (first + valid - 1) / 8), first, new ulong[words], mask));
            if (mask.Length > 64) Assert.Throws<ArgumentOutOfRangeException>(() => MaskExpand.Bits(source, first, new ulong[words - 1], mask));
            if (mask.Length == 0) return;

            int before = random.Next(0, 9);
            using var builder = new NativeBooleanBuilder();
            for (int i = 0; i < before; i++) builder.Append(i % 2 == 0);
            builder.AppendBits(System.Runtime.InteropServices.MemoryMarshal.AsBytes(slots.AsSpan(0, words)), 0, (SlotMask)mask);
            using var array = (Apache.Arrow.BooleanArray)builder.ExportArray();
            Assert.Equal(before + mask.Length, array.Length);
            Assert.Equal(mask.Length - valid, array.NullCount);
            for (int i = 0; i < mask.Length; i++)
                Assert.Equal(mask[i] != 0 ? expected[i] : null, array.GetValue(before + i));
        }

        static void Check<T>(byte[] mask, Random random) where T : unmanaged, IEquatable<T>
        {
            int valid = mask.Count(m => m != 0);
            T[] values = Random<T>(valid, random);
            T[] dictionary = Random<T>(17, random);
            int[] indices = [.. Enumerable.Range(0, valid).Select(_ => random.Next(dictionary.Length))];
            T[] expected = new T[mask.Length], gathered = new T[mask.Length];
            for (int i = 0, j = 0; i < mask.Length; i++)
            {
                if (mask[i] == 0) continue;
                gathered[i] = dictionary[indices[j]];
                expected[i] = values[j++];
            }
            string what = $"{typeof(T).Name}, {mask.Length} slots, {valid} valid";

            // The same mask as bits of a bitmap, from an odd offset, with garbage around it.
            int offset = random.Next(0, 13);
            byte[] bitmap = new byte[(offset + mask.Length + 7) / 8];
            random.NextBytes(bitmap);
            for (int i = 0; i < mask.Length; i++)
            {
                int bit = offset + i;
                bitmap[bit >> 3] = (byte)(mask[i] != 0 ? bitmap[bit >> 3] | 1 << (bit & 7) : bitmap[bit >> 3] & ~(1 << (bit & 7)));
            }
            SlotMask bits = SlotMask.FromBits(bitmap, offset, mask.Length);
            Assert.Equal(valid, bits.CountValid());
            Assert.True(bits.ToBytes(new byte[mask.Length]).SequenceEqual(mask.Select(m => m != 0 ? (byte)1 : (byte)0).ToArray()));
            for (int at = 0; at < mask.Length; at += 64)
            {
                int count = Math.Min(64, mask.Length - at);
                SlotMask bytes = mask;
                Assert.Equal(count == 64 ? bytes.Word(at) : bytes.PartialWord(at, count), count == 64 ? bits.Word(at) : bits.PartialWord(at, count));
            }
            T[] fromBits = Random<T>(mask.Length, random);
            values.CopyTo(fromBits, 0);
            MaskExpand.InPlace(fromBits.AsSpan(), valid, SlotMask.FromBits(bitmap, offset, mask.Length));
            Assert.True(fromBits.AsSpan().SequenceEqual(expected), $"in place from bits: {what}");
            fromBits = Random<T>(mask.Length, random);
            MaskExpand.Scatter<T>(values, fromBits, SlotMask.FromBits(bitmap, offset, mask.Length));
            Assert.True(fromBits.AsSpan().SequenceEqual(expected), $"scatter from bits: {what}");
            fromBits = Random<T>(mask.Length, random);
            MaskExpand.Gather<T>(dictionary, indices, fromBits, SlotMask.FromBits(bitmap, offset, mask.Length));
            Assert.True(fromBits.AsSpan().SequenceEqual(gathered), $"gather from bits: {what}");

            // The slots start as garbage and the one past the mask must stay untouched.
            T[] slots = Random<T>(mask.Length + 1, random);
            T past = slots[^1];
            values.CopyTo(slots, 0);
            MaskExpand.InPlace(slots.AsSpan(), valid, mask);
            Assert.True(slots.AsSpan(0, mask.Length).SequenceEqual(expected), $"in place: {what}");
            Assert.Equal(past, slots[^1]);

            slots = Random<T>(mask.Length + 1, random);
            past = slots[^1];
            MaskExpand.Scatter<T>(values, slots, mask);
            Assert.True(slots.AsSpan(0, mask.Length).SequenceEqual(expected), $"scatter: {what}");
            Assert.Equal(past, slots[^1]);

            slots = Random<T>(mask.Length + 1, random);
            past = slots[^1];
            MaskExpand.Gather<T>(dictionary, indices, slots, mask);
            Assert.True(slots.AsSpan(0, mask.Length).SequenceEqual(gathered), $"gather: {what}");
            Assert.Equal(past, slots[^1]);

            // A count of values other than the mask's valid slots is refused.
            T[] more = Random<T>(valid + 1, random);
            int[] moreIndices = [.. indices, 0];
            if (valid < mask.Length) Assert.Throws<ArgumentException>(() => MaskExpand.InPlace(Random<T>(mask.Length, random).AsSpan(), valid + 1, mask));
            Assert.Throws<ArgumentException>(() => MaskExpand.Scatter<T>(more, new T[mask.Length], mask));
            Assert.Throws<ArgumentException>(() => MaskExpand.Gather<T>(dictionary, moreIndices, new T[mask.Length], mask));
            if (valid == 0) return;
            Assert.Throws<ArgumentException>(() => MaskExpand.InPlace(Random<T>(mask.Length, random).AsSpan(), valid - 1, mask));
            Assert.Throws<ArgumentException>(() => MaskExpand.Scatter<T>(values.AsSpan(0, valid - 1), new T[mask.Length], mask));
            Assert.Throws<ArgumentException>(() => MaskExpand.Gather<T>(dictionary, indices.AsSpan(0, valid - 1), new T[mask.Length], mask));
        }

        static T[] Random<T>(int count, Random random) where T : unmanaged
        {
            var values = new T[count];
            random.NextBytes(System.Runtime.InteropServices.MemoryMarshal.AsBytes(values.AsSpan()));
            return values;
        }
    }

    private readonly record struct Triple(int A, int B, int C);

    [Fact]
    public void RleBitPacked_ManyShortRunsOfBothKinds_ReadInAnyChunks()
    {
        var random = new Random(17);
        foreach (int width in (int[])[1, 2, 3, 7, 8, 9, 15, 20, 32])
        {
            for (int round = 0; round < 20; round++)
            {
                // Short runs, as writers emit for random levels: bit-packed groups and RLE runs of a few values.
                List<byte> data = [];
                List<int> expected = [];
                long limit = width == 32 ? uint.MaxValue : (1L << width) - 1;
                for (int run = 0; run < 60; run++)
                {
                    if (random.Next(2) == 0)
                    {
                        // Now and then long runs too (more than a 64-bit word of bits).
                        int count = random.Next(8) == 0 ? random.Next(60, 300) : random.Next(1, 20);
                        int value = (int)random.NextInt64(limit + 1);
                        Varint(data, (ulong)count << 1);
                        for (int k = 0; k < (width + 7) / 8; k++) data.Add((byte)((uint)value >> (8 * k)));
                        expected.AddRange(Enumerable.Repeat(value, count));
                    }
                    else
                    {
                        int groups = random.Next(8) == 0 ? random.Next(7, 40) : random.Next(1, 4);
                        int[] values = [.. Enumerable.Range(0, groups * 8).Select(_ => (int)random.NextInt64(limit + 1))];
                        Varint(data, ((ulong)groups << 1) | 1);
                        data.AddRange(Pack(values, width));
                        expected.AddRange(values);
                    }
                }
                if (round % 2 == 1)
                {
                    // A last bit-packed run cut short: two groups declared, the bytes of 5 values there.
                    int[] values = [.. Enumerable.Range(0, 16).Select(_ => (int)random.NextInt64(limit + 1))];
                    Varint(data, (2UL << 1) | 1);
                    data.AddRange(Pack(values, width).Take(5 * width / 8));
                    expected.AddRange(values.Take(5 * width / 8 * 8 / width));
                }

                byte[] encoded = [.. data];
                var decoder = new RleBitPackedDecoder(encoded, width);
                var read = new int[expected.Count + 5];
                int done = 0;
                while (true)
                {
                    int chunk = Math.Min(random.Next(1, 50), read.Length - done);
                    int got = decoder.Read(read.AsSpan(done, chunk));
                    done += got;
                    if (got < chunk || done == read.Length) break;
                }
                Assert.Equal(expected.Count, done);
                Assert.True(read.AsSpan(0, done).SequenceEqual(expected.ToArray()), $"width {width}, round {round}");

                if (width == 1)
                {
                    // As bits, from an odd offset, in chunks: the same values, the bits before the offset kept.
                    int offset = random.Next(0, 13);
                    byte[] bitmap = new byte[(offset + expected.Count + 5 + 7) / 8 + 1];
                    random.NextBytes(bitmap);
                    byte[] before = (byte[])bitmap.Clone();
                    var bits = new RleBitPackedDecoder(encoded, 1);
                    int got = 0;
                    while (true)
                    {
                        int chunk = random.Next(1, 50);
                        int n = bits.ReadBits(bitmap, offset + got, Math.Min(chunk, expected.Count + 5 - got));
                        got += n;
                        if (n < chunk || got >= expected.Count + 5) break;
                    }
                    Assert.Equal(expected.Count, got);
                    for (int i = 0; i < offset; i++) Assert.Equal(Bits.Get(before, i), Bits.Get(bitmap, i));
                    for (int i = 0; i < got; i++) Assert.True(Bits.Get(bitmap, offset + i) == (expected[i] != 0), $"round {round}, bit {i}");

                    // All at once into a fresh bitmap: everything, or a prefix; the bits after it kept, the ones counted.
                    foreach (int want in (int[])[expected.Count + 5, expected.Count, random.Next(0, expected.Count + 1)])
                    {
                        byte[] all = new byte[(want + 7) / 8 + random.Next(0, 3)];
                        random.NextBytes(all);
                        byte[] old = (byte[])all.Clone();
                        int gotAll = RleBitPackedDecoder.ReadAllBits(encoded, all, want, out long ones);
                        int n = Math.Min(want, expected.Count);
                        Assert.Equal(n, gotAll);
                        Assert.Equal(expected.Take(n).Count(v => v != 0), ones);
                        for (int i = 0; i < n; i++) Assert.True(Bits.Get(all, i) == (expected[i] != 0), $"all at once: round {round}, bit {i} of {want}");
                        for (int i = n; i < all.Length * 8; i++) Assert.Equal(Bits.Get(old, i), Bits.Get(all, i));
                    }
                }
            }
        }

        // Damaged data is refused as a fresh decoder refuses it.
        Assert.Throws<ParquetFormatException>(() => RleBitPackedDecoder.ReadAllBits([4 << 1, 2], new byte[1], 4, out _));        // an RLE value wider than 1 bit
        Assert.Throws<ParquetFormatException>(() => RleBitPackedDecoder.ReadAllBits([4 << 1], new byte[1], 4, out _));           // an RLE run without its value
        Assert.Throws<ParquetFormatException>(() => RleBitPackedDecoder.ReadAllBits([0x81, 0x80], new byte[1], 4, out _));       // a header cut short
        Assert.Throws<ParquetFormatException>(() => RleBitPackedDecoder.ReadAllBits([.. Enumerable.Repeat((byte)0xFF, 11)], new byte[1], 4, out _)); // a header too long
        Assert.Throws<ArgumentOutOfRangeException>(() => RleBitPackedDecoder.ReadAllBits([2, 1], new byte[1], 9, out _));        // no room for the bits
        // A long varint header (a run of 200 ones) is read like a short one.
        byte[] ones200 = new byte[25];
        Assert.Equal(200, RleBitPackedDecoder.ReadAllBits([0x90, 0x03, 1], ones200, 200, out long set200));
        Assert.Equal(200, set200);
        Assert.All(ones200, b => Assert.Equal(0xFF, b));

        static void Varint(List<byte> data, ulong value)
        {
            for (; value >= 0x80; value >>= 7) data.Add((byte)(value | 0x80));
            data.Add((byte)value);
        }

        static byte[] Pack(int[] values, int width)
        {
            var packed = new byte[values.Length * width / 8];
            for (int i = 0; i < values.Length; i++)
            {
                for (int k = 0; k < width; k++)
                {
                    long bit = (long)i * width + k;
                    if (((uint)values[i] >> k & 1) != 0) packed[bit >> 3] |= (byte)(1 << (int)(bit & 7));
                }
            }
            return packed;
        }
    }

    [Fact]
    public void NativeMemoryPool_SizeClasses_AreTheSmallestThatFit()
    {
        Assert.Equal(-1, NativeMemoryPool.ClassOf(0));
        Assert.Equal(-1, NativeMemoryPool.ClassOf(64 * 1024 - 1));
        Assert.Equal(-1, NativeMemoryPool.ClassOf((1L << 30) + 1));
        long previous = 0;
        for (int c = 0; c < 57; c++)
        {
            long size = NativeMemoryPool.ClassSize(c);
            Assert.True(size > previous && size <= previous + previous / 4 + 1 || c == 0, $"class {c}: {size} after {previous}");
            Assert.Equal(c, NativeMemoryPool.ClassOf(size));
            if (c > 0) Assert.Equal(c, NativeMemoryPool.ClassOf(previous + 1));
            previous = size;
        }
        Assert.Equal(1L << 30, previous);
        Assert.Equal(64 * 1024, NativeMemoryPool.ClassSize(0));
    }

    [Fact]
    public void NonTemporal_CopyAndGather_EqualPlainWrites_AtEveryAlignment()
    {
        var random = new Random(23);
        foreach (int length in (int[])[0, 1, 31, 255, 256, 257, 300, 1000, 4096 + 17])
        {
            byte[] source = new byte[length + 8];
            random.NextBytes(source);
            for (int sourceOffset = 0; sourceOffset < 8; sourceOffset += 3)
            {
                for (int offset = 0; offset < 64; offset++)
                {
                    byte[] destination = new byte[length + 128];
                    random.NextBytes(destination);
                    byte[] before = (byte[])destination.Clone();
                    NonTemporal.Copy(source.AsSpan(sourceOffset, length), destination.AsSpan(offset, length));
                    Assert.True(destination.AsSpan(offset, length).SequenceEqual(source.AsSpan(sourceOffset, length)), $"copy of {length} at {offset}");
                    Assert.True(destination.AsSpan(0, offset).SequenceEqual(before.AsSpan(0, offset)));
                    Assert.True(destination.AsSpan(offset + length).SequenceEqual(before.AsSpan(offset + length)));
                }
            }
        }
        Assert.Throws<ArgumentException>(() => NonTemporal.Copy(new byte[10], new byte[9]));

        foreach (int n in (int[])[0, 1, 3, 15, 16, 17, 63, 64, 65, 1000])
        {
            Check<int>(n, random);
            Check<long>(n, random);
            Check<Int128>(n, random);
            Check<Triple>(n, random); // 12 bytes: plain writes
        }

        static void Check<T>(int n, Random random) where T : unmanaged, IEquatable<T>
        {
            T[] dictionary = Random<T>(37, random);
            int[] indices = [.. Enumerable.Range(0, n).Select(_ => random.Next(dictionary.Length))];
            for (int offset = 0; offset < 4; offset++)
            {
                T[] destination = Random<T>(n + 8, random);
                T[] before = (T[])destination.Clone();
                NonTemporal.Gather<T>(dictionary, indices, destination.AsSpan(offset, n));
                for (int i = 0; i < destination.Length; i++)
                {
                    T expected = i >= offset && i < offset + n ? dictionary[indices[i - offset]] : before[i];
                    Assert.True(expected.Equals(destination[i]), $"{typeof(T).Name}: {n} values at {offset}, slot {i}");
                }
            }
            if (n > 0) Assert.Throws<ArgumentException>(() => NonTemporal.Gather<T>(dictionary, indices, new T[n - 1]));
        }

        static T[] Random<T>(int count, Random random) where T : unmanaged
        {
            var values = new T[count];
            random.NextBytes(System.Runtime.InteropServices.MemoryMarshal.AsBytes(values.AsSpan()));
            return values;
        }
    }

    [Fact]
    public void BinaryViewBuilder_WritesPastCaches_BuildsTheSameViews()
    {
        var random = new Random(29);
        foreach (int rows in (int[])[0, 1, 255, 256, 257, 1000, 3000])
        {
            foreach (bool nulls in (bool[])[false, true])
            {
                // Lengths: inline (≤ 12), long, and now and then larger than a staging block (16 KiB).
                byte[] mask = nulls ? [.. Enumerable.Range(0, rows).Select(_ => (byte)(random.Next(4) == 0 ? 0 : 1))] : [];
                int valid = nulls ? mask.Count(m => m != 0) : rows;
                int[] lengths = [.. Enumerable.Range(0, valid).Select(_ => random.Next(10) switch { 0 => random.Next(20_000, 40_000), < 5 => random.Next(0, 13), _ => random.Next(13, 200) })];
                var source = new List<byte>();
                int[] starts = new int[valid];
                for (int j = 0; j < valid; j++)
                {
                    source.AddRange(new byte[3]); // gaps, as length prefixes leave in a page
                    starts[j] = source.Count;
                    byte[] value = new byte[lengths[j]];
                    random.NextBytes(value);
                    source.AddRange(value);
                }
                byte[] bytes = [.. source];
                long longBytes = lengths.Where(l => l > NativeBinaryViewBuilder.MaxInline).Sum(l => (long)l);

                string[] Build(bool pastCaches)
                {
                    using var builder = new NativeBinaryViewBuilder(Apache.Arrow.Types.BinaryViewType.Default) { WritesPastCaches = pastCaches };
                    builder.AppendValues(bytes, starts, lengths, mask, longBytes);
                    using var array = (Apache.Arrow.BinaryViewArray)builder.ExportArray();
                    Assert.Equal(rows, array.Length);
                    return [.. Enumerable.Range(0, rows).Select(i => array.IsNull(i) ? "null" : Convert.ToHexString(array.GetBytes(i)))];
                }

                Assert.Equal(Build(false), Build(true));
            }
        }
    }

    [Fact]
    public void Bits_CopyFillCountAndWord_EqualBitByBit()
    {
        var random = new Random(31);
        foreach (int length in (int[])[1, 2, 8, 9, 17, 40])
        {
            byte[] source = new byte[length];
            random.NextBytes(source);
            int bitsIn = length * 8;
            for (int from = 0; from < Math.Min(bitsIn, 20); from += 3)
            {
                // Word: the bits from `from`, those past the bitmap reading as 0.
                ulong expectedWord = 0;
                for (int k = 0; k < 64 && from + k < bitsIn; k++) expectedWord |= (Bit(source, from + k) ? 1UL : 0) << k;
                Assert.Equal(expectedWord, Bits.Word(source, from));
                for (int count = 0; from + count <= bitsIn; count += count < 20 ? 1 : 13)
                {
                    Assert.Equal(Enumerable.Range(from, count).Count(i => Bit(source, i)), Bits.Count(source, from, count));
                    foreach (int to in (int[])[0, 1, 7, 8, 13])
                    {
                        // Destinations exactly as long as needed (the slow ends) and with room to spare.
                        foreach (int spare in (int[])[0, 9])
                        {
                            byte[] destination = new byte[(to + count + 7) / 8 + spare];
                            random.NextBytes(destination);
                            byte[] expected = (byte[])destination.Clone();
                            for (int i = 0; i < count; i++) SetBit(expected, to + i, Bit(source, from + i));
                            Bits.Copy(source, from, destination, to, count);
                            Assert.True(expected.AsSpan().SequenceEqual(destination), $"copy {count} bits {from} → {to} ({length} bytes, spare {spare})");

                            bool value = random.Next(2) == 0;
                            for (int i = 0; i < count; i++) SetBit(expected, to + i, value);
                            Bits.Fill(destination, to, count, value);
                            Assert.True(expected.AsSpan().SequenceEqual(destination), $"fill {count} bits at {to} with {value}");
                        }
                    }
                }
            }
        }
        Assert.Throws<ArgumentOutOfRangeException>(() => Bits.Copy(new byte[1], 4, new byte[2], 0, 5));
        Assert.Throws<ArgumentOutOfRangeException>(() => Bits.Fill(new byte[1], 4, 5, true));

        static bool Bit(byte[] bitmap, int i) => (bitmap[i >> 3] >> (i & 7) & 1) != 0;
        static void SetBit(byte[] bitmap, int i, bool value) =>
            bitmap[i >> 3] = (byte)(value ? bitmap[i >> 3] | 1 << (i & 7) : bitmap[i >> 3] & ~(1 << (i & 7)));
    }

    [Fact]
    public void SnappyBlock_DecodesWhatSnappierEncodes_AndRejectsDamage()
    {
        var random = new Random(5);
        List<byte[]> inputs = [[], [42], "abcabcabcabcabcabcabcabcabcabcabcabcabcabcabc"u8.ToArray()];
        foreach (int size in (int[])[1, 7, 15, 16, 17, 63, 64, 65, 1000, 70_000, 300_000])
        {
            byte[] noise = new byte[size];
            random.NextBytes(noise);
            inputs.Add(noise);
            byte[] text = new byte[size];
            for (int i = 0; i < size; i++) text[i] = (byte)"lorem ipsum dolor sit amet "[(i * 7 + i / 13) % 27];
            inputs.Add(text);
            byte[] runs = new byte[size];
            for (int i = 0; i < size; i++) runs[i] = (byte)(i / (1 + i % 9) % 3); // short periods: overlapping copies
            inputs.Add(runs);
        }
        foreach (byte[] input in inputs)
        {
            byte[] compressed = new byte[Snappier.Snappy.GetMaxCompressedLength(input.Length)];
            int n = Snappier.Snappy.Compress(input, compressed);
            byte[] output = new byte[input.Length];
            Assert.Equal(input.Length, SnappyBlock.UncompressedLength(compressed.AsSpan(0, n)));
            Assert.Equal(input.Length, SnappyBlock.Decompress(compressed.AsSpan(0, n), output));
            Assert.True(output.AsSpan().SequenceEqual(input), $"{input.Length} bytes");

            if (n < 2) continue;
            for (int round = 0; round < 50; round++)
            {
                byte[] damaged = compressed[..n];
                damaged[random.Next(n)] ^= (byte)random.Next(1, 256);
                int cut = random.Next(2) == 0 ? n : random.Next(n);
                try
                {
                    SnappyBlock.Decompress(damaged.AsSpan(0, cut), new byte[input.Length]);
                }
                catch (InvalidDataException)
                {
                }
            }
        }
    }

    [Fact]
    public unsafe void ExportedBatch_KeepsItsMemory_UntilTheExportIsReleased_ThoughDisposed()
    {
        using var file = Kaminari.Seseragi.Files.Parquet.ParquetFile.Open(ParquetFixtures.FullPath("writers/pyarrow.parquet"));
        using var reader = file.OpenReader(new() { MaxDegreeOfParallelism = 1 });
        Apache.Arrow.RecordBatch batch = reader.ReadBatches().First();
        Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetMemoryManager(batch.Column(0).Data.Buffers[1].Memory, out NativeBufferMemory? memory));
        NativeBatchMemory owner = memory!.Owner;

        Apache.Arrow.C.CArrowArray* exported = Apache.Arrow.C.CArrowArray.Create();
        Apache.Arrow.C.CArrowArrayExporter.ExportRecordBatch(batch, exported);
        batch.Dispose();
        Assert.False(owner.IsFreed); // the export's pins hold it
        Apache.Arrow.C.CArrowArray.Free(exported); // releases the export
        Assert.True(owner.IsFreed);

        // A pin alone (what any consumer handing the memory on takes) keeps it too.
        Apache.Arrow.RecordBatch pinned = reader.ReadBatches().First();
        Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetMemoryManager(pinned.Column(0).Data.Buffers[1].Memory, out NativeBufferMemory? pinnedMemory));
        System.Buffers.MemoryHandle handle = pinned.Column(0).Data.Buffers[1].Memory.Pin();
        pinned.Dispose();
        Assert.False(pinnedMemory!.Owner.IsFreed);
        handle.Dispose();
        Assert.True(pinnedMemory.Owner.IsFreed);

        // Without an export, disposing frees it at once.
        Apache.Arrow.RecordBatch other = reader.ReadBatches().First();
        Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetMemoryManager(other.Column(0).Data.Buffers[1].Memory, out NativeBufferMemory? otherMemory));
        other.Dispose();
        Assert.True(otherMemory!.Owner.IsFreed);
    }
}
