using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Filters;
using static Sazanami.Tests.FilterTestData;

namespace Sazanami.Tests;

/// <summary><see cref="ArrowFilter"/> against a model gather, and the lifetime of the memory it shares.</summary>
public class ArrowFilterTests
{
    private static ulong[] RandomMask(Random random, int length)
    {
        var mask = new ulong[Masks.Words(length) + 1];
        int shape = random.Next(7);
        bool inRun = random.Next(2) == 0;
        for (int i = 0; i < length; i++)
        {
            bool set = shape switch
            {
                0 => false,
                1 => true,
                2 => random.Next(100) == 0,
                3 => random.Next(10) == 0,
                4 => random.Next(2) == 0,
                5 => random.Next(20) != 0,
                _ => (inRun ^= random.Next(40) == 0),
            };
            if (set) mask[i >> 6] |= 1UL << (i & 63);
        }
        // Bits past the array's rows are ignored.
        mask[^1] = ulong.MaxValue;
        if (length % 64 != 0) mask[length >> 6] |= ~Masks.LastWord(length);
        return mask;
    }

    private static bool SameValue(object? a, object? b) =>
        a is null || b is null ? a is null && b is null
        : a is double x && b is double y ? BitConverter.DoubleToInt64Bits(x) == BitConverter.DoubleToInt64Bits(y) || double.IsNaN(x) && double.IsNaN(y)
        : Compare(a, b) == 0;

    [Fact]
    public void RandomArrays_KeepTheSelectedRows()
    {
        var random = new Random(1114);
        int[] lengths = [0, 1, 5, 63, 64, 65, 128, 129, 300, 1000, 5000, 65536];
        for (int round = 0; round < 1200; round++)
        {
            ColumnKind kind = Kinds[random.Next(Kinds.Length)];
            int length = lengths[random.Next(lengths.Length)];
            int offset = random.Next(3) == 0 ? random.Next(1, 70) : 0;
            double nulls = random.Next(3) switch { 0 => 0, 1 => 0.1, _ => 0.6 };
            object?[] values = Values(random, kind, length + offset, nulls);
            IArrowArray whole = random.Next(5) switch
            {
                0 when kind.Dictionaries => BuildDictionary(random, kind, values),
                0 or 1 or 2 => BuildNative(kind, values),
                _ => Build(random, kind, values),
            };
            IArrowArray array = offset == 0 ? whole : ArrowArrayFactory.Slice(whole, offset, length);
            ulong[] mask = RandomMask(random, length);
            bool? share = random.Next(3) switch { 0 => true, 1 => false, _ => null };
            using IArrowArray filtered = ArrowFilter.Filter(array, mask, share);
            int[] kept = [.. Enumerable.Range(0, length).Where(i => (mask[i >> 6] >> (i & 63) & 1) != 0)];
            string where = $"round {round}: {kind} length {length} offset {offset} kept {kept.Length}";
            Assert.True(kept.Length == filtered.Length, where);
            Assert.Equal(filtered.NullCount, Enumerable.Range(0, filtered.Length).Count(filtered.IsNull));
            if (filtered.NullCount == 0) Assert.True(filtered.Data.Buffers[0].IsEmpty, $"{where}: a validity buffer without nulls");
            for (int j = 0; j < kept.Length; j++)
            {
                object? want = values[offset + kept[j]], got = Read(kind, filtered, j);
                Assert.True(SameValue(want, got), $"{where}: row {j} (from {kept[j]})");
                // A null view of a copy points nowhere (the input's may point into buffers the copy lacks).
                if (filtered is BinaryViewArray && share == false && want is null)
                    Assert.True(filtered.Data.Buffers[1].Span.Slice((filtered.Data.Offset + j) * 16, 16).IndexOfAnyExcept((byte)0) < 0, $"{where}: null view {j}");
            }
            whole.Dispose();
        }
    }

    private static unsafe nint Address(ArrowBuffer buffer) =>
        (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(buffer.Span));

    private static NativeBatchMemory Owner(ArrowBuffer buffer) =>
        MemoryMarshal.TryGetMemoryManager(buffer.Memory, out NativeBufferMemory? manager) ? manager.Owner : throw new InvalidOperationException("Not native.");

    private static object?[] LongStrings(int n) =>
        [.. Enumerable.Range(0, n).Select(i => i % 5 == 4 ? null : (object)Encoding.UTF8.GetBytes(i % 2 == 0 ? $"s{i}" : $"a long value number {i}"))];

    [Fact]
    public void Views_ShareTheirDataWhenDenseAndCopyWhenSparse()
    {
        ColumnKind kind = Kinds.Single(k => k.Name == "view");
        object?[] values = LongStrings(1000);
        using IArrowArray source = BuildNative(kind, values);
        nint data = Address(source.Data.Buffers[2]);

        ulong[] dense = new ulong[Masks.Words(1000)];
        Masks.Fill(dense, 1000, true);
        dense[0] = ~1UL; // all rows but the first
        using (IArrowArray shared = ArrowFilter.Filter(source, dense))
        {
            Assert.Equal(source.Data.Buffers.Length, shared.Data.Buffers.Length);
            Assert.Equal(data, Address(shared.Data.Buffers[2]));
            Assert.Same(Owner(source.Data.Buffers[2]), Owner(shared.Data.Buffers[2]));
        }

        ulong[] sparse = new ulong[Masks.Words(1000)];
        sparse[3] = 0b1010;
        using (IArrowArray copied = ArrowFilter.Filter(source, sparse))
        {
            Assert.Equal(2, copied.Length);
            Assert.NotEqual(data, Address(copied.Data.Buffers[2]));
            Assert.Equal(((BinaryViewArray)source).GetBytes(193).ToArray(), ((BinaryViewArray)copied).GetBytes(0).ToArray());
        }

        // Forced either way; values from managed memory are always copied.
        using (IArrowArray forced = ArrowFilter.Filter(source, sparse, shareViewData: true)) Assert.Equal(data, Address(forced.Data.Buffers[2]));
        using (IArrowArray forced = ArrowFilter.Filter(source, dense, shareViewData: false)) Assert.NotEqual(data, Address(forced.Data.Buffers[2]));
        IArrowArray managed = Build(new Random(3), kind, values);
        using IArrowArray fromManaged = ArrowFilter.Filter(managed, dense, shareViewData: true);
        Assert.Equal(1, fromManaged.Data.Buffers.Length - 2); // the kept long values, put together
        for (int i = 0; i < fromManaged.Length; i++)
            Assert.True(SameValue(values[i + 1], Read(kind, fromManaged, i)));
    }

    [Fact]
    public void SharedMemory_LivesUntilTheLastArrayIsDisposed()
    {
        ColumnKind kind = Kinds.Single(k => k.Name == "view");
        object?[] values = LongStrings(200);
        IArrowArray source = BuildNative(kind, values);
        NativeBatchMemory owner = Owner(source.Data.Buffers[2]);
        ulong[] all = new ulong[Masks.Words(200)];
        Masks.Fill(all, 200, true);
        IArrowArray filtered = ArrowFilter.Filter(source, all, shareViewData: true);
        source.Dispose();
        Assert.False(owner.IsFreed);
        for (int i = 0; i < 200; i++) Assert.True(SameValue(values[i], Read(kind, filtered, i)), $"row {i}");
        filtered.Dispose();
        Assert.True(owner.IsFreed);
    }

    private static readonly string[] Entries = ["x", "yy", "zzz"];

    [Fact]
    public void Dictionaries_AreSharedFromNativeMemory()
    {
        using var builder = new NativeDictionaryBuilder(new DictionaryType(Int32Type.Default, StringType.Default, false));
        foreach (string s in Entries) builder.Entries.Append(Encoding.UTF8.GetBytes(s));
        for (int i = 0; i < 100; i++)
        {
            if (i % 7 == 0) builder.AppendNull();
            else builder.Append(i % 3);
        }
        IArrowArray source = builder.ExportArray();
        NativeBatchMemory owner = Owner(source.Data.Dictionary!.Buffers[2]);
        var mask = new ulong[2];
        mask[0] = 0xF0F0F0F0F0F0F0F0;
        mask[1] = 0b111;
        var filtered = (DictionaryArray)ArrowFilter.Filter(source, mask);
        Assert.Equal(Address(source.Data.Dictionary.Buffers[2]), Address(filtered.Data.Dictionary!.Buffers[2]));
        source.Dispose();
        Assert.False(owner.IsFreed);
        int[] kept = [.. Enumerable.Range(0, 100).Where(i => (mask[i >> 6] >> (i & 63) & 1) != 0)];
        Assert.Equal(kept.Length, filtered.Length);
        var strings = (StringArray)filtered.Dictionary;
        for (int j = 0; j < kept.Length; j++)
        {
            int i = kept[j];
            if (i % 7 == 0) Assert.True(filtered.IsNull(j));
            else Assert.Equal(Entries[i % 3], strings.GetString(((Int32Array)filtered.Indices).GetValue(j)!.Value));
        }
        filtered.Dispose();
        Assert.True(owner.IsFreed);
    }

    [Fact]
    public void Dictionaries_FromManagedMemoryAreCopied()
    {
        IArrowArray dictionary = Build(new Random(5), Kinds.Single(k => k.Name == "utf8"), ["a"u8.ToArray(), null, "b"u8.ToArray()]);
        var data = new ArrayData(new DictionaryType(Int16Type.Default, StringType.Default, false), 3, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(MemoryMarshal.AsBytes<short>([2, 1, 0]).ToArray())], [], dictionary.Data);
        using var filtered = (DictionaryArray)ArrowFilter.Filter(ArrowArrayFactory.BuildArray(data), [0b101]);
        Assert.Equal(2, filtered.Length);
        Assert.Equal([(short)2, (short)0], ((Int16Array)filtered.Indices).Values.ToArray());
        Assert.True(filtered.Dictionary.IsNull(1));
        Assert.Equal("b", ((StringArray)filtered.Dictionary).GetString(2));
    }

    [Fact]
    public void NullAndNestedArrays()
    {
        using IArrowArray nulls = ArrowFilter.Filter(new NullArray(100), [0b1011, 0]);
        Assert.Equal(3, nulls.Length);
        Assert.IsType<NullArray>(nulls);
        var list = new ListArray.Builder(Int32Type.Default);
        list.Append();
        list.Append();
        Assert.Throws<NotSupportedException>(() => ArrowFilter.Filter(list.Build(), [1]));
        Assert.Throws<ArgumentException>(() => ArrowFilter.Filter(new NullArray(100), [1]));
    }
}
