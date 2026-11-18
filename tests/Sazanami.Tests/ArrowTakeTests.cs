using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Filters;
using Kaminari.Sazanami.Arrow.Builders;
using static Sazanami.Tests.FilterTestData;

namespace Sazanami.Tests;

/// <summary><see cref="ArrowTake"/> against a model gather, the null rows, the bounds, and the lifetime of the memory it shares.</summary>
public class ArrowTakeTests
{
    private const int Null = ArrowTake.NullRow;

    /// <summary>The string and binary kinds again, laid out with long offsets; fixed-size binaries of an odd width.</summary>
    private static readonly ColumnKind[] LargeKinds =
    [
        new("lutf8", LargeStringType.Default, Kinds.Single(k => k.Name == "utf8").Pool),
        new("lbinary", LargeBinaryType.Default, Kinds.Single(k => k.Name == "binary").Pool),
        new("fsb3", new FixedSizeBinaryType(3), ["abc"u8.ToArray(), "abd"u8.ToArray(), (byte[])[0, 0, 0], (byte[])[0xFF, 0, 1]], 3),
    ];

    /// <summary>Indices of a random shape into an array of <paramref name="length"/> rows.</summary>
    private static int[] RandomRows(Random random, int length)
    {
        if (length == 0) return random.Next(2) == 0 ? [] : [.. Enumerable.Repeat(Null, random.Next(1, 70))];
        int n = random.Next(4) switch { 0 => length, 1 => random.Next(length + 1), 2 => 2 * length, _ => random.Next(10) };
        return random.Next(9) switch
        {
            0 => [],
            1 => [.. Enumerable.Range(0, length)],
            2 => [.. Enumerable.Range(0, length).Reverse()],
            3 => [.. Enumerable.Range(0, n).Select(_ => random.Next(length))],
            4 => [.. Enumerable.Repeat(random.Next(length), n)],
            5 => [.. Enumerable.Range(0, length).Where(_ => random.Next(10) == 0)],
            6 => [.. Enumerable.Range(0, n).Select(_ => random.Next(5) == 0 ? Null : random.Next(length))],
            7 => [.. Enumerable.Repeat(Null, n)],
            _ => [.. Enumerable.Range(0, n).Select(_ => random.Next(length)).Order()],
        };
    }

    private static bool SameValue(object? a, object? b) =>
        a is null || b is null ? a is null && b is null
        : a is double x && b is double y ? BitConverter.DoubleToInt64Bits(x) == BitConverter.DoubleToInt64Bits(y) || double.IsNaN(x) && double.IsNaN(y)
        : Compare(a, b) == 0;

    /// <summary>A string or binary array with long offsets (in managed memory), from one with int offsets.</summary>
    internal static IArrowArray ToLarge(IArrowArray array, IArrowType type)
    {
        ArrayData data = array.Data;
        ReadOnlySpan<int> offsets = MemoryMarshal.Cast<byte, int>(data.Buffers[1].Span).Slice(data.Offset, data.Length + 1);
        long[] wide = [.. offsets.ToArray().Select(o => (long)o)];
        ArrowBuffer validity = data.Buffers[0].IsEmpty ? ArrowBuffer.Empty : new ArrowBuffer(ValidityFrom(array));
        ArrowBuffer[] buffers = type is LargeListType
            ? [validity, new ArrowBuffer(MemoryMarshal.AsBytes(wide.AsSpan()).ToArray())]
            : [validity, new ArrowBuffer(MemoryMarshal.AsBytes(wide.AsSpan()).ToArray()), new ArrowBuffer(data.Buffers[2].Span.ToArray())];
        ArrayData[] children = type is LargeListType ? [data.Children[0]] : [];
        return ArrowArrayFactory.BuildArray(new ArrayData(type, data.Length, data.NullCount, 0, buffers, children));

        static byte[] ValidityFrom(IArrowArray array)
        {
            var bits = new byte[(array.Length + 7) / 8 + 1];
            for (int i = 0; i < array.Length; i++)
            {
                if (!array.IsNull(i)) bits[i >> 3] |= (byte)(1 << (i & 7));
            }
            return bits;
        }
    }

    private static object? ReadAny(ColumnKind kind, IArrowArray array, int i) => array switch
    {
        LargeBinaryArray large => large.IsNull(i) ? null : large.GetBytes(i).ToArray(),
        _ => Read(kind, array, i),
    };

    private static void AssertTaken(IArrowArray taken, int[] rows, Func<int, object?> want, Func<int, object?> got, string where)
    {
        Assert.True(rows.Length == taken.Length, where);
        Assert.Equal(taken.NullCount, Enumerable.Range(0, taken.Length).Count(taken.IsNull));
        if (taken.NullCount == 0 && taken is not NullArray) Assert.True(taken.Data.Buffers[0].IsEmpty, $"{where}: a validity buffer without nulls");
        for (int j = 0; j < rows.Length; j++)
            Assert.True(SameValue(rows[j] == Null ? null : want(rows[j]), got(j)), $"{where}: row {j} (from {rows[j]})");
    }

    [Fact]
    public void RandomArrays_TakeTheRowsNamed()
    {
        var random = new Random(1118);
        int[] lengths = [0, 1, 5, 63, 64, 65, 128, 129, 300, 1000, 5000];
        ColumnKind[] kinds = [.. Kinds, .. LargeKinds];
        for (int round = 0; round < 1500; round++)
        {
            ColumnKind kind = kinds[random.Next(kinds.Length)];
            bool large = kind.Type is LargeStringType or LargeBinaryType;
            ColumnKind built = large ? Kinds.Single(k => k.Name == (kind.Type is LargeStringType ? "utf8" : "binary")) : kind;
            int length = lengths[random.Next(lengths.Length)];
            int offset = random.Next(3) == 0 ? random.Next(1, 70) : 0;
            double nulls = random.Next(3) switch { 0 => 0, 1 => 0.1, _ => 0.6 };
            object?[] values = Values(random, built, length + offset, nulls);
            IArrowArray whole = random.Next(5) switch
            {
                0 when built.Dictionaries && !large => BuildDictionary(random, built, values),
                0 or 1 or 2 => BuildNative(built, values),
                _ => Build(random, built, values),
            };
            if (large) whole = ToLarge(whole, kind.Type);
            IArrowArray array = offset == 0 ? whole : ArrowArrayFactory.Slice(whole, offset, length);
            int[] rows = RandomRows(random, length);
            bool? share = random.Next(3) switch { 0 => true, 1 => false, _ => null };
            using IArrowArray taken = ArrowTake.Take(array, rows, share);
            string where = $"round {round}: {kind} length {length} offset {offset} rows {rows.Length}";
            AssertTaken(taken, rows, i => values[offset + i], j => ReadAny(kind, taken, j), where);
            // A null view of a copy points nowhere (the input's may point into buffers the copy lacks).
            if (taken is BinaryViewArray && share == false)
            {
                for (int j = 0; j < rows.Length; j++)
                {
                    if (taken.IsNull(j))
                        Assert.True(taken.Data.Buffers[1].Span.Slice(j * 16, 16).IndexOfAnyExcept((byte)0) < 0, $"{where}: null view {j}");
                }
            }
            whole.Dispose();
        }
    }

    [Fact]
    public void Buffers_AreAsLongAsTheirValues()
    {
        // Each buffer's length is the bytes written for it (the batch's allocations keep their sizes).
        ColumnKind i64 = Kinds.Single(k => k.Name == "i64"), utf8 = Kinds.Single(k => k.Name == "utf8");
        using IArrowArray longs = BuildNative(i64, [.. Enumerable.Range(0, 300).Select(i => i % 7 == 0 ? null : (object)(Int128)i)]);
        using IArrowArray taken = ArrowTake.Take(longs, [5, 7, 299, 0, 7]);
        Assert.Equal(5 * 8, taken.Data.Buffers[1].Length);
        Assert.Equal(8, taken.Data.Buffers[0].Length); // one word of validity
        using IArrowArray strings = Build(new Random(2), utf8, ["ab"u8.ToArray(), "cde"u8.ToArray(), null]);
        using IArrowArray text = ArrowTake.Take(strings, [1, 0, 1, 2]);
        Assert.Equal(5 * 4, text.Data.Buffers[1].Length);
        Assert.Equal(3 + 2 + 3, text.Data.Buffers[2].Length);
        // In one batch's memory too: rows 2 (a null string) and 1 ("cde").
        var source = new RecordBatch(new Schema([new Field("a", Int64Type.Default, true), new Field("s", StringType.Default, true)], null),
            [ArrowArrayFactory.Slice(longs, 0, 3), strings], 3);
        using RecordBatch batch = ArrowTake.Take(source, [2, 1]);
        Assert.Equal(2 * 8, batch.Column(0).Data.Buffers[1].Length);
        Assert.Equal(3, batch.Column(1).Data.Buffers[2].Length);
    }

    [Fact]
    public void Indices_OutsideTheArray_Throw()
    {
        using IArrowArray array = BuildNative(Kinds.Single(k => k.Name == "i32"), [(Int128)1, (Int128)2, (Int128)3]);
        Assert.Throws<ArgumentOutOfRangeException>(() => ArrowTake.Take(array, [0, 3]));
        Assert.Throws<ArgumentOutOfRangeException>(() => ArrowTake.Take(array, [-2, 0]));
        Assert.Throws<ArgumentOutOfRangeException>(() => ArrowTake.Take(array, [int.MinValue]));
        using IArrowArray empty = BuildNative(Kinds.Single(k => k.Name == "i32"), []);
        Assert.Throws<ArgumentOutOfRangeException>(() => ArrowTake.Take(empty, [0]));
        using IArrowArray none = ArrowTake.Take(empty, []);
        Assert.Equal(0, none.Length);
    }

    [Fact]
    public void Overflow_IsReported_BeforeAllocating()
    {
        // One value of 1 MiB taken 2048 times passes 2 GB.
        IArrowArray strings = Build(new Random(1), Kinds.Single(k => k.Name == "utf8"), [new byte[1 << 20]]);
        Assert.Throws<OverflowException>(() => ArrowTake.Take(strings, new int[2048]));
        // A fixed-size list of 2^20 elements taken 2048 times holds more elements than an array takes.
        var type = new FixedSizeListType(new Field("item", Int8Type.Default, true), 1 << 20);
        var fixedList = new FixedSizeListArray(type, 1, new Int8Array.Builder().AppendRange(new sbyte[1 << 20]).Build(), ArrowBuffer.Empty, 0);
        Assert.Throws<OverflowException>(() => ArrowTake.Take(fixedList, new int[2048]));
        // ... as do list rows.
        var lists = new ListArray.Builder(Int8Type.Default);
        lists.Append();
        ((Int8Array.Builder)lists.ValueBuilder).AppendRange(new sbyte[1 << 20]);
        Assert.Throws<OverflowException>(() => ArrowTake.Take(lists.Build(), new int[2048]));
        // Columns taken side by side fail as the serial take does (not with Parallel's wrapper).
        var schema = new Schema([new Field("s", StringType.Default, true), new Field("i", Int32Type.Default, true)], null);
        using var batch = new RecordBatch(schema, [strings, new Int32Array.Builder().Append(1).Build()], 1);
        foreach (int parallel in (int[])[1, 4]) Assert.Throws<OverflowException>(() => ArrowTake.Take(batch, new int[2048], maxDegreeOfParallelism: parallel));
    }

    [Fact]
    public void ForeignBuffers_ShorterThanTheirArray_Throw()
    {
        // Ten int64 rows claimed over five values' bytes: refused, not read past the buffer.
        var data = new ArrayData(Int64Type.Default, 10, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(new byte[40])]);
        Assert.Throws<ArgumentOutOfRangeException>(() => ArrowTake.Take(ArrowArrayFactory.BuildArray(data), [9]));
        var odd = new ArrayData(new FixedSizeBinaryType(3), 10, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(new byte[15])]);
        Assert.Throws<ArgumentOutOfRangeException>(() => ArrowTake.Take(ArrowArrayFactory.BuildArray(odd), [9]));
        var bits = new ArrayData(BooleanType.Default, 100, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(new byte[2])]);
        Assert.Throws<ArgumentException>(() => ArrowTake.Take(ArrowArrayFactory.BuildArray(bits), [99]));
    }

    private static readonly StructType NestedType = new(
    [
        new Field("a", Int32Type.Default, true),
        new Field("l", new ListType(new Field("item", StringType.Default, true)), true),
        new Field("m", new MapType(StringType.Default, Int64Type.Default), true),
        new Field("f", new FixedSizeListType(new Field("item", Int16Type.Default, true), 2), true),
        new Field("ll", new LargeListType(new Field("item", StringViewType.Default, true)), true),
        new Field("d", new DictionaryType(Int32Type.Default, StringType.Default, false), true),
    ]);

    /// <summary>A struct array of every nested layout (and a dictionary), with random nulls at every level.</summary>
    internal static IArrowArray Nested(Random random, int length)
    {
        var a = new Int32Array.Builder();
        var lists = new ListArray.Builder(StringType.Default);
        var largeItems = new ListArray.Builder(StringViewType.Default);
        var maps = new MapArray.Builder(new MapType(StringType.Default, Int64Type.Default));
        var fixedValues = new Int16Array.Builder();
        var indices = new Int32Array.Builder();
        var validity = new byte[(length + 7) / 8];
        var fixedValidity = new byte[(length + 7) / 8];
        for (int i = 0; i < length; i++)
        {
            if (random.Next(6) != 0) validity[i / 8] |= (byte)(1 << (i % 8));
            a.Append(random.Next(4) == 0 ? null : i);
            if (random.Next(5) == 0) lists.AppendNull();
            else
            {
                lists.Append();
                int n = random.Next(4);
                for (int k = 0; k < n; k++) ((StringArray.Builder)lists.ValueBuilder).Append(random.Next(5) == 0 ? null : $"v{i}.{k}");
            }
            if (random.Next(5) == 0) largeItems.AppendNull();
            else
            {
                largeItems.Append();
                int n = random.Next(4);
                for (int k = 0; k < n; k++)
                    ((StringViewArray.Builder)largeItems.ValueBuilder).Append(random.Next(5) == 0 ? null : $"a longer value {i}.{k} of a large list");
            }
            if (random.Next(5) == 0) maps.AppendNull();
            else
            {
                maps.Append();
                int n = random.Next(3);
                for (int k = 0; k < n; k++)
                {
                    ((StringArray.Builder)maps.KeyBuilder).Append($"k{k}");
                    ((Int64Array.Builder)maps.ValueBuilder).Append(i * 10L + k);
                }
            }
            if (random.Next(5) != 0) fixedValidity[i / 8] |= (byte)(1 << (i % 8));
            fixedValues.Append((short)i).Append((short)-i);
            if (random.Next(6) == 0) indices.AppendNull();
            else indices.Append(random.Next(3));
        }
        var fixedList = new FixedSizeListArray(new FixedSizeListType(new Field("item", Int16Type.Default, true), 2), length, fixedValues.Build(),
            new ArrowBuffer(fixedValidity), -1);
        IArrowArray entries = new StringArray.Builder().Append("x").Append("yy").Append("zzz").Build();
        var dictionary = new DictionaryArray((DictionaryType)NestedType.Fields[5].DataType, indices.Build(), entries);
        IArrowArray[] children =
            [a.Build(), lists.Build(), maps.Build(), fixedList, ToLarge(largeItems.Build(), NestedType.Fields[4].DataType), dictionary];
        return new StructArray(NestedType, length, children, new ArrowBuffer(validity), -1);
    }

    [Fact]
    public void NestedArrays_TakeTheRowsNamed_AndTheirElements()
    {
        var random = new Random(1119);
        foreach (int length in (int[])[0, 1, 5, 64, 65, 200])
        {
            for (int round = 0; round < 12; round++)
            {
                IArrowArray whole = Nested(random, length);
                int offset = length > 2 && random.Next(2) == 0 ? random.Next(1, 3) : 0;
                IArrowArray array = ArrowArrayFactory.Slice(whole, offset, length - offset);
                int[] rows = RandomRows(random, array.Length);
                using IArrowArray taken = ArrowTake.Take(array, rows);
                string[] expected = [.. rows.Select(r => r == Null ? "null" : Render(array, r))];
                Assert.Equal(expected, Enumerable.Range(0, taken.Length).Select(i => Render(taken, i)));
                // The children of null rows are null too.
                var s = (StructArray)taken;
                for (int j = 0; j < rows.Length; j++)
                {
                    if (rows[j] != Null) continue;
                    Assert.All(s.Fields, f => Assert.True(f.IsNull(j)));
                    // ... and so are the elements of a fixed-size list (the only layout that keeps elements for null rows).
                    var pairs = (FixedSizeListArray)s.Fields[3];
                    Assert.True(pairs.Values.IsNull(2 * (pairs.Offset + j)) && pairs.Values.IsNull(2 * (pairs.Offset + j) + 1), $"elements of null row {j}");
                }
            }
        }
    }

    [Fact]
    public void NullRows_FromAnEmptyArray_AreNullsOfItsType()
    {
        using IArrowArray empty = Nested(new Random(4), 0);
        using IArrowArray taken = ArrowTake.Take(empty, [Null, Null, Null]);
        Assert.Equal(3, taken.Length);
        Assert.Equal(3, taken.NullCount);
        var s = (StructArray)taken;
        Assert.All(s.Fields, f => Assert.Equal(3, f.NullCount));
        Assert.Equal(6, ((FixedSizeListArray)s.Fields[3]).Values.Length);
        Assert.Equal(0, ((LargeListArray)s.Fields[4]).Values.Length);
        foreach (ColumnKind kind in (ColumnKind[])[.. Kinds, .. LargeKinds])
        {
            IArrowArray none = kind.Type is LargeStringType or LargeBinaryType
                ? ToLarge(BuildNative(Kinds.Single(k => k.Name == "utf8"), []), kind.Type)
                : BuildNative(kind, []);
            using IArrowArray nulls = ArrowTake.Take(none, [Null, Null]);
            Assert.Equal(2, nulls.NullCount);
            Assert.Null(ReadAny(kind, nulls, 1));
        }
        using IArrowArray nullType = ArrowTake.Take(new NullArray(0), [Null, Null]);
        Assert.IsType<NullArray>(nullType);
        Assert.Equal(2, nullType.Length);
    }

    [Fact]
    public void RecordBatches_TakeEveryColumn_SerialOrSideBySide()
    {
        var random = new Random(1120);
        ColumnKind[] kinds = [.. Kinds.Where(k => k.Name is "i64" or "f64" or "bool" or "view" or "utf8" or "dec" or "fsb")];
        object?[][] values = [.. kinds.Select(k => Values(random, k, 3000, 0.2))];
        IArrowArray[] columns = [.. kinds.Select((k, c) => BuildNative(k, values[c])), Nested(random, 3000), new NullArray(3000)];
        var schema = new Schema([.. kinds.Select(k => new Field(k.Name, k.Type, true)), new Field("n", NestedType, true), new Field("z", NullType.Default, true)], null);
        using var batch = new RecordBatch(schema, columns, 3000);
        int[] rows = [.. Enumerable.Range(0, 5000).Select(_ => random.Next(8) == 0 ? Null : random.Next(3000))];
        foreach (int parallel in (int[])[1, 4])
        {
            using RecordBatch taken = ArrowTake.Take(batch, rows, maxDegreeOfParallelism: parallel);
            Assert.Equal(rows.Length, taken.Length);
            Assert.Same(schema, taken.Schema);
            for (int c = 0; c < kinds.Length; c++)
                AssertTaken(taken.Column(c), rows, i => values[c][i], j => Read(kinds[c], taken.Column(c), j), $"parallel {parallel} column {kinds[c]}");
            Assert.Equal(rows.Select(r => r == Null ? "null" : Render(columns[^2], r)), Enumerable.Range(0, rows.Length).Select(j => Render(taken.Column(kinds.Length), j)));
            Assert.Equal(rows.Length, taken.Column(kinds.Length + 1).NullCount);
        }
        Assert.Throws<ArgumentOutOfRangeException>(() => ArrowTake.Take(batch, [3000]));
        Assert.Throws<ArgumentOutOfRangeException>(() => ArrowTake.Take(batch, [0], maxDegreeOfParallelism: 0));
    }

    private static unsafe nint Address(ArrowBuffer buffer) =>
        (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(buffer.Span));

    private static NativeMemoryOwner Owner(ArrowBuffer buffer) =>
        MemoryMarshal.TryGetMemoryManager(buffer.Memory, out NativeBufferMemory? manager) ? manager.Owner : throw new InvalidOperationException("Not native.");

    private static object?[] LongStrings(int n) =>
        [.. Enumerable.Range(0, n).Select(i => i % 5 == 4 ? null : (object)Encoding.UTF8.GetBytes(i % 2 == 0 ? $"s{i}" : $"a long value number {i}"))];

    [Fact]
    public void Views_ShareTheirDataForAPermutation_AndCopyForAFewRows()
    {
        ColumnKind kind = Kinds.Single(k => k.Name == "view");
        object?[] values = LongStrings(1000);
        IArrowArray source = BuildNative(kind, values);
        nint data = Address(source.Data.Buffers[2]);
        NativeMemoryOwner owner = Owner(source.Data.Buffers[2]);
        int[] permutation = [.. Enumerable.Range(0, 1000).OrderBy(i => (i * 7919) % 1000)];
        int[] few = [193, 1, 193, Null];

        IArrowArray shared = ArrowTake.Take(source, permutation);
        Assert.Equal(data, Address(shared.Data.Buffers[2]));
        Assert.Same(owner, Owner(shared.Data.Buffers[2]));
        using (IArrowArray copied = ArrowTake.Take(source, few))
        {
            Assert.NotEqual(data, Address(copied.Data.Buffers[2]));
            Assert.Equal(((BinaryViewArray)source).GetBytes(193).ToArray(), ((BinaryViewArray)copied).GetBytes(2).ToArray());
            Assert.True(copied.IsNull(3));
        }
        using (IArrowArray forced = ArrowTake.Take(source, few, shareViewData: true)) Assert.Equal(data, Address(forced.Data.Buffers[2]));
        using (IArrowArray forced = ArrowTake.Take(source, permutation, shareViewData: false)) Assert.NotEqual(data, Address(forced.Data.Buffers[2]));

        // The shared memory lives until the last array over it goes.
        source.Dispose();
        Assert.False(owner.IsFreed);
        for (int j = 0; j < permutation.Length; j++) Assert.True(SameValue(values[permutation[j]], Read(kind, shared, j)), $"row {j}");
        shared.Dispose();
        Assert.True(owner.IsFreed);
    }

    /// <summary>
    /// A view array over native memory whose long values spread over <paramref name="buffers"/> data buffers: value i
    /// (every fifth null) is "value i, a long one" in buffer i % buffers.
    /// </summary>
    internal static unsafe IArrowArray ManyBufferViews(int rows, int buffers)
    {
        var export = new NativeBatchExport();
        var strings = new byte[buffers][];
        var at = new int[buffers];
        for (int b = 0; b < buffers; b++) strings[b] = new byte[rows * 32];
        var views = (Int128*)export.Allocate(16L * rows);
        var validity = (byte*)export.Allocate((rows + 7) / 8);
        int nulls = 0;
        for (int i = 0; i < rows; i++)
        {
            var view = new Span<byte>(views + i, 16);
            if (i % 5 == 4)
            {
                nulls++;
                MemoryMarshal.Write(view, 99); // a null view's garbage, naming a buffer that may be left out
                MemoryMarshal.Write(view[8..], buffers - 1 - i % buffers);
                continue;
            }
            validity[i >> 3] |= (byte)(1 << (i & 7));
            byte[] value = Encoding.UTF8.GetBytes($"value {i}, a long one");
            int b = i % buffers;
            value.CopyTo(strings[b].AsSpan(at[b]));
            MemoryMarshal.Write(view, value.Length);
            value.AsSpan(0, 4).CopyTo(view[4..]);
            MemoryMarshal.Write(view[8..], b);
            MemoryMarshal.Write(view[12..], at[b]);
            at[b] += value.Length;
        }
        var pointers = new nint[2 + buffers];
        (pointers[0], pointers[1]) = ((nint)validity, (nint)views);
        for (int b = 0; b < buffers; b++)
        {
            var bytes = (byte*)export.Allocate(Math.Max(1, at[b]));
            strings[b].AsSpan(0, at[b]).CopyTo(new Span<byte>(bytes, at[b]));
            pointers[2 + b] = (nint)bytes;
        }
        return export.ImportArray(export.NewArray(rows, nulls, pointers, []), StringViewType.Default);
    }

    [Fact]
    public void SharedViews_KeepOnlyTheBuffersTheyName_Renumbered()
    {
        IArrowArray source = ManyBufferViews(500, 40);
        var strings = (StringViewArray)source;
        NativeMemoryOwner owner = Owner(source.Data.Buffers[2]);
        // Rows 3, 7, 46, 8 and nulls (row 4, a null row): buffers 3, 7, 6, 8, in their order 3, 6, 7, 8.
        int[] rows = [3, 7, 46, 4, 8, Null];
        var few = (StringViewArray)ArrowTake.Take(source, rows, shareViewData: true);
        Assert.Equal(2 + 4, few.Data.Buffers.Length);
        Assert.Equal(((int[])[3, 6, 7, 8]).Select(b => Address(source.Data.Buffers[2 + b])), few.Data.Buffers.Skip(2).Select(Address));
        for (int j = 0; j < rows.Length; j++)
        {
            if (rows[j] == Null || rows[j] % 5 == 4)
            {
                Assert.True(few.IsNull(j));
                Assert.True(few.Data.Buffers[1].Span.Slice(j * 16, 16).IndexOfAnyExcept((byte)0) < 0, $"null view {j} cleared");
            }
            else Assert.Equal(strings.GetString(rows[j]), few.GetString(j));
        }
        // A permutation names every buffer a valid value is in (buffers b % 5 == 4 hold null rows' garbage only).
        int[] all = [.. Enumerable.Range(0, 500).Reverse()];
        var every = (StringViewArray)ArrowTake.Take(source, all);
        Assert.Equal(Enumerable.Range(0, 40).Where(b => b % 5 != 4).Select(b => Address(source.Data.Buffers[2 + b])), every.Data.Buffers.Skip(2).Select(Address));
        // The memory lives until the last array over it goes.
        source.Dispose();
        Assert.False(owner.IsFreed);
        Assert.Equal("value 46, a long one", few.GetString(2));
        Assert.Equal("value 498, a long one", every.GetString(1));
        few.Dispose();
        Assert.False(owner.IsFreed);
        every.Dispose();
        Assert.True(owner.IsFreed);
    }

    [Fact]
    public void Dictionaries_AreSharedFromNativeMemory()
    {
        using var builder = new NativeDictionaryBuilder(new DictionaryType(Int32Type.Default, StringType.Default, false));
        string[] entries = ["x", "yy", "zzz"];
        foreach (string s in entries) builder.Entries.Append(Encoding.UTF8.GetBytes(s));
        for (int i = 0; i < 100; i++)
        {
            if (i % 7 == 0) builder.AppendNull();
            else builder.Append(i % 3);
        }
        IArrowArray source = builder.ExportArray();
        NativeMemoryOwner owner = Owner(source.Data.Dictionary!.Buffers[2]);
        int[] rows = [99, 7, 8, Null, 8, 50];
        var taken = (DictionaryArray)ArrowTake.Take(source, rows);
        Assert.Equal(Address(source.Data.Dictionary.Buffers[2]), Address(taken.Data.Dictionary!.Buffers[2]));
        source.Dispose();
        Assert.False(owner.IsFreed);
        var strings = (StringArray)taken.Dictionary;
        for (int j = 0; j < rows.Length; j++)
        {
            int i = rows[j];
            if (i == Null || i % 7 == 0) Assert.True(taken.IsNull(j));
            else Assert.Equal(entries[i % 3], strings.GetString(((Int32Array)taken.Indices).GetValue(j)!.Value));
        }
        taken.Dispose();
        Assert.True(owner.IsFreed);
    }

    /// <summary>A row of any array as text (nested values included).</summary>
    internal static string Render(IArrowArray array, int i)
    {
        if (array.IsNull(i)) return "null";
        return array switch
        {
            StructArray s => "{" + string.Join(",", s.Fields.Select(f => Render(f, i))) + "}",
            MapArray m => "{" + string.Join(",", Enumerable.Range(m.ValueOffsets[i], m.GetValueLength(i)).Select(j => $"{Render(m.Keys, j)}:{Render(m.Values, j)}")) + "}",
            ListArray l => "[" + string.Join(",", Enumerable.Range(l.ValueOffsets[i], l.GetValueLength(i)).Select(j => Render(l.Values, j))) + "]",
            LargeListArray l => "[" + string.Join(",", Enumerable.Range((int)l.ValueOffsets[i], (int)l.GetValueLength(i)).Select(j => Render(l.Values, j))) + "]",
            FixedSizeListArray f => "[" + Render(f.Values, 2 * (f.Offset + i)) + "," + Render(f.Values, 2 * (f.Offset + i) + 1) + "]",
            DictionaryArray d => Render(d.Dictionary, ((Int32Array)d.Indices).GetValue(i)!.Value),
            StringArray s => s.GetString(i),
            StringViewArray s => s.GetString(i),
            Int16Array x => x.GetValue(i).ToString()!,
            Int32Array x => x.GetValue(i).ToString()!,
            Int64Array x => x.GetValue(i).ToString()!,
            _ => throw new NotSupportedException(array.GetType().Name),
        };
    }
}
