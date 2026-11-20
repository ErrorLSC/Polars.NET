using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Arrow.Compute;
using static Sazanami.Tests.FilterTestData;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="ArrowBatchAppender"/>: ranges of many batches (sliced, with nulls, of every layout the appender takes)
/// appended and taken as batches holding their rows in order; inputs disposed right after appending; rows of another
/// dictionary refused until the batch is taken; long view values copied, null views pointing nowhere.
/// </summary>
public class ArrowBatchAppenderTests
{
    private static readonly ColumnKind[] LargeKinds =
    [
        new("lutf8", LargeStringType.Default, Kinds.Single(k => k.Name == "utf8").Pool),
        new("lbinary", LargeBinaryType.Default, Kinds.Single(k => k.Name == "binary").Pool),
    ];

    private static RecordBatch Batch(IArrowArray array) => new(new Schema([new Field("c", array.Data.DataType, true)], null), [array], array.Length);

    /// <summary>A random array of a kind, maybe sliced (the slice is what counts; the whole is returned to dispose).</summary>
    private static (IArrowArray Array, IArrowArray Whole) Source(Random random, ColumnKind kind, bool sameDictionary, IArrowArray? dictionary)
    {
        bool large = kind.Type is LargeStringType or LargeBinaryType;
        ColumnKind built = large ? Kinds.Single(k => k.Name == (kind.Type is LargeStringType ? "utf8" : "binary")) : kind;
        int length = random.Next(4) switch { 0 => 0, 1 => random.Next(1, 5), 2 => random.Next(60, 70), _ => random.Next(100, 400) };
        int offset = random.Next(3) == 0 ? random.Next(1, 40) : 0;
        object?[] values = Values(random, built, length + offset, random.Next(3) switch { 0 => 0, 1 => 0.1, _ => 0.6 });
        IArrowArray whole = random.Next(3) switch
        {
            0 when dictionary is not null => Rekeyed(random, values, dictionary, sameDictionary),
            0 or 1 => BuildNative(built, values),
            _ => Build(random, built, values),
        };
        if (large) whole = ArrowTakeTests.ToLarge(whole, kind.Type);
        return (offset == 0 ? whole : ArrowArrayFactory.Slice(whole, offset, length), whole);
    }

    /// <summary>A dictionary array over <paramref name="dictionary"/> (or a copy of it: equal values, other memory).</summary>
    private static IArrowArray Rekeyed(Random random, object?[] values, IArrowArray dictionary, bool same)
    {
        var indices = new Int32Array.Builder();
        foreach (object? value in values)
        {
            if (value is null) indices.AppendNull();
            else indices.Append(random.Next(dictionary.Length));
        }
        // The same memory (a reference of its own: disposing the array lets only that go), or a copy of the values.
        IArrowArray entries = same ? ArrowArrayFactory.BuildArray(dictionary.Data.Retain()) : ArrowTake.Take(dictionary, [.. Enumerable.Range(0, dictionary.Length)]);
        return new DictionaryArray(new DictionaryType(Int32Type.Default, dictionary.Data.DataType, false), indices.Build(), entries);
    }

    [Fact]
    public void RangesOfManyBatches_ComeBackInOrder_InEveryFlatLayout()
    {
        var random = new Random(1121);
        ColumnKind[] kinds = [.. Kinds, .. LargeKinds];
        for (int round = 0; round < 600; round++)
        {
            ColumnKind kind = kinds[random.Next(kinds.Length)];
            IArrowType type = kind.Type;
            using var appender = new ArrowBatchAppender(new Schema([new Field("c", type, true)], null));
            List<string> expected = [];
            int pieces = random.Next(1, 12);
            for (int p = 0; p < pieces; p++)
            {
                var (array, whole) = Source(random, kind, false, null);
                int start = array.Length == 0 ? 0 : random.Next(array.Length);
                int count = random.Next(array.Length - start + 1);
                Assert.True(appender.TryAppend(Batch(array), start, count));
                expected.AddRange(Enumerable.Range(start, count).Select(i => Show(ReadAny(kind, array, i))));
                whole.Dispose(); // the appender holds copies
                if (random.Next(5) == 0) Check(appender, expected, $"round {round} {kind} piece {p}", c => Show(Read(kind, c, 0)), kind);
            }
            Check(appender, expected, $"round {round} {kind}", null, kind);
        }
    }

    private static object? ReadAny(ColumnKind kind, IArrowArray array, int i) => array switch
    {
        LargeBinaryArray large => large.IsNull(i) ? null : large.GetBytes(i).ToArray(),
        _ => Read(kind, array, i),
    };

    /// <summary>A value as text (bytes in hex, floats by their bits).</summary>
    private static string Show(object? value) => value switch
    {
        null => "null",
        byte[] bytes => Convert.ToHexString(bytes),
        double d => BitConverter.DoubleToInt64Bits(d).ToString(System.Globalization.CultureInfo.InvariantCulture),
        float f => BitConverter.SingleToInt32Bits(f).ToString(System.Globalization.CultureInfo.InvariantCulture),
        _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)!,
    };

    /// <summary>Takes the batch and compares it with the rows expected (cleared after), rendered by the kind's reader or <see cref="ArrowTakeTests.Render"/>.</summary>
    private static void Check(ArrowBatchAppender appender, List<string> expected, string where, Func<IArrowArray, string>? unused = null, ColumnKind? kind = null)
    {
        Assert.Equal(expected.Count, appender.Rows);
        using RecordBatch taken = appender.Take();
        Assert.Equal(0, appender.Rows);
        IArrowArray column = taken.Column(0);
        Assert.Equal(expected.Count, taken.Length);
        Assert.Equal(Enumerable.Range(0, column.Length).Count(column.IsNull), column.NullCount);
        if (column.NullCount == 0 && column is not NullArray) Assert.True(column.Data.Buffers[0].IsEmpty, $"{where}: a validity buffer without nulls");
        Assert.True(expected.SequenceEqual(Enumerable.Range(0, column.Length).Select(i => kind is null ? ArrowTakeTests.Render(column, i) : Show(ReadAny(kind, column, i)))), where);
        expected.Clear();
    }

    [Fact]
    public void NestedRanges_ComeBackInOrder_WithTheirElements()
    {
        var random = new Random(1122);
        for (int round = 0; round < 120; round++)
        {
            IArrowArray first = ArrowTakeTests.Nested(random, 1);
            using var appender = new ArrowBatchAppender(new Schema([new Field("s", first.Data.DataType, true)], null));
            first.Dispose();
            List<string> expected = [];
            for (int p = 0, pieces = random.Next(1, 8); p < pieces; p++)
            {
                int length = random.Next(4) switch { 0 => 0, 1 => random.Next(1, 4), _ => random.Next(30, 140) };
                IArrowArray whole = ArrowTakeTests.Nested(random, length);
                int offset = length > 2 && random.Next(2) == 0 ? random.Next(1, 3) : 0;
                IArrowArray array = ArrowArrayFactory.Slice(whole, offset, length - offset);
                int start = array.Length == 0 ? 0 : random.Next(array.Length);
                int count = random.Next(array.Length - start + 1);
                // The nested dictionary is the same values each time (built anew): it joins.
                Assert.True(appender.TryAppend(Batch(array), start, count), $"round {round} piece {p}");
                expected.AddRange(Enumerable.Range(start, count).Select(i => ArrowTakeTests.Render(array, i)));
                whole.Dispose();
            }
            Check(appender, expected, $"round {round}");
        }
    }

    [Fact]
    public void RowsOfAnotherDictionary_AreRefused_UntilTheBatchIsTaken()
    {
        var random = new Random(1123);
        ColumnKind utf8 = Kinds.Single(k => k.Name == "utf8");
        IArrowArray dictionary = BuildNative(utf8, ["a"u8.ToArray(), "bb"u8.ToArray(), "a much longer value than twelve bytes"u8.ToArray()]);
        IArrowArray other = BuildNative(utf8, ["a"u8.ToArray(), "bb"u8.ToArray(), "something else"u8.ToArray()]);
        var type = new DictionaryType(Int32Type.Default, StringType.Default, false);
        using var appender = new ArrowBatchAppender(new Schema([new Field("d", type, true)], null));
        List<string> expected = [];
        for (int p = 0; p < 20; p++)
        {
            // The same dictionary, or equal values elsewhere in memory: both join.
            var (array, whole) = Source(random, utf8, sameDictionary: random.Next(2) == 0, dictionary);
            if (array is not DictionaryArray)
            {
                whole.Dispose();
                continue;
            }
            Assert.True(appender.TryAppend(Batch(array), 0, array.Length));
            expected.AddRange(Enumerable.Range(0, array.Length).Select(i => ArrowTakeTests.Render(array, i)));
            whole.Dispose();
        }
        using IArrowArray foreign = Rekeyed(random, ["x", null, "y"], other, same: true);
        RecordBatch batch = Batch(foreign);
        {
            int before = appender.Rows;
            Assert.False(appender.TryAppend(batch, 0, 3));
            Assert.Equal(before, appender.Rows); // nothing appended
            Check(appender, expected, "first dictionary");
            Assert.True(appender.TryAppend(batch, 0, 3));
            expected.AddRange(Enumerable.Range(0, 3).Select(i => ArrowTakeTests.Render(foreign, i)));
        }
        Check(appender, expected, "second dictionary");
        dictionary.Dispose();
        other.Dispose();
    }

    [Fact]
    public void LongViewValues_AreCopied_AndNullViewsPointNowhere()
    {
        using IArrowArray views = ArrowTakeTests.ManyBufferViews(300, 5);
        var schema = new Schema([new Field("v", views.Data.DataType, true)], null);
        using var appender = new ArrowBatchAppender(schema);
        var batch = new RecordBatch(schema, [views], views.Length);
        Assert.True(appender.TryAppend(batch, 10, 100));
        Assert.True(appender.TryAppend(batch, 250, 50));
        using RecordBatch taken = appender.Take();
        IArrowArray column = taken.Column(0);
        Assert.Equal([.. Enumerable.Range(10, 100), .. Enumerable.Range(250, 50)], Enumerable.Range(0, 150).Select(i =>
            ArrowTakeTests.Render(column, i) == ArrowTakeTests.Render(views, i < 100 ? 10 + i : 250 + i - 100) ? (i < 100 ? 10 + i : 250 + i - 100) : -1));
        Assert.True(column.Data.Buffers.Length <= 3, "one data buffer of the appender's");
        for (int i = 0; i < column.Length; i++)
        {
            if (column.IsNull(i)) Assert.True(column.Data.Buffers[1].Span.Slice(i * 16, 16).IndexOfAnyExcept((byte)0) < 0, $"null view {i}");
        }
    }

    [Fact]
    public void BatchesOfSeveralColumns_AreTakenWhole_AndTheAppenderStartsAgain()
    {
        var random = new Random(1124);
        ColumnKind[] kinds = [Kinds.Single(k => k.Name == "i64"), Kinds.Single(k => k.Name == "utf8"), Kinds.Single(k => k.Name == "bool")];
        var schema = new Schema([.. kinds.Select((k, i) => new Field($"c{i}", k.Type, true))], null);
        using var appender = new ArrowBatchAppender(schema);
        for (int round = 0; round < 3; round++)
        {
            List<string> expected = [];
            for (int p = 0; p < 5; p++)
            {
                int length = random.Next(1, 200);
                IArrowArray[] columns = [.. kinds.Select(k => BuildNative(k, Values(random, k, length, 0.2)))];
                using var batch = new RecordBatch(schema, columns, length);
                int start = random.Next(length), count = random.Next(length - start + 1);
                Assert.True(appender.TryAppend(batch, start, count));
                expected.AddRange(Enumerable.Range(start, count).Select(r => string.Join("|", columns.Select((c, k) => Show(Read(kinds[k], c, r))))));
            }
            using RecordBatch taken = appender.Take();
            Assert.Equal(expected, Enumerable.Range(0, taken.Length).Select(r => string.Join("|", Enumerable.Range(0, 3).Select(c => Show(Read(kinds[c], taken.Column(c), r))))));
        }
        Assert.True(appender.Bytes == 0);
        using RecordBatch empty = appender.Take();
        Assert.Equal(0, empty.Length);
    }

    /// <summary>A fixed-size list sliced itself (its own offset, not a parent's): the elements of the rows appended.</summary>
    [Fact]
    public void SlicedFixedSizeLists_AppendTheirRowsElements()
    {
        var values = new Int16Array.Builder();
        for (short i = 0; i < 20; i++) values.Append(i);
        var type = new FixedSizeListType(new Field("item", Int16Type.Default, true), 2);
        var whole = new FixedSizeListArray(type, 10, values.Build(), ArrowBuffer.Empty, 0);
        IArrowArray sliced = ArrowArrayFactory.Slice(whole, 3, 5);
        using var appender = new ArrowBatchAppender(new Schema([new Field("f", type, true)], null));
        Assert.True(appender.TryAppend(Batch(sliced), 1, 3));
        using RecordBatch taken = appender.Take();
        Assert.Equal(Enumerable.Range(1, 3).Select(i => ArrowTakeTests.Render(sliced, i)), Enumerable.Range(0, 3).Select(i => ArrowTakeTests.Render(taken.Column(0), i)));
        whole.Dispose();
    }

    /// <summary>A null column (no buffers at all) beside another: its rows counted.</summary>
    [Fact]
    public void NullColumns_CountTheirRows()
    {
        var schema = new Schema([new Field("n", NullType.Default, true), new Field("i", Int32Type.Default, true)], null);
        using var appender = new ArrowBatchAppender(schema);
        for (int p = 0; p < 3; p++)
        {
            var batch = new RecordBatch(schema, [new NullArray(10), new Int32Array.Builder().AppendRange(Enumerable.Range(p * 10, 10)).Build()], 10);
            Assert.True(appender.TryAppend(batch, 2, 5));
        }
        using RecordBatch taken = appender.Take();
        Assert.Equal(15, taken.Length);
        Assert.Equal(15, taken.Column(0).NullCount);
        Assert.Equal([2, 3, 4, 5, 6, 12, 13, 14, 15, 16, 22, 23, 24, 25, 26], ((Int32Array)taken.Column(1)).Values.ToArray());
    }

    [Fact]
    public void TypesWithoutALayoutHere_AreNotTaken()
    {
        var union = new UnionType([new Field("a", Int32Type.Default, true)], [0], UnionMode.Dense);
        Assert.False(ArrowBatchAppender.Supports(new Schema([new Field("u", union, true)], null)));
        var ree = new RunEndEncodedType(Int32Type.Default, Int64Type.Default);
        Assert.False(ArrowBatchAppender.Supports(new Schema([new Field("r", new ListType(new Field("item", ree, true)), true)], null)));
        Assert.Throws<NotSupportedException>(() => new ArrowBatchAppender(new Schema([new Field("u", union, true)], null)));
        Assert.True(ArrowBatchAppender.Supports(new Schema([new Field("n", NullType.Default, true), new Field("m", new MapType(StringType.Default, Int64Type.Default), true)], null)));
    }
}
