using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Filters;
using static Sazanami.Tests.FilterTestData;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="ArrowSizes"/>: a slice's logical bytes are those of a compact copy of it (every flat kind, dictionaries,
/// structs, lists, fixed-size lists, at random offsets), whatever else shares its buffers; the layouts without a copy
/// here (large, list views, run-end encoded, unions) against bytes counted by hand; retained bytes count each buffer
/// once.
/// </summary>
public class ArrowSizesTests
{
    private static ulong[] All(int length)
    {
        var mask = new ulong[Masks.Words(length) + 1];
        Masks.Fill(mask, length, true);
        return mask;
    }

    /// <summary>The logical bytes of a slice against those of its compact copy.</summary>
    private static void SameAsCopy(IArrowArray whole, int offset, int length, string where)
    {
        IArrowArray slice = ArrowArrayFactory.Slice(whole, offset, length);
        using IArrowArray copy = ArrowFilter.Filter(slice, All(length), shareViewData: false);
        Assert.True(ArrowSizes.LogicalBytes(copy, 0, length) == ArrowSizes.LogicalBytes(slice, 0, length), where);
        Assert.Equal(ArrowSizes.LogicalBytes(whole, offset, length), ArrowSizes.LogicalBytes(slice, 0, length));
    }

    [Fact]
    public void FlatSlices_TakeTheBytesOfTheirCopies()
    {
        var random = new Random(77);
        for (int round = 0; round < 600; round++)
        {
            ColumnKind kind = Kinds[random.Next(Kinds.Length)];
            int length = random.Next(1, 3000);
            double nulls = random.Next(3) switch { 0 => 0, 1 => 0.01, _ => 0.4 };
            object?[] values = Values(random, kind, length, nulls);
            using IArrowArray whole = random.Next(4) == 0 && kind.Dictionaries ? BuildDictionary(random, kind, values) : BuildNative(kind, values);
            int offset = random.Next(length), count = random.Next(length - offset + 1);
            SameAsCopy(whole, offset, count, $"round {round}: {kind} [{offset}, +{count}) of {length}");
        }
    }

    [Fact]
    public void FixedWidthValues_CountTheirBitsAndTheValidityOnlyWithANull()
    {
        var ints = new Int32Array.Builder().AppendRange([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).Build();
        Assert.Equal(16, ArrowSizes.LogicalBytes(ints, 3, 4));
        var withNull = new Int64Array.Builder().Append(1).AppendNull().Append(3).Append(4).Build();
        Assert.Equal(1 + 16, ArrowSizes.LogicalBytes(withNull, 0, 2)); // the null among them
        Assert.Equal(16, ArrowSizes.LogicalBytes(withNull, 2, 2));     // no null: no bitmap
        var bools = new BooleanArray.Builder().AppendRange(Enumerable.Range(0, 20).Select(i => i % 3 == 0)).Build();
        Assert.Equal(2, ArrowSizes.LogicalBytes(bools, 1, 9));
        Assert.Equal(0, ArrowSizes.LogicalBytes(ints, 4, 0));
    }

    [Fact]
    public void StringsAndViews_CountTheValuesOfTheirRowsAlone()
    {
        string[] values = ["a", "bb", "a value of more than twelve bytes", "ccc", "another long value here", "d"];
        var utf8 = new StringArray.Builder().AppendRange(values).Build();
        // Rows 1..3: offsets 4 x 4, values 2 + 33 + 3.
        Assert.Equal(16 + 38, ArrowSizes.LogicalBytes(utf8, 1, 3));
        var views = new StringViewArray.Builder().AppendRange(values).Build();
        // Rows 1..4: views 4 x 16, the long values 33 + 23 outside them.
        Assert.Equal(64 + 56, ArrowSizes.LogicalBytes(views, 1, 4));
        var large = new LargeStringArray.Builder().AppendRange(values).Build();
        Assert.Equal(8 * 4 + 38, ArrowSizes.LogicalBytes(large, 1, 3));
    }

    [Fact]
    public void ViewsOfAnEngine_SharingBigDataBuffers_CountTheirRowsAlone()
    {
        // Many arrays' views point into one big data buffer (as an engine's string columns do): a slice of 3 rows is
        // its views and the long values they point to, not the buffer.
        string[] values = [.. Enumerable.Range(0, 10_000).Select(i => $"a long value of row number {i:D6}")];
        var views = new StringViewArray.Builder().AppendRange(values).Build();
        long one = 16 + values[0].Length;
        Assert.Equal(3 * one, ArrowSizes.LogicalBytes(views, 5000, 3));
        Assert.True(views.Data.Buffers.Sum(b => (long)b.Length) > 1000 * one);
        SameAsCopy(views, 5000, 3, "views");
    }

    [Fact]
    public void Nested_CountTheItemsAndChildrenOfTheirRows()
    {
        var random = new Random(78);
        // Lists of ints (some null, some empty), structs of a string and a long, fixed-size lists of 3 shorts.
        var lists = new ListArray.Builder(Int32Type.Default);
        var listValues = (Int32Array.Builder)lists.ValueBuilder;
        var names = new StringArray.Builder();
        var amounts = new Int64Array.Builder();
        var triples = new FixedSizeListArray.Builder(Int16Type.Default, 3);
        var tripleValues = (Int16Array.Builder)triples.ValueBuilder;
        const int Rows = 2000;
        for (int i = 0; i < Rows; i++)
        {
            if (random.Next(10) == 0) lists.AppendNull();
            else
            {
                lists.Append();
                for (int k = random.Next(5); k > 0; k--) listValues.Append(random.Next());
            }
            if (random.Next(7) == 0) names.AppendNull();
            else names.Append(new string('n', random.Next(30)));
            amounts.Append(random.NextInt64());
            if (random.Next(9) == 0) triples.AppendNull();
            else
            {
                triples.Append();
                for (int k = 0; k < 3; k++) tripleValues.Append((short)random.Next(short.MaxValue));
            }
        }
        IArrowArray name = names.Build(), amount = amounts.Build();
        var structType = new StructType([new Field("name", StringType.Default, true), new Field("amount", Int64Type.Default, false)]);
        var structs = new StructArray(structType, Rows, [name, amount], ArrowBuffer.Empty, 0);
        IArrowArray[] columns = [lists.Build(), structs, triples.Build()];
        for (int round = 0; round < 200; round++)
        {
            int offset = random.Next(Rows), count = random.Next(Rows - offset + 1);
            foreach (IArrowArray column in columns) SameAsCopy(column, offset, count, $"round {round}: {column.Data.DataType.Name} [{offset}, +{count})");
        }
        // By hand: list rows 0..1 of [[1, 2], [3]] are offsets 3 x 4 and items 3 x 4.
        var small = new ListArray.Builder(Int32Type.Default);
        var items = (Int32Array.Builder)small.ValueBuilder;
        small.Append();
        items.Append(1).Append(2);
        small.Append();
        items.Append(3);
        small.Append();
        items.Append(4);
        Assert.Equal(12 + 12, ArrowSizes.LogicalBytes(small.Build(), 0, 2));
    }

    private static ArrowBuffer Buffer<T>(params T[] values) where T : struct => new(MemoryMarshal.AsBytes<T>(values).ToArray());

    [Fact]
    public void LargeListsAndListViews_CountTheItemsTheirRowsHold()
    {
        var values = new Int64Array.Builder().AppendRange(Enumerable.Range(0, 10).Select(i => (long)i)).Build();
        // Large lists [0, 1], [2, 3, 4], [5], [6, 7, 8, 9]: rows 1..2 are offsets 3 x 8 and items 4 x 8.
        var large = new LargeListArray(new LargeListType(new Field("item", Int64Type.Default, false)), 4, Buffer<long>(0, 2, 5, 6, 10), values, ArrowBuffer.Empty, 0, 0);
        Assert.Equal(24 + 32, ArrowSizes.LogicalBytes(large, 1, 2));
        // List views (offset, size): (6, 4), (0, 2), (2, 3), (9, 0): rows 0..2 are offsets and sizes 3 x 8, items 4 + 2 + 3.
        var view = new ListViewArray(new ListViewType(new Field("item", Int64Type.Default, false)), 4, Buffer(6, 0, 2, 9), Buffer(4, 2, 3, 0), values, ArrowBuffer.Empty, 0, 0);
        Assert.Equal(24 + 9 * 8, ArrowSizes.LogicalBytes(view, 0, 3));
        Assert.Equal(8, ArrowSizes.LogicalBytes(view, 3, 1)); // an empty one: its offset and size
    }

    [Fact]
    public void RunEndEncoded_CountTheRunsCoveringTheirRows()
    {
        // Runs ending at 2, 5, 9 of 10, 20, 30: rows 3..5 lie in the runs ending at 5 and 9.
        var ends = new Int32Array.Builder().AppendRange([2, 5, 9]).Build();
        var values = new Int64Array.Builder().AppendRange([10, 20, 30]).Build();
        var runs = new RunEndEncodedArray(ends, values);
        Assert.Equal(2 * 4 + 2 * 8, ArrowSizes.LogicalBytes(runs, 3, 3));
        Assert.Equal(4 + 8, ArrowSizes.LogicalBytes(runs, 0, 2));
        Assert.Equal(3 * 4 + 3 * 8, ArrowSizes.LogicalBytes(runs, 0, 9));
    }

    [Fact]
    public void Unions_CountTheirTypeIdsAndTheChildValuesTheyPointTo()
    {
        var ints = new Int32Array.Builder().AppendRange([1, 2, 3]).Build();
        var strings = new StringArray.Builder().AppendRange(["x", "yy"]).Build();
        Field[] fields = [new("a", Int32Type.Default, true), new("b", StringType.Default, true)];
        // Dense, rows a0 b0 a1 b1 a2: rows 1..3 are type ids and offsets 3 x 5, ints [1, 1] (4), strings [0, 1] (offsets 12, values 3).
        var dense = new DenseUnionArray(new UnionType(fields, [0, 1], UnionMode.Dense), 5, [ints, strings],
            Buffer<byte>(0, 1, 0, 1, 0), Buffer(0, 0, 1, 1, 2), 0, 0);
        Assert.Equal(15 + 4 + 12 + 3, ArrowSizes.LogicalBytes(dense, 1, 3));
        // Sparse, every child as long as the union: rows 0..1 are type ids 2, ints 2 x 4, strings offsets 12 and values 3.
        var sparseInts = new Int32Array.Builder().AppendRange([1, 2, 3]).Build();
        var sparseStrings = new StringArray.Builder().AppendRange(["x", "yy", "z"]).Build();
        var sparse = new SparseUnionArray(new UnionType(fields, [0, 1], UnionMode.Sparse), 3, [sparseInts, sparseStrings], Buffer<byte>(0, 1, 0), 0, 0);
        Assert.Equal(2 + 8 + 12 + 3, ArrowSizes.LogicalBytes(sparse, 0, 2));
    }

    [Fact]
    public void RetainedBytes_CountEachBufferOnce_HoweverManySlicesHoldIt()
    {
        string[] values = [.. Enumerable.Range(0, 1000).Select(i => $"value number {i:D5} of the column")];
        var schema = new Schema([new Field("s", StringViewType.Default, false), new Field("n", Int64Type.Default, false)], null);
        var batch = new RecordBatch(schema,
            [new StringViewArray.Builder().AppendRange(values).Build(), new Int64Array.Builder().AppendRange(Enumerable.Range(0, 1000).Select(i => (long)i)).Build()], 1000);
        long whole = batch.Column(0).Data.Buffers.Concat(batch.Column(1).Data.Buffers).Sum(b => (long)b.Length);
        Assert.Equal(whole, ArrowSizes.RetainedBytes(batch));
        // Slices keep the whole buffers: counted once with their batch, and alone as much.
        Assert.Equal(whole, ArrowSizes.RetainedBytes([batch, batch.Slice(10, 5), batch.Slice(500, 400)]));
        Assert.Equal(whole, ArrowSizes.RetainedBytes([batch.Slice(10, 5)]));
        // Another batch of its own buffers adds its own.
        var other = new RecordBatch(schema,
            [new StringViewArray.Builder().AppendRange(values).Build(), new Int64Array.Builder().AppendRange(Enumerable.Range(0, 1000).Select(i => (long)i)).Build()], 1000);
        Assert.Equal(2 * whole, ArrowSizes.RetainedBytes([batch, other, other.Slice(1, 1)]));
        // The logical bytes of a slice stay its own rows'.
        Assert.Equal(5 * (16 + values[0].Length + 8), ArrowSizes.LogicalBytes(batch.Slice(10, 5)));
    }
}
