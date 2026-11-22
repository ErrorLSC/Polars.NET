using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Compute;
using static Sazanami.Tests.FilterTestData;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="ArrowConcat"/>: batches of every flat layout (sliced, with nulls, empty, built natively or not) and of
/// every nested one concatenated into one batch holding their rows in order, on one thread and several; view data
/// shared and kept alive after the inputs go; other schemas and dictionaries refused.
/// </summary>
public class ArrowConcatTests
{
    private static readonly ColumnKind[] LargeKinds =
    [
        new("lutf8", LargeStringType.Default, Kinds.Single(k => k.Name == "utf8").Pool),
        new("lbinary", LargeBinaryType.Default, Kinds.Single(k => k.Name == "binary").Pool),
    ];

    private static RecordBatch Batch(IArrowArray array) => new(new Schema([new Field("c", array.Data.DataType, true)], null), [array], array.Length);

    private static string Show(object? value) => value switch
    {
        null => "null",
        byte[] bytes => Convert.ToHexString(bytes),
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString()!,
    };

    /// <summary>A random array of a kind, maybe sliced (the slice is what counts; the whole is returned to dispose).</summary>
    private static (IArrowArray Array, IArrowArray Whole) Source(Random random, ColumnKind kind)
    {
        bool large = kind.Type is LargeStringType or LargeBinaryType;
        ColumnKind built = large ? Kinds.Single(k => k.Name == (kind.Type is LargeStringType ? "utf8" : "binary")) : kind;
        int length = random.Next(4) switch { 0 => 0, 1 => random.Next(1, 5), 2 => random.Next(60, 70), _ => random.Next(100, 400) };
        int offset = random.Next(3) == 0 ? random.Next(1, 40) : 0;
        object?[] values = Values(random, built, length + offset, random.Next(3) switch { 0 => 0, 1 => 0.1, _ => 0.6 });
        IArrowArray whole = random.Next(2) == 0 ? BuildNative(built, values) : Build(random, built, values);
        if (large) whole = ArrowTakeTests.ToLarge(whole, kind.Type);
        return (offset == 0 ? whole : ArrowArrayFactory.Slice(whole, offset, length), whole);
    }

    private static string ReadAny(ColumnKind kind, IArrowArray array, int i) => Show(array switch
    {
        LargeStringArray s => s.IsNull(i) ? null : s.GetBytes(i).ToArray(),
        LargeBinaryArray b => b.IsNull(i) ? null : b.GetBytes(i).ToArray(),
        _ => Read(kind, array, i),
    });

    [Fact]
    public void BatchesOfEveryFlatLayout_ComeBackInOrder()
    {
        var random = new Random(1122);
        ColumnKind[] kinds = [.. Kinds, .. LargeKinds];
        for (int round = 0; round < 400; round++)
        {
            ColumnKind kind = kinds[random.Next(kinds.Length)];
            List<(IArrowArray Array, IArrowArray Whole)> parts = [.. Enumerable.Range(0, random.Next(1, 12)).Select(_ => Source(random, kind))];
            List<string> expected = [.. parts.SelectMany(p => Enumerable.Range(0, p.Array.Length).Select(i => ReadAny(kind, p.Array, i)))];
            int threads = random.Next(2) == 0 ? 1 : 4;
            using RecordBatch joined = ArrowConcat.Concatenate([.. parts.Select(p => Batch(p.Array))], threads);
            // The inputs may go at once: shared view data stays with the output.
            foreach (var (_, whole) in parts) whole.Dispose();
            IArrowArray column = joined.Column(0);
            Assert.Equal(expected.Count, joined.Length);
            Assert.Equal(expected.Count(v => v == "null"), column.NullCount);
            Assert.Equal(expected, Enumerable.Range(0, column.Length).Select(i => ReadAny(kind, column, i)));
        }
    }

    /// <summary>The nested layouts of <see cref="ArrowTakeTests.Nested"/> but its dictionary.</summary>
    private static IArrowArray NestedWithoutDictionary(Random random, int length)
    {
        var whole = (StructArray)ArrowTakeTests.Nested(random, length);
        var type = new StructType([.. ((StructType)whole.Data.DataType).Fields.Take(5)]);
        return new StructArray(type, whole.Length, [.. whole.Fields.Take(5)], whole.NullBitmapBuffer, whole.NullCount);
    }

    [Fact]
    public void NestedBatches_ComeBackInOrder_TheirElementsToo()
    {
        var random = new Random(1123);
        for (int round = 0; round < 60; round++)
        {
            List<IArrowArray> parts = [];
            foreach (int _ in Enumerable.Range(0, random.Next(1, 8)))
            {
                int length = random.Next(4) switch { 0 => 0, 1 => random.Next(1, 5), _ => random.Next(60, 200) };
                IArrowArray whole = NestedWithoutDictionary(random, length);
                int offset = length > 2 && random.Next(2) == 0 ? random.Next(1, 3) : 0;
                parts.Add(ArrowArrayFactory.Slice(whole, offset, length - offset));
            }
            List<string> expected = [.. parts.SelectMany(p => Enumerable.Range(0, p.Length).Select(i => ArrowTakeTests.Render(p, i)))];
            foreach (int threads in (int[])[1, 3])
            {
                using RecordBatch joined = ArrowConcat.Concatenate([.. parts.Select(Batch)], threads);
                Assert.Equal(expected, Enumerable.Range(0, joined.Length).Select(i => ArrowTakeTests.Render(joined.Column(0), i)));
            }
        }
    }

    [Fact]
    public void ManyColumns_AndOneBatch_AreConcatenatedToo()
    {
        var random = new Random(1124);
        ColumnKind[] kinds = [.. Kinds.Where(k => k.Name is "i64" or "f64" or "view" or "utf8" or "bool")];
        List<RecordBatch> batches = [];
        List<IArrowArray> wholes = [];
        for (int b = 0; b < 6; b++)
        {
            List<IArrowArray> columns = [];
            int length = random.Next(50, 300);
            foreach (ColumnKind kind in kinds)
            {
                IArrowArray array = BuildNative(kind, Values(random, kind, length, 0.2));
                columns.Add(array);
                wholes.Add(array);
            }
            batches.Add(new RecordBatch(new Schema([.. kinds.Select(k => new Field(k.Name, k.Type, true))], null), columns, length));
        }
        using RecordBatch joined = ArrowConcat.Concatenate(batches, 8);
        for (int c = 0; c < kinds.Length; c++)
        {
            List<string> expected = [.. batches.SelectMany(b => Enumerable.Range(0, b.Length).Select(i => ReadAny(kinds[c], b.Column(c), i)))];
            Assert.Equal(expected, Enumerable.Range(0, joined.Length).Select(i => ReadAny(kinds[c], joined.Column(c), i)));
        }
        using RecordBatch one = ArrowConcat.Concatenate([batches[0]], 4);
        Assert.Equal(batches[0].Length, one.Length);
        foreach (IArrowArray whole in wholes) whole.Dispose();
    }

    [Fact]
    public void OtherSchemas_AndDictionaries_AreRefused()
    {
        var ints = Batch(new Int32Array.Builder().Append(1).Build());
        var longs = Batch(new Int64Array.Builder().Append(1).Build());
        Assert.Throws<ArgumentException>(() => ArrowConcat.Concatenate([ints, longs], 1));
        Assert.Throws<ArgumentException>(() => ArrowConcat.Concatenate([], 1));
        var type = new DictionaryType(Int32Type.Default, StringType.Default, false);
        var dictionary = new DictionaryArray(type, new Int32Array.Builder().Append(0).Build(), new StringArray.Builder().Append("x").Build());
        Assert.Throws<NotSupportedException>(() => ArrowConcat.Concatenate([Batch(dictionary), Batch(dictionary)], 1));
    }
}
