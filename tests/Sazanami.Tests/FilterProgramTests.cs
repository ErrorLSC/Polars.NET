using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Filters;
using static Sazanami.Tests.FilterTestData;

namespace Sazanami.Tests;

/// <summary><see cref="FilterProgram"/> against a model: the IR's semantics evaluated row by row on model values.</summary>
public class FilterProgramTests
{
    private static readonly int[] Lengths = [0, 1, 2, 7, 8, 15, 16, 31, 32, 33, 63, 64, 65, 127, 128, 129, 200, 1000, 4099, 65536];

    // ---------------------------------------------------------------------------------------------------------------
    // The model

    private static bool? Eval(FilterNode node, Func<int, object?> column) => node switch
    {
        FilterNode.Constant c => c.Value,
        FilterNode.Not n => Eval(n.Operand, column) is bool b ? !b : null,
        FilterNode.And a => a.Operands.Select(o => Eval(o, column)).Aggregate((bool?)true, (x, y) => x == false || y == false ? false : x == true && y == true ? true : null),
        FilterNode.Or o => o.Operands.Select(x => Eval(x, column)).Aggregate((bool?)false, (x, y) => x == true || y == true ? true : x == false && y == false ? false : null),
        FilterNode.Coalesce c => c.Operands.Select(o => Eval(o, column)).FirstOrDefault(v => v is not null),
        FilterNode.IsNull n => column(n.Column) is null,
        FilterNode.IsNan n => column(n.Column) is double d ? double.IsNaN(d) : null,
        FilterNode.BooleanColumn b => column(b.Column) is Int128 v ? v == 1 : null,
        FilterNode.Compare c => column(c.Column) is { } v ? Holds(c.Op, Compare(v, Model(c.Value))) : null,
        FilterNode.InList i => column(i.Column) is { } v ? i.Values.Any(x => Compare(v, Model(x)) == 0) : null,
        FilterNode.StartsWith s => column(s.Column) is byte[] v ? v.AsSpan().StartsWith(s.Prefix) : null,
        _ => throw new ArgumentException($"{node}"),
    };

    private static object Model(FilterScalar s) => s.Kind switch
    {
        FilterScalarKind.Integer => s.IntegerValue,
        FilterScalarKind.Float => s.FloatValue,
        _ => s.BytesValue!,
    };

    private static bool Holds(CompareOp op, int order) => op switch
    {
        CompareOp.Eq => order == 0,
        CompareOp.NotEq => order != 0,
        CompareOp.Lt => order < 0,
        CompareOp.LtEq => order <= 0,
        CompareOp.Gt => order > 0,
        _ => order >= 0,
    };

    // ---------------------------------------------------------------------------------------------------------------
    // Random programs

    private static FilterNode RandomNode(Random random, ColumnKind[] kinds, int depth)
    {
        int pick = random.Next(depth <= 0 ? 6 : 10);
        if (pick >= 6)
        {
            FilterNode[] Operands() => [.. Enumerable.Range(0, random.Next(1, 4)).Select(_ => RandomNode(random, kinds, depth - 1))];
            return pick switch
            {
                6 => new FilterNode.And(Operands()),
                7 => new FilterNode.Or(Operands()),
                8 => new FilterNode.Not(RandomNode(random, kinds, depth - 1)),
                _ => new FilterNode.Coalesce(Operands()),
            };
        }
        if (pick == 5) return new FilterNode.Constant(random.Next(3) switch { 0 => true, 1 => false, _ => null });
        return RandomLeaf(random, kinds);
    }

    private static FilterNode RandomLeaf(Random random, ColumnKind[] kinds)
    {
        int c = random.Next(kinds.Length);
        ColumnKind kind = kinds[c];
        object Value() => kind.Pool[random.Next(kind.Pool.Length)];
        int choice = random.Next(10);
        if (choice == 0) return new FilterNode.IsNull(c);
        if (choice == 1 && kind.IsFloating) return new FilterNode.IsNan(c);
        if (choice == 1 && kind.IsBoolean) return new FilterNode.BooleanColumn(c);
        if (choice == 2 && kind.IsBytes)
        {
            byte[] v = (byte[])Value();
            return new FilterNode.StartsWith(c, v[..random.Next(v.Length + 1)]);
        }
        if (choice <= 3)
        {
            // Long lists go through the hash set, short ones through vector equalities.
            int n = random.Next(2) == 0 ? random.Next(0, 4) : random.Next(9, 14);
            return new FilterNode.InList(c, [.. Enumerable.Range(0, n).Select(_ => kind.Scalar(Value()))]);
        }
        return new FilterNode.Compare(c, (CompareOp)random.Next(6), kind.Scalar(Value()));
    }

    /// <summary>A column of a kind, laid out at random: managed or native memory, sliced at an offset, as a dictionary.</summary>
    private static (IArrowArray Array, IArrowType Declared) RandomColumn(Random random, ColumnKind kind, int length, out object?[] model)
    {
        double nulls = random.Next(4) switch { 0 => 0, 1 => 0.05, 2 => 0.5, _ => 0.97 };
        int offset = random.Next(3) == 0 ? random.Next(1, 70) : 0;
        object?[] values = Values(random, kind, length + offset, nulls);
        model = values[offset..];
        if (kind.Dictionaries && random.Next(5) == 0)
            return (ArrowArrayFactory.Slice(BuildDictionary(random, kind, values), offset, length), random.Next(2) == 0 ? kind.Type : new DictionaryType(Int32Type.Default, kind.Type, false));
        IArrowArray array = random.Next(3) == 0 && kind.Type is not HalfFloatType ? BuildNative(kind, values) : Build(random, kind, values);
        return (offset == 0 ? array : ArrowArrayFactory.Slice(array, offset, length), kind.Type);
    }

    [Fact]
    public void RandomPrograms_MatchTheModel()
    {
        var random = new Random(20261114);
        for (int round = 0; round < 1500; round++)
        {
            int length = Lengths[random.Next(Lengths.Length)];
            ColumnKind[] kinds = [.. Enumerable.Range(0, random.Next(1, 4)).Select(_ => Kinds[random.Next(Kinds.Length)])];
            var models = new object?[kinds.Length][];
            var arrays = new IArrowArray[kinds.Length];
            var columns = new FilterColumn[kinds.Length];
            for (int c = 0; c < kinds.Length; c++)
            {
                (arrays[c], IArrowType declared) = RandomColumn(random, kinds[c], length, out models[c]);
                columns[c] = new FilterColumn([$"c{c}"], declared);
            }
            FilterNode root = RandomNode(random, kinds, 3);
            AssertMatches(columns, root, arrays, models, length, $"round {round}");
        }
    }

    private static void AssertMatches(FilterColumn[] columns, FilterNode root, IArrowArray[] arrays, object?[][] models, int length, string where)
    {
        FilterProgram program = FilterProgram.Create(columns, root);
        var selected = new ulong[Masks.Words(length) + 1];
        selected.AsSpan().Fill(0xA5A5A5A5A5A5A5A5); // stale bits must be overwritten
        int count = program.Evaluate(arrays, length, selected);
        int expected = 0;
        for (int i = 0; i < length; i++)
        {
            bool want = Eval(root, c => models[c][i]) == true;
            bool got = (selected[i >> 6] >> (i & 63) & 1) != 0;
            if (want) expected++;
            Assert.True(want == got, $"{where}: row {i} of {length}: expected {want}, got {got}\n{root}\ncolumns: {string.Join(", ", columns.Select(c => c.Type.Name))}\nvalues: {string.Join(", ", models.Select(m => Render(m[i])))}");
        }
        Assert.Equal(expected, count);
        if (length % 64 != 0) Assert.Equal(0UL, selected[length >> 6] & ~Masks.LastWord(length));
        Assert.True(selected.AsSpan(Masks.Words(length)).IndexOfAnyExcept(0xA5A5A5A5A5A5A5A5) < 0, "words past the mask were written");
    }

    private static string Render(object? v) => v switch
    {
        null => "null",
        byte[] b => Convert.ToHexString(b),
        double d => d.ToString("R"),
        _ => v.ToString()!,
    };

    // ---------------------------------------------------------------------------------------------------------------
    // Cases the random rounds may hit rarely

    private static ColumnKind Kind(string name) => Kinds.Single(k => k.Name == name);

    private static int[] Selected(FilterNode root, ColumnKind kind, object?[] values, IArrowArray? array = null)
    {
        array ??= Build(new Random(1), kind, values);
        FilterProgram program = FilterProgram.Create([new FilterColumn(["x"], kind.Type)], root);
        var mask = new ulong[Masks.Words(values.Length)];
        program.Evaluate([array], values.Length, mask);
        return [.. Enumerable.Range(0, values.Length).Where(i => (mask[i >> 6] >> (i & 63) & 1) != 0)];
    }

    [Theory]
    [InlineData("f64")]
    [InlineData("f32")]
    [InlineData("f16")]
    public void Floating_NaNIsTheGreatestAndEqualsItself(string name)
    {
        ColumnKind kind = Kind(name);
        object?[] values = [double.NaN, -0.0, 0.0, 1.0, double.PositiveInfinity, null];
        FilterNode Cmp(CompareOp op, double v) => new FilterNode.Compare(0, op, FilterScalar.Float(v));
        Assert.Equal([0], Selected(Cmp(CompareOp.Gt, double.PositiveInfinity), kind, values));
        Assert.Equal([0, 4], Selected(Cmp(CompareOp.GtEq, double.PositiveInfinity), kind, values));
        Assert.Equal([1, 2, 3, 4], Selected(Cmp(CompareOp.Lt, double.NaN), kind, values));
        Assert.Equal([0, 1, 2, 3, 4], Selected(Cmp(CompareOp.LtEq, double.NaN), kind, values));
        Assert.Equal([0], Selected(Cmp(CompareOp.Eq, double.NaN), kind, values));
        Assert.Equal([], Selected(Cmp(CompareOp.Gt, double.NaN), kind, values));
        Assert.Equal([1, 2], Selected(Cmp(CompareOp.Eq, -0.0), kind, values));
        Assert.Equal([0, 3, 4], Selected(Cmp(CompareOp.NotEq, 0.0), kind, values));
        Assert.Equal([0], Selected(new FilterNode.IsNan(0), kind, values));
        // Null stays null under NOT: the null row is never selected.
        Assert.Equal([1, 2, 3, 4], Selected(new FilterNode.Not(Cmp(CompareOp.Eq, double.NaN)), kind, values));
        Assert.Equal([5], Selected(new FilterNode.IsNull(0), kind, values));
    }

    [Theory]
    [InlineData("view")]
    [InlineData("utf8")]
    public void Strings_CompareByCodePoint(string name)
    {
        ColumnKind kind = Kind(name);
        // Long values sharing the first four bytes, inline ones of every length, code points above ASCII.
        object?[] values = ["Banana"u8.ToArray(), "apple"u8.ToArray(), "äpfel"u8.ToArray(), "abcdXXXXXXXXXXXX1"u8.ToArray(), "abcdXXXXXXXXXXXX2"u8.ToArray(), "abcd"u8.ToArray(), "abc"u8.ToArray(), ""u8.ToArray(), null];
        FilterNode Cmp(CompareOp op, ReadOnlySpan<byte> v) => new FilterNode.Compare(0, op, FilterScalar.Bytes(v.ToArray()));
        Assert.Equal([0, 1, 3, 4, 5, 6, 7], Selected(Cmp(CompareOp.Lt, "b"u8), kind, values));
        Assert.Equal([2], Selected(Cmp(CompareOp.Gt, "apple"u8), kind, values));
        Assert.Equal([1, 2, 4], Selected(Cmp(CompareOp.Gt, "abcdXXXXXXXXXXXX1"u8), kind, values));
        Assert.Equal([3], Selected(Cmp(CompareOp.Eq, "abcdXXXXXXXXXXXX1"u8), kind, values));
        Assert.Equal([0, 1, 2, 4, 5, 6, 7], Selected(Cmp(CompareOp.NotEq, "abcdXXXXXXXXXXXX1"u8), kind, values));
        Assert.Equal([3, 4, 5], Selected(new FilterNode.StartsWith(0, "abcd"u8.ToArray()), kind, values));
        Assert.Equal([3, 4], Selected(new FilterNode.StartsWith(0, "abcdXXXXX"u8.ToArray()), kind, values));
        Assert.Equal([0, 1, 2, 3, 4, 5, 6, 7], Selected(new FilterNode.StartsWith(0, []), kind, values));
        Assert.Equal([5, 7], Selected(new FilterNode.InList(0, [FilterScalar.Bytes("abcd"u8.ToArray()), FilterScalar.Bytes(""u8.ToArray()), FilterScalar.Bytes("zz"u8.ToArray())]), kind, values));
    }

    [Fact]
    public void Views_DecideFromTheViewWhereItCan()
    {
        // Exactly 12 bytes is still inline; a long value whose first four bytes differ, or whose length differs, is
        // decided without its data buffer (here the view points at one that does not exist).
        byte[] twelve = "abcdefghijkl"u8.ToArray();
        var views = new byte[3 * 16];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(views, 12);
        twelve.CopyTo(views, 4);
        for (int i = 1; i < 3; i++)
        {
            Span<byte> view = views.AsSpan(i * 16, 16);
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(view, i == 1 ? 20 : 30);
            "zzzz"u8.CopyTo(view[4..]);
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(view[8..], 77); // no such buffer
        }
        IArrowArray array = ArrowArrayFactory.BuildArray(new ArrayData(StringViewType.Default, 3, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(views)]));
        int[] Run(FilterNode node)
        {
            FilterProgram program = FilterProgram.Create([new FilterColumn(["x"], StringViewType.Default)], node);
            var mask = new ulong[1];
            program.Evaluate([array], 3, mask);
            return [.. Enumerable.Range(0, 3).Where(i => (mask[0] >> i & 1) != 0)];
        }
        Assert.Equal([0], Run(new FilterNode.Compare(0, CompareOp.Eq, FilterScalar.Bytes(twelve))));
        Assert.Equal([], Run(new FilterNode.Compare(0, CompareOp.Eq, FilterScalar.Bytes("abcdXXXXXXXXXXXXXXXX"u8.ToArray()))));
        Assert.Equal([], Run(new FilterNode.Compare(0, CompareOp.Eq, FilterScalar.Bytes("zzzzXXXXXXXXXXXXXXXXXXXXX"u8.ToArray()))));
        Assert.Equal([0], Run(new FilterNode.Compare(0, CompareOp.Lt, FilterScalar.Bytes("b"u8.ToArray()))));
        Assert.Equal([0], Run(new FilterNode.StartsWith(0, "abc"u8.ToArray())));
    }

    [Fact]
    public void Views_NullSlotsAreNotRead()
    {
        // Null views of foreign arrays may hold anything: here they point at a data buffer that does not exist.
        ColumnKind kind = Kind("view");
        object?[] values = [.. Enumerable.Range(0, 300).Select(i => i % 3 == 0 ? null : (object)Encoding.UTF8.GetBytes($"value-{i}-long-enough"))];
        IArrowArray array = Build(new Random(7), kind, values);
        int[] hits = Selected(new FilterNode.StartsWith(0, "value-1"u8.ToArray()), kind, values, array);
        Assert.Equal([.. Enumerable.Range(0, 300).Where(i => i % 3 != 0 && $"{i}".StartsWith('1'))], hits);
    }

    [Fact]
    public void Dictionary_NullEntriesAndNullIndices()
    {
        ColumnKind kind = Kind("utf8");
        IArrowArray dictionary = Build(new Random(2), kind, ["a"u8.ToArray(), null, "b"u8.ToArray()]);
        byte[] indices = [0, 1, 2, 99]; // the last is null, its index garbage
        var data = new ArrayData(new DictionaryType(Int8Type.Default, StringType.Default, false), 4, 1, 0,
            [new ArrowBuffer((byte[])[0b0111]), new ArrowBuffer(indices)], [], dictionary.Data);
        IArrowArray array = ArrowArrayFactory.BuildArray(data);
        int[] Run(FilterNode node)
        {
            FilterProgram program = FilterProgram.Create([new FilterColumn(["x"], StringType.Default)], node);
            var mask = new ulong[1];
            program.Evaluate([array], 4, mask);
            return [.. Enumerable.Range(0, 4).Where(i => (mask[0] >> i & 1) != 0)];
        }
        Assert.Equal([1, 3], Run(new FilterNode.IsNull(0)));
        Assert.Equal([0, 2], Run(new FilterNode.Not(new FilterNode.IsNull(0))));
        Assert.Equal([2], Run(new FilterNode.Not(new FilterNode.Compare(0, CompareOp.Eq, FilterScalar.Bytes("a"u8.ToArray())))));
        Assert.Equal([1, 2, 3], Run(new FilterNode.Coalesce([new FilterNode.Compare(0, CompareOp.Gt, FilterScalar.Bytes("a"u8.ToArray())), new FilterNode.Constant(true)])));

        // An index past the entries is an error, not a read past them.
        indices[3] = 3;
        var bad = ArrowArrayFactory.BuildArray(new ArrayData(data.DataType, 4, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(indices)], [], dictionary.Data));
        FilterProgram p = FilterProgram.Create([new FilterColumn(["x"], StringType.Default)], new FilterNode.IsNull(0));
        Assert.Throws<InvalidDataException>(() => p.Evaluate([bad], 4, new ulong[1]));
    }

    [Fact]
    public void Boolean_ComparesAsZeroAndOne()
    {
        ColumnKind kind = Kind("bool");
        object?[] values = [(Int128)0, (Int128)1, null, (Int128)1];
        FilterNode Cmp(CompareOp op, int v) => new FilterNode.Compare(0, op, FilterScalar.Integer(v));
        Assert.Equal([1, 3], Selected(new FilterNode.BooleanColumn(0), kind, values));
        Assert.Equal([0], Selected(new FilterNode.Not(new FilterNode.BooleanColumn(0)), kind, values));
        Assert.Equal([0], Selected(Cmp(CompareOp.Lt, 1), kind, values));
        Assert.Equal([0, 1, 3], Selected(Cmp(CompareOp.GtEq, 0), kind, values));
        Assert.Equal([], Selected(Cmp(CompareOp.Gt, 1), kind, values));
        Assert.Equal([0, 1, 3], Selected(new FilterNode.InList(0, [FilterScalar.Integer(1), FilterScalar.Integer(0)]), kind, values));
    }

    [Fact]
    public void And_StopsOnlyWhenEveryRowIsFalse()
    {
        // The first operand is false on every row but the last: the second must still decide that row.
        ColumnKind kind = Kind("i64");
        object?[] values = [.. Enumerable.Range(0, 130).Select(i => (object)(Int128)i)];
        FilterNode node = new FilterNode.And([
            new FilterNode.Compare(0, CompareOp.GtEq, FilterScalar.Integer(129)),
            new FilterNode.Compare(0, CompareOp.Lt, FilterScalar.Integer(200))]);
        Assert.Equal([129], Selected(node, kind, values));
        FilterNode none = new FilterNode.And([new FilterNode.Constant(false), new FilterNode.IsNull(0)]);
        Assert.Equal([], Selected(none, kind, values));
        FilterNode all = new FilterNode.Or([new FilterNode.Constant(true), new FilterNode.IsNull(0)]);
        Assert.Equal(130, Selected(all, kind, values).Length);
    }

    [Fact]
    public void Create_RefusesWhatTheColumnsCannotTake()
    {
        FilterColumn[] columns =
        [
            new(["i8"], Int8Type.Default), new(["f32"], FloatType.Default), new(["s"], StringType.Default), new(["b"], BooleanType.Default),
            new(["list"], new ListType(Int32Type.Default)),
        ];
        void Refused(FilterNode node) => Assert.Throws<ArgumentException>(() => FilterProgram.Create(columns, node));
        Refused(new FilterNode.Compare(0, CompareOp.Eq, FilterScalar.Integer(128)));          // outside Int8
        Refused(new FilterNode.Compare(0, CompareOp.Eq, FilterScalar.Float(1)));              // a float for an integer
        Refused(new FilterNode.Compare(1, CompareOp.Eq, FilterScalar.Float(0.1)));            // not a float exactly
        Refused(new FilterNode.Compare(1, CompareOp.Eq, FilterScalar.Integer(1)));
        Refused(new FilterNode.Compare(2, CompareOp.Eq, FilterScalar.Integer(1)));
        Refused(new FilterNode.Compare(3, CompareOp.Eq, FilterScalar.Integer(2)));
        Refused(new FilterNode.IsNan(0));
        Refused(new FilterNode.StartsWith(0, [1]));
        Refused(new FilterNode.BooleanColumn(0));
        Refused(new FilterNode.IsNull(5));
        Refused(new FilterNode.IsNull(-1));
        Refused(new FilterNode.Compare(4, CompareOp.Eq, FilterScalar.Integer(1)));
        Refused(new FilterNode.And([]));
        Refused(new FilterNode.InList(1, [FilterScalar.Float(0.1)]));
        FilterProgram.Create(columns, new FilterNode.Compare(1, CompareOp.Eq, FilterScalar.Float(0.5))); // exact
        FilterProgram.Create(columns, new FilterNode.IsNull(4)); // a null test fits any column
    }

    [Fact]
    public void Evaluate_RefusesArraysThatDoNotMatch()
    {
        FilterProgram program = FilterProgram.Create([new FilterColumn(["x"], new TimestampType(TimeUnit.Microsecond, (string?)null))],
            new FilterNode.Compare(0, CompareOp.Gt, FilterScalar.Integer(0)));
        IArrowArray nanos = ArrowArrayFactory.BuildArray(new ArrayData(new TimestampType(TimeUnit.Nanosecond, (string?)null), 1, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(new byte[8])]));
        Assert.Throws<ArgumentException>(() => program.Evaluate([nanos], 1, new ulong[1]));
        IArrowArray utc = ArrowArrayFactory.BuildArray(new ArrayData(new TimestampType(TimeUnit.Microsecond, "UTC"), 1, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(new byte[8])]));
        Assert.Throws<ArgumentException>(() => program.Evaluate([utc], 1, new ulong[1]));
        IArrowArray micros = ArrowArrayFactory.BuildArray(new ArrayData(new TimestampType(TimeUnit.Microsecond, (string?)null), 1, 0, 0, [ArrowBuffer.Empty, new ArrowBuffer(new byte[8])]));
        Assert.Throws<ArgumentException>(() => program.Evaluate([micros], 2, new ulong[1]));   // length
        Assert.Throws<ArgumentException>(() => program.Evaluate([], 1, new ulong[1]));         // columns
        Assert.Throws<ArgumentException>(() => program.Evaluate([micros], 1, []));             // mask
        Assert.Equal(0, program.Evaluate([micros], 1, new ulong[1]));
    }
}
