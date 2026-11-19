using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Expressions;
using static Sazanami.Tests.FilterTestData;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="ValueProgram"/> against <see cref="ValueModel"/>: random expressions over columns of every layout
/// (managed or native, sliced, dictionaries), computed into Arrow arrays.
/// </summary>
public class ValueProgramTests
{
    private static readonly int[] Lengths = [0, 1, 7, 31, 32, 33, 63, 64, 65, 127, 129, 1000, 4099];

    private static readonly ColumnKind[] Arithmetic = [.. Kinds.Where(k => ValueModel.IsArithmetic(k.Type))];

    private static ColumnKind Kind(string name) => Kinds.Single(k => k.Name == name);

    private static (IArrowArray Array, IArrowType Declared) RandomColumn(Random random, ColumnKind kind, int length, out object?[] model)
    {
        double nulls = random.Next(4) switch { 0 => 0, 1 => 0.05, 2 => 0.5, _ => 0.97 };
        int offset = random.Next(3) == 0 ? random.Next(1, 70) : 0;
        object?[] values = Values(random, kind, length + offset, nulls);
        model = values[offset..];
        if (random.Next(5) == 0)
            return (ArrowArrayFactory.Slice(BuildDictionary(random, kind, values), offset, length), random.Next(2) == 0 ? kind.Type : new DictionaryType(Int32Type.Default, kind.Type, false));
        IArrowArray array = random.Next(3) == 0 ? BuildNative(kind, values) : Build(random, kind, values);
        return (offset == 0 ? array : ArrowArrayFactory.Slice(array, offset, length), kind.Type);
    }

    /// <summary>Equal model values; floating ones bit for bit (the sign of zero kept), NaN as NaN.</summary>
    private static bool Same(object? expected, object? actual) => (expected, actual) switch
    {
        (null, null) => true,
        (double e, double a) => double.IsNaN(e) ? double.IsNaN(a) : BitConverter.DoubleToInt64Bits(e) == BitConverter.DoubleToInt64Bits(a),
        (Int128 e, Int128 a) => e == a,
        _ => false,
    };

    [Fact]
    public void RandomExpressions_MatchTheModel()
    {
        var random = new Random(20261119);
        for (int round = 0; round < 3000; round++)
        {
            int length = Lengths[random.Next(Lengths.Length)];
            ColumnKind[] kinds = [.. Enumerable.Range(0, random.Next(1, 4)).Select(_ => Arithmetic[random.Next(Arithmetic.Length)])];
            var models = new object?[kinds.Length][];
            var arrays = new IArrowArray[kinds.Length];
            var types = new IArrowType[kinds.Length];
            for (int c = 0; c < kinds.Length; c++) (arrays[c], types[c]) = RandomColumn(random, kinds[c], length, out models[c]);
            ValueNode root = ValueModel.RandomExpression(random, types, random.Next(kinds.Length), out IArrowType type);
            var program = ValueProgram.Create(types, root);
            Assert.Equal(type.TypeId, program.Type.TypeId);
            using IArrowArray result = program.Evaluate(arrays, length);
            Assert.Equal(length, result.Length);
            Assert.Equal(type.TypeId, result.Data.DataType.TypeId);
            ColumnKind resultKind = Kinds.First(k => k.Type.TypeId == type.TypeId);
            int nulls = 0;
            for (int i = 0; i < length; i++)
            {
                object? expected = ValueModel.Value(root, c => models[c][i], types, type), actual = Read(resultKind, result, i);
                if (expected is null) nulls++;
                Assert.True(Same(expected, actual), $"round {round}, row {i} of {length}: expected {expected}, got {actual}\n{root}\n{string.Join(", ", types.Select(t => t.Name))}");
            }
            Assert.Equal(nulls, result.NullCount);
        }
    }

    private static ValueNode Col(int c) => new ValueNode.Column(c);

    private static ValueNode Int(long v) => new ValueNode.Scalar(StoredScalar.Integer(v));

    private static ValueNode Float(double v) => new ValueNode.Scalar(StoredScalar.Float(v));

    private static ValueNode Op(ArithmeticOp op, ValueNode l, ValueNode r) => new ValueNode.Arithmetic(op, l, r);

    private static object?[] Run(ColumnKind kind, object?[] values, ValueNode root, out IArrowType type)
    {
        var program = ValueProgram.Create([kind.Type], root);
        type = program.Type;
        using IArrowArray result = program.Evaluate([Build(new Random(1), kind, values)], values.Length);
        ColumnKind resultKind = Kinds.First(k => k.Type.TypeId == program.Type.TypeId);
        return [.. Enumerable.Range(0, values.Length).Select(i => Read(resultKind, result, i))];
    }

    [Fact]
    public void Integers_WrapAndDivideAsDotNet()
    {
        ColumnKind i32 = Kind("i32");
        object?[] values = [(Int128)int.MaxValue, (Int128)int.MinValue, (Int128)(-7), (Int128)7, null];
        Assert.Equal([(Int128)int.MinValue, (Int128)(int.MinValue + 1), (Int128)(-6), (Int128)8, null], Run(i32, values, Op(ArithmeticOp.Add, Col(0), Int(1)), out _));
        Assert.Equal([(Int128)1, (Int128)0, (Int128)49, (Int128)49, null], Run(i32, values, Op(ArithmeticOp.Multiply, Col(0), Col(0)), out _)); // (2^31 - 1)^2 wraps to 1
        // Truncated: -7 / 2 = -3, -7 % 2 = -1; MinValue / -1 wraps, MinValue % -1 is 0; by zero null.
        Assert.Equal([(Int128)1073741823, (Int128)(-1073741824), (Int128)(-3), (Int128)3, null], Run(i32, values, Op(ArithmeticOp.Divide, Col(0), Int(2)), out _));
        Assert.Equal([(Int128)1, (Int128)0, (Int128)(-1), (Int128)1, null], Run(i32, values, Op(ArithmeticOp.Remainder, Col(0), Int(2)), out _));
        Assert.Equal([(Int128)(-int.MaxValue), (Int128)int.MinValue, (Int128)7, (Int128)(-7), null], Run(i32, values, Op(ArithmeticOp.Divide, Col(0), Int(-1)), out _));
        Assert.Equal([(Int128)0, (Int128)0, (Int128)0, (Int128)0, null], Run(i32, values, Op(ArithmeticOp.Remainder, Col(0), Int(-1)), out _));
        Assert.Equal([null, null, null, null, null], Run(i32, values, Op(ArithmeticOp.Divide, Col(0), Int(0)), out _));
        // A zero divisor among the values: null on its row only.
        object?[] divisors = [(Int128)2, (Int128)0, (Int128)(-1), null];
        Assert.Equal([(Int128)5, null, (Int128)(-10), null], Run(i32, divisors, Op(ArithmeticOp.Divide, Int(10), Col(0)), out _));
        Assert.Equal([(Int128)(-int.MaxValue), (Int128)int.MinValue, (Int128)7, (Int128)(-7), null], Run(i32, values, new ValueNode.Negate(Col(0)), out _));
        // Narrowing wraps; to floating rounds to nearest.
        Assert.Equal([(Int128)(-1), (Int128)0, (Int128)(-7), (Int128)7, null], Run(i32, values, new ValueNode.Cast(Col(0), Int8Type.Default), out _));
        ColumnKind i64 = Kind("i64");
        object?[] big = [(Int128)((1L << 53) + 1), (Int128)long.MaxValue, (Int128)(-((1L << 60) + 3))];
        Assert.Equal([(double)((1L << 53) + 1), (double)long.MaxValue, (double)(-((1L << 60) + 3))], Run(i64, big, new ValueNode.Cast(Col(0), DoubleType.Default), out _));
        Assert.Equal([(double)(float)((1L << 53) + 1), (double)(float)long.MaxValue, (double)(float)(-((1L << 60) + 3))], Run(i64, big, new ValueNode.Cast(Col(0), FloatType.Default), out _));
        ColumnKind u64 = Kind("u64");
        object?[] unsigned = [(Int128)ulong.MaxValue, (Int128)((1UL << 63) + 1025)];
        Assert.Equal([(double)ulong.MaxValue, (double)((1UL << 63) + 1025)], Run(u64, unsigned, new ValueNode.Cast(Col(0), DoubleType.Default), out _));
        Assert.Equal([(Int128)(ulong.MaxValue - 1), (Int128)((1UL << 63) + 1024)], Run(u64, unsigned, Op(ArithmeticOp.Subtract, Col(0), Int(1)), out _));
    }

    [Fact]
    public void Floats_FollowIeee_InTheirWidth()
    {
        ColumnKind f64 = Kind("f64"), f32 = Kind("f32");
        object?[] values = [0.0, -0.0, 1.0, double.NaN, double.PositiveInfinity, -7.5, null];
        object?[] negated = Run(f64, values, new ValueNode.Negate(Col(0)), out _);
        Assert.Equal(BitConverter.DoubleToInt64Bits(-0.0), BitConverter.DoubleToInt64Bits((double)negated[0]!)); // the sign flips: -0
        Assert.Equal(0L, BitConverter.DoubleToInt64Bits((double)negated[1]!));
        Assert.Equal([double.PositiveInfinity, double.NegativeInfinity, 1.0, double.NaN, 0.0, 1 / -7.5, null], Run(f64, values, Op(ArithmeticOp.Divide, Float(1), Col(0)), out _));
        Assert.Equal([0.0, -0.0, 1.0, double.NaN, double.NaN, -1.5, null], Run(f64, values, Op(ArithmeticOp.Remainder, Col(0), Float(2)), out _));
        // Float32 arithmetic rounds in Float32: 1 + 2^-24 is 1 there.
        object?[] ones = [1.0];
        Assert.Equal([1.0], Run(f32, ones, Op(ArithmeticOp.Add, Col(0), Float(Math.Pow(2, -24))), out _));
        Assert.Equal([1.0 + Math.Pow(2, -24)], Run(f64, ones, Op(ArithmeticOp.Add, Col(0), Float(Math.Pow(2, -24))), out _));
    }

    [Fact]
    public void Dictionaries_NullArrays_AndOwnership()
    {
        ColumnKind i16 = Kind("i16");
        object?[] values = [(Int128)1, null, (Int128)short.MaxValue, (Int128)1, (Int128)(-3)];
        IArrowArray dictionary = BuildDictionary(new Random(3), i16, values);
        var program = ValueProgram.Create([new DictionaryType(Int32Type.Default, Int16Type.Default, false)], Op(ArithmeticOp.Add, Col(0), Int(1)));
        using (IArrowArray result = program.Evaluate([dictionary], values.Length))
            Assert.Equal([(Int128)2, null, (Int128)short.MinValue, (Int128)2, (Int128)(-2)], Enumerable.Range(0, values.Length).Select(i => Read(i16, result, i)));
        // A null array (a column a batch lacks): null on every row.
        using (IArrowArray result = ValueProgram.Create([Int16Type.Default], Op(ArithmeticOp.Add, Col(0), Int(1))).Evaluate([new NullArray(3)], 3))
            Assert.Equal(3, result.NullCount);
        // A column passed through is copied: the result outlives its input.
        IArrowArray input = BuildNative(i16, values);
        IArrowArray copy = ValueProgram.Create([Int16Type.Default], Col(0)).Evaluate([input], values.Length);
        input.Dispose();
        using (copy) Assert.Equal(values, Enumerable.Range(0, values.Length).Select(i => Read(i16, copy, i)));
    }

    [Fact]
    public void IllTypedExpressions_AreRefused()
    {
        IArrowType[] types = [Int32Type.Default, Int64Type.Default, DoubleType.Default, StringType.Default, Date32Type.Default, UInt8Type.Default];
        Assert.Throws<ArgumentException>(() => ValueProgram.Create(types, Op(ArithmeticOp.Add, Col(0), Col(1))));      // two types
        Assert.Throws<ArgumentException>(() => ValueProgram.Create(types, Op(ArithmeticOp.Add, Int(1), Int(2))));       // scalars alone
        Assert.Throws<ArgumentException>(() => ValueProgram.Create(types, Op(ArithmeticOp.Add, Col(3), Col(3))));       // strings
        Assert.Throws<ArgumentException>(() => ValueProgram.Create(types, Op(ArithmeticOp.Add, Col(4), Int(1))));       // dates take no arithmetic
        Assert.Throws<ArgumentException>(() => ValueProgram.Create(types, new ValueNode.Negate(Col(5))));               // unsigned
        Assert.Throws<ArgumentException>(() => ValueProgram.Create(types, new ValueNode.Cast(Col(2), Int32Type.Default))); // floating to integer
        Assert.Throws<ArgumentException>(() => ValueProgram.Create(types, Op(ArithmeticOp.Add, Col(0), Float(0.5))));  // a scalar of another kind
        Assert.Throws<ArgumentException>(() => ValueProgram.Create(types, Op(ArithmeticOp.Add, Col(5), Int(300))));    // outside the type
        Assert.Throws<ArgumentException>(() => ValueProgram.Create(types, Col(9)));
    }
}
