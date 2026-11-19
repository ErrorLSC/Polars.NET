using System.Buffers;
using System.Collections.Frozen;
using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Expressions;
using static Kaminari.Sazanami.Filters.ValueTests;

namespace Kaminari.Sazanami.Filters;

/// <summary>
/// The test of one filter leaf on the values of a column (never a dictionary: dictionaries are tested on their
/// values, see <see cref="FilterProgram"/>).
/// </summary>
internal abstract class LeafTest
{
    /// <summary>The leaf's outcome on a null value: true (<c>IsNull</c>) or else null.</summary>
    public virtual bool NullIsTrue => false;

    /// <summary>
    /// Sets bit <c>i</c> of <paramref name="mask"/> where the test holds on value <c>i</c> of <paramref name="data"/>
    /// (from its offset), for <paramref name="length"/> values; bits of null slots are left to the caller to mask.
    /// <paramref name="valid"/> marks the valid slots (empty: all), for layouts whose null slots must not be read.
    /// </summary>
    public abstract void Run(ArrayData data, int length, ReadOnlySpan<ulong> valid, Span<ulong> mask);
}

/// <summary>A test with the same outcome on every value.</summary>
internal sealed class ConstantTest(bool value) : LeafTest
{
    public override void Run(ArrayData data, int length, ReadOnlySpan<ulong> valid, Span<ulong> mask) => Masks.Fill(mask, length, value);
}

/// <summary><c>IsNull</c>: false on every value, true on nulls.</summary>
internal sealed class NullTest : LeafTest
{
    public static NullTest Instance { get; } = new();

    public override bool NullIsTrue => true;

    public override void Run(ArrayData data, int length, ReadOnlySpan<ulong> valid, Span<ulong> mask) => Masks.Fill(mask, length, false);
}

/// <summary>A test on a boolean column, given by its outcomes on false and on true.</summary>
internal sealed class BooleanTest(bool onFalse, bool onTrue) : LeafTest
{
    public override void Run(ArrayData data, int length, ReadOnlySpan<ulong> valid, Span<ulong> mask)
    {
        if (onFalse == onTrue)
        {
            Masks.Fill(mask, length, onTrue);
            return;
        }
        Masks.Load(data.Buffers[1].Span, data.Offset, length, mask);
        if (onFalse) Masks.Not(mask, length);
    }
}

/// <summary>A test of fixed-width values: a column's, or values computed from columns (see <see cref="ExpressionTest"/>).</summary>
internal abstract class NumericLeafTest<T> : LeafTest where T : unmanaged, INumber<T>
{
    /// <summary>Sets bit <c>i</c> of <paramref name="mask"/> where the test holds on <paramref name="values"/>[i]; clears the bits past the last value.</summary>
    public abstract void RunValues(ReadOnlySpan<T> values, Span<ulong> mask);

    public sealed override void Run(ArrayData data, int length, ReadOnlySpan<ulong> valid, Span<ulong> mask) =>
        RunValues(ValueTypes.Values<T>(data, length), mask);
}

/// <summary>A comparison of fixed-width values with a scalar, by one of the <see cref="ValueTests"/>.</summary>
internal sealed class NumericTest<T, TTest>(T scalar) : NumericLeafTest<T>
    where T : unmanaged, INumber<T>
    where TTest : struct, IValueTest<T>
{
    public override void RunValues(ReadOnlySpan<T> values, Span<ulong> mask) => CompareKernels.Run<T, TTest>(values, scalar, mask);
}

/// <summary>A test of fixed-width values with the same outcome on every value.</summary>
internal sealed class NumericConstantTest<T>(bool value) : NumericLeafTest<T> where T : unmanaged, INumber<T>
{
    public override void RunValues(ReadOnlySpan<T> values, Span<ulong> mask) => Masks.Fill(mask, values.Length, value);
}

/// <summary>
/// A test computed on a column's values by a <see cref="ValuePredicate"/> over that one column (an expression of
/// it compared with a scalar, or with another of it): the predicate gives its own outcomes, nulls it computes
/// (a division by zero) included. Run on a dictionary's entries like any leaf.
/// </summary>
internal sealed class ExpressionTest(ValuePredicate predicate) : LeafTest
{
    public ValuePredicate Predicate { get; } = predicate;

    public override bool NullIsTrue => Predicate.NullIsTrue;

    public override void Run(ArrayData data, int length, ReadOnlySpan<ulong> valid, Span<ulong> mask) =>
        throw new InvalidOperationException("An expression test gives its outcomes itself (Outcomes).");

    /// <summary>Writes the rows where the predicate is true into <paramref name="t"/>, false into <paramref name="f"/>.</summary>
    public void Outcomes(ArrayData data, int length, Span<ulong> t, Span<ulong> f)
    {
        using var scratch = new ValueScratch();
        Predicate.Evaluate(new ValueContext(data, length, scratch), t, f);
    }
}

/// <summary>
/// Membership of a fixed-width column in a set of values: a vector equality per value for short lists, a hash set
/// otherwise. Floating values match by the IR's equality (NaN equals NaN, -0 equals 0).
/// </summary>
internal sealed class NumericInListTest<T> : NumericLeafTest<T> where T : unmanaged, INumber<T>
{
    private const int ShortList = 8;
    private readonly T[] _values;          // distinct numbers (NaN apart; -0 and 0 are one, as T's equality has it)
    private readonly bool _nan;            // the list holds NaN
    private readonly FrozenSet<T>? _set;   // for long lists

    public NumericInListTest(IEnumerable<T> values)
    {
        HashSet<T> numbers = [];
        foreach (T value in values)
        {
            if (T.IsNaN(value)) _nan = true;
            else numbers.Add(value);
        }
        _values = [.. numbers];
        if (_values.Length > ShortList) _set = numbers.ToFrozenSet();
    }

    public override void RunValues(ReadOnlySpan<T> values, Span<ulong> mask)
    {
        int length = values.Length, words = Masks.Words(length);
        if (_set is { } set)
        {
            for (int w = 0; w < words; w++)
            {
                int first = w << 6, n = Math.Min(64, length - first);
                ulong word = 0;
                for (int k = 0; k < n; k++)
                {
                    T v = values[first + k];
                    bool hit = T.IsNaN(v) ? _nan : set.Contains(v);
                    if (hit) word |= 1UL << k;
                }
                mask[w] = word;
            }
            return;
        }
        Masks.Fill(mask, length, false);
        if (_values.Length == 0 && !_nan) return;
        ulong[] one = ArrayPool<ulong>.Shared.Rent(words);
        try
        {
            Span<ulong> hits = one.AsSpan(0, words);
            foreach (T value in _values)
            {
                CompareKernels.Run<T, Eq<T>>(values, value, hits);
                Masks.Or(mask[..words], hits);
            }
            if (_nan)
            {
                CompareKernels.Run<T, IsNaN<T>>(values, T.Zero, hits);
                Masks.Or(mask[..words], hits);
            }
        }
        finally
        {
            ArrayPool<ulong>.Shared.Return(one);
        }
    }
}

/// <summary>A test of a string or binary column (Utf8 / Binary, their views, FixedSizeBinary).</summary>
internal sealed class BytesTest<TTest>(TTest test) : LeafTest where TTest : struct, IBytesTest
{
    public override void Run(ArrayData data, int length, ReadOnlySpan<ulong> valid, Span<ulong> mask)
    {
        switch (data.DataType)
        {
            case StringType or BinaryType:
            {
                ReadOnlySpan<int> offsets = data.Buffers[1].Span.CastTo<int>().Slice(data.Offset, length + 1);
                BinaryKernels.Offsets(test, offsets, data.Buffers[2].Span, length, valid, mask);
                break;
            }
            case StringViewType or BinaryViewType:
            {
                ReadOnlySpan<byte> views = data.Buffers[1].Span.Slice(data.Offset * 16, length * 16);
                var buffers = new ReadOnlyMemory<byte>[data.Buffers.Length - 2];
                for (int b = 0; b < buffers.Length; b++) buffers[b] = data.Buffers[b + 2].Memory;
                if (test is BytesTests.Equal equal) BinaryKernels.ViewsEqual(equal.Literal, views, buffers, length, valid, mask);
                else BinaryKernels.Views(test, views, buffers, length, valid, mask);
                break;
            }
            case FixedSizeBinaryType f:
            {
                ReadOnlySpan<byte> values = data.Buffers[1].Span.Slice(data.Offset * f.ByteWidth, length * f.ByteWidth);
                BinaryKernels.Fixed(test, values, f.ByteWidth, length, valid, mask);
                break;
            }
            default:
                throw new InvalidOperationException($"No byte test on a {data.DataType.Name} column.");
        }
    }
}

/// <summary>The tests of fixed-width values against scalars, shared by column leaves and computed values.</summary>
internal static class NumericTests
{
    /// <summary><c>value op scalar</c> in the IR's order (for floating values NaN is the greatest value and equals itself).</summary>
    public static NumericLeafTest<T> Compare<T>(CompareOp op, T scalar, bool floating) where T : unmanaged, INumber<T>
    {
        if (floating && T.IsNaN(scalar))
        {
            return op switch
            {
                CompareOp.Eq or CompareOp.GtEq => new NumericTest<T, IsNaN<T>>(scalar),
                CompareOp.NotEq or CompareOp.Lt => new NumericTest<T, NotNaN<T>>(scalar),
                CompareOp.LtEq => new NumericConstantTest<T>(true),
                _ => new NumericConstantTest<T>(false),
            };
        }
        return op switch
        {
            CompareOp.Eq => new NumericTest<T, Eq<T>>(scalar),
            CompareOp.NotEq => new NumericTest<T, NotEq<T>>(scalar),
            CompareOp.Lt => new NumericTest<T, Lt<T>>(scalar),
            CompareOp.LtEq => new NumericTest<T, LtEq<T>>(scalar),
            CompareOp.Gt => floating ? new NumericTest<T, GtOrNaN<T>>(scalar) : new NumericTest<T, Gt<T>>(scalar),
            _ => floating ? new NumericTest<T, GtEqOrNaN<T>>(scalar) : new NumericTest<T, GtEq<T>>(scalar),
        };
    }
}
