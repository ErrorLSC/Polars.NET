using System.Buffers;
using System.Collections.Frozen;
using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
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

/// <summary>A comparison of a fixed-width column with a scalar, by one of the <see cref="ValueTests"/>.</summary>
internal sealed class NumericTest<T, TTest>(T scalar) : LeafTest
    where T : unmanaged, INumber<T>
    where TTest : struct, IValueTest<T>
{
    public override void Run(ArrayData data, int length, ReadOnlySpan<ulong> valid, Span<ulong> mask) =>
        CompareKernels.Run<T, TTest>(FilterProgram.Values<T>(data, length), scalar, mask);
}

/// <summary>
/// Membership of a fixed-width column in a set of values: a vector equality per value for short lists, a hash set
/// otherwise. Floating values match by the IR's equality (NaN equals NaN, -0 equals 0).
/// </summary>
internal sealed class NumericInListTest<T> : LeafTest where T : unmanaged, INumber<T>
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

    public override void Run(ArrayData data, int length, ReadOnlySpan<ulong> valid, Span<ulong> mask)
    {
        ReadOnlySpan<T> values = FilterProgram.Values<T>(data, length);
        int words = Masks.Words(length);
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
                BinaryKernels.Views(test, views, buffers, length, valid, mask);
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
