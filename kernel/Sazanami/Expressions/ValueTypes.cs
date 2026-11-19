using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Kaminari.Sazanami.Expressions;

/// <summary>Builds something for the storage type <typeparamref name="T"/> of a fixed-width Arrow type (see <see cref="ValueTypes.Visit{TVisitor, TResult}"/>).</summary>
internal interface INumericVisitor<out TResult>
{
    /// <param name="floating">Whether the type is a floating one (Float16, Float32, Float64).</param>
    TResult Visit<T>(bool floating) where T : unmanaged, INumber<T>;
}

/// <summary>The value types of the expression and filter kernels: how Arrow types are stored, compared and computed with.</summary>
internal static class ValueTypes
{
    /// <summary>The value type of a column (a dictionary's values).</summary>
    public static IArrowType ValueType(IArrowType type) => type is DictionaryType d ? d.ValueType : type;

    public static bool IsFloating(IArrowType type) => type is HalfFloatType or FloatType or DoubleType;

    /// <summary>Strings and binaries (all their layouts but the large ones), compared by their bytes.</summary>
    public static bool IsBytes(IArrowType type) =>
        type is StringType or BinaryType or StringViewType or BinaryViewType or FixedSizeBinaryType and not (Decimal128Type or Decimal256Type);

    /// <summary>The types arithmetic takes: integers of 8 to 64 bits, Float32 and Float64.</summary>
    public static bool IsArithmetic(IArrowType type) =>
        type is Int8Type or UInt8Type or Int16Type or UInt16Type or Int32Type or UInt32Type or Int64Type or UInt64Type or FloatType or DoubleType;

    /// <summary>Signed integers and floating values (the types negation takes).</summary>
    public static bool IsSigned(IArrowType type) => type is Int8Type or Int16Type or Int32Type or Int64Type or FloatType or DoubleType;

    /// <summary>Whether values of <paramref name="a"/> and of <paramref name="b"/> are values of one type (dictionaries by their values; units, zones, scales and widths alike).</summary>
    public static bool Same(IArrowType a, IArrowType b) => (ValueType(a), ValueType(b)) switch
    {
        (TimestampType x, TimestampType y) => x.Unit == y.Unit && x.Timezone == y.Timezone,
        (Time32Type x, Time32Type y) => x.Unit == y.Unit,
        (Time64Type x, Time64Type y) => x.Unit == y.Unit,
        (DurationType x, DurationType y) => x.Unit == y.Unit,
        (Decimal128Type x, Decimal128Type y) => x.Scale == y.Scale,
        (FixedSizeBinaryType x, FixedSizeBinaryType y) => x.ByteWidth == y.ByteWidth,
        var (x, y) => x.TypeId == y.TypeId,
    };

    /// <summary>Whether <paramref name="type"/> is stored as fixed-width numbers (see <see cref="Visit{TVisitor, TResult}"/>).</summary>
    public static bool IsNumeric(IArrowType type) => ValueType(type) is Int8Type or UInt8Type or Int16Type or UInt16Type or Int32Type or Date32Type or Time32Type
        or UInt32Type or Int64Type or Date64Type or Time64Type or TimestampType or DurationType or UInt64Type or Decimal128Type or HalfFloatType or FloatType or DoubleType;

    /// <summary>
    /// Calls <paramref name="visitor"/> with the type the values of <paramref name="type"/> (a dictionary's values) are
    /// stored as: integers as themselves, dates, times, timestamps and durations as their 32- or 64-bit counts,
    /// Decimal128 as its unscaled <see cref="Int128"/>, floating values as themselves.
    /// </summary>
    /// <exception cref="ArgumentException">The type is not stored as fixed-width numbers.</exception>
    public static TResult Visit<TVisitor, TResult>(IArrowType type, TVisitor visitor) where TVisitor : INumericVisitor<TResult> => ValueType(type) switch
    {
        Int8Type => visitor.Visit<sbyte>(false),
        UInt8Type => visitor.Visit<byte>(false),
        Int16Type => visitor.Visit<short>(false),
        UInt16Type => visitor.Visit<ushort>(false),
        Int32Type or Date32Type or Time32Type => visitor.Visit<int>(false),
        UInt32Type => visitor.Visit<uint>(false),
        Int64Type or Date64Type or Time64Type or TimestampType or DurationType => visitor.Visit<long>(false),
        UInt64Type => visitor.Visit<ulong>(false),
        Decimal128Type => visitor.Visit<Int128>(false),
        HalfFloatType => visitor.Visit<Half>(true),
        FloatType => visitor.Visit<float>(true),
        DoubleType => visitor.Visit<double>(true),
        var other => throw new ArgumentException($"{other.Name} values are not numbers."),
    };

    /// <summary>A scalar as <typeparamref name="T"/>, exactly (else an error naming <paramref name="what"/>, the column or value it meets).</summary>
    /// <exception cref="ArgumentException">The scalar is of another kind, or <typeparamref name="T"/> cannot hold it exactly.</exception>
    public static T Exact<T>(StoredScalar value, bool floating, string what) where T : unmanaged, INumber<T>
    {
        if (floating)
        {
            if (value.Kind != StoredScalarKind.Float) throw new ArgumentException($"Floating {what} compares with a floating value, not {value}.");
            T v = T.CreateSaturating(value.FloatValue);
            if (double.IsNaN(value.FloatValue) || double.CreateSaturating(v) == value.FloatValue) return v;
            throw new ArgumentException($"{value} is not exactly a value of {what}.");
        }
        if (value.Kind != StoredScalarKind.Integer) throw new ArgumentException($"{what} compares with an integer value, not {value}.");
        try
        {
            return T.CreateChecked(value.IntegerValue);
        }
        catch (OverflowException)
        {
            throw new ArgumentException($"{value} is outside the values of {what}.");
        }
    }

    /// <summary>The <paramref name="length"/> values of a fixed-width array from its offset.</summary>
    public static ReadOnlySpan<T> Values<T>(ArrayData data, int length) where T : unmanaged =>
        data.Buffers[1].Span.CastTo<T>().Slice(data.Offset, length);

    /// <summary>Whether an array has null slots to look at (a validity bitmap and a null count).</summary>
    public static bool HasNulls(ArrayData data) => data.NullCount != 0 && data.Buffers.Length > 0 && !data.Buffers[0].IsEmpty;
}
