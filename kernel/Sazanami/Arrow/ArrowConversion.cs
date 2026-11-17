using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Filters;
using Kaminari.Sazanami.Kernels;

namespace Kaminari.Sazanami.Arrow;

/// <summary>
/// A conversion of arrays of one Arrow type into another, compiled by <see cref="ArrowWidening"/> once per pair of
/// types and applied to every array. The result lives in native memory of its own (as the arrays of
/// <see cref="NativeBatchExport"/>); parts that do not change (struct fields, a dictionary's values, buffers kept as
/// they are) are shared with the source when its memory is a native batch's, else copied. The source must stay alive
/// while it is converted; the caller disposes it afterwards.
/// </summary>
internal abstract unsafe class ArrowConversion(IArrowType target)
{
    /// <summary>The type of the arrays made.</summary>
    public IArrowType Target { get; } = target;

    /// <summary>Whether arrays come out as they went in.</summary>
    public virtual bool IsIdentity => false;

    /// <summary>The array converted; <paramref name="source"/> itself when nothing changes (<see cref="IsIdentity"/>).</summary>
    /// <exception cref="OverflowException">A value is beyond the target's range (a timestamp into a finer unit).</exception>
    public IArrowArray Convert(IArrowArray source)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (IsIdentity) return source;
        var export = new NativeBatchExport();
        try
        {
            return export.ImportArray(Build(export, source.Data), Target);
        }
        catch
        {
            export.FreeAll();
            throw;
        }
    }

    /// <summary>The converted array of <paramref name="source"/> (its offset and length) in the export's memory.</summary>
    internal abstract NativeCArray* Build(NativeBatchExport export, ArrayData source);

    // ---- shared helpers -------------------------------------------------------------------------------------------

    /// <summary>The source as it is: its buffers shared with it (or copied), its offset kept.</summary>
    internal static NativeCArray* Share(NativeBatchExport export, ArrayData data)
    {
        var pointers = new nint[data.Buffers.Length];
        for (int b = 0; b < pointers.Length; b++)
        {
            ArrowBuffer buffer = data.Buffers[b];
            if (buffer.IsEmpty || export.TryShare(buffer, out pointers[b])) continue;
            byte* copy = (byte*)export.Allocate(buffer.Length);
            buffer.Span.CopyTo(new Span<byte>(copy, buffer.Length));
            pointers[b] = (nint)copy;
        }
        var children = new nint[data.Children?.Length ?? 0];
        for (int c = 0; c < children.Length; c++) children[c] = (nint)Share(export, data.Children![c]);
        NativeCArray* dictionary = data.Dictionary is null ? null : Share(export, data.Dictionary);
        NativeCArray* array = export.NewArray(data.Length, NullCount(data), pointers, children, dictionary);
        array->Offset = data.Offset;
        return array;
    }

    /// <summary>The source's validity from bit 0 (null when every slot is valid), and its null count.</summary>
    internal static (nint Bits, long Nulls) Validity(NativeBatchExport export, ArrayData data)
    {
        int nulls = NullCount(data);
        if (nulls == 0) return (0, 0);
        long bytes = Masks.Words(data.Length) * 8L;
        byte* bits = (byte*)export.Allocate(bytes);
        Bits.Copy(data.Buffers[0].Span, data.Offset, new Span<byte>(bits, (int)bytes), 0, data.Length);
        return ((nint)bits, nulls);
    }

    /// <summary>The null slots of an array (counted when not known).</summary>
    internal static int NullCount(ArrayData data)
    {
        if (data.DataType is NullType) return data.Length;
        if (data.Buffers.Length == 0 || data.Buffers[0].IsEmpty) return 0;
        if (data.NullCount >= 0) return data.NullCount;
        return data.Length - (int)Bits.Count(data.Buffers[0].Span, data.Offset, data.Length);
    }

    /// <summary>Whether slot <paramref name="index"/> of the source is valid.</summary>
    internal static bool IsValid(ArrayData data, int index) =>
        data.Buffers.Length == 0 || data.Buffers[0].IsEmpty || Bits.Get(data.Buffers[0].Span, data.Offset + index);

    /// <summary>The source's values as <typeparamref name="T"/>, from its offset.</summary>
    internal static ReadOnlySpan<T> Values<T>(ArrayData data) where T : unmanaged =>
        MemoryMarshal.Cast<byte, T>(data.Buffers[1].Span).Slice(data.Offset, data.Length);
}

/// <summary>Arrays as they are.</summary>
internal sealed unsafe class IdentityConversion(IArrowType target) : ArrowConversion(target)
{
    public override bool IsIdentity => true;

    internal override NativeCArray* Build(NativeBatchExport export, ArrayData source) => Share(export, source);
}

/// <summary>Arrays of nulls of the target type, as long as the source (a field the source lacks, the null type).</summary>
internal sealed unsafe class NullsConversion(IArrowType target) : ArrowConversion(target)
{
    internal override NativeCArray* Build(NativeBatchExport export, ArrayData source) => NullArrays.Build(export, Target, source.Length);
}

/// <summary>Numbers into numbers (integers, floats): each value converted as the CLR converts it.</summary>
internal static class NumericConversion
{
    public static ArrowConversion Create(IArrowType from, IArrowType to) => from switch
    {
        Int8Type => To<sbyte>(to),
        UInt8Type => To<byte>(to),
        Int16Type => To<short>(to),
        UInt16Type => To<ushort>(to),
        Int32Type => To<int>(to),
        UInt32Type => To<uint>(to),
        Int64Type => To<long>(to),
        UInt64Type => To<ulong>(to),
        HalfFloatType => To<Half>(to),
        FloatType => To<float>(to),
        DoubleType => To<double>(to),
        _ => throw new NotSupportedException(from.Name),
    };

    private static ArrowConversion To<TFrom>(IArrowType to) where TFrom : unmanaged, INumberBase<TFrom> => to switch
    {
        Int8Type => new NumericConversion<TFrom, sbyte>(to),
        UInt8Type => new NumericConversion<TFrom, byte>(to),
        Int16Type => new NumericConversion<TFrom, short>(to),
        UInt16Type => new NumericConversion<TFrom, ushort>(to),
        Int32Type => new NumericConversion<TFrom, int>(to),
        UInt32Type => new NumericConversion<TFrom, uint>(to),
        Int64Type => new NumericConversion<TFrom, long>(to),
        UInt64Type => new NumericConversion<TFrom, ulong>(to),
        HalfFloatType => new NumericConversion<TFrom, Half>(to),
        FloatType => new NumericConversion<TFrom, float>(to),
        DoubleType => new NumericConversion<TFrom, double>(to),
        _ => throw new NotSupportedException(to.Name),
    };
}

internal sealed unsafe class NumericConversion<TFrom, TTo>(IArrowType target) : ArrowConversion(target)
    where TFrom : unmanaged, INumberBase<TFrom>
    where TTo : unmanaged, INumberBase<TTo>
{
    internal override NativeCArray* Build(NativeBatchExport export, ArrayData source)
    {
        var (validity, nulls) = Validity(export, source);
        var values = (TTo*)export.Allocate((long)source.Length * sizeof(TTo));
        NumericKernels.Convert(Values<TFrom>(source), new Span<TTo>(values, source.Length));
        return export.NewArray(source.Length, nulls, [validity, (nint)values], []);
    }
}

/// <summary>
/// Integers of a time unit into another (dates into timestamps too): multiplied (failing beyond the target's range,
/// null slots aside) or divided rounding down.
/// </summary>
internal sealed unsafe class ScaleConversion(IArrowType target, int sourceWidth, int targetWidth, long multiply, long divide) : ArrowConversion(target)
{
    internal override NativeCArray* Build(NativeBatchExport export, ArrayData source)
    {
        var (validity, nulls) = Validity(export, source);
        int length = source.Length;
        void* values = export.Allocate((long)length * targetWidth);
        ReadOnlySpan<int> narrow = sourceWidth == 4 ? Values<int>(source) : default;
        ReadOnlySpan<long> wide = sourceWidth == 8 ? Values<long>(source) : default;
        for (int i = 0; i < length; i++)
        {
            long value = sourceWidth == 4 ? narrow[i] : wide[i];
            long result;
            if (multiply != 1)
            {
                long high = Math.BigMul(value, multiply, out long low);
                if (high != low >> 63)
                {
                    if (IsValid(source, i))
                        throw new OverflowException($"{value} is beyond the range of {ArrowTypes.Describe(Target)}.");
                    low = 0;
                }
                result = low;
            }
            else
            {
                result = Math.DivRem(value, divide, out long remainder);
                if (remainder < 0) result--;
            }
            if (targetWidth == 4) ((int*)values)[i] = (int)result;
            else ((long*)values)[i] = result;
        }
        return export.NewArray(length, nulls, [validity, (nint)values], []);
    }
}

/// <summary>Integers and decimals into decimals of no smaller scale and no fewer integer digits.</summary>
internal sealed unsafe class DecimalConversion : ArrowConversion
{
    private readonly IArrowType _source;
    private readonly int _shift;

    public DecimalConversion(IArrowType source, IArrowType target) : base(target)
    {
        _source = source;
        int sourceScale = source is IntegerType ? 0 : ArrowTypes.DecimalParameters(source).Scale;
        _shift = ArrowTypes.DecimalParameters(target).Scale - sourceScale;
    }

    internal override NativeCArray* Build(NativeBatchExport export, ArrayData source)
    {
        var (validity, nulls) = Validity(export, source);
        int length = source.Length;
        if (Target is Decimal128Type)
        {
            var values = (Int128*)export.Allocate(length * 16L);
            Int128 factor = Decimals.Pow10(_shift);
            for (int i = 0; i < length; i++) values[i] = checked(Decimals.Read(_source, source, i) * factor);
            return export.NewArray(length, nulls, [validity, (nint)values], []);
        }
        var wide = (byte*)export.Allocate(length * 32L);
        BigInteger wideFactor = BigInteger.Pow(10, _shift);
        for (int i = 0; i < length; i++) Decimals.Write256(Decimals.ReadBig(_source, source, i) * wideFactor, new Span<byte>(wide + 32L * i, 32));
        return export.NewArray(length, nulls, [validity, (nint)wide], []);
    }
}

/// <summary>Decimals into floats: the nearest float (exact for integers the float holds).</summary>
internal sealed unsafe class DecimalToFloatConversion(IArrowType source, IArrowType target) : ArrowConversion(target)
{
    private readonly int _scale = ArrowTypes.DecimalParameters(source).Scale;

    internal override NativeCArray* Build(NativeBatchExport export, ArrayData data)
    {
        var (validity, nulls) = Validity(export, data);
        int length = data.Length, width = Target switch { HalfFloatType => 2, FloatType => 4, _ => 8 };
        var values = (byte*)export.Allocate((long)length * width);
        for (int i = 0; i < length; i++)
        {
            double value = ToDouble(Decimals.ReadBig(source, data, i), _scale);
            switch (width)
            {
                case 2: ((Half*)values)[i] = (Half)value; break;
                case 4: ((float*)values)[i] = (float)value; break;
                default: ((double*)values)[i] = value; break;
            }
        }
        return export.NewArray(length, nulls, [validity, (nint)values], []);

        // Correctly rounded: exact operands divide exactly-rounded; else through the decimal text.
        static double ToDouble(BigInteger unscaled, int scale)
        {
            if (scale <= 22 && BigInteger.Abs(unscaled) <= (BigInteger.One << 53)) return (double)unscaled / Math.Pow(10, scale);
            return double.Parse($"{unscaled}e-{scale}", System.Globalization.CultureInfo.InvariantCulture);
        }
    }
}

/// <summary>Structs into structs: the target's fields by name from the source's (converted), missing ones null.</summary>
internal sealed unsafe class StructConversion(IArrowType target, (int Source, ArrowConversion Conversion)[] fields) : ArrowConversion(target)
{
    internal override NativeCArray* Build(NativeBatchExport export, ArrayData source)
    {
        var (validity, nulls) = Validity(export, source);
        var children = new nint[fields.Length];
        for (int i = 0; i < fields.Length; i++)
        {
            var (from, conversion) = fields[i];
            children[i] = from < 0
                ? (nint)NullArrays.Build(export, conversion.Target, source.Length)
                : (nint)conversion.Build(export, source.Children[from].Slice(source.Offset, source.Length));
        }
        return export.NewArray(source.Length, nulls, [validity], children);
    }
}

/// <summary>Lists (and maps, lists of entries) into lists of converted elements: the offsets rebased to 0.</summary>
internal sealed unsafe class ListConversion(IArrowType target, ArrowConversion elements) : ArrowConversion(target)
{
    internal override NativeCArray* Build(NativeBatchExport export, ArrayData source)
    {
        var (validity, nulls) = Validity(export, source);
        int length = source.Length;
        ReadOnlySpan<int> offsets = MemoryMarshal.Cast<byte, int>(source.Buffers[1].Span).Slice(source.Offset, length + 1);
        var rebased = (int*)export.Allocate((length + 1L) * sizeof(int));
        int first = offsets[0];
        for (int i = 0; i <= length; i++) rebased[i] = offsets[i] - first;
        NativeCArray* child = elements.Build(export, source.Children[0].Slice(first, offsets[length] - first));
        return export.NewArray(length, nulls, [validity, (nint)rebased], [(nint)child]);
    }
}

/// <summary>Fixed-size lists into fixed-size lists of converted elements.</summary>
internal sealed unsafe class FixedSizeListConversion(FixedSizeListType target, ArrowConversion elements) : ArrowConversion(target)
{
    internal override NativeCArray* Build(NativeBatchExport export, ArrayData source)
    {
        var (validity, nulls) = Validity(export, source);
        int size = target.ListSize;
        NativeCArray* child = elements.Build(export, source.Children[0].Slice(source.Offset * size, source.Length * size));
        return export.NewArray(source.Length, nulls, [validity], [(nint)child]);
    }
}

/// <summary>A dictionary of strings or binaries into its values' type: each row's value.</summary>
internal sealed unsafe class DictionaryDecodeConversion(IArrowType target) : ArrowConversion(target)
{
    internal override NativeCArray* Build(NativeBatchExport export, ArrayData source)
    {
        IArrowArray values = ArrowArrayFactory.BuildArray(source.Dictionary!);
        var indexType = (IntegerType)((DictionaryType)source.DataType).IndexType;
        using NativeColumnBuilder builder = NativeColumnBuilder.Create(Target);
        builder.Reserve(source.Length);
        for (int i = 0; i < source.Length; i++)
        {
            if (!IsValid(source, i))
            {
                builder.AppendNull();
                continue;
            }
            int index = Index(source, indexType, i);
            if (values.IsNull(index))
            {
                builder.AppendNull();
                continue;
            }
            ReadOnlySpan<byte> bytes = values switch
            {
                BinaryArray b => b.GetBytes(index),
                BinaryViewArray b => b.GetBytes(index),
                _ => throw new NotSupportedException(values.Data.DataType.Name),
            };
            Append(builder, bytes);
        }
        return builder.Export(export);

        static void Append(NativeColumnBuilder builder, ReadOnlySpan<byte> bytes)
        {
            if (builder is NativeBinaryViewBuilder views) views.Append(bytes);
            else ((NativeBinaryBuilder)builder).Append(bytes);
        }

        static int Index(ArrayData data, IntegerType type, int i) => type.BitWidth switch
        {
            8 => type.IsSigned ? Values<sbyte>(data)[i] : Values<byte>(data)[i],
            16 => type.IsSigned ? Values<short>(data)[i] : Values<ushort>(data)[i],
            32 => type.IsSigned ? Values<int>(data)[i] : checked((int)Values<uint>(data)[i]),
            _ => checked((int)(type.IsSigned ? Values<long>(data)[i] : (long)Values<ulong>(data)[i])),
        };
    }
}

/// <summary>
/// Strings or binaries into a dictionary of them: the values themselves (in the dictionary's layout), indexed in
/// order (0, 1, 2, …).
/// </summary>
internal sealed unsafe class DictionaryEncodeConversion(DictionaryType target) : ArrowConversion(target)
{
    internal override NativeCArray* Build(NativeBatchExport export, ArrayData source)
    {
        if (target.IndexType is not Int32Type) throw new NotSupportedException($"Dictionaries indexed by {target.IndexType.Name}.");
        var (validity, nulls) = Validity(export, source);
        var indices = (int*)export.Allocate(source.Length * 4L);
        for (int i = 0; i < source.Length; i++) indices[i] = i;
        NativeCArray* dictionary = ArrowTypes.SameType(source.DataType, target.ValueType)
            ? Share(export, source)
            : RelayoutConversion.Copy(export, source, target.ValueType);
        return export.NewArray(source.Length, nulls, [validity, (nint)indices], [], dictionary);
    }
}

/// <summary>Strings (binaries) from the plain layout into the view layout or back: the values copied.</summary>
internal sealed unsafe class RelayoutConversion(IArrowType target) : ArrowConversion(target)
{
    internal override NativeCArray* Build(NativeBatchExport export, ArrayData source) => Copy(export, source, Target);

    /// <summary>The values of <paramref name="source"/> appended one by one to an array of <paramref name="target"/>.</summary>
    internal static NativeCArray* Copy(NativeBatchExport export, ArrayData source, IArrowType target)
    {
        IArrowArray values = ArrowArrayFactory.BuildArray(source);
        using NativeColumnBuilder builder = NativeColumnBuilder.Create(target);
        builder.Reserve(source.Length);
        for (int i = 0; i < source.Length; i++)
        {
            if (values.IsNull(i))
            {
                builder.AppendNull();
                continue;
            }
            ReadOnlySpan<byte> bytes = values switch
            {
                BinaryArray b => b.GetBytes(i),
                BinaryViewArray b => b.GetBytes(i),
                _ => throw new NotSupportedException(source.DataType.Name),
            };
            if (builder is NativeBinaryViewBuilder views) views.Append(bytes);
            else ((NativeBinaryBuilder)builder).Append(bytes);
        }
        return builder.Export(export);
    }
}

/// <summary>Arrays holding only nulls, of any type.</summary>
internal static unsafe class NullArrays
{
    /// <summary><paramref name="length"/> nulls of <paramref name="type"/> (struct fields null too, lists empty), in native memory.</summary>
    public static IArrowArray Of(IArrowType type, int length)
    {
        ArgumentNullException.ThrowIfNull(type);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        if (type is NullType) return new NullArray(length);
        var export = new NativeBatchExport();
        try
        {
            return export.ImportArray(Build(export, type, length), type);
        }
        catch
        {
            export.FreeAll();
            throw;
        }
    }

    internal static NativeCArray* Build(NativeBatchExport export, IArrowType type, int length)
    {
        using NativeColumnBuilder builder = NativeColumnBuilder.Create(type);
        builder.Reserve(length);
        for (int i = 0; i < length; i++) builder.AppendNull();
        return builder.Export(export);
    }
}

/// <summary>Decimal values read and written as integers.</summary>
internal static class Decimals
{
    private static readonly Int128[] Powers = [.. Enumerable.Range(0, 39).Select(Power)];

    public static Int128 Pow10(int exponent) => Powers[exponent];

    private static Int128 Power(int exponent)
    {
        Int128 p = 1;
        for (int i = 0; i < exponent; i++) p *= 10;
        return p;
    }

    /// <summary>The unscaled value of slot <paramref name="i"/> of an integer or Decimal128 array.</summary>
    public static Int128 Read(IArrowType type, ArrayData data, int i) => type switch
    {
        Int8Type => ArrowConversion.Values<sbyte>(data)[i],
        UInt8Type => ArrowConversion.Values<byte>(data)[i],
        Int16Type => ArrowConversion.Values<short>(data)[i],
        UInt16Type => ArrowConversion.Values<ushort>(data)[i],
        Int32Type => ArrowConversion.Values<int>(data)[i],
        UInt32Type => ArrowConversion.Values<uint>(data)[i],
        Int64Type => ArrowConversion.Values<long>(data)[i],
        UInt64Type => ArrowConversion.Values<ulong>(data)[i],
        Decimal128Type => ArrowConversion.Values<Int128>(data)[i],
        _ => throw new NotSupportedException(type.Name),
    };

    /// <summary>The unscaled value of slot <paramref name="i"/> of an integer or decimal array.</summary>
    public static BigInteger ReadBig(IArrowType type, ArrayData data, int i) => type is Decimal256Type
        ? new BigInteger(data.Buffers[1].Span.Slice((data.Offset + i) * 32, 32), isUnsigned: false, isBigEndian: false)
        : (BigInteger)Read(type, data, i);

    /// <summary>Writes a value as 32 bytes of two's complement, little-endian.</summary>
    public static void Write256(BigInteger value, Span<byte> target)
    {
        target.Fill(value.Sign < 0 ? (byte)0xFF : (byte)0);
        if (!value.TryWriteBytes(target, out _, isUnsigned: false, isBigEndian: false))
            throw new OverflowException($"{value} does not fit a 256-bit decimal.");
    }
}

/// <summary>Number conversions over spans: portable vector paths for the common widenings, the CLR's conversion otherwise.</summary>
internal static class NumericKernels
{
    public static void Convert<TFrom, TTo>(ReadOnlySpan<TFrom> source, Span<TTo> target)
        where TFrom : unmanaged, INumberBase<TFrom>
        where TTo : unmanaged, INumberBase<TTo>
    {
        int done = 0;
        if (Vector128.IsHardwareAccelerated)
        {
            if (typeof(TFrom) == typeof(int) && typeof(TTo) == typeof(long))
                done = WidenInt32(Cast<TFrom, int>(source), Cast<TTo, long>(target));
            else if (typeof(TFrom) == typeof(uint) && (typeof(TTo) == typeof(long) || typeof(TTo) == typeof(ulong)))
                done = WidenUInt32(Cast<TFrom, uint>(source), Cast<TTo, ulong>(target));
            else if (typeof(TFrom) == typeof(short) && typeof(TTo) == typeof(int))
                done = WidenInt16(Cast<TFrom, short>(source), Cast<TTo, int>(target));
            else if (typeof(TFrom) == typeof(float) && typeof(TTo) == typeof(double))
                done = WidenSingle(Cast<TFrom, float>(source), Cast<TTo, double>(target));
            else if (typeof(TFrom) == typeof(int) && typeof(TTo) == typeof(double))
                done = Int32ToDouble(Cast<TFrom, int>(source), Cast<TTo, double>(target));
            else if (typeof(TFrom) == typeof(long) && typeof(TTo) == typeof(double))
                done = Int64ToDouble(Cast<TFrom, long>(source), Cast<TTo, double>(target));
            else if (typeof(TFrom) == typeof(double) && typeof(TTo) == typeof(float))
                done = NarrowDouble(Cast<TFrom, double>(source), Cast<TTo, float>(target));
        }
        for (int i = done; i < source.Length; i++) target[i] = TTo.CreateTruncating(source[i]);
    }

    private static Span<TTo> Cast<TFrom, TTo>(Span<TFrom> span) where TFrom : unmanaged where TTo : unmanaged => MemoryMarshal.Cast<TFrom, TTo>(span);

    private static ReadOnlySpan<TTo> Cast<TFrom, TTo>(ReadOnlySpan<TFrom> span) where TFrom : unmanaged where TTo : unmanaged => MemoryMarshal.Cast<TFrom, TTo>(span);

    private static int WidenInt32(ReadOnlySpan<int> source, Span<long> target)
    {
        int i = 0;
        ref int s = ref MemoryMarshal.GetReference(source);
        ref long t = ref MemoryMarshal.GetReference(target);
        for (; i + Vector128<int>.Count <= source.Length; i += Vector128<int>.Count)
        {
            Vector128<int> v = Vector128.LoadUnsafe(ref s, (nuint)i);
            Vector128.WidenLower(v).StoreUnsafe(ref t, (nuint)i);
            Vector128.WidenUpper(v).StoreUnsafe(ref t, (nuint)(i + 2));
        }
        return i;
    }

    private static int WidenUInt32(ReadOnlySpan<uint> source, Span<ulong> target)
    {
        int i = 0;
        ref uint s = ref MemoryMarshal.GetReference(source);
        ref ulong t = ref MemoryMarshal.GetReference(target);
        for (; i + Vector128<uint>.Count <= source.Length; i += Vector128<uint>.Count)
        {
            Vector128<uint> v = Vector128.LoadUnsafe(ref s, (nuint)i);
            Vector128.WidenLower(v).StoreUnsafe(ref t, (nuint)i);
            Vector128.WidenUpper(v).StoreUnsafe(ref t, (nuint)(i + 2));
        }
        return i;
    }

    private static int WidenInt16(ReadOnlySpan<short> source, Span<int> target)
    {
        int i = 0;
        ref short s = ref MemoryMarshal.GetReference(source);
        ref int t = ref MemoryMarshal.GetReference(target);
        for (; i + Vector128<short>.Count <= source.Length; i += Vector128<short>.Count)
        {
            Vector128<short> v = Vector128.LoadUnsafe(ref s, (nuint)i);
            Vector128.WidenLower(v).StoreUnsafe(ref t, (nuint)i);
            Vector128.WidenUpper(v).StoreUnsafe(ref t, (nuint)(i + 4));
        }
        return i;
    }

    private static int WidenSingle(ReadOnlySpan<float> source, Span<double> target)
    {
        int i = 0;
        ref float s = ref MemoryMarshal.GetReference(source);
        ref double t = ref MemoryMarshal.GetReference(target);
        for (; i + Vector128<float>.Count <= source.Length; i += Vector128<float>.Count)
        {
            Vector128<float> v = Vector128.LoadUnsafe(ref s, (nuint)i);
            Vector128.WidenLower(v).StoreUnsafe(ref t, (nuint)i);
            Vector128.WidenUpper(v).StoreUnsafe(ref t, (nuint)(i + 2));
        }
        return i;
    }

    private static int Int32ToDouble(ReadOnlySpan<int> source, Span<double> target)
    {
        int i = 0;
        ref int s = ref MemoryMarshal.GetReference(source);
        ref double t = ref MemoryMarshal.GetReference(target);
        for (; i + Vector128<int>.Count <= source.Length; i += Vector128<int>.Count)
        {
            Vector128<int> v = Vector128.LoadUnsafe(ref s, (nuint)i);
            Vector128.ConvertToDouble(Vector128.WidenLower(v)).StoreUnsafe(ref t, (nuint)i);
            Vector128.ConvertToDouble(Vector128.WidenUpper(v)).StoreUnsafe(ref t, (nuint)(i + 2));
        }
        return i;
    }

    private static int Int64ToDouble(ReadOnlySpan<long> source, Span<double> target)
    {
        int i = 0;
        ref long s = ref MemoryMarshal.GetReference(source);
        ref double t = ref MemoryMarshal.GetReference(target);
        for (; i + Vector128<long>.Count <= source.Length; i += Vector128<long>.Count)
            Vector128.ConvertToDouble(Vector128.LoadUnsafe(ref s, (nuint)i)).StoreUnsafe(ref t, (nuint)i);
        return i;
    }

    private static int NarrowDouble(ReadOnlySpan<double> source, Span<float> target)
    {
        int i = 0;
        ref double s = ref MemoryMarshal.GetReference(source);
        ref float t = ref MemoryMarshal.GetReference(target);
        for (; i + 2 * Vector128<double>.Count <= source.Length; i += 2 * Vector128<double>.Count)
        {
            Vector128<double> lower = Vector128.LoadUnsafe(ref s, (nuint)i), upper = Vector128.LoadUnsafe(ref s, (nuint)(i + 2));
            Vector128.Narrow(lower, upper).StoreUnsafe(ref t, (nuint)i);
        }
        return i;
    }
}
