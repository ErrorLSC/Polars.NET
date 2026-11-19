using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Filters;
using Kaminari.Sazanami.Kernels;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Arrow.TypeSystem;

namespace Kaminari.Sazanami.Arrow.Compute;

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

/// <summary>
/// Lists of any layout (list, large list, list view, large list view; maps as lists of entries) into lists of any
/// layout, their elements converted. A view's elements are gathered row by row into a list (views may overlap or come
/// out of order); between layouts of one kind the elements are converted as a slice.
/// </summary>
internal sealed unsafe class ListConversion(IArrowType target, ArrowConversion elements) : ArrowConversion(target)
{
    internal override NativeCArray* Build(NativeBatchExport export, ArrayData source)
    {
        var (validity, nulls) = Validity(export, source);
        int length = source.Length;
        bool sourceView = source.DataType is ListViewType or LargeListViewType;
        bool sourceLarge = source.DataType is LargeListType or LargeListViewType;
        bool targetView = Target is ListViewType or LargeListViewType;
        bool targetLarge = Target is LargeListType or LargeListViewType;
        int width = targetLarge ? 8 : 4;
        // Offsets (length + 1 of them, or one per row for a view) and a view's sizes, in the target's width.
        var offsets = (byte*)export.AllocateUninitialized((length + 1L) * width);
        byte* sizes = targetView ? (byte*)export.AllocateUninitialized(Math.Max(1, length) * (long)width) : null;
        IntegerType width64 = sourceLarge ? Int64Type.Default : Int32Type.Default;
        var offsetsOf = new Slots(source, 1, width64);
        Slots counts = sourceView ? new Slots(source, 2, width64) : default;
        ArrayData child;
        if (!sourceView)
        {
            long first = offsetsOf[0];
            for (int i = 0; i < length; i++)
            {
                long start = offsetsOf[i], end = offsetsOf[i + 1];
                Put(offsets, targetLarge, i, start - first);
                if (targetView) Put(sizes, targetLarge, i, end - start);
            }
            long last = offsetsOf[length];
            if (!targetView) Put(offsets, targetLarge, length, last - first);
            child = source.Children[0].Slice(checked((int)first), checked((int)(last - first)));
        }
        else if (targetView)
        {
            // Views into views: the same rows over the whole child (a null row's offset and size are not looked at).
            for (int i = 0; i < length; i++)
            {
                bool valid = offsetsOf.IsValid(i);
                Put(offsets, targetLarge, i, valid ? offsetsOf[i] : 0);
                Put(sizes, targetLarge, i, valid ? counts[i] : 0);
            }
            child = source.Children[0];
        }
        else
        {
            // Views into a list: each row's elements gathered after the previous row's.
            long total = 0;
            for (int i = 0; i < length; i++)
            {
                Put(offsets, targetLarge, i, total);
                if (offsetsOf.IsValid(i)) total += counts[i];
            }
            Put(offsets, targetLarge, length, total);
            using var rows = new Buffers.NativeArray<int>(checked((int)total));
            for (int i = 0, at = 0; i < length; i++)
            {
                if (!offsetsOf.IsValid(i)) continue;
                long start = offsetsOf[i], size = counts[i];
                for (long k = 0; k < size; k++) rows[at++] = checked((int)(start + k));
            }
            IArrowArray gathered = ArrowTake.Take(ArrowArrayFactory.BuildArray(source.Children[0]), rows.AsSpan(0, (int)total));
            export.Hold(gathered);
            child = gathered.Data;
        }
        NativeCArray* converted = elements.Build(export, child);
        return targetView
            ? export.NewArray(length, nulls, [validity, (nint)offsets, (nint)sizes], [(nint)converted])
            : export.NewArray(length, nulls, [validity, (nint)offsets], [(nint)converted]);

        // A 32-bit target refuses elements past int.MaxValue.
        static void Put(byte* buffer, bool large, int i, long value)
        {
            if (large) ((long*)buffer)[i] = value;
            else ((int*)buffer)[i] = value <= int.MaxValue ? (int)value : throw new OverflowException("A list column holds more than 2^31 elements in one batch.");
        }
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

/// <summary>A dictionary into its values' type (converted by <c>values</c>): each row's value, taken from the dictionary.</summary>
internal sealed unsafe class DictionaryDecodeConversion(IArrowType target, ArrowConversion values) : ArrowConversion(target)
{
    internal override NativeCArray* Build(NativeBatchExport export, ArrayData source)
    {
        var indexType = (IntegerType)((DictionaryType)source.DataType).IndexType;
        int entries = source.Dictionary!.Length;
        using var rows = new Buffers.NativeArray<int>(Math.Max(1, source.Length));
        var slots = new Slots(source, 1, indexType);
        for (int i = 0; i < source.Length; i++)
        {
            if (!slots.IsValid(i))
            {
                rows[i] = ArrowTake.NullRow;
                continue;
            }
            long index = slots[i];
            rows[i] = (ulong)index < (ulong)entries ? (int)index : throw new InvalidDataException($"Dictionary index {index} is past its {entries} entries.");
        }
        IArrowArray taken = ArrowTake.Take(ArrowArrayFactory.BuildArray(source.Dictionary), rows.AsSpan(0, source.Length));
        export.Hold(taken);
        return values.Build(export, taken.Data);
    }
}

/// <summary>
/// The slots of an array read row by row without going back to its buffers each time: whether a row is valid, and an
/// integer of a fixed-width buffer (offsets, sizes, dictionary indices) at the row.
/// </summary>
internal readonly ref struct Slots
{
    private readonly ReadOnlySpan<byte> _valid;
    private readonly ReadOnlySpan<byte> _values;
    private readonly int _offset;
    private readonly int _width;
    private readonly bool _signed;

    /// <param name="data">The array.</param>
    /// <param name="buffer">The buffer of integers (1: offsets or indices, 2: a view's sizes).</param>
    /// <param name="type">Their type.</param>
    public Slots(ArrayData data, int buffer, IntegerType type)
    {
        _valid = data.Buffers.Length == 0 || data.Buffers[0].IsEmpty ? default : data.Buffers[0].Span;
        _values = data.Buffers[buffer].Span;
        _offset = data.Offset;
        _width = type.BitWidth / 8;
        _signed = type.IsSigned;
    }

    public bool IsValid(int i) => _valid.IsEmpty || (_valid[(_offset + i) >> 3] >> ((_offset + i) & 7) & 1) != 0;

    public long this[int i]
    {
        get
        {
            int at = (_offset + i) * _width;
            return (_width, _signed) switch
            {
                (1, true) => (sbyte)_values[at],
                (1, false) => _values[at],
                (2, true) => System.Buffers.Binary.BinaryPrimitives.ReadInt16LittleEndian(_values[at..]),
                (2, false) => System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(_values[at..]),
                (4, true) => System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(_values[at..]),
                (4, false) => System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(_values[at..]),
                (_, true) => System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(_values[at..]),
                _ => checked((long)System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(_values[at..])),
            };
        }
    }
}

/// <summary>A dictionary's indices read as numbers.</summary>
internal static class DictionaryIndices
{
    /// <summary>The index of row <paramref name="i"/> (from the array's offset).</summary>
    public static long Read(ArrayData data, IntegerType type, int i) => type.BitWidth switch
    {
        8 => type.IsSigned ? Values<sbyte>(data)[i] : Values<byte>(data)[i],
        16 => type.IsSigned ? Values<short>(data)[i] : Values<ushort>(data)[i],
        32 => type.IsSigned ? Values<int>(data)[i] : Values<uint>(data)[i],
        _ => type.IsSigned ? Values<long>(data)[i] : checked((long)Values<ulong>(data)[i]),
    };

    private static ReadOnlySpan<T> Values<T>(ArrayData data) where T : unmanaged => data.Buffers[1].Span.CastTo<T>().Slice(data.Offset, data.Length);
}

/// <summary>
/// A dictionary into one with wider indices (or the same) and its values converted: the indices widened, the
/// dictionary converted (shared with the source when its values stay as they are).
/// </summary>
internal sealed unsafe class DictionaryIndexConversion(DictionaryType target, ArrowConversion values) : ArrowConversion(target)
{
    internal override NativeCArray* Build(NativeBatchExport export, ArrayData source)
    {
        var (validity, nulls) = Validity(export, source);
        var from = (IntegerType)((DictionaryType)source.DataType).IndexType;
        int width = ((IntegerType)target.IndexType).BitWidth / 8;
        var indices = (byte*)export.Allocate(Math.Max(1, source.Length) * (long)width);
        var slots = new Slots(source, 1, from);
        for (int i = 0; i < source.Length; i++)
        {
            long index = slots.IsValid(i) ? slots[i] : 0;
            switch (width)
            {
                case 1: ((sbyte*)indices)[i] = (sbyte)index; break;
                case 2: ((short*)indices)[i] = (short)index; break;
                case 4: ((int*)indices)[i] = (int)index; break;
                default: ((long*)indices)[i] = index; break;
            }
        }
        NativeCArray* dictionary = values.Build(export, source.Dictionary!);
        return export.NewArray(source.Length, nulls, [validity, (nint)indices], [], dictionary);
    }
}

/// <summary>
/// Values into a dictionary of them (converted by <c>values</c>): the values themselves, indexed in order (0, 1,
/// 2, …); the index type must reach the batch's length.
/// </summary>
internal sealed unsafe class DictionaryEncodeConversion(DictionaryType target, ArrowConversion values) : ArrowConversion(target)
{
    internal override NativeCArray* Build(NativeBatchExport export, ArrayData source)
    {
        var index = (IntegerType)target.IndexType;
        int width = index.BitWidth / 8;
        long most = width switch { 1 => index.IsSigned ? sbyte.MaxValue : byte.MaxValue, 2 => index.IsSigned ? short.MaxValue : ushort.MaxValue, _ => int.MaxValue };
        if (source.Length - 1 > most)
            throw new OverflowException($"{source.Length} values cannot be indexed by the dictionary's {index.Name} indices.");
        var (validity, nulls) = Validity(export, source);
        var indices = (byte*)export.Allocate(Math.Max(1, source.Length) * (long)width);
        for (int i = 0; i < source.Length; i++)
        {
            switch (width)
            {
                case 1: indices[i] = (byte)i; break;
                case 2: ((short*)indices)[i] = (short)i; break;
                case 4: ((int*)indices)[i] = i; break;
                default: ((long*)indices)[i] = i; break;
            }
        }
        NativeCArray* dictionary = values.Build(export, source);
        return export.NewArray(source.Length, nulls, [validity, (nint)indices], [], dictionary);
    }
}

/// <summary>
/// Strings (binaries) from one layout into another (plain, large, view): between plain and large the offsets are
/// rewritten in the other width and the value bytes shared; otherwise the values are copied.
/// </summary>
internal sealed unsafe class RelayoutConversion(IArrowType target) : ArrowConversion(target)
{
    internal override NativeCArray* Build(NativeBatchExport export, ArrayData source) =>
        (source.DataType, Target) switch
        {
            (StringType or BinaryType, LargeStringType or LargeBinaryType) => Widen(export, source),
            (LargeStringType or LargeBinaryType, StringType or BinaryType) when Narrows(source) => Narrow(export, source),
            _ => Copy(export, source, Target),
        };

    /// <summary>The bytes of value <paramref name="i"/> of a string or binary array of any layout.</summary>
    internal static ReadOnlySpan<byte> Bytes(IArrowArray values, int i) => values switch
    {
        BinaryArray b => b.GetBytes(i),
        LargeBinaryArray b => b.GetBytes(i),
        BinaryViewArray b => b.GetBytes(i),
        _ => throw new NotSupportedException(values.Data.DataType.Name),
    };

    /// <summary>The values of <paramref name="source"/> appended one by one to an array of <paramref name="target"/>.</summary>
    internal static NativeCArray* Copy(NativeBatchExport export, ArrayData source, IArrowType target)
    {
        IArrowArray values = ArrowArrayFactory.BuildArray(source);
        using NativeColumnBuilder builder = NativeColumnBuilder.Create(target);
        builder.Reserve(source.Length);
        var bytes = (IBytesBuilder)builder;
        for (int i = 0; i < source.Length; i++)
        {
            if (values.IsNull(i)) builder.AppendNull();
            else bytes.Append(Bytes(values, i));
        }
        return builder.Export(export);
    }

    /// <summary>32-bit offsets into 64-bit ones over the same bytes.</summary>
    private static NativeCArray* Widen(NativeBatchExport export, ArrayData source)
    {
        var (validity, nulls) = Validity(export, source);
        ReadOnlySpan<int> offsets = source.Buffers[1].Span.CastTo<int>().Slice(source.Offset, source.Length + 1);
        var wide = (long*)export.Allocate((source.Length + 1) * 8L);
        for (int i = 0; i <= source.Length; i++) wide[i] = offsets[i];
        return export.NewArray(source.Length, nulls, [validity, (nint)wide, Data(export, source.Buffers[2])], []);
    }

    /// <summary>Whether a large array's offsets all fit 32 bits (its bytes then shared as they are).</summary>
    private static bool Narrows(ArrayData source) =>
        source.Buffers[1].Span.CastTo<long>()[source.Offset + source.Length] <= int.MaxValue;

    /// <summary>64-bit offsets into 32-bit ones over the same bytes (every offset fits).</summary>
    private static NativeCArray* Narrow(NativeBatchExport export, ArrayData source)
    {
        var (validity, nulls) = Validity(export, source);
        ReadOnlySpan<long> offsets = source.Buffers[1].Span.CastTo<long>().Slice(source.Offset, source.Length + 1);
        var narrow = (int*)export.Allocate((source.Length + 1) * 4L);
        for (int i = 0; i <= source.Length; i++) narrow[i] = (int)offsets[i];
        return export.NewArray(source.Length, nulls, [validity, (nint)narrow, Data(export, source.Buffers[2])], []);
    }

    /// <summary>A value buffer shared with the source (or copied when its memory is not a native batch's).</summary>
    private static nint Data(NativeBatchExport export, ArrowBuffer buffer)
    {
        if (buffer.IsEmpty) return 0;
        if (export.TryShare(buffer, out nint pointer)) return pointer;
        byte* copy = (byte*)export.Allocate(buffer.Length);
        buffer.Span.CopyTo(new Span<byte>(copy, buffer.Length));
        return (nint)copy;
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
        if (type is ListViewType or LargeListViewType)
        {
            // No builder makes views: null rows of size 0 over an empty child.
            long width = type is LargeListViewType ? 8 : 4;
            nint validity = (nint)export.Allocate(Math.Max(1, Filters.Masks.Words(length)) * 8L);
            nint offsets = (nint)export.Allocate(Math.Max(1, length) * width), sizes = (nint)export.Allocate(Math.Max(1, length) * width);
            return export.NewArray(length, length, [validity, offsets, sizes], [(nint)Build(export, ArrowWidening.ListElement(type), 0)]);
        }
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
