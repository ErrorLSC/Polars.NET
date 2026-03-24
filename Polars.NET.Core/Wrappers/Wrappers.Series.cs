using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;
using Polars.NET.Core.Helpers;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    // --- chunks ---
    public static SeriesHandle SeriesRechunk(SeriesHandle handle)
        => NativeBindings.pl_series_rechunk(handle);
    public static nuint SeriesChunkLengths(SeriesHandle handle)
        => NativeBindings.pl_series_chunk_lengths(handle);
    // --- Constructors ---
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, ReadOnlySpan<sbyte> data, ReadOnlySpan<byte> validity = default)
    {
        ref sbyte dataRef = ref MemoryMarshal.GetReference(data);
        
        ref byte validRef = ref Unsafe.NullRef<byte>();

        if (!validity.IsEmpty)
        {
            validRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_i8(
            name, 
            ref dataRef, 
            ref validRef, 
            (UIntPtr)data.Length));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, sbyte[] data, byte[]? validity = null)
    {
        return SeriesNew(
            name, 
            new ReadOnlySpan<sbyte>(data), 
            validity == null ? default : new ReadOnlySpan<byte>(validity));
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, ReadOnlySpan<byte> data, ReadOnlySpan<byte> validity = default)
    {
        ref byte dataRef = ref MemoryMarshal.GetReference(data);
        
        ref byte validRef = ref Unsafe.NullRef<byte>();

        if (!validity.IsEmpty)
        {
            validRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_u8(
            name, 
            ref dataRef, 
            ref validRef, 
            (UIntPtr)data.Length));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, byte[] data, byte[]? validity = null)
    {
        return SeriesNew(
            name, 
            new ReadOnlySpan<byte>(data), 
            validity == null ? default : new ReadOnlySpan<byte>(validity));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, ReadOnlySpan<short> data, ReadOnlySpan<byte> validity = default)
    {
        ref short dataRef = ref MemoryMarshal.GetReference(data);
        
        ref byte validRef = ref Unsafe.NullRef<byte>();

        if (!validity.IsEmpty)
        {
            validRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_i16(
            name, 
            ref dataRef, 
            ref validRef, 
            (UIntPtr)data.Length));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, short[] data, byte[]? validity = null)
    {
        return SeriesNew(
            name, 
            new ReadOnlySpan<short>(data), 
            validity == null ? default : new ReadOnlySpan<byte>(validity));
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, ReadOnlySpan<ushort> data, ReadOnlySpan<byte> validity = default)
    {
        ref ushort dataRef = ref MemoryMarshal.GetReference(data);
        
        ref byte validRef = ref Unsafe.NullRef<byte>();

        if (!validity.IsEmpty)
        {
            validRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_u16(
            name, 
            ref dataRef, 
            ref validRef, 
            (UIntPtr)data.Length));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, ushort[] data, byte[]? validity = null)
    {
        return SeriesNew(
            name, 
            new ReadOnlySpan<ushort>(data), 
            validity == null ? default : new ReadOnlySpan<byte>(validity));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, ReadOnlySpan<int> data, ReadOnlySpan<byte> validity = default)
    {
        ref int dataRef = ref MemoryMarshal.GetReference(data);
        
        ref byte validRef = ref Unsafe.NullRef<byte>();

        if (!validity.IsEmpty)
        {
            validRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_i32(
            name, 
            ref dataRef, 
            ref validRef, 
            (UIntPtr)data.Length));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, int[] data, byte[]? validity = null)
    {
        return SeriesNew(
            name, 
            new ReadOnlySpan<int>(data), 
            validity == null ? default : new ReadOnlySpan<byte>(validity));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, ReadOnlySpan<uint> data, ReadOnlySpan<byte> validity = default)
    {
        ref uint dataRef = ref MemoryMarshal.GetReference(data);
        
        ref byte validRef = ref Unsafe.NullRef<byte>();

        if (!validity.IsEmpty)
        {
            validRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_u32(
            name, 
            ref dataRef, 
            ref validRef, 
            (UIntPtr)data.Length));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, uint[] data, byte[]? validity = null)
    {
        return SeriesNew(
            name, 
            new ReadOnlySpan<uint>(data), 
            validity == null ? default : new ReadOnlySpan<byte>(validity));
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, ReadOnlySpan<long> data, ReadOnlySpan<byte> validity = default)
    {
        ref long dataRef = ref MemoryMarshal.GetReference(data);
        
        ref byte validRef = ref Unsafe.NullRef<byte>();

        if (!validity.IsEmpty)
        {
            validRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_i64(
            name, 
            ref dataRef, 
            ref validRef, 
            (UIntPtr)data.Length));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, long[] data, byte[]? validity = null)
    {
        return SeriesNew(
            name, 
            new ReadOnlySpan<long>(data), 
            validity == null ? default : new ReadOnlySpan<byte>(validity));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, ReadOnlySpan<ulong> data, ReadOnlySpan<byte> validity = default)
    {
        ref ulong dataRef = ref MemoryMarshal.GetReference(data);
        
        ref byte validRef = ref Unsafe.NullRef<byte>();

        if (!validity.IsEmpty)
        {
            validRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_u64(
            name, 
            ref dataRef, 
            ref validRef, 
            (UIntPtr)data.Length));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, ulong[] data, byte[]? validity = null)
    {
        return SeriesNew(
            name, 
            new ReadOnlySpan<ulong>(data), 
            validity == null ? default : new ReadOnlySpan<byte>(validity));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, ReadOnlySpan<Int128> data, ReadOnlySpan<byte> validity = default)
    {
        ref Int128 dataRef = ref MemoryMarshal.GetReference(data);
        
        ref byte validRef = ref Unsafe.NullRef<byte>();

        if (!validity.IsEmpty)
        {
            validRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_i128(
            name, 
            ref dataRef, 
            ref validRef, 
            (UIntPtr)data.Length));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, Int128[] data, byte[]? validity = null)
    {
        return SeriesNew(
            name, 
            new ReadOnlySpan<Int128>(data), 
            validity == null ? default : new ReadOnlySpan<byte>(validity));
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, ReadOnlySpan<UInt128> data, ReadOnlySpan<byte> validity = default)
    {
        ref UInt128 dataRef = ref MemoryMarshal.GetReference(data);
        
        ref byte validRef = ref Unsafe.NullRef<byte>();

        if (!validity.IsEmpty)
        {
            validRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_u128(
            name, 
            ref dataRef, 
            ref validRef, 
            (UIntPtr)data.Length));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, UInt128[] data, byte[]? validity = null)
    {
        return SeriesNew(
            name, 
            new ReadOnlySpan<UInt128>(data), 
            validity == null ? default : new ReadOnlySpan<byte>(validity));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, ReadOnlySpan<Half> data, ReadOnlySpan<byte> validity = default)
    {
        ref Half dataRef = ref MemoryMarshal.GetReference(data);
        
        ref byte validRef = ref Unsafe.NullRef<byte>();

        if (!validity.IsEmpty)
        {
            validRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_f16(
            name, 
            ref dataRef, 
            ref validRef, 
            (UIntPtr)data.Length));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, Half[] data, byte[]? validity = null)
    {
        return SeriesNew(
            name, 
            new ReadOnlySpan<Half>(data), 
            validity == null ? default : new ReadOnlySpan<byte>(validity));
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, ReadOnlySpan<float> data, ReadOnlySpan<byte> validity = default)
    {
        ref float dataRef = ref MemoryMarshal.GetReference(data);
        
        ref byte validRef = ref Unsafe.NullRef<byte>();

        if (!validity.IsEmpty)
        {
            validRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_f32(
            name, 
            ref dataRef, 
            ref validRef, 
            (UIntPtr)data.Length));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, float[] data, byte[]? validity = null)
    {
        return SeriesNew(
            name, 
            new ReadOnlySpan<float>(data), 
            validity == null ? default : new ReadOnlySpan<byte>(validity));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, ReadOnlySpan<double> data, ReadOnlySpan<byte> validity = default)
    {
        ref double dataRef = ref MemoryMarshal.GetReference(data);
        
        ref byte validRef = ref Unsafe.NullRef<byte>();

        if (!validity.IsEmpty)
        {
            validRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_f64(
            name, 
            ref dataRef, 
            ref validRef, 
            (UIntPtr)data.Length));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(string name, double[] data, byte[]? validity = null)
    {
        return SeriesNew(
            name, 
            new ReadOnlySpan<double>(data), 
            validity == null ? default : new ReadOnlySpan<byte>(validity));
    }
        
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(
        string name, 
        ReadOnlySpan<byte> valuesBitmask, 
        ReadOnlySpan<byte> validityBitmask, 
        UIntPtr length)
    {
        ref byte valuesRef = ref valuesBitmask.IsEmpty 
            ? ref Unsafe.NullRef<byte>() 
            : ref MemoryMarshal.GetReference(valuesBitmask);

        ref byte validRef = ref Unsafe.NullRef<byte>();
        if (!validityBitmask.IsEmpty)
        {
            validRef = ref MemoryMarshal.GetReference(validityBitmask);
        }

        return ErrorHelper.Check(NativeBindings.pl_series_new_bool(
            name, 
            ref valuesRef, 
            ref validRef, 
            length));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNew(
        string name, 
        byte[] valuesBitmask, 
        byte[]? validityBitmask, 
        UIntPtr length)
    {
        return SeriesNew(
            name, 
            new ReadOnlySpan<byte>(valuesBitmask), 
            validityBitmask == null ? default : new ReadOnlySpan<byte>(validityBitmask),
            length);
    }

    /// <summary>
    /// Create String Series using Arrow StringView (German Strings Layout).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static SeriesHandle SeriesNewStringSimd(string name, ReadOnlySpan<string?> data)
    {
        // Blank data edge case
        if (data.IsEmpty)
        {
            return ErrorHelper.Check(
                NativeBindings.pl_series_new_str_simd(
                    name, 
                    ref Unsafe.NullRef<byte>(),            // Null Pointer
                    0,    
                    ref Unsafe.NullRef<ArrowStringView>(), // Null Pointer
                    ref Unsafe.NullRef<byte>(),            // Null Pointer
                    0     
                )
            );
        }

        // SIMD Packer (StringView Edition)
        var (views, dataBuffer, validity) = StringPacker.PackStringView(data);

        // Prepare Ref
        
        // DataBuffer might be null (All inlined)
        ref byte pData = ref dataBuffer == null || dataBuffer.Length == 0 
            ? ref Unsafe.NullRef<byte>() 
            : ref MemoryMarshal.GetArrayDataReference(dataBuffer);

        // Views array
        ref ArrowStringView pViews = ref views == null || views.Length == 0
            ? ref Unsafe.NullRef<ArrowStringView>()
            : ref MemoryMarshal.GetArrayDataReference(views);

        // Validity bitmap
        ref byte pValid = ref validity == null || validity.Length == 0
            ? ref Unsafe.NullRef<byte>()
            : ref MemoryMarshal.GetArrayDataReference(validity);

        // Send to Rust
        return ErrorHelper.Check(
            NativeBindings.pl_series_new_str_simd(
                name,
                ref pData,
                (nuint)(dataBuffer?.Length ?? 0),
                ref pViews,
                ref pValid,
                (nuint)data.Length
            )
        );
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNewStringSimd(string name, string?[] data)
        => SeriesNewStringSimd(name, new ReadOnlySpan<string?>(data));
    
    /// <summary>
    /// Create DateTime Series from pre-calculated Microseconds.
    /// </summary>
    /// <param name="name">Series Name</param>
    /// <param name="values">Physical values (Microseconds from 1970-01-01)</param>
    /// <param name="validity">Null bitmap (can be null)</param>
    /// <param name="timeZone">"Asia/Shanghai", "UTC" or null (Naive)</param>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static SeriesHandle SeriesNewDatetime(
        string name, 
        ReadOnlySpan<long> values, 
        ReadOnlySpan<byte> validity = default, 
        string? timeZone = null)
    {
        // Get Values Ref
        ref long pValsRef = ref values.IsEmpty
            ? ref Unsafe.NullRef<long>()
            : ref MemoryMarshal.GetReference(values);

        // Get Validity Ref
        ref byte pValidRef = ref Unsafe.NullRef<byte>();
        if (!validity.IsEmpty)
        {
            pValidRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(
            NativeBindings.pl_series_new_datetime(
                name, 
                ref pValsRef, 
                ref pValidRef, 
                (UIntPtr)values.Length,
                PlTimeUnit.Microseconds,
                timeZone
            )
        );
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNewDatetime(
        string name, 
        long[] values, 
        byte[]? validity, 
        string? timeZone = null)
    {
        return SeriesNewDatetime(
            name, 
            new ReadOnlySpan<long>(values), 
            validity == null ? default : new ReadOnlySpan<byte>(validity), 
            timeZone
        );
    }
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static SeriesHandle SeriesNewDate(string name, ReadOnlySpan<int> values, ReadOnlySpan<byte> validity = default)
    {
        ref int pValsRef = ref values.IsEmpty
            ? ref Unsafe.NullRef<int>()
            : ref MemoryMarshal.GetReference(values);

        ref byte pValidRef = ref Unsafe.NullRef<byte>();
        if (!validity.IsEmpty)
        {
            pValidRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(
            NativeBindings.pl_series_new_date(name, ref pValsRef, ref pValidRef, (UIntPtr)values.Length)
        );
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNewDate(string name, int[] values, byte[]? validity = null)
    {
        return SeriesNewDate(
            name, 
            new ReadOnlySpan<int>(values), 
            validity == null ? default : new ReadOnlySpan<byte>(validity)
        );
    }
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static SeriesHandle SeriesNewTime(string name, ReadOnlySpan<long> values, ReadOnlySpan<byte> validity = default)
    {
        // 1. Get Values Ref
        ref long pValsRef = ref values.IsEmpty
            ? ref Unsafe.NullRef<long>()
            : ref MemoryMarshal.GetReference(values);

        // 2. Get Validity Ref
        ref byte pValidRef = ref Unsafe.NullRef<byte>();
        if (!validity.IsEmpty)
        {
            pValidRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(
            NativeBindings.pl_series_new_time(name, ref pValsRef, ref pValidRef, (UIntPtr)values.Length)
        );
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNewTime(string name, long[] values, byte[]? validity = null)
    {
        return SeriesNewTime(
            name, 
            new ReadOnlySpan<long>(values), 
            validity == null ? default : new ReadOnlySpan<byte>(validity)
        );
    }
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static SeriesHandle SeriesNewDuration(string name, ReadOnlySpan<long> values, ReadOnlySpan<byte> validity = default)
    {
        ref long pValsRef = ref values.IsEmpty
            ? ref Unsafe.NullRef<long>()
            : ref MemoryMarshal.GetReference(values);

        ref byte pValidRef = ref Unsafe.NullRef<byte>();
        if (!validity.IsEmpty)
        {
            pValidRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(
            NativeBindings.pl_series_new_duration(
                name, 
                ref pValsRef, 
                ref pValidRef, 
                (UIntPtr)values.Length, 
                PlTimeUnit.Microseconds
            )
        );
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNewDuration(string name, long[] values, byte[]? validity = null)
    {
        return SeriesNewDuration(
            name, 
            new ReadOnlySpan<long>(values), 
            validity == null ? default : new ReadOnlySpan<byte>(validity)
        );
    }
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static SeriesHandle SeriesNewDecimal(string name, ReadOnlySpan<Int128> values, ReadOnlySpan<byte> validity, int scale)
    {
        ref Int128 pValsRef = ref values.IsEmpty
            ? ref Unsafe.NullRef<Int128>()
            : ref MemoryMarshal.GetReference(values);

        ref byte pValidRef = ref Unsafe.NullRef<byte>();
        if (!validity.IsEmpty)
        {
            pValidRef = ref MemoryMarshal.GetReference(validity);
        }

        return ErrorHelper.Check(
            NativeBindings.pl_series_new_decimal(
                name, 
                ref pValsRef, 
                ref pValidRef, 
                (UIntPtr)values.Length, 
                UIntPtr.Zero, // Precision=0 (Auto)
                (UIntPtr)scale
            )
        );
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNewDecimal(string name, Int128[] values, byte[]? validity, int scale)
    {
        return SeriesNewDecimal(
            name, 
            new ReadOnlySpan<Int128>(values), 
            validity == null ? default : new ReadOnlySpan<byte>(validity),
            scale
        );
    }
    // =================================================================
    // FixedSizeList (2D Array) Wrapper
    // =================================================================
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static SeriesHandle SeriesNewFixedArray<T>(string name, ReadOnlySpan<T> flatData, int width, ReadOnlySpan<byte> validity = default) 
        where T : unmanaged
    {
        int flatLen = flatData.Length;
        int height = flatLen / width;
        
        UIntPtr uFlatLen = (UIntPtr)flatLen;
        UIntPtr uHeight = (UIntPtr)height;
        UIntPtr uWidth = (UIntPtr)width;

        // get data ref
        ref T pDataRef = ref flatData.IsEmpty ? ref Unsafe.NullRef<T>() : ref MemoryMarshal.GetReference(flatData);
        
        // get Validity ref
        ref byte pValidRef = ref validity.IsEmpty ? ref Unsafe.NullRef<byte>() : ref MemoryMarshal.GetReference(validity);
        if (typeof(T) == typeof(sbyte))
        {
            return ErrorHelper.Check(NativeBindings.pl_series_new_array_i8(
                name, ref Unsafe.As<T, sbyte>(ref pDataRef), uFlatLen, ref pValidRef, uHeight, uWidth));
        }
        else if (typeof(T) == typeof(byte))
        {
            return ErrorHelper.Check(NativeBindings.pl_series_new_array_u8(
                name, ref Unsafe.As<T, byte>(ref pDataRef), uFlatLen, ref pValidRef, uHeight, uWidth));
        }
        else if (typeof(T) == typeof(short))
        {
            return ErrorHelper.Check(NativeBindings.pl_series_new_array_i16(
                name, ref Unsafe.As<T, short>(ref pDataRef), uFlatLen, ref pValidRef, uHeight, uWidth));
        }
        else if (typeof(T) == typeof(ushort))
        {
            return ErrorHelper.Check(NativeBindings.pl_series_new_array_u16(
                name, ref Unsafe.As<T, ushort>(ref pDataRef), uFlatLen, ref pValidRef, uHeight, uWidth));
        }
        else if(typeof(T) == typeof(int))
        {
            return ErrorHelper.Check(NativeBindings.pl_series_new_array_i32(
                name, ref Unsafe.As<T, int>(ref pDataRef), uFlatLen, ref pValidRef, uHeight, uWidth));
        }
        else if(typeof(T) == typeof(uint))
        {
            return ErrorHelper.Check(NativeBindings.pl_series_new_array_u32(
                name, ref Unsafe.As<T, uint>(ref pDataRef), uFlatLen, ref pValidRef, uHeight, uWidth));
        }
        else if (typeof(T) == typeof(long))
        {
            return ErrorHelper.Check(NativeBindings.pl_series_new_array_i64(
                name, ref Unsafe.As<T, long>(ref pDataRef), uFlatLen, ref pValidRef, uHeight, uWidth));
        }
        else if (typeof(T) == typeof(ulong))
        {
            return ErrorHelper.Check(NativeBindings.pl_series_new_array_u64(
                name, ref Unsafe.As<T, ulong>(ref pDataRef), uFlatLen, ref pValidRef, uHeight, uWidth));
        }
        else if (typeof(T) == typeof(Int128))
        {
            return ErrorHelper.Check(NativeBindings.pl_series_new_array_i128(
                name, ref Unsafe.As<T, Int128>(ref pDataRef), uFlatLen, ref pValidRef, uHeight, uWidth));
        }
        else if (typeof(T) == typeof(UInt128))
        {
            return ErrorHelper.Check(NativeBindings.pl_series_new_array_u128(
                name, ref Unsafe.As<T, UInt128>(ref pDataRef), uFlatLen, ref pValidRef, uHeight, uWidth));
        }
        else if (typeof(T) == typeof(Half))
        {
            return ErrorHelper.Check(NativeBindings.pl_series_new_array_f16(
                name, ref Unsafe.As<T, Half>(ref pDataRef), uFlatLen, ref pValidRef, uHeight, uWidth));
        }
        else if (typeof(T) == typeof(float))
        {
            return ErrorHelper.Check(NativeBindings.pl_series_new_array_f32(
                name, ref Unsafe.As<T, float>(ref pDataRef), uFlatLen, ref pValidRef, uHeight, uWidth));
        }
        else if (typeof(T) == typeof(double))
        {
            return ErrorHelper.Check(NativeBindings.pl_series_new_array_f64(
                name, ref Unsafe.As<T, double>(ref pDataRef), uFlatLen, ref pValidRef, uHeight, uWidth));
        }
        else if (typeof(T) == typeof(decimal))
        {
            unsafe
            {
                fixed (T* ptr = &pDataRef)
                {
                    decimal* pDec = (decimal*)ptr;
                    var (int128Values, scale) = DecimalPacker.Pack(pDec, flatLen);
                    
                    ref Int128 pValsRef = ref MemoryMarshal.GetArrayDataReference(int128Values);
                    
                    return ErrorHelper.Check(NativeBindings.pl_series_new_array_decimal(
                        name, ref pValsRef, uFlatLen, ref pValidRef, uHeight, uWidth, (UIntPtr)scale));
                }
            }
        }
        else
        {
            throw new NotSupportedException($"Type '{typeof(T).Name}' is not supported for FixedSizeList Series.");
        }
    }

    /// <summary>
    /// Create Fixed Size Series (Zero-Copy from C# 2D Array).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNewFixedArray<T>(string name, T[,] data) 
        where T : unmanaged
    {
        int width = data.GetLength(1);
        
        if (data.Length == 0)
        {
            return SeriesNewFixedArray(name, ReadOnlySpan<T>.Empty, width);
        }

        ref T firstElement = ref Unsafe.As<byte, T>(ref MemoryMarshal.GetArrayDataReference(data));

        ReadOnlySpan<T> flatSpan = MemoryMarshal.CreateReadOnlySpan(ref firstElement, data.Length);

        return SeriesNewFixedArray(name, flatSpan, width);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static SeriesHandle SeriesNewStruct(string name, ReadOnlySpan<SeriesHandle> handles)
    {
        int len = handles.Length;
        if (len == 0)
        {
            return ErrorHelper.Check(
                NativeBindings.pl_series_new_struct(name, ref Unsafe.NullRef<IntPtr>(), 0)
            );
        }

        Span<IntPtr> pointers = len <= 128 ? stackalloc IntPtr[len] : new IntPtr[len];
        Span<bool> locks = len <= 128 ? stackalloc bool[len] : new bool[len];

        using var handlesLock = new SafeHandleSpanLock<SeriesHandle>(handles, pointers, locks);

        return ErrorHelper.Check(
            NativeBindings.pl_series_new_struct(
                name, 
                ref MemoryMarshal.GetReference(pointers), 
                (nuint)len
            )
        );
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SeriesHandle SeriesNewStruct(string name, SeriesHandle[] handles)
        => SeriesNewStruct(name, new ReadOnlySpan<SeriesHandle>(handles));
    public static SeriesHandle CloneSeries(SeriesHandle handle)
        => ErrorHelper.Check(NativeBindings.pl_series_clone(handle));
    // --- Properties ---
    public static string GetSeriesDtypeString(SeriesHandle h)
    {
        var ptr = NativeBindings.pl_series_dtype_str(h);
        return ErrorHelper.CheckString(ptr);
    }
    /// <summary>
    /// Get DataType Handle from Series
    /// </summary>
    public static DataTypeHandle GetSeriesDataType(SeriesHandle handle)
    {
        return ErrorHelper.Check(NativeBindings.pl_series_get_dtype(handle));
    }
    public static long SeriesLen(SeriesHandle h) => (long)NativeBindings.pl_series_len(h);
    
    public static string SeriesName(SeriesHandle h) 
    {
        var ptr = NativeBindings.pl_series_name(h);
        return ErrorHelper.CheckString(ptr) ;
    }
    
    public static void SeriesRename(SeriesHandle h, string name) => NativeBindings.pl_series_rename(h, name);

    // --- DataFrame Conversion ---
    public static DataFrameHandle SeriesToFrame(SeriesHandle h) 
    {
        return ErrorHelper.Check(NativeBindings.pl_series_to_frame(h));
    }
    public static long? SeriesGetInt(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_i64(s, (UIntPtr)idx, out long val)) return val;
        return null;
    }
    public static Int128? SeriesGetInt128(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_i128(s, (UIntPtr)idx, out Int128 val)) return val;
        return null;
    }
    public static UInt128? SeriesGetUInt128(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_u128(s, (UIntPtr)idx, out UInt128 val)) return val;
        return null;
    }

    public static double? SeriesGetDouble(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_f64(s, (UIntPtr)idx, out double val)) return val;
        return null;
    }

    public static bool? SeriesGetBool(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_bool(s, (UIntPtr)idx, out bool val)) return val;
        return null;
    }

    public static string? SeriesGetString(SeriesHandle s, long idx)
    {
        IntPtr ptr = NativeBindings.pl_series_get_str(s, (UIntPtr)idx);
        return ErrorHelper.CheckString(ptr); 
    }

    public static decimal? SeriesGetDecimal(SeriesHandle s, long idx)
    {
        // Get Int128 raw value and scale
        if (NativeBindings.pl_series_get_decimal(s, (UIntPtr)idx, out Int128 val, out UIntPtr scalePtr))
        {
            int scale = (int)scalePtr;

            // Boundary Check ：C# decimal max Scale is 28
            // If Polars Scale > 28，C# decimal is not able to save such data
            if (scale >= DecimalPacker.PowersOf10Int128.Length) 
            {
                // Fallback: lose accuracy or return null
                try { return (decimal)val / (decimal)Math.Pow(10, scale); }
                catch { return null; }
            }

            // Int128 -> Decimal
            
            Int128 divisor = DecimalPacker.PowersOf10Int128[scale];

            // Integer Part
            Int128 intPart = val / divisor;
            // Fractional Part
            Int128 remPart = val % divisor;

            try 
            {
                // Int part
                decimal dInt = (decimal)intPart;
                
                // rem part
                decimal dRem = (decimal)remPart;
                decimal dDivisor = (decimal)divisor; 
                
                // Assemble
                return dInt + (dRem / dDivisor);
            }
            catch (OverflowException)
            {
                return null;
            }
        }
        return null;
    }
    // Date: Days since 1970-01-01
    public static DateOnly? SeriesGetDate(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_date(s, (UIntPtr)idx, out int days))
        {
            // 719162 is days from 0001-01-01 to 1970-01-01
            return DateOnly.FromDayNumber(days + 719162); 
        }
        return null;
    }

    // Time: Nanoseconds since midnight
    public static TimeOnly? SeriesGetTime(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_time(s, (UIntPtr)idx, out long ns))
        {
            // .NET Ticks = 100ns
            long ticks = ns / 100;
            return new TimeOnly(ticks);
        }
        return null;
    }

    // Datetime: Microseconds since 1970-01-01 (Assuming 'us' time unit)
    public static DateTime? SeriesGetDatetime(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_datetime(s, (UIntPtr)idx, out long us))
        {
            // .NET Ticks = 100ns. 1 us = 10 ticks.
            // Unix Epoch Ticks = 621355968000000000
            long ticks = (us * 10) + 621355968000000000L;
            return new DateTime(ticks, DateTimeKind.Unspecified); // Default Unspecified
        }
        return null;
    }

    // Duration: Microseconds (Assuming 'us')
    public static TimeSpan? SeriesGetDuration(SeriesHandle s, long idx)
    {
        if (NativeBindings.pl_series_get_duration(s, (UIntPtr)idx, out long us))
        {
            // 1 us = 10 ticks
            return new TimeSpan(us * 10);
        }
        return null;
    }
    // --- Arrow Integration ---

    public static unsafe IArrowArray SeriesToArrow(SeriesHandle h)
    {
        using var contextHandle = NativeBindings.pl_series_to_arrow(h);
        
        var cArray = new CArrowArray();
        var cSchema = new CArrowSchema();
        
        NativeBindings.pl_arrow_array_export(contextHandle, &cArray);
        NativeBindings.pl_arrow_schema_export(contextHandle, &cSchema);
        bool ownershipTransferred = false;
        try
        {
            var importedField = CArrowSchemaImporter.ImportField(&cSchema);
            
            var array = CArrowArrayImporter.ImportArray(&cArray, importedField.DataType);
            ownershipTransferred = true;
            return array;
        }
        finally
        {
            if (!ownershipTransferred)
            {
                // CArrowArray.Free(&cArray);
                // CArrowSchema.Free(&cSchema);
            }
        }
    }
    /// <summary>
    /// Imports an Arrow Array via C Data Interface.
    /// </summary>
    public static unsafe SeriesHandle SeriesFromArrow(string name, CArrowArray* cArray, CArrowSchema* cSchema)
        => ErrorHelper.Check(NativeBindings.pl_arrow_to_series(name, cArray, cSchema));
    public static SeriesHandle SeriesCast(SeriesHandle s, DataTypeHandle dtype)
        => ErrorHelper.Check(NativeBindings.pl_series_cast(s, dtype));
    public static SeriesHandle SeriesIsNull(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_null(s));
    public static SeriesHandle SeriesIsNotNull(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_not_null(s));
    public static SeriesHandle SeriesDropNulls(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_drop_nulls(s));
    public static bool SeriesIsNullAt(SeriesHandle s, long idx) => NativeBindings.pl_series_is_null_at(s, (UIntPtr)idx);
    public static SeriesHandle SeriesIsNan(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_nan(s));
    public static SeriesHandle SeriesIsNotNan(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_not_nan(s));
    public static SeriesHandle SeriesIsFinite(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_finite(s));
    public static SeriesHandle SeriesIsInfinite(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_is_infinite(s));
    public static long SeriesNullCount(SeriesHandle s) => (long)NativeBindings.pl_series_null_count(s);
    public static SeriesHandle SeriesUnique(SeriesHandle handle) => ErrorHelper.Check(NativeBindings.pl_series_unique(handle));
    public static SeriesHandle SeriesUniqueStable(SeriesHandle handle) => ErrorHelper.Check(NativeBindings.pl_series_unique_stable(handle));
    public static ulong SeriesNUnique(SeriesHandle handle) => NativeBindings.pl_series_n_unique(handle);
    // Ops
    public static SeriesHandle SeriesAdd(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_add(s1, s2));
    public static SeriesHandle SeriesSub(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_sub(s1, s2));
    public static SeriesHandle SeriesMul(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_mul(s1, s2));
    public static SeriesHandle SeriesDiv(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_div(s1, s2));

    public static SeriesHandle SeriesEq(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_eq(s1, s2));
    public static SeriesHandle SeriesNeq(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_neq(s1, s2));
    public static SeriesHandle SeriesGt(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_gt(s1, s2));
    public static SeriesHandle SeriesLt(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_lt(s1, s2));
    public static SeriesHandle SeriesGtEq(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_gt_eq(s1, s2));
    public static SeriesHandle SeriesLtEq(SeriesHandle s1, SeriesHandle s2) => ErrorHelper.Check(NativeBindings.pl_series_lt_eq(s1, s2));

    // Aggs
    public static SeriesHandle SeriesSum(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_sum(s));
    public static SeriesHandle SeriesMean(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_mean(s));
    public static SeriesHandle SeriesMin(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_min(s));
    public static SeriesHandle SeriesMax(SeriesHandle s) => ErrorHelper.Check(NativeBindings.pl_series_max(s));
    // Slice
    public static SeriesHandle SeriesSlice(SeriesHandle handle, long offset, long length)
        => ErrorHelper.Check(NativeBindings.pl_series_slice(handle, offset, (UIntPtr)length));
    // Sort
    public static SeriesHandle SeriesSort(
        SeriesHandle series, 
        bool descending = false,
        bool nullsLast = false,
        bool multithreaded = true, 
        bool maintainOrder = false
    )
    {
        return ErrorHelper.Check(NativeBindings.pl_series_sort(
            series, 
            descending, 
            nullsLast, 
            multithreaded, 
            maintainOrder
        ));
    }
    public static DataFrameHandle SeriesStructUnnest(SeriesHandle series)   
        => ErrorHelper.Check(NativeBindings.pl_series_struct_unnest(series));
    public static DataFrameHandle SeriesValueCounts(
        SeriesHandle series,
        bool sort,
        bool parallel,
        string name,
        bool normalize)
    {
        return ErrorHelper.Check(NativeBindings.pl_series_value_counts(
            series,
            sort,
            parallel,
            name,
            normalize
        ));
    }
}