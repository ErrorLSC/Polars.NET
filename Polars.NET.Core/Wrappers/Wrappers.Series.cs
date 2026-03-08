using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;
using Polars.NET.Core.Helpers;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    // --- Constructors ---
    public static SeriesHandle SeriesNew(string name, sbyte[] data, byte[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_i8(name, data, validity, (UIntPtr)data.Length));
    public static SeriesHandle SeriesNew(string name, byte[] data, byte[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_u8(name, data, validity, (UIntPtr)data.Length));
    public static SeriesHandle SeriesNew(string name, short[] data, byte[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_i16(name, data, validity, (UIntPtr)data.Length));
    public static SeriesHandle SeriesNew(string name, ushort[] data, byte[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_u16(name, data, validity, (UIntPtr)data.Length));
    public static SeriesHandle SeriesNew(string name, int[] data, byte[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_i32(name, data, validity, (UIntPtr)data.Length));
    public static SeriesHandle SeriesNew(string name, uint[] data, byte[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_u32(name, data, validity, (UIntPtr)data.Length));
    public static SeriesHandle SeriesNew(string name, long[] data, byte[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_i64(name, data, validity, (UIntPtr)data.Length));
    public static SeriesHandle SeriesNew(string name, ulong[] data, byte[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_u64(name, data, validity, (UIntPtr)data.Length));
    public static SeriesHandle SeriesNew(string name, Int128[] data, byte[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_i128(name, data, validity, (UIntPtr)data.Length));
    public static SeriesHandle SeriesNew(string name, UInt128[] data, byte[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_u128(name, data, validity, (UIntPtr)data.Length));
    public static SeriesHandle SeriesNew(string name, Half[] data, byte[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_f16(name, data, validity, (UIntPtr)data.Length));
    public static SeriesHandle SeriesNew(string name, float[] data, byte[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_f32(name, data, validity, (UIntPtr)data.Length));
    public static SeriesHandle SeriesNew(string name, double[] data, byte[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_f64(name, data, validity, (UIntPtr)data.Length));
        
    public static SeriesHandle SeriesNew(string name, byte[] valuesBitmask, byte[]? validityBitmask, UIntPtr length)
        => ErrorHelper.Check(NativeBindings.pl_series_new_bool(name, valuesBitmask, validityBitmask, length));

    public static SeriesHandle SeriesNew(string name, string?[] data)
    {
        return ErrorHelper.Check(
            NativeBindings.pl_series_new_str(name, data, (UIntPtr)data.Length)
        );
    }
    /// <summary>
    /// [SIMD High Performance] Create String Series using Arrow StringView (German Strings Layout).
    /// </summary>
    public unsafe static SeriesHandle SeriesNewStringSimd(string name, string?[] data)
    {
        // 0. Blank data edge case
        // 换成纯指针后，空数组的边缘情况变得极其优雅，直接全传 null (0) 即可
        if (data.Length == 0)
        {
            return ErrorHelper.Check(
                NativeBindings.pl_series_new_str_simd(
                    name, 
                    null, // values_ptr (dataBuffer)
                    0,    // values_len
                    null, // views_ptr
                    null, // validity_ptr
                    0     // len
                )
            );
        }

        // 1. SIMD Packer (StringView Edition)
        var (views, dataBuffer, validity) = StringPacker.PackStringView(data);

        // 2. Prepare ptrs for Rust
        // C# 的黑魔法：如果数组是 null，fixed 块内的指针会自动变成安全的 null 指针
        fixed (ArrowStringView* pViews = views)
        fixed (byte* pData = dataBuffer)
        fixed (byte* pValid = validity)
        {
            return ErrorHelper.Check(
                NativeBindings.pl_series_new_str_simd(
                    name,
                    pData,
                    (nuint)(dataBuffer?.Length ?? 0),
                    pViews,
                    pValid,
                    (nuint)data.Length
                )
            );
        }
    }
    /// <summary>
    /// [SIMD] Create DateTime Series from pre-calculated Microseconds.
    /// </summary>
    /// <param name="name">Series Name</param>
    /// <param name="values">Physical values (Microseconds from 1970-01-01)</param>
    /// <param name="validity">Null bitmap (can be null)</param>
    /// <param name="timeZone">"Asia/Shanghai", "UTC" or null (Naive)</param>
    public unsafe static SeriesHandle SeriesNewDatetime(
        string name, 
        long[] values, 
        byte[]? validity, 
        string? timeZone = null)
    {
        // 0. Deal blank array
        if (values.Length == 0)
        {
            long dummy = 0;
            return ErrorHelper.Check(
                NativeBindings.pl_series_new_datetime(
                    name, 
                    ref dummy, 
                    IntPtr.Zero, 
                    UIntPtr.Zero, 
                    PlTimeUnit.Microseconds, // unit=us
                    timeZone
                )
            );
        }

        // Get ref
        ref long pValsRef = ref MemoryMarshal.GetArrayDataReference(values);

        // Fix Validity
        fixed (byte* pValid = validity)
        {
            return ErrorHelper.Check(
                NativeBindings.pl_series_new_datetime(
                    name, 
                    ref pValsRef, 
                    (IntPtr)pValid, 
                    (UIntPtr)values.Length,
                    PlTimeUnit.Microseconds,
                    timeZone
                )
            );
        }
    }
    public unsafe static SeriesHandle SeriesNewDate(string name, int[] values, byte[]? validity)
    {
         if (values.Length == 0) 
         {
             int dummy = 0;
             return ErrorHelper.Check(NativeBindings.pl_series_new_date(name, ref dummy, IntPtr.Zero, UIntPtr.Zero));
         }

         ref int pVals = ref MemoryMarshal.GetArrayDataReference(values);
         fixed (byte* pValid = validity)
         {
             return ErrorHelper.Check(
                 NativeBindings.pl_series_new_date(name, ref pVals, (IntPtr)pValid, (UIntPtr)values.Length)
             );
         }
    }
    public unsafe static SeriesHandle SeriesNewTime(string name, long[] values, byte[]? validity)
    {
        if (values.Length == 0)
        {
            long dummy = 0;
            return ErrorHelper.Check(NativeBindings.pl_series_new_time(name, ref dummy, IntPtr.Zero, UIntPtr.Zero));
        }

        ref long pVals = ref MemoryMarshal.GetArrayDataReference(values);
        fixed (byte* pValid = validity)
        {
            return ErrorHelper.Check(
                NativeBindings.pl_series_new_time(name, ref pVals, (IntPtr)pValid, (UIntPtr)values.Length)
            );
        }
    }
    public unsafe static SeriesHandle SeriesNewDuration(string name, long[] values, byte[]? validity)
    {
        if (values.Length == 0)
        {
            long dummy = 0;
            // Unit = 1 (Microseconds)
            return ErrorHelper.Check(NativeBindings.pl_series_new_duration(name, ref dummy, IntPtr.Zero, UIntPtr.Zero, PlTimeUnit.Microseconds));
        }

        ref long pVals = ref MemoryMarshal.GetArrayDataReference(values);
        fixed (byte* pValid = validity)
        {
            // Unit = 1 (Microseconds)
            return ErrorHelper.Check(
                NativeBindings.pl_series_new_duration(name, ref pVals, (IntPtr)pValid, (UIntPtr)values.Length, PlTimeUnit.Microseconds)
            );
        }
    }
    public unsafe static SeriesHandle SeriesNewDecimal(string name, Int128[] values, byte[]? validity, int scale)
    {
        if (values.Length == 0)
        {
            Int128 dummy = 0;
            return ErrorHelper.Check(NativeBindings.pl_series_new_decimal(name, ref dummy, IntPtr.Zero, UIntPtr.Zero, UIntPtr.Zero, (UIntPtr)scale));
        }

        ref Int128 pVals = ref MemoryMarshal.GetArrayDataReference(values);
        fixed (byte* pValid = validity)
        {
            return ErrorHelper.Check(
                NativeBindings.pl_series_new_decimal(
                    name, 
                    ref pVals, 
                    (IntPtr)pValid, 
                    (UIntPtr)values.Length, 
                    UIntPtr.Zero, // Precision=0 (Auto)
                    (UIntPtr)scale
                )
            );
        }
    }
    // =================================================================
    // FixedSizeList (2D Array) Wrapper
    // =================================================================

    /// <summary>
    /// Create Fixed Size Series (Zero-Copy from C# 2D Array).
    /// </summary>
    /// <typeparam name="T">Unmanaged Type (int, double, etc.)</typeparam>
    /// <param name="name">Series Name</param>
    /// <param name="data">C# MultiDimension Array (T[,])</param>
    /// <returns>SeriesHandle</returns>
    /// <exception cref="NotSupportedException">When T is not primitive type</exception>
    public static unsafe SeriesHandle SeriesNewFixedArray<T>(string name, T[,] data) 
        where T : unmanaged
    {
        // Get Dimension Data
        // GetLength(0) = Parent Length
        // GetLength(1) = Fixed Width
        int height = data.GetLength(0);
        int width = data.GetLength(1);
        UIntPtr flatLen = (UIntPtr)data.Length;
        UIntPtr uHeight = (UIntPtr)height;
        UIntPtr uWidth = (UIntPtr)width;

        // Pin Array Memory
        fixed (T* pData = data)
        {
            // Type Distribution

            if (typeof(T) == typeof(int))
            {
                return ErrorHelper.Check(
                    NativeBindings.pl_series_new_array_i32(name, (int*)pData, flatLen, IntPtr.Zero, uHeight, uWidth)
                );
            }
            else if (typeof(T) == typeof(long))
            {
                return ErrorHelper.Check(
                    NativeBindings.pl_series_new_array_i64(name, (long*)pData, flatLen, IntPtr.Zero, uHeight, uWidth)
                );
            }
            else if (typeof(T) == typeof(double))
            {
                return ErrorHelper.Check(
                    NativeBindings.pl_series_new_array_f64(name, (double*)pData, flatLen, IntPtr.Zero, uHeight, uWidth)
                );
            }
            else if (typeof(T) == typeof(float))
            {
                return ErrorHelper.Check(
                    NativeBindings.pl_series_new_array_f32(name, (float*)pData, flatLen, IntPtr.Zero, uHeight, uWidth)
                );
            }
            else if (typeof(T) == typeof(byte))
            {
                return ErrorHelper.Check(
                    NativeBindings.pl_series_new_array_u8(name, (byte*)pData, flatLen, IntPtr.Zero, uHeight, uWidth)
                );
            }
            else if (typeof(T) == typeof(sbyte))
            {
                return ErrorHelper.Check(
                    NativeBindings.pl_series_new_array_i8(name, (sbyte*)pData, flatLen, IntPtr.Zero, uHeight, uWidth)
                );
            }
            else if (typeof(T) == typeof(short))
            {
                return ErrorHelper.Check(
                    NativeBindings.pl_series_new_array_i16(name, (short*)pData, flatLen, IntPtr.Zero, uHeight, uWidth)
                );
            }
            else if (typeof(T) == typeof(ushort))
            {
                return ErrorHelper.Check(
                    NativeBindings.pl_series_new_array_u16(name, (ushort*)pData, flatLen, IntPtr.Zero, uHeight, uWidth)
                );
            }
            else if (typeof(T) == typeof(uint))
            {
                return ErrorHelper.Check(
                    NativeBindings.pl_series_new_array_u32(name, (uint*)pData, flatLen, IntPtr.Zero, uHeight, uWidth)
                );
            }
            else if (typeof(T) == typeof(ulong))
            {
                return ErrorHelper.Check(
                    NativeBindings.pl_series_new_array_u64(name, (ulong*)pData, flatLen, IntPtr.Zero, uHeight, uWidth)
                );
            }
            else if (typeof(T) == typeof(decimal))
            {
                // T is decimal, so pData is actually decimal*
                decimal* pDec = (decimal*)pData;

                // Call Packer to convert decimal[] layout to Int128[] layout + Auto Scale
                // Note: Ensure DecimalPacker has Pack(decimal* src, int len) overload
                // If not, use the logic provided in previous turns.
                var (int128Values, scale) = DecimalPacker.Pack(pDec, (int)flatLen);

                // Pin the converted Int128 values
                fixed (Int128* pVals = int128Values)
                {
                    return ErrorHelper.Check(
                        NativeBindings.pl_series_new_array_decimal(
                            name,
                            pVals,
                            (UIntPtr)flatLen,
                            IntPtr.Zero, // Dense Array = No Validity Bitmap
                            (UIntPtr)height,
                            (UIntPtr)width,
                            (UIntPtr)scale
                        )
                    );
                }
            }
            else if (typeof(T) == typeof(Int128))
            {
                Int128* pRaw = (Int128*)pData;

                // // Check whether mem layout need Swap
                // if (ArrayHelper.Int128NeedsSwap)
                // {
                //     // Need Swap：Alloc new array -> Swap Copy -> transfer new ptr
                //     Int128[] swapped = Int128Packer.PackDense(pRaw, (int)flatLen);
                //     fixed (Int128* pSwapped = swapped)
                //     {
                //         return ErrorHelper.Check(
                //             NativeBindings.pl_series_new_array_i128(name, pSwapped, flatLen, IntPtr.Zero, uHeight, uWidth)
                //         );
                //     }
                // }
                // else
                // {
                //     // Zero-Copy
                return ErrorHelper.Check(
                    NativeBindings.pl_series_new_array_i128(name, pRaw, flatLen, IntPtr.Zero, uHeight, uWidth)
                    );
                // }
            }
            else if (typeof(T) == typeof(UInt128))
            {
                UInt128* pRaw = (UInt128*)pData;
                //  if (ArrayHelper.Int128NeedsSwap)
                //  {
                //     UInt128[] swapped = Int128Packer.PackDense(pRaw, (int)flatLen);
                //     fixed (UInt128* pSwapped = swapped)
                //     {
                //         return ErrorHelper.Check(
                //             NativeBindings.pl_series_new_array_u128(name, pSwapped, flatLen, IntPtr.Zero, uHeight, uWidth)
                //         );
                //     }
                //  }
                //  else
                //  {
                    return ErrorHelper.Check(
                        NativeBindings.pl_series_new_array_u128(name, pRaw, flatLen, IntPtr.Zero, uHeight, uWidth)
                    );
                //  }
            }
            else
            {
                throw new NotSupportedException($"Type '{typeof(T).Name}' is not supported for FixedSizeList Series.");
            }
        }
    }
    public static SeriesHandle SeriesNewStruct(string name, SeriesHandle[] handles)
    {
        // Borrow Semantics
        using var handlesLock = new SafeHandleLock<SeriesHandle>(handles);
        
        var newSeriesPtr = NativeBindings.pl_series_new_struct(
            name, 
            handlesLock.Pointers, 
            (nuint)handlesLock.Pointers.Length
        );

        return ErrorHelper.Check(newSeriesPtr);
    }
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