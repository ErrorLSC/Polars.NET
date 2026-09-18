namespace Polars.NET.Core.Helpers;

using System.Runtime.CompilerServices;
// using System.Runtime.InteropServices;
/// <summary>
/// Packed column metadata descriptor for zero-overhead cursor reads.
/// Fits cleanly into CPU registers (16 bytes).
/// </summary>
// [StructLayout(LayoutKind.Sequential, Pack = 4)]
// public readonly struct ColumnMeta(
//     PlDataType dtype,
//     int scale = 0,
//     int precision = 0,
//     PlTimeUnit timeUnit = PlTimeUnit.Microseconds,
//     string? timeZone = null)
// {
//     public readonly PlDataType DType = dtype;
//     public readonly PlTimeUnit TimeUnit = timeUnit;
//     public readonly int Scale = scale;
//     public readonly int Precision = precision;
//     public readonly string? TimeZone = timeZone;
// }
internal static class RowCursor
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IsSupportedNumericType(Type t) =>
        t == typeof(int) || t == typeof(long) || t == typeof(double) ||
        t == typeof(float) || t == typeof(uint) || t == typeof(ulong) ||
        t == typeof(short) || t == typeof(ushort) || t == typeof(byte) ||
        t == typeof(sbyte);

    // [MethodImpl(MethodImplOptions.AggressiveInlining)]
    // public static T GetValue<T>(SeriesHandle series, long index, in ColumnMeta meta)
    // {
    //     var type = typeof(T);
    //     var underlying = Nullable.GetUnderlyingType(type) ?? type;
    //     bool isNullable = type != underlying;
    //     var dtype = meta.DType;

    //     // 1. Numeric Fast Paths (Exact Physical Match)
    //     if (underlying == typeof(int) && dtype == PlDataType.Int32)
    //     {
    //         int val = PolarsWrapper.SeriesGetInt32Fast(series, index);
    //         return isNullable ? (T)(object)(int?)val : Unsafe.As<int, T>(ref val);
    //     }
    //     if (underlying == typeof(long) && dtype == PlDataType.Int64)
    //     {
    //         long val = PolarsWrapper.SeriesGetInt64Fast(series, index);
    //         return isNullable ? (T)(object)(long?)val : Unsafe.As<long, T>(ref val);
    //     }
    //     if (underlying == typeof(double) && dtype == PlDataType.Float64)
    //     {
    //         double val = PolarsWrapper.SeriesGetDoubleFast(series, index);
    //         return isNullable ? (T)(object)(double?)val : Unsafe.As<double, T>(ref val);
    //     }
    //     if (underlying == typeof(float) && dtype == PlDataType.Float32)
    //     {
    //         float val = PolarsWrapper.SeriesGetSingleFast(series, index);
    //         return isNullable ? (T)(object)(float?)val : Unsafe.As<float, T>(ref val);
    //     }
    //     if (underlying == typeof(uint) && dtype == PlDataType.UInt32)
    //     {
    //         uint val = PolarsWrapper.SeriesGetUInt32Fast(series, index);
    //         return isNullable ? (T)(object)(uint?)val : Unsafe.As<uint, T>(ref val);
    //     }
    //     if (underlying == typeof(ulong) && dtype == PlDataType.UInt64)
    //     {
    //         ulong val = PolarsWrapper.SeriesGetUInt64Fast(series, index);
    //         return isNullable ? (T)(object)(ulong?)val : Unsafe.As<ulong, T>(ref val);
    //     }
    //     if (underlying == typeof(short) && dtype == PlDataType.Int16)
    //     {
    //         short val = PolarsWrapper.SeriesGetInt16Fast(series, index);
    //         return isNullable ? (T)(object)(short?)val : Unsafe.As<short, T>(ref val);
    //     }
    //     if (underlying == typeof(ushort) && dtype == PlDataType.UInt16)
    //     {
    //         ushort val = PolarsWrapper.SeriesGetUInt16Fast(series, index);
    //         return isNullable ? (T)(object)(ushort?)val : Unsafe.As<ushort, T>(ref val);
    //     }
    //     if (underlying == typeof(byte) && dtype == PlDataType.UInt8)
    //     {
    //         byte val = PolarsWrapper.SeriesGetUInt8Fast(series, index);
    //         return isNullable ? (T)(object)(byte?)val : Unsafe.As<byte, T>(ref val);
    //     }
    //     if (underlying == typeof(sbyte) && dtype == PlDataType.Int8)
    //     {
    //         sbyte val = PolarsWrapper.SeriesGetInt8Fast(series, index);
    //         return isNullable ? (T)(object)(sbyte?)val : Unsafe.As<sbyte, T>(ref val);
    //     }
    //     if (underlying == typeof(Half) && dtype == PlDataType.Float16)
    //     {
    //         Half val = PolarsWrapper.SeriesGetHalfFast(series, index);
    //         return isNullable ? (T)(object)(Half?)val : Unsafe.As<Half, T>(ref val);
    //     }
    //     if (underlying == typeof(Int128))
    //     {
    //         Int128 val = PolarsWrapper.SeriesGetInt128Fast(series, index);
    //         return isNullable ? (T)(object)(Int128?)val : Unsafe.As<Int128, T>(ref val);
    //     }
    //     if (underlying == typeof(UInt128))
    //     {
    //         UInt128 val = PolarsWrapper.SeriesGetUInt128Fast(series, index);
    //         return isNullable ? (T)(object)(UInt128?)val : Unsafe.As<UInt128, T>(ref val);
    //     }

    //     // 2. Numeric Coercion
    //     if (IsSupportedNumericType(underlying) && IsNumeric(dtype))
    //     {
    //         return CoerceNumericValue<T>(series, index, dtype, underlying);
    //     }

    //     // 3. Boolean
    //     if (underlying == typeof(bool))
    //     {
    //         bool val = PolarsWrapper.SeriesGetBoolFast(series, index);
    //         return isNullable ? (T)(object)(bool?)val : Unsafe.As<bool, T>(ref val);
    //     }

    //     // 4. String & Categorical / Enum
    //     if (underlying == typeof(string))
    //     {
    //         if (dtype != PlDataType.Categorical && dtype != PlDataType.Enum)
    //         {
    //             string? strVal = PolarsWrapper.SeriesGetStringFast(series, index);
    //             return Unsafe.As<string?, T>(ref strVal);
    //         }
    //         // else
    //         // {
    //         //     string? strVal = PolarsWrapper.SeriesGetCatOrEnumStringFast(series, index);
    //         //     return Unsafe.As<string?, T>(ref strVal);
    //         // }
    //     }

    //     // 5. Decimal
    //     if (underlying == typeof(decimal))
    //     {
    //         decimal val = PolarsWrapper.SeriesGetDecimalFast(series, index, scale: meta.Scale, precision: meta.Precision);
    //         return isNullable ? (T)(object)(decimal?)val : Unsafe.As<decimal, T>(ref val);
    //     }

    //     // 6. Temporal
    //     if (underlying == typeof(DateOnly))
    //     {
    //         DateOnly val = PolarsWrapper.SeriesGetDateFast(series, index);
    //         return isNullable ? (T)(object)(DateOnly?)val : Unsafe.As<DateOnly, T>(ref val);
    //     }

    //     if (underlying == typeof(TimeOnly))
    //     {
    //         TimeOnly val = PolarsWrapper.SeriesGetTimeFast(series, index);
    //         return isNullable ? (T)(object)(TimeOnly?)val : Unsafe.As<TimeOnly, T>(ref val);
    //     }

    //     if (underlying == typeof(TimeSpan))
    //     {
    //         TimeSpan val = PolarsWrapper.SeriesGetDurationFast(series, index, meta.TimeUnit);
    //         return isNullable ? (T)(object)(TimeSpan?)val : Unsafe.As<TimeSpan, T>(ref val);
    //     }

    //     if (underlying == typeof(DateTime))
    //     {
    //         DateTime dt = PolarsWrapper.SeriesGetDatetimeFast(series, index, meta.TimeUnit, null);
    //         return isNullable ? (T)(object)(DateTime?)dt : Unsafe.As<DateTime, T>(ref dt);
    //     }

    //     if (underlying == typeof(ValueTuple<DateTime, string>))
    //     {
    //         DateTime dt = PolarsWrapper.SeriesGetDatetimeFast(series, index, meta.TimeUnit, meta.TimeZone);
    //         var tuple = (dt, meta.TimeZone ?? string.Empty);
    //         return isNullable ? (T)(object)((DateTime, string)?)tuple : Unsafe.As<(DateTime, string), T>(ref tuple);
    //     }

    //     // 7. Universal Path (Arrow Infrastructure)
    //     using var slice = PolarsWrapper.SeriesSlice(series, index, 1);
    //     using var column = PolarsWrapper.SeriesToArrow(slice);
    //     return ArrowReader.ReadItem<T>(column, 0)!;
    // }
    /// <summary>
    /// Reads physical numeric scalar and converts it to the underlying non-nullable numeric type,
    /// then packages it into T (handling both T and Nullable&lt;T&gt; without heap allocation).
    /// </summary>
    internal static T CoerceNumericValue<T>(SeriesHandle series, long index, PlDataType actualDtype, Type underlyingType)
    {
        if (actualDtype == PlDataType.Int64)
            return RowCursor.CoerceFromInt64<T>(PolarsWrapper.SeriesGetInt64Fast(series, index), underlyingType);
        if (actualDtype == PlDataType.Int32)
            return RowCursor.CoerceFromInt64<T>(PolarsWrapper.SeriesGetInt32Fast(series, index), underlyingType);
        if (actualDtype == PlDataType.Int16)
            return RowCursor.CoerceFromInt64<T>(PolarsWrapper.SeriesGetInt16Fast(series, index), underlyingType);
        if (actualDtype == PlDataType.Int8)
            return RowCursor.CoerceFromInt64<T>(PolarsWrapper.SeriesGetInt8Fast(series, index), underlyingType);

        if (actualDtype == PlDataType.UInt64)
            return RowCursor.CoerceFromUInt64<T>(PolarsWrapper.SeriesGetUInt64Fast(series, index), underlyingType);
        if (actualDtype == PlDataType.UInt32)
            return RowCursor.CoerceFromUInt64<T>(PolarsWrapper.SeriesGetUInt32Fast(series, index), underlyingType);
        if (actualDtype == PlDataType.UInt16)
            return RowCursor.CoerceFromUInt64<T>(PolarsWrapper.SeriesGetUInt16Fast(series, index), underlyingType);
        if (actualDtype == PlDataType.UInt8)
            return RowCursor.CoerceFromUInt64<T>(PolarsWrapper.SeriesGetUInt8Fast(series, index), underlyingType);

        if (actualDtype == PlDataType.Float64)
            return RowCursor.CoerceFromDouble<T>(PolarsWrapper.SeriesGetDoubleFast(series, index), underlyingType);
        if (actualDtype == PlDataType.Float32)
            return RowCursor.CoerceFromDouble<T>(PolarsWrapper.SeriesGetSingleFast(series, index), underlyingType);
        if (actualDtype == PlDataType.Float16)
            return RowCursor.CoerceFromDouble<T>((float)PolarsWrapper.SeriesGetHalfFast(series, index), underlyingType);

        throw new InvalidOperationException($"Cannot coerce non-numeric series of type {actualDtype} to {underlyingType.Name}");
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static T CoerceFromInt64<T>(long v, Type underlying)
    {
        bool isNullable = typeof(T) != underlying;

        if (underlying == typeof(int))
        {
            int res = (int)v;
            return isNullable ? (T)(object)(int?)res : Unsafe.As<int, T>(ref res);
        }
        if (underlying == typeof(long))
        {
            long res = v;
            return isNullable ? (T)(object)(long?)res : Unsafe.As<long, T>(ref res);
        }
        if (underlying == typeof(double))
        {
            double res = v;
            return isNullable ? (T)(object)(double?)res : Unsafe.As<double, T>(ref res);
        }
        if (underlying == typeof(float))
        {
            float res = v;
            return isNullable ? (T)(object)(float?)res : Unsafe.As<float, T>(ref res);
        }
        if (underlying == typeof(uint))
        {
            uint res = (uint)v;
            return isNullable ? (T)(object)(uint?)res : Unsafe.As<uint, T>(ref res);
        }
        if (underlying == typeof(ulong))
        {
            ulong res = (ulong)v;
            return isNullable ? (T)(object)(ulong?)res : Unsafe.As<ulong, T>(ref res);
        }
        if (underlying == typeof(short))
        {
            short res = (short)v;
            return isNullable ? (T)(object)(short?)res : Unsafe.As<short, T>(ref res);
        }
        if (underlying == typeof(ushort))
        {
            ushort res = (ushort)v;
            return isNullable ? (T)(object)(ushort?)res : Unsafe.As<ushort, T>(ref res);
        }
        if (underlying == typeof(byte))
        {
            byte res = (byte)v;
            return isNullable ? (T)(object)(byte?)res : Unsafe.As<byte, T>(ref res);
        }
        if (underlying == typeof(sbyte))
        {
            sbyte res = (sbyte)v;
            return isNullable ? (T)(object)(sbyte?)res : Unsafe.As<sbyte, T>(ref res);
        }
        if (underlying == typeof(Half))
        {
            Half res = (Half)(float)v;
            return isNullable ? (T)(object)(Half?)res : Unsafe.As<Half, T>(ref res);
        }

        throw new InvalidOperationException($"Unsupported target numeric coercion type: {underlying.Name}");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static T CoerceFromUInt64<T>(ulong v, Type underlying)
    {
        bool isNullable = typeof(T) != underlying;

        if (underlying == typeof(ulong))
        {
            ulong res = v;
            return isNullable ? (T)(object)(ulong?)res : Unsafe.As<ulong, T>(ref res);
        }
        if (underlying == typeof(long))
        {
            long res = (long)v;
            return isNullable ? (T)(object)(long?)res : Unsafe.As<long, T>(ref res);
        }
        if (underlying == typeof(uint))
        {
            uint res = (uint)v;
            return isNullable ? (T)(object)(uint?)res : Unsafe.As<uint, T>(ref res);
        }
        if (underlying == typeof(int))
        {
            int res = (int)v;
            return isNullable ? (T)(object)(int?)res : Unsafe.As<int, T>(ref res);
        }
        if (underlying == typeof(double))
        {
            double res = v;
            return isNullable ? (T)(object)(double?)res : Unsafe.As<double, T>(ref res);
        }
        if (underlying == typeof(float))
        {
            float res = v;
            return isNullable ? (T)(object)(float?)res : Unsafe.As<float, T>(ref res);
        }
        if (underlying == typeof(ushort))
        {
            ushort res = (ushort)v;
            return isNullable ? (T)(object)(ushort?)res : Unsafe.As<ushort, T>(ref res);
        }
        if (underlying == typeof(short))
        {
            short res = (short)v;
            return isNullable ? (T)(object)(short?)res : Unsafe.As<short, T>(ref res);
        }
        if (underlying == typeof(byte))
        {
            byte res = (byte)v;
            return isNullable ? (T)(object)(byte?)res : Unsafe.As<byte, T>(ref res);
        }
        if (underlying == typeof(sbyte))
        {
            sbyte res = (sbyte)v;
            return isNullable ? (T)(object)(sbyte?)res : Unsafe.As<sbyte, T>(ref res);
        }
        if (underlying == typeof(Half))
        {
            Half res = (Half)(float)v;
            return isNullable ? (T)(object)(Half?)res : Unsafe.As<Half, T>(ref res);
        }

        throw new InvalidOperationException($"Unsupported target numeric coercion type: {underlying.Name}");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static T CoerceFromDouble<T>(double v, Type underlying)
    {
        bool isNullable = typeof(T) != underlying;

        if (underlying == typeof(double))
        {
            double res = v;
            return isNullable ? (T)(object)(double?)res : Unsafe.As<double, T>(ref res);
        }
        if (underlying == typeof(float))
        {
            float res = (float)v;
            return isNullable ? (T)(object)(float?)res : Unsafe.As<float, T>(ref res);
        }
        if (underlying == typeof(Half))
        {
            Half res = (Half)(float)v;
            return isNullable ? (T)(object)(Half?)res : Unsafe.As<Half, T>(ref res);
        }
        if (underlying == typeof(int))
        {
            int res = (int)v;
            return isNullable ? (T)(object)(int?)res : Unsafe.As<int, T>(ref res);
        }
        if (underlying == typeof(long))
        {
            long res = (long)v;
            return isNullable ? (T)(object)(long?)res : Unsafe.As<long, T>(ref res);
        }
        if (underlying == typeof(uint))
        {
            uint res = (uint)v;
            return isNullable ? (T)(object)(uint?)res : Unsafe.As<uint, T>(ref res);
        }
        if (underlying == typeof(ulong))
        {
            ulong res = (ulong)v;
            return isNullable ? (T)(object)(ulong?)res : Unsafe.As<ulong, T>(ref res);
        }
        if (underlying == typeof(short))
        {
            short res = (short)v;
            return isNullable ? (T)(object)(short?)res : Unsafe.As<short, T>(ref res);
        }
        if (underlying == typeof(ushort))
        {
            ushort res = (ushort)v;
            return isNullable ? (T)(object)(ushort?)res : Unsafe.As<ushort, T>(ref res);
        }
        if (underlying == typeof(byte))
        {
            byte res = (byte)v;
            return isNullable ? (T)(object)(byte?)res : Unsafe.As<byte, T>(ref res);
        }
        if (underlying == typeof(sbyte))
        {
            sbyte res = (sbyte)v;
            return isNullable ? (T)(object)(sbyte?)res : Unsafe.As<sbyte, T>(ref res);
        }

        throw new InvalidOperationException($"Unsupported target numeric coercion type: {underlying.Name}");
    }
    internal static bool IsNumeric(PlDataType dtype) => dtype switch
    {
        PlDataType.Int8 or PlDataType.Int16 or PlDataType.Int32 or PlDataType.Int64 or
        PlDataType.UInt8 or PlDataType.UInt16 or PlDataType.UInt32 or PlDataType.UInt64 or
        PlDataType.Float32 or PlDataType.Float64 or PlDataType.Decimal or
        PlDataType.Int128 or PlDataType.UInt128 or PlDataType.Float16=> true,
        _ => false
    };
}
