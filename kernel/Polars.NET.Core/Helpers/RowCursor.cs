using Polars.NET.Core.Enums;
using System.Runtime.CompilerServices;
using Polars.NET.Core.Wrappers;
using Kaminari.Sazanami.Arrow;

namespace Polars.NET.Core.Helpers;

internal static class RowCursor
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IsSupportedNumericType(Type t) =>
        t == typeof(int) || t == typeof(long) || t == typeof(double) ||
        t == typeof(float) || t == typeof(uint) || t == typeof(ulong) ||
        t == typeof(short) || t == typeof(ushort) || t == typeof(byte) ||
        t == typeof(sbyte);

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
