using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;
using System.Runtime.CompilerServices;

namespace Polars.CSharp;

public partial class Series : IDisposable,IPolarsSeries
{
    // ==========================================
    // Scalar Accessors (Native Speed)
    // ==========================================

    /// <summary>
    /// Get an item at the specified index.
    /// </summary>
    public T? GetValue<T>(long index, bool? uncheck = false)
    {
        if (uncheck == false)
        {
            if (index < 0 || index >= Length)
                throw new IndexOutOfRangeException($"Index {index} is out of bounds for Series length {Length}.");
        }
        
        if (this.IsNullAt(index))
            return default;

        var type = typeof(T);
        var underlying = Nullable.GetUnderlyingType(type) ?? type;

        bool isNullable = type != underlying;

        // 1. Numeric Fast Path
        if (underlying == typeof(int) && DataType == DataType.Int32)
        {
            int val = PolarsWrapper.SeriesGetInt32Fast(Handle, index);
            return isNullable ? (T)(object)(int?)val : Unsafe.As<int, T>(ref val);
        }
        if (underlying == typeof(long) && DataType == DataType.Int64)
        {
            long val = PolarsWrapper.SeriesGetInt64Fast(Handle, index);
            return isNullable ? (T)(object)(long?)val : Unsafe.As<long, T>(ref val);
        }
        if (underlying == typeof(uint) && DataType == DataType.UInt32)
        {
            uint val = PolarsWrapper.SeriesGetUInt32Fast(Handle, index);
            return isNullable ? (T)(object)(uint?)val : Unsafe.As<uint, T>(ref val);
        }
        if (underlying == typeof(ulong) && DataType == DataType.UInt64)
        {
            ulong val = PolarsWrapper.SeriesGetUInt64Fast(Handle, index);
            return isNullable ? (T)(object)(ulong?)val : Unsafe.As<ulong, T>(ref val);
        }
        if (underlying == typeof(short) && DataType == DataType.Int16)
        {
            short val = PolarsWrapper.SeriesGetInt16Fast(Handle, index);
            return isNullable ? (T)(object)(short?)val : Unsafe.As<short, T>(ref val);
        }
        if (underlying == typeof(ushort) && DataType == DataType.UInt16)
        {
            ushort val = PolarsWrapper.SeriesGetUInt16Fast(Handle, index);
            return isNullable ? (T)(object)(ushort?)val : Unsafe.As<ushort, T>(ref val);
        }
        if (underlying == typeof(sbyte) && DataType == DataType.Int8)
        {
            sbyte val = PolarsWrapper.SeriesGetInt8Fast(Handle, index);
            return isNullable ? (T)(object)(sbyte?)val : Unsafe.As<sbyte, T>(ref val);
        }
        if (underlying == typeof(byte) && DataType == DataType.UInt8)
        {
            byte val = PolarsWrapper.SeriesGetUInt8Fast(Handle, index);
            return isNullable ? (T)(object)(byte?)val : Unsafe.As<byte, T>(ref val);
        }
        if (underlying == typeof(double) && DataType == DataType.Float64)
        {
            double val = PolarsWrapper.SeriesGetDoubleFast(Handle, index);
            return isNullable ? (T)(object)(double?)val : Unsafe.As<double, T>(ref val);
        }
        if (underlying == typeof(float) && DataType == DataType.Float32)
        {
            float val = PolarsWrapper.SeriesGetSingleFast(Handle, index);
            return isNullable ? (T)(object)(float?)val : Unsafe.As<float, T>(ref val);
        }
        if (underlying == typeof(Half) && DataType == DataType.Float16)
        {
            Half val = PolarsWrapper.SeriesGetHalfFast(Handle, index);
            return isNullable ? (T)(object)(Half?)val : Unsafe.As<Half, T>(ref val);
        }
        if (underlying == typeof(Int128))
        {
            Int128 val = PolarsWrapper.SeriesGetInt128Fast(Handle, index);
            return isNullable ? (T)(object)(Int128?)val : Unsafe.As<Int128, T>(ref val);
        }
        if (underlying == typeof(UInt128))
        {
            UInt128 val = PolarsWrapper.SeriesGetUInt128Fast(Handle, index);
            return isNullable ? (T)(object)(UInt128?)val : Unsafe.As<UInt128, T>(ref val);
        }

        if (IsSupportedNumericType(underlying) && DataType.IsNumeric)
        {
            return CoerceNumericValue<T>(index, DataType, underlying);
        }       
        // ==============================================================
        // 2. Boolean
        // ==============================================================
        if (underlying == typeof(bool))
        {
            bool val = PolarsWrapper.SeriesGetBoolFast(Handle, index);
            return isNullable ? (T)(object)(bool?)val : Unsafe.As<bool, T>(ref val);
        }

        // ==============================================================
        // 3. String (Reference type - zero boxing via ref reinterpret)
        // ==============================================================
        if (underlying == typeof(string) && !DataType.IsCategorical)
        {
            string? strVal = PolarsWrapper.SeriesGetStringFast(Handle, index);
            return Unsafe.As<string?, T>(ref strVal);
        }

        // ==============================================================
        // 4. Decimal
        // ==============================================================
        if (underlying == typeof(decimal))
        {   
            int scale = DataType.Scale;
            int precision = DataType.Precision;
            decimal val = PolarsWrapper.SeriesGetDecimalFast(Handle, index, scale, precision);
            return isNullable ? (T)(object)(decimal?)val : Unsafe.As<decimal, T>(ref val);
        }

        // ==============================================================
        // 5. Temporal (Time)
        // ==============================================================
        if (underlying == typeof(DateOnly))
        {
            DateOnly val = PolarsWrapper.SeriesGetDateFast(Handle, index);
            return isNullable ? (T)(object)(DateOnly?)val : Unsafe.As<DateOnly, T>(ref val);
        }

        if (underlying == typeof(TimeOnly))
        {
            TimeOnly val = PolarsWrapper.SeriesGetTimeFast(Handle, index);
            return isNullable ? (T)(object)(TimeOnly?)val : Unsafe.As<TimeOnly, T>(ref val);
        }

        if (underlying == typeof(TimeSpan))
        {
            TimeUnit timeUnit = DataType.TimeUnit;
            TimeSpan val = PolarsWrapper.SeriesGetDurationFast(Handle, index, timeUnit.ToNative());
            return isNullable ? (T)(object)(TimeSpan?)val : Unsafe.As<TimeSpan, T>(ref val);
        }

        if (underlying == typeof(DateTime))
        {
            TimeUnit timeUnit = DataType.TimeUnit;
            DateTime dt = PolarsWrapper.SeriesGetDatetimeFast(Handle, index, timeUnit.ToNative(), null);
            return isNullable ? (T)(object)(DateTime?)dt : Unsafe.As<DateTime, T>(ref dt);
        }

        if (underlying == typeof(ValueTuple<DateTime, string>))
        {
            TimeUnit timeUnit = DataType.TimeUnit;
            string timeZone = DataType.TimeZone!;
            DateTime dt = PolarsWrapper.SeriesGetDatetimeFast(Handle, index, timeUnit.ToNative(), timeZone);
            var tuple = (dt, timeZone);
            return isNullable ? (T)(object)((DateTime, string)?)tuple : Unsafe.As<(DateTime, string), T>(ref tuple);
        }

        // ==============================================================
        // Universal Path - using Arrow Infrastructure
        // For Struct, List, F# Option, DateTimeOffset .etc
        // ==============================================================

        using var slice = Slice(index, 1);

        using var column = slice.ToArrow();

        return ArrowReader.ReadItem<T>(column, 0);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsSupportedNumericType(Type t) =>
        t == typeof(int) || t == typeof(long) || t == typeof(double) ||
        t == typeof(float) || t == typeof(uint) || t == typeof(ulong) ||
        t == typeof(short) || t == typeof(ushort) || t == typeof(byte) ||
        t == typeof(sbyte);

    /// <summary>
    /// Reads physical numeric scalar and converts it to the underlying non-nullable numeric type,
    /// then packages it into T (handling both T and Nullable&lt;T&gt; without heap allocation).
    /// </summary>
    private T CoerceNumericValue<T>(long index, DataType actualDtype, Type underlyingType)
    {
        if (actualDtype == DataType.Int64)
            return CoerceFromInt64<T>(PolarsWrapper.SeriesGetInt64Fast(Handle, index), underlyingType);
        if (actualDtype == DataType.Int32)
            return CoerceFromInt64<T>(PolarsWrapper.SeriesGetInt32Fast(Handle, index), underlyingType);
        if (actualDtype == DataType.Int16)
            return CoerceFromInt64<T>(PolarsWrapper.SeriesGetInt16Fast(Handle, index), underlyingType);
        if (actualDtype == DataType.Int8)
            return CoerceFromInt64<T>(PolarsWrapper.SeriesGetInt8Fast(Handle, index), underlyingType);

        if (actualDtype == DataType.UInt64)
            return CoerceFromUInt64<T>(PolarsWrapper.SeriesGetUInt64Fast(Handle, index), underlyingType);
        if (actualDtype == DataType.UInt32)
            return CoerceFromUInt64<T>(PolarsWrapper.SeriesGetUInt32Fast(Handle, index), underlyingType);
        if (actualDtype == DataType.UInt16)
            return CoerceFromUInt64<T>(PolarsWrapper.SeriesGetUInt16Fast(Handle, index), underlyingType);
        if (actualDtype == DataType.UInt8)
            return CoerceFromUInt64<T>(PolarsWrapper.SeriesGetUInt8Fast(Handle, index), underlyingType);

        if (actualDtype == DataType.Float64)
            return CoerceFromDouble<T>(PolarsWrapper.SeriesGetDoubleFast(Handle, index), underlyingType);
        if (actualDtype == DataType.Float32)
            return CoerceFromDouble<T>(PolarsWrapper.SeriesGetSingleFast(Handle, index), underlyingType);
        if (actualDtype == DataType.Float16)
            return CoerceFromDouble<T>((float)PolarsWrapper.SeriesGetHalfFast(Handle, index), underlyingType);

        throw new InvalidOperationException($"Cannot coerce non-numeric series of type {actualDtype} to {underlyingType.Name}");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T CoerceFromInt64<T>(long v, Type underlying)
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
    private static T CoerceFromUInt64<T>(ulong v, Type underlying)
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
    private static T CoerceFromDouble<T>(double v, Type underlying)
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
    /// <summary>
    /// Get an item at the specified index as object (boxed).
    /// </summary>
    /// <param name="index"></param>
    /// <returns></returns>
    /// <exception cref="NotSupportedException"></exception>
    public object? this[int index]
    {
        get
        {
            return DataType.Kind switch
            {
                // Integer
                DataTypeKind.Int8 => GetValue<sbyte?>(index),
                DataTypeKind.Int16 => GetValue<short?>(index),
                DataTypeKind.Int32 => GetValue<int?>(index),
                DataTypeKind.Int64 => GetValue<long?>(index),
                DataTypeKind.Int128 => GetValue<Int128?>(index),
                DataTypeKind.UInt8 => GetValue<byte?>(index),
                DataTypeKind.UInt16 => GetValue<ushort?>(index),
                DataTypeKind.UInt32 => GetValue<uint?>(index),
                DataTypeKind.UInt64 => GetValue<ulong?>(index),
                DataTypeKind.UInt128 => GetValue<UInt128?>(index),
                DataTypeKind.Decimal => GetValue<decimal?>(index),

                // float
                DataTypeKind.Float16 => GetValue<Half?>(index),
                DataTypeKind.Float32 => GetValue<float?>(index),
                DataTypeKind.Float64 => GetValue<double?>(index),

                // bool
                DataTypeKind.Boolean => GetValue<bool?>(index),

                // stirng
                DataTypeKind.String => GetValue<string>(index),

                // Duration
                DataTypeKind.Duration => GetValue<TimeSpan?>(index),

                //  Time -> TimeOnly
                DataTypeKind.Time => GetValue<TimeOnly?>(index),

                // DateTime
                DataTypeKind.Date => GetValue<DateOnly?>(index),
                DataTypeKind.Datetime => string.IsNullOrEmpty(DataType.TimeZone)
                    ? GetValue<DateTime?>(index)
                    : (object?)GetValue<DateTimeOffset?>(index),

                // Binary
                DataTypeKind.Binary => GetValue<byte[]>(index),

                // Complex Types
                DataTypeKind.List => GetValue<object>(index),
                DataTypeKind.Categorical => GetValue<object>(index),
                DataTypeKind.Struct => GetValue<object>(index),
                DataTypeKind.Array => GetValue<object>(index),

                _ => throw new NotSupportedException($"Indexer not supported for type {DataType.Kind}")
            };
        }
        set
        {
            if (index < 0 || index >= Length)
                throw new IndexOutOfRangeException($"Index {index} is out of bounds for length {Length}.");
            if (value is null)
                throw new ArgumentNullException(nameof(value), "Cannot set null via indexer currently.");

            using var idxSeries = Series.From("idx", [(uint)index]);

            var valArray = System.Array.CreateInstance(value.GetType(), 1);
            valArray.SetValue(value, 0);

            using var valSeries = Series.From("val", (dynamic)valArray);

            var newHandle = PolarsWrapper.SeriesSetWithIndex(Handle, idxSeries.Handle, valSeries.Handle);
            ReplaceInnerHandle(newHandle);
        }
    }
    /// <summary>
    /// e.g. s[s > 5] = 10;
    /// </summary>
    public object? this[Series key]
    {
        get
        {
            if (key.DataType == DataType.Boolean)
                return Filter(key);
            if (key.DataType == DataType.UInt32) return Take(Pl.Lit(key));
            throw new NotSupportedException($"Getter not supported for Series key type: {key.DataType}");
        }
        set
        {
            ArgumentNullException.ThrowIfNull(key);

            var valArray = System.Array.CreateInstance(value!.GetType(), 1);
            valArray.SetValue(value, 0);
            using var valSeries = Series.From("val", (dynamic)valArray);

            if (key.DataType == DataType.Boolean)
            {
                var newHandle = PolarsWrapper.SeriesSetWithMask(Handle, key.Handle, valSeries.Handle);
                ReplaceInnerHandle(newHandle);
            }
            else if (key.DataType.Kind is DataTypeKind.UInt32 or DataTypeKind.UInt64)
            {
                var newHandle = PolarsWrapper.SeriesSetWithIndex(Handle, key.Handle, valSeries.Handle);
                ReplaceInnerHandle(newHandle);
            }
            else
            {
                throw new ArgumentException($"Cannot set Series with key of dtype: {key.DataType}. Use Boolean or UInt32.");
            }
        }
    }

    /// <summary>
    /// e.g. s[0, 2, 5] = 99;
    /// </summary>
    public object? this[int[] indices]
    {
        get
        {
           return Take(Pl.Lit(indices));
        }
        set
        {
            uint[] uIndices = [.. indices.Select(i => (uint)i)];
            using var idxSeries = Series.From("idx", uIndices);

            this[idxSeries] = value;
        }
    }

    /// <summary>
    /// Range Indexer (__getitem__ for slices)
    /// e.g. s[1..5] / s[..^1]
    /// </summary>
    public Series this[Range range]
    {
        get => Slice(range);
    }
}
