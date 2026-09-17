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

        // 1. Numeric
        if (underlying == typeof(int) && DataType == DataType.Int32)
            return (T)(object)PolarsWrapper.SeriesGetInt32Fast(Handle, index);

        if (underlying == typeof(long) && DataType == DataType.Int64)
            return (T)(object)PolarsWrapper.SeriesGetInt64Fast(Handle, index);

        if (underlying == typeof(uint) && DataType == DataType.UInt32)
            return (T)(object)PolarsWrapper.SeriesGetUInt32Fast(Handle, index);

        if (underlying == typeof(ulong) && DataType == DataType.UInt64)
            return (T)(object)PolarsWrapper.SeriesGetUInt64Fast(Handle, index);

        if (underlying == typeof(short) && DataType == DataType.Int16)
            return (T)(object)PolarsWrapper.SeriesGetInt16Fast(Handle, index);

        if (underlying == typeof(ushort) && DataType == DataType.UInt16)
            return (T)(object)PolarsWrapper.SeriesGetUInt16Fast(Handle, index);

        if (underlying == typeof(sbyte) && DataType == DataType.Int8)
            return (T)(object)PolarsWrapper.SeriesGetInt8Fast(Handle, index);

        if (underlying == typeof(byte) && DataType == DataType.UInt8)
            return (T)(object)PolarsWrapper.SeriesGetUInt8Fast(Handle, index);
        if (underlying == typeof(Int128))
            return (T)(object)PolarsWrapper.SeriesGetInt128Fast(Handle, index);

        if (underlying == typeof(UInt128))
            return (T)(object)PolarsWrapper.SeriesGetUInt128Fast(Handle, index);

        if (underlying == typeof(double) && DataType == DataType.Float64)
            return (T)(object)PolarsWrapper.SeriesGetDoubleFast(Handle, index);
        if (underlying == typeof(float) && DataType == DataType.Float32)
            return (T)(object)PolarsWrapper.SeriesGetSingleFast(Handle, index);
        if (underlying == typeof(Half) && DataType == DataType.Float16)
            return (T)(object)PolarsWrapper.SeriesGetHalfFast(Handle, index);

        if (IsSupportedNumericType(underlying) && DataType.IsNumeric)
        {
            return CoerceNumericValue<T>(index, DataType, underlying);
        }
        // 2. Boolean
        if (underlying == typeof(bool))
            return (T)(object)PolarsWrapper.SeriesGetBoolFast(Handle, index);

        // 3. String
        if (underlying == typeof(string) && !DataType.IsCategorical)
        {
            string? strVal = PolarsWrapper.SeriesGetStringFast(Handle,index);
            return (T?)(object?)strVal;
        }

        // 4. Decimal
        if (underlying == typeof(decimal))
        {   
            int scale = DataType.Scale;
            int precision = DataType.Precision;
            return (T)(object)PolarsWrapper.SeriesGetDecimalFast(Handle, index,scale,precision);
        }
        // 5. Temporal (Time)
        if (underlying == typeof(DateOnly))
            return (T)(object)PolarsWrapper.SeriesGetDateFast(Handle, index);

        if (underlying == typeof(TimeOnly))
            return (T)(object)PolarsWrapper.SeriesGetTimeFast(Handle, index);

        if (underlying == typeof(TimeSpan))
        {
            TimeUnit timeUnit = DataType.TimeUnit;
            return (T)(object)PolarsWrapper.SeriesGetDurationFast(Handle, index,timeUnit.ToNative());
        }
        if (underlying == typeof(DateTime))
        {
            TimeUnit timeUnit = DataType.TimeUnit;
            DateTime dt = PolarsWrapper.SeriesGetDatetimeFast(Handle, index,timeUnit.ToNative(),null);

            return (T)(object)dt;
        }
        if (underlying == typeof(ValueTuple<DateTime, string>))
        {
            TimeUnit timeUnit = DataType.TimeUnit;
            string timeZone = DataType.TimeZone!;
            var dt = PolarsWrapper.SeriesGetDatetimeFast(Handle, index,timeUnit.ToNative(),timeZone);

            return (T)(object)(dt,timeZone);
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

    private T? CoerceNumericValue<T>(long index, DataType actualDtype, Type targetType)
    {
        object val;
        if (actualDtype == DataType.Int64)
            val = PolarsWrapper.SeriesGetInt64Fast(Handle, index);
        else if (actualDtype == DataType.Int32)
            val = PolarsWrapper.SeriesGetInt32Fast(Handle, index);
        else if (actualDtype == DataType.Float64)
            val = PolarsWrapper.SeriesGetDoubleFast(Handle, index);
        else if (actualDtype == DataType.Float32)
            val = PolarsWrapper.SeriesGetSingleFast(Handle, index);
        else if (actualDtype == DataType.Float16)
            val = PolarsWrapper.SeriesGetHalfFast(Handle, index);
        else if (actualDtype == DataType.UInt64)
            val = PolarsWrapper.SeriesGetUInt64Fast(Handle, index);
        else if (actualDtype == DataType.UInt32)
            val = PolarsWrapper.SeriesGetUInt32Fast(Handle, index);
        else if (actualDtype == DataType.Int16)
            val = PolarsWrapper.SeriesGetInt16Fast(Handle, index);
        else if (actualDtype == DataType.UInt16)
            val = PolarsWrapper.SeriesGetUInt16Fast(Handle, index);
        else if (actualDtype == DataType.Int8)
            val = PolarsWrapper.SeriesGetInt8Fast(Handle, index);
        else if (actualDtype == DataType.UInt8)
            val = PolarsWrapper.SeriesGetUInt8Fast(Handle, index);
        else
            throw new InvalidOperationException($"Cannot coerce non-numeric series of type {actualDtype} to {targetType.Name}");

        return (T?)(object?)Convert.ChangeType(val, targetType);
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
