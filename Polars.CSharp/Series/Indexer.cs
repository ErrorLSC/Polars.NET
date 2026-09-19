using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;
using Polars.NET.Core.Helpers;
using Polars.NET.Core.Arrow;
using System.Runtime.CompilerServices;

namespace Polars.CSharp;

public partial class Series : IDisposable,IPolarsSeries
{
    /// <summary>
    /// Get an item at the specified index.
    /// </summary>
    public T? GetValue<T>(long index, bool uncheck = false)
    {
        if (!uncheck)
        {
            if (index < 0 || index >= Length)
                throw new IndexOutOfRangeException($"Index {index} is out of bounds for Series length {Length}.");
        }

        if (this.IsNullAt(index,uncheck:true))
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

        if (RowCursor.IsSupportedNumericType(underlying) && DataType.IsNumeric)
        {
            return RowCursor.CoerceNumericValue<T>(Handle,index, (PlDataType)DataType.Kind, underlying);
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
        if (DataType.IsCategorical)
        {
            string? strVal = PolarsWrapper.SeriesGetCatOrEnumFast(Handle, index, (PlCategoricalPhysical)DataType.Categories!.Physical());
            return Unsafe.As<string?, T>(ref strVal);
        }
        if (DataType.IsEnum)
        {
            string? strVal = PolarsWrapper.SeriesGetCatOrEnumFast(Handle, index, (PlCategoricalPhysical)DataType.EnumCategories!.Physical());
            return Unsafe.As<string?, T>(ref strVal);
        }
        if (underlying == typeof(string) && !DataType.IsCategorical && !DataType.IsEnum)
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
