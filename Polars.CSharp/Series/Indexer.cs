using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;

public partial class Series : IDisposable,IPolarsSeries
{
    // ==========================================
    // Scalar Accessors (Native Speed)
    // ==========================================

    /// <summary>
    /// Get an item at the specified index.
    /// Supports: int, long, double, bool, string, decimal, DateTime, TimeSpan, DateOnly, TimeOnly.
    /// </summary>
    public T? GetValue<T>(long index)
    {
        var type = typeof(T);
        var underlying = Nullable.GetUnderlyingType(type) ?? type;

        if (index < 0 || index >= Length)
            throw new IndexOutOfRangeException($"Index {index} is out of bounds for Series length {Length}.");

        // 1. Numeric
        if (underlying == typeof(int)) 
            return (T?)(object?)(int?)PolarsWrapper.SeriesGetInt(Handle, index); 
        if (underlying == typeof(uint)) 
            return (T?)(object?)(uint?)PolarsWrapper.SeriesGetInt(Handle, index); 
        if (underlying == typeof(long)) 
            return (T?)(object?)PolarsWrapper.SeriesGetInt(Handle, index);
        if (underlying == typeof(ulong)) 
            return (T?)(object?)(ulong?)PolarsWrapper.SeriesGetInt(Handle, index); 
        if (underlying == typeof(Int128)) 
            return (T?)(object?)PolarsWrapper.SeriesGetInt128(Handle, index);

        if (underlying == typeof(UInt128)) 
            return (T?)(object?)PolarsWrapper.SeriesGetUInt128(Handle, index);

        if (underlying == typeof(double)) 
            return (T?)(object?)PolarsWrapper.SeriesGetDouble(Handle, index);

        if (underlying == typeof(float)) 
            return (T?)(object?)(float?)PolarsWrapper.SeriesGetDouble(Handle, index);
        if (underlying == typeof(Half)) 
            return (T?)(object?)(Half?)PolarsWrapper.SeriesGetDouble(Handle, index);

        // 2. Boolean
        if (underlying == typeof(bool)) 
            return (T?)(object?)PolarsWrapper.SeriesGetBool(Handle, index);

        // 3. String
        if (underlying == typeof(string) && DataType != DataType.Categorical()) 
        {
            if (PolarsWrapper.SeriesIsNullAt(Handle, index))
            {
                return default!; 
            }

            var strVal = PolarsWrapper.SeriesGetString(Handle, index);
            
            return (T)(object)strVal!;
        }

        // 4. Decimal
        if (underlying == typeof(decimal))
            return (T?)(object?)PolarsWrapper.SeriesGetDecimal(Handle, index);

        // 5. Temporal (Time)
        if (underlying == typeof(DateOnly))
            return (T?)(object?)PolarsWrapper.SeriesGetDate(Handle, index);
            
        if (underlying == typeof(TimeOnly))
            return (T?)(object?)PolarsWrapper.SeriesGetTime(Handle, index);
            
        if (underlying == typeof(TimeSpan))
            return (T?)(object?)PolarsWrapper.SeriesGetDuration(Handle, index);
        if (underlying == typeof(DateTime))
        {

            var dtTuple = PolarsWrapper.SeriesGetDatetime(Handle, index);

            if (!dtTuple.HasValue) 
                return default;
            
            return (T)(object)dtTuple.Value.Value;
        }
        if (underlying == typeof(ValueTuple<DateTime, string>))
        {
            var dtTuple = PolarsWrapper.SeriesGetDatetime(Handle, index);
            if (!dtTuple.HasValue) 
                return default;

            return (T)(object)dtTuple.Value;
        }

        // ==============================================================
        // Universal Path - using Arrow Infrastructure
        // For Struct, List, F# Option, DateTimeOffset .etc
        // ==============================================================
        
        using var slice = Slice(index, 1);
        
        var column = slice.ToArrow();

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