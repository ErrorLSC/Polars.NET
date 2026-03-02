#pragma warning disable CS1591
using Polars.NET.Core;
using Apache.Arrow;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Helpers;

namespace Polars.CSharp;

/// <summary>
/// Represents a single column of data (1-dimensional array).
/// <para>
/// A Series is backed by Apache Arrow arrays and supports eager execution.
/// Operations on Series are generally performed immediately.
/// </para>
/// </summary>
public partial class Series : IDisposable,IPolarsSeries
{
    internal SeriesHandle Handle { get; }

    internal Series(SeriesHandle handle)
    {
        Handle = handle;
    }
    internal Series(string name, SeriesHandle handle)
    {
        PolarsWrapper.SeriesRename(handle, name);
        Handle = handle;
    }
    /// <summary>
    /// Rename Series
    /// </summary>
    /// <param name="newName"></param>
    public void Rename(string newName)
    {
        this.Name = newName;
    }

    /// <summary>
    /// Date Ops
    /// </summary>
    public SeriesDtOps Dt => new(this);
    /// <summary>
    /// String Ops
    /// </summary>
    public SeriesStrOps Str => new(this);
    /// <summary>
    /// Access list operations.
    /// </summary>
    public SeriesListOps List => new(this);
    /// <summary>
    /// Access Fixed-Size List (Array) operations.
    /// </summary>
    public SeriesArrayOps Array => new(this);
    /// <summary>
    /// Access struct operations.
    /// </summary>
    public SeriesStructOps Struct => new(this);

    /// <summary>
    /// Clone the Series
    /// </summary>
    /// <returns></returns>
    public Series Clone() => new(PolarsWrapper.CloneSeries(Handle));

    internal Series ApplyExpr(Expr expr)
    {
        using var df = new DataFrame(this);

        using var dfRes = df.Select(expr);

        return dfRes[0];
    }

    internal Series ApplyBinaryExpr(Series other, Func<Expr, Expr, Expr> op)
    {
        string leftName = this.Name;
        string rightName = other.Name;
        
        Series? tempRight = null;

        try
        {
            Series rightSeries;
            
            if (leftName == rightName)
            {
                rightName = "__other_temp__";
                tempRight = other.Clone();
                tempRight.Name = rightName;
                rightSeries = tempRight;
            }
            else
            {
                rightSeries = other;
            }

            using var df = new DataFrame([this, rightSeries]);

            using var resDf = df.Select(op(Polars.Col(leftName), Polars.Col(rightName)));

            return resDf[0];
        }
        finally
        {
            tempRight?.Dispose();
        }
    }
    // ==========================================
    // Metadata
    // ==========================================

    /// <summary>
    /// Get the string representation of the Series data type (e.g. "i64", "str", "datetime(μs)").
    /// </summary>
    public string DataTypeName => PolarsWrapper.GetSeriesDtypeString(Handle);
    /// <summary>
    /// Gets the DataType of the Series.
    /// </summary>
    /// <remarks>
    /// This property creates a new DataType instance every time it is accessed.
    /// Since DataType wraps a native handle, consider caching it locally if accessed frequently in a loop.
    /// </remarks>
    public DataType DataType
    {
        get
        {
            var handle = PolarsWrapper.GetSeriesDataType(Handle);
            
            return new DataType(handle);
        }
    }
    IPolarsDataType IPolarsSeries.DataType => DataType;
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
        if (underlying == typeof(string) && this.DataType != DataType.Categorical) 
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
        // if (underlying == typeof(DateTime))
            // return (T?)(object?)PolarsWrapper.SeriesGetDatetime(Handle, index);

        // ==============================================================
        // 🐢 Universal Path - using Arrow Infrastructure
        // For Struct, List, F# Option, DateTimeOffset .etc
        // ==============================================================
        
        using var slice = Slice(index, 1);
        
        var column = slice.ToArrow();

        return ArrowReader.ReadItem<T>(column, 0);
    }
    
    /// <summary>
    /// Get an item at the specified index as object (boxed).
    /// </summary>
    /// <summary>
    /// Syntax sugar: s[index]
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
                    DataTypeKind.Datetime => string.IsNullOrEmpty(this.DataType.TimeZone) 
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
    }
    // ==========================================
    // Arithmetic Operators
    // ==========================================
    /// <summary>
    /// Add Series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator +(Series left, Series right)
    {
        return new Series(PolarsWrapper.SeriesAdd(left.Handle, right.Handle));
    }
    /// <summary>
    /// Minus Series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator -(Series left, Series right)
    {
        return new Series(PolarsWrapper.SeriesSub(left.Handle, right.Handle));
    }
    /// <summary>
    /// Multiple Series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator *(Series left, Series right)
    {
        return new Series(PolarsWrapper.SeriesMul(left.Handle, right.Handle));
    }
    /// <summary>
    /// Divide Series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator /(Series left, Series right)
    {
        return new Series(PolarsWrapper.SeriesDiv(left.Handle, right.Handle));
    }
    /// <summary>
    /// Calculate absolute value.
    /// <para>Implemented via Expr composition.</para>
    /// </summary>
    public Series Abs() => ApplyExpr(Polars.Col(Name).Abs());
    /// <summary>
    /// Calculate square value.
    /// <para>Implemented via Expr composition.</para>
    /// </summary>
    public Series Sqrt() => ApplyExpr(Polars.Col(Name).Sqrt());
    /// <summary>
    /// Calculate the cube root of the expression.
    /// </summary>
    public Series Cbrt() => ApplyExpr(Polars.Col(Name).Cbrt());
    /// <summary>
    /// Calculate exponent value.
    /// <para>Implemented via Expr composition.</para>
    /// </summary>
    public Series Pow(double exponent) => ApplyExpr(Polars.Col(Name).Pow(exponent));
    /// <summary>
    /// Calculate the power of the Euler's number.
    /// </summary>
    public Series Exp() =>  ApplyExpr(Polars.Col(Name).Exp());
    /// <summary>
    /// Calculate the ln of Number 
    /// </summary>
    /// <param name="baseVal"></param>
    /// <returns></returns>
    public Series Ln(double baseVal = Math.E) => ApplyExpr(Polars.Col(Name).Ln(baseVal));
    // ==========================================
    // Linear Algebra (Dot Product)
    // ==========================================

    /// <summary>
    /// Compute the dot/inner product between two Series.
    /// <para>
    /// The behavior is equivalent to `(this * other).Sum()`.
    /// </para>
    /// </summary>
    /// <param name="other">The other Series to compute the dot product with.</param>
    /// <returns>A Series of length 1 containing the result.</returns>
    public Series Dot(Series other)
        => ApplyBinaryExpr(other, (left, right) => left.Dot(right));
    /// <summary>
    /// Compute the dot/inner product and return the scalar value directly.
    /// </summary>
    /// <typeparam name="T">The type of the result (e.g. double, long).</typeparam>
    /// <param name="other">The other Series.</param>
    /// <returns>The dot product value.</returns>
    public T? Dot<T>(Series other) => Dot(other).GetValue<T>(0);
    /// <summary>
    /// Round the number
    /// </summary>
    /// <param name="decimals"></param>
    /// <returns></returns>
    public Series Round(uint decimals) => ApplyExpr(Polars.Col(Name).Round(decimals));
    /// <summary>Compute the element-wise sign (-1, 0, 1).</summary>
    public Series Sign() => ApplyExpr(Polars.Col(Name).Sign());

    /// <summary>Rounds up to the nearest integer.</summary>
    public Series Ceil() => ApplyExpr(Polars.Col(Name).Ceil());

    /// <summary>Rounds down to the nearest integer.</summary>
    public Series Floor() => ApplyExpr(Polars.Col(Name).Floor());
    
    // ==========================================
    // Bitwise Operators (<<, >>)
    // ==========================================

    /// <summary>
    /// Bitwise left shift operation.
    /// </summary>
    public static Series operator <<(Series left, int right)
        => left.ApplyExpr(Polars.Col(left.Name) << right);

    /// <summary>
    /// Bitwise right shift operation.
    /// <para>
    /// For signed integers, this is arithmetic shift.
    /// For unsigned integers, this is logical shift.
    /// </para>
    /// </summary>
    public static Series operator >>(Series left, int right)
        => left.ApplyExpr(Polars.Col(left.Name) >> right);

    // ==========================================
    // Trigonometry
    // ==========================================

    /// <summary>Compute the element-wise sine.</summary>
    public Series Sin() => ApplyExpr(Polars.Col(Name).Sin());

    /// <summary>Compute the element-wise cosine.</summary>
    public Series Cos() => ApplyExpr(Polars.Col(Name).Cos());

    /// <summary>Compute the element-wise tangent.</summary>
    public Series Tan() => ApplyExpr(Polars.Col(Name).Tan());

    /// <summary>Compute the element-wise inverse sine.</summary>
    public Series ArcSin() => ApplyExpr(Polars.Col(Name).ArcSin());

    /// <summary>Compute the element-wise inverse cosine.</summary>
    public Series ArcCos() => ApplyExpr(Polars.Col(Name).ArcCos());

    /// <summary>Compute the element-wise inverse tangent.</summary>
    public Series ArcTan() => ApplyExpr(Polars.Col(Name).ArcTan());

    // Hyperbolic
    /// <summary>
    /// Compute the element-wise hyperbolic sine.
    /// </summary>
    public Series Sinh() => ApplyExpr(Polars.Col(Name).Sinh());

    /// <summary>
    /// Compute the element-wise hyperbolic cosine.
    /// </summary>
    public Series Cosh() => ApplyExpr(Polars.Col(Name).Cosh());

    /// <summary>
    /// Compute the element-wise hyperbolic tangent.
    /// </summary>
    public Series Tanh() => ApplyExpr(Polars.Col(Name).Tanh());

    /// <summary>
    /// Compute the element-wise inverse hyperbolic sine.
    /// </summary>
    public Series ArcSinh() => ApplyExpr(Polars.Col(Name).ArcSinh());

    /// <summary>
    /// Compute the element-wise inverse hyperbolic cosine.
    /// </summary>
    public Series ArcCosh() => ApplyExpr(Polars.Col(Name).ArcCosh());

    /// <summary>
    /// Compute the element-wise inverse hyperbolic tangent.
    /// </summary>
    public Series ArcTanh() => ApplyExpr(Polars.Col(Name).ArcTanh());

    // ==========================================
    // Comparison Methods & Operators
    // ==========================================
    /// <summary>
    /// Compare whether two Series is equal
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series Eq(Series other) => new(PolarsWrapper.SeriesEq(Handle, other.Handle));
    /// <summary>
    /// Compare whether two Series is not equal
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series Neq(Series other) => new(PolarsWrapper.SeriesNeq(Handle, other.Handle));
    /// <summary>
    /// Compare whether left series is greater than right series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator >(Series left, Series right) 
        => new(PolarsWrapper.SeriesGt(left.Handle, right.Handle));
    /// <summary>
    /// Compare whether left series is less than right series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator <(Series left, Series right) 
        => new(PolarsWrapper.SeriesLt(left.Handle, right.Handle));
    /// <summary>
    /// Compare whether left series is greater than or equal to right series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator >=(Series left, Series right) 
        => new(PolarsWrapper.SeriesGtEq(left.Handle, right.Handle));
    /// <summary>
    /// Compare whether left series is less than or equal to right series
    /// </summary>
    /// <param name="left"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Series operator <=(Series left, Series right) 
        => new(PolarsWrapper.SeriesLtEq(left.Handle, right.Handle));

    /// <summary>
    /// Compare whether left series is greater than right series
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series Gt(Series other) => this > other;
    /// <summary>
    /// Compare whether left series is less than right series
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series Lt(Series other) => this < other;
    /// <summary>
    /// Compare whether left series is greater than or equal to right series
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series GtEq(Series other) => this >= other;
    /// <summary>
    /// Compare whether left series is less than or equal to right series
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Series LtEq(Series other) => this <= other;

    // -------------------------------------------------------------------------
    // Boolean Aggregation
    // -------------------------------------------------------------------------

    /// <summary>
    /// <inheritdoc cref="Expr.Any(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.Any(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> (boolean, length 1).</returns>
    public Series Any(bool ignoreNulls = false) => ApplyExpr(Polars.Col(Name).Any(ignoreNulls));

    /// <summary>
    /// <inheritdoc cref="Expr.All(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.All(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> (boolean, length 1).</returns>
    public Series All(bool ignoreNulls = false) => ApplyExpr(Polars.Col(Name).All(ignoreNulls));

    // -------------------------------------------------------------------------
    // Aggregation
    // -------------------------------------------------------------------------

    /// <summary>
    /// <inheritdoc cref="Expr.First" path="/summary"/>
    /// </summary>
    /// <returns>A new <see cref="Series"/> containing the first value (length 1).</returns>
    public Series First() => ApplyExpr(Polars.Col(Name).First());

    /// <summary>
    /// <inheritdoc cref="Expr.Last" path="/summary"/>
    /// </summary>
    /// <returns>A new <see cref="Series"/> containing the last value (length 1).</returns>
    public Series Last() => ApplyExpr(Polars.Col(Name).Last());
    /// <summary>
    /// Sum series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Sum() => new(PolarsWrapper.SeriesSum(Handle));
    /// <summary>
    /// Mean series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Mean() => new(PolarsWrapper.SeriesMean(Handle));
    /// <summary>
    /// Min series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Min() => new(PolarsWrapper.SeriesMin(Handle));
    /// <summary>
    /// Max series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Max() => new(PolarsWrapper.SeriesMax(Handle));
    /// <summary>
    /// Product series into 1 length series(Scalar)
    /// </summary>
    /// <returns></returns>
    public Series Product() => ApplyExpr(Polars.Col(Name).Product());
    /// <summary>
    /// First series element into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? First<T>() => First().GetValue<T>(0);
    /// <summary>
    /// Last series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Last<T>() => Last().GetValue<T>(0);
    /// <summary>
    /// Sum series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Sum<T>() => Sum().GetValue<T>(0);
    /// <summary>
    /// Mean series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Mean<T>() => Mean().GetValue<T>(0);
    /// <summary>
    /// Min series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Min<T>() => Min().GetValue<T>(0);
    /// <summary>
    /// Max series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Max<T>() => Max().GetValue<T>(0);
    /// <summary>
    /// Product series into scalar
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T? Product<T>() => Product().GetValue<T>(0);
    // ==========================================
    // Constructors
    // ==========================================

   // 1. Signed Integers
    public Series(string name, sbyte[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, sbyte?[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, short[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, short?[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, int[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, int?[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, long[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, long?[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, Int128[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, Int128?[] data) => Handle = SeriesFactory.Create(name, data);

    // 2. Unsigned Integers
    public Series(string name, byte[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, byte?[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, ushort[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, ushort?[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, uint[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, uint?[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, ulong[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, ulong?[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, UInt128[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, UInt128?[] data) => Handle = SeriesFactory.Create(name, data);

    // 3. Floating Point
    public Series(string name, Half[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, Half?[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, float[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, float?[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, double[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, double?[] data) => Handle = SeriesFactory.Create(name, data);

    // 4. Bool, String, Decimal
    public Series(string name, bool[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, bool?[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, string?[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, decimal[] data ) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, decimal?[] data) => Handle = SeriesFactory.Create(name, data);

    // 5. Temporal
    public Series(string name, DateTime[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, DateTime?[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, DateTimeOffset[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, DateTimeOffset?[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, DateOnly[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, DateOnly?[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, TimeOnly[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, TimeOnly?[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, TimeSpan[] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, TimeSpan?[] data) => Handle = SeriesFactory.Create(name, data);

    // 6. Fixed Size Arrays (2D)
    public Series(string name, sbyte[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, byte[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, short[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, ushort[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, int[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, uint[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, long[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, ulong[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, Half[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, float[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, double[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, decimal[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, Int128[,] data) => Handle = SeriesFactory.Create(name, data);
    public Series(string name, UInt128[,] data) => Handle = SeriesFactory.Create(name, data);

    // ==========================================
    // Properties
    // ==========================================
    /// <summary>
    /// Length of the Series.
    /// </summary>
    public long Length => PolarsWrapper.SeriesLen(Handle);
    /// <summary>
    /// Return the length of the Series.
    /// </summary>
    public long Len() => Length;
    /// <summary>
    /// Name of the Series.
    /// </summary>
    public string Name 
    {
        get => PolarsWrapper.SeriesName(Handle);
        set => PolarsWrapper.SeriesRename(Handle, value);
    }
    /// <summary>
    /// Get the number of null values in the Series.
    /// </summary>
    public long NullCount => PolarsWrapper.SeriesNullCount(Handle);

    // ==========================================
    // Operations
    // ==========================================

    /// <summary>
    /// Cast the Series to a different DataType.
    /// </summary>
    public Series Cast(DataType dtype)=> new(PolarsWrapper.SeriesCast(Handle, dtype.Handle));
    /// <summary>
    /// Get a slice of this Series.
    /// </summary>
    /// <param name="offset">Start index. Negative values count from the end.</param>
    /// <param name="length">Length of the slice.</param>
    public Series Slice(long offset, long length)
    {
        var newHandle = PolarsWrapper.SeriesSlice(Handle, offset, length);
        return new Series(newHandle);
    }
    
    /// <summary>
    /// <inheritdoc cref="Expr.Reverse" path="/summary"/>
    /// </summary>
    /// <returns>A new <see cref="Series"/> with the order reversed.</returns>
    public Series Reverse() => ApplyExpr(Polars.Col(Name).Reverse());
    /// <summary>
    /// Convert Series to Arrow Array
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T[] ToArray<T>()
    {  
        var col = this.ToArrow();
        return ArrowReader.ReadColumn<T>(col);
    }
    // ==========================================
    // Null Checks & Boolean Masks
    // ==========================================

    /// <summary>
    /// Check whether indexed value is null。
    /// </summary>
    public bool IsNullAt(long index) => PolarsWrapper.SeriesIsNullAt(Handle, index);
    /// <summary>
    /// Return a Boolean series, where null value will be masked as true.
    /// </summary>
    public Series IsNull()
    {
        var newHandle = PolarsWrapper.SeriesIsNull(Handle);
        return new Series(newHandle);
    }

    /// <summary>
    /// Return a Boolean series, where null value will be masked as false.
    /// </summary>
    public Series IsNotNull()
    {
        var newHandle = PolarsWrapper.SeriesIsNotNull(Handle);
        return new Series(newHandle);
    }
    // ==========================================
    // Drop Nulls and Nans
    // ==========================================
    /// <summary>
    /// Drop Null Values
    /// </summary>
    public Series DropNulls()
    {
        var newHandle = PolarsWrapper.SeriesDropNulls(Handle);
        return new Series(newHandle);
    }
    /// <summary>
    /// Drop Nan Values
    /// </summary>
    public Series DropNans()
        => ApplyExpr(Polars.Col(Name).DropNans());
    // ==========================================
    // Fill Ops
    // ==========================================
    /// <summary>
    /// Fill null values with a specified value.
    /// </summary>
    public Series FillNull(object value) => ApplyExpr(Polars.Col(Name).FillNull(value));
    /// <summary>
    /// Fill null values with a specific strategy (Forward).
    /// </summary>
    public Series ForwardFill(uint? limit = null) => ApplyExpr(Polars.Col(Name).ForwardFill(limit));
    /// <summary>
    /// Fill null values with a specific strategy (Backward).
    /// </summary>
    public Series BackwardFill(uint? limit = null) => ApplyExpr(Polars.Col(Name).BackwardFill(limit));
    /// <summary>
    /// Interpolate intermediate values.
    /// </summary>
    /// <inheritdoc cref="Expr.Interpolate(InterpolationMethod)"/>
    public Series Interpolate(InterpolationMethod method = InterpolationMethod.Linear)
        => ApplyExpr(Polars.Col(Name).Interpolate(method));
    /// <summary>
    /// Interpolate intermediate values based on the values of another Series.
    /// <para>
    /// Useful for linear interpolation across unevenly spaced data.
    /// </para>
    /// </summary>
    /// <param name="by">The Series to use for interpolation (e.g. timestamps).</param>
    /// <returns>A new Series with interpolated values.</returns>
    public Series InterpolateBy(Series by)
        => ApplyBinaryExpr(by, (left, right) => left.InterpolateBy(right));
    /// <summary>
    /// Fill floating point NaN values with a specified value.
    /// Note: This is different from FillNull. It only handles IEEE 754 NaN.
    /// </summary>
    public Series FillNan(object value) => ApplyExpr(Polars.Col(Name).FillNan(value));
    // ==========================================
    // Top-K & Bottom-K
    // ==========================================
    /// <summary>
    /// Get the top k values.
    /// </summary>
    public Series TopK(int k) => ApplyExpr(Polars.Col(Name).TopK(k));

    /// <summary>
    /// Get the bottom k values.
    /// </summary>
    public Series BottomK(int k) => ApplyExpr(Polars.Col(Name).BottomK(k));
    /// <summary>
    /// <inheritdoc cref="Expr.TopKBy(int, Expr[], bool[])" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.TopKBy(int, Expr[], bool[])" path="/param"/>
    /// <returns>A new <see cref="Series"/> containing the top k elements.</returns>
    public Series TopKBy(int k, Expr[] by, bool[] reverse)
        => ApplyExpr(Polars.Col(Name).TopKBy(k, by, reverse));
    /// <summary>
    /// <inheritdoc cref="Expr.BottomKBy(int, Expr[], bool[])" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.BottomKBy(int, Expr[], bool[])" path="/param"/>
    /// <returns>A new <see cref="Series"/> containing the bottom k elements.</returns>
    public Series BottomKBy(int k, Expr[] by, bool[] reverse)
        => ApplyExpr(Polars.Col(Name).BottomKBy(k, by, reverse));
    /// <summary>
    /// <inheritdoc cref="Expr.TopKBy(int, Expr[], bool[])" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.TopKBy(int, Expr[], bool[])" path="/param"/>
    /// <returns>A new <see cref="Series"/> containing the top k elements.</returns>
    public Series TopKBy(int k, Expr by, bool reverse = false) 
        => TopKBy(k, [by], [reverse]);
    /// <summary>
    /// <inheritdoc cref="Expr.BottomKBy(int, Expr[], bool[])" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.BottomKBy(int, Expr[], bool[])" path="/param"/>
    /// <returns>A new <see cref="Series"/> containing the bottom k elements.</returns>
    public Series BottomKBy(int k, Expr by, bool reverse = false)
        => BottomKBy(k, [by], [reverse]);
    /// <summary>
    /// Get the top k values sorted by another Series.
    /// </summary>
    public Series TopKBy(int k, Series by, bool reverse = false)
        => ApplyExpr(Polars.Col(Name).TopKBy(k, Polars.Lit(by), reverse));
    /// <summary>
    /// Get the bottom k values sorted by another Series.
    /// </summary>
    public Series BottomKBy(int k, Series by, bool reverse = false)
        => ApplyExpr(Polars.Col(Name).BottomKBy(k, Polars.Lit(by), reverse));

    // ==========================================
    // Statistical Ops
    // ==========================================
    /// <summary>
    /// <inheritdoc cref="Expr.Count()" path="/summary"/>
    /// </summary>
    /// <returns>A new <see cref="Series"/> containing the count of non-null values.</returns>
    public Series Count() => ApplyExpr(Polars.Col(Name).Count());

    /// <summary>
    /// <inheritdoc cref="Expr.Std(int)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.Std(int)" path="/param"/>
    /// <returns>A new <see cref="Series"/> containing the standard deviation (length 1).</returns>
    public Series Std(int ddof = 1) => ApplyExpr(Polars.Col(Name).Std(ddof));

    /// <summary>
    /// <inheritdoc cref="Expr.Var(int)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.Var(int)" path="/param"/>
    /// <returns>A new <see cref="Series"/> containing the variance (length 1).</returns>
    public Series Var(int ddof = 1) => ApplyExpr(Polars.Col(Name).Var(ddof));

    /// <summary>
    /// <inheritdoc cref="Expr.Median()" path="/summary"/>
    /// </summary>
    /// <returns>A new <see cref="Series"/> containing the median value (length 1).</returns>
    public Series Median() => ApplyExpr(Polars.Col(Name).Median());
    /// <summary>
    /// <inheritdoc cref="Expr.Median()" path="/summary"/>
    /// </summary>
    /// <returns>A new <see cref="Series"/> containing the mode value (length 1).</returns>
    public Series Mode() => ApplyExpr(Polars.Col(Name).Mode());

    /// <summary>
    /// <inheritdoc cref="Expr.Skew(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.Skew(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> containing the skewness (length 1).</returns>
    public Series Skew(bool bias = true) => ApplyExpr(Polars.Col(Name).Skew(bias));

    /// <summary>
    /// <inheritdoc cref="Expr.Kurtosis(bool, bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.Kurtosis(bool, bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> containing the kurtosis (length 1).</returns>
    public Series Kurtosis(bool fisher = true, bool bias = true) 
        => ApplyExpr(Polars.Col(Name).Kurtosis(fisher, bias));

    /// <summary>
    /// <inheritdoc cref="Expr.Quantile(double, QuantileMethod)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.Quantile(double, QuantileMethod)" path="/param"/>
    /// <returns>A new <see cref="Series"/> containing the quantile value (length 1).</returns>
    public Series Quantile(double quantile, QuantileMethod method = QuantileMethod.Linear)
        => ApplyExpr(Polars.Col(Name).Quantile(quantile, method));

    /// <summary>
    /// <inheritdoc cref="Expr.PctChange(int)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.PctChange(int)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the percentage change.</returns>
    public Series PctChange(int n = 1) => ApplyExpr(Polars.Col(Name).PctChange(n));
    /// <summary>
    /// <inheritdoc cref="Expr.Rank(RankMethod, bool, ulong?)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.Rank(RankMethod, bool, ulong?)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the ranks.</returns>
    public Series Rank(RankMethod method = RankMethod.Average, bool descending = false, ulong? seed = null)
        => ApplyExpr(Polars.Col(Name).Rank(method, descending, seed));
    // ==========================================
    // Cumulative Functions
    // ==========================================
    /// <summary>
    /// <inheritdoc cref="Expr.CumSum(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.CumSum(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the cumulative sum.</returns>
    public Series CumSum(bool reverse = false) 
        => ApplyExpr(Polars.Col(Name).CumSum(reverse));

    /// <summary>
    /// <inheritdoc cref="Expr.CumMax(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.CumMax(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the cumulative maximum.</returns>
    public Series CumMax(bool reverse = false) 
        => ApplyExpr(Polars.Col(Name).CumMax(reverse));

    /// <summary>
    /// <inheritdoc cref="Expr.CumMin(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.CumMin(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the cumulative minimum.</returns>
    public Series CumMin(bool reverse = false) 
        => ApplyExpr(Polars.Col(Name).CumMin(reverse));

    /// <summary>
    /// <inheritdoc cref="Expr.CumProd(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.CumProd(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the cumulative product.</returns>
    public Series CumProd(bool reverse = false) 
        => ApplyExpr(Polars.Col(Name).CumProd(reverse));

    /// <summary>
    /// <inheritdoc cref="Expr.CumCount(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.CumCount(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the cumulative count.</returns>
    public Series CumCount(bool reverse = false) 
        => ApplyExpr(Polars.Col(Name).CumCount(reverse));
    // ==========================================
    // EWM Functions
    // ==========================================
    /// <summary>
    /// <inheritdoc cref="Expr.EwmMean(double, bool, bool, int, bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.EwmMean(double, bool, bool, int, bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the EWM mean.</returns>
    public Series EwmMean(double alpha, bool adjust = true, bool bias = true, int minPeriods = 1, bool ignoreNulls = false)
        => ApplyExpr(Polars.Col(Name).EwmMean(alpha, adjust, bias, minPeriods, ignoreNulls));

    /// <summary>
    /// <inheritdoc cref="Expr.EwmStd(double, bool, bool, int, bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.EwmStd(double, bool, bool, int, bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the EWM standard deviation.</returns>
    public Series EwmStd(double alpha, bool adjust = true, bool bias = true, int minPeriods = 1, bool ignoreNulls = false)
        => ApplyExpr(Polars.Col(Name).EwmStd(alpha, adjust, bias, minPeriods, ignoreNulls));

    /// <summary>
    /// <inheritdoc cref="Expr.EwmVar(double, bool, bool, int, bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.EwmVar(double, bool, bool, int, bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the EWM variance.</returns>
    public Series EwmVar(double alpha, bool adjust = true, bool bias = true, int minPeriods = 1, bool ignoreNulls = false)
        => ApplyExpr(Polars.Col(Name).EwmVar(alpha, adjust, bias, minPeriods, ignoreNulls));
    
    // -------------------------------------------------------------------------
    // EWM By (Time/Index based)
    // -------------------------------------------------------------------------

    /// <summary>
    /// <inheritdoc cref="Expr.EwmMeanBy(Expr, string)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.EwmMeanBy(Expr, string)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the time/index-based EWM mean.</returns>
    public Series EwmMeanBy(Expr by, string halfLife)
        => ApplyExpr(Polars.Col(Name).EwmMeanBy(by, halfLife));

    // ==========================================
    // Float Checks
    // ==========================================
    /// <summary>
    /// Check whether this series is NaN
    /// </summary>
    /// <returns></returns>
    public Series IsNan() => new(PolarsWrapper.SeriesIsNan(Handle));
    /// <summary>
    /// Check whether this series is not NaN
    /// </summary>
    /// <returns></returns>
    public Series IsNotNan() => new(PolarsWrapper.SeriesIsNotNan(Handle));
    /// <summary>
    /// Check whether this series is finite
    /// </summary>
    /// <returns></returns>
    public Series IsFinite() => new(PolarsWrapper.SeriesIsFinite(Handle));
    /// <summary>
    /// Check whether this series is infinite
    /// </summary>
    /// <returns></returns>
    public Series IsInfinite() => new(PolarsWrapper.SeriesIsInfinite(Handle));

    // ==========================================
    // Unique Ops and Boolean Mask
    // ==========================================
    /// <summary>
    /// Count the number of unique values in this Series.
    /// </summary>
    public ulong NUnique => PolarsWrapper.SeriesNUnique(Handle);

    /// <summary>
    /// Get the unique elements of this Series.
    /// </summary>
    public Series Unique() => new(PolarsWrapper.SeriesUnique(Handle));

    /// <summary>
    /// Get the unique elements of this Series, maintaining the order of appearance.
    /// </summary>
    public Series UniqueStable() => new(PolarsWrapper.SeriesUniqueStable(Handle));

    /// <summary>
    /// Get a boolean mask indicating which values are unique.
    /// <para>Implemented via DataFrame expression composition.</para>
    /// </summary>
    public Series IsUnique() => ApplyExpr(Polars.Col(Name).IsUnique());

    /// <summary>
    /// Get a boolean mask indicating which values are duplicated.
    /// <para>Implemented via DataFrame expression composition.</para>
    /// </summary>
    public Series IsDuplicated() => ApplyExpr(Polars.Col(Name).IsDuplicated());
    /// <summary>
    /// Check if values are between lower and upper bounds.
    /// </summary>
    public Series IsBetween(object lower, object upper) 
        => ApplyExpr(Polars.Col(Name).IsBetween(Expr.MakeLit(lower), Expr.MakeLit(upper)));
    /// <summary>
    /// Check if values are between lower and upper bounds.
    /// </summary>
    public Series IsBetween(Expr lower, Expr upper) 
        => ApplyExpr(Polars.Col(Name).IsBetween(lower, upper));
    /// <summary>
    /// Check if the value is in given collection.
    /// </summary>
    public Series IsIn(Expr other, bool nullsEqual = false)
        => ApplyExpr(Polars.Col(Name).IsIn(other,nullsEqual));
    /// <summary>
    /// Filter a series.
    /// <br/>
    /// Mostly useful in <c>group_by</c> context or when you want to filter an expression based on another expression within a <c>Select</c> context.
    /// </summary>
    /// <param name="predicate">Boolean expression used to filter the current expression.</param>
    /// <returns>A new series with filtered values.</returns>
    public Series Filter(Expr predicate) 
        => ApplyExpr(Polars.Col(Name).Filter(predicate));
    // ==========================================
    // Common Ops 
    // ==========================================
    /// <summary>
    /// Sort this Series.
    /// </summary>
    /// <param name="descending">Sort in descending order.</param>
    /// <param name="nullsLast">Place null values last (default behavior depends on ascending/descending).</param>
    /// <param name="multithreaded">Use parallel sorting (default: true).</param>
    /// <param name="maintainOrder">Use stable sort (maintain order of equal elements) (default: false).</param>
    public Series Sort(
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false, 
        bool multithreaded = true)
    {
        var h = PolarsWrapper.SeriesSort(
            Handle, 
            descending, 
            nullsLast, 
            multithreaded, 
            maintainOrder
        );
        return new Series(h);
    }
    /// <summary>
    /// Explode a list column into multiple rows.
    /// The resulting Series will be longer than the original.
    /// </summary>
    public Series Explode(bool emptyAsNull=true,bool keepNulls=true) => ApplyExpr(Polars.Col(Name).Explode(emptyAsNull,keepNulls));
    /// <summary>
    /// Aggregate values into a list.
    /// Result is a Series with 1 row containing a List of all values.
    /// </summary>
    public Series Implode() => ApplyExpr(Polars.Col(Name).Implode());
    /// <summary>
    /// Unnest a Struct column into a DataFrame.
    /// Shortcut for <see cref="SeriesStructOps.Unnest"/>.
    /// </summary>
    public DataFrame Unnest() => Struct.Unnest();
    /// <summary>
    /// Count the occurrences of unique values.
    /// <para>
    /// Similar to SQL <c>GROUP BY val COUNT(*)</c>.
    /// </para>
    /// </summary>
    /// <param name="sort">Sort the output by count in descending order. Default is true.</param>
    /// <param name="parallel">Execute in parallel. Default is true.</param>
    /// <param name="name">The name of the count column. Default is "count".</param>
    /// <param name="normalize">If true, the count column will contain probabilities (fractions) instead of absolute counts. Default is false.</param>
    /// <returns>A DataFrame with the series values and their counts.</returns>
    /// <example>
    /// <code>
    /// var s = Series.From("fruit", new[] { "apple", "apple", "banana" });
    /// 
    /// // Default: sorted, absolute counts
    /// s.ValueCounts().Show();
    /// 
    /// // Normalized (percentage)
    /// s.ValueCounts(normalize: true, name: "prob").Show();
    /// // Result
    /// ┌────────┬───────┐
    /// │ fruit  ┆ count │
    /// │ ---    ┆ ---   │
    /// │ str    ┆ u32   │
    /// ╞════════╪═══════╡
    /// │ apple  ┆ 3     │
    /// │ orange ┆ 2     │
    /// │ banana ┆ 1     │
    /// └────────┴───────┘
    /// </code>
    /// </example>
    public DataFrame ValueCounts(bool sort = true, bool parallel = true, string name = "count", bool normalize = false)
    {
        var dfHandle = PolarsWrapper.SeriesValueCounts(Handle, sort, parallel, name, normalize);
        return new DataFrame(dfHandle);
    }
    // ==========================================
    // Conversions (Arrow / DataFrame)
    // ==========================================

    /// <summary>
    /// Zero-copy convert to Apache Arrow Array.
    /// </summary>
    public IArrowArray ToArrow()
        => PolarsWrapper.SeriesToArrow(Handle);
    /// <summary>
    /// Low-level entry point: Create Series from existing Arrow Array.
    /// </summary>
    public static Series FromArrow(string name, IArrowArray arrowArray)
    {
        var handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
        return new Series(handle);
    }
    // ==========================================
    // Window & Rolling
    // ==========================================
    /// <summary>
    /// Calculate the difference with the previous value (n-th lag).
    /// </summary>
    public Series Diff(long n = 1) => ApplyExpr(Polars.Col(Name).Diff(n));

    /// <summary>
    /// Shift values by the given number of indices.
    /// </summary>
    public Series Shift(long n = 1) => ApplyExpr(Polars.Col(Name).Shift(n));

    /// <summary>  
    /// <inheritdoc cref="Expr.RollingMin(string, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMin(string, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling minimum.</returns>
    public Series RollingMin(string windowSize, int minPeriods = 1, double[]? weights= null, bool center=false) 
        => ApplyExpr(Polars.Col(Name).RollingMin(windowSize, minPeriods,weights,center));
    /// <summary>
    /// <inheritdoc cref="Expr.RollingMin(TimeSpan, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMin(TimeSpan, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling minimum.</returns>
    public Series RollingMin(TimeSpan windowSize, int minPeriods = 1, double[]? weights= null, bool center=false) 
        => ApplyExpr(Polars.Col(Name).RollingMin(windowSize, minPeriods, weights, center));
    /// <summary>  
    /// <inheritdoc cref="Expr.RollingMax(string, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMax(string, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling maximum.</returns>
    public Series RollingMax(string windowSize, int minPeriods = 1, double[]? weights= null, bool center=false) 
        => ApplyExpr(Polars.Col(Name).RollingMax(windowSize, minPeriods,weights,center));
    /// <summary>
    /// <inheritdoc cref="Expr.RollingMax(TimeSpan, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMax(TimeSpan, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling maximum.</returns>
    public Series RollingMax(TimeSpan windowSize, int minPeriods = 1,double[]? weights= null, bool center=false) 
        => ApplyExpr(Polars.Col(Name).RollingMax(windowSize, minPeriods, weights,center));
    /// <summary>
    /// <inheritdoc cref="Expr.RollingMean(string, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMean(string, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling mean.</returns>
    public Series RollingMean(string windowSize, int minPeriods = 1,double[]? weights= null, bool center=false) 
        => ApplyExpr(Polars.Col(Name).RollingMean(windowSize, minPeriods, weights,center));
    /// <summary>
    /// <inheritdoc cref="Expr.RollingMean(TimeSpan, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMean(TimeSpan, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling maximum.</returns>
    public Series RollingMean(TimeSpan windowSize, int minPeriods = 1,double[]? weights= null, bool center=false) 
        => ApplyExpr(Polars.Col(Name).RollingMean(windowSize, minPeriods,weights,center));
    /// <summary>
    /// <inheritdoc cref="Expr.RollingSum(string, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingSum(string, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling sum.</returns>
    public Series RollingSum(string windowSize, int minPeriods = 1,double[]? weights= null, bool center=false) 
        => ApplyExpr(Polars.Col(Name).RollingSum(windowSize, minPeriods,weights,center));
    /// <summary>
    /// <inheritdoc cref="Expr.RollingMean(TimeSpan, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMean(TimeSpan, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling maximum.</returns>
    public Series RollingSum(TimeSpan windowSize, int minPeriods = 1,double[]? weights= null, bool center=false) 
        => ApplyExpr(Polars.Col(Name).RollingSum(windowSize, minPeriods,weights,center));
    /// <summary>
    /// <inheritdoc cref="Expr.RollingStd(string, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingStd(string, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling standard deviation.</returns>
    public Series RollingStd(string windowSize, int minPeriods = 1, double[]? weights = null, bool center = false)
        => ApplyExpr(Polars.Col(Name).RollingStd(windowSize, minPeriods, weights, center));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingStd(TimeSpan, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingStd(TimeSpan, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling standard deviation.</returns>
    public Series RollingStd(TimeSpan windowSize, int minPeriods = 1, double[]? weights = null, bool center = false)
        => ApplyExpr(Polars.Col(Name).RollingStd(windowSize, minPeriods, weights, center));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingVar(string, int, double[], bool, byte)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingVar(string, int, double[], bool, byte)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling variance.</returns>
    public Series RollingVar(string windowSize, int minPeriods = 1, double[]? weights = null, bool center = false, byte ddof = 1)
        => ApplyExpr(Polars.Col(Name).RollingVar(windowSize, minPeriods, weights, center, ddof));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingVar(TimeSpan, int, double[], bool, byte)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingVar(TimeSpan, int, double[], bool, byte)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling variance.</returns>
    public Series RollingVar(TimeSpan windowSize, int minPeriods = 1, double[]? weights = null, bool center = false, byte ddof = 1)
        => ApplyExpr(Polars.Col(Name).RollingVar(windowSize, minPeriods, weights, center, ddof));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingMedian(string, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMedian(string, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling median.</returns>
    public Series RollingMedian(string windowSize, int minPeriods = 1, double[]? weights = null, bool center = false)
        => ApplyExpr(Polars.Col(Name).RollingMedian(windowSize, minPeriods, weights, center));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingMedian(TimeSpan, int,double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMedian(TimeSpan, int,double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling median.</returns>
    public Series RollingMedian(TimeSpan windowSize, int minPeriods = 1)
        => ApplyExpr(Polars.Col(Name).RollingMedian(windowSize, minPeriods));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingSkew(string, int, double[], bool, bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingSkew(string, int, double[], bool, bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling skew.</returns>
    public Series RollingSkew(string windowSize, int minPeriods = 1, double[]? weights = null, bool center = false, bool bias = true)
        => ApplyExpr(Polars.Col(Name).RollingSkew(windowSize, minPeriods, weights, center, bias));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingSkew(TimeSpan, int, double[], bool, bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingSkew(TimeSpan, int, double[], bool, bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling skew.</returns>
    public Series RollingSkew(TimeSpan windowSize, int minPeriods = 1, double[]? weights = null, bool center = false, bool bias = true)
        => ApplyExpr(Polars.Col(Name).RollingSkew(windowSize, minPeriods, weights, center, bias));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingKurtosis(string, int, double[], bool, bool, bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingKurtosis(string, int, double[], bool, bool, bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling kurtosis.</returns>
    public Series RollingKurtosis(string windowSize, int minPeriods = 1, double[]? weights = null, bool center = false, bool fisher = true, bool bias = true)
        => ApplyExpr(Polars.Col(Name).RollingKurtosis(windowSize, minPeriods, weights, center, fisher, bias));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingKurtosis(TimeSpan, int, double[], bool, bool, bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingKurtosis(TimeSpan, int, double[], bool, bool, bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling kurtosis.</returns>
    public Series RollingKurtosis(TimeSpan windowSize, int minPeriods = 1, double[]? weights = null, bool center = false, bool fisher = true, bool bias = true)
        => ApplyExpr(Polars.Col(Name).RollingKurtosis(windowSize, minPeriods, weights, center, fisher, bias));

    // -------------------------------------------------------------------------
    // Rolling Rank & Quantile
    // -------------------------------------------------------------------------

    /// <summary>
    /// <inheritdoc cref="Expr.RollingRank(string, int, RankMethod, ulong?, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingRank(string, int, RankMethod, ulong?, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling rank.</returns>
    public Series RollingRank(string windowSize, int minPeriods = 1, RankMethod method = RankMethod.Average, ulong? seed = null, double[]? weights = null, bool center = false)
        => ApplyExpr(Polars.Col(Name).RollingRank(windowSize, minPeriods, method, seed, weights, center));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingRank(TimeSpan, int, RankMethod, ulong?, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingRank(TimeSpan, int, RankMethod, ulong?, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling rank.</returns>
    public Series RollingRank(TimeSpan windowSize, int minPeriods = 1, RankMethod method = RankMethod.Average, ulong? seed = null, double[]? weights = null, bool center = false)
        => ApplyExpr(Polars.Col(Name).RollingRank(windowSize, minPeriods, method, seed, weights, center));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingQuantile(double, QuantileMethod, string, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingQuantile(double, QuantileMethod, string, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling quantile.</returns>
    public Series RollingQuantile(double quantile, QuantileMethod method, string windowSize, int minPeriods = 1, double[]? weights = null, bool center = false)
        => ApplyExpr(Polars.Col(Name).RollingQuantile(quantile, method, windowSize, minPeriods, weights, center));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingQuantile(double, QuantileMethod, TimeSpan, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingQuantile(double, QuantileMethod, TimeSpan, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling quantile.</returns>
    public Series RollingQuantile(double quantile, QuantileMethod method, TimeSpan windowSize, int minPeriods = 1, double[]? weights = null, bool center = false)
        => ApplyExpr(Polars.Col(Name).RollingQuantile(quantile, method, windowSize, minPeriods, weights, center));
    // -------------------------------------------------------------------------
    // Rolling ... By (Dynamic Window based on another column, usually Time)
    // -------------------------------------------------------------------------
    /// <summary>
    /// <inheritdoc cref="Expr.RollingMeanBy(string, Expr, int, ClosedWindow)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMeanBy(string, Expr, int, ClosedWindow)" path="/remarks"/>
    /// <inheritdoc cref="Expr.RollingMeanBy(string, Expr, int, ClosedWindow)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling mean.</returns>
    public Series RollingMeanBy(string windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
        => ApplyExpr(Polars.Col(Name).RollingMeanBy(windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingMeanBy(TimeSpan, Expr, int, ClosedWindow)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMeanBy(TimeSpan, Expr, int, ClosedWindow)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling mean.</returns>
    public Series RollingMeanBy(TimeSpan windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
        => ApplyExpr(Polars.Col(Name).RollingMeanBy(windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingSumBy(string, Expr, int, ClosedWindow)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingSumBy(string, Expr, int, ClosedWindow)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling sum.</returns>
    public Series RollingSumBy(string windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
        => ApplyExpr(Polars.Col(Name).RollingSumBy(windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingSumBy(TimeSpan, Expr, int, ClosedWindow)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingSumBy(TimeSpan, Expr, int, ClosedWindow)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling sum.</returns>
    public Series RollingSumBy(TimeSpan windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
        => ApplyExpr(Polars.Col(Name).RollingSumBy(windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingMinBy(string, Expr, int, ClosedWindow)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMinBy(string, Expr, int, ClosedWindow)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling minimum.</returns>
    public Series RollingMinBy(string windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
        => ApplyExpr(Polars.Col(Name).RollingMinBy(windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingMinBy(TimeSpan, Expr, int, ClosedWindow)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMinBy(TimeSpan, Expr, int, ClosedWindow)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling minimum.</returns>
    public Series RollingMinBy(TimeSpan windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
        => ApplyExpr(Polars.Col(Name).RollingMinBy(windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingMaxBy(string, Expr, int, ClosedWindow)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMaxBy(string, Expr, int, ClosedWindow)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling maximum.</returns>
    public Series RollingMaxBy(string windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
        => ApplyExpr(Polars.Col(Name).RollingMaxBy(windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingMaxBy(TimeSpan, Expr, int, ClosedWindow)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMaxBy(TimeSpan, Expr, int, ClosedWindow)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling maximum.</returns>
    public Series RollingMaxBy(TimeSpan windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
        => ApplyExpr(Polars.Col(Name).RollingMaxBy(windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingStdBy(string, Expr, int, ClosedWindow)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingStdBy(string, Expr, int, ClosedWindow)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling standard deviation.</returns>
    public Series RollingStdBy(string windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
        => ApplyExpr(Polars.Col(Name).RollingStdBy(windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingStdBy(TimeSpan, Expr, int, ClosedWindow)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingStdBy(TimeSpan, Expr, int, ClosedWindow)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling standard deviation.</returns>
    public Series RollingStdBy(TimeSpan windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
        => ApplyExpr(Polars.Col(Name).RollingStdBy(windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingVarBy(string, Expr, int, ClosedWindow, byte)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingVarBy(string, Expr, int, ClosedWindow, byte)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling variance.</returns>
    public Series RollingVarBy(string windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left, byte ddof = 1)
        => ApplyExpr(Polars.Col(Name).RollingVarBy(windowSize, by, minPeriods, closed, ddof));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingVarBy(TimeSpan, Expr, int, ClosedWindow, byte)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingVarBy(TimeSpan, Expr, int, ClosedWindow, byte)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling variance.</returns>
    public Series RollingVarBy(TimeSpan windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left, byte ddof = 1)
        => ApplyExpr(Polars.Col(Name).RollingVarBy(windowSize, by, minPeriods, closed, ddof));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingMedianBy(string, Expr, int, ClosedWindow)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMedianBy(string, Expr, int, ClosedWindow)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling median.</returns>
    public Series RollingMedianBy(string windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
        => ApplyExpr(Polars.Col(Name).RollingMedianBy(windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingMedianBy(TimeSpan, Expr, int, ClosedWindow)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMedianBy(TimeSpan, Expr, int, ClosedWindow)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling median.</returns>
    public Series RollingMedianBy(TimeSpan windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
        => ApplyExpr(Polars.Col(Name).RollingMedianBy(windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingRankBy(string, Expr, RollingRankMethod, ulong?, int, ClosedWindow)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingRankBy(string, Expr, RollingRankMethod, ulong?, int, ClosedWindow)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling rank.</returns>
    public Series RollingRankBy(string windowSize, Expr by, RollingRankMethod method = RollingRankMethod.Average, ulong? seed = null, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
        => ApplyExpr(Polars.Col(Name).RollingRankBy(windowSize, by, method, seed, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingRankBy(TimeSpan, Expr, RollingRankMethod, ulong?, int, ClosedWindow)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingRankBy(TimeSpan, Expr, RollingRankMethod, ulong?, int, ClosedWindow)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling rank.</returns>
    public Series RollingRankBy(TimeSpan windowSize, Expr by, RollingRankMethod method = RollingRankMethod.Average, ulong? seed = null, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
        => ApplyExpr(Polars.Col(Name).RollingRankBy(windowSize, by, method, seed, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingQuantileBy(double, QuantileMethod, string, Expr, int, ClosedWindow)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingQuantileBy(double, QuantileMethod, string, Expr, int, ClosedWindow)" path="/param"/>
    /// <inheritdoc cref="Expr.RollingQuantileBy(double, QuantileMethod, string, Expr, int, ClosedWindow)" path="/remarks"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling quantile.</returns>
    public Series RollingQuantileBy(double quantile, QuantileMethod method, string windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
        => ApplyExpr(Polars.Col(Name).RollingQuantileBy(quantile, method, windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingQuantileBy(double, QuantileMethod, string, Expr, int, ClosedWindow)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingQuantileBy(double, QuantileMethod, string, Expr, int, ClosedWindow)" path="/param"/>
    /// <inheritdoc cref="Expr.RollingQuantileBy(double, QuantileMethod, string, Expr, int, ClosedWindow)" path="/remarks"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling quantile.</returns>
    public Series RollingQuantileBy(double quantile, QuantileMethod method, TimeSpan windowSize, Expr by, int minPeriods = 1, ClosedWindow closed = ClosedWindow.Left)
        => ApplyExpr(Polars.Col(Name).RollingQuantileBy(quantile, method, windowSize, by, minPeriods, closed));

    // ==========================================
    // UDF
    // ==========================================
    /// <summary>
    /// Apply a custom C# function to the series (element-wise).
    /// <para>Warning: This is slower than native expressions because it runs in the .NET runtime.</para>
    /// </summary>
    public Series Map<TInput, TOutput>(Func<TInput, TOutput> function, DataType outputType)
        => ApplyExpr(Polars.Col(Name).Map(function, outputType));
    /// <summary>
    /// Apply a raw Arrow-to-Arrow UDF.
    /// </summary>
    public Series Map(Func<IArrowArray, IArrowArray> function, DataType outputType)
        => ApplyExpr(Polars.Col(Name).Map(function, outputType));
    // ==========================================
    // Display (Show)
    // ==========================================
    /// <summary>
    /// Returns the string representation of the Series (ASCII table).
    /// This allows Console.WriteLine(s) to print the table directly.
    /// </summary>
    public override string ToString()
    {
        if (Handle.IsInvalid) return "Series (Disposed)";
        using var df = this.ToFrame();
        return df.ToString();
    }

    /// <summary>
    /// Print the DataFrame to Console.
    /// </summary>
    public void Show() => Console.WriteLine(ToString());
    // ==========================================
    // High-Level Factories
    // ==========================================
    /// <summary>
    /// Create a Series from a list of objects, primitives, or nested lists.
    /// Uses SeriesFactory to automatically select the fastest path (SIMD for Arrays, Reflection for Lists).
    /// </summary>
    public static Series From<T>(string name, IEnumerable<T> data) 
    {
        var handle = SeriesFactory.CreateGenericType(name, data);

        // var arrowArray = ArrowConverter.Build(data);
        // var handle = ArrowFfiBridge.ImportSeries(name, arrowArray);
        return new Series(handle);
    }
    /// <summary>
    /// Convert this single Series into a DataFrame.
    /// </summary>
    public DataFrame ToFrame()
        => new(PolarsWrapper.SeriesToFrame(Handle));
    IPolarsDataFrame IPolarsSeries.ToFrame()
    {
        return ToFrame();
    }
    /// <summary>
    /// Dispose the underlying SeriesHandle.
    /// </summary>
    public void Dispose() => Handle.Dispose();
}

/// <summary>
/// Date Ops Namespace
/// </summary>
public class SeriesDtOps
{
    private readonly Series _series;
    internal SeriesDtOps(Series series) { _series = series; }

    private Series Apply(Func<Expr, Expr> op) 
    {
        var expr = op(Polars.Col(_series.Name));
        
        return _series.ApplyExpr(expr);
    }
    /// <summary>Get the year from the underlying date/datetime.</summary>
    public Series Year() => Apply(e => e.Dt.Year());
    /// <summary>Get the quarter from the underlying date/datetime.</summary>
    public Series Quarter() => Apply(e => e.Dt.Quarter());
    /// <summary>Get the month from the underlying date/datetime.</summary>
    public Series Month() => Apply(e => e.Dt.Month());
    /// <summary>Get the day from the underlying date/datetime.</summary>
    public Series Day() => Apply(e => e.Dt.Day());
    /// <summary>Get the ordinal day (day of year) from the underlying date/datetime.</summary>
    public Series OrdinalDay() => Apply(e => e.Dt.OrdinalDay());
    /// <summary>Get the weekday from the underlying date/datetime.</summary>
    public Series WeekDay() => Apply(e => e.Dt.Weekday());
    /// <summary>Get the hour from the underlying datetime.</summary>
    public Series Hour() => Apply(e => e.Dt.Hour());
    /// <summary>Get the minute from the underlying datetime.</summary>
    public Series Minute() => Apply(e => e.Dt.Minute());
    /// <summary>Get the second from the underlying datetime.</summary>
    public Series Second() => Apply(e => e.Dt.Second());
    /// <summary>Get the millisecond from the underlying datetime.</summary>
    public Series Millisecond() => Apply(e => e.Dt.Millisecond());
    /// <summary>Get the microsecond from the underlying datetime.</summary>
    public Series Microsecond() => Apply(e => e.Dt.Microsecond());
    /// <summary>Get the nanosecond from the underlying datetime.</summary>
    public Series Nanosecond() => Apply(e => e.Dt.Nanosecond());

    /// <summary>
    /// Cast to Date (remove time component).
    /// </summary>
    /// <returns></returns>
    public Series Date() => Apply(e => e.Dt.Date());
    /// <summary>
    /// Cast to Time (remove Date component).
    /// </summary>
    /// <returns></returns>
    public Series Time() => Apply(e => e.Dt.Time());
    // ==========================================
    // Truncate & Round
    // ==========================================

    /// <summary>
    /// Truncate the datetimes to the given interval (e.g. "1d", "1h", "15m").
    /// </summary>
    public Series Truncate(string every) => Apply(e => e.Dt.Truncate(every));
    /// <summary>
    /// Truncate the datetimes to the given timespan
    /// </summary>
    /// <param name="every"></param>
    /// <returns></returns>
    public Series Truncate(TimeSpan every) => Apply(e => e.Dt.Truncate(every));
    /// <summary>
    /// Round the datetimes to the given interval.
    /// </summary>
    public Series Round(string every) => Apply(e => e.Dt.Round(every));
    /// <summary>
    /// Round the datetimes to the given timespan interval.
    /// </summary>
    /// <param name="every"></param>
    /// <returns></returns>
    public Series Round(TimeSpan every) => Apply(e => e.Dt.Round(every));
    // ==========================================
    // Offset
    // ==========================================

    /// <summary>
    /// Offset the datetimes by a given duration expression.
    /// </summary>
    public Series OffsetBy(Expr by) => Apply(e => e.Dt.OffsetBy(by));
    /// <summary>
    /// Offset the datetimes by a constant duration string (e.g., "1d", "-2h").
    /// </summary>
    public Series OffsetBy(string duration) => Apply(e => e.Dt.OffsetBy(duration));
    /// <summary>
    /// Offset the datetimes by TimeSpan
    /// </summary>
    /// <param name="duration"></param>
    /// <returns></returns>
    public Series OffsetBy(TimeSpan duration) => Apply(e => e.Dt.OffsetBy(duration));

    // ==========================================
    // Timestamp
    // ==========================================

    /// <summary>
    /// Convert the datetime to an integer timestamp (Unix epoch).
    /// </summary>
    /// <param name="timeUnit">
    /// The desired TimeUnit for the resulting Datetime.
    /// <para><b>Note:</b> Only sub-second units (<see cref="TimeUnit.Nanoseconds"/>, <see cref="TimeUnit.Microseconds"/>, <see cref="TimeUnit.Milliseconds"/>) are supported.</para>
    /// </param>
    public Series Timestamp(TimeUnit timeUnit = TimeUnit.Microseconds) => Apply(e => e.Dt.Timestamp(timeUnit));
    /// <summary>
    /// Combine the date from the underlying date/datetime with the time from another expression.
    /// <para>The resulting Series will have the specified TimeUnit.</para>
    /// </summary>
    /// <param name="time">An expression yielding the Time component.</param>
    /// <param name="timeUnit">
    /// The desired TimeUnit for the resulting Datetime.
    /// <para><b>Note:</b> Only sub-second units (<see cref="TimeUnit.Nanoseconds"/>, <see cref="TimeUnit.Microseconds"/>, <see cref="TimeUnit.Milliseconds"/>) are supported.</para>
    /// </param>
    public Series Combine(Expr time,TimeUnit timeUnit) => Apply(e => e.Dt.Combine(time,timeUnit));

    // ==========================================
    // TimeZone
    // ==========================================
    /// <summary>
    /// Convert from one timezone to another.
    /// Resulting Series will have the given time zone.
    /// </summary>
    /// <param name="tz">Target time zone (e.g. "Asia/Shanghai")</param>    
    public Series ConvertTimeZone(string tz) => Apply(e => e.Dt.ConvertTimeZone(tz));

    /// <summary>
    /// Replace the time zone of a Series.
    /// This does not change the underlying timestamp, only the metadata.
    /// </summary>
    public Series ReplaceTimeZone(string? timeZone, string? ambiguous = null, string? nonExistent = "raise")
         =>Apply(e => e.Dt.ReplaceTimeZone(timeZone,ambiguous,nonExistent));
    // ==========================================
    // BusinessDays
    // ==========================================
    /// <summary>
    /// Add business days to the date column.
    /// </summary>
    /// <param name="n">Number of business days to add (can be negative).</param>
    /// <param name="holidays">List of holidays (dates to skip).</param>
    /// <param name="weekMask">
    /// Array of 7 bools indicating business days, starting from Monday. 
    /// Default is Mon-Fri.
    /// </param>
    /// <param name="roll">Strategy for handling non-business days.</param>
    public Series AddBusinessDays(
        int n, 
        IEnumerable<DateOnly>? holidays = null, 
        bool[]? weekMask = null, 
        Roll roll = Roll.Raise)
        =>Apply(e => e.Dt.AddBusinessDays(n,holidays,weekMask,roll));
    /// <summary>
    /// Add business days to the date column.
    /// </summary>
    public Series AddBusinessDays(
        Expr n, 
        IEnumerable<DateOnly>? holidays = null, 
        bool[]? weekMask = null, 
        Roll roll = Roll.Raise)
        =>Apply(e => e.Dt.AddBusinessDays(n,holidays,weekMask,roll));
    /// <summary>
    /// Check if the date is a business day.
    /// </summary>
    public Series IsBusinessDay(IEnumerable<DateOnly>? holidays = null, bool[]? weekMask = null)
        =>Apply(e => e.Dt.IsBusinessDay(holidays,weekMask));
}

/// <summary>
/// Series String Ops
/// </summary>
public class SeriesStrOps
{
    private readonly Series _series;
    internal SeriesStrOps(Series series) { _series = series; }

    private Series Apply(Func<Expr, Expr> op) 
        => _series.ApplyExpr(op(Polars.Col(_series.Name)));

    /// <summary>
    /// Transfer String to UpperClass.
    /// </summary>
    /// <returns></returns>
    public Series ToUpper() => Apply(e => e.Str.ToUpper());
    /// <summary>
    /// Transfer String to LowerClass.
    /// </summary>
    /// <returns></returns>
    public Series ToLower() => Apply(e => e.Str.ToLower());
    /// <summary>
    /// Get length in bytes.
    /// </summary>
    public Series Len() => Apply(e => e.Str.Len());
    /// <summary>
    /// Check if the string contains a substring that matches a pattern.
    /// </summary>
    /// <param name="pattern"></param>
    /// <returns></returns>
    public Series Contains(string pattern) => Apply(e => e.Str.Contains(pattern));
    /// <summary>
    /// Slice string by length.
    /// </summary>
    /// <param name="offset"></param>
    /// <param name="length"></param>
    /// <returns></returns>
    public Series Slice(long offset, ulong length) => Apply(e => e.Str.Slice(offset, length));
    /// <summary>
    /// Split the string by a substring.
    /// </summary>
    /// <param name="separator"></param>
    /// <returns></returns>
    public Series Split(string separator) => Apply(e => e.Str.Split(separator));
    /// <summary>
    /// Replace charaters in a string.
    /// </summary>
    /// <param name="pattern"></param>
    /// <param name="value"></param>
    /// <param name="useRegex"></param>
    /// <returns></returns>
    public Series ReplaceAll(string pattern, string value, bool useRegex = false)
        => Apply(e => e.Str.ReplaceAll(pattern, value,useRegex));
    /// <summary>
    /// Extract charaters in string by Regex.
    /// </summary>
    /// <param name="pattern"></param>
    /// <param name="groupIndex"></param>
    /// <returns></returns>
    public Series Extract(string pattern, uint groupIndex)
        => Apply(e => e.Str.Extract(pattern, groupIndex));
    // ==========================================
    // Strip / Clean
    // ==========================================

    /// <summary>
    /// Remove leading and trailing characters.
    /// If matches is null, whitespace is removed.
    /// </summary>
    /// <param name="matches">The set of characters to be removed.</param>
    public Series StripChars(string? matches = null)
        => Apply(e => e.Str.StripChars(matches));

    /// <summary>
    /// Remove leading characters.
    /// If matches is null, whitespace is removed.
    /// </summary>
    public Series StripCharsStart(string? matches = null)
        => Apply(e => e.Str.StripCharsStart(matches));

    /// <summary>
    /// Remove trailing characters.
    /// If matches is null, whitespace is removed.
    /// </summary>
    public Series StripCharsEnd(string? matches = null)
        => Apply(e => e.Str.StripCharsEnd(matches));
    /// <summary>
    /// Remove a specific prefix string.
    /// </summary>
    public Series StripPrefix(string prefix)
        => Apply(e => e.Str.StripPrefix(prefix));

    /// <summary>
    /// Remove a specific suffix string.
    /// </summary>
    public Series StripSuffix(string suffix)
        => Apply(e => e.Str.StripPrefix(suffix));

    // ==========================================
    // Boolean Checks
    // ==========================================

    /// <summary>
    /// Check if the string starts with the given prefix.
    /// </summary>
    public Series StartsWith(string prefix)
        => Apply(e => e.Str.StartsWith(prefix));

    /// <summary>
    /// Check if the string ends with the given suffix.
    /// </summary>
    public Series EndsWith(string suffix)
        => Apply(e => e.Str.StripSuffix(suffix ));

    // ==========================================
    // Temporal Parsing
    // ==========================================

    /// <summary>
    /// Convert string to Date using the specified format.
    /// </summary>
    public Series ToDate(string format)
        => Apply(e => e.Str.ToDate(format));

    /// <summary>
    /// Convert string to Datetime using the specified format.
    /// </summary>
    public Series ToDatetime(string format)
        => Apply(e => e.Str.ToDatetime(format));
}

/// <summary>
/// Series List Ops Namespace
/// </summary>
public class SeriesListOps
{
    private readonly Series _series;
    internal SeriesListOps(Series series) { _series = series; }

    private Series Apply(Func<Expr, Expr> op) 
        => _series.ApplyExpr(op(Polars.Col(_series.Name)));

    /// <summary>
    /// Get the length of the arrays.
    /// </summary>
    public Series Len() => Apply(e => e.List.Len());

    /// <summary>
    /// Get the first element.
    /// </summary>
    public Series First() => Apply(e => e.List.First());

    /// <summary>
    /// Get the element at the given index.
    /// </summary>
    public Series Get(int index) => Apply(e => e.List.Get(index));

    /// <summary>
    /// Join elements with a separator.
    /// </summary>
    public Series Join(string separator) => Apply(e => e.List.Join(separator));

    /// <summary>
    /// Calculate the sum of the list elements (element-wise).
    /// </summary>
    public Series Sum() => Apply(e => e.List.Sum());

    /// <summary>
    /// Calculate the min of the list elements.
    /// </summary>
    public Series Min() => Apply(e => e.List.Min());

    /// <summary>
    /// Calculate the max of the list elements.
    /// </summary>
    public Series Max() => Apply(e => e.List.Max());

    /// <summary>
    /// Calculate the mean of the list elements.
    /// </summary>
    public Series Mean() => Apply(e => e.List.Mean());

    /// <summary>
    /// Sort the arrays in the list.
    /// </summary>
    public Series Sort(bool descending = false,bool nullsLast=false,bool maintainOrder= false) 
        => Apply(e => e.List.Sort(descending,nullsLast,maintainOrder));

    /// <summary>
    /// Check if the list contains the given item.
    /// </summary>
    public Series Contains(int item, bool nullsEqual = false) => Apply(e => e.List.Contains(item, nullsEqual));
    /// <summary>
    /// Check if the list contains the given item.
    /// </summary>
    public Series Contains(string item, bool nullsEqual= false) => Apply(e => e.List.Contains(item, nullsEqual));
    /// <summary>
    /// Concat this list series with another list series.
    /// Result is a new Series with the lists concatenated.
    /// </summary>
    public Series Concat(Series other)
        => _series.ApplyBinaryExpr(other, (left, right) => left.List.Concat(right));
    /// <summary>Reverse elements in list.</summary>
    public Series Reverse() => Apply(e => e.List.Reverse());
}
/// <summary>
/// Wrapper for Array (Fixed-Size List) operations on a Series.
/// </summary>
public class SeriesArrayOps
{
    private readonly Series _series;
    internal SeriesArrayOps(Series series) { _series = series; }

    private Series Apply(Func<Expr, Expr> op) 
        => _series.ApplyExpr(op(Polars.Col(_series.Name)));

    // --- Aggregations ---
    
    /// <summary>Compute the max value of every sub-array.</summary>
    public Series Max() => Apply(e => e.Array.Max());

    /// <summary>Compute the min value of every sub-array.</summary>
    public Series Min() => Apply(e => e.Array.Min());

    /// <summary>Compute the sum of every sub-array.</summary>
    public Series Sum() => Apply(e => e.Array.Sum());

    /// <summary>Compute the mean of every sub-array.</summary>
    public Series Mean() => Apply(e => e.Array.Mean());

    /// <summary>Compute the median of every sub-array.</summary>
    public Series Median() => Apply(e => e.Array.Median());

    /// <summary>Compute the standard deviation of every sub-array.</summary>
    public Series Std(byte ddof = 1) => Apply(e => e.Array.Std(ddof));

    /// <summary>Compute the variance of every sub-array.</summary>
    public Series Var(byte ddof = 1) => Apply(e => e.Array.Var(ddof));

    // --- Boolean ---

    /// <summary>Check if any element in the sub-array is true.</summary>
    public Series Any() => Apply(e => e.Array.Any());

    /// <summary>Check if all elements in the sub-array are true.</summary>
    public Series All() => Apply(e => e.Array.All());

    // --- Sort & Search ---

    /// <summary>Sort elements in every sub-array.</summary>
    public Series Sort(bool descending = false, bool nullsLast = false, bool maintainOrder = false) 
        => Apply(e => e.Array.Sort(descending, nullsLast, maintainOrder));

    /// <summary>Reverse elements in every sub-array.</summary>
    public Series Reverse() => Apply(e => e.Array.Reverse());

    /// <summary>Get the index of the minimum value in every sub-array.</summary>
    public Series ArgMin() => Apply(e => e.Array.ArgMin());

    /// <summary>Get the index of the maximum value in every sub-array.</summary>
    public Series ArgMax() => Apply(e => e.Array.ArgMax());

    // --- Structure ---

    /// <summary>Get element at index from every sub-array.</summary>
    public Series Get(int index, bool nullOnOob = true) 
        => Apply(e => e.Array.Get(index, nullOnOob));

    /// <summary>Join elements with a separator.</summary>
    public Series Join(string separator, bool ignoreNulls = true) 
        => Apply(e => e.Array.Join(separator, ignoreNulls));

    /// <summary>
    /// Explode the array column into multiple rows.
    /// The resulting Series will be longer than the original.
    /// </summary>
    public Series Explode(bool emptyAsNull = true, bool keepNulls = true) => Apply(e => e.Array.Explode(emptyAsNull,keepNulls));

    /// <summary>
    /// Convert array to struct. Useful for splitting embeddings into feature columns.
    /// </summary>
    public Series ToStruct() => Apply(e => e.Array.ToStruct());

    /// <summary>
    /// Cast to variable-size List.
    /// </summary>
    public Series ToList() => Apply(e => e.Array.ToList());

    // --- Logic / Set ---

    /// <summary>Check if sub-array contains a specific item.</summary>
    public Series Contains(int item, bool nullsEqual = false) 
        => Apply(e => e.Array.Contains(item, nullsEqual));
    /// <summary>Check if sub-array contains a specific item.</summary>
    public Series Contains(double item, bool nullsEqual = false) 
        => Apply(e => e.Array.Contains(item, nullsEqual));
    /// <summary>Check if sub-array contains a specific item.</summary>   
    public Series Contains(Expr item, bool nullsEqual = false) 
        => Apply(e => e.Array.Contains(item, nullsEqual));

    /// <summary>Get unique elements in every sub-array.</summary>
    public Series Unique(bool stable = false) => Apply(e => e.Array.Unique(stable));
}
/// <summary>
/// Series Struct Ops Namespace
/// </summary>
public class SeriesStructOps
{
    private readonly Series _series;
    internal SeriesStructOps(Series series) { _series = series; }

    private Series Apply(Func<Expr, Expr> op) 
        => _series.ApplyExpr(op(Polars.Col(_series.Name)));

    /// <summary>
    /// Retrieve a field from the struct by name.
    /// Returns a new Series of that field's type.
    /// </summary>
    public Series Field(string name) => Apply(e => e.Struct.Field(name));

    /// <summary>
    /// Retrieve a field from the struct by index.
    /// </summary>
    public Series Field(int index) => Apply(e => e.Struct.Field(index));

    /// <summary>
    /// Rename the fields of the struct.
    /// </summary>
    public Series RenameFields(params string[] names) => Apply(e => e.Struct.RenameFields(names));

    /// <summary>
    /// Convert struct to JSON string.
    /// </summary>
    public Series JsonEncode() => Apply(e => e.Struct.JsonEncode());
    /// <summary>
    /// Unnest the struct column into a DataFrame.
    /// Each field of the struct becomes a separate column.
    /// </summary>
    public DataFrame Unnest()
    {
        var dfHandle = PolarsWrapper.SeriesStructUnnest(_series.Handle);
        return new DataFrame(dfHandle);
    }
}