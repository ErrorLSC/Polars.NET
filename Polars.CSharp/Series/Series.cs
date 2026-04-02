using Polars.NET.Core;
using Apache.Arrow;
using Polars.NET.Core.Arrow;

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
        Name = newName;
    }

    /// <summary>
    /// Reallocates the Series to ensure that all its underlying memory is physically contiguous.
    /// </summary>
    /// <remarks>
    /// Polars Operations like Appending or Filtering can create fragmented memory chunks. 
    /// Calling Rechunk() merges these chunks into a single contiguous Arrow array. 
    /// This is CRITICAL before zero-copy extracting native pointers for Tensors or FFI.
    /// </remarks>
    /// <returns>A new Series instance backed by contiguous memory.</returns>
    public Series Rechunk()
        => new(PolarsWrapper.SeriesRechunk(Handle));

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
    /// Gets the number of underlying Arrow memory chunks.
    /// </summary>
    public long NChunks => (long)PolarsWrapper.SeriesChunkCounts(Handle);

    /// <summary>
    /// Determines if the Series memory is physically contiguous (i.e., consists of a single chunk).
    /// </summary>
    public bool IsContiguous => NChunks == 1L;

    /// <summary>
    /// True if the Series is empty.
    /// </summary>
    public bool IsEmpty => Length == 0;

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
    // Indexing & Searching (Forwarded to Expr)
    // ==========================================

    /// <inheritdoc cref="Expr.Get(Expr, bool)"/>
    public Series Get(Expr index, bool nullOnOutOfBounds = false)
        => ApplyExpr(Polars.Col(Name).Get(index, nullOnOutOfBounds));

    /// <inheritdoc cref="Expr.Get(ulong, bool)"/>
    public Series Get(ulong index, bool nullOnOutOfBounds = false)
        => ApplyExpr(Polars.Col(Name).Get(index, nullOnOutOfBounds));

    /// <inheritdoc cref="Expr.Gather(Expr)"/>
    public Series Gather(Expr indices)
        => ApplyExpr(Polars.Col(Name).Gather(indices));

    /// <inheritdoc cref="Expr.Take(Expr)"/>
    public Series Take(Expr indices)
        => ApplyExpr(Polars.Col(Name).Take(indices));

    /// <inheritdoc cref="Expr.GatherEvery(ulong, ulong)"/>
    public Series GatherEvery(ulong n, ulong offset = 0)
        => ApplyExpr(Polars.Col(Name).GatherEvery(n, offset));

    /// <inheritdoc cref="Expr.ArgUnique()"/>
    public Series ArgUnique()
        => ApplyExpr(Polars.Col(Name).ArgUnique());

    /// <inheritdoc cref="Expr.ArgMax()"/>
    public Series ArgMax()
        => ApplyExpr(Polars.Col(Name).ArgMax());

    /// <inheritdoc cref="Expr.ArgMin()"/>
    public Series ArgMin()
        => ApplyExpr(Polars.Col(Name).ArgMin());

    /// <inheritdoc cref="Expr.ArgSort(bool, bool)"/>
    public Series ArgSort(bool descending = false, bool nullsLast = false)
        => ApplyExpr(Polars.Col(Name).ArgSort(descending, nullsLast));

    /// <inheritdoc cref="Expr.IndexOf(Expr)"/>
    public Series IndexOf(Expr element)
        => ApplyExpr(Polars.Col(Name).IndexOf(element));

    /// <inheritdoc cref="Expr.SearchSorted(Expr, SearchSortedSide, bool)"/>
    public Series SearchSorted(Expr element, SearchSortedSide side = SearchSortedSide.Any, bool descending = false)
        => ApplyExpr(Polars.Col(Name).SearchSorted(element, side, descending));
    
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
        => new(PolarsWrapper.SeriesSlice(Handle, offset, length));
    /// <summary>
    /// <inheritdoc cref="Expr.Reverse" path="/summary"/>
    /// </summary>
    /// <returns>A new <see cref="Series"/> with the order reversed.</returns>
    public Series Reverse() => ApplyExpr(Polars.Col(Name).Reverse());
    /// <summary>
    /// Append a Series to this one.
    /// The resulting series will consist of multiple chunks.
    /// </summary>
    /// <param name="other">Series to append.</param>
    public void Append(Series other) => PolarsWrapper.SeriesAppend(Handle,other.Handle);    
    /// <summary>
    /// Extend the memory backed by this Series with the values from another.
    /// Different from append, which adds the chunks from other to the chunks of this series, extend appends the data from other to the underlying memory locations and thus may cause a reallocation (which is expensive).
    /// If this does not cause a reallocation, the resulting data structure will not have any extra chunks and thus will yield faster queries.
    /// </summary>
    /// <param name="other">Series to extend the series with.</param>
    public Series Extend(Series other)
    {
        PolarsWrapper.SeriesExtend(Handle,other.Handle);
        return this;
    }
    /// <summary>
    /// Extremely fast method for extending the Series with ‘n’ copies of a value.
    /// </summary>
    /// <param name="value">A constant literal value or a unit expression with which to extend the expression result Series; can pass None to extend with nulls.</param>
    /// <param name="n">The number of additional values that will be added.</param>
    /// <returns></returns>
    public Series ExtendConstant(Expr value,Expr n) => ApplyExpr(Polars.Col(Name).ExtendConstant(value,n));
    /// <inheritdoc cref="ExtendConstant(Expr,Expr)"/>
    public Series ExtendConstant(object value,int n) => ApplyExpr(Polars.Col(Name).ExtendConstant(value,n));

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
    /// Get an approximation of the number of unique values in this Series.
    /// Uses HyperLogLog algorithm for fast, memory-efficient counting.
    /// </summary>
    /// <returns>Approximate count of unique values.</returns>
    public long ApproxNUnique() => PolarsWrapper.SeriesApproxNUnique(Handle);
    
    /// <summary>
    /// Count the number of unique values in this Series.
    /// </summary>
    public long NUnique() => PolarsWrapper.SeriesNUnique(Handle);

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
    // Conversions
    // ==========================================
    /// <summary>
    /// Convert Series to Array
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T[] ToArray<T>()
        => ArrowReader.ReadColumn<T>(ToArrow());

    /// <summary>
    /// Convert this single Series into a DataFrame.
    /// </summary>
    public DataFrame ToFrame()
        => new(PolarsWrapper.SeriesToFrame(Handle));
    IPolarsDataFrame IPolarsSeries.ToFrame()
        => ToFrame();
   
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
        return PolarsWrapper.SeriesToString(Handle);
    }

    /// <summary>
    /// Print the DataFrame to Console.
    /// </summary>
    public void Show() => Console.WriteLine(ToString());


    /// <summary>
    /// Dispose the underlying SeriesHandle.
    /// </summary>
    public void Dispose()
    {
        Handle?.Dispose();
        GC.SuppressFinalize(this); 
    }
}
