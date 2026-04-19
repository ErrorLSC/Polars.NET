using Polars.NET.Core;
using Apache.Arrow;
using Polars.NET.Core.Arrow;
using Pl = Polars.CSharp.Polars;

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
    internal SeriesHandle Handle { get; private set; }
    private void ReplaceInnerHandle(SeriesHandle newHandle)
    {
        var oldHandle = Handle;
        Handle = newHandle;
        oldHandle?.Dispose(); 
    }
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
    public Series Rename(string newName)
    {
        Name = newName;
        return this;
    }
    /// <summary>
    /// Rename Series. Same as Rename.
    /// </summary>
    /// <param name="newName"></param>
    /// <returns></returns>
    public Series Alias(string newName) => Rename(newName);

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
    /// Shrink Series memory usage.This won't return a new Series
    /// </summary>
    public void ShrinkToFitInplace() => PolarsWrapper.SeriesShrinkToFit(Handle);
    /// <summary>
    /// Shrink Series memory usage.
    /// </summary>
    /// <returns>A new Series</returns>
    public Series ShrinkToFit() 
    {
        var newS = Clone();
        PolarsWrapper.SeriesShrinkToFit(newS.Handle);
        return newS;
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
    /// Access binary operations.
    /// </summary>
    public SeriesBinaryOps Bin => new(this);
    /// <summary>
    /// Access categorical operations.
    /// </summary>
    public SeriesCategoricalOps Cat => new(this);

    /// <summary>
    /// Clone the Series
    /// </summary>
    /// <returns></returns>
    public Series Clone() => new(PolarsWrapper.CloneSeries(Handle));

    internal Series ApplyExpr(IntoExpr expr)
    {
        using var df = new DataFrame(this);

        using var dfRes = df.Select(expr);

        return dfRes[0];
    }

    internal Series ApplyBinaryExpr(IntoExpr other, Func<Expr, Expr, Expr> op)
    {
        using Expr rightExpr = other.Consume();
        
        using Expr combinedExpr = op(Pl.Col(Name), rightExpr);
        
        return ApplyExpr(combinedExpr);
    }
    // ==========================================
    // Metadata
    // ==========================================

    /// <summary>
    /// Gets the current metadata sortedness flags of this Series. (O(1) operation)
    /// </summary>
    public SortStateFlags SortedFlags => (SortStateFlags)PolarsWrapper.SeriesGetSortedFlags(Handle);
    /// <summary>
    /// Checks if the Series is sorted according to the given rules.
    /// If the metadata flag matches, returns O(1). Otherwise, scans the data O(N).
    /// </summary>
    public bool IsSorted(bool descending = false, bool nullsLast = false) => PolarsWrapper.SeriesIsSorted(Handle, descending, nullsLast);
    /// <summary>
    /// Flags the Series as ‘sorted’. Enables downstream code to user fast paths for sorted arrays.
    /// </summary>
    /// <param name="descending">If the Series order is descending.</param>
    /// <returns></returns>
    public Series SetSorted(bool descending = false) => new(PolarsWrapper.SeriesSetSortedFlag(Handle,descending));
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
    /// Shape of this Series. 
    /// In Polars, a Series is always 1D, so this returns an array of length 1.
    /// </summary>
    public long[] Shape => [Length];

    // ==========================================
    // Indexing & Searching (Forwarded to Expr)
    // ==========================================

    /// <inheritdoc cref="Expr.Get(Expr, bool)"/>
    public Series Get(Expr index, bool nullOnOutOfBounds = false)
        => ApplyExpr(Pl.Col(Name).Get(index, nullOnOutOfBounds));

    /// <inheritdoc cref="Expr.Get(ulong, bool)"/>
    public Series Get(ulong index, bool nullOnOutOfBounds = false)
        => ApplyExpr(Pl.Col(Name).Get(index, nullOnOutOfBounds));

    /// <inheritdoc cref="Expr.Gather(IntoExpr)"/>
    public Series Gather(IntoExpr indices) => ApplyExpr(Pl.Col(Name).Gather(indices));
    /// <inheritdoc cref="Take(Series)"/>
    public Series Gather(ReadOnlySpan<int> indices) => ApplyExpr(Pl.Col(Name).Gather(indices));
    /// <inheritdoc cref="Expr.Take(IntoExpr)"/>
    public Series Take(IntoExpr indices) => Gather(indices);
    /// <inheritdoc cref="Expr.Take(IntoExpr)"/>
    public Series Take(ReadOnlySpan<int> indices) => Gather(indices);
    /// <summary>
    /// Take elements by physical integer indices.
    /// Note: Negative indices are not supported. All values must be >= 0.
    /// </summary>
    public Series Take(Series indices)
    {
        if (!indices.DataType.IsInteger)
        {
            throw new ArgumentException($"Take requires an integer Series, but got {indices.DataType.Kind}.", nameof(indices));
        }

        try
        {
            return new Series(PolarsWrapper.SeriesTake(Handle, indices.Handle));
        }
        catch (Exception ex) when (ex.Message.Contains("OutOfBounds") || ex.Message.Contains("out of bounds"))
        {
            throw new ArgumentOutOfRangeException(
                nameof(indices), 
                "Index out of bounds. Please ensure no negative indices are used and all values are within the Series length.");
        }
    }

    /// <inheritdoc cref="Expr.GatherEvery(ulong, ulong)"/>
    public Series GatherEvery(ulong n, ulong offset = 0)
        => ApplyExpr(Pl.Col(Name).GatherEvery(n, offset));

    /// <inheritdoc cref="Expr.ArgUnique()"/>
    public Series ArgUnique()
        => ApplyExpr(Pl.Col(Name).ArgUnique());


    /// <inheritdoc cref="Expr.ArgSort(bool, bool)"/>
    public Series ArgSort(bool descending = false, bool nullsLast = false)
        => ApplyExpr(Pl.Col(Name).ArgSort(descending, nullsLast));

    /// <inheritdoc cref="Expr.IndexOf(Expr)"/>
    public Series IndexOf(Expr element)
        => ApplyExpr(Pl.Col(Name).IndexOf(element));

    /// <inheritdoc cref="Expr.SearchSorted(Expr, SearchSortedSide, bool)"/>
    public Series SearchSorted(Expr element, SearchSortedSide side = SearchSortedSide.Any, bool descending = false)
        => ApplyExpr(Pl.Col(Name).SearchSorted(element, side, descending));
    
    // -------------------------------------------------------------------------
    // Boolean Aggregation
    // -------------------------------------------------------------------------

    /// <summary>
    /// <inheritdoc cref="Expr.Any(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.Any(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> (boolean, length 1).</returns>
    public Series Any(bool ignoreNulls = false) => ApplyExpr(Pl.Col(Name).Any(ignoreNulls));

    /// <summary>
    /// <inheritdoc cref="Expr.All(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.All(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> (boolean, length 1).</returns>
    public Series All(bool ignoreNulls = false) => ApplyExpr(Pl.Col(Name).All(ignoreNulls));

    /// <summary>
    /// <inheritdoc cref="Expr.Any(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.Any(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> (boolean, length 1).</returns>
    public bool? AnyAsScalar(bool ignoreNulls = false) => (bool?)ApplyExpr(Pl.Col(Name).Any(ignoreNulls))[0];

    /// <summary>
    /// <inheritdoc cref="Expr.All(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.All(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> (boolean, length 1).</returns>
    public bool? AllAsScalar(bool ignoreNulls = false) => (bool?)ApplyExpr(Pl.Col(Name).All(ignoreNulls))[0];

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
    /// Cast Series to another DataType.
    /// </summary>
    /// <param name="dtype">The target type (can be Polars DataType or .NET Type)</param>
    /// <param name="strict">Throws an error if conversion had overflows.</param>
    /// <param name="wrapNumerical">Allows wrapping numerical overflow.</param>
    public Series Cast(DataType dtype, bool strict = true, bool wrapNumerical = false)
    {
        if (strict && wrapNumerical)
        {
            throw new ArgumentException("Cannot set both 'strict' and 'wrapNumerical' to true.");
        }

        var h = PolarsWrapper.SeriesCast(Handle, dtype.Handle, strict, wrapNumerical);
        return new Series(h);
    }
    /// <summary>
    /// Cast the series to a specific .NET type.
    /// </summary>
    /// <typeparam name="T">Target .NET type (e.g., int, double, string).</typeparam>
    /// <param name="strict">If true, throw an error if the cast fails; if false, invalid values become null.</param>
    /// <param name="wrapNumerical">If true, wrap numerical values instead of saturating.</param>
    /// <returns>A new Series of the target type.</returns>
    public Series Cast<T>(bool strict = true, bool wrapNumerical = false)
        => Cast(DataType.FromNetType<T>(), strict, wrapNumerical);
    
    /// <summary>
    /// Get a slice of this Series.
    /// </summary>
    /// <param name="offset">Start index. Negative values count from the end.</param>
    /// <param name="length">Length of the slice.</param>
    public Series Slice(long offset, long length)
        => new(PolarsWrapper.SeriesSlice(Handle, offset, length));
    /// <inheritdoc cref="Slice(long,long)"/>
    public Series Slice(Range range)
    {
        long length = Length;
        
        long start = range.Start.IsFromEnd 
            ? length - range.Start.Value 
            : range.Start.Value;
            
        long end = range.End.IsFromEnd 
            ? length - range.End.Value 
            : range.End.Value;

        start = Math.Max(0, Math.Min(start, length));
        end = Math.Max(0, Math.Min(end, length));
        
        long sliceLength = end - start;
        
        if (sliceLength <= 0)
        {
            return Slice(0, 0); 
        }

        return Slice(start, sliceLength);
    }
    /// <summary>
    /// <inheritdoc cref="Expr.Reverse" path="/summary"/>
    /// </summary>
    /// <returns>A new <see cref="Series"/> with the order reversed.</returns>
    public Series Reverse() => ApplyExpr(Pl.Col(Name).Reverse());
    /// <summary>
    /// Append a Series to this one.
    /// The resulting series will consist of multiple chunks.
    /// </summary>
    /// <param name="other">Series to append.</param>
    public Series Append(Series other)
    {
        PolarsWrapper.SeriesAppend(Handle,other.Handle);    
        return this;
    }
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
    public Series ExtendConstant(Expr value,Expr n) => ApplyExpr(Pl.Col(Name).ExtendConstant(value,n));
    /// <inheritdoc cref="ExtendConstant(Expr,Expr)"/>
    public Series ExtendConstant(object value,int n) => ApplyExpr(Pl.Col(Name).ExtendConstant(value,n));
    /// <summary>
    /// Reshape this Series to a flat Series or an Array Series.
    /// </summary>
    /// <param name="dimensions">Tuple of the dimension sizes. If a -1 is used in any of the dimensions, that dimension is inferred.</param>
    /// <returns>Tuple of the dimension sizes. If a -1 is used in any of the dimensions, that dimension is inferred.</returns>
    public Series Reshape(ReadOnlySpan<long> dimensions) => new(PolarsWrapper.SeriesReshape(Handle, dimensions));
    
    // ==========================================
    // Null Checks & Boolean Masks
    // ==========================================

    /// <summary>
    /// Check whether indexed value is null。
    /// </summary>
    public bool IsNullAt(long index) => PolarsWrapper.SeriesIsNullAt(Handle, index);

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
        => ApplyExpr(Pl.Col(Name).DropNans());
    // ==========================================
    // Fill Ops
    // ==========================================
    /// <summary>
    /// Fill null values with a specified value.
    /// </summary>
    public Series FillNull(object value) => ApplyExpr(Pl.Col(Name).FillNull(value));
    /// <summary>
    /// Fill null values with a specific strategy (Forward).
    /// </summary>
    public Series ForwardFill(uint? limit = null) => ApplyExpr(Pl.Col(Name).ForwardFill(limit));
    /// <summary>
    /// Fill null values with a specific strategy (Backward).
    /// </summary>
    public Series BackwardFill(uint? limit = null) => ApplyExpr(Pl.Col(Name).BackwardFill(limit));
    /// <summary>
    /// Interpolate intermediate values.
    /// </summary>
    /// <inheritdoc cref="Expr.Interpolate(InterpolationMethod)"/>
    public Series Interpolate(InterpolationMethod method = InterpolationMethod.Linear)
        => ApplyExpr(Pl.Col(Name).Interpolate(method));
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
    public Series FillNan(object value) => ApplyExpr(Pl.Col(Name).FillNan(value));
    // ==========================================
    // Top-K & Bottom-K
    // ==========================================
    /// <summary>
    /// Get the top k values.
    /// </summary>
    public Series TopK(int k) => ApplyExpr(Pl.Col(Name).TopK(k));

    /// <summary>
    /// Get the bottom k values.
    /// </summary>
    public Series BottomK(int k) => ApplyExpr(Pl.Col(Name).BottomK(k));
    /// <summary>
    /// <inheritdoc cref="Expr.TopKBy(int, Expr[], bool[])" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.TopKBy(int, Expr[], bool[])" path="/param"/>
    /// <returns>A new <see cref="Series"/> containing the top k elements.</returns>
    public Series TopKBy(int k, Expr[] by, bool[] reverse)
        => ApplyExpr(Pl.Col(Name).TopKBy(k, by, reverse));
    /// <summary>
    /// <inheritdoc cref="Expr.BottomKBy(int, Expr[], bool[])" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.BottomKBy(int, Expr[], bool[])" path="/param"/>
    /// <returns>A new <see cref="Series"/> containing the bottom k elements.</returns>
    public Series BottomKBy(int k, Expr[] by, bool[] reverse)
        => ApplyExpr(Pl.Col(Name).BottomKBy(k, by, reverse));
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
        => ApplyExpr(Pl.Col(Name).TopKBy(k, Pl.Lit(by), reverse));
    /// <summary>
    /// Get the bottom k values sorted by another Series.
    /// </summary>
    public Series BottomKBy(int k, Series by, bool reverse = false)
        => ApplyExpr(Pl.Col(Name).BottomKBy(k, Pl.Lit(by), reverse));

    // ==========================================
    // Statistical Ops
    // ==========================================

    /// <summary>
    /// <inheritdoc cref="Expr.Skew(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.Skew(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> containing the skewness (length 1).</returns>
    public Series Skew(bool bias = true) => ApplyExpr(Pl.Col(Name).Skew(bias));

    /// <summary>
    /// <inheritdoc cref="Expr.Kurtosis(bool, bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.Kurtosis(bool, bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> containing the kurtosis (length 1).</returns>
    public Series Kurtosis(bool fisher = true, bool bias = true) 
        => ApplyExpr(Pl.Col(Name).Kurtosis(fisher, bias));

    /// <summary>
    /// <inheritdoc cref="Expr.Quantile(double, QuantileMethod)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.Quantile(double, QuantileMethod)" path="/param"/>
    /// <returns>A new <see cref="Series"/> containing the quantile value (length 1).</returns>
    public Series Quantile(double quantile, QuantileMethod method = QuantileMethod.Linear)
        => ApplyExpr(Pl.Col(Name).Quantile(quantile, method));

    /// <summary>
    /// <inheritdoc cref="Expr.PctChange(int)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.PctChange(int)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the percentage change.</returns>
    public Series PctChange(int n = 1) => ApplyExpr(Pl.Col(Name).PctChange(n));
    /// <summary>
    /// <inheritdoc cref="Expr.Rank(RankMethod, bool, ulong?)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.Rank(RankMethod, bool, ulong?)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the ranks.</returns>
    public Series Rank(RankMethod method = RankMethod.Average, bool descending = false, ulong? seed = null)
        => ApplyExpr(Pl.Col(Name).Rank(method, descending, seed));
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
    /// Get the unique elements of this Series.
    /// </summary>
    public Series Unique() => new(PolarsWrapper.SeriesUnique(Handle));

    /// <summary>
    /// Get the unique elements of this Series, maintaining the order of appearance.
    /// </summary>
    public Series UniqueStable() => new(PolarsWrapper.SeriesUniqueStable(Handle));
    /// <summary>
    /// Check if values are between lower and upper bounds.
    /// </summary>
    public Series IsBetween(object lower, object upper) 
        => ApplyExpr(Pl.Col(Name).IsBetween(Expr.MakeLit(lower), Expr.MakeLit(upper)));
    /// <summary>
    /// Check if values are between lower and upper bounds.
    /// </summary>
    public Series IsBetween(Expr lower, Expr upper) 
        => ApplyExpr(Pl.Col(Name).IsBetween(lower, upper));
    /// <summary>
    /// Filter a series.
    /// <br/>
    /// Mostly useful in <c>group_by</c> context or when you want to filter an expression based on another expression within a <c>Select</c> context.
    /// </summary>
    /// <param name="predicate">Boolean expression/Series used to filter the current expression.</param>
    /// <returns>A new series with filtered values.</returns>
    public Series Filter(Expr predicate) 
        => ApplyExpr(Pl.Col(Name).Filter(predicate));
    /// <inheritdoc cref="Filter(Expr)"/>
    public Series Filter(Series predicate) 
        => ApplyExpr(Pl.Col(Name).Filter(Pl.Lit(predicate)));
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
    public Series Explode(bool emptyAsNull=true,bool keepNulls=true) => ApplyExpr(Pl.Col(Name).Explode(emptyAsNull,keepNulls));
    /// <summary>
    /// Unnest a Struct column into a DataFrame.
    /// Shortcut for <see cref="SeriesStructOps.Unnest"/>.
    /// </summary>
    public DataFrame Unnest() => Struct.Unnest();
    /// <summary>
    /// Take values from self or other based on the given mask.
    /// Where mask evaluates true, take values from self. Where mask evaluates false, take values from other.
    /// </summary>
    /// <param name="mask">Boolean Series.</param>
    /// <param name="other">Series of same type.</param>
    /// <returns>Series</returns>
    public Series ZipWith(Series mask, Series other) => new(PolarsWrapper.SeriesZipWith(Handle,mask.Handle,other.Handle));
    /// <summary>
    /// Get dummy/indicator variables.
    /// </summary>
    /// <param name="separator">Separator/delimiter used when generating column names.</param>
    /// <param name="dropFirst">Remove the first category from the variable being encoded.</param>
    /// <param name="dropNulls">If there are None values in the series, a null column is not generated. Null values in the input are represented by zero vectors.</param>
    /// <returns></returns>
    public DataFrame ToDummies(string? separator = "_",bool dropFirst = false, bool dropNulls = false) => new(PolarsWrapper.SeriesToDummies(Handle,separator,dropFirst,dropNulls));
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
    public Series Diff(long n = 1) => ApplyExpr(Pl.Col(Name).Diff(n));

    /// <summary>
    /// Shift values by the given number of indices.
    /// </summary>
    public Series Shift(long n = 1) => ApplyExpr(Pl.Col(Name).Shift(n));

    // ==========================================
    // UDF
    // ==========================================
    /// <summary>
    /// Apply a custom C# function to the series (element-wise).
    /// <para>Warning: This is slower than native expressions because it runs in the .NET runtime.</para>
    /// </summary>
    public Series Map<TInput, TOutput>(Func<TInput, TOutput> function, DataType outputType)
        => ApplyExpr(Pl.Col(Name).Map(function, outputType));
    /// <summary>
    /// Apply a raw Arrow-to-Arrow UDF.
    /// </summary>
    public Series Map(Func<IArrowArray, IArrowArray> function, DataType outputType)
        => ApplyExpr(Pl.Col(Name).Map(function, outputType));
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
