#pragma warning disable CS1591
#pragma warning disable CS1573
using Apache.Arrow;
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

/// <summary>
/// A Polars Expr
/// </summary>
public partial class Expr : IDisposable,IEquatable<Expr>
{
    internal ExprHandle Handle { get; }

    internal Expr(ExprHandle handle)
    {
        Handle = handle;
    }
    internal ExprHandle CloneHandle() => PolarsWrapper.CloneExpr(Handle);
    /// <summary>
    /// Clone Expr
    /// </summary>
    /// <returns></returns>
    public Expr Clone() => new(CloneHandle());

    // ==========================================
    // Columns
    // ==========================================
    public Expr Exclude(params string[] names) => ToSelector().Exclude(names).ToExpr();
    public Expr Exclude(params ReadOnlySpan<DataType> dtypes) => ToSelector().Exclude(dtypes).ToExpr();

    // ==========================================
    // Sort
    // ==========================================
    /// <summary>
    /// Sort the expression.
    /// </summary>
    /// <param name="descending">If true, sort in descending order. Default is false.</param>
    /// <param name="nullsLast">Whether to place null values last. Default is false.</param>
    /// <param name="multithreaded">If true, sort in multiple threads. Default is true.</param>
    /// <param name="maintainOrder">If true, maintain the order of equal elements. Default is false.</param>
    /// <param name="limit">Limit the sort output (for optimization purposes).</param>
    public Expr Sort(
        bool descending = false,
        bool nullsLast = false,
        bool multithreaded = true,
        bool maintainOrder = false,
        uint? limit = null)
    {
        return new Expr(PolarsWrapper.Sort(
            CloneHandle(), 
            descending, 
            nullsLast, 
            multithreaded, 
            maintainOrder, 
            limit
        ));
    }
    // ==========================================
    // Indexing & Searching (Arg / Index / Search)
    // ==========================================
    /// <summary>
    /// Get a single value by index. Returns a scalar.
    /// </summary>
    /// <param name="index">The index expression.</param>
    /// <param name="nullOnOutOfBounds">If true, returns Null when the index is out of bounds instead of raising an error.</param>
    public Expr Get(Expr index, bool nullOnOutOfBounds = false)
        => new(PolarsWrapper.Get(CloneHandle(), index.CloneHandle(), nullOnOutOfBounds));
    /// <summary>
    /// Get a single value by index. Returns a scalar.
    /// </summary>
    /// <param name="index">The index number.</param>
    /// <param name="nullOnOutOfBounds">If true, returns Null when the index is out of bounds instead of raising an error.</param>
    public Expr Get(ulong index, bool nullOnOutOfBounds = false)
        => new(PolarsWrapper.Get(CloneHandle(), Pl.Lit(index).Handle, nullOnOutOfBounds));
    /// <summary>
    /// Gather values by an index expression.
    /// </summary>
    public Expr Gather(IntoExprColumn indices)
        => new(PolarsWrapper.Gather(CloneHandle(), indices.Consume().Handle));
    public Expr Gather(ReadOnlySpan<int> indices) => Gather(Pl.Lit(indices));
    /// <summary>
    /// LINQ-like alias for Gather.
    /// </summary>
    public Expr Take(IntoExprColumn indices) => Gather(indices);
    public Expr Take(ReadOnlySpan<int> indices) => Take(Pl.Lit(indices));
    
    /// <summary>
    /// Take every nth value starting from an offset.
    /// </summary>
    public Expr GatherEvery(ulong n, ulong offset = 0)
       => new(PolarsWrapper.GatherEvery(CloneHandle(), (nuint)n, (nuint)offset));
    /// <summary>
    /// Get the index of the unique values.
    /// </summary>
    public Expr ArgUnique() => new(PolarsWrapper.ArgUnique(CloneHandle()));

    /// <summary>
    /// Get the index values that would sort this expression.
    /// </summary>
    /// <param name="descending">If true, sort in descending order. Default is false.</param>
    /// <param name="nullsLast">If true, place null values last. Default is false.</param>
    public Expr ArgSort(bool descending = false, bool nullsLast = false)
        => new(PolarsWrapper.ArgSort(CloneHandle(), descending, nullsLast));

    /// <inheritdoc cref="Pl.SqlExpr(string)"/>
    public static Expr SqlExpr(string sql) => Pl.SqlExpr(sql);

    /// <inheritdoc cref="Pl.SqlExprs"/>
    public static Expr[] SqlExprs(IEnumerable<string> sqls) => [.. sqls.Select(SqlExpr)];
    // ---------------------------------------------------
    // Methods
    // ---------------------------------------------------
    /// <summary>
    /// Set a new name for a column
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public Expr Alias(string name) =>
        new(PolarsWrapper.Alias(CloneHandle(), name));
    /// <summary>
    /// Reverse the selection.
    /// <para>This is useful in a GroupBy context to reverse the order of the group.</para>
    /// </summary>
    /// <returns>A new expression with the order reversed.</returns>
    public Expr Reverse() => new(PolarsWrapper.Reverse(CloneHandle()));
    /// <summary>
    /// Create a single chunk of memory for this Series.
    /// </summary>
    /// <returns></returns>
    public Expr Rechunk() => new(PolarsWrapper.Rechunk(CloneHandle()));
    /// <summary>
    /// Return the single value in the group or series.
    /// <para>
    /// This is strict: it expects the group/series to contain exactly <b>one</b> element.
    /// </para>
    /// </summary>
    /// <remarks>
    /// If the group contains more than one element, this will throw an error at runtime.
    /// It is safer than <see cref="First"/> when you expect uniqueness (e.g., getting the ID of a group).
    /// </remarks>
    /// <param name="allowEmpty">
    /// If <c>true</c> and the group is empty, it returns <c>null</c> instead of throwing an error.
    /// Default is <c>true</c>.
    /// </param>
    /// <returns>A new expression representing the single item.</returns>
    public Expr Item(bool allowEmpty=true) => new(PolarsWrapper.Item(CloneHandle(),allowEmpty));
    /// <summary>
    /// Calculate the lower bound.
    /// Returns a unit Series with the lowest value possible for the dtype of this expression.
    /// </summary>
    public Expr LowerBound() => new(PolarsWrapper.LowerBound(CloneHandle()));
    /// <summary>
    /// Calculate the upper bound.
    /// Returns a unit Series with the highest value possible for the dtype of this expression.
    /// </summary>
    public Expr UpperBound() => new(PolarsWrapper.UpperBound(CloneHandle()));
    /// <summary>
    /// Cast to physical representation of the logical dtype.
    /// </summary>
    public Expr ToPhysical() => new(PolarsWrapper.ExprToPhysical(CloneHandle()));
    // ==========================================
    // Random
    // ==========================================
    /// <summary>
    /// Shuffle the contents of this expression.Note this is shuffled independently of any other column or Expression. If you want each row to stay the same use df.sample(shuffle=True)
    /// </summary>
    /// <param name="seed">Seed for the random number generator. If set to None (default), a random seed is generated each time the shuffle is called.</param>
    public Expr Shuffle(ulong? seed=null) => new(PolarsWrapper.ExprShuffle(CloneHandle(),seed));
    /// <summary>
    /// Sample from this expression.
    /// </summary>
    /// <param name="n">Number of items to return. Default to 1</param>
    /// <param name="withReplacement">Allow values to be sampled more than once.</param>
    /// <param name="shuffle">Shuffle the order of sampled data points.</param>
    /// <param name="seed">Seed for the random number generator. If set to None (default), a random seed is generated for each sample operation.</param>
    public Expr Sample(ulong n=1,bool withReplacement=false,bool shuffle=false,ulong? seed=null) 
        => new(PolarsWrapper.ExprSampleN(CloneHandle(),Pl.Lit(n).Handle,withReplacement,shuffle,seed));
    /// <inheritdoc cref="Expr.Sample(ulong, bool, bool, ulong?)"/>
    /// <param name="fraction">Fraction of items to return.</param>
    public Expr Sample(double fraction,bool withReplacement=false,bool shuffle=false,ulong? seed=null) 
        => new(PolarsWrapper.ExprSampleFrac(CloneHandle(),Pl.Lit(fraction).Handle,withReplacement,shuffle,seed));

    // ==========================================
    // Rounding & Sign
    // ==========================================
    /// <summary>
    /// Round underlying floating point data by decimals digits.
    /// </summary>
    /// <param name="decimals">Number of decimals to round by.</param>
    /// <param name="mode">The rounding strategy used. A “rounded value” is a value with at most decimals decimal places (e.g. integers when decimals=0, multiples of 0.1 when decimals=1, 0.01 when decimals=2, and so on).
    /// Strategies that start with half_ round all values to the nearest rounded value, only using the strategy to break ties when a value falls exactly between two rounded values (e.g. 0.5 when decimals=0, 0.05 when decimals=1). 
    /// Other rounding strategies specify explicitly which rounded value is chosen and always apply (not just for tiebreaks).</param>
    public Expr Round(uint decimals=0,RoundMode mode=RoundMode.HalfToEven) => new(PolarsWrapper.Round(CloneHandle(), decimals,mode.ToNative()));
    /// <summary>
    /// Round to a number of significant figures.
    /// </summary>
    /// <param name="digits">Number of significant figures to round to.</param>
    public Expr RoundSigFigs(int digits) => new(PolarsWrapper.RoundSigFigs(CloneHandle(),digits));
    /// <summary>Compute the element-wise sign (-1, 0, 1).</summary>
    public Expr Sign() => new(PolarsWrapper.Sign(CloneHandle()));

    /// <summary>Rounds up to the nearest integer.</summary>
    public Expr Ceil() => new(PolarsWrapper.Ceil(CloneHandle()));

    /// <summary>Rounds down to the nearest integer.</summary>
    public Expr Floor() => new(PolarsWrapper.Floor(CloneHandle()));

    /// <summary>
    /// Return indices where expression evaluates True.
    /// </summary>
    /// <returns>Expression of data type UInt32.</returns>
    public Expr ArgTrue() => Pl.ArgWhere(this);

    // ==========================================
    // Null Handling
    // ==========================================

    /// <summary>
    /// Fill null values using the specified value
    /// </summary>
    /// <param name="value">Value used to fill null values.</param>
    public Expr FillNull(IntoExpr value) => new(PolarsWrapper.FillNull(CloneHandle(), value.Consume().Handle));
    /// <summary>
    /// Fill null values using the specified strategy
    /// </summary>
    /// <param name="strategy">Strategy used to fill null values.</param>
    /// <param name="limit">Number of consecutive null values to fill when using the ‘forward’ or ‘backward’ strategy.</param>
    public Expr FillNull(FillNullStrategy strategy, uint? limit = null)
        => new(PolarsWrapper.FillNullWithStrategy(CloneHandle(), strategy.ToNative(), limit));
    /// <summary>
    /// Fill null values with a specific strategy (Forward).
    /// </summary>
    /// <param name="limit">Max number of consecutive nulls to fill. (Default null = infinite)</param>
    public Expr ForwardFill(uint limit = 0) => FillNull(FillNullStrategy.Forward,limit);
    /// <summary>
    /// Fill null values with a specific strategy (Backward).
    /// </summary>
    /// <param name="limit">Max number of consecutive nulls to fill. (Default null = infinite)</param>
    public Expr BackwardFill(uint limit = 0) => FillNull(FillNullStrategy.Backward,limit);
    /// <summary>
    /// Interpolate intermediate values. The interpolation method can be configured.
    /// <para>Nulls at the beginning and end of the series remain null.</para>
    /// </summary>
    /// <param name="method">Interpolation method (Linear or Nearest).</param>
    public Expr Interpolate(InterpolationMethod method = InterpolationMethod.Linear)
        => new(PolarsWrapper.Interpolate(CloneHandle(), method.ToNative()));
    /// <summary>
    /// Interpolate intermediate values based on the values of another column.
    /// <para>
    /// This is useful when the data is not equally spaced, for example when interpolating based on a timestamp column.
    /// </para>
    /// </summary>
    /// <param name="by">The column to use for interpolation (e.g. a timestamp column).</param>
    /// <returns>A new expression with interpolated values.</returns>
    public Expr InterpolateBy(IntoExprColumn by) => new(PolarsWrapper.InterpolateBy(CloneHandle(), by.Consume().Handle));

    /// <summary>
    /// Fill floating point NaN values with a specified value.
    /// Note: This is different from FillNull. It only handles IEEE 754 NaN.
    /// </summary>
    public Expr FillNan(IntoExpr value) => new(PolarsWrapper.FillNan(CloneHandle(), value.Consume().Handle));
    /// <summary>
    /// Drop null values.
    /// </summary>
    public Expr DropNulls() => new(PolarsWrapper.DropNulls(CloneHandle()));
    /// <summary>
    /// Drop nan values.
    /// </summary>
    public Expr DropNans() => new(PolarsWrapper.DropNans(CloneHandle()));
    // ==========================================
    // Top-K & Bottom-K
    // ==========================================
    /// <summary>
    /// Get the top k largest values.
    /// <para>This is much faster than Sort().Head(k) for large datasets.</para>
    /// </summary>
    public Expr TopK(int k) => new(PolarsWrapper.TopK(CloneHandle(), (uint)k));

    /// <summary>
    /// Get the bottom k smallest values.
    /// <para>This is much faster than Sort().Tail(k) for large datasets.</para>
    /// </summary>
    public Expr BottomK(int k) => new(PolarsWrapper.BottomK(CloneHandle(), (uint)k));
    /// <summary>
    /// Get the top <paramref name="k"/> rows according to the sorting criteria defined by <paramref name="by"/>.
    /// </summary>
    /// <param name="k">Number of rows to return.</param>
    /// <param name="by">The expressions (columns) to sort by.</param>
    /// <param name="reverse">
    /// Controls the sorting direction for each expression in <paramref name="by"/>.
    /// <para>
    /// For <b>TopK</b>: 
    /// <br/>- <c>false</c> (default): Sorts <b>descending</b> (picks largest values).
    /// <br/>- <c>true</c>: Sorts <b>ascending</b> (picks smallest values, acting like BottomK for this column).
    /// </para>
    /// Length must match <paramref name="by"/>.
    /// </param>
    /// <returns>A new expression.</returns>
    /// <exception cref="ArgumentException">If the length of <paramref name="by"/> and <paramref name="reverse"/> do not match.</exception>
    public Expr TopKBy(int k, Expr[] by, bool[] reverse)
    {
        if (by.Length != reverse.Length)
            throw new ArgumentException("The length of 'by' and 'reverse' must match.");

        var byHandles = System.Array.ConvertAll(by, e => e.CloneHandle());

        return new Expr(PolarsWrapper.TopKBy(CloneHandle(), (uint)k, byHandles, reverse));
    }

    /// <summary>
    /// Get the top <paramref name="k"/> rows according to a single sorting criterion.
    /// <para>This is a convenience overload for a single expression.</para>
    /// </summary>
    /// <param name="k">Number of rows to return.</param>
    /// <param name="by">The expression (column) to sort by.</param>
    /// <param name="reverse">
    /// <inheritdoc cref="TopKBy(int, Expr[], bool[])" path="/param[@name='reverse']/node()"/>
    /// </param>
    /// <returns>A new expression.</returns>
    public Expr TopKBy(int k, Expr by, bool reverse = false) => TopKBy(k, [by], [reverse]);

    /// <summary>
    /// Get the bottom <paramref name="k"/> rows according to the sorting criteria defined by <paramref name="by"/>.
    /// </summary>
    /// <param name="k">Number of rows to return.</param>
    /// <param name="by">The expressions (columns) to sort by.</param>
    /// <param name="reverse">
    /// Controls the sorting direction for each expression in <paramref name="by"/>.
    /// <para>
    /// For <b>BottomK</b>: 
    /// <br/>- <c>false</c> (default): Sorts <b>ascending</b> (picks smallest values).
    /// <br/>- <c>true</c>: Sorts <b>descending</b> (picks largest values, acting like TopK for this column).
    /// </para>
    /// Length must match <paramref name="by"/>.
    /// </param>
    /// <returns>A new expression.</returns>
    public Expr BottomKBy(int k, Expr[] by, bool[] reverse)
    {
        if (by.Length != reverse.Length)
            throw new ArgumentException("The length of 'by' and 'reverse' must match.");

        var byHandles = System.Array.ConvertAll(by, e => e.CloneHandle());

        return new Expr(PolarsWrapper.BottomKBy(CloneHandle(), (uint)k, byHandles, reverse));
    }

    /// <summary>
    /// Get the bottom <paramref name="k"/> rows according to a single sorting criterion.
    /// <para>This is a convenience overload for a single expression.</para>
    /// </summary>
    /// <inheritdoc cref="BottomKBy(int, Expr[], bool[])" path="/param[@name='reverse']"/>
    /// <param name="k">Number of rows to return.</param>
    /// <param name="by">The expression (column) to sort by.</param>
    /// <param name="reverse">See <see cref="BottomKBy(int, Expr[], bool[])"/>.</param>
    /// <returns>A new expression.</returns>
    public Expr BottomKBy(int k, Expr by, bool reverse = false) => BottomKBy(k, [by], [reverse]);

    // ==========================================
    // Statistical Ops
    // ==========================================
    /// <summary>
    /// Compute the sample skewness of a data set.
    /// </summary>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    /// <returns>A series which length is 1</returns>
    public Expr Skew(bool bias = true) => new(PolarsWrapper.Skew(CloneHandle(), bias));
    /// <summary>
    /// Compute the kurtosis (Fisher or Pearson) of a dataset.
    /// </summary>
    /// <param name="fisher">If True, Fisher’s definition is used (normal ==> 0.0). If False, Pearson’s definition is used (normal ==> 3.0).</param>
    /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
    /// <returns>A series which length is 1</returns>
    public Expr Kurtosis(bool fisher = true, bool bias = true) => new(PolarsWrapper.Kurtosis(CloneHandle(), fisher, bias));
    /// <summary>
    /// Computes percentage change between values.
    /// Percentage change (as fraction) between current element and most-recent non-null element at least n period(s) before the current element.
    /// Computes the change from the previous row by default.
    /// </summary>
    /// <param name="n">periods to shift for forming percent change.</param>
    /// <returns>A series which length is 1</returns>
    public Expr PctChange(int n = 1) => new(PolarsWrapper.PctChange(CloneHandle(), n));
    /// <summary>
    /// Assign ranks to data, dealing with ties appropriately.
    /// </summary>
    /// <param name="method">
    /// The method used to assign ranks to tied elements. See <see cref="RankMethod"/> for details.
    /// Default is <see cref="RankMethod.Average"/>.</param>
    /// <param name="descending">Rank in descending order.</param>
    /// <param name="seed">If method="random", use this as seed.</param>
    /// <returns></returns>
    public Expr Rank(RankMethod method = RankMethod.Average, bool descending = false, ulong? seed = null)
        => new(PolarsWrapper.Rank(CloneHandle(), method.ToNative(), descending, seed));

    // ==========================================
    // Logic / Comparison
    // ==========================================
    /// <summary>
    /// Filter a single column.
    /// <br/>
    /// Mostly useful in <c>group_by</c> context or when you want to filter an expression based on another expression within a <c>Select</c> context.
    /// </summary>
    /// <param name="predicate">Boolean expression used to filter the current expression.</param>
    /// <returns>A new expression with filtered values.</returns>
    public Expr Filter(Expr predicate)
        => new(PolarsWrapper.Filter(CloneHandle(),predicate.CloneHandle()));
    /// <summary>
    /// Compress the column data using run-length encoding.
    /// Run-length encoding (RLE) encodes data by storing each run of identical values as a single value and its length.
    /// </summary>
    /// <returns>Expression/Series of data type Struct with fields len of data type UInt32 and value of the original data type.</returns>  
    public Expr Rle() => new(PolarsWrapper.Rle(CloneHandle()));
    /// <summary>
    /// Get a distinct integer ID for each run of identical values.
    /// The ID starts at 0 and increases by one each time the value of the column changes.
    /// </summary>
    /// <returns>Expression/Series of data type UInt32.</returns>
    public Expr RleId() => new(PolarsWrapper.RleId(CloneHandle()));
    /// <summary>
    /// Get a boolean mask of the local maximum peaks.
    /// </summary>
    public Expr PeakMax() => new(PolarsWrapper.PeakMax(CloneHandle()));
    /// <summary>
    /// Get a boolean mask of the local minimum peaks.
    /// </summary>
    public Expr PeakMin() => new(PolarsWrapper.PeakMin(CloneHandle()));
    /// <summary>
    /// Bin continuous values into discrete categories.
    /// </summary>
    /// <param name="breaks">List of unique cut points.</param>
    /// <param name="labels">Names of the categories. The number of labels must be equal to the number of cut points plus one.</param>
    /// <param name="leftClosed">Set the intervals to be left-closed instead of right-closed.</param>
    /// <param name="includeBreaks">Include a column with the right endpoint of the bin each observation falls in. This will change the data type of the output from an Enum to a Struct.</param>
    /// <returns>Expression/Series of data type Enum if include_breaks is set to False (default), otherwise an expression of data type Struct.</returns>
    public Expr Cut(ReadOnlySpan<double> breaks,string[]? labels = null,bool leftClosed=false,bool includeBreaks=false)
        => new(PolarsWrapper.Cut(CloneHandle(),breaks,labels,leftClosed,includeBreaks));
    /// <summary>
    /// Bin continuous values into discrete categories based on their quantiles.
    /// </summary>
    /// <param name="quantiles">Either a list of quantile probabilities between 0 and 1 or a positive integer determining the number of bins with uniform probability.</param>
    /// <param name="labels">Names of the categories. The number of labels must be equal to the number of categories.</param>
    /// <param name="leftClosed">Set the intervals to be left-closed instead of right-closed.</param>
    /// <param name="allowDuplicates">If set to True, duplicates in the resulting quantiles are dropped, rather than raising a DuplicateError. This can happen even with unique probabilities, depending on the data.</param>
    /// <param name="includeBreaks">Include a column with the right endpoint of the bin each observation falls in. This will change the data type of the output from a Categorical to a Struct.</param>
    /// <returns>Expression/Series of data type Categorical if include_breaks is set to False (default), otherwise an expression of data type Struct.</returns>
    public Expr QCut(ReadOnlySpan<double> quantiles,string[]? labels = null,bool leftClosed=false,bool allowDuplicates=false,bool includeBreaks =false)
        => new(PolarsWrapper.QCut(CloneHandle(),quantiles,labels,leftClosed,allowDuplicates,includeBreaks));
    /// <inheritdoc cref="Expr.QCut(ReadOnlySpan{double}, string[], bool, bool, bool)"/>
    public Expr QCut(int quantiles,string[]? labels = null,bool leftClosed=false,bool allowDuplicates=false,bool includeBreaks =false)
        => new(PolarsWrapper.QCutUniform(CloneHandle(),(nuint)quantiles,labels,leftClosed,allowDuplicates,includeBreaks));
    /// <summary>
    /// Replace the given values by different values of the same data type.
    /// </summary>
    /// <param name="old">Value or sequence of values to replace. Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals.</param>
    /// <param name="newExpr">Value or sequence of values to replace by. Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals.</param>
    public Expr Replace(IntoExpr old,IntoExpr newExpr) => new(PolarsWrapper.ExprReplace(CloneHandle(),old.Consume().Handle,newExpr.Consume().Handle));
    /// <summary>
    /// Replace all values by different values.
    /// </summary>
    /// <param name="old">Value or sequence of values to replace. Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals.</param>
    /// <param name="newExpr">Value or sequence of values to replace by. Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals. Length must match the length of old or have length 1.</param>
    /// <param name="defaultExpr">Set values that were not replaced to this value. If no default is specified, (default), an error is raised if any values were not replaced. Accepts expression input. Non-expression inputs are parsed as literals.</param>
    /// <param name="returnDataType">The data type of the resulting expression. If set to null (default), the data type is determined automatically based on the other inputs.</param>
    public Expr ReplaceStrict(IntoExpr old,IntoExpr newExpr,IntoExpr? defaultExpr=null,IntoDataTypeExpr? returnDataType=null)
    {
        ExprHandle? realDefault = defaultExpr?.Consume().Handle;
        DataTypeExprHandle? dtype = returnDataType?.Consume().Handle;
        return new(PolarsWrapper.ExprReplaceStrict(CloneHandle(),old.Consume().Handle,newExpr.Consume().Handle,realDefault,dtype));
    }
    /// <summary>
    /// Replace values using a mapping (Dictionary/IEnumerable of KeyValuePair).
    /// </summary>
    public Expr Replace<TKey, TValue>(IEnumerable<KeyValuePair<TKey, TValue>> mapping)
        => Replace(mapping.Select(k => k.Key), mapping.Select(v => v.Value));
    
    /// <summary>
    /// Replace values strictly using a mapping.
    /// </summary>
    public Expr ReplaceStrict<TKey, TValue>(
        IEnumerable<KeyValuePair<TKey, TValue>> mapping, 
        IntoExpr? defaultExpr = null, 
        IntoDataTypeExpr? returnDataType = null)
        => ReplaceStrict(mapping.Select(k => k.Key), mapping.Select(v => v.Value),defaultExpr,returnDataType);
    /// <summary>
    /// Replace values using two sequences (IEnumerable) for old and new values.
    /// </summary>
    public Expr Replace<TOld, TNew>(IEnumerable<TOld> oldValues, IEnumerable<TNew> newValues)
    {
        using Series oldSeries = Pl.Series("old", oldValues);
        using Series newSeries = Pl.Series("new", newValues);

        return Replace(oldSeries, newSeries);
    }
    /// <summary>
    /// Replace values strictly using two sequences (IEnumerable).
    /// </summary>
    public Expr ReplaceStrict<TOld, TNew>(
        IEnumerable<TOld> oldValues, 
        IEnumerable<TNew> newValues, 
        IntoExpr? defaultExpr = null, 
        IntoDataTypeExpr? returnDataType = null)
    {
        using Series oldSeries = Pl.Series("old", oldValues);
        using Series newSeries = Pl.Series("new", newValues);

        return ReplaceStrict(oldSeries, newSeries, defaultExpr, returnDataType);
    }
    /// <summary>
    /// Append expressions.
    /// This is done by adding the chunks of other to this Series.
    /// </summary>
    /// <param name="other">Expression to append.</param>
    /// <param name="upcast">Cast both Series to the same supertype.</param>
    public Expr Append(IntoExpr other,bool upcast = true) => new(PolarsWrapper.Append(CloneHandle(),other.Consume().Handle,upcast));
    /// <summary>
    /// Extremely fast method for extending the Series with ‘n’ copies of a value.
    /// </summary>
    /// <param name="value">A constant literal value or a unit expression with which to extend the expression result Series; can pass None to extend with nulls.</param>
    /// <param name="n">The number of additional values that will be added.</param>
    public Expr ExtendConstant(IntoExpr value,Expr n)
        => new(PolarsWrapper.ExtendConstant(CloneHandle(),value.Consume().Handle,n.CloneHandle()));
    /// <summary>
    /// Set values outside the given boundaries to the boundary value.
    /// </summary>
    /// <param name="lowerBound">Lower bound. Accepts expression input. Non-expression inputs are parsed as literals. Strings are parsed as column names.</param>
    /// <param name="upperBound">Upper bound. Accepts expression input. Non-expression inputs are parsed as literals. Strings are parsed as column names.</param>
    public Expr Clip(IntoExprColumn? lowerBound = null, IntoExprColumn? upperBound = null)
    {
        if (lowerBound is null && upperBound is null)
        {
            throw new ArgumentException("At least one of 'lowerBound' or 'upperBound' must be provided.");
        }

        if (lowerBound is not null && upperBound is null)
        {
            var hMin = PolarsWrapper.ClipMin(Handle, lowerBound?.Consume().Handle!);
            return new Expr(hMin);
        }

        if (lowerBound is null && upperBound is not null)
        {
            var hMax = PolarsWrapper.ClipMax(Handle, upperBound?.Consume().Handle!);
            return new Expr(hMax);
        }

        var hFull = PolarsWrapper.Clip(
            Handle, 
            lowerBound?.Consume().Handle!, 
            upperBound?.Consume().Handle!
        );
        
        return new Expr(hFull);
    }
    // ==========================================
    // Casting
    // ==========================================

    /// <summary>
    /// Cast expression to another data type.
    /// </summary>
    /// <param name="dtype">The target type (can be .NET Type, Polars DataType, or DataTypeExpr)</param>
    /// <param name="strict">Throws an error if conversion had overflows.</param>
    /// <param name="wrapNumerical">Allows wrapping numerical overflow.</param>
    public Expr Cast(IntoDataTypeExpr dtype, bool strict = true, bool wrapNumerical = false)
    {
        if (strict && wrapNumerical)
        {
            throw new ArgumentException("Cannot set both 'strict' and 'wrapNumerical' to true.");
        }

        using var targetDTypeExpr = dtype.Consume();

        var h = PolarsWrapper.ExprCast(this.CloneHandle(), targetDTypeExpr.Handle, strict, wrapNumerical);
        return new Expr(h);
    }
    /// <inheritdoc cref="Cast"/>
    public Expr Cast<T>( bool strict = true, bool wrapNumerical = false)
        => Cast(DataType.FromNetType<T>(),strict,wrapNumerical);

    // ==========================================
    // UDF / Map
    // ==========================================
    /// <summary>
    /// Apply a custom C# function to the expression.
    /// This runs locally in the .NET runtime, converting data between Polars and .NET.
    /// </summary>
    /// <typeparam name="TInput">Input type (e.g. int, double, string)</typeparam>
    /// <typeparam name="TOutput">Output type (e.g. int, double, string)</typeparam>
    /// <param name="function">The function to apply.</param>
    /// <param name="outputType">The Polars data type of the output column.</param>
    /// <summary>
    /// Apply a custom C# function to the expression (High-Level).
    /// </summary>
    public Expr Map<TInput, TOutput>(Func<TInput, TOutput> function, DataType outputType)
        => new(PolarsWrapper.Map(CloneHandle(), UdfUtils.Wrap(function), outputType.Handle));

    /// <summary>
    /// Apply a raw Arrow-to-Arrow UDF. (Advanced / Internal use)
    /// </summary>
    public Expr Map(Func<IArrowArray, IArrowArray> function, DataType outputType)
        => new(PolarsWrapper.Map(CloneHandle(), function, outputType.Handle));

    /// <summary>
    /// Apply a multi-column UDF that receives multiple Arrow arrays and returns a single new column.
    /// The first expression is <c>this</c>.
    /// </summary>
    /// <param name="function">Receives all input arrays (note: including <c>this</c> as the first element).</param>
    /// <param name="outputType">The expected output data type.</param>
    /// <param name="additionalExprs">Other columns/expressions as IntoExprColumn.</param>
    public Expr Map(
        Func<IReadOnlyList<IArrowArray>, IArrowArray> function,
        DataType outputType,
        params IntoExprColumn[] additionalExprs)
    {
        if (additionalExprs == null || additionalExprs.Length == 0)
            throw new ArgumentException("At least one additional expression is required for MapMany.");
        
        // Convert IntoExprColumn -> Expr (dealing with ownership)
        var otherExprs = additionalExprs
            .Select(e => e.Consume())      
            .Select(e => e.Handle)         
            .ToArray();
        
        // this will be transferred in the wrapper
        return new(PolarsWrapper.MapMany(
            CloneHandle(),
            otherExprs,
            function,
            outputType.Handle));
    }

    // ==========================================
    // Window & Offset
    // ==========================================
    #region Window & Offset Functions

    /// <summary>
    /// Apply a window function over a subgroup.
    /// <para>
    /// This is similar to SQL's `OVER (PARTITION BY ...)` clause.
    /// Unlike <see cref="DataFrame.GroupBy(IntoExprColumn,bool)"/>, this does not reduce the number of rows.
    /// The result is broadcasted back to the original rows.
    /// </para>
    /// </summary>
    /// <param name="partitionBy">The columns to partition by.</param>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     group = new[] { "A", "A", "A", "B", "B" },
    ///     val = new[] { 10, 20, 30, 100, 200 }
    /// });
    /// 
    /// // Calculate mean per group and subtract it from the value
    /// // The result has the same shape as the original DataFrame (5 rows)
    /// df.Select(
    ///     Pl.Col("group"),
    ///     Pl.Col("val"),
    ///     Pl.Col("val").Mean().Over("group").Alias("group_mean"),
    ///     (Pl.Col("val") - Pl.Col("val").Mean().Over("group")).Alias("diff_from_mean")
    /// ).Show();
    /// /* Output:
    /// shape: (5, 4)
    /// ┌───────┬─────┬────────────┬────────────────┐
    /// │ group ┆ val ┆ group_mean ┆ diff_from_mean │
    /// │ ---   ┆ --- ┆ ---        ┆ ---            │
    /// │ str   ┆ i32 ┆ f64        ┆ f64            │
    /// ╞═══════╪═════╪════════════╪════════════════╡
    /// │ A     ┆ 10  ┆ 20.0       ┆ -10.0          │
    /// │ A     ┆ 20  ┆ 20.0       ┆ 0.0            │
    /// ...
    /// └───────┴─────┴────────────┴────────────────┘
    /// */
    /// </code>
    /// </example>
    public Expr Over(params IntoExprColumn[] partitionBy)
        => Over((IEnumerable<IntoExprColumn>)partitionBy);

    /// <summary>
    /// Compute expressions over the given groups.
    /// This expression is similar to performing a group by aggregation and joining the result back into the original DataFrame.
    /// The outcome is similar to how window functions work in PostgreSQL.
    /// </summary>
    /// <param name="partitionBy">Column(s) to group by. Accepts expression input. Strings are parsed as column names.</param>
    /// <param name="orderBy">Order the window functions/aggregations with the partitioned groups by the result of the expression passed to order_by.</param>
    /// <param name="descending">In case ‘order_by’ is given, indicate whether to order in ascending or descending order.</param>
    /// <param name="nullsLast">In case ‘order_by’ is given, indicate whether to order the nulls in last position.</param>
    /// <param name="multithreaded"></param>
    /// <param name="maintainOrder"></param>
    /// <param name="mappingStrategy"></param>
    /// <returns></returns>
    public Expr Over(
        IEnumerable<IntoExprColumn>? partitionBy = null,
        IEnumerable<IntoExprColumn>? orderBy = null,
        bool descending = false,
        bool nullsLast = false,
        bool multithreaded = true, 
        bool maintainOrder = false, 
        WindowMappingStrategy mappingStrategy = WindowMappingStrategy.GroupsToRows)
    {
        var exprArray = partitionBy?.ToArray() ?? [];
        var orderArray = orderBy?.ToArray() ?? [];

        if (exprArray.Length == 0 && orderArray.Length == 0) 
        {
            return this; 
        }

        var handles = System.Array.ConvertAll(exprArray, e => e.Consume().Handle);
        var orderHandles = System.Array.ConvertAll(orderArray, e => e.Consume().Handle);

        return new Expr(PolarsWrapper.Over(
            CloneHandle(), 
            handles,
            orderHandles,
            descending,
            nullsLast,
            multithreaded,
            maintainOrder,
            mappingStrategy.ToNative()
        ));
    }    
    /// <summary>
    /// Shift values by the given number of indices.
    /// Positive values shift downstream, negative values shift upstream.
    /// </summary>
    public Expr Shift(Expr n) => new(PolarsWrapper.Shift(CloneHandle(), n.CloneHandle()));
    /// <summary>
    /// Shift values by 1 index downstream.
    /// </summary>
    public Expr Shift() => Shift(1);

    #endregion

    /// <summary>
    /// Explode a list expression.
    /// <para>
    /// This turns a list column into a long column (flattening).
    /// </para>
    /// <para>
    /// <b>Warning:</b> When used in <see cref="DataFrame.Select(IntoExprColumn[])"/> with other columns, 
    /// it may cause a length mismatch error if the other columns are not broadcasted. 
    /// Use <see cref="DataFrame.Explode(string[])"/> for safely exploding columns while repeating others.
    /// </para>
    /// </summary>
    /// <param name="emptyAsNull">
    /// If <c>true</c>, empty lists are exploded into a single <c>null</c> value. 
    /// If <c>false</c>, rows with empty lists are removed from the result.
    /// </param>
    /// <param name="keepNulls">
    /// If <c>true</c>, <c>null</c> values in the column are preserved as <c>null</c> in the result. 
    /// If <c>false</c>, rows with <c>null</c> values are removed.
    /// </param>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     id = new[] { 1, 2 },
    ///     tags = new[] { new[] { "a", "b" }, new[] { "c" } }
    /// });
    /// 
    /// // Example 1: Expression Explode (Flatten single column)
    /// df.Select(
    ///     Pl.Col("tags").Explode().Alias("tags_flat")
    /// ).Show();
    /// /* Output:
    /// shape: (3, 1)
    /// ┌───────────┐
    /// │ tags_flat │
    /// │ ---       │
    /// │ str       │
    /// ╞═══════════╡
    /// │ a         │
    /// │ b         │
    /// │ c         │
    /// └───────────┘
    /// */
    /// 
    /// // Example 2: To keep 'id' aligned, use DataFrame.Explode:
    /// // df.Explode("tags").Show();
    /// </code>
    /// </example>
    public Expr Explode(bool emptyAsNull=true,bool keepNulls=true) => new(PolarsWrapper.Explode(CloneHandle(),emptyAsNull,keepNulls));
    /// <summary>
    /// Returns the first non-null value between this expression and other expressions.
    /// Syntactic sugar for <c>Pl.Coalesce(this, others)</c>.
    /// </summary>
    /// <param name="others">Fallback expressions, column names, or literals.</param>
    /// <returns>A new coalesced expression.</returns>
    /// <example>
    /// <code>
    /// // Fill nulls in "val_a" with values from "val_b", and fallback to 0 if both are null
    /// df.Select(
    ///     Pl.Col("val_a").Coalesce("val_b", 0).Alias("merged_val")
    /// );
    /// </code>
    /// </example>
    public Expr Coalesce(params IntoExprColumn[] others)
    {
        if (others == null || others.Length == 0) return this;

        var allExprs = new IntoExprColumn[others.Length + 1];
        allExprs[0] = this; 
        
        for (int i = 0; i < others.Length; i++)
        {
            allExprs[i + 1] = others[i];
        }

        return Pl.Coalesce(allExprs);
    }
    internal static Expr Ternary(Expr predicate, Expr truthy, Expr falsy)
    {
        var handle = PolarsWrapper.IfElse(
            predicate.CloneHandle(), 
            truthy.CloneHandle(), 
            falsy.CloneHandle());
            
        return new Expr(handle);
    }

    /// <summary>
    /// Indicate that the expression is sorted.
    /// This is a hint to the optimizer and does not actually sort the data.
    /// </summary>
    /// <param name="descending">Whether the column is sorted in descending order.</param>
    /// <param name="nullsLast">Whether null values appear last. (Placeholder for Polars 0.54+)</param>
    /// <returns>A new expression with the sorted flag set.</returns>
    public Expr SetSorted(bool descending = false, bool nullsLast = false) => new(PolarsWrapper.ExprSetSorted(CloneHandle(), descending, nullsLast));
    /// <summary>
    /// Reshape the column into a multi-dimensional array.
    /// </summary>
    /// <param name="dimensions">Tuple of the dimension sizes. If a -1 is used, that dimension is inferred.</param>
    public Expr Reshape(ReadOnlySpan<long> dimensions) => new (PolarsWrapper.ExprReshape(CloneHandle(), dimensions));
    /// <summary>
    /// Get a slice of this expression.
    /// </summary>
    /// <param name="offset">Start index. Negative indexing is supported.</param>
    /// <param name="length">Length of the slice. If set to None, all rows starting at the offset will be selected.</param>
    public Expr Slice(long offset,ulong? length=null)
    {
        ulong realLength = length ?? ulong.MaxValue;
        return new(PolarsWrapper.ExprSlice(CloneHandle(),Pl.Lit(offset).Handle,Pl.Lit(realLength).Handle));
    }
    /// <summary>
    /// Print the value that this expression evaluates to and pass on the value.
    /// </summary>
    public Expr Inspect(string format="{}") => new(PolarsWrapper.ExprInspect(CloneHandle(),format));
    /// <summary>
    /// Reinterpret the underlying bits as a signed/unsigned integer.
    /// This operation is only allowed for numeric types of the same size. For lower bits numbers, you can safely use the cast operation.
    /// </summary>
    /// <param name="signed">If True, reinterpret as signed integer. Otherwise, reinterpret as unsigned integer.</param>
    public Expr Reinterpret(bool signed=true) => new(PolarsWrapper.ExprReinterpret(CloneHandle(),signed));
    /// <summary>
    /// Repeat the elements in this Series as specified in the given expression.
    /// The repeated elements are expanded into a List.
    /// </summary>
    /// <param name="by">Numeric column that determines how often the values will be repeated. The column will be coerced to UInt32. Give this dtype to make the coercion a no-op.</param>
    /// <returns>Expression/Series of data type List, where the inner data type is equal to the original data type.</returns>
    public Expr RepeatBy(IntoExpr by) => new(PolarsWrapper.ExprRepeatBy(CloneHandle(),by.Consume().Handle));
   /// <summary>
    /// Apply a user-defined function to this expression, returning the result of the function.
    /// This allows encapsulating a sequence of Polars expression operations into a reusable function.
    /// </summary>
    /// <typeparam name="T">The return type of the function.</typeparam>
    /// <param name="function">
    /// A function that receives the current expression and returns a value of type <typeparamref name="T"/>.
    /// Typically this function wraps several Polars API calls that operate on the given expression.
    /// </param>
    /// <returns>The result of applying <paramref name="function"/> to this expression.</returns>
    public T Pipe<T>(Func<Expr, T> function) => function(this);
    
    // ==========================================
    // Namespaces
    // ==========================================

    /// <summary>
    /// Access temporal (Date/Time) operations.
    /// </summary>
    public DtOps Dt => new(this);

    /// <summary>
    /// Access string manipulation operations.
    /// </summary>
    public StringOps Str => new(this);
    /// <summary>
    /// Access binary operations.
    /// </summary>
    public BinaryOps Bin => new(this);
    /// <summary>
    /// Access categorical operations.
    /// </summary>
    public CategoricalOps Cat => new(this);
    /// <summary>
    /// Access list operations.
    /// </summary>
    public ListOps List => new(this);

    /// <summary>
    /// Access struct operations.
    /// </summary>
    public StructOps Struct => new(this);

    /// <summary>
    /// Access column renaming operations.
    /// </summary>
    public NameOps Name => new(this);
    /// <summary>
    /// Access array operations.
    /// </summary>
    public ArrayOps Array => new(this);
    /// <summary>
    /// Access expression meta operations.
    /// </summary>
    public MetaOps Meta => new(this);
    /// <summary>
    /// Access expression extension datatype operations.
    /// </summary>
    public ExtensionOps Ext => new(this);
    // ==========================================
    // Bridges
    // ==========================================
    public Selector ToSelector() => new(PolarsWrapper.ToSelector(CloneHandle()));

    // ==========================================
    // Clean Up
    // ==========================================
    /// <summary>
    /// Dispose a handle.
    /// </summary>
    public void Dispose()
    {
        Handle?.Dispose();
        GC.SuppressFinalize(this); 
    }
    public override string ToString()
    {
        if (Handle.IsInvalid) return "Expr (Disposed)";
        return PolarsWrapper.ExprToString(Handle);
    }
    /// <summary>
    /// Decide whether two Exprs are same
    /// </summary>
    public bool Equals(Expr? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        bool result = PolarsWrapper.ExprEquals(Handle, other.Handle);
        return result;
    }
    public override bool Equals(object? obj) 
        => Equals(obj as Expr);
    /// <summary>
    /// Get hashcode based on handles
    /// </summary>
    public override int GetHashCode()
    {
        if (Handle.IsInvalid) return 0;

        var roots = string.Join(",", Meta.RootNames());
        return roots.GetHashCode();
    }

}









