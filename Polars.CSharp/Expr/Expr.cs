#pragma warning disable CS1591
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

    /// <summary>
    /// Column Exprs (name: string)
    /// </summary>
    /// <param name="names"></param>
    /// <returns></returns>
    public static Expr Col(params string[] names) => Pl.Col(names);
    /// <summary>
    /// Select all columns, same as Col("*")
    /// </summary>
    /// <returns></returns>
    public static Expr All() => Col("*");
    public Expr Exclude(params string[] names) => ToSelector().Exclude(names).ToExpr();

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
    public Expr Gather(Expr indices)
        => new(PolarsWrapper.Gather(CloneHandle(), indices.CloneHandle()));

    /// <summary>
    /// LINQ-like alias for Gather.
    /// </summary>
    public Expr Take(Expr indices) => Gather(indices);

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
    /// Get the index of the maximum value.
    /// </summary>
    public Expr ArgMax() => new(PolarsWrapper.ArgMax(CloneHandle()));

    /// <summary>
    /// Get the index of the minimum value.
    /// </summary>
    public Expr ArgMin() => new(PolarsWrapper.ArgMin(CloneHandle()));

    /// <summary>
    /// Get the index values that would sort this expression.
    /// </summary>
    /// <param name="descending">If true, sort in descending order. Default is false.</param>
    /// <param name="nullsLast">If true, place null values last. Default is false.</param>
    public Expr ArgSort(bool descending = false, bool nullsLast = false)
        => new(PolarsWrapper.ArgSort(CloneHandle(), descending, nullsLast));

    /// <summary>
    /// Find the index of the first occurrence of a specific value.
    /// </summary>
    /// <param name="element">The element expression to search for.</param>
    public Expr IndexOf(Expr element) => new(PolarsWrapper.IndexOf(CloneHandle(), element.CloneHandle()));

    /// <summary>
    /// Find indices where elements should be inserted to maintain order (Binary Search).
    /// </summary>
    /// <param name="element">The element expression to insert/search.</param>
    /// <param name="side">The insertion side (Any, Left, Right). Default is Any.</param>
    /// <param name="descending">Whether the target column is sorted in descending order. Default is false.</param>
    public Expr SearchSorted(Expr element, SearchSortedSide side = SearchSortedSide.Any, bool descending = false)
        => new(PolarsWrapper.SearchSorted(CloneHandle(), element.CloneHandle(), side.ToNative(), descending));

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

    // ==========================================
    // Aggregation
    // ==========================================
    /// <summary>
    /// Get the first value of the group/series.
    /// </summary>
    /// <returns>A new expression representing the first value.</returns>
    public Expr First(bool ignoreNulls = false) => new(PolarsWrapper.First(CloneHandle(),ignoreNulls));
    /// <summary>
    /// Get the last value of the group/series.
    /// </summary>
    /// <returns>A new expression representing the last value.</returns>
    public Expr Last(bool ignoreNulls = false) => new(PolarsWrapper.Last(CloneHandle(),ignoreNulls));

    /// <summary>
    /// Get the first n rows.
    /// </summary>
    /// <param name="n">Number of rows to return.</param>
    /// <returns></returns>
    public Expr Head(int n = 10) => new(PolarsWrapper.Head(CloneHandle(),n));
    /// <summary>
    /// Get the last n rows.
    /// </summary>
    /// <param name="n">Number of rows to return.</param>
    /// <returns></returns>
    public Expr Tail(int n = 10) => new(PolarsWrapper.Tail(CloneHandle(),n));

    /// <summary>
    /// Check if <b>all</b> values in the boolean expression are <c>true</c>.
    /// <para>This is a boolean aggregation.</para>
    /// </summary>
    /// <param name="ignoreNulls">
    /// If <c>true</c>, null values are ignored. 
    /// If <c>false</c> (default), the result propagates nulls (i.e., if there is a null and no false, the result might be null).
    /// </param>
    /// <returns>A new expression representing the boolean result.</returns>
    public Expr All(bool ignoreNulls=false) => new(PolarsWrapper.All(CloneHandle(),ignoreNulls));
    /// <summary>
    /// Check if <b>any</b> value in the boolean expression is <c>true</c>.
    /// <para>This is a boolean aggregation.</para>
    /// </summary>
    /// <param name="ignoreNulls">
    /// If <c>true</c>, null values are ignored. 
    /// If <c>false</c> (default), the result propagates nulls.
    /// </param>
    /// <returns>A new expression representing the boolean result.</returns>
    public Expr Any(bool ignoreNulls=false) => new(PolarsWrapper.Any(CloneHandle(),ignoreNulls));
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
    /// Calculate the sum of the values in the group or column.
    /// <para>
    /// Behavior depends on context:
    /// <list type="bullet">
    /// <item>In <see cref="DataFrame.GroupBy(Expr[])"/>: Calculates the sum for each group.</item>
    /// <item>In <see cref="DataFrame.Select(IntoExpr[])"/>: Calculates the sum of the entire column (scalar result).</item>
    /// </list>
    /// </para>
    /// </summary>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     group = new[] { "A", "A", "B", "B" },
    ///     val = new[] { 1, 2, 3, 4 }
    /// });
    /// 
    /// // 1. GroupBy Aggregation
    /// df.GroupBy("group").Agg(
    ///     Col("val").Sum().Alias("sum"),
    ///     Col("val").Mean().Alias("mean")
    /// ).Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌───────┬─────┬──────┐
    /// │ group ┆ sum ┆ mean │
    /// │ ---   ┆ --- ┆ ---  │
    /// │ str   ┆ i32 ┆ f64  │
    /// ╞═══════╪═════╪══════╡
    /// │ A     ┆ 3   ┆ 1.5  │
    /// │ B     ┆ 7   ┆ 3.5  │
    /// └───────┴─────┴──────┘
    /// */
    /// 
    /// // 2. Global Aggregation (Select)
    /// df.Select(
    ///     Col("val").Sum().Alias("total_sum"),
    ///     Col("val").Count().Alias("total_count")
    /// ).Show();
    /// /* Output:
    /// shape: (1, 2)
    /// ┌───────────┬─────────────┐
    /// │ total_sum ┆ total_count │
    /// │ ---       ┆ ---         │
    /// │ i32       ┆ u32         │
    /// ╞═══════════╪═════════════╡
    /// │ 10        ┆ 4           │
    /// └───────────┴─────────────┘
    /// */
    /// </code>
    /// </example>
    public Expr Sum() => new(PolarsWrapper.Sum(CloneHandle()));

    /// <summary>
    /// Compute the mean of an expression
    /// </summary>
    public Expr Mean() => new(PolarsWrapper.Mean(CloneHandle()));

    /// <summary>
    /// Compute the max of an expression
    /// </summary>
    public Expr Max() => new(PolarsWrapper.Max(CloneHandle()));

    /// <summary>
    /// Compute the min of an expression
    /// </summary>
    public Expr Min() => new(PolarsWrapper.Min(CloneHandle()));
    /// <summary>
    /// Count the number of null.
    /// </summary>
    public Expr NullCount() => new(PolarsWrapper.NullCount(CloneHandle()));
    /// <summary>
    /// Count unique values.
    /// Notes: Null is considered to be a unique value for the purposes of this operation.
    /// </summary>
    public Expr NUnique() => new(PolarsWrapper.NUnique(CloneHandle()));
    /// <summary>
    /// Approximate count of unique values.
    /// This is done using the HyperLogLog++ algorithm for cardinality estimation.
    /// </summary>
    /// <returns></returns>
    public Expr ApproxNUnique() => new(PolarsWrapper.ApproxNUnique(CloneHandle()));
    /// <summary>
    /// Compute the product of an expression
    /// </summary>
    /// <returns></returns>
    public Expr Product() => new(PolarsWrapper.Product(CloneHandle()));
    
    // ==========================================
    // Math
    // ==========================================

    /// <summary>
    /// Calculate the absolute value of the expression.
    /// </summary>
    public Expr Abs() => new(PolarsWrapper.Abs(CloneHandle()));

    /// <summary>
    /// Calculate the square root of the expression.
    /// </summary>
    public Expr Sqrt() => new(PolarsWrapper.Sqrt(CloneHandle()));

    /// <summary>
    /// Calculate the cube root of the expression.
    /// </summary>
    public Expr Cbrt() => new(PolarsWrapper.Cbrt(CloneHandle()));

    /// <summary>
    /// Calculate the power of the expression with a given exponent expression.
    /// </summary>
    public Expr Pow(Expr exponent) => new(PolarsWrapper.Pow(CloneHandle(), exponent.CloneHandle()));

    /// <summary>
    /// Calculate the power of the expression with a given numeric exponent.
    /// </summary>
    public Expr Pow(double exponent) => new(PolarsWrapper.Pow(CloneHandle(), PolarsWrapper.Lit(exponent)));
    /// <summary>
    /// Compute the dot/inner product between two expressions.
    /// <para>
    /// The dot product is the sum of the products of the corresponding entries of the two sequences of numbers.
    /// </para>
    /// </summary>
    /// <param name="other">The other expression to compute the dot product with.</param>
    /// <returns>A scalar expression representing the dot product result.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new 
    /// {
    ///     a = new[] { 1, 2, 3 },
    ///     b = new[] { 4, 5, 6 }
    /// });
    /// 
    /// // (1*4) + (2*5) + (3*6) = 4 + 10 + 18 = 32
    /// df.Select(Col("a").Dot(Col("b"))).Show();
    /// </code>
    /// </example>
    public Expr Dot(Expr other) => new(PolarsWrapper.Dot(CloneHandle(), other.CloneHandle()));
    /// <summary>
    /// Calculate the power of the Euler's number.
    /// </summary>
    public Expr Exp() => new(PolarsWrapper.Exp(CloneHandle()));

    /// <summary>
    /// Calculate the ln of Number 
    /// </summary>
    /// <param name="baseVal"></param>
    /// <returns></returns>
    public Expr Ln(double baseVal = Math.E) => new(PolarsWrapper.Log(CloneHandle(), baseVal));

    // ==========================================
    // Trigonometry
    // ==========================================

    /// <summary>Compute the element-wise sine.</summary>
    public Expr Sin() => new(PolarsWrapper.Sin(CloneHandle()));

    /// <summary>Compute the element-wise cosine.</summary>
    public Expr Cos() => new(PolarsWrapper.Cos(CloneHandle()));

    /// <summary>Compute the element-wise tangent.</summary>
    public Expr Tan() => new(PolarsWrapper.Tan(CloneHandle()));

    /// <summary>Compute the element-wise inverse sine.</summary>
    public Expr ArcSin() => new(PolarsWrapper.ArcSin(CloneHandle()));

    /// <summary>Compute the element-wise inverse cosine.</summary>
    public Expr ArcCos() => new(PolarsWrapper.ArcCos(CloneHandle()));

    /// <summary>Compute the element-wise inverse tangent.</summary>
    public Expr ArcTan() => new(PolarsWrapper.ArcTan(CloneHandle()));

    // Hyperbolic
    public Expr Sinh() => new(PolarsWrapper.Sinh(CloneHandle()));
    public Expr Cosh() => new(PolarsWrapper.Cosh(CloneHandle()));
    public Expr Tanh() => new(PolarsWrapper.Tanh(CloneHandle()));

    public Expr ArcSinh() => new(PolarsWrapper.ArcSinh(CloneHandle()));
    public Expr ArcCosh() => new(PolarsWrapper.ArcCosh(CloneHandle()));
    public Expr ArcTanh() => new(PolarsWrapper.ArcTanh(CloneHandle()));

    // ==========================================
    // Rounding & Sign
    // ==========================================
    /// <summary>
    /// Round the number
    /// </summary>
    /// <param name="decimals"></param>
    /// <returns></returns>
    public Expr Round(uint decimals) => new(PolarsWrapper.Round(CloneHandle(), decimals));
    /// <summary>Compute the element-wise sign (-1, 0, 1).</summary>
    public Expr Sign() => new(PolarsWrapper.Sign(CloneHandle()));

    /// <summary>Rounds up to the nearest integer.</summary>
    public Expr Ceil() => new(PolarsWrapper.Ceil(CloneHandle()));

    /// <summary>Rounds down to the nearest integer.</summary>
    public Expr Floor() => new(PolarsWrapper.Floor(CloneHandle()));

    // ==========================================
    // Null Handling
    // ==========================================

    /// <summary>
    /// Fill null values with a specified value.
    /// </summary>
    /// <param name="fillValue">The expression (or literal) to replace nulls with.</param>
    public Expr FillNull(Expr fillValue) => new(PolarsWrapper.FillNull(CloneHandle(), fillValue.CloneHandle()));
    /// <summary>
    /// Fill null values with a specified literal value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Expr FillNull(object value) => FillNull(MakeLit(value));
    /// <summary>
    /// Fill null values with a specific strategy (Forward).
    /// </summary>
    /// <param name="limit">Max number of consecutive nulls to fill. (Default null = infinite)</param>
    public Expr ForwardFill(uint? limit = null) => new(PolarsWrapper.ForwardFill(CloneHandle(), limit ?? 0));
    /// <summary>
    /// Fill null values with a specific strategy (Backward).
    /// </summary>
    public Expr BackwardFill(uint? limit = null) => new(PolarsWrapper.BackwardFill(CloneHandle(), limit ?? 0));
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
    public Expr InterpolateBy(Expr by) => new(PolarsWrapper.InterpolateBy(CloneHandle(), by.CloneHandle()));
    /// <summary>
    /// Evaluate whether the expression is null.
    /// </summary>
    public Expr IsNull() => new(PolarsWrapper.IsNull(CloneHandle()));
    /// <summary>
    /// Evaluate whether the expression is not null.
    /// </summary>
    public Expr IsNotNull() => new(PolarsWrapper.IsNotNull(CloneHandle()));
    /// <summary>
    /// Fill floating point NaN values with a specified value.
    /// Note: This is different from FillNull. It only handles IEEE 754 NaN.
    /// </summary>
    public Expr FillNan(object value) => new(PolarsWrapper.FillNan(CloneHandle(), MakeLit(value).Handle));
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
    // Unique and Duplicated
    // ==========================================
    /// <summary>
    /// Create a boolean expression indicating whether the value is unique.
    /// </summary>
    public Expr IsUnique() => new(PolarsWrapper.ExprIsUnique(CloneHandle()));

    /// <summary>
    /// Create a boolean expression indicating whether the value is duplicated.
    /// </summary>
    public Expr IsDuplicated() => new(PolarsWrapper.ExprIsDuplicated(CloneHandle()));

    /// <summary>
    /// Get unique values.
    /// </summary>
    public Expr Unique() => new(PolarsWrapper.ExprUnique(CloneHandle()));

    /// <summary>
    /// Get unique values, maintaining order.
    /// </summary>
    public Expr UniqueStable() => new(PolarsWrapper.ExprUniqueStable(CloneHandle()));
    // ==========================================
    // Statistical Ops
    // ==========================================

    /// <summary>
    /// Return the number of non-null elements in the column.
    /// </summary>
    public Expr Count() => new(PolarsWrapper.Count(CloneHandle()));
    /// <summary>
    /// Return the number of elements in the column.
    /// Null values count towards the total.
    /// </summary>
    /// <returns></returns>
    public Expr Len() => new(PolarsWrapper.ExprLen(CloneHandle()));

    /// <summary>
    /// Get the standard deviation.
    /// </summary>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
    /// <returns>A series which length is 1</returns>
    public Expr Std(int ddof = 1) => new(PolarsWrapper.Std(CloneHandle(), ddof));

    /// <summary>
    /// Get the variance.
    /// </summary>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
    /// <returns>A series which length is 1</returns>
    public Expr Var(int ddof = 1) => new(PolarsWrapper.Var(CloneHandle(), ddof));

    /// <summary>
    /// Get the median value.
    /// </summary>
    /// <returns>A series which length is 1</returns>
    public Expr Median() => new(PolarsWrapper.Median(CloneHandle()));
    /// <summary>
    /// Get the mode value.
    /// </summary>
    /// <returns>A series which length is 1</returns>
    public Expr Mode() => new(PolarsWrapper.Mode(CloneHandle()));
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
    /// Get the quantile value.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0.</param>
    /// <param name="method">['nearest’, ‘higher’, ‘lower’, ‘midpoint’, ‘linear’] Interpolation method.</param>
    /// <returns>A series which length is 1</returns>
    public Expr Quantile(double quantile, QuantileMethod method = QuantileMethod.Linear)
        => new(PolarsWrapper.Quantile(CloneHandle(), quantile, method.ToNative()));
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
    // Cumulative Functions
    // ==========================================
    /// <summary>
    /// Get an array with the cumulative sum computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    public Expr CumSum(bool reverse = false) => new(PolarsWrapper.CumSum(CloneHandle(), reverse));
    /// <summary>
    /// Get an array with the cumulative max computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    public Expr CumMax(bool reverse = false) => new(PolarsWrapper.CumMax(CloneHandle(), reverse));
    /// <summary>
    /// Get an array with the cumulative min computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    public Expr CumMin(bool reverse = false) => new(PolarsWrapper.CumMin(CloneHandle(), reverse));
    /// <summary>
    /// Get an array with the cumulative prod computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    public Expr CumProd(bool reverse = false) => new(PolarsWrapper.CumProd(CloneHandle(), reverse));
    /// <summary>
    /// Get an array with the cumulative count computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    public Expr CumCount(bool reverse = false) => new(PolarsWrapper.CumCount(CloneHandle(), reverse));
    // ==========================================
    // EWM Functions
    // ==========================================
    /// <summary>
    /// Compute exponentially-weighted moving average.
    /// </summary>
    /// <param name="alpha">
    /// Specify smoothing factor alpha directly. 
    /// <para>Constraint: <c>0 &lt; alpha &lt;= 1</c></para>
    /// </param>
    /// <param name="adjust">
    /// If <c>true</c>, divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings (viewing data as finite history). 
    /// If <c>false</c>, assume infinite history.
    /// </param>
    /// <param name="bias">
    /// If <c>true</c>, use a biased estimator (Standard deviation uses <c>N</c> in denominator). 
    /// If <c>false</c>, use an unbiased estimator (Standard deviation uses <c>N-1</c>).
    /// <para>Note: This is primarily relevant for Variance/StdDev. For Mean, it typically defaults to true.</para>
    /// </param>
    /// <param name="minPeriods">Minimum number of observations in window required to have a value (otherwise result is null).</param>
    /// <param name="ignoreNulls">Ignore missing values when calculating weights.</param>
    /// <returns>A new expression representing the EWM mean.</returns>
    public Expr EwmMean(double alpha, bool adjust = true, bool bias = true, int minPeriods = 1, bool ignoreNulls = false)
        => new(PolarsWrapper.EwmMean(CloneHandle(), alpha, adjust, bias, minPeriods, ignoreNulls));
    /// <summary>
    /// Compute exponentially-weighted moving standard deviation.
    /// </summary>
    /// <inheritdoc cref="EwmMean"/>
    /// <returns>A new expression representing the EWM standard deviation.</returns>
    public Expr EwmStd(double alpha, bool adjust = true, bool bias = true, int minPeriods = 1, bool ignoreNulls = false)
        => new(PolarsWrapper.EwmStd(CloneHandle(), alpha, adjust, bias, minPeriods, ignoreNulls));
    /// <summary>
    /// Compute exponentially-weighted moving variance.
    /// </summary>
    /// <inheritdoc cref="EwmMean"/>
    /// <returns>A new expression representing the EWM variance.</returns>
    public Expr EwmVar(double alpha, bool adjust = true, bool bias = true, int minPeriods = 1, bool ignoreNulls = false)
        => new(PolarsWrapper.EwmVar(CloneHandle(), alpha, adjust, bias, minPeriods, ignoreNulls));
    /// <summary>
    /// Compute exponentially-weighted moving average based on a temporal or index column.
    /// </summary>
    /// <param name="by">
    /// The column used to determine the distance between observations.
    /// <para>Supported data types: <c>Date</c>, <c>DateTime</c>, <c>UInt64</c>, <c>UInt32</c>, <c>Int64</c>, or <c>Int32</c>.</para>
    /// </param>
    /// <param name="halfLife">
    /// The unit over which an observation decays to half its value.
    /// <para>Supported string formats:</para>
    /// <list type="bullet">
    ///     <item><term>Time units</term><description><c>ns</c> (nanosecond), <c>us</c> (microsecond), <c>ms</c> (millisecond), <c>s</c> (second), <c>m</c> (minute), <c>h</c> (hour), <c>d</c> (day), <c>w</c> (week).</description></item>
    ///     <item><term>Index units</term><description><c>i</c> (index count). Example: <c>"2i"</c> means decay by half every 2 index steps.</description></item>
    ///     <item><term>Compound</term><description>Example: <c>"3d12h4m25s"</c>.</description></item>
    /// </list>
    /// <para>
    /// <b>Warning:</b> <paramref name="halfLife"/> is treated as a constant duration. 
    /// Calendar durations such as months (<c>mo</c>) or years (<c>y</c>) are <b>NOT</b> supported because they vary in length. 
    /// Please express such durations in hours (e.g. use <c>'730h'</c> instead of <c>'1mo'</c>).
    /// </para>
    /// </param>
    /// <returns>A new expression representing the time/index-based EWM mean.</returns>
    public Expr EwmMeanBy(Expr by, string halfLife)
        => new(PolarsWrapper.EwmMeanBy(
            CloneHandle(),
            by.CloneHandle(),
            halfLife
        ));
    
    // ==========================================
    // Logic / Comparison
    // ==========================================

    /// <summary>
    /// Check if the value is between lower and upper bounds (inclusive).
    /// </summary>
    public Expr IsBetween(Expr lower, Expr upper)
        => new(PolarsWrapper.IsBetween(CloneHandle(), lower.CloneHandle(), upper.CloneHandle()));
    /// <summary>
    /// Check if the value is in given collection.
    /// </summary>
    public Expr IsIn(Expr other, bool nullsEqual = false)
     => new(PolarsWrapper.IsIn(CloneHandle(),other.CloneHandle(),nullsEqual));
    /// <summary>
    /// Filter a single column.
    /// <br/>
    /// Mostly useful in <c>group_by</c> context or when you want to filter an expression based on another expression within a <c>Select</c> context.
    /// </summary>
    /// <param name="predicate">Boolean expression used to filter the current expression.</param>
    /// <returns>A new expression with filtered values.</returns>
    public Expr Filter(Expr predicate)
        => new(PolarsWrapper.Filter(CloneHandle(),predicate.CloneHandle()));

    // ==========================================
    // Extend Constant
    // ==========================================
    /// <summary>
    /// Extremely fast method for extending the Series with ‘n’ copies of a value.
    /// </summary>
    /// <param name="value">A constant literal value or a unit expression with which to extend the expression result Series; can pass None to extend with nulls.</param>
    /// <param name="n">The number of additional values that will be added.</param>
    /// <returns></returns>
    public Expr ExtendConstant(Expr value,Expr n)
        => new(PolarsWrapper.ExtendConstant(CloneHandle(),value.CloneHandle(),n.CloneHandle()));
    /// <summary>
    /// Extend the column with a constant value (Syntax Sugar).
    /// </summary>
    public Expr ExtendConstant(object value, ulong n)
        => ExtendConstant(MakeLit(value), MakeLit(n));

    /// <summary>
    /// Extend the column with a constant value (Syntax Sugar for int).
    /// </summary>
    public Expr ExtendConstant(object value, int n)
        => ExtendConstant(MakeLit(value), MakeLit(n));
    // ==========================================
    // Casting
    // ==========================================

    /// <summary>
    /// Cast the expression to a different data type.
    /// </summary>
    public Expr Cast(DataType dtype, bool strict = false)
        => new(PolarsWrapper.ExprCast(CloneHandle(), dtype.Handle, strict));

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

    // ==========================================
    // Window & Offset
    // ==========================================
    #region Window & Offset Functions

    /// <summary>
    /// Apply a window function over a subgroup.
    /// <para>
    /// This is similar to SQL's `OVER (PARTITION BY ...)` clause.
    /// Unlike <see cref="DataFrame.GroupBy(Expr[])"/>, this does not reduce the number of rows.
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
    ///     Col("group"),
    ///     Col("val"),
    ///     Col("val").Mean().Over("group").Alias("group_mean"),
    ///     (Col("val") - Col("val").Mean().Over("group")).Alias("diff_from_mean")
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
    public Expr Over(params Expr[] partitionBy)
        => new (PolarsWrapper.Over(CloneHandle(), System.Array.ConvertAll(partitionBy, e => e.CloneHandle())));

    /// <summary>
    /// Window function: Apply aggregation over specific groups.
    /// Example: Col("Amt").Sum().Over("Group", "Date")
    /// </summary>
    public Expr Over(params string[] partitionBy)
        => Over(System.Array.ConvertAll(partitionBy, Pl.Col));
    
    /// <summary>
    /// Shift values by the given number of indices.
    /// Positive values shift downstream, negative values shift upstream.
    /// </summary>
    public Expr Shift(long n = 1) => new(PolarsWrapper.Shift(CloneHandle(), n));

    /// <summary>
    /// Calculate the difference with the previous value (n-th lag).
    /// Null values are propagated.
    /// </summary>
    public Expr Diff(long n = 1) => new(PolarsWrapper.Diff(CloneHandle(), n));

    #endregion

    /// <summary>
    /// Explode a list expression.
    /// <para>
    /// This turns a list column into a long column (flattening).
    /// </para>
    /// <para>
    /// <b>Warning:</b> When used in <see cref="DataFrame.Select(IntoExpr[])"/> with other columns, 
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
    ///     Col("tags").Explode().Alias("tags_flat")
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
    /// Aggregate values into a list.
    /// <para>
    /// This is the opposite of <see cref="Explode"/>. It collects values from multiple rows into a single list.
    /// </para>
    /// </summary>
    /// <example>
    /// <code>
    /// // Collect all IDs and Tags into single lists
    /// df.Select(
    ///     Col("id").Implode().Alias("all_ids"), 
    ///     Col("tags").Implode().Alias("nested_tags")
    /// ).Show();
    /// /* Output:
    /// shape: (1, 2)
    /// ┌───────────┬─────────────────────┐
    /// │ all_ids   ┆ nested_tags         │
    /// │ ---       ┆ ---                 │
    /// │ list[i32] ┆ list[list[str]]     │
    /// ╞═══════════╪═════════════════════╡
    /// │ [1, 2]    ┆ [["a", "b"], ["c"]] │
    /// └───────────┴─────────────────────┘
    /// */
    /// </code>
    /// </example>
    public Expr Implode() => new(PolarsWrapper.Implode(CloneHandle()));
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









