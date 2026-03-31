#pragma warning disable CS1573
using Polars.NET.Core;
using Polars.NET.Core.Helpers;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Lazily join with another LazyFrame.
    /// <para>
    /// Polars will optimize the join execution order. 
    /// Note: Both frames must be LazyFrames.
    /// </para>
    /// </summary>
    /// <seealso cref="DataFrame.Join(DataFrame, Expr[], Expr[], JoinType,string?,JoinValidation,JoinCoalesce,JoinMaintainOrder,JoinSide,bool,long?,ulong)"/>
    /// <example>
    /// <code>
    /// var lf1 = df1.Lazy();
    /// var lf2 = df2.Lazy();
    /// 
    /// // Lazy Left Join
    /// var joined = lf1.Join(lf2, Col("id"), Col("id"), JoinType.Left)
    ///                 .Collect();
    ///                 
    /// /* Output:
    /// shape: (3, 3)
    /// ┌─────┬─────────┬───────┐
    /// │ id  ┆ name    ┆ score │
    /// │ --- ┆ ---     ┆ ---   │
    /// │ i32 ┆ str     ┆ i32   │
    /// ╞═════╪═════════╪═══════╡
    /// │ 1   ┆ Alice   ┆ 90    │
    /// │ 2   ┆ Bob     ┆ 80    │
    /// │ 3   ┆ Charlie ┆ null  │
    /// └─────┴─────────┴───────┘
    /// */
    /// </code>
    /// </example>
    public LazyFrame Join(LazyFrame other,        
        Expr[] leftOn, 
        Expr[] rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        var lOn = leftOn.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var rOn = rightOn.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var lfClone = CloneHandle();
        var otherClone = other.CloneHandle();
        return new LazyFrame(PolarsWrapper.Join(
            lfClone, 
            otherClone, 
            lOn, 
            rOn, 
            how.ToNative(),
            suffix,
            validation.ToNative(),
            coalesce.ToNative(),
            maintainOrder.ToNative(),
            joinSide.ToNative(),
            nullsEqual,
            sliceOffset,
            sliceLen
        ));
    }
    /// <summary>
    /// Join with another LazyFrame using column names.
    /// </summary>
    public LazyFrame Join(LazyFrame other,         
        string[] leftOn, 
        string[] rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        var lExprs = leftOn.Select(Pl.Col).ToArray();
        var rExprs = rightOn.Select(Pl.Col).ToArray();
        return Join(
            other, 
            lExprs, 
            rExprs, 
            how, 
            suffix, 
            validation, 
            coalesce, 
            maintainOrder, 
            joinSide,
            nullsEqual, 
            sliceOffset, 
            sliceLen
        );
    }

    /// <summary>
    /// Join with another LazyFrame using a single column pair.
    /// </summary>
    public LazyFrame Join(LazyFrame other,
        string leftOn, 
        string rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        return Join(
            other, 
            [leftOn], 
            [rightOn], 
            how, 
            suffix, 
            validation, 
            coalesce, 
            maintainOrder, 
            joinSide,
            nullsEqual, 
            sliceOffset, 
            sliceLen
        );
    }

    /// <summary>
    /// Perform an As-of join (also known as a time-series join).
    /// <para>
    /// This is similar to a left join except that we match on nearest key rather than equal keys.
    /// The join keys must be sorted.
    /// </para>
    /// </summary>
    /// <param name="other">The right LazyFrame to join with.</param>
    /// <param name="leftOn">Join key of the left LazyFrame. Must be sorted.</param>
    /// <param name="rightOn">Join key of the right LazyFrame. Must be sorted.</param>
    /// <param name="toleranceStr">
    /// Tolerance as a time duration string (e.g., "2h", "10s", "1d"). 
    /// Matches that are further away than this duration are discarded.
    /// </param>
    /// <param name="toleranceInt">
    /// Tolerance as a numeric integer (e.g., for integer-based timestamps or simple counters).
    /// </param>
    /// <param name="toleranceFloat">
    /// Tolerance as a floating point number.
    /// </param>
    /// <param name="strategy">
    /// The strategy to determine which value is "nearest" (Backward, Forward, or Nearest).
    /// Defaults to <see cref="AsofStrategy.Backward"/>.
    /// </param>
    /// <param name="leftBy">
    /// Columns to match exactly (equivalence join) before performing the as-of join. 
    /// Useful for joining separate time-series per group (e.g., by "Symbol").
    /// </param>
    /// <param name="rightBy">
    /// Columns to match exactly in the right DataFrame.
    /// </param>
    /// <param name="allowEq">
    /// If true, allow exact matches to be included in the result. 
    /// If false, a match must be strictly unequal (e.g. less than for Backward strategy) to the key.
    /// </param>
    /// <param name="checkSorted">
    /// Check if the join keys are sorted. 
    /// If false, the user must ensure keys are sorted; otherwise results are undefined (but execution is faster).
    /// </param>
    /// <param name="suffix">Suffix to append to columns with name conflicts. Defaults to "_right".</param>
    /// <param name="validation">Check if join keys are unique (mostly relevant for the 'by' columns).</param>
    /// <param name="coalesce">How to coalesce the join keys.</param>
    /// <param name="maintainOrder">How to maintain the order of the join.</param>
    /// <param name="joinSide">pecifies the strategy for the hash join build side.</param>
    /// <param name="nullsEqual">Consider nulls as equal.</param>
    /// <param name="sliceOffset">Slice the result starting at this offset (optimization).</param>
    /// <param name="sliceLen">Length of the slice to keep.</param>
    /// <example>
    /// <code>
    /// // Trades: Events happening at specific times
    /// var trades = DataFrame.FromColumns(new
    /// {
    ///     time = new[] { 10, 20, 30 },
    ///     stock = new[] { "A", "A", "A" }
    /// }).Lazy();
    /// 
    /// // Quotes: Price updates (irregular intervals)
    /// // 9->100, 15->101, 25->102, 40->103
    /// var quotes = DataFrame.FromColumns(new
    /// {
    ///     time = new[] { 9, 15, 25, 40 },
    ///     bid = new[] { 100, 101, 102, 103 }
    /// }).Lazy();
    /// 
    /// // Find the latest quote BEFORE or AT the trade time
    /// var asof = trades.JoinAsOf(
    ///     quotes, 
    ///     leftOn: Col("time"), 
    ///     rightOn: Col("time"),
    ///     strategy: AsofStrategy.Backward
    /// );
    /// 
    /// var df = asof.Collect();
    /// df.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌──────┬───────┬─────┐
    /// │ time ┆ stock ┆ bid │
    /// │ ---  ┆ ---   ┆ --- │
    /// │ i32  ┆ str   ┆ i32 │
    /// ╞══════╪═══════╪═════╡
    /// │ 10   ┆ A     ┆ 100 │ // Matches time 9
    /// │ 20   ┆ A     ┆ 101 │ // Matches time 15
    /// │ 30   ┆ A     ┆ 102 │ // Matches time 25
    /// └──────┴───────┴─────┘
    /// */
    /// </code>
    /// </example>
    internal LazyFrame JoinAsOf(
        LazyFrame other, 
        Expr leftOn, Expr rightOn, 
        string? toleranceStr = null,
        long? toleranceInt = null,
        double? toleranceFloat = null,
        AsofStrategy strategy = AsofStrategy.Backward,
        Expr[]? leftBy = null,
        Expr[]? rightBy = null,
        bool allowEq = true,
        bool checkSorted = true,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        var lfClone = CloneHandle();
        var otherClone = other.CloneHandle();
        
        var lOn = PolarsWrapper.CloneExpr(leftOn.Handle);
        var rOn = PolarsWrapper.CloneExpr(rightOn.Handle);
        
        var lBy = leftBy?.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var rBy = rightBy?.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();

        return new LazyFrame(PolarsWrapper.JoinAsOf(
            lfClone, otherClone,
            [lOn], [rOn], // Wrap single Expr into array
            lBy, rBy,
            strategy.ToNative(),
            toleranceStr,
            toleranceInt,
            toleranceFloat,
            allowEq,
            checkSorted,
            suffix,
            validation.ToNative(),
            coalesce.ToNative(),
            maintainOrder.ToNative(),
            joinSide.ToNative(),
            nullsEqual,
            sliceOffset,
            sliceLen
        ));
    }
    // 1. String Tolerance
    /// <inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide ,bool, long?, ulong)"/>
    /// <param name="tolerance">
    /// Tolerance as a time duration string (e.g., "2h", "10s", "1d"). 
    /// Matches that are further away than this duration are discarded.
    /// </param>
    public LazyFrame JoinAsOf(LazyFrame other, Expr leftOn, Expr rightOn, string tolerance, AsofStrategy strategy = AsofStrategy.Backward, Expr[]? leftBy = null, Expr[]? rightBy = null)
        => JoinAsOf(other, leftOn, rightOn, toleranceStr: tolerance, strategy: strategy, leftBy: leftBy, rightBy: rightBy);

    // 2. TimeSpan Tolerance
    /// <inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder, JoinSide,bool, long?, ulong)"/>
    /// <param name="tolerance">
    /// Tolerance as a <see cref="TimeSpan"/>. 
    /// Matches that are further away than this duration are discarded.
    /// </param>
    public LazyFrame JoinAsOf(LazyFrame other, Expr leftOn, Expr rightOn, TimeSpan tolerance, AsofStrategy strategy = AsofStrategy.Backward, Expr[]? leftBy = null, Expr[]? rightBy = null)
        => JoinAsOf(other, leftOn, rightOn, toleranceStr: DurationFormatter.ToPolarsString(tolerance), strategy: strategy, leftBy: leftBy, rightBy: rightBy);

    // 3. Int Tolerance (e.g. integer timestamps)
    /// <inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide, bool, long?, ulong)"/>
    /// <param name="tolerance">
    /// Tolerance as a numeric integer (e.g., for integer-based timestamps or simple counters).
    /// </param>
    public LazyFrame JoinAsOf(LazyFrame other, Expr leftOn, Expr rightOn, long tolerance, AsofStrategy strategy = AsofStrategy.Backward, Expr[]? leftBy = null, Expr[]? rightBy = null)
        => JoinAsOf(other, leftOn, rightOn, toleranceInt: tolerance, strategy: strategy, leftBy: leftBy, rightBy: rightBy);

    // 4. Double Tolerance (e.g. float keys)
    /// <inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide, bool, long?, ulong)"/>
    /// <param name="tolerance">
    /// Tolerance as a floating point number.
    /// </param>
    public LazyFrame JoinAsOf(LazyFrame other, Expr leftOn, Expr rightOn, double tolerance, AsofStrategy strategy = AsofStrategy.Backward, Expr[]? leftBy = null, Expr[]? rightBy = null)
        => JoinAsOf(other, leftOn, rightOn, toleranceFloat: tolerance, strategy: strategy, leftBy: leftBy, rightBy: rightBy);
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
       /// <summary>
    /// Join with another DataFrame using expression expressions.
    /// </summary>
    /// <param name="other">DataFrame to join with.</param>
    /// <param name="leftOn">Left keys.</param>
    /// <param name="rightOn">Right keys.</param>
    /// <param name="how">Join type (Inner, Left, etc.).</param>
    /// <param name="suffix">Suffix to append to columns with same name in right DataFrame. Default "_right".</param>
    /// <param name="validation">Check if join keys are unique.</param>
    /// <param name="coalesce">How to coalesce the join keys.</param>
    /// <param name="maintainOrder">How to maintain the order of the join.</param>
    /// <param name="joinSide">Specifies the strategy for the hash join build side.</param>
    /// <param name="nullsEqual">Consider nulls as equal.</param>
    /// <param name="sliceOffset">Slice the result starting at this offset.</param>
    /// <param name="sliceLen">Length of the slice.</param>
    public DataFrame Join(
        DataFrame other, 
        Expr[] leftOn, 
        Expr[] rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        var lf = Lazy().Join(
            other.Lazy(),
            leftOn,
            rightOn,
            how,
            suffix,
            validation,
            coalesce,
            maintainOrder,
            joinSide,
            nullsEqual,
            sliceOffset,
            sliceLen
        );

        return lf.Collect();
    }
    /// <summary>
    /// Join with another DataFrame using column names.
    /// </summary>
    /// <param name="other">The right DataFrame to join with.</param>
    /// <param name="leftOn">Column names in the left DataFrame to join on.</param>
    /// <param name="rightOn">Column names in the right DataFrame to join on.</param>
    /// <param name="how">Type of join (Inner, Left, Outer, Cross, etc.). Default is Inner.</param>
    /// <param name="suffix">Suffix to append to columns with same name in right DataFrame. Default "_right".</param>
    /// <param name="validation">Check if join keys are unique.</param>
    /// <param name="coalesce">How to coalesce the join keys.</param>
    /// <param name="maintainOrder">How to maintain the order of the join.</param>
    /// <param name="joinSide">Specifies the strategy for the hash join build side.</param>
    /// <param name="nullsEqual">Consider nulls as equal.</param>
    /// <param name="sliceOffset">Slice the result starting at this offset.</param>
    /// <param name="sliceLen">Length of the slice.</param>
    /// <returns>A new DataFrame resulting from the join.</returns>
    /// <example>
    /// <code>
    /// var dfCustomers = DataFrame.FromColumns(new
    /// {
    ///     id = new[] { 1, 2, 3 },
    ///     name = new[] { "Alice", "Bob", "Charlie" }
    /// });
    /// 
    /// var dfOrders = DataFrame.FromColumns(new
    /// {
    ///     id = new[] { 1, 2, 4 },
    ///     amount = new[] { 100, 200, 500 }
    /// });
    /// 
    /// // Perform an Inner Join on the "id" column
    /// var joined = dfCustomers.Join(
    ///     dfOrders, 
    ///     leftOn: new[] { "id" }, 
    ///     rightOn: new[] { "id" }, 
    ///     how: JoinType.Inner
    /// );
    /// 
    /// joined.Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌─────┬───────┬────────┐
    /// │ id  ┆ name  ┆ amount │
    /// │ --- ┆ ---   ┆ ---    │
    /// │ i32 ┆ str   ┆ i32    │
    /// ╞═════╪═══════╪════════╡
    /// │ 1   ┆ Alice ┆ 100    │
    /// │ 2   ┆ Bob   ┆ 200    │
    /// └─────┴───────┴────────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Join(
        DataFrame other, 
        string[] leftOn, 
        string[] rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        var lExprs = leftOn.Select(Pl.Col).ToArray();
        var rExprs = rightOn.Select(Pl.Col).ToArray();
        return Join(
            other, 
            lExprs, 
            rExprs, 
            how, 
            suffix, 
            validation, 
            coalesce, 
            maintainOrder, 
            joinSide,
            nullsEqual, 
            sliceOffset, 
            sliceLen
        );
    }

    /// <summary>
    /// Join with another DataFrame using a single column pair (Convenience overload).
    /// </summary>
    /// <param name="other">The right DataFrame to join with.</param>
    /// <param name="leftOn">The column name in the left DataFrame.</param>
    /// <param name="rightOn">The column name in the right DataFrame.</param>
    /// <param name="how">Type of join. Default is Inner.</param>
    /// <param name="suffix">Suffix to append to columns with same name in right DataFrame. Default "_right".</param>
    /// <param name="validation">Check if join keys are unique.</param>
    /// <param name="coalesce">How to coalesce the join keys.</param>
    /// <param name="maintainOrder">How to maintain the order of the join.</param>
    /// <param name="joinSide">Specifies the strategy for the hash join build side.</param>
    /// <param name="nullsEqual">Consider nulls as equal.</param>
    /// <param name="sliceOffset">Slice the result starting at this offset.</param>
    /// <param name="sliceLen">Length of the slice.</param>
    /// <seealso cref="Join(DataFrame, string[], string[], JoinType,string?,JoinValidation,JoinCoalesce,JoinMaintainOrder,JoinSide,bool,long?,ulong)"/>
    public DataFrame Join(
        DataFrame other, 
        string leftOn, 
        string rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        return Join(
            other, 
            [leftOn], 
            [rightOn], 
            how, 
            suffix, 
            validation, 
            coalesce, 
            maintainOrder, 
            joinSide,
            nullsEqual, 
            sliceOffset, 
            sliceLen
        );
    }
    /// <inheritdoc cref="LazyFrame.JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide, bool, long?, ulong)"/>
    /// <example>
    /// <code>
    /// // Trades: Events happening at specific times
    /// var trades = DataFrame.FromColumns(new
    /// {
    ///     time = new[] { 10, 20, 30 },
    ///     stock = new[] { "A", "A", "A" }
    /// });
    /// 
    /// // Quotes: Price updates (irregular intervals)
    /// // 9->100, 15->101, 25->102, 40->103
    /// var quotes = DataFrame.FromColumns(new
    /// {
    ///     time = new[] { 9, 15, 25, 40 },
    ///     bid = new[] { 100, 101, 102, 103 }
    /// });
    /// 
    /// // Find the latest quote BEFORE or AT the trade time
    /// var asof = trades.JoinAsOf(
    ///     quotes, 
    ///     leftOn: Col("time"), 
    ///     rightOn: Col("time"),
    ///     strategy: AsofStrategy.Backward
    /// );
    /// 
    /// asof.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌──────┬───────┬─────┐
    /// │ time ┆ stock ┆ bid │
    /// │ ---  ┆ ---   ┆ --- │
    /// │ i32  ┆ str   ┆ i32 │
    /// ╞══════╪═══════╪═════╡
    /// │ 10   ┆ A     ┆ 100 │ // Matches time 9
    /// │ 20   ┆ A     ┆ 101 │ // Matches time 15
    /// │ 30   ┆ A     ┆ 102 │ // Matches time 25
    /// └──────┴───────┴─────┘
    /// */
    /// </code>
    /// </example>
    internal DataFrame JoinAsOf(
        DataFrame other, 
        Expr leftOn, Expr rightOn, 
        string? toleranceStr = null,
        long? toleranceInt = null,
        double? toleranceFloat = null,
        AsofStrategy strategy = AsofStrategy.Backward,
        Expr[]? leftBy = null,
        Expr[]? rightBy = null,
        bool allowEq = true,
        bool checkSorted = true,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        return this.Lazy().JoinAsOf(
            other.Lazy(),
            leftOn, rightOn,
            toleranceStr,
            toleranceInt,
            toleranceFloat,
            strategy,
            leftBy,
            rightBy,
            allowEq,
            checkSorted,
            suffix,
            validation,
            coalesce,
            maintainOrder,
            joinSide,
            nullsEqual,
            sliceOffset,
            sliceLen
        ).Collect();
    }

    /// <inheritdoc cref="JoinAsOf(DataFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide, bool, long?, ulong)"/>
    public DataFrame JoinAsOf(
        DataFrame other, 
        Expr leftOn, Expr rightOn, 
        string tolerance,
        AsofStrategy strategy = AsofStrategy.Backward,
        Expr[]? leftBy = null,
        Expr[]? rightBy = null,
        bool allowEq = true,
        bool checkSorted = true,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        return JoinAsOf(
            other,
            leftOn,
            rightOn,
            tolerance,
            null,null,
            strategy,leftBy,rightBy,allowEq,checkSorted,suffix,
            validation,coalesce,maintainOrder,joinSide,nullsEqual,sliceOffset,sliceLen
        );
    }

    /// <inheritdoc cref="JoinAsOf(DataFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide, bool, long?, ulong)"/>
    public DataFrame JoinAsOf(
        DataFrame other, 
        Expr leftOn, Expr rightOn, 
        TimeSpan tolerance,
        AsofStrategy strategy = AsofStrategy.Backward,
        Expr[]? leftBy = null,
        Expr[]? rightBy = null,
        bool allowEq = true,
        bool checkSorted = true,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        return JoinAsOf(
            other,
            leftOn,
            rightOn,
            DurationFormatter.ToPolarsString(tolerance),
            null,null,
            strategy,leftBy,rightBy,allowEq,checkSorted,suffix,
            validation,coalesce,maintainOrder,joinSide,nullsEqual,sliceOffset,sliceLen
        );
    }
    /// <inheritdoc cref="JoinAsOf(DataFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide, bool, long?, ulong)"/>
    public DataFrame JoinAsOf(
        DataFrame other, 
        Expr leftOn, Expr rightOn, 
        int tolerance,
        AsofStrategy strategy = AsofStrategy.Backward,
        Expr[]? leftBy = null,
        Expr[]? rightBy = null,
        bool allowEq = true,
        bool checkSorted = true,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        return JoinAsOf(
            other,
            leftOn,
            rightOn,
            null,
            tolerance,null,
            strategy,leftBy,rightBy,allowEq,checkSorted,suffix,
            validation,coalesce,maintainOrder,joinSide,nullsEqual,sliceOffset,sliceLen
        );
    }
    /// <inheritdoc cref="JoinAsOf(DataFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide, bool, long?, ulong)"/>
    public DataFrame JoinAsOf(
        DataFrame other, 
        Expr leftOn, Expr rightOn, 
        double tolerance,
        AsofStrategy strategy = AsofStrategy.Backward,
        Expr[]? leftBy = null,
        Expr[]? rightBy = null,
        bool allowEq = true,
        bool checkSorted = true,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        return JoinAsOf(
            other,
            leftOn,
            rightOn,
            null,
            null,tolerance,
            strategy,leftBy,rightBy,allowEq,checkSorted,suffix,
            validation,coalesce,maintainOrder,joinSide,nullsEqual,sliceOffset,sliceLen
        );
    }
}