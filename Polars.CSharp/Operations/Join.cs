#pragma warning disable CS1591
using Polars.NET.Core;
using Polars.NET.Core.Helpers;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

/// <summary>
/// Smart tolerance type for AsOf joins. 
/// Implicitly converts from string ("2h"), TimeSpan, int, long, or double.
/// </summary>
public readonly struct AsOfTolerance
{
    internal readonly string? StringVal;
    internal readonly long? IntVal;
    internal readonly double? FloatVal;

    private AsOfTolerance(string? s, long? i, double? d)
    {
        StringVal = s; IntVal = i; FloatVal = d;
    }

    public static implicit operator AsOfTolerance(string val) => new(val, null, null);
    public static implicit operator AsOfTolerance(TimeSpan val) => new(DurationFormatter.ToPolarsString(val), null, null);
    public static implicit operator AsOfTolerance(long val) => new(null, val, null);
    public static implicit operator AsOfTolerance(int val) => new(null, val, null);
    public static implicit operator AsOfTolerance(double val) => new(null, null, val);
}

public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Lazily join with another LazyFrame.
    /// <para>
    /// Polars will optimize the join execution order. 
    /// Note: Both frames must be LazyFrames.
    /// </para>
    /// </summary>
    /// <example>
    /// <code>
    /// var lf1 = df1.Lazy();
    /// var lf2 = df2.Lazy();
    /// 
    /// // Lazy Left Join
    /// var joined = lf1.Join(lf2, Col("id"), JoinType.Left)
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
    public LazyFrame Join(
        LazyFrame other,
        IntoExprColumn on,
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
        using var safeOn = on.Consume();
        
        var lOn = new[] { PolarsWrapper.CloneExpr(safeOn.Handle) };
        var rOn = new[] { PolarsWrapper.CloneExpr(safeOn.Handle) };

        return new LazyFrame(PolarsWrapper.Join(
            CloneHandle(), other.CloneHandle(), lOn, rOn, 
            how.ToNative(), suffix, validation.ToNative(), coalesce.ToNative(), 
            maintainOrder.ToNative(), joinSide.ToNative(), nullsEqual, sliceOffset, sliceLen
        ));
    }
    /// <summary>
    /// Join using specific left and right columns (Strings or Exprs).
    /// Usage: lf.Join(other, leftOn: "userId", rightOn: Pl.Col("id"))
    /// </summary>
    public LazyFrame Join(
        LazyFrame other,
        IntoExprColumn leftOn,
        IntoExprColumn rightOn,
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
        using var safeLeft = leftOn.Consume();
        using var safeRight = rightOn.Consume();

        var lOn = new[] { PolarsWrapper.CloneExpr(safeLeft.Handle) };
        var rOn = new[] { PolarsWrapper.CloneExpr(safeRight.Handle) };

        return new LazyFrame(PolarsWrapper.Join(
            CloneHandle(), other.CloneHandle(), lOn, rOn, 
            how.ToNative(), suffix, validation.ToNative(), coalesce.ToNative(), 
            maintainOrder.ToNative(), joinSide.ToNative(), nullsEqual, sliceOffset, sliceLen
        ));
    }

    /// <summary>
    /// Join using multiple shared column names.
    /// Usage: lf.Join(other, on: ["date", "region"])
    /// </summary>
    public LazyFrame Join(
        LazyFrame other,
        IEnumerable<string> on,
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
        var cols = on as string[] ?? [.. on];
        var lOn = new ExprHandle[cols.Length];
        var rOn = new ExprHandle[cols.Length];

        for (int i = 0; i < cols.Length; i++)
        {
            var handle = Pl.Col(cols[i]).Handle;
            lOn[i] = PolarsWrapper.CloneExpr(handle);
            rOn[i] = PolarsWrapper.CloneExpr(handle);
        }

        return new LazyFrame(PolarsWrapper.Join(
            CloneHandle(), other.CloneHandle(), lOn, rOn, 
            how.ToNative(), suffix, validation.ToNative(), coalesce.ToNative(), 
            maintainOrder.ToNative(), joinSide.ToNative(), nullsEqual, sliceOffset, sliceLen
        ));
    }
    /// <summary>
    /// Join using specific multiple expressions or column names.
    /// Usage: lf.Join(other, leftOn: ["lid", "ldate"], rightOn: ["rid", "rdate"])
    /// </summary>
    public LazyFrame Join(
        LazyFrame other,
        IEnumerable<IntoExprColumn> leftOn,
        IEnumerable<IntoExprColumn> rightOn,
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
        var lArr = leftOn as IntoExprColumn[] ?? [.. leftOn];
        var rArr = rightOn as IntoExprColumn[] ?? [.. rightOn];

        var lOn = new ExprHandle[lArr.Length];
        for (int i = 0; i < lArr.Length; i++)
        {
            using var safeExpr = lArr[i].Consume();
            lOn[i] = PolarsWrapper.CloneExpr(safeExpr.Handle);
        }

        var rOn = new ExprHandle[rArr.Length];
        for (int i = 0; i < rArr.Length; i++)
        {
            using var safeExpr = rArr[i].Consume();
            rOn[i] = PolarsWrapper.CloneExpr(safeExpr.Handle);
        }

        return new LazyFrame(PolarsWrapper.Join(
            CloneHandle(), other.CloneHandle(), lOn, rOn, 
            how.ToNative(), suffix, validation.ToNative(), coalesce.ToNative(), 
            maintainOrder.ToNative(), joinSide.ToNative(), nullsEqual, sliceOffset, sliceLen
        ));
    }
    /// <summary>
    /// Join using multiple shared expressions (or mixed strings/exprs via IntoExpr).
    /// Usage: lf.Join(other, on: [Pl.Col("date"), Pl.Col("region")])
    /// </summary>
    public LazyFrame Join(
        LazyFrame other,
        IEnumerable<IntoExprColumn> on,
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
        var arr = on as IntoExprColumn[] ?? [.. on];

        var lOn = new ExprHandle[arr.Length];
        var rOn = new ExprHandle[arr.Length];

        for (int i = 0; i < arr.Length; i++)
        {
            using var safeExpr = arr[i].Consume();
            
            lOn[i] = PolarsWrapper.CloneExpr(safeExpr.Handle);
            rOn[i] = PolarsWrapper.CloneExpr(safeExpr.Handle);
        }

        return new LazyFrame(PolarsWrapper.Join(
            CloneHandle(), other.CloneHandle(), lOn, rOn, 
            how.ToNative(), suffix, validation.ToNative(), coalesce.ToNative(), 
            maintainOrder.ToNative(), joinSide.ToNative(), nullsEqual, sliceOffset, sliceLen
        ));
    }
    /// <summary>
    /// Join using specific multiple expressions. (C# Array Covariance Helper)
    /// Usage: lf.Join(other, leftOn: new[] { Pl.Col("A") }, rightOn: new[] { Pl.Col("B") })
    /// </summary>
    public LazyFrame Join(
        LazyFrame other,
        IEnumerable<Expr> leftOn,
        IEnumerable<Expr> rightOn,
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
        var lArr = leftOn.Select(e => (IntoExprColumn)e).ToArray();
        var rArr = rightOn.Select(e => (IntoExprColumn)e).ToArray();

        return Join(other, lArr, rArr, how, suffix, validation, coalesce, maintainOrder, joinSide, nullsEqual, sliceOffset, sliceLen);
    }

    /// <summary>
    /// Join using multiple shared expressions. (C# Array Covariance Helper)
    /// Usage: lf.Join(other, on: new[] { Pl.Col("A"), Pl.Col("B") })
    /// </summary>
    public LazyFrame Join(
        LazyFrame other,
        IEnumerable<Expr> on,
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
        var onArr = on.Select(e => (IntoExprColumn)e).ToArray();

        return Join(other, onArr, how, suffix, validation, coalesce, maintainOrder, joinSide, nullsEqual, sliceOffset, sliceLen);
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
    /// <param name="tolerance">
    /// Tolerance as a time duration string (e.g., "2h", "10s", "1d"), int or double or TimeSpan. 
    /// Matches that are further away than this duration are discarded.
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
    ///     leftOn: Pl.Col("time"), 
    ///     rightOn: Pl.Col("time"),
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
    internal LazyFrame JoinAsOfInternal(
        LazyFrame other,
        IntoExprColumn leftOn, IntoExprColumn rightOn,
        IEnumerable<string>? leftBy, IEnumerable<string>? rightBy,
        AsOfTolerance? tolerance, AsofStrategy strategy,
        bool allowEq, bool checkSorted, string? suffix,
        JoinValidation validation, JoinCoalesce coalesce, JoinMaintainOrder maintainOrder,
        JoinSide joinSide, bool nullsEqual, long? sliceOffset, ulong sliceLen)
    {
        using var safeLeftOn = leftOn.Consume();
        using var safeRightOn = rightOn.Consume();
        var lOnHandles = new[] { PolarsWrapper.CloneExpr(safeLeftOn.Handle) };
        var rOnHandles = new[] { PolarsWrapper.CloneExpr(safeRightOn.Handle) };

        var lByArr = leftBy as string[] ?? leftBy?.ToArray();
        ExprHandle[]? lByHandles = null;
        if (lByArr != null)
        {
            lByHandles = new ExprHandle[lByArr.Length];
            for (int i = 0; i < lByArr.Length; i++)
            {
                using var safeExpr = ((IntoExprColumn)lByArr[i]).Consume();
                lByHandles[i] = PolarsWrapper.CloneExpr(safeExpr.Handle);
            }
        }

        var rByArr = rightBy as string[] ?? rightBy?.ToArray();
        ExprHandle[]? rByHandles = null;
        if (rByArr != null)
        {
            rByHandles = new ExprHandle[rByArr.Length];
            for (int i = 0; i < rByArr.Length; i++)
            {
                using var safeExpr = ((IntoExprColumn)rByArr[i]).Consume();
                rByHandles[i] = PolarsWrapper.CloneExpr(safeExpr.Handle);
            }
        }

        var h = PolarsWrapper.JoinAsOf(
            CloneHandle(), other.CloneHandle(),
            lOnHandles, rOnHandles, lByHandles, rByHandles,
            strategy.ToNative(),
            tolerance?.StringVal, tolerance?.IntVal, tolerance?.FloatVal,
            allowEq, checkSorted, suffix,
            validation.ToNative(), coalesce.ToNative(), maintainOrder.ToNative(),
            joinSide.ToNative(), nullsEqual, sliceOffset, sliceLen
        );

        return new LazyFrame(h);
    }
    /// <summary>
    /// Join AsOf using a shared 'on' column and optional shared 'by' columns.
    /// Usage: lf.JoinAsOf(other, on: "time", tolerance: "2h", by: ["ticker"])
    /// </summary>
    /// <inheritdoc cref="LazyFrame.JoinAsOfInternal(LazyFrame, IntoExprColumn, IntoExprColumn, IEnumerable{string}?, IEnumerable{string}?, AsOfTolerance?, AsofStrategy, bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder, JoinSide, bool, long?, ulong)"/>
    public LazyFrame JoinAsOf(
        LazyFrame other,
        IntoExprColumn on,
        AsOfTolerance? tolerance = null,
        IEnumerable<string>? by = null,
        AsofStrategy strategy = AsofStrategy.Backward,
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
        return JoinAsOfInternal(
            other, on, on, by, by, tolerance, strategy,
            allowEq, checkSorted, suffix, validation, coalesce, 
            maintainOrder, joinSide, nullsEqual, sliceOffset, sliceLen
        );
    }

    /// <summary>
    /// Join AsOf using independent 'leftOn'/'rightOn' and 'leftBy'/'rightBy' columns.
    /// Usage: lf.JoinAsOf(other, leftOn: "time_L", rightOn: "time_R", tolerance: TimeSpan.FromMinutes(5))
    /// </summary>
    /// <inheritdoc cref="LazyFrame.JoinAsOfInternal(LazyFrame, IntoExprColumn, IntoExprColumn, IEnumerable{string}?, IEnumerable{string}?, AsOfTolerance?, AsofStrategy, bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder, JoinSide, bool, long?, ulong)"/>
    public LazyFrame JoinAsOf(
        LazyFrame other,
        IntoExprColumn leftOn,
        IntoExprColumn rightOn,
        AsOfTolerance? tolerance = null,
        IEnumerable<string>? leftBy = null,
        IEnumerable<string>? rightBy = null,
        AsofStrategy strategy = AsofStrategy.Backward,
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
        return JoinAsOfInternal(
            other, leftOn, rightOn, leftBy, rightBy, tolerance, strategy,
            allowEq, checkSorted, suffix, validation, coalesce, 
            maintainOrder, joinSide, nullsEqual, sliceOffset, sliceLen
        );
    }
    /// <summary>
    /// Join with another LazyFrame using multiple boolean expressions.
    /// Usage: lf.JoinWhere(other, [Pl.Col("start") &gt; Pl.Col("end"), Pl.Col("id") != null], how: JoinType.Left)
    /// </summary>
    public LazyFrame JoinWhere(
        LazyFrame other,
        IEnumerable<IntoExprColumn> predicates,
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        bool nullsEqual = false)
    {
        var predArr = predicates as IntoExprColumn[] ?? [.. predicates];
        if (predArr.Length == 0)
            throw new ArgumentException("At least one predicate must be provided for JoinWhere.", nameof(predicates));

        var handles = new ExprHandle[predArr.Length];
        for (int i = 0; i < predArr.Length; i++)
        {
            using var safeExpr = predArr[i].Consume();
            handles[i] = PolarsWrapper.CloneExpr(safeExpr.Handle);
        }

        var h = PolarsWrapper.JoinWhere(
            CloneHandle(),
            other.CloneHandle(),
            handles,
            how.ToNative(),
            suffix,
            validation.ToNative(),
            coalesce.ToNative(),
            maintainOrder.ToNative(),
            nullsEqual
        );

        return new LazyFrame(h);
    }

    /// <summary>
    /// Join using a single condition (Most common use case).
    /// Usage: lf.JoinWhere(other, Pl.Col("A") &gt; Pl.Col("B"), how: JoinType.Inner)
    /// </summary>
    public LazyFrame JoinWhere(
        LazyFrame other,
        IntoExprColumn predicate,
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        bool nullsEqual = false)
    {
        return JoinWhere(
            other, 
            [predicate], 
            how, suffix, validation, coalesce, maintainOrder, nullsEqual
        );
    }

    /// <summary>
    /// Note: You cannot use optional parameters (like 'how' or 'suffix') with this overload. Use collection expressions `[...]` instead.
    /// Usage: lf.JoinWhere(other, Pl.Col("A") &gt; Pl.Col("B"), Pl.Col("C") > 0)
    /// </summary>
    public LazyFrame JoinWhere(LazyFrame other, params IntoExprColumn[] predicates)
    {
        if (predicates.Length == 0) return this;
        
        return JoinWhere(other, (IEnumerable<IntoExprColumn>)predicates);
    }
    /// <summary>
    /// Take two sorted DataFrames and merge them by the sorted key.
    /// The output of this operation will also be sorted. 
    /// It is the callers responsibility that the frames are sorted in ascending order by the key, 
    /// with null keys at the end, otherwise the order of the output will not make sense.
    /// The schemas of both LazyFrames must be equal.
    /// </summary>
    /// <param name="other">Other DataFrame that must be merged</param>
    /// <param name="key">Key that is sorted.</param>
    /// <param name="maintainOrder">If True, the output is guaranteed to have left-biased ordering for equal keys: 
    /// rows from the left frame appear before rows from the right frame when their keys are equal.</param>
    public LazyFrame MergeSorted(LazyFrame other, string key,bool maintainOrder=false)
        => new(PolarsWrapper.MergeSorted(CloneHandle(),other.CloneHandle(),key,maintainOrder));

}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Join with another DataFrame using expression expressions.
    /// </summary>
    /// <param name="other">DataFrame to join with.</param>
    /// <param name="on">Join Key</param>
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
        IntoExprColumn on,
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
        using var right = other.Lazy();
        var lf = Lazy().Join(
            right,
            on,
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
        IntoExprColumn leftOn,
        IntoExprColumn rightOn,
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
        using var right = other.Lazy();
        return Lazy().Join(
            right, 
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
        ).Collect();
    }
    /// <summary>
    /// Join using specific multiple expressions or column names.
    /// Usage: lf.Join(other, leftOn: ["lid", "ldate"], rightOn: ["rid", "rdate"])
    /// </summary>
    public DataFrame Join(
        DataFrame other,
        IEnumerable<IntoExprColumn> leftOn,
        IEnumerable<IntoExprColumn> rightOn,
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
        using var right = other.Lazy();
        return Lazy().Join(
            right, 
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
        ).Collect();
    }

    /// <summary>
    /// Join with another DataFrame using a single column pair (Convenience overload).
    /// </summary>
    /// <param name="other">The right DataFrame to join with.</param>
    /// <param name="on">The column names as join keys .</param>
    /// <param name="how">Type of join. Default is Inner.</param>
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
        IEnumerable<string> on,
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
        using var right = other.Lazy();
        return Lazy().Join(
            right, 
            on,
            how, 
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
    public DataFrame Join(
        DataFrame other,
        IEnumerable<IntoExprColumn> on,
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
        using var right = other.Lazy();
        return Lazy().Join(
            right, 
            on,
            how, 
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
    /// <summary>
    /// Join using specific multiple expressions. (C# Array Covariance Helper)
    /// Usage: lf.Join(other, leftOn: [Pl.Col("A")], rightOn: [Pl.Col("B")])
    /// </summary>
    public DataFrame Join(
        DataFrame other,
        IEnumerable<Expr> leftOn,
        IEnumerable<Expr> rightOn,
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
        using var right = other.Lazy();
        return Lazy().Join(
            right, 
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
        ).Collect();
    }

    /// <summary>
    /// Join using multiple shared expressions. (C# Array Covariance Helper)
    /// Usage: df.Join(other, on: [Pl.Col("A"), Pl.Col("B")])
    /// </summary>
    public DataFrame Join(
        DataFrame other,
        IEnumerable<Expr> on,
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
        using var right = other.Lazy();
        return Lazy().Join(
            right, 
            on, 
            how, 
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
    /// <inheritdoc cref="LazyFrame.JoinAsOfInternal(LazyFrame, IntoExprColumn, IntoExprColumn, IEnumerable{string}?, IEnumerable{string}?, AsOfTolerance?, AsofStrategy, bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder, JoinSide, bool, long?, ulong)"/>
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
    ///     leftOn: Pl.Col("time"), 
    ///     rightOn: Pl.Col("time"),
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
    /// <summary>
    /// Join AsOf using a shared 'on' column and optional shared 'by' columns.
    /// Usage: lf.JoinAsOf(other, on: "time", tolerance: "2h", by: ["ticker"])
    /// </summary>
    public DataFrame JoinAsOf(
        DataFrame other,
        IntoExprColumn on,
        AsOfTolerance? tolerance = null,
        IEnumerable<string>? by = null,
        AsofStrategy strategy = AsofStrategy.Backward,
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
        using var right = other.Lazy();
        return Lazy().JoinAsOf(
            right, on, tolerance, by, strategy,
            allowEq, checkSorted, suffix, validation, coalesce, 
            maintainOrder, joinSide, nullsEqual, sliceOffset, sliceLen
        ).Collect();
    }

    /// <summary>
    /// Join AsOf using independent 'leftOn'/'rightOn' and 'leftBy'/'rightBy' columns.
    /// Usage: lf.JoinAsOf(other, leftOn: "time_L", rightOn: "time_R", tolerance: TimeSpan.FromMinutes(5))
    /// </summary>
    /// <inheritdoc cref="LazyFrame.JoinAsOfInternal(LazyFrame, IntoExprColumn, IntoExprColumn, IEnumerable{string}?, IEnumerable{string}?, AsOfTolerance?, AsofStrategy, bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder, JoinSide, bool, long?, ulong)"/>
    public DataFrame JoinAsOf(
        DataFrame other,
        IntoExprColumn leftOn,
        IntoExprColumn rightOn,
        AsOfTolerance? tolerance = null,
        IEnumerable<string>? leftBy = null,
        IEnumerable<string>? rightBy = null,
        AsofStrategy strategy = AsofStrategy.Backward,
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
        using var right = other.Lazy();
        return Lazy().JoinAsOf(
            right, leftOn, rightOn,tolerance, leftBy, rightBy,  strategy,
            allowEq, checkSorted, suffix, validation, coalesce, 
            maintainOrder, joinSide, nullsEqual, sliceOffset, sliceLen
        ).Collect();
    }
    /// <summary>
    /// Join with another DataFrame using multiple boolean expressions.
    /// Usage: lf.JoinWhere(other, [Pl.Col("start") &gt; Pl.Col("end"), Pl.Col("id") != null], how: JoinType.Left)
    /// </summary>
    public DataFrame JoinWhere(
        DataFrame other,
        IEnumerable<IntoExprColumn> predicates,
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        bool nullsEqual = false)
    {
        using var right = other.Lazy();
        return Lazy().JoinWhere(
            right, predicates,how,
            suffix, validation, coalesce, 
            maintainOrder, nullsEqual
        ).Collect();
    }

    /// <summary>
    /// Join using a single condition (Most common use case).
    /// Usage: lf.JoinWhere(other, Pl.Col("A") &gt; Pl.Col("B"), how: JoinType.Inner)
    /// </summary>
    public DataFrame JoinWhere(
        DataFrame other,
        IntoExprColumn predicate,
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        bool nullsEqual = false)
    {
        return JoinWhere(
            other, 
            [predicate], 
            how, suffix, validation, coalesce, maintainOrder, nullsEqual
        );
    }

    /// <summary>
    /// Note: You cannot use optional parameters (like 'how' or 'suffix') with this overload. Use collection expressions `[...]` instead.
    /// Usage: lf.JoinWhere(other, Pl.Col("A") &gt; Pl.Col("B"), Pl.Col("C") > 0)
    /// </summary>
    public DataFrame JoinWhere(DataFrame other, params IntoExprColumn[] predicates)
    {
        if (predicates.Length == 0) return this;
        
        return JoinWhere(other, (IEnumerable<IntoExprColumn>)predicates);
    }
    /// <inheritdoc cref="LazyFrame.MergeSorted(LazyFrame, string, bool)"/>
    public DataFrame MergeSorted(DataFrame other, string key,bool maintainOrder=false)
    {
        using var right = other.Lazy();  
        using var left = Lazy();
        return left.MergeSorted(right,key,maintainOrder).Collect();
    }
}