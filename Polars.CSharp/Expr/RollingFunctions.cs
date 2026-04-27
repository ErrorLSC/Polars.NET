#pragma warning disable CS1573
using Polars.NET.Core;
using Polars.NET.Core.Helpers;

namespace Polars.CSharp;

/// <summary>
/// A Polars Expr
/// </summary>
public partial class Expr : IDisposable
{
    // ==========================================
    // Rolling Window Functions
    // ==========================================
    /// <summary>
    /// Create rolling groups based on a temporal or integer column.
    /// </summary>
    /// <param name="indexColumn"> Column used to group based on the time window. Often of type Date/Datetime. 
    /// This column must be sorted in ascending order. 
    /// In case of a rolling group by on indices, dtype needs to be one of {UInt32, UInt64, Int32, Int64}. 
    /// Note that the first three get temporarily cast to Int64, so if performance matters use an Int64 column.</param>
    /// <param name="period">Length of the window - must be non-negative.</param>
    /// <param name="offset">Offset of the window. Default is -period.</param>
    /// <param name="closed">Define which sides of the temporal interval are closed (inclusive).</param>
    public Expr Rolling(IntoExprColumn indexColumn,IntoDuration period,IntoDuration? offset=null,ClosedInterval closed = ClosedInterval.Right)
    {
        ExprHandle index = indexColumn.Consume().Handle;
        string periodStr = period.Value;
        string offsetStr = offset?.Value ?? ("-" + periodStr);
        return new(PolarsWrapper.ExprRolling(CloneHandle(),index,periodStr,offsetStr,closed.ToNative()));
    }

    /// <summary>
    /// Apply a rolling min (moving min) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(IntoDuration,int,double[],bool)"/>
    public Expr RollingMin(IntoDuration windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => new(PolarsWrapper.RollingMin(CloneHandle(), windowSize.Value, minPeriods, weights,center));
    /// <summary>
    /// Apply a rolling max (moving max) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(IntoDuration,int,double[],bool)"/>
    public Expr RollingMax(IntoDuration windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => new(PolarsWrapper.RollingMax(CloneHandle(), windowSize.Value, minPeriods,weights,center));
    /// <summary>
    /// Apply a rolling mean (moving average) over a window.
    /// </summary>
    /// <param name="windowSize">
    /// The size of the window formatted as a string duration. Or use .NET TimeSpan directly.
    /// <para>Examples: <c>"3i"</c> (3 index rows), <c>"1d"</c> (1 day), <c>"1h"</c> (1 hour).</para>
    /// </param>
    /// <param name="minPeriods">
    /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
    /// </param>
    /// <param name="weights">
    /// Optional weights to apply to the window.
    /// <para>The length of the array should match the window size (if using fixed row windows).</para>
    /// <para>Default is <c>null</c> (unweighted).</para>
    /// </param>
    /// <param name="center">
    /// If <c>true</c>, the window is centered on the current observation.
    /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
    /// </param>
    /// <returns>A new expression representing the rolling mean.</returns>
    /// <example>
    /// <code>
    /// // Rolling mean of 3 rows ("3i"), centered
    /// df.Select(
    ///     Col("val").RollingMean("3i", minPeriods: 1, center: true).Alias("roll_mean")
    /// );
    /// </code>
    /// </example>
    public Expr RollingMean(IntoDuration windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => new(PolarsWrapper.RollingMean(CloneHandle(), windowSize.Value, minPeriods,weights,center));
    /// <summary>
    /// Apply a rolling sum (moving sum) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(IntoDuration,int,double[],bool)"/>
    public Expr RollingSum(IntoDuration windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => new(PolarsWrapper.RollingSum(CloneHandle(), windowSize.Value, minPeriods, weights,center));
    /// <summary>
    /// Apply a rolling standard deviation (moving std) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(IntoDuration,int,double[],bool)"/>
    public Expr RollingStd(IntoDuration windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => new(PolarsWrapper.RollingStd(CloneHandle(), windowSize.Value, minPeriods, weights, center));
    /// <summary>
    /// Apply a rolling variance (moving var) over a window.
    /// </summary>
    /// <param name="ddof">
    /// “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. 
    /// <para>By default ddof is 1.</para>
    /// </param>
    /// <param name="windowSize">
    /// <inheritdoc cref="RollingMean(IntoDuration, int, double[], bool)" path="/param[@name='windowSize']/node()"/>
    /// </param>
    /// <param name="minPeriods">
    /// <inheritdoc cref="RollingMean(IntoDuration, int, double[], bool)" path="/param[@name='minPeriods']/node()"/>
    /// </param>
    /// <param name="weights">
    /// <inheritdoc cref="RollingMean(IntoDuration, int, double[], bool)" path="/param[@name='weights']/node()"/>
    /// </param>
    /// <param name="center">
    /// <inheritdoc cref="RollingMean(IntoDuration, int, double[], bool)" path="/param[@name='center']/node()"/>
    /// </param>
    /// <returns>A new expression representing the rolling variance.</returns>
    public Expr RollingVar(IntoDuration windowSize, int minPeriods = 1,double[]? weights = null,bool center=false, byte ddof =1)
        => new(PolarsWrapper.RollingVar(CloneHandle(), windowSize.Value, minPeriods,weights,center, ddof));
    /// <summary>
    /// Apply a rolling median (moving median) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(IntoDuration,int,double[],bool)"/>
    public Expr RollingMedian(IntoDuration windowSize, int minPeriods = 1,double[]? weights = null,bool center=false)
        => new(PolarsWrapper.RollingMedian(CloneHandle(), windowSize.Value, minPeriods, weights,center));
    /// <summary>
    /// Apply a rolling skew (moving skew) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(IntoDuration,int,double[],bool)"/>
    public Expr RollingSkew(IntoDuration windowSize, int minPeriods = 1,double[]? weights = null,bool center=false, bool bias=true)
        => new(PolarsWrapper.RollingSkew(CloneHandle(), windowSize.Value, minPeriods, weights,center,bias));
    /// <summary>
    /// Apply a rolling kurtosis (moving kurtosis) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(IntoDuration,int,double[],bool)"/>
    public Expr RollingKurtosis(IntoDuration windowSize, int minPeriods = 1,double[]? weights = null,bool center=false, bool fisher = true,bool bias=true)
        => new(PolarsWrapper.RollingKurtosis(CloneHandle(), windowSize.Value, minPeriods, weights,center,fisher, bias));
    /// <summary>
    /// Apply a rolling rank (moving rank) over a window.
    /// </summary>
    /// <inheritdoc cref="RollingMean(IntoDuration,int,double[],bool)"/>
    /// <param name="method">
    /// The method used to assign ranks to tied elements. See <see cref="RankMethod"/> for details.
    /// Default is <see cref="RankMethod.Average"/>.</param>
    /// <param name="seed">If method="random", use this as seed.</param>
    public Expr RollingRank(IntoDuration windowSize, int minPeriods = 1,RankMethod method=RankMethod.Average, ulong? seed=null,double[]? weights = null,bool center=false)
        => new(PolarsWrapper.RollingRank(CloneHandle(), windowSize.Value, minPeriods,method.ToNative(),seed,weights, center));
    /// <summary>
    /// Apply a rolling quantile over a fixed window.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0 (e.g., 0.5 for median).</param>
    /// <param name="method">Interpolation method when the quantile lies between two data points.</param>
    /// <param name="windowSize">
    /// The size of the window. 
    /// <para>Format: <c>"3i"</c> (3 rows) or just a number string <c>"3"</c>.</para>
    /// <para>For time-based windows (e.g. "2h"), use <see cref="RollingQuantileBy(double,QuantileMethod,IntoDuration,Expr,int,ClosedInterval)"/> instead.</para>
    /// </param>
    /// <param name="weights">
    /// Optional weights for the window. The length must match the parsed window size.
    /// <para>If <c>null</c>, equal weights are used.</para>
    /// </param>
    /// <param name="minPeriods">
    /// <inheritdoc cref="RollingMean(IntoDuration, int, double[], bool)" path="/param[@name='minPeriods']/node()"/>
    /// </param>
    /// <param name="center">
    /// <inheritdoc cref="RollingMean(IntoDuration, int, double[], bool)" path="/param[@name='center']/node()"/>
    /// </param>
    /// <returns>A new expression representing the rolling quantile.</returns>
    public Expr RollingQuantile(
        double quantile, 
        QuantileMethod method, 
        IntoDuration windowSize, 
        int minPeriods = 1, 
        double[]? weights = null,
        bool center =false)
    {
        return new Expr(PolarsWrapper.RollingQuantile(
            CloneHandle(), 
            quantile,
            method.ToNative(),
            windowSize.Value, 
            minPeriods,
            weights,
            center
        ));
    }
    /// <summary>
    /// Apply a rolling mean (moving average) over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// <para>
    /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
    /// </para>
    /// </summary>
    /// <remarks>
    /// The <paramref name="by"/> column must be sorted in ascending order.
    /// </remarks>
    /// <param name="windowSize">
    /// The size of the dynamic window.
    /// <para>Supported duration strings: <c>"1d"</c>, <c>"2h"</c>, <c>"10s"</c>, <c>"500ms"</c>, etc.</para>
    /// </param>
    /// <param name="by">
    /// The column used to define the window (the "time" axis). 
    /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
    /// </param>
    /// <param name="minPeriods">The minimum number of observations in the window required to have a non-null result.</param>
    /// <param name="closed">
    /// Defines how the window interval is closed. 
    /// Default is <see cref="ClosedInterval.Left"/> <c>[t - window, t)</c>.
    /// </param>
    /// <returns>A new expression representing the dynamic rolling mean.</returns>
    /// <example>
    /// <code>
    /// // Python: pl.col("index").rolling_mean_by("date", window_size="2h", closed="both")
    /// // C#:
    /// Col("index").RollingMeanBy("2h", Col("date"), closed: ClosedInterval.Both);
    /// </code>
    /// </example>
    public Expr RollingMeanBy(IntoDuration windowSize, Expr by, int minPeriods = 1, ClosedInterval closed = ClosedInterval.Left)
    {
        return new Expr(PolarsWrapper.RollingMeanBy(
            CloneHandle(),
            windowSize.Value,
            minPeriods,
            by.CloneHandle(),
            closed.ToNative()
        ));
    }
    /// <summary>
    /// Apply a rolling sum over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <inheritdoc cref="RollingMeanBy(IntoDuration,Expr,int,ClosedInterval)" path="/param"/>
    /// <inheritdoc cref="RollingMeanBy(IntoDuration,Expr,int,ClosedInterval)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling sum.</returns>
    public Expr RollingSumBy(IntoDuration windowSize, Expr by, int minPeriods = 1, ClosedInterval closed = ClosedInterval.Left)
    {
        return new Expr(PolarsWrapper.RollingSumBy(
            CloneHandle(),
            windowSize.Value,
            minPeriods,
            by.CloneHandle(),
            closed.ToNative()
        ));
    }
    /// <summary>
    /// Apply the rolling minimum over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <inheritdoc cref="RollingMeanBy(IntoDuration,Expr,int,ClosedInterval)" path="/param"/>
    /// <inheritdoc cref="RollingMeanBy(IntoDuration,Expr,int,ClosedInterval)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling minimum.</returns>
    public Expr RollingMinBy(IntoDuration windowSize, Expr by, int minPeriods = 1, ClosedInterval closed = ClosedInterval.Left)
    {
        return new Expr(PolarsWrapper.RollingMinBy(
            CloneHandle(),
            windowSize.Value,
            minPeriods,
            by.CloneHandle(),
            closed.ToNative()
        ));
    }
    /// <summary>
    /// Apply the rolling maximum over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <inheritdoc cref="RollingMeanBy(IntoDuration,Expr,int,ClosedInterval)" path="/param"/>
    /// <inheritdoc cref="RollingMeanBy(IntoDuration,Expr,int,ClosedInterval)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling maximum.</returns>
    public Expr RollingMaxBy(IntoDuration windowSize, Expr by, int minPeriods = 1, ClosedInterval closed = ClosedInterval.Left)
    {
        return new Expr(PolarsWrapper.RollingMaxBy(
            CloneHandle(),
            windowSize.Value,
            minPeriods,
            by.CloneHandle(),
            closed.ToNative()
        ));
    }
    /// <summary>
    /// Apply the rolling standard deviation over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <inheritdoc cref="RollingMeanBy(IntoDuration,Expr,int,ClosedInterval)" path="/param"/>
    /// <inheritdoc cref="RollingMeanBy(IntoDuration,Expr,int,ClosedInterval)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling standard deviation.</returns>
    public Expr RollingStdBy(IntoDuration windowSize, Expr by, int minPeriods = 1, ClosedInterval closed = ClosedInterval.Left)
    {
        return new Expr(PolarsWrapper.RollingStdBy(
            CloneHandle(),
            windowSize.Value,
            minPeriods,
            by.CloneHandle(),
            closed.ToNative()
        ));
    }
    /// <summary>
    /// Apply the rolling variance over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// 
    /// /// <param name="windowSize">
    /// <inheritdoc cref="RollingMeanBy(IntoDuration, Expr, int, ClosedInterval)" path="/param[@name='windowSize']/node()"/>
    /// </param>
    /// 
    /// /// <param name="by">
    /// <inheritdoc cref="RollingMeanBy(IntoDuration, Expr, int, ClosedInterval)" path="/param[@name='by']/node()"/>
    /// </param>
    /// 
    /// /// <param name="minPeriods">
    /// <inheritdoc cref="RollingMeanBy(IntoDuration, Expr, int, ClosedInterval)" path="/param[@name='minPeriods']/node()"/>
    /// </param>
    /// 
    /// /// <param name="closed">
    /// <inheritdoc cref="RollingMeanBy(IntoDuration, Expr, int, ClosedInterval)" path="/param[@name='closed']/node()"/>
    /// </param>
    /// 
    /// /// <param name="ddof">
    /// “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. 
    /// <para>By default ddof is 1.</para>
    /// </param>
    /// 
    /// <returns>A new expression representing the dynamic rolling variance.</returns>
    public Expr RollingVarBy(IntoDuration windowSize, Expr by, int minPeriods = 1, ClosedInterval closed = ClosedInterval.Left, byte ddof=1)
    {
        return new Expr(PolarsWrapper.RollingVarBy(
            CloneHandle(),
            windowSize.Value,
            minPeriods,
            by.CloneHandle(),
            closed.ToNative(),
            ddof
        ));
    }
    /// <summary>
    /// Apply the rolling median over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <inheritdoc cref="RollingMeanBy(IntoDuration,Expr,int,ClosedInterval)" path="/param"/>
    /// <inheritdoc cref="RollingMeanBy(IntoDuration,Expr,int,ClosedInterval)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling median.</returns>
    public Expr RollingMedianBy(IntoDuration windowSize, Expr by, int minPeriods = 1, ClosedInterval closed = ClosedInterval.Left)
    {
        return new Expr(PolarsWrapper.RollingMedianBy(
            CloneHandle(),
            windowSize.Value,
            minPeriods,
            by.CloneHandle(),
            closed.ToNative()
        ));
    }
    /// <summary>
    /// Compute the rolling rank over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// 
    /// <param name="windowSize">
    /// <inheritdoc cref="RollingMeanBy(IntoDuration, Expr, int, ClosedInterval)" path="/param[@name='windowSize']/node()"/>
    /// </param>
    /// 
    /// <param name="by">
    /// <inheritdoc cref="RollingMeanBy(IntoDuration, Expr, int, ClosedInterval)" path="/param[@name='by']/node()"/>
    /// </param>
    /// 
    /// <param name="method">The method used to assign ranks to tied elements.</param>
    /// 
    /// <param name="seed">Seed for the random method (only relevant when method is Random).</param>
    /// 
    /// <param name="minPeriods">
    /// <inheritdoc cref="RollingMeanBy(IntoDuration, Expr, int, ClosedInterval)" path="/param[@name='minPeriods']/node()"/>
    /// </param>
    /// 
    /// <param name="closed">
    /// <inheritdoc cref="RollingMeanBy(IntoDuration, Expr, int, ClosedInterval)" path="/param[@name='closed']/node()"/>
    /// </param>
    /// 
    /// <returns>A new expression representing the dynamic rolling rank.</returns>
    public Expr RollingRankBy(
        IntoDuration windowSize, 
        Expr by, 
        RollingRankMethod method = RollingRankMethod.Average, 
        ulong? seed = null,
        int minPeriods = 1, 
        ClosedInterval closed = ClosedInterval.Left)
    {
        return new Expr(PolarsWrapper.RollingRankBy(
            CloneHandle(),
            windowSize.Value,
            by.CloneHandle(),
            method.ToNative(),
            seed,
            minPeriods,
            closed.ToNative()
        ));
    }
    /// <summary>
    /// Compute the rolling quantile over a dynamic window defined by the values in the <paramref name="by"/> column.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0 (e.g., 0.5 for median).</param>
    /// <param name="method">Interpolation method when the quantile lies between two data points.</param>
    ///     
    /// <param name="windowSize">
    /// The size of the time window as a <see cref="TimeSpan"/>.
    /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
    /// </param>
    /// <param name="by"><inheritdoc cref="RollingMeanBy(IntoDuration,Expr,int,ClosedInterval)" path="/param[@name='by']/node()"/></param>
    /// <param name="minPeriods"><inheritdoc cref="RollingMeanBy(IntoDuration,Expr,int,ClosedInterval)" path="/param[@name='minPeriods']/node()"/></param>
    /// <param name="closed"><inheritdoc cref="RollingMeanBy(IntoDuration,Expr,int,ClosedInterval)" path="/param[@name='closed']/node()"/></param>
    ///
    /// <inheritdoc cref="RollingMeanBy(IntoDuration,Expr,int,ClosedInterval)" path="/remarks"/>
    /// <returns>A new expression representing the dynamic rolling quantile.</returns>
    public Expr RollingQuantileBy(
        double quantile,
        QuantileMethod method,
        IntoDuration windowSize,
        Expr by,
        int minPeriods = 1,
        ClosedInterval closed = ClosedInterval.Left)
    {
        return new Expr(PolarsWrapper.RollingQuantileBy(
            CloneHandle(),
            quantile,
            method.ToNative(),
            windowSize.Value,
            minPeriods,
            by.CloneHandle(),
            closed.ToNative()
        ));
    }
}