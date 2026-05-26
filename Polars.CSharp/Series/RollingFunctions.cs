using Polars.NET.Core;

namespace Polars.CSharp;

public partial class Series : IDisposable,IPolarsSeries
{
    /// <summary>  
    /// <inheritdoc cref="Expr.RollingMin(IntoDuration, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMin(IntoDuration, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling minimum.</returns>
    public Series RollingMin(IntoDuration windowSize, int minPeriods = 1, double[]? weights= null, bool center=false) 
        => ApplyExpr(Polars.Col(Name).RollingMin(windowSize, minPeriods,weights,center));
    /// <summary>  
    /// <inheritdoc cref="Expr.RollingMax(IntoDuration, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMax(IntoDuration, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling maximum.</returns>
    public Series RollingMax(IntoDuration windowSize, int minPeriods = 1, double[]? weights= null, bool center=false) 
        => ApplyExpr(Polars.Col(Name).RollingMax(windowSize, minPeriods,weights,center));
    /// <summary>
    /// <inheritdoc cref="Expr.RollingMean(IntoDuration, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMean(IntoDuration, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling mean.</returns>
    public Series RollingMean(IntoDuration windowSize, int minPeriods = 1,double[]? weights= null, bool center=false) 
        => ApplyExpr(Polars.Col(Name).RollingMean(windowSize, minPeriods, weights,center));
    /// <summary>
    /// <inheritdoc cref="Expr.RollingSum(IntoDuration, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingSum(IntoDuration, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling sum.</returns>
    public Series RollingSum(IntoDuration windowSize, int minPeriods = 1,double[]? weights= null, bool center=false) 
        => ApplyExpr(Polars.Col(Name).RollingSum(windowSize, minPeriods,weights,center));
    /// <summary>
    /// <inheritdoc cref="Expr.RollingStd(IntoDuration, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingStd(IntoDuration, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling standard deviation.</returns>
    public Series RollingStd(IntoDuration windowSize, int minPeriods = 1, double[]? weights = null, bool center = false)
        => ApplyExpr(Polars.Col(Name).RollingStd(windowSize, minPeriods, weights, center));
    /// <summary>
    /// <inheritdoc cref="Expr.RollingVar(IntoDuration, int, double[], bool, byte)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingVar(IntoDuration, int, double[], bool, byte)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling variance.</returns>
    public Series RollingVar(IntoDuration windowSize, int minPeriods = 1, double[]? weights = null, bool center = false, byte ddof = 1)
        => ApplyExpr(Polars.Col(Name).RollingVar(windowSize, minPeriods, weights, center, ddof));
    /// <summary>
    /// <inheritdoc cref="Expr.RollingMedian(IntoDuration, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMedian(IntoDuration, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling median.</returns>
    public Series RollingMedian(IntoDuration windowSize, int minPeriods = 1, double[]? weights = null, bool center = false)
        => ApplyExpr(Polars.Col(Name).RollingMedian(windowSize, minPeriods, weights, center));
    /// <summary>
    /// <inheritdoc cref="Expr.RollingSkew(IntoDuration, int, double[], bool, bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingSkew(IntoDuration, int, double[], bool, bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling skew.</returns>
    public Series RollingSkew(IntoDuration windowSize, int minPeriods = 1, double[]? weights = null, bool center = false, bool bias = true)
        => ApplyExpr(Polars.Col(Name).RollingSkew(windowSize, minPeriods, weights, center, bias));
    /// <summary>
    /// <inheritdoc cref="Expr.RollingKurtosis(IntoDuration, int, double[], bool, bool, bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingKurtosis(IntoDuration, int, double[], bool, bool, bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling kurtosis.</returns>
    public Series RollingKurtosis(IntoDuration windowSize, int minPeriods = 1, double[]? weights = null, bool center = false, bool fisher = true, bool bias = true)
        => ApplyExpr(Polars.Col(Name).RollingKurtosis(windowSize, minPeriods, weights, center, fisher, bias));
    // -------------------------------------------------------------------------
    // Rolling Rank & Quantile
    // -------------------------------------------------------------------------

    /// <summary>
    /// <inheritdoc cref="Expr.RollingRank(IntoDuration, int, RankMethod, ulong?, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingRank(IntoDuration, int, RankMethod, ulong?, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling rank.</returns>
    public Series RollingRank(IntoDuration windowSize, int minPeriods = 1, RankMethod method = RankMethod.Average, ulong? seed = null, double[]? weights = null, bool center = false)
        => ApplyExpr(Polars.Col(Name).RollingRank(windowSize, minPeriods, method, seed, weights, center));
    /// <summary>
    /// <inheritdoc cref="Expr.RollingQuantile(double, QuantileMethod, IntoDuration, int, double[], bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingQuantile(double, QuantileMethod, IntoDuration, int, double[], bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the rolling quantile.</returns>
    public Series RollingQuantile(double quantile, QuantileMethod method, IntoDuration windowSize, int minPeriods = 1, double[]? weights = null, bool center = false)
        => ApplyExpr(Polars.Col(Name).RollingQuantile(quantile, method, windowSize, minPeriods, weights, center));
    // -------------------------------------------------------------------------
    // Rolling ... By (Dynamic Window based on another column, usually Time)
    // -------------------------------------------------------------------------
    /// <summary>
    /// <inheritdoc cref="Expr.RollingMeanBy(IntoDuration, Expr, int, ClosedInterval)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMeanBy(IntoDuration, Expr, int, ClosedInterval)" path="/remarks"/>
    /// <inheritdoc cref="Expr.RollingMeanBy(IntoDuration, Expr, int, ClosedInterval)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling mean.</returns>
    public Series RollingMeanBy(IntoDuration windowSize, Expr by, int minPeriods = 1, ClosedInterval closed = ClosedInterval.Left)
        => ApplyExpr(Polars.Col(Name).RollingMeanBy(windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingSumBy(IntoDuration, Expr, int, ClosedInterval)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingSumBy(IntoDuration, Expr, int, ClosedInterval)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling sum.</returns>
    public Series RollingSumBy(IntoDuration windowSize, Expr by, int minPeriods = 1, ClosedInterval closed = ClosedInterval.Left)
        => ApplyExpr(Polars.Col(Name).RollingSumBy(windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingMinBy(IntoDuration, Expr, int, ClosedInterval)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMinBy(IntoDuration, Expr, int, ClosedInterval)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling minimum.</returns>
    public Series RollingMinBy(IntoDuration windowSize, Expr by, int minPeriods = 1, ClosedInterval closed = ClosedInterval.Left)
        => ApplyExpr(Polars.Col(Name).RollingMinBy(windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingMaxBy(IntoDuration, Expr, int, ClosedInterval)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMaxBy(IntoDuration, Expr, int, ClosedInterval)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling maximum.</returns>
    public Series RollingMaxBy(IntoDuration windowSize, Expr by, int minPeriods = 1, ClosedInterval closed = ClosedInterval.Left)
        => ApplyExpr(Polars.Col(Name).RollingMaxBy(windowSize, by, minPeriods, closed));
    /// <summary>
    /// <inheritdoc cref="Expr.RollingStdBy(IntoDuration, Expr, int, ClosedInterval)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingStdBy(IntoDuration, Expr, int, ClosedInterval)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling standard deviation.</returns>
    public Series RollingStdBy(IntoDuration windowSize, Expr by, int minPeriods = 1, ClosedInterval closed = ClosedInterval.Left)
        => ApplyExpr(Polars.Col(Name).RollingStdBy(windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingVarBy(IntoDuration, Expr, int, ClosedInterval, byte)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingVarBy(IntoDuration, Expr, int, ClosedInterval, byte)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling variance.</returns>
    public Series RollingVarBy(IntoDuration windowSize, Expr by, int minPeriods = 1, ClosedInterval closed = ClosedInterval.Left, byte ddof = 1)
        => ApplyExpr(Polars.Col(Name).RollingVarBy(windowSize, by, minPeriods, closed, ddof));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingMedianBy(IntoDuration, Expr, int, ClosedInterval)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingMedianBy(IntoDuration, Expr, int, ClosedInterval)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling median.</returns>
    public Series RollingMedianBy(IntoDuration windowSize, Expr by, int minPeriods = 1, ClosedInterval closed = ClosedInterval.Left)
        => ApplyExpr(Polars.Col(Name).RollingMedianBy(windowSize, by, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingRankBy(IntoDuration, Expr, RollingRankMethod, ulong?, int, ClosedInterval)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingRankBy(IntoDuration, Expr, RollingRankMethod, ulong?, int, ClosedInterval)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling rank.</returns>
    public Series RollingRankBy(IntoDuration windowSize, Expr by, RollingRankMethod method = RollingRankMethod.Average, ulong? seed = null, int minPeriods = 1, ClosedInterval closed = ClosedInterval.Left)
        => ApplyExpr(Polars.Col(Name).RollingRankBy(windowSize, by, method, seed, minPeriods, closed));

    /// <summary>
    /// <inheritdoc cref="Expr.RollingQuantileBy(double, QuantileMethod, IntoDuration, Expr, int, ClosedInterval)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.RollingQuantileBy(double, QuantileMethod, IntoDuration, Expr, int, ClosedInterval)" path="/param"/>
    /// <inheritdoc cref="Expr.RollingQuantileBy(double, QuantileMethod, IntoDuration, Expr, int, ClosedInterval)" path="/remarks"/>
    /// <returns>A new <see cref="Series"/> with the dynamic rolling quantile.</returns>
    public Series RollingQuantileBy(double quantile, QuantileMethod method, IntoDuration windowSize, Expr by, int minPeriods = 1, ClosedInterval closed = ClosedInterval.Left)
        => ApplyExpr(Polars.Col(Name).RollingQuantileBy(quantile, method, windowSize, by, minPeriods, closed));
}
