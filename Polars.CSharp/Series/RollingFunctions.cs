using Polars.NET.Core;

namespace Polars.CSharp;

public partial class Series : IDisposable,IPolarsSeries
{
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
}
