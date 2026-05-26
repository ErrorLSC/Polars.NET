namespace Polars.FSharp

[<AutoOpen>]
module SeriesRollingOps =
    open System
    type Series with
        /// <summary>
        /// Apply a rolling min (moving min) over a window.
        /// </summary>
        /// <param name="windowSize">
        /// The size of the window formatted as a string duration.
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
        /// <returns>A new <see cref="Series"/> with the rolling minimum.</returns>
        member this.RollingMin(windowSize: Dur, ?minPeriod: int,?weights: float[], ?center: bool) =
            this.ApplyExpr(Expr.Col(this.Name).RollingMin(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
        /// <summary>
        /// Apply a rolling max (moving max) over a window.
        /// </summary>
        /// <param name="windowSize">
        /// The size of the window formatted as a string duration.
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
        /// <returns>A new <see cref="Series"/> with the rolling maximum.</returns>
        member this.RollingMax(windowSize: Dur, ?minPeriod: int,?weights: float[], ?center:bool) =
            this.ApplyExpr(Expr.Col(this.Name).RollingMax(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
        /// <summary>
        /// Apply a rolling average (moving average) over a window.
        /// </summary>
        /// <param name="windowSize">
        /// The size of the window formatted as a string duration.
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
        /// <returns>A new <see cref="Series"/> with the rolling average.</returns>
        member this.RollingMean(windowSize: Dur, ?minPeriod: int,?weights: float[], ?center:bool) =
            this.ApplyExpr(Expr.Col(this.Name).RollingMean(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
        /// <summary>
        /// Apply a rolling sum (moving sum) over a window.
        /// </summary>
        /// <param name="windowSize">
        /// The size of the window formatted as a string duration.
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
        /// <returns>A new <see cref="Series"/> with the rolling sum.</returns>
        member this.RollingSum(windowSize: Dur, ?minPeriod: int,?weights: float[], ?center:bool) =
            this.ApplyExpr(Expr.Col(this.Name).RollingSum(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
        /// <summary>
        /// Apply a rolling median (moving median) over a window.
        /// </summary>
        /// <param name="windowSize">
        /// The size of the window formatted as a string duration.
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
        /// <returns>A new <see cref="Series"/> with the rolling median.</returns>
        member this.RollingMedian(windowSize: Dur, ?minPeriod: int,?weights: float[], ?center:bool) =
            this.ApplyExpr(Expr.Col(this.Name).RollingMedian(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
        /// <summary>
        /// Apply a rolling standard deviation (moving standard deviation) over a window.
        /// </summary>
        /// <param name="windowSize">
        /// The size of the window formatted as a string duration.
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
        /// <returns>A new <see cref="Series"/> with the rolling standard deviation.</returns>
        member this.RollingStd(windowSize: Dur, ?minPeriod: int,?weights: float[], ?center:bool) =
            this.ApplyExpr(Expr.Col(this.Name).RollingStd(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
        /// <summary>
        /// Apply a rolling variance (moving variance) over a window.
        /// </summary>
        /// <param name="windowSize">
        /// The size of the window formatted as a string duration.
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
        /// <returns>A new <see cref="Series"/> with the rolling variance.</returns>
        member this.RollingVar(windowSize: Dur, ?minPeriod: int,?weights: float[], ?center:bool,?ddof:uint8) =
            this.ApplyExpr(Expr.Col(this.Name).RollingVar(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center,?ddof=ddof))
        /// <summary>
        /// Apply a rolling skew (moving skew) over a window.
        /// </summary>
        /// <param name="windowSize">
        /// The size of the window formatted as a string duration.
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
        /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
        /// <returns>A new <see cref="Series"/> with the rolling skew.</returns>
        member this.RollingSkew(windowSize: Dur, ?minPeriod: int,?weights: float[], ?center:bool,?bias:bool) =
            this.ApplyExpr(Expr.Col(this.Name).RollingSkew(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center,?bias=bias))
        /// <summary>
        /// Apply a rolling kurtosis over a window.
        /// </summary>
        /// <param name="windowSize">
        /// The size of the window formatted as a string duration.
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
        /// <param name="fisher">If True, Fisher’s definition is used (normal ==> 0.0). If False, Pearson’s definition is used (normal ==> 3.0).</param>
        /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
        /// <returns>A new <see cref="Series"/> with the rolling skew.</returns>
        member this.RollingKurtosis(windowSize: Dur, ?minPeriod: int,?weights: float[], ?center:bool,?fisher:bool,?bias:bool) =
            this.ApplyExpr(Expr.Col(this.Name).RollingKurtosis(windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center,?fisher=fisher,?bias=bias))
        /// <summary>
        /// Apply a rolling rank (moving rank) over a window.
        /// </summary>
        /// <param name="method">
        /// The method used to assign ranks to tied elements. See <see cref="RankMethod"/> for details.
        /// Default is <see cref="RankMethod.Average"/>.</param>
        /// <param name="seed">If method="random", use this as seed.
        /// </param>
        /// <param name="windowSize">
        /// The size of the time window as a <see cref="TimeSpan"/>.
        /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
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
        member this.RollingRank(windowSize: Dur, ?minPeriod: int,?method:RankMethod,?seed:uint64,?weights: float[], ?center:bool) =
            this.ApplyExpr(Expr.Col(this.Name).RollingRank(windowSize, ?minPeriod=minPeriod,?method=method,?seed=seed,?weights=weights,?center=center))
        /// <summary>
        /// Apply a rolling quantile over a fixed window.
        /// </summary>
        /// <param name="quantile">Quantile between 0.0 and 1.0 (e.g., 0.5 for median).</param>
        /// <param name="method">Interpolation method when the quantile lies between two data points.</param>
        /// <param name="windowSize">
        /// The size of the window. 
        /// <para>Format: <c>"3i"</c> (3 rows) or just a number string <c>"3"</c>.</para>
        /// <para>For time-based windows (e.g. "2h"), use <see cref="RollingQuantileBy(double,QuantileMethod,string,Expr,int,ClosedWindow)"/> instead.</para>
        /// </param>
        /// <param name="weights">
        /// Optional weights for the window. The length must match the parsed window size.
        /// <para>If <c>null</c>, equal weights are used.</para>
        /// </param>
        /// <param name="minPeriods">
        /// The minimum number of observations in the window required to have a value (otherwise <c>null</c>).
        /// </param>
        /// <param name="center">
        /// If <c>true</c>, the window is centered on the current observation.
        /// <para>Default is <c>false</c> (right-aligned window, <c>[i-window, i]</c>).</para>
        /// </param>
        /// <returns>A new series representing the rolling quantile.</returns>
        member this.RollingQuantile(quantile:float,method:QuantileMethod,windowSize: Dur, ?minPeriod: int,?weights: float[], ?center:bool) =
            this.ApplyExpr(Expr.Col(this.Name).RollingQuantile(quantile,method,windowSize, ?minPeriod=minPeriod,?weights=weights,?center=center))
        // ==========================================
        // Rolling ... By
        // ==========================================

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
        /// <param name="minPeriods">
        /// The minimum number of observations in the window required to have a non-null result.</param>
        /// <param name="closed">
        /// Defines how the window interval is closed. 
        /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
        /// </param>
        /// <returns>A new series representing the dynamic rolling mean.</returns>
        member this.RollingMeanBy(windowSize: Dur, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
            this.ApplyExpr(Expr.Col(this.Name).RollingMeanBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
        /// <summary>
        /// Apply a rolling sum (moving sum) over a dynamic window defined by the values in the <paramref name="by"/> column.
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
        /// <param name="minPeriods">
        /// The minimum number of observations in the window required to have a non-null result.</param>
        /// <param name="closed">
        /// Defines how the window interval is closed. 
        /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
        /// </param>
        /// <returns>A new series representing the dynamic rolling sum.</returns>
        member this.RollingSumBy(windowSize: Dur, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
            this.ApplyExpr(Expr.Col(this.Name).RollingSumBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
        /// <summary>
        /// Apply a rolling min (moving min) over a dynamic window defined by the values in the <paramref name="by"/> column.
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
        /// <param name="minPeriods">
        /// The minimum number of observations in the window required to have a non-null result.</param>
        /// <param name="closed">
        /// Defines how the window interval is closed. 
        /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
        /// </param>
        /// <returns>A new series representing the dynamic rolling min.</returns>
        member this.RollingMinBy(windowSize: Dur, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
            this.ApplyExpr(Expr.Col(this.Name).RollingMinBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
        /// <summary>
        /// Apply a rolling max (moving max) over a dynamic window defined by the values in the <paramref name="by"/> column.
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
        /// <param name="minPeriods">
        /// The minimum number of observations in the window required to have a non-null result.</param>
        /// <param name="closed">
        /// Defines how the window interval is closed. 
        /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
        /// </param>
        /// <returns>A new series representing the dynamic rolling max.</returns>
        member this.RollingMaxBy(windowSize: Dur, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
            this.ApplyExpr(Expr.Col(this.Name).RollingMaxBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
        /// <summary>
        /// Apply a rolling median (moving median) over a dynamic window defined by the values in the <paramref name="by"/> column.
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
        /// <param name="minPeriods">
        /// The minimum number of observations in the window required to have a non-null result.</param>
        /// <param name="closed">
        /// Defines how the window interval is closed. 
        /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
        /// </param>
        /// <returns>A new series representing the dynamic rolling max.</returns>
        member this.RollingMedianBy(windowSize: Dur, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
            this.ApplyExpr(Expr.Col(this.Name).RollingMedianBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
        /// <summary>
        /// Apply a rolling standard deviation (moving standard deviation) over a dynamic window defined by the values in the <paramref name="by"/> column.
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
        /// <param name="minPeriods">
        /// The minimum number of observations in the window required to have a non-null result.</param>
        /// <param name="closed">
        /// Defines how the window interval is closed. 
        /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
        /// </param>
        /// <returns>A new series representing the dynamic rolling standard deviation.</returns>
        member this.RollingStdBy(windowSize: Dur, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
            this.ApplyExpr(Expr.Col(this.Name).RollingStdBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod))
        /// <summary>
        /// Apply a rolling variance (moving variance) over a dynamic window defined by the values in the <paramref name="by"/> column.
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
        /// <param name="minPeriods">
        /// The minimum number of observations in the window required to have a non-null result.</param>
        /// <param name="closed">
        /// Defines how the window interval is closed. 
        /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
        /// </param>
        /// <param name="ddof">
        /// “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. 
        /// <para>By default ddof is 1.</para>
        /// </param>
        /// <returns>A new series representing the dynamic rolling variance.</returns>
        member this.RollingVarBy(windowSize: Dur, by: Expr, ?closed: ClosedWindow, ?minPeriod: int,?ddof:uint8) =
            this.ApplyExpr(Expr.Col(this.Name).RollingVarBy(windowSize, by, ?closed=closed, ?minPeriod=minPeriod,?ddof=ddof))
        /// <summary>
        /// Apply a rolling rank (moving rank) over a dynamic window defined by the values in the <paramref name="by"/> column.
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
        /// <param name="method">The method used to assign ranks to tied elements.
        /// </param>
        /// <param name="seed">Seed for the random method (only relevant when method is Random).
        /// </param>
        /// <param name="minPeriods">
        /// The minimum number of observations in the window required to have a non-null result.</param>
        /// <param name="closed">
        /// Defines how the window interval is closed. 
        /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
        /// </param>
        /// <returns>A new series representing the dynamic rolling rank.</returns>
        member this.RollingRankBy(windowSize: Dur, by: Expr, ?method:RollingRankMethod,?seed:uint64,?closed: ClosedWindow, ?minPeriod: int) =
            this.ApplyExpr(Expr.Col(this.Name).RollingRankBy(windowSize, by,?method=method,?seed=seed, ?closed=closed, ?minPeriod=minPeriod))
        /// <summary>
        /// Apply a rolling quantile (moving quantile) over a dynamic window defined by the values in the <paramref name="by"/> column.
        /// <para>
        /// Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).
        /// </para>
        /// </summary>
        /// <remarks>
        /// The <paramref name="by"/> column must be sorted in ascending order.
        /// </remarks>
        /// <param name="quantile">Quantile between 0.0 and 1.0 (e.g., 0.5 for median).
        /// </param>
        /// <param name="method">Interpolation method when the quantile lies between two data points.
        /// </param>
        /// <param name="windowSize">
        /// The size of the time window as a <see cref="TimeSpan"/>.
        /// <para>This will be automatically converted to a Polars duration string (e.g., <c>01:30:00</c> -> <c>"1h30m"</c>).</para>
        /// </param>
        /// <param name="by">
        /// The column used to define the window (the "time" axis). 
        /// <para>Typically a <c>Date</c> or <c>DateTime</c> column, but can also be monotonic integers.</para>
        /// </param>
        /// <param name="minPeriods">
        /// The minimum number of observations in the window required to have a non-null result.</param>
        /// <param name="closed">
        /// Defines how the window interval is closed. 
        /// Default is <see cref="ClosedWindow.Left"/> <c>[t - window, t)</c>.
        /// </param>
        /// <returns>A new series representing the dynamic rolling quantile.</returns>
        member this.RollingQuantileBy(quantile: float, method: QuantileMethod, windowSize: Dur, by: Expr, ?closed: ClosedWindow, ?minPeriod: int) =
            this.ApplyExpr(Expr.Col(this.Name).RollingQuantileBy(quantile, method, windowSize, by, ?closed=closed, ?minPeriod=minPeriod))