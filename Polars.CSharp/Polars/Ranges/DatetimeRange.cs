using Polars.NET.Core;

namespace Polars.CSharp;
public readonly partial struct Polars
{
    /// <summary>
    /// Generate a datetime range.
    /// </summary>
    /// <param name="start">Lower bound of the datetime range.</param>
    /// <param name="end">Upper bound of the datetime range.</param>
    /// <param name="interval">Interval of the range periods</param>
    /// <param name="closed">Define which sides of the range are closed</param>
    /// <param name="unit">Time unit of the resulting Datetime data type.</param>
    /// <param name="timeZone">Time zone of the resulting Datetime data type.</param>
    public static Expr DatetimeRange(
        IntoExprColumn? start = null, 
        IntoExprColumn? end = null, 
        IntoDuration? interval = null,
        ClosedInterval closed = ClosedInterval.Both,
        TimeUnit unit=TimeUnit.Microseconds,
        string? timeZone=null)
    {
        using var realStart = start?.Consume();
        using var realEnd = end?.Consume();
        string actualInterval = interval.HasValue ? interval.Value.Value : "1d";
        var handle = PolarsWrapper.DatetimeRange(
            realStart?.CloneHandle(),
            realEnd?.CloneHandle(),
            actualInterval,
            null,
            closed.ToNative(),
            unit.ToNative(),
            timeZone
        );

        return new Expr(handle);
    }
    /// <inheritdoc cref="DatetimeRange"/>
    public static Series DatetimeRangeAsSeries(
        IntoExprColumn? start = null, 
        IntoExprColumn? end = null, 
        IntoDuration? interval = null,
        ClosedInterval closed = ClosedInterval.Both,
        TimeUnit unit=TimeUnit.Microseconds,
        string? timeZone=null,
        string name="datetime")
    {
        var expr = DatetimeRange(start,end,interval,closed,unit,timeZone);
        Series series = Series(expr);
        series.Rename(name);
        return series;
    }
    /// <summary>
    /// Create a column of datetime ranges.
    /// </summary>
    /// <inheritdoc cref="DatetimeRange"/>
    public static Expr DatetimeRanges(
        IntoExprColumn? start = null, 
        IntoExprColumn? end = null, 
        IntoDuration? interval = null,
        ClosedInterval closed = ClosedInterval.Both,
        TimeUnit unit=TimeUnit.Microseconds,
        string? timeZone=null)
    {
        using var realStart = start?.Consume();
        using var realEnd = end?.Consume();
        string actualInterval = interval.HasValue ? interval.Value.Value : "1d";
        var handle = PolarsWrapper.DatetimeRanges(
            realStart?.CloneHandle(),
            realEnd?.CloneHandle(),
            actualInterval,
            null,
            closed.ToNative(),
            unit.ToNative(),
            timeZone
        );

        return new Expr(handle);
    }
    /// <inheritdoc cref="DatetimeRanges"/>
    public static Series DatetimeRangesAsSeries(
        IntoExprColumn? start = null, 
        IntoExprColumn? end = null, 
        IntoDuration? interval = null,
        ClosedInterval closed = ClosedInterval.Both,
        TimeUnit unit=TimeUnit.Microseconds,
        string? timeZone=null,
        string name="datetime")
    {
        var expr = DatetimeRanges(start,end,interval,closed,unit,timeZone);
        Series series = Series(expr);
        series.Rename(name);
        return series;
    }
}