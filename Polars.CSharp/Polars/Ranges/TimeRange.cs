using Polars.NET.Core;

namespace Polars.CSharp;
public readonly partial struct Polars
{
    /// <summary>
    /// Generate a time range.
    /// </summary>
    /// <param name="start">Lower bound of the time range. If omitted, defaults to TimeOnly.MinValue</param>
    /// <param name="end">Upper bound of the time range. If omitted, defaults to TimeOnly.MaxValue</param>
    /// <param name="interval">Interval of the range periods</param>
    /// <param name="closed">Define which sides of the range are closed.</param>
    /// <returns></returns>
    public static Expr TimeRange(
        IntoExpr? start = null, 
        IntoExpr? end = null, 
        IntoDuration? interval = null, 
        ClosedWindow closed = ClosedWindow.Both)
    {
        using Expr realStart = start?.Consume() ?? Lit(TimeOnly.MinValue);
        using Expr realEnd = end?.Consume() ?? Lit(TimeOnly.MaxValue);
        
        string actualInterval = interval.HasValue ? interval.Value.Value : "1h"; 

        var handle = PolarsWrapper.TimeRange(
            realStart.CloneHandle(), 
            realEnd.CloneHandle(), 
            actualInterval, 
            closed.ToNative()
        );

        return new Expr(handle);
    }
    /// <inheritdoc cref="TimeRange"/>
    public static Series TimeRangeAsSeries(
        IntoExpr? start = null, 
        IntoExpr? end = null, 
        IntoDuration? interval = null, 
        ClosedWindow closed = ClosedWindow.Both,
        string name = "time")
    {
        var expr = TimeRange(start,end,interval,closed);
        Series series = Series(expr);
        series.Rename(name);
        return series;
    }
    
    /// <summary>
    /// Create a column of time ranges.
    /// </summary>
    /// <inheritdoc cref="TimeRange"/>
    public static Expr TimeRanges(
        IntoExpr? start = null, 
        IntoExpr? end = null, 
        IntoDuration? interval = null, 
        ClosedWindow closed = ClosedWindow.Both)
    {
        using Expr realStart = start?.Consume() ?? Lit(TimeOnly.MinValue);
        using Expr realEnd = end?.Consume() ?? Lit(TimeOnly.MaxValue);
        
        string actualInterval = interval.HasValue ? interval.Value.Value : "1h";

        var handle = PolarsWrapper.TimeRanges(
            realStart.CloneHandle(), 
            realEnd.CloneHandle(), 
            actualInterval, 
            closed.ToNative()
        );

        return new Expr(handle);
    }
    /// <inheritdoc cref="TimeRanges"/>
    public static Series TimeRangesAsSeries(
        IntoExpr? start = null, 
        IntoExpr? end = null, 
        IntoDuration? interval = null, 
        ClosedWindow closed = ClosedWindow.Both,
        string name = "time")
    {
        var expr = TimeRanges(start,end,interval,closed);
        Series series = Series(expr);
        series.Rename(name);
        return series;
    }
}