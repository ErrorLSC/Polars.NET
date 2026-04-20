using Polars.NET.Core;

namespace Polars.CSharp;
public readonly partial struct Polars
{
    /// <summary>
    /// Generate a date range.
    /// </summary>
    /// <param name="start">Lower bound of the date range.</param>
    /// <param name="end">Upper bound of the date range.</param>
    /// <param name="interval">Interval of the range periods, “1w2d” # 1 week, 2 days.Default is 1 day.</param>
    /// <param name="closed">Define which sides of the range are closed</param>
    /// <returns>Column of data type Date</returns>
    public static Expr DateRange(
        IntoColumnExpr? start = null, 
        IntoColumnExpr? end = null, 
        IntoDuration? interval = null,
        ClosedWindow closed = ClosedWindow.Both)
    {
        using var realStart = start?.Consume();
        using var realEnd = end?.Consume();
        string actualInterval = interval.HasValue ? interval.Value.Value : "1d";
        var handle = PolarsWrapper.DateRange(
            realStart?.CloneHandle(),
            realEnd?.CloneHandle(),
            actualInterval,
            null,
            closed.ToNative()
        );

        return new Expr(handle);
    }
    /// <inheritdoc cref="DateRange"/>
    public static Series DateRangeAsSeries(
        IntoColumnExpr? start = null, 
        IntoColumnExpr? end = null, 
        IntoDuration? interval = null, 
        ClosedWindow closed = ClosedWindow.Both,
        string name="date")
    {
        var expr = DateRange(start,end,interval,closed);
        Series series = Series(expr);
        series.Rename(name);
        return series;
    }
    /// <summary>
    /// Create a column of date ranges.
    /// </summary>
    /// <param name="start">Lower bound of the date range.</param>
    /// <param name="end">Upper bound of the date range.</param>
    /// <param name="interval">Interval of the range periods, “1w2d” # 1 week, 2 days.Default is 1 day.</param>
    /// <param name="closed">Define which sides of the range are closed</param>
    /// <returns>Column of data type Date</returns>
    public static Expr DateRanges(
        IntoColumnExpr? start = null, 
        IntoColumnExpr? end = null, 
        IntoDuration? interval = null,
        ClosedWindow closed = ClosedWindow.Both)
    {
        using var realStart = start?.Consume();
        using var realEnd = end?.Consume();
        string actualInterval = interval.HasValue ? interval.Value.Value : "1d";
        var handle = PolarsWrapper.DateRanges(
            realStart?.CloneHandle(),
            realEnd?.CloneHandle(),
            actualInterval,
            null,
            closed.ToNative()
        );

        return new Expr(handle);
    }
    /// <inheritdoc cref="DateRanges"/>
    public static Series DateRangesAsSeries(
        IntoColumnExpr? start = null, 
        IntoColumnExpr? end = null, 
        IntoDuration? interval = null, 
        ClosedWindow closed = ClosedWindow.Both,
        string name="date")
    {
        var expr = DateRanges(start,end,interval,closed);
        Series series = Series(expr);
        series.Rename(name);
        return series;
    }
}