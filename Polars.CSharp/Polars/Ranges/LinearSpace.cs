using Polars.NET.Core;

namespace Polars.CSharp;
public readonly partial struct Polars
{
    /// <summary>
    /// Generate a series of equally-spaced points.
    /// </summary>
    /// <param name="start">Lower bound of the linear space.</param>
    /// <param name="end">Upper bound of the linear space.</param>
    /// <param name="numSamples">Number of samples to generate.</param>
    /// <param name="closed">Whether the intervals are closed or open.</param>
    public static Expr LinearSpace(
        IntoExprColumn start, 
        IntoExprColumn end, 
        IntoExprColumn numSamples, 
        ClosedInterval closed = ClosedInterval.Both)
    {
        using var realStart = start.Consume();
        using var realEnd = end.Consume();
        using var realNumSamples = numSamples.Consume();

        var handle = PolarsWrapper.LinearSpace(
            realStart.CloneHandle(),
            realEnd.CloneHandle(),
            realNumSamples.CloneHandle(),
            closed.ToNative()
        );

        return new Expr(handle);
    }

    /// <inheritdoc cref="LinearSpace"/>
    public static Series LinearSpaceAsSeries(
        IntoExprColumn start, 
        IntoExprColumn end, 
        IntoExprColumn numSamples, 
        ClosedInterval closed = ClosedInterval.Both,
        string name = "linear_space")
    {
        var expr = LinearSpace(start, end, numSamples, closed);
        
        Series series = CreateSeries(expr);
        series.Rename(name);
        
        return series;
    }
    /// <summary>
    /// Create a column of linearly-spaced sequences for each row.
    /// </summary>
    /// <param name="start">Lower bound.</param>
    /// <param name="end">Upper bound.</param>
    /// <param name="numSamples">Number of samples.</param>
    /// <param name="closed">Whether the intervals are closed or open.</param>
    /// <param name="asArray">If true, returns an Array dtype instead of List. Requires numSamples to be a constant.</param>
    public static Expr LinearSpaces(
        IntoExprColumn start, 
        IntoExprColumn end, 
        IntoExprColumn numSamples, 
        ClosedInterval closed = ClosedInterval.Both,
        bool asArray = false)
    {
        using var realStart = start.Consume();
        using var realEnd = end.Consume();
        using var realNumSamples = numSamples.Consume();

        var handle = PolarsWrapper.LinearSpaces(
            realStart.CloneHandle(),
            realEnd.CloneHandle(),
            realNumSamples.CloneHandle(),
            closed.ToNative(),
            asArray
        );

        return new Expr(handle);
    }

    /// <inheritdoc cref="LinearSpaces"/>
    public static Series LinearSpacesAsSeries(
        IntoExprColumn start, 
        IntoExprColumn end, 
        IntoExprColumn numSamples, 
        ClosedInterval closed = ClosedInterval.Both,
        bool asArray = false,
        string name = "linear_spaces")
    {
        var expr = LinearSpaces(start, end, numSamples, closed, asArray);
        Series series = CreateSeries(expr);
        series.Rename(name);
        
        return series;
    }
}