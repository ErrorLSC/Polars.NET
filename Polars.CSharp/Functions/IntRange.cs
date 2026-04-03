using Polars.NET.Core;

namespace Polars.CSharp;
public readonly partial struct Polars
{
    /// <summary>
    /// Generate a range of integers as an Expression.
    /// </summary>
    /// <param name="start">Start of the range (inclusive).</param>
    /// <param name="end">End of the range (exclusive). If set to Null (default), the value of start is used and start is set to 0.</param>
    /// <param name="step">Step size of the range.</param>
    /// <param name="dtype">Integer data type of the ranges. Defaults to Int64.</param>
    /// <returns>A Literal Expression containing the integer series.</returns>
    public static Expr IntRange(IntoExpr start, IntoExpr? end = null, long step = 1, DataType? dtype = null)
    {
        var actualDtype = dtype ?? DataType.Int64;
        
        using Expr realStart = end is null ? Lit(0) : start.Consume();
        using Expr realEnd = end is null ? start.Consume() : end.Value.Consume();

        return new(PolarsWrapper.IntRange(realStart.CloneHandle(), realEnd.CloneHandle(), step, actualDtype.Handle));
    }
    /// <summary>
    /// Generate a range of integers as a Series.
    /// </summary>
    /// <param name="start">Start of the range (inclusive).</param>
    /// <param name="end">End of the range (exclusive). If set to Null (default), the value of start is used and start is set to 0.</param>
    /// <param name="step">Step size of the range.</param>
    /// <param name="name">The name of generated series.</param>
    /// <param name="dtype">Integer data type of the ranges. Defaults to Int64.</param>
    /// <returns>A Literal Expression containing the integer series.</returns>
    public static Series IntRangeAsSeries(IntoExpr start, IntoExpr? end=null, long step = 1, string name = "int", DataType? dtype = null)
    {
        var expr = IntRange(start,end,step,dtype);
        using var df = new DataFrame().WithColumns(expr);
        var series = df[0];
        series.Rename(name);
        return series;
    }

    /// <summary>
    /// Generate a range of integers for each row of the input columns.
    /// Resulting column is of dtype List(dtype).
    /// </summary>
    public static Expr IntRanges(IntoExpr start, IntoExpr? end = null, IntoExpr? step = null, DataType? dtype = null)
    {
        var actualDtype = dtype ?? DataType.Int64;

        using Expr realStart = end is null ? Lit(0) : start.Consume();
        using Expr realEnd = end is null ? start.Consume() : end.Value.Consume();

        using Expr realStep = step is null ? Lit(1) : step.Value.Consume();

        return new Expr(PolarsWrapper.IntRanges(
            realStart.CloneHandle(), 
            realEnd.CloneHandle(), 
            realStep.CloneHandle(), 
            actualDtype.Handle 
        ));
    }
    /// <inheritdoc cref="IntRanges"/>
    public static Series IntRangesAsSeries(IntoExpr start, IntoExpr? end=null, IntoExpr? step = null, string name = "int", DataType? dtype = null)
    {
        var expr = IntRanges(start,end,step,dtype);
        using var df = new DataFrame().WithColumns(expr);
        var series = df[0];
        series.Rename(name);
        return series;
    }
}