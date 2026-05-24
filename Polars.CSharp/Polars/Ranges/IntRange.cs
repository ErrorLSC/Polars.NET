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
    /// <param name="datatype">Integer data type of the ranges. Defaults to Int64.</param>
    /// <returns>A Literal Expression containing the integer series.</returns>
    public static Expr IntRange(IntoExprColumn start, IntoExprColumn? end = null, long step = 1, IntoDataTypeExpr? datatype = null)
    {
        IntoDataTypeExpr resolvedDtype = datatype ?? DataType.Int64;
        
        using DataTypeExpr actualDtypeExpr = resolvedDtype.Consume();
        
        using Expr realStart = end is null ? Lit(0) : start.Consume();
        using Expr realEnd = end is null ? start.Consume() : end.Value.Consume();

        Expr expr = new(PolarsWrapper.IntRange(
            realStart.CloneHandle(), 
            realEnd.CloneHandle(), 
            step, 
            actualDtypeExpr.Handle 
        ));
        return expr.SetSorted(descending: step < 0);
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
    public static Series IntRangeAsSeries(IntoExprColumn start, IntoExprColumn? end=null, long step = 1, string name = "int", IntoDataTypeExpr? dtype = null)
    {
        var expr = IntRange(start,end,step,dtype);
        Series series = Series(expr);
        series.Rename(name);
        return series.SetSorted(descending:step < 0);
    }

    /// <summary>
    /// Generate a range of integers for each row of the input columns.
    /// Resulting column is of dtype List(dtype).
    /// </summary>
    public static Expr IntRanges(IntoExprColumn start, IntoExprColumn? end = null, IntoExprColumn? step = null, IntoDataTypeExpr? datatype = null)
    {
        IntoDataTypeExpr resolvedDtype = datatype ?? DataType.Int64;

        using DataTypeExpr actualDtypeExpr = resolvedDtype.Consume();

        using Expr realStart = end is null ? Lit(0) : start.Consume();
        using Expr realEnd = end is null ? start.Consume() : end.Value.Consume();

        using Expr realStep = step is null ? Lit(1) : step.Value.Consume();

        return new Expr(PolarsWrapper.IntRanges(
            realStart.CloneHandle(), 
            realEnd.CloneHandle(), 
            realStep.CloneHandle(), 
            actualDtypeExpr.Handle 
        ));
    }
    /// <inheritdoc cref="IntRanges"/>
    public static Series IntRangesAsSeries(IntoExprColumn start, IntoExprColumn? end=null, IntoExprColumn? step = null, string name = "int", IntoDataTypeExpr? datatype = null)
    {
        var expr = IntRanges(start,end,step,datatype);
        Series series = Series(expr);
        series.Rename(name);
        return series;
    }
}