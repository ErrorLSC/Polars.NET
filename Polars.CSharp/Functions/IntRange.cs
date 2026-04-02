#pragma warning disable CS1591
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
    public static Expr IntRange(Expr start, Expr? end = null, long step = 1, DataType? dtype = null)
    {
        var actualDtype = dtype ?? DataType.Int64;
        
        Expr realStart;
        Expr realEnd;

        if (end is null)
        {
            realStart = Lit(0);
            realEnd = start;
        }
        else
        {
            realStart = start;
            realEnd = end;
        }

        return new(PolarsWrapper.IntRange(realStart.CloneHandle(), realEnd.CloneHandle(), step, actualDtype.Handle));
    }

    /// <summary>
    /// IntRange (Convenience Overload for int)
    /// </summary>
    public static Expr IntRange(int start, int? end = null, int step = 1, DataType? dtype = null)
    {
        if (end is null)
        {
            return IntRange(Lit(0), Lit(start), step, dtype);
        }
        
        return IntRange(Lit(start), Lit(end.Value), step, dtype);
    }

    public static Series IntRangeAsSeries(Expr start, Expr? end=null, long step = 1, string name = "int", DataType? dtype = null)
    {
        var expr = IntRange(start,end,step,dtype);
        using var df = new DataFrame().WithColumns(expr);
        var series = df[0];
        series.Rename(name);
        return series;
    }

    /// <summary>
    /// IntRangeAsSeries (Convenience Overload for int)
    /// </summary>
    public static Series IntRangeAsSeries(int start, int? end=null, int step = 1, string name = "int", DataType? dtype = null)
        => IntRangeAsSeries(start, end, (long)step, name, dtype);
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

    /// <summary>
    /// IntRanges (Convenience Overload for int)
    /// </summary>
    public static Expr IntRanges(int start, int? end = null, int step = 1, DataType? dtype = null)
    {
        if (end is null)
        {
            return IntRanges(Lit(0), Lit(start), Lit(step), dtype);
        }
        
        return IntRanges(Lit(start), Lit(end.Value), Lit(step), dtype);
    }
}