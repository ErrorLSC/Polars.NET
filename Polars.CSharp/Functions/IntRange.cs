#pragma warning disable CS1591
using Polars.NET.Core;

namespace Polars.CSharp;
public readonly partial struct Polars
{
    /// <summary>
    /// Generate a range of integers as an Expression.
    /// <para>
    /// This evaluates immediately to a Series wrapped in a Literal Expression
    /// </para>
    /// </summary>
    /// <param name="start">Start of the range (inclusive).</param>
    /// <param name="end">End of the range (exclusive).</param>
    /// <param name="step">Step size of the range.</param>
    /// <param name="name">Name of the generated series.</param>
    /// <returns>A Literal Expression containing the integer series.</returns>
    public static Expr IntRange(long start, long end, long step = 1, string name = "int")
    {
        if (step == 0)
            throw new ArgumentException("Step must not be zero.", nameof(step));

        long count = 0;
        if (step > 0 && end > start)
        {
            count = (end - start + step - 1) / step;
        }
        else if (step < 0 && end < start)
        {
            count = (start - end + step + 1) / step; 
        }

        if (count <= 0)
        {
            using var emptySeries = new Series(name, Array.Empty<long>());
            return Lit(emptySeries);
        }

        long[] arr = new long[count];
        long current = start;

        for (long i = 0; i < count; i++)
        {
            arr[i] = current;
            current += step;
        }

        var series = new Series(name, arr);
        
        // TODO: 如果未来 C# API 开放了 series.SetSortedFlag()，可在此处加上
        // series.SetSortedFlag(step > 0 ? IsSorted.Ascending : IsSorted.Descending);

        return Lit(series);
    }

    /// <summary>
    /// IntRange (Convenience Overload for int)
    /// </summary>
    public static Expr IntRange(int start, int end, int step = 1, string name = "int")
        => IntRange(start, end, (long)step, name);

    public static Series IntRangeAsSeries(long start, long end, long step = 1, string name = "int")
    {
        if (step == 0)
            throw new ArgumentException("Step must not be zero.", nameof(step));

        long count = 0;
        if (step > 0 && end > start)
        {
            count = (end - start + step - 1) / step;
        }
        else if (step < 0 && end < start)
        {
            count = (start - end + step + 1) / step; 
        }

        if (count <= 0)
        {
            using var emptySeries = new Series(name, Array.Empty<long>());
            return emptySeries;
        }

        long[] arr = new long[count];
        long current = start;

        for (long i = 0; i < count; i++)
        {
            arr[i] = current;
            current += step;
        }

        var series = new Series(name, arr);
        
        // TODO: 如果未来 C# API 开放了 series.SetSortedFlag()，可在此处加上
        // series.SetSortedFlag(step > 0 ? IsSorted.Ascending : IsSorted.Descending);

        return series;
    }

    /// <summary>
    /// IntRangeAsSeries (Convenience Overload for int)
    /// </summary>
    public static Series IntRangeAsSeries(int start, int end, int step = 1, string name = "int")
        => IntRangeAsSeries(start, end, (long)step, name);
}