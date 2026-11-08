using System.Linq.Expressions;

namespace Polars.CSharp.Linq;

/// <summary>
/// Provides LINQ IEnumerable extensions for C# Polars DataFrame and LazyFrame.
/// </summary>
public static class GroupAggregationExtensions
{
    /// <summary>
    /// Computes sample standard deviation of a group by selector with default ddof = 1.
    /// Null values are skipped; the result is null when the group has no more than ddof non-null values.
    /// </summary>
    public static double? Std<TSource, TResult>(
        this IEnumerable<TSource> source,
        Expression<Func<TSource, TResult>> selector)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes standard deviation of a group by selector with explicit degree of freedom (ddof).
    /// </summary>
    public static double? Std<TSource, TResult>(
        this IEnumerable<TSource> source,
        Expression<Func<TSource, TResult>> selector,
        byte ddof)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes sample variance of a group by selector with default ddof = 1.
    /// Null values are skipped; the result is null when the group has no more than ddof non-null values.
    /// </summary>
    public static double? Var<TSource, TResult>(
        this IEnumerable<TSource> source,
        Expression<Func<TSource, TResult>> selector)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Computes variance of a group by selector with explicit degree of freedom (ddof).
    /// </summary>
    public static double? Var<TSource, TResult>(
        this IEnumerable<TSource> source,
        Expression<Func<TSource, TResult>> selector,
        byte ddof)
        => throw MarkerExceptionHelper.Throw();
}
