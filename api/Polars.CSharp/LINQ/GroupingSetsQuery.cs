using System.Linq.Expressions;

namespace Polars.CSharp.Linq;

/// <summary>
/// A grouped sequence modified by <c>RollUp()</c>, <c>Cube()</c> or <c>GroupingSets(...)</c>, waiting for its projection.
/// The only operation is <see cref="Select{TResult}"/>, which projects each group of every grouping level; filter,
/// sort or page the projected rows afterwards. Query syntax works too: <c>from g in q.Cube() select new { ... }</c>.
/// </summary>
/// <typeparam name="TKey">The type of the grouping key.</typeparam>
/// <typeparam name="TSource">The type of the grouped elements.</typeparam>
public sealed class GroupingSetsQuery<TKey, TSource>
{
    private readonly IQueryProvider _provider;
    private readonly Expression _expression;

    internal GroupingSetsQuery(IQueryProvider provider, Expression expression)
    {
        _provider = provider;
        _expression = expression;
    }

    /// <summary>
    /// Projects each group of every grouping level (subtotals and grand totals included) into a result element.
    /// Inside the selector, <c>g.Key</c> members outside the current level are null; use
    /// <see cref="LinqExtensions.Grouping{TProp}(TProp)"/> or <see cref="LinqExtensions.GroupingId{TKey}(TKey)"/>
    /// to tell them apart from null key values.
    /// </summary>
    public IQueryable<TResult> Select<TResult>(Expression<Func<IGrouping<TKey, TSource>, TResult>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        return _provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                new Func<IQueryable<IGrouping<TKey, TSource>>, Expression<Func<IGrouping<TKey, TSource>, TResult>>, IQueryable<TResult>>(Queryable.Select).Method,
                _expression,
                Expression.Quote(selector)
            )
        );
    }
}

/// <summary>
/// The expression-tree nodes of the parameterless <c>RollUp()</c>, <c>Cube()</c> and <c>GroupingSets(...)</c>. They keep
/// the <see cref="IQueryable{T}"/> type <c>Queryable.Select</c> needs, and the names the query translator recognizes.
/// They are never executed: the translator rejects them unless a Select follows.
/// </summary>
internal static class GroupingSetsMarkers
{
    internal static IQueryable<IGrouping<TKey, TSource>> RollUp<TKey, TSource>(IQueryable<IGrouping<TKey, TSource>> source)
        => throw MarkerExceptionHelper.Throw();

    internal static IQueryable<IGrouping<TKey, TSource>> Cube<TKey, TSource>(IQueryable<IGrouping<TKey, TSource>> source)
        => throw MarkerExceptionHelper.Throw();

    internal static IQueryable<IGrouping<TKey, TSource>> GroupingSets<TKey, TSource>(IQueryable<IGrouping<TKey, TSource>> source, string[][] sets)
        => throw MarkerExceptionHelper.Throw();

    internal static IQueryable<IGrouping<TKey, TSource>> GroupingSets<TKey, TSource>(IQueryable<IGrouping<TKey, TSource>> source, Expression<Func<TKey, object>>[] setSelectors)
        => throw MarkerExceptionHelper.Throw();
}
