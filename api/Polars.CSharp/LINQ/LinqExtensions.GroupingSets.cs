using System.Linq.Expressions;

namespace Polars.CSharp.Linq;

public static partial class LinqExtensions
{
    // =========================================================================
    // Multidimensional Grouping Extensions (ROLLUP, CUBE, GROUPING SETS)
    // Extended on IQueryable<IGrouping<TKey, TSource>>
    // =========================================================================

    // -------------------------------------------------------------------------
    // ROLLUP
    // -------------------------------------------------------------------------

    /// <summary>
    /// Modifies a grouped sequence to generate hierarchical subtotal and grand total groupings (ROLLUP) across key dimensions.
    /// Follow with <c>.Select(...)</c>, the only operation available, to specify projections and aggregations.
    /// </summary>
    public static GroupingSetsQuery<TKey, TSource> RollUp<TKey, TSource>(
        this IQueryable<IGrouping<TKey, TSource>> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        return new GroupingSetsQuery<TKey, TSource>(
            source.Provider,
            Expression.Call(
                null,
                new Func<IQueryable<IGrouping<TKey, TSource>>, IQueryable<IGrouping<TKey, TSource>>>(GroupingSetsMarkers.RollUp<TKey, TSource>).Method,
                source.Expression
            )
        );
    }

    /// <inheritdoc cref="RollUp{TKey, TSource}(IQueryable{IGrouping{TKey, TSource}})"/>
    public static GroupingSetsQuery<TKey, TSource> Rollup<TKey, TSource>(
        this IQueryable<IGrouping<TKey, TSource>> source)
        => source.RollUp();

    /// <summary>
    /// Modifies a grouped sequence to generate hierarchical subtotal and grand total groupings (ROLLUP) across key dimensions,
    /// projecting each group into a result element using a result selector function with separate key and group parameters.
    /// </summary>
    public static IQueryable<TResult> RollUp<TKey, TSource, TResult>(
        this IQueryable<IGrouping<TKey, TSource>> source,
        Expression<Func<TKey, IEnumerable<TSource>, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(resultSelector);

        return source.Provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                new Func<IQueryable<IGrouping<TKey, TSource>>, Expression<Func<TKey, IEnumerable<TSource>, TResult>>, IQueryable<TResult>>(RollUp<TKey, TSource, TResult>).Method,
                source.Expression,
                Expression.Quote(resultSelector)
            )
        );
    }

    /// <inheritdoc cref="RollUp{TKey, TSource, TResult}(IQueryable{IGrouping{TKey, TSource}}, Expression{Func{TKey, IEnumerable{TSource}, TResult}})"/>
    public static IQueryable<TResult> Rollup<TKey, TSource, TResult>(
        this IQueryable<IGrouping<TKey, TSource>> source,
        Expression<Func<TKey, IEnumerable<TSource>, TResult>> resultSelector)
        => source.RollUp(resultSelector);

    /// <summary>
    /// Modifies a grouped sequence to generate hierarchical subtotal and grand total groupings (ROLLUP) across key dimensions,
    /// projecting each group into a result element using a result selector function.
    /// </summary>
    public static IQueryable<TResult> RollUp<TKey, TSource, TResult>(
        this IQueryable<IGrouping<TKey, TSource>> source,
        Expression<Func<IGrouping<TKey, TSource>, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(resultSelector);

        return source.Provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                new Func<IQueryable<IGrouping<TKey, TSource>>, Expression<Func<IGrouping<TKey, TSource>, TResult>>, IQueryable<TResult>>(RollUp<TKey, TSource, TResult>).Method,
                source.Expression,
                Expression.Quote(resultSelector)
            )
        );
    }

    /// <inheritdoc cref="RollUp{TKey, TSource, TResult}(IQueryable{IGrouping{TKey, TSource}}, Expression{Func{IGrouping{TKey, TSource}, TResult}})"/>
    public static IQueryable<TResult> Rollup<TKey, TSource, TResult>(
        this IQueryable<IGrouping<TKey, TSource>> source,
        Expression<Func<IGrouping<TKey, TSource>, TResult>> resultSelector)
        => source.RollUp(resultSelector);

    // -------------------------------------------------------------------------
    // CUBE
    // -------------------------------------------------------------------------

    /// <summary>
    /// Modifies a grouped sequence to generate all 2^N dimensional cross-tabulation groupings (CUBE) across key dimensions.
    /// Follow with <c>.Select(...)</c>, the only operation available, to specify projections and aggregations.
    /// </summary>
    public static GroupingSetsQuery<TKey, TSource> Cube<TKey, TSource>(
        this IQueryable<IGrouping<TKey, TSource>> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        return new GroupingSetsQuery<TKey, TSource>(
            source.Provider,
            Expression.Call(
                null,
                new Func<IQueryable<IGrouping<TKey, TSource>>, IQueryable<IGrouping<TKey, TSource>>>(GroupingSetsMarkers.Cube<TKey, TSource>).Method,
                source.Expression
            )
        );
    }

    /// <summary>
    /// Modifies a grouped sequence to generate all 2^N dimensional cross-tabulation groupings (CUBE) across key dimensions,
    /// projecting each group into a result element using a result selector function with separate key and group parameters.
    /// </summary>
    public static IQueryable<TResult> Cube<TKey, TSource, TResult>(
        this IQueryable<IGrouping<TKey, TSource>> source,
        Expression<Func<TKey, IEnumerable<TSource>, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(resultSelector);

        return source.Provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                new Func<IQueryable<IGrouping<TKey, TSource>>, Expression<Func<TKey, IEnumerable<TSource>, TResult>>, IQueryable<TResult>>(Cube<TKey, TSource, TResult>).Method,
                source.Expression,
                Expression.Quote(resultSelector)
            )
        );
    }

    /// <summary>
    /// Modifies a grouped sequence to generate all 2^N dimensional cross-tabulation groupings (CUBE) across key dimensions,
    /// projecting each group into a result element using a result selector function.
    /// </summary>
    public static IQueryable<TResult> Cube<TKey, TSource, TResult>(
        this IQueryable<IGrouping<TKey, TSource>> source,
        Expression<Func<IGrouping<TKey, TSource>, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(resultSelector);

        return source.Provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                new Func<IQueryable<IGrouping<TKey, TSource>>, Expression<Func<IGrouping<TKey, TSource>, TResult>>, IQueryable<TResult>>(Cube<TKey, TSource, TResult>).Method,
                source.Expression,
                Expression.Quote(resultSelector)
            )
        );
    }

    // -------------------------------------------------------------------------
    // GROUPING SETS
    // -------------------------------------------------------------------------

    /// <summary>
    /// Modifies a grouped sequence to generate custom multidimensional groupings (GROUPING SETS) for specified subsets of keys.
    /// Follow with <c>.Select(...)</c>, the only operation available, to specify projections and aggregations.
    /// </summary>
    public static GroupingSetsQuery<TKey, TSource> GroupingSets<TKey, TSource>(
        this IQueryable<IGrouping<TKey, TSource>> source,
        string[][] sets)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(sets);

        return new GroupingSetsQuery<TKey, TSource>(
            source.Provider,
            Expression.Call(
                null,
                new Func<IQueryable<IGrouping<TKey, TSource>>, string[][], IQueryable<IGrouping<TKey, TSource>>>(GroupingSetsMarkers.GroupingSets<TKey, TSource>).Method,
                source.Expression,
                Expression.Constant(sets, typeof(string[][]))
            )
        );
    }

    /// <summary>
    /// Modifies a grouped sequence to generate custom multidimensional groupings (GROUPING SETS) for specified subsets of keys using strongly-typed key selectors.
    /// Use <c>k => new { k.A, k.B }</c> for composite subsets, or <c>k => new { }</c> for the grand-total empty set ().
    /// Follow with <c>.Select(...)</c>, the only operation available, to specify projections and aggregations.
    /// </summary>
    public static GroupingSetsQuery<TKey, TSource> GroupingSets<TKey, TSource>(
        this IQueryable<IGrouping<TKey, TSource>> source,
        params Expression<Func<TKey, object>>[] setSelectors)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(setSelectors);

        return new GroupingSetsQuery<TKey, TSource>(
            source.Provider,
            Expression.Call(
                null,
                new Func<IQueryable<IGrouping<TKey, TSource>>, Expression<Func<TKey, object>>[], IQueryable<IGrouping<TKey, TSource>>>(GroupingSetsMarkers.GroupingSets<TKey, TSource>).Method,
                source.Expression,
                Expression.Constant(setSelectors, typeof(Expression<Func<TKey, object>>[]))
            )
        );
    }

    /// <summary>
    /// Modifies a grouped sequence to generate custom multidimensional groupings (GROUPING SETS) for specified subsets of keys,
    /// projecting each group into a result element using a result selector function with separate key and group parameters.
    /// </summary>
    public static IQueryable<TResult> GroupingSets<TKey, TSource, TResult>(
        this IQueryable<IGrouping<TKey, TSource>> source,
        string[][] sets,
        Expression<Func<TKey, IEnumerable<TSource>, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(sets);
        ArgumentNullException.ThrowIfNull(resultSelector);

        return source.Provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                new Func<IQueryable<IGrouping<TKey, TSource>>, string[][], Expression<Func<TKey, IEnumerable<TSource>, TResult>>, IQueryable<TResult>>(GroupingSets<TKey, TSource, TResult>).Method,
                source.Expression,
                Expression.Constant(sets, typeof(string[][])),
                Expression.Quote(resultSelector)
            )
        );
    }

    /// <summary>
    /// Modifies a grouped sequence to generate custom multidimensional groupings (GROUPING SETS) for specified subsets of keys using strongly-typed key selectors,
    /// projecting each group into a result element using a result selector function with separate key and group parameters.
    /// Use <c>k => new { k.A, k.B }</c> for composite subsets, or <c>k => new { }</c> for the grand-total empty set ().
    /// </summary>
    public static IQueryable<TResult> GroupingSets<TKey, TSource, TResult>(
        this IQueryable<IGrouping<TKey, TSource>> source,
        Expression<Func<TKey, object>>[] setSelectors,
        Expression<Func<TKey, IEnumerable<TSource>, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(setSelectors);
        ArgumentNullException.ThrowIfNull(resultSelector);

        return source.Provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                new Func<IQueryable<IGrouping<TKey, TSource>>, Expression<Func<TKey, object>>[], Expression<Func<TKey, IEnumerable<TSource>, TResult>>, IQueryable<TResult>>(GroupingSets<TKey, TSource, TResult>).Method,
                source.Expression,
                Expression.Constant(setSelectors, typeof(Expression<Func<TKey, object>>[])),
                Expression.Quote(resultSelector)
            )
        );
    }

    /// <summary>
    /// Modifies a grouped sequence to generate custom multidimensional groupings (GROUPING SETS) for specified subsets of keys,
    /// projecting each group into a result element using a result selector function.
    /// </summary>
    public static IQueryable<TResult> GroupingSets<TKey, TSource, TResult>(
        this IQueryable<IGrouping<TKey, TSource>> source,
        string[][] sets,
        Expression<Func<IGrouping<TKey, TSource>, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(sets);
        ArgumentNullException.ThrowIfNull(resultSelector);

        return source.Provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                new Func<IQueryable<IGrouping<TKey, TSource>>, string[][], Expression<Func<IGrouping<TKey, TSource>, TResult>>, IQueryable<TResult>>(GroupingSets<TKey, TSource, TResult>).Method,
                source.Expression,
                Expression.Constant(sets, typeof(string[][])),
                Expression.Quote(resultSelector)
            )
        );
    }

    /// <summary>
    /// Modifies a grouped sequence to generate custom multidimensional groupings (GROUPING SETS) for specified subsets of keys using strongly-typed key selectors,
    /// projecting each group into a result element using a result selector function.
    /// Use <c>k => new { k.A, k.B }</c> for composite subsets, or <c>k => new { }</c> for the grand-total empty set ().
    /// </summary>
    public static IQueryable<TResult> GroupingSets<TKey, TSource, TResult>(
        this IQueryable<IGrouping<TKey, TSource>> source,
        Expression<Func<TKey, object>>[] setSelectors,
        Expression<Func<IGrouping<TKey, TSource>, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(setSelectors);
        ArgumentNullException.ThrowIfNull(resultSelector);

        return source.Provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                new Func<IQueryable<IGrouping<TKey, TSource>>, Expression<Func<TKey, object>>[], Expression<Func<IGrouping<TKey, TSource>, TResult>>, IQueryable<TResult>>(GroupingSets<TKey, TSource, TResult>).Method,
                source.Expression,
                Expression.Constant(setSelectors, typeof(Expression<Func<TKey, object>>[])),
                Expression.Quote(resultSelector)
            )
        );
    }

    /// <summary>
    /// Maps to the SQL-99 standard GROUPING(col) function.
    /// Returns 1 when the specified column is aggregated/collapsed (subtotal/grand total row) in a ROLLUP, CUBE, or GROUPING SETS query; otherwise returns 0.
    /// Can only be used inside the result/projection selector of GroupingSets, RollUp, or Cube.
    /// </summary>
    /// <typeparam name="TProp">The type of the grouping key property.</typeparam>
    /// <param name="property">The grouping key property.</param>
    /// <returns>1 if the column is collapsed in the current subtotal level; otherwise 0.</returns>
    public static int Grouping<TProp>(this TProp property)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Maps to the SQL-99 standard GROUPING(col) function using a strongly-typed key property selector.
    /// Returns 1 when the specified column is aggregated/collapsed (subtotal/grand total row) in a ROLLUP, CUBE, or GROUPING SETS query; otherwise returns 0.
    /// Can only be used inside the result/projection selector of GroupingSets, RollUp, or Cube.
    /// </summary>
    /// <typeparam name="TKey">The type of the grouping key.</typeparam>
    /// <typeparam name="TProp">The type of the grouping key property.</typeparam>
    /// <param name="key">The grouping key.</param>
    /// <param name="propertySelector">A selector selecting the key property.</param>
    /// <returns>1 if the column is collapsed in the current subtotal level; otherwise 0.</returns>
    public static int Grouping<TKey, TProp>(this TKey key, Expression<Func<TKey, TProp>> propertySelector)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Maps to the SQL GROUPING_ID() function over every grouping key, in key order.
    /// Returns a bit vector with one bit per key, the first key being the most significant bit; a bit is 1 when
    /// its key is aggregated/collapsed (subtotal/grand total row) at the current grouping level.
    /// Can only be used inside the result/projection selector of GroupingSets, RollUp, or Cube, applied to the whole key
    /// (<c>key</c> in a two-parameter selector, <c>g.Key</c> otherwise).
    /// </summary>
    /// <typeparam name="TKey">The type of the grouping key.</typeparam>
    /// <param name="key">The grouping key.</param>
    /// <returns>The grouping bit vector of the current subtotal level.</returns>
    public static int GroupingId<TKey>(this TKey key)
        => throw MarkerExceptionHelper.Throw();

    /// <summary>
    /// Maps to the SQL GROUPING_ID(col1, col2, ...) function over the key properties listed by the selector.
    /// Use <c>k => new { k.A, k.B }</c> for several properties or <c>k => k.A</c> for one. The result has one bit per
    /// listed property, the first being the most significant bit; a bit is 1 when its property is aggregated/collapsed
    /// (subtotal/grand total row) at the current grouping level.
    /// Can only be used inside the result/projection selector of GroupingSets, RollUp, or Cube, applied to the whole key
    /// (<c>key</c> in a two-parameter selector, <c>g.Key</c> otherwise).
    /// </summary>
    /// <typeparam name="TKey">The type of the grouping key.</typeparam>
    /// <param name="key">The grouping key.</param>
    /// <param name="columns">A selector listing the key properties, in bit order.</param>
    /// <returns>The grouping bit vector of the current subtotal level.</returns>
    public static int GroupingId<TKey>(this TKey key, Expression<Func<TKey, object>> columns)
        => throw MarkerExceptionHelper.Throw();
}
