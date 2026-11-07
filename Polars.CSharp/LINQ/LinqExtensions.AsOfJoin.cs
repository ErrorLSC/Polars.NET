using System.Linq.Expressions;

namespace Polars.CSharp.Linq;

public static partial class LinqExtensions
{
    private static Expression GetSourceExpression<T>(IEnumerable<T> source)
    {
        return source is IQueryable queryable
            ? queryable.Expression
            : Expression.Constant(source, typeof(IEnumerable<T>));
    }

    /// <summary>
    /// Performs an AsOf (time-series tolerance) join on the specified key columns.
    /// </summary>
    public static IQueryable<TResult> JoinAsOf<TOuter, TInner, TKey, TResult>(
        this IQueryable<TOuter> outer,
        IEnumerable<TInner> inner,
        Expression<Func<TOuter, TKey>> outerKeySelector,
        Expression<Func<TInner, TKey>> innerKeySelector,
        Expression<Func<TOuter, TInner, TResult>> resultSelector,
        AsOfTolerance? tolerance = null,
        AsofStrategy strategy = AsofStrategy.Backward)
    {
        ArgumentNullException.ThrowIfNull(outer);
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(outerKeySelector);
        ArgumentNullException.ThrowIfNull(innerKeySelector);
        ArgumentNullException.ThrowIfNull(resultSelector);

        return outer.Provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                new Func<IQueryable<TOuter>, IEnumerable<TInner>, Expression<Func<TOuter, TKey>>, Expression<Func<TInner, TKey>>, Expression<Func<TOuter, TInner, TResult>>, AsOfTolerance?, AsofStrategy, IQueryable<TResult>>(JoinAsOf<TOuter, TInner, TKey, TResult>).Method,
                outer.Expression,
                GetSourceExpression(inner),
                Expression.Quote(outerKeySelector),
                Expression.Quote(innerKeySelector),
                Expression.Quote(resultSelector),
                Expression.Constant(tolerance, typeof(AsOfTolerance?)),
                Expression.Constant(strategy, typeof(AsofStrategy))
            )
        );
    }

    /// <summary>
    /// Performs an AsOf (time-series tolerance) join on the specified key columns, partitioned by group keys.
    /// </summary>
    public static IQueryable<TResult> JoinAsOf<TOuter, TInner, TKey, TBy, TResult>(
        this IQueryable<TOuter> outer,
        IEnumerable<TInner> inner,
        Expression<Func<TOuter, TKey>> outerKeySelector,
        Expression<Func<TInner, TKey>> innerKeySelector,
        Expression<Func<TOuter, TBy>> outerBySelector,
        Expression<Func<TInner, TBy>> innerBySelector,
        Expression<Func<TOuter, TInner, TResult>> resultSelector,
        AsOfTolerance? tolerance = null,
        AsofStrategy strategy = AsofStrategy.Backward)
    {
        ArgumentNullException.ThrowIfNull(outer);
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(outerKeySelector);
        ArgumentNullException.ThrowIfNull(innerKeySelector);
        ArgumentNullException.ThrowIfNull(outerBySelector);
        ArgumentNullException.ThrowIfNull(innerBySelector);
        ArgumentNullException.ThrowIfNull(resultSelector);

        return outer.Provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                new Func<IQueryable<TOuter>, IEnumerable<TInner>, Expression<Func<TOuter, TKey>>, Expression<Func<TInner, TKey>>, Expression<Func<TOuter, TBy>>, Expression<Func<TInner, TBy>>, Expression<Func<TOuter, TInner, TResult>>, AsOfTolerance?, AsofStrategy, IQueryable<TResult>>(JoinAsOf<TOuter, TInner, TKey, TBy, TResult>).Method,
                outer.Expression,
                GetSourceExpression(inner),
                Expression.Quote(outerKeySelector),
                Expression.Quote(innerKeySelector),
                Expression.Quote(outerBySelector),
                Expression.Quote(innerBySelector),
                Expression.Quote(resultSelector),
                Expression.Constant(tolerance, typeof(AsOfTolerance?)),
                Expression.Constant(strategy, typeof(AsofStrategy))
            )
        );
    }

    /// <summary>
    /// Performs an AsOf (time-series tolerance) join on the specified key columns, pairing outer and inner rows into a ValueTuple.
    /// </summary>
    public static IQueryable<(TOuter Outer, TInner Inner)> JoinAsOf<TOuter, TInner, TKey>(
        this IQueryable<TOuter> outer,
        IEnumerable<TInner> inner,
        Expression<Func<TOuter, TKey>> outerKeySelector,
        Expression<Func<TInner, TKey>> innerKeySelector,
        AsOfTolerance? tolerance = null,
        AsofStrategy strategy = AsofStrategy.Backward)
        => outer.JoinAsOf(inner, outerKeySelector, innerKeySelector, (o, i) => ValueTuple.Create(o, i), tolerance, strategy);

    /// <summary>
    /// Performs an AsOf (time-series tolerance) join on the specified key columns partitioned by group keys, pairing outer and inner rows into a ValueTuple.
    /// </summary>
    public static IQueryable<(TOuter Outer, TInner Inner)> JoinAsOf<TOuter, TInner, TKey, TBy>(
        this IQueryable<TOuter> outer,
        IEnumerable<TInner> inner,
        Expression<Func<TOuter, TKey>> outerKeySelector,
        Expression<Func<TInner, TKey>> innerKeySelector,
        Expression<Func<TOuter, TBy>> outerBySelector,
        Expression<Func<TInner, TBy>> innerBySelector,
        AsOfTolerance? tolerance = null,
        AsofStrategy strategy = AsofStrategy.Backward)
        => outer.JoinAsOf(inner, outerKeySelector, innerKeySelector, outerBySelector, innerBySelector, (o, i) => ValueTuple.Create(o, i), tolerance, strategy);
}
