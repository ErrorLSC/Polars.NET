using Polars.NET.Core;

namespace Polars.CSharp;
/// <summary>
/// Builder for GroupByAggs
/// </summary>
public class GroupByBuilder
{
    private readonly DataFrame _df;
    private readonly Expr[] _by;
    
    private Expr? _havingExpr = null;

    internal GroupByBuilder(DataFrame df, Expr[] by)
    {
        _df = df;
        _by = by;
    }

    /// <summary>
    /// Filter groups with a predicate after aggregation.
    /// </summary>
    public GroupByBuilder Having(Expr predicate)
    {
        _havingExpr = predicate;
        return this; // 链式调用
    }

    /// <summary>
    /// Aggregate with specified expressions.
    /// Under the hood, this routes through the Lazy engine to maximize performance and optimizations.
    /// </summary>
    /// <param name="aggs">Aggregation expressions</param>
    /// <returns>A new aggregated DataFrame</returns>
    public DataFrame Agg(params Expr[] aggs)
    {
        var lazyGrouped = _df.Lazy().GroupBy(_by);

        if (_havingExpr is not null)
        {
            lazyGrouped = lazyGrouped.Having(_havingExpr);
        }

        return lazyGrouped.Agg(aggs).Collect();
    }
}

/// <summary>
/// A helper class to construct a dynamic groupby operation on a DataFrame.
/// </summary>
public class DynamicGroupBy
{
    private readonly DataFrame _df;
    private readonly string _indexColumn;
    private readonly TimeSpan _every;
    private readonly TimeSpan? _period;
    private readonly TimeSpan? _offset;
    private readonly Expr[]? _by;
    private readonly Label _label;
    private readonly bool _includeBoundaries;
    private readonly ClosedWindow _closedWindow;
    private readonly StartBy _startBy;

    internal DynamicGroupBy(
        DataFrame df,
        string indexColumn,
        TimeSpan every,
        TimeSpan? period,
        TimeSpan? offset,
        Expr[]? by,
        Label label,
        bool includeBoundaries,
        ClosedWindow closedWindow,
        StartBy startBy)
    {
        _df = df;
        _indexColumn = indexColumn;
        _every = every;
        _period = period;
        _offset = offset;
        _by = by;
        _label = label;
        _includeBoundaries = includeBoundaries;
        _closedWindow = closedWindow;
        _startBy = startBy;
    }

    /// <summary>
    /// Apply aggregations to the dynamic group.
    /// </summary>
    public DataFrame Agg(params Expr[] aggs)
    {
        return _df.Lazy()
            .GroupByDynamic(
                _indexColumn,
                _every,
                _period,
                _offset,
                _by,
                _label,
                _includeBoundaries,
                _closedWindow,
                _startBy
            )
            .Agg(aggs)
            .Collect();
    }
}