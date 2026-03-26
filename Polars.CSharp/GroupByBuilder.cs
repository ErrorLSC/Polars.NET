using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;
/// <summary>
/// Builder for GroupByAggs
/// </summary>
public class GroupByBuilder
{
    private readonly DataFrame _df;
    private readonly Expr[] _by;

    private readonly Expr[] _keys;
    
    private Expr? _havingExpr = null;

    internal GroupByBuilder(DataFrame df, Expr[] by)
    {
        _df = df;
        _by = by;
        _keys = by;
    }

    /// <summary>
    /// Filter groups with a predicate after aggregation.
    /// </summary>
    public GroupByBuilder Having(Expr predicate)
    {
        _havingExpr = predicate;
        return this; 
    }

    /// <summary>
    /// Count the number of values in each group.
    /// </summary>
    public DataFrame Count()
        => Agg(Polars.All().Count());

    /// <summary>
    /// Aggregate all columns into lists. 
    /// </summary>
    public DataFrame All()
        => Agg(Polars.All()); 
    
    /// <summary>
    /// Aggregate the first values in the group.
    /// </summary>
    /// <param name="ignoreNulls">Ignore null values (default False). If set to True, the first non-null value for each aggregation is returned, 
    /// otherwise None is returned if no non-null value exists.</param>
    /// <returns></returns>
    public DataFrame First(bool ignoreNulls=false)
        => Agg(Polars.All().First(ignoreNulls)); 
    /// <summary>
    /// Aggregate the last values in the group.
    /// </summary>
    /// <param name="ignoreNulls">Ignore null values (default False). If set to True, the last non-null value for each aggregation is returned, 
    /// otherwise None is returned if no non-null value exists.</param>
    /// <returns></returns>
    public DataFrame Last(bool ignoreNulls=false)
        => Agg(Polars.All().Last(ignoreNulls)); 
    /// <summary>
    /// Get the first n rows of each group.
    /// </summary>
    public DataFrame Head(int n = 10)
    {
        var aggregated = Agg(Polars.All().Head(n));

        string[] keyNames = _keys
            .Select(expr => expr.Meta.OutputName())
            .Where(name => !string.IsNullOrEmpty(name))
            .ToArray()!;

        return aggregated.Explode(Cs.All().Exclude(keyNames)); 
    }

    /// <summary>
    /// Get the last n rows of each group.
    /// </summary>
    public DataFrame Tail(int n = 10)
    {
        var aggregated = Agg(Polars.All().Tail(n));

        string[] keyNames = _keys
            .Select(expr => expr.Meta.OutputName())
            .Where(name => !string.IsNullOrEmpty(name))
            .ToArray()!;

        return aggregated.Explode(Cs.All().Exclude(keyNames));
    }
    /// <summary>
    /// Return the number of rows in each group.
    /// </summary>
    /// <param name="name">Assign a name to the resulting column; if unset, defaults to “len”.</param>
    /// <returns></returns>
    public DataFrame Len(string name="len")
        => Agg(Polars.Len().Alias(name));
    /// <summary>
    /// Reduce the groups to the maximal value.
    /// </summary>
    /// <returns></returns>
    public DataFrame Max()
        => Agg(Polars.All().Max());
    /// <summary>
    /// Reduce the groups to the minimal value.
    /// </summary>
    /// <returns></returns>
    public DataFrame Min()
        => Agg(Polars.All().Min());
    /// <summary>
    /// Reduce the groups to the median value.
    /// </summary>
    /// <returns></returns>
    public DataFrame Median()
        => Agg(Polars.All().Median()); 
    /// <summary>
    /// Reduce the groups to the mean value.
    /// </summary>
    /// <returns></returns>
    public DataFrame Mean()
        => Agg(Polars.All().Mean()); 
    /// <summary>
    /// Count the unique values per group.
    /// </summary>
    /// <returns></returns>   
    public DataFrame NUnique()
        => Agg(Polars.All().NUnique());
    /// <summary>
    /// Reduce the groups to the sum.
    /// </summary>
    /// <returns></returns>  
    public DataFrame Sum()
        => Agg(Polars.All().Sum());  
    /// <summary>
    /// Compute the quantile per group.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0.</param>
    /// <param name="interpolation">Interpolation method.</param>
    /// <returns></returns>          
    public DataFrame Quantile(double quantile,QuantileMethod interpolation = QuantileMethod.Linear)
        => Agg(Polars.All().Quantile(quantile,interpolation));   

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