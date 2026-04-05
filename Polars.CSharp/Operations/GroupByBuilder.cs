#pragma warning disable CS1591 
using Cs = Polars.CSharp.Polars.Selectors;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;
/// <summary>
/// Builder for GroupByAggs
/// </summary>
public class GroupByBuilder
{
    private readonly DataFrame _df;

    private readonly Expr[] _keys;
    private readonly bool _maintainOrder;
    private Expr? _havingExpr = null;

    internal GroupByBuilder(DataFrame df, Expr[] by,bool maintainOrder)
    {
        _df = df;
        _keys = by;
        _maintainOrder=maintainOrder;
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
        => Agg(Pl.All().Count());

    /// <summary>
    /// Aggregate all columns into lists. 
    /// </summary>
    public DataFrame All()
        => Agg(Pl.All()); 
    
    /// <summary>
    /// Aggregate the first values in the group.
    /// </summary>
    /// <param name="ignoreNulls">Ignore null values (default False). If set to True, the first non-null value for each aggregation is returned, 
    /// otherwise None is returned if no non-null value exists.</param>
    /// <returns></returns>
    public DataFrame First(bool ignoreNulls=false)
        => Agg(Pl.All().First(ignoreNulls)); 
    /// <summary>
    /// Aggregate the last values in the group.
    /// </summary>
    /// <param name="ignoreNulls">Ignore null values (default False). If set to True, the last non-null value for each aggregation is returned, 
    /// otherwise None is returned if no non-null value exists.</param>
    /// <returns></returns>
    public DataFrame Last(bool ignoreNulls=false)
        => Agg(Pl.All().Last(ignoreNulls)); 
    /// <summary>
    /// Get the first n rows of each group.
    /// </summary>
    public DataFrame Head(int n = 10)
    {
        var aggregated = Agg(Pl.All().Head(n));

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
        var aggregated = Agg(Pl.All().Tail(n));

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
        => Agg(Pl.Len().Alias(name));
    /// <summary>
    /// Reduce the groups to the maximal value.
    /// </summary>
    /// <returns></returns>
    public DataFrame Max()
        => Agg(Pl.All().Max());
    /// <summary>
    /// Reduce the groups to the minimal value.
    /// </summary>
    /// <returns></returns>
    public DataFrame Min()
        => Agg(Pl.All().Min());
    /// <summary>
    /// Reduce the groups to the median value.
    /// </summary>
    /// <returns></returns>
    public DataFrame Median()
        => Agg(Pl.All().Median()); 
    /// <summary>
    /// Reduce the groups to the mean value.
    /// </summary>
    /// <returns></returns>
    public DataFrame Mean()
        => Agg(Pl.All().Mean()); 
    /// <summary>
    /// Count the unique values per group.
    /// </summary>
    /// <returns></returns>   
    public DataFrame NUnique()
        => Agg(Pl.All().NUnique());
    /// <summary>
    /// Reduce the groups to the sum.
    /// </summary>
    /// <returns></returns>  
    public DataFrame Sum()
        => Agg(Pl.All().Sum());  
    /// <summary>
    /// Compute the quantile per group.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0.</param>
    /// <param name="interpolation">Interpolation method.</param>
    /// <returns></returns>          
    public DataFrame Quantile(double quantile,QuantileMethod interpolation = QuantileMethod.Linear)
        => Agg(Pl.All().Quantile(quantile,interpolation));   

    /// <summary>
    /// Aggregate with specified expressions.
    /// Under the hood, this routes through the Lazy engine to maximize performance and optimizations.
    /// </summary>
    /// <param name="aggs">Aggregation expressions</param>
    /// <returns>A new aggregated DataFrame</returns>
    public DataFrame Agg(params Expr[] aggs)
    {
        var lazyGrouped = _df.Lazy().GroupBy(_keys.Select(e => (IntoExpr)e));

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
    private readonly string _every;        
    private readonly string? _period;     
    private readonly string? _offset;      
    private readonly Expr[]? _by;
    private readonly Label _label;
    private readonly bool _includeBoundaries;
    private readonly ClosedWindow _closedWindow;
    private readonly StartBy _startBy;
    
    private Expr? _havingExpr = null;      

    internal DynamicGroupBy(
        DataFrame df,
        string indexColumn,
        string every,                      
        string? period,
        string? offset,
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
    /// Filter groups with a predicate after aggregation.
    /// </summary>
    public DynamicGroupBy Having(Expr predicate)
    {
        _havingExpr = predicate;
        return this;
    }

    /// <summary>
    /// Apply aggregations to the dynamic group.
    /// </summary>
    public DataFrame Agg(params Expr[] aggs)
    {
        var lazyGrouped = _df.Lazy()
            .GroupByDynamic(
                _indexColumn,
                _every, 
                _period!, 
                _offset!, 
                _by?.Select(e => (IntoExpr)e),
                _label,
                _includeBoundaries,
                _closedWindow,
                _startBy
            );

        if (_havingExpr is not null)
        {
            lazyGrouped = lazyGrouped.Having(_havingExpr);
        }

        return lazyGrouped.Agg(aggs).Collect();
    }

    public DataFrame Count() => Agg(Pl.All().Count());
    
    public DataFrame All() => Agg(Pl.All()); 
    
    public DataFrame First(bool ignoreNulls = false) => Agg(Pl.All().First(ignoreNulls)); 
    
    public DataFrame Last(bool ignoreNulls = false) => Agg(Pl.All().Last(ignoreNulls)); 
    
    public DataFrame Len(string name = "len") => Agg(Pl.Len().Alias(name));
    
    public DataFrame Max() => Agg(Pl.All().Max());
    
    public DataFrame Min() => Agg(Pl.All().Min());
    
    public DataFrame Median() => Agg(Pl.All().Median()); 
    
    public DataFrame Mean() => Agg(Pl.All().Mean()); 

    public DataFrame NUnique() => Agg(Pl.All().NUnique());

    public DataFrame Sum() => Agg(Pl.All().Sum());  
    
    public DataFrame Quantile(double quantile, QuantileMethod interpolation = QuantileMethod.Linear)
        => Agg(Pl.All().Quantile(quantile, interpolation));   

    public DataFrame Head(int n = 10)
    {
        var aggregated = Agg(Pl.All().Head(n));
        var keyNames = (_by ?? [])
            .Select(expr => expr.Meta.OutputName())
            .Where(name => !string.IsNullOrEmpty(name))
            .Select(name => name!) 
            .ToList();
        
        keyNames.Add(_indexColumn);

        return aggregated.Explode(Cs.All().Exclude(keyNames.ToArray())); 
    }

    public DataFrame Tail(int n = 10)
    {
        var aggregated = Agg(Pl.All().Tail(n));
        var keyNames = (_by ?? [])
            .Select(expr => expr.Meta.OutputName())
            .Where(name => !string.IsNullOrEmpty(name))
            .Select(name => name!)
            .ToList();

        keyNames.Add(_indexColumn);

        return aggregated.Explode(Cs.All().Exclude(keyNames.ToArray()));
    }
}