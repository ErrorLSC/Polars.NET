using Polars.NET.Core;

namespace Polars.CSharp;
/// <summary>
/// Intermediate builder for LazyGroupBy operations.
/// Holds the LazyFrame handle (ownership transferred to this builder) and grouping keys.
/// </summary>
public sealed class LazyGroupBy : IDisposable
{
    private readonly LazyFrameHandle _lfHandle;
    private readonly ExprHandle[] _ownedKeyHandles; 
    private bool _disposed;

    /// <summary>
    /// Count the number of values in each group.
    /// </summary>
    public LazyFrame Count()
        => Agg(Polars.All().Count());

    /// <summary>
    /// Aggregate all columns into lists. 
    /// </summary>
    public LazyFrame All()
        => Agg(Polars.All()); 
    
    /// <summary>
    /// Aggregate the first values in the group.
    /// </summary>
    /// <param name="ignoreNulls">Ignore null values (default False). If set to True, the first non-null value for each aggregation is returned, 
    /// otherwise None is returned if no non-null value exists.</param>
    /// <returns></returns>
    public LazyFrame First(bool ignoreNulls=false)
        => Agg(Polars.All().First(ignoreNulls)); 
    /// <summary>
    /// Aggregate the last values in the group.
    /// </summary>
    /// <param name="ignoreNulls">Ignore null values (default False). If set to True, the last non-null value for each aggregation is returned, 
    /// otherwise None is returned if no non-null value exists.</param>
    /// <returns></returns>
    public LazyFrame Last(bool ignoreNulls=false)
        => Agg(Polars.All().Last(ignoreNulls)); 
    /// <summary>
    /// Get the first n rows of each group.
    /// </summary>
    /// <param name="n">Number of rows to return.</param>
    /// <returns></returns>
    public LazyFrame Head(int n=10)
        => Agg(Polars.All().Head(n)); 
    /// <summary>
    /// Get the last n rows of each group.
    /// </summary>
    /// <param name="n">Number of rows to return.</param>
    /// <returns></returns>
    public LazyFrame Tail(int n=10)
        => Agg(Polars.All().Tail(n)); 
    /// <summary>
    /// Return the number of rows in each group.
    /// </summary>
    /// <param name="name">Assign a name to the resulting column; if unset, defaults to “len”.</param>
    /// <returns></returns>
    public LazyFrame Len(string name="len")
        => Agg(Polars.Len().Alias(name));
    /// <summary>
    /// Reduce the groups to the maximal value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Max()
        => Agg(Polars.All().Max());
    /// <summary>
    /// Reduce the groups to the minimal value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Min()
        => Agg(Polars.All().Min());
    /// <summary>
    /// Reduce the groups to the median value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Median()
        => Agg(Polars.All().Median()); 
    /// <summary>
    /// Reduce the groups to the mean value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Mean()
        => Agg(Polars.All().Mean()); 
    /// <summary>
    /// Count the unique values per group.
    /// </summary>
    /// <returns></returns>   
    public LazyFrame NUnique()
        => Agg(Polars.All().NUnique());
    /// <summary>
    /// Reduce the groups to the sum.
    /// </summary>
    /// <returns></returns>  
    public LazyFrame Sum()
        => Agg(Polars.All().Sum());  
    /// <summary>
    /// Compute the quantile per group.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0.</param>
    /// <param name="interpolation">Interpolation method.</param>
    /// <returns></returns>          
    public LazyFrame Quantile(double quantile,QuantileMethod interpolation = QuantileMethod.Linear)
        => Agg(Polars.All().Quantile(quantile,interpolation));   
    internal LazyGroupBy(LazyFrameHandle lfHandle, Expr[] keys)
    {
        _lfHandle = lfHandle;

        _ownedKeyHandles = new ExprHandle[keys.Length];
        for (int i = 0; i < keys.Length; i++)
        {
            _ownedKeyHandles[i] = PolarsWrapper.CloneExpr(keys[i].Handle);
        }
    }

    /// <summary>
    /// Filter groups with a list of predicates after aggregation.
    /// Using this method is equivalent to adding the predicates to the aggregation and filtering afterwards.
    /// </summary>
    /// <param name="predicate">Expressions that evaluate to a boolean value for each group. Typically, this requires the use of an aggregation function.</param>
    /// <returns></returns>
    public LazyGroupBy Having(Expr predicate)
    {
        _havingExpr = predicate;
        return this; 
    }
    private Expr? _havingExpr = null;

    /// <summary>
    /// Apply aggregations to the group.
    /// </summary>
    public LazyFrame Agg(params Expr[] aggs)
    {
        ObjectDisposedException.ThrowIf(_disposed, nameof(LazyGroupBy));

        var aggHandles = new ExprHandle[aggs.Length];
        for (int i = 0; i < aggs.Length; i++)
        {
            aggHandles[i] = PolarsWrapper.CloneExpr(aggs[i].Handle);
        }

        var keysForRust = new ExprHandle[_ownedKeyHandles.Length];
        for(int i=0; i<_ownedKeyHandles.Length; i++)
        {
            keysForRust[i] = PolarsWrapper.CloneExpr(_ownedKeyHandles[i]);
        }
        
        ExprHandle? havingHandle = null;
        if (_havingExpr is not null)
        {
            havingHandle = PolarsWrapper.CloneExpr(_havingExpr.Handle);
        }
        
        var resHandle = PolarsWrapper.LazyGroupByAgg(_lfHandle, keysForRust, aggHandles, havingHandle);
        
        return new LazyFrame(resHandle);
    }
    /// <summary>
    /// Dispose all used handles
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            foreach (var h in _ownedKeyHandles)
            {
                if (h != null && !h.IsInvalid) h.Dispose();
            }
            
            if (!_lfHandle.IsClosed) 
            {
                 _lfHandle.Dispose();
            }

            _disposed = true;
            
        }
        
        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// Intermediate builder for LazyDynamicGroupBy operations.
/// </summary>
public class LazyDynamicGroupBy
{
    private readonly LazyFrameHandle _lfHandle;
    private readonly Expr[] _keys;
    private readonly string _indexColumn;
    private readonly string _every;
    private readonly string _period;
    private readonly string _offset;
    private readonly Label _label; 
    private readonly StartBy _startBy;
    private readonly bool _includeBoundaries;
    private readonly ClosedWindow _closedWindow;

    internal LazyDynamicGroupBy(
        LazyFrameHandle lfHandle,
        string indexColumn,
        string every,
        string period,
        string offset,
        Expr[] keys,
        Label label, 
        bool includeBoundaries,
        ClosedWindow closedWindow,
        StartBy startBy)
    {
        _lfHandle = lfHandle;
        _indexColumn = indexColumn;
        _every = every;
        _period = period;
        _offset = offset;
        _keys = keys;
        _label = label;
        _includeBoundaries = includeBoundaries;
        _closedWindow = closedWindow;
        _startBy = startBy;
    }
    /// <summary>
    /// Apply aggregations to the group.
    /// This consumes the internal LazyFrame handle.
    /// </summary>
    public LazyFrame Agg(params Expr[] aggs)
    {
        var keyHandles = _keys.Select(k => PolarsWrapper.CloneExpr(k.Handle)).ToArray();
        
        var aggHandles = aggs.Select(a => PolarsWrapper.CloneExpr(a.Handle)).ToArray();
        var newHandle = PolarsWrapper.LazyGroupByDynamic(
                _lfHandle,
                _indexColumn,
                _every,
                _period,
                _offset,
                _label.ToNative(),
                _includeBoundaries,
                _closedWindow.ToNative(),
                _startBy.ToNative(),
                keyHandles,
                aggHandles
            );

            return new LazyFrame(newHandle);
    }
}