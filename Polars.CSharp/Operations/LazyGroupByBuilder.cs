#pragma warning disable CS1591 
using Polars.NET.Core;
using Cs = Polars.CSharp.Polars.Selectors;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

internal enum GroupByType
{
    Standard,
    Dynamic,
    Rolling
}

/// <summary>
/// A unified builder for Standard, Dynamic, and Rolling GroupBy operations.
/// </summary>
public sealed class LazyGroupBy : IDisposable
{
    private readonly LazyFrameHandle _lfHandle;
    private readonly GroupByType _type;
    private readonly Expr[] _keys;
    private readonly bool _maintainOrder;
    private bool _disposed;
    // --- Dynamic / Rolling ---
    private readonly string? _indexColumn;
    private readonly string? _period;
    private readonly string? _offset;
    private readonly ClosedWindow _closedWindow;

    // --- Dynamic ---
    private readonly string? _every;
    private readonly Label _label; 
    private readonly StartBy _startBy;
    private readonly bool _includeBoundaries;

    /// <summary>
    /// Count the number of values in each group.
    /// </summary>
    public LazyFrame Count()
        => Agg(Pl.All().Count());

    /// <summary>
    /// Aggregate all columns into lists. 
    /// </summary>
    public LazyFrame All()
        => Agg(Pl.All()); 
    
    /// <summary>
    /// Aggregate the first values in the group.
    /// </summary>
    /// <param name="ignoreNulls">Ignore null values (default False). If set to True, the first non-null value for each aggregation is returned, 
    /// otherwise None is returned if no non-null value exists.</param>
    /// <returns></returns>
    public LazyFrame First(bool ignoreNulls=false)
        => Agg(Pl.All().First(ignoreNulls)); 
    /// <summary>
    /// Aggregate the last values in the group.
    /// </summary>
    /// <param name="ignoreNulls">Ignore null values (default False). If set to True, the last non-null value for each aggregation is returned, 
    /// otherwise None is returned if no non-null value exists.</param>
    /// <returns></returns>
    public LazyFrame Last(bool ignoreNulls=false)
        => Agg(Pl.All().Last(ignoreNulls)); 
    /// <summary>
    /// Get the first n rows of each group.
    /// </summary>
    public LazyFrame Head(int n = 10)
    {
        var aggregated = Agg(Pl.All().Head(n));

        var keyNames = (_keys ?? [])
            .Select(expr => expr.Meta.OutputName())
            .Where(name => !string.IsNullOrEmpty(name))
            .Select(name => name!)
            .ToList();

        if (!string.IsNullOrEmpty(_indexColumn))
        {
            keyNames.Add(_indexColumn);
        }

        return aggregated.Explode(Cs.All().Exclude(keyNames.ToArray())); 
    }

    /// <summary>
    /// Get the last n rows of each group.
    /// </summary>
    public LazyFrame Tail(int n = 10)
    {
        var aggregated = Agg(Pl.All().Tail(n));

        var keyNames = (_keys ?? [])
            .Select(expr => expr.Meta.OutputName())
            .Where(name => !string.IsNullOrEmpty(name))
            .Select(name => name!)
            .ToList();

        if (!string.IsNullOrEmpty(_indexColumn))
        {
            keyNames.Add(_indexColumn);
        }

        return aggregated.Explode(Cs.All().Exclude(keyNames.ToArray())); 
    }
    /// <summary>
    /// Return the number of rows in each group.
    /// </summary>
    /// <param name="name">Assign a name to the resulting column; if unset, defaults to “len”.</param>
    /// <returns></returns>
    public LazyFrame Len(string name="len")
        => Agg(Pl.Len().Alias(name));
    /// <summary>
    /// Reduce the groups to the maximal value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Max()
        => Agg(Pl.All().Max());
    /// <summary>
    /// Reduce the groups to the minimal value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Min()
        => Agg(Pl.All().Min());
    /// <summary>
    /// Reduce the groups to the median value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Median()
        => Agg(Pl.All().Median()); 
    /// <summary>
    /// Reduce the groups to the mean value.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Mean()
        => Agg(Pl.All().Mean()); 
    /// <summary>
    /// Count the unique values per group.
    /// </summary>
    /// <returns></returns>   
    public LazyFrame NUnique()
        => Agg(Pl.All().NUnique());
    /// <summary>
    /// Reduce the groups to the sum.
    /// </summary>
    /// <returns></returns>  
    public LazyFrame Sum()
        => Agg(Pl.All().Sum());  
    /// <summary>
    /// Compute the quantile per group.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0.</param>
    /// <param name="interpolation">Interpolation method.</param>
    /// <returns></returns>          
    public LazyFrame Quantile(double quantile,QuantileMethod interpolation = QuantileMethod.Linear)
        => Agg(Pl.All().Quantile(quantile,interpolation));   
    internal LazyGroupBy(LazyFrameHandle lfHandle, Expr[] keys, bool maintainOrder)
    {
        _lfHandle = lfHandle;
        _type = GroupByType.Standard;
        _keys = keys;
        _maintainOrder = maintainOrder;
    }

    internal LazyGroupBy(
        LazyFrameHandle lfHandle, string indexColumn, string every, string? period, string? offset, 
        Expr[] keys, Label label, bool includeBoundaries, ClosedWindow closedWindow, StartBy startBy)
    {
        _lfHandle = lfHandle;
        _type = GroupByType.Dynamic;
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

    internal LazyGroupBy(
        LazyFrameHandle lfHandle, string indexColumn, string? period, string? offset, 
        Expr[] keys, ClosedWindow closedWindow)
    {
        _lfHandle = lfHandle;
        _type = GroupByType.Rolling;
        _indexColumn = indexColumn;
        _period = period;
        _offset = offset;
        _keys = keys;
        _closedWindow = closedWindow;
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

        var aggHandles = aggs.Select(a => PolarsWrapper.CloneExpr(a.Handle)).ToArray();
        ExprHandle? havingHandle = _havingExpr is not null ? PolarsWrapper.CloneExpr(_havingExpr.Handle) : null;

        var keysForRust = _keys.Select(k => PolarsWrapper.CloneExpr(k.Handle)).ToArray();
        LazyFrameHandle resHandle = _type switch
        {
            GroupByType.Standard => PolarsWrapper.LazyGroupByAgg(
                                _lfHandle, keysForRust, aggHandles, havingHandle, _maintainOrder),
            GroupByType.Dynamic => PolarsWrapper.LazyGroupByDynamic(
                                _lfHandle, _indexColumn!, _every!, _period!, _offset!,
                                _label.ToNative(), _includeBoundaries, _closedWindow.ToNative(), _startBy.ToNative(),
                                keysForRust, aggHandles, havingHandle),
            GroupByType.Rolling => PolarsWrapper.LazyGroupByRolling(
                                _lfHandle, _indexColumn!, _period!, _offset!, _closedWindow.ToNative(),
                                keysForRust, aggHandles, havingHandle),
            _ => throw new InvalidOperationException("Unknown GroupBy mode."),
        };
        return new LazyFrame(resHandle);
    }
    /// <summary>
    /// Dispose all used handles
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            if (_keys != null)
            {
                foreach (var k in _keys)
                {
                    k?.Dispose();
                }
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