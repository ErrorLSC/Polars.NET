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
    /// Apply aggregations to the group.
    /// </summary>
    // public LazyFrame Agg(params Expr[] aggs)
    // {
    //     ObjectDisposedException.ThrowIf(_disposed,nameof(LazyGroupBy));

    //     var aggHandles = new ExprHandle[aggs.Length];
    //     for (int i = 0; i < aggs.Length; i++)
    //     {
    //         aggHandles[i] = PolarsWrapper.CloneExpr(aggs[i].Handle);
    //     }

    //     var keysForRust = new ExprHandle[_ownedKeyHandles.Length];
    //     for(int i=0; i<_ownedKeyHandles.Length; i++)
    //     {
    //         keysForRust[i] = PolarsWrapper.CloneExpr(_ownedKeyHandles[i]);
    //     }
        
    //     var resHandle = PolarsWrapper.LazyGroupByAgg(_lfHandle, keysForRust, aggHandles);
        
    //     return new LazyFrame(resHandle);
    // }
    public LazyGroupBy Having(Expr predicate)
    {
        _havingExpr = predicate;
        return this; // 链式调用
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
        
        // 🌟 核心适配 3：为 Having 表达式克隆独立的底层句柄
        ExprHandle? havingHandle = null;
        if (_havingExpr is not null)
        {
            havingHandle = PolarsWrapper.CloneExpr(_havingExpr.Handle);
        }
        
        // 🌟 把 havingHandle 传给底层 (注意方法签名的参数对应)
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