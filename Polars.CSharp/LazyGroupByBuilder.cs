using Polars.Native;

namespace Polars.CSharp;
/// <summary>
/// Intermediate builder for LazyGroupBy operations.
/// Holds the LazyFrame handle (ownership transferred to this builder) and grouping keys.
/// </summary>
public class LazyGroupBy
{
    private readonly LazyFrameHandle _lfHandle; // 这是克隆来的 Handle
    private readonly Expr[] _keys;

    internal LazyGroupBy(LazyFrameHandle lfHandle, Expr[] keys)
    {
        _lfHandle = lfHandle;
        _keys = keys;
    }

    /// <summary>
    /// Apply aggregations to the group.
    /// This consumes the internal LazyFrame handle.
    /// </summary>
    public LazyFrame Agg(params Expr[] aggs)
    {
        // 1. 准备 Key Handles (Clone)
        var keyHandles = _keys.Select(k => PolarsWrapper.CloneExpr(k.Handle)).ToArray();
        
        // 2. 准备 Agg Handles (Clone)
        var aggHandles = aggs.Select(a => PolarsWrapper.CloneExpr(a.Handle)).ToArray();

        // 3. 调用 Wrapper
        // 注意：这里传入的是 _lfHandle。
        // NativeBindings.pl_lazy_groupby_agg 会消耗这个 handle。
        // 因为我们在创建 LazyGroupBy 时已经 Clone 过了，所以这里消耗的是副本，安全！
        var resHandle = PolarsWrapper.LazyGroupByAgg(_lfHandle, keyHandles, aggHandles);
        
        return new LazyFrame(resHandle);
    }
}