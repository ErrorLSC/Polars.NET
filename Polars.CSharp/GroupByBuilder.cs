using Polars.NET.Core;

namespace Polars.CSharp;
/// <summary>
/// Builder for GroupByAggs
/// </summary>
public class GroupByBuilder
{
    private readonly DataFrame _df;
    private readonly Expr[] _by;

    internal GroupByBuilder(DataFrame df, Expr[] by)
    {
        _df = df;
        _by = by;
    }
    /// <summary>
    /// Aggregate with specified expressions
    /// </summary>
    /// <param name="aggs"></param>
    /// <returns></returns>
    public DataFrame Agg(params Expr[] aggs)
    {
        // 同样需要 Clone Expr Handle
        var byHandles = _by.Select(b => PolarsWrapper.CloneExpr(b.Handle)).ToArray();
        var aggHandles = aggs.Select(a => PolarsWrapper.CloneExpr(a.Handle)).ToArray();

        //
        var h = PolarsWrapper.GroupByAgg(_df.Handle, byHandles, aggHandles);
        return new DataFrame(h);
    }
}