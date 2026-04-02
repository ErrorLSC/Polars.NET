#pragma warning disable CS1573
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary> Cast all columns to the specified DataType. </summary>
    public LazyFrame Cast(DataType dtype, bool strict = true)
        => Select(Pl.All().Cast(dtype, strict));

    /// <summary> Cast a specific Expression, String, Selector, or Type to a DataType. </summary>
    public LazyFrame Cast(IntoExpr expr, DataType dtype, bool strict = true)
        => WithColumns(expr.Consume().Cast(dtype, strict));

    /// <summary> 
    /// Cast multiple targets using tuples.
    /// Usage: lf.Cast( ("Age", DataType.Int32), (Cs.Numeric(), DataType.Float32), (DataType.Float64, DataType.Float32) )
    /// </summary>
    public LazyFrame Cast(params (IntoExpr Expr, DataType Dtype)[] casts)
    {
        if (casts.Length == 0) return this;
        var castExprs = new IntoExpr[casts.Length];
        for (int i = 0; i < casts.Length; i++)
            castExprs[i] = casts[i].Expr.Consume().Cast(casts[i].Dtype, strict: true);
        return WithColumns(castExprs);
    }

    /// <summary> Bridge for C# Type tuples (Prevents tuple implicit cast errors) </summary>
    public LazyFrame Cast(params (IntoExpr Expr, Type Dtype)[] casts)
    {
        if (casts.Length == 0) return this;
        var castExprs = new IntoExpr[casts.Length];
        for (int i = 0; i < casts.Length; i++)
            castExprs[i] = casts[i].Expr.Consume().Cast(casts[i].Dtype, strict: true); // Type 会在这里隐式转为 DataType
        return WithColumns(castExprs);
    }

    /// <summary> Cast using a dictionary mapping. </summary>
    public LazyFrame Cast(IDictionary<string, DataType> dtypes, bool strict = true)
    {
        var castExprs = new IntoExpr[dtypes.Count];
        int i = 0;
        foreach (var kvp in dtypes)
            castExprs[i++] = Pl.Col(kvp.Key).Cast(kvp.Value, strict);
        return WithColumns(castExprs);
    }

    /// <summary> Bridge for C# Type dictionary </summary>
    public LazyFrame Cast(IDictionary<string, Type> dtypes, bool strict = true)
    {
        var castExprs = new IntoExpr[dtypes.Count];
        int i = 0;
        foreach (var kvp in dtypes)
            castExprs[i++] = Pl.Col(kvp.Key).Cast((DataType)kvp.Value, strict);
        return WithColumns(castExprs);
    }
}
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary> Cast all columns to the specified DataType. </summary>
    public DataFrame Cast(DataType dtype, bool strict = true)
        => Select(Pl.All().Cast(dtype, strict));

    /// <summary> Cast a specific Expression, String, Selector, or Type to a DataType. </summary>
    public DataFrame Cast(IntoExpr expr, DataType dtype, bool strict = true)
        => WithColumns(expr.Consume().Cast(dtype, strict));

    /// <summary> 
    /// Cast multiple targets using tuples.
    /// Usage: lf.Cast( ("Age", DataType.Int32), (Cs.Numeric(), DataType.Float32), (DataType.Float64, DataType.Float32) )
    /// </summary>
    public DataFrame Cast(params (IntoExpr Expr, DataType Dtype)[] casts)
    {
        if (casts.Length == 0) return this;
        var castExprs = new IntoExpr[casts.Length];
        for (int i = 0; i < casts.Length; i++)
            castExprs[i] = casts[i].Expr.Consume().Cast(casts[i].Dtype, strict: true);
        return WithColumns(castExprs);
    }

    /// <summary> Bridge for C# Type tuples (Prevents tuple implicit cast errors) </summary>
    public DataFrame Cast(params (IntoExpr Expr, Type Dtype)[] casts)
    {
        if (casts.Length == 0) return this;
        var castExprs = new IntoExpr[casts.Length];
        for (int i = 0; i < casts.Length; i++)
            castExprs[i] = casts[i].Expr.Consume().Cast(casts[i].Dtype, strict: true);
        return WithColumns(castExprs);
    }

    /// <summary> Cast using a dictionary mapping. </summary>
    public DataFrame Cast(IDictionary<string, DataType> dtypes, bool strict = true)
    {
        var castExprs = new IntoExpr[dtypes.Count];
        int i = 0;
        foreach (var kvp in dtypes)
            castExprs[i++] = Pl.Col(kvp.Key).Cast(kvp.Value, strict);
        return WithColumns(castExprs);
    }

    /// <summary> Bridge for C# Type dictionary </summary>
    public DataFrame Cast(IDictionary<string, Type> dtypes, bool strict = true)
    {
        var castExprs = new IntoExpr[dtypes.Count];
        int i = 0;
        foreach (var kvp in dtypes)
            castExprs[i++] = Pl.Col(kvp.Key).Cast((DataType)kvp.Value, strict);
        return WithColumns(castExprs);
    }
}