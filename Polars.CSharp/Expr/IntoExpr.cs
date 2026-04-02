#pragma warning disable CS1591
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;
using Polars.NET.Core;
namespace Polars.CSharp;

/// <summary>
/// A type that can be implicitly converted into a Polars Expression.
/// Matches the 'IntoExpr' concept in Rust and Python Polars.
/// </summary>
public readonly struct IntoExpr
{
    private readonly Expr _expr;
    private readonly bool _ownsExpr;

    public static implicit operator IntoExpr(Expr expr) => new(expr, ownsExpr: false);

    public static implicit operator IntoExpr(Selector selector) => new(selector.ToExpr(), ownsExpr: true);
    public static implicit operator IntoExpr(string name) => new(Pl.Col(name), ownsExpr: true);
    public static implicit operator IntoExpr(Series series) => new(Pl.Lit(series), ownsExpr: true);
    public static implicit operator IntoExpr(DataType dtype) => new(Cs.ByDtype(dtype).ToExpr(), ownsExpr: true);
    public static implicit operator IntoExpr(Type type) => new(Cs.ByDtype(type).ToExpr(), ownsExpr: true);

    private IntoExpr(Expr expr, bool ownsExpr) 
    {
        ArgumentNullException.ThrowIfNull(expr);
        _expr = expr;
        _ownsExpr = ownsExpr;
    }

    /// <summary>
    /// 智能消耗 Expr，确保内存绝对安全。
    /// </summary>
    public Expr Consume()
    {
        if (_ownsExpr) return _expr;
        
        return new Expr(PolarsWrapper.CloneExpr(_expr.Handle));
    }
}

/// <summary>
/// A type that can be implicitly converted into a Polars Selector.
/// Ideal for methods like Unique(), Drop(), etc. that expect column subsets.
/// </summary>
public readonly struct IntoSelector
{
    private readonly Selector _selector;
    private readonly bool _ownsSelector;

    public static implicit operator IntoSelector(Selector selector) => new(selector, ownsSelector: false);

    public static implicit operator IntoSelector(string name) => new(Cs.ByName(name), ownsSelector: true);
    public static implicit operator IntoSelector(DataType dtype) => new(Cs.ByDtype(dtype), ownsSelector: true);
    public static implicit operator IntoSelector(Type type) => new(Cs.ByDtype(type), ownsSelector: true);
    public static implicit operator IntoSelector(Expr expr) => new(expr.ToSelector(), ownsSelector: true);

    private IntoSelector(Selector selector, bool ownsSelector)
    {
        _selector = selector;
        _ownsSelector = ownsSelector;
    }

    public Selector Consume()
    {
        if (_ownsSelector)
        {
            return _selector;
        }
        else
        {
            return new Selector(_selector.CloneHandle());
        }
    }
}