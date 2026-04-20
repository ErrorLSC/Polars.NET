using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Get the top k rows according to a single expression, string, or selector.
    /// Usage: lf.TopK(5, "Sales") or lf.TopK(10, Pl.Col("Age") * 2, reverse: true)
    /// </summary>
    public LazyFrame TopK(int k, IntoColumnExpr by, bool reverse = false)
    {
        using var safeExpr = by.Consume();
        
        var handles = new[] { PolarsWrapper.CloneExpr(safeExpr.Handle) };
        var h = PolarsWrapper.LazyFrameTopK(CloneHandle(), (uint)k, handles, [reverse]);
        
        return new LazyFrame(h);
    }

    /// <summary>
    /// Get the top k rows according to multiple column names with a uniform reverse setting.
    /// Usage: lf.TopK(5, ["Score", "Age"], reverse: false)
    /// </summary>
    public LazyFrame TopK(int k, IEnumerable<string> by, bool reverse = false)
    {
        var cols = by as string[] ?? [.. by];
        if (cols.Length == 0) return this;

        var handles = new ExprHandle[cols.Length];
        var revArray = new bool[cols.Length];

        for (int i = 0; i < cols.Length; i++)
        {
            handles[i] = PolarsWrapper.CloneExpr(Pl.Col(cols[i]).Handle);
            revArray[i] = reverse;
        }

        var h = PolarsWrapper.LazyFrameTopK(CloneHandle(), (uint)k, handles, revArray);
        return new LazyFrame(h);
    }
    
    /// <summary>
    /// Get the top k rows with specific reverse directions using Tuples.
    /// Usage: lf.TopK(5, ("Score", false), ("Age", true), (Cs.Numeric(), false))
    /// </summary>
    public LazyFrame TopK(int k, params (IntoColumnExpr By, bool Reverse)[] configs)
    {
        if (configs.Length == 0) return this;

        var handles = new ExprHandle[configs.Length];
        var revArray = new bool[configs.Length];

        for (int i = 0; i < configs.Length; i++)
        {
            using var safeExpr = configs[i].By.Consume();
            handles[i] = PolarsWrapper.CloneExpr(safeExpr.Handle);
            revArray[i] = configs[i].Reverse;
        }

        var h = PolarsWrapper.LazyFrameTopK(CloneHandle(), (uint)k, handles, revArray);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Master method: Get the top k rows by multiple Expressions with optional parallel boolean arrays.
    /// Usage: lf.TopK(10, myColumns.Select(c => Pl.Col(c)))
    /// </summary>
    public LazyFrame TopK(int k, IEnumerable<Expr> by, bool[]? reverse = null)
    {
        var exprArray = by as Expr[] ?? [.. by];
        if (exprArray.Length == 0) return this;

        var revArray = reverse ?? new bool[exprArray.Length]; 

        var handles = new ExprHandle[exprArray.Length];
        for (int i = 0; i < exprArray.Length; i++)
        {
            handles[i] = PolarsWrapper.CloneExpr(exprArray[i].Handle);
        }

        var h = PolarsWrapper.LazyFrameTopK(CloneHandle(), (uint)k, handles, revArray);
        return new LazyFrame(h);
    }

    /// <summary>
    /// Get the Bottom k rows according to a single expression, string, or selector.
    /// Usage: lf.BottomK(5, "Sales") or lf.BottomK(10, Pl.Col("Age") * 2, reverse: true)
    /// </summary>
    public LazyFrame BottomK(int k, IntoColumnExpr by, bool reverse = false)
    {
        using var safeExpr = by.Consume();
        
        var handles = new[] { PolarsWrapper.CloneExpr(safeExpr.Handle) };
        var h = PolarsWrapper.LazyFrameBottomK(CloneHandle(), (uint)k, handles, [reverse]);
        
        return new LazyFrame(h);
    }

    /// <summary>
    /// Get the Bottom k rows according to multiple column names with a uniform reverse setting.
    /// Usage: lf.BottomK(5, ["Score", "Age"], reverse: false)
    /// </summary>
    public LazyFrame BottomK(int k, IEnumerable<string> by, bool reverse = false)
    {
        var cols = by as string[] ?? [.. by];
        if (cols.Length == 0) return this;

        var handles = new ExprHandle[cols.Length];
        var revArray = new bool[cols.Length];

        for (int i = 0; i < cols.Length; i++)
        {
            handles[i] = PolarsWrapper.CloneExpr(Pl.Col(cols[i]).Handle);
            revArray[i] = reverse;
        }

        var h = PolarsWrapper.LazyFrameBottomK(CloneHandle(), (uint)k, handles, revArray);
        return new LazyFrame(h);
    }
    
    /// <summary>
    /// Get the Bottom k rows with specific reverse directions using Tuples.
    /// Usage: lf.BottomK(5, ("Score", false), ("Age", true), (Cs.Numeric(), false))
    /// </summary>
    public LazyFrame BottomK(int k, params (IntoColumnExpr By, bool Reverse)[] configs)
    {
        if (configs.Length == 0) return this;

        var handles = new ExprHandle[configs.Length];
        var revArray = new bool[configs.Length];

        for (int i = 0; i < configs.Length; i++)
        {
            using var safeExpr = configs[i].By.Consume();
            handles[i] = PolarsWrapper.CloneExpr(safeExpr.Handle);
            revArray[i] = configs[i].Reverse;
        }

        var h = PolarsWrapper.LazyFrameBottomK(CloneHandle(), (uint)k, handles, revArray);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Master method: Get the Bottom k rows by multiple Expressions with optional parallel boolean arrays.
    /// Usage: lf.BottomK(10, myColumns.Select(c => Pl.Col(c)))
    /// </summary>
    public LazyFrame BottomK(int k, IEnumerable<Expr> by, bool[]? reverse = null)
    {
        var exprArray = by as Expr[] ?? [.. by];
        if (exprArray.Length == 0) return this;

        var revArray = reverse ?? new bool[exprArray.Length]; 

        var handles = new ExprHandle[exprArray.Length];
        for (int i = 0; i < exprArray.Length; i++)
        {
            handles[i] = PolarsWrapper.CloneExpr(exprArray[i].Handle);
        }

        var h = PolarsWrapper.LazyFrameBottomK(CloneHandle(), (uint)k, handles, revArray);
        return new LazyFrame(h);
    }
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Get the top k rows according to the given expressions.
    /// <para>This selects the largest values.</para>
    /// </summary>
    public DataFrame TopK(int k, IEnumerable<Expr> by, bool[] reverse) => Lazy().TopK(k, by, reverse) .Collect();

    /// <summary>
    /// Get the top k rows according to a single expression.
    /// </summary>
    public DataFrame TopK(int k, IntoColumnExpr by, bool reverse = false) => Lazy().TopK(k, by, reverse).Collect();
    /// <summary>
    /// Master method: Get the top k rows by multiple Expressions with optional parallel boolean arrays.
    /// Usage: df.TopK(10, myColumns.Select(c => Pl.Col(c)))
    /// </summary>
    public DataFrame TopK(int k, IEnumerable<string> colName, bool reverse = false) => Lazy().TopK(k, colName, reverse).Collect();
    /// <summary>
    /// Get the top k rows with specific reverse directions using Tuples.
    /// Usage: df.TopK(5, ("Score", false), ("Age", true), (Cs.Numeric(), false))
    /// </summary>
    public DataFrame TopK(int k, params (IntoColumnExpr By, bool Reverse)[] configs) => Lazy().TopK(k, configs).Collect();
    /// <summary>
    /// Get the top k rows according to the given expressions.
    /// <para>This selects the largest values.</para>
    /// </summary>
    public DataFrame BottomK(int k, IEnumerable<Expr> by, bool[] reverse) => Lazy().BottomK(k, by, reverse) .Collect();

    /// <summary>
    /// Get the Bottom k rows according to a single expression.
    /// </summary>
    public DataFrame BottomK(int k, IntoColumnExpr by, bool reverse = false) => Lazy().BottomK(k, by, reverse).Collect();
    /// <summary>
    /// Master method: Get the Bottom k rows by multiple Expressions with optional parallel boolean arrays.
    /// Usage: df.BottomK(10, myColumns.Select(c => Pl.Col(c)))
    /// </summary>
    public DataFrame BottomK(int k, IEnumerable<string> colName, bool reverse = false) => Lazy().BottomK(k, colName, reverse).Collect();
    /// <summary>
    /// Get the Bottom k rows with specific reverse directions using Tuples.
    /// Usage: df.BottomK(5, ("Score", false), ("Age", true), (Cs.Numeric(), false))
    /// </summary>
    public DataFrame BottomK(int k, params (IntoColumnExpr By, bool Reverse)[] configs) => Lazy().BottomK(k, configs).Collect();
}