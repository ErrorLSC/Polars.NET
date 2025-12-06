using Polars.Native;

namespace Polars.CSharp;

/// <summary>
/// Polars C# API 的主要入口点。
/// </summary>
public static class Polars
{
    /// <summary>
    /// Column Expr (name: string)
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public static Expr Col(string name)
    {
        return new Expr(PolarsWrapper.Col(name));
    }
    /// <summary>
    /// Column Exprs (name: string)
    /// </summary>
    /// <param name="names"></param>
    /// <returns></returns>
    public static Expr Cols(params string[] names)
    {
        return new Expr(PolarsWrapper.Cols(names));
    }
    /// <summary>
    /// All Columns Exprs (name: string)
    /// </summary>
    /// <returns></returns>
    public static Expr All()
    {
        // 假设 Wrapper 有 Col("*") 或者专门的 All 绑定
        return Col("*"); 
    }

    // --- Literals ---
    /// <summary>
    /// Create a literal expression from a string value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Expr Lit(string value) => new Expr(PolarsWrapper.Lit(value));
    /// <summary>
    /// Create a literal expression from a int value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Expr Lit(int value)    => new Expr(PolarsWrapper.Lit(value));
    /// <summary>
    /// Create a literal expression from a double value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Expr Lit(double value) => new Expr(PolarsWrapper.Lit(value));
    /// <summary>
    /// Create a literal expression from a DateTime value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Expr Lit(DateTime value) => new Expr(PolarsWrapper.Lit(value));
    // ==========================================
    // Control Flow
    // ==========================================

    /// <summary>
    /// If-Else control flow: if predicate evaluates to true, return trueExpr, otherwise return falseExpr.
    /// Similar to SQL's CASE WHEN ... THEN ... ELSE ... END.
    /// </summary>
    public static Expr IfElse(Expr predicate, Expr trueExpr, Expr falseExpr)
    {
        // 三个输入都需要 Clone，因为底层会消耗它们
        var p = PolarsWrapper.CloneExpr(predicate.Handle);
        var t = PolarsWrapper.CloneExpr(trueExpr.Handle);
        var f = PolarsWrapper.CloneExpr(falseExpr.Handle);
        
        return new Expr(PolarsWrapper.IfElse(p, t, f));
    }
}