using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// Polars Static Helpers
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
    public static Expr Col(params string[] names)
    {
        return new Expr(PolarsWrapper.Cols(names));
    }
    /// <summary>
    /// Return the lines count of current context.
    /// </summary>
    public static Expr Len()
        => new(PolarsWrapper.Len());
    // --- Literals ---
    /// <summary>
    /// Create a literal expression from a string value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Expr Lit(string value) => new(PolarsWrapper.Lit(value));
    /// <summary>
    /// Create a literal expression from a int value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Expr Lit(int value)    => new(PolarsWrapper.Lit(value));
    /// <summary>
    /// Create a literal expression from a double value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Expr Lit(double value) => new(PolarsWrapper.Lit(value));
    /// <summary>
    /// Create a literal expression from a DateTime value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Expr Lit(DateTime value) => new(PolarsWrapper.Lit(value));
    /// <summary>
    /// Create a literal expression from a long value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Expr Lit(bool value) => new(PolarsWrapper.Lit(value));
    /// <summary>
    /// Create a literal expression from a long value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Expr Lit(long value) => new(PolarsWrapper.Lit(value));
    /// <summary>
    /// Create a literal expression from a float value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Expr Lit(float value) => new(PolarsWrapper.Lit(value));
    
    // ---------------------------------------------------------
    // Selectors Entry Points
    // ---------------------------------------------------------

    /// <summary>
    /// Select all columns.
    /// </summary>
    public static Selector All() 
        => new(PolarsWrapper.SelectorAll());

    /// <summary>
    /// Select all numeric columns (Int, Float, etc.).
    /// </summary>
    public static Selector Numeric() 
        => new(PolarsWrapper.SelectorNumeric());

    /// <summary>
    /// Select all string/utf8 columns.
    /// </summary>
    public static Selector String() 
        => new(PolarsWrapper.SelectorByDtype(PlDataType.String));

    /// <summary>
    /// Select all date columns.
    /// </summary>
    public static Selector Date() 
        => new(PolarsWrapper.SelectorByDtype(PlDataType.Date));

    /// <summary>
    /// Select all datetime columns (any precision/timezone).
    /// </summary>
    public static Selector Datetime() 
        => new(PolarsWrapper.SelectorByDtype(PlDataType.Datetime));

    /// <summary>
    /// Select columns by specific PlDataType.
    /// </summary>
    public static Selector DType(PlDataType type) 
        => new(PolarsWrapper.SelectorByDtype(type));

    /// <summary>
    /// String matching selectors namespace.
    /// Usage: Polars.Selectors.StartsWith("A")
    /// </summary>
    public static class Selectors
    {
        /// <summary>
        /// Select column whose name starts with given prefix.
        /// </summary>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public static Selector StartsWith(string prefix) 
            => new(PolarsWrapper.SelectorStartsWith(prefix));
        /// <summary>
        /// Select column whose name ends with given suffix.
        /// </summary>
        /// <param name="suffix"></param>
        /// <returns></returns>
        public static Selector EndsWith(string suffix) 
            => new(PolarsWrapper.SelectorEndsWith(suffix));
        /// <summary>
        /// Select column whose name contains given string.
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public static Selector Contains(string str) 
            => new(PolarsWrapper.SelectorContains(str));
        /// <summary>
        /// Select column whose name matches given string.
        /// </summary>
        /// <param name="regex"></param>
        /// <returns></returns>
        public static Selector Matches(string regex) 
            => new(PolarsWrapper.SelectorMatch(regex));
    }
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
    // ==========================================
    // Struct Operations
    // ==========================================

    /// <summary>
    /// Combine multiple expressions into a Struct expression.
    /// </summary>
    public static Expr AsStruct(params Expr[] exprs)
    {
        // 必须 Clone 所有输入的 Handle，因为 Wrapper 会消耗它们
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new Expr(PolarsWrapper.AsStruct(handles));
    }
    // ==========================================
    // SQL Context
    // ==========================================
    /// <summary>
    /// Create a new SQL Context.
    /// </summary>
    public static SqlContext Sql() => new();
}