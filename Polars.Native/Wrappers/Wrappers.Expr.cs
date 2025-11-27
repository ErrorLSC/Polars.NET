namespace Polars.Native;

public static partial class PolarsWrapper
{
    // Unary Nodes (消耗 1 个 Expr)
    private static ExprHandle UnaryOp(Func<ExprHandle, ExprHandle> op, ExprHandle expr)
    {
        var h = op(expr);
        expr.SetHandleAsInvalid();
        return ErrorHelper.Check(h);
    }
    // Binary Nodes (消耗 2 个 Expr)
    private static ExprHandle BinaryOp(Func<ExprHandle, ExprHandle, ExprHandle> op, ExprHandle l, ExprHandle r)
    {
        var h = op(l, r);
        l.SetHandleAsInvalid();
        r.SetHandleAsInvalid();
        return ErrorHelper.Check(h);
    }
    // --- Expr Ops (工厂方法) ---
    // 这些方法返回新的 ExprHandle，所有权在 C# 这边，直到传给 Filter/Select
    // Leaf Nodes (不消耗其他 Expr)
    public static ExprHandle Col(string name) => ErrorHelper.Check(NativeBindings.pl_expr_col(name));
    public static ExprHandle Lit(int val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_i32(val));
    public static ExprHandle Lit(string val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_str(val));
    public static ExprHandle Lit(double val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_f64(val));

    public static ExprHandle Alias(ExprHandle expr, string name) 
    {
        var h = NativeBindings.pl_expr_alias(expr, name);
        expr.SetHandleAsInvalid();
        return ErrorHelper.Check(h);
    }
    // Aggregate
    public static ExprHandle Sum(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_sum, e);
    public static ExprHandle Mean(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_mean, e);
    public static ExprHandle Max(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_max, e);
    public static ExprHandle Min(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_min, e);
    public static ExprHandle DtYear(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_year, e);
    public static ExprHandle StrContains(ExprHandle e, string pat) 
    {
        var h = NativeBindings.pl_expr_str_contains(e, pat);
        e.SetHandleAsInvalid();
        return ErrorHelper.Check(h);
    }


    // Compare
    public static ExprHandle Eq(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_eq, l, r);
    public static ExprHandle Neq(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_neq, l, r);
    public static ExprHandle Gt(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_gt, l, r);
    public static ExprHandle GtEq(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_gt_eq, l, r);
    public static ExprHandle Lt(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_lt, l, r);
    public static ExprHandle LtEq(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_lt_eq, l, r);
    // Arithmetic
    public static ExprHandle Add(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_add, l, r);
    public static ExprHandle Sub(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_sub, l, r);
    public static ExprHandle Div(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_div, l, r);
    public static ExprHandle Rem(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_rem, l, r);
    public static ExprHandle Mul(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_mul, l, r);
    // Logic
    public static ExprHandle And(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_and, l, r);
    public static ExprHandle Or(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_or, l, r);
    public static ExprHandle Not(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_not, e);

    // Null Handling
    public static ExprHandle FillNull(ExprHandle expr, ExprHandle fillValue) 
        => BinaryOp(NativeBindings.pl_expr_fill_null, expr, fillValue);

    public static ExprHandle IsNull(ExprHandle expr) 
        => UnaryOp(NativeBindings.pl_expr_is_null, expr);

    public static ExprHandle IsNotNull(ExprHandle expr) 
        => UnaryOp(NativeBindings.pl_expr_is_not_null, expr);
    
    // expr clone
    public static ExprHandle CloneExpr(ExprHandle expr)
    {
        return ErrorHelper.Check(NativeBindings.pl_expr_clone(expr));
    }

}