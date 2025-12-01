namespace Polars.Native;

public static partial class PolarsWrapper
{
    // Unary Nodes (消耗 1 个 Expr)
    private static ExprHandle UnaryOp(Func<ExprHandle, ExprHandle> op, ExprHandle expr)
    {
        var h = op(expr);
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // Binary Nodes (消耗 2 个 Expr)
    private static ExprHandle BinaryOp(Func<ExprHandle, ExprHandle, ExprHandle> op, ExprHandle l, ExprHandle r)
    {
        var h = op(l, r);
        l.TransferOwnership();
        r.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    private static ExprHandle UnaryStrOp(Func<ExprHandle, ExprHandle> op, ExprHandle expr) 
    => UnaryOp(op, expr);
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
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // Aggregate
    public static ExprHandle Sum(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_sum, e);
    public static ExprHandle Mean(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_mean, e);
    public static ExprHandle Max(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_max, e);
    public static ExprHandle Min(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_min, e);
    public static ExprHandle Abs(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_abs, e);
    public static ExprHandle DtYear(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_year, e);
    // String Ops
    public static ExprHandle StrContains(ExprHandle e, string pat) 
    {
        var h = NativeBindings.pl_expr_str_contains(e, pat);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle StrToUpper(ExprHandle e) => UnaryStrOp(NativeBindings.pl_expr_str_to_uppercase, e);
    public static ExprHandle StrToLower(ExprHandle e) => UnaryStrOp(NativeBindings.pl_expr_str_to_lowercase, e);
    public static ExprHandle StrLenBytes(ExprHandle e) => UnaryStrOp(NativeBindings.pl_expr_str_len_bytes, e);

    public static ExprHandle StrSlice(ExprHandle e, long offset, ulong length)
    {
        var h = NativeBindings.pl_expr_str_slice(e, offset, length);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle StrReplaceAll(ExprHandle e, string pat, string val)
    {
        var h = NativeBindings.pl_expr_str_replace_all(e, pat, val);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrSplit(ExprHandle e, string pat) 
    {
        var h = NativeBindings.pl_expr_str_split(e, pat);
        e.TransferOwnership();
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
    // Math
    public static ExprHandle Pow(ExprHandle b, ExprHandle e) => BinaryOp(NativeBindings.pl_expr_pow, b, e);
    public static ExprHandle Sqrt(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_sqrt, e);
    public static ExprHandle Exp(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_exp, e);
    public static ExprHandle Log(ExprHandle expr, double baseVal)
    {
        var h = NativeBindings.pl_expr_log(expr, baseVal);
        expr.TransferOwnership(); // 消耗掉 expr
        return ErrorHelper.Check(h);
    }
    public static ExprHandle Round(ExprHandle e, uint decimals)
    {
        var h = NativeBindings.pl_expr_round(e, decimals);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // IsBetween
    public static ExprHandle IsBetween(ExprHandle expr, ExprHandle lower, ExprHandle upper)
    {
        var h = NativeBindings.pl_expr_is_between(expr, lower, upper);
        // 记得销毁所有输入 Handle
        expr.TransferOwnership();
        lower.TransferOwnership();
        upper.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    // Lit DateTime
    public static ExprHandle Lit(DateTime dt)
    {
        // C# DateTime.Ticks 是自 0001-01-01 以来的 100ns 单位
        // Unix Epoch 是 1970-01-01
        long unixEpochTicks = 621355968000000000;
        long ticksSinceEpoch = dt.Ticks - unixEpochTicks;
        long micros = ticksSinceEpoch / 10; // 100ns -> 1us (除以10)
        
        return ErrorHelper.Check(NativeBindings.pl_expr_lit_datetime(micros));
    }

    // List
    public static ExprHandle ListFirst(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_list_first, e);
    
    public static ExprHandle ListGet(ExprHandle e, long index)
    {
        var h = NativeBindings.pl_expr_list_get(e, index);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    // Naming
    public static ExprHandle Prefix(ExprHandle e, string p)
    {
        var h = NativeBindings.pl_expr_prefix(e, p);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    
    public static ExprHandle Suffix(ExprHandle e, string s)
    {
        var h = NativeBindings.pl_expr_suffix(e, s);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // Expr Length
    public static ExprHandle Len() => ErrorHelper.Check(NativeBindings.pl_expr_len());
    // expr clone
    public static ExprHandle CloneExpr(ExprHandle expr)
    {
        return ErrorHelper.Check(NativeBindings.pl_expr_clone(expr));
    }

}