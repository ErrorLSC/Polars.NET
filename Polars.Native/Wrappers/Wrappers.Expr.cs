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
    private static ExprHandle UnaryDtOp(Func<ExprHandle, ExprHandle> op, ExprHandle expr) 
        => UnaryOp(op, expr);
    // --- Expr Ops (工厂方法) ---
    // 这些方法返回新的 ExprHandle，所有权在 C# 这边，直到传给 Filter/Select
    // Leaf Nodes (不消耗其他 Expr)
    public static ExprHandle Col(string name) => ErrorHelper.Check(NativeBindings.pl_expr_col(name));
    public static ExprHandle Cols(string[] names)
    {
        return UseUtf8StringArray(names, ptrs => 
        {
            return ErrorHelper.Check(NativeBindings.pl_expr_cols(ptrs, (UIntPtr)ptrs.Length));
        });
    }
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
    // Temporal
    public static ExprHandle DtYear(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_dt_year, e);
    public static ExprHandle DtMonth(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_month, e);
    public static ExprHandle DtDay(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_day, e);
    public static ExprHandle DtOrdinalDay(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_ordinal_day, e);
    public static ExprHandle DtWeekday(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_weekday, e);
    public static ExprHandle DtHour(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_hour, e);
    public static ExprHandle DtMinute(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_minute, e);
    public static ExprHandle DtSecond(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_second, e);
    public static ExprHandle DtMillisecond(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_millisecond, e);
    public static ExprHandle DtMicrosecond(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_microsecond, e);
    public static ExprHandle DtNanosecond(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_nanosecond, e);
    
    public static ExprHandle DtToString(ExprHandle e, string format)
    {
        var h = NativeBindings.pl_expr_dt_to_string(e, format);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle DtDate(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_date, e);
    public static ExprHandle DtTime(ExprHandle e) => UnaryDtOp(NativeBindings.pl_expr_dt_time, e);
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

    public static ExprHandle Explode(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_explode, e);
    
    public static ExprHandle ListJoin(ExprHandle e, string sep)
    {
        var h = NativeBindings.pl_expr_list_join(e, sep);
        e.TransferOwnership(); // [关键] 必须转移所有权
        return ErrorHelper.Check(h);
    }

    public static ExprHandle ListLen(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_list_len, e);
    // --- List Aggs ---
    public static ExprHandle ListSum(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_list_sum, e);
    public static ExprHandle ListMin(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_list_min, e);
    public static ExprHandle ListMax(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_list_max, e);
    public static ExprHandle ListMean(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_list_mean, e);

    // --- List Other ---
    public static ExprHandle ListSort(ExprHandle e, bool descending)
    {
        var h = NativeBindings.pl_expr_list_sort(e, descending);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle ListContains(ExprHandle listExpr, ExprHandle itemExpr)
    {
        var h = NativeBindings.pl_expr_list_contains(listExpr, itemExpr);
        listExpr.TransferOwnership();
        itemExpr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // --- Struct ---
    public static ExprHandle AsStruct(ExprHandle[] exprs)
    {
        var raw = HandlesToPtrs(exprs);
        return ErrorHelper.Check(NativeBindings.pl_expr_as_struct(raw, (UIntPtr)raw.Length));
    }

    public static ExprHandle StructFieldByName(ExprHandle e, string name)
    {
        var h = NativeBindings.pl_expr_struct_field_by_name(e, name);
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
    // Window
    public static ExprHandle Over(ExprHandle expr, ExprHandle[] partitionBy)
    {
        // 1. 处理分组列表 (HandlesToPtrs 会自动 TransferOwnership)
        var rawPartition = HandlesToPtrs(partitionBy);
        
        // 2. 调用 Native
        var h = NativeBindings.pl_expr_over(expr, rawPartition, (UIntPtr)rawPartition.Length);
        
        // 3. 处理主表达式 (必须 TransferOwnership)
        expr.TransferOwnership();
        
        return ErrorHelper.Check(h);
    }
    // Expr Length
    public static ExprHandle Len() => ErrorHelper.Check(NativeBindings.pl_expr_len());
    // expr clone
    public static ExprHandle CloneExpr(ExprHandle expr)
    {
        return ErrorHelper.Check(NativeBindings.pl_expr_clone(expr));
    }
    public static ExprHandle Cast(ExprHandle expr, PlDataType dtype, bool strict = false)
    {
        var h = NativeBindings.pl_expr_cast(expr, dtype, strict);
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
}