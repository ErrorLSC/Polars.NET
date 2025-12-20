namespace Polars.NET.Core;

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
    private static ExprHandle UnaryStrOp(Func<ExprHandle, string, ExprHandle> func, ExprHandle e, string arg)
    {
        var h = func(e, arg);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    private static ExprHandle UnaryStrOpNullable(Func<ExprHandle, string?, ExprHandle> func, ExprHandle e, string? arg)
    {
        var h = func(e, arg);
        
        // [内存模型] Expr 是 Move 语义，必须转移所有权给 Rust
        e.TransferOwnership();
        
        return ErrorHelper.Check(h);
    }
    private static ExprHandle UnaryDtOp(Func<ExprHandle, ExprHandle> op, ExprHandle expr) 
        => UnaryOp(op, expr);
    public static ExprHandle RollingOp(Func<ExprHandle, string ,UIntPtr,ExprHandle> op, ExprHandle expr, string windowSize,int minPeriods)
    {
        var h = op(expr, windowSize,(UIntPtr)minPeriods);
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    private static ExprHandle RollingByOp(Func<ExprHandle, string, UIntPtr,ExprHandle, string, ExprHandle> op, ExprHandle expr, string windowSize, int minPeriods,ExprHandle by, string closed)
    {
        var h = op(expr, windowSize,(UIntPtr)minPeriods, by, closed);
        expr.TransferOwnership();
        by.TransferOwnership(); // by 也是 Expr，会被消耗
        return ErrorHelper.Check(h);
    }
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
    public static ExprHandle Lit(float val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_f32(val));
    public static ExprHandle Lit(long val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_i64(val));
    public static ExprHandle Lit(bool val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_bool(val));
    public static ExprHandle LitNull() => ErrorHelper.Check(NativeBindings.pl_expr_lit_null());
    public static ExprHandle Lit(DateTime dt)
    {
        // C# DateTime.Ticks 是自 0001-01-01 以来的 100ns 单位
        // Unix Epoch 是 1970-01-01
        long unixEpochTicks = 621355968000000000;
        long ticksSinceEpoch = dt.Ticks - unixEpochTicks;
        long micros = ticksSinceEpoch / 10; // 100ns -> 1us (除以10)
        
        return ErrorHelper.Check(NativeBindings.pl_expr_lit_datetime(micros));
    }
    // Alias

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
    // Truncate / Round (Expr + String)
    public static ExprHandle DtTruncate(ExprHandle e, string every) 
        => UnaryStrOp(NativeBindings.pl_expr_dt_truncate, e, every); // 注意这里用 UnaryStrOp

    public static ExprHandle DtRound(ExprHandle e, string every)
        => UnaryStrOp(NativeBindings.pl_expr_dt_round, e, every);

    // OffsetBy (Expr + Expr)
    public static ExprHandle DtOffsetBy(ExprHandle e, ExprHandle by)
        => BinaryOp(NativeBindings.pl_expr_dt_offset_by, e, by);

    // Timestamp (Expr + Int)
    public static ExprHandle DtTimestamp(ExprHandle e, int unitCode)
    {
        var h = NativeBindings.pl_expr_dt_timestamp(e, unitCode);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    // TimeZone
    public static ExprHandle DtConvertTimeZone(ExprHandle e, string timeZone)
    {
        // 这是一个 UnaryStrOp，但我们可以直接手写以复用 TransferOwnership 逻辑
        // 或者复用之前的辅助函数 if you have generic one
        var h = NativeBindings.pl_expr_dt_convert_time_zone(e, timeZone);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle DtReplaceTimeZone(
        ExprHandle e, 
        string? timeZone, 
        string? ambiguous = null, 
        string? nonExistent = "raise")
    {
        var h = NativeBindings.pl_expr_dt_replace_time_zone(e, timeZone, ambiguous, nonExistent);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
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

    public static ExprHandle StrReplaceAll(ExprHandle e, string pat, string val,bool useRegex = false)
    {
        var h = NativeBindings.pl_expr_str_replace_all(e, pat, val,useRegex);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrSplit(ExprHandle e, string pat) 
    {
        var h = NativeBindings.pl_expr_str_split(e, pat);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrExtract(ExprHandle e, string pat, uint groupIndex)
    {
        var h = NativeBindings.pl_expr_str_extract(e, pat, groupIndex);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StrStripChars(ExprHandle e, string? matches = null)
        => UnaryStrOpNullable(NativeBindings.pl_expr_str_strip_chars, e, matches);

    public static ExprHandle StrStripCharsStart(ExprHandle e, string? matches = null)
        => UnaryStrOpNullable(NativeBindings.pl_expr_str_strip_chars_start, e, matches);

    public static ExprHandle StrStripCharsEnd(ExprHandle e, string? matches = null)
        => UnaryStrOpNullable(NativeBindings.pl_expr_str_strip_chars_end, e, matches);
    public static ExprHandle StrStripPrefix(ExprHandle e, string prefix)
        => UnaryStrOp(NativeBindings.pl_expr_str_strip_prefix, e, prefix);

    public static ExprHandle StrStripSuffix(ExprHandle e, string suffix)
        => UnaryStrOp(NativeBindings.pl_expr_str_strip_suffix, e, suffix);
    public static ExprHandle StrStartsWith(ExprHandle e, string prefix)
        => UnaryStrOp(NativeBindings.pl_expr_str_starts_with, e, prefix);

    public static ExprHandle StrEndsWith(ExprHandle e, string suffix)
        => UnaryStrOp(NativeBindings.pl_expr_str_ends_with, e, suffix);

    public static ExprHandle StrToDate(ExprHandle e, string format)
        => UnaryStrOp(NativeBindings.pl_expr_str_to_date, e, format);

    public static ExprHandle StrToDatetime(ExprHandle e, string format)
        => UnaryStrOp(NativeBindings.pl_expr_str_to_datetime, e, format);
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
    public static ExprHandle Xor(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_xor, l, r);

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
    // Statistics
    public static ExprHandle Count(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_count, e);
    public static ExprHandle Std(ExprHandle e, int ddof) 
    {
        var h = NativeBindings.pl_expr_std(e, (byte)ddof);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle Var(ExprHandle e, int ddof)
    {
        var h = NativeBindings.pl_expr_var(e, (byte)ddof);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle Median(ExprHandle e) => UnaryOp(NativeBindings.pl_expr_median, e);
    
    public static ExprHandle Quantile(ExprHandle e, double quantile, string method)
    {
        var h = NativeBindings.pl_expr_quantile(e, quantile, method);
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
    public static ExprHandle StructFieldByIndex(ExprHandle e, long index)
        {
            var h = NativeBindings.pl_expr_struct_field_by_index(e, index);
            e.TransferOwnership(); // 链式调用惯例
            return ErrorHelper.Check(h);
        }

    public static ExprHandle StructRenameFields(ExprHandle e, string[] names)
    {
        var h = NativeBindings.pl_expr_struct_rename_fields(e, names, (UIntPtr)names.Length);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle StructJsonEncode(ExprHandle e)
    {
        var h = NativeBindings.pl_expr_struct_json_encode(e);
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
    public static ExprHandle ExprCast(ExprHandle expr, DataTypeHandle dtype, bool strict)
    {
        return ErrorHelper.Check(NativeBindings.pl_expr_cast(expr, dtype, strict));
    }

    // Shift
    public static ExprHandle Shift(ExprHandle e, long n)
    {
        var h = NativeBindings.pl_expr_shift(e, n);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    // Diff
    public static ExprHandle Diff(ExprHandle e, long n)
    {
        var h = NativeBindings.pl_expr_diff(e, n);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    // Fill
    public static ExprHandle ForwardFill(ExprHandle e, uint limit)
    {
        var h = NativeBindings.pl_expr_forward_fill(e, limit);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle BackwardFill(ExprHandle e, uint limit)
    {
        var h = NativeBindings.pl_expr_backward_fill(e, limit);
        e.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static ExprHandle RollingMean(ExprHandle e, string w, int minPeriods) => RollingOp(NativeBindings.pl_expr_rolling_mean, e, w, minPeriods);
    public static ExprHandle RollingMax(ExprHandle e, string w, int minPeriods) => RollingOp(NativeBindings.pl_expr_rolling_max, e, w, minPeriods);
    public static ExprHandle RollingMin(ExprHandle e, string w, int minPeriods) => RollingOp(NativeBindings.pl_expr_rolling_min, e, w, minPeriods);
    public static ExprHandle RollingSum(ExprHandle e, string w, int minPeriods) => RollingOp(NativeBindings.pl_expr_rolling_sum, e, w, minPeriods);
    public static ExprHandle RollingMeanBy(ExprHandle e, string w, int minPeriods, ExprHandle by, string closed) => RollingByOp(NativeBindings.pl_expr_rolling_mean_by, e, w, minPeriods, by, closed);
    public static ExprHandle RollingSumBy(ExprHandle e, string w, int minPeriods, ExprHandle by, string closed) => RollingByOp(NativeBindings.pl_expr_rolling_sum_by, e, w, minPeriods, by, closed);
    public static ExprHandle RollingMinBy(ExprHandle e, string w, int minPeriods, ExprHandle by, string closed) => RollingByOp(NativeBindings.pl_expr_rolling_min_by, e, w, minPeriods, by, closed);
    public static ExprHandle RollingMaxBy(ExprHandle e, string w, int minPeriods, ExprHandle by, string closed) => RollingByOp(NativeBindings.pl_expr_rolling_max_by, e, w, minPeriods, by, closed);
    public static ExprHandle IfElse(ExprHandle pred, ExprHandle ifTrue, ExprHandle ifFalse)
    {
        var h = NativeBindings.pl_expr_if_else(pred, ifTrue, ifFalse);
        
        // 三个输入都被消耗了
        pred.TransferOwnership();
        ifTrue.TransferOwnership();
        ifFalse.TransferOwnership();
        
        return ErrorHelper.Check(h);
    }
}