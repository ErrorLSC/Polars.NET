using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;

namespace Polars.Native;

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public delegate void CleanupCallback(IntPtr userData);

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public unsafe delegate int UdfCallback(
    CArrowArray* inArray, 
    CArrowSchema* inSchema, 
    CArrowArray* outArray, 
    CArrowSchema* outSchema,
    byte* msgBuf
);
unsafe internal partial class NativeBindings
{
    const string LibName = "native_shim";
    
    [DllImport(LibName)] public static extern void pl_expr_free(IntPtr ptr);
    [DllImport(LibName)] public static extern void pl_lazy_frame_free(IntPtr ptr);
    [DllImport(LibName)] public static extern void pl_selector_free(IntPtr ptr);
    [DllImport(LibName)]
    public static extern DataFrameHandle pl_read_csv([MarshalAs(UnmanagedType.LPUTF8Str)] string path,
    bool tryParseDates
    );
    [DllImport(LibName)]
    public static extern void pl_dataframe_free(IntPtr ptr);
    // String Free
    [DllImport(LibName)] public static extern void pl_free_string(IntPtr ptr);
    [DllImport(LibName)] public static extern void pl_to_arrow(DataFrameHandle handle, CArrowArray* arr, CArrowSchema* schema);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_col([MarshalAs(UnmanagedType.LPUTF8Str)] string name);
    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_cols(IntPtr[] names, UIntPtr len);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_lit_i32(int val);

    [DllImport(LibName)]
    public static extern UIntPtr pl_dataframe_height(DataFrameHandle df);
    
    [DllImport(LibName)]
    public static extern UIntPtr pl_dataframe_width(DataFrameHandle df);
    [DllImport(LibName)] public static extern IntPtr pl_dataframe_get_column_name(DataFrameHandle df, UIntPtr index);
    // Scalars
    [DllImport(LibName)] 
    public static extern bool pl_dataframe_get_i64(
        DataFrameHandle df, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string colName, 
        UIntPtr row, 
        out long outVal // <--- C# 的 out 关键字
    );

    [DllImport(LibName)] 
    public static extern bool pl_dataframe_get_f64(
        DataFrameHandle df, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string colName, 
        UIntPtr row, 
        out double outVal
    );
    [DllImport(LibName)] public static extern IntPtr pl_dataframe_get_string(DataFrameHandle df, [MarshalAs(UnmanagedType.LPUTF8Str)] string colName, UIntPtr row);
    [DllImport(LibName)]
    public static extern DataFrameHandle pl_head(DataFrameHandle df, UIntPtr n);
    [DllImport(LibName)]
    public static extern DataFrameHandle pl_tail(DataFrameHandle df, UIntPtr n);

    [DllImport(LibName)]
    public static extern DataFrameHandle pl_filter(DataFrameHandle df, ExprHandle expr);

    [DllImport(LibName)] 
    public static extern DataFrameHandle pl_with_columns(DataFrameHandle df, IntPtr[] exprs, UIntPtr len);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_lit_str([MarshalAs(UnmanagedType.LPUTF8Str)] string val);

    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_lit_f64(double val);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_mul(ExprHandle left, ExprHandle right);
    // 比较
    [DllImport(LibName)] public static extern ExprHandle pl_expr_eq(ExprHandle left, ExprHandle right);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_neq(ExprHandle l, ExprHandle r);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_gt(ExprHandle left, ExprHandle right);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_gt_eq(ExprHandle l, ExprHandle r);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_lt(ExprHandle l, ExprHandle r);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_lt_eq(ExprHandle l, ExprHandle r);

    // 算术
    [DllImport(LibName)] public static extern ExprHandle pl_expr_add(ExprHandle l, ExprHandle r);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_sub(ExprHandle l, ExprHandle r);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_div(ExprHandle l, ExprHandle r);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_rem(ExprHandle l, ExprHandle r);

    // 逻辑
    [DllImport(LibName)] public static extern ExprHandle pl_expr_and(ExprHandle l, ExprHandle r);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_or(ExprHandle l, ExprHandle r);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_not(ExprHandle e);
    // 聚合
    [DllImport(LibName)] public static extern ExprHandle pl_expr_sum(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_mean(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_max(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_min(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_abs(ExprHandle expr);
    // null ops
    [DllImport(LibName)] public static extern ExprHandle pl_expr_fill_null(ExprHandle expr, ExprHandle fillValue);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_is_null(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_is_not_null(ExprHandle expr);
    // Math ops
    [DllImport(LibName)] public static extern ExprHandle pl_expr_pow(ExprHandle baseExpr, ExprHandle exponent);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_sqrt(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_exp(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_log(ExprHandle expr, double baseVal);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_round(ExprHandle expr, uint decimals);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_is_between(ExprHandle expr, ExprHandle lower, ExprHandle upper);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_lit_datetime(long micros);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_alias(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string name);

    [DllImport(LibName)]
    public static extern DataFrameHandle pl_select(DataFrameHandle df, IntPtr[] exprs, UIntPtr len);
    // Temporal
    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_dt_year(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_dt_month(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_dt_day(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_dt_ordinal_day(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_dt_weekday(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_dt_hour(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_dt_minute(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_dt_second(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_dt_millisecond(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_dt_microsecond(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_dt_nanosecond(ExprHandle expr);
    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_dt_to_string(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string format);
    
    [DllImport(LibName)] public static extern ExprHandle pl_expr_dt_date(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_dt_time(ExprHandle expr);
    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_clone(ExprHandle expr);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_map(
        ExprHandle expr, 
        UdfCallback callback, 
        PlDataType returnType,
        CleanupCallback cleanup,
        IntPtr userData          
    );

    [DllImport(LibName)]
    public static extern DataFrameHandle pl_groupby_agg(
        DataFrameHandle df, 
        IntPtr[] byExprs, UIntPtr byLen,
        IntPtr[] aggExprs, UIntPtr aggLen
    );

    // Join 签名
    [DllImport(LibName)]
    public static extern DataFrameHandle pl_join(
        DataFrameHandle left,
        DataFrameHandle right,
        IntPtr[] leftOn, UIntPtr leftLen,
        IntPtr[] rightOn, UIntPtr rightLen,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string how
    );
    [DllImport(LibName)] 
    public static extern DataFrameHandle pl_sort(DataFrameHandle df, ExprHandle expr, bool descending);
    [DllImport(LibName)] 
    public static extern DataFrameHandle pl_explode(DataFrameHandle df, IntPtr[] exprs, UIntPtr len);
    // Parquet
    [DllImport(LibName)] 
    public static extern void pl_write_csv(DataFrameHandle df, [MarshalAs(UnmanagedType.LPUTF8Str)] string path);
    [DllImport(LibName)] 
    public static extern void pl_write_parquet(DataFrameHandle df, [MarshalAs(UnmanagedType.LPUTF8Str)] string path);
    [DllImport(LibName)] 
    public static extern DataFrameHandle pl_read_parquet([MarshalAs(UnmanagedType.LPUTF8Str)] string path);

    // Lazy
    [DllImport(LibName)] 
    public static extern LazyFrameHandle pl_scan_csv([MarshalAs(UnmanagedType.LPUTF8Str)] string path,
    bool tryParseDates
    );

    [DllImport(LibName)] 
    public static extern LazyFrameHandle pl_scan_parquet([MarshalAs(UnmanagedType.LPUTF8Str)] string path);
    
    [DllImport(LibName)] 
    public static extern LazyFrameHandle pl_lazy_filter(LazyFrameHandle lf, ExprHandle expr);
    [DllImport(LibName)] 
    public static extern LazyFrameHandle pl_lazy_select(LazyFrameHandle lf, IntPtr[] exprs, UIntPtr len);
    [DllImport(LibName)] 
    public static extern LazyFrameHandle pl_lazy_sort(LazyFrameHandle lf, ExprHandle expr, bool desc);
    [DllImport(LibName)] 
    public static extern LazyFrameHandle pl_lazy_groupby_agg(
        LazyFrameHandle lf, 
        IntPtr[] keys, UIntPtr keysLen, 
        IntPtr[] aggs, UIntPtr aggsLen
    );
    [DllImport(LibName)]
    public static extern DataFrameHandle pl_lazy_collect(LazyFrameHandle lf);
    [DllImport(LibName)]
    public static extern LazyFrameHandle pl_lazy_clone(LazyFrameHandle lf);

    [DllImport(LibName)] public static extern LazyFrameHandle pl_lazy_limit(LazyFrameHandle lf, uint n);
    [DllImport(LibName)] public static extern LazyFrameHandle pl_lazy_with_columns(LazyFrameHandle lf, IntPtr[] exprs, UIntPtr len);
    [DllImport(LibName)] 
    public static extern LazyFrameHandle pl_lazy_explode(LazyFrameHandle lf, IntPtr[] exprs, UIntPtr len);
    // --- Reshaping (Lazy) ---
    [DllImport(LibName)] 
    public static extern LazyFrameHandle pl_lazy_unpivot(
        LazyFrameHandle lf,
        IntPtr[] idVars, UIntPtr idLen,
        IntPtr[] valVars, UIntPtr valLen,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string varName,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string valName
    );

    // --- Streaming & Sink ---
    [DllImport(LibName)] 
    public static extern DataFrameHandle pl_lazy_collect_streaming(LazyFrameHandle lf);

    [DllImport(LibName)] 
    public static extern void pl_lazy_sink_parquet(
        LazyFrameHandle lf, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path
    );
    // String Ops
    [DllImport(LibName)] public static extern ExprHandle pl_expr_str_contains(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string pat);

    [DllImport(LibName)] public static extern ExprHandle pl_expr_str_to_uppercase(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_str_to_lowercase(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_str_len_bytes(ExprHandle expr);
    
    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_str_slice(ExprHandle expr, long offset, ulong length);
    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_str_split(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string pat);
    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_str_replace_all(
        ExprHandle expr, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string pat, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string val
    );

    // List Ops
    [DllImport(LibName)] public static extern ExprHandle pl_expr_list_first(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_list_get(ExprHandle expr, long index);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_explode(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_list_join(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string sep);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_list_len(ExprHandle expr);
    // List Aggs
    [DllImport(LibName)] public static extern ExprHandle pl_expr_list_sum(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_list_min(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_list_max(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_list_mean(ExprHandle expr);
    
    // List Other
    [DllImport(LibName)] public static extern ExprHandle pl_expr_list_sort(ExprHandle expr, bool descending);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_list_contains(ExprHandle expr, ExprHandle item);
    // Naming
    [DllImport(LibName)] public static extern ExprHandle pl_expr_prefix(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string prefix);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_suffix(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string suffix);
    // --- Reshaping (Eager) ---
    [DllImport(LibName)] 
    public static extern DataFrameHandle pl_pivot(
        DataFrameHandle df,
        IntPtr[] values, UIntPtr valuesLen,
        IntPtr[] index, UIntPtr indexLen,
        IntPtr[] columns, UIntPtr columnsLen,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string aggFn
    );

    [DllImport(LibName)] 
    public static extern DataFrameHandle pl_unpivot(
        DataFrameHandle df,
        IntPtr[] idVars, UIntPtr idLen,
        IntPtr[] valVars, UIntPtr valLen,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string varName,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string valName
    );
    // Expr Len
    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_len();
    [DllImport(LibName)] public static extern IntPtr pl_get_last_error();
    [DllImport(LibName)] public static extern void pl_free_error_msg(IntPtr ptr);
    [DllImport(LibName)] 
    public static extern SelectorHandle pl_selector_clone(SelectorHandle sel);
    // Selectors
    [DllImport(LibName)] public static extern SelectorHandle pl_selector_all();
    
    [DllImport(LibName)] 
    public static extern SelectorHandle pl_selector_exclude(
        SelectorHandle sel, 
        IntPtr[] names,
        UIntPtr len
    );

    [DllImport(LibName)] public static extern ExprHandle pl_selector_into_expr(SelectorHandle sel);
    // Struct
    [DllImport(LibName)] public static extern ExprHandle pl_expr_as_struct(IntPtr[] exprs, UIntPtr len);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_struct_field_by_name(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string name);
    // Window
    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_over(
        ExprHandle expr, 
        IntPtr[] partitionBy, 
        UIntPtr len
    );
}