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
    
    [LibraryImport(LibName)] public static partial void pl_expr_free(IntPtr ptr);
    [LibraryImport(LibName)] public static partial void pl_lazy_frame_free(IntPtr ptr);
    [LibraryImport(LibName)] public static partial void pl_selector_free(IntPtr ptr);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial DataFrameHandle pl_read_csv(string path,[MarshalAs(UnmanagedType.U1)] bool tryParseDates);
    [LibraryImport(LibName)]
    public static partial void pl_dataframe_free(IntPtr ptr);
    // String Free
    [LibraryImport(LibName)] public static partial void pl_free_string(IntPtr ptr);
    [LibraryImport(LibName)] public static partial void pl_to_arrow(DataFrameHandle handle, CArrowArray* arr, CArrowSchema* schema);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial ExprHandle pl_expr_col(string name);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_cols(IntPtr[] names, UIntPtr len);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_lit_i32(int val);

    [LibraryImport(LibName)]
    public static partial UIntPtr pl_dataframe_height(DataFrameHandle df);
    
    [LibraryImport(LibName)]
    public static partial UIntPtr pl_dataframe_width(DataFrameHandle df);
    [LibraryImport(LibName)] public static partial IntPtr pl_dataframe_get_column_name(DataFrameHandle df, UIntPtr index);
    // Scalars
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_dataframe_get_i64(
        DataFrameHandle df, 
        string colName, // 这里不需要 [MarshalAs] 了，上面统一定义了
        UIntPtr row, 
        out long outVal // <--- 基础类型的 out 不需要任何修饰，直接用！
    );

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_dataframe_get_f64(
        DataFrameHandle df, 
        string colName, 
        UIntPtr row, 
        out double outVal // <--- double 也是 blittable 类型，直接用
    );
    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_dataframe_clone(DataFrameHandle df);
    [LibraryImport(LibName)] public static partial IntPtr pl_dataframe_get_string(DataFrameHandle df, [MarshalAs(UnmanagedType.LPUTF8Str)] string colName, UIntPtr row);
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_head(DataFrameHandle df, UIntPtr n);
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_tail(DataFrameHandle df, UIntPtr n);

    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_filter(DataFrameHandle df, ExprHandle expr);

    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_with_columns(DataFrameHandle df, IntPtr[] exprs, UIntPtr len);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_lit_str([MarshalAs(UnmanagedType.LPUTF8Str)] string val);

    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_lit_f64(double val);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_mul(ExprHandle left, ExprHandle right);
    // 比较
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_eq(ExprHandle left, ExprHandle right);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_neq(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_gt(ExprHandle left, ExprHandle right);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_gt_eq(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lt(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_lt_eq(ExprHandle l, ExprHandle r);

    // 算术
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_add(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_sub(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_div(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rem(ExprHandle l, ExprHandle r);

    // 逻辑
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_and(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_or(ExprHandle l, ExprHandle r);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_not(ExprHandle e);
    // 聚合
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_sum(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_mean(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_max(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_min(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_abs(ExprHandle expr);
    // null ops
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_fill_null(ExprHandle expr, ExprHandle fillValue);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_is_null(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_is_not_null(ExprHandle expr);
    // Math ops
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_pow(ExprHandle baseExpr, ExprHandle exponent);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_sqrt(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_exp(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_log(ExprHandle expr, double baseVal);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_round(ExprHandle expr, uint decimals);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_is_between(ExprHandle expr, ExprHandle lower, ExprHandle upper);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_lit_datetime(long micros);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_alias(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string name);

    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_select(DataFrameHandle df, IntPtr[] exprs, UIntPtr len);
    // Temporal
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_dt_year(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_month(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_day(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_ordinal_day(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_weekday(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_hour(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_minute(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_second(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_millisecond(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_microsecond(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_nanosecond(ExprHandle expr);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_dt_to_string(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string format);
    
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_date(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_dt_time(ExprHandle expr);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_clone(ExprHandle expr);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_map(
        ExprHandle expr, 
        UdfCallback callback, 
        PlDataType returnType,
        CleanupCallback cleanup,
        IntPtr userData          
    );

    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_groupby_agg(
        DataFrameHandle df, 
        IntPtr[] byExprs, UIntPtr byLen,
        IntPtr[] aggExprs, UIntPtr aggLen
    );

    // Join 签名
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_join(
        DataFrameHandle left,
        DataFrameHandle right,
        IntPtr[] leftOn, UIntPtr leftLen,
        IntPtr[] rightOn, UIntPtr rightLen,
        PlJoinType how
    );
    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_sort(DataFrameHandle df, ExprHandle expr, [MarshalAs(UnmanagedType.U1)] bool descending);
    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_explode(DataFrameHandle df, IntPtr[] exprs, UIntPtr len);
    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_concat_vertical(
        IntPtr[] dfs, 
        UIntPtr len
    );
    // Parquet
    [LibraryImport(LibName)] 
    public static partial void pl_write_csv(DataFrameHandle df, [MarshalAs(UnmanagedType.LPUTF8Str)] string path);
    [LibraryImport(LibName)] 
    public static partial void pl_write_parquet(DataFrameHandle df, [MarshalAs(UnmanagedType.LPUTF8Str)] string path);
    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_read_parquet([MarshalAs(UnmanagedType.LPUTF8Str)] string path);

    // Lazy
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_csv(string path,[MarshalAs(UnmanagedType.U1)] bool tryParseDates
    );

    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_scan_parquet([MarshalAs(UnmanagedType.LPUTF8Str)] string path);
    // Lazy Introspection
    [LibraryImport(LibName)] public static partial IntPtr pl_lazy_schema(LazyFrameHandle lf);
    [LibraryImport(LibName)] public static partial IntPtr pl_lazy_explain(LazyFrameHandle lf,[MarshalAs(UnmanagedType.U1)] bool optimized);
    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_lazy_filter(LazyFrameHandle lf, ExprHandle expr);
    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_lazy_select(LazyFrameHandle lf, IntPtr[] exprs, UIntPtr len);
    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_lazy_sort(LazyFrameHandle lf, ExprHandle expr, [MarshalAs(UnmanagedType.U1)] bool desc);
    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_lazy_groupby_agg(
        LazyFrameHandle lf, 
        IntPtr[] keys, UIntPtr keysLen, 
        IntPtr[] aggs, UIntPtr aggsLen
    );
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_lazy_collect(LazyFrameHandle lf);
    [LibraryImport(LibName)]
    public static partial LazyFrameHandle pl_lazy_clone(LazyFrameHandle lf);

    [LibraryImport(LibName)] public static partial LazyFrameHandle pl_lazy_limit(LazyFrameHandle lf, uint n);
    [LibraryImport(LibName)] public static partial LazyFrameHandle pl_lazy_with_columns(LazyFrameHandle lf, IntPtr[] exprs, UIntPtr len);
    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_lazy_explode(LazyFrameHandle lf, IntPtr[] exprs, UIntPtr len);
    // --- Reshaping (Lazy) ---
    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_lazy_unpivot(
        LazyFrameHandle lf,
        IntPtr[] idVars, UIntPtr idLen,
        IntPtr[] valVars, UIntPtr valLen,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string varName,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string valName
    );
    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_lazy_concat(
        IntPtr[] lfs, 
        UIntPtr len,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        [MarshalAs(UnmanagedType.U1)] bool parallel
    );
    // --- Streaming & Sink ---
    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_lazy_collect_streaming(LazyFrameHandle lf);

    [LibraryImport(LibName)] 
    public static partial void pl_lazy_sink_parquet(
        LazyFrameHandle lf, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path
    );
    // String Ops
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_contains(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string pat);

    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_to_uppercase(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_to_lowercase(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_str_len_bytes(ExprHandle expr);
    
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_slice(ExprHandle expr, long offset, ulong length);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_split(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string pat);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_replace_all(
        ExprHandle expr, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string pat, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string val
    );

    // List Ops
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_first(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_get(ExprHandle expr, long index);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_explode(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_join(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string sep);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_len(ExprHandle expr);
    // List Aggs
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_sum(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_min(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_max(ExprHandle expr);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_mean(ExprHandle expr);
    
    // List Other
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_sort(ExprHandle expr,[MarshalAs(UnmanagedType.U1)] bool descending);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_list_contains(ExprHandle expr, ExprHandle item);
    // Naming
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_prefix(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string prefix);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_suffix(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string suffix);
    // --- Reshaping (Eager) ---
    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_pivot(
        DataFrameHandle df,
        IntPtr[] values, UIntPtr valuesLen,
        IntPtr[] index, UIntPtr indexLen,
        IntPtr[] columns, UIntPtr columnsLen,
        PlPivotAgg aggFn
    );

    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_unpivot(
        DataFrameHandle df,
        IntPtr[] idVars, UIntPtr idLen,
        IntPtr[] valVars, UIntPtr valLen,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string varName,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string valName
    );

    // Expr Len
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_len();
    [LibraryImport(LibName)] public static partial IntPtr pl_get_last_error();
    [LibraryImport(LibName)] public static partial void pl_free_error_msg(IntPtr ptr);
    [LibraryImport(LibName)] 
    public static partial SelectorHandle pl_selector_clone(SelectorHandle sel);
    // Selectors
    [LibraryImport(LibName)] public static partial SelectorHandle pl_selector_all();
    
    [LibraryImport(LibName)] 
    public static partial SelectorHandle pl_selector_exclude(
        SelectorHandle sel, 
        IntPtr[] names,
        UIntPtr len
    );

    [LibraryImport(LibName)] public static partial ExprHandle pl_selector_into_expr(SelectorHandle sel);
    // Struct
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_as_struct(IntPtr[] exprs, UIntPtr len);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_struct_field_by_name(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string name);
    // Window
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_over(
        ExprHandle expr, 
        IntPtr[] partitionBy, 
        UIntPtr len
    );

    // SQL Context
    [LibraryImport(LibName)] 
    public static partial SqlContextHandle pl_sql_context_new();

    [LibraryImport(LibName)] 
    public static partial void pl_sql_context_free(IntPtr ptr);

    [LibraryImport(LibName)] 
    public static partial void pl_sql_context_register(SqlContextHandle ctx, IntPtr name, LazyFrameHandle lf);

    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_sql_context_execute(SqlContextHandle ctx, IntPtr query);
}