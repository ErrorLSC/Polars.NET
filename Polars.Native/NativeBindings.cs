using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;

[assembly: DisableRuntimeMarshalling]

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
    public static partial DataFrameHandle pl_read_csv(
        string path,
        IntPtr[]? schemaNames,    // Schema Keys
        IntPtr[]? schemaTypes,    // Schema Values (DataTypeContext*)
        UIntPtr schemaLen,
        [MarshalAs(UnmanagedType.I1)] bool hasHeader,
        byte separator,
        UIntPtr skipRows,
        [MarshalAs(UnmanagedType.I1)] bool tryParseDates // [新增]
    );
    [LibraryImport(LibName)]
    public static partial void pl_dataframe_free(IntPtr ptr);
    // String Free
    [LibraryImport(LibName)] public static partial void pl_free_string(IntPtr ptr);
    [LibraryImport(LibName)] public static partial void pl_to_arrow(DataFrameHandle handle, CArrowArray* arr, CArrowSchema* schema);
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_dataframe_from_arrow_record_batch(
        CArrowArray* cArray, 
        CArrowSchema* cSchema
    );
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_dataframe_new(
        IntPtr[] columns, 
        UIntPtr len
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial ExprHandle pl_expr_col(string name);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_cols(IntPtr[] names, UIntPtr len);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_lit_i32(int val);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_lit_bool([MarshalAs(UnmanagedType.I1)] bool val);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_lit_i64(long val);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_lit_f32(float val);
    [LibraryImport(LibName)]
    public static partial IntPtr pl_dataframe_schema(DataFrameHandle df);
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
    [LibraryImport(LibName)]
    public static partial LazyFrameHandle pl_dataframe_lazy(DataFrameHandle df);
    [LibraryImport(LibName)] public static partial IntPtr pl_dataframe_get_string(DataFrameHandle df, [MarshalAs(UnmanagedType.LPUTF8Str)] string colName, UIntPtr row);
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_head(DataFrameHandle df, UIntPtr n);
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_tail(DataFrameHandle df, UIntPtr n);

    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_filter(DataFrameHandle df, ExprHandle expr);

    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_with_columns(DataFrameHandle df, IntPtr[] exprs, UIntPtr len);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_dataframe_drop(DataFrameHandle df, string name);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_dataframe_rename(DataFrameHandle df, string oldName, string newName);

    // subset 是字符串指针数组
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_dataframe_drop_nulls(DataFrameHandle df, IntPtr[] subset, UIntPtr len);

    // seed 传 ulong 指针 (nullable)
    [LibraryImport(LibName)]
    public static unsafe partial DataFrameHandle pl_dataframe_sample_n(DataFrameHandle df, UIntPtr n, [MarshalAs(UnmanagedType.U1)] bool replacement, [MarshalAs(UnmanagedType.I1)] bool shuffle, ulong* seed);

    [LibraryImport(LibName)]
    public static unsafe partial DataFrameHandle pl_dataframe_sample_frac(DataFrameHandle df, double frac, [MarshalAs(UnmanagedType.U1)] bool replacement, [MarshalAs(UnmanagedType.I1)] bool shuffle, ulong* seed);
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
        DataTypeHandle returnType,
        CleanupCallback cleanup,
        IntPtr userData          
    );
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_cast(ExprHandle expr, DataTypeHandle dtype, [MarshalAs(UnmanagedType.U1)] bool strict);
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
    public static partial DataFrameHandle pl_concat(
        IntPtr[] dfs, 
        UIntPtr len,
        PlConcatType how
    );
    // Parquet
    [LibraryImport(LibName)] 
    public static partial void pl_write_csv(DataFrameHandle df, [MarshalAs(UnmanagedType.LPUTF8Str)] string path);
    [LibraryImport(LibName)] 
    public static partial void pl_write_parquet(DataFrameHandle df, [MarshalAs(UnmanagedType.LPUTF8Str)] string path);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_dataframe_write_ipc(DataFrameHandle df, string path);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_dataframe_write_json(DataFrameHandle df, string path);
    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_read_parquet([MarshalAs(UnmanagedType.LPUTF8Str)] string path);
    // --- JSON IO ---

    // Read JSON
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial DataFrameHandle pl_read_json(string path);

    // Scan NDJSON
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial LazyFrameHandle pl_scan_ndjson(string path);
    // Lazy
    // [IO: CSV Scan (Lazy)]
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_csv(
        string path,
        IntPtr[]? schemaNames,
        IntPtr[]? schemaTypes,
        UIntPtr schemaLen,
        [MarshalAs(UnmanagedType.I1)] bool hasHeader,
        byte separator,
        UIntPtr skipRows,
        [MarshalAs(UnmanagedType.I1)] bool tryParseDates // [新增]
    );

    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_scan_parquet([MarshalAs(UnmanagedType.LPUTF8Str)] string path);
    // IPC
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial DataFrameHandle pl_read_ipc(string path);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial LazyFrameHandle pl_scan_ipc(string path);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial void pl_lazy_sink_ipc(LazyFrameHandle lf, string path);
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
    public static partial LazyFrameHandle pl_lazy_join(
        LazyFrameHandle left, 
        LazyFrameHandle right,
        IntPtr[] leftOn, UIntPtr leftLen,
        IntPtr[] rightOn, UIntPtr rightLen,
        PlJoinType how
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_lazy_join_asof(
        LazyFrameHandle left, LazyFrameHandle right,
        ExprHandle leftOn, ExprHandle rightOn,
        IntPtr[] leftBy, UIntPtr leftByLen,
        IntPtr[] rightBy, UIntPtr rightByLen,
        string strategy, string? tolerance
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
        [MarshalAs(UnmanagedType.LPUTF8Str)] string? varName,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string? valName
    );
    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_lazy_concat(
        IntPtr[] lfs, 
        UIntPtr len,
        int how,
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
    // String Cleaning
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_strip_chars(ExprHandle e, string? matches);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_strip_chars_start(ExprHandle e, string? matches);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_strip_chars_end(ExprHandle e, string? matches);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_strip_prefix(ExprHandle e, string prefix);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_strip_suffix(ExprHandle e, string suffix);

    // Anchors
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_starts_with(ExprHandle e, string prefix);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_ends_with(ExprHandle e, string suffix);
    // Parsing
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_to_date(ExprHandle e, string format);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_str_to_datetime(ExprHandle e, string format);
    
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_slice(ExprHandle expr, long offset, ulong length);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_split(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string pat);
    [LibraryImport(LibName)] 
    public static partial ExprHandle pl_expr_str_replace_all(
        ExprHandle expr, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string pat, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string val,
        [MarshalAs(UnmanagedType.U1)] bool useRegex
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial ExprHandle pl_expr_str_extract(
        ExprHandle expr, 
        string pat, 
        UIntPtr groupIndex
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
        [MarshalAs(UnmanagedType.LPUTF8Str)] string? varName,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string? valName
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

    // Shift / Diff
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_shift(ExprHandle expr, long n);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_diff(ExprHandle expr, long n);

    // Fill
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_forward_fill(ExprHandle expr, uint limit);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_backward_fill(ExprHandle expr, uint limit);
    // Rolling Window
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rolling_mean(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string windowSize);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rolling_sum(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string windowSize);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rolling_min(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string windowSize);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_rolling_max(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string windowSize);

    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_mean_by(ExprHandle expr, string windowSize, ExprHandle by, string closed);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_sum_by(ExprHandle expr, string windowSize, ExprHandle by, string closed);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_min_by(ExprHandle expr, string windowSize, ExprHandle by, string closed);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] public static partial ExprHandle pl_expr_rolling_max_by(ExprHandle expr, string windowSize, ExprHandle by, string closed);
    [LibraryImport(LibName)] public static partial ExprHandle pl_expr_if_else(ExprHandle pred, ExprHandle ifTrue, ExprHandle ifFalse);
    // Statistical
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_count(ExprHandle e);
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_std(ExprHandle e, byte ddof);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_var(ExprHandle e, byte ddof);

    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_median(ExprHandle e);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial ExprHandle pl_expr_quantile(ExprHandle e, double quantile, string interpol);
    // --- Series Lifecycle ---
    [LibraryImport(LibName)]
    public static partial void pl_series_free(IntPtr ptr);
    [LibraryImport(LibName)]
    public static partial void pl_free_c_string(IntPtr ptr);
    [LibraryImport(LibName)]
    public static partial void pl_arrow_array_free(IntPtr ptr);
    // --- Series Getters ---
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_get_i64(SeriesHandle s, UIntPtr idx, out long val);

    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_get_f64(SeriesHandle s, UIntPtr idx, out double val);

    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_get_bool(SeriesHandle s, UIntPtr idx, [MarshalAs(UnmanagedType.I1)] out bool val);

    [LibraryImport(LibName)]
    public static partial IntPtr pl_series_get_str(SeriesHandle s, UIntPtr idx);

    // Decimal: out Int128, out UIntPtr (scale)
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_get_decimal(SeriesHandle s, UIntPtr idx, out Int128 val, out UIntPtr scale);
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_get_date(SeriesHandle s, UIntPtr idx, out int val);

    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_get_time(SeriesHandle s, UIntPtr idx, out long val);

    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_get_datetime(SeriesHandle s, UIntPtr idx, out long val);

    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_get_duration(SeriesHandle s, UIntPtr idx, out long val);
    // --- Series Constructors ---
    // DataFrame -> Series (ByName)
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_dataframe_get_column(DataFrameHandle df, string name);
    // DataFrame -> Series (ByIndex)
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_dataframe_get_column_at(DataFrameHandle df, UIntPtr index);
    // Series -> DataFrame
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_series_to_frame(SeriesHandle s);
    // 数值类型：支持 validity 位图
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_i32(string name, int[] ptr, byte[]? validity, UIntPtr len);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_i64(string name, long[] ptr, byte[]? validity, UIntPtr len);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_f64(string name, double[] ptr, byte[]? validity, UIntPtr len);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_bool(
        string name, 
        byte[] ptr, 
        byte[]? validity, 
        UIntPtr len
    );

    // 字符串类型：IntPtr[] 里的 IntPtr.Zero 代表 null
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_str(string name, IntPtr[] strs, UIntPtr len);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_series_new_decimal(
        string name, 
        Int128[] ptr,      // 对应 Rust 的 *const i128
        byte[]? validity, 
        UIntPtr len,
        UIntPtr scale
    );
    // --- Series Properties ---
    [LibraryImport(LibName)]
    public static partial IntPtr pl_series_dtype_str(SeriesHandle s);
    [LibraryImport(LibName)]
    public static partial UIntPtr pl_series_len(SeriesHandle h);

    [LibraryImport(LibName)]
    public static partial IntPtr pl_series_name(SeriesHandle h);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_series_rename(SeriesHandle h, string name);
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_is_null(SeriesHandle s);

    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_is_not_null(SeriesHandle s);
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.I1)]
    public static partial bool pl_series_is_null_at(SeriesHandle s, UIntPtr idx);
    [LibraryImport(LibName)]
    public static partial UIntPtr pl_series_null_count(SeriesHandle s);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_is_nan(SeriesHandle s);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_is_not_nan(SeriesHandle s);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_is_finite(SeriesHandle s);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_is_infinite(SeriesHandle s);
    // --- Series Cast ---
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_series_cast(SeriesHandle s, DataTypeHandle dtype);

    // --- Arrow Export ---
    [LibraryImport(LibName)]
    public static partial ArrowArrayContextHandle pl_series_to_arrow(SeriesHandle h);

    [LibraryImport(LibName)]
    public static partial void pl_arrow_array_export(ArrowArrayContextHandle ptr, void* out_c_array);

    [LibraryImport(LibName)]
    public static partial void pl_arrow_schema_export(ArrowArrayContextHandle ptr, void* out_c_schema);

    // --- DataType ---
    [LibraryImport(LibName)]
    public static partial void pl_datatype_free(IntPtr ptr);

    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_new_primitive(int code);

    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_new_decimal(UIntPtr precision, UIntPtr scale);

    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_new_categorical();
    // Arithmetic
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_add(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_sub(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_mul(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_div(SeriesHandle s1, SeriesHandle s2);

    // Comparison
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_eq(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_neq(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_gt(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_lt(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_gt_eq(SeriesHandle s1, SeriesHandle s2);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_lt_eq(SeriesHandle s1, SeriesHandle s2);

    // Aggregation
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_sum(SeriesHandle s);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_mean(SeriesHandle s);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_min(SeriesHandle s);
    [LibraryImport(LibName)] public static partial SeriesHandle pl_series_max(SeriesHandle s);

}