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

    [DllImport(LibName)]
    public static extern DataFrameHandle pl_read_csv([MarshalAs(UnmanagedType.LPUTF8Str)] string path,
    bool tryParseDates
    );

    [DllImport(LibName)]
    public static extern void pl_free_dataframe(IntPtr ptr);
    // String Free
    [DllImport(LibName)] public static extern void pl_free_string(IntPtr ptr);
    [DllImport(LibName)] public static extern void pl_to_arrow(DataFrameHandle handle, CArrowArray* arr, CArrowSchema* schema);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_col([MarshalAs(UnmanagedType.LPUTF8Str)] string name);

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
    // null ops
    [DllImport(LibName)] public static extern ExprHandle pl_expr_fill_null(ExprHandle expr, ExprHandle fillValue);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_is_null(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_is_not_null(ExprHandle expr);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_is_between(ExprHandle expr, ExprHandle lower, ExprHandle upper);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_lit_datetime(long micros);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_alias(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string name);

    [DllImport(LibName)]
    public static extern DataFrameHandle pl_select(DataFrameHandle df, IntPtr[] exprs, UIntPtr len);

    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_dt_year(ExprHandle expr);

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
    public static extern DataFrameHandle pl_lazy_collect(LazyFrameHandle lf);
    [DllImport(LibName)]
    public static extern LazyFrameHandle pl_lazy_clone(LazyFrameHandle lf);

    [DllImport(LibName)] public static extern LazyFrameHandle pl_lazy_limit(LazyFrameHandle lf, uint n);
    [DllImport(LibName)] public static extern LazyFrameHandle pl_lazy_with_columns(LazyFrameHandle lf, IntPtr[] exprs, UIntPtr len);

    // String Ops
    [DllImport(LibName)] public static extern ExprHandle pl_expr_str_contains(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string pat);

    [DllImport(LibName)] public static extern ExprHandle pl_expr_str_to_uppercase(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_str_to_lowercase(ExprHandle expr);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_str_len_bytes(ExprHandle expr);
    
    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_str_slice(ExprHandle expr, long offset, ulong length);
    
    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_str_replace_all(
        ExprHandle expr, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string pat, 
        [MarshalAs(UnmanagedType.LPUTF8Str)] string val
    );

    [DllImport(LibName)] public static extern IntPtr pl_get_last_error();
    [DllImport(LibName)] public static extern void pl_free_error_msg(IntPtr ptr);



}