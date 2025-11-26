using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;

namespace Polars.Native;
unsafe internal partial class NativeBindings
{
    const string LibName = "native_shim";

    [DllImport(LibName)]
    public static extern DataFrameHandle pl_read_csv([MarshalAs(UnmanagedType.LPUTF8Str)] string path,
    bool tryParseDates
    );

    [DllImport(LibName)]
    public static extern void pl_free_dataframe(IntPtr ptr);

    [DllImport(LibName)]
    public static extern void pl_to_arrow(DataFrameHandle handle, CArrowArray* arr, CArrowSchema* schema);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_col([MarshalAs(UnmanagedType.LPUTF8Str)] string name);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_lit_i32(int val);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_gt(ExprHandle left, ExprHandle right);

    [DllImport(LibName)]
    public static extern DataFrameHandle pl_filter(DataFrameHandle df, ExprHandle expr);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_lit_str([MarshalAs(UnmanagedType.LPUTF8Str)] string val);

    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_lit_f64(double val);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_eq(ExprHandle left, ExprHandle right);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_mul(ExprHandle left, ExprHandle right);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_alias(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string name);

    [DllImport(LibName)]
    public static extern DataFrameHandle pl_select(DataFrameHandle df, IntPtr[] exprs, UIntPtr len);

    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_sum(ExprHandle expr);

    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_mean(ExprHandle expr);

    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_max(ExprHandle expr);

    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_min(ExprHandle expr);

    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_dt_year(ExprHandle expr);

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

    [DllImport(LibName)] public static extern LazyFrameHandle pl_lazy_limit(LazyFrameHandle lf, uint n);
    [DllImport(LibName)] public static extern LazyFrameHandle pl_lazy_with_columns(LazyFrameHandle lf, IntPtr[] exprs, UIntPtr len);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_str_contains(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string pat);

    [DllImport(LibName)] public static extern IntPtr pl_get_last_error();
    [DllImport(LibName)] public static extern void pl_free_error_msg(IntPtr ptr);

}