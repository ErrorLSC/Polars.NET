using System;
using System.IO;
using Apache.Arrow.C; // 假设你有这个库
using Apache.Arrow;

namespace Polars.Native;

public static class PolarsWrapper
{
    // --- 辅助方法：批量转换 Handle ---
    private static IntPtr[] HandlesToPtrs(PolarsHandle[] handles)
    {
        if (handles == null || handles.Length == 0) return System.Array.Empty<IntPtr>();
        var ptrs = new IntPtr[handles.Length];
        for (int i = 0; i < handles.Length; i++)
        {
            // DangerousGetHandle 不会增加引用计数，但我们知道 Rust 会立刻消耗掉它
            ptrs[i] = handles[i].DangerousGetHandle();
            
            // 关键：标记 Handle 为无效，防止 C# 用户再次使用它
            // 因为 Rust 已经拿走了所有权 (Box::from_raw)
            handles[i].SetHandleAsInvalid(); 
        }
        return ptrs;
    }

    // --- IO ---
    public static DataFrameHandle ReadCsv(string path, bool tryParseDates)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"CSV not found: {path}");
        return ErrorHelper.Check(NativeBindings.pl_read_csv(path, tryParseDates));
    }

    public static LazyFrameHandle ScanCsv(string path, bool tryParseDates)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"CSV not found: {path}");
        return ErrorHelper.Check(NativeBindings.pl_scan_csv(path, tryParseDates));
    }

    public static DataFrameHandle ReadParquet(string path)
    {
         if (!File.Exists(path)) throw new FileNotFoundException($"Parquet not found: {path}");
         return ErrorHelper.Check(NativeBindings.pl_read_parquet(path));
    }

    public static LazyFrameHandle ScanParquet(string path) {
        if (!File.Exists(path)) throw new FileNotFoundException($"Parquet not found: {path}");
        return ErrorHelper.Check(NativeBindings.pl_scan_parquet(path));
    } 

    public static void WriteCsv(DataFrameHandle df, string path) => NativeBindings.pl_write_csv(df, path);
    public static void WriteParquet(DataFrameHandle df, string path) => NativeBindings.pl_write_parquet(df, path);
    // 显式把 Handle 转成 Arrow 数据
    public static unsafe RecordBatch Collect(DataFrameHandle handle)
    {
        var array = CArrowArray.Create();
        var schema = CArrowSchema.Create();
        try
        {
            NativeBindings.pl_to_arrow(handle, array, schema);
            ErrorHelper.CheckVoid();
            var managedSchema = CArrowSchemaImporter.ImportSchema(schema);
            return CArrowArrayImporter.ImportRecordBatch(array, managedSchema);
        }
        finally
        {
            CArrowArray.Free(array);
            CArrowSchema.Free(schema);
        }
    }

    // --- Eager Ops ---
    public static DataFrameHandle Filter(DataFrameHandle df, ExprHandle expr)
    {
        var h = NativeBindings.pl_filter(df, expr);
        expr.SetHandleAsInvalid(); // Expr 被消耗了
        return ErrorHelper.Check(h);
    }

    public static DataFrameHandle Select(DataFrameHandle df, ExprHandle[] exprs)
    {
        var rawExprs = HandlesToPtrs(exprs);
        return ErrorHelper.Check(NativeBindings.pl_select(df, rawExprs, (UIntPtr)rawExprs.Length));
    }

    public static DataFrameHandle Join(DataFrameHandle left, DataFrameHandle right, ExprHandle[] leftOn, ExprHandle[] rightOn, string how)
    {
        var lPtrs = HandlesToPtrs(leftOn);
        var rPtrs = HandlesToPtrs(rightOn);
        return ErrorHelper.Check(NativeBindings.pl_join(left, right, lPtrs, (UIntPtr)lPtrs.Length, rPtrs, (UIntPtr)rPtrs.Length, how));
    }

    // GroupBy 封装
    public static DataFrameHandle GroupByAgg(DataFrameHandle df, ExprHandle[] by, ExprHandle[] agg)
    {
        // 转换两个数组
        var rawBy = HandlesToPtrs(by);
        var rawAgg = HandlesToPtrs(agg);
        return ErrorHelper.Check(NativeBindings.pl_groupby_agg(
            df, 
            rawBy, (UIntPtr)rawBy.Length,
            rawAgg, (UIntPtr)rawAgg.Length
        ));
    }

    // --- Expr Ops (工厂方法) ---
    // 这些方法返回新的 ExprHandle，所有权在 C# 这边，直到传给 Filter/Select
    // Leaf Nodes (不消耗其他 Expr)
    public static ExprHandle Col(string name) => ErrorHelper.Check(NativeBindings.pl_expr_col(name));
    public static ExprHandle Lit(int val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_i32(val));
    public static ExprHandle Lit(string val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_str(val));
    public static ExprHandle Lit(double val) => ErrorHelper.Check(NativeBindings.pl_expr_lit_f64(val));
    // Unary Nodes (消耗 1 个 Expr)
    private static ExprHandle UnaryOp(Func<ExprHandle, ExprHandle> op, ExprHandle expr)
    {
        var h = op(expr);
        expr.SetHandleAsInvalid();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle Alias(ExprHandle expr, string name) 
    {
        var h = NativeBindings.pl_expr_alias(expr, name);
        expr.SetHandleAsInvalid();
        return ErrorHelper.Check(h);
    }
    
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
    private static ExprHandle BinaryOp(Func<ExprHandle, ExprHandle, ExprHandle> op, ExprHandle l, ExprHandle r)
    {
        var h = op(l, r);
        l.SetHandleAsInvalid();
        r.SetHandleAsInvalid();
        return ErrorHelper.Check(h);
    }

    public static ExprHandle Mul(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_mul, l, r);
    public static ExprHandle Gt(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_gt, l, r);
    public static ExprHandle Eq(ExprHandle l, ExprHandle r) => BinaryOp(NativeBindings.pl_expr_eq, l, r);

    // --- Lazy Ops ---
    public static LazyFrameHandle LazySelect(LazyFrameHandle lf, ExprHandle[] exprs)
    {
        var rawExprs = HandlesToPtrs(exprs);
        // 注意：lf 也是被消耗的！
        var newLf = NativeBindings.pl_lazy_select(lf, rawExprs, (UIntPtr)rawExprs.Length);
        lf.SetHandleAsInvalid(); 
        return ErrorHelper.Check(newLf);
    }

    public static DataFrameHandle LazyCollect(LazyFrameHandle lf)
    {
        var df = NativeBindings.pl_lazy_collect(lf);
        lf.SetHandleAsInvalid();
        return ErrorHelper.Check(df);
    }
    public static LazyFrameHandle LazyFilter(LazyFrameHandle lf, ExprHandle expr)
    {
        var h = NativeBindings.pl_lazy_filter(lf, expr);
        lf.SetHandleAsInvalid();   
        expr.SetHandleAsInvalid(); 
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle LazySort(LazyFrameHandle lf, ExprHandle expr, bool desc)
    {
        var h = NativeBindings.pl_lazy_sort(lf, expr, desc);
        lf.SetHandleAsInvalid();
        expr.SetHandleAsInvalid();
        return ErrorHelper.Check(h);
    }

    public static LazyFrameHandle LazyLimit(LazyFrameHandle lf, uint n)
    {
        var h = NativeBindings.pl_lazy_limit(lf, n);
        lf.SetHandleAsInvalid();
        return ErrorHelper.Check(h);
    }
    
    public static LazyFrameHandle LazyWithColumns(LazyFrameHandle lf, ExprHandle[] handles)
    {
        var raw = HandlesToPtrs(handles);
        var h = NativeBindings.pl_lazy_with_columns(lf, raw, (UIntPtr)raw.Length);
        lf.SetHandleAsInvalid();
        return ErrorHelper.Check(h);
    }
}