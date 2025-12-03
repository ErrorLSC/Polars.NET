namespace Polars.Native;

public static partial class PolarsWrapper
{
    public static string GetSchemaString(LazyFrameHandle lf)
    {
        // 借用操作，不 TransferOwnership
        IntPtr ptr = NativeBindings.pl_lazy_schema(lf);
        return ErrorHelper.CheckString(ptr); // 假设你提取了 CheckString 逻辑，或者手动写 try-finally
    }
    public static Dictionary<string, string> GetSchema(LazyFrameHandle lf)
    {
        var json = GetSchemaString(lf);
        if (string.IsNullOrEmpty(json)) return [];
        
        // 简单解析 (假设没有嵌套 JSON 结构，只是简单的 Key:Value)
        // 或者引入 System.Text.Json
        try 
        {
            return System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(json) 
                   ?? [];
        }
        catch 
        {
            return [];
        }
    }
    public static string Explain(LazyFrameHandle lf, bool optimized)
    {
        IntPtr ptr = NativeBindings.pl_lazy_explain(lf, optimized);
        return ErrorHelper.CheckString(ptr);
    }
    public static LazyFrameHandle LazySelect(LazyFrameHandle lf, ExprHandle[] exprs)
    {
        var rawExprs = HandlesToPtrs(exprs);
        var newLf = NativeBindings.pl_lazy_select(lf, rawExprs, (UIntPtr)rawExprs.Length);
        lf.TransferOwnership(); 
        return ErrorHelper.Check(newLf);
    }

    public static DataFrameHandle LazyCollect(LazyFrameHandle lf)
    {
        var df = NativeBindings.pl_lazy_collect(lf);
        lf.TransferOwnership();
        return ErrorHelper.Check(df);
    }
    public static LazyFrameHandle LazyFilter(LazyFrameHandle lf, ExprHandle expr)
    {
        var h = NativeBindings.pl_lazy_filter(lf, expr);
        lf.TransferOwnership();   
        expr.TransferOwnership(); 
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle LazySort(LazyFrameHandle lf, ExprHandle expr, bool desc)
    {
        var h = NativeBindings.pl_lazy_sort(lf, expr, desc);
        lf.TransferOwnership();
        expr.TransferOwnership();
        return ErrorHelper.Check(h);
    }

    public static LazyFrameHandle LazyLimit(LazyFrameHandle lf, uint n)
    {
        var h = NativeBindings.pl_lazy_limit(lf, n);
        lf.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle LazyGroupByAgg(LazyFrameHandle lf, ExprHandle[] keys, ExprHandle[] aggs)
    {
        var keyPtrs = HandlesToPtrs(keys);
        var aggPtrs = HandlesToPtrs(aggs);
        
        // lf 也会被消耗
        var h = NativeBindings.pl_lazy_groupby_agg(
            lf, 
            keyPtrs, (UIntPtr)keyPtrs.Length, 
            aggPtrs, (UIntPtr)aggPtrs.Length
        );
        
        lf.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle LazyWithColumns(LazyFrameHandle lf, ExprHandle[] handles)
    {
        var raw = HandlesToPtrs(handles);
        var h = NativeBindings.pl_lazy_with_columns(lf, raw, (UIntPtr)raw.Length);
        lf.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle LazyExplode(LazyFrameHandle lf, ExprHandle[] exprs)
    {
        var raw = HandlesToPtrs(exprs);
        var newLf = NativeBindings.pl_lazy_explode(lf, raw, (UIntPtr)raw.Length);
        lf.TransferOwnership(); // 链式调用消耗旧 LF
        return ErrorHelper.Check(newLf);
    }
    public static LazyFrameHandle LazyUnpivot(LazyFrameHandle lf, string[] index, string[] on, string variableName, string valueName)
    {
        return UseUtf8StringArray(index, iPtrs =>
            UseUtf8StringArray(on, oPtrs =>
            {
                var h = NativeBindings.pl_lazy_unpivot(
                    lf,
                    iPtrs, (UIntPtr)iPtrs.Length,
                    oPtrs, (UIntPtr)oPtrs.Length,
                    variableName,
                    valueName
                );
                lf.TransferOwnership();
                return ErrorHelper.Check(h);
            })
        );
    }
    public static LazyFrameHandle LazyConcat(LazyFrameHandle[] lfs, bool rechunk = false, bool parallel = true)
    {
        var ptrs = HandlesToPtrs(lfs); // 转移所有权
        var h = NativeBindings.pl_lazy_concat(ptrs, (UIntPtr)ptrs.Length, rechunk, parallel);
        return ErrorHelper.Check(h);
    }
    // [新增] Streaming Collect
    public static DataFrameHandle CollectStreaming(LazyFrameHandle lf)
    {
        var df = NativeBindings.pl_lazy_collect_streaming(lf);
        lf.TransferOwnership();
        return ErrorHelper.Check(df);
    }
    // --- Clone Ops ---
    public static LazyFrameHandle LazyClone(LazyFrameHandle lf)
    {
        // 注意：这里不需要 Invalidate lf，因为 Rust 侧只是借用
        return ErrorHelper.Check(NativeBindings.pl_lazy_clone(lf));
    }


}