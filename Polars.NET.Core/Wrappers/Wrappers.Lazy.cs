namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    public static string GetSchemaString(LazyFrameHandle lf)
    {
        // 借用操作，不 TransferOwnership
        IntPtr ptr = NativeBindings.pl_lazy_schema(lf);
        return ErrorHelper.CheckString(ptr); // 假设你提取了 CheckString 逻辑，或者手动写 try-finally
    }
    /// <summary>
    /// 获取 LazyFrame 的 Schema Handle。
    /// 这可能会触发 Rust 端的类型推断和 LogicalPlan 优化。
    /// </summary>
    public static SchemaHandle GetLazySchema(LazyFrameHandle lf)
    {
        // 必须使用 DangerousAddRef 吗？
        // 对于单个 Handle，LibraryImport 会自动处理。
        // 但这里 lf 只是传进去获取一个新的 Handle，不用特殊处理。
        return ErrorHelper.Check(NativeBindings.pl_lazy_frame_get_schema(lf));
    }

    /// <summary>
    /// 获取 Schema 长度
    /// </summary>
    public static ulong GetSchemaLen(SchemaHandle schema)
    {
        return (ulong)NativeBindings.pl_schema_len(schema);
    }

    /// <summary>
    /// 获取 Schema 指定索引的字段
    /// </summary>
    public static void GetSchemaFieldAt(SchemaHandle schema, ulong index, out string name, out DataTypeHandle typeHandle)
    {
        NativeBindings.pl_schema_get_at_index(
            schema, 
            (UIntPtr)index, 
            out IntPtr namePtr, 
            out var outTypeHandle
        );

        typeHandle = ErrorHelper.Check(outTypeHandle);

        name = ErrorHelper.CheckString(namePtr);
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
    /// <summary>
    /// Wrapper for Lazy GroupBy Dynamic.
    /// </summary>
    public static LazyFrameHandle LazyGroupByDynamic(
        LazyFrameHandle lf,
        string indexCol,
        string every,
        string period,
        string offset,
        int label,
        bool includeBoundaries,
        int closedWindow,
        int startBy,
        ExprHandle[] keys,  // 接收转换好的指针数组
        ExprHandle[] aggs)  // 接收转换好的指针数组
    {
        var keyPtrs = HandlesToPtrs(keys);
        var aggPtrs = HandlesToPtrs(aggs);
        var h = NativeBindings.pl_lazy_group_by_dynamic(
            lf,
            indexCol,
            every,
            period,
            offset,
            label,
            includeBoundaries,
            closedWindow,
            startBy,
            keyPtrs, (UIntPtr)keys.Length,
            aggPtrs, (UIntPtr)aggs.Length
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
    public static LazyFrameHandle LazyUnpivot(LazyFrameHandle lf, string[] index, string[] on, string? variableName, string? valueName)
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
    public static LazyFrameHandle LazyConcat(LazyFrameHandle[] handles,PlConcatType how, bool rechunk = false, bool parallel = true)
    {
        var ptrs = HandlesToPtrs(handles); // 转移所有权
        var h = NativeBindings.pl_lazy_concat(ptrs, (UIntPtr)ptrs.Length,(int)how, rechunk, parallel);
        foreach (var handle in handles)
        {
            handle.TransferOwnership();
        }
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle Join(
        LazyFrameHandle left, LazyFrameHandle right, 
        ExprHandle[] leftOn, ExprHandle[] rightOn, 
        PlJoinType how)
    {
        var lPtrs = HandlesToPtrs(leftOn);
        var rPtrs = HandlesToPtrs(rightOn);
        
        var h = NativeBindings.pl_lazy_join(
            left, right, 
            lPtrs, (UIntPtr)lPtrs.Length, 
            rPtrs, (UIntPtr)rPtrs.Length, 
            how
        );

        // 两个 LF 都被 Rust 消耗了
        left.TransferOwnership();
        right.TransferOwnership();
        
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle JoinAsOf(
        LazyFrameHandle left, LazyFrameHandle right,
        ExprHandle leftOn, ExprHandle rightOn,
        ExprHandle[]? leftBy, ExprHandle[]? rightBy, // 允许为 null
        string strategy, string? tolerance)
    {
        // 1. 处理数组 (HandlesToPtrs 内部已经处理了 null 检查，如果是 null 会返回空数组)
        var lByPtrs = HandlesToPtrs(leftBy ?? Array.Empty<ExprHandle>());
        var rByPtrs = HandlesToPtrs(rightBy ?? Array.Empty<ExprHandle>());

        // 2. 直接调用 Native
        var h = NativeBindings.pl_lazy_join_asof(
            left, right, 
            leftOn, rightOn,
            lByPtrs, (UIntPtr)lByPtrs.Length,
            rByPtrs, (UIntPtr)rByPtrs.Length,
            strategy, tolerance
        );

        // 3. 消耗所有权 (TransferOwnership)
        left.TransferOwnership();
        right.TransferOwnership();
        leftOn.TransferOwnership();
        rightOn.TransferOwnership();
        
        // leftBy 和 rightBy 已经在 HandlesToPtrs 里被 TransferOwnership 了，不用再管

        return ErrorHelper.Check(h);
    }
    // [新增] Streaming Collect
    public static DataFrameHandle CollectStreaming(LazyFrameHandle lf)
    {
        var df = NativeBindings.pl_lazy_collect_streaming(lf);
        lf.TransferOwnership();
        return ErrorHelper.Check(df);
    }
    public static Task<DataFrameHandle> LazyCollectAsync(LazyFrameHandle handle)
    {        
        return Task.Run(() => LazyCollect(handle));
    }
    // --- Clone Ops ---
    public static LazyFrameHandle LazyClone(LazyFrameHandle lf)
    {
        // 注意：这里不需要 Invalidate lf，因为 Rust 侧只是借用
        return ErrorHelper.Check(NativeBindings.pl_lazy_clone(lf));
    }
}