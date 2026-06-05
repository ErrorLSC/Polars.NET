using System.Runtime.InteropServices;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public readonly partial struct PolarsWrapper
{
    /// <summary>
    /// Get the schema handle for LazyFrame 
    /// </summary>
    public static SchemaHandle GetLazySchema(LazyFrameHandle lf)
        => ErrorHelper.Check(NativeBindings.pl_lazyframe_get_schema(lf));

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

    public static DataFrameHandle LazyCollect(LazyFrameHandle lf,PlEngine engine,bool useStreaming)
    {
        var df = NativeBindings.pl_lazy_collect(lf,engine,useStreaming);
        lf.TransferOwnership();
        return ErrorHelper.Check(df);
    }
    public static DataFrameHandle[] LazyCollectAll(LazyFrameHandle[] lfs, PlEngine engine)
    {
        if (lfs == null || lfs.Length == 0) return [];

        var inPtrs = HandlesToPtrs(lfs)!; 
        
        var outPtrs = new nint[lfs.Length];

        int status = NativeBindings.pl_lazy_collect_all(
            inPtrs, 
            (nuint)lfs.Length, 
            outPtrs, 
            engine
        );

        ErrorHelper.CheckStatus(status); 

        var result = new DataFrameHandle[lfs.Length];
        for (int i = 0; i < lfs.Length; i++)
        {
            result[i] = new DataFrameHandle(outPtrs[i]);
        }

        return result;
    }

    public static Task<DataFrameHandle[]> LazyCollectAllAsync(LazyFrameHandle[] lfs, PlEngine engine)
        => Task.Run(() => LazyCollectAll(lfs, engine));
    public static string LazyExplainAll(LazyFrameHandle[] lfs)
    {
        if (lfs == null || lfs.Length == 0) return "";

        var inPtrs = HandlesToPtrs(lfs)!; 
        
        var outPtrs = new nint[lfs.Length];

        int status = NativeBindings.pl_lazy_explain_all(
            inPtrs, 
            (nuint)lfs.Length, 
            out nint plan 
        );

        ErrorHelper.CheckStatus(status); 

        return ErrorHelper.CheckString(plan);
    }
    public static LazyFrameHandle LazyFilter(LazyFrameHandle lf, ExprHandle expr)
    {
        var h = NativeBindings.pl_lazy_filter(lf, expr);
        lf.TransferOwnership();   
        expr.TransferOwnership(); 
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle LazySlice(LazyFrameHandle lf, long offset, uint len)
    {
        var h = NativeBindings.pl_lazyframe_slice(lf, offset,len);
        lf.TransferOwnership();   
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle LazyRename(LazyFrameHandle lf, string[] existing, string[] newNames, bool strict)
    {
        var h = NativeBindings.pl_lazyframe_rename(
            lf, 
            existing, 
            (nuint)existing.Length, 
            newNames, 
            (nuint)newNames.Length, 
            strict
        );
        lf.TransferOwnership();   
        
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle LazyFrameSort(
        LazyFrameHandle lf, 
        ExprHandle[] exprs, 
        bool[] descending,
        bool[] nullsLast,
        bool maintainOrder)
    {
        if ((descending.Length != 1 && descending.Length != exprs.Length) ||
            (nullsLast.Length != 1 && nullsLast.Length != exprs.Length))
        {
                throw new ArgumentException("Sort options length mismatch.");
        }

        var exprPtrs = HandlesToPtrs(exprs);

        unsafe
        {
            fixed (bool* descPtr = descending)
            fixed (bool* nullsPtr = nullsLast)
            {
                var h = NativeBindings.pl_lazyframe_sort(
                    lf,
                    exprPtrs,
                    (UIntPtr)exprs.Length,
                    descPtr,
                    (UIntPtr)descending.Length,
                    nullsPtr,
                    (UIntPtr)nullsLast.Length,
                    maintainOrder
                );

                lf.TransferOwnership();

                return ErrorHelper.Check(h);
            }
        }
    }
    public static LazyFrameHandle LazyFrameTopK(LazyFrameHandle lf, uint k, ExprHandle[] by, bool[] reverse)
    {
        var byPtrs = HandlesToPtrs(by);
        unsafe
        {
            fixed (bool* rPtr = reverse)
            {
                var h = NativeBindings.pl_lazyframe_top_k(
                    lf, 
                    k, 
                    byPtrs, 
                    (UIntPtr)byPtrs.Length, 
                    rPtr, 
                    (UIntPtr)reverse.Length
                );
                lf.TransferOwnership();
                return ErrorHelper.Check(h);
            }
        }
    }

    public static LazyFrameHandle LazyFrameBottomK(LazyFrameHandle lf, uint k, ExprHandle[] by, bool[] reverse)
    {
        var byPtrs = HandlesToPtrs(by);
        unsafe
        {
            fixed (bool* rPtr = reverse)
            {
                var h = NativeBindings.pl_lazyframe_bottom_k(
                    lf, 
                    k, 
                    byPtrs, 
                    (UIntPtr)byPtrs.Length, 
                    rPtr, 
                    (UIntPtr)reverse.Length
                );
                lf.TransferOwnership();
                return ErrorHelper.Check(h);
            }
        }
    }
    public static LazyFrameHandle LazyFrameUnnest(LazyFrameHandle lf, SelectorHandle selector,string? separator)
    {
        var h = ErrorHelper.Check(NativeBindings.pl_lazyframe_unnest(lf, selector, separator));
        lf.TransferOwnership();
        selector.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle LazyFrameDrop(LazyFrameHandle lf, SelectorHandle selector)
    {
        var h = ErrorHelper.Check(NativeBindings.pl_lazyframe_drop(lf, selector));
        lf.TransferOwnership();
        selector.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle LazyFrameDropNulls(LazyFrameHandle lf, SelectorHandle? selector)
    {   
        var selPtr = selector?.TransferOwnership() ?? IntPtr.Zero;
        var h = ErrorHelper.Check(NativeBindings.pl_lazyframe_drop_nulls(lf, selPtr));
        lf.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle LazyFrameDropNans(LazyFrameHandle lf, SelectorHandle? selector)
    {   
        var selPtr = selector?.TransferOwnership() ?? IntPtr.Zero;
        var h = ErrorHelper.Check(NativeBindings.pl_lazyframe_drop_nans(lf, selPtr));
        lf.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle LazyUnique(
        LazyFrameHandle lfHandle, 
        SelectorHandle selector, 
        PlUniqueKeepStrategy keep,
        bool maintainOrder)
    {
        IntPtr selPtr = selector?.DangerousGetHandle() ?? IntPtr.Zero;
        var h = NativeBindings.pl_lazyframe_unique(lfHandle,selPtr,keep,maintainOrder);
        lfHandle.TransferOwnership();
        selector?.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle LazyGroupByAgg(LazyFrameHandle lf, ExprHandle[] keys, ExprHandle[] aggs,ExprHandle? havingExpr,bool maintainOrder)
    {
        var keyPtrs = HandlesToPtrs(keys);
        var aggPtrs = HandlesToPtrs(aggs);
        IntPtr havingExprPtr = havingExpr?.TransferOwnership() ?? IntPtr.Zero;
        
        var h = NativeBindings.pl_lazy_groupby_agg(
            lf, 
            keyPtrs, (UIntPtr)keyPtrs.Length, 
            aggPtrs, (UIntPtr)aggPtrs.Length,havingExprPtr,maintainOrder
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
        PlLabel label,
        bool includeBoundaries,
        PlClosedInterval ClosedInterval,
        PlStartBy startBy,
        ExprHandle[] keys,  
        ExprHandle[] aggs,
        ExprHandle? havingExpr)  
    {
        var keyPtrs = HandlesToPtrs(keys);
        var aggPtrs = HandlesToPtrs(aggs);
        nint havingExprPtr = havingExpr?.TransferOwnership() ?? nint.Zero;
        var h = NativeBindings.pl_lazy_group_by_dynamic(
            lf,
            indexCol,
            every,
            period,
            offset,
            label,
            includeBoundaries,
            ClosedInterval,
            startBy,
            keyPtrs, (nuint)keys.Length,
            aggPtrs, (nuint)aggs.Length,
            havingExprPtr
        );
        lf.TransferOwnership();

        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle LazyGroupByRolling(
        LazyFrameHandle lf,
        string indexCol,
        string period,
        string offset,
        PlClosedInterval ClosedInterval,
        ExprHandle[] keys,  
        ExprHandle[] aggs,
        ExprHandle? havingExpr)  
    {
        var keyPtrs = HandlesToPtrs(keys);
        var aggPtrs = HandlesToPtrs(aggs);
        nint havingExprPtr = havingExpr?.TransferOwnership() ?? nint.Zero;
        var h = NativeBindings.pl_lazy_group_by_rolling(
            lf,
            indexCol,
            period,
            offset,
            ClosedInterval,
            keyPtrs, (nuint)keys.Length,
            aggPtrs, (nuint)aggs.Length,
            havingExprPtr
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
    public static LazyFrameHandle LazyExplode(LazyFrameHandle lf, SelectorHandle selector, bool emptyAsNull,bool keepNulls)
    {
        var newLf = NativeBindings.pl_lazyframe_explode(lf, selector,emptyAsNull,keepNulls);
        lf.TransferOwnership(); 
        selector.TransferOwnership();
        return ErrorHelper.Check(newLf);
    }
    public static LazyFrameHandle LazyPivot(
        LazyFrameHandle lf, 
        SelectorHandle on, 
        DataFrameHandle onColumns, 
        SelectorHandle index, 
        SelectorHandle values, 
        ExprHandle? aggExpr, 
        PlPivotAgg aggCode, 
        bool maintainOrder, 
        string? separator,
        PlPivotColumnNaming columnNaming)
    {
        IntPtr aggExprPtr = aggExpr?.TransferOwnership() ?? IntPtr.Zero;

        var h = NativeBindings.pl_lazyframe_pivot(
            lf,
            on,
            onColumns,
            index,
            values,
            aggExprPtr,
            aggCode,
            maintainOrder,
            separator,
            columnNaming
        );
        
        lf.TransferOwnership();

        on.TransferOwnership();
        index.TransferOwnership();
        values.TransferOwnership();

        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle LazyUnpivot(LazyFrameHandle lf, SelectorHandle index, SelectorHandle? on, string? variableName, string? valueName)
    {
        var onPtr = on?.TransferOwnership() ?? IntPtr.Zero;
        var h = NativeBindings.pl_lazyframe_unpivot(
            lf,
            index,
            onPtr, 
            variableName,
            valueName
        );
        lf.TransferOwnership();
        index.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle LazyConcat(LazyFrameHandle[] handles,PlConcatType how, bool rechunk = false, bool parallel = true)
    {
        var ptrs = HandlesToPtrs(handles); 
        var h = NativeBindings.pl_lazy_concat(ptrs, (UIntPtr)ptrs.Length,(int)how, rechunk, parallel);
        foreach (var handle in handles)
        {
            handle.TransferOwnership();
        }
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle Join(
        LazyFrameHandle left, 
        LazyFrameHandle right, 
        ExprHandle[] leftOn, 
        ExprHandle[] rightOn, 
        PlJoinType how,
        string? suffix,
        PlJoinValidation validation,
        PlJoinCoalesce coalesce,
        PlJoinMaintainOrder maintainOrder,
        PlJoinSide joinSide,
        bool nullsEqual,
        long? sliceOffset,
        ulong sliceLen)
    {
        var lPtrs = HandlesToPtrs(leftOn);
        var rPtrs = HandlesToPtrs(rightOn);
        unsafe 
        {
            long offsetVal = sliceOffset.GetValueOrDefault();
            IntPtr offsetPtr = sliceOffset.HasValue ? (IntPtr)(&offsetVal) : IntPtr.Zero;

            var h = NativeBindings.pl_lazyframe_join(
                left, 
                right, 
                lPtrs, (UIntPtr)lPtrs.Length, 
                rPtrs, (UIntPtr)rPtrs.Length, 
                how,
                suffix,         
                validation,
                coalesce,
                maintainOrder,
                joinSide,
                nullsEqual,
                offsetPtr,      
                (UIntPtr)sliceLen
            );
            left.TransferOwnership();
            right.TransferOwnership();
        
            return ErrorHelper.Check(h);
        }
    }
    public static LazyFrameHandle JoinAsOf(
        LazyFrameHandle left, LazyFrameHandle right,
        ExprHandle[] leftOn, ExprHandle[] rightOn,
        ExprHandle[]? leftBy, ExprHandle[]? rightBy,
        // Options
        PlAsofStrategy strategy, // "backward", etc. (Need convert to byte inside Wrapper or pass enum)
        string? toleranceStr,
        long? toleranceInt,
        double? toleranceFloat,
        bool allowEq,
        bool checkSorted,
        // JoinArgs
        string? suffix,
        PlJoinValidation validation,
        PlJoinCoalesce coalesce,
        PlJoinMaintainOrder maintainOrder,
        PlJoinSide joinSide,
        bool nullsEqual,
        long? sliceOffset,
        ulong sliceLen)
    {
        var lPtrs = HandlesToPtrs(leftOn);
        var rPtrs = HandlesToPtrs(rightOn);
        var lByPtrs = HandlesToPtrs(leftBy ?? []);
        var rByPtrs = HandlesToPtrs(rightBy ?? []);

        unsafe 
        {
            // Tolerance Pointers
            long tIntVal = toleranceInt.GetValueOrDefault();
            IntPtr tIntPtr = toleranceInt.HasValue ? (IntPtr)(&tIntVal) : IntPtr.Zero;

            double tFloatVal = toleranceFloat.GetValueOrDefault();
            IntPtr tFloatPtr = toleranceFloat.HasValue ? (IntPtr)(&tFloatVal) : IntPtr.Zero;

            // Slice Pointer
            long sOffVal = sliceOffset.GetValueOrDefault();
            IntPtr sOffPtr = sliceOffset.HasValue ? (IntPtr)(&sOffVal) : IntPtr.Zero;

            var h = NativeBindings.pl_lazyframe_join_asof(
                left, right,
                lPtrs, (UIntPtr)lPtrs.Length,
                rPtrs, (UIntPtr)rPtrs.Length,
                lByPtrs, (UIntPtr)lByPtrs.Length,
                rByPtrs, (UIntPtr)rByPtrs.Length,
                strategy,
                toleranceStr,
                tIntPtr,
                tFloatPtr,
                allowEq,
                checkSorted,
                suffix,
                validation,
                coalesce,
                maintainOrder,
                joinSide,
                nullsEqual,
                sOffPtr,
                (UIntPtr)sliceLen
            );

            left.TransferOwnership();
            right.TransferOwnership();

            return ErrorHelper.Check(h);
        }
    }
    public static LazyFrameHandle JoinWhere(
        LazyFrameHandle left, 
        LazyFrameHandle right, 
        ExprHandle[] predicates, 
        PlJoinType how,
        string? suffix,
        PlJoinValidation validation,
        PlJoinCoalesce coalesce,
        PlJoinMaintainOrder maintainOrder,
        bool nullsEqual)
    {
        var lPtrs = HandlesToPtrs(predicates);

        var h = NativeBindings.pl_lazyframe_join_where(
            left, 
            right, 
            lPtrs, (nuint)lPtrs.Length, 
            how,
            suffix,         
            validation,
            coalesce,
            maintainOrder,
            nullsEqual
        );
        left.TransferOwnership();
        right.TransferOwnership();
    
        return ErrorHelper.Check(h);
    }
    public struct PlMatchToSchemaConfig
    {
        public PlMissingColumnsPolicyType MissingColumnsType;
        public ExprHandle? MissingColumnsExpr; 
        public PlMissingColumnsPolicy MissingStructFields;
        public PlExtraColumnsPolicy ExtraStructFields;
        public PlUpcastOrForbid IntegerCast;
        public PlUpcastOrForbid FloatCast;
    }
    public static LazyFrameHandle MatchToSchema(
        LazyFrameHandle lf,
        SchemaHandle schema,
        PlExtraColumnsPolicy extraColumnsCode,
        PlMatchToSchemaConfig defaultConfig, 
        IReadOnlyDictionary<string, PlMatchToSchemaConfig>? overrides)
    {
        int ovLen = overrides?.Count ?? 0;
        
        string[]? ovNames = ovLen > 0 ? new string[ovLen] : null; 
        byte[]? ovMissingType = ovLen > 0 ? new byte[ovLen] : null;
        IntPtr[]? ovMissingExprPtrs = ovLen > 0 ? new IntPtr[ovLen] : null;
        byte[]? ovMissingStruct = ovLen > 0 ? new byte[ovLen] : null;
        byte[]? ovExtraStruct = ovLen > 0 ? new byte[ovLen] : null;
        byte[]? ovIntCast = ovLen > 0 ? new byte[ovLen] : null;
        byte[]? ovFloatCast = ovLen > 0 ? new byte[ovLen] : null;

        IntPtr defExprPtr = defaultConfig.MissingColumnsExpr?.TransferOwnership() ?? IntPtr.Zero;

        if (overrides != null && ovLen > 0)
        {
            int i = 0;
            foreach (var kvp in overrides)
            {
                var cfg = kvp.Value;
                
                ovNames![i] = kvp.Key; 
                ovMissingType![i] = (byte)cfg.MissingColumnsType;
                ovMissingExprPtrs![i] = cfg.MissingColumnsExpr?.TransferOwnership() ?? IntPtr.Zero;
                ovMissingStruct![i] = (byte)cfg.MissingStructFields;
                ovExtraStruct![i] = (byte)cfg.ExtraStructFields;
                ovIntCast![i] = (byte)cfg.IntegerCast;
                ovFloatCast![i] = (byte)cfg.FloatCast;
                i++;
            }
        }

        var h = NativeBindings.pl_lazyframe_match_to_schema(
            lf, schema, (byte)extraColumnsCode,
            (byte)defaultConfig.MissingColumnsType, defExprPtr,
            (byte)defaultConfig.MissingStructFields, (byte)defaultConfig.ExtraStructFields,
            (byte)defaultConfig.IntegerCast, (byte)defaultConfig.FloatCast,
            ovNames, ovMissingType, ovMissingExprPtrs, ovMissingStruct, ovExtraStruct, ovIntCast, ovFloatCast,
            (nuint)ovLen
        );

        lf.TransferOwnership();
        
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle LazyFrameWithRowIndex(LazyFrameHandle lf, string name, int? offset = null)
    {
        int rustOffset = offset ?? -1;
        var h = NativeBindings.pl_lazyframe_with_row_index(lf, name, rustOffset);
        lf.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static LazyFrameHandle MergeSorted(
        LazyFrameHandle lf,
        LazyFrameHandle other,
        string key,
        bool maintainOrder)
    {
        var h = NativeBindings.pl_lazyframe_merge_sorted(lf,other,key,maintainOrder);
        lf.TransferOwnership();
        other.TransferOwnership();
        return ErrorHelper.Check(h);
    }
    public static Task<DataFrameHandle> LazyCollectAsync(
        LazyFrameHandle handle, 
        PlEngine engine,
        bool useStreaming, 
        CancellationToken cancellationToken) 
            => Task.Run(() => LazyCollect(handle, engine,useStreaming), cancellationToken);
    // --- Clone Ops ---
    public static LazyFrameHandle LazyClone(LazyFrameHandle lf)
        => ErrorHelper.Check(NativeBindings.pl_lazy_clone(lf));
}