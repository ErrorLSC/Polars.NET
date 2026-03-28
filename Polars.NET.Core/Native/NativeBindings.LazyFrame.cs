using System.Runtime.InteropServices;
using Apache.Arrow.C;

namespace Polars.NET.Core.Native;
unsafe internal partial class NativeBindings
{
    [LibraryImport(LibName)] public static partial void pl_lazy_frame_free(IntPtr ptr);

    [LibraryImport(LibName)]
    public static partial LazyFrameHandle pl_lazy_frame_scan_stream(
        CArrowSchema* schema,
        delegate* unmanaged[Cdecl]<void*, Arrow.CArrowArrayStream*> callback,
        delegate* unmanaged[Cdecl]<void*, void> destroyCallback,
        void* userData
    );
    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_lazyframe_slice(LazyFrameHandle lf, long offset, UIntPtr length);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_lazy_group_by_dynamic(
        LazyFrameHandle lf,
        string indexCol,
        string every,
        string period,
        string offset,
        int label,
        [MarshalAs(UnmanagedType.I1)] bool includeBoundaries,
        int closedWindow,
        int startBy,
        IntPtr[] keys, UIntPtr keysLen,
        IntPtr[] aggs, UIntPtr aggsLen
    );
    [LibraryImport(LibName)] public static partial SchemaHandle pl_lazyframe_get_schema(LazyFrameHandle lf);
    [LibraryImport(LibName)] public static partial IntPtr pl_lazy_explain(LazyFrameHandle lf,[MarshalAs(UnmanagedType.U1)] bool optimized);
    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_lazy_filter(LazyFrameHandle lf, ExprHandle expr);
    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_lazy_select(LazyFrameHandle lf, IntPtr[] exprs, UIntPtr len);
    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_lazyframe_sort(
        LazyFrameHandle lf,
        IntPtr[] exprs,
        UIntPtr exprLen,
        bool* descending,
        UIntPtr descendingLen,
        bool* nullsLast,
        UIntPtr nullsLastLen,
        [MarshalAs(UnmanagedType.I1)] bool maintainOrder
    );
    [LibraryImport(LibName)]
    public static partial LazyFrameHandle pl_lazyframe_top_k(
        LazyFrameHandle lf,
        uint k,
        IntPtr[] by_ptrs,
        UIntPtr by_len,
        bool* reverse,
        UIntPtr reverse_len
    );

    [LibraryImport(LibName)]
    public static partial LazyFrameHandle pl_lazyframe_bottom_k(
        LazyFrameHandle lf,
        uint k,
        IntPtr[] by_ptrs,
        UIntPtr by_len,
        bool* reverse,
        UIntPtr reverse_len
    );
    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_lazy_groupby_agg(
        LazyFrameHandle lf, 
        IntPtr[] keys, UIntPtr keysLen, 
        IntPtr[] aggs, UIntPtr aggsLen,
        IntPtr havingExpr
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_lazyframe_join(
        LazyFrameHandle left, 
        LazyFrameHandle right,
        IntPtr[] leftOn, UIntPtr leftLen,
        IntPtr[] rightOn, UIntPtr rightLen,
        PlJoinType how,
        string? suffix,
        PlJoinValidation validation,
        PlJoinCoalesce coalesce,
        PlJoinMaintainOrder maintainOrder,
        PlJoinSide joinSide,
        [MarshalAs(UnmanagedType.U1)] bool nullsEqual,
        IntPtr sliceOffset,
        UIntPtr sliceLen
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_lazyframe_join_asof(
        LazyFrameHandle left,
        LazyFrameHandle right,
        IntPtr[] leftOn, UIntPtr leftLen,
        IntPtr[] rightOn, UIntPtr rightLen,
        // AsOf Keys
        IntPtr[] leftBy, UIntPtr leftByLen,
        IntPtr[] rightBy, UIntPtr rightByLen,
        PlAsofStrategy strategy, // u8
        // Tolerances
        string? toleranceStr,
        IntPtr toleranceInt,   // *const i64
        IntPtr toleranceFloat, // *const f64
        // AsOf Flags
        [MarshalAs(UnmanagedType.U1)] bool allowEq,
        [MarshalAs(UnmanagedType.U1)] bool checkSorted,
        // JoinArgs Common
        string? suffix,
        PlJoinValidation validation,
        PlJoinCoalesce coalesce,
        PlJoinMaintainOrder maintainOrder,
        PlJoinSide joinSide,
        [MarshalAs(UnmanagedType.U1)] bool nullsEqual,
        IntPtr sliceOffset, // *const i64
        UIntPtr sliceLen
    );
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_lazy_collect(LazyFrameHandle lf,[MarshalAs(UnmanagedType.U1)] bool useStreaming);
    [LibraryImport(LibName)]
    public static partial LazyFrameHandle pl_lazy_clone(LazyFrameHandle lf);

    [LibraryImport(LibName)] public static partial LazyFrameHandle pl_lazy_limit(LazyFrameHandle lf, uint n);
    [LibraryImport(LibName)] public static partial LazyFrameHandle pl_lazy_with_columns(LazyFrameHandle lf, IntPtr[] exprs, UIntPtr len);
    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_lazyframe_explode(
        LazyFrameHandle lf,
        SelectorHandle selector,
        [MarshalAs(UnmanagedType.U1)] bool emptyAsNull,
        [MarshalAs(UnmanagedType.U1)] bool keepNulls);
    // --- Reshaping (Lazy) ---
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_lazyframe_rename(
        LazyFrameHandle lf, 
        string[] existing, 
        UIntPtr existingLen, 
        string[] newNames, 
        UIntPtr newLen, 
        [MarshalAs(UnmanagedType.U1)] bool strict
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_lazyframe_pivot(
        LazyFrameHandle lf,
        SelectorHandle on,
        DataFrameHandle onColumns,
        SelectorHandle index,
        SelectorHandle values,
        IntPtr aggExpr,
        PlPivotAgg aggCode, 
        [MarshalAs(UnmanagedType.U1)] bool maintainOrder,
        string? separator
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial LazyFrameHandle pl_lazyframe_unpivot(
        LazyFrameHandle lf,
        SelectorHandle index,
        SelectorHandle? on, // Nullable
        string? varName,
        string? valName
    );
    [LibraryImport(LibName)] 
    public static partial LazyFrameHandle pl_lazy_concat(
        IntPtr[] lfs, 
        UIntPtr len,
        int how,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        [MarshalAs(UnmanagedType.U1)] bool parallel
    );
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_lazyframe_unnest(
        LazyFrameHandle lf, 
        SelectorHandle selector,
        string? separator
    );
    [LibraryImport(LibName)]
    public static partial LazyFrameHandle pl_lazyframe_drop(
        LazyFrameHandle lf, 
        SelectorHandle selector
    );
    [LibraryImport(LibName)]
    public static partial LazyFrameHandle pl_lazyframe_unique(
        LazyFrameHandle lf, 
        IntPtr selector,
        PlUniqueKeepStrategy keep,
        [MarshalAs(UnmanagedType.I1)] bool maintainOrder
    );
    // --- Streaming & Sink ---
    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_lazy_collect_streaming(LazyFrameHandle lf);
}