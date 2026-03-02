using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow.C;
using Polars.NET.Core.Arrow;

[assembly: DisableRuntimeMarshalling]

namespace Polars.NET.Core.Native;

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public unsafe delegate Arrow.CArrowArrayStream* StreamFactoryCallback(void* userData);

unsafe internal partial class NativeBindings
{
    [LibraryImport(LibName)]
    public static partial void pl_dataframe_free(IntPtr ptr);
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
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_dataframe_new_from_stream(
        Arrow.CArrowArrayStream* stream
    );
    [LibraryImport(LibName)]
    public static partial SchemaHandle pl_dataframe_get_schema(DataFrameHandle df);
    [LibraryImport(LibName)]
    public static partial UIntPtr pl_dataframe_height(DataFrameHandle df);
    
    [LibraryImport(LibName)]
    public static partial UIntPtr pl_dataframe_width(DataFrameHandle df);
    [LibraryImport(LibName)] public static partial IntPtr pl_dataframe_get_column_name(DataFrameHandle df, UIntPtr index);
    [LibraryImport(LibName)] public static partial IntPtr pl_dataframe_to_string(DataFrameHandle df);
    // Scalars
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_dataframe_get_i64(
        DataFrameHandle df, 
        string colName, 
        UIntPtr row, 
        out long outVal
    );

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_dataframe_get_f64(
        DataFrameHandle df, 
        string colName, 
        UIntPtr row, 
        out double outVal
    );
    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_dataframe_clone(DataFrameHandle df);
    [LibraryImport(LibName)]
    public static partial LazyFrameHandle pl_dataframe_lazy(DataFrameHandle df);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial IntPtr pl_dataframe_get_string(DataFrameHandle df, string colName, UIntPtr row);
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
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_dataframe_rename_many(DataFrameHandle df, string[] oldNames, string[] newNames, nuint count);

    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_dataframe_drop_nulls(DataFrameHandle df, IntPtr[] subset, UIntPtr len);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_df_unique_stable(
        DataFrameHandle df,
        [In] 
        string[]? subset,
        UIntPtr subset_len,
        PlUniqueKeepStrategy keep,
        long slice_offset,
        UIntPtr slice_len,
        byte slice_valid
    );

    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_dataframe_sample_n(DataFrameHandle df, UIntPtr n, [MarshalAs(UnmanagedType.U1)] bool replacement, [MarshalAs(UnmanagedType.I1)] bool shuffle, ulong* seed);

    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_dataframe_sample_frac(DataFrameHandle df, double frac, [MarshalAs(UnmanagedType.U1)] bool replacement, [MarshalAs(UnmanagedType.I1)] bool shuffle, ulong* seed);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_dataframe_unnest(
        DataFrameHandle df,
        string[] cols,
        UIntPtr len,
        string? separator
    );
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_select(DataFrameHandle df, IntPtr[] exprs, UIntPtr len);
    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_dataframe_slice(DataFrameHandle df, long offset, UIntPtr length);
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_groupby_agg(
        DataFrameHandle df, 
        IntPtr[] byExprs, UIntPtr byLen,
        IntPtr[] aggExprs, UIntPtr aggLen
    );
    // Join
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_join(
        DataFrameHandle left,
        DataFrameHandle right,
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
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_dataframe_sort(
        DataFrameHandle df,
        IntPtr[] exprs,
        UIntPtr exprLen,
        bool* descending, 
        UIntPtr descendingLen,
        bool* nullsLast,
        UIntPtr nullsLastLen,
        [MarshalAs(UnmanagedType.U1)] bool maintainOrder
    );
    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_dataframe_explode(
        DataFrameHandle df, 
        SelectorHandle selector,
        [MarshalAs(UnmanagedType.U1)] bool emptyAsNull,
        [MarshalAs(UnmanagedType.U1)] bool keepNulls);
    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_dataframe_concat(
        IntPtr[] dfs, 
        UIntPtr len,
        PlConcatType how,
        [MarshalAs(UnmanagedType.U1)] bool checkDuplicates,
        [MarshalAs(UnmanagedType.U1)] bool strict,
        [MarshalAs(UnmanagedType.U1)] bool unitLengthAsScalar
    );
    // --- Reshaping (Eager) ---
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_dataframe_pivot(
        DataFrameHandle df,
        SelectorHandle on,      // on_ptr (columns)
        SelectorHandle index,   // index_ptr
        SelectorHandle values,  // values_ptr
        IntPtr aggExpr,     // agg_expr_ptr
        PlPivotAgg aggCode,     // agg_code
        [MarshalAs(UnmanagedType.U1)] bool maintainOrder,
        [MarshalAs(UnmanagedType.U1)] bool sortColumns,
        string? separator       // separator_ptr
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)] 
    public static partial DataFrameHandle pl_unpivot(
        DataFrameHandle df,
        SelectorHandle index, 
        SelectorHandle? on, // Nullable
        string? varName,
        string? valName
    );

    [LibraryImport(LibName)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial void pl_dataframe_export_batches(
        DataFrameHandle df,
        ArrowStreamInterop.SinkCallback callback,
        ArrowStreamInterop.CleanupCallback cleanup,
        IntPtr userData
    );
    // Stack Ops
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_hstack(
        DataFrameHandle df, 
        IntPtr[] cols, 
        UIntPtr len
    );

    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_vstack(
        DataFrameHandle df, 
        DataFrameHandle other
    );
}