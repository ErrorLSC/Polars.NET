using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;

[assembly: DisableRuntimeMarshalling]

namespace Polars.NET.Core.Native;

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
public unsafe delegate Arrow.CArrowArrayStream* StreamFactoryCallback(void* userData);

unsafe internal partial class NativeBindings
{
    [LibraryImport(LibName)]
    public static partial void pl_dataframe_free(IntPtr ptr);

    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_dataframe_new(
        IntPtr[] columns, 
        UIntPtr len
    );

    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_dataframe_from_schema(
        SchemaHandle schema,
        nuint length
    );

    [LibraryImport(LibName)]
    public static partial SchemaHandle pl_dataframe_get_schema(DataFrameHandle df);
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_dataframe_height(DataFrameHandle df, out uint height);
    
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_dataframe_width(DataFrameHandle df,out uint width);
    [LibraryImport(LibName)] public static partial IntPtr pl_dataframe_get_column_name(DataFrameHandle df, UIntPtr index);
    [LibraryImport(LibName)] public static partial IntPtr pl_dataframe_to_string(DataFrameHandle df);
    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_dataframe_clone(DataFrameHandle df);
    [LibraryImport(LibName)]
    public static partial LazyFrameHandle pl_dataframe_lazy(DataFrameHandle df);
    [LibraryImport(LibName)] public static partial DataFrameHandle pl_dataframe_rechunk(DataFrameHandle df);
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_head(DataFrameHandle df, UIntPtr n);
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_tail(DataFrameHandle df, UIntPtr n);

    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_filter(DataFrameHandle df, ExprHandle expr);

    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_with_columns(DataFrameHandle df, IntPtr[] exprs, UIntPtr len);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_dataframe_drop_many(DataFrameHandle df, string[] columns, nuint len);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SeriesHandle pl_dataframe_drop_in_place(DataFrameHandle df, string name);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_dataframe_rename(DataFrameHandle df, string oldName, string newName);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_dataframe_rename_many(DataFrameHandle df, string[] oldNames, string[] newNames, nuint count);

    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_dataframe_drop_nulls(DataFrameHandle df, IntPtr[] subset, UIntPtr len);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_df_unique(
        DataFrameHandle df,
        [In] 
        string[]? subset,
        UIntPtr subset_len,
        PlUniqueKeepStrategy keep,
        [MarshalAs(UnmanagedType.U1)] bool maintain_order,
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
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_dataframe_explode(
        DataFrameHandle df,
        string[] cols,
        UIntPtr len,
        [MarshalAs(UnmanagedType.U1)] bool emptyAsNull,
        [MarshalAs(UnmanagedType.U1)] bool keepNulls);
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_select(DataFrameHandle df, IntPtr[] exprs, UIntPtr len);
    [LibraryImport(LibName)] 
    public static partial DataFrameHandle pl_dataframe_slice(DataFrameHandle df, long offset, UIntPtr length);
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
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_dataframe_hash_rows(
        DataFrameHandle df,
        ulong seed,
        [MarshalAs(UnmanagedType.U1)] bool has_seed
    );
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_dataframe_estimated_size(
        DataFrameHandle handle, 
        out nuint size
    );
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_dataframe_is_duplicated(DataFrameHandle handle);
    [LibraryImport(LibName)]
    public static partial SeriesHandle pl_dataframe_is_unique(DataFrameHandle handle);
}