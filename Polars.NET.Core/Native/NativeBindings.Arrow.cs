using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow.C;
using Polars.NET.Core.Arrow;

namespace Polars.NET.Core.Native;

unsafe internal partial class NativeBindings
{
    [LibraryImport(LibName)]
    public static partial void pl_arrow_array_free(IntPtr ptr);

    [LibraryImport(LibName)]
    public static partial void pl_arrow_array_export(ArrowArrayContextHandle ptr, void* out_c_array);

    [LibraryImport(LibName)]
    public static partial void pl_arrow_schema_export(ArrowArrayContextHandle ptr, void* out_c_schema);

    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_dataframe_new_from_stream(
        Arrow.CArrowArrayStream* stream
    );
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_dataframe_new_from_stream_strict_type(
        Arrow.CArrowArrayStream* stream
    );
    [LibraryImport(LibName)] 
    public static partial void pl_to_arrow(DataFrameHandle handle, CArrowArray* arr, CArrowSchema* schema);
    [LibraryImport(LibName)]
    public static partial DataFrameHandle pl_dataframe_from_arrow_record_batch(
        CArrowArray* cArray, 
        CArrowSchema* cSchema
    );
    [LibraryImport(LibName)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial void pl_dataframe_export_batches(
        DataFrameHandle df,
        ArrowStreamInterop.SinkCallback callback,
        ArrowStreamInterop.CleanupCallback cleanup,
        IntPtr userData
    );
    [LibraryImport(LibName)]
    public static partial int pl_dataframe_export_to_stream(
        DataFrameHandle df_ptr, 
        Apache.Arrow.C.CArrowArrayStream* out_stream,
        ReadOnlySpan<int> colIndices, 
        nuint numCols,     
        ulong* shuffleSeed  
    );
    [LibraryImport(LibName)]
    [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvCdecl) })]
    public static partial LazyFrameHandle pl_lazy_map_batches(
        LazyFrameHandle lf, 
        ArrowStreamInterop.SinkCallback callback,
        ArrowStreamInterop.CleanupCallback cleanup,
        IntPtr userData 
    );

}