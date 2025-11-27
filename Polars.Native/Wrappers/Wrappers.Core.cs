using Apache.Arrow.C;
using Apache.Arrow;

namespace Polars.Native;

public static partial class PolarsWrapper
{
    // 辅助：批量转换 Handle
    private static IntPtr[] HandlesToPtrs(PolarsHandle[] handles)
    {
        if (handles == null || handles.Length == 0) return System.Array.Empty<IntPtr>();
        var ptrs = new IntPtr[handles.Length];
        for (int i = 0; i < handles.Length; i++)
        {
            ptrs[i] = handles[i].DangerousGetHandle();
            handles[i].SetHandleAsInvalid(); 
        }
        return ptrs;
    }

    // 辅助：获取行列数 (利用之前加的 API)
    public static long DataFrameHeight(DataFrameHandle df) => (long)NativeBindings.pl_dataframe_height(df);
    public static long DataFrameWidth(DataFrameHandle df) => (long)NativeBindings.pl_dataframe_width(df);

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
}