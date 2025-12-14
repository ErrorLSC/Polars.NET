using Apache.Arrow.C;
using Apache.Arrow;
using System.Runtime.InteropServices;

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
            // [修改] 使用 TransferOwnership()
            // 这步操作做了两件事：
            // 1. 获取原始指针传给 Rust
            // 2. 标记 C# Handle 为无效，防止 GC 二次释放
            // (因为 Rust 侧的 Vec<Expr> 已经接管了这些指针的生命周期)
            ptrs[i] = handles[i].TransferOwnership();
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
    private static R UseUtf8StringArray<R>(string[] strings, Func<IntPtr[], R> action)
    {
        if (strings == null || strings.Length == 0)
        {
            return action(System.Array.Empty<IntPtr>());
        }

        var ptrs = new IntPtr[strings.Length];
        try
        {
            // 分配内存
            for (int i = 0; i < strings.Length; i++)
            {
                ptrs[i] = Marshal.StringToCoTaskMemUTF8(strings[i]);
            }

            // 执行操作
            return action(ptrs);
        }
        finally
        {
            // 清理内存
            for (int i = 0; i < ptrs.Length; i++)
            {
                if (ptrs[i] != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(ptrs[i]);
                }
            }
        }
    }
}