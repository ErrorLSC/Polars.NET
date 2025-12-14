using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;

namespace Polars.Native;

public static partial class PolarsWrapper
{
    private static readonly CleanupCallback s_cleanupDelegate = CleanupTrampoline;

    private static void CleanupTrampoline(IntPtr userData)
    {
        try
        {
            if (userData != IntPtr.Zero)
            {
                GCHandle handle = GCHandle.FromIntPtr(userData);
                if (handle.IsAllocated) handle.Free();
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[Polars C#] Error freeing UDF handle: {ex}");
        }
    }

    // public static ExprHandle Map(ExprHandle expr, Func<IArrowArray, IArrowArray> func)
    // {
    //     return Map(expr, func);
    // }

    public static ExprHandle Map(ExprHandle expr, Func<IArrowArray, IArrowArray> func, DataTypeHandle outputType)
    {
        unsafe int Trampoline(CArrowArray* inArr, CArrowSchema* inSch, CArrowArray* outArr, CArrowSchema* outSch, byte* msgBuf)
        {
            try 
            {
                // 1. 导入
                var field = CArrowSchemaImporter.ImportField(inSch);
                var array = CArrowArrayImporter.ImportArray(inArr, field.DataType);

                // 2. 执行用户逻辑
                var resultArray = func(array);

                // 3. 导出 (关键修复区域)
                
                // [Fix 1] 彻底清空输出结构体的内存。
                // CArrowArray 和 CArrowSchema 是结构体，如果不清零，
                // Arrow C# 的 Exporter 可能会保留未初始化的字段 (如 flags 或 dictionary)，
                // 导致 Polars 在合并多个 Chunk 时认为它们 Schema 不兼容。
                *outArr = default;
                *outSch = default;

                // [Fix 2] 导出 Array
                CArrowArrayExporter.ExportArray(resultArray, outArr);

                // [Fix 3] 导出 Schema，使用固定名字而非空字符串
                // 虽然 Polars 通常忽略 UDF 返回的列名，但在 C Data Interface 中，
                // name 指针如果是 NULL 或者空，有时会引发未定义行为。
                var outField = new Field("result", resultArray.Data.DataType, true);
                CArrowSchemaExporter.ExportField(outField, outSch);
                
                return 0;
            }
            catch (Exception ex)
            {
                string errorMsg = ex.ToString(); 
                byte[] bytes = System.Text.Encoding.UTF8.GetBytes(errorMsg);
                int maxLen = 1023; 
                int copyLen = Math.Min(bytes.Length, maxLen);
                Marshal.Copy(bytes, 0, (IntPtr)msgBuf, copyLen);
                msgBuf[copyLen] = 0; 
                
                // 异常时也要清零
                *outArr = default; 
                *outSch = default;

                return 1; 
            }
        }

        unsafe 
        {
            UdfCallback callback = Trampoline;
            GCHandle gcHandle = GCHandle.Alloc(callback);
            IntPtr userData = GCHandle.ToIntPtr(gcHandle);

            try
            {
                var h = NativeBindings.pl_expr_map(
                    expr,
                    callback,
                    outputType,
                    s_cleanupDelegate,
                    userData
                );
                expr.TransferOwnership();
                return ErrorHelper.Check(h);
            }
            catch
            {
                if (gcHandle.IsAllocated) gcHandle.Free();
                throw;
            }
        }
    }
}