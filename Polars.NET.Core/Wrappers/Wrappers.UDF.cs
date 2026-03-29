using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public readonly partial struct PolarsWrapper
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

    public static ExprHandle Map(ExprHandle expr, Func<IArrowArray, IArrowArray> func, DataTypeHandle outputType)
    {
        unsafe int Trampoline(CArrowArray* inArr, CArrowSchema* inSch, CArrowArray* outArr, CArrowSchema* outSch, byte* msgBuf)
        {
            try 
            {
                // Import Arrow
                var field = CArrowSchemaImporter.ImportField(inSch);
                var array = CArrowArrayImporter.ImportArray(inArr, field.DataType);

                // UDF execuate
                var resultArray = func(array);

                // Clear output struct memory
                *outArr = default;
                *outSch = default;

                CArrowArrayExporter.ExportArray(resultArray, outArr);

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