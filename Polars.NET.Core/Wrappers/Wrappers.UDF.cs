using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.C;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public readonly partial struct PolarsWrapper
{
    private static readonly NativeBindings.CleanupCallback s_cleanupDelegate = CleanupTrampoline;

    private static void CleanupTrampoline(nint userData)
    {
        try
        {
            if (userData != nint.Zero)
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
    private static unsafe IArrowArray ImportSingle(CArrowArray* arr, CArrowSchema* sch)
    {
        var field = CArrowSchemaImporter.ImportField(sch);
        return CArrowArrayImporter.ImportArray(arr, field.DataType);
    }

    private static unsafe void ExportResult(IArrowArray result, CArrowArray* outArr, CArrowSchema* outSch)
    {
        *outArr = default;
        *outSch = default;
        CArrowArrayExporter.ExportArray(result, outArr);
        var outField = new Field("result", result.Data.DataType, true);
        CArrowSchemaExporter.ExportField(outField, outSch);
    }

    private static unsafe void WriteErrorToBuffer(byte* buffer, int bufferLength, Exception ex)
    {
        string msg = ex.ToString();
        byte[] bytes = Encoding.UTF8.GetBytes(msg);
        int copyLen = Math.Min(bytes.Length, bufferLength - 1);
        Marshal.Copy(bytes, 0, (nint)buffer, copyLen);
        buffer[copyLen] = 0;
    }

    public static ExprHandle Map(ExprHandle expr, Func<IArrowArray, IArrowArray> func, DataTypeHandle outputType)
    {
        unsafe
        {
            NativeBindings.UdfCallback callback = (inArr, inSch, outArr, outSch, msgBuf) =>
            {
                try
                {
                    var array = ImportSingle(inArr, inSch);
                    var result = func(array);
                    ExportResult(result, outArr, outSch);
                    return 0;
                }
                catch (Exception ex)
                {
                    WriteErrorToBuffer(msgBuf, 1024, ex);
                    *outArr = default;
                    *outSch = default;
                    return 1;
                }
            };

            var handle = GCHandle.Alloc(callback);
            try
            {
                var h = NativeBindings.pl_expr_map(expr, callback, outputType, s_cleanupDelegate, GCHandle.ToIntPtr(handle));
                var checkedHandle = ErrorHelper.Check(h);   
                expr.TransferOwnership();                   
                return checkedHandle;
            }
            catch
            {
                if (handle.IsAllocated) handle.Free();
                throw;
            }
        }
    }
    public static ExprHandle MapMany(
        ExprHandle main,
        ExprHandle[] others,
        Func<IReadOnlyList<IArrowArray>, IArrowArray> function,
        DataTypeHandle outputType)
    {
        unsafe
        {
            NativeBindings.MultiUdfCallback callback = (n, arrs, schs, outArr, outSch, _, errorBuf, errorBufLen) =>
            {
                try
                {
                    var inputs = new List<IArrowArray>((int)n);
                    for (int i = 0; i < n; i++)
                        inputs.Add(ImportSingle(arrs[i], schs[i]));
                    var result = function(inputs);
                    ExportResult(result, outArr, outSch);
                    return 0;
                }
                catch (Exception ex)
                {
                    WriteErrorToBuffer(errorBuf, (int)errorBufLen, ex);
                    *outArr = default;
                    *outSch = default;
                    return 1;
                }
            };

            var handle = GCHandle.Alloc(callback);
            try
            {
                var argsPtrs = HandlesToPtrs(others);
                var h = NativeBindings.pl_expr_map_many(
                    main, argsPtrs, (nuint)argsPtrs.Length,
                    callback, outputType, s_cleanupDelegate, GCHandle.ToIntPtr(handle));
                var checkedHandle = ErrorHelper.Check(h);  
                main.TransferOwnership();                   
                return checkedHandle;
            }
            catch
            {
                if (handle.IsAllocated) handle.Free();
                throw;
            }
        }
    }
}