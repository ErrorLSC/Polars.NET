using System.Runtime.InteropServices;
using Apache.Arrow.C;

namespace Polars.NET.Core.Native;
unsafe internal partial class NativeBindings
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void CleanupCallback(nint userData);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate int UdfCallback(
        CArrowArray* inArray, 
        CArrowSchema* inSchema, 
        CArrowArray* outArray, 
        CArrowSchema* outSchema,
        byte* msgBuf
    );
    [LibraryImport(LibName)]
    public static partial ExprHandle pl_expr_map(
        ExprHandle expr, 
        UdfCallback callback, 
        DataTypeHandle returnType,
        CleanupCallback cleanup,
        nint userData          
    );

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate int MultiUdfCallback(
        uint numArgs,
        CArrowArray** inArrays,
        CArrowSchema** inSchemas,
        CArrowArray* outArray,
        CArrowSchema* outSchema,
        nint userData,
        byte* errorBuf,
        uint errorBufLen
    );
    [LibraryImport(LibName)]
    public static unsafe partial ExprHandle pl_expr_map_many(
        ExprHandle baseExpr,
        nint[] additionalArgs,    
        nuint additionalCount,
        MultiUdfCallback callback,
        DataTypeHandle outputType,
        CleanupCallback cleanup,
        nint userData
    );
}