using System.Runtime.InteropServices;

namespace Polars.Native;

public static partial class PolarsWrapper
{
    public static SqlContextHandle SqlContextNew() 
        => ErrorHelper.Check(NativeBindings.pl_sql_context_new());

    public static void SqlRegister(SqlContextHandle ctx, string name, LazyFrameHandle lf)
    {
        var namePtr = Marshal.StringToCoTaskMemUTF8(name);
        try 
        {
            NativeBindings.pl_sql_context_register(ctx, namePtr, lf);
            // 注册会消耗 LazyFrame
            lf.TransferOwnership();
            ErrorHelper.CheckVoid();
        }
        finally
        {
            Marshal.FreeCoTaskMem(namePtr);
        }
    }

    public static LazyFrameHandle SqlExecute(SqlContextHandle ctx, string query)
    {
        var queryPtr = Marshal.StringToCoTaskMemUTF8(query);
        try
        {
            var h = NativeBindings.pl_sql_context_execute(ctx, queryPtr);
            return ErrorHelper.Check(h);
        }
        finally
        {
            Marshal.FreeCoTaskMem(queryPtr);
        }
    }
}