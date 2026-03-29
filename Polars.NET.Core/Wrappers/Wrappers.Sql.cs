using System.Runtime.InteropServices;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public readonly partial struct PolarsWrapper
{
    public static SqlContextHandle SqlContextNew() 
        => ErrorHelper.Check(NativeBindings.pl_sql_context_new());

    public static void SqlRegister(SqlContextHandle ctx, string name, LazyFrameHandle lf)
    {
        NativeBindings.pl_sql_context_register(ctx, name, lf);
        
        lf.TransferOwnership();
        
        ErrorHelper.CheckVoid();
    }

    public static void SqlUnRegister(SqlContextHandle ctx, string name)
    {
        NativeBindings.pl_sql_context_unregister(ctx, name);
        
        ErrorHelper.CheckVoid();
    }

    public static LazyFrameHandle SqlExecute(SqlContextHandle ctx, string query)
        => ErrorHelper.Check(NativeBindings.pl_sql_context_execute(ctx, query));
    public static string[] SqlGetTables(SqlContextHandle ctx)
    {
        IntPtr ptr = NativeBindings.pl_sql_context_get_tables(ctx, out nuint len);
        
        ErrorHelper.CheckVoid();

        if (ptr == IntPtr.Zero)
        {
            return [];
        }

        try
        {
            if (len == 0)
            {
                return [];
            }

            string[] tables = new string[len];

            for (int i = 0; i < (int)len; i++)
            {
                IntPtr strPtr = Marshal.ReadIntPtr(ptr, i * IntPtr.Size);
                
                tables[i] = Marshal.PtrToStringUTF8(strPtr) ?? string.Empty;
            }
            
            return tables;
        }
        finally
        {
            NativeBindings.pl_sql_context_free_tables_array(ptr, len);
        }
    }
}