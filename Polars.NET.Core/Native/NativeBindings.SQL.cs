using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

internal partial class NativeBindings
{
    // SQL Context
    [LibraryImport(LibName)] 
    public static partial SqlContextHandle pl_sql_context_new();

    [LibraryImport(LibName)] 
    public static partial void pl_sql_context_free(IntPtr ptr);

    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] 
    public static partial void pl_sql_context_register(SqlContextHandle ctx, string name, LazyFrameHandle lf);
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] 
    public static partial void pl_sql_context_unregister(SqlContextHandle ctx, string name);

    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)] 
    public static partial LazyFrameHandle pl_sql_context_execute(SqlContextHandle ctx, string query);

    // Get all registered tables. Returns a pointer to an array of C-strings.
    // The length of the array is written to 'len'.
    [LibraryImport(LibName)] 
    public static partial IntPtr pl_sql_context_get_tables(SqlContextHandle ctx, out nuint len);

    // Free the string array allocated by Rust.
    // Requires the exact pointer and length returned by pl_sql_context_get_tables.
    [LibraryImport(LibName)] 
    public static partial void pl_sql_context_free_tables_array(IntPtr ptr, nuint len);
}