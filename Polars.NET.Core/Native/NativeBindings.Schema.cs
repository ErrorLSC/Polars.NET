using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

unsafe internal partial class NativeBindings
{
    // Schema
    [LibraryImport(LibName)]
    public static partial void pl_schema_free(IntPtr ptr);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SchemaHandle pl_schema_new(
        string[] names, 
        IntPtr[] dtypes, 
        UIntPtr len
    );
    // Introspection
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_schema_len(SchemaHandle schema,out uint len);

    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_schema_get_at_index(
        SchemaHandle schema,
        UIntPtr index,
        out IntPtr namePtr,
        out DataTypeHandle dtypeHandle
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_schema_add_field(
        IntPtr schema,  // *mut SchemaContext
        string name,    // *const c_char
        IntPtr dtype    // *mut DataTypeContext
    );
}