using System.Runtime.InteropServices;
using Apache.Arrow.C;

namespace Polars.NET.Core.Native;

unsafe internal partial class NativeBindings
{
    // --- DataType ---
    [LibraryImport(LibName)]
    public static partial void pl_datatype_free(IntPtr ptr);
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_datatype_export_arrow_schema(DataTypeHandle dataTypePtr,CArrowSchema* outSchema);
    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_new_primitive(int code);

    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_new_decimal(UIntPtr precision, UIntPtr scale);

    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_new_categorical(CategoriesHandle categories);
    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_new_list(DataTypeHandle inner);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataTypeHandle pl_datatype_new_datetime(byte unit, string? timezone);
    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_new_duration(byte unit);
    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_new_array(
        DataTypeHandle inner,
        ref nuint shape,   
        nuint ndim         
    );

    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_datatype_get_array_width(
        DataTypeHandle dtype,
        out uint width
    );
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_datatype_get_array_shape(
        DataTypeHandle dtype,
        out nint out_shape,
        out nuint out_len
    );

    [LibraryImport(LibName)]
    public static partial void pl_free_shape(IntPtr data, nuint len);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataTypeHandle pl_datatype_new_struct(
    string[] names, 
    [In] IntPtr[] types, 
    UIntPtr len
    );
    [LibraryImport(LibName)]
    public static partial IntPtr pl_datatype_to_string(DataTypeHandle handle);
    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_clone(DataTypeHandle handle);
    // 1. GetKind 
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_datatype_get_kind(DataTypeHandle handle,out PlDataType kind);

    // 2. GetTimeUnit 
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_datatype_get_time_unit(DataTypeHandle handle, out PlTimeUnit unit);

    // 3. GetDecimalInfo  
    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_datatype_get_decimal_info(DataTypeHandle handle, out int precision, out int scale);

    // 4. GetTimeZone  
    [LibraryImport(LibName)]
    public static partial IntPtr pl_datatype_get_timezone(DataTypeHandle handle);
    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_get_inner(DataTypeHandle handle);

    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_datatype_get_struct_len(DataTypeHandle handle, out uint len);

    [LibraryImport(LibName)]
    [return: MarshalAs(UnmanagedType.U1)]
    public static partial bool pl_datatype_get_struct_field(
        DataTypeHandle handle, 
        UIntPtr index, 
        out IntPtr namePtr,       
        out DataTypeHandle typeHandle 
    );
    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_new_enum(FrozenCategoriesHandle frozenCategories);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataTypeHandle pl_datatype_new_extension(
        string name,
        DataTypeHandle inner_dtype,
        string? metadata
    );
    [LibraryImport(LibName)]
    public static partial CategoriesHandle pl_datatype_get_categories(DataTypeHandle ptr);

    [LibraryImport(LibName)]
    public static partial FrozenCategoriesHandle pl_datatype_get_enum_categories(DataTypeHandle ptr);

    [LibraryImport(LibName)]
    public static partial int pl_datatype_eq(DataTypeHandle a, DataTypeHandle b, [MarshalAs(UnmanagedType.U1)] out bool isEqual);
    [LibraryImport(LibName)]
    public static partial int pl_datatype_get_extension_name(DataTypeHandle dtype,out nint name);
    [LibraryImport(LibName)]
    public static partial int pl_datatype_get_extension_metadata(DataTypeHandle dtype,out nint metadata);
    [LibraryImport(LibName)]
    public static partial int pl_datatype_get_extension_storage(DataTypeHandle dtype, out DataTypeHandle storage);
}