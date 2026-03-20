using System.Runtime.InteropServices;
using Apache.Arrow.C;

namespace Polars.NET.Core.Native;

unsafe internal partial class NativeBindings
{
    // --- DataType ---
    [LibraryImport(LibName)]
    public static partial void pl_datatype_free(IntPtr ptr);
    [LibraryImport(LibName)]
    public static partial void pl_datatype_export_arrow_schema(DataTypeHandle dataTypePtr,CArrowSchema* outSchema);
    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_new_primitive(int code);

    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_new_decimal(UIntPtr precision, UIntPtr scale);

    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_new_categorical();
    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_new_list(DataTypeHandle inner);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataTypeHandle pl_datatype_new_datetime(byte unit, string? timezone);
    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_new_duration(byte unit);
    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_new_array(
        DataTypeHandle inner, 
        UIntPtr width
    );

    [LibraryImport(LibName)]
    public static partial UIntPtr pl_datatype_get_array_width(
        DataTypeHandle dtype
    );
    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_new_struct(
    [In] IntPtr[] names, 
    [In] IntPtr[] types, 
    UIntPtr len
    );
    [LibraryImport(LibName)]
    public static partial IntPtr pl_datatype_to_string(DataTypeHandle handle);
    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_clone(DataTypeHandle handle);
    // 1. GetKind -
    [LibraryImport(LibName)]
    public static partial int pl_datatype_get_kind(DataTypeHandle handle);

    // 2. GetTimeUnit -
    [LibraryImport(LibName)]
    public static partial int pl_datatype_get_time_unit(DataTypeHandle handle);

    // 3. GetDecimalInfo - 
    [LibraryImport(LibName)]
    public static partial void pl_datatype_get_decimal_info(DataTypeHandle handle, out int precision, out int scale);

    // 4. GetTimeZone - 
    [LibraryImport(LibName)]
    public static partial IntPtr pl_datatype_get_timezone(DataTypeHandle handle);
    [LibraryImport(LibName)]
    public static partial DataTypeHandle pl_datatype_get_inner(DataTypeHandle handle);

    [LibraryImport(LibName)]
    public static partial UIntPtr pl_datatype_get_struct_len(DataTypeHandle handle);

    [LibraryImport(LibName)]
    public static partial void pl_datatype_get_struct_field(
        DataTypeHandle handle, 
        UIntPtr index, 
        out IntPtr namePtr,       
        out DataTypeHandle typeHandle 
    );
}