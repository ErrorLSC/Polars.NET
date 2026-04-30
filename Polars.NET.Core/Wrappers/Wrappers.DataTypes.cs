using System.Runtime.InteropServices;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public readonly partial struct PolarsWrapper
{
    public static DataTypeHandle CloneHandle(DataTypeHandle handle) => ErrorHelper.Check(NativeBindings.pl_datatype_clone(handle));
    public static DataTypeHandle NewPrimitiveType(int code) => ErrorHelper.Check(NativeBindings.pl_datatype_new_primitive(code));
    public static DataTypeHandle NewDecimalType(int precision, int scale) => ErrorHelper.Check(NativeBindings.pl_datatype_new_decimal((UIntPtr)precision, (UIntPtr)scale));
    public static DataTypeHandle NewCategoricalType(CategoriesHandle categories) => ErrorHelper.Check(NativeBindings.pl_datatype_new_categorical(categories));
    public static DataTypeHandle NewListType(DataTypeHandle innerType)
       => ErrorHelper.Check(NativeBindings.pl_datatype_new_list(innerType));
    public static DataTypeHandle NewDateTimeType(byte unit, string? timezone)
        => ErrorHelper.Check(NativeBindings.pl_datatype_new_datetime(unit,timezone));
    public static DataTypeHandle NewDurationType(byte unit) 
        => ErrorHelper.Check(NativeBindings.pl_datatype_new_duration(unit));
    // public static DataTypeHandle NewArrayType(DataTypeHandle inner, ulong width)
    //     => ErrorHelper.Check(NativeBindings.pl_datatype_new_array(inner, (UIntPtr)width));
    public static DataTypeHandle NewArrayType(DataTypeHandle inner, uint width)
        => NewArrayType(inner, new ReadOnlySpan<uint>([width])); 
    public static DataTypeHandle NewArrayType(DataTypeHandle inner, ReadOnlySpan<uint> shape)
    {
        if (shape.IsEmpty)
            throw new ArgumentException("Shape must not be empty.", nameof(shape));

        Span<nuint> nuShape = stackalloc nuint[shape.Length];
        for (int i = 0; i < shape.Length; i++)
        {
            nuShape[i] = shape[i];
        }

        ref nuint shapeRef = ref MemoryMarshal.GetReference(nuShape);

        return ErrorHelper.Check(
            NativeBindings.pl_datatype_new_array(inner, ref shapeRef, (nuint)shape.Length)
        );
    }
    
    public static DataTypeHandle NewStructType(string[] names, DataTypeHandle[] types)
    {
        if (names.Length != types.Length) 
            throw new ArgumentException("Names and Types must have same length");
        var typePtrs = HandlesToPtrs(types);
        var h = NativeBindings.pl_datatype_new_struct(names, typePtrs, (nuint)names.Length);

        return ErrorHelper.Check(h);
    }
    
    /// <summary>
    /// Get Dtype String
    /// </summary>
    public static string GetDataTypeString(DataTypeHandle handle)
    {
        IntPtr strPtr = NativeBindings.pl_datatype_to_string(handle);
        
        if (strPtr == IntPtr.Zero) return "unknown";

        return ErrorHelper.CheckString(strPtr);
    }

    /// <summary>
    /// Get TimeZone String
    /// </summary>
    public static string? GetTimeZone(DataTypeHandle handle)
    {

        nint ptr = NativeBindings.pl_datatype_get_timezone(handle);
        
        return ErrorHelper.CheckString(ptr);
    }
    /// <summary>
    /// Get DataType Kind
    /// </summary>
    public static PlDataType GetDataTypeKind(DataTypeHandle handle)
    {
        bool success = NativeBindings.pl_datatype_get_kind(handle,out PlDataType kind);
        ErrorHelper.CheckBool(success);
        return kind;
    }

    /// <summary>
    /// Get Time Unit
    /// </summary>
    public static PlTimeUnit GetTimeUnit(DataTypeHandle handle)
    {
        bool success = NativeBindings.pl_datatype_get_time_unit(handle,out PlTimeUnit unit);
        ErrorHelper.CheckBool(success);
        return unit;
    }

    /// <summary>
    /// Get Decimal Precision and Scal
    /// </summary>
    public static void GetDecimalInfo(DataTypeHandle handle, out int precision, out int scale)
    {
        bool success = NativeBindings.pl_datatype_get_decimal_info(handle, out precision, out scale);

        ErrorHelper.CheckBool(success);
    }
    // ==========================================
    // DataType Introspection Wrappers
    // ==========================================

    /// <summary>
    /// Get List InnerType handle。
    /// </summary>
    public static DataTypeHandle GetInnerType(DataTypeHandle handle)
        => ErrorHelper.Check(NativeBindings.pl_datatype_get_inner(handle));
    public static ulong DataTypeGetArrayWidth(DataTypeHandle dtype)
    {  
        bool success = NativeBindings.pl_datatype_get_array_width(dtype, out uint width);
        
        ErrorHelper.CheckBool(success);
        
        return width;
    }
    public static uint[] GetArrayShape(DataTypeHandle handle)
    {
        bool success = NativeBindings.pl_datatype_get_array_shape(
            handle,
            out nint rawShape,
            out nuint len
        );
        ErrorHelper.CheckBool(success);

        if (rawShape == IntPtr.Zero || len == 0)
            return [];

        var result = new uint[len];
        unsafe
        {
            var src = (nuint*)rawShape;
            for (int i = 0; i < (int)len; i++)
            {
                result[i] = (uint)src[i];
            }
        }

        NativeBindings.pl_free_shape(rawShape, len);
        return result;
    }
    /// <summary>
    /// Get Struct field length
    /// </summary>
    public static ulong GetStructLen(DataTypeHandle handle)
    {        
        bool success = NativeBindings.pl_datatype_get_struct_len(handle,out uint len);
        
        ErrorHelper.CheckBool(success); 
        
        return len;
    }

    /// <summary>
    /// Get Struct field info for specified index
    /// </summary>
    public static void GetStructField(DataTypeHandle handle, ulong index, out string name, out DataTypeHandle typeHandle)
    {
        bool success = NativeBindings.pl_datatype_get_struct_field(
            handle, 
            (UIntPtr)index, 
            out IntPtr namePtr, 
            out var outTypeHandle
        );

        ErrorHelper.CheckBool(success); 

        typeHandle = ErrorHelper.Check(outTypeHandle);
        
        name = ErrorHelper.CheckString(namePtr);
    }
    public static DataTypeHandle NewEnumType(FrozenCategoriesHandle frozenCategories) 
        => ErrorHelper.Check(NativeBindings.pl_datatype_new_enum(frozenCategories));
    public static DataTypeHandle NewExtensionType(string name, DataTypeHandle innerType, string? metadata)
        => ErrorHelper.Check(NativeBindings.pl_datatype_new_extension(name, innerType, metadata));
    public static CategoriesHandle GetCategories(DataTypeHandle dtype)
        => ErrorHelper.Check(NativeBindings.pl_datatype_get_categories(dtype));
    public static FrozenCategoriesHandle GetEnumCategories(DataTypeHandle dtype)
        => ErrorHelper.Check(NativeBindings.pl_datatype_get_enum_categories(dtype));
    public static bool DataTypeEq(DataTypeHandle a,DataTypeHandle b)
    {
        int status = NativeBindings.pl_datatype_eq(a,b,out bool result);
        ErrorHelper.CheckStatus(status);
        return result;
    }
    public static string DataTypeGetExtensionName(DataTypeHandle dtype)
    {
        int status = NativeBindings.pl_datatype_get_extension_name(dtype,out nint name);
        ErrorHelper.CheckStatus(status);
        return ErrorHelper.CheckString(name);
    }
    public static string DataTypeGetExtensionMetadata(DataTypeHandle dtype)
    {
        int status = NativeBindings.pl_datatype_get_extension_metadata(dtype,out nint metadata);
        ErrorHelper.CheckStatus(status);
        return ErrorHelper.CheckString(metadata);
    }
    public static DataTypeHandle DataTypeGetExtensionStorage(DataTypeHandle dtype)
    {
        int status = NativeBindings.pl_datatype_get_extension_storage(dtype,out DataTypeHandle storage);
        ErrorHelper.CheckStatus(status);
        return ErrorHelper.Check(storage);
    }
}