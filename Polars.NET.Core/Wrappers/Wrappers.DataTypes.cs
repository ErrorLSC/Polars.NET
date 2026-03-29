using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public readonly partial struct PolarsWrapper
{
    public static DataTypeHandle CloneHandle(DataTypeHandle handle) => ErrorHelper.Check(NativeBindings.pl_datatype_clone(handle));
    public static DataTypeHandle NewPrimitiveType(int code) => ErrorHelper.Check(NativeBindings.pl_datatype_new_primitive(code));
    public static DataTypeHandle NewDecimalType(int precision, int scale) => ErrorHelper.Check(NativeBindings.pl_datatype_new_decimal((UIntPtr)precision, (UIntPtr)scale));
    public static DataTypeHandle NewCategoricalType() => ErrorHelper.Check(NativeBindings.pl_datatype_new_categorical());
    public static DataTypeHandle NewListType(DataTypeHandle innerType)
       => ErrorHelper.Check(NativeBindings.pl_datatype_new_list(innerType));
    public static DataTypeHandle NewDateTimeType(byte unit, string? timezone)
        => ErrorHelper.Check(NativeBindings.pl_datatype_new_datetime(unit,timezone));
    public static DataTypeHandle NewDurationType(byte unit) 
        => ErrorHelper.Check(NativeBindings.pl_datatype_new_duration(unit));
    public static DataTypeHandle NewArrayType(DataTypeHandle inner, ulong width)
    {
        return ErrorHelper.Check(NativeBindings.pl_datatype_new_array(inner, (UIntPtr)width));
    }
    public static DataTypeHandle NewStructType(string[] names, DataTypeHandle[] types)
    {
        if (names.Length != types.Length) 
            throw new ArgumentException("Names and Types must have same length");
        var typePtrs = HandlesToPtrs(types);

        return UseUtf8StringArray(names, (namePtrs) => 
        {
            return ErrorHelper.Check(
                NativeBindings.pl_datatype_new_struct(
                    namePtrs, 
                    typePtrs, 
                    (UIntPtr)names.Length
                )
            );
        });
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
    /// Get Struct field info for specified index。
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
}