using System.Runtime.InteropServices;

namespace Polars.NET.Core;
public static partial class PolarsWrapper
{
    public static DataTypeHandle CloneHandle(DataTypeHandle handle)
    {
         // 假设 NativeBindings.pl_datatype_clone 已经存在 (之前写过)
         return ErrorHelper.Check(NativeBindings.pl_datatype_clone(handle));
    }
    public static DataTypeHandle NewPrimitiveType(int code) => ErrorHelper.Check(NativeBindings.pl_datatype_new_primitive(code));
    public static DataTypeHandle NewDecimalType(int precision, int scale) => ErrorHelper.Check(NativeBindings.pl_datatype_new_decimal((UIntPtr)precision, (UIntPtr)scale));
    public static DataTypeHandle NewCategoricalType() => ErrorHelper.Check(NativeBindings.pl_datatype_new_categorical());
    public static DataTypeHandle NewListType(DataTypeHandle innerType)
       => ErrorHelper.Check(NativeBindings.pl_datatype_new_list(innerType));
    public static DataTypeHandle NewDateTimeType(int unit, string? timezone)
        => ErrorHelper.Check(NativeBindings.pl_datatype_new_datetime(unit,timezone));
    public static DataTypeHandle NewDurationType(int unit) 
        => ErrorHelper.Check(NativeBindings.pl_datatype_new_duration(unit));
    public static DataTypeHandle NewStructType(string[] names, DataTypeHandle[] types)
    {
        if (names.Length != types.Length) 
            throw new ArgumentException("Names and Types must have same length");
        var typePtrs = HandlesToPtrs(types);

        // 3. 字符串数组编组 & 调用 Native
        return UseUtf8StringArray(names, (namePtrs) => 
        {
            // 注意：你的 UseUtf8StringArray action 签名是 Func<IntPtr[], R>
            // 所以这里直接用 namePtrs
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
    /// 获取 Rust 端的类型字符串表示。
    /// 采用 "Clone-Consume" 模式，确保线程安全和内存安全。
    /// </summary>
    public static string GetDataTypeString(DataTypeHandle handle)
    {
        // 1. 调用 Rust (Borrow 模式)
        IntPtr strPtr = NativeBindings.pl_datatype_to_string(handle.DangerousGetHandle());
        
        if (strPtr == IntPtr.Zero) return "unknown";

        // 2. 读取字符串并清理 (字符串本身的内存还是要释放的)
        try
        {
            return Marshal.PtrToStringUTF8(strPtr) ?? "unknown";
        }
        finally
        {
            // 释放 Rust 分配的字符串内存 (CString)
            NativeBindings.pl_free_string(strPtr);
        }
    }

    /// <summary>
    /// 获取 TimeZone 字符串。
    /// 严格遵循: Rust Alloc -> C# Copy -> Rust Free
    /// </summary>
    public static string? GetTimeZone(DataTypeHandle handle)
    {
        // 既然是 Get 属性，其实不需要 TransferOwnership (Clone)，
        // 直接传 handle.DangerousGetHandle() 给 Rust Borrow 即可。
        // 这样性能最好，且只要 Handle 没死，指针就安全。
        
        IntPtr ptr = IntPtr.Zero;
        try
        {
            // 这里传入 handle 会自动调用 handle.DangerousGetHandle()
            // 如果你定义 Parameter 是 IntPtr，LibraryImport 生成的代码会处理
            ptr = NativeBindings.pl_datatype_get_timezone(handle.DangerousGetHandle());
            
            if (ptr == IntPtr.Zero) return null;
            
            // UTF8 处理
            return Marshal.PtrToStringUTF8(ptr);
        }
        finally
        {
            if (ptr != IntPtr.Zero)
            {
                NativeBindings.pl_free_string(ptr);
            }
        }
    }
    /// <summary>
    /// 获取 DataType 的 Kind。
    /// <para>策略: Borrow (借用)。直接读取指针，不转移所有权。</para>
    /// </summary>
    public static int GetDataTypeKind(DataTypeHandle handle)
    {
        // 只要 handle 没有被 Dispose，DangerousGetHandle 就是安全的
        return NativeBindings.pl_datatype_get_kind(handle.DangerousGetHandle());
    }

    /// <summary>
    /// 获取时间单位。
    /// <para>策略: Borrow (借用)。</para>
    /// </summary>
    public static int GetTimeUnit(DataTypeHandle handle)
    {
        return NativeBindings.pl_datatype_get_time_unit(handle.DangerousGetHandle());
    }

    /// <summary>
    /// 获取 Decimal 的精度和刻度。
    /// <para>策略: Borrow (借用)。</para>
    /// </summary>
    public static void GetDecimalInfo(DataTypeHandle handle, out int precision, out int scale)
    {
        NativeBindings.pl_datatype_get_decimal_info(handle.DangerousGetHandle(), out precision, out scale);
    }
    // ==========================================
    // DataType Introspection Wrappers
    // ==========================================

    /// <summary>
    /// 获取 List 类型的内部元素类型 Handle。
    /// </summary>
    public static DataTypeHandle GetListInnerType(DataTypeHandle handle)
    {
        // 这里的 NativeBindings.pl_datatype_get_inner 返回的是一个新的 Handle (Clone)
        // 使用 DangerousGetHandle 借用传入的 handle
        return ErrorHelper.Check(NativeBindings.pl_datatype_get_inner(handle.DangerousGetHandle()));
    }

    /// <summary>
    /// 获取 Struct 类型的字段数量。
    /// </summary>
    public static ulong GetStructLen(DataTypeHandle handle)
    {
        return (ulong)NativeBindings.pl_datatype_get_struct_len(handle.DangerousGetHandle());
    }

    /// <summary>
    /// 获取 Struct 类型指定索引的字段信息。
    /// </summary>
    public static void GetStructField(DataTypeHandle handle, ulong index, out string name, out DataTypeHandle typeHandle)
    {
        NativeBindings.pl_datatype_get_struct_field(
            handle.DangerousGetHandle(), 
            (UIntPtr)index, 
            out IntPtr namePtr, 
            out var outTypeHandle
        );

        // 检查 Handle 有效性
        typeHandle = ErrorHelper.Check(outTypeHandle);

        // 处理字符串 (Copy & Free)
        try
        {
            name = Marshal.PtrToStringUTF8(namePtr) ?? "";
        }
        finally
        {
            NativeBindings.pl_free_string(namePtr);
        }
    }
}