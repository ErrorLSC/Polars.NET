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
        // Rust 会 Box::from_raw(ptrToConsume) -> 使用 -> Drop
        IntPtr strPtr = NativeBindings.pl_datatype_to_string(handle);
        handle.TransferOwnership();
        // 4. 读取字符串并清理
        try
        {
            return Marshal.PtrToStringUTF8(strPtr) ?? "unknown";
        }
        finally
        {
            // 必须调用 Rust 的 free 来释放字符串内存
            // 千万不能用 Marshal.FreeCoTaskMem，否则立马崩
            NativeBindings.pl_free_string(strPtr);
        }
    }
}