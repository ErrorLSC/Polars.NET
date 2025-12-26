namespace Polars.NET.Core;

public static partial class PolarsWrapper
{

    /// <summary>
    /// 从名称和类型创建 Schema (用于 read_csv 的 schema_overrides)。
    /// </summary>
    public static SchemaHandle NewSchema(string[] names, DataTypeHandle[] types)
    {
        if (names.Length != types.Length)
            throw new ArgumentException("Names and Types must have same length");

        var typePtrs = HandlesToPtrs(types);

        return UseUtf8StringArray(names, (namePtrs) => 
        {
            return ErrorHelper.Check(
                NativeBindings.pl_schema_new(namePtrs, typePtrs, (UIntPtr)names.Length)
            );
        });
    }
        /// <summary>
    /// [升级版] 将 Schema 字典转换为临时的 SchemaHandle 供 Native 调用。
    /// 自动处理：SafeHandleLock -> Marshal Strings -> New Schema -> Action -> Dispose Schema
    /// </summary>
    private static T WithSchemaHandle<T>(
        Dictionary<string, DataTypeHandle>? schema, 
        Func<SchemaHandle, T> action)
    {
        // 1. 如果 Schema 为空，直接传无效 Handle (Rust 端判空)
        if (schema == null || schema.Count == 0)
        {
            return action(new SchemaHandle()); // Invalid Handle (IntPtr.Zero)
        }

        // 复用之前的逻辑来准备数组
        var names = schema.Keys.ToArray();
        var handles = schema.Values.ToArray();

        // 2. 锁定 DataTypeHandles 并提取指针
        using var locker = new SafeHandleLock<DataTypeHandle>(handles);
        // locker 是 ref struct 不能被捕获，但 typePtrs 是 IntPtr[] (普通对象)，可以被捕获。
        // 只要 locker 在 UseUtf8StringArray 返回前不被 Dispose，这些指针就是有效的。
        var typePtrs = locker.Pointers;
        
        // 3. 转换字符串数组
        return UseUtf8StringArray(names, namePtrs => 
        {
            // 4. [关键] 调用 Rust 创建 Schema 对象
            // 这个 Handle 是临时的，专为本次 IO 操作创建
            using var schemaHandle = ErrorHelper.Check(
                NativeBindings.pl_schema_new(
                    namePtrs, 
                    typePtrs, 
                    (UIntPtr)names.Length
                )
            );

            // 5. 执行实际的 IO 操作 (传入 SchemaHandle)
            return action(schemaHandle);
        });
    }
}