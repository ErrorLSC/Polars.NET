using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;

namespace Polars.Native;

public static partial class PolarsWrapper
{
    // ==========================================
    // 1. 生命周期管理 (Cleanup)
    // ==========================================
    // 这个函数会被 Rust 在 Drop 时调用
    // MonoPInvokeCallback 特性在某些环境（如 Unity/IL2CPP）是必须的，但在普通 .NET Core 中可选
    // 为了保险起见，保持它是静态的即可。
    // 这个委托用于防止 CleanupTrampoline 被 GC 回收
    private static readonly CleanupCallback s_cleanupDelegate = CleanupTrampoline;

    private static void CleanupTrampoline(IntPtr userData)
    {
        try
        {
            if (userData != IntPtr.Zero)
            {
                // 将 IntPtr 还原为 GCHandle 并释放
                GCHandle handle = GCHandle.FromIntPtr(userData);
                if (handle.IsAllocated)
                {
                    handle.Free();
                }
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[Polars C#] Error freeing UDF handle: {ex}");
        }
    }
    // ==========================================
    // 2. Map API
    // ==========================================
    // 对外的高层 API
    // 用户传入：Func<IArrowArray, IArrowArray> (输入 Arrow 数组，返回 Arrow 数组)
    public static ExprHandle Map(ExprHandle expr, Func<IArrowArray, IArrowArray> func)
    {
        return Map(expr, func, PlDataType.SameAsInput);
    }
    public static ExprHandle Map(ExprHandle expr, Func<IArrowArray, IArrowArray> func, PlDataType outputType)
    {
        // 1. 定义通过 FFI 调用的回调函数
        // 这个函数会被 Rust 调用
    unsafe int Trampoline(CArrowArray* inArr, CArrowSchema* inSch, CArrowArray* outArr, CArrowSchema* outSch, byte* msgBuf)
        {
            try 
            {
                // A. 导入 Rust 传来的数据
                // ImportSchema 会读取 CArrowSchema 并生成 C# 的 Field/Schema 对象
                // ImportArray 需要这个 Schema 才能正确解析数据
                var field = CArrowSchemaImporter.ImportField(inSch); // 注意：这里通常是 ImportField 而不是 Schema，因为我们传的是一列
                var array = CArrowArrayImporter.ImportArray(inArr, field.DataType);

                // B. 执行用户逻辑
                var resultArray = func(array);

                // C. 导出结果给 Rust (关键修改点)
                
                // 1. 导出数据 (CArrowArray)
                CArrowArrayExporter.ExportArray(resultArray, outArr);

                // 2. 导出类型元数据 (CArrowSchema)
                // 我们需要构建一个 Field 对象来描述这个数组（名字为空字符串即可，类型取数组的类型，设为可空）
                var outField = new Field("", resultArray.Data.DataType, true);
                CArrowSchemaExporter.ExportField(outField, outSch);
                return 0;
            }
            catch (Exception ex)
            {
                // 2. 异常处理逻辑
                
                // 将异常信息转为 UTF-8
                string errorMsg = ex.ToString(); // 或者 ex.Message，看你需要多详细
                byte[] bytes = System.Text.Encoding.UTF8.GetBytes(errorMsg);
                
                // 防止缓冲区溢出 (Rust 那边给了 1024 字节)
                int maxLen = 1023; // 留一个给 \0
                int copyLen = Math.Min(bytes.Length, maxLen);
                
                // 写入 Unmanaged 内存
                Marshal.Copy(bytes, 0, (IntPtr)msgBuf, copyLen);
                msgBuf[copyLen] = 0; // Null Terminator

                // 标记 outArr 为 release=null，虽然我们返回 1 后 Rust 不会读它，但这是好习惯
                outArr = default; 
                outSch = default;

                return 1; // Error code
            }
        }

        // 2. 创建委托实例
        unsafe {UdfCallback callback = Trampoline;

        // 2. 分配 GCHandle (Pin 住 callback)
        GCHandle gcHandle = GCHandle.Alloc(callback);

        // 3. 将 GCHandle 转为 IntPtr 传给 Rust
        IntPtr userData = GCHandle.ToIntPtr(gcHandle);

        try
        {
            // 3. 调用 Rust
            // 传入: 表达式, 回调, 输出类型, 清理回调, GCHandle指针
            var h = NativeBindings.pl_expr_map(
                expr,
                callback,
                outputType,
                s_cleanupDelegate,
                userData
            );

            // 成功移交所有权
            expr.TransferOwnership();
            
            return ErrorHelper.Check(h);
        }
        catch
            {
            // 如果调用 Rust 失败（比如 Rust 那边直接 Panic 没返回 Expr），
            // 我们需要手动释放，否则就泄漏了
            if (gcHandle.IsAllocated) gcHandle.Free();
            throw;
            }
        }
    }
}