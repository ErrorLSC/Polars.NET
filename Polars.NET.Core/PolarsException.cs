using System.Runtime.InteropServices;

namespace Polars.NET.Core;

internal static class ErrorHelper
{
    // 检查 Handle 是否有效，无效则抛出 Rust 异常
    public static T Check<T>(T handle) where T : PolarsHandle
    {
        if (!handle.IsInvalid) return handle;

        // 获取错误消息
        IntPtr msgPtr = NativeBindings.pl_get_last_error();
        if (msgPtr == IntPtr.Zero)
        {
            throw new Exception("Polars operation failed (Unknown Error).");
        }

        try
        {
            string msg = Marshal.PtrToStringUTF8(msgPtr) ?? "Unknown Rust Error";
            throw new Exception($"[Polars Error] {msg}");
        }
        finally
        {
            NativeBindings.pl_free_error_msg(msgPtr);
        }
    }

    // 针对返回 void 的情况
    public static void CheckVoid()
    {
        IntPtr msgPtr = NativeBindings.pl_get_last_error();
        if (msgPtr != IntPtr.Zero)
        {
            try
            {
                string msg = Marshal.PtrToStringUTF8(msgPtr) ?? "Unknown Rust Error";
                throw new Exception($"[Polars Void Error] {msg}");
            }
            finally
            {
                NativeBindings.pl_free_error_msg(msgPtr);
            }
        }
    }
    internal static string CheckString(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero) 
        {
            CheckVoid(); // 检查是否有 Rust 错误
            return string.Empty; 
        }
        try { return Marshal.PtrToStringUTF8(ptr) ?? ""; }
        finally { NativeBindings.pl_free_string(ptr); }
    }
    /// <summary>
    /// 专门用于处理可能包含 null 的字符串数组（用于 Series 数据）。
    /// Null 字符串会被转换为 IntPtr.Zero。
    /// </summary>
    internal static T UseNullableUtf8StringArray<T>(string?[]? arr, Func<IntPtr[], T> action)
    {
        if (arr == null || arr.Length == 0)
        {
            return action(Array.Empty<IntPtr>());
        }

        int len = arr.Length;
        var ptrs = new IntPtr[len];

        try
        {
            for (int i = 0; i < len; i++)
            {
                // [关键逻辑] Null -> Zero, Non-Null -> Alloc
                if (arr[i] == null)
                {
                    ptrs[i] = IntPtr.Zero;
                }
                else
                {
                    ptrs[i] = Marshal.StringToCoTaskMemUTF8(arr[i]);
                }
            }

            return action(ptrs);
        }
        finally
        {
            // 统一清理
            for (int i = 0; i < len; i++)
            {
                if (ptrs[i] != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(ptrs[i]);
                }
            }
        }
    }
}