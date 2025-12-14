using System;
using System.Runtime.InteropServices;

namespace Polars.Native;

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
        ErrorHelper.CheckVoid(); // 检查是否有 Rust 错误
        return string.Empty; 
    }
    try { return Marshal.PtrToStringUTF8(ptr) ?? ""; }
    finally { NativeBindings.pl_free_string(ptr); }
    }
}