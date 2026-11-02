using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Text;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

[Serializable]
public class PolarsException : Exception
{
    public PolarsException(string message) : base(message) { }
    public PolarsException(string message, Exception inner) : base(message, inner) { }

    public PolarsException() : base()
    {
    }
}
internal static class ErrorHelper
{
    // =========================================================================
    // 0.Raw Pointer Check
    // =========================================================================
    public static nint Check(nint ptr)
    {
        if (ptr == nint.Zero)
        {
            ThrowRustError();
        }
        return ptr;
    }
    // =========================================================================
    // 1. Handle Check
    // =========================================================================
    public static T Check<T>(T handle) where T : PolarsHandle
    {
        if (!handle.IsInvalid)
        {
            return handle;
        }

        ThrowRustError();

        return null!;
    }

    // =========================================================================
    // 2. Void Check
    // =========================================================================
    public static void CheckVoid()
    {
        IntPtr msgPtr = NativeBindings.pl_get_last_error();

        if (msgPtr != IntPtr.Zero)
        {
            HandleErrorPtrAndThrow(msgPtr);
        }
    }

    // =========================================================================
    // 3. String Check
    // =========================================================================
    public static string CheckString(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            IntPtr errPtr = NativeBindings.pl_get_last_error();
            if (errPtr != IntPtr.Zero)
            {
                HandleErrorPtrAndThrow(errPtr);
            }

            return string.Empty;
        }

        try
        {
            return Marshal.PtrToStringUTF8(ptr) ?? string.Empty;
        }
        finally
        {
            NativeBindings.pl_free_string(ptr);
        }
    }

    // =========================================================================
    // 3.1 UTF-8 Byte Stream Check
    // =========================================================================
    public static string CheckUtf8String(nint ptr, nuint len)
    {
        if (ptr == nint.Zero || len == 0)
        {
            if (ptr != nint.Zero)
            {
                NativeBindings.pl_free_utf8_bytes(ptr, len);
            }
            return string.Empty;
        }

        if (len > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(len), "UTF-8 byte stream is too large for .NET string.");
        }

        try
        {
            unsafe
            {
                var utf8Span = new ReadOnlySpan<byte>((void*)ptr, (int)len);
                int charCount = Encoding.UTF8.GetCharCount(utf8Span);

                // Allocate UTF-16 chars on stack for common lengths to avoid intermediate heap allocation
                Span<char> charBuffer = charCount <= 512
                    ? stackalloc char[charCount]
                    : new char[charCount];

                int actualChars = Encoding.UTF8.GetChars(utf8Span, charBuffer);
                return new string(charBuffer[..actualChars]);
            }
        }
        finally
        {
            NativeBindings.pl_free_utf8_bytes(ptr, len);
        }
    }
    // =========================================================================
    // 4. Status Code Check (for C ABI int returns)
    // =========================================================================
    public static void CheckStatus(int statusCode)
    {
        if (statusCode == 0)
        {
            return;
        }

        ThrowRustError();
    }

    // =========================================================================
    // 5. Bool Check (for ffi_bool_try! macro)
    // =========================================================================
    public static void CheckBool(bool success)
    {
        if (success)
        {
            return;
        }

        ThrowRustError();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    [DoesNotReturn]
    private static void ThrowRustError()
    {
        IntPtr msgPtr = NativeBindings.pl_get_last_error();
        if (msgPtr == IntPtr.Zero)
        {
            throw new PolarsException("Polars operation failed with an unknown error (Handle is invalid but no error message set).");
        }

        HandleErrorPtrAndThrow(msgPtr);
    }

    [DoesNotReturn]
    private static void HandleErrorPtrAndThrow(IntPtr msgPtr)
    {
        string msg;
        try
        {
            msg = Marshal.PtrToStringUTF8(msgPtr) ?? "Unknown Rust Error (UTF8 Decode Failed)";
        }
        finally
        {
            NativeBindings.pl_free_error_msg(msgPtr);
        }

        throw new PolarsException(msg);
    }
}