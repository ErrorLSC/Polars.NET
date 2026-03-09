using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

[Serializable]
public class PolarsException : Exception
{
    public PolarsException(string message) : base(message) { }
    public PolarsException(string message, Exception inner) : base(message, inner) { }
}
internal static class ErrorHelper
{
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