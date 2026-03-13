using System.Runtime.InteropServices;

namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    // Helper : Transform Handles,used in move ptr to Rust
    internal static IntPtr[] HandlesToPtrs(PolarsHandle[] handles)
    {
        if (handles == null || handles.Length == 0) return Array.Empty<IntPtr>();
        
        var ptrs = new IntPtr[handles.Length];
        for (int i = 0; i < handles.Length; i++)
        {
            // 1. Get original pointer for Rust
            // 2. Set C# Handle as invalid preventing GC double free，
            ptrs[i] = handles[i].TransferOwnership();
        }
        return ptrs;
    }

    private static R UseUtf8StringArray<R>(string[]? strings, Func<IntPtr[], R> action)
    {
        if (strings == null || strings.Length == 0)
        {
            return action(Array.Empty<IntPtr>());
        }

        var ptrs = new IntPtr[strings.Length];
        try
        {
            // Alloc memory
            for (int i = 0; i < strings.Length; i++)
            {
                ptrs[i] = Marshal.StringToCoTaskMemUTF8(strings[i]);
            }

            return action(ptrs);
        }
        finally
        {
            // free memory
            for (int i = 0; i < ptrs.Length; i++)
            {
                if (ptrs[i] != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(ptrs[i]);
                }
            }
        }
    }
    /// <summary>
    /// Lock a set of SafeHandles and get its raw pointer.
    /// Use ref struct for zero GC and stack only.
    /// Used in Rust borrowing ptr from C#.
    /// </summary>
    /// <typeparam name="T"> SafeHandle Type</typeparam>
    internal readonly ref struct SafeHandleLock<T> where T : SafeHandle
    {
        private readonly T[] _handles;
        private readonly bool[] _locks;
        
        public readonly IntPtr[] Pointers;

        public SafeHandleLock(T[]? handles)
        {
            if (handles == null)
            {
                _handles = [];
                _locks = [];
                Pointers = [];
                return;
            }

            _handles = handles;
            int len = handles.Length;
            _locks = new bool[len];
            Pointers = new IntPtr[len];

            bool success = false;
            try
            {
                for (int i = 0; i < len; i++)
                {
                    // 1. Add ref for handles
                    handles[i].DangerousAddRef(ref _locks[i]);
                    
                    // 2. Only if locked successfully then get its raw pointer
                    if (_locks[i])
                    {
                        Pointers[i] = handles[i].DangerousGetHandle();
                    }
                }
                success = true;
            }
            finally
            {
                // If exception occured, we have to release handles which have been locked.
                if (!success)
                {
                    Dispose();
                }
            }
        }

        public void Dispose()
        {
            if (_handles == null) return;

            for (int i = 0; i < _handles.Length; i++)
            {
                if (_locks[i])
                {
                    _handles[i].DangerousRelease();
                    _locks[i] = false; 
                }
            }
        }
    }
}

internal readonly ref struct SafeHandleSpanLock<T> where T : SafeHandle
{
    private readonly ReadOnlySpan<T> _handles;
    private readonly Span<bool> _locks;

    public SafeHandleSpanLock(ReadOnlySpan<T> handles, Span<IntPtr> pointers, Span<bool> locks)
    {
        _handles = handles;
        _locks = locks;
        locks.Clear(); 

        bool success = false;
        try
        {
            for (int i = 0; i < handles.Length; i++)
            {
                handles[i].DangerousAddRef(ref locks[i]);
                
                if (locks[i])
                {
                    pointers[i] = handles[i].DangerousGetHandle();
                }
            }
            success = true;
        }
        finally
        {
            if (!success)
            {
                Dispose();
            }
        }
    }

    public void Dispose()
    {
        for (int i = 0; i < _handles.Length; i++)
        {
            if (_locks[i])
            {
                _handles[i].DangerousRelease();
                _locks[i] = false;
            }
        }
    }
}