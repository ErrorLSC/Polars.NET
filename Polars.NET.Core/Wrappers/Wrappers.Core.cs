using System.Runtime.InteropServices;

namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    // 辅助：批量转换 Handle
    internal static IntPtr[] HandlesToPtrs(PolarsHandle[] handles)
    {
        if (handles == null || handles.Length == 0) return Array.Empty<IntPtr>();
        
        var ptrs = new IntPtr[handles.Length];
        for (int i = 0; i < handles.Length; i++)
        {
            // 这步操作做了两件事：
            // 1. 获取原始指针传给 Rust
            // 2. 标记 C# Handle 为无效，防止 GC 二次释放
            // (因为 Rust 侧的 Vec<Expr> 已经接管了这些指针的生命周期)
            ptrs[i] = handles[i].TransferOwnership();
        }
        return ptrs;
    }
    /// <summary>
    /// 封装单个字符串的 Marshaling (针对有返回值的场景)
    /// </summary>
    private static T UseUtf8String<T>(string str, Func<IntPtr, T> action)
    {
        IntPtr ptr = Marshal.StringToCoTaskMemUTF8(str);
        try
        {
            return action(ptr);
        }
        finally
        {
            Marshal.FreeCoTaskMem(ptr);
        }
    }

    /// <summary>
    /// 封装单个字符串的 Marshaling (针对 void 场景)
    /// </summary>
    private static void UseUtf8String(string str, Action<IntPtr> action)
    {
        IntPtr ptr = Marshal.StringToCoTaskMemUTF8(str);
        try
        {
            action(ptr);
        }
        finally
        {
            Marshal.FreeCoTaskMem(ptr);
        }
    }

    private static R UseUtf8StringArray<R>(string[] strings, Func<IntPtr[], R> action)
    {
        if (strings == null || strings.Length == 0)
        {
            return action(Array.Empty<IntPtr>());
        }

        var ptrs = new IntPtr[strings.Length];
        try
        {
            // 分配内存
            for (int i = 0; i < strings.Length; i++)
            {
                ptrs[i] = Marshal.StringToCoTaskMemUTF8(strings[i]);
            }

            // 执行操作
            return action(ptrs);
        }
        finally
        {
            // 清理内存
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
    /// <summary>
    /// 安全锁定一组 SafeHandle，并提取其原始指针。
    /// 使用 ref struct 确保零 GC 开销且只能在栈上使用。
    /// </summary>
    /// <typeparam name="T">具体的 SafeHandle 类型</typeparam>
    internal readonly ref struct SafeHandleLock<T> where T : SafeHandle
    {
        private readonly T[] _handles;
        private readonly bool[] _locks;
        
        // [改进] 直接对外提供指针数组，省去调用方再次遍历的开销
        public readonly IntPtr[] Pointers;

        public SafeHandleLock(T[] handles)
        {
            if (handles == null)
            {
                _handles = Array.Empty<T>();
                _locks = Array.Empty<bool>();
                Pointers = Array.Empty<IntPtr>();
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
                    // 1. 尝试锁定
                    handles[i].DangerousAddRef(ref _locks[i]);
                    
                    // 2. 只有锁定成功且未抛出异常，才获取指针
                    // (虽然 DangerousAddRef 如果失败通常抛异常，或者是 bool ref 为 false，双重保险)
                    if (_locks[i])
                    {
                        Pointers[i] = handles[i].DangerousGetHandle();
                    }
                }
                success = true;
            }
            finally
            {
                // [关键改进] 如果构造过程中发生异常（success == false），
                // 必须手动回滚，释放那些已经成功锁定的 Handle
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
                    _locks[i] = false; // 防止多次 Dispose 导致的多次 Release
                }
            }
        }
    }
}