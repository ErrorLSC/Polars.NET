using System;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Polars.NET.Core;

// 1. 基类：实现通用的所有权转移逻辑
public abstract class PolarsHandle : SafeHandle
{
    protected PolarsHandle() : base(IntPtr.Zero, true) { }

    public override bool IsInvalid => handle == IntPtr.Zero;

    // [通用逻辑] 转移所有权 (SuppressRelease 的语义)
    // 调用此方法意味着：Rust 已经接管了这块内存，C# 不再负责释放。
    public IntPtr TransferOwnership()
    {
        IntPtr ptr = handle;
        this.SetHandleAsInvalid(); // 标记无效，阻止 GC 调用 ReleaseHandle
        return ptr;
    }
}

// 2. Expr Handle
public class ExprHandle : PolarsHandle
{
    protected override bool ReleaseHandle()
    {
        // 只有当 TransferOwnership 没被调用时（即 C# 这边用完了但没传给 Rust），才会走到这里
        NativeBindings.pl_expr_free(handle);
        return true;
    }
}

// 3. DataFrame Handle
public class DataFrameHandle : PolarsHandle
{
    protected override bool ReleaseHandle()
    {
        NativeBindings.pl_dataframe_free(handle);
        return true;
    }
}

// 4. LazyFrame Handle
public class LazyFrameHandle : PolarsHandle
{
    protected override bool ReleaseHandle()
    {
        // [修复] 必须调用专门的 LazyFrame 释放函数
        NativeBindings.pl_lazy_frame_free(handle);
        return true;
    }
}

// 5. Selector Handle
public class SelectorHandle : PolarsHandle
{
    protected override bool ReleaseHandle()
    {
        // [修复] 必须调用专门的 Selector 释放函数
        NativeBindings.pl_selector_free(handle);
        return true;
    }
}

public class SqlContextHandle : PolarsHandle
{
    protected override bool ReleaseHandle()
    {
        NativeBindings.pl_sql_context_free(handle);
        return true;
    }
}

public class SeriesHandle : PolarsHandle
{
    // 必须提供无参构造函数给 P/Invoke 使用
    public SeriesHandle() : base() { }

    protected override bool ReleaseHandle()
    {
        // 只有当句柄有效时才释放
        if (!IsInvalid)
        {
            NativeBindings.pl_series_free(handle);
        }
        return true;
    }
}

// [修复] 继承自 PolarsHandle
public class ArrowArrayContextHandle : PolarsHandle
{
    public ArrowArrayContextHandle() : base() { }

    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            NativeBindings.pl_arrow_array_free(handle);
        }
        return true;
    }
}

public class DataTypeHandle : PolarsHandle
{
    public DataTypeHandle() : base() { }
    protected override bool ReleaseHandle()
    {
        NativeBindings.pl_datatype_free(handle);
        return true;
    }
}