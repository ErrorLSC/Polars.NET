using System;
using System.Runtime.InteropServices;

namespace Polars.Native;

// 基类：所有 Polars 指针都应该继承它
public abstract class PolarsHandle : SafeHandle
{
    protected PolarsHandle() : base(IntPtr.Zero, true) { }
    public override bool IsInvalid => handle == IntPtr.Zero;
}

public class DataFrameHandle : PolarsHandle
{
    protected override bool ReleaseHandle()
    {
        // 只有 DataFrame 需要显式释放
        NativeBindings.pl_free_dataframe(handle);
        return true;
    }
}

// Expr 和 LazyFrame 通常被 Rust 函数消耗掉 (Consumed)，
// 所以 C# 不需要调用 free，但需要防止用户二次使用。
public class ExprHandle : PolarsHandle
{
    protected override bool ReleaseHandle() => true; 
}

public class LazyFrameHandle : PolarsHandle
{
    protected override bool ReleaseHandle() => true;
}