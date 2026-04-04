using System.Runtime.InteropServices;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

// Base Class
public abstract class PolarsHandle : SafeHandle
{
    protected PolarsHandle() : base(IntPtr.Zero, true) { }

    public override bool IsInvalid => handle == IntPtr.Zero;

    // This means Rust will take care of this piece of memory，C# won't free this。
    public IntPtr TransferOwnership()
    {
        IntPtr ptr = handle;
        SetHandle(IntPtr.Zero);
        return ptr;
    }
}

// 2. Expr Handle
public class ExprHandle : PolarsHandle
{
    protected override bool ReleaseHandle()
    {
        NativeBindings.pl_expr_free(handle);
        return true;
    }
    internal ExprHandle(IntPtr ptr) => SetHandle(ptr);

    public ExprHandle() 
    { 
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
        NativeBindings.pl_lazy_frame_free(handle);
        return true;
    }
}

// 5. Selector Handle
public class SelectorHandle : PolarsHandle
{
    protected override bool ReleaseHandle()
    {
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
    public SeriesHandle() : base() { }

    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            NativeBindings.pl_series_free(handle);
        }
        return true;
    }
}

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
        if (!IsInvalid)
        {
            NativeBindings.pl_datatype_free(handle);
        }
        return true;
    }
}

public class SchemaHandle : PolarsHandle
{
    public SchemaHandle() : base() { }

    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            NativeBindings.pl_schema_free(handle);
        }
        return true;
    }
}

public class CatalogHandle : PolarsHandle
{
    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            NativeBindings.pl_catalog_unity_free(handle);
        }
        return true;
    }
}