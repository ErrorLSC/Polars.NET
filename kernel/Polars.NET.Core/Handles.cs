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

public class DataTypeExprHandle : PolarsHandle
{
    protected override bool ReleaseHandle()
    {
        NativeBindings.pl_datatype_expr_free(handle);
        return true;
    }
    internal DataTypeExprHandle(IntPtr ptr) => SetHandle(ptr);

    public DataTypeExprHandle()
    {
    }

}

// 3. DataFrame Handle
public class DataFrameHandle : PolarsHandle
{
    internal DataFrameHandle(IntPtr ptr) => SetHandle(ptr);

    public DataFrameHandle()
    {
    }

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

/// <summary>An opened object store rooted at one location, for listing and reading files.</summary>
public class StorageHandle : PolarsHandle
{
    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            NativeBindings.pl_storage_free(handle);
        }
        return true;
    }
}

/// <summary>
/// A Unity Catalog connection. Unlike the other handles it owns no native memory: it holds the managed client.
/// </summary>
public sealed class CatalogHandle : IDisposable
{
    // The renewable credentials of each table (and access) resolved through this handle, by table id.
    private readonly System.Collections.Concurrent.ConcurrentDictionary<(string TableId, bool Write), ulong> _credentials = new();
    private volatile bool _disposed;

    internal Kaminari.Seseragi.Lakehouse.Catalog.Unity.UnityCatalogClient Client { get; }

    internal CatalogHandle(Kaminari.Seseragi.Lakehouse.Catalog.Unity.UnityCatalogClient client) => Client = client;

    /// <summary>
    /// The id of the renewable credential source of a table, for <see cref="Delta.HostCredentials.Key"/>: one per
    /// table and access, taking the credentials just vended. Renewal asks the catalog again; after this handle is
    /// disposed the credentials held are used until they expire.
    /// </summary>
    internal ulong RenewableCredentials(string tableId, bool write, Kaminari.Seseragi.Lakehouse.Catalog.Unity.TemporaryCredentials vended)
    {
        var current = Vended(vended);
        ulong id = _credentials.GetOrAdd((tableId, write), key =>
            Delta.HostCredentials.Register(new Kaminari.Seseragi.Lakehouse.Storage.RenewableCredentials(() => Fetch(key.TableId, key.Write), current)));
        Delta.HostCredentials.Get(id).Offer(current);
        return id;
    }

    private Kaminari.Seseragi.Lakehouse.Storage.VendedCredentials Fetch(string tableId, bool write)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(CatalogHandle), "The Unity Catalog connection is closed, so the table's credentials cannot be renewed.");
        return Vended(Client.GetTableCredentials(tableId, write));
    }

    private static Kaminari.Seseragi.Lakehouse.Storage.VendedCredentials Vended(Kaminari.Seseragi.Lakehouse.Catalog.Unity.TemporaryCredentials credentials) =>
        new(credentials.ToStorageOptions(), credentials.ExpiresAt);

    public void Dispose()
    {
        _disposed = true;
        Client.Dispose();
    }
}

public class CategoriesHandle : PolarsHandle
{
    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            NativeBindings.pl_categories_free(handle);
        }
        return true;
    }
}

public class FrozenCategoriesHandle : PolarsHandle
{
    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            NativeBindings.pl_frozencategories_free(handle);
        }
        return true;
    }
}

public class CollectBatchesHandle : PolarsHandle
{
    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            NativeBindings.pl_collect_batches_free(handle);
        }
        return true;
    }
}