using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Polars.NET.Core.Delta;

/// <summary>Opens the storage of a table: plain file calls for a local path, the object store for a URL.</summary>
internal static class TableStorage
{
    /// <summary>
    /// The table's storage, and the root its data file paths are relative to. Inside a <see cref="CatalogScope"/> for
    /// the table the storage brings the table's managing catalog along, through which the kernel reads and commits.
    /// </summary>
    public static (IWritableFileSystem Store, string Root) Open(string tablePath, CloudArgs? cloud)
    {
        IWritableFileSystem store;
        string root;
        if (tablePath.Contains("://", StringComparison.Ordinal))
        {
            root = tablePath.TrimEnd('/');
            store = new ObjectStoreFileSystem(root, cloud);
        }
        else
        {
            var local = new LocalFileSystem(tablePath);
            root = local.Root;
            store = local;
        }
        return (CatalogScope.CatalogFor(tablePath) is { } catalog ? new CatalogManagedFileSystem(store, catalog) : store, root);
    }

    /// <summary>The root data file paths are relative to, for storage <see cref="Open"/> gave (wrapped or not).</summary>
    public static string RootOf(IFileSystem fileSystem, string tablePath) => fileSystem switch
    {
        TrackingFileSystem tracking => RootOf(tracking.Inner, tablePath),
        CatalogManagedFileSystem managed => RootOf(managed.Inner, tablePath),
        LocalFileSystem local => local.Root,
        _ => tablePath.TrimEnd('/'),
    };
}

/// <summary>
/// The managing catalog of the table an operation works on, for the duration of the operation: the catalog API
/// enters it, and <see cref="TableStorage.Open"/> brings it along for that table's location. A catalog-managed table
/// opened outside such a scope (by its path) is refused by the kernel.
/// </summary>
internal static class CatalogScope
{
    private static readonly AsyncLocal<(string Location, ICatalogCommitter Catalog)?> Current = new();

    /// <summary>Makes <paramref name="catalog"/> the managing catalog of the table at <paramref name="location"/> until disposed.</summary>
    public static IDisposable Enter(string location, ICatalogCommitter catalog)
    {
        var previous = Current.Value;
        Current.Value = (Normalize(location), catalog);
        return new Exit(previous);
    }

    /// <summary>The managing catalog entered for the table at <paramref name="tablePath"/>, or null.</summary>
    public static ICatalogCommitter? CatalogFor(string tablePath) =>
        Current.Value is var (location, catalog) && location == Normalize(tablePath) ? catalog : null;

    private static string Normalize(string location) => location.TrimEnd('/');

    private sealed class Exit((string, ICatalogCommitter)? previous) : IDisposable
    {
        public void Dispose() => Current.Value = previous;
    }
}
