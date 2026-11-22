using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.NET.Core.Exceptions;

namespace Polars.NET.Core.Delta;

/// <summary>
/// The store of files given by URLs (<c>s3://bucket/dir/x.json</c>): the bucket's (or container's) files through an
/// <see cref="ObjectStoreFileSystem"/> (the managed clients where they serve the location, else the native store), as
/// datasets read and write them. Local paths need none.
/// </summary>
internal static class ManagedStores
{
    /// <summary>
    /// The store of <paramref name="paths"/> and what to dispose once done with it; both null for local paths.
    /// </summary>
    /// <exception cref="PolarsException">The paths lie in different buckets.</exception>
    public static (FileSystemStore? Store, IDisposable? Owner) Open(string[] paths, CloudArgs cloud)
    {
        if (paths.Length == 0 || !IsUrl(paths[0])) return (null, null);
        string root = AuthorityRoot(paths[0]);
        if (paths.Any(p => !p.StartsWith(root + "/", StringComparison.Ordinal)))
            throw new PolarsException($"The files of one scan lie in different buckets: {root} and others.");
        var fileSystem = new ObjectStoreFileSystem(root, cloud);
        return (new FileSystemStore(fileSystem, root), fileSystem);
    }

    /// <summary>Whether <paramref name="path"/> is a URL (a cloud location) rather than a local path.</summary>
    public static bool IsUrl(string path) => path.Contains("://", StringComparison.Ordinal);

    // The bucket (or container) of a URL: scheme and authority.
    private static string AuthorityRoot(string url)
    {
        int start = url.IndexOf("://", StringComparison.Ordinal) + 3;
        int slash = url.IndexOf('/', start);
        return slash < 0 ? url : url[..slash];
    }
}
