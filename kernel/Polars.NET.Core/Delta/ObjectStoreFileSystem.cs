using System.Globalization;
using System.Text;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Native;

namespace Polars.NET.Core.Delta;

/// <summary>The cloud settings a scan and a file system share.</summary>
/// <param name="Provider">The cloud provider, or None for a local path.</param>
/// <param name="Retries">How often a failed request is retried.</param>
/// <param name="RetryTimeoutMs">The time limit of one request with its retries.</param>
/// <param name="RetryInitBackoffMs">The first wait between retries.</param>
/// <param name="RetryMaxBackoffMs">The longest wait between retries.</param>
/// <param name="CacheTtl">How long cached files are kept.</param>
/// <param name="Keys">Configuration keys, such as credentials.</param>
/// <param name="Values">The values of <paramref name="Keys"/>.</param>
public sealed record CloudArgs(
    PlCloudProvider Provider = PlCloudProvider.None,
    nuint Retries = 2,
    ulong RetryTimeoutMs = 10_000,
    ulong RetryInitBackoffMs = 100,
    ulong RetryMaxBackoffMs = 15_000,
    ulong CacheTtl = 0,
    string[]? Keys = null,
    string[]? Values = null)
{
    /// <summary>No cloud: a local path.</summary>
    public static CloudArgs Local { get; } = new();
}

/// <summary>
/// A table on an object store (or a local path), reached through the store the engine itself uses, so the same
/// credentials and retry settings apply to the log as to the data.
/// </summary>
public sealed class ObjectStoreFileSystem : IFileSystem, IDisposable
{
    private readonly StorageHandle _handle;
    private readonly string _root;
    private readonly CloudArgs _cloud;
    // Stores opened for absolute URIs outside the table, by the directory of the file.
    private readonly Dictionary<string, ObjectStoreFileSystem> _outside = new(StringComparer.Ordinal);

    /// <summary>Opens the store behind a table location.</summary>
    /// <param name="tableUrl">The table location: a cloud URL or a local path.</param>
    /// <param name="cloud">Cloud settings; null for none.</param>
    public ObjectStoreFileSystem(string tableUrl, CloudArgs? cloud = null)
    {
        cloud ??= CloudArgs.Local;
        _root = tableUrl.TrimEnd('/');
        _cloud = cloud;
        _handle = ErrorHelper.Check(NativeBindings.pl_storage_open(
            tableUrl,
            cloud.Provider,
            cloud.Retries,
            cloud.RetryTimeoutMs,
            cloud.RetryInitBackoffMs,
            cloud.RetryMaxBackoffMs,
            cloud.CacheTtl,
            cloud.Keys,
            cloud.Values,
            (nuint)(cloud.Keys?.Length ?? 0)));
    }

    /// <inheritdoc/>
    public IReadOnlyList<FileEntry> List(string directory, string? startFrom = null)
    {
        NativeBindings.pl_storage_list(_handle, directory, startFrom, out FfiBuffer raw);
        byte[] bytes = NativeBuffer.TakeToArray(raw);
        ErrorHelper.CheckVoid();

        string prefix = directory.Length == 0 ? "" : directory.TrimEnd('/') + "/";
        var entries = new List<FileEntry>();
        foreach (string line in Encoding.UTF8.GetString(bytes).Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            string[] parts = line.Split('\t');
            entries.Add(new FileEntry(
                prefix + parts[0],
                long.Parse(parts[1], CultureInfo.InvariantCulture),
                DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(parts[2], CultureInfo.InvariantCulture))));
        }
        entries.Sort(static (a, b) => string.CompareOrdinal(a.Path, b.Path));
        return entries;
    }

    /// <inheritdoc/>
    public byte[]? ReadAll(string path) => Read(path, 0, -1);

    /// <inheritdoc/>
    public byte[]? ReadRange(string path, long offset, int length) => Read(path, (ulong)offset, length);

    private byte[]? Read(string path, ulong offset, long length)
    {
        if (path.Contains("://", StringComparison.Ordinal))
        {
            // An absolute URI: inside the table it is read as a relative path; elsewhere through a store opened at
            // the file's directory with the same settings. The path part of a URI is percent-encoded.
            if (path.StartsWith(_root + "/", StringComparison.Ordinal))
            {
                path = Uri.UnescapeDataString(path[(_root.Length + 1)..]);
            }
            else
            {
                int slash = path.LastIndexOf('/');
                string directory = path[..slash];
                if (!_outside.TryGetValue(directory, out ObjectStoreFileSystem? store))
                    _outside[directory] = store = new ObjectStoreFileSystem(directory, _cloud);
                return store.Read(Uri.UnescapeDataString(path[(slash + 1)..]), offset, length);
            }
        }

        NativeBindings.pl_storage_read(_handle, path, offset, length, out FfiBuffer raw, out bool found);
        byte[] bytes = NativeBuffer.TakeToArray(raw);
        ErrorHelper.CheckVoid();
        return found ? bytes : null;
    }

    /// <summary>Closes the store.</summary>
    public void Dispose()
    {
        foreach (ObjectStoreFileSystem store in _outside.Values) store.Dispose();
        _outside.Clear();
        _handle.Dispose();
    }
}
