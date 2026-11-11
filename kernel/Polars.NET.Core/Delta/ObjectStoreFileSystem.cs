using System.Globalization;
using System.Text;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Storage;
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

    /// <summary>
    /// Settings for a location given only configuration keys: the provider follows from the URL's scheme (s3, az /
    /// abfs, gs, http, hf); a local path gets none.
    /// </summary>
    public static CloudArgs ForLocation(string location, string[]? keys, string[]? values) =>
        new CloudArgs(Keys: keys, Values: values).WithProviderFrom(location);

    /// <summary>These settings, with the provider taken from the location's URL scheme when none is set.</summary>
    public CloudArgs WithProviderFrom(string location)
    {
        ArgumentNullException.ThrowIfNull(location);
        if (Provider != PlCloudProvider.None) return this;
        int colon = location.IndexOf("://", StringComparison.Ordinal);
        string scheme = colon > 0 ? location[..colon].ToLowerInvariant() : "";
        PlCloudProvider provider = scheme switch
        {
            "s3" or "s3a" => PlCloudProvider.Aws,
            "az" or "azure" or "abfs" or "abfss" or "adl" => PlCloudProvider.Azure,
            "gs" or "gcs" => PlCloudProvider.Gcp,
            "http" or "https" => PlCloudProvider.Http,
            "hf" => PlCloudProvider.HuggingFace,
            _ => PlCloudProvider.None,
        };
        return this with { Provider = provider };
    }
}

/// <summary>Which object store implementation serves the Delta kernel's file access.</summary>
public enum ObjectStoreBackend
{
    /// <summary>The managed clients where they serve the location and settings, else the native store.</summary>
    Auto,

    /// <summary>The managed clients only; a location they cannot serve is an error (the reason is in the message).</summary>
    Managed,

    /// <summary>The native store only.</summary>
    Native,
}

/// <summary>
/// A table on an object store (or a local path). S3 locations are served by the managed clients
/// (<see cref="CloudStorage"/>) when they implement the given settings; anything else goes through the native store
/// the engine itself uses, with the same credentials and retry settings.
/// </summary>
public sealed class ObjectStoreFileSystem : IWritableFileSystem, ITailReadable, IDisposable
{
    /// <summary>The environment variable choosing the backend: <c>auto</c> (default), <c>managed</c> or <c>native</c>.</summary>
    public const string BackendVariable = "POLARS_NET_OBJECT_STORE";

    private readonly CloudFileSystem? _managed;
    private readonly StorageHandle? _handle;
    private readonly string _root;
    private readonly CloudArgs _cloud;
    // Native stores opened for absolute URIs outside the table, by the directory of the file.
    private readonly Dictionary<string, ObjectStoreFileSystem> _outside = new(StringComparer.Ordinal);

    /// <summary>The backend <see cref="BackendVariable"/> selects.</summary>
    public static ObjectStoreBackend ConfiguredBackend =>
        Environment.GetEnvironmentVariable(BackendVariable)?.Trim().ToLowerInvariant() switch
        {
            "managed" => ObjectStoreBackend.Managed,
            "native" => ObjectStoreBackend.Native,
            _ => ObjectStoreBackend.Auto,
        };

    /// <summary>Opens the store behind a table location.</summary>
    /// <param name="tableUrl">The table location: a cloud URL or a local path.</param>
    /// <param name="cloud">Cloud settings; null for none.</param>
    /// <param name="backend">The implementation to use; null for <see cref="ConfiguredBackend"/>.</param>
    public ObjectStoreFileSystem(string tableUrl, CloudArgs? cloud = null, ObjectStoreBackend? backend = null)
    {
        cloud ??= CloudArgs.Local;
        _root = tableUrl.TrimEnd('/');
        _cloud = cloud;
        backend ??= ConfiguredBackend;

        if (backend != ObjectStoreBackend.Native)
        {
            Dictionary<string, string> options = Options(cloud);
            RenewableCredentials? renewable = HostCredentials.Find(options);
            options.Remove(HostCredentials.Key);
            _managed = CloudStorage.TryOpen(_root, options, Retry(cloud), out string? reason, renewable);
            if (_managed is not null) return;
            if (backend == ObjectStoreBackend.Managed)
                throw new NotSupportedException($"The managed object store cannot open '{_root}': {reason}.");
        }

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

    /// <summary>The implementation serving this location: <c>managed</c> or <c>native</c>.</summary>
    public string Implementation => _managed is null ? "native" : "managed";

    private static Dictionary<string, string> Options(CloudArgs cloud)
    {
        var options = new Dictionary<string, string>(StringComparer.Ordinal);
        if (cloud.Keys is { } keys && cloud.Values is { } values)
            for (int i = 0; i < Math.Min(keys.Length, values.Length); i++) options[keys[i]] = values[i];
        return options;
    }

    private static RetrySettings Retry(CloudArgs cloud) => new(
        (int)Math.Min(cloud.Retries, int.MaxValue),
        TimeSpan.FromMilliseconds(cloud.RetryTimeoutMs),
        TimeSpan.FromMilliseconds(cloud.RetryInitBackoffMs),
        TimeSpan.FromMilliseconds(cloud.RetryMaxBackoffMs));

    /// <inheritdoc/>
    public IReadOnlyList<FileEntry> List(string directory, string? startFrom = null)
    {
        if (_managed is not null) return _managed.List(directory, startFrom);
        NativeBindings.pl_storage_list(_handle!, directory, startFrom, out FfiBuffer raw);
        byte[] bytes = NativeBuffer.TakeToArray(raw);
        ErrorHelper.CheckVoid();
        return Parse(bytes, directory);
    }

    /// <summary>Entries from the native listing text: <c>path TAB size TAB milliseconds</c> per line, paths relative to <paramref name="directory"/>.</summary>
    private static List<FileEntry> Parse(byte[] bytes, string directory)
    {
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
        // Listing from an offset fetches the file named exactly the offset on its own; Azure's offset listing
        // (startFrom) already includes it, so it would be reported twice.
        int kept = 0;
        for (int i = 0; i < entries.Count; i++)
            if (kept == 0 || entries[i].Path != entries[kept - 1].Path) entries[kept++] = entries[i];
        entries.RemoveRange(kept, entries.Count - kept);
        return entries;
    }

    /// <inheritdoc/>
    public IReadOnlyList<FileEntry> ListAll(string directory)
    {
        if (_managed is not null) return _managed.ListAll(directory);
        NativeBindings.pl_storage_list_all(_handle!, directory, out FfiBuffer raw);
        byte[] bytes = NativeBuffer.TakeToArray(raw);
        ErrorHelper.CheckVoid();
        return Parse(bytes, directory);
    }

    /// <inheritdoc/>
    public byte[]? ReadAll(string path) => _managed is not null ? _managed.ReadAll(path) : Read(path, 0, -1);

    /// <inheritdoc/>
    public bool CanReadTails => true;

    /// <inheritdoc/>
    public FileTail? ReadTail(string path, int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        if (_managed is not null) return _managed.ReadTail(path, length);
        if (Outside(ref path) is { } store) return store.ReadTail(path, length);
        NativeBindings.pl_storage_read_tail(_handle!, path, (ulong)length, out FfiBuffer raw, out ulong size, out bool found);
        byte[] bytes = NativeBuffer.TakeToArray(raw);
        ErrorHelper.CheckVoid();
        return found ? new FileTail(bytes, (long)size) : null;
    }

    /// <summary>
    /// For an absolute URI outside the table, the store opened at the file's directory (with the same settings) and
    /// the file's name; inside the table, null with the path made relative. The path part of a URI is
    /// percent-encoded. Safe from several threads (footers are read in parallel).
    /// </summary>
    private ObjectStoreFileSystem? Outside(ref string path)
    {
        if (!path.Contains("://", StringComparison.Ordinal)) return null;
        if (path.StartsWith(_root + "/", StringComparison.Ordinal))
        {
            path = Uri.UnescapeDataString(path[(_root.Length + 1)..]);
            return null;
        }
        int slash = path.LastIndexOf('/');
        string directory = path[..slash];
        path = Uri.UnescapeDataString(path[(slash + 1)..]);
        lock (_outside)
        {
            if (!_outside.TryGetValue(directory, out ObjectStoreFileSystem? store))
                _outside[directory] = store = new ObjectStoreFileSystem(directory, _cloud, ObjectStoreBackend.Native);
            return store;
        }
    }

    /// <inheritdoc/>
    public byte[]? ReadRange(string path, long offset, int length) =>
        _managed is not null ? _managed.ReadRange(path, offset, length) : Read(path, (ulong)offset, length);

    private byte[]? Read(string path, ulong offset, long length)
    {
        // An absolute URI: inside the table it is read as a relative path; elsewhere through its own store.
        if (Outside(ref path) is { } store) return store.Read(path, offset, length);

        NativeBindings.pl_storage_read(_handle!, path, offset, length, out FfiBuffer raw, out bool found);
        byte[] bytes = NativeBuffer.TakeToArray(raw);
        ErrorHelper.CheckVoid();
        return found ? bytes : null;
    }

    /// <inheritdoc/>
    public bool TryCreate(string path, ReadOnlyMemory<byte> content) =>
        _managed is not null ? _managed.TryCreate(path, content) : Put(path, content, createOnly: true);

    /// <inheritdoc/>
    public void Overwrite(string path, ReadOnlyMemory<byte> content)
    {
        if (_managed is not null) _managed.Overwrite(path, content);
        else Put(path, content, createOnly: false);
    }

    /// <inheritdoc/>
    public bool Delete(string path)
    {
        if (_managed is not null) return _managed.Delete(path);
        NativeBindings.pl_storage_delete(_handle!, path, out bool deleted);
        ErrorHelper.CheckVoid();
        return deleted;
    }

    private unsafe bool Put(string path, ReadOnlyMemory<byte> content, bool createOnly)
    {
        bool created;
        fixed (byte* data = content.Span)
        {
            NativeBindings.pl_storage_put(_handle!, path, data, (nuint)content.Length, createOnly, out created);
        }
        ErrorHelper.CheckVoid();
        return created;
    }

    /// <summary>Closes the store.</summary>
    public void Dispose()
    {
        foreach (ObjectStoreFileSystem store in _outside.Values) store.Dispose();
        _outside.Clear();
        _handle?.Dispose();
    }
}
