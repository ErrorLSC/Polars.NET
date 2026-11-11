using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Storage;

/// <summary>
/// A table on an object store, rooted at a location such as <c>s3://bucket/dir/table</c>. Paths are relative to the
/// root; absolute URIs (percent-encoded, as a table log writes them) are read inside or outside the root.
/// </summary>
public sealed class CloudFileSystem : IWritableFileSystem, ITailReadable
{
    private readonly string _rootUrl;
    private readonly string _bucket;
    private readonly string _prefix; // the root's key, without a trailing slash; empty at the bucket's top
    private readonly ObjectStoreClient _client;
    private readonly IReadOnlySet<string> _schemes;
    private readonly Func<string, ObjectStoreClient> _openBucket;
    private readonly Dictionary<string, ObjectStoreClient> _others = new(StringComparer.Ordinal);

    internal CloudFileSystem(
        string rootUrl, string bucket, string prefix, ObjectStoreClient client, IReadOnlySet<string> schemes,
        Func<string, ObjectStoreClient> openBucket)
    {
        _rootUrl = rootUrl.TrimEnd('/');
        _bucket = bucket;
        _prefix = prefix.Trim('/');
        _client = client;
        _schemes = schemes;
        _openBucket = openBucket;
    }

    /// <summary>The root location.</summary>
    public string Root => _rootUrl;

    /// <summary>The client of the root's bucket.</summary>
    internal ObjectStoreClient Client => _client;

    private string Key(string relative)
    {
        relative = relative.Trim('/');
        if (_prefix.Length == 0) return relative;
        return relative.Length == 0 ? _prefix : _prefix + "/" + relative;
    }

    /// <summary>The client and key of a path: relative to the root, or an absolute URI.</summary>
    private (ObjectStoreClient Client, string Key) Resolve(string path)
    {
        int schemeEnd = path.IndexOf("://", StringComparison.Ordinal);
        if (schemeEnd < 0) return (_client, Key(path));
        if (path.StartsWith(_rootUrl + "/", StringComparison.Ordinal))
            return (_client, Key(Uri.UnescapeDataString(path[(_rootUrl.Length + 1)..])));

        string scheme = path[..schemeEnd].ToLowerInvariant();
        if (!_schemes.Contains(scheme))
            throw new NotSupportedException($"The file system of '{_rootUrl}' cannot read '{path}'.");
        string rest = path[(schemeEnd + 3)..];
        int slash = rest.IndexOf('/');
        string bucket = slash < 0 ? rest : rest[..slash];
        string key = slash < 0 ? "" : Uri.UnescapeDataString(rest[(slash + 1)..]);
        if (bucket == _bucket) return (_client, key);
        lock (_others)
        {
            if (!_others.TryGetValue(bucket, out ObjectStoreClient? other))
                _others[bucket] = other = _openBucket(bucket);
            return (other, key);
        }
    }

    private static string DirectoryPrefix(string key) => key.Length == 0 ? "" : key + "/";

    private static string EntryPrefix(string directory)
    {
        directory = directory.Trim('/');
        return directory.Length == 0 ? "" : directory + "/";
    }

    /// <inheritdoc/>
    public IReadOnlyList<FileEntry> List(string directory, string? startFrom = null)
    {
        string prefix = DirectoryPrefix(Key(directory));
        string entryPrefix = EntryPrefix(directory);
        var names = new Dictionary<string, FileEntry>(StringComparer.Ordinal);

        void Add(ObjectInfo info)
        {
            if (!info.Key.StartsWith(prefix, StringComparison.Ordinal)) return;
            string name = info.Key[prefix.Length..];
            if (name.Length == 0 || name.Contains('/')) return; // directory markers and deeper keys
            if (startFrom is not null && string.CompareOrdinal(name, startFrom) < 0) return;
            names[name] = new FileEntry(entryPrefix + name, info.Size, info.LastModified);
        }

        // Listing after the offset skips the start of a long log; the offset itself is excluded by the store, so the
        // file named exactly that is asked for on its own.
        foreach (ObjectInfo info in _client.List(prefix, delimited: true, startFrom is null ? null : prefix + startFrom))
            Add(info);
        if (!string.IsNullOrEmpty(startFrom) && _client.Head(prefix + startFrom) is { } exact)
            Add(exact);

        return Sorted(names.Values);
    }

    /// <inheritdoc/>
    public IReadOnlyList<FileEntry> ListAll(string directory)
    {
        string prefix = DirectoryPrefix(Key(directory));
        string entryPrefix = EntryPrefix(directory);
        var entries = new List<FileEntry>();
        foreach (ObjectInfo info in _client.List(prefix, delimited: false, startAfter: null))
        {
            if (!info.Key.StartsWith(prefix, StringComparison.Ordinal) || info.Key.EndsWith('/')) continue;
            string relative = info.Key[prefix.Length..];
            if (relative.Length > 0) entries.Add(new FileEntry(entryPrefix + relative, info.Size, info.LastModified));
        }
        return Sorted(entries);
    }

    private static List<FileEntry> Sorted(IEnumerable<FileEntry> entries)
    {
        var list = entries.ToList();
        list.Sort(static (a, b) => string.CompareOrdinal(a.Path, b.Path));
        return list;
    }

    /// <inheritdoc/>
    public byte[]? ReadAll(string path)
    {
        var (client, key) = Resolve(path);
        return client.Get(key);
    }

    /// <inheritdoc/>
    public byte[]? ReadRange(string path, long offset, int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        var (client, key) = Resolve(path);
        if (length == 0) return client.Head(key) is null ? null : [];
        return client.GetRange(key, offset, length);
    }

    /// <inheritdoc/>
    public bool CanReadTails => true;

    /// <inheritdoc/>
    public FileTail? ReadTail(string path, int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        var (client, key) = Resolve(path);
        return client.GetTail(key, length) is var (tail, size) ? new FileTail(tail, size) : null;
    }

    /// <inheritdoc/>
    public bool TryCreate(string path, ReadOnlyMemory<byte> content) => _client.PutIfAbsent(Key(path), content);

    /// <inheritdoc/>
    public void Overwrite(string path, ReadOnlyMemory<byte> content) => _client.Put(Key(path), content);

    /// <inheritdoc/>
    public bool Delete(string path)
    {
        // Stores answer a delete of a missing object as a success; ask first so the answer means the same everywhere.
        string key = Key(path);
        if (_client.Head(key) is null) return false;
        _client.Delete(key);
        return true;
    }
}
