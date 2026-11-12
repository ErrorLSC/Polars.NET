using System.Text;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Delta.Log;

namespace Seseragi.Lakehouse.Tests;

/// <summary>A table held in memory, built commit by commit.</summary>
internal sealed class MemoryFileSystem : IWritableFileSystem
{
    private readonly SortedDictionary<string, (byte[] Content, DateTimeOffset Modified)> _files = new(StringComparer.Ordinal);

    public MemoryFileSystem Put(string path, string content, DateTimeOffset? modified = null)
    {
        _files[path] = (Encoding.UTF8.GetBytes(content), modified ?? DateTimeOffset.UnixEpoch);
        return this;
    }

    public MemoryFileSystem Commit(long version, params string[] actionLines) =>
        Put(LogFile.CommitPath(version), string.Join("\n", actionLines), DateTimeOffset.UnixEpoch.AddSeconds(version));

    public IReadOnlyList<FileEntry> List(string directory, string? startFrom = null)
    {
        lock (_files) return ListLocked(directory, startFrom);
    }

    private List<FileEntry> ListLocked(string directory, string? startFrom)
    {
        string prefix = directory.Length == 0 ? "" : directory.TrimEnd('/') + "/";
        var result = new List<FileEntry>();
        foreach (var (path, file) in _files)
        {
            if (!path.StartsWith(prefix, StringComparison.Ordinal)) continue;
            string name = path[prefix.Length..];
            if (name.Contains('/')) continue;
            if (startFrom is not null && string.CompareOrdinal(name, startFrom) < 0) continue;
            result.Add(new FileEntry(path, file.Content.Length, file.Modified));
        }
        return result;
    }

    public bool TryCreate(string path, ReadOnlyMemory<byte> content)
    {
        lock (_files)
        {
            if (_files.ContainsKey(path)) return false;
            _files[path] = (content.ToArray(), DateTimeOffset.UtcNow);
            return true;
        }
    }

    public IReadOnlyList<FileEntry> ListAll(string directory)
    {
        string prefix = directory.Length == 0 ? "" : directory.TrimEnd('/') + "/";
        lock (_files)
            return [.. _files.Where(f => f.Key.StartsWith(prefix, StringComparison.Ordinal)).Select(f => new FileEntry(f.Key, f.Value.Content.Length, f.Value.Modified))];
    }

    public bool Delete(string path)
    {
        lock (_files) return _files.Remove(path);
    }

    public void Overwrite(string path, ReadOnlyMemory<byte> content)
    {
        lock (_files) _files[path] = (content.ToArray(), DateTimeOffset.UtcNow);
    }

    /// <summary>Sets the modification time of a file.</summary>
    public MemoryFileSystem Touch(string path, DateTimeOffset modified)
    {
        lock (_files) _files[path] = (_files[path].Content, modified);
        return this;
    }

    public MemoryFileSystem PutBytes(string path, byte[] content)
    {
        _files[path] = (content, DateTimeOffset.UnixEpoch);
        return this;
    }

    /// <summary>The paths read whole, in order.</summary>
    public List<string> Reads { get; } = [];

    public byte[]? ReadAll(string path)
    {
        lock (_files)
        {
            Reads.Add(path);
            return _files.TryGetValue(path, out var file) ? file.Content : null;
        }
    }

    public byte[]? ReadRange(string path, long offset, int length)
    {
        if (!_files.TryGetValue(path, out var file)) return null;
        if (offset >= file.Content.Length) return [];
        return file.Content.AsSpan((int)offset, (int)Math.Min(length, file.Content.Length - offset)).ToArray();
    }
}

internal static class Lines
{
    public const string Protocol = """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}""";

    public static string Metadata(string partitionColumns = "", string configuration = "") =>
        """{"metaData":{"id":"t1","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[__PC__],"configuration":{__CFG__},"createdTime":1}}"""
            .Replace("__PC__", partitionColumns).Replace("__CFG__", configuration);

    public static string Add(string path, string? dv = null) =>
        "{\"add\":{\"path\":\"" + path + "\",\"partitionValues\":{},\"size\":10,\"modificationTime\":5,\"dataChange\":true" + Dv(dv) + "}}";

    public static string Remove(string path, string? dv = null) =>
        "{\"remove\":{\"path\":\"" + path + "\",\"deletionTimestamp\":9,\"dataChange\":true" + Dv(dv) + "}}";

    private static string Dv(string? id) => id is null
        ? ""
        : ",\"deletionVector\":{\"storageType\":\"u\",\"pathOrInlineDv\":\"" + id + "\",\"offset\":1,\"sizeInBytes\":34,\"cardinality\":2}";
}
