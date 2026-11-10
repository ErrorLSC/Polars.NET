using Kaminari.Seseragi.Lakehouse.Delta;

namespace Kaminari.Seseragi.Lakehouse.Engine;

/// <summary>
/// Wraps the storage of one write operation and remembers the files it creates (data, change data, deletion
/// vectors), so that when the operation fails before its commit lands they can be deleted again instead of being
/// left for a full vacuum.
/// </summary>
/// <remarks>
/// Deleting is only safe when the commit certainly did not happen. That is the case when the operation failed
/// before trying to write its commit, or when the commit was refused (put-if-absent said the version exists, up to
/// a <see cref="DeltaCommitConflictException"/>). When writing the commit file itself failed (a timeout, a broken
/// connection), the store may still have taken it: the files are kept.
/// </remarks>
public sealed class TrackingFileSystem(IWritableFileSystem inner) : IWritableFileSystem, IDisposable
{
    /// <summary>Disposes the wrapped storage, which this wrapper owns.</summary>
    public void Dispose() => (Inner as IDisposable)?.Dispose();

    private readonly List<string> _created = [];
    private bool _committed;
    private bool _commitOutcomeUnknown;

    /// <summary>The wrapped storage.</summary>
    public IWritableFileSystem Inner { get; } = inner ?? throw new ArgumentNullException(nameof(inner));

    /// <summary>The files created so far, outside the log.</summary>
    public IReadOnlyList<string> Created => _created;

    /// <summary>
    /// Deletes the files this operation created, when its commit certainly did not land. Best effort: a file that
    /// cannot be deleted is left. Returns the files deleted.
    /// </summary>
    public IReadOnlyList<string> Rollback()
    {
        if (_committed || _commitOutcomeUnknown) return [];
        var deleted = new List<string>();
        foreach (string path in _created)
        {
            try
            {
                if (Inner.Delete(path)) deleted.Add(path);
            }
            catch (Exception e) // best effort: the operation is failing already, its own exception goes on
            {
                System.Diagnostics.Trace.TraceWarning($"Could not delete {path} after a failed write: {e.Message}");
            }
        }
        _created.Clear();
        return deleted;
    }

    /// <inheritdoc/>
    public bool TryCreate(string path, ReadOnlyMemory<byte> content)
    {
        if (!IsCommit(path))
        {
            bool created = Inner.TryCreate(path, content);
            if (created && !path.StartsWith("_delta_log/", StringComparison.Ordinal)) _created.Add(path);
            return created;
        }
        try
        {
            bool landed = Inner.TryCreate(path, content);
            _committed |= landed;
            return landed;
        }
        catch
        {
            _commitOutcomeUnknown = true;
            throw;
        }
    }

    private static bool IsCommit(string path) =>
        path.StartsWith("_delta_log/", StringComparison.Ordinal) && path.EndsWith(".json", StringComparison.Ordinal)
        && path.Length == "_delta_log/".Length + 25 && path.AsSpan("_delta_log/".Length, 20).IndexOfAnyExceptInRange('0', '9') < 0;

    /// <inheritdoc/>
    public void Overwrite(string path, ReadOnlyMemory<byte> content) => Inner.Overwrite(path, content);
    /// <inheritdoc/>
    public bool Delete(string path) => Inner.Delete(path);
    /// <inheritdoc/>
    public IReadOnlyList<FileEntry> ListAll(string directory) => Inner.ListAll(directory);
    /// <inheritdoc/>
    public IReadOnlyList<string> ListDirectories(string directory) => Inner.ListDirectories(directory);
    /// <inheritdoc/>
    public bool DeleteDirectoryIfEmpty(string directory) => Inner.DeleteDirectoryIfEmpty(directory);
    /// <inheritdoc/>
    public IReadOnlyList<FileEntry> List(string directory, string? startFrom = null) => Inner.List(directory, startFrom);
    /// <inheritdoc/>
    public byte[]? ReadAll(string path) => Inner.ReadAll(path);
    /// <inheritdoc/>
    public byte[]? ReadRange(string path, long offset, int length) => Inner.ReadRange(path, offset, length);
}
