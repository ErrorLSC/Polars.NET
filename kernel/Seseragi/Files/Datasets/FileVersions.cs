using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files;

/// <summary>
/// The version of each file of a dataset as a store first read it (its size and ETag): later opens ask the store for
/// that version, so that a file replaced between reads fails the read ("changed while it was read") instead of being
/// read at another length or mixed with what was read before. Files a store opens without a version (local files,
/// memory) are not kept: they are read as they are. Safe for concurrent use.
/// </summary>
internal sealed class FileVersions(int files)
{
    // Boxed StoredFile values, set once.
    private readonly object?[] _versions = new object?[files];

    /// <summary>The version file <paramref name="index"/> was first opened at, if kept.</summary>
    public StoredFile? Get(int index) => Volatile.Read(ref _versions[index]) is StoredFile version ? version : null;

    /// <summary>
    /// Keeps the version <paramref name="source"/> (file <paramref name="index"/>) was read at, if none is kept yet: once
    /// read (a store learns a version from its first read), and when it knows one.
    /// </summary>
    public void Note(int index, IRandomAccess source)
    {
        if (source is IStoredSource { Stored: var stored } && (stored.ETag is not null || stored.LastModified is not null))
            Interlocked.CompareExchange(ref _versions[index], stored, null);
    }
}
