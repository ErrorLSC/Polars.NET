using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>A commit the managing catalog ratified, as it names it: a staged file under <c>_delta_log/_staged_commits</c>.</summary>
/// <param name="Version">The version the commit became.</param>
/// <param name="FileName">The staged file's name (<c>&lt;version&gt;.&lt;uuid&gt;.json</c>).</param>
/// <param name="Size">The staged file's size in bytes.</param>
/// <param name="ModificationTime">When the staged file was written.</param>
public sealed record RatifiedCommit(long Version, string FileName, long Size, DateTimeOffset ModificationTime);

/// <summary>What the managing catalog says about a table's log.</summary>
/// <param name="LatestVersion">The table's latest version: published commit files after it are not (yet) the table's.</param>
/// <param name="Commits">Ratified commits the catalog still holds, published or not, in any order.</param>
public sealed record CatalogLog(long LatestVersion, IReadOnlyList<RatifiedCommit> Commits);

/// <summary>A commit offered to the managing catalog: written as a staged file, waiting to be ratified.</summary>
/// <param name="Version">The version it is to become.</param>
/// <param name="FileName">The staged file's name.</param>
/// <param name="Size">The staged file's size in bytes.</param>
/// <param name="ModificationTime">When the staged file was written.</param>
/// <param name="Timestamp">The commit's in-commit timestamp, in milliseconds since the epoch.</param>
/// <param name="LatestPublishedVersion">The newest version published to <c>_delta_log</c>, for the catalog's bookkeeping.</param>
/// <param name="ReadProtocol">The protocol the commit builds on.</param>
/// <param name="ReadMetadata">The metadata the commit builds on.</param>
/// <param name="Protocol">The protocol the commit sets; null when it keeps the old one.</param>
/// <param name="Metadata">The metadata the commit sets; null when it keeps the old one.</param>
/// <param name="Domains">The domain metadata the commit sets or removes.</param>
public sealed record CatalogCommit(
    long Version, string FileName, long Size, DateTimeOffset ModificationTime, long Timestamp, long LatestPublishedVersion,
    ProtocolAction ReadProtocol, MetadataAction ReadMetadata, ProtocolAction? Protocol, MetadataAction? Metadata,
    IReadOnlyList<DomainMetadataAction> Domains);

/// <summary>What became of a commit offered to the catalog, when that is known.</summary>
public enum CatalogCommitOutcome
{
    /// <summary>The catalog ratified it: the version is this commit's.</summary>
    Ratified,
    /// <summary>Another writer's commit has the version: this one did not happen.</summary>
    Conflict,
}

/// <summary>
/// The catalog refused the commit for a reason other than a concurrent writer (a rule of the catalog, a bad request):
/// it certainly did not happen.
/// </summary>
public sealed class CatalogCommitRejectedException(string message, Exception? inner = null) : DeltaException(message, inner);

/// <summary>
/// Whether the catalog ratified the commit is not known (the connection broke after the request was sent, and asking
/// again did not settle it). The commit may have happened: nothing it wrote may be removed.
/// </summary>
public sealed class CatalogCommitOutcomeUnknownException(string message, Exception? inner = null) : DeltaException(message, inner);

/// <summary>
/// The catalog that manages a table (the <c>catalogManaged</c> table feature): it says which commits make up the
/// table, and it ratifies new ones. A table's storage brings it along (<see cref="ICatalogManagedStorage"/>).
/// </summary>
public interface ICatalogCommitter
{
    /// <summary>Asks the catalog for the table's latest version and the ratified commits it holds.</summary>
    CatalogLog GetLog();

    /// <summary>Offers a staged commit to the catalog.</summary>
    /// <exception cref="CatalogCommitRejectedException">The catalog refused it; it did not happen.</exception>
    /// <exception cref="CatalogCommitOutcomeUnknownException">Whether it happened is not known.</exception>
    CatalogCommitOutcome Ratify(CatalogCommit commit);
}

/// <summary>Storage of a table whose catalog manages it: the kernel reads and commits through <see cref="Catalog"/>.</summary>
public interface ICatalogManagedStorage
{
    /// <summary>The table's managing catalog; null when the storage reaches the table by path only.</summary>
    ICatalogCommitter? Catalog { get; }
}

/// <summary>
/// A table's storage together with the catalog that manages the table. Files are read and written as by the wrapped
/// storage; log files are never deleted (log cleanup is the catalog's business).
/// </summary>
public sealed class CatalogManagedFileSystem(IWritableFileSystem inner, ICatalogCommitter catalog)
    : IWritableFileSystem, ITailReadable, ICatalogManagedStorage, IDisposable
{
    /// <summary>The wrapped storage.</summary>
    public IWritableFileSystem Inner { get; } = inner ?? throw new ArgumentNullException(nameof(inner));

    /// <inheritdoc/>
    public ICatalogCommitter? Catalog { get; } = catalog ?? throw new ArgumentNullException(nameof(catalog));

    /// <summary>Disposes the wrapped storage, which this wrapper owns.</summary>
    public void Dispose() => (Inner as IDisposable)?.Dispose();

    /// <inheritdoc/>
    public bool TryCreate(string path, ReadOnlyMemory<byte> content) => Inner.TryCreate(path, content);
    /// <inheritdoc/>
    public void Overwrite(string path, ReadOnlyMemory<byte> content) => Inner.Overwrite(path, content);
    /// <inheritdoc/>
    public bool Delete(string path) =>
        path.StartsWith(LogFile.LogDirectory + "/", StringComparison.Ordinal)
            ? throw new DeltaProtocolException($"The table is catalog-managed: its log files are not deleted by its writers ({path}).")
            : Inner.Delete(path);
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
    /// <inheritdoc/>
    public bool CanReadTails => Inner is ITailReadable { CanReadTails: true };
    /// <inheritdoc/>
    public FileTail? ReadTail(string path, int length) =>
        Inner is ITailReadable tails ? tails.ReadTail(path, length) : throw new NotSupportedException("The wrapped storage cannot read tails.");
}

/// <summary>Where catalog-managed tables keep their commits, and how the kernel finds a table's catalog.</summary>
public static class CatalogManaged
{
    /// <summary>The table feature.</summary>
    public const string Feature = TableFeatures.CatalogManaged;

    /// <summary>The directory staged commits are written to.</summary>
    public const string StagedDirectory = LogFile.LogDirectory + "/_staged_commits";

    /// <summary>The path of a staged commit file.</summary>
    public static string StagedPath(string fileName) => $"{StagedDirectory}/{fileName}";

    /// <summary>A new staged commit file name for <paramref name="version"/>.</summary>
    public static string NewStagedName(long version) => $"{version:D20}.{Guid.NewGuid()}.json";

    /// <summary>The managing catalog the storage brings along, or null.</summary>
    public static ICatalogCommitter? CatalogOf(IFileSystem fileSystem) => (fileSystem as ICatalogManagedStorage)?.Catalog;

    /// <summary>Whether the protocol has the table managed by a catalog.</summary>
    public static bool IsCatalogManaged(ProtocolAction protocol) =>
        protocol.ReaderFeatures?.Contains(Feature) == true || protocol.WriterFeatures?.Contains(Feature) == true;

    /// <summary>
    /// The log files as the catalog has them: published files after its latest version left out, and the commits it
    /// ratified taking the place of published files of the same version (the catalog's copy is authoritative).
    /// </summary>
    internal static List<LogFile> Apply(List<LogFile> files, CatalogLog log)
    {
        files.RemoveAll(f => f.Version > log.LatestVersion || f is { Kind: LogFileKind.CompactedCommit } && f.EndVersion > log.LatestVersion);
        foreach (RatifiedCommit commit in log.Commits)
        {
            if (commit.Version > log.LatestVersion) continue;
            files.RemoveAll(f => f.Kind == LogFileKind.Commit && f.Version == commit.Version);
            files.Add(new LogFile(LogFileKind.Commit, commit.Version,
                new FileEntry(StagedPath(commit.FileName), commit.Size, commit.ModificationTime)));
        }
        return files;
    }

    /// <summary>
    /// The content of the commit of <paramref name="version"/>: the catalog's staged file when it holds the version,
    /// else the published file; null when neither exists.
    /// </summary>
    internal static byte[]? ReadCommit(IFileSystem fileSystem, CatalogLog? log, long version)
    {
        if (log is not null)
        {
            if (version > log.LatestVersion) return null;
            foreach (RatifiedCommit commit in log.Commits)
            {
                if (commit.Version == version) return fileSystem.ReadAll(StagedPath(commit.FileName));
            }
        }
        return fileSystem.ReadAll(LogFile.CommitPath(version));
    }
}
