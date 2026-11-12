using System.Globalization;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>What a restore did.</summary>
/// <param name="Version">The version committed; null when the table already was as the target.</param>
/// <param name="RestoredFiles">Files added back.</param>
/// <param name="RemovedFiles">Files removed.</param>
/// <param name="RestoredBytes">Size of the files added back.</param>
/// <param name="RemovedBytes">Size of the files removed.</param>
/// <param name="MissingFiles">Files of the target that no longer exist (vacuumed) and were left out.</param>
public sealed record RestoreResult(long? Version, int RestoredFiles, int RemovedFiles, long RestoredBytes, long RemovedBytes, IReadOnlyList<string> MissingFiles);

/// <summary>
/// Brings a table back to an earlier version with a new commit: the files of that version come back, the others
/// are removed, and its schema and properties return. History stays: the restore is a version like any other.
/// As Spark, the protocol is not downgraded unless allowed, application transactions are not rolled back, and
/// the change data feed sees the restore as the rows of the files it removes and adds.
/// </summary>
public static class Restore
{
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="current">The latest state of the table.</param>
    /// <param name="target">The state to bring back.</param>
    /// <param name="ignoreMissingFiles">Leave out files of the target that no longer exist, instead of failing.</param>
    /// <param name="protocolDowngradeAllowed">Take the target's protocol even when it is lower than the current one.</param>
    /// <param name="now">The writer's clock; the current time when null.</param>
    /// <exception cref="DeltaException">Files of the target are missing, or the table cannot be written by this kernel.</exception>
    public static RestoreResult Run(
        IWritableFileSystem fileSystem, Snapshot current, Snapshot target, bool ignoreMissingFiles = false,
        bool protocolDowngradeAllowed = false, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(current);
        ArgumentNullException.ThrowIfNull(target);
        if (target.Version > current.Version)
            throw new DeltaException($"Version {target.Version} is after the latest version {current.Version}; there is nothing to restore.");
        if (target.Metadata.Id != current.Metadata.Id)
            throw new DeltaException("The version to restore belongs to another table (its table id differs).");

        static (string, string?) Key(AddAction f) => (f.Path, f.DeletionVector?.UniqueId);
        var currentKeys = current.Files.Select(Key).ToHashSet();
        var targetKeys = target.Files.Select(Key).ToHashSet();
        long clock = (now ?? DateTimeOffset.UtcNow).ToUnixTimeMilliseconds();

        // Files to bring back must still exist: a vacuum may have deleted them.
        var missing = new List<string>();
        var restored = new List<AddAction>();
        foreach (AddAction file in target.Files.Where(f => !currentKeys.Contains(Key(f))))
        {
            string? gone = !Exists(fileSystem, file.DecodedPath) ? file.DecodedPath
                : file.DeletionVector is { StorageType: "u" } dv && !Exists(fileSystem, dv.FilePath()) ? dv.FilePath()
                : null;
            if (gone is null) restored.Add(file);
            else missing.Add(gone);
        }
        if (missing.Count > 0 && !ignoreMissingFiles)
            throw new DeltaException(
                $"{missing.Count} file(s) of version {target.Version} no longer exist (vacuumed?), such as {string.Join(", ", missing.Take(5))}. " +
                "Restore with missing files ignored to bring back the rest.");
        AddAction[] removed = [.. current.Files.Where(f => !targetKeys.Contains(Key(f)))];

        ProtocolAction? protocol = Protocol(current.Protocol, target.Protocol, protocolDowngradeAllowed);
        // The metadata of the target version, except that identity columns keep their high water marks: values
        // generated since must not be handed out again (as Spark restores).
        MetadataAction restoredMetadata = KeepCheckpointProtection(IdentityColumns.KeepHighWaterMarks(target.Metadata, current.Metadata), current.Metadata);
        bool metadataChanges = !SameMetadata(current.Metadata, restoredMetadata);
        if (restored.Count == 0 && removed.Length == 0 && protocol is null && !metadataChanges)
            return new RestoreResult(null, 0, 0, 0, 0, missing);

        var transaction = Transaction.Start(fileSystem, current, "RESTORE");
        // A restore sets the whole table: any concurrent change to it is a conflict.
        transaction.ReadPredicate(null).ReadFiles(current.Files);
        if (protocol is not null) transaction.SetProtocol(protocol);
        if (metadataChanges) transaction.SetMetadata(restoredMetadata);
        foreach (AddAction file in removed)
        {
            transaction.Add(new RemoveAction
            {
                Path = file.Path,
                DeletionTimestamp = clock,
                DataChange = true,
                ExtendedFileMetadata = true,
                PartitionValues = file.PartitionValues,
                Size = file.Size,
                DeletionVector = file.DeletionVector,
                BaseRowId = file.BaseRowId,
                DefaultRowCommitVersion = file.DefaultRowCommitVersion,
            });
        }
        foreach (AddAction file in restored)
        {
            transaction.Add(new AddAction
            {
                Path = file.Path,
                PartitionValues = file.PartitionValues,
                Size = file.Size,
                ModificationTime = file.ModificationTime,
                DataChange = true,
                Stats = file.Stats,
                Tags = file.Tags,
                DeletionVector = file.DeletionVector,
                BaseRowId = file.BaseRowId,
                DefaultRowCommitVersion = file.DefaultRowCommitVersion,
                ClusteringProvider = file.ClusteringProvider,
            });
        }

        long restoredBytes = restored.Sum(f => f.Size), removedBytes = removed.Sum(f => f.Size);
        transaction.WithParameter("version", target.Version.ToString(CultureInfo.InvariantCulture))
                   .WithMetric("numRestoredFiles", restored.Count)
                   .WithMetric("numRemovedFiles", removed.Length)
                   .WithMetric("restoredFilesSize", restoredBytes)
                   .WithMetric("removedFilesSize", removedBytes)
                   .WithMetric("numOfFilesAfterRestore", current.Files.Count - removed.Length + restored.Count)
                   .WithMetric("tableSizeAfterRestore", current.Files.Sum(f => f.Size) - removedBytes + restoredBytes);
        long version = transaction.Commit(now);
        return new RestoreResult(version, restored.Count, removed.Length, restoredBytes, removedBytes, missing);
    }

    /// <summary>The protocol the restore commits, or null to keep the current one.</summary>
    /// <summary>
    /// The restored metadata with the current checkpoint protection boundary kept: the protocol keeps the feature
    /// (features are never removed by a restore), so its boundary must stay too. Spark loses it here.
    /// </summary>
    private static MetadataAction KeepCheckpointProtection(MetadataAction restored, MetadataAction current)
    {
        string key = TableFeatures.RequireCheckpointProtectionBeforeVersionKey;
        if (!current.Configuration.TryGetValue(key, out string? boundary)) return restored;
        if (restored.Configuration.TryGetValue(key, out string? had) && had == boundary) return restored;
        return new MetadataAction
        {
            Id = restored.Id, Name = restored.Name, Description = restored.Description, FormatProvider = restored.FormatProvider,
            FormatOptions = restored.FormatOptions, SchemaString = restored.SchemaString, PartitionColumns = restored.PartitionColumns,
            Configuration = new Dictionary<string, string>(restored.Configuration) { [key] = boundary }, CreatedTime = restored.CreatedTime,
        };
    }

    private static ProtocolAction? Protocol(ProtocolAction current, ProtocolAction target, bool downgradeAllowed)
    {
        if (downgradeAllowed) return SameProtocol(current, target) ? null : target;
        // Never lower: what both support (listing what a legacy version implied when features come in).
        ProtocolAction merged = TableFeatures.Union(current, target);
        return SameProtocol(current, merged) ? null : merged;
    }

    private static bool SameProtocol(ProtocolAction a, ProtocolAction b) =>
        a.MinReaderVersion == b.MinReaderVersion && a.MinWriterVersion == b.MinWriterVersion
        && SameSet(a.ReaderFeatures, b.ReaderFeatures) && SameSet(a.WriterFeatures, b.WriterFeatures);

    private static bool SameSet(IReadOnlyList<string>? a, IReadOnlyList<string>? b) =>
        (a is null) == (b is null) && (a is null || a.ToHashSet(StringComparer.Ordinal).SetEquals(b!));

    private static bool SameMetadata(MetadataAction a, MetadataAction b) =>
        a.SchemaString == b.SchemaString && a.Name == b.Name && a.Description == b.Description
        && a.PartitionColumns.SequenceEqual(b.PartitionColumns)
        && a.Configuration.Count == b.Configuration.Count
        && a.Configuration.All(kv => b.Configuration.TryGetValue(kv.Key, out string? v) && v == kv.Value);

    private static bool Exists(IFileSystem fileSystem, string path) => fileSystem.ReadRange(path, 0, 1) is not null;
}
