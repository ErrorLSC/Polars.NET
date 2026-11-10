using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Delta.Log;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>What a file of the change feed contributes.</summary>
public enum ChangeFileKind
{
    /// <summary>A change data file: its rows carry their own <c>_change_type</c>.</summary>
    Cdc,
    /// <summary>A data file that was added: its live rows are inserts.</summary>
    Insert,
    /// <summary>A data file that was removed: the rows that were live in it are deletes.</summary>
    Delete,
    /// <summary>
    /// A data file that stayed but whose deletion vector changed: the rows newly marked are deletes, and rows no
    /// longer marked are inserts. See <see cref="ChangeFile.ChangedRows"/>.
    /// </summary>
    DeletionVectorChange,
}

/// <summary>One file to read for the change feed, with the commit it belongs to.</summary>
public sealed class ChangeFile
{
    /// <summary>What the file contributes.</summary>
    public required ChangeFileKind Kind { get; init; }
    /// <summary>The path as written in the log.</summary>
    public required string Path { get; init; }
    /// <summary>The partition values of the file, keyed as in the log; null when the log does not give them.</summary>
    public IReadOnlyDictionary<string, string?>? PartitionValues { get; init; }
    /// <summary>
    /// The deletion vector of the file: for an insert or delete, the rows to leave out; for a
    /// deletion vector change, the vector after the commit.
    /// </summary>
    public DeletionVectorDescriptor? DeletionVector { get; init; }
    /// <summary>For a deletion vector change, the vector before the commit; null when the file had none.</summary>
    public DeletionVectorDescriptor? PreviousDeletionVector { get; init; }
    /// <summary>The version of the commit.</summary>
    public required long Version { get; init; }
    /// <summary>The time of the commit.</summary>
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>The path with URL encoding removed.</summary>
    public string DecodedPath => Uri.UnescapeDataString(Path);

    /// <summary>
    /// For a deletion vector change, the row positions that the commit deleted and those it brought back.
    /// </summary>
    public (long[] Deleted, long[] Restored) ChangedRows(IFileSystem fileSystem)
    {
        if (Kind != ChangeFileKind.DeletionVectorChange)
            throw new InvalidOperationException("Only a deletion vector change has changed rows.");

        long[] before = PreviousDeletionVector is null ? [] : Actions.DeletionVector.Read(fileSystem, PreviousDeletionVector).DeletedRows;
        long[] after = DeletionVector is null ? [] : Actions.DeletionVector.Read(fileSystem, DeletionVector).DeletedRows;
        return (Except(after, before), Except(before, after));
    }

    // Both inputs ascend, so one pass finds the values of the first that the second lacks.
    private static long[] Except(long[] values, long[] without)
    {
        var result = new List<long>();
        int j = 0;
        foreach (long value in values)
        {
            while (j < without.Length && without[j] < value) j++;
            if (j >= without.Length || without[j] != value) result.Add(value);
        }
        return [.. result];
    }
}

/// <summary>Finds the files that hold the row-level changes between two versions of a table.</summary>
public static class ChangeFeed
{
    /// <summary>The table property that turns the change data feed on.</summary>
    public const string EnableKey = "delta.enableChangeDataFeed";

    /// <summary>Lists the change files of the commits from <paramref name="startVersion"/> to <paramref name="endVersion"/>, both included.</summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="startVersion">The first commit to report.</param>
    /// <param name="endVersion">The last commit to report, or null for the latest.</param>
    /// <param name="requireEnabled">
    /// When true, fail unless the change data feed was enabled for the whole range, as the protocol asks of a reader.
    /// When false, a table without it is still read from its added and removed files.
    /// </param>
    /// <param name="parquet">Reads checkpoints, see <see cref="Snapshot.Load"/>.</param>
    /// <exception cref="DeltaException">A version is not in the log, or the feed was not enabled.</exception>
    public static List<ChangeFile> Read(
        IFileSystem fileSystem, long startVersion, long? endVersion = null, bool requireEnabled = true, IParquetReader? parquet = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);

        CommitTimeline timeline = CommitTimeline.Load(fileSystem, parquet);
        long end = endVersion ?? timeline.LatestVersion;
        if (startVersion > end)
            throw new DeltaException($"The start version {startVersion} is after the end version {end}.");
        if (startVersion < timeline.EarliestVersion || end > timeline.LatestVersion)
            throw new DeltaException(
                $"Versions {startVersion} to {end} are not all in the log, which holds versions {timeline.EarliestVersion} to {timeline.LatestVersion}.");

        // The state just before the range says whether the feed is already on and which protocol applies.
        ProtocolAction? protocol = null;
        MetadataAction? metadata = null;
        if (startVersion > 0)
        {
            Snapshot before = Snapshot.Load(fileSystem, startVersion - 1, parquet);
            protocol = before.Protocol;
            metadata = before.Metadata;
        }

        var result = new List<ChangeFile>();
        for (long version = startVersion; version <= end; version++)
        {
            byte[] content = fileSystem.ReadAll(LogFile.CommitPath(version))
                ?? throw new DeltaException($"The commit of version {version} disappeared while reading the log.");
            List<DeltaAction> actions = ActionParser.ParseCommit(content);

            foreach (DeltaAction action in actions)
            {
                if (action is ProtocolAction p) protocol = p;
                else if (action is MetadataAction m) metadata = m;
            }
            if (protocol is null || metadata is null)
                throw new DeltaException($"The commit of version {version} comes before any protocol or metaData action.");
            TableFeatures.EnsureReadable(protocol, metadata);
            if (requireEnabled && !(metadata.Configuration.TryGetValue(EnableKey, out string? on) && string.Equals(on, "true", StringComparison.OrdinalIgnoreCase)))
                throw new DeltaException($"The change data feed is not enabled at version {version}.");

            AppendCommit(result, actions, version, timeline.TimestampOf(version));
        }
        return result;
    }

    /// <summary>
    /// Lists the change files of the commits made from <paramref name="start"/> to <paramref name="end"/>, both
    /// included. Returns an empty list when no commit falls in the range.
    /// </summary>
    public static List<ChangeFile> ReadBetween(
        IFileSystem fileSystem, DateTimeOffset start, DateTimeOffset? end = null, bool requireEnabled = true, IParquetReader? parquet = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);

        CommitTimeline timeline = CommitTimeline.Load(fileSystem, parquet);
        long? first = timeline.VersionAtOrAfter(start);
        if (first is null) return [];
        long last = timeline.LatestVersion;
        if (end is DateTimeOffset until)
        {
            if (until < timeline.TimestampOf(timeline.EarliestVersion)) return [];
            last = timeline.VersionAsOf(until);
        }
        return first > last ? [] : Read(fileSystem, first.Value, last, requireEnabled, parquet);
    }

    private static void AppendCommit(List<ChangeFile> result, List<DeltaAction> actions, long version, DateTimeOffset timestamp)
    {
        // A commit that wrote change data files is described by them alone.
        bool hasCdc = false;
        foreach (DeltaAction action in actions)
        {
            if (action is not CdcAction cdc) continue;
            hasCdc = true;
            result.Add(new ChangeFile
            {
                Kind = ChangeFileKind.Cdc,
                Path = cdc.Path,
                PartitionValues = cdc.PartitionValues,
                Version = version,
                Timestamp = timestamp,
            });
        }
        if (hasCdc) return;

        // Otherwise the changes are the files added and removed with dataChange. A file removed and added again
        // in the same commit only had its deletion vector replaced.
        var removed = new Dictionary<string, RemoveAction>(StringComparer.Ordinal);
        foreach (DeltaAction action in actions)
        {
            if (action is RemoveAction { DataChange: true } remove) removed[remove.Path] = remove;
        }

        foreach (DeltaAction action in actions)
        {
            if (action is not AddAction { DataChange: true } add) continue;
            if (removed.Remove(add.Path, out RemoveAction? previous))
            {
                result.Add(new ChangeFile
                {
                    Kind = ChangeFileKind.DeletionVectorChange,
                    Path = add.Path,
                    PartitionValues = add.PartitionValues,
                    DeletionVector = add.DeletionVector,
                    PreviousDeletionVector = previous.DeletionVector,
                    Version = version,
                    Timestamp = timestamp,
                });
            }
            else
            {
                result.Add(new ChangeFile
                {
                    Kind = ChangeFileKind.Insert,
                    Path = add.Path,
                    PartitionValues = add.PartitionValues,
                    DeletionVector = add.DeletionVector,
                    Version = version,
                    Timestamp = timestamp,
                });
            }
        }

        foreach (DeltaAction action in actions)
        {
            if (action is RemoveAction remove && removed.ContainsKey(remove.Path) && ReferenceEquals(removed[remove.Path], remove))
            {
                result.Add(new ChangeFile
                {
                    Kind = ChangeFileKind.Delete,
                    Path = remove.Path,
                    PartitionValues = remove.PartitionValues,
                    DeletionVector = remove.DeletionVector,
                    Version = version,
                    Timestamp = timestamp,
                });
            }
        }
    }
}
