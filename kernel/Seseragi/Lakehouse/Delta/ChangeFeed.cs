using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;

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
        List<long> result = [];
        int j = 0;
        foreach (long value in values)
        {
            while (j < without.Length && without[j] < value) j++;
            if (j >= without.Length || without[j] != value) result.Add(value);
        }
        return [.. result];
    }
}

/// <summary>The changes of a range of versions and the table state whose schema they are read with.</summary>
public sealed class ChangeRange
{
    /// <summary>The files holding the changes, in commit order.</summary>
    public required List<ChangeFile> Files { get; init; }

    /// <summary>
    /// The table at the version whose schema the changes are read with (Spark's choice): the end version when the
    /// table maps columns (at its latest version), else the latest version.
    /// </summary>
    public required Snapshot ReadSnapshot { get; init; }
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
    /// <param name="allowSchemaChanges">Read the range even when the table's schema changed in it in a way the read
    /// schema cannot hold (Spark's <c>unsafeBatchReadOnIncompatibleSchemaChanges</c>); see <see cref="ReadRange"/>.</param>
    public static List<ChangeFile> Read(
        IFileSystem fileSystem, long startVersion, long? endVersion = null, bool requireEnabled = true, IParquetReader? parquet = null,
        bool allowSchemaChanges = false) =>
        ReadRange(fileSystem, startVersion, endVersion, requireEnabled, parquet, allowSchemaChanges).Files;

    /// <summary>
    /// The change files of the commits from <paramref name="startVersion"/> to <paramref name="endVersion"/> (both
    /// included), and the table state whose schema they are read with. As Spark, a range is refused when the table's
    /// schema in it cannot be read with that schema: a column renamed, dropped (or dropped and added again) or
    /// changed in type. The change feed does not record such changes, so a consumer replaying it
    /// would not see them; read the parts before and after the change separately. Columns added are fine.
    /// </summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="startVersion">The first commit to report.</param>
    /// <param name="endVersion">The last commit to report, or null for the latest.</param>
    /// <param name="requireEnabled">Fail unless the change data feed was enabled for the whole range.</param>
    /// <param name="parquet">Reads checkpoints, see <see cref="Snapshot.Load"/>.</param>
    /// <param name="allowSchemaChanges">Skip the schema check: older changes are read with the read schema, by
    /// physical name under column mapping (Spark's <c>unsafeBatchReadOnIncompatibleSchemaChanges</c>).</param>
    /// <exception cref="DeltaException">A version is not in the log, the feed was not enabled, or the schema changed incompatibly.</exception>
    public static ChangeRange ReadRange(
        IFileSystem fileSystem, long startVersion, long? endVersion = null, bool requireEnabled = true, IParquetReader? parquet = null,
        bool allowSchemaChanges = false)
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

        // Spark reads a column-mapped table with the end version's schema, any other with the latest one.
        Snapshot latest = Snapshot.Load(fileSystem, null, parquet);
        Snapshot readSnapshot = latest.ColumnMappingMode != ColumnMappingMode.None && end != latest.Version
            ? Snapshot.Load(fileSystem, end, parquet)
            : latest;
        bool byPhysicalName = readSnapshot.ColumnMappingMode != ColumnMappingMode.None;
        StructType readSchema = readSnapshot.Schema;
        long? changedAt = null;
        MetadataAction? startMetadata = null;

        List<ChangeFile> result = [];
        for (long version = startVersion; version <= end; version++)
        {
            byte[] content = fileSystem.ReadAll(LogFile.CommitPath(version))
                ?? throw new DeltaException($"The commit of version {version} disappeared while reading the log.");
            List<DeltaAction> actions = ActionParser.ParseCommit(content);

            bool metadataChanged = false;
            foreach (DeltaAction action in actions)
            {
                if (action is ProtocolAction p) protocol = p;
                else if (action is MetadataAction m)
                {
                    metadata = m;
                    metadataChanged = true;
                }
            }
            if (version == startVersion) startMetadata = metadata;
            else if (metadataChanged && changedAt is null && !allowSchemaChanges && metadata is not null
                && !ReadCompatible(StructType.Parse(metadata.SchemaString), readSchema, byPhysicalName))
                changedAt = version;
            if (protocol is null || metadata is null)
                throw new DeltaException($"The commit of version {version} comes before any protocol or metaData action.");
            TableFeatures.EnsureReadable(protocol, metadata);
            if (requireEnabled && !(metadata.Configuration.TryGetValue(EnableKey, out string? on) && string.Equals(on, "true", StringComparison.OrdinalIgnoreCase)))
                throw new DeltaException($"The change data feed is not enabled at version {version}.");

            AppendCommit(result, actions, version, timeline.TimestampOf(version));
        }

        if (changedAt is long at)
            throw new DeltaException(
                $"Reading the changes from version {startVersion} to {end} failed: the schema changed incompatibly at version {at} " +
                $"(a column renamed, dropped or changed in type), and the changes are read with the schema of version {readSnapshot.Version}. " +
                $"Read versions {startVersion} to {at - 1} and {at} to {end} separately, or allow schema changes.");
        if (!allowSchemaChanges && startMetadata is not null
            && !ReadCompatible(StructType.Parse(startMetadata.SchemaString), readSchema, byPhysicalName))
            throw new DeltaException(
                $"Reading the changes from version {startVersion} to {end} failed: the data of version {startVersion} cannot be read with " +
                $"the schema of version {readSnapshot.Version}, which renamed, dropped or changed the type of a column. Start the range " +
                "at the version that made the change, or allow schema changes.");
        return new ChangeRange { Files = result, ReadSnapshot = readSnapshot };
    }

    /// <summary>
    /// Whether data written with <paramref name="data"/> reads correctly with <paramref name="read"/>, as Spark's
    /// <c>isReadCompatible</c>: every field is still there (names any case; under column mapping the same physical
    /// field too, so a column dropped and added again does not pass) and of the same type (a widening does not pass).
    /// Fields the read schema adds are fine; nullability is not compared (Spark reads across NOT NULL changes).
    /// </summary>
    internal static bool ReadCompatible(DeltaType data, DeltaType read, bool byPhysicalName)
    {
        switch (data, read)
        {
            case (StructType d, StructType r):
                foreach (StructField field in d.Fields)
                {
                    StructField? match = r.Fields.FirstOrDefault(f => string.Equals(f.Name, field.Name, StringComparison.OrdinalIgnoreCase));
                    if (match is null) return false;
                    if (byPhysicalName && match.PhysicalName != field.PhysicalName) return false;
                    if (!ReadCompatible(field.Type, match.Type, byPhysicalName)) return false;
                }
                return true;
            case (ArrayType d, ArrayType r):
                return ReadCompatible(d.ElementType, r.ElementType, byPhysicalName);
            case (MapType d, MapType r):
                return ReadCompatible(d.KeyType, r.KeyType, byPhysicalName) && ReadCompatible(d.ValueType, r.ValueType, byPhysicalName);
            case (StructType or ArrayType or MapType, _) or (_, StructType or ArrayType or MapType):
                return false;
            default:
                return data.ToString() == read.ToString();
        }
    }

    /// <summary>
    /// Lists the change files of the commits made from <paramref name="start"/> to <paramref name="end"/>, both
    /// included. Returns an empty list when no commit falls in the range.
    /// </summary>
    public static List<ChangeFile> ReadBetween(
        IFileSystem fileSystem, DateTimeOffset start, DateTimeOffset? end = null, bool requireEnabled = true, IParquetReader? parquet = null,
        bool allowSchemaChanges = false) =>
        ReadRangeBetween(fileSystem, start, end, requireEnabled, parquet, allowSchemaChanges)?.Files ?? [];

    /// <summary>
    /// <see cref="ReadRange"/> for the commits made from <paramref name="start"/> to <paramref name="end"/>, both
    /// included; null when no commit falls in the range.
    /// </summary>
    public static ChangeRange? ReadRangeBetween(
        IFileSystem fileSystem, DateTimeOffset start, DateTimeOffset? end = null, bool requireEnabled = true, IParquetReader? parquet = null,
        bool allowSchemaChanges = false)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);

        CommitTimeline timeline = CommitTimeline.Load(fileSystem, parquet);
        long? first = timeline.VersionAtOrAfter(start);
        if (first is null) return null;
        long last = timeline.LatestVersion;
        if (end is DateTimeOffset until)
        {
            if (until < timeline.TimestampOf(timeline.EarliestVersion)) return null;
            last = timeline.VersionAsOf(until);
        }
        return first > last ? null : ReadRange(fileSystem, first.Value, last, requireEnabled, parquet, allowSchemaChanges);
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
