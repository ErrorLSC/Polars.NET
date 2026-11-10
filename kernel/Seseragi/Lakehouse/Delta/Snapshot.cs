using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Skipping;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>The state of a table at one version: its protocol, metadata and the data files that make it up.</summary>
public sealed class Snapshot
{
    private StructType? _schema;

    /// <summary>The table version.</summary>
    public required long Version { get; init; }
    /// <summary>The protocol in force.</summary>
    public required ProtocolAction Protocol { get; init; }
    /// <summary>The metadata in force.</summary>
    public required MetadataAction Metadata { get; init; }
    /// <summary>The data files of the table, oldest first.</summary>
    public required IReadOnlyList<AddAction> Files { get; init; }
    /// <summary>The latest version each writing application recorded, by application id.</summary>
    public required IReadOnlyDictionary<string, TxnAction> AppTransactions { get; init; }
    /// <summary>The live domain metadata, by domain.</summary>
    public required IReadOnlyDictionary<string, DomainMetadataAction> DomainMetadata { get; init; }
    /// <summary>The modification time of the commit file of this version.</summary>
    public required DateTimeOffset CommitFileTime { get; init; }

    /// <summary>The table schema.</summary>
    public StructType Schema => _schema ??= StructType.Parse(Metadata.SchemaString);

    /// <summary>The partition columns in order, by logical name.</summary>
    public IReadOnlyList<string> PartitionColumns => Metadata.PartitionColumns;

    /// <summary>
    /// How columns are found in the data files. Unless it is <see cref="Delta.ColumnMappingMode.None"/>, a data file
    /// names its columns by <see cref="StructField.PhysicalName"/> (and <see cref="StructField.FieldId"/>), and the
    /// partition values and statistics in the log are keyed by physical names too.
    /// </summary>
    public ColumnMappingMode ColumnMappingMode => TableFeatures.ColumnMappingModeOf(Metadata);

    /// <summary>The partition values of a file keyed by logical column name, whatever the column mapping mode.</summary>
    public Dictionary<string, string?> PartitionValuesOf(AddAction file)
    {
        ArgumentNullException.ThrowIfNull(file);
        return LogicalPartitionValues(file.PartitionValues);
    }

    /// <summary>
    /// Partition values as the log writes them (keyed by physical name under column mapping), keyed by logical
    /// column name instead. A column the log gives no value for is null.
    /// </summary>
    public Dictionary<string, string?> LogicalPartitionValues(IReadOnlyDictionary<string, string?> logged)
    {
        ArgumentNullException.ThrowIfNull(logged);
        var values = new Dictionary<string, string?>(PartitionColumns.Count, StringComparer.Ordinal);
        foreach (string column in PartitionColumns)
        {
            string key = Schema.Field(column)?.PhysicalName ?? column;
            values[column] = logged.TryGetValue(key, out string? value) ? value : null;
        }
        return values;
    }

    /// <summary>
    /// The data files that can hold a row matching a predicate, judged by partition values and file statistics.
    /// Files are left out only when it is certain they hold none.
    /// </summary>
    public List<AddAction> FilesMatching(Predicate predicate)
    {
        var filter = new Skipping.FileFilter(Schema, PartitionColumns, predicate);
        var files = new List<AddAction>();
        foreach (AddAction file in Files)
        {
            if (filter.MightMatch(file)) files.Add(file);
        }
        return files;
    }

    /// <summary>Reads the table at its latest version, or at <paramref name="version"/>.</summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="version">The version to read, or null for the latest.</param>
    /// <param name="parquet">
    /// Reads checkpoints. Without it the state is rebuilt from the JSON commits alone, which works only while the
    /// log still starts at version 0.
    /// </param>
    /// <exception cref="DeltaTableNotFoundException">The location holds no Delta table.</exception>
    /// <exception cref="DeltaProtocolException">The table needs a feature this kernel does not implement.</exception>
    /// <exception cref="DeltaException">The version does not exist or the log is not valid.</exception>
    public static Snapshot Load(IFileSystem fileSystem, long? version = null, IParquetReader? parquet = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);

        LogSegment segment = LogSegment.Find(fileSystem, version, useCheckpoints: parquet is not null);

        // Newest first: the commits from the last one back, then the checkpoint they build on.
        var replay = new LogReplay();
        for (int i = segment.Commits.Count - 1; i >= 0; i--)
        {
            LogFile commit = segment.Commits[i];
            byte[] content = fileSystem.ReadAll(commit.Entry.Path)
                ?? throw new DeltaException($"The commit of version {commit.Version} disappeared while reading the log.");
            replay.Apply(ActionParser.ParseCommit(content));
        }
        foreach (LogFile file in segment.Checkpoint)
        {
            ApplyCheckpointFile(fileSystem, parquet!, replay, file);
        }

        ProtocolAction protocol = replay.Protocol ?? throw new DeltaException("The log holds no protocol action.");
        MetadataAction metadata = replay.Metadata ?? throw new DeltaException("The log holds no metaData action.");
        TableFeatures.EnsureReadable(protocol, metadata);

        var snapshot = new Snapshot
        {
            Version = segment.Version,
            Protocol = protocol,
            Metadata = metadata,
            Files = replay.ActiveFiles(),
            AppTransactions = replay.AppTransactions,
            DomainMetadata = replay.LiveDomainMetadata(),
            CommitFileTime = segment.LastModified,
        };
        if (snapshot.ColumnMappingMode != ColumnMappingMode.None) EnsureMapped(snapshot.Schema);
        return snapshot;
    }

    /// <summary>Under column mapping every field, nested ones included, must carry a physical name and an id.</summary>
    private static void EnsureMapped(DeltaType type)
    {
        switch (type)
        {
            case StructType s:
                foreach (StructField field in s.Fields)
                {
                    if (!field.Metadata.ContainsKey(StructField.PhysicalNameKey) || field.FieldId is null)
                        throw new DeltaException($"The table uses column mapping but the column '{field.Name}' has no physical name or id.");
                    EnsureMapped(field.Type);
                }
                break;
            case ArrayType a:
                EnsureMapped(a.ElementType);
                break;
            case MapType m:
                EnsureMapped(m.KeyType);
                EnsureMapped(m.ValueType);
                break;
        }
    }

    /// <summary>
    /// Reads the table as it was at a point in time: the latest version committed at or before
    /// <paramref name="timestamp"/>. A time after the last commit gives the latest version.
    /// </summary>
    /// <exception cref="DeltaException">The time is before the earliest commit still in the log.</exception>
    public static Snapshot LoadAsOf(IFileSystem fileSystem, DateTimeOffset timestamp, IParquetReader? parquet = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        long version = CommitTimeline.Load(fileSystem, parquet).VersionAsOf(timestamp);
        return Load(fileSystem, version, parquet);
    }

    private static void ApplyCheckpointFile(IFileSystem fileSystem, IParquetReader parquet, LogReplay replay, LogFile file)
    {
        string path = file.Entry.Path;
        List<DeltaAction> actions;
        if (path.EndsWith(".json", StringComparison.Ordinal))
        {
            byte[] content = fileSystem.ReadAll(path)
                ?? throw new DeltaException($"The checkpoint {path} disappeared while reading the log.");
            actions = ActionParser.ParseCommit(content);
        }
        else
        {
            actions = ActionParser.ParseCheckpointRows(parquet.ReadAsJsonLines(path));
        }
        replay.Apply(actions);

        // A V2 checkpoint keeps its file actions in sidecar files.
        foreach (DeltaAction action in actions)
        {
            if (action is not SidecarAction sidecar) continue;
            if (sidecar.Path.Contains("://", StringComparison.Ordinal))
                throw new DeltaException($"The checkpoint sidecar {sidecar.Path} is outside the table, which this kernel does not read.");
            string sidecarPath = $"{LogFile.LogDirectory}/_sidecars/{sidecar.Path}";
            replay.Apply(ActionParser.ParseCheckpointRows(parquet.ReadAsJsonLines(sidecarPath)));
        }
    }
}
