using System.Globalization;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// Changes the table properties and the protocol of a table, as Spark's <c>ALTER TABLE … SET TBLPROPERTIES</c>:
/// properties that turn a feature on bring the feature into the protocol, <c>delta.feature.&lt;name&gt;</c> adds a
/// feature, <c>delta.minReaderVersion</c> / <c>delta.minWriterVersion</c> raise the versions. The protocol is
/// never lowered.
/// </summary>
public static partial class AlterTable
{
    /// <summary>The operation name recorded in <c>commitInfo</c>, as Spark records it.</summary>
    public const string Operation = "SET TBLPROPERTIES";

    private const string FeaturePrefix = "delta.feature.";
    private const string ConstraintPrefix = "delta.constraints.";
    private const string MinReaderVersionKey = "delta.minReaderVersion";
    private const string MinWriterVersionKey = "delta.minWriterVersion";
    private const string CheckpointPolicyKey = Checkpoints.PolicyKey;

    /// <summary>The table properties Delta defines, in their canonical spelling; keys match without regard to case.</summary>
    private static readonly string[] KnownKeys =
    [
        TableFeatures.AppendOnlyKey, TableFeatures.EnableDeletionVectorsKey, TableFeatures.ColumnMappingModeKey,
        SchemaEvolution.MaxColumnIdKey, ChangeFeed.EnableKey, SchemaEvolution.EnableTypeWideningKey,
        CommitTimeline.InCommitTimestampsKey, CommitTimeline.EnablementVersionKey, CommitTimeline.EnablementTimestampKey,
        Checkpoints.IntervalKey, Checkpoints.TombstoneRetentionKey, CheckpointPolicyKey, LogCleanup.RetentionKey, LogCleanup.EnabledKey, LogCleanup.TruncateHistoryRetentionKey,
        "delta.checkpointRetentionDuration", "delta.sampleRetentionDuration",
        "delta.checkpoint.writeStatsAsJson", "delta.checkpoint.writeStatsAsStruct", "delta.targetFileSize",
        "delta.tuneFileSizesForRewrites", "delta.dataSkippingNumIndexedCols", "delta.dataSkippingStatsColumns",
        "delta.isolationLevel", FilePrefixes.RandomizeKey, FilePrefixes.LengthKey, "delta.setTransactionRetentionDuration",
        "delta.compatibility.symlinkFormatManifest.enabled", "delta.autoOptimize.optimizeWrite", "delta.autoOptimize.autoCompact",
        "delta.enableRowTracking", RowTracking.SuspendedKey, TableFeatures.RequireCheckpointProtectionBeforeVersionKey, "delta.enableIcebergCompatV1", "delta.enableIcebergCompatV2", "delta.universalFormat.enabledFormats",
    ];

    /// <summary>Properties that turn a feature on: true brings the feature into the protocol.</summary>
    private static readonly Dictionary<string, string> EnablingKeys = new(StringComparer.Ordinal)
    {
        [TableFeatures.AppendOnlyKey] = TableFeatures.AppendOnly,
        [TableFeatures.EnableDeletionVectorsKey] = TableFeatures.DeletionVectors,
        [ChangeFeed.EnableKey] = TableFeatures.ChangeDataFeed,
        [SchemaEvolution.EnableTypeWideningKey] = TableFeatures.TypeWidening,
        [CommitTimeline.InCommitTimestampsKey] = TableFeatures.InCommitTimestamp,
        [RowTracking.EnableKey] = RowTracking.Feature,
        ["delta.enableIcebergCompatV1"] = "icebergCompatV1",
        ["delta.enableIcebergCompatV2"] = "icebergCompatV2",
    };

    /// <summary>
    /// The features that <see cref="AddFeatures"/> also turns on, since adding them is asking to use them: the
    /// property that makes writers use the feature is set to true unless the table already sets it.
    /// </summary>
    private static readonly Dictionary<string, string> TurnedOnWhenAdded = new(StringComparer.Ordinal)
    {
        [TableFeatures.DeletionVectors] = TableFeatures.EnableDeletionVectorsKey,
        [TableFeatures.ChangeDataFeed] = ChangeFeed.EnableKey,
        [TableFeatures.AppendOnly] = TableFeatures.AppendOnlyKey,
    };

    /// <summary>
    /// Sets table properties, bringing the features they turn on into the protocol. Returns the version committed,
    /// or null when nothing changes.
    /// </summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="table">The latest state of the table.</param>
    /// <param name="properties">The properties to set.</param>
    /// <param name="refuseUnknown">Refuse a <c>delta.</c> property Delta does not define; otherwise it is stored as given.</param>
    /// <param name="now">The writer's clock; the current time when null.</param>
    /// <exception cref="DeltaException">A property cannot be set on this table, or the table cannot be written by this kernel.</exception>
    public static long? SetProperties(
        IWritableFileSystem fileSystem, Snapshot table, IReadOnlyDictionary<string, string> properties, bool refuseUnknown = true,
        DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(properties);
        var change = new Change(table);
        var recorded = new List<KeyValuePair<string, string>>();
        foreach (var (key, value) in properties)
        {
            ArgumentNullException.ThrowIfNull(value, nameof(properties));
            recorded.Add(new(change.Apply(key, value, refuseUnknown), value));
        }
        // Spark checks the retentions set together (not against the table's other properties).
        Intervals.ValidateSet(recorded.GroupBy(p => p.Key).ToDictionary(g => g.Key, g => g.Last().Value));
        return change.Commit(fileSystem, recorded, now);
    }

    /// <summary>
    /// Adds table features to the protocol. Deletion vectors, the change data feed and append-only are also turned
    /// on (their property set to true) unless the table sets the property already. Returns the version committed,
    /// or null when the table has the features already.
    /// </summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="table">The latest state of the table.</param>
    /// <param name="features">Feature names, matched without regard to case.</param>
    /// <param name="allowProtocolIncrease">Allow raising the reader or writer version; otherwise refuse when the
    /// table's versions do not let the feature be listed.</param>
    /// <param name="now">The writer's clock; the current time when null.</param>
    /// <exception cref="DeltaException">A feature is not one this kernel supports, or the protocol would have to be raised when that is not allowed.</exception>
    public static long? AddFeatures(
        IWritableFileSystem fileSystem, Snapshot table, IEnumerable<string> features, bool allowProtocolIncrease = true,
        DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(features);
        var change = new Change(table);
        var recorded = new List<KeyValuePair<string, string>>();
        foreach (string name in features)
        {
            string feature = TableFeatures.SupportedFeatureNamed(name)
                ?? throw new DeltaException($"The table feature '{name}' is unknown or not supported by this kernel.");
            recorded.Add(new(FeaturePrefix + feature.ToLowerInvariant(), "supported"));
            change.Features.Add(feature);
            if (TurnedOnWhenAdded.TryGetValue(feature, out string? key) && !table.Metadata.Configuration.ContainsKey(key))
                recorded.Add(new(change.Apply(key, "true", refuseUnknown: true), "true"));
        }
        if (recorded.Count == 0) throw new ArgumentException("No feature to add.", nameof(features));

        ProtocolAction protocol = change.Protocol();
        if (!allowProtocolIncrease
            && (protocol.MinReaderVersion > table.Protocol.MinReaderVersion || protocol.MinWriterVersion > table.Protocol.MinWriterVersion))
            throw new DeltaException(
                $"Adding the feature needs protocol version {protocol.MinReaderVersion}/{protocol.MinWriterVersion} (reader/writer); " +
                $"the table is at {table.Protocol.MinReaderVersion}/{table.Protocol.MinWriterVersion} and raising it is not allowed.");
        return change.Commit(fileSystem, recorded, now);
    }

    /// <summary>
    /// Changes the clustering columns, as Spark's <c>ALTER TABLE … CLUSTER BY (…)</c> / <c>CLUSTER BY NONE</c> (empty):
    /// one commit naming the new columns (and bringing the clustering feature in when the table has none). Data
    /// already clustered stays as it is until an OPTIMIZE FULL. Returns the version committed, or null for
    /// <c>NONE</c> on a table that is not clustered.
    /// </summary>
    /// <exception cref="DeltaException">The table is partitioned, or the columns cannot cluster it.</exception>
    public static long? ClusterBy(IWritableFileSystem fileSystem, Snapshot table, IReadOnlyList<string> columns, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(columns);
        if (table.PartitionColumns.Count > 0) throw new DeltaException("ALTER TABLE CLUSTER BY cannot be applied to a partitioned table.");
        IReadOnlyList<string[]>? current = Clustering.PhysicalColumns(table.DomainMetadata);
        if (columns.Count == 0 && current is null) return null;

        string[] old = current is null ? [] : [.. Clustering.Fields(table.Schema, current).Select(Clustering.LogicalName)];
        List<StructField[]> resolved = Clustering.Resolve(table.Schema, columns, [], table.Metadata.Configuration);
        var transaction = Transaction.Start(fileSystem, table, ClusterByOperation)
            .WithParameter("oldClusteringColumns", string.Join(',', old))
            .WithParameter("newClusteringColumns", string.Join(',', resolved.Select(Clustering.LogicalName)));
        ProtocolAction protocol = TableFeatures.WithFeatures(table.Protocol, [Clustering.Feature]);
        if (!ReferenceEquals(protocol, table.Protocol)) transaction.SetProtocol(protocol);
        transaction.Add(Clustering.DomainFor(resolved.Select(Clustering.PhysicalPath)));
        return transaction.Commit(now);
    }

    /// <summary>The operation name of <see cref="ClusterBy"/> in <c>commitInfo</c>, as Spark records it.</summary>
    public const string ClusterByOperation = "CLUSTER BY";

    /// <summary>What one alteration changes, gathered property by property.</summary>
    private sealed class Change(Snapshot table)
    {
        private readonly Dictionary<string, string> _configuration = new(table.Metadata.Configuration, StringComparer.Ordinal);
        private int? _readerVersion;
        private int? _writerVersion;
        private bool _nameMapping;

        public List<string> Features { get; } = [];

        /// <summary>Takes in one property; returns the key as recorded in the commit.</summary>
        public string Apply(string key, string value, bool refuseUnknown)
        {
            if (key.StartsWith(FeaturePrefix, StringComparison.OrdinalIgnoreCase))
            {
                string name = key[FeaturePrefix.Length..];
                if (!value.Equals("supported", StringComparison.OrdinalIgnoreCase) && !value.Equals("enabled", StringComparison.OrdinalIgnoreCase))
                    throw new DeltaException($"The property {key} takes 'supported', not '{value}'.");
                Features.Add(TableFeatures.SupportedFeatureNamed(name)
                    ?? throw new DeltaException($"The table feature '{name}' is unknown or not supported by this kernel."));
                return key.ToLowerInvariant(); // as Spark records it
            }
            if (key.StartsWith(ConstraintPrefix, StringComparison.OrdinalIgnoreCase))
                throw new DeltaException("Check constraints are added with their own operation, which this kernel does not offer yet.");
            if (key.Equals(MinReaderVersionKey, StringComparison.OrdinalIgnoreCase))
            {
                _readerVersion = Math.Max(_readerVersion ?? 0, Version(key, value, 1, TableFeatures.MaxReaderVersion));
                return MinReaderVersionKey;
            }
            if (key.Equals(MinWriterVersionKey, StringComparison.OrdinalIgnoreCase))
            {
                _writerVersion = Math.Max(_writerVersion ?? 0, Version(key, value, 2, TableFeatures.MaxWriterVersion));
                return MinWriterVersionKey;
            }

            string? known = KnownKeys.FirstOrDefault(k => k.Equals(key, StringComparison.OrdinalIgnoreCase));
            if (known is null)
            {
                if (refuseUnknown && key.StartsWith("delta.", StringComparison.OrdinalIgnoreCase))
                    throw new DeltaException($"The table property '{key}' is not one Delta defines.");
                _configuration[key] = value;
                return key;
            }

            if (EnablingKeys.TryGetValue(known, out string? feature))
            {
                if (!bool.TryParse(value, out bool on))
                    throw new DeltaException($"The property {known} takes true or false, not '{value}'.");
                if (on)
                {
                    if (!TableFeatures.SupportedWriterFeatures.Contains(feature))
                        throw new DeltaException($"The property {known} turns on the table feature '{feature}', which this kernel does not support.");
                    Features.Add(feature);
                }
            }
            else if (known == RowTracking.SuspendedKey)
            {
                if (!bool.TryParse(value, out _)) throw new DeltaException($"The property {known} takes true or false, not '{value}'.");
            }
            else if (known == TableFeatures.ColumnMappingModeKey)
            {
                MappingMode(value);
            }
            else if (known is FilePrefixes.RandomizeKey or FilePrefixes.LengthKey)
            {
                FilePrefixes.Validate(known, value);
            }
            else if (Intervals.Keys.Contains(known))
            {
                Intervals.Validate(known, value);
            }
            else if (known == CheckpointPolicyKey)
            {
                if (value == "v2") Features.Add(TableFeatures.V2Checkpoint);
                else if (value != "classic") throw new DeltaException($"The property {known} takes 'classic' or 'v2', not '{value}'.");
            }
            else if (known == "delta.universalFormat.enabledFormats" && value.Length > 0)
            {
                throw new DeltaException($"The property {known} turns on UniForm, which this kernel does not support.");
            }
            _configuration[known] = value;
            return known;
        }

        private void MappingMode(string value)
        {
            ColumnMappingMode from = table.ColumnMappingMode;
            ColumnMappingMode to = value switch
            {
                "none" => ColumnMappingMode.None,
                "name" => ColumnMappingMode.Name,
                "id" => ColumnMappingMode.Id,
                _ => throw new DeltaException($"The property {TableFeatures.ColumnMappingModeKey} takes none, name or id, not '{value}'."),
            };
            if (from == to) return;
            // Spark allows none → name only (name → none rewrites every file, which this kernel does not do).
            if (from != ColumnMappingMode.None || to != ColumnMappingMode.Name)
                throw new DeltaException($"Changing the column mapping mode from '{Name(from)}' to '{value}' is not supported.");
            _nameMapping = true;
            Features.Add(TableFeatures.ColumnMapping);

            static string Name(ColumnMappingMode mode) => mode.ToString().ToLowerInvariant();
        }

        private static int Version(string key, string value, int lowest, int highest) =>
            int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out int version) && version >= lowest && version <= highest
                ? version
                : throw new DeltaException($"The property {key} takes a version from {lowest} to {highest}, not '{value}'.");

        /// <summary>The protocol after the change: never lower than the table's.</summary>
        public ProtocolAction Protocol()
        {
            try
            {
                return TableFeatures.WithFeatures(TableFeatures.WithVersions(table.Protocol, _readerVersion, _writerVersion), Features);
            }
            catch (DeltaProtocolException e)
            {
                throw new DeltaException(e.Message);
            }
        }

        /// <summary>Commits the change; null when there is none.</summary>
        public long? Commit(IWritableFileSystem fileSystem, List<KeyValuePair<string, string>> recorded, DateTimeOffset? now)
        {
            ProtocolAction protocol = Protocol();
            MetadataAction current = table.Metadata;
            DateTimeOffset clock = now ?? DateTimeOffset.UtcNow;

            if (IsOn(_configuration, CommitTimeline.InCommitTimestampsKey) && !IsOn(current.Configuration, CommitTimeline.InCommitTimestampsKey))
            {
                // Turned on now: in-commit timestamps start with this commit, after the time of the one before.
                long previous = CommitTimeline.Load(fileSystem, current).TimestampOf(table.Version).ToUnixTimeMilliseconds();
                long timestamp = Math.Max(clock.ToUnixTimeMilliseconds(), previous + 1);
                _configuration[CommitTimeline.EnablementVersionKey] = (table.Version + 1).ToString(CultureInfo.InvariantCulture);
                _configuration[CommitTimeline.EnablementTimestampKey] = timestamp.ToString(CultureInfo.InvariantCulture);
                clock = DateTimeOffset.FromUnixTimeMilliseconds(timestamp);
            }

            // Row tracking coming in names its materialized columns (as Spark); turned on, every file must have its
            // row ids: files without are added again (dataChange false) and the commit gives them ids. Spark takes
            // three commits (protocol, backfill, property); here it is one, so no reader sees a half-done state.
            bool trackingOn = IsOn(_configuration, RowTracking.EnableKey);
            if (trackingOn || (Features.Contains(RowTracking.Feature) && !RowTracking.IsSupported(table.Protocol)))
            {
                foreach (var (key, value) in RowTracking.WithMaterializedColumnNames(_configuration)) _configuration.TryAdd(key, value);
            }
            List<AddAction> backfill = trackingOn && !RowTracking.IsEnabled(table.Protocol, table.Metadata)
                ? [.. table.Files.Where(f => f.BaseRowId is null).Select(Backfilled)]
                : [];

            MetadataAction metadata = new()
            {
                Id = current.Id, Name = current.Name, Description = current.Description, FormatProvider = current.FormatProvider,
                FormatOptions = current.FormatOptions, SchemaString = current.SchemaString, PartitionColumns = current.PartitionColumns,
                Configuration = _configuration, CreatedTime = current.CreatedTime,
            };
            if (_nameMapping) metadata = SchemaEvolution.WithNameMapping(metadata);

            bool protocolChanges = !ReferenceEquals(protocol, table.Protocol);
            bool metadataChanges = metadata.SchemaString != current.SchemaString || !SameConfiguration(metadata.Configuration, current.Configuration);
            if (!protocolChanges && !metadataChanges) return null;

            var transaction = Transaction.Start(fileSystem, table, Operation)
                .WithParameter("properties", JsonText.Object(recorded));
            if (protocolChanges) transaction.SetProtocol(protocol);
            if (metadataChanges) transaction.SetMetadata(metadata);
            foreach (AddAction add in backfill) transaction.Add(add);
            return transaction.Commit(clock);
        }

        /// <summary>A file added again as it is, to get row ids; no data changes.</summary>
        private static AddAction Backfilled(AddAction f) => new()
        {
            Path = f.Path, PartitionValues = f.PartitionValues, Size = f.Size, ModificationTime = f.ModificationTime, DataChange = false,
            Stats = f.Stats, Tags = f.Tags, DeletionVector = f.DeletionVector, ClusteringProvider = f.ClusteringProvider,
        };

        private static bool IsOn(IReadOnlyDictionary<string, string> configuration, string key) =>
            configuration.TryGetValue(key, out string? value) && string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);

        private static bool SameConfiguration(IReadOnlyDictionary<string, string> a, IReadOnlyDictionary<string, string> b) =>
            a.Count == b.Count && a.All(kv => b.TryGetValue(kv.Key, out string? v) && v == kv.Value);
    }
}
