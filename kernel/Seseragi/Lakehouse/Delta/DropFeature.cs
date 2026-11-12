using System.Buffers;
using System.Collections.Frozen;
using System.Globalization;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// The data rewrites some features need before they can be dropped (deletion vectors purged, files rewritten in
/// their table types or under logical names). The engine layer implements them; each works on the table as it is
/// when called and returns the version it committed, or null when nothing needed rewriting.
/// </summary>
public interface IFeatureRewriter
{
    /// <summary>Rewrites every file with a deletion vector without it (Spark's <c>REORG … APPLY (PURGE)</c>).</summary>
    long? PurgeDeletionVectors();

    /// <summary>Rewrites every file holding a column in a type older than the table's (type widening) in the table's types.</summary>
    long? RewriteWidenedFiles();

    /// <summary>Rewrites every file under logical column names and commits the table without column mapping.</summary>
    long? RemoveColumnMapping();
}

/// <summary>
/// Drops a table feature, as delta-spark 4.4's <c>ALTER TABLE … DROP FEATURE</c> (without <c>TRUNCATE HISTORY</c>):
/// the feature's traces are removed first (properties turned off, data rewritten), each step its own commit and
/// each skipped when already done, so an interrupted drop is finished by running it again. A writer feature then
/// leaves the protocol in one commit. A reader feature is dropped under checkpoint protection: three empty commits
/// with a checkpoint each, then the commit that drops it, adds the <c>checkpointProtection</c> writer feature and
/// records its own version as <c>delta.requireCheckpointProtectionBeforeVersion</c>, checkpointed too. Readers that
/// lack the dropped feature then never need the commits before it.
/// </summary>
public static class DropFeature
{
    /// <summary>The operation name of the commit that drops the feature, as Spark records it.</summary>
    public const string Operation = "DROP FEATURE";

    /// <summary>The operation name of the commits before the protected checkpoints, as Spark records it.</summary>
    public const string EmptyCommitOperation = "Empty Commit";

    /// <summary>How many checkpointed empty commits come before the drop of a reader feature (Spark's barrier).</summary>
    public const int BarrierCheckpoints = 3;

    private const string UnsetOperation = "UNSET TBLPROPERTIES";
    private const string UnbackfillOperation = "ROW TRACKING UNBACKFILL";

    /// <summary>Features Spark cannot drop.</summary>
    private static readonly FrozenSet<string> NonRemovable = ((string[])
    [
        TableFeatures.AppendOnly, TableFeatures.Invariants, TableFeatures.ChangeDataFeed, TableFeatures.GeneratedColumns,
        TableFeatures.IdentityColumns, TableFeatures.TimestampNtz, Clustering.Feature,
    ]).ToFrozenSet(StringComparer.Ordinal);

    /// <summary>
    /// Drops the feature; returns the version of the commit that dropped it.
    /// </summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="latest">Loads the latest state of the table (after cleanup the log may start at a checkpoint).</param>
    /// <param name="featureName">The feature, any case.</param>
    /// <param name="rewriter">The data rewrites; needed for deletion vectors, type widening and column mapping.</param>
    /// <param name="now">The writer's clock; the current time when null.</param>
    /// <exception cref="DeltaException">The feature is not in the protocol, cannot be dropped, or something depends on it.</exception>
    public static long Run(IWritableFileSystem fileSystem, Func<Snapshot> latest, string featureName, IFeatureRewriter? rewriter = null, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(latest);
        ArgumentException.ThrowIfNullOrEmpty(featureName);

        Snapshot table = latest();
        string feature = Validate(table, featureName, rewriter);
        bool protect = TableFeatures.IsReaderWriterFeature(feature);

        // Traces can come back while the drop runs (a writer that read the table before a property was turned off):
        // the drop commit is then refused and the traces removed again.
        for (int attempt = 0; attempt < 5; attempt++)
        {
            RemoveTraces(fileSystem, latest, feature, rewriter, now);
            if (protect)
            {
                for (int i = 0; i < BarrierCheckpoints; i++)
                {
                    Transaction.Start(fileSystem, latest(), EmptyCommitOperation).Commit(now);
                    Checkpoints.Write(fileSystem, latest(), now); // a barrier: must exist, so failures are not swallowed
                }
            }

            // The drop commits at exactly the version after the state it checked: its protection boundary is its own
            // version, and nothing may slip in between the check and the commit.
            for (int retry = 0; retry < 10; retry++)
            {
                table = latest();
                if (Traces(table, feature) is not null) break; // back to removing them
                Transaction drop = Transaction.Start(fileSystem, table, Operation)
                    .WithParameter("featureName", feature)
                    .WithParameter("truncateHistory", false)
                    .SetProtocol(TableFeatures.WithoutFeature(table.Protocol, feature, protect))
                    .SetMetadata(DroppedMetadata(table.Metadata, feature, protect ? table.Version + 1 : null));
                if (feature == RowTracking.Feature && table.DomainMetadata.TryGetValue(RowTracking.Domain, out DomainMetadataAction? ids) && !ids.Removed)
                    drop.Add(new DomainMetadataAction { Domain = ids.Domain, Configuration = ids.Configuration, Removed = true });
                long version;
                try
                {
                    version = drop.Commit(now, maxRetries: 0);
                }
                catch (DeltaCommitConflictException)
                {
                    continue; // another commit took the version: check again on the new state
                }
                if (protect) Checkpoints.Write(fileSystem, latest(), now);
                return version;
            }
        }
        throw new DeltaException($"The table feature '{feature}' could not be dropped: other writers kept bringing back its traces.");
    }

    // ------------------------------------------------------------------ checks

    /// <summary>The feature's protocol name, after the checks Spark makes before it changes anything.</summary>
    private static string Validate(Snapshot table, string featureName, IFeatureRewriter? rewriter)
    {
        string? inProtocol = TableFeatures.WriterFeaturesOf(table.Protocol).Concat(TableFeatures.ReaderFeaturesOf(table.Protocol))
            .FirstOrDefault(f => string.Equals(f, featureName, StringComparison.OrdinalIgnoreCase));
        if (inProtocol is null)
        {
            throw TableFeatures.SupportedFeatureNamed(featureName) is { } known
                ? new DeltaException($"Cannot drop {known} from this table because it is not currently present in the table's protocol.")
                : new DeltaException($"Cannot drop {featureName} because it is not a table feature this kernel knows.");
        }
        string feature = inProtocol;

        if (NonRemovable.Contains(feature))
            throw new DeltaException($"Cannot drop {feature} because dropping this feature is not supported.");
        if (feature == TableFeatures.CheckpointProtection)
            throw new DeltaException(
                "Cannot drop checkpointProtection: that needs the history before it truncated (Spark's DROP FEATURE … TRUNCATE HISTORY), which this kernel does not offer.");
        if (!TableFeatures.SupportedWriterFeatures.Contains(feature))
            throw new DeltaException($"Cannot drop {feature}: this kernel does not support the feature.");

        string[] dependents = [.. TableFeatures.WriterFeaturesOf(table.Protocol).Where(f => DependsOn(f, feature))];
        if (dependents.Length > 0)
            throw new DeltaException($"Cannot drop table feature `{feature}` because some other features ({string.Join(", ", dependents)}) in this table depend on `{feature}`.");

        switch (feature)
        {
            case TableFeatures.CheckConstraints when table.Metadata.Configuration.Keys.Any(k => k.StartsWith("delta.constraints.", StringComparison.OrdinalIgnoreCase)):
                throw new DeltaException("Cannot drop the CHECK constraints table feature: the table has check constraints; drop them first.");
            case TableFeatures.DomainMetadata when table.DomainMetadata.Values.Any(d => !d.Removed):
                throw new DeltaException(
                    $"Cannot drop domainMetadata: the table has domains ({string.Join(", ", table.DomainMetadata.Values.Where(d => !d.Removed).Select(d => d.Domain))}).");
            case TableFeatures.ColumnMapping when InvalidNames(table.Schema).FirstOrDefault() is string name:
                throw new DeltaException(
                    $"Found invalid character(s) among ' ,;{{}}()\\n\\t=' in the column names of your schema ('{name}'): without column mapping " +
                    "the data files hold the columns under these names. Rename the columns first.");
            case TableFeatures.DeletionVectors or TableFeatures.TypeWidening or TableFeatures.TypeWideningPreview or TableFeatures.ColumnMapping
                when rewriter is null:
                throw new DeltaException($"Dropping {feature} rewrites data files, which needs the engine.");
        }
        return feature;
    }

    /// <summary>The characters Parquet column names cannot hold without column mapping (Spark's check).</summary>
    private static readonly SearchValues<char> InvalidNameCharacters = SearchValues.Create(" ,;{}()\n\t=");

    /// <summary>The field names, at any depth, that cannot name a column in a data file without column mapping.</summary>
    private static IEnumerable<string> InvalidNames(Schema.DeltaType type) => type switch
    {
        Schema.StructType s => s.Fields.SelectMany(f => f.Name.AsSpan().ContainsAny(InvalidNameCharacters) ? [f.Name, .. InvalidNames(f.Type)] : InvalidNames(f.Type)),
        Schema.ArrayType a => InvalidNames(a.ElementType),
        Schema.MapType m => InvalidNames(m.KeyType).Concat(InvalidNames(m.ValueType)),
        _ => [],
    };

    /// <summary>
    /// The table without column mapping, as Spark's REMOVE COLUMN MAPPING commits it: the schema without column ids
    /// and physical names, the configuration without the mapping mode and the highest column id. With the data
    /// files rewritten under the logical names, it is what that commit sets. The clustering columns, which the
    /// clustering domain names by physical names, come back under their logical names; null when the table is not
    /// clustered. (Spark leaves the domain as it was, naming columns the table no longer has, after which its own
    /// OPTIMIZE and DESCRIBE DETAIL fail on the table.)
    /// </summary>
    /// <param name="table">The table under column mapping.</param>
    public static (MetadataAction Metadata, DomainMetadataAction? Clustering) WithoutColumnMapping(Snapshot table)
    {
        ArgumentNullException.ThrowIfNull(table);
        Dictionary<string, string> configuration = new(table.Metadata.Configuration);
        configuration.Remove(TableFeatures.ColumnMappingModeKey);
        configuration.Remove(Schema.SchemaEvolution.MaxColumnIdKey);
        MetadataAction metadata = WithSchema(WithConfiguration(table.Metadata, configuration), Schema.SchemaEvolution.WithoutColumnMapping(table.Schema));
        DomainMetadataAction? clustering = Clustering.PhysicalColumns(table.DomainMetadata) is { Count: > 0 } columns
            ? Clustering.DomainFor(Clustering.Fields(table.Schema, columns).Select(string[] (path) => [.. path.Select(f => f.Name)]))
            : null;
        return (metadata, clustering);
    }

    /// <summary>Whether <paramref name="feature"/> needs <paramref name="dependency"/> in the protocol.</summary>
    private static bool DependsOn(string feature, string dependency) => (feature, dependency) switch
    {
        (RowTracking.Feature or Clustering.Feature, TableFeatures.DomainMetadata) => true,
        _ => false,
    };

    /// <summary>What still ties the table to the feature; null when nothing does.</summary>
    private static string? Traces(Snapshot table, string feature)
    {
        IReadOnlyDictionary<string, string> configuration = table.Metadata.Configuration;
        return feature switch
        {
            TableFeatures.V2Checkpoint when Is(configuration, Checkpoints.PolicyKey, "v2") => "the v2 checkpoint policy",
            TableFeatures.InCommitTimestamp when IsTrue(configuration, CommitTimeline.InCommitTimestampsKey)
                || configuration.ContainsKey(CommitTimeline.EnablementVersionKey) || configuration.ContainsKey(CommitTimeline.EnablementTimestampKey) =>
                "in-commit timestamps turned on",
            RowTracking.Feature when !Is(configuration, RowTracking.EnableKey, "false") || !IsTrue(configuration, RowTracking.SuspendedKey) =>
                "row tracking turned on",
            RowTracking.Feature when table.Files.Any(f => f.BaseRowId is not null || f.DefaultRowCommitVersion is not null) => "files with row ids",
            TableFeatures.DeletionVectors when IsTrue(configuration, TableFeatures.EnableDeletionVectorsKey) => "deletion vectors turned on",
            TableFeatures.DeletionVectors when table.Files.Any(f => f.DeletionVector is not null) => "files with deletion vectors",
            TableFeatures.TypeWidening or TableFeatures.TypeWideningPreview when configuration.ContainsKey(Schema.SchemaEvolution.EnableTypeWideningKey) =>
                "type widening turned on",
            TableFeatures.TypeWidening or TableFeatures.TypeWideningPreview when table.Metadata.SchemaString.Contains($"\"{Schema.SchemaEvolution.TypeChangesKey}\"", StringComparison.Ordinal) =>
                "type changes in the schema",
            TableFeatures.ColumnMapping when table.ColumnMappingMode != ColumnMappingMode.None => "column mapping",
            _ => null,
        };
    }

    // ------------------------------------------------------------------ removing traces

    private static void RemoveTraces(IWritableFileSystem fileSystem, Func<Snapshot> latest, string feature, IFeatureRewriter? rewriter, DateTimeOffset? now)
    {
        Snapshot table = latest();
        switch (feature)
        {
            case TableFeatures.V2Checkpoint when Is(table.Metadata.Configuration, Checkpoints.PolicyKey, "v2"):
                AlterTable.SetProperties(fileSystem, table, new Dictionary<string, string> { [Checkpoints.PolicyKey] = "classic" }, now: now);
                break;

            case TableFeatures.InCommitTimestamp when Traces(table, feature) is not null:
            {
                // Spark: UNSET the enablement properties, leaving the flag false.
                string[] keys = [CommitTimeline.EnablementTimestampKey, CommitTimeline.EnablementVersionKey, CommitTimeline.InCommitTimestampsKey];
                string[] present = [.. keys.Where(table.Metadata.Configuration.ContainsKey)];
                Dictionary<string, string> configuration = new(table.Metadata.Configuration);
                foreach (string key in keys) configuration.Remove(key);
                configuration[CommitTimeline.InCommitTimestampsKey] = "false";
                Transaction.Start(fileSystem, table, UnsetOperation)
                    .WithParameter("properties", JsonText.StringArray(present))
                    .WithParameter("ifExists", true)
                    .SetMetadata(WithConfiguration(table.Metadata, configuration))
                    .Commit(now);
                break;
            }

            case RowTracking.Feature:
            {
                if (!Is(table.Metadata.Configuration, RowTracking.EnableKey, "false") || !IsTrue(table.Metadata.Configuration, RowTracking.SuspendedKey))
                {
                    AlterTable.SetProperties(fileSystem, table,
                        new Dictionary<string, string> { [RowTracking.EnableKey] = "false", [RowTracking.SuspendedKey] = "true" }, now: now);
                    table = latest();
                }
                // Files keep their data; the row ids leave the log (Spark's unbackfill), the commit assigning none.
                AddAction[] tracked = [.. table.Files.Where(f => f.BaseRowId is not null || f.DefaultRowCommitVersion is not null)];
                if (tracked.Length > 0)
                {
                    Transaction unbackfill = Transaction.Start(fileSystem, table, UnbackfillOperation).WithParameter("batchId", "0").ReadFiles(tracked);
                    foreach (AddAction file in tracked) unbackfill.Add(WithoutRowIds(file));
                    unbackfill.Commit(now);
                }
                break;
            }

            case TableFeatures.DeletionVectors:
            {
                if (IsTrue(table.Metadata.Configuration, TableFeatures.EnableDeletionVectorsKey)) Unset(fileSystem, table, TableFeatures.EnableDeletionVectorsKey, now);
                rewriter!.PurgeDeletionVectors();
                Tombstones(fileSystem, latest(), now);
                break;
            }

            case TableFeatures.TypeWidening or TableFeatures.TypeWideningPreview:
            {
                if (table.Metadata.Configuration.ContainsKey(Schema.SchemaEvolution.EnableTypeWideningKey))
                    Unset(fileSystem, table, Schema.SchemaEvolution.EnableTypeWideningKey, now);
                rewriter!.RewriteWidenedFiles();
                RemoveTypeChanges(fileSystem, latest(), now);
                break;
            }

            case TableFeatures.ColumnMapping when table.ColumnMappingMode != ColumnMappingMode.None:
                rewriter!.RemoveColumnMapping();
                break;
        }
    }

    /// <summary>Spark's UNSET TBLPROPERTIES of one property.</summary>
    private static void Unset(IWritableFileSystem fileSystem, Snapshot table, string key, DateTimeOffset? now)
    {
        Dictionary<string, string> configuration = new(table.Metadata.Configuration);
        configuration.Remove(key);
        Transaction.Start(fileSystem, table, UnsetOperation)
            .WithParameter("properties", JsonText.StringArray([key]))
            .WithParameter("ifExists", true)
            .SetMetadata(WithConfiguration(table.Metadata, configuration))
            .Commit(now);
    }

    /// <summary>
    /// Spark's "Deletion Vector Tombstones": a remove for every deletion vector file only removed files still refer
    /// to, so that a vacuum by a client without deletion vectors (which does not see them through the removes)
    /// keeps them for the time travel the retention allows. Committed even when there are none, as Spark does.
    /// </summary>
    private static void Tombstones(IWritableFileSystem fileSystem, Snapshot table, DateTimeOffset? now)
    {
        long at = (now ?? DateTimeOffset.UtcNow).ToUnixTimeMilliseconds();
        HashSet<string> live = [.. table.Files.Select(f => f.DeletionVector).OfType<DeletionVectorDescriptor>().Select(dv => dv.UniqueId)];
        HashSet<string> already = new(table.Tombstones.Select(r => r.Path), StringComparer.Ordinal);
        Transaction tombstones = Transaction.Start(fileSystem, table, "Deletion Vector Tombstones");
        foreach (DeletionVectorDescriptor dv in table.Tombstones.Select(r => r.DeletionVector).OfType<DeletionVectorDescriptor>()
            .Where(dv => !live.Contains(dv.UniqueId)).DistinctBy(dv => dv.UniqueId))
        {
            // Vectors in the table's own files (storage type u); inline ones have no file, absolute ones lie elsewhere.
            if (dv.StorageType != "u" || !already.Add(dv.FilePath())) continue;
            string path = dv.FilePath();
            tombstones.Add(new RemoveAction { Path = path, DeletionTimestamp = at, DataChange = false });
        }
        tombstones.Commit(now);
    }

    /// <summary>
    /// Spark's DROP FEATURE commit that only removes the <c>delta.typeChanges</c> metadata, recording the fields that
    /// had it as Spark's <c>UpdateColumnMetadata</c> does: dotted path, type name, metadata before the change.
    /// </summary>
    private static void RemoveTypeChanges(IWritableFileSystem fileSystem, Snapshot table, DateTimeOffset? now)
    {
        string key = Schema.SchemaEvolution.TypeChangesKey;
        if (!table.Metadata.SchemaString.Contains($"\"{key}\"", StringComparison.Ordinal)) return;
        List<(string[] Path, Schema.StructField Field)> changed = [];
        var schema = (Schema.StructType)Strip(table.Schema, key, [], changed);
        Transaction.Start(fileSystem, table, Operation)
            .WithParameter("columns", "[" + string.Join(",", changed.Select(c => Schema.StructType.FieldJson(c.Field, Schema.ColumnNames.Display(c.Path), typeName: true))) + "]")
            .SetMetadata(WithSchema(table.Metadata, schema))
            .Commit(now);
    }

    /// <summary>
    /// The type without the metadata key on any field, at any depth; the fields that had it are collected (as they
    /// were) with their paths (<c>element</c>, <c>key</c>, <c>value</c> inside arrays and maps).
    /// </summary>
    private static Schema.DeltaType Strip(Schema.DeltaType type, string key, string[] path, List<(string[] Path, Schema.StructField Field)> changed) => type switch
    {
        Schema.StructType s => new Schema.StructType([.. s.Fields.Select(f =>
        {
            string[] at = [.. path, f.Name];
            if (f.Metadata.ContainsKey(key)) changed.Add((at, f));
            return new Schema.StructField
            {
                Name = f.Name, Type = Strip(f.Type, key, at, changed), Nullable = f.Nullable,
                Metadata = f.Metadata.Where(kv => kv.Key != key).ToDictionary(),
                JsonMetadataKeys = new HashSet<string>(f.JsonMetadataKeys.Where(k => k != key)),
            };
        })]),
        Schema.ArrayType a => new Schema.ArrayType(Strip(a.ElementType, key, [.. path, "element"], changed), a.ContainsNull),
        Schema.MapType m => new Schema.MapType(
            Strip(m.KeyType, key, [.. path, "key"], changed), Strip(m.ValueType, key, [.. path, "value"], changed), m.ValueContainsNull),
        _ => type,
    };

    private static AddAction WithoutRowIds(AddAction f) => new()
    {
        Path = f.Path, PartitionValues = f.PartitionValues, Size = f.Size, ModificationTime = f.ModificationTime, DataChange = false,
        Stats = f.Stats, Tags = f.Tags, DeletionVector = f.DeletionVector, ClusteringProvider = f.ClusteringProvider,
    };

    // ------------------------------------------------------------------ the drop commit's metadata

    /// <summary>
    /// The metadata of the drop commit: the feature's remaining properties gone (row tracking's), and under checkpoint
    /// protection the boundary set to this commit's version.
    /// </summary>
    private static MetadataAction DroppedMetadata(MetadataAction metadata, string feature, long? boundary)
    {
        Dictionary<string, string> configuration = new(metadata.Configuration);
        if (feature == RowTracking.Feature)
        {
            string[] keys = [RowTracking.EnableKey, RowTracking.SuspendedKey, RowTracking.MaterializedRowIdKey, RowTracking.MaterializedRowCommitVersionKey];
            foreach (string key in keys) configuration.Remove(key);
        }
        if (boundary is long version)
            configuration[TableFeatures.RequireCheckpointProtectionBeforeVersionKey] = version.ToString(CultureInfo.InvariantCulture);
        return WithConfiguration(metadata, configuration);
    }

    private static MetadataAction WithConfiguration(MetadataAction m, IReadOnlyDictionary<string, string> configuration) => new()
    {
        Id = m.Id, Name = m.Name, Description = m.Description, FormatProvider = m.FormatProvider, FormatOptions = m.FormatOptions,
        SchemaString = m.SchemaString, PartitionColumns = m.PartitionColumns, Configuration = configuration, CreatedTime = m.CreatedTime,
    };

    private static MetadataAction WithSchema(MetadataAction m, Schema.StructType schema) => new()
    {
        Id = m.Id, Name = m.Name, Description = m.Description, FormatProvider = m.FormatProvider, FormatOptions = m.FormatOptions,
        SchemaString = schema.ToJson(), PartitionColumns = m.PartitionColumns, Configuration = m.Configuration, CreatedTime = m.CreatedTime,
    };

    private static bool Is(IReadOnlyDictionary<string, string> configuration, string key, string value) =>
        configuration.TryGetValue(key, out string? v) && string.Equals(v, value, StringComparison.OrdinalIgnoreCase);

    private static bool IsTrue(IReadOnlyDictionary<string, string> configuration, string key) => Is(configuration, key, "true");
}
