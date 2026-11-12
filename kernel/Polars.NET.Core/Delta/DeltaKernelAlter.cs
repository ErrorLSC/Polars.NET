using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Parquet;
using Polars.NET.Core.Exceptions;

namespace Polars.NET.Core.Delta;

/// <summary>
/// Table properties, table features and columns of a Delta table through the managed kernel (see
/// <see cref="AlterTable"/>). The public Delta API calls it.
/// </summary>
internal static class DeltaKernelAlter
{
    /// <summary>Sets table properties; returns the version committed, or null when nothing changes.</summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="properties">The properties to set.</param>
    /// <param name="raiseIfNotExists">Refuse a <c>delta.</c> property Delta does not define.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static long? SetTableProperties(
        string tablePath, IReadOnlyDictionary<string, string> properties, bool raiseIfNotExists = false, CloudArgs? cloud = null)
    {
        ArgumentNullException.ThrowIfNull(properties);
        if (properties.Count == 0) throw new ArgumentException("No property to set.", nameof(properties));
        return Run(tablePath, cloud, table => AlterTable.SetProperties(table.FileSystem, table.Snapshot, properties, raiseIfNotExists));
    }

    /// <summary>
    /// Adds a table feature (deletion vectors, the change data feed and append-only are turned on too); returns the
    /// version committed, or null when the table has it already.
    /// </summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="feature">The feature name.</param>
    /// <param name="allowProtocolIncrease">Allow raising the table's reader or writer version.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static long? AddFeature(string tablePath, string feature, bool allowProtocolIncrease = true, CloudArgs? cloud = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(feature);
        return Run(tablePath, cloud, table => AlterTable.AddFeatures(table.FileSystem, table.Snapshot, [feature], allowProtocolIncrease));
    }

    /// <summary>
    /// Changes the clustering columns (liquid clustering; empty for none); returns the version committed, or null
    /// when the table was not clustered and none are given.
    /// </summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="columns">The clustering columns, dotted for nested fields; at most four.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static long? ClusterBy(string tablePath, IReadOnlyList<string> columns, CloudArgs? cloud = null)
    {
        ArgumentNullException.ThrowIfNull(columns);
        return Run(tablePath, cloud, table => AlterTable.ClusterBy(table.FileSystem, table.Snapshot, columns));
    }

    /// <summary>Renames a column (dotted for a nested field); returns the version committed. Needs column mapping.</summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="column">The column, dotted for a nested field (<c>element</c>, <c>key</c>, <c>value</c> inside arrays and maps).</param>
    /// <param name="newName">The new name of the column (of its last part when nested).</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static long RenameColumn(string tablePath, string column, string newName, CloudArgs? cloud = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(column);
        ArgumentException.ThrowIfNullOrEmpty(newName);
        return Run(tablePath, cloud, table => AlterTable.RenameColumn(table.FileSystem, table.Snapshot, column, newName))!.Value;
    }

    /// <summary>
    /// Drops columns in one commit; returns the version committed, or null when <paramref name="ifExists"/> and none
    /// exists. Needs column mapping.
    /// </summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="columns">The columns, dotted for nested fields.</param>
    /// <param name="ifExists">Skip columns the table does not have.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static long? DropColumns(string tablePath, IReadOnlyList<string> columns, bool ifExists = false, CloudArgs? cloud = null)
    {
        ArgumentNullException.ThrowIfNull(columns);
        return Run(tablePath, cloud, table => AlterTable.DropColumns(table.FileSystem, table.Snapshot, columns, ifExists));
    }

    /// <summary>
    /// Widens a column's type to the Delta type that holds <paramref name="type"/> (the type a write of such data
    /// gives the column); returns the version committed, or null when the column has it already. Needs type widening.
    /// </summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="column">The column, dotted for a nested field (<c>element</c>, <c>key</c>, <c>value</c> inside arrays and maps).</param>
    /// <param name="type">The engine type.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static long? ChangeColumnType(string tablePath, string column, DataTypeHandle type, CloudArgs? cloud = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(column);
        ArgumentNullException.ThrowIfNull(type);
        Kaminari.Seseragi.Lakehouse.Delta.Schema.DeltaType deltaType = DeltaKernelWrite.DeltaTypeOf(type, column);
        return Run(tablePath, cloud, table => AlterTable.ChangeColumnType(table.FileSystem, table.Snapshot, column, deltaType));
    }

    /// <summary>Lets a column hold nulls (Spark's <c>ALTER COLUMN … DROP NOT NULL</c>); returns the version committed.</summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="column">The column, dotted for nested fields (<c>element</c> / <c>value</c> for an array's or map's).</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static long DropNotNull(string tablePath, string column, CloudArgs? cloud = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(column);
        return Run(tablePath, cloud, table => AlterTable.DropNotNull(table.FileSystem, table.Snapshot, column))!.Value;
    }

    /// <summary>Sets a column's comment (Spark's <c>ALTER COLUMN … COMMENT</c>); returns the version committed.</summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="column">The column, dotted for nested fields.</param>
    /// <param name="comment">The comment; empty is kept as empty.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static long SetComment(string tablePath, string column, string comment, CloudArgs? cloud = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(column);
        ArgumentNullException.ThrowIfNull(comment);
        return Run(tablePath, cloud, table => AlterTable.SetComment(table.FileSystem, table.Snapshot, column, comment))!.Value;
    }

    /// <summary>Moves a column (Spark's <c>ALTER COLUMN … FIRST / AFTER</c>); returns the version committed.</summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="column">The column, dotted for nested fields.</param>
    /// <param name="after">The field of the same struct to place it after (its name only); null for the first place.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    public static long MoveColumn(string tablePath, string column, string? after, CloudArgs? cloud = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(column);
        return Run(tablePath, cloud, table => AlterTable.MoveColumn(table.FileSystem, table.Snapshot, column, after))!.Value;
    }

    /// <summary>
    /// Drops a table feature, as Spark's <c>ALTER TABLE … DROP FEATURE</c> (see <see cref="Kaminari.Seseragi.Lakehouse.Delta.DropFeature"/>);
    /// returns the version of the commit that dropped it.
    /// </summary>
    /// <param name="tablePath">The table location: a local directory or a cloud URL.</param>
    /// <param name="feature">The feature name, any case.</param>
    /// <param name="cloud">Cloud settings for a cloud URL.</param>
    /// <param name="truncateHistory">Spark's <c>TRUNCATE HISTORY</c>: delete the history instead of protecting it.</param>
    public static long DropFeature(string tablePath, string feature, CloudArgs? cloud = null, bool truncateHistory = false)
    {
        ArgumentException.ThrowIfNullOrEmpty(feature);
        try
        {
            // The drop itself writes log files only; each data rewrite opens the table on its own, so its rollback
            // covers its own commit and never the drop's other commits.
            using DmlTable table = DmlTable.Open(tablePath, cloud);
            EnsureRewritable(table, feature);
            var parquet = new EngineParquetReader(table.Root, cloud);
            return Kaminari.Seseragi.Lakehouse.Delta.DropFeature.Run(
                table.FileSystem, () => Snapshot.Load(table.FileSystem, null, parquet), feature, new EngineRewriter(tablePath, cloud),
                truncateHistory: truncateHistory, snapshotAt: version => Snapshot.Load(table.FileSystem, version, parquet),
                afterCommit: version => DeltaKernelWrite.TryCheckpoint(table.FileSystem, tablePath, version, cloud));
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }

        // Refuses, before anything is committed, a drop whose data rewrite could not write the table's files (map
        // columns; see DeltaKernelWrite.EnsureNoMaps).
        static void EnsureRewritable(DmlTable table, string feature)
        {
            Snapshot snapshot = table.Snapshot;
            if (!snapshot.Schema.Fields.Any(f => !snapshot.PartitionColumns.Contains(f.Name) && DeltaKernelWrite.HasMap(f.Type))) return;
            bool rewrites = feature.ToLowerInvariant() switch
            {
                "deletionvectors" => DeltaKernelOptimize.PurgeFiles(table).Count > 0,
                "typewidening" or "typewidening-preview" =>
                    snapshot.Files.Count > 0 && snapshot.Metadata.SchemaString.Contains(SchemaEvolution.TypeChangesKey, StringComparison.Ordinal),
                "columnmapping" => snapshot.Files.Count > 0 && snapshot.ColumnMappingMode != ColumnMappingMode.None,
                _ => false,
            };
            if (rewrites) DeltaKernelWrite.EnsureNoMaps(snapshot.Schema.Fields.Where(f => !snapshot.PartitionColumns.Contains(f.Name)));
        }
    }

    /// <summary>The data rewrites of a drop, through the engine; each one opens the table at its latest version.</summary>
    private sealed class EngineRewriter(string tablePath, CloudArgs? cloud) : IFeatureRewriter
    {
        public long? PurgeDeletionVectors() => DeltaKernelOptimize.Reorg(tablePath, cloud, DeltaKernelOptimize.PurgeFiles)?.Version;

        public long? RewriteWidenedFiles() => DeltaKernelOptimize.Reorg(tablePath, cloud, Widened)?.Version;

        /// <summary>
        /// Spark's REMOVE COLUMN MAPPING: every file rewritten under the logical column names (one by one, deleted
        /// rows left out, row ids kept), committed with the table's metadata without column mapping, as data that
        /// did not change.
        /// </summary>
        public long? RemoveColumnMapping() => ConflictRetry.Run(() =>
        {
            try
            {
                using DmlTable dml = DmlTable.Open(tablePath, cloud);
                Snapshot snapshot = dml.Snapshot;
                if (snapshot.ColumnMappingMode == ColumnMappingMode.None) return (long?)null;
                var (metadata, clustering) = Kaminari.Seseragi.Lakehouse.Delta.DropFeature.WithoutColumnMapping(snapshot);
                StructType schema = StructType.Parse(metadata.SchemaString);

                var plan = new DmlPlan(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                foreach (AddAction file in snapshot.Files)
                {
                    // Placed and given statistics as the new metadata says: without column mapping, files go back to
                    // partition directories unless the table randomizes file prefixes (as Spark).
                    foreach (AddAction add in DeltaKernelWrite.WriteFiles(
                        dml, dml.ScanFile(file), schema, plan.Now, checkNotNull: false, properties: metadata.Configuration))
                        plan.Adds.Add(DeltaKernelOptimize.Rearranged(add));
                    plan.Remove(file, dataChange: false);
                }

                Transaction transaction = Transaction.Start(dml.FileSystem, snapshot, "REMOVE COLUMN MAPPING")
                    .ReadFiles(snapshot.Files)
                    .SetMetadata(metadata)
                    .WithMetric("numOutputBytes", plan.Adds.Sum(a => a.Size));
                if (clustering is not null) transaction.Add(clustering);
                foreach (RemoveAction remove in plan.Removes) transaction.Add(remove);
                foreach (AddAction add in plan.Adds) transaction.Add(add);
                long version = transaction.Commit();
                DeltaKernelWrite.TryCheckpoint(dml.FileSystem, tablePath, version, cloud);
                return version;
            }
            catch (DeltaException e)
            {
                throw new PolarsException(e.Message, e);
            }
        });

        /// <summary>The files holding a column in a type older than the table's, found by their footers.</summary>
        private static List<AddAction> Widened(DmlTable table)
        {
            Snapshot snapshot = table.Snapshot;
            IReadOnlyList<AddAction> files = snapshot.Files;
            (long Size, ParquetMetadata Footer)[] footers = ParquetFooters.Read(table.FileSystem, [.. files.Select(f => f.DecodedPath)]);
            return [.. files.Where((_, i) => FileTypes.HasOlderTypes(snapshot.Schema, snapshot.PartitionColumns, footers[i].Footer))];
        }
    }

    private static long? Run(string tablePath, CloudArgs? cloud, Func<DmlTable, long?> alter)
    {
        try
        {
            using DmlTable table = DmlTable.Open(tablePath, cloud);
            long? version = alter(table);
            if (version is long committed) DeltaKernelWrite.TryCheckpoint(table.FileSystem, tablePath, committed, cloud);
            return version;
        }
        catch (DeltaException e)
        {
            throw new PolarsException(e.Message, e);
        }
    }
}
