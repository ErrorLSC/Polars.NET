using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta;

public static partial class AlterTable
{
    /// <summary>The operation name of <see cref="UnsetProperties"/>, as Spark records it.</summary>
    public const string UnsetPropertiesOperation = "UNSET TBLPROPERTIES";

    /// <summary>
    /// Removes table properties, as Spark's <c>ALTER TABLE … UNSET TBLPROPERTIES</c>: one metadata commit; a property
    /// that turned a feature on stops using it, the feature staying in the protocol (drop it with
    /// <see cref="DropFeature"/>). Properties that only another operation may change are refused, naming it: the column
    /// mapping mode (Spark rewrites the table to remove column mapping; here <c>DropFeature</c> does), CHECK
    /// constraints (<c>DropConstraint</c>), features and protocol versions (<c>DropFeature</c>), and the properties the
    /// table keeps itself. Returns the version committed, or null when there is nothing to remove.
    /// </summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="table">The latest state of the table.</param>
    /// <param name="keys">The properties; a <c>delta.</c> key matches without regard to case.</param>
    /// <param name="ifExists">Skip properties the table does not have instead of failing (Spark always skips them).</param>
    /// <param name="now">The writer's clock; the current time when null.</param>
    /// <exception cref="DeltaException">A property cannot be removed this way, or the table does not have it and
    /// <paramref name="ifExists"/> is not set.</exception>
    public static long? UnsetProperties(
        IWritableFileSystem fileSystem, Snapshot table, IReadOnlyList<string> keys, bool ifExists = false, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(keys);
        if (keys.Count == 0) throw new ArgumentException("No property to remove.", nameof(keys));

        var configuration = new Dictionary<string, string>(table.Metadata.Configuration, StringComparer.Ordinal);
        bool removed = false;
        foreach (string key in keys)
        {
            ArgumentException.ThrowIfNullOrEmpty(key, nameof(keys));
            if (UnsetRefused(key) is { } refusal) throw new DeltaException(refusal);
            string? present = configuration.ContainsKey(key) ? key
                : key.StartsWith("delta.", StringComparison.OrdinalIgnoreCase)
                    ? configuration.Keys.FirstOrDefault(k => string.Equals(k, key, StringComparison.OrdinalIgnoreCase))
                    : null;
            if (present is null)
            {
                if (ifExists) continue;
                throw new DeltaException($"The table has no property '{key}'. To skip properties the table does not have, pass ifExists.");
            }
            removed |= configuration.Remove(present);
        }
        if (!removed) return null;

        return Transaction.Start(fileSystem, table, UnsetPropertiesOperation)
            .WithParameter("properties", JsonText.StringArray(keys))
            .WithParameter("ifExists", ifExists)
            .SetMetadata(WithSchema(table.Metadata, table.Schema, table.PartitionColumns, configuration))
            .Commit(now);
    }

    /// <summary>Why a property cannot be removed by <see cref="UnsetProperties"/>; null when it can.</summary>
    private static string? UnsetRefused(string key)
    {
        bool Is(string other) => string.Equals(key, other, StringComparison.OrdinalIgnoreCase);
        if (Is(TableFeatures.ColumnMappingModeKey))
            return $"The property {key} cannot be removed: drop column mapping with Delta.DropFeature(\"columnMapping\"), which rewrites the files under their column names.";
        if (key.StartsWith(ConstraintPrefix, StringComparison.OrdinalIgnoreCase))
            return $"The property {key} cannot be removed: drop CHECK constraints with Delta.DropConstraint.";
        if (key.StartsWith(FeaturePrefix, StringComparison.OrdinalIgnoreCase) || Is(MinReaderVersionKey) || Is(MinWriterVersionKey))
            return $"The property {key} cannot be removed: the protocol is not lowered this way; drop table features with Delta.DropFeature.";
        if (Is(SchemaEvolution.MaxColumnIdKey) || Is(RowTracking.MaterializedRowIdKey) || Is(RowTracking.MaterializedRowCommitVersionKey)
            || Is(CommitTimeline.EnablementVersionKey) || Is(CommitTimeline.EnablementTimestampKey)
            || Is(TableFeatures.RequireCheckpointProtectionBeforeVersionKey))
            return $"The property {key} cannot be removed: the table keeps it itself.";
        return null;
    }
}
