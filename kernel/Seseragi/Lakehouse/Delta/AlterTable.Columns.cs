using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// Column changes, as Spark's <c>ALTER TABLE … RENAME COLUMN</c>, <c>DROP COLUMNS</c> and <c>ALTER COLUMN … TYPE</c>:
/// metadata-only commits, no data file is rewritten. Renaming and dropping need column mapping (the files keep their
/// physical names, so a renamed column reads its old data and a dropped one is no longer read); changing a type
/// needs type widening. Columns are named as in Spark: dotted for nested fields (any case), backticks around a part
/// holding a dot, <c>element</c> for an array element and <c>key</c> / <c>value</c> for a map's.
/// </summary>
public static partial class AlterTable
{
    /// <summary>The operation name of <see cref="RenameColumn"/> in <c>commitInfo</c>, as Spark records it.</summary>
    public const string RenameColumnOperation = "RENAME COLUMN";

    /// <summary>The operation name of <see cref="DropColumns"/> in <c>commitInfo</c>, as Spark records it.</summary>
    public const string DropColumnsOperation = "DROP COLUMNS";

    /// <summary>The operation name of <see cref="ChangeColumnType"/> in <c>commitInfo</c>, as Spark records it.</summary>
    public const string ChangeColumnOperation = "CHANGE COLUMN";

    /// <summary>
    /// Renames a column (a nested field too: <paramref name="newName"/> is the new name of the last part). Partition
    /// columns and <c>delta.dataSkippingStatsColumns</c> follow; clustering columns are kept by physical name, so they
    /// need no change. Returns the version committed.
    /// </summary>
    /// <exception cref="DeltaException">The table has no column mapping, the column does not exist, or the new name
    /// is taken (names compare without regard to case, so changing only the case of a name is refused as in Spark).</exception>
    public static long RenameColumn(IWritableFileSystem fileSystem, Snapshot table, string column, string newName, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentException.ThrowIfNullOrEmpty(column);
        ArgumentException.ThrowIfNullOrEmpty(newName);
        RequireMapping(table, "Renaming a column");

        StructType schema = table.Schema;
        List<Step> steps = Resolve(schema, column);
        if (steps[^1].Kind != StepKind.Field)
            throw new DeltaException($"'{column}' names the {steps[^1].Name} of {Display(steps[..^1])}, which cannot be renamed.");
        var parent = (StructType)TypeAt(schema, steps[..^1]);
        if (parent.Fields.FirstOrDefault(f => string.Equals(f.Name, newName, StringComparison.OrdinalIgnoreCase)) is { } taken)
            throw new DeltaException($"Cannot rename '{Display(steps)}': a column '{Display([.. steps[..^1], new Step(StepKind.Field, taken.Name)])}' already exists.");

        string old = steps[^1].Name;
        var renamed = (StructType)Replace(schema, steps[..^1], 0, type => new StructType(
            [.. ((StructType)type).Fields.Select(f => f.Name == old ? Copy(f, name: newName) : f)]));
        List<Step> newSteps = [.. steps[..^1], new Step(StepKind.Field, newName)];

        IReadOnlyList<string> partitionColumns = steps.Count == 1
            ? [.. table.PartitionColumns.Select(p => string.Equals(p, old, StringComparison.OrdinalIgnoreCase) ? newName : p)]
            : table.PartitionColumns;
        var configuration = new Dictionary<string, string>(table.Metadata.Configuration);
        RewriteStatsColumns(configuration, steps, entry => [.. newSteps.Select(s => s.Name), .. entry.Skip(steps.Count)]);

        return Transaction.Start(fileSystem, table, RenameColumnOperation)
            .WithParameter("oldColumnPath", Display(steps))
            .WithParameter("newColumnPath", Display(newSteps))
            .SetMetadata(WithSchema(table.Metadata, renamed, partitionColumns, configuration))
            .Commit(now);
    }

    /// <summary>
    /// Drops columns (nested fields too) in one commit. Their data stays in the files until they are rewritten, but
    /// is no longer read; a column added later under the same name is a new column. Entries of
    /// <c>delta.dataSkippingStatsColumns</c> naming them go too. Returns the version committed, or null when
    /// <paramref name="ifExists"/> and none of the columns exists.
    /// </summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="table">The latest state of the table.</param>
    /// <param name="columns">The columns to drop.</param>
    /// <param name="ifExists">Skip columns the table does not have instead of failing.</param>
    /// <param name="now">The writer's clock; the current time when null.</param>
    /// <exception cref="DeltaException">The table has no column mapping, a column does not exist, is (or holds) a
    /// partition or clustering column, or dropping would leave the table or a struct without fields.</exception>
    public static long? DropColumns(
        IWritableFileSystem fileSystem, Snapshot table, IReadOnlyList<string> columns, bool ifExists = false, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(columns);
        if (columns.Count == 0) throw new ArgumentException("No column to drop.", nameof(columns));
        RequireMapping(table, "Dropping a column");

        StructType schema = table.Schema;
        IReadOnlyList<string[]> clustering = Clustering.PhysicalColumns(table.DomainMetadata) ?? [];
        var configuration = new Dictionary<string, string>(table.Metadata.Configuration);
        List<string> dropped = [];
        foreach (string column in columns)
        {
            ArgumentException.ThrowIfNullOrEmpty(column, nameof(columns));
            List<Step> steps;
            try
            {
                steps = Resolve(schema, column);
            }
            catch (DeltaException) when (ifExists)
            {
                continue;
            }
            string shown = Display(steps);
            if (steps[^1].Kind != StepKind.Field)
                throw new DeltaException($"'{column}' names the {steps[^1].Name} of {Display(steps[..^1])}, which cannot be dropped.");
            if (steps.Count == 1 && table.PartitionColumns.Any(p => string.Equals(p, steps[0].Name, StringComparison.OrdinalIgnoreCase)))
                throw new DeltaException($"Dropping partition columns ({shown}) is not allowed.");
            // Spark refuses dropping a clustering column but lets a struct holding one go, which leaves a table its
            // later ALTERs fail on; both are refused here.
            if (PhysicalPath(schema, steps) is { } physical
                && clustering.Any(c => c.Length >= physical.Length && c.Take(physical.Length).SequenceEqual(physical)))
                throw new DeltaException($"Dropping clustering columns ({shown}) is not allowed; change the clustering columns first (CLUSTER BY).");

            var parent = (StructType)TypeAt(schema, steps[..^1]);
            if (parent.Fields.Count == 1)
                throw new DeltaException(steps.Count == 1
                    ? $"Cannot drop '{shown}': it is the table's only column."
                    : $"Cannot drop '{shown}': it is the only field of {Display(steps[..^1])}.");
            string name = steps[^1].Name;
            schema = (StructType)Replace(schema, steps[..^1], 0, type => new StructType([.. ((StructType)type).Fields.Where(f => f.Name != name)]));
            RewriteStatsColumns(configuration, steps, _ => null);
            dropped.Add(shown);
        }
        if (dropped.Count == 0) return null;

        return Transaction.Start(fileSystem, table, DropColumnsOperation)
            .WithParameter("columns", JsonText.StringArray(dropped))
            .SetMetadata(WithSchema(table.Metadata, schema, table.PartitionColumns, configuration))
            .Commit(now);
    }

    /// <summary>
    /// Widens the type of a column, a nested field, or an array element or map key or value, as type widening allows
    /// (see <see cref="SchemaEvolution.CanWiden"/>): the change is recorded in <c>delta.typeChanges</c> and older files
    /// are read in their own type and cast. Returns the version committed, or null when the column has the type already.
    /// </summary>
    /// <exception cref="DeltaException">Type widening is not on, the column does not exist or is not of a primitive
    /// type, or the change is not a widening (or needs a table feature the table lacks).</exception>
    public static long? ChangeColumnType(IWritableFileSystem fileSystem, Snapshot table, string column, DeltaType type, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentException.ThrowIfNullOrEmpty(column);
        ArgumentNullException.ThrowIfNull(type);

        StructType schema = table.Schema;
        List<Step> steps = Resolve(schema, column);
        string shown = Display(steps);
        DeltaType current = TypeAt(schema, steps);
        if (current is StructType or ArrayType or MapType || type is StructType or ArrayType or MapType)
            throw new DeltaException($"Cannot change '{shown}' from {current} to {type}: only columns of a primitive type change their type.");
        if (current.ToString() == type.ToString()) return null;
        if (!SchemaEvolution.CanWiden(current, type))
            throw new DeltaException($"Cannot change '{shown}' from {current} to {type}: type widening does not allow it.");
        if (!SchemaEvolution.WideningEnabled(table.Metadata, table.Protocol))
            throw new DeltaException(
                $"Changing '{shown}' from {current} to {type} needs type widening: set {SchemaEvolution.EnableTypeWideningKey} to true first.");

        // The table's schema with the one type replaced, merged in as a widening write would: the change is recorded
        // where Spark records it (on the field, or on the nearest field with the path to the element, key or value).
        var incoming = (StructType)Replace(schema, steps, 0, _ => type);
        SchemaEvolution.Merge(table.Metadata, table.Protocol, incoming, evolve: true, out MetadataAction? changed);
        if (changed is null) return null;

        // Spark records the field as asked for: its path as the name, the new type, its metadata before this change.
        StructField recorded = steps[^1].Kind == StepKind.Field
            ? Copy(FieldAt(schema, steps), type: type)
            : new StructField { Name = shown, Type = type, Nullable = steps[^1].Kind != StepKind.Key && ContainsNull(TypeAt(schema, steps[..^1])) };
        return Transaction.Start(fileSystem, table, ChangeColumnOperation)
            .WithParameter("column", StructType.FieldJson(recorded, shown))
            .SetMetadata(changed)
            .Commit(now);

        static bool ContainsNull(DeltaType container) => container switch
        {
            ArrayType a => a.ContainsNull,
            MapType m => m.ValueContainsNull,
            _ => true,
        };
    }

    private enum StepKind { Field, Element, Key, Value }

    /// <summary>One part of a resolved column path: a struct field (by its name in the schema) or a container step.</summary>
    private readonly record struct Step(StepKind Kind, string Name);

    private static void RequireMapping(Snapshot table, string what)
    {
        if (table.ColumnMappingMode == ColumnMappingMode.None)
            throw new DeltaException(
                $"{what} needs column mapping: set {TableFeatures.ColumnMappingModeKey} to 'name' first (no data is rewritten).");
    }

    /// <summary>The steps of a column path in the schema; field names match without regard to case.</summary>
    private static List<Step> Resolve(StructType schema, string column)
    {
        IReadOnlyList<string> parts = ColumnNames.Split(column);
        List<Step> steps = [];
        DeltaType type = schema;
        foreach (string part in parts)
        {
            switch (type)
            {
                case StructType s:
                    StructField field = s.Fields.FirstOrDefault(f => f.Name == part)
                        ?? s.Fields.FirstOrDefault(f => string.Equals(f.Name, part, StringComparison.OrdinalIgnoreCase))
                        ?? throw new DeltaException(steps.Count == 0
                            ? $"The column '{column}' is not in the table's schema."
                            : $"The column '{column}' is not in the table's schema: {Display(steps)} has no field '{part}'.");
                    steps.Add(new Step(StepKind.Field, field.Name));
                    type = field.Type;
                    break;
                case ArrayType a when part.Equals("element", StringComparison.OrdinalIgnoreCase):
                    steps.Add(new Step(StepKind.Element, "element"));
                    type = a.ElementType;
                    break;
                case MapType m when part.Equals("key", StringComparison.OrdinalIgnoreCase):
                    steps.Add(new Step(StepKind.Key, "key"));
                    type = m.KeyType;
                    break;
                case MapType m when part.Equals("value", StringComparison.OrdinalIgnoreCase):
                    steps.Add(new Step(StepKind.Value, "value"));
                    type = m.ValueType;
                    break;
                case ArrayType:
                    throw new DeltaException($"'{column}' is not a valid column: {Display(steps)} is an array; its element is {Display(steps)}.element.");
                case MapType:
                    throw new DeltaException($"'{column}' is not a valid column: {Display(steps)} is a map; name its key or value ({Display(steps)}.key, .value).");
                default:
                    throw new DeltaException($"'{column}' is not a valid column: {Display(steps)} is not a struct.");
            }
        }
        return steps;
    }

    private static string Display(IEnumerable<Step> steps) => ColumnNames.Display(steps.Select(s => s.Name));

    private static DeltaType TypeAt(StructType schema, IReadOnlyList<Step> steps)
    {
        DeltaType type = schema;
        foreach (Step step in steps) type = Child(type, step);
        return type;
    }

    private static StructField FieldAt(StructType schema, IReadOnlyList<Step> steps) =>
        ((StructType)TypeAt(schema, [.. steps.Take(steps.Count - 1)])).Fields.First(f => f.Name == steps[^1].Name);

    private static DeltaType Child(DeltaType type, Step step) => (type, step.Kind) switch
    {
        (StructType s, StepKind.Field) => s.Fields.First(f => f.Name == step.Name).Type,
        (ArrayType a, StepKind.Element) => a.ElementType,
        (MapType m, StepKind.Key) => m.KeyType,
        (MapType m, StepKind.Value) => m.ValueType,
        _ => throw new InvalidOperationException("The path does not fit the schema."),
    };

    /// <summary>The physical names along a path of struct fields only; null when it passes an array or map.</summary>
    private static string[]? PhysicalPath(StructType schema, IReadOnlyList<Step> steps)
    {
        var physical = new string[steps.Count];
        DeltaType type = schema;
        for (int i = 0; i < steps.Count; i++)
        {
            if (steps[i].Kind != StepKind.Field) return null;
            StructField field = ((StructType)type).Fields.First(f => f.Name == steps[i].Name);
            physical[i] = field.PhysicalName;
            type = field.Type;
        }
        return physical;
    }

    /// <summary>The type with what the path leads to replaced by <paramref name="change"/> of it.</summary>
    private static DeltaType Replace(DeltaType type, IReadOnlyList<Step> steps, int index, Func<DeltaType, DeltaType> change)
    {
        if (index == steps.Count) return change(type);
        Step step = steps[index];
        return (type, step.Kind) switch
        {
            (StructType s, StepKind.Field) => new StructType(
                [.. s.Fields.Select(f => f.Name == step.Name ? Copy(f, type: Replace(f.Type, steps, index + 1, change)) : f)]),
            (ArrayType a, StepKind.Element) => new ArrayType(Replace(a.ElementType, steps, index + 1, change), a.ContainsNull),
            (MapType m, StepKind.Key) => new MapType(Replace(m.KeyType, steps, index + 1, change), m.ValueType, m.ValueContainsNull),
            (MapType m, StepKind.Value) => new MapType(m.KeyType, Replace(m.ValueType, steps, index + 1, change), m.ValueContainsNull),
            _ => throw new InvalidOperationException("The path does not fit the schema."),
        };
    }

    private static StructField Copy(StructField field, string? name = null, DeltaType? type = null) => new()
    {
        Name = name ?? field.Name, Type = type ?? field.Type, Nullable = field.Nullable, Metadata = field.Metadata,
        JsonMetadataKeys = field.JsonMetadataKeys,
    };

    /// <summary>
    /// Rewrites the entries of <c>delta.dataSkippingStatsColumns</c> at or below the column <paramref name="steps"/>
    /// names (<paramref name="rewrite"/> gives the new parts, or null to drop the entry); the property is written
    /// again as Spark writes it when an entry changed.
    /// </summary>
    private static void RewriteStatsColumns(Dictionary<string, string> configuration, IReadOnlyList<Step> steps, Func<IReadOnlyList<string>, IReadOnlyList<string>?> rewrite)
    {
        if (!configuration.TryGetValue(FileStatistics.StatsColumnsKey, out string? property)) return;
        bool changed = false;
        List<IReadOnlyList<string>> entries = [];
        foreach (IReadOnlyList<string> entry in FileStatistics.StatsColumns(property))
        {
            bool under = entry.Count >= steps.Count
                && steps.Select((s, i) => string.Equals(s.Name, entry[i], StringComparison.OrdinalIgnoreCase)).All(x => x);
            if (!under)
            {
                entries.Add(entry);
                continue;
            }
            changed = true;
            if (rewrite(entry) is { } kept) entries.Add(kept);
        }
        if (changed) configuration[FileStatistics.StatsColumnsKey] = string.Join(',', entries.Select(ColumnNames.Quoted));
    }

    private static MetadataAction WithSchema(
        MetadataAction metadata, StructType schema, IReadOnlyList<string> partitionColumns, IReadOnlyDictionary<string, string> configuration) => new()
    {
        Id = metadata.Id, Name = metadata.Name, Description = metadata.Description, FormatProvider = metadata.FormatProvider,
        FormatOptions = metadata.FormatOptions, SchemaString = schema.ToJson(), PartitionColumns = partitionColumns,
        Configuration = configuration, CreatedTime = metadata.CreatedTime,
    };
}
