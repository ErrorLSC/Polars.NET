using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// CHECK constraints and column defaults, as Spark's <c>ALTER TABLE … ADD / DROP CONSTRAINT</c> and
/// <c>ALTER COLUMN … SET / DROP DEFAULT</c>: metadata commits only. The expressions are stored as given (trimmed);
/// whether they are valid, and whether the table's rows meet a new constraint, the caller checks against the same
/// <see cref="Snapshot"/> before committing (a concurrent commit then makes this one conflict).
/// </summary>
public static partial class AlterTable
{
    /// <summary>The operation name of adding a CHECK constraint, as Spark records it.</summary>
    public const string AddConstraintOperation = "ADD CONSTRAINT";

    /// <summary>The operation name of dropping a CHECK constraint, as Spark records it.</summary>
    public const string DropConstraintOperation = "DROP CONSTRAINT";

    /// <summary>
    /// Adds a CHECK constraint: <c>delta.constraints.&lt;name&gt;</c> (the name lower-cased, as Spark keys it) set to the
    /// expression, and the checkConstraints feature brought into the protocol (writer 3 on a legacy protocol, as
    /// Spark). Returns the version committed.
    /// </summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="table">The state of the table the caller checked the expression and the rows against.</param>
    /// <param name="name">The constraint's name, any case; not empty.</param>
    /// <param name="expression">The expression, stored as given (trimmed).</param>
    /// <param name="now">The writer's clock; the current time when null.</param>
    /// <exception cref="DeltaException">The table has a constraint of that name (in any case), or the name is empty.</exception>
    public static long AddConstraint(IWritableFileSystem fileSystem, Snapshot table, string name, string expression, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(expression);
        string key = ConstraintKey(name);
        if (ConstraintKeyOf(table.Metadata.Configuration, name) is not null)
            throw new DeltaException($"Constraint '{name.ToLowerInvariant()}' already exists. Please delete the old constraint first.");
        string text = expression.Trim();
        if (text.Length == 0) throw new DeltaException($"The CHECK constraint '{name}' has no expression.");

        var configuration = new Dictionary<string, string>(table.Metadata.Configuration, StringComparer.Ordinal) { [key] = text };
        ProtocolAction protocol = TableFeatures.WriterFeaturesOf(table.Protocol).Contains(TableFeatures.CheckConstraints, StringComparer.Ordinal)
            ? table.Protocol
            : table.Protocol.MinWriterVersion < 7
                ? new ProtocolAction { MinReaderVersion = table.Protocol.MinReaderVersion, MinWriterVersion = 3 }
                : TableFeatures.WithFeatures(table.Protocol, [TableFeatures.CheckConstraints]);
        Transaction transaction = Transaction.Start(fileSystem, table, AddConstraintOperation)
            .WithParameter("name", name)
            .WithParameter("expr", text)
            // Every row was read: a concurrent commit adding rows conflicts (they were not checked).
            .ReadPredicate(null)
            .SetMetadata(WithSchema(table.Metadata, table.Schema, table.PartitionColumns, configuration));
        if (!ReferenceEquals(protocol, table.Protocol)) transaction.SetProtocol(protocol);
        return transaction.Commit(now);
    }

    /// <summary>
    /// Drops a CHECK constraint, matched by name without regard to case. Returns the version committed, or null when
    /// the table has no such constraint and <paramref name="ifExists"/> is set (nothing is committed then). The
    /// checkConstraints feature stays in the protocol (drop it with <see cref="DropFeature"/>).
    /// </summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="table">The latest state of the table.</param>
    /// <param name="name">The constraint's name, any case.</param>
    /// <param name="ifExists">Skip a missing constraint instead of failing.</param>
    /// <param name="now">The writer's clock; the current time when null.</param>
    /// <exception cref="DeltaException">The table has no such constraint and <paramref name="ifExists"/> is not set.</exception>
    public static long? DropConstraint(IWritableFileSystem fileSystem, Snapshot table, string name, bool ifExists = false, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(name);
        string? key = ConstraintKeyOf(table.Metadata.Configuration, name);
        if (key is null)
        {
            if (ifExists) return null;
            throw new DeltaException(
                $"Cannot drop nonexistent constraint {name} from the table. To skip a constraint the table does not have, pass ifExists.");
        }
        string expression = table.Metadata.Configuration[key];
        var configuration = table.Metadata.Configuration.Where(p => p.Key != key).ToDictionary(StringComparer.Ordinal);
        return Transaction.Start(fileSystem, table, DropConstraintOperation)
            .WithParameter("name", name)
            .WithParameter("expr", expression)
            .WithParameter("existed", "true")
            .SetMetadata(WithSchema(table.Metadata, table.Schema, table.PartitionColumns, configuration))
            .Commit(now);
    }

    /// <summary>
    /// Sets a column's default (field metadata <c>CURRENT_DEFAULT</c>), bringing the allowColumnDefaults feature into
    /// the protocol when the table has none (Spark asks for the feature first). Returns the version committed.
    /// </summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="table">The state of the table the caller checked the expression against.</param>
    /// <param name="column">A top-level column, any case.</param>
    /// <param name="expression">The default, stored as given (trimmed).</param>
    /// <param name="now">The writer's clock; the current time when null.</param>
    /// <exception cref="DeltaException">The column does not exist, is nested, or is a generated or identity column.</exception>
    public static long SetColumnDefault(IWritableFileSystem fileSystem, Snapshot table, string column, string expression, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(expression);
        StructField field = DefaultableColumn(table.Schema, column);
        string text = expression.Trim();
        if (text.Length == 0) throw new DeltaException($"The default of the column {field.Name} has no expression.");
        StructField changed = Copy(field, metadata: new Dictionary<string, string>(field.Metadata) { [Constraints.CurrentDefaultKey] = text });
        ProtocolAction protocol = TableFeatures.WithFeatures(table.Protocol, [TableFeatures.AllowColumnDefaults]);
        Transaction transaction = Transaction.Start(fileSystem, table, ChangeColumnOperation)
            .WithParameter("column", StructType.FieldJson(changed, field.Name, typeName: true))
            .SetMetadata(WithSchema(table.Metadata, WithTopLevel(table.Schema, changed), table.PartitionColumns, table.Metadata.Configuration));
        if (!ReferenceEquals(protocol, table.Protocol)) transaction.SetProtocol(protocol);
        return transaction.Commit(now);
    }

    /// <summary>
    /// Drops a column's default. Returns the version committed, or null when the column has none (nothing is
    /// committed then; Spark commits).
    /// </summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="table">The latest state of the table.</param>
    /// <param name="column">A top-level column, any case.</param>
    /// <param name="now">The writer's clock; the current time when null.</param>
    /// <exception cref="DeltaException">The column does not exist or is nested.</exception>
    public static long? DropColumnDefault(IWritableFileSystem fileSystem, Snapshot table, string column, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(table);
        StructField field = TopLevelColumn(table.Schema, column);
        if (!field.Metadata.ContainsKey(Constraints.CurrentDefaultKey)) return null;
        StructField changed = Copy(field, metadata: field.Metadata.Where(p => p.Key != Constraints.CurrentDefaultKey).ToDictionary(StringComparer.Ordinal));
        return Transaction.Start(fileSystem, table, ChangeColumnOperation)
            .WithParameter("column", StructType.FieldJson(changed, field.Name, typeName: true))
            .SetMetadata(WithSchema(table.Metadata, WithTopLevel(table.Schema, changed), table.PartitionColumns, table.Metadata.Configuration))
            .Commit(now);
    }

    /// <summary>
    /// The refusal of a <c>delta.constraints.*</c> table property set directly (by setting properties or creating a
    /// table), as Spark refuses the former: a constraint is added with its own operation, which checks it.
    /// </summary>
    public static string ConstraintPropertyRefused(string key) =>
        $"The table property {key} cannot be set directly: add CHECK constraints with Delta.AddConstraint, which checks them.";

    /// <summary>The configuration key a constraint of this name is added under: the name lower-cased, as Spark.</summary>
    /// <exception cref="DeltaException">The name is empty.</exception>
    public static string ConstraintKey(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        if (name.Trim().Length == 0) throw new DeltaException("A CHECK constraint needs a name.");
        return ConstraintPrefix + name.ToLowerInvariant();
    }

    /// <summary>The key of the table's constraint of this name, matched without regard to case; null when there is none.</summary>
    private static string? ConstraintKeyOf(IReadOnlyDictionary<string, string> configuration, string name)
    {
        string wanted = ConstraintKey(name);
        return configuration.Keys.FirstOrDefault(k => string.Equals(k, wanted, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>A top-level column that may take a default: not a generated or an identity column.</summary>
    /// <exception cref="DeltaException">The column does not exist, is nested, or is generated or an identity column.</exception>
    public static StructField DefaultableColumn(StructType schema, string column)
    {
        StructField field = TopLevelColumn(schema, column);
        if (field.Metadata.ContainsKey(Constraints.GenerationExpressionKey))
            throw new DeltaException($"The column {field.Name} is a generated column, which cannot also have a default value.");
        if (field.Metadata.ContainsKey(IdentityColumns.StartKey))
            throw new DeltaException($"The column {field.Name} is an identity column, which cannot also have a default value.");
        return field;
    }

    /// <summary>A top-level column, matched exactly, else without regard to case.</summary>
    /// <exception cref="DeltaException">The table has no such top-level column.</exception>
    private static StructField TopLevelColumn(StructType schema, string column)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentException.ThrowIfNullOrEmpty(column);
        return schema.Fields.FirstOrDefault(f => f.Name == column)
            ?? schema.Fields.FirstOrDefault(f => string.Equals(f.Name, column, StringComparison.OrdinalIgnoreCase))
            ?? throw new DeltaException(column.Contains('.', StringComparison.Ordinal)
                ? $"The table has no top-level column '{column}': only top-level columns have defaults."
                : $"The table has no column '{column}'.");
    }

    private static StructType WithTopLevel(StructType schema, StructField field) =>
        new([.. schema.Fields.Select(f => f.Name == field.Name ? field : f)]);
}
