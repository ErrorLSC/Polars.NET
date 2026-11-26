using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>A column to add with <see cref="AlterTable.AddColumns"/>.</summary>
/// <param name="Column">The column's path: its name, or dotted for a field of a struct (<c>address.zip</c>, with
/// <c>element</c> / <c>key</c> / <c>value</c> through arrays and maps: <c>items.element.price</c>); a part holding a dot
/// in backticks. Every part but the last names what exists.</param>
/// <param name="Type">Its type; the column is nullable (Delta adds no NOT NULL column).</param>
/// <param name="Comment">Its comment; null for none.</param>
/// <param name="After">The field of the same struct to place it after (its name only); null to place it last, unless
/// <paramref name="First"/>.</param>
/// <param name="First">Place it first in its struct.</param>
public readonly record struct NewColumn(string Column, DeltaType Type, string? Comment = null, string? After = null, bool First = false);

public static partial class AlterTable
{
    /// <summary>The operation name of <see cref="AddColumns"/>, as Spark records it.</summary>
    public const string AddColumnsOperation = "ADD COLUMNS";

    /// <summary>
    /// Adds columns (nested fields too) in one metadata commit, as Spark's <c>ALTER TABLE … ADD COLUMNS</c>: rows already
    /// written read null for them. Under column mapping they get ids and physical names. A column holding timestamps
    /// without time zone brings the timestampNtz feature into the protocol (Spark asks for it first). Returns the
    /// version committed.
    /// </summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="table">The latest state of the table.</param>
    /// <param name="columns">The columns, added in order (a later one may be placed after an earlier one).</param>
    /// <param name="now">The writer's clock; the current time when null.</param>
    /// <exception cref="DeltaException">A column exists already (names compare without regard to case), its parent is
    /// not a struct or does not exist, or <c>After</c> names no field of its struct.</exception>
    public static long AddColumns(IWritableFileSystem fileSystem, Snapshot table, IReadOnlyList<NewColumn> columns, DateTimeOffset? now = null)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(columns);
        if (columns.Count == 0) throw new ArgumentException("No column to add.", nameof(columns));

        StructType schema = table.Schema;
        List<string> recorded = [];
        foreach (NewColumn column in columns)
        {
            ArgumentException.ThrowIfNullOrEmpty(column.Column, nameof(columns));
            ArgumentNullException.ThrowIfNull(column.Type, nameof(columns));
            if (column.First && column.After is not null)
                throw new ArgumentException($"The column '{column.Column}' is placed both first and after '{column.After}'.", nameof(columns));
            IReadOnlyList<string> parts = ColumnNames.Split(column.Column);
            string name = parts[^1];
            List<Step> parentSteps = parts.Count == 1 ? [] : Resolve(schema, [.. parts.Take(parts.Count - 1)], column.Column);
            if (TypeAt(schema, parentSteps) is not StructType parent)
                throw new DeltaException($"Cannot add '{column.Column}': {Display(parentSteps)} is not a struct.");
            string shown = Display([.. parentSteps, new Step(StepKind.Field, name)]);
            if (parent.Fields.FirstOrDefault(f => string.Equals(f.Name, name, StringComparison.OrdinalIgnoreCase)) is { } taken)
                throw new DeltaException($"Cannot add column '{shown}': '{Display([.. parentSteps, new Step(StepKind.Field, taken.Name)])}' already exists.");

            StructField? anchor = column.After is null ? null
                : parent.Fields.FirstOrDefault(f => f.Name == column.After)
                  ?? parent.Fields.FirstOrDefault(f => string.Equals(f.Name, column.After, StringComparison.OrdinalIgnoreCase))
                  ?? throw new DeltaException(parentSteps.Count == 0
                      ? $"Cannot add '{shown}' after '{column.After}': the table has no column '{column.After}'."
                      : $"Cannot add '{shown}' after '{column.After}': {Display(parentSteps)} has no field '{column.After}'.");
            var field = new StructField
            {
                Name = name, Type = column.Type, Nullable = true,
                Metadata = column.Comment is null ? EmptyMap.Instance : new Dictionary<string, string> { [CommentKey] = column.Comment },
            };
            schema = (StructType)Replace(schema, parentSteps, 0, type =>
            {
                List<StructField> fields = [.. ((StructType)type).Fields];
                fields.Insert(column.First ? 0 : anchor is null ? fields.Count : fields.IndexOf(anchor) + 1, field);
                return new StructType(fields);
            });
            string? position = column.First ? "FIRST" : anchor is null ? null : $"AFTER {anchor.Name}";
            string entry = $"{{\"column\":{StructType.FieldJson(field, shown, typeName: true)}";
            recorded.Add(position is null ? entry + "}" : $"{entry},\"position\":\"{JsonEncodedText.Encode(position)}\"}}");
        }

        MetadataAction metadata = SchemaEvolution.WithAddedFields(table.Metadata, schema);
        StructType added = StructType.Parse(metadata.SchemaString);
        ProtocolAction protocol = SchemaEvolution.HasNtz(added) && !TableFeatures.WriterFeaturesOf(table.Protocol).Contains(TableFeatures.TimestampNtz, StringComparer.Ordinal)
            ? TableFeatures.WithFeatures(table.Protocol, [TableFeatures.TimestampNtz])
            : table.Protocol;
        Transaction transaction = Transaction.Start(fileSystem, table, AddColumnsOperation)
            .WithParameter("columns", "[" + string.Join(",", recorded) + "]")
            .SetMetadata(metadata);
        if (!ReferenceEquals(protocol, table.Protocol)) transaction.SetProtocol(protocol);
        return transaction.Commit(now);
    }
}
