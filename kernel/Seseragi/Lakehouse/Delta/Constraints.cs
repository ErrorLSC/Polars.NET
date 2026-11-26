using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// A CHECK constraint, a column invariant or a generated column of a table: a Spark SQL expression every row a write
/// adds must make true (a null result violates it too).
/// </summary>
/// <param name="Name">The constraint's name, as Spark reports it: a CHECK constraint's name; for a column invariant,
/// <c>EXPRESSION(('a &gt; 3))</c>; for a generated column, <c>Generated Column</c>.</param>
/// <param name="Expression">The expression as Spark shows it: as the table stores it; for a generated column,
/// <c>u &lt;=&gt; upper(s)</c>.</param>
/// <param name="Bound">The text the expression is bound from where it differs from <paramref name="Expression"/> (a
/// generated column's, its parts quoted and parenthesized); null otherwise.</param>
public readonly record struct DeltaConstraint(string Name, string Expression, string? Bound = null)
{
    /// <summary>The text to bind: <see cref="Bound"/>, else <see cref="Expression"/>.</summary>
    public string Text => Bound ?? Expression;
}

/// <summary>A generated column: its value is the expression over the row's other columns.</summary>
/// <param name="Column">The column, as the table names it (a top-level column).</param>
/// <param name="Expression">The generation expression as the table stores it.</param>
public readonly record struct DeltaGeneratedColumn(string Column, string Expression);

/// <summary>The CHECK constraints (<c>delta.constraints.&lt;name&gt;</c>) and column invariants (field metadata
/// <c>delta.invariants</c>) of a table.</summary>
public static class Constraints
{
    /// <summary>The configuration key prefix of a CHECK constraint, followed by its name.</summary>
    public const string ConfigurationPrefix = "delta.constraints.";

    /// <summary>The field metadata key of a column invariant: <c>{"expression": {"expression": "a &gt; 3"}}</c>.</summary>
    public const string InvariantsKey = "delta.invariants";

    /// <summary>The field metadata key of a generated column's expression.</summary>
    public const string GenerationExpressionKey = "delta.generationExpression";

    /// <summary>The name Spark reports a generated column's check under.</summary>
    public const string GeneratedColumnName = "Generated Column";

    /// <summary>The table's generated columns, in schema order.</summary>
    /// <exception cref="DeltaProtocolException">A nested field is generated (Delta generates top-level columns).</exception>
    public static IReadOnlyList<DeltaGeneratedColumn> GeneratedColumns(StructType schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        foreach (StructField field in schema.Fields)
            if (HasNestedGenerated(field.Type))
                throw new DeltaProtocolException($"A field inside '{field.Name}' is a generated column; Delta generates top-level columns only.");
        return [.. schema.Fields.Where(f => f.Metadata.ContainsKey(GenerationExpressionKey)).Select(f => new DeltaGeneratedColumn(f.Name, f.Metadata[GenerationExpressionKey]))];

        static bool HasNestedGenerated(DeltaType type) => type switch
        {
            StructType s => s.Fields.Any(f => f.Metadata.ContainsKey(GenerationExpressionKey) || HasNestedGenerated(f.Type)),
            ArrayType a => HasNestedGenerated(a.ElementType),
            MapType m => HasNestedGenerated(m.KeyType) || HasNestedGenerated(m.ValueType),
            _ => false,
        };
    }

    /// <summary>
    /// The table's CHECK constraints, by name, then its column invariants, fields in schema order (nested ones too),
    /// then its generated columns, each as the check <c>column &lt;=&gt; expression</c> (Spark's).
    /// </summary>
    /// <exception cref="DeltaProtocolException">An invariant is not of the form Delta writes, or sits inside an array
    /// or a map.</exception>
    public static IReadOnlyList<DeltaConstraint> Of(MetadataAction metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        return Of(StructType.Parse(metadata.SchemaString), metadata.Configuration);
    }

    /// <inheritdoc cref="Of(MetadataAction)"/>
    public static IReadOnlyList<DeltaConstraint> Of(StructType schema, IReadOnlyDictionary<string, string> configuration)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(configuration);
        List<DeltaConstraint> constraints =
        [
            .. configuration
                .Where(p => p.Key.StartsWith(ConfigurationPrefix, StringComparison.Ordinal))
                .OrderBy(p => p.Key, StringComparer.Ordinal)
                .Select(p => new DeltaConstraint(p.Key[ConfigurationPrefix.Length..], p.Value)),
        ];
        AddInvariants(schema, inCollection: false, constraints);
        foreach (DeltaGeneratedColumn generated in GeneratedColumns(schema))
            constraints.Add(new DeltaConstraint(
                GeneratedColumnName, $"{generated.Column} <=> {generated.Expression}",
                $"`{generated.Column.Replace("`", "``")}` <=> ({generated.Expression})"));
        return constraints;
    }

    private static void AddInvariants(StructType type, bool inCollection, List<DeltaConstraint> constraints)
    {
        foreach (StructField field in type.Fields)
        {
            if (field.Metadata.TryGetValue(InvariantsKey, out string? json))
            {
                if (inCollection)
                    throw new DeltaProtocolException($"The column invariant of '{field.Name}' sits inside an array or a map, which this kernel cannot enforce.");
                string expression = InvariantExpression(field.Name, json);
                constraints.Add(new DeltaConstraint($"EXPRESSION(('{expression}))", expression));
            }
            Nested(field.Type, inCollection, constraints);
        }

        static void Nested(DeltaType type, bool inCollection, List<DeltaConstraint> constraints)
        {
            switch (type)
            {
                case StructType s: AddInvariants(s, inCollection, constraints); break;
                case ArrayType a: Nested(a.ElementType, true, constraints); break;
                case MapType m:
                    Nested(m.KeyType, true, constraints);
                    Nested(m.ValueType, true, constraints);
                    break;
            }
        }
    }

    private static string InvariantExpression(string field, string json)
    {
        try
        {
            using JsonDocument doc = JsonDocument.Parse(json);
            if (doc.RootElement.TryGetProperty("expression", out JsonElement outer)
                && outer.ValueKind == JsonValueKind.Object
                && outer.TryGetProperty("expression", out JsonElement text)
                && text.ValueKind == JsonValueKind.String)
                return text.GetString()!;
        }
        catch (JsonException)
        {
        }
        throw new DeltaProtocolException($"The column invariant of '{field}' is not of the form {{\"expression\": {{\"expression\": \"...\"}}}}: {json}");
    }
}
