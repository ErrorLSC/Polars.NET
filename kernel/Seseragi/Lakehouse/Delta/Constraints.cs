using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// A CHECK constraint or a column invariant of a table: a Spark SQL expression every row a write adds must make true
/// (a null result violates it too).
/// </summary>
/// <param name="Name">The constraint's name, as Spark reports it: a CHECK constraint's name; for a column invariant,
/// <c>EXPRESSION(('a &gt; 3))</c>.</param>
/// <param name="Expression">The expression as the table stores it.</param>
public readonly record struct DeltaConstraint(string Name, string Expression);

/// <summary>The CHECK constraints (<c>delta.constraints.&lt;name&gt;</c>) and column invariants (field metadata
/// <c>delta.invariants</c>) of a table.</summary>
public static class Constraints
{
    /// <summary>The configuration key prefix of a CHECK constraint, followed by its name.</summary>
    public const string ConfigurationPrefix = "delta.constraints.";

    /// <summary>The field metadata key of a column invariant: <c>{"expression": {"expression": "a &gt; 3"}}</c>.</summary>
    public const string InvariantsKey = "delta.invariants";

    /// <summary>
    /// The table's CHECK constraints, by name, then its column invariants, fields in schema order (nested ones too).
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
