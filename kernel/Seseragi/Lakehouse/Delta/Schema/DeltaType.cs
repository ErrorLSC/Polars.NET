using System.Text.Json;

namespace Kaminari.Seseragi.Lakehouse.Delta.Schema;

/// <summary>A data type of the Delta schema.</summary>
public abstract class DeltaType
{
    private protected DeltaType() { }

    /// <summary>The type as the schema JSON writes it when it is a plain name, else a readable rendering.</summary>
    public abstract override string ToString();
}

/// <summary>A type written as a bare name: <c>string</c>, <c>long</c>, <c>timestamp_ntz</c> and so on.</summary>
public sealed class PrimitiveType : DeltaType
{
#pragma warning disable CS1591
    public const string String = "string";
    public const string Long = "long";
    public const string Integer = "integer";
    public const string Short = "short";
    public const string Byte = "byte";
    public const string Float = "float";
    public const string Double = "double";
    public const string Boolean = "boolean";
    public const string Binary = "binary";
    public const string Date = "date";
    public const string Timestamp = "timestamp";
    public const string TimestampNtz = "timestamp_ntz";
    public const string Variant = "variant";
#pragma warning restore CS1591

    /// <summary>The type name. A name this kernel does not know is kept as it is.</summary>
    public string Name { get; }

    /// <summary>Creates a primitive type by name.</summary>
    public PrimitiveType(string name) => Name = name;

    /// <inheritdoc/>
    public override string ToString() => Name;
}

/// <summary>A fixed-point decimal.</summary>
public sealed class DecimalType(int precision, int scale) : DeltaType
{
    /// <summary>Total number of digits.</summary>
    public int Precision { get; } = precision;
    /// <summary>Digits after the decimal point.</summary>
    public int Scale { get; } = scale;

    /// <inheritdoc/>
    public override string ToString() => $"decimal({Precision},{Scale})";
}

/// <summary>An array of one element type.</summary>
public sealed class ArrayType(DeltaType elementType, bool containsNull) : DeltaType
{
    /// <summary>The element type.</summary>
    public DeltaType ElementType { get; } = elementType;
    /// <summary>Whether elements may be null.</summary>
    public bool ContainsNull { get; } = containsNull;

    /// <inheritdoc/>
    public override string ToString() => $"array<{ElementType}>";
}

/// <summary>A map from one key type to one value type.</summary>
public sealed class MapType(DeltaType keyType, DeltaType valueType, bool valueContainsNull) : DeltaType
{
    /// <summary>The key type.</summary>
    public DeltaType KeyType { get; } = keyType;
    /// <summary>The value type.</summary>
    public DeltaType ValueType { get; } = valueType;
    /// <summary>Whether values may be null.</summary>
    public bool ValueContainsNull { get; } = valueContainsNull;

    /// <inheritdoc/>
    public override string ToString() => $"map<{KeyType},{ValueType}>";
}

/// <summary>A field of a struct.</summary>
public sealed class StructField
{
    /// <summary>The metadata key holding the physical column name under column mapping.</summary>
    public const string PhysicalNameKey = "delta.columnMapping.physicalName";
    /// <summary>The metadata key holding the column id under column mapping.</summary>
    public const string ColumnIdKey = "delta.columnMapping.id";

    /// <summary>The logical field name.</summary>
    public required string Name { get; init; }
    /// <summary>The field type.</summary>
    public required DeltaType Type { get; init; }
    /// <summary>Whether the field may be null.</summary>
    public bool Nullable { get; init; } = true;
    /// <summary>Field metadata. A string value is kept as it is, any other value as its JSON text.</summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = EmptyMap.Instance;

    /// <summary>
    /// The metadata keys whose values are JSON other than a string (numbers, booleans, objects), so the schema is
    /// written back as it was read. The column mapping id is always a number.
    /// </summary>
    public IReadOnlySet<string> JsonMetadataKeys { get; init; } = EmptySet;

    private static readonly IReadOnlySet<string> EmptySet = new HashSet<string>();

    /// <summary>The column id under column mapping, or null when the field has none.</summary>
    public int? FieldId => Metadata.TryGetValue(ColumnIdKey, out string? id) && int.TryParse(id, out int value) ? value : null;

    /// <summary>The name of the column in the data files: the mapped physical name when there is one, else <see cref="Name"/>.</summary>
    public string PhysicalName => Metadata.TryGetValue(PhysicalNameKey, out string? physical) ? physical : Name;
}

/// <summary>A struct of named fields; also the type of a table schema.</summary>
public sealed class StructType : DeltaType
{
    /// <summary>The fields in order.</summary>
    public IReadOnlyList<StructField> Fields { get; }

    /// <summary>Creates a struct from its fields.</summary>
    public StructType(IReadOnlyList<StructField> fields) => Fields = fields;

    /// <summary>Finds a field by its logical name, or returns null.</summary>
    public StructField? Field(string name)
    {
        foreach (StructField field in Fields)
        {
            if (field.Name == name) return field;
        }
        return null;
    }

    /// <inheritdoc/>
    public override string ToString() => $"struct<{string.Join(",", Fields.Select(f => $"{f.Name}:{f.Type}"))}>";

    /// <summary>The schema as the JSON of <c>metaData.schemaString</c>.</summary>
    public string ToJson()
    {
        using var stream = new MemoryStream();
        using (var json = new Utf8JsonWriter(stream, Kaminari.Seseragi.Lakehouse.Delta.Actions.ActionWriter.JsonOptions))
        {
            WriteType(json, this);
        }
        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }

    /// <summary>
    /// One field as the schema JSON writes it, under <paramref name="name"/> (Spark's <c>CHANGE COLUMN</c> records
    /// the changed field so, with its dotted path as the name).
    /// </summary>
    internal static string FieldJson(StructField field, string name)
    {
        using var stream = new MemoryStream();
        using (var json = new Utf8JsonWriter(stream, Kaminari.Seseragi.Lakehouse.Delta.Actions.ActionWriter.JsonOptions))
        {
            WriteField(json, field, name);
        }
        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }

    private static void WriteField(Utf8JsonWriter json, StructField field, string name)
    {
        json.WriteStartObject();
        json.WriteString("name", name);
        json.WritePropertyName("type");
        WriteType(json, field.Type);
        json.WriteBoolean("nullable", field.Nullable);
        json.WriteStartObject("metadata");
        foreach (var (key, value) in field.Metadata)
        {
            json.WritePropertyName(key);
            if (field.JsonMetadataKeys.Contains(key) || (key == StructField.ColumnIdKey && long.TryParse(value, out _)))
                json.WriteRawValue(value);
            else
                json.WriteStringValue(value);
        }
        json.WriteEndObject();
        json.WriteEndObject();
    }

    private static void WriteType(Utf8JsonWriter json, DeltaType type)
    {
        switch (type)
        {
            case StructType s:
                json.WriteStartObject();
                json.WriteString("type", "struct");
                json.WriteStartArray("fields");
                foreach (StructField field in s.Fields) WriteField(json, field, field.Name);
                json.WriteEndArray();
                json.WriteEndObject();
                break;
            case ArrayType a:
                json.WriteStartObject();
                json.WriteString("type", "array");
                json.WritePropertyName("elementType");
                WriteType(json, a.ElementType);
                json.WriteBoolean("containsNull", a.ContainsNull);
                json.WriteEndObject();
                break;
            case MapType m:
                json.WriteStartObject();
                json.WriteString("type", "map");
                json.WritePropertyName("keyType");
                WriteType(json, m.KeyType);
                json.WritePropertyName("valueType");
                WriteType(json, m.ValueType);
                json.WriteBoolean("valueContainsNull", m.ValueContainsNull);
                json.WriteEndObject();
                break;
            default:
                json.WriteStringValue(type.ToString());
                break;
        }
    }

    /// <summary>Reads a schema from the JSON of <c>metaData.schemaString</c>.</summary>
    /// <exception cref="DeltaException">The text is not a valid schema.</exception>
    public static StructType Parse(string schemaJson)
    {
        try
        {
            using JsonDocument doc = JsonDocument.Parse(schemaJson);
            if (ReadType(doc.RootElement) is StructType schema) return schema;
            throw new DeltaException("The table schema is not a struct.");
        }
        catch (JsonException e)
        {
            throw new DeltaException($"The table schema is not valid JSON: {e.Message}", e);
        }
    }

    private static DeltaType ReadType(JsonElement e)
    {
        if (e.ValueKind == JsonValueKind.String)
        {
            string name = e.GetString()!;
            if (name.StartsWith("decimal", StringComparison.Ordinal))
            {
                return ReadDecimal(name);
            }
            return new PrimitiveType(name);
        }

        if (e.ValueKind != JsonValueKind.Object || !e.TryGetProperty("type", out JsonElement kind) || kind.ValueKind != JsonValueKind.String)
            throw new DeltaException($"Unexpected type in the table schema: {e.GetRawText()}");

        switch (kind.GetString())
        {
            case "struct":
            {
                var fields = new List<StructField>();
                foreach (JsonElement f in Prop(e, "fields").EnumerateArray())
                {
                    fields.Add(new StructField
                    {
                        Name = Prop(f, "name").GetString()!,
                        Type = ReadType(Prop(f, "type")),
                        Nullable = !f.TryGetProperty("nullable", out JsonElement n) || n.ValueKind != JsonValueKind.False,
                        Metadata = ReadMetadata(f, out IReadOnlySet<string> jsonKeys),
                        JsonMetadataKeys = jsonKeys,
                    });
                }
                return new StructType(fields);
            }
            case "array":
                return new ArrayType(ReadType(Prop(e, "elementType")), Flag(e, "containsNull"));
            case "map":
                return new MapType(ReadType(Prop(e, "keyType")), ReadType(Prop(e, "valueType")), Flag(e, "valueContainsNull"));
            default:
                throw new DeltaException($"Unexpected type in the table schema: {e.GetRawText()}");
        }
    }

    private static DecimalType ReadDecimal(string name)
    {
        // "decimal" alone means decimal(10,0); otherwise "decimal(p,s)" with optional spaces.
        ReadOnlySpan<char> rest = name.AsSpan("decimal".Length).Trim();
        if (rest.Length == 0) return new DecimalType(10, 0);
        if (rest[0] == '(' && rest[^1] == ')')
        {
            rest = rest[1..^1];
            int comma = rest.IndexOf(',');
            if (comma > 0 && int.TryParse(rest[..comma].Trim(), out int precision) && int.TryParse(rest[(comma + 1)..].Trim(), out int scale))
                return new DecimalType(precision, scale);
        }
        throw new DeltaException($"Unexpected decimal type in the table schema: {name}");
    }

    private static JsonElement Prop(JsonElement e, string name) =>
        e.TryGetProperty(name, out JsonElement value)
            ? value
            : throw new DeltaException($"The table schema lacks '{name}' in {e.GetRawText()}");

    private static bool Flag(JsonElement e, string name) =>
        !e.TryGetProperty(name, out JsonElement value) || value.ValueKind != JsonValueKind.False;

    private static IReadOnlyDictionary<string, string> ReadMetadata(JsonElement field, out IReadOnlySet<string> jsonKeys)
    {
        jsonKeys = new HashSet<string>();
        if (!field.TryGetProperty("metadata", out JsonElement m) || m.ValueKind != JsonValueKind.Object) return EmptyMap.Instance;

        Dictionary<string, string>? map = null;
        HashSet<string>? json = null;
        foreach (JsonProperty p in m.EnumerateObject())
        {
            map ??= new Dictionary<string, string>(StringComparer.Ordinal);
            if (p.Value.ValueKind == JsonValueKind.String)
            {
                map[p.Name] = p.Value.GetString()!;
            }
            else
            {
                map[p.Name] = p.Value.GetRawText();
                (json ??= new HashSet<string>(StringComparer.Ordinal)).Add(p.Name);
            }
        }
        if (json is not null) jsonKeys = json;
        return map ?? EmptyMap.Instance;
    }
}

internal static class EmptyMap
{
    public static readonly IReadOnlyDictionary<string, string> Instance = new Dictionary<string, string>();
    public static readonly IReadOnlyDictionary<string, string?> Nullable = new Dictionary<string, string?>();
}
