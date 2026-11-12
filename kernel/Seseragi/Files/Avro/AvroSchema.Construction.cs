using System.Collections.Frozen;
using System.Text.Json;

namespace Kaminari.Seseragi.Files.Avro;

// Factory methods that build schemas in code. Every node is validated as it is built (names, logical types,
// unions, defaults), so a constructed schema is one the parser would read back to the same model: for any schema
// built here, AvroSchema.Parse(schema.ToJson()) has the same types, logical types, ids, defaults and properties.
// Constructed schemas cannot be recursive (a record cannot contain itself); Parse still reads recursive schemas.
public abstract partial class AvroSchema
{
    /// <summary><c>"null"</c>.</summary>
    public static AvroPrimitiveSchema Null { get; } = new(AvroType.Null);
    /// <summary><c>"boolean"</c>.</summary>
    public static AvroPrimitiveSchema Boolean { get; } = new(AvroType.Boolean);
    /// <summary><c>"int"</c>.</summary>
    public static AvroPrimitiveSchema Int { get; } = new(AvroType.Int);
    /// <summary><c>"long"</c>.</summary>
    public static AvroPrimitiveSchema Long { get; } = new(AvroType.Long);
    /// <summary><c>"float"</c>.</summary>
    public static AvroPrimitiveSchema Float { get; } = new(AvroType.Float);
    /// <summary><c>"double"</c>.</summary>
    public static AvroPrimitiveSchema Double { get; } = new(AvroType.Double);
    /// <summary><c>"bytes"</c>.</summary>
    public static AvroPrimitiveSchema Bytes { get; } = new(AvroType.Bytes);
    /// <summary><c>"string"</c>.</summary>
    public static AvroPrimitiveSchema String { get; } = new(AvroType.String);

    /// <summary><c>date</c> on <c>int</c> (days since the epoch).</summary>
    public static AvroPrimitiveSchema Date { get; } = new(AvroType.Int) { Logical = AvroLogicalKind.Date };
    /// <summary><c>time-millis</c> on <c>int</c>.</summary>
    public static AvroPrimitiveSchema TimeMillis { get; } = new(AvroType.Int) { Logical = AvroLogicalKind.TimeMillis };
    /// <summary><c>time-micros</c> on <c>long</c>.</summary>
    public static AvroPrimitiveSchema TimeMicros { get; } = new(AvroType.Long) { Logical = AvroLogicalKind.TimeMicros };
    /// <summary><c>local-timestamp-millis</c> on <c>long</c>.</summary>
    public static AvroPrimitiveSchema LocalTimestampMillis { get; } = new(AvroType.Long) { Logical = AvroLogicalKind.LocalTimestampMillis };
    /// <summary><c>local-timestamp-micros</c> on <c>long</c>.</summary>
    public static AvroPrimitiveSchema LocalTimestampMicros { get; } = new(AvroType.Long) { Logical = AvroLogicalKind.LocalTimestampMicros };
    /// <summary><c>local-timestamp-nanos</c> on <c>long</c>.</summary>
    public static AvroPrimitiveSchema LocalTimestampNanos { get; } = new(AvroType.Long) { Logical = AvroLogicalKind.LocalTimestampNanos };
    /// <summary><c>uuid</c> on <c>string</c>.</summary>
    public static AvroPrimitiveSchema Uuid { get; } = new(AvroType.String) { Logical = AvroLogicalKind.Uuid };
    /// <summary>Spark's day-time interval: a <c>long</c> of microseconds.</summary>
    public static AvroPrimitiveSchema SparkDayTimeInterval { get; } = new(AvroType.Long) { Logical = AvroLogicalKind.SparkDayTimeInterval };
    /// <summary>Spark's year-month interval: an <c>int</c> of months.</summary>
    public static AvroPrimitiveSchema SparkYearMonthInterval { get; } = new(AvroType.Int) { Logical = AvroLogicalKind.SparkYearMonthInterval };

    /// <summary>A primitive type with extra attributes (for example an annotation this package does not know).</summary>
    /// <param name="type">A primitive type: <c>null</c>, <c>boolean</c>, <c>int</c>, <c>long</c>, <c>float</c>,
    /// <c>double</c>, <c>bytes</c> or <c>string</c>.</param>
    /// <param name="properties">Attributes as JSON text, by name.</param>
    public static AvroPrimitiveSchema Primitive(AvroType type, IEnumerable<(string Name, string Json)>? properties = null)
    {
        if (type is not (AvroType.Null or AvroType.Boolean or AvroType.Int or AvroType.Long or AvroType.Float
            or AvroType.Double or AvroType.Bytes or AvroType.String))
            throw new ArgumentException($"{type} is not a primitive type.", nameof(type));
        return Checked(new AvroPrimitiveSchema(type)
        {
            Properties = AvroConstruction.Properties(properties, AvroSchemaParser.PrimitiveKeys, "a primitive type"),
        });
    }

    /// <summary><c>timestamp-millis</c> on <c>long</c>: an instant.</summary>
    /// <param name="adjustToUtc">Iceberg's <c>adjust-to-utc</c>: true for <c>timestamptz</c>, false for a wall-clock
    /// <c>timestamp</c>; null leaves the attribute out.</param>
    /// <param name="timezone">The time zone kept in <c>kaminari.timezone</c>, or null.</param>
    public static AvroPrimitiveSchema TimestampMillis(bool? adjustToUtc = null, string? timezone = null) =>
        Timestamp(AvroLogicalKind.TimestampMillis, adjustToUtc, timezone);

    /// <summary><c>timestamp-micros</c> on <c>long</c>: an instant.</summary>
    /// <inheritdoc cref="TimestampMillis" path="/param"/>
    public static AvroPrimitiveSchema TimestampMicros(bool? adjustToUtc = null, string? timezone = null) =>
        Timestamp(AvroLogicalKind.TimestampMicros, adjustToUtc, timezone);

    /// <summary><c>timestamp-nanos</c> on <c>long</c>: an instant.</summary>
    /// <inheritdoc cref="TimestampMillis" path="/param"/>
    public static AvroPrimitiveSchema TimestampNanos(bool? adjustToUtc = null, string? timezone = null) =>
        Timestamp(AvroLogicalKind.TimestampNanos, adjustToUtc, timezone);

    private static AvroPrimitiveSchema Timestamp(AvroLogicalKind kind, bool? adjustToUtc, string? timezone)
    {
        if (timezone is { Length: 0 }) throw new ArgumentException("A time zone must not be empty.", nameof(timezone));
        return new AvroPrimitiveSchema(AvroType.Long)
        {
            Logical = kind,
            Timezone = timezone,
            Properties = adjustToUtc is bool adjust ? AvroProperties.OfBool(AvroProperties.AdjustToUtc, adjust) : AvroProperties.Empty,
        };
    }

    /// <summary><c>decimal</c> on <c>bytes</c>.</summary>
    public static AvroPrimitiveSchema DecimalBytes(int precision, int scale)
    {
        AvroConstruction.CheckDecimal(precision, scale, int.MaxValue);
        return new(AvroType.Bytes) { Logical = AvroLogicalKind.Decimal, Precision = precision, Scale = scale };
    }

    /// <summary><c>decimal</c> on a named <c>fixed</c> (as Iceberg writes decimals, e.g. <c>decimal_9_2</c>).</summary>
    /// <param name="size">The fixed size in bytes; by default the smallest that holds <paramref name="precision"/> digits.</param>
    /// <param name="name">The name (a dotted name carries its namespace).</param>
    /// <param name="precision">The total number of digits.</param>
    /// <param name="scale">The digits after the decimal point.</param>
    /// <param name="ns">The namespace, or null.</param>
    public static AvroFixedSchema DecimalFixed(string name, int precision, int scale, int? size = null, string? ns = null)
    {
        AvroConstruction.CheckDecimal(precision, scale, int.MaxValue);
        int bytes = size ?? AvroConstruction.MinimalDecimalSize(precision);
        if (bytes <= 0 || AvroSchemaParser.MaxDecimalDigits(bytes) < precision)
            throw new ArgumentException($"A fixed of {bytes} bytes cannot hold {precision} decimal digits.", nameof(size));
        var (n, space) = AvroConstruction.Name(name, ns);
        return new(n, space, bytes) { Logical = AvroLogicalKind.Decimal, Precision = precision, Scale = scale };
    }

    /// <summary><c>uuid</c> on a named <c>fixed(16)</c> (as Iceberg writes uuids, e.g. <c>uuid_fixed</c>).</summary>
    public static AvroFixedSchema UuidFixed(string name, string? ns = null)
    {
        var (n, space) = AvroConstruction.Name(name, ns);
        return new(n, space, 16) { Logical = AvroLogicalKind.Uuid };
    }

    /// <summary>Avro's <c>duration</c> on a named <c>fixed(12)</c>.</summary>
    public static AvroFixedSchema Duration(string name, string? ns = null)
    {
        var (n, space) = AvroConstruction.Name(name, ns);
        return new(n, space, 12) { Logical = AvroLogicalKind.Duration };
    }

    /// <summary>A named <c>fixed</c>.</summary>
    /// <param name="properties">Attributes as JSON text, by name.</param>
    /// <param name="name">The name (a dotted name carries its namespace).</param>
    /// <param name="size">The size in bytes.</param>
    /// <param name="ns">The namespace, or null.</param>
    /// <param name="doc">The <c>doc</c> attribute, or null.</param>
    /// <param name="aliases">The <c>aliases</c> attribute, or null.</param>
    public static AvroFixedSchema Fixed(string name, int size, string? ns = null, string? doc = null,
        IEnumerable<string>? aliases = null, IEnumerable<(string Name, string Json)>? properties = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(size);
        var (n, space) = AvroConstruction.Name(name, ns);
        return Checked(new AvroFixedSchema(n, space, size)
        {
            Doc = doc,
            Aliases = [.. aliases ?? []],
            Properties = AvroConstruction.Properties(properties, AvroSchemaParser.FixedKeys, $"fixed '{name}'"),
        });
    }

    /// <summary>A named <c>enum</c>.</summary>
    /// <param name="default">The symbol readers use for a symbol they do not know, or null.</param>
    /// <param name="properties">Attributes as JSON text, by name.</param>
    /// <param name="name">The name (a dotted name carries its namespace).</param>
    /// <param name="symbols">The symbols, in order.</param>
    /// <param name="ns">The namespace, or null.</param>
    /// <param name="doc">The <c>doc</c> attribute, or null.</param>
    /// <param name="aliases">The <c>aliases</c> attribute, or null.</param>
    public static AvroEnumSchema Enum(string name, IEnumerable<string> symbols, string? ns = null, string? @default = null,
        string? doc = null, IEnumerable<string>? aliases = null, IEnumerable<(string Name, string Json)>? properties = null)
    {
        var (n, space) = AvroConstruction.Name(name, ns);
        List<string> list = [.. symbols];
        HashSet<string> seen = [];
        foreach (string symbol in list)
        {
            if (!AvroSchemaParser.IsValidName(symbol)) throw new ArgumentException($"'{symbol}' is not a valid enum symbol.", nameof(symbols));
            if (!seen.Add(symbol)) throw new ArgumentException($"Enum '{name}' repeats the symbol '{symbol}'.", nameof(symbols));
        }
        if (@default is not null && !seen.Contains(@default))
            throw new ArgumentException($"The default '{@default}' is not a symbol of enum '{name}'.", nameof(@default));
        return new AvroEnumSchema(n, space, list)
        {
            Default = @default,
            Doc = doc,
            Aliases = [.. aliases ?? []],
            Properties = AvroConstruction.Properties(properties, AvroSchemaParser.EnumKeys, $"enum '{name}'"),
        };
    }

    /// <summary>A named <c>record</c> with <paramref name="fields"/> in order (each field's position is assigned here,
    /// so one <see cref="AvroField"/> may be used in several records).</summary>
    /// <param name="properties">Attributes as JSON text, by name.</param>
    /// <param name="name">The name (a dotted name carries its namespace).</param>
    /// <param name="fields">The fields, in order.</param>
    /// <param name="ns">The namespace, or null.</param>
    /// <param name="doc">The <c>doc</c> attribute, or null.</param>
    /// <param name="aliases">The <c>aliases</c> attribute, or null.</param>
    public static AvroRecordSchema Record(string name, IEnumerable<AvroField> fields, string? ns = null, string? doc = null,
        IEnumerable<string>? aliases = null, IEnumerable<(string Name, string Json)>? properties = null)
    {
        var (n, space) = AvroConstruction.Name(name, ns);
        var record = new AvroRecordSchema(n, space)
        {
            Doc = doc,
            Aliases = [.. aliases ?? []],
            Properties = AvroConstruction.Properties(properties, AvroSchemaParser.RecordKeys, $"record '{name}'"),
        };
        List<AvroField> list = [];
        HashSet<string> seen = [];
        foreach (AvroField field in fields)
        {
            if (!seen.Add(field.Name)) throw new ArgumentException($"Record '{name}' has two fields named '{field.Name}'.", nameof(fields));
            list.Add(field.At(list.Count));
        }
        record.Fields = list;
        return record;
    }

    /// <summary>An <c>array</c>.</summary>
    /// <param name="elementId">Iceberg's <c>element-id</c>, or null.</param>
    /// <param name="properties">Other attributes as JSON text, by name.</param>
    /// <param name="items">The item type.</param>
    public static AvroArraySchema Array(AvroSchema items, int? elementId = null, IEnumerable<(string Name, string Json)>? properties = null)
    {
        ArgumentNullException.ThrowIfNull(items);
        return Checked(new AvroArraySchema(items)
        {
            Properties = AvroConstruction.Merge(
                AvroProperties.OfId(AvroProperties.ElementId, elementId),
                AvroConstruction.Properties(properties, AvroSchemaParser.ArrayKeys, "an array")),
        });
    }

    /// <summary>A <c>map</c> (string keys).</summary>
    /// <param name="keyId">Iceberg's <c>key-id</c>, or null.</param>
    /// <param name="valueId">Iceberg's <c>value-id</c>, or null.</param>
    /// <param name="properties">Other attributes as JSON text, by name.</param>
    /// <param name="values">The value type.</param>
    public static AvroMapSchema Map(AvroSchema values, int? keyId = null, int? valueId = null,
        IEnumerable<(string Name, string Json)>? properties = null)
    {
        ArgumentNullException.ThrowIfNull(values);
        return Checked(new AvroMapSchema(values)
        {
            Properties = AvroConstruction.Merge(
                AvroProperties.OfIds((AvroProperties.KeyId, keyId), (AvroProperties.ValueId, valueId)),
                AvroConstruction.Properties(properties, AvroSchemaParser.MapKeys, "a map")),
        });
    }

    /// <summary>
    /// A map whose keys need not be strings, as Iceberg and DuckDB write it: an <c>array</c> with
    /// <c>logicalType: map</c> of records named <paramref name="entriesName"/> with the fields <c>key</c> and
    /// <c>value</c>. Iceberg names the record after the ids (<c>k117_v118</c>) and puts them on the two fields.
    /// </summary>
    public static AvroArraySchema ArrayMap(string entriesName, AvroSchema key, AvroSchema value, int? keyId = null,
        int? valueId = null, string? ns = null)
    {
        var entries = Record(entriesName, [AvroField.Required("key", key, keyId), AvroField.Required("value", value, valueId)], ns);
        if (!AvroArraySchema.IsMapEntries(entries))
            throw new ArgumentException("A map key must be a single non-null type (or [\"null\", T]).", nameof(key));
        return new AvroArraySchema(entries) { Logical = AvroLogicalKind.Map };
    }

    /// <summary>A union of <paramref name="branches"/>, in order (a value names its branch by index).</summary>
    /// <exception cref="ArgumentException">A branch is a union, or two branches have the same type (named types:
    /// the same full name).</exception>
    public static AvroUnionSchema Union(params ReadOnlySpan<AvroSchema> branches)
    {
        if (branches.IsEmpty) throw new ArgumentException("A union needs at least one branch.", nameof(branches));
        HashSet<string> seen = [];
        foreach (AvroSchema branch in branches)
        {
            ArgumentNullException.ThrowIfNull(branch, nameof(branches));
            if (branch is AvroUnionSchema) throw new ArgumentException("A union may not contain a union.", nameof(branches));
            // Logical annotations do not make a different type: ["long", timestamp-micros] repeats long.
            string key = branch is AvroNamedSchema named ? "named:" + named.FullName : branch.Type.ToString();
            if (!seen.Add(key)) throw new ArgumentException($"A union may not hold {key} twice.", nameof(branches));
        }
        return new AvroUnionSchema([.. branches]);
    }

    /// <summary><c>["null", <paramref name="type"/>]</c>.</summary>
    public static AvroUnionSchema Nullable(AvroSchema type) => type is AvroUnionSchema
        ? throw new ArgumentException("Nullable takes a single type; build a wider union with Union.", nameof(type))
        : Union(Null, type);

    // A node with custom properties must read back as the same node: an attribute the parser would interpret
    // (a known logicalType, precision, a Spark interval tag...) belongs in the typed factory instead.
    private static T Checked<T>(T node) where T : AvroSchema
    {
        if (node.Properties.Count == 0) return node;
        AvroSchema back = Parse(node.ToJson());
        if (back.Logical != node.Logical || back.Precision != node.Precision || back.Scale != node.Scale
            || back.Timezone != node.Timezone || back.Properties.Count != node.Properties.Count)
            throw new ArgumentException(
                $"The attributes {string.Join(", ", node.Properties.Keys)} would change how the type reads; use the typed factory for a logical type.");
        return node;
    }
}

public sealed partial class AvroField
{
    /// <summary>A field without a null branch. With <paramref name="defaultJson"/> a reader whose writer lacks the
    /// field uses that value (JSON as the specification encodes defaults, validated against the type).</summary>
    /// <param name="fieldId">Iceberg's <c>field-id</c>, or null.</param>
    /// <param name="defaultJson">The default as JSON text (<c>"0"</c>, <c>"\"abc\""</c>, <c>"null"</c> for a union
    /// whose first matching branch is null...), or null for none.</param>
    /// <param name="properties">Other attributes as JSON text, by name (for example <c>("order", "\"ignore\"")</c>).</param>
    /// <param name="name">The name (a dotted name carries its namespace).</param>
    /// <param name="type">The field type.</param>
    /// <param name="doc">The <c>doc</c> attribute, or null.</param>
    /// <param name="aliases">The <c>aliases</c> attribute, or null.</param>
    public static AvroField Required(string name, AvroSchema type, int? fieldId = null, string? defaultJson = null,
        string? doc = null, IEnumerable<string>? aliases = null, IEnumerable<(string Name, string Json)>? properties = null)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(type);
        JsonElement? @default = null;
        if (defaultJson is not null)
        {
            @default = AvroConstruction.Json(defaultJson, $"the default of '{name}'");
            AvroDefaults.Encode(type, @default.Value, name); // validates
        }
        return new AvroField(name, type, -1)
        {
            Doc = doc,
            Aliases = [.. aliases ?? []],
            Default = @default,
            Properties = AvroConstruction.Merge(
                AvroProperties.OfId(AvroProperties.FieldId, fieldId),
                AvroConstruction.Properties(properties, AvroSchemaParser.FieldKeys, $"field '{name}'")),
        };
    }

    /// <summary>An optional field: <c>["null", <paramref name="type"/>]</c> with <c>"default": null</c>, as Iceberg
    /// writes optional fields (a reader whose writer lacks the field reads null).</summary>
    /// <inheritdoc cref="Required" path="/param[@name='fieldId']"/>
    public static AvroField Optional(string name, AvroSchema type, int? fieldId = null, string? doc = null,
        IEnumerable<string>? aliases = null, IEnumerable<(string Name, string Json)>? properties = null) =>
        Required(name, AvroSchema.Nullable(type), fieldId, "null", doc, aliases, properties);

    // This field at a position of its record (a copy unless it is already there).
    internal AvroField At(int position) => position == Position
        ? this
        : new AvroField(Name, Schema, position) { Doc = Doc, Aliases = Aliases, Default = Default, Properties = Properties };
}

/// <summary>Validation and helpers shared by the factory methods.</summary>
internal static class AvroConstruction
{
    /// <summary>A valid name and namespace; a dotted <paramref name="name"/> carries its own namespace.</summary>
    public static (string Name, string? Namespace) Name(string name, string? ns)
    {
        ArgumentNullException.ThrowIfNull(name);
        int dot = name.LastIndexOf('.');
        if (dot >= 0)
        {
            ns = name[..dot];
            name = name[(dot + 1)..];
        }
        if (!AvroSchemaParser.IsValidName(name)) throw new ArgumentException($"'{name}' is not a valid Avro name.", nameof(name));
        if (!string.IsNullOrEmpty(ns))
        {
            foreach (string part in ns.Split('.'))
                if (!AvroSchemaParser.IsValidName(part)) throw new ArgumentException($"'{ns}' is not a valid Avro namespace.", nameof(ns));
        }
        return (name, string.IsNullOrEmpty(ns) ? null : ns);
    }

    public static void CheckDecimal(int precision, int scale, int maxPrecision)
    {
        if (precision <= 0 || precision > maxPrecision) throw new ArgumentOutOfRangeException(nameof(precision), "Precision must be positive.");
        if (scale < 0 || scale > precision) throw new ArgumentOutOfRangeException(nameof(scale), "Scale must be between 0 and the precision.");
    }

    public static int MinimalDecimalSize(int precision)
    {
        int bytes = 1;
        while (AvroSchemaParser.MaxDecimalDigits(bytes) < precision) bytes++;
        return bytes;
    }

    public static JsonElement Json(string json, string what)
    {
        try
        {
            using JsonDocument doc = JsonDocument.Parse(json);
            return doc.RootElement.Clone();
        }
        catch (JsonException e)
        {
            throw new ArgumentException($"{what} is not valid JSON: {e.Message}", e);
        }
    }

    /// <summary>Attributes given as JSON text, refusing the names the node writes itself.</summary>
    public static IReadOnlyDictionary<string, JsonElement> Properties(IEnumerable<(string Name, string Json)>? properties,
        FrozenSet<string> reserved, string what)
    {
        if (properties is null) return AvroProperties.Empty;
        Dictionary<string, JsonElement> result = new(StringComparer.Ordinal);
        foreach (var (name, json) in properties)
        {
            ArgumentNullException.ThrowIfNull(name);
            if (reserved.Contains(name)) throw new ArgumentException($"'{name}' is part of {what} itself, not an extra attribute.");
            if (!result.TryAdd(name, Json(json, $"the attribute '{name}'"))) throw new ArgumentException($"The attribute '{name}' is given twice.");
        }
        return result.Count == 0 ? AvroProperties.Empty : result;
    }

    /// <summary>Typed attributes (ids) and extra ones; the same name in both is an error.</summary>
    public static IReadOnlyDictionary<string, JsonElement> Merge(IReadOnlyDictionary<string, JsonElement> typed,
        IReadOnlyDictionary<string, JsonElement> extra)
    {
        if (extra.Count == 0) return typed;
        if (typed.Count == 0) return extra;
        Dictionary<string, JsonElement> result = new(typed, StringComparer.Ordinal);
        foreach (var (name, value) in extra)
            if (!result.TryAdd(name, value)) throw new ArgumentException($"The attribute '{name}' is given both as a parameter and as a property.");
        return result;
    }
}
