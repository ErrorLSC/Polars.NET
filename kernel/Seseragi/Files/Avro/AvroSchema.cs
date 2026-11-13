using System.Buffers;
using System.Collections.Frozen;
using System.Globalization;
using System.Text.Json;

namespace Kaminari.Seseragi.Files.Avro;

/// <summary>The type of an Avro schema node (Avro 1.12 specification).</summary>
public enum AvroType
{
    /// <summary><c>null</c>.</summary>
    Null,
    /// <summary><c>boolean</c>.</summary>
    Boolean,
    /// <summary><c>int</c> (32-bit).</summary>
    Int,
    /// <summary><c>long</c> (64-bit).</summary>
    Long,
    /// <summary><c>float</c>.</summary>
    Float,
    /// <summary><c>double</c>.</summary>
    Double,
    /// <summary><c>bytes</c>.</summary>
    Bytes,
    /// <summary><c>string</c> (UTF-8).</summary>
    String,
    /// <summary><c>record</c>.</summary>
    Record,
    /// <summary><c>enum</c>.</summary>
    Enum,
    /// <summary><c>array</c>.</summary>
    Array,
    /// <summary><c>map</c> (string keys).</summary>
    Map,
    /// <summary>A union (JSON array of branches).</summary>
    Union,
    /// <summary><c>fixed</c>.</summary>
    Fixed,
}

/// <summary>A logical type annotation understood by this package.</summary>
public enum AvroLogicalKind
{
    /// <summary>No (valid) logical type.</summary>
    None,
    /// <summary><c>decimal</c> on <c>bytes</c> or <c>fixed</c>, with <see cref="AvroSchema.Precision"/> and <see cref="AvroSchema.Scale"/>.</summary>
    Decimal,
    /// <summary><c>uuid</c> on <c>string</c> or <c>fixed(16)</c>.</summary>
    Uuid,
    /// <summary><c>date</c>: days since the epoch, on <c>int</c>.</summary>
    Date,
    /// <summary><c>time-millis</c> on <c>int</c>.</summary>
    TimeMillis,
    /// <summary><c>time-micros</c> on <c>long</c>.</summary>
    TimeMicros,
    /// <summary><c>timestamp-millis</c>: an instant, on <c>long</c>.</summary>
    TimestampMillis,
    /// <summary><c>timestamp-micros</c>: an instant, on <c>long</c>.</summary>
    TimestampMicros,
    /// <summary><c>timestamp-nanos</c>: an instant, on <c>long</c>.</summary>
    TimestampNanos,
    /// <summary><c>local-timestamp-millis</c>: a wall-clock time, on <c>long</c>.</summary>
    LocalTimestampMillis,
    /// <summary><c>local-timestamp-micros</c>: a wall-clock time, on <c>long</c>.</summary>
    LocalTimestampMicros,
    /// <summary><c>local-timestamp-nanos</c>: a wall-clock time, on <c>long</c>.</summary>
    LocalTimestampNanos,
    /// <summary><c>duration</c>: months, days and milliseconds, on <c>fixed(12)</c>.</summary>
    Duration,
    /// <summary>Spark's day-time interval: a long of microseconds tagged with <c>spark.sql.catalyst.type</c>.</summary>
    SparkDayTimeInterval,
    /// <summary>Spark's year-month interval: an int of months tagged with <c>spark.sql.catalyst.type</c>.</summary>
    SparkYearMonthInterval,
    /// <summary><c>map</c> on an <c>array</c> of two-field records (key, value): a map whose keys need not be strings,
    /// as Iceberg and DuckDB write them. The key may be <c>["null", T]</c> (DuckDB) but no key may be null.</summary>
    Map,
}

/// <summary>
/// A node of a parsed Avro schema (Avro 1.12 specification). Obtain one with <see cref="Parse(string)"/> or from
/// <see cref="AvroReader.WriterSchema"/>; the model is read-only.
/// </summary>
/// <remarks>
/// Attributes this package does not interpret (for example Iceberg's <c>field-id</c>, <c>element-id</c>,
/// <c>key-id</c> and <c>value-id</c>, or a logical type it does not know) are kept in <see cref="Properties"/> and
/// written back by <see cref="ToJson"/>. Attributes on a reference to a named type (<c>{"type": "Name", …}</c>) or
/// on a wrapper (<c>{"type": {…}, …}</c>) have no node of their own and are dropped.
/// </remarks>
public abstract partial class AvroSchema
{
    internal AvroSchema(AvroType type) => Type = type;

    /// <summary>The kind of node.</summary>
    public AvroType Type { get; }

    /// <summary>A valid logical type annotation; invalid or unknown annotations are not interpreted, as the
    /// specification requires (the value is read as its underlying type), and stay in <see cref="Properties"/>.</summary>
    public AvroLogicalKind Logical { get; internal init; }

    /// <summary>The precision of a <see cref="AvroLogicalKind.Decimal"/>.</summary>
    public int Precision { get; internal init; }

    /// <summary>The scale of a <see cref="AvroLogicalKind.Decimal"/>.</summary>
    public int Scale { get; internal init; }

    /// <summary>
    /// The time zone of a <c>timestamp-*</c> (instant) value, kept in the <see cref="TimezoneProperty"/> schema
    /// attribute. Avro itself has no place for it; other readers ignore the attribute and read a UTC instant.
    /// </summary>
    public string? Timezone { get; internal init; }

    /// <summary>Schema attribute carrying the original Arrow time zone of a <c>timestamp-*</c> field.</summary>
    public const string TimezoneProperty = "kaminari.timezone";

    /// <summary>The attributes of this node that the parser does not turn into typed members, by name, as JSON,
    /// written back as they are. A few of them also have a typed accessor (<see cref="AdjustToUtc"/>,
    /// <see cref="AvroArraySchema.ElementId"/>, …).</summary>
    public IReadOnlyDictionary<string, JsonElement> Properties { get; internal init; } = AvroProperties.Empty;

    /// <summary>
    /// Iceberg's <c>adjust-to-utc</c> attribute on a <c>timestamp-*</c> type: <c>true</c> for an instant
    /// (<c>timestamptz</c>), <c>false</c> for a wall-clock time (<c>timestamp</c>), null when absent or not a JSON
    /// boolean. A <c>timestamp-*</c> with <c>false</c> reads as a timestamp without a time zone.
    /// </summary>
    public bool? AdjustToUtc => Properties.TryGetValue(AvroProperties.AdjustToUtc, out JsonElement value)
        && value.ValueKind is JsonValueKind.True or JsonValueKind.False
            ? value.GetBoolean()
            : null;

    /// <summary>Parses Avro schema JSON.</summary>
    /// <exception cref="AvroFormatException">The JSON is not a valid Avro schema.</exception>
    public static AvroSchema Parse(string json) => AvroSchemaParser.Parse(json);

    /// <inheritdoc cref="Parse(string)"/>
    public static AvroSchema Parse(ReadOnlySpan<byte> utf8Json) => AvroSchemaParser.Parse(utf8Json);

    /// <summary>
    /// The Arrow schema this Avro schema reads as (a record's fields; any other type as one column <c>value</c>),
    /// with field ids as <c>PARQUET:field_id</c> metadata. Writing with this schema as
    /// <see cref="AvroWriteOptions.WriterSchema"/> accepts exactly these Arrow types.
    /// </summary>
    /// <exception cref="NotSupportedException">The schema is recursive (Arrow cannot represent it).</exception>
    public Apache.Arrow.Schema ToArrowSchema() =>
        new([.. AvroReadSchema.TopLevelFields(this).Select(f => AvroReadSchema.ToField(f.Name, f.Schema, f.FieldId))], null);

    /// <summary>The schema as Avro JSON; named types are written in full once, then by full name.</summary>
    public string ToJson() => AvroSchemaJsonWriter.Write(this);

    /// <inheritdoc cref="ToJson"/>
    public override string ToString() => ToJson();
}

/// <summary>A primitive type (<c>null</c>, <c>boolean</c>, <c>int</c>, <c>long</c>, <c>float</c>, <c>double</c>,
/// <c>bytes</c>, <c>string</c>), possibly with a logical type.</summary>
public sealed class AvroPrimitiveSchema : AvroSchema
{
    internal AvroPrimitiveSchema(AvroType type) : base(type) { }
}

/// <summary>A named type: record, enum or fixed.</summary>
public abstract class AvroNamedSchema : AvroSchema
{
    internal AvroNamedSchema(AvroType type, string name, string? ns) : base(type)
    {
        Name = name;
        Namespace = ns;
    }

    /// <summary>The name, without the namespace.</summary>
    public string Name { get; }

    /// <summary>The namespace, or null for the null namespace.</summary>
    public string? Namespace { get; }

    /// <summary>The name qualified by the namespace.</summary>
    public string FullName => string.IsNullOrEmpty(Namespace) ? Name : $"{Namespace}.{Name}";

    /// <summary>The <c>doc</c> attribute.</summary>
    public string? Doc { get; internal init; }

    /// <summary>The <c>aliases</c> attribute (not used for resolution yet).</summary>
    public IReadOnlyList<string> Aliases { get; internal init; } = [];
}

/// <summary>A record. Records can be recursive: a field's schema may be the record itself.</summary>
public sealed class AvroRecordSchema : AvroNamedSchema
{
    internal AvroRecordSchema(string name, string? ns) : base(AvroType.Record, name, ns) { }

    /// <summary>The fields, in order.</summary>
    // Filled after construction so that recursive references can point back at the record.
    public IReadOnlyList<AvroField> Fields { get; internal set; } = [];
}

/// <summary>A field of a record.</summary>
public sealed partial class AvroField
{
    internal AvroField(string name, AvroSchema schema, int position)
    {
        Name = name;
        Schema = schema;
        Position = position;
    }

    /// <summary>The field name.</summary>
    public string Name { get; }

    /// <summary>The field type.</summary>
    public AvroSchema Schema { get; }

    /// <summary>The position of the field in its record.</summary>
    public int Position { get; }

    /// <summary>The <c>doc</c> attribute.</summary>
    public string? Doc { get; internal init; }

    /// <summary>The <c>aliases</c> attribute (not used for resolution yet).</summary>
    public IReadOnlyList<string> Aliases { get; internal init; } = [];

    /// <summary>The <c>default</c> value, if the field declares one (JSON, as the specification encodes it).</summary>
    public JsonElement? Default { get; internal init; }

    // The default as raw JSON text, for the schema writer.
    internal string? DefaultJson => Default?.GetRawText();

    /// <summary>The attributes of the field the package does not interpret (for example <c>field-id</c>, <c>order</c>).</summary>
    public IReadOnlyDictionary<string, JsonElement> Properties { get; internal init; } = AvroProperties.Empty;

    /// <summary>The <c>field-id</c> attribute (Iceberg), or null.</summary>
    public int? FieldId => AvroProperties.Id(Properties, AvroProperties.FieldId);
}

/// <summary>An enum.</summary>
public sealed class AvroEnumSchema : AvroNamedSchema
{
    internal AvroEnumSchema(string name, string? ns, IReadOnlyList<string> symbols) : base(AvroType.Enum, name, ns) =>
        Symbols = symbols;

    /// <summary>The symbols, in order (a value is encoded as its index).</summary>
    public IReadOnlyList<string> Symbols { get; }

    /// <summary>The symbol readers use for an unknown symbol, or null.</summary>
    public string? Default { get; internal init; }
}

/// <summary>A fixed-size binary.</summary>
public sealed class AvroFixedSchema : AvroNamedSchema
{
    internal AvroFixedSchema(string name, string? ns, int size) : base(AvroType.Fixed, name, ns) => Size = size;

    /// <summary>The size in bytes.</summary>
    public int Size { get; }
}

/// <summary>An array.</summary>
public sealed class AvroArraySchema : AvroSchema
{
    internal AvroArraySchema(AvroSchema items) : base(AvroType.Array) => Items = items;

    /// <summary>The item type.</summary>
    public AvroSchema Items { get; }

    /// <summary>The <c>element-id</c> attribute (Iceberg), or null.</summary>
    public int? ElementId => AvroProperties.Id(Properties, AvroProperties.ElementId);

    /// <summary>Whether <paramref name="items"/> can be the entries of an array-encoded map: a record with exactly
    /// two fields (the rule Iceberg applies) whose key is a single type or <c>["null", T]</c>.</summary>
    internal static bool IsMapEntries(AvroSchema items) =>
        items is AvroRecordSchema { Fields.Count: 2 } entries && MapKey(entries) is not null;

    /// <summary>The key type of map entries (the non-null branch of a nullable key), or null when unusable.</summary>
    internal static AvroSchema? MapKey(AvroRecordSchema entries) => entries.Fields[0].Schema switch
    {
        AvroUnionSchema { Branches.Count: 2, NullIndex: >= 0 } u when u.Branches[1 - u.NullIndex] is var b
            && b.Type is not (AvroType.Null or AvroType.Union) => u.Branches[1 - u.NullIndex],
        AvroUnionSchema => null,
        { Type: AvroType.Null } => null,
        var key => key,
    };
}

/// <summary>A map (string keys).</summary>
public sealed class AvroMapSchema : AvroSchema
{
    internal AvroMapSchema(AvroSchema values) : base(AvroType.Map) => Values = values;

    /// <summary>The value type.</summary>
    public AvroSchema Values { get; }

    /// <summary>The <c>key-id</c> attribute (Iceberg), or null.</summary>
    public int? KeyId => AvroProperties.Id(Properties, AvroProperties.KeyId);

    /// <summary>The <c>value-id</c> attribute (Iceberg), or null.</summary>
    public int? ValueId => AvroProperties.Id(Properties, AvroProperties.ValueId);
}

/// <summary>A union.</summary>
public sealed class AvroUnionSchema : AvroSchema
{
    internal AvroUnionSchema(IReadOnlyList<AvroSchema> branches) : base(AvroType.Union)
    {
        Branches = branches;
        NullIndex = IndexOfNull(branches);
    }

    /// <summary>The branches, in order (a value is encoded with its branch index).</summary>
    public IReadOnlyList<AvroSchema> Branches { get; }

    /// <summary>Index of the <c>null</c> branch, or -1.</summary>
    public int NullIndex { get; }

    private static int IndexOfNull(IReadOnlyList<AvroSchema> branches)
    {
        for (int i = 0; i < branches.Count; i++)
            if (branches[i].Type == AvroType.Null) return i;
        return -1;
    }
}

/// <summary>Schema attributes kept as JSON, and the id attributes Iceberg defines.</summary>
internal static class AvroProperties
{
    public const string FieldId = "field-id";
    public const string ElementId = "element-id";
    public const string KeyId = "key-id";
    public const string ValueId = "value-id";
    public const string AdjustToUtc = "adjust-to-utc";

    public static readonly IReadOnlyDictionary<string, JsonElement> Empty = FrozenDictionary<string, JsonElement>.Empty;

    /// <summary>An integer id attribute: a JSON number, or a string holding one.</summary>
    public static int? Id(IReadOnlyDictionary<string, JsonElement> properties, string name) =>
        properties.TryGetValue(name, out JsonElement value) && value.ValueKind switch
        {
            JsonValueKind.Number => value.TryGetInt32(out int n) ? n : (int?)null,
            JsonValueKind.String => int.TryParse(value.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out int n) ? n : null,
            _ => null,
        } is int id
            ? id
            : null;

    /// <summary>Properties holding one integer attribute, or none when <paramref name="id"/> is null.</summary>
    public static IReadOnlyDictionary<string, JsonElement> OfId(string name, int? id)
    {
        if (id is not int value) return Empty;
        using JsonDocument doc = JsonDocument.Parse(value.ToString(CultureInfo.InvariantCulture));
        return new Dictionary<string, JsonElement> { [name] = doc.RootElement.Clone() };
    }

    /// <summary>Properties holding one boolean attribute.</summary>
    public static IReadOnlyDictionary<string, JsonElement> OfBool(string name, bool value)
    {
        using JsonDocument doc = JsonDocument.Parse(value ? "true" : "false");
        return new Dictionary<string, JsonElement> { [name] = doc.RootElement.Clone() };
    }

    /// <summary>Properties holding several integer attributes (null ids are left out).</summary>
    public static IReadOnlyDictionary<string, JsonElement> OfIds(params ReadOnlySpan<(string Name, int? Id)> ids)
    {
        Dictionary<string, JsonElement>? result = null;
        foreach (var (name, id) in ids)
        {
            if (OfId(name, id) is { Count: 1 } one) (result ??= [])[name] = one[name];
        }
        return result ?? Empty;
    }
}

/// <summary>Parses Avro schema JSON (Utf8 JSON DOM, no reflection).</summary>
internal static class AvroSchemaParser
{
    private static readonly FrozenDictionary<string, AvroType> Primitives = new Dictionary<string, AvroType>
    {
        ["null"] = AvroType.Null,
        ["boolean"] = AvroType.Boolean,
        ["int"] = AvroType.Int,
        ["long"] = AvroType.Long,
        ["float"] = AvroType.Float,
        ["double"] = AvroType.Double,
        ["bytes"] = AvroType.Bytes,
        ["string"] = AvroType.String,
    }.ToFrozenDictionary();

    private static readonly FrozenDictionary<string, AvroLogicalKind> LogicalNames = new Dictionary<string, AvroLogicalKind>
    {
        ["decimal"] = AvroLogicalKind.Decimal,
        ["uuid"] = AvroLogicalKind.Uuid,
        ["date"] = AvroLogicalKind.Date,
        ["time-millis"] = AvroLogicalKind.TimeMillis,
        ["time-micros"] = AvroLogicalKind.TimeMicros,
        ["timestamp-millis"] = AvroLogicalKind.TimestampMillis,
        ["timestamp-micros"] = AvroLogicalKind.TimestampMicros,
        ["timestamp-nanos"] = AvroLogicalKind.TimestampNanos,
        ["local-timestamp-millis"] = AvroLogicalKind.LocalTimestampMillis,
        ["local-timestamp-micros"] = AvroLogicalKind.LocalTimestampMicros,
        ["local-timestamp-nanos"] = AvroLogicalKind.LocalTimestampNanos,
        ["duration"] = AvroLogicalKind.Duration,
    }.ToFrozenDictionary();

    // Attributes each kind of node interprets itself; every other attribute is kept in Properties.
    internal static readonly FrozenSet<string> PrimitiveKeys = FrozenSet.ToFrozenSet(["type"]);
    internal static readonly FrozenSet<string> ArrayKeys = FrozenSet.ToFrozenSet(["type", "items"]);
    internal static readonly FrozenSet<string> MapKeys = FrozenSet.ToFrozenSet(["type", "values"]);
    internal static readonly FrozenSet<string> RecordKeys = FrozenSet.ToFrozenSet(["type", "name", "namespace", "doc", "aliases", "fields"]);
    internal static readonly FrozenSet<string> EnumKeys = FrozenSet.ToFrozenSet(["type", "name", "namespace", "doc", "aliases", "symbols", "default"]);
    internal static readonly FrozenSet<string> FixedKeys = FrozenSet.ToFrozenSet(["type", "name", "namespace", "doc", "aliases", "size"]);
    internal static readonly FrozenSet<string> FieldKeys = FrozenSet.ToFrozenSet(["name", "type", "doc", "aliases", "default"]);

    private static readonly SearchValues<char> NameStart =
        SearchValues.Create("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_");

    private static readonly SearchValues<char> NameRest =
        SearchValues.Create("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_0123456789");

    public static AvroSchema Parse(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return new Context().Parse(doc.RootElement, null);
    }

    public static AvroSchema Parse(ReadOnlySpan<byte> utf8Json)
    {
        var reader = new Utf8JsonReader(utf8Json);
        using var doc = JsonDocument.ParseValue(ref reader);
        return new Context().Parse(doc.RootElement, null);
    }

    /// <summary>Whether <paramref name="name"/> is a valid Avro name (<c>[A-Za-z_][A-Za-z0-9_]*</c>).</summary>
    public static bool IsValidName(ReadOnlySpan<char> name) =>
        !name.IsEmpty && NameStart.Contains(name[0]) && !name[1..].ContainsAnyExcept(NameRest);

    /// <summary>The most decimal digits a signed value of <paramref name="bytes"/> bytes holds.</summary>
    internal static int MaxDecimalDigits(int bytes) => Context.MaxDecimalPrecision(bytes);

    private sealed class Context
    {
        private readonly Dictionary<string, AvroNamedSchema> _named = [];

        public AvroSchema Parse(JsonElement e, string? enclosingNs) => e.ValueKind switch
        {
            JsonValueKind.String => Reference(e.GetString()!, enclosingNs),
            JsonValueKind.Array => ParseUnion(e, enclosingNs),
            JsonValueKind.Object => ParseObject(e, enclosingNs),
            _ => throw Bad($"an Avro schema must be a string, an array or an object, not {e.ValueKind}"),
        };

        private AvroSchema Reference(string name, string? enclosingNs)
        {
            if (Primitives.TryGetValue(name, out var t)) return new AvroPrimitiveSchema(t);
            // A name without a dot is relative to the enclosing namespace; fall back to the null namespace.
            if (!name.Contains('.') && !string.IsNullOrEmpty(enclosingNs) &&
                _named.TryGetValue($"{enclosingNs}.{name}", out var scoped))
                return scoped;
            return _named.TryGetValue(name, out var named)
                ? named
                : throw Bad($"unknown Avro type name '{name}'");
        }

        private AvroUnionSchema ParseUnion(JsonElement e, string? ns)
        {
            List<AvroSchema> branches = [];
            foreach (var b in e.EnumerateArray())
            {
                var branch = Parse(b, ns);
                if (branch.Type == AvroType.Union) throw Bad("Avro unions may not immediately contain other unions");
                branches.Add(branch);
            }
            if (branches.Count == 0) throw Bad("an Avro union needs at least one branch");
            return new AvroUnionSchema(branches);
        }

        private AvroSchema ParseObject(JsonElement e, string? enclosingNs)
        {
            if (!e.TryGetProperty("type", out var typeProp)) throw Bad("an Avro schema object needs a \"type\"");
            if (typeProp.ValueKind != JsonValueKind.String)
            {
                // {"type": {...}} or {"type": [...]}: a wrapped schema; logical types apply only to primitives here.
                return Parse(typeProp, enclosingNs);
            }
            string type = typeProp.GetString()!;
            return type switch
            {
                "record" or "error" => ParseRecord(e, enclosingNs),
                "enum" => ParseEnum(e, enclosingNs),
                "fixed" => ParseFixed(e, enclosingNs),
                "array" => ParseArray(e, enclosingNs),
                "map" => new AvroMapSchema(Parse(Required(e, "values"), enclosingNs)) { Properties = Rest(e, MapKeys) },
                _ when Primitives.TryGetValue(type, out var t) => WithLogical(e, new AvroPrimitiveSchema(t), t),
                _ => Reference(type, enclosingNs),
            };
        }

        private AvroArraySchema ParseArray(JsonElement e, string? enclosingNs)
        {
            var items = Parse(Required(e, "items"), enclosingNs);
            bool map = e.TryGetProperty("logicalType", out var lt) && lt.ValueKind == JsonValueKind.String
                && lt.GetString() == "map" && AvroArraySchema.IsMapEntries(items);
            return new AvroArraySchema(items)
            {
                Logical = map ? AvroLogicalKind.Map : AvroLogicalKind.None,
                Properties = Rest(e, ArrayKeys, map ? ["logicalType"] : []),
            };
        }

        private static AvroSchema WithLogical(JsonElement e, AvroSchema baseSchema, AvroType type)
        {
            var (kind, precision, scale) = LogicalOf(e, type, fixedSize: 0);
            string? timezone = kind is AvroLogicalKind.TimestampMillis or AvroLogicalKind.TimestampMicros or AvroLogicalKind.TimestampNanos
                && e.TryGetProperty(AvroSchema.TimezoneProperty, out var tz) && tz.ValueKind == JsonValueKind.String && tz.GetString() is { Length: > 0 } zone
                ? zone
                : null;
            var properties = Rest(e, PrimitiveKeys, Interpreted(kind, timezone is not null));
            return kind == AvroLogicalKind.None && properties.Count == 0
                ? baseSchema
                : new AvroPrimitiveSchema(type)
                {
                    Logical = kind, Precision = precision, Scale = scale, Timezone = timezone, Properties = properties,
                };
        }

        private static (AvroLogicalKind Kind, int Precision, int Scale) LogicalOf(JsonElement e, AvroType type, int fixedSize)
        {
            if (e.TryGetProperty("logicalType", out var lt) && lt.ValueKind == JsonValueKind.String &&
                LogicalNames.TryGetValue(lt.GetString()!, out var kind))
            {
                return kind switch
                {
                    AvroLogicalKind.Decimal => DecimalOf(e, type, fixedSize),
                    AvroLogicalKind.Uuid when type is AvroType.String || (type == AvroType.Fixed && fixedSize == 16) => (kind, 0, 0),
                    AvroLogicalKind.Date or AvroLogicalKind.TimeMillis when type == AvroType.Int => (kind, 0, 0),
                    AvroLogicalKind.TimeMicros or AvroLogicalKind.TimestampMillis or AvroLogicalKind.TimestampMicros
                        or AvroLogicalKind.TimestampNanos or AvroLogicalKind.LocalTimestampMillis
                        or AvroLogicalKind.LocalTimestampMicros or AvroLogicalKind.LocalTimestampNanos
                        when type == AvroType.Long => (kind, 0, 0),
                    AvroLogicalKind.Duration when type == AvroType.Fixed && fixedSize == 12 => (kind, 0, 0),
                    _ => (AvroLogicalKind.None, 0, 0),
                };
            }
            if (e.TryGetProperty("spark.sql.catalyst.type", out var catalyst) && catalyst.ValueKind == JsonValueKind.String)
            {
                string? s = catalyst.GetString();
                if (type == AvroType.Long && s == "interval day to second") return (AvroLogicalKind.SparkDayTimeInterval, 0, 0);
                if (type == AvroType.Int && s == "interval year to month") return (AvroLogicalKind.SparkYearMonthInterval, 0, 0);
            }
            return (AvroLogicalKind.None, 0, 0);
        }

        private static (AvroLogicalKind, int, int) DecimalOf(JsonElement e, AvroType type, int fixedSize)
        {
            if (type is not (AvroType.Bytes or AvroType.Fixed)) return (AvroLogicalKind.None, 0, 0);
            if (!e.TryGetProperty("precision", out var p) || !p.TryGetInt32(out int precision) || precision <= 0)
                return (AvroLogicalKind.None, 0, 0);
            int scale = 0;
            if (e.TryGetProperty("scale", out var s) && (!s.TryGetInt32(out scale) || scale < 0))
                return (AvroLogicalKind.None, 0, 0);
            if (scale > precision) return (AvroLogicalKind.None, 0, 0);
            if (type == AvroType.Fixed && precision > MaxDecimalPrecision(fixedSize)) return (AvroLogicalKind.None, 0, 0);
            return (AvroLogicalKind.Decimal, precision, scale);
        }

        /// <summary>The attributes a logical type annotation used (they are not kept as properties).</summary>
        private static string[] Interpreted(AvroLogicalKind kind, bool timezone) => kind switch
        {
            AvroLogicalKind.None => [],
            AvroLogicalKind.Decimal => ["logicalType", "precision", "scale"],
            AvroLogicalKind.SparkDayTimeInterval or AvroLogicalKind.SparkYearMonthInterval => ["spark.sql.catalyst.type"],
            _ when timezone => ["logicalType", AvroSchema.TimezoneProperty],
            _ => ["logicalType"],
        };

        /// <summary>The attributes of <paramref name="e"/> other than <paramref name="known"/> and
        /// <paramref name="interpreted"/>, detached from the document.</summary>
        private static IReadOnlyDictionary<string, JsonElement> Rest(JsonElement e, FrozenSet<string> known,
            params ReadOnlySpan<string> interpreted)
        {
            Dictionary<string, JsonElement>? rest = null;
            foreach (var property in e.EnumerateObject())
            {
                if (known.Contains(property.Name) || interpreted.Contains(property.Name)) continue;
                (rest ??= new(StringComparer.Ordinal))[property.Name] = property.Value.Clone();
            }
            return rest ?? AvroProperties.Empty;
        }

        /// <summary>floor(log10(2^(8n-1) - 1)): the most decimal digits a signed n-byte value holds.</summary>
        internal static int MaxDecimalPrecision(int bytes) =>
            bytes <= 0 ? 0 : (int)Math.Floor(Math.Log10(Math.Pow(2, 8.0 * bytes - 1) - 1));

        private (string Name, string? Ns) NameOf(JsonElement e, string? enclosingNs)
        {
            string name = Required(e, "name").GetString() ?? throw Bad("an Avro name must be a string");
            string? ns = e.TryGetProperty("namespace", out var nsProp) && nsProp.ValueKind == JsonValueKind.String
                ? nsProp.GetString()
                : enclosingNs;
            int dot = name.LastIndexOf('.');
            if (dot >= 0)
            {
                ns = name[..dot];
                name = name[(dot + 1)..];
            }
            return (name, string.IsNullOrEmpty(ns) ? null : ns);
        }

        private void Register(AvroNamedSchema schema)
        {
            if (!_named.TryAdd(schema.FullName, schema)) throw Bad($"the Avro type name '{schema.FullName}' is defined twice");
        }

        private static IReadOnlyList<string> Aliases(JsonElement e) =>
            e.TryGetProperty("aliases", out var a) && a.ValueKind == JsonValueKind.Array
                ? [.. a.EnumerateArray().Where(x => x.ValueKind == JsonValueKind.String).Select(x => x.GetString()!)]
                : [];

        private static string? Doc(JsonElement e) =>
            e.TryGetProperty("doc", out var d) && d.ValueKind == JsonValueKind.String ? d.GetString() : null;

        private AvroRecordSchema ParseRecord(JsonElement e, string? enclosingNs)
        {
            var (name, ns) = NameOf(e, enclosingNs);
            var record = new AvroRecordSchema(name, ns) { Doc = Doc(e), Aliases = Aliases(e), Properties = Rest(e, RecordKeys) };
            Register(record);
            var fieldsProp = Required(e, "fields");
            if (fieldsProp.ValueKind != JsonValueKind.Array) throw Bad($"the fields of Avro record '{record.FullName}' must be an array");
            List<AvroField> fields = [];
            HashSet<string> seen = [];
            foreach (var f in fieldsProp.EnumerateArray())
            {
                string fieldName = Required(f, "name").GetString() ?? throw Bad("an Avro field name must be a string");
                if (!seen.Add(fieldName)) throw Bad($"Avro record '{record.FullName}' has two fields named '{fieldName}'");
                var schema = Parse(Required(f, "type"), ns);
                fields.Add(new AvroField(fieldName, schema, fields.Count)
                {
                    Doc = Doc(f),
                    Aliases = Aliases(f),
                    Default = f.TryGetProperty("default", out var def) ? def.Clone() : null,
                    Properties = Rest(f, FieldKeys),
                });
            }
            record.Fields = fields;
            return record;
        }

        private AvroEnumSchema ParseEnum(JsonElement e, string? enclosingNs)
        {
            var (name, ns) = NameOf(e, enclosingNs);
            var symbolsProp = Required(e, "symbols");
            if (symbolsProp.ValueKind != JsonValueKind.Array) throw Bad("Avro enum symbols must be an array");
            List<string> symbols = [.. symbolsProp.EnumerateArray().Select(s => s.GetString() ?? throw Bad("an Avro enum symbol must be a string"))];
            if (symbols.Distinct().Count() != symbols.Count) throw Bad($"Avro enum '{name}' repeats a symbol");
            var schema = new AvroEnumSchema(name, ns, symbols)
            {
                Doc = Doc(e),
                Aliases = Aliases(e),
                Default = e.TryGetProperty("default", out var d) && d.ValueKind == JsonValueKind.String ? d.GetString() : null,
                Properties = Rest(e, EnumKeys),
            };
            Register(schema);
            return schema;
        }

        private AvroFixedSchema ParseFixed(JsonElement e, string? enclosingNs)
        {
            var (name, ns) = NameOf(e, enclosingNs);
            if (!Required(e, "size").TryGetInt32(out int size) || size < 0) throw Bad($"Avro fixed '{name}' needs a non-negative size");
            var (kind, precision, scale) = LogicalOf(e, AvroType.Fixed, size);
            var schema = new AvroFixedSchema(name, ns, size)
            {
                Doc = Doc(e),
                Aliases = Aliases(e),
                Logical = kind,
                Precision = precision,
                Scale = scale,
                Properties = Rest(e, FixedKeys, Interpreted(kind, timezone: false)),
            };
            Register(schema);
            return schema;
        }

        private static JsonElement Required(JsonElement e, string property) =>
            e.TryGetProperty(property, out var v) ? v : throw Bad($"an Avro schema object is missing \"{property}\"");
    }

    private static AvroFormatException Bad(string message) => new($"Invalid Avro schema: {message}.");
}
