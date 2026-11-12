using System.Buffers;
using System.Collections.Frozen;
using System.Text.Json;

namespace Kaminari.Seseragi.Files.Avro;

internal enum AvroType
{
    Null, Boolean, Int, Long, Float, Double, Bytes, String,
    Record, Enum, Array, Map, Union, Fixed,
}

internal enum AvroLogicalKind
{
    None,
    Decimal,
    Uuid,
    Date,
    TimeMillis,
    TimeMicros,
    TimestampMillis,
    TimestampMicros,
    TimestampNanos,
    LocalTimestampMillis,
    LocalTimestampMicros,
    LocalTimestampNanos,
    Duration,
    /// <summary>Spark's day-time interval: a long of microseconds tagged with <c>spark.sql.catalyst.type</c>.</summary>
    SparkDayTimeInterval,
    /// <summary>Spark's year-month interval: an int of months tagged with <c>spark.sql.catalyst.type</c>.</summary>
    SparkYearMonthInterval,
}

/// <summary>A node of a parsed Avro schema (Avro 1.12 specification).</summary>
internal abstract class AvroSchema(AvroType type)
{
    public AvroType Type { get; } = type;

    /// <summary>A valid logical type annotation; invalid or unknown annotations are dropped at parse time,
    /// as the specification requires (the value is read as its underlying type).</summary>
    public AvroLogicalKind Logical { get; init; }

    public int Precision { get; init; }
    public int Scale { get; init; }

    /// <summary>
    /// The time zone of a <c>timestamp-*</c> (instant) value, kept in the <see cref="TimezoneProperty"/> schema
    /// attribute. Avro itself has no place for it; other readers ignore the attribute and read a UTC instant.
    /// </summary>
    public string? Timezone { get; init; }

    /// <summary>Schema attribute carrying the original Arrow time zone of a <c>timestamp-*</c> field.</summary>
    public const string TimezoneProperty = "kaminari.timezone";

    public static AvroSchema Parse(string json) => AvroSchemaParser.Parse(json);

    public static AvroSchema Parse(ReadOnlySpan<byte> utf8Json) => AvroSchemaParser.Parse(utf8Json);

    public string ToJson() => AvroSchemaJsonWriter.Write(this);

    public override string ToString() => ToJson();
}

internal sealed class AvroPrimitiveSchema(AvroType type) : AvroSchema(type);

internal abstract class AvroNamedSchema(AvroType type, string name, string? ns) : AvroSchema(type)
{
    public string Name { get; } = name;
    public string? Namespace { get; } = ns;
    public string FullName => string.IsNullOrEmpty(Namespace) ? Name : $"{Namespace}.{Name}";
    public string? Doc { get; init; }
    public IReadOnlyList<string> Aliases { get; init; } = [];
}

internal sealed class AvroRecordSchema(string name, string? ns) : AvroNamedSchema(AvroType.Record, name, ns)
{
    // Filled after construction so that recursive references can point back at the record.
    public IReadOnlyList<AvroField> Fields { get; internal set; } = [];
}

internal sealed record AvroField(string Name, AvroSchema Schema, int Position)
{
    public string? Doc { get; init; }
    /// <summary>The raw JSON default value, if the field declares one.</summary>
    public string? DefaultJson { get; init; }
}

internal sealed class AvroEnumSchema(string name, string? ns, IReadOnlyList<string> symbols)
    : AvroNamedSchema(AvroType.Enum, name, ns)
{
    public IReadOnlyList<string> Symbols { get; } = symbols;
    public string? Default { get; init; }
}

internal sealed class AvroFixedSchema(string name, string? ns, int size) : AvroNamedSchema(AvroType.Fixed, name, ns)
{
    public int Size { get; } = size;
}

internal sealed class AvroArraySchema(AvroSchema items) : AvroSchema(AvroType.Array)
{
    public AvroSchema Items { get; } = items;
}

internal sealed class AvroMapSchema(AvroSchema values) : AvroSchema(AvroType.Map)
{
    public AvroSchema Values { get; } = values;
}

internal sealed class AvroUnionSchema(IReadOnlyList<AvroSchema> branches) : AvroSchema(AvroType.Union)
{
    public IReadOnlyList<AvroSchema> Branches { get; } = branches;

    /// <summary>Index of the <c>null</c> branch, or -1.</summary>
    public int NullIndex { get; } = IndexOfNull(branches);

    private static int IndexOfNull(IReadOnlyList<AvroSchema> branches)
    {
        for (int i = 0; i < branches.Count; i++)
            if (branches[i].Type == AvroType.Null) return i;
        return -1;
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
                "array" => new AvroArraySchema(Parse(Required(e, "items"), enclosingNs)),
                "map" => new AvroMapSchema(Parse(Required(e, "values"), enclosingNs)),
                _ when Primitives.TryGetValue(type, out var t) => WithLogical(e, new AvroPrimitiveSchema(t), t),
                _ => Reference(type, enclosingNs),
            };
        }

        private static AvroSchema WithLogical(JsonElement e, AvroSchema baseSchema, AvroType type)
        {
            var (kind, precision, scale) = LogicalOf(e, type, fixedSize: 0);
            string? timezone = kind is AvroLogicalKind.TimestampMillis or AvroLogicalKind.TimestampMicros or AvroLogicalKind.TimestampNanos
                && e.TryGetProperty(AvroSchema.TimezoneProperty, out var tz) && tz.ValueKind == JsonValueKind.String && tz.GetString() is { Length: > 0 } zone
                ? zone
                : null;
            return kind == AvroLogicalKind.None
                ? baseSchema
                : new AvroPrimitiveSchema(type) { Logical = kind, Precision = precision, Scale = scale, Timezone = timezone };
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

        /// <summary>floor(log10(2^(8n-1) - 1)): the most decimal digits a signed n-byte value holds.</summary>
        private static int MaxDecimalPrecision(int bytes) =>
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
            var record = new AvroRecordSchema(name, ns) { Doc = Doc(e), Aliases = Aliases(e) };
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
                    DefaultJson = f.TryGetProperty("default", out var def) ? def.GetRawText() : null,
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
            };
            Register(schema);
            return schema;
        }

        private static JsonElement Required(JsonElement e, string property) =>
            e.TryGetProperty(property, out var v) ? v : throw Bad($"an Avro schema object is missing \"{property}\"");
    }

    private static AvroFormatException Bad(string message) => new($"Invalid Avro schema: {message}.");
}
