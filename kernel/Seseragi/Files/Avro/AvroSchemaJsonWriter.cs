using System.Buffers;
using System.Text;
using System.Text.Json;

namespace Kaminari.Seseragi.Files.Avro;

/// <summary>Writes an <see cref="AvroSchema"/> as JSON with <see cref="Utf8JsonWriter"/>. A named type is
/// written in full the first time and by its full name afterwards.</summary>
internal static class AvroSchemaJsonWriter
{
    public static string Write(AvroSchema schema) => Encoding.UTF8.GetString(WriteUtf8(schema));

    public static byte[] WriteUtf8(AvroSchema schema)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var w = new Utf8JsonWriter(buffer))
        {
            WriteSchema(w, schema, null, new Dictionary<string, AvroNamedSchema>(StringComparer.Ordinal));
        }
        return buffer.WrittenSpan.ToArray();
    }

    private static void WriteSchema(Utf8JsonWriter w, AvroSchema schema, string? enclosingNs, Dictionary<string, AvroNamedSchema> written)
    {
        switch (schema)
        {
            case AvroUnionSchema union:
                w.WriteStartArray();
                foreach (var b in union.Branches) WriteSchema(w, b, enclosingNs, written);
                w.WriteEndArray();
                return;

            // A named type is defined once and referenced by name after; a different type of the same name would be
            // read back as the first one.
            case AvroNamedSchema named when written.TryGetValue(named.FullName, out var first):
                if (!ReferenceEquals(first, named))
                    throw new InvalidOperationException($"Two different Avro types are named '{named.FullName}'.");
                w.WriteStringValue(named.FullName);
                return;

            case AvroNamedSchema named when !written.TryAdd(named.FullName, named):
                throw new InvalidOperationException("Unreachable.");

            case AvroRecordSchema record:
                w.WriteStartObject();
                w.WriteString("type"u8, "record"u8);
                WriteName(w, record, enclosingNs);
                w.WriteStartArray("fields"u8);
                foreach (var f in record.Fields)
                {
                    w.WriteStartObject();
                    w.WriteString("name"u8, f.Name);
                    if (f.Doc is { } fdoc) w.WriteString("doc"u8, fdoc);
                    w.WritePropertyName("type"u8);
                    WriteSchema(w, f.Schema, record.Namespace, written);
                    if (f.DefaultJson is { } def)
                    {
                        w.WritePropertyName("default"u8);
                        w.WriteRawValue(def);
                    }
                    WriteAliases(w, f.Aliases);
                    WriteProperties(w, f.Properties);
                    w.WriteEndObject();
                }
                w.WriteEndArray();
                WriteProperties(w, record.Properties);
                w.WriteEndObject();
                return;

            case AvroEnumSchema e:
                w.WriteStartObject();
                w.WriteString("type"u8, "enum"u8);
                WriteName(w, e, enclosingNs);
                w.WriteStartArray("symbols"u8);
                foreach (var s in e.Symbols) w.WriteStringValue(s);
                w.WriteEndArray();
                if (e.Default is { } d) w.WriteString("default"u8, d);
                WriteProperties(w, e.Properties);
                w.WriteEndObject();
                return;

            case AvroFixedSchema f:
                w.WriteStartObject();
                w.WriteString("type"u8, "fixed"u8);
                WriteName(w, f, enclosingNs);
                w.WriteNumber("size"u8, f.Size);
                WriteLogical(w, f);
                WriteProperties(w, f.Properties);
                w.WriteEndObject();
                return;

            case AvroArraySchema a:
                w.WriteStartObject();
                w.WriteString("type"u8, "array"u8);
                w.WritePropertyName("items"u8);
                WriteSchema(w, a.Items, enclosingNs, written);
                WriteLogical(w, a);
                WriteProperties(w, a.Properties);
                w.WriteEndObject();
                return;

            case AvroMapSchema m:
                w.WriteStartObject();
                w.WriteString("type"u8, "map"u8);
                w.WritePropertyName("values"u8);
                WriteSchema(w, m.Values, enclosingNs, written);
                WriteProperties(w, m.Properties);
                w.WriteEndObject();
                return;

            default:
                if (schema.Logical == AvroLogicalKind.None && schema.Properties.Count == 0)
                {
                    w.WriteStringValue(PrimitiveName(schema.Type));
                    return;
                }
                w.WriteStartObject();
                w.WriteString("type"u8, PrimitiveName(schema.Type));
                WriteLogical(w, schema);
                WriteProperties(w, schema.Properties);
                w.WriteEndObject();
                return;
        }
    }

    private static void WriteProperties(Utf8JsonWriter w, IReadOnlyDictionary<string, JsonElement> properties)
    {
        foreach (var (name, value) in properties)
        {
            w.WritePropertyName(name);
            value.WriteTo(w);
        }
    }

    private static void WriteAliases(Utf8JsonWriter w, IReadOnlyList<string> aliases)
    {
        if (aliases.Count == 0) return;
        w.WriteStartArray("aliases"u8);
        foreach (string alias in aliases) w.WriteStringValue(alias);
        w.WriteEndArray();
    }

    // Name, namespace, doc and aliases of a named type.
    private static void WriteName(Utf8JsonWriter w, AvroNamedSchema s, string? enclosingNs)
    {
        w.WriteString("name"u8, s.Name);
        if (s.Namespace != enclosingNs) w.WriteString("namespace"u8, s.Namespace ?? "");
        if (s.Doc is { } doc) w.WriteString("doc"u8, doc);
        WriteAliases(w, s.Aliases);
    }

    private static void WriteLogical(Utf8JsonWriter w, AvroSchema s)
    {
        switch (s.Logical)
        {
            case AvroLogicalKind.None:
                return;
            case AvroLogicalKind.SparkDayTimeInterval:
                w.WriteString("spark.sql.catalyst.type"u8, "interval day to second"u8);
                return;
            case AvroLogicalKind.SparkYearMonthInterval:
                w.WriteString("spark.sql.catalyst.type"u8, "interval year to month"u8);
                return;
            case AvroLogicalKind.Decimal:
                w.WriteString("logicalType"u8, "decimal"u8);
                w.WriteNumber("precision"u8, s.Precision);
                w.WriteNumber("scale"u8, s.Scale);
                return;
            default:
                w.WriteString("logicalType"u8, LogicalName(s.Logical));
                if (s.Timezone is { } tz) w.WriteString(AvroSchema.TimezoneProperty, tz);
                return;
        }
    }

    private static string PrimitiveName(AvroType t) => t switch
    {
        AvroType.Null => "null",
        AvroType.Boolean => "boolean",
        AvroType.Int => "int",
        AvroType.Long => "long",
        AvroType.Float => "float",
        AvroType.Double => "double",
        AvroType.Bytes => "bytes",
        AvroType.String => "string",
        _ => throw new InvalidOperationException($"{t} is not a primitive Avro type."),
    };

    private static string LogicalName(AvroLogicalKind k) => k switch
    {
        AvroLogicalKind.Uuid => "uuid",
        AvroLogicalKind.Date => "date",
        AvroLogicalKind.TimeMillis => "time-millis",
        AvroLogicalKind.TimeMicros => "time-micros",
        AvroLogicalKind.TimestampMillis => "timestamp-millis",
        AvroLogicalKind.TimestampMicros => "timestamp-micros",
        AvroLogicalKind.TimestampNanos => "timestamp-nanos",
        AvroLogicalKind.LocalTimestampMillis => "local-timestamp-millis",
        AvroLogicalKind.LocalTimestampMicros => "local-timestamp-micros",
        AvroLogicalKind.LocalTimestampNanos => "local-timestamp-nanos",
        AvroLogicalKind.Duration => "duration",
        AvroLogicalKind.Map => "map",
        _ => throw new InvalidOperationException($"No logicalType name for {k}."),
    };
}
