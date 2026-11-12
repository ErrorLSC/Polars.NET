using System.Buffers;
using System.Text;
using System.Text.Json;

namespace Kaminari.Seseragi.Lakehouse.Catalog.Unity;

/// <summary>The kinds of column type Unity Catalog accepts.</summary>
public enum UnityTypeKind
{
    /// <summary>boolean</summary>
    Boolean,
    /// <summary>8-bit signed integer</summary>
    Byte,
    /// <summary>16-bit signed integer</summary>
    Short,
    /// <summary>32-bit signed integer</summary>
    Int,
    /// <summary>64-bit signed integer</summary>
    Long,
    /// <summary>32-bit float</summary>
    Float,
    /// <summary>64-bit float</summary>
    Double,
    /// <summary>calendar date</summary>
    Date,
    /// <summary>timestamp with time zone</summary>
    Timestamp,
    /// <summary>timestamp without time zone</summary>
    TimestampNtz,
    /// <summary>UTF-8 string</summary>
    String,
    /// <summary>binary</summary>
    Binary,
    /// <summary>null</summary>
    Null,
    /// <summary>fixed-point decimal</summary>
    Decimal,
    /// <summary>array of one element type</summary>
    Array,
    /// <summary>map from one key type to one value type</summary>
    Map,
    /// <summary>struct of named fields</summary>
    Struct,
}

/// <summary>A named field of a <see cref="UnityDataType"/> struct.</summary>
public readonly record struct UnityField(string Name, UnityDataType Type);

/// <summary>
/// A column type as Unity Catalog describes it. It renders the three forms the REST API asks for:
/// <c>type_name</c>, <c>type_text</c> and <c>type_json</c>.
/// </summary>
public sealed class UnityDataType
{
    /// <summary>The kind of this type.</summary>
    public UnityTypeKind Kind { get; }

    /// <summary>Decimal precision; 0 for other kinds.</summary>
    public int Precision { get; }

    /// <summary>Decimal scale; 0 for other kinds.</summary>
    public int Scale { get; }

    /// <summary>The element type of an array, else null.</summary>
    public UnityDataType? Element { get; }

    /// <summary>The key type of a map, else null.</summary>
    public UnityDataType? Key { get; }

    /// <summary>The value type of a map, else null.</summary>
    public UnityDataType? Value { get; }

    /// <summary>The fields of a struct, else empty.</summary>
    public IReadOnlyList<UnityField> Fields { get; }

    private UnityDataType(
        UnityTypeKind kind,
        int precision = 0,
        int scale = 0,
        UnityDataType? element = null,
        UnityDataType? key = null,
        UnityDataType? value = null,
        IReadOnlyList<UnityField>? fields = null)
    {
        Kind = kind;
        Precision = precision;
        Scale = scale;
        Element = element;
        Key = key;
        Value = value;
        Fields = fields ?? [];
    }

#pragma warning disable CS1591 // the names say it all
    public static UnityDataType Boolean { get; } = new(UnityTypeKind.Boolean);
    public static UnityDataType Byte { get; } = new(UnityTypeKind.Byte);
    public static UnityDataType Short { get; } = new(UnityTypeKind.Short);
    public static UnityDataType Int { get; } = new(UnityTypeKind.Int);
    public static UnityDataType Long { get; } = new(UnityTypeKind.Long);
    public static UnityDataType Float { get; } = new(UnityTypeKind.Float);
    public static UnityDataType Double { get; } = new(UnityTypeKind.Double);
    public static UnityDataType Date { get; } = new(UnityTypeKind.Date);
    public static UnityDataType Timestamp { get; } = new(UnityTypeKind.Timestamp);
    public static UnityDataType TimestampNtz { get; } = new(UnityTypeKind.TimestampNtz);
    public static UnityDataType String { get; } = new(UnityTypeKind.String);
    public static UnityDataType Binary { get; } = new(UnityTypeKind.Binary);
    public static UnityDataType Null { get; } = new(UnityTypeKind.Null);
#pragma warning restore CS1591

    /// <summary>A decimal with the given precision and scale.</summary>
    public static UnityDataType Decimal(int precision, int scale) =>
        new(UnityTypeKind.Decimal, precision: precision, scale: scale);

    /// <summary>An array of <paramref name="element"/>.</summary>
    public static UnityDataType Array(UnityDataType element) =>
        new(UnityTypeKind.Array, element: element ?? throw new ArgumentNullException(nameof(element)));

    /// <summary>A map from <paramref name="key"/> to <paramref name="value"/>.</summary>
    public static UnityDataType Map(UnityDataType key, UnityDataType value) =>
        new(UnityTypeKind.Map,
            key: key ?? throw new ArgumentNullException(nameof(key)),
            value: value ?? throw new ArgumentNullException(nameof(value)));

    /// <summary>A struct of the given fields.</summary>
    public static UnityDataType Struct(IEnumerable<UnityField> fields) =>
        new(UnityTypeKind.Struct, fields: [.. fields ?? throw new ArgumentNullException(nameof(fields))]);

    /// <summary>The <c>type_name</c> form, for example <c>LONG</c> or <c>ARRAY</c>.</summary>
    public string TypeName => Kind switch
    {
        UnityTypeKind.Boolean => "BOOLEAN",
        UnityTypeKind.Byte => "BYTE",
        UnityTypeKind.Short => "SHORT",
        UnityTypeKind.Int => "INT",
        UnityTypeKind.Long => "LONG",
        UnityTypeKind.Float => "FLOAT",
        UnityTypeKind.Double => "DOUBLE",
        UnityTypeKind.Date => "DATE",
        UnityTypeKind.Timestamp => "TIMESTAMP",
        UnityTypeKind.TimestampNtz => "TIMESTAMP_NTZ",
        UnityTypeKind.String => "STRING",
        UnityTypeKind.Binary => "BINARY",
        UnityTypeKind.Null => "NULL",
        UnityTypeKind.Decimal => "DECIMAL",
        UnityTypeKind.Array => "ARRAY",
        UnityTypeKind.Map => "MAP",
        UnityTypeKind.Struct => "STRUCT",
        _ => throw new InvalidOperationException($"Unknown type kind {Kind}."),
    };

    /// <summary>The <c>type_text</c> form, for example <c>bigint</c> or <c>array&lt;string&gt;</c>.</summary>
    public string TypeText
    {
        get
        {
            var sb = new StringBuilder();
            AppendTypeText(sb);
            return sb.ToString();
        }
    }

    private void AppendTypeText(StringBuilder sb)
    {
        switch (Kind)
        {
            case UnityTypeKind.Boolean: sb.Append("boolean"); break;
            case UnityTypeKind.Byte: sb.Append("tinyint"); break;
            case UnityTypeKind.Short: sb.Append("smallint"); break;
            case UnityTypeKind.Int: sb.Append("int"); break;
            case UnityTypeKind.Long: sb.Append("bigint"); break;
            case UnityTypeKind.Float: sb.Append("float"); break;
            case UnityTypeKind.Double: sb.Append("double"); break;
            case UnityTypeKind.Date: sb.Append("date"); break;
            case UnityTypeKind.Timestamp: sb.Append("timestamp"); break;
            case UnityTypeKind.TimestampNtz: sb.Append("timestamp_ntz"); break;
            case UnityTypeKind.String: sb.Append("string"); break;
            case UnityTypeKind.Binary: sb.Append("binary"); break;
            case UnityTypeKind.Null: sb.Append("null"); break;
            case UnityTypeKind.Decimal:
                sb.Append("decimal(").Append(Precision).Append(',').Append(Scale).Append(')');
                break;
            case UnityTypeKind.Array:
                sb.Append("array<");
                Element!.AppendTypeText(sb);
                sb.Append('>');
                break;
            case UnityTypeKind.Map:
                sb.Append("map<");
                Key!.AppendTypeText(sb);
                sb.Append(',');
                Value!.AppendTypeText(sb);
                sb.Append('>');
                break;
            case UnityTypeKind.Struct:
                sb.Append("struct<");
                for (int i = 0; i < Fields.Count; i++)
                {
                    if (i > 0) sb.Append(',');
                    sb.Append(Fields[i].Name).Append(':');
                    Fields[i].Type.AppendTypeText(sb);
                }
                sb.Append('>');
                break;
            default:
                throw new InvalidOperationException($"Unknown type kind {Kind}.");
        }
    }

    /// <summary>
    /// The <c>type_json</c> form of a column of this type: a JSON object with the column name, the type,
    /// <c>nullable</c> and empty <c>metadata</c>, as the service itself produces it.
    /// </summary>
    public string ToTypeJson(string columnName)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            WriteField(writer, columnName, this);
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private static void WriteField(Utf8JsonWriter writer, string name, UnityDataType type)
    {
        writer.WriteStartObject();
        writer.WriteString("name", name);
        writer.WritePropertyName("type");
        type.WriteType(writer);
        writer.WriteBoolean("nullable", true);
        writer.WriteStartObject("metadata");
        writer.WriteEndObject();
        writer.WriteEndObject();
    }

    private void WriteType(Utf8JsonWriter writer)
    {
        switch (Kind)
        {
            case UnityTypeKind.Boolean: writer.WriteStringValue("boolean"); break;
            case UnityTypeKind.Byte: writer.WriteStringValue("byte"); break;
            case UnityTypeKind.Short: writer.WriteStringValue("short"); break;
            case UnityTypeKind.Int: writer.WriteStringValue("integer"); break;
            case UnityTypeKind.Long: writer.WriteStringValue("long"); break;
            case UnityTypeKind.Float: writer.WriteStringValue("float"); break;
            case UnityTypeKind.Double: writer.WriteStringValue("double"); break;
            case UnityTypeKind.Date: writer.WriteStringValue("date"); break;
            case UnityTypeKind.Timestamp: writer.WriteStringValue("timestamp"); break;
            case UnityTypeKind.TimestampNtz: writer.WriteStringValue("timestamp_ntz"); break;
            case UnityTypeKind.String: writer.WriteStringValue("string"); break;
            case UnityTypeKind.Binary: writer.WriteStringValue("binary"); break;
            case UnityTypeKind.Null: writer.WriteStringValue("null"); break;
            case UnityTypeKind.Decimal: writer.WriteStringValue(TypeText); break;
            case UnityTypeKind.Array:
                writer.WriteStartObject();
                writer.WriteString("type", "array");
                writer.WritePropertyName("elementType");
                Element!.WriteType(writer);
                writer.WriteBoolean("containsNull", true);
                writer.WriteEndObject();
                break;
            case UnityTypeKind.Map:
                writer.WriteStartObject();
                writer.WriteString("type", "map");
                writer.WritePropertyName("keyType");
                Key!.WriteType(writer);
                writer.WritePropertyName("valueType");
                Value!.WriteType(writer);
                writer.WriteBoolean("valueContainsNull", true);
                writer.WriteEndObject();
                break;
            case UnityTypeKind.Struct:
                writer.WriteStartObject();
                writer.WriteString("type", "struct");
                writer.WriteStartArray("fields");
                foreach (var field in Fields)
                {
                    WriteField(writer, field.Name, field.Type);
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
                break;
            default:
                throw new InvalidOperationException($"Unknown type kind {Kind}.");
        }
    }

    /// <inheritdoc/>
    public override string ToString() => TypeText;
}
