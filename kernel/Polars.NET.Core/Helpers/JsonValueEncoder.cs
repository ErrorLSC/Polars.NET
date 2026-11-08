using System.Buffers;
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;

namespace Polars.NET.Core.Helpers;

/// <summary>
/// Encodes a value as a JSON string. JSON-shaped values (null, strings, numbers, booleans, dates, Guid,
/// byte[] as base64, <see cref="JsonElement"/>, dictionaries and sequences of these) are written directly with
/// <see cref="Utf8JsonWriter"/>, without reflection. Only other objects (POCOs, enums, TimeSpan, ...) fall back
/// to reflection-based <see cref="JsonSerializer"/>, which is why the entry point is annotated.
/// Output matches <c>JsonSerializer.Serialize(object)</c> with default options for the shapes handled here.
/// </summary>
internal static class JsonValueEncoder
{
    [RequiresUnreferencedCode(AotMessages.JsonPocoFallback)]
    [RequiresDynamicCode(AotMessages.JsonPocoFallback)]
    public static string Encode(object? value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            Write(writer, value);
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    [RequiresUnreferencedCode(AotMessages.JsonPocoFallback)]
    [RequiresDynamicCode(AotMessages.JsonPocoFallback)]
    private static void Write(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null: writer.WriteNullValue(); break;
            case string s: writer.WriteStringValue(s); break;
            case char c: writer.WriteStringValue(c.ToString()); break;
            case bool b: writer.WriteBooleanValue(b); break;
            case sbyte v: writer.WriteNumberValue(v); break;
            case byte v: writer.WriteNumberValue(v); break;
            case short v: writer.WriteNumberValue(v); break;
            case ushort v: writer.WriteNumberValue(v); break;
            case int v: writer.WriteNumberValue(v); break;
            case uint v: writer.WriteNumberValue(v); break;
            case long v: writer.WriteNumberValue(v); break;
            case ulong v: writer.WriteNumberValue(v); break;
            case float v: writer.WriteNumberValue(v); break;
            case double v: writer.WriteNumberValue(v); break;
            case decimal v: writer.WriteNumberValue(v); break;
            case DateTime v: writer.WriteStringValue(v); break;
            case DateTimeOffset v: writer.WriteStringValue(v); break;
            case Guid v: writer.WriteStringValue(v); break;
            case byte[] bytes: writer.WriteBase64StringValue(bytes); break;
            case JsonElement element: element.WriteTo(writer); break;
            case JsonDocument document: document.WriteTo(writer); break;
            case IDictionary<string, object?> dict:
                writer.WriteStartObject();
                foreach (var kv in dict)
                {
                    writer.WritePropertyName(kv.Key);
                    Write(writer, kv.Value);
                }
                writer.WriteEndObject();
                break;
            case IDictionary dict when AllKeysAreStrings(dict):
                writer.WriteStartObject();
                foreach (DictionaryEntry kv in dict)
                {
                    writer.WritePropertyName((string)kv.Key);
                    Write(writer, kv.Value);
                }
                writer.WriteEndObject();
                break;
            case IEnumerable sequence when value is not IDictionary && !IsGenericDictionary(value.GetType()):
                writer.WriteStartArray();
                foreach (var item in sequence)
                {
                    Write(writer, item);
                }
                writer.WriteEndArray();
                break;
            default:
                // POCOs, enums, TimeSpan, non-string-keyed dictionaries, ...: reflection-based serializer.
                JsonSerializer.Serialize(writer, value, value.GetType());
                break;
        }
    }

    // Dictionary<string, int>, F# Map<string, int>, ...: their entries can't be read without reflection,
    // so they go to JsonSerializer (as a JSON object) instead of being written as a sequence of pairs.
    [RequiresUnreferencedCode(AotMessages.JsonPocoFallback)]
    private static bool IsGenericDictionary(Type type)
    {
        foreach (var iface in type.GetInterfaces())
        {
            if (!iface.IsGenericType) continue;
            var def = iface.GetGenericTypeDefinition();
            if (def == typeof(IDictionary<,>) || def == typeof(IReadOnlyDictionary<,>)) return true;
        }
        return false;
    }

    private static bool AllKeysAreStrings(IDictionary dict)
    {
        foreach (var key in dict.Keys)
        {
            if (key is not string) return false;
        }
        return true;
    }
}
