using System.Text.Json;

namespace Kaminari.Seseragi.Lakehouse.Delta.Actions;

/// <summary>
/// Small JSON texts the log stores inside strings (operation parameters, field metadata), written with
/// <see cref="Utf8JsonWriter"/>: no reflection, so trimming and native AOT keep working.
/// </summary>
public static class JsonText
{
    /// <summary>A JSON array of strings, such as <c>["a","b"]</c>.</summary>
    public static string StringArray(IEnumerable<string> values)
    {
        ArgumentNullException.ThrowIfNull(values);
        return Write(json =>
        {
            json.WriteStartArray();
            foreach (string value in values) json.WriteStringValue(value);
            json.WriteEndArray();
        });
    }

    /// <summary>A JSON array of objects with string values, such as <c>[{"k":"v"}]</c>.</summary>
    public static string ObjectArray(IEnumerable<IEnumerable<KeyValuePair<string, string>>> objects)
    {
        ArgumentNullException.ThrowIfNull(objects);
        return Write(json =>
        {
            json.WriteStartArray();
            foreach (var properties in objects)
            {
                json.WriteStartObject();
                foreach (var (name, value) in properties) json.WriteString(name, value);
                json.WriteEndObject();
            }
            json.WriteEndArray();
        });
    }

    /// <summary>
    /// A JSON array with one object appended: <paramref name="array"/>'s elements copied as they are (null or not
    /// an array: none), then the new object's string properties.
    /// </summary>
    public static string Append(string? array, IEnumerable<KeyValuePair<string, string>> item)
    {
        ArgumentNullException.ThrowIfNull(item);
        JsonDocument? existing = null;
        try
        {
            if (!string.IsNullOrEmpty(array)) existing = JsonDocument.Parse(array);
        }
        catch (JsonException)
        {
            existing = null;
        }
        using (existing)
        {
            return Write(json =>
            {
                json.WriteStartArray();
                if (existing?.RootElement.ValueKind == JsonValueKind.Array)
                {
                    foreach (JsonElement element in existing.RootElement.EnumerateArray()) element.WriteTo(json);
                }
                json.WriteStartObject();
                foreach (var (name, value) in item) json.WriteString(name, value);
                json.WriteEndObject();
                json.WriteEndArray();
            });
        }
    }

    private static string Write(Action<Utf8JsonWriter> body)
    {
        var buffer = new System.Buffers.ArrayBufferWriter<byte>();
        using (var json = new Utf8JsonWriter(buffer, ActionWriter.JsonOptions)) body(json);
        return System.Text.Encoding.UTF8.GetString(buffer.WrittenSpan);
    }
}
