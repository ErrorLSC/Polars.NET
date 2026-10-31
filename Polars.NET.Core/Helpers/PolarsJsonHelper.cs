using System.Buffers;
using System.Text;
using System.Text.Json;

namespace Polars.NET.Core.Helpers;

internal static class PolarsJsonHelper
{
    public static string SerializeDictionary<TValue>(IEnumerable<KeyValuePair<string, TValue>> dict)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            foreach (var kvp in dict)
            {
                if (kvp.Value is null)
                {
                    writer.WriteNull(kvp.Key);
                }
                else
                {
                    writer.WriteString(kvp.Key, kvp.Value.ToString());
                }
            }
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }
}
