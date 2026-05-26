using System.Text.Json;

namespace Polars.NET.Core.Helpers;

internal static class JsonNormalizeHelper
{
    internal static readonly Func<object?, string> DefaultEncoder = 
        obj => JsonSerializer.Serialize(obj);

    private static void NormalizeJson(
        IDictionary<string, object?> data,
        string keyString,
        IDictionary<string, object?> normalizedDict,
        string separator,
        int maxLevel,
        Func<object?, string> encoder)
    {
        if (maxLevel > 0)
        {
            string keyRoot = string.IsNullOrEmpty(keyString) ? "" : $"{keyString}{separator}";
            int nestedMaxLevel = maxLevel - 1;

            foreach (var kvp in data)
            {
                string newKey = $"{keyRoot}{kvp.Key}";

                if (kvp.Value is IDictionary<string, object?> nestedDict)
                {
                    NormalizeJson(nestedDict, newKey, normalizedDict, separator, nestedMaxLevel, encoder);
                }
                else
                {
                    normalizedDict[newKey] = kvp.Value;
                }
            }
        }
        else
        {
            normalizedDict[keyString] = encoder(data);
        }
    }

    private static Dictionary<string, object?> NormalizeJsonOrdered(
        IDictionary<string, object?> data,
        string separator,
        int maxLevel,
        Func<object?, string> encoder)
    {
        var top = new Dictionary<string, object?>();
        var nestedData = new Dictionary<string, object?>();

        foreach (var kvp in data)
        {
            if (kvp.Value is IDictionary<string, object?> nestedDict)
            {
                nestedData[kvp.Key] = nestedDict;
            }
            else
            {
                top[kvp.Key] = kvp.Value;
            }
        }

        var normalized = new Dictionary<string, object?>(top);

        NormalizeJson(
            data: nestedData,
            keyString: "",
            normalizedDict: normalized,
            separator: separator,
            maxLevel: maxLevel,
            encoder: encoder
        );

        return normalized;
    }

    internal static object SimpleJsonNormalize(
        object data,
        string separator,
        int maxLevel,
        Func<object?, string> encoder)
    {
        if (maxLevel > 0)
        {
            if (data is IDictionary<string, object?> dictData)
            {
                return NormalizeJsonOrdered(dictData, separator, maxLevel, encoder);
            }
            else if (data is IEnumerable<IDictionary<string, object?>> listData)
            {
                var normalizedList = new List<IDictionary<string, object?>>();
                foreach (var row in listData)
                {
                    var normalizedRow = SimpleJsonNormalize(row, separator, maxLevel, encoder);
                    normalizedList.Add((IDictionary<string, object?>)normalizedRow);
                }
                return normalizedList;
            }
        }
        
        return data; 
    }
    /// <summary>
    /// Recursively convert a <see cref="JsonElement"/> into a corresponding CLR object.
    /// </summary>
    /// <param name="element">The JSON element to convert.</param>
    /// <returns>
    /// A boxed primitive (string, long, double, bool), null, 
    /// a <see cref="Dictionary{string, object?}"/> for JSON objects,
    /// or an <see cref="List{object?}"/> for JSON arrays.
    /// </returns>
    public static object? ConvertJsonElement(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => ParseJsonNumber(element),
            JsonValueKind.Object => ConvertJsonObject(element),
            JsonValueKind.Array => ConvertJsonArray(element),
            _ => element.GetRawText()
        };
    }

    private static object ParseJsonNumber(JsonElement element)
    {
        if (element.TryGetInt64(out long i))
            return i;
        if (element.TryGetDouble(out double d))
            return d;
        return element.GetRawText();
    }

    private static Dictionary<string, object?> ConvertJsonObject(JsonElement element)
    {
        var dict = new Dictionary<string, object?>();
        foreach (var property in element.EnumerateObject())
        {
            dict[property.Name] = ConvertJsonElement(property.Value);
        }
        return dict;
    }

    private static List<object?> ConvertJsonArray(JsonElement element)
    {
        var list = new List<object?>();
        foreach (var item in element.EnumerateArray())
        {
            list.Add(ConvertJsonElement(item));
        }
        return list;
    }
    /// <summary>
    /// Converts a JSON object element into an <see cref="IDictionary{string, object?}"/>.
    /// </summary>
    public static IDictionary<string, object?> ConvertJsonElementToDict(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Object)
            throw new ArgumentException("Element must be a JSON object.", nameof(element));
        return (IDictionary<string, object?>)ConvertJsonElement(element)!;
    }
}