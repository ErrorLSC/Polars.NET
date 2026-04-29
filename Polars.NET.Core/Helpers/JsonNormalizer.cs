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
}