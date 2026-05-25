#pragma warning disable CS1573 
using System.Text.Json;
using Polars.NET.Core.Helpers;

namespace Polars.CSharp;

public readonly partial struct Polars
{
    /// <summary>
    /// Normalize semi-structured deserialized JSON data into a flat table.
    /// Dictionary objects that will not be unnested/normalized are encoded as json string data.
    /// </summary>
    /// <param name="data">Deserialized JSON objects.</param>
    /// <param name="separator">Nested records will generate names separated by sep. e.g., for separator=".", {"foo": {"bar": 0}} -> foo.bar.</param>
    /// <param name="maxLevel">Max number of levels(depth of dict) to normalize. If None, normalizes all levels.</param>
    /// <param name="schema">Overwrite the Schema when the normalized data is passed to the DataFrame constructor.</param>
    /// <param name="strict">Whether Polars should be strict when constructing the DataFrame.</param>
    /// <param name="inferSchemaLength">Number of rows to take into consideration to determine the schema.</param>
    /// <param name="encoder">Custom JSON encoder function; if not given, JsonSerializer.Serialize is used.</param>
    /// <exception cref="ArgumentException"></exception>
    public static DataFrame JsonNormalize(
        object data, // IDictionary<string, object?> or IEnumerable<IDictionary<string, object?>>
        string separator = ".",
        int? maxLevel = null,
        IntoSchema? schema = null,
        bool strict = true,
        uint? inferSchemaLength = 100, 
        Func<object?, string>? encoder = null)
    {
        int actualMaxLevel = maxLevel.HasValue ? maxLevel.Value + 1 : int.MaxValue;

        encoder ??= JsonNormalizeHelper.DefaultEncoder;

        // Unify format to List 
        IEnumerable<IDictionary<string, object?>> dataList;
        if (data is IDictionary<string, object?> singleDict)
        {
            dataList = [singleDict];
        }
        else if (data is IEnumerable<IDictionary<string, object?>> enumerable && data is not string)
        {
            dataList = enumerable;
        }
        else
        {
            throw new ArgumentException("Expected an IDictionary<string, object?> or IEnumerable<IDictionary<string, object?>>.");
        }

        // Normalize
        var normalizedData = JsonNormalizeHelper.SimpleJsonNormalize(
            dataList, 
            separator, 
            actualMaxLevel, 
            encoder
        );

        return CSharp.DataFrame.FromDicts(
            (IEnumerable<IDictionary<string, object?>>)normalizedData, 
            schema, 
            null, 
            strict,
            inferSchemaLength
        );
    }
    /// <summary>
    /// Normalize a JSON string representing a single object or an array of objects.
    /// </summary>
    /// <param name="json">A valid JSON string.</param>
    /// <inheritdoc cref="JsonNormalize(object, string, int?, IntoSchema?, bool, uint?, Func{object?, string}?)"/>
    public static DataFrame JsonNormalize(
        string json,
        string separator = ".",
        int? maxLevel = null,
        IntoSchema? schema = null,
        bool strict = true,
        uint? inferSchemaLength = 100,
        Func<object?, string>? encoder = null)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        object data;
        if (root.ValueKind == JsonValueKind.Object)
        {
            data = JsonNormalizeHelper.ConvertJsonElementToDict(root);
        }
        else if (root.ValueKind == JsonValueKind.Array)
        {
            var list = new List<IDictionary<string, object?>>();
            foreach (var element in root.EnumerateArray())
            {
                list.Add(JsonNormalizeHelper.ConvertJsonElementToDict(element));
            }
            data = list;
        }
        else
        {
            throw new ArgumentException("JSON string must be an object or an array of objects.");
        }

        return JsonNormalize(data, separator, maxLevel, schema, strict, inferSchemaLength, encoder);
    }
}