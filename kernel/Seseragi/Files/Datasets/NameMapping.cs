using System.Text.Json;

namespace Kaminari.Seseragi.Files;

/// <summary>
/// Field ids for files written without them, by their fields' names (Iceberg's name mapping, the table property
/// <c>schema.name-mapping.default</c>): a file field takes the id of the mapped field listing its name at its level
/// (a struct's fields under the struct's entry, a list's element and a map's key and value under the list's or the
/// map's); a field no entry names has no id.
/// </summary>
/// <param name="Fields">The top-level fields' entries.</param>
public sealed record NameMapping(IReadOnlyList<MappedField> Fields)
{
    /// <summary>
    /// The mapping from its JSON (as Iceberg stores it): an array of objects with <c>names</c> (strings),
    /// <c>field-id</c> (optional) and <c>fields</c> (optional, the nested entries).
    /// </summary>
    /// <exception cref="FormatException">The text is not such an array.</exception>
    public static NameMapping Parse(string json)
    {
        ArgumentNullException.ThrowIfNull(json);
        try
        {
            using JsonDocument document = JsonDocument.Parse(json);
            return new NameMapping(Entries(document.RootElement, "the mapping"));
        }
        catch (JsonException e)
        {
            throw new FormatException($"The name mapping is not valid JSON: {e.Message}", e);
        }

        static MappedField[] Entries(JsonElement array, string where)
        {
            if (array.ValueKind != JsonValueKind.Array) throw new FormatException($"The entries of {where} are not a JSON array.");
            return [.. array.EnumerateArray().Select(entry => Entry(entry, where))];
        }

        static MappedField Entry(JsonElement entry, string where)
        {
            if (entry.ValueKind != JsonValueKind.Object) throw new FormatException($"An entry of {where} is not a JSON object.");
            int? id = null;
            if (entry.TryGetProperty("field-id", out JsonElement idValue) && idValue.ValueKind != JsonValueKind.Null)
            {
                if (idValue.ValueKind != JsonValueKind.Number || !idValue.TryGetInt32(out int parsed))
                    throw new FormatException($"An entry of {where} has a field-id that is not an integer.");
                id = parsed;
            }
            if (!entry.TryGetProperty("names", out JsonElement namesValue) || namesValue.ValueKind != JsonValueKind.Array)
                throw new FormatException($"An entry of {where} has no names array.");
            string[] names = [.. namesValue.EnumerateArray().Select(n => n.ValueKind == JsonValueKind.String
                ? n.GetString()!
                : throw new FormatException($"An entry of {where} has a name that is not a string."))];
            string here = names.Length > 0 ? $"'{names[0]}'" : where;
            MappedField[]? fields = entry.TryGetProperty("fields", out JsonElement nested) && nested.ValueKind != JsonValueKind.Null
                ? Entries(nested, here)
                : null;
            return new MappedField(id, names, fields);
        }
    }
}

/// <summary>An entry of a <see cref="NameMapping"/>.</summary>
/// <param name="FieldId">The id of the fields named so (null: such fields have none).</param>
/// <param name="Names">The names the field had over time (any of them matches).</param>
/// <param name="Fields">The entries of the nested fields (a struct's fields, a list's element, a map's key and value).</param>
public sealed record MappedField(int? FieldId, IReadOnlyList<string> Names, IReadOnlyList<MappedField>? Fields = null);
