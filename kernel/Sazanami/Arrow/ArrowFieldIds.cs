using Apache.Arrow;
using System.Globalization;

namespace Kaminari.Sazanami.Arrow;

/// <summary>
/// Field ids in Arrow field metadata: the integer stored under <see cref="MetadataKey"/>, the key pyarrow,
/// pyiceberg and Parquet writers use (<c>PARQUET:field_id</c>). The Kaminari readers attach it to fields whose
/// source carries an id (an Avro <c>field-id</c>, <c>element-id</c>, <c>key-id</c> or <c>value-id</c>) and the
/// writers turn it back into the format's id attribute.
/// </summary>
public static class ArrowFieldIds
{
    /// <summary>The field-metadata key that holds a field id.</summary>
    public const string MetadataKey = "PARQUET:field_id";

    /// <summary>The id of <paramref name="field"/>, or null when it has none.</summary>
    /// <exception cref="FormatException">The metadata value is not an integer.</exception>
    public static int? Of(Field field) => Of(field, MetadataKey);

    /// <summary>The id stored under <paramref name="key"/> in the metadata of <paramref name="field"/>, or null.</summary>
    /// <exception cref="FormatException">The metadata value is not an integer.</exception>
    public static int? Of(Field field, string key) =>
        field.HasMetadata && field.Metadata.TryGetValue(key, out string? id)
            ? int.TryParse(id, NumberStyles.Integer, CultureInfo.InvariantCulture, out int value)
                ? value
                : throw new FormatException($"Field '{field.Name}' has an invalid id '{id}'.")
            : null;

    /// <summary>A field like <paramref name="field"/> with the id <paramref name="id"/> (none when null), keeping
    /// its other metadata.</summary>
    public static Field With(Field field, int? id)
    {
        List<KeyValuePair<string, string>> metadata = field.HasMetadata
            ? [.. field.Metadata.Where(m => m.Key != MetadataKey)]
            : [];
        if (id is int value) metadata.Add(new(MetadataKey, value.ToString(CultureInfo.InvariantCulture)));
        return new(field.Name, field.DataType, field.IsNullable, metadata);
    }
}
