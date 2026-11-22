using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Seseragi.Files.Json.Reading;

namespace Kaminari.Seseragi.Files.Json;

/// <summary>
/// The columns rows of <typeparamref name="T"/> are written and read as JSON with: <see cref="ArrowRowBuilder{T}"/>'s
/// layout, with GUIDs as strings and dictionaries keyed by strings as objects (as JSON serializers write them).
/// </summary>
internal static class JsonRowSchema<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>
{
    /// <summary>The rows' batches as written: the layout's types, GUID and object columns marked.</summary>
    public static Schema Write { get; } = JsonRowSchema.Of(ArrowRowBuilder<T>.Schema, ArrowRowBuilder<T>.Columns, write: true);

    /// <summary>
    /// The columns read for the rows: GUIDs marked, dictionaries keyed by strings as maps, enums named by strings (names
    /// or numbers, as the rows read them).
    /// </summary>
    public static Schema Read { get; } = JsonRowSchema.Of(ArrowRowBuilder<T>.Schema, ArrowRowBuilder<T>.Columns, write: false);
}

internal static class JsonRowSchema
{
    public static Schema Of(Schema schema, IReadOnlyList<RowNode> columns, bool write) =>
        new([.. schema.FieldsList.Select((f, i) => Field(f, columns[i], write))], schema.Metadata);

    private static Field Field(Field field, RowNode node, bool write)
    {
        if (node.IsGuid) return Marked(field, field.DataType, JsonColumn.GuidKey);
        switch (field.DataType)
        {
            case LargeListType { ValueField: { DataType: StructType entry } item } when node.IsMap:
            {
                RowNode entryNode = node.Children[0];
                Field key = Field(entry.Fields[0], entryNode.Children[0], write), value = Field(entry.Fields[1], entryNode.Children[1], write);
                if (key.DataType is not (StringType or LargeStringType or StringViewType))
                    return With(field, new LargeListType(new Field(item.Name, new StructType([key, value]), item.IsNullable, item.Metadata)));
                return write
                    ? Marked(field, new LargeListType(new Field(item.Name, new StructType([key, value]), item.IsNullable, item.Metadata)), JsonColumn.ObjectKey)
                    : With(field, new MapType(new Field("key", key.DataType, false), value));
            }
            case LargeListType list:
                return With(field, new LargeListType(Field(list.ValueField, node.Children[0], write)));
            case StructType structType:
                return With(field, new StructType([.. structType.Fields.Select((f, i) => Field(f, node.Children[i], write))]));
            case DictionaryType when !write:
                return With(field, StringViewType.Default); // an enum's names (or numbers) as text
            default:
                return field;
        }
    }

    private static Field With(Field field, IArrowType type) => new(field.Name, type, field.IsNullable, field.Metadata);

    private static Field Marked(Field field, IArrowType type, string key) =>
        new(field.Name, type, field.IsNullable, new Dictionary<string, string>(field.Metadata ?? new Dictionary<string, string>()) { [key] = "" });
}
