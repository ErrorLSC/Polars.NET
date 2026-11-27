using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Sazanami.Marshalling;
using Kaminari.Seseragi.Files.Csv.Reading;

namespace Kaminari.Seseragi.Files.Csv;

/// <summary>
/// The columns rows of <typeparamref name="T"/> are written and read as CSV with: <see cref="ArrowRowBuilder{T}"/>'s
/// layout, one column per member, with GUIDs as their text, enums as their member names (read from names in any case
/// or numbers) and decimals written with the fewest digits that hold them. CSV holds one value per field: a member that
/// is a record, a collection, a dictionary or bytes is refused (<see cref="NotSupportedException"/> naming it).
/// </summary>
internal static class CsvRowSchema<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>
{
    private static readonly (Schema? Write, Schema? Read, string? Refusal) Layout = CsvRowSchema.Of(typeof(T), ArrowRowBuilder<T>.Schema, ArrowRowBuilder<T>.Columns);

    /// <summary>The rows' batches as written: the layout's types, GUIDs and decimals marked.</summary>
    /// <exception cref="NotSupportedException">A member CSV cannot hold.</exception>
    public static Schema Write => Layout.Write ?? throw new NotSupportedException(Layout.Refusal);

    /// <summary>The columns read for the rows: GUIDs marked, enums as their member names (strings).</summary>
    /// <exception cref="NotSupportedException">A member CSV cannot hold.</exception>
    public static Schema Read => Layout.Read ?? throw new NotSupportedException(Layout.Refusal);
}

internal static class CsvRowSchema
{
    public static (Schema? Write, Schema? Read, string? Refusal) Of(Type rows, Schema schema, IReadOnlyList<RowNode> columns)
    {
        for (int i = 0; i < schema.FieldsList.Count; i++)
            if (Refusal(rows, schema.FieldsList[i], columns[i]) is { } refusal) return (null, null, refusal);
        return (new Schema([.. schema.FieldsList.Select((f, i) => Field(f, columns[i], write: true))], schema.Metadata),
            new Schema([.. schema.FieldsList.Select((f, i) => Field(f, columns[i], write: false))], schema.Metadata), null);
    }

    // Why a member cannot be a CSV column, or null.
    private static string? Refusal(Type rows, Field field, RowNode node)
    {
        if (node.IsGuid) return null;
        string? what = field.DataType switch
        {
            Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type => null, // Arrow's decimals are fixed-size binaries
            BinaryType or LargeBinaryType or BinaryViewType or FixedSizeBinaryType => "bytes",
            ListType or LargeListType or FixedSizeListType => "a collection",
            MapType => "a dictionary",
            StructType => "a record",
            _ => null,
        };
        return what is null ? null
            : $"The member '{field.Name}' of {rows.Name} is {what}: a CSV field holds one value. Leave the member out of the rows, "
              + "make it a flat value (text, a number, a date...), or write such rows as JSON or Parquet.";
    }

    private static Field Field(Field field, RowNode node, bool write)
    {
        if (node.IsGuid) return Marked(field, field.DataType, CsvRowColumns.GuidKey, "");
        return field.DataType switch
        {
            Decimal128Type when write && field.Metadata?.ContainsKey(WideIntegers.MetadataKey) != true => Marked(field, field.DataType, CsvRowColumns.ShortestKey, ""),
            // An enum's names (in any case) or numbers, read as its member names.
            DictionaryType when !write && node.EnumType is { } enumType =>
                Marked(field, StringViewType.Default, CsvRowColumns.EnumKey, EnumNameLookup.Members(enumType)),
            DictionaryType when !write => new Field(field.Name, StringViewType.Default, field.IsNullable, field.Metadata),
            _ => field,
        };
    }

    private static Field Marked(Field field, IArrowType type, string key, string value) =>
        new(field.Name, type, field.IsNullable, new Dictionary<string, string>(field.Metadata ?? new Dictionary<string, string>()) { [key] = value });
}
