using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Arrow.TypeSystem;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// How the columns of an IPC dataset's files unite (<see cref="DatasetSchemaMode.UnionByName"/>): as
/// <see cref="ArrowWidening.Common"/> unites them, but strings, binaries and lists in several layouts (plain, large,
/// view) keep the layout of the file that had the column first, at any depth (Arrow IPC files keep the layout their
/// writer chose: a dataset of files from several writers mixes them); dictionaries stay dictionaries, their indices
/// widened to a type holding both (a dictionary beside plain values gives plain values).
/// </summary>
internal static class IpcDatasetTypes
{
    public static IArrowType? Common(IArrowType a, IArrowType b, bool lossy)
    {
        if (ArrowTypes.SameType(a, b)) return a;
        return (a, b) switch
        {
            (StringType or LargeStringType or StringViewType, StringType or LargeStringType or StringViewType) => a,
            (BinaryType or LargeBinaryType or BinaryViewType, BinaryType or LargeBinaryType or BinaryViewType) => a,
            // Dictionaries: indices widened to a type holding both, values united; beside plain values, plain values.
            (DictionaryType x, DictionaryType y) => Common(x.ValueType, y.ValueType, lossy) is { } values
                ? ArrowWidening.CommonInteger((IntegerType)x.IndexType, (IntegerType)y.IndexType, lossy: false) is IntegerType index
                    ? new DictionaryType(index, values, x.Ordered && y.Ordered)
                    : values
                : null,
            (DictionaryType x, _) => Common(x.ValueType, b, lossy),
            (_, DictionaryType y) => Common(a, y.ValueType, lossy),
            (StructType x, StructType y) => Struct(x, y, lossy),
            (ListType or LargeListType or ListViewType or LargeListViewType, ListType or LargeListType or ListViewType or LargeListViewType) =>
                Common(ArrowWidening.ListElement(a), ArrowWidening.ListElement(b), lossy) is { } element ? ArrowWidening.WithElement(a, a, element) : null,
            (FixedSizeListType x, FixedSizeListType y) when x.ListSize == y.ListSize => Common(x.ValueDataType, y.ValueDataType, lossy) is { } element
                ? new FixedSizeListType(new Field(x.ValueField.Name, element, nullable: true), x.ListSize) : null,
            (MapType x, MapType y) => Common(x.KeyField.DataType, y.KeyField.DataType, lossy) is { } key
                && Common(x.ValueField.DataType, y.ValueField.DataType, lossy) is { } value
                ? new MapType(new Field(x.KeyField.Name, key, nullable: false), new Field(x.ValueField.Name, value, nullable: true), x.KeySorted && y.KeySorted)
                : null,
            _ => ArrowWidening.Common(a, b, lossy),
        };
    }

    /// <summary>Structs united field by field, by name: <paramref name="a"/>'s first, then those only <paramref name="b"/> has (nullable).</summary>
    private static StructType? Struct(StructType a, StructType b, bool lossy)
    {
        List<Field> fields = [];
        foreach (Field field in a.Fields)
        {
            int at = b.GetFieldIndex(field.Name, StringComparer.Ordinal);
            if (at < 0)
            {
                fields.Add(new Field(field.Name, field.DataType, nullable: true, field.Metadata));
                continue;
            }
            if (Common(field.DataType, b.Fields[at].DataType, lossy) is not { } type) return null;
            fields.Add(new Field(field.Name, type, nullable: true, field.Metadata));
        }
        foreach (Field field in b.Fields)
        {
            if (a.GetFieldIndex(field.Name, StringComparer.Ordinal) < 0) fields.Add(new Field(field.Name, field.DataType, nullable: true, field.Metadata));
        }
        return new StructType(fields);
    }
}
