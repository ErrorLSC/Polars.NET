using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Arrow.TypeSystem;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// How the columns of an IPC dataset's files unite (<see cref="DatasetSchemaMode.UnionByName"/>): as
/// <see cref="ArrowWidening.Common"/> unites them, but strings and binaries in two layouts (plain, view) keep the layout
/// of the file that had the column first, at any depth (Arrow IPC files keep the layout their writer chose: a dataset
/// of files from several writers mixes them).
/// </summary>
internal static class IpcDatasetTypes
{
    public static IArrowType? Common(IArrowType a, IArrowType b, bool lossy)
    {
        if (ArrowTypes.SameType(a, b)) return a;
        return (a, b) switch
        {
            (StringType or StringViewType, StringType or StringViewType) => a,
            (BinaryType or BinaryViewType, BinaryType or BinaryViewType) => a,
            (StructType x, StructType y) => Struct(x, y, lossy),
            (ListType x, ListType y) => Common(x.ValueDataType, y.ValueDataType, lossy) is { } element
                ? new ListType(new Field(x.ValueField.Name, element, nullable: true)) : null,
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
