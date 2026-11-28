using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Schema;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Files whose names match the dataset's ignoring case (<see cref="DatasetOptions.CaseInsensitiveNames"/>): each
/// file's top-level columns and struct fields (level by level, lists' elements and maps' keys and values gone
/// through) renamed to the dataset's names they match (<see cref="ArrowNames.Match"/>: the same name, else the one
/// equal ignoring case that no field names exactly); fields matching none keep their names (the dataset has none of
/// them, at any case: they are not read).
/// </summary>
/// <param name="columns">The dataset's columns from files.</param>
/// <param name="exempt">The top-level columns not from files (partition columns, the file path column).</param>
internal sealed class NameProjection(Apache.Arrow.Schema columns, Func<string, bool> exempt) : FileProjection
{
    private readonly Field[] _top = [.. columns.FieldsList.Where(f => !exempt(f.Name))];

    /// <exception cref="ParquetFormatException">Two fields of the file match one of the dataset's ignoring case, neither exactly.</exception>
    protected override void Name(SchemaShape own, ColumnMapping[] columns, Dictionary<SchemaElement, string> names)
    {
        Level([.. columns.Select(c => (c.Node, c.Read))], _top, "");

        // The fields of one level matched to the dataset's there, those matched renamed and matched below.
        void Level((SchemaNode Node, ReadNode Read)[] fields, IReadOnlyList<Field> targets, string path)
        {
            int[] matched = ArrowNames.Match([.. fields.Select(f => f.Node.Name)], [.. targets.Select(t => t.Name)],
                (first, second, target) => new ParquetFormatException(
                    $"The file's {(path.Length == 0 ? "columns" : $"fields of '{path}'")} '{first}' and '{second}' both match the dataset's '{target}' ignoring case "
                    + "(CaseInsensitiveNames): rename one of them, or read the files with exact names."));
            for (int i = 0; i < fields.Length; i++)
            {
                if (matched[i] < 0) continue;
                Field target = targets[matched[i]];
                names[fields[i].Node.Element] = target.Name;
                Descend(fields[i].Read, target.DataType, path.Length == 0 ? target.Name : $"{path}.{target.Name}");
            }
        }

        // Below a matched field: a struct's fields by name; a list's element, a map's key and value gone through.
        void Descend(ReadNode read, IArrowType type, string path)
        {
            if (type is DictionaryType d) type = d.ValueType;
            switch (read.Kind)
            {
                case ReadKind.Struct when type is StructType s && read.Node is { } node:
                    Level([.. node.Children.Select((child, k) => (child, read.Children[k]))], s.Fields, path);
                    break;
                case ReadKind.List or ReadKind.FixedSizeList when Element(type) is { } element:
                    Descend(read.Children[0], element, path + "[]");
                    break;
                case ReadKind.Map when type is MapType m:
                    Descend(read.Children[0].Children[0], m.KeyField.DataType, path + "{key}");
                    Descend(read.Children[0].Children[1], m.ValueField.DataType, path + "{value}");
                    break;
            }
        }
    }

    private static IArrowType? Element(IArrowType type) => type switch
    {
        ListType l => l.ValueDataType,
        LargeListType l => l.ValueDataType,
        ListViewType l => l.ValueDataType,
        LargeListViewType l => l.ValueDataType,
        FixedSizeListType l => l.ValueDataType,
        _ => null,
    };
}
