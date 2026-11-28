using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Schema;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Files read by field id (<see cref="ParquetFieldMatching.ByFieldId"/>): each file's schema elements renamed to the
/// dataset schema's names by their ids (top-level columns and struct fields, level by level; lists' elements and maps'
/// keys and values gone through), its fields without a matching id renamed out of reach, the stored Arrow schema
/// renamed alike. Everything after (selection, conversions, missing columns, filters, statistics, page indexes, Bloom
/// filters) then works by the schema's names. Files without any field id take their ids from the name mapping. One
/// projection per file shape, kept; safe on any thread.
/// </summary>
internal sealed class FieldIdProjection : FileProjection
{
    /// <summary>A field of the schema at some level: its name there and its type (whose struct fields are matched below it).</summary>
    private sealed record Target(string Name, IArrowType Type);

    private readonly Dictionary<int, Target> _top;
    private readonly Dictionary<StructType, Dictionary<int, Target>> _structs = new(ReferenceEqualityComparer.Instance);
    private readonly NameMapping? _mapping;

    /// <summary>The prefix of the names of file fields no schema field matches (no query names them).</summary>
    internal const string Unmatched = "\u001funmatched ";

    /// <param name="schema">The dataset's schema, its fields with their ids.</param>
    /// <param name="mapping">The ids of files without any, by their fields' names; or null.</param>
    /// <param name="exempt">The top-level columns not from files (partition columns, the file path column): no id needed.</param>
    /// <exception cref="ArgumentException">A field without an id, or an id twice.</exception>
    public FieldIdProjection(Apache.Arrow.Schema schema, NameMapping? mapping, Func<string, bool> exempt)
    {
        _mapping = mapping;
        HashSet<int> ids = [];
        _top = Level([.. schema.FieldsList.Where(f => !exempt(f.Name))], "");

        Dictionary<int, Target> Level(IReadOnlyList<Field> fields, string parent)
        {
            Dictionary<int, Target> level = [];
            foreach (Field field in fields)
            {
                string path = parent.Length == 0 ? field.Name : $"{parent}.{field.Name}";
                if (ArrowFieldIds.Of(field) is not int id)
                    throw new ArgumentException($"The schema's field '{path}' has no field id (PARQUET:field_id): matching by field id needs one on every column and struct field.", "options");
                if (!ids.Add(id)) throw new ArgumentException($"The schema's field '{path}' has the field id {id}, which another field has too.", "options");
                level[id] = new Target(field.Name, field.DataType);
                Nested(field.DataType, path);
            }
            return level;
        }

        void Nested(IArrowType type, string path)
        {
            switch (type)
            {
                case StructType s:
                    _structs[s] = Level(s.Fields, path);
                    break;
                case ListType l:
                    Nested(l.ValueDataType, path + "[]");
                    break;
                case LargeListType l:
                    Nested(l.ValueDataType, path + "[]");
                    break;
                case FixedSizeListType l:
                    Nested(l.ValueDataType, path + "[]");
                    break;
                case MapType m:
                    Nested(m.KeyField.DataType, path + "{key}");
                    Nested(m.ValueField.DataType, path + "{value}");
                    break;
            }
        }
    }

    /// <exception cref="ParquetFormatException">The file has no field ids and there is no name mapping.</exception>
    protected override void Name(SchemaShape own, ColumnMapping[] columns, Dictionary<SchemaElement, string> names)
    {
        SchemaElement[] elements = own.Elements;
        bool ids = false;
        for (int e = 1; e < elements.Length && !ids; e++) ids = elements[e].FieldId is not null;
        if (!ids && _mapping is null)
            throw new ParquetFormatException("The file has no field ids to match the schema's by: give the table's name mapping (ParquetDatasetOptions.NameMapping).");
        int unmatched = 0;
        foreach (ColumnMapping column in columns) Match(column.Node, column.Read, _top, ids ? null : _mapping!.Fields);

        // A file field at a level: renamed to the schema field of its id (from the file, or the mapping), its nested
        // fields matched below; out of reach when no schema field has its id.
        void Match(SchemaNode node, ReadNode read, Dictionary<int, Target> level, IReadOnlyList<MappedField>? mapped)
        {
            MappedField? entry = mapped is null ? null : Find(mapped, node.Name);
            int? id = mapped is null ? node.Element.FieldId : entry?.FieldId;
            if (id is not int found || !level.TryGetValue(found, out Target? target))
            {
                names[node.Element] = Unmatched + unmatched++;
                return;
            }
            names[node.Element] = target.Name;
            Descend(read, target.Type, mapped is null ? null : entry?.Fields ?? []);
        }

        // Below a matched field: a struct's fields by id; a list's element, a map's key and value gone through.
        void Descend(ReadNode read, IArrowType type, IReadOnlyList<MappedField>? mapped)
        {
            switch (read.Kind)
            {
                case ReadKind.Struct when type is StructType s && read.Node is { } node && _structs.TryGetValue(s, out Dictionary<int, Target>? level):
                    for (int k = 0; k < read.Children.Length; k++) Match(node.Children[k], read.Children[k], level, mapped);
                    break;
                case ReadKind.List or ReadKind.FixedSizeList when ElementType(type) is { } element:
                {
                    ReadNode item = read.Children[0];
                    Descend(item, element, Below(mapped, item.Node, 0));
                    break;
                }
                case ReadKind.Map when type is MapType m:
                {
                    ReadNode entries = read.Children[0];
                    Descend(entries.Children[0], m.KeyField.DataType, Below(mapped, entries.Children[0].Node, 0));
                    Descend(entries.Children[1], m.ValueField.DataType, Below(mapped, entries.Children[1].Node, 1));
                    break;
                }
            }
        }
    }

    /// <summary>The entries of a list's element or a map's key or value: by the node's name, else by position.</summary>
    private static IReadOnlyList<MappedField>? Below(IReadOnlyList<MappedField>? mapped, SchemaNode? node, int position)
    {
        if (mapped is null) return null;
        MappedField? entry = node is null ? null : Find(mapped, node.Name);
        entry ??= mapped.Count == 1 ? mapped[0] : position < mapped.Count ? mapped[position] : null;
        return entry?.Fields ?? [];
    }

    private static MappedField? Find(IReadOnlyList<MappedField> mapped, string name)
    {
        foreach (MappedField entry in mapped)
        {
            foreach (string alias in entry.Names)
            {
                if (alias == name) return entry;
            }
        }
        return null;
    }

    private static IArrowType? ElementType(IArrowType type) => type switch
    {
        ListType l => l.ValueDataType,
        LargeListType l => l.ValueDataType,
        FixedSizeListType l => l.ValueDataType,
        _ => null,
    };
}
