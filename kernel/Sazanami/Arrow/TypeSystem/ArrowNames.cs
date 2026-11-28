using Apache.Arrow;
using Apache.Arrow.Types;

namespace Kaminari.Sazanami.Arrow.TypeSystem;

/// <summary>
/// Names matched ignoring case, as readers that resolve columns case-insensitively match a file's fields to a table's
/// (Spark by default, DuckDB): level by level (top-level fields, then a struct's fields within the matched struct;
/// lists' elements and maps' keys and values gone through), a field taking the target of its own name, else the one
/// target equal to it ignoring case that no field matches exactly.
/// </summary>
internal static class ArrowNames
{
    /// <summary>
    /// For each source name, the target it matches (-1: none): the target of the same name; else the only target equal
    /// to it ignoring case that no source names exactly (none when two targets are).
    /// </summary>
    /// <param name="twice">The error of two sources matching one target, neither exactly: (first source, second source, target).</param>
    public static int[] Match(IReadOnlyList<string> sources, IReadOnlyList<string> targets, Func<string, string, string, Exception> twice)
    {
        int[] matched = [.. Enumerable.Repeat(-1, sources.Count)];
        var exact = new Dictionary<string, int>(targets.Count, StringComparer.Ordinal);
        for (int t = 0; t < targets.Count; t++) exact.TryAdd(targets[t], t);
        var claimed = new int[targets.Count]; // the source (+1) that matched each target, exactly or not
        var exactly = new bool[targets.Count];
        for (int s = 0; s < sources.Count; s++)
        {
            if (exact.TryGetValue(sources[s], out int t) && claimed[t] == 0)
            {
                matched[s] = t;
                claimed[t] = s + 1;
                exactly[t] = true;
            }
        }
        for (int s = 0; s < sources.Count; s++)
        {
            if (matched[s] >= 0 || exact.ContainsKey(sources[s])) continue;
            int found = -1;
            for (int t = 0; t < targets.Count; t++)
            {
                if (exactly[t] || !string.Equals(sources[s], targets[t], StringComparison.OrdinalIgnoreCase)) continue;
                if (found >= 0)
                {
                    found = -2; // two targets differ in case alone: not bound
                    break;
                }
                found = t;
            }
            if (found < 0) continue;
            if (claimed[found] != 0) throw twice(sources[claimed[found] - 1], sources[s], targets[found]);
            matched[s] = found;
            claimed[found] = s + 1;
        }
        return matched;
    }

    /// <summary>
    /// <paramref name="source"/>'s fields named as <paramref name="target"/>'s they match ignoring case (see
    /// <see cref="Match"/>), at every level; fields matching none keep their names. Types and metadata are kept.
    /// </summary>
    /// <param name="twice">The error of two fields matching one, neither exactly: (path of the level, first, second, target).</param>
    public static Schema Respell(Schema source, IReadOnlyList<Field> target, Func<string, string, string, string, Exception> twice) =>
        new([.. Respell(source.FieldsList, target, "", twice)], source.Metadata);

    private static Field[] Respell(IReadOnlyList<Field> source, IReadOnlyList<Field> target, string path, Func<string, string, string, string, Exception> twice)
    {
        int[] matched = Match([.. source.Select(f => f.Name)], [.. target.Select(f => f.Name)], (a, b, t) => twice(path, a, b, t));
        var fields = new Field[source.Count];
        for (int i = 0; i < fields.Length; i++)
        {
            Field field = source[i];
            if (matched[i] < 0)
            {
                fields[i] = field;
                continue;
            }
            Field to = target[matched[i]];
            string at = path.Length == 0 ? to.Name : $"{path}.{to.Name}";
            fields[i] = new Field(to.Name, Respell(field.DataType, to.DataType, at, twice), field.IsNullable, field.Metadata);
        }
        return fields;
    }

    private static IArrowType Respell(IArrowType source, IArrowType target, string path, Func<string, string, string, string, Exception> twice) => (source, target) switch
    {
        (StructType s, StructType t) => new StructType([.. Respell(s.Fields, t.Fields, path, twice)]),
        (ListType s, _) when Element(target) is { } t => new ListType(Item(s.ValueField, t, path + "[]", twice)),
        (LargeListType s, _) when Element(target) is { } t => new LargeListType(Item(s.ValueField, t, path + "[]", twice)),
        (ListViewType s, _) when Element(target) is { } t => new ListViewType(Item(s.ValueField, t, path + "[]", twice)),
        (LargeListViewType s, _) when Element(target) is { } t => new LargeListViewType(Item(s.ValueField, t, path + "[]", twice)),
        (FixedSizeListType s, _) when Element(target) is { } t => new FixedSizeListType(Item(s.ValueField, t, path + "[]", twice), s.ListSize),
        (MapType s, MapType t) => new MapType(Item(s.KeyField, t.KeyField.DataType, path + "{key}", twice),
            Item(s.ValueField, t.ValueField.DataType, path + "{value}", twice), s.KeySorted),
        (DictionaryType s, DictionaryType t) => new DictionaryType(s.IndexType, Respell(s.ValueType, t.ValueType, path, twice), s.Ordered),
        (DictionaryType s, _) => new DictionaryType(s.IndexType, Respell(s.ValueType, target, path, twice), s.Ordered),
        _ => source,
    };

    // A list's element, a map's key or value: its own name kept (matched by position), its type respelled.
    private static Field Item(Field field, IArrowType target, string path, Func<string, string, string, string, Exception> twice) =>
        new(field.Name, Respell(field.DataType, target, path, twice), field.IsNullable, field.Metadata);

    private static IArrowType? Element(IArrowType type) => type switch
    {
        ListType l => l.ValueDataType,
        LargeListType l => l.ValueDataType,
        ListViewType l => l.ValueDataType,
        LargeListViewType l => l.ValueDataType,
        FixedSizeListType l => l.ValueDataType,
        _ => null,
    };

    /// <summary>
    /// The first level of the fields (top-level, then struct fields at any depth) holding two names equal ignoring
    /// case: the level's path ("" at the top) and the two names; null when there is none.
    /// </summary>
    public static (string Path, string First, string Second)? CaseTwins(IReadOnlyList<Field> fields) => Twins(fields, "");

    private static (string Path, string First, string Second)? Twins(IReadOnlyList<Field> fields, string path)
    {
        var seen = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (Field field in fields)
        {
            if (!seen.TryAdd(field.Name, field.Name)) return (path, seen[field.Name], field.Name);
        }
        foreach (Field field in fields)
        {
            string at = path.Length == 0 ? field.Name : $"{path}.{field.Name}";
            if (Below(field.DataType, at) is { } found) return found;
        }
        return null;

        static (string, string, string)? Below(IArrowType type, string at) => type switch
        {
            StructType s => Twins(s.Fields, at),
            MapType m => Below(m.KeyField.DataType, at + "{key}") ?? Below(m.ValueField.DataType, at + "{value}"),
            DictionaryType d => Below(d.ValueType, at),
            _ when Element(type) is { } element => Below(element, at + "[]"),
            _ => null,
        };
    }
}
