using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Schema;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// How a dataset's files are read under the dataset's names: each file's schema elements renamed (top-level columns
/// and struct fields; a list's element and a map's key and value keep theirs), the stored Arrow schema renamed alike,
/// so that everything after (selection, conversions, missing columns, filters, statistics, page indexes, Bloom filters)
/// works by the dataset's names. A subclass says how fields match (<see cref="FieldIdProjection"/> by id,
/// <see cref="NameProjection"/> by name ignoring case). One projection per file shape, kept; safe on any thread.
/// </summary>
internal abstract class FileProjection
{
    private readonly object _lock = new();
    private readonly Dictionary<SchemaShape, SchemaShape> _projected = new(ReferenceEqualityComparer.Instance);
    private const int MaxShapes = 256;

    /// <summary>The shape <paramref name="file"/> is read through (made once per shape of the file's own).</summary>
    /// <exception cref="ParquetFormatException">The file cannot be matched to the dataset (the subclass says why).</exception>
    public SchemaShape Of(ParquetFile file)
    {
        SchemaShape own = file.OwnShape;
        lock (_lock)
        {
            if (_projected.TryGetValue(own, out SchemaShape? known)) return known;
        }
        SchemaShape made = Project(own);
        lock (_lock)
        {
            if (_projected.TryGetValue(own, out SchemaShape? known)) return known;
            if (_projected.Count < MaxShapes) _projected.Add(own, made);
        }
        return made;
    }

    /// <summary>
    /// The new names of the file's elements (<paramref name="names"/>; an element left out keeps its own), from its
    /// columns as the reader sees them (lists and maps by the backward-compatibility rules).
    /// </summary>
    protected abstract void Name(SchemaShape own, ColumnMapping[] columns, Dictionary<SchemaElement, string> names);

    private SchemaShape Project(SchemaShape own)
    {
        // The structure as the reader sees it; types do not matter.
        var (_, columns) = own.Mapping(new ParquetReadOptions());
        var names = new Dictionary<SchemaElement, string>(ReferenceEqualityComparer.Instance);
        Name(own, columns, names);
        SchemaElement[] renamed = [.. own.Elements.Select(e => names.TryGetValue(e, out string? name) ? Renamed(e, name) : e)];
        Apache.Arrow.Schema? stored = own.StoredSchema is { } s ? new Apache.Arrow.Schema([.. s.FieldsList.Select(Stored)], s.Metadata) : null;
        return SchemaShape.WithStored(renamed, stored);

        // The stored Arrow schema renamed as the elements are: its top-level fields and struct fields by their names
        // in the file (they refine the elements of those names).
        Field Stored(Field field)
        {
            ColumnMapping? column = System.Array.Find(columns, c => c.Node.Name == field.Name);
            return column is null ? field : Rename(field, column.Node, column.Read);
        }

        Field Rename(Field field, SchemaNode node, ReadNode read) =>
            new(names.GetValueOrDefault(node.Element, field.Name), RenameType(field.DataType, read), field.IsNullable, field.Metadata);

        IArrowType RenameType(IArrowType type, ReadNode read) => (type, read.Kind) switch
        {
            (StructType s, ReadKind.Struct) when read.Node is { } node => new StructType([.. s.Fields.Select(f =>
            {
                int k = System.Array.FindIndex(node.Children, c => c.Name == f.Name);
                return k < 0 ? f : Rename(f, node.Children[k], read.Children[k]);
            })]),
            (ListType l, ReadKind.List or ReadKind.FixedSizeList) => new ListType(Item(l.ValueField, read.Children[0])),
            (LargeListType l, ReadKind.List or ReadKind.FixedSizeList) => new LargeListType(Item(l.ValueField, read.Children[0])),
            (FixedSizeListType l, ReadKind.List or ReadKind.FixedSizeList) => new FixedSizeListType(Item(l.ValueField, read.Children[0]), l.ListSize),
            (MapType m, ReadKind.Map) => new MapType(Item(m.KeyField, read.Children[0].Children[0]), Item(m.ValueField, read.Children[0].Children[1]), m.KeySorted),
            _ => type,
        };

        // An element, key or value: its name kept (matched by position), its type renamed.
        Field Item(Field field, ReadNode read) => new(field.Name, RenameType(field.DataType, read), field.IsNullable, field.Metadata);
    }

    private static SchemaElement Renamed(SchemaElement e, string name) => new()
    {
        Type = e.Type, TypeLength = e.TypeLength, Repetition = e.Repetition, Name = name, NumChildren = e.NumChildren,
        ConvertedType = e.ConvertedType, Scale = e.Scale, Precision = e.Precision, FieldId = e.FieldId, LogicalType = e.LogicalType,
    };
}
