using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Kaminari.Seseragi.Files.Parquet.Schema;

/// <summary>A top-level column of the file: its Arrow field, its node, how to read it, and for a flat leaf its value mapping.</summary>
internal sealed record ColumnMapping(Field Field, SchemaNode Node, ReadNode Read, LeafType? Leaf)
{
    /// <summary>Whether the column is one leaf directly under the root (no nesting, no repetition).</summary>
    public bool IsFlat => Leaf is not null;
}

/// <summary>How a node of the Arrow schema is read.</summary>
internal enum ReadKind { Leaf, Struct, List, FixedSizeList, Map }

/// <summary>
/// A node of the Arrow schema with how it is assembled from Dremel levels. Each entry of a leaf (one value, null, or
/// absent element) carries a definition level <c>def</c> and a repetition level <c>rep</c>. An entry makes a slot of
/// this node when <c>rep &lt;= SlotRep</c> and <c>def &gt;= SlotDef</c>; the slot is null when <c>def &lt; NullDef</c>.
/// For lists and maps, an entry is an element when <c>def &gt;= ElementDef</c>, and entries at <c>rep ==
/// ElementRep</c> add elements to the current slot.
/// </summary>
internal sealed class ReadNode
{
    public required Field Field { get; init; }
    public required ReadKind Kind { get; init; }
    public ReadNode[] Children { get; init; } = [];

    /// <summary>For a leaf: its Parquet node and value mapping.</summary>
    public SchemaNode? Leaf { get; init; }
    public LeafType? LeafType { get; init; }

    public int SlotDef { get; init; }
    public int SlotRep { get; init; }
    public int NullDef { get; init; }
    public int ElementDef { get; init; }
    public int ElementRep { get; init; }

    /// <summary>A map key: a null is an error (Arrow keys are never null).</summary>
    public bool RejectNulls { get; init; }

    public int ListSize => (Field.DataType as FixedSizeListType)?.ListSize ?? 0;

    /// <summary>The leaves under this node, in column order.</summary>
    public IEnumerable<ReadNode> Leaves() => Kind == ReadKind.Leaf ? [this] : Children.SelectMany(c => c.Leaves());

    /// <summary>The first leaf: its levels define this node's slots.</summary>
    public ReadNode FirstLeaf => Kind == ReadKind.Leaf ? this : Children[0].FirstLeaf;

    public ReadNode With(Field field, ReadNode[] children) => new()
    {
        Field = field, Kind = Kind, Children = children, Leaf = Leaf, LeafType = LeafType, SlotDef = SlotDef, SlotRep = SlotRep,
        NullDef = NullDef, ElementDef = ElementDef, ElementRep = ElementRep, RejectNulls = RejectNulls,
    };
}

/// <summary>
/// Maps the Parquet schema tree to an Arrow schema: leaves by <see cref="LeafTypes"/>, groups as structs, lists and
/// maps by the rules of LogicalTypes.md (backward compatibility included), refined by the Arrow schema a writer stored
/// in the <c>ARROW:schema</c> key (time zones, durations, fixed-size lists, dictionary columns, field metadata).
/// Where that schema disagrees with the Parquet types, the Parquet types win.
/// </summary>
internal static class ArrowMapping
{
    public const string ArrowSchemaKey = "ARROW:schema";

    public static (Apache.Arrow.Schema Schema, ColumnMapping[] Columns) Map(ParquetSchemaTree tree, FileMetaData meta, ParquetReadOptions options)
    {
        Apache.Arrow.Schema? stored = StoredArrowSchema(meta);
        var columns = new ColumnMapping[tree.Root.Children.Length];
        for (int i = 0; i < columns.Length; i++)
        {
            SchemaNode node = tree.Root.Children[i];
            Field? hint = stored?.FieldsList.FirstOrDefault(f => f.Name == node.Name);
            ReadNode read = NodeRead(node, hint, options);
            LeafType? leaf = read.Kind == ReadKind.Leaf ? read.LeafType : null;
            columns[i] = new ColumnMapping(read.Field, node, read, leaf);
        }
        return (new Apache.Arrow.Schema([.. columns.Select(c => c.Field)], null), columns);
    }

    /// <summary>The Arrow schema stored by the writer, or null (absent or unreadable: it only refines the mapping).</summary>
    public static Apache.Arrow.Schema? StoredArrowSchema(FileMetaData meta)
    {
        string? encoded = meta.KeyValueMetadata?.LastOrDefault(kv => kv.Key == ArrowSchemaKey)?.Value;
        if (string.IsNullOrEmpty(encoded)) return null;
        try
        {
            byte[] bytes = Convert.FromBase64String(encoded);
            using var reader = new ArrowStreamReader(new MemoryStream(bytes));
            return reader.Schema;
        }
        catch (Exception e) when (e is FormatException or InvalidDataException or IOException or ArgumentException
                                      or InvalidOperationException or IndexOutOfRangeException or NotSupportedException or OverflowException)
        {
            return null;
        }
    }

    // ------------------------------------------------------------------
    // Leaves
    // ------------------------------------------------------------------

    private static LeafType Leaf(SchemaNode node, Field? hint, ParquetReadOptions options)
    {
        LeafType leaf = Hinted(node, hint, options);
        return !options.ViewTypes || leaf.Dictionary ? leaf : leaf.ArrowType switch
        {
            StringType => leaf with { ArrowType = StringViewType.Default },
            BinaryType => leaf with { ArrowType = BinaryViewType.Default },
            _ => leaf,
        };
    }

    private static LeafType Hinted(SchemaNode node, Field? hint, ParquetReadOptions options)
    {
        LeafType leaf = LeafTypes.Resolve(node.Element, options);
        if (hint is null) return leaf;
        IArrowType want = hint.DataType;
        switch (leaf.Kind, leaf.ArrowType, want)
        {
            case (ValueKind.Int64, TimestampType ours, TimestampType theirs) when ours.Unit == theirs.Unit:
                return leaf with { ArrowType = new TimestampType(ours.Unit, Zone(ours, theirs)) };
            case (ValueKind.Int64, TimestampType ours, TimestampType theirs):
            {
                // Writers store units Parquet lacks (seconds) or a format version lacks (nanoseconds in 1.0) in another.
                long from = LeafTypes.Nanos(ours.Unit), to = LeafTypes.Nanos(theirs.Unit);
                return leaf with
                {
                    ArrowType = new TimestampType(theirs.Unit, Zone(ours, theirs)),
                    Kind = ValueKind.RescaledInt64,
                    Unit = theirs.Unit,
                    Multiplier = from > to ? from / to : 1,
                    Divisor = to > from ? to / from : 1,
                };
            }
            case (ValueKind.Int96Timestamp, _, TimestampType theirs):
                return leaf with { ArrowType = new TimestampType(theirs.Unit, theirs.Timezone), Unit = theirs.Unit };
            case (ValueKind.Int64, Int64Type, Int8Type or Int16Type or Int32Type or UInt8Type or UInt16Type or UInt32Type):
                return leaf with { ArrowType = want, Kind = ValueKind.NarrowInt64 };
            case (ValueKind.Int32, Int32Type, Int8Type or Int16Type or UInt8Type or UInt16Type or UInt32Type):
                return leaf with
                {
                    ArrowType = want,
                    Kind = want switch
                    {
                        Int8Type => ValueKind.Int8,
                        Int16Type => ValueKind.Int16,
                        UInt8Type => ValueKind.UInt8,
                        UInt16Type => ValueKind.UInt16,
                        _ => ValueKind.Int32,
                    },
                };
            case (ValueKind.FlbaTimestamp, TimestampType ours, TimestampType theirs):
                return leaf with { ArrowType = new TimestampType(ours.Unit, Zone(ours, theirs)) };
            case (ValueKind.Int64, Int64Type, DurationType duration):
                return leaf with { ArrowType = duration };
            case (ValueKind.Utf8 or ValueKind.Binary, _, DictionaryType { ValueType: StringType or LargeStringType or BinaryType or LargeBinaryType }):
                return leaf with { ArrowType = new DictionaryType(Int32Type.Default, leaf.ArrowType, false), Dictionary = true };
            default:
                return leaf;
        }
    }

    /// <summary>
    /// The zone of a timestamp: the stored schema names the zone of a UTC-adjusted value; it cannot make a local
    /// value an instant or an instant local (Parquet's isAdjustedToUTC decides that).
    /// </summary>
    private static string? Zone(TimestampType ours, TimestampType theirs) =>
        ours.Timezone is not null && !string.IsNullOrEmpty(theirs.Timezone) ? theirs.Timezone : ours.Timezone;

    // ------------------------------------------------------------------
    // Nodes: leaves, structs, lists, maps, with their level rules
    // ------------------------------------------------------------------

    /// <summary>The definition level at which the nearest repeated ancestor's element exists (0 without one).</summary>
    private static int AncestorDef(SchemaNode node)
    {
        for (SchemaNode? p = node.Parent; p is not null; p = p.Parent)
        {
            if (p.Repetition == FieldRepetition.Repeated && p.Parent is not null) return p.MaxDefinition;
        }
        return 0;
    }

    /// <summary>Any node; a repeated node outside a LIST or MAP is a required list of required elements.</summary>
    private static ReadNode NodeRead(SchemaNode node, Field? hint, ParquetReadOptions options)
    {
        if (node.Repetition == FieldRepetition.Repeated)
        {
            ReadNode element = TypeRead(node, node.Name, hint is { DataType: ListType l } ? l.ValueField : null, options, asElement: true, nullable: false);
            int ancestor = AncestorDef(node);
            return new ReadNode
            {
                Field = MakeField(node.Name, new ListType(element.Field), false, node, hint),
                Kind = ReadKind.List,
                Children = [element],
                SlotDef = ancestor, SlotRep = node.MaxRepetition - 1, NullDef = ancestor,
                ElementDef = node.MaxDefinition, ElementRep = node.MaxRepetition,
            };
        }
        return TypeRead(node, node.Name, hint, options, asElement: false, nullable: node.Repetition != FieldRepetition.Required);
    }

    /// <summary>
    /// A node read once: a leaf, a list or map when annotated so, else a struct. As a list element (a repeated node
    /// standing for the element itself), its slots are the entries where the element exists, never null.
    /// </summary>
    private static ReadNode TypeRead(SchemaNode node, string name, Field? hint, ParquetReadOptions options, bool asElement, bool nullable)
    {
        int slotDef = asElement ? node.MaxDefinition : AncestorDef(node);
        int nullDef = asElement ? node.MaxDefinition : node.MaxDefinition;
        if (node.IsLeaf)
        {
            LeafType leaf = Leaf(node, hint, options);
            return new ReadNode
            {
                Field = MakeField(name, leaf.ArrowType, nullable, node, hint),
                Kind = ReadKind.Leaf, Leaf = node, LeafType = leaf,
                SlotDef = slotDef, SlotRep = node.MaxRepetition, NullDef = nullDef,
            };
        }
        SchemaElement e = node.Element;
        bool isList = e.LogicalType?.Kind is LogicalTypeKind.List or LogicalTypeKind.FiniteVector
                      || (e.LogicalType is null && e.ConvertedType == ConvertedType.List);
        bool isMap = e.LogicalType?.Kind == LogicalTypeKind.Map
                     || (e.LogicalType is null && e.ConvertedType == ConvertedType.Map)
                     || (e.ConvertedType == ConvertedType.MapKeyValue && node.Parent?.Element.ConvertedType != ConvertedType.Map);
        if (isList && ListRead(node, name, hint, options, slotDef, nullDef, nullable) is { } list) return list;
        if (isMap && MapRead(node, name, hint, options, slotDef, nullDef, nullable) is { } map) return map;

        if (node.Children.Length == 0)
            throw new ParquetFormatException($"The Parquet group '{string.Join('.', node.Path)}' has no fields, which an Arrow struct cannot hold.");
        var hints = hint?.DataType as StructType;
        ReadNode[] children = [.. node.Children.Select(c => NodeRead(c, hints?.Fields.FirstOrDefault(f => f.Name == c.Name), options))];
        Field field = MakeField(name, new StructType([.. children.Select(c => c.Field)]), nullable, node, hint);
        if (e.LogicalType?.Kind == LogicalTypeKind.Variant) field = WithMetadata(field, "ARROW:extension:name", "arrow.parquet.variant");
        return new ReadNode
        {
            Field = field, Kind = ReadKind.Struct, Children = children,
            SlotDef = slotDef, SlotRep = node.MaxRepetition, NullDef = nullDef,
        };
    }

    /// <summary>A LIST-annotated group: its single repeated child, read by the backward-compatibility rules.</summary>
    private static ReadNode? ListRead(SchemaNode node, string name, Field? hint, ParquetReadOptions options, int slotDef, int nullDef, bool nullable)
    {
        if (node.Children is not [{ Repetition: FieldRepetition.Repeated } repeated]) return null; // not a list after all: a struct
        Field? elementHint = hint?.DataType switch
        {
            ListType l => l.ValueField,
            FixedSizeListType f => f.ValueField,
            _ => null,
        };
        ReadNode element;
        if (repeated.IsLeaf
            || repeated.Children.Length != 1                                                 // rule 2 (and empty groups)
            || repeated.Children is [{ Repetition: FieldRepetition.Repeated }]               // rule 3
            || repeated.Name == "array" || repeated.Name == node.Name + "_tuple")            // rule 4
            element = TypeRead(repeated, repeated.Name, elementHint, options, asElement: true, nullable: false); // rule 1 for a leaf
        else
            element = NodeRead(repeated.Children[0], elementHint, options);                  // rule 5: three levels

        int size = node.Element.LogicalType is { Kind: LogicalTypeKind.FiniteVector, NumElements: > 0 } vector ? vector.NumElements
            : hint?.DataType is FixedSizeListType fixedSize ? fixedSize.ListSize : 0;
        // Fixed-size lists hold a placeholder per item of a null slot: read so when the items are leaves.
        bool fixedSizeList = size > 0 && element.Kind == ReadKind.Leaf;
        IArrowType type = fixedSizeList ? new FixedSizeListType(element.Field, size) : new ListType(element.Field);
        return new ReadNode
        {
            Field = MakeField(name, type, nullable, node, hint),
            Kind = fixedSizeList ? ReadKind.FixedSizeList : ReadKind.List,
            Children = [element],
            SlotDef = slotDef, SlotRep = node.MaxRepetition, NullDef = nullDef,
            ElementDef = repeated.MaxDefinition, ElementRep = repeated.MaxRepetition,
        };
    }

    /// <summary>A MAP-annotated group: one repeated group of a key and, optionally, a value (by position).</summary>
    private static ReadNode? MapRead(SchemaNode node, string name, Field? hint, ParquetReadOptions options, int slotDef, int nullDef, bool nullable)
    {
        if (node.Children is not [{ Repetition: FieldRepetition.Repeated, IsLeaf: false } keyValue] || keyValue.Children.Length is < 1 or > 2) return null;
        if (keyValue.Children[0].Repetition == FieldRepetition.Repeated) return null;
        var hints = hint?.DataType as MapType;
        SchemaNode keyNode = keyValue.Children[0];
        ReadNode key = TypeRead(keyNode, "key", hints?.KeyField, options, asElement: false, nullable: false);
        key = key.With(key.Field, key.Children).WithRejectNulls();
        if (keyValue.Children.Length == 1)
        {
            // A map without values is the list of its keys (LogicalTypes.md allows a set of keys; Arrow C++ reads it so).
            ReadNode keys = key.With(Renamed(key.Field, keyNode.Name), key.Children).WithRejectNulls();
            return new ReadNode
            {
                Field = MakeField(name, new ListType(keys.Field), nullable, node, hint),
                Kind = ReadKind.List, Children = [keys],
                SlotDef = slotDef, SlotRep = node.MaxRepetition, NullDef = nullDef,
                ElementDef = keyValue.MaxDefinition, ElementRep = keyValue.MaxRepetition,
            };
        }
        ReadNode value = NodeRead(keyValue.Children[1], hints?.ValueField, options);
        value = value.With(Renamed(value.Field, "value"), value.Children);
        var mapType = new MapType(key.Field, value.Field, hints?.KeySorted ?? false);
        var entries = new ReadNode
        {
            Field = new Field("entries", mapType.KeyValueType, false),
            Kind = ReadKind.Struct, Children = [key, value],
            SlotDef = keyValue.MaxDefinition, SlotRep = keyValue.MaxRepetition, NullDef = keyValue.MaxDefinition,
        };
        return new ReadNode
        {
            Field = MakeField(name, mapType, nullable, node, hint),
            Kind = ReadKind.Map, Children = [entries],
            SlotDef = slotDef, SlotRep = node.MaxRepetition, NullDef = nullDef,
            ElementDef = keyValue.MaxDefinition, ElementRep = keyValue.MaxRepetition,
        };
    }

    // ------------------------------------------------------------------
    // Projection by path
    // ------------------------------------------------------------------

    /// <summary>
    /// The node with only the struct fields on <paramref name="paths"/> (relative to it; an empty path takes the
    /// whole node). Lists are traversed (paths name the element's fields), maps by <c>key</c> and <c>value</c>.
    /// </summary>
    /// <exception cref="ArgumentException">A path names a field the node does not have.</exception>
    public static ReadNode Prune(ReadNode node, IReadOnlyList<string[]> paths, string where)
    {
        if (paths.Count == 0 || paths.Any(p => p.Length == 0)) return node;
        switch (node.Kind)
        {
            case ReadKind.Struct:
            {
                List<ReadNode> kept = [];
                foreach (ReadNode child in node.Children)
                {
                    string[][] below = [.. paths.Where(p => p[0] == child.Field.Name).Select(p => p[1..])];
                    if (below.Length > 0) kept.Add(Prune(child, below, $"{where}.{child.Field.Name}"));
                }
                foreach (string[] p in paths)
                {
                    if (!node.Children.Any(c => c.Field.Name == p[0])) throw new ArgumentException($"'{where}' has no field '{p[0]}'.");
                }
                return node.With(Retyped(node.Field, new StructType([.. kept.Select(k => k.Field)])), [.. kept]);
            }
            case ReadKind.List or ReadKind.FixedSizeList:
            {
                ReadNode element = Prune(node.Children[0], paths, where);
                IArrowType type = node.Kind == ReadKind.List ? new ListType(element.Field) : new FixedSizeListType(element.Field, node.ListSize);
                return node.With(Retyped(node.Field, type), [element]);
            }
            case ReadKind.Map:
            {
                ReadNode entries = node.Children[0];
                ReadNode key = entries.Children[0], value = entries.Children[1];
                foreach (string[] p in paths)
                {
                    if (p[0] is not ("key" or "value")) throw new ArgumentException($"'{where}' is a map: paths go through 'key' or 'value', not '{p[0]}'.");
                }
                string[][] valuePaths = [.. paths.Where(p => p[0] == "value").Select(p => p[1..])];
                if (valuePaths.Length > 0) value = Prune(value, valuePaths, $"{where}.value");
                var type = new MapType(key.Field, value.Field, ((MapType)node.Field.DataType).KeySorted);
                ReadNode newEntries = entries.With(new Field("entries", type.KeyValueType, false), [key, value]);
                return node.With(Retyped(node.Field, type), [newEntries]);
            }
            default:
                throw new ArgumentException($"'{where}' is not a struct, list or map: no field below it can be selected.");
        }
    }

    private static Field Retyped(Field f, IArrowType type) => new(f.Name, type, f.IsNullable, f.Metadata);

    private static Field Renamed(Field f, string name) => new(name, f.DataType, f.IsNullable, f.Metadata);

    private static Field WithMetadata(Field f, string key, string value)
    {
        Dictionary<string, string> metadata = new(StringComparer.Ordinal);
        if (f.Metadata is not null)
        {
            foreach (var (k, v) in f.Metadata) metadata[k] = v;
        }
        metadata[key] = value;
        return new Field(f.Name, f.DataType, f.IsNullable, metadata);
    }

    // ------------------------------------------------------------------

    /// <summary>A field with the node's field id and the stored schema's field metadata.</summary>
    private static Field MakeField(string name, IArrowType type, bool nullable, SchemaNode node, Field? hint)
    {
        Dictionary<string, string>? metadata = null;
        if (hint?.Metadata is { Count: > 0 } stored)
        {
            metadata = new(StringComparer.Ordinal);
            foreach (var (k, v) in stored) metadata[k] = v;
        }
        if (node.Element.FieldId is int id) (metadata ??= new(StringComparer.Ordinal))[ArrowFieldIds.MetadataKey] = id.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return new Field(name, type, nullable, metadata);
    }
}

internal static class ReadNodeExtensions
{
    public static ReadNode WithRejectNulls(this ReadNode node) => new()
    {
        Field = node.Field, Kind = node.Kind, Children = node.Children, Leaf = node.Leaf, LeafType = node.LeafType,
        SlotDef = node.SlotDef, SlotRep = node.SlotRep, NullDef = node.NullDef, ElementDef = node.ElementDef,
        ElementRep = node.ElementRep, RejectNulls = node.Kind == ReadKind.Leaf,
    };
}
