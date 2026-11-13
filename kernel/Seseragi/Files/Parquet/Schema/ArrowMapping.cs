using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Kaminari.Seseragi.Files.Parquet.Schema;

/// <summary>A top-level column of the file: its Arrow field, its node, and for a flat leaf how its values map.</summary>
internal sealed record ColumnMapping(Field Field, SchemaNode Node, LeafType? Leaf)
{
    /// <summary>Whether the column is one leaf directly under the root (no nesting, no repetition).</summary>
    public bool IsFlat => Leaf is not null;
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
            LeafType? leaf = node.IsLeaf && node.Repetition != FieldRepetition.Repeated ? Leaf(node, hint, options) : null;
            Field field = leaf is not null ? LeafField(node, leaf, hint) : NodeField(node, hint, options);
            columns[i] = new ColumnMapping(field, node, leaf);
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

    private static Field LeafField(SchemaNode node, LeafType leaf, Field? hint) =>
        MakeField(node.Name, leaf.ArrowType, node.Repetition != FieldRepetition.Required, node, hint);

    // ------------------------------------------------------------------
    // Groups and repeated fields
    // ------------------------------------------------------------------

    /// <summary>The field of any node; a repeated node outside a LIST or MAP is a required list of required elements.</summary>
    private static Field NodeField(SchemaNode node, Field? hint, ParquetReadOptions options)
    {
        if (node.Repetition == FieldRepetition.Repeated)
        {
            Field element = ElementField(node, node.Name, hint is { DataType: ListType l } ? l.ValueField : null, options, nullable: false);
            return MakeField(node.Name, new ListType(element), false, node, hint);
        }
        return MakeField(node.Name, NodeType(node, hint, options), node.Repetition != FieldRepetition.Required, node, hint);
    }

    /// <summary>The Arrow type of a node read once (its own repetition aside).</summary>
    private static IArrowType NodeType(SchemaNode node, Field? hint, ParquetReadOptions options)
    {
        if (node.IsLeaf) return Leaf(node, hint, options).ArrowType;
        SchemaElement e = node.Element;
        bool isList = e.LogicalType?.Kind is LogicalTypeKind.List or LogicalTypeKind.FiniteVector
                      || (e.LogicalType is null && e.ConvertedType == ConvertedType.List);
        bool isMap = e.LogicalType?.Kind == LogicalTypeKind.Map
                     || (e.LogicalType is null && e.ConvertedType == ConvertedType.Map)
                     || (e.ConvertedType == ConvertedType.MapKeyValue && node.Parent?.Element.ConvertedType != ConvertedType.Map);
        if (isList && List(node, hint, options) is { } list) return list;
        if (isMap && Map(node, hint, options) is { } map) return map;
        return Struct(node, hint, options);
    }

    private static StructType Struct(SchemaNode node, Field? hint, ParquetReadOptions options)
    {
        var hints = hint?.DataType as StructType;
        return new StructType([.. node.Children.Select(c => NodeField(c, hints?.Fields.FirstOrDefault(f => f.Name == c.Name), options))]);
    }

    /// <summary>A LIST-annotated group: its single repeated child, read by the backward-compatibility rules.</summary>
    private static IArrowType? List(SchemaNode node, Field? hint, ParquetReadOptions options)
    {
        if (node.Children is not [{ Repetition: FieldRepetition.Repeated } repeated]) return null; // not a list after all: a struct
        Field? elementHint = hint?.DataType switch
        {
            ListType l => l.ValueField,
            FixedSizeListType f => f.ValueField,
            _ => null,
        };
        Field element;
        if (repeated.IsLeaf)
            element = ElementField(repeated, repeated.Name, elementHint, options, nullable: false);                // rule 1
        else if (repeated.Children.Length > 1
                 || repeated.Children is [{ Repetition: FieldRepetition.Repeated }]                               // rule 3
                 || repeated.Name == "array" || repeated.Name == node.Name + "_tuple"                               // rule 4
                 || repeated.Children.Length == 0)
            element = MakeField(repeated.Name, GroupAsElement(repeated, elementHint, options), false, repeated, elementHint); // rule 2
        else
            element = NodeField(repeated.Children[0], elementHint, options);                                           // rule 5: 3 levels

        if (hint?.DataType is FixedSizeListType fixedSize && node.Element.LogicalType?.Kind != LogicalTypeKind.FiniteVector)
            return new FixedSizeListType(element, fixedSize.ListSize);
        if (node.Element.LogicalType is { Kind: LogicalTypeKind.FiniteVector, NumElements: > 0 } vector)
            return new FixedSizeListType(element, vector.NumElements);
        return new ListType(element);
    }

    /// <summary>The element of a list whose repeated node is itself the element (rules 1 to 4): required.</summary>
    private static Field ElementField(SchemaNode repeated, string name, Field? hint, ParquetReadOptions options, bool nullable) =>
        MakeField(name, repeated.IsLeaf ? Leaf(repeated, hint, options).ArrowType : GroupAsElement(repeated, hint, options), nullable, repeated, hint);

    /// <summary>A repeated group read as one element: a list or map when annotated so, else a struct of its fields.</summary>
    private static IArrowType GroupAsElement(SchemaNode group, Field? hint, ParquetReadOptions options) => NodeType(group, hint, options);

    /// <summary>A MAP-annotated group: one repeated group of a key and, optionally, a value (by position).</summary>
    private static IArrowType? Map(SchemaNode node, Field? hint, ParquetReadOptions options)
    {
        if (node.Children is not [{ Repetition: FieldRepetition.Repeated, IsLeaf: false } keyValue] || keyValue.Children.Length is < 1 or > 2) return null;
        var hints = hint?.DataType as MapType;
        SchemaNode keyNode = keyValue.Children[0];
        Field key = keyNode.Repetition == FieldRepetition.Repeated
            ? NodeField(keyNode, hints?.KeyField, options)
            : MakeField("key", NodeType(keyNode, hints?.KeyField, options), false, keyNode, hints?.KeyField);
        // A map without values is the list of its keys (LogicalTypes.md allows a set of keys; Arrow C++ reads it so).
        if (keyValue.Children.Length == 1) return new ListType(MakeField(keyNode.Name, key.DataType, false, keyNode, hints?.KeyField));
        Field value = Renamed(NodeField(keyValue.Children[1], hints?.ValueField, options), "value");
        return new MapType(key, value, hints?.KeySorted ?? false);
    }

    private static Field Renamed(Field f, string name) => new(name, f.DataType, f.IsNullable, f.Metadata);

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
