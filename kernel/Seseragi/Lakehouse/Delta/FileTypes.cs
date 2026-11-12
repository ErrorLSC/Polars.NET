using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Parquet;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// The columns data files hold, read from the footers: which files a type widening left in an older type (Spark's
/// <c>ReorgTableHelper.fileHasDifferentTypes</c>, which picks the files <c>REORG … APPLY (UPGRADE TYPE)</c> and
/// dropping type widening rewrite), and which still hold the data of dropped columns
/// (<c>ReorgTableHelper.fileHasExtraColumns</c>, part of what <c>REORG … APPLY (PURGE)</c> rewrites).
/// </summary>
public static class FileTypes
{
    /// <summary>
    /// Whether the file holds a column (at any depth: struct fields, array elements, map keys and values) in a type
    /// other than the table's. Columns are found by their physical names; a column the file lacks, and a type the
    /// footer does not tell apart (timestamps, strings, binary), count as the table's.
    /// </summary>
    /// <param name="schema">The table schema.</param>
    /// <param name="partitionColumns">The partition columns, which the files do not hold.</param>
    /// <param name="footer">The file's footer.</param>
    public static bool HasOlderTypes(StructType schema, IReadOnlyCollection<string> partitionColumns, ParquetMetadata footer)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(partitionColumns);
        ArgumentNullException.ThrowIfNull(footer);
        if (footer.Schema.Count == 0) return false;
        Node root = Root(footer);
        return schema.Fields.Any(f => !partitionColumns.Contains(f.Name) && Child(root, f.PhysicalName) is Node node && Differs(f.Type, node));

        static bool Differs(DeltaType table, Node file) => table switch
        {
            StructType s when file.Children.Length > 0 =>
                s.Fields.Any(f => Child(file, f.PhysicalName) is Node node && Differs(f.Type, node)),
            ArrayType a when ListElement(file) is Node element => Differs(a.ElementType, element),
            MapType m when MapEntry(file) is { Children: [Node key, Node value] } =>
                Differs(m.KeyType, key) || Differs(m.ValueType, value),
            PrimitiveType or DecimalType when file.Children.Length == 0 && TypeOf(file.Element) is DeltaType held => !Same(table, held),
            _ => false,
        };

        // The Delta type a leaf holds, or null when the footer does not tell it apart from others.
        static DeltaType? TypeOf(ParquetSchemaElement leaf)
        {
            if (leaf.DecimalScale is int scale) return leaf.DecimalPrecision is int precision ? new DecimalType(precision, scale) : null;
            return leaf.PhysicalType switch
            {
                0 => new PrimitiveType(PrimitiveType.Boolean),
                1 when leaf.IsDate => new PrimitiveType(PrimitiveType.Date),
                1 => new PrimitiveType(leaf.IntBitWidth switch
                {
                    8 => PrimitiveType.Byte,
                    16 => PrimitiveType.Short,
                    _ => PrimitiveType.Integer,
                }),
                2 when leaf.TimeUnit is null => new PrimitiveType(PrimitiveType.Long),
                4 => new PrimitiveType(PrimitiveType.Float),
                5 => new PrimitiveType(PrimitiveType.Double),
                _ => null,
            };
        }

        static bool Same(DeltaType table, DeltaType held) => (table, held) switch
        {
            (DecimalType a, DecimalType b) => a.Precision == b.Precision && a.Scale == b.Scale,
            (PrimitiveType a, PrimitiveType b) => a.Name == b.Name,
            _ => false,
        };
    }

    /// <summary>
    /// Whether the file holds a column (at any depth: struct fields, also inside array elements and map keys and
    /// values) that the table does not have, as the data of a dropped column. Columns are matched by their physical
    /// names in any case, as Spark; the hidden columns of materialized row ids and commit versions are not extra.
    /// </summary>
    /// <param name="schema">The table schema.</param>
    /// <param name="hiddenColumns">The names of the materialized row tracking columns, which the schema does not list.</param>
    /// <param name="footer">The file's footer.</param>
    public static bool HasExtraColumns(StructType schema, IReadOnlyCollection<string> hiddenColumns, ParquetMetadata footer)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(hiddenColumns);
        ArgumentNullException.ThrowIfNull(footer);
        return footer.Schema.Count > 0 && Extra(schema, Root(footer));

        // Spark walks the file's fields and looks each path up in the table (SchemaUtils.findNestedFieldIgnoreCase).
        bool Extra(StructType table, Node group) => group.Children.Any(column =>
            table.Fields.FirstOrDefault(f => string.Equals(f.PhysicalName, column.Element.Name, StringComparison.OrdinalIgnoreCase)) switch
            {
                null => !hiddenColumns.Contains(column.Element.Name),
                StructField field => Nested(field.Type, column),
            });

        // A file group under a table column: its struct fields are looked up in the table's struct, through list
        // elements and map entries. A group where the table holds a plain value has nothing the table knows.
        bool Nested(DeltaType table, Node file) => table switch
        {
            _ when file.Children.Length == 0 => false,
            StructType s => Extra(s, file),
            ArrayType a => ListElement(file) is Node element && Nested(a.ElementType, element),
            MapType m => MapEntry(file) is { Children: [Node key, Node value] } && (Nested(m.KeyType, key) || Nested(m.ValueType, value)),
            _ => true,
        };
    }

    // The footer's schema as a tree.
    private static Node Root(ParquetMetadata footer)
    {
        int at = 0;
        return Read(footer.Schema, ref at);

        static Node Read(IReadOnlyList<ParquetSchemaElement> schema, ref int at)
        {
            ParquetSchemaElement element = schema[at++];
            var children = new Node[element.ChildCount];
            for (int i = 0; i < children.Length; i++) children[i] = Read(schema, ref at);
            return new Node(element, children);
        }
    }

    private static Node? Child(Node group, string name) => Array.Find(group.Children, c => c.Element.Name == name);

    // The element of a list: the standard three levels (list group, repeated group, element), or the legacy two.
    private static Node? ListElement(Node list) => list.Children switch
    {
        [{ Element.Repetition: 2, Children: [Node element] }] => element,
        [{ Element.Repetition: 2 } repeated] => repeated,
        _ => null,
    };

    // The repeated key/value group of a map.
    private static Node? MapEntry(Node map) => map.Children is [{ Element.Repetition: 2 } entry] ? entry : null;

    /// <summary>A node of the footer's schema with its children.</summary>
    private sealed record Node(ParquetSchemaElement Element, Node[] Children);
}
