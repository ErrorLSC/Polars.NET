using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Parquet;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// The types data files hold their columns in, read from the footers: which files a type widening left in an older
/// type (Spark's <c>ReorgTableHelper.fileHasDifferentTypes</c>, which picks the files <c>REORG … APPLY (UPGRADE
/// TYPE)</c> and dropping type widening rewrite).
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
        int at = 0;
        Node root = Read(footer.Schema, ref at);
        return schema.Fields.Any(f => !partitionColumns.Contains(f.Name) && Child(root, f.PhysicalName) is Node node && Differs(f.Type, node));

        static Node Read(IReadOnlyList<ParquetSchemaElement> schema, ref int at)
        {
            ParquetSchemaElement element = schema[at++];
            var children = new Node[element.ChildCount];
            for (int i = 0; i < children.Length; i++) children[i] = Read(schema, ref at);
            return new Node(element, children);
        }

        static Node? Child(Node group, string name) => Array.Find(group.Children, c => c.Element.Name == name);

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

        // The element of a list: the standard three levels (list group, repeated group, element), or the legacy two.
        static Node? ListElement(Node list) => list.Children switch
        {
            [{ Element.Repetition: 2, Children: [Node element] }] => element,
            [{ Element.Repetition: 2 } repeated] => repeated,
            _ => null,
        };

        // The repeated key/value group of a map.
        static Node? MapEntry(Node map) => map.Children is [{ Element.Repetition: 2 } entry] ? entry : null;

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

    /// <summary>A node of the footer's schema with its children.</summary>
    private sealed record Node(ParquetSchemaElement Element, Node[] Children);
}
