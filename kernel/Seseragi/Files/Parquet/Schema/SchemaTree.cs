using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Kaminari.Seseragi.Files.Parquet.Schema;

/// <summary>
/// One node of the Parquet schema tree: its element, children, and the maximum definition and repetition levels of
/// the values at and below it (Dremel encoding).
/// </summary>
internal sealed class SchemaNode
{
    public required SchemaElement Element { get; init; }
    public SchemaNode? Parent { get; init; }
    public SchemaNode[] Children { get; set; } = [];

    /// <summary>The definition level at which this node is present (for a leaf: its value is not null).</summary>
    public required int MaxDefinition { get; init; }

    /// <summary>The repetition level of this node (the number of repeated nodes on the path, itself included).</summary>
    public required int MaxRepetition { get; init; }

    /// <summary>The index of the leaf (column chunk order), or -1 for a group.</summary>
    public int LeafIndex { get; set; } = -1;

    /// <summary>The path from below the root, names only.</summary>
    public required string[] Path { get; init; }

    public string Name => Element.Name;
    public bool IsLeaf => LeafIndex >= 0;
    public FieldRepetition Repetition => Element.Repetition ?? FieldRepetition.Required;
}

/// <summary>The schema tree of a file, built from its depth-first <c>SchemaElement</c> list.</summary>
internal sealed class ParquetSchemaTree
{
    private ParquetSchemaTree(SchemaNode root, SchemaNode[] leaves)
    {
        Root = root;
        Leaves = leaves;
    }

    public SchemaNode Root { get; }

    /// <summary>The leaves in column chunk order.</summary>
    public SchemaNode[] Leaves { get; }

    /// <summary>Builds the tree (the element list was checked by <see cref="SchemaTree.CountLeaves"/>).</summary>
    /// <exception cref="ParquetFormatException">The tree nests too deeply or a leaf has an impossible type.</exception>
    public static ParquetSchemaTree Build(SchemaElement[] elements)
    {
        SchemaTree.CountLeaves(elements);
        List<SchemaNode> leaves = [];
        var root = new SchemaNode { Element = elements[0], MaxDefinition = 0, MaxRepetition = 0, Path = [] };
        int at = 1;
        root.Children = Children(root, elements[0].NumChildren ?? 0, 0);
        return new ParquetSchemaTree(root, [.. leaves]);

        SchemaNode[] Children(SchemaNode parent, int count, int depth)
        {
            if (depth > 200) throw new ParquetFormatException("The Parquet schema nests too deeply.");
            var children = new SchemaNode[count];
            for (int i = 0; i < count; i++)
            {
                SchemaElement e = elements[at++];
                FieldRepetition repetition = e.Repetition ?? FieldRepetition.Required;
                var node = new SchemaNode
                {
                    Element = e,
                    Parent = parent,
                    MaxDefinition = parent.MaxDefinition + (repetition == FieldRepetition.Required ? 0 : 1),
                    MaxRepetition = parent.MaxRepetition + (repetition == FieldRepetition.Repeated ? 1 : 0),
                    Path = [.. parent.Path, e.Name],
                };
                if (SchemaTree.IsLeaf(e))
                {
                    if (e.Type is not (>= ParquetType.Boolean and <= ParquetType.FixedLenByteArray))
                        throw new ParquetFormatException($"The Parquet column '{string.Join('.', node.Path)}' has an unknown physical type {(int?)e.Type}.");
                    if (e.Type == ParquetType.FixedLenByteArray && e.TypeLength is not > 0)
                        throw new ParquetFormatException($"The Parquet column '{string.Join('.', node.Path)}' is a FIXED_LEN_BYTE_ARRAY without a positive length.");
                    if (node.MaxDefinition > 255 || node.MaxRepetition > 255)
                        throw new ParquetFormatException($"The Parquet column '{string.Join('.', node.Path)}' nests too deeply.");
                    node.LeafIndex = leaves.Count;
                    leaves.Add(node);
                }
                else node.Children = Children(node, e.NumChildren ?? 0, depth + 1);
                children[i] = node;
            }
            return children;
        }
    }
}
