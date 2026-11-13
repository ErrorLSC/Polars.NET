namespace Kaminari.Seseragi.Files.Parquet.Metadata;

/// <summary>The schema tree stored depth first in <c>FileMetaData.schema</c>.</summary>
internal static class SchemaTree
{
    /// <summary>A leaf has a physical type and no children; any other element is a group (perhaps empty).</summary>
    public static bool IsLeaf(SchemaElement e) => e.Type is not null && (e.NumChildren ?? 0) == 0;

    /// <summary>The number of leaves, after checking that the root's subtree takes exactly every element.</summary>
    /// <exception cref="ParquetFormatException">The child counts do not describe one tree over the elements.</exception>
    public static int CountLeaves(SchemaElement[] schema)
    {
        if (schema.Length == 0) throw new ParquetFormatException("The Parquet schema is empty (no root element).");
        if (IsLeaf(schema[0])) throw new ParquetFormatException("The Parquet schema root is not a group.");
        Stack<int> remaining = new();
        remaining.Push(Children(schema[0]));
        int at = 1, leaves = 0;
        while (remaining.Count > 0)
        {
            int left = remaining.Pop();
            if (left == 0) continue;
            remaining.Push(left - 1);
            if (at >= schema.Length)
                throw new ParquetFormatException($"The Parquet schema has {schema.Length} elements, fewer than its child counts call for.");
            SchemaElement e = schema[at++];
            if (IsLeaf(e)) leaves++;
            else remaining.Push(Children(e));
        }
        if (at != schema.Length)
            throw new ParquetFormatException($"The Parquet schema has {schema.Length} elements, but its tree takes {at}.");
        return leaves;

        static int Children(SchemaElement e) => e.NumChildren is null or >= 0
            ? e.NumChildren ?? 0
            : throw new ParquetFormatException($"The Parquet schema element '{e.Name}' has {e.NumChildren} children.");
    }
}
