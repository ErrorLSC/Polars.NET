using Apache.Arrow.Types;
using Kaminari.Sazanami.Ordering;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Schema;

namespace Kaminari.Seseragi.Files.Parquet.Writing;

/// <summary>
/// The sort order a writer declares (<see cref="ParquetWriteOptions.SortingColumns"/>): the footer's
/// <c>sorting_columns</c> (leaf ordinals), and, to check the rows, each key's column and its fields.
/// </summary>
/// <param name="Thrift">The declaration, as every row group holds it.</param>
/// <param name="Columns">Each key's top-level column and the struct fields below it.</param>
/// <param name="Order">Each key's direction and null placement.</param>
/// <param name="Names">Each key's path, joined by dots, for messages.</param>
internal sealed record SortingKeys(SortingColumn[] Thrift, (int Column, int[] Fields)[] Columns, SortKey[] Order, string[] Names)
{
    /// <summary>The declaration of <paramref name="options"/> over a file of <paramref name="schema"/>; null when there is none.</summary>
    /// <param name="schema">The file's Arrow schema.</param>
    /// <param name="elements">The Parquet schema, its root first.</param>
    /// <param name="options">The writer's options.</param>
    /// <exception cref="ArgumentException">A key names no leaf, a repeated leaf, one without an order, or a leaf twice; or
    /// the rows are to be checked and a key's values cannot be.</exception>
    public static SortingKeys? Of(Apache.Arrow.Schema schema, SchemaElement[] elements, ParquetWriteOptions options)
    {
        if (options.SortingColumns is not { Count: > 0 } sorting) return null;
        ParquetSchemaTree tree = ParquetSchemaTree.Build(elements);
        var thrift = new SortingColumn[sorting.Count];
        var columns = new (int, int[])[sorting.Count];
        var names = new string[sorting.Count];
        for (int k = 0; k < sorting.Count; k++)
        {
            ParquetSortingColumn key = sorting[k];
            string name = names[k] = string.Join('.', key.Path);
            SchemaNode leaf = tree.Leaves.FirstOrDefault(l => l.Path.AsSpan().SequenceEqual(key.Path.ToArray()))
                ?? throw new ArgumentException($"ParquetWriteOptions.SortingColumns names '{name}', which is no leaf column of the schema (a path names the "
                                               + "Parquet names from the top, as ['s', 'x'] for a struct field; the columns are "
                                               + string.Join(", ", tree.Leaves.Where(l => l.MaxRepetition == 0).Select(l => $"'{string.Join('.', l.Path)}'")) + ").", "options");
            if (leaf.MaxRepetition != 0)
                throw new ArgumentException($"ParquetWriteOptions.SortingColumns names '{name}', which lies in a list or a map: a row holds many values "
                                            + "there, so rows have no order by it.", "options");
            if (leaf.Element.Type == ParquetType.Int96 || leaf.Element.ConvertedType == ConvertedType.Interval)
                throw new ArgumentException($"ParquetWriteOptions.SortingColumns names '{name}', stored as {(leaf.Element.Type == ParquetType.Int96 ? "INT96" : "INTERVAL")}, "
                                            + "whose values have no order in Parquet.", "options");
            for (int j = 0; j < k; j++)
            {
                if (thrift[j].ColumnIndex == leaf.LeafIndex)
                    throw new ArgumentException($"ParquetWriteOptions.SortingColumns names '{name}' twice: a later key orders only rows the keys before it tie.", "options");
            }
            thrift[k] = new SortingColumn(leaf.LeafIndex, key.Descending, key.NullsFirst);
            SchemaNode top = leaf;
            while (top.Parent!.Parent is not null) top = top.Parent;
            columns[k] = Fields(schema, System.Array.IndexOf(tree.Root.Children, top), key.Path, name, options.VerifySortingColumns);
        }
        return new SortingKeys(thrift, columns, [.. sorting.Select(s => new SortKey(s.Descending, s.NullsFirst))], names);
    }

    /// <summary>The top-level column and struct fields a key's path goes through in the Arrow schema.</summary>
    private static (int, int[]) Fields(Apache.Arrow.Schema schema, int column, IReadOnlyList<string> path, string name, bool verify)
    {
        IArrowType type = schema.FieldsList[column].DataType;
        var fields = new int[path.Count - 1];
        for (int level = 1; level < path.Count; level++)
        {
            int f = type is StructType s ? s.Fields.ToList().FindIndex(c => c.Name == path[level]) : -1;
            if (f < 0)
            {
                if (verify) throw Unchecked(name, type);
                return (column, []);
            }
            fields[level - 1] = f;
            type = ((StructType)type).Fields[f].DataType;
        }
        if (verify && !ArrowOrder.CanOrder(type)) throw Unchecked(name, type);
        return (column, fields);

        static ArgumentException Unchecked(string name, IArrowType type) =>
            new($"VerifySortingColumns cannot check the order of '{name}' ({type.Name}): its values have no order the check knows. Leave the key out, "
                + "or declare the order without the check.", "options");
    }
}
