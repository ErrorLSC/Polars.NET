using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Schema;

namespace Kaminari.Seseragi.Files.Parquet.Pruning;

/// <summary>
/// The sort orders of a footer: what each row group declares (<c>sorting_columns</c>, an order of the rows within
/// that row group) and what the footer proves of the whole file.
/// </summary>
internal static class SortOrders
{
    /// <summary>
    /// The keys row group <paramref name="group"/> declares, by leaf path. <c>column_idx</c> is the column chunk's
    /// ordinal (the leaf's, as pyarrow, arrow-rs and Polars take it). A declaration naming no leaf, a repeated leaf
    /// (inside a list or a map: no order of rows) or one leaf twice says nothing: it reads as none.
    /// </summary>
    public static ParquetSortingColumn[] Declared(RowGroup group, ParquetSchemaTree tree)
    {
        if (group.SortingColumns is not { Length: > 0 } sorting) return [];
        var keys = new ParquetSortingColumn[sorting.Length];
        for (int k = 0; k < sorting.Length; k++)
        {
            SortingColumn s = sorting[k];
            if ((uint)s.ColumnIndex >= (uint)tree.Leaves.Length) return [];
            SchemaNode leaf = tree.Leaves[s.ColumnIndex];
            if (leaf.MaxRepetition != 0) return [];
            for (int j = 0; j < k; j++)
            {
                if (sorting[j].ColumnIndex == s.ColumnIndex) return [];
            }
            keys[k] = new ParquetSortingColumn(leaf.Path, s.Descending, s.NullsFirst);
        }
        return keys;
    }

    /// <summary>
    /// The longest prefix of the declared keys the whole file is sorted by, as far as the footer proves it (none when
    /// it does not). Every row group holding rows must declare the same keys, and the row groups must follow each
    /// other in that order by their statistics: at each boundary the first key's values are ordered (the last row
    /// group's bounds against the next one's, which truncated bounds keep sound), nulls where the key puts them. Where
    /// the first key orders every boundary strictly, the declaration holds whole; where a boundary ties, only the
    /// first key does. Statistics or null counts missing, a float key whose NaN count is not known to be 0, or a key
    /// type without an order the statistics model prove nothing.
    /// </summary>
    public static ParquetSortingColumn[] Proven(FileMetaData meta)
    {
        ParquetSchemaTree tree;
        try
        {
            tree = SchemaShape.Of(meta).Tree;
        }
        catch (ParquetFormatException)
        {
            return [];
        }
        List<RowGroup> groups = [.. meta.RowGroups.Where(g => g.NumRows > 0)];
        if (groups.Count == 0) return [];
        ParquetSortingColumn[] declared = Declared(groups[0], tree);
        if (declared.Length == 0) return [];
        SortingColumn[] first = groups[0].SortingColumns!;
        foreach (RowGroup group in groups)
        {
            if (group.SortingColumns is not { } other || !other.AsSpan().SequenceEqual(first)) return [];
        }

        SortingColumn key = first[0];
        SchemaNode leaf = tree.Leaves[key.ColumnIndex];
        LeafDomain? domain;
        try
        {
            domain = LeafDomain.Create(leaf, LeafTypes.Resolve(leaf.Element, Defaults), meta.ColumnOrders, tree.Leaves.Length);
        }
        catch (ParquetFormatException)
        {
            return [];
        }
        if (domain is null) return [];
        int proven = declared.Length;
        for (int g = 1; g < groups.Count; g++)
        {
            if (!new RowGroupStatistics(groups[g - 1]).TryGet(leaf.LeafIndex, domain, out LeafStatistics a)
                || !new RowGroupStatistics(groups[g]).TryGet(leaf.LeafIndex, domain, out LeafStatistics b))
                return [];
            if (Boundary(domain, key, a, b) is not int order || order > 0) return [];
            if (order == 0) proven = 1;
        }
        return proven == declared.Length ? declared : declared[..proven];
    }

    private static readonly ParquetReadOptions Defaults = new();

    /// <summary>
    /// How the last row of a row group (statistics <paramref name="a"/>) compares with the first of the next
    /// (<paramref name="b"/>) on the key: negative before, 0 possibly tied, positive out of order; null when the
    /// statistics do not tell.
    /// </summary>
    private static int? Boundary(LeafDomain domain, SortingColumn key, in LeafStatistics a, in LeafStatistics b)
    {
        if (a.NullCount is not long aNulls || b.NullCount is not long bNulls) return null;
        if (domain.NaNPossible(a) || domain.NaNPossible(b)) return null;
        bool aAllNull = aNulls == a.ValueCount, bAllNull = bNulls == b.ValueCount;
        if (key.NullsFirst)
        {
            // Nulls lead each row group: the next one may start with nulls only after a row group of nulls alone.
            if (bNulls > 0) return aAllNull ? 0 : 1;
            if (aAllNull) return -1;
        }
        else
        {
            // Nulls close each row group: one holding nulls may be followed by nulls alone.
            if (aNulls > 0) return bAllNull ? 0 : 1;
            if (bAllNull) return -1;
        }
        // Both hold values at the boundary: ascending, the last is at most a's max and the first at least b's min.
        byte[]? last = key.Descending ? a.Min : a.Max, next = key.Descending ? b.Max : b.Min;
        if (last is null || next is null || !domain.TryCompareBounds(last, next, out int sign)) return null;
        return key.Descending ? -sign : sign;
    }
}
