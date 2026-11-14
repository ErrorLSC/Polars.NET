using Kaminari.Sazanami.Rows;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Kaminari.Seseragi.Files.Parquet.Pruning;

/// <summary>
/// Pruning by column indexes: the rows of a row group whose pages may hold a row the filter keeps. The leaves' pages
/// do not line up, so the row group is cut where any tested leaf starts a page; on each cut, every leaf with a usable
/// index is one page, whose bounds and null counts stand for it, and a leaf without one stands by its row group's
/// statistics. Page statistics bound every subset of the page's rows, so this keeps every row that may pass.
/// </summary>
internal static class PagePruning
{
    /// <summary>The rows to keep; null when no tested leaf has a usable column index (the row group stays whole).</summary>
    public static RowRanges? Keep(PruneNode filter, LeafDomain[] leaves, RowGroup group, int g, PageIndexes indexes)
    {
        Dictionary<int, ChunkPageIndex> indexed = [];
        foreach (LeafDomain leaf in leaves)
        {
            if (indexes.Get(g, leaf.Leaf, withColumns: true) is { Columns: not null } index) indexed[leaf.Leaf] = index;
        }
        if (indexed.Count == 0) return null;
        long rows = group.NumRows;
        long[] cuts = [.. indexed.Values.SelectMany(i => i.FirstRows).Append(rows).Distinct().Order()];
        var unit = new PageStatistics(group, indexed);
        List<(long Start, long Count)> kept = [];
        for (int i = 0; i + 1 < cuts.Length; i++)
        {
            unit.Row = cuts[i];
            if ((filter.Evaluate(unit) & Outcomes.True) != 0) kept.Add((cuts[i], cuts[i + 1] - cuts[i]));
        }
        return RowRanges.Of([.. kept]);
    }

    /// <summary>The statistics around one row: its page's for leaves with a column index, the row group's for the others.</summary>
    private sealed class PageStatistics(RowGroup group, Dictionary<int, ChunkPageIndex> indexed) : IUnitStatistics
    {
        private readonly RowGroupStatistics _group = new(group);

        public long Row { get; set; }

        public bool TryGet(int leaf, LeafDomain domain, out LeafStatistics statistics)
        {
            if (!indexed.TryGetValue(leaf, out ChunkPageIndex? index)) return _group.TryGet(leaf, domain, out statistics);
            ColumnIndex columns = index.Columns!;
            int page = index.PageOf(Row);
            long rows = index.RowsOf(page);
            // A flat leaf holds a value or a null per row. Bounds follow the column order, as row group bounds do.
            // null_pages is believed only where null_counts agree: parquet-mr 1.13 files written without statistics
            // (apache/data/datapage_v1-*-checksum) mark every page all-null, with empty bounds and no counts.
            long? nulls = columns.NullCounts?[page] is long n && n >= 0 && n <= rows ? n : null;
            bool allNull = columns.NullPages[page] && nulls == rows;
            byte[]? min = null, max = null;
            if (!columns.NullPages[page] && domain.TypedStatistics) (min, max) = (columns.MinValues[page], columns.MaxValues[page]);
            long? nans = columns.NanCounts?[page] is long m && m >= 0 && m <= rows ? m : null;
            statistics = new LeafStatistics(min, max, allNull ? rows : nulls, rows, nans);
            return true;
        }
    }
}
