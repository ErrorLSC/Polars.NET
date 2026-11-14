using System.Collections.Concurrent;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// The page index of one column chunk, checked: where each page lies and the row it starts at (OffsetIndex), and,
/// when present and of the same pages, their bounds and null counts (ColumnIndex).
/// </summary>
internal sealed class ChunkPageIndex
{
    private ChunkPageIndex(long[] firstRows, long[] offsets, int[] sizes, long groupRows, ColumnIndex? columns)
    {
        FirstRows = firstRows;
        Offsets = offsets;
        Sizes = sizes;
        GroupRows = groupRows;
        Columns = columns;
    }

    /// <summary>The row each page starts at, ascending from 0.</summary>
    public long[] FirstRows { get; }

    /// <summary>Where each page (its header) starts in the file.</summary>
    public long[] Offsets { get; }

    /// <summary>Each page's size in the file, header included.</summary>
    public int[] Sizes { get; }

    /// <summary>The rows of the row group.</summary>
    public long GroupRows { get; }

    /// <summary>The pages' bounds and null counts; null when the chunk has no usable column index.</summary>
    public ColumnIndex? Columns { get; }

    public int PageCount => FirstRows.Length;

    /// <summary>The rows of page <paramref name="page"/>.</summary>
    public long RowsOf(int page) => (page + 1 < FirstRows.Length ? FirstRows[page + 1] : GroupRows) - FirstRows[page];

    /// <summary>The page holding row <paramref name="row"/>.</summary>
    public int PageOf(long row)
    {
        int i = Array.BinarySearch(FirstRows, row);
        return i >= 0 ? i : ~i - 1;
    }

    /// <summary>
    /// The index of a chunk lying at <paramref name="chunkStart"/> (<paramref name="chunkLength"/> bytes) in a row
    /// group of <paramref name="groupRows"/> rows; null when the offset index does not describe such a chunk (then
    /// nothing is taken from either index). A column index of other pages is dropped.
    /// </summary>
    public static ChunkPageIndex? Create(OffsetIndex? offsets, ColumnIndex? columns, long groupRows, long chunkStart, long chunkLength)
    {
        if (offsets is null || groupRows <= 0) return null;
        PageLocation[] pages = offsets.PageLocations;
        int n = pages.Length;
        if (n == 0 || pages[0].FirstRowIndex != 0) return null;
        long end = chunkStart + chunkLength;
        var firstRows = new long[n];
        var at = new long[n];
        var sizes = new int[n];
        for (int i = 0; i < n; i++)
        {
            PageLocation p = pages[i];
            if (p.FirstRowIndex >= groupRows || i > 0 && p.FirstRowIndex <= firstRows[i - 1]) return null;
            if (p.CompressedPageSize <= 0 || p.Offset < chunkStart || p.Offset > end - p.CompressedPageSize) return null;
            if (i > 0 && p.Offset < at[i - 1] + sizes[i - 1]) return null; // pages overlap or go backwards
            firstRows[i] = p.FirstRowIndex;
            at[i] = p.Offset;
            sizes[i] = p.CompressedPageSize;
        }
        if (columns is not null && (columns.NullPages.Length != n || columns.MinValues.Length != n || columns.MaxValues.Length != n
                                    || columns.NullCounts is { } nulls && nulls.Length != n || columns.NanCounts is { } nans && nans.Length != n))
            columns = null;
        return new ChunkPageIndex(firstRows, at, sizes, groupRows, columns);
    }
}

/// <summary>
/// The page indexes of a file's column chunks, read when first asked for (nearby ones in one read) and kept for every
/// reader of the file. An index that is missing, lies outside the file or does not parse is taken as absent.
/// </summary>
internal sealed class PageIndexes(ParquetFile file)
{
    private const long CoalesceGap = 1 << 20;

    private readonly ConcurrentDictionary<(int Group, int Leaf), OffsetIndex?> _offsets = new();
    private readonly ConcurrentDictionary<(int Group, int Leaf), ColumnIndex?> _columns = new();

    /// <summary>The checked index of a chunk loaded before (null when it has none or it is unusable).</summary>
    /// <param name="group">The row group.</param>
    /// <param name="leaf">The leaf (column chunk) in it.</param>
    /// <param name="withColumns">Whether its column index is wanted (it must have been loaded).</param>
    public ChunkPageIndex? Get(int group, int leaf, bool withColumns)
    {
        _offsets.TryGetValue((group, leaf), out OffsetIndex? offsets);
        ColumnIndex? columns = withColumns && _columns.TryGetValue((group, leaf), out ColumnIndex? c) ? c : null;
        RowGroup g = file.Thrift.RowGroups[group];
        ColumnMetaData? meta = g.Columns[leaf].MetaData;
        if (meta is null) return null;
        (long start, long length) = ParquetReader.ChunkRange(meta, file.Source.Length, "", ParquetReader.IsOldParquetMr(file.Thrift.CreatedBy));
        return ChunkPageIndex.Create(offsets, columns, g.NumRows, start, length);
    }

    /// <summary>Loads the offset indexes of <paramref name="offsets"/> and the column indexes of <paramref name="columns"/> not loaded yet.</summary>
    public async ValueTask LoadAsync(IEnumerable<(int Group, int Leaf)> offsets, IEnumerable<(int Group, int Leaf)> columns, bool useAsync,
        CancellationToken ct)
    {
        List<(long Start, int Length, (int, int) Key, bool IsOffset)> wanted = [];
        foreach (var key in offsets.Distinct())
        {
            if (_offsets.ContainsKey(key)) continue;
            ColumnChunk chunk = file.Thrift.RowGroups[key.Group].Columns[key.Leaf];
            if (Range(chunk.OffsetIndexOffset, chunk.OffsetIndexLength) is { } r) wanted.Add((r.Start, r.Length, key, true));
            else _offsets.TryAdd(key, null);
        }
        foreach (var key in columns.Distinct())
        {
            if (_columns.ContainsKey(key)) continue;
            ColumnChunk chunk = file.Thrift.RowGroups[key.Group].Columns[key.Leaf];
            if (Range(chunk.ColumnIndexOffset, chunk.ColumnIndexLength) is { } r) wanted.Add((r.Start, r.Length, key, false));
            else _columns.TryAdd(key, null);
        }
        if (wanted.Count == 0) return;
        wanted.Sort((a, b) => a.Start.CompareTo(b.Start));
        int i = 0;
        while (i < wanted.Count)
        {
            // One read for indexes that lie close together (writers put them all near the footer).
            long start = wanted[i].Start, end = start + wanted[i].Length;
            int j = i + 1;
            while (j < wanted.Count && wanted[j].Start - end <= CoalesceGap && Math.Max(end, wanted[j].Start + wanted[j].Length) - start <= 64L << 20)
                end = Math.Max(end, wanted[j].Start + wanted[j++].Length);
            byte[] bytes = new byte[end - start];
            await ParquetFile.ReadExactlyAsync(file.Source, start, bytes, useAsync, ct).ConfigureAwait(false);
            for (; i < j; i++)
            {
                var (at, length, key, isOffset) = wanted[i];
                ReadOnlySpan<byte> span = bytes.AsSpan((int)(at - start), length);
                if (isOffset) _offsets.TryAdd(key, Parse(OffsetIndex.Parse, span));
                else _columns.TryAdd(key, Parse(ColumnIndex.Parse, span));
            }
        }

        (long Start, int Length)? Range(long? offset, int? length) =>
            offset is long o && length is int l && o >= 0 && l > 0 && o <= file.Source.Length - l ? (o, l) : null;

        static T? Parse<T>(ParseSpan<T> parse, ReadOnlySpan<byte> data) where T : class
        {
            try
            {
                return parse(data);
            }
            catch (ParquetFormatException)
            {
                return null; // a damaged index is no index
            }
        }
    }

    private delegate T ParseSpan<out T>(ReadOnlySpan<byte> data);
}
