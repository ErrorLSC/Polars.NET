using Kaminari.Samidare.IR;

namespace Polars.NET.Core.Delta;

/// <summary>
/// How a Delta table scan is made again with a row filter (the query compiler's IR, columns by logical name), at the
/// version the first scan read: the files the filter rules out are left out, the rows are the same. A scan whose
/// options number rows or limit them has none (leaving files out would change which rows those are), nor one whose given
/// schema changes a column's type (a filter then reads other values than the statistics describe).
/// </summary>
/// <param name="version">The version the first scan read.</param>
/// <param name="scan">Scans the table at that version with a filter (null: every file), reporting the number of data files scanned.</param>
internal sealed class DeltaScanOrigin(long version, Func<ScalarExpr?, Action<int>?, LazyFrameHandle> scan)
{
    /// <summary>The version the scan reads.</summary>
    public long Version { get; } = version;

    /// <summary>The scan again, without the files <paramref name="filter"/> rules out.</summary>
    public LazyFrameHandle Scan(ScalarExpr? filter, Action<int>? scannedFiles = null) => scan(filter, scannedFiles);

    /// <summary>Whether the scan can be made again leaving files out, with the same rows: no row limit, no row numbers.</summary>
    public static bool AllowsSkipping(ulong? nRows, string? rowIndexName) => nRows is null && rowIndexName is null;
}
