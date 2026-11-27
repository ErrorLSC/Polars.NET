namespace Polars.NET.Core.Arrow;

/// <summary>
/// What a query reads from a stream source, as the engine's optimizer found it: the columns (null: all of them) and
/// at most how many leading rows (null: all). A source does less work by honouring it, or may ignore it: the engine
/// keeps only the requested columns and rows of whatever the stream gives, matching columns by name.
/// </summary>
/// <param name="Columns">The columns the query reads; null when it reads all.</param>
/// <param name="RowLimit">The leading rows the query keeps; null when it keeps all.</param>
public readonly record struct StreamScanRequest(string[]? Columns, long? RowLimit)
{
    /// <summary>Every column and every row.</summary>
    public static StreamScanRequest All => default;
}
