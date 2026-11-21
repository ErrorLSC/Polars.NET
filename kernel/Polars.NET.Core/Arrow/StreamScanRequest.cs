using System.Runtime.InteropServices;

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

    /// <summary>The request from the engine's callback arguments (column count and row limit -1 mean all).</summary>
    internal static unsafe StreamScanRequest FromNative(byte** columns, nint columnCount, long rowLimit)
    {
        string[]? names = null;
        if (columnCount >= 0)
        {
            names = new string[columnCount];
            for (int i = 0; i < names.Length; i++) names[i] = Marshal.PtrToStringUTF8((nint)columns[i])!;
        }
        return new(names, rowLimit >= 0 ? rowLimit : null);
    }
}
