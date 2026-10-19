using Apache.Arrow.Ipc;
using Microsoft.ML;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;

namespace Polars.NET.ML.DataView;

/// <summary>
/// A zero-copy ML.NET IDataView implementation backed by an IPolarsDataFrame interface.
/// </summary>
/// <remarks>
/// Creates a new IDataView backed by Polars.
/// </remarks>
public sealed class PolarsDataView(IPolarsDataFrame df, bool enableMacroShuffle = false) : IDataView
{
    private readonly IPolarsDataFrame _df = df ?? throw new ArgumentNullException(nameof(df));
    private readonly long _rowCount = df.Height;

    /// <inheritdoc/>
    public bool CanShuffle { get; } = enableMacroShuffle;

    /// <inheritdoc/>
    public DataViewSchema Schema { get; } = BuildSchema(df);
    /// <inheritdoc/>
    public long? GetRowCount() => _rowCount;

    // ==========================================
    // Cursor Factory
    // ==========================================
    /// <inheritdoc/>
    public DataViewRowCursor GetRowCursor(IEnumerable<DataViewSchema.Column> columnsNeeded, Random? rand = null)
    {
        int[] indices = columnsNeeded?.Select(c => c.Index).ToArray() ?? [];

        ulong? seed = rand != null ? (ulong)rand.Next() : null;

        IArrowArrayStream arrowStream = _df.ToArrowStream(indices, seed);

        var batches = new ArrowStreamEnumerable(arrowStream);

        return new PolarsRowCursor(Schema, batches,columnsNeeded);
    }

    /// <inheritdoc/>
    public DataViewRowCursor[] GetRowCursorSet(IEnumerable<DataViewSchema.Column> columnsNeeded, int n, Random? rand = null)
    {
        if (n <= 1 || _rowCount == 0)
        {
            return [GetRowCursor(columnsNeeded, rand)];
        }

        int cursorCount = (int)Math.Min(n, _rowCount);
        if (cursorCount <= 1)
        {
            return [GetRowCursor(columnsNeeded, rand)];
        }

        int[] indices = columnsNeeded?.Select(c => c.Index).ToArray() ?? [];
        long rowsPerCursor = _rowCount / cursorCount;
        long remainder = _rowCount % cursorCount;

        var cursors = new DataViewRowCursor[cursorCount];
        long currentOffset = 0;
        int createdCount = 0;

        try
        {
            for (int i = 0; i < cursorCount; i++)
            {
                long sliceLength = rowsPerCursor + (i < remainder ? 1 : 0);
                long sliceOffset = currentOffset;
                currentOffset += sliceLength;

                IPolarsDataFrame sliceDf = _df.Slice(sliceOffset, (ulong)sliceLength);
                ulong? seed = rand != null ? (ulong)rand.Next() : null;
                IArrowArrayStream arrowStream = sliceDf.ToArrowStream(indices, seed);
                var batches = new ArrowStreamEnumerable(arrowStream);

                cursors[i] = new PolarsRowCursor(Schema, batches, columnsNeeded, sliceDf, baseRowOffset: sliceOffset);
                createdCount++;
            }

            return cursors;
        }
        catch
        {
            for (int i = 0; i < createdCount; i++)
            {
                cursors[i]?.Dispose();
            }
            throw;
        }
    }

    private static DataViewSchema BuildSchema(IPolarsDataFrame df)
    {
        var builder = new DataViewSchema.Builder();
        IPolarsSchema schema = df.Schema;

        foreach (var kvp in schema)
        {
            builder.AddColumn(kvp.Key, ArrowDataViewMapper.GetDataViewType(kvp.Value.GetArrowType()));
        }

        return builder.ToSchema();
    }
}