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
/// <param name="df">The Polars DataFrame.</param>
/// <param name="enableMacroShuffle">
/// If true, performs zero-copy Chunk-level shuffling in Rust. 
/// Warning: This disables ML.NET's row-level shuffling. Best for pre-randomized data.
/// </param>
public sealed class PolarsDataView(IPolarsDataFrame df, bool enableMacroShuffle = false) : IDataView
{
    private readonly IPolarsDataFrame _df = df ?? throw new ArgumentNullException(nameof(df));
    private readonly DataViewSchema _schema = BuildSchema(df);
    private readonly long _rowCount = df.Height;

    /// <inheritdoc/>
    public bool CanShuffle { get; } = enableMacroShuffle;

    /// <inheritdoc/>
    public DataViewSchema Schema => _schema;
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
        
        return new PolarsRowCursor(_schema, batches,columnsNeeded);
    }

    /// <inheritdoc/>
    public DataViewRowCursor[] GetRowCursorSet(IEnumerable<DataViewSchema.Column> columnsNeeded, int n, Random? rand = null)
        => [GetRowCursor(columnsNeeded, rand)];

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