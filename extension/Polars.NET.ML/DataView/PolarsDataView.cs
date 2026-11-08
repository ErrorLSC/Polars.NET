using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
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

    private readonly (DataViewSchema Schema, DictionaryKeyColumn?[] KeyColumns) _schemaInfo = BuildSchema(df);

    /// <inheritdoc/>
    public DataViewSchema Schema => _schemaInfo.Schema;
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

        return new PolarsRowCursor(Schema, batches, columnsNeeded, _schemaInfo.KeyColumns);
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

                cursors[i] = new PolarsRowCursor(Schema, batches, columnsNeeded, _schemaInfo.KeyColumns, sliceDf, baseRowOffset: sliceOffset);
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

    private static (DataViewSchema, DictionaryKeyColumn?[]) BuildSchema(IPolarsDataFrame df)
    {
        var columns = new List<(string Name, IArrowType ArrowType)>();
        var keyColumns = new List<DictionaryKeyColumn?>();
        var dictIndices = new List<int>();
        foreach (var kvp in df.Schema)
        {
            IArrowType arrowType = kvp.Value.GetArrowType();
            DictionaryKeyColumn? key = null;
            if (arrowType is DictionaryType)
            {
                // Enum: the category table is part of the type. Categorical: discovered by a dictionary pre-scan.
                string[]? enumCategories = kvp.Value.GetEnumCategories();
                if (enumCategories != null)
                {
                    key = DictionaryKeyColumn.FromCategories(enumCategories);
                }
                else
                {
                    key = new DictionaryKeyColumn();
                    dictIndices.Add(columns.Count);
                }
            }
            columns.Add((kvp.Key, arrowType));
            keyColumns.Add(key);
        }

        if (dictIndices.Count > 0)
        {
            CollectCategories(df, dictIndices, keyColumns);
        }

        var builder = new DataViewSchema.Builder();
        for (int i = 0; i < columns.Count; i++)
        {
            if (keyColumns[i] is { } key)
                builder.AddColumn(columns[i].Name, key.KeyType, key.BuildAnnotations());
            else
                builder.AddColumn(columns[i].Name, ArrowDataViewMapper.GetDataViewType(columns[i].ArrowType));
        }

        return (builder.ToSchema(), [.. keyColumns]);
    }

    /// <summary>
    /// One pass over the Categorical columns only, reading each batch's dictionary (not its rows), to build the
    /// global category table that fixes the key cardinality and the key of every category.
    /// </summary>
    private static void CollectCategories(IPolarsDataFrame df, List<int> dictIndices, List<DictionaryKeyColumn?> keyColumns)
    {
        int[] indices = [.. dictIndices];
        foreach (RecordBatch batch in new ArrowStreamEnumerable(df.ToArrowStream(indices)))
        {
            for (int j = 0; j < indices.Length; j++)
            {
                keyColumns[indices[j]]!.AddCategories(((DictionaryArray)batch.Column(j)).Dictionary);
            }
        }
    }
}