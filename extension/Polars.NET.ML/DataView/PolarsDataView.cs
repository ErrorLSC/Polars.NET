using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Microsoft.ML;
using Polars.NET.Core;
using Microsoft.ML.Data;
using Polars.NET.Core.Arrow;

namespace Polars.NET.ML.DataView;

/// <summary>
/// A zero-copy ML.NET IDataView implementation backed by an IPolarsDataFrame interface.
/// </summary>
/// <remarks>
/// Creates a new IDataView backed by Polars.
/// <para>
/// Map columns (<c>map[key, value]</c>): a map of numbers is one fixed-size vector of its values over the column's
/// key vocabulary (slot names: the keys as text; a key the row lacks is 0, a null value NaN for floating values and
/// 0 for integers, a null map all zero; a key repeated keeps its first value). The vocabulary is every key the frame
/// holds, sorted, unless <paramref name="mapKeys"/> gives it: give the training data's keys (its SlotNames) when
/// predicting, so the vectors have the slots the model was trained on. A map of text or booleans is two
/// variable-length vectors, <c>{name}.Keys</c> and <c>{name}.Values</c>. Maps of other values are refused: convert
/// them in Polars first (for instance with <c>MapOps.Get(key)</c>).
/// </para>
/// </remarks>
/// <param name="df">The frame.</param>
/// <param name="enableMacroShuffle">Shuffle chunks in the engine instead of rows in ML.NET.</param>
/// <param name="mapKeys">Key vocabularies of map columns of numbers, by column name (keys as text).</param>
public sealed class PolarsDataView(IPolarsDataFrame df, bool enableMacroShuffle, IReadOnlyDictionary<string, IReadOnlyList<string>>? mapKeys) : IDataView
{
    /// <summary>Creates a new IDataView backed by Polars.</summary>
    public PolarsDataView(IPolarsDataFrame df, bool enableMacroShuffle = false) : this(df, enableMacroShuffle, null) { }

    private readonly IPolarsDataFrame _df = df ?? throw new ArgumentNullException(nameof(df));
    private readonly long _rowCount = df.Height;

    /// <inheritdoc/>
    public bool CanShuffle { get; } = enableMacroShuffle;

    private readonly (DataViewSchema Schema, DictionaryKeyColumn?[] KeyColumns, ViewColumn[] Views) _schemaInfo = BuildSchema(df, mapKeys);

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
        int[] indices = FrameColumns(columnsNeeded);

        ulong? seed = rand != null ? (ulong)rand.Next() : null;

        IArrowArrayStream arrowStream = _df.ToArrowStream(indices, seed);

        var batches = new ArrowStreamEnumerable(arrowStream);

        return new PolarsRowCursor(Schema, batches, columnsNeeded, _schemaInfo.KeyColumns, _schemaInfo.Views, indices);
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

        int[] indices = FrameColumns(columnsNeeded);
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

                cursors[i] = new PolarsRowCursor(Schema, batches, columnsNeeded, _schemaInfo.KeyColumns, _schemaInfo.Views, indices, sliceDf, baseRowOffset: sliceOffset);
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

    /// <summary>The frame columns the DataView columns read, each once, in order (all of them for none).</summary>
    private int[] FrameColumns(IEnumerable<DataViewSchema.Column>? columnsNeeded)
    {
        IEnumerable<DataViewSchema.Column> needed = columnsNeeded ?? Schema;
        return [.. needed.Select(c => _schemaInfo.Views[c.Index].FrameColumn).Distinct()];
    }

    private static (DataViewSchema, DictionaryKeyColumn?[], ViewColumn[]) BuildSchema(IPolarsDataFrame df, IReadOnlyDictionary<string, IReadOnlyList<string>>? mapKeys)
    {
        var columns = new List<(string Name, IArrowType ArrowType)>();
        var keyColumns = new List<DictionaryKeyColumn?>();
        var dictIndices = new List<(int View, int Frame)>();
        var views = new List<ViewColumn>();
        var mapColumns = new List<(string Name, DataViewType Type, DataViewSchema.Annotations? Annotations)>();
        int frameColumn = -1;
        foreach (var kvp in df.Schema)
        {
            frameColumn++;
            IArrowType arrowType = kvp.Value.GetArrowType();
            if (arrowType is MapType map)
            {
                IArrowType keyType = map.KeyField.DataType, value = map.ValueField.DataType;
                if (MapColumnLayout.IsNumeric(value) && MapKeyText.Supports(keyType))
                {
                    MapVocabulary vocabulary = mapKeys?.TryGetValue(kvp.Key, out var given) == true
                        ? MapVocabulary.Given(given)
                        : MapVocabulary.Scanned(df, frameColumn);
                    if (vocabulary.Keys.Length == 0)
                        throw new NotSupportedException(
                            $"The map column '{kvp.Key}' holds no keys to make vector slots of: give its keys (mapKeys), or leave the column out.");
                    views.Add(new ViewColumn(frameColumn, ColumnPart.Whole, vocabulary));
                    columns.Add((kvp.Key, arrowType));
                    keyColumns.Add(null);
                    mapColumns.Add((kvp.Key, new VectorDataViewType((PrimitiveDataViewType)ArrowDataViewMapper.GetDataViewType(value), vocabulary.Keys.Length), vocabulary.SlotNames()));
                }
                else
                {
                    var keys = new VectorDataViewType(MapColumnLayout.PartItem(kvp.Key, "keys", keyType), 0);
                    var values = new VectorDataViewType(MapColumnLayout.PartItem(kvp.Key, "values", value), 0);
                    views.Add(new ViewColumn(frameColumn, ColumnPart.MapKeys));
                    columns.Add(($"{kvp.Key}.Keys", arrowType));
                    keyColumns.Add(null);
                    mapColumns.Add(($"{kvp.Key}.Keys", keys, null));
                    views.Add(new ViewColumn(frameColumn, ColumnPart.MapValues));
                    columns.Add(($"{kvp.Key}.Values", arrowType));
                    keyColumns.Add(null);
                    mapColumns.Add(($"{kvp.Key}.Values", values, null));
                }
                continue;
            }
            views.Add(new ViewColumn(frameColumn, ColumnPart.Whole));
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
                    dictIndices.Add((columns.Count, frameColumn));
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
            else if (columns[i].ArrowType is MapType)
            {
                var (name, type, annotations) = mapColumns.First(m => m.Name == columns[i].Name);
                builder.AddColumn(name, type, annotations);
            }
            else
                builder.AddColumn(columns[i].Name, ArrowDataViewMapper.GetDataViewType(columns[i].ArrowType));
        }

        return (builder.ToSchema(), [.. keyColumns], [.. views]);
    }

    /// <summary>
    /// One pass over the Categorical columns only, reading each batch's dictionary (not its rows), to build the
    /// global category table that fixes the key cardinality and the key of every category.
    /// </summary>
    private static void CollectCategories(IPolarsDataFrame df, List<(int View, int Frame)> dictIndices, List<DictionaryKeyColumn?> keyColumns)
    {
        int[] frames = [.. dictIndices.Select(static d => d.Frame)];
        foreach (RecordBatch batch in new ArrowStreamEnumerable(df.ToArrowStream(frames)))
        {
            for (int j = 0; j < frames.Length; j++)
            {
                keyColumns[dictIndices[j].View]!.AddCategories(((DictionaryArray)batch.Column(j)).Dictionary);
            }
        }
    }
}