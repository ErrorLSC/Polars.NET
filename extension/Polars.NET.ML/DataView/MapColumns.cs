using System.Collections.Frozen;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.ML;
using Microsoft.ML.Data;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;

namespace Polars.NET.ML.DataView;

/// <summary>Which part of its frame column a DataView column reads.</summary>
internal enum ColumnPart
{
    /// <summary>The column itself (for a map of numbers: its values in the key vocabulary's slots).</summary>
    Whole,
    /// <summary>A map's keys, row by row, as a variable-length vector.</summary>
    MapKeys,
    /// <summary>A map's values, row by row, as a variable-length vector.</summary>
    MapValues,
}

/// <summary>A DataView column: the frame column it reads, which part of it, and a numeric map's key vocabulary.</summary>
internal sealed record ViewColumn(int FrameColumn, ColumnPart Part, MapVocabulary? Vocabulary = null);

/// <summary>
/// The key vocabulary of a map of numbers: slot <c>i</c> of the column's vector holds the value of key
/// <c>Keys[i]</c> (keys as text: as given, else every key of the frame sorted ordinally).
/// </summary>
internal sealed class MapVocabulary
{
    private readonly FrozenDictionary<string, int> _slots;

    private MapVocabulary(string[] keys)
    {
        Keys = keys;
        _slots = keys.Select(static (k, i) => KeyValuePair.Create(k, i)).ToFrozenDictionary(StringComparer.Ordinal);
    }

    public string[] Keys { get; }

    public bool TryGetSlot(string key, out int slot) => _slots.TryGetValue(key, out slot);

    /// <summary>The given keys, in their order, each once.</summary>
    public static MapVocabulary Given(IEnumerable<string> keys) => new([.. keys.Distinct(StringComparer.Ordinal)]);

    /// <summary>Every key the frame's map column at <paramref name="column"/> holds, sorted ordinally.</summary>
    public static MapVocabulary Scanned(IPolarsDataFrame df, int column)
    {
        HashSet<string> keys = new(StringComparer.Ordinal);
        foreach (RecordBatch batch in new ArrowStreamEnumerable(df.ToArrowStream([column])))
        {
            using (batch)
            {
                var maps = (MapArray)batch.Column(0);
                Func<int, string?> text = MapKeyText.For(maps.Keys);
                for (int row = 0; row < maps.Length; row++)
                {
                    if (maps.IsNull(row)) continue;
                    for (int entry = maps.ValueOffsets[row]; entry < maps.ValueOffsets[row + 1]; entry++)
                        if (text(entry) is { } key) keys.Add(key);
                }
            }
        }
        string[] sorted = [.. keys];
        System.Array.Sort(sorted, StringComparer.Ordinal);
        return new MapVocabulary(sorted);
    }

    /// <summary>The standard ML.NET <c>SlotNames</c> annotation: the keys, slot by slot.</summary>
    public DataViewSchema.Annotations SlotNames()
    {
        var builder = new DataViewSchema.Annotations.Builder();
        string[] keys = Keys;
        builder.Add(
            "SlotNames",
            new VectorDataViewType(TextDataViewType.Instance, keys.Length),
            (ref VBuffer<ReadOnlyMemory<char>> value) =>
            {
                var editor = VBufferEditor.Create(ref value, keys.Length);
                for (int i = 0; i < keys.Length; i++) editor.Values[i] = keys[i].AsMemory();
                value = editor.Commit();
            });
        return builder.ToAnnotations();
    }
}

/// <summary>Map keys as the text that names their slots.</summary>
internal static class MapKeyText
{
    /// <summary>Whether keys of <paramref name="type"/> name slots: text, integers and booleans.</summary>
    public static bool Supports(IArrowType type) => type is StringType or LargeStringType or StringViewType or BooleanType
        or Int8Type or Int16Type or Int32Type or Int64Type or UInt8Type or UInt16Type or UInt32Type or UInt64Type;

    /// <summary>The text of each key of <paramref name="keys"/> (null for a null key, which a map does not hold).</summary>
    public static Func<int, string?> For(IArrowArray keys) => keys switch
    {
        StringViewArray s => i => s.GetString(i),
        StringArray s => i => s.GetString(i),
        LargeStringArray s => i => s.GetString(i),
        BooleanArray b => i => b.GetValue(i) is { } v ? (v ? "true" : "false") : null,
        Int8Array a => i => a.GetValue(i)?.ToString(CultureInfo.InvariantCulture),
        Int16Array a => i => a.GetValue(i)?.ToString(CultureInfo.InvariantCulture),
        Int32Array a => i => a.GetValue(i)?.ToString(CultureInfo.InvariantCulture),
        Int64Array a => i => a.GetValue(i)?.ToString(CultureInfo.InvariantCulture),
        UInt8Array a => i => a.GetValue(i)?.ToString(CultureInfo.InvariantCulture),
        UInt16Array a => i => a.GetValue(i)?.ToString(CultureInfo.InvariantCulture),
        UInt32Array a => i => a.GetValue(i)?.ToString(CultureInfo.InvariantCulture),
        UInt64Array a => i => a.GetValue(i)?.ToString(CultureInfo.InvariantCulture),
        _ => throw new NotSupportedException($"Map keys of type '{keys.Data.DataType.Name}' do not name vector slots."),
    };
}

/// <summary>How the DataView lays out the map columns of a frame (see <see cref="PolarsDataView"/>).</summary>
internal static class MapColumnLayout
{
    /// <summary>Whether a map of <paramref name="value"/> values is one vector of its values over the key vocabulary.</summary>
    public static bool IsNumeric(IArrowType value) => value is Int8Type or Int16Type or Int32Type or Int64Type
        or UInt8Type or UInt16Type or UInt32Type or UInt64Type or FloatType or DoubleType;

    /// <summary>The DataView item type of a map's keys or values as a variable-length vector, or a friendly refusal.</summary>
    public static PrimitiveDataViewType PartItem(string column, string part, IArrowType type) => type switch
    {
        StringType or LargeStringType or StringViewType => TextDataViewType.Instance,
        BooleanType => BooleanDataViewType.Instance,
        _ when IsNumeric(type) => (PrimitiveDataViewType)ArrowDataViewMapper.GetDataViewType(type),
        _ => throw new NotSupportedException(
            $"The map column '{column}' has {part} of type '{type.Name}', which no ML.NET vector holds. Convert them in Polars first: "
            + "take the keys you need as columns (MapOps.Get(key)), or cast the values to numbers or text."),
    };
}
