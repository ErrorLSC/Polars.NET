using System.Globalization;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Arrays;

namespace Seseragi.Files.Tests;

/// <summary>Renders Arrow values as invariant strings, so columns can be compared across physical types
/// (Int8 vs Int32, LargeString vs String, dictionary vs plain).</summary>
internal static class ArrowRender
{
    public static List<string> Rows(RecordBatch batch) =>
        [.. Enumerable.Range(0, batch.Length).Select(r => string.Join(" | ", Enumerable.Range(0, batch.ColumnCount).Select(c => Value(batch.Column(c), r))))];

    public static List<string> Rows(IEnumerable<RecordBatch> batches) => [.. batches.SelectMany(Rows)];

    public static string Value(IArrowArray a, int i)
    {
        if (a.IsNull(i)) return "null";
        return a switch
        {
            BooleanArray b => b.GetValue(i)!.Value ? "true" : "false",
            StringArray s => '"' + s.GetString(i) + '"',
            LargeStringArray s => '"' + s.GetString(i) + '"',
            StringViewArray s => '"' + s.GetString(i) + '"',
            BinaryArray b => Hex(b.GetBytes(i)),
            LargeBinaryArray b => Hex(b.GetBytes(i)),
            BinaryViewArray b => Hex(b.GetBytes(i)),
            Decimal128Array d => d.GetString(i)!,
            Decimal256Array d => d.GetString(i)!,
            FixedSizeBinaryArray f => Hex(f.GetBytes(i)),
            MonthDayNanosecondIntervalArray m => m.GetValue(i)!.Value is var v ? $"{v.Months}m{v.Days}d{v.Nanoseconds}ns" : "",
            DictionaryArray d => Value(d.Dictionary, Convert.ToInt32(((IConvertible?)Boxed(d.Indices, i))!, CultureInfo.InvariantCulture)),
            StructArray s => "{" + string.Join(", ", s.Fields.Select((f, k) => $"{((Apache.Arrow.Types.StructType)s.Data.DataType).Fields[k].Name}: {Value(f, i)}")) + "}",
            MapArray m => "{" + string.Join(", ", Enumerable.Range(m.ValueOffsets[i], m.GetValueLength(i)).Select(k => $"{Value(m.Keys, k)}: {Value(m.Values, k)}")) + "}",
            ListArray l => "[" + string.Join(", ", Enumerable.Range(l.ValueOffsets[i], l.GetValueLength(i)).Select(k => Value(l.Values, k))) + "]",
            LargeListArray l => "[" + string.Join(", ", Enumerable.Range((int)l.ValueOffsets[i], (int)l.GetValueLength(i)).Select(k => Value(l.Values, k))) + "]",
            FixedSizeListArray l => "[" + string.Join(", ", Enumerable.Range((l.Offset + i) * ((Apache.Arrow.Types.FixedSizeListType)l.Data.DataType).ListSize, ((Apache.Arrow.Types.FixedSizeListType)l.Data.DataType).ListSize).Select(k => Value(l.Values, k))) + "]",
            _ => Convert.ToString(Boxed(a, i), CultureInfo.InvariantCulture) ?? "?",
        };
    }

    private static object? Boxed(IArrowArray a, int i) => a switch
    {
        Int8Array x => (object?)x.GetValue(i), // object, or the arms unify to double? and big integers lose digits
        Int16Array x => x.GetValue(i),
        Int32Array x => x.GetValue(i),
        Int64Array x => x.GetValue(i),
        UInt8Array x => x.GetValue(i),
        UInt16Array x => x.GetValue(i),
        UInt32Array x => x.GetValue(i),
        UInt64Array x => x.GetValue(i),
        HalfFloatArray x => (float?)x.GetValue(i),
        FloatArray x => x.GetValue(i),
        DoubleArray x => x.GetValue(i),
        Date32Array x => x.Values[i],
        Date64Array x => x.Values[i],
        Time32Array x => x.Values[i],
        Time64Array x => x.Values[i],
        TimestampArray x => x.Values[i],
        DurationArray x => x.Values[i],
        YearMonthIntervalArray x => x.GetValue(i)!.Value.Months,
        _ => throw new NotSupportedException(a.GetType().Name),
    };

    private static string Hex(ReadOnlySpan<byte> b) => "0x" + Convert.ToHexString(b);
}
