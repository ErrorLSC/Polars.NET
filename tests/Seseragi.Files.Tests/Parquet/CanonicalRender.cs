using System.Globalization;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// Renders Arrow values in the canonical text form of <c>TestData/parquet/parquet_check.py</c> and its superset
/// <c>TestData/ipc/arrow_canonical.py</c> (see their docstrings), so a column read here hashes like the same column
/// read by pyarrow, DuckDB or Polars.
/// </summary>
internal static class CanonicalRender
{
    /// <summary>The first 16 hex digits of the SHA-256 of the rendered values joined by newlines.</summary>
    public static string Hash(IEnumerable<string> values) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(string.Join('\n', values))))[..16];

    public static IEnumerable<string> Column(IEnumerable<RecordBatch> batches, int column) =>
        batches.SelectMany(b => Enumerable.Range(0, b.Length).Select(i => Value(b.Column(column), i)));

    public static string Value(IArrowArray array, int i)
    {
        // Unions and run-end encoded arrays have no validity of their own: their values come from the children.
        if (array is UnionArray u) return Union(u, i);
        if (array is RunEndEncodedArray r) return Value(r.Values, Physical(r, i));
        if (array is NullArray || array.IsNull(i)) return "null";
        return array switch
        {
            BooleanArray b => b.GetValue(i)!.Value ? "true" : "false",
            Int8Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            Int16Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            Int32Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            Int64Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            UInt8Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            UInt16Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            UInt32Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            UInt64Array a => a.GetValue(i)!.Value.ToString(CultureInfo.InvariantCulture),
            HalfFloatArray a => "f:" + BitConverter.HalfToUInt16Bits(a.GetValue(i)!.Value).ToString("x4", CultureInfo.InvariantCulture),
            FloatArray a => "f:" + BitConverter.SingleToUInt32Bits(a.GetValue(i)!.Value).ToString("x8", CultureInfo.InvariantCulture),
            DoubleArray a => "f:" + BitConverter.DoubleToUInt64Bits(a.GetValue(i)!.Value).ToString("x16", CultureInfo.InvariantCulture),
            Decimal32Array d => $"d:{Unscaled(d.ValueBuffer.Span, d.Offset + i, 4)}e-{((Decimal32Type)d.Data.DataType).Scale}",
            Decimal64Array d => $"d:{Unscaled(d.ValueBuffer.Span, d.Offset + i, 8)}e-{((Decimal64Type)d.Data.DataType).Scale}",
            Decimal128Array d => $"d:{Unscaled(d.ValueBuffer.Span, d.Offset + i, 16)}e-{((Decimal128Type)d.Data.DataType).Scale}",
            Decimal256Array d => $"d:{Unscaled(d.ValueBuffer.Span, d.Offset + i, 32)}e-{((Decimal256Type)d.Data.DataType).Scale}",
            Date32Array d => "D:" + Raw<int>(d, i).ToString(CultureInfo.InvariantCulture),
            Date64Array d => "D:" + FloorDays(Raw<long>(d, i)).ToString(CultureInfo.InvariantCulture),
            Time32Array t => "t:" + ((BigInteger)Raw<int>(t, i) * Nanos(((Time32Type)t.Data.DataType).Unit)).ToString(CultureInfo.InvariantCulture),
            Time64Array t => "t:" + ((BigInteger)Raw<long>(t, i) * Nanos(((Time64Type)t.Data.DataType).Unit)).ToString(CultureInfo.InvariantCulture),
            TimestampArray t => "T:" + ((BigInteger)Raw<long>(t, i) * Nanos(((TimestampType)t.Data.DataType).Unit)).ToString(CultureInfo.InvariantCulture),
            DurationArray d => "P:" + ((BigInteger)Raw<long>(d, i) * Nanos(((DurationType)d.Data.DataType).Unit)).ToString(CultureInfo.InvariantCulture),
            MonthDayNanosecondIntervalArray m => m.GetValue(i)!.Value is var v ? $"I:{v.Months},{v.Days},{v.Nanoseconds}" : "",
            YearMonthIntervalArray y => "Y:" + Raw<int>(y, i).ToString(CultureInfo.InvariantCulture),
            DayTimeIntervalArray t => $"J:{Raw<int>(t, 2 * i)},{Raw<int>(t, 2 * i + 1)}",
            LargeStringArray s => "u:" + Convert.ToHexStringLower(s.GetBytes(i)),
            LargeBinaryArray b => "b:" + Convert.ToHexStringLower(b.GetBytes(i)),
            StringArray s => "u:" + Convert.ToHexStringLower(s.GetBytes(i)),
            StringViewArray s => "u:" + Convert.ToHexStringLower(s.GetBytes(i)),
            BinaryViewArray b => "b:" + Convert.ToHexStringLower(b.GetBytes(i)),
            BinaryArray b => "b:" + Convert.ToHexStringLower(b.GetBytes(i)),
            FixedSizeBinaryArray f => "b:" + Convert.ToHexStringLower(f.GetBytes(i)),
            DictionaryArray d => Value(d.Dictionary, Index(d.Indices, i)),
            MapArray m => "M{" + string.Join(",", Enumerable.Range(m.ValueOffsets[i], m.GetValueLength(i)).Select(k => $"{Value(m.Keys, k)}=>{Value(m.Values, k)}")) + "}",
            ListArray l => "[" + string.Join(",", Enumerable.Range(l.ValueOffsets[i], l.GetValueLength(i)).Select(k => Value(l.Values, k))) + "]",
            LargeListArray l => "[" + string.Join(",", Range(Raw<long>(l, i), Raw<long>(l, i + 1) - Raw<long>(l, i)).Select(k => Value(l.Values, k))) + "]",
            ListViewArray l => "[" + string.Join(",", Range(Raw<int>(l, i), Raw2<int>(l, i)).Select(k => Value(l.Values, k))) + "]",
            LargeListViewArray l => "[" + string.Join(",", Range(Raw<long>(l, i), Raw2<long>(l, i)).Select(k => Value(l.Values, k))) + "]",
            FixedSizeListArray f => "[" + string.Join(",", Enumerable.Range((f.Offset + i) * ((FixedSizeListType)f.Data.DataType).ListSize, ((FixedSizeListType)f.Data.DataType).ListSize).Select(k => Value(f.Values, k))) + "]",
            StructArray s => "{" + string.Join(",", ((StructType)s.Data.DataType).Fields.Select((f, k) => $"{f.Name}:{Value(s.Fields[k], i)}")) + "}",
            _ => throw new NotSupportedException($"No canonical form for {array.Data.DataType}."),
        };
    }

    private static T Raw<T>(IArrowArray a, int i) where T : unmanaged =>
        MemoryMarshal.Cast<byte, T>(a.Data.Buffers[1].Span)[a.Data.Offset + i];

    /// <summary>The third buffer's value (list view sizes).</summary>
    private static T Raw2<T>(IArrowArray a, int i) where T : unmanaged =>
        MemoryMarshal.Cast<byte, T>(a.Data.Buffers[2].Span)[a.Data.Offset + i];

    /// <summary>Milliseconds to whole days, rounding down (as Python's <c>//</c>).</summary>
    private static BigInteger FloorDays(long ms)
    {
        BigInteger q = BigInteger.DivRem(ms, 86_400_000, out BigInteger r);
        return r < 0 ? q - 1 : q;
    }

    private static IEnumerable<int> Range(long start, long count) => Enumerable.Range(checked((int)start), checked((int)count));

    private static string Union(UnionArray u, int i)
    {
        var type = (UnionType)u.Data.DataType;
        byte id = u.TypeIds[i];
        int child = System.Array.IndexOf(type.TypeIds, (int)id);
        int at = u is DenseUnionArray d ? d.ValueOffsets[i] : u.Offset + i;
        return $"U{id}:{Value(u.Fields[child], at)}";
    }

    /// <summary>The run holding logical slot <paramref name="i"/>.</summary>
    private static int Physical(RunEndEncodedArray r, int i)
    {
        long logical = r.Offset + i;
        IArrowArray ends = r.RunEnds;
        for (int k = 0; k < ends.Length; k++)
        {
            long end = ends switch
            {
                Int16Array a => a.GetValue(k)!.Value,
                Int32Array a => a.GetValue(k)!.Value,
                _ => ((Int64Array)ends).GetValue(k)!.Value,
            };
            if (end > logical) return k;
        }
        throw new InvalidOperationException("A slot past the last run.");
    }

    private static int Index(IArrowArray indices, int i) => indices switch
    {
        Int8Array a => a.GetValue(i)!.Value,
        Int16Array a => a.GetValue(i)!.Value,
        Int32Array a => a.GetValue(i)!.Value,
        UInt8Array a => a.GetValue(i)!.Value,
        UInt16Array a => a.GetValue(i)!.Value,
        UInt32Array a => (int)a.GetValue(i)!.Value,
        Int64Array a => (int)a.GetValue(i)!.Value,
        UInt64Array a => (int)a.GetValue(i)!.Value,
        _ => throw new NotSupportedException(),
    };

    private static long Nanos(TimeUnit unit) => unit switch
    {
        TimeUnit.Second => 1_000_000_000,
        TimeUnit.Millisecond => 1_000_000,
        TimeUnit.Microsecond => 1_000,
        _ => 1,
    };

    private static string Unscaled(ReadOnlySpan<byte> values, int index, int width) =>
        new BigInteger(values.Slice(index * width, width), isUnsigned: false, isBigEndian: false).ToString(CultureInfo.InvariantCulture);
}
