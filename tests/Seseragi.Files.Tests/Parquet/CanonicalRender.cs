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
/// Renders Arrow values in the canonical text form of <c>TestData/parquet/parquet_check.py</c> (see its docstring),
/// so a column read here hashes like the same column read by pyarrow or DuckDB.
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
        if (array.IsNull(i)) return "null";
        return array switch
        {
            NullArray => "null",
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
            Decimal128Array d => $"d:{Unscaled(d.ValueBuffer.Span, d.Offset + i, 16)}e-{((Decimal128Type)d.Data.DataType).Scale}",
            Decimal256Array d => $"d:{Unscaled(d.ValueBuffer.Span, d.Offset + i, 32)}e-{((Decimal256Type)d.Data.DataType).Scale}",
            Date32Array d => "D:" + Raw<int>(d, i).ToString(CultureInfo.InvariantCulture),
            Time32Array t => "t:" + ((BigInteger)Raw<int>(t, i) * Nanos(((Time32Type)t.Data.DataType).Unit)).ToString(CultureInfo.InvariantCulture),
            Time64Array t => "t:" + ((BigInteger)Raw<long>(t, i) * Nanos(((Time64Type)t.Data.DataType).Unit)).ToString(CultureInfo.InvariantCulture),
            TimestampArray t => "T:" + ((BigInteger)Raw<long>(t, i) * Nanos(((TimestampType)t.Data.DataType).Unit)).ToString(CultureInfo.InvariantCulture),
            DurationArray d => "P:" + ((BigInteger)Raw<long>(d, i) * Nanos(((DurationType)d.Data.DataType).Unit)).ToString(CultureInfo.InvariantCulture),
            MonthDayNanosecondIntervalArray m => m.GetValue(i)!.Value is var v ? $"I:{v.Months},{v.Days},{v.Nanoseconds}" : "",
            StringArray s => "u:" + Convert.ToHexStringLower(s.GetBytes(i)),
            BinaryArray b => "b:" + Convert.ToHexStringLower(b.GetBytes(i)),
            FixedSizeBinaryArray f => "b:" + Convert.ToHexStringLower(f.GetBytes(i)),
            DictionaryArray d => Value(d.Dictionary, Index(d.Indices, i)),
            ListArray l => "[" + string.Join(",", Enumerable.Range(l.ValueOffsets[i], l.GetValueLength(i)).Select(k => Value(l.Values, k))) + "]",
            StructArray s => "{" + string.Join(",", ((StructType)s.Data.DataType).Fields.Select((f, k) => $"{f.Name}:{Value(s.Fields[k], i)}")) + "}",
            _ => throw new NotSupportedException($"No canonical form for {array.Data.DataType}."),
        };
    }

    private static T Raw<T>(IArrowArray a, int i) where T : unmanaged =>
        MemoryMarshal.Cast<byte, T>(a.Data.Buffers[1].Span)[a.Data.Offset + i];

    private static int Index(IArrowArray indices, int i) => indices switch
    {
        Int8Array a => a.GetValue(i)!.Value,
        Int16Array a => a.GetValue(i)!.Value,
        Int32Array a => a.GetValue(i)!.Value,
        UInt32Array a => (int)a.GetValue(i)!.Value,
        Int64Array a => (int)a.GetValue(i)!.Value,
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
