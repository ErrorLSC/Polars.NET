using System.Globalization;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using static Seseragi.Files.Tests.TestBatches;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// Batches with a column of every type the Parquet writer writes (random values with their edge cases and nulls),
/// and a canonical text of each value that the batches written and the batches read back share: the reader returns
/// other but equivalent types (views for strings, days for date64, milliseconds for time32 seconds, decimal128 for
/// narrower decimals, month-day-nano for every interval).
/// </summary>
internal static class ParquetWriteData
{
    [StructLayout(LayoutKind.Sequential)]
    public readonly record struct DayTime(int Days, int Millis);

    [StructLayout(LayoutKind.Sequential)]
    public readonly record struct MonthDayNano(int Months, int Days, long Nanos);

    [StructLayout(LayoutKind.Sequential)]
    public readonly record struct Int256(Int128 Low, Int128 High);

    private static readonly string[] Words = ["", "a", "b", "apple", "Apfel", "雷", "😀x", "zz", new string('q', 70), "q" + new string('é', 40), "\u007f"];

    /// <summary>The columns of <see cref="Wide"/>: name, type, nullable.</summary>
    public static (string Name, IArrowType Type, bool Nullable)[] WideColumns =>
    [
        ("b", BooleanType.Default, true), ("i8", Int8Type.Default, true), ("i16", Int16Type.Default, true),
        ("i32", Int32Type.Default, true), ("i64", Int64Type.Default, true), ("u8", UInt8Type.Default, true),
        ("u16", UInt16Type.Default, true), ("u32", UInt32Type.Default, true), ("u64", UInt64Type.Default, true),
        ("f32", FloatType.Default, true), ("f64", DoubleType.Default, true), ("f16", HalfFloatType.Default, true),
        ("d32", Date32Type.Default, true), ("d64", Date64Type.Default, true),
        ("t32s", new Time32Type(TimeUnit.Second), true), ("t32ms", new Time32Type(TimeUnit.Millisecond), true),
        ("t64us", new Time64Type(TimeUnit.Microsecond), true), ("t64ns", new Time64Type(TimeUnit.Nanosecond), true),
        ("ts_s", new TimestampType(TimeUnit.Second, (string?)null), true), ("ts_ms_utc", new TimestampType(TimeUnit.Millisecond, "UTC"), true),
        ("ts_us_tokyo", new TimestampType(TimeUnit.Microsecond, "Asia/Tokyo"), true), ("ts_ns", new TimestampType(TimeUnit.Nanosecond, (string?)null), true),
        ("dur_s", DurationType.Second, true), ("dur_ns", DurationType.Nanosecond, true),
        ("dec32", new Decimal32Type(9, 2), true), ("dec64", new Decimal64Type(18, 4), true), ("dec5", new Decimal128Type(5, 1), true),
        ("dec15", new Decimal128Type(15, 3), true), ("dec30", new Decimal128Type(30, 5), true), ("dec60", new Decimal256Type(60, 10), true),
        ("str", StringType.Default, true), ("lstr", LargeStringType.Default, true), ("vstr", StringViewType.Default, true),
        ("bin", BinaryType.Default, true), ("lbin", LargeBinaryType.Default, true), ("vbin", BinaryViewType.Default, true),
        ("fsb", new FixedSizeBinaryType(3), true),
        ("dict_str", new DictionaryType(Int32Type.Default, StringType.Default, false), true),
        ("dict_i64", new DictionaryType(Int8Type.Default, Int64Type.Default, false), true),
        ("iv_ym", new IntervalType(IntervalUnit.YearMonth), true), ("iv_dt", new IntervalType(IntervalUnit.DayTime), true),
        ("iv_mdn", new IntervalType(IntervalUnit.MonthDayNanosecond), true),
        ("nul", NullType.Default, true), ("req_i64", Int64Type.Default, false), ("req_str", StringType.Default, false),
    ];

    /// <summary><paramref name="rows"/> random rows of every column of <see cref="WideColumns"/>; a fifth of the nullable values null.</summary>
    public static RecordBatch Wide(int seed, int rows)
    {
        var random = new Random(seed);
        List<IArrowArray> arrays = [];
        foreach (var (name, type, nullable) in WideColumns)
        {
            bool[] valid = [.. Enumerable.Range(0, rows).Select(_ => !nullable || random.Next(5) != 0)];
            arrays.Add(Column(type, valid, random));
        }
        var schema = new Schema([.. WideColumns.Select(c => new Field(c.Name, c.Type, c.Nullable))], null);
        return new RecordBatch(schema, arrays, rows);
    }

    private static T Pick<T>(Random r, T[] edges, Func<Random, T> any) => r.Next(4) == 0 ? edges[r.Next(edges.Length)] : any(r);

    private static T?[] Values<T>(bool[] valid, Random r, T[] edges, Func<Random, T> any) where T : unmanaged =>
        [.. valid.Select(v => v ? Pick(r, edges, any) : (T?)null)];

    private static IArrowArray Column(IArrowType type, bool[] valid, Random r)
    {
        long Long(Random x) => x.NextInt64(long.MinValue, long.MaxValue);
        switch (type)
        {
            case BooleanType:
            {
                var b = new BooleanArray.Builder();
                foreach (bool v in valid)
                {
                    if (v) b.Append(r.Next(2) == 0);
                    else b.AppendNull();
                }
                return b.Build();
            }
            case Int8Type: return Fixed(type, Values(valid, r, [sbyte.MinValue, sbyte.MaxValue, (sbyte)0, (sbyte)-1], x => (sbyte)x.Next(-128, 128)));
            case Int16Type: return Fixed(type, Values(valid, r, [short.MinValue, short.MaxValue, (short)0], x => (short)x.Next(-500, 500)));
            case Int32Type: return Fixed(type, Values(valid, r, [int.MinValue, int.MaxValue, 0, -1], x => x.Next(-1000, 1000)));
            case Int64Type: return Fixed(type, Values(valid, r, [long.MinValue, long.MaxValue, 0L, -1L], Long));
            case UInt8Type: return Fixed(type, Values(valid, r, [byte.MinValue, byte.MaxValue], x => (byte)x.Next(256)));
            case UInt16Type: return Fixed(type, Values(valid, r, [ushort.MinValue, ushort.MaxValue], x => (ushort)x.Next(65536)));
            case UInt32Type: return Fixed(type, Values(valid, r, [uint.MinValue, uint.MaxValue, 1u << 31], x => (uint)x.NextInt64(0, 1L << 32)));
            case UInt64Type: return Fixed(type, Values(valid, r, [ulong.MinValue, ulong.MaxValue, 1UL << 63], x => (ulong)Long(x)));
            case FloatType:
                return Fixed(type, Values(valid, r, [float.NaN, -0f, 0f, float.PositiveInfinity, float.NegativeInfinity, float.MaxValue, float.Epsilon], x => (float)(x.NextDouble() * 200 - 100)));
            case DoubleType:
                return Fixed(type, Values(valid, r, [double.NaN, -0d, 0d, double.PositiveInfinity, double.MinValue, double.Epsilon], x => x.NextDouble() * 2e6 - 1e6));
            case HalfFloatType:
                return Fixed(type, Values(valid, r, [Half.NaN, Half.NegativeZero, Half.Zero, Half.MaxValue, Half.NegativeInfinity], x => (Half)(x.NextDouble() * 100 - 50)));
            case Date32Type: return Fixed(type, Values(valid, r, [int.MinValue, int.MaxValue, 0], x => x.Next(-100_000, 100_000)));
            case Date64Type: return Fixed(type, Values(valid, r, [0L, -86_400_000L], x => x.Next(-100_000, 100_000) * 86_400_000L));
            case Time32Type { Unit: TimeUnit.Second }: return Fixed(type, Values(valid, r, [0, 86_399], x => x.Next(86_400)));
            case Time32Type: return Fixed(type, Values(valid, r, [0, 86_399_999], x => x.Next(86_400_000)));
            case Time64Type { Unit: TimeUnit.Microsecond }: return Fixed(type, Values(valid, r, [0L, 86_399_999_999L], x => x.NextInt64(86_400_000_000L)));
            case Time64Type: return Fixed(type, Values(valid, r, [0L, 86_399_999_999_999L], x => x.NextInt64(86_400_000_000_000L)));
            case TimestampType { Unit: TimeUnit.Second }:
                return Fixed(type, Values(valid, r, [0L, -1L, long.MaxValue / 1000, long.MinValue / 1000], x => x.NextInt64(-5_000_000_000L, 5_000_000_000L)));
            case TimestampType or DurationType: return Fixed(type, Values(valid, r, [0L, -1L, long.MaxValue, long.MinValue], Long));
            case Decimal32Type: return Fixed(type, Values(valid, r, [999_999_999, -999_999_999, 0], x => x.Next(-999_999_999, 999_999_999)));
            case Decimal64Type: return Fixed(type, Values(valid, r, [999_999_999_999_999_999L, -999_999_999_999_999_999L], x => x.NextInt64(-999_999_999_999_999_999L, 999_999_999_999_999_999L)));
            case Decimal128Type d:
            {
                Int128 most = Int128.Parse(new string('9', d.Precision), CultureInfo.InvariantCulture);
                return Fixed(type, Values(valid, r, [most, -most, Int128.Zero, Int128.NegativeOne],
                    x => most / x.NextInt64(1, long.MaxValue) * (x.Next(2) == 0 ? 1 : -1)));
            }
            case Decimal256Type d:
            {
                BigInteger most = BigInteger.Parse(new string('9', d.Precision), CultureInfo.InvariantCulture);
                return Fixed(type, Values(valid, r, [To256(most), To256(-most), To256(BigInteger.Zero)],
                    x => To256(most / (x.Next(1, 1_000_000) * (BigInteger)x.NextInt64(1, long.MaxValue)) * (x.Next(2) == 0 ? 1 : -1))));
            }
            case FixedSizeBinaryType f:
            {
                byte[] raw = new byte[valid.Length * f.ByteWidth];
                r.NextBytes(raw);
                return ArrowArrayFactory.BuildArray(new ArrayData(type, valid.Length, valid.Count(v => !v), 0, [Validity(valid), new ArrowBuffer(raw)]));
            }
            case StringType or LargeStringType or StringViewType:
            {
                string?[] values = [.. valid.Select(v => v ? Words[r.Next(Words.Length)] + (r.Next(3) == 0 ? r.Next(1000).ToString(CultureInfo.InvariantCulture) : "") : null)];
                return type switch
                {
                    StringType => Strings(values),
                    LargeStringType => Build(new LargeStringArray.Builder(), values, static (b, s) => b.Append(s), static b => b.AppendNull(), static b => b.Build()),
                    _ => Build(new StringViewArray.Builder(), values, static (b, s) => b.Append(s), static b => b.AppendNull(), static b => b.Build()),
                };
            }
            case BinaryType or LargeBinaryType or BinaryViewType:
            {
                byte[]?[] values = [.. valid.Select(v => v ? Bytes(r) : null)];
                return type switch
                {
                    BinaryType => Build(new BinaryArray.Builder(), values, static (b, s) => b.Append((ReadOnlySpan<byte>)s), static b => b.AppendNull(), static b => b.Build()),
                    LargeBinaryType => Build(new LargeBinaryArray.Builder(), values, static (b, s) => b.Append((ReadOnlySpan<byte>)s), static b => b.AppendNull(), static b => b.Build()),
                    _ => Build(new BinaryViewArray.Builder(), values, static (b, s) => b.Append((ReadOnlySpan<byte>)s), static b => b.AppendNull(), static b => b.Build()),
                };
            }
            case DictionaryType { ValueType: StringType } d:
            {
                // A null among the dictionary's values too: rows pointing at it are null.
                IArrowArray dictionary = Strings(["red", "green", null, "blue", ""]);
                int?[] indices = [.. valid.Select(v => v ? r.Next(5) : (int?)null)];
                return new DictionaryArray(d, Fixed(Int32Type.Default, indices), dictionary);
            }
            case DictionaryType d:
            {
                IArrowArray dictionary = Fixed(Int64Type.Default, [(long?)7, long.MinValue, 0, -3]);
                sbyte?[] indices = [.. valid.Select(v => v ? (sbyte)r.Next(4) : (sbyte?)null)];
                return new DictionaryArray(d, Fixed(Int8Type.Default, indices), dictionary);
            }
            case IntervalType { Unit: IntervalUnit.YearMonth }: return Fixed(type, Values(valid, r, [0, int.MaxValue], x => x.Next(0, 1000)));
            case IntervalType { Unit: IntervalUnit.DayTime }: return Fixed(type, Values(valid, r, [new DayTime(0, 0), new DayTime(int.MaxValue, int.MaxValue)], x => new DayTime(x.Next(1000), x.Next(86_400_000))));
            case IntervalType:
                return Fixed(type, Values(valid, r, [new MonthDayNano(0, 0, 0), new MonthDayNano(int.MaxValue, int.MaxValue, uint.MaxValue * 1_000_000L)],
                    x => new MonthDayNano(x.Next(100), x.Next(100), x.NextInt64(1_000_000) * 1_000_000)));
            case NullType: return new NullArray(valid.Length);
            default: throw new NotSupportedException(type.Name);
        }

        static byte[] Bytes(Random r)
        {
            byte[] b = new byte[r.Next(4) == 0 ? 80 : r.Next(6)];
            r.NextBytes(b);
            if (r.Next(8) == 0) b.AsSpan().Fill(0xFF);
            return b;
        }
    }

    /// <summary>Sorted columns over 8 row groups (so that bounds skip groups), with NaNs, zeros and long strings in them.</summary>
    /// <param name="nulls">Whether nulls (and NaNs, which leave a float chunk without bounds) are among the values.</param>
    public static RecordBatch Sorted(int rows, Random r, bool nulls = true)
    {
        int[] i32 = [.. Enumerable.Range(0, rows).Select(_ => r.Next(-10_000, 10_000)).Order()];
        uint[] u32 = [.. Enumerable.Range(0, rows).Select(_ => (uint)r.NextInt64(0, 1L << 32)).Order()];
        ulong[] u64 = [.. Enumerable.Range(0, rows).Select(_ => (ulong)r.NextInt64(long.MinValue, long.MaxValue)).Order()];
        double[] f64 = [.. Enumerable.Range(0, rows).Select(_ => r.Next(-50, 50) * 0.5).Order().Select(v => nulls && r.Next(40) == 0 ? double.NaN : v == 0 && r.Next(2) == 0 ? -0d : v)];
        Int128[] dec30 = [.. Enumerable.Range(0, rows).Select(_ => (Int128)r.NextInt64(-1_000_000, 1_000_000) * 1_000_000_000_000).Order()];
        int[] dec5 = [.. Enumerable.Range(0, rows).Select(_ => r.Next(-99_999, 100_000)).Order()];
        string[] str = [.. Enumerable.Range(0, rows).Select(_ => r.Next(3) == 0 ? new string('q', 70) + r.Next(100).ToString("D2") : "k" + r.Next(10_000).ToString("D4"))
            .Order(StringComparer.Ordinal)];
        int[] day = [.. Enumerable.Range(0, rows).Select(_ => r.Next(-20_000, 20_000)).Order()];
        bool[] valid = [.. Enumerable.Range(0, rows).Select(_ => !nulls || r.Next(10) != 0)];
        T?[] V<T>(T[] values) where T : unmanaged => [.. values.Select((v, i) => valid[i] ? v : (T?)null)];
        return Batch(
            ("i32", Fixed(Int32Type.Default, V(i32)), true), ("u32", Fixed(UInt32Type.Default, V(u32)), true),
            ("u64", Fixed(UInt64Type.Default, V(u64)), true), ("f64", Fixed(DoubleType.Default, V(f64)), true),
            ("dec30", Fixed(new Decimal128Type(30, 5), V(dec30)), true), ("dec5", Fixed(new Decimal128Type(5, 1), [.. V(dec5).Select(v => (Int128?)v)]), true),
            ("str", Strings([.. str.Select((v, i) => valid[i] ? v : null)]), true), ("day", Fixed(Date32Type.Default, V(day)), true));
    }

    public static Int256 To256(BigInteger value)
    {
        byte[] bytes = new byte[32];
        value.TryWriteBytes(bytes, out _, isUnsigned: false, isBigEndian: false);
        if (value.Sign < 0)
        {
            int used = value.GetByteCount();
            bytes.AsSpan(used).Fill(0xFF);
        }
        return MemoryMarshal.Read<Int256>(bytes);
    }

    private static IArrowArray Build<TBuilder, TValue>(TBuilder builder, TValue?[] values, Action<TBuilder, TValue> append, Action<TBuilder> appendNull, Func<TBuilder, IArrowArray> build)
        where TValue : class
    {
        foreach (TValue? v in values)
        {
            if (v is null) appendNull(builder);
            else append(builder, v);
        }
        return build(builder);
    }

    // ------------------------------------------------------------------
    // Canonical values
    // ------------------------------------------------------------------

    /// <summary>The values of a column as canonical text, row by row.</summary>
    public static string[] Canonical(IArrowArray array) => [.. Enumerable.Range(0, array.Length).Select(i => Canonical(array, i))];

    /// <summary>A value as text that equivalent types share ("null" for null).</summary>
    public static string Canonical(IArrowArray a, int i)
    {
        if (a is DictionaryArray d)
        {
            if (d.IsNull(i)) return "null";
            int index = d.Indices switch
            {
                Int8Array x => x.Values[i],
                Int16Array x => x.Values[i],
                Int32Array x => x.Values[i],
                UInt8Array x => x.Values[i],
                UInt16Array x => x.Values[i],
                UInt32Array x => (int)x.Values[i],
                _ => (int)((Int64Array)d.Indices).Values[i],
            };
            return Canonical(d.Dictionary, index);
        }
        if (a.IsNull(i)) return "null";
        T V<T>() where T : unmanaged => MemoryMarshal.Cast<byte, T>(a.Data.Buffers[1].Span).Slice(a.Data.Offset, a.Length)[i];
        string Hex(ReadOnlySpan<byte> b) => "b:" + Convert.ToHexString(b);
        IArrowType type = a.Data.DataType;
        return type switch
        {
            BooleanType => ((BooleanArray)a).GetValue(i)!.Value ? "true" : "false",
            Int8Type => V<sbyte>().ToString(CultureInfo.InvariantCulture),
            Int16Type => V<short>().ToString(CultureInfo.InvariantCulture),
            Int32Type => V<int>().ToString(CultureInfo.InvariantCulture),
            Int64Type => V<long>().ToString(CultureInfo.InvariantCulture),
            UInt8Type => V<byte>().ToString(CultureInfo.InvariantCulture),
            UInt16Type => V<ushort>().ToString(CultureInfo.InvariantCulture),
            UInt32Type => V<uint>().ToString(CultureInfo.InvariantCulture),
            UInt64Type => V<ulong>().ToString(CultureInfo.InvariantCulture),
            FloatType => "f" + BitConverter.SingleToInt32Bits(V<float>()).ToString("X8", CultureInfo.InvariantCulture),
            DoubleType => "d" + BitConverter.DoubleToInt64Bits(V<double>()).ToString("X16", CultureInfo.InvariantCulture),
            HalfFloatType => "f" + BitConverter.SingleToInt32Bits((float)V<Half>()).ToString("X8", CultureInfo.InvariantCulture), // as a float: readers may widen it
            Date32Type => "day" + V<int>().ToString(CultureInfo.InvariantCulture),
            Date64Type => "day" + (V<long>() / 86_400_000).ToString(CultureInfo.InvariantCulture),
            Time32Type t => "t" + ((long)V<int>() * Nanos(t.Unit)).ToString(CultureInfo.InvariantCulture),
            Time64Type t => "t" + ((BigInteger)V<long>() * Nanos(t.Unit)).ToString(CultureInfo.InvariantCulture),
            TimestampType t => $"ts{(BigInteger)V<long>() * Nanos(t.Unit)}{(string.IsNullOrEmpty(t.Timezone) ? "" : "Z")}",
            DurationType t => "dur" + ((BigInteger)V<long>() * Nanos(t.Unit)).ToString(CultureInfo.InvariantCulture),
            Decimal32Type x => Dec(V<int>(), x.Scale),
            Decimal64Type x => Dec(V<long>(), x.Scale),
            Decimal128Type x => Dec((BigInteger)V<Int128>(), x.Scale),
            Decimal256Type x => Dec(new BigInteger(a.Data.Buffers[1].Span.Slice((a.Data.Offset + i) * 32, 32), isUnsigned: false, isBigEndian: false), x.Scale),
            FixedSizeBinaryType f => Hex(a.Data.Buffers[1].Span.Slice((a.Data.Offset + i) * f.ByteWidth, f.ByteWidth)),
            StringType => "s:" + ((StringArray)a).GetString(i),
            LargeStringType => "s:" + ((LargeStringArray)a).GetString(i),
            StringViewType => "s:" + ((StringViewArray)a).GetString(i),
            BinaryType => Hex(((BinaryArray)a).GetBytes(i)),
            LargeBinaryType => Hex(((LargeBinaryArray)a).GetBytes(i)),
            BinaryViewType => Hex(((BinaryViewArray)a).GetBytes(i)),
            IntervalType { Unit: IntervalUnit.YearMonth } => $"iv{V<int>()},0,0",
            IntervalType { Unit: IntervalUnit.DayTime } => $"iv0,{V<DayTime>().Days},{V<DayTime>().Millis * 1_000_000L}",
            IntervalType => $"iv{V<MonthDayNano>().Months},{V<MonthDayNano>().Days},{V<MonthDayNano>().Nanos}",
            _ => throw new NotSupportedException(type.Name),
        };

        static string Dec(BigInteger unscaled, int scale) => $"{unscaled}e-{scale}";
    }

    private static long Nanos(TimeUnit unit) => unit switch
    {
        TimeUnit.Second => 1_000_000_000,
        TimeUnit.Millisecond => 1_000_000,
        TimeUnit.Microsecond => 1_000,
        _ => 1,
    };

    /// <summary>The canonical values of every column of the batches, column by column.</summary>
    public static List<string>[] Columns(IEnumerable<RecordBatch> batches, int columns)
    {
        List<string>[] result = [.. Enumerable.Range(0, columns).Select(_ => (List<string>)[])];
        foreach (RecordBatch batch in batches)
        {
            for (int c = 0; c < columns; c++) result[c].AddRange(Canonical(batch.Column(c)));
        }
        return result;
    }

    public static string Utf8Hex(string s) => Convert.ToHexString(Encoding.UTF8.GetBytes(s));
}
