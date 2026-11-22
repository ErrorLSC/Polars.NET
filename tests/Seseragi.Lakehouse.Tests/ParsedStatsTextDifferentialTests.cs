using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Seseragi.Lakehouse.Tests;

/// <summary>
/// <see cref="ParsedStatsText"/> (the text of <c>add.stats_parsed</c> written byte by byte) against
/// <see cref="JsonWriterParsedStatsText"/> (written with a Utf8JsonWriter, as it was) as the oracle: the same bytes for
/// every row of random statistics structs (every type a checkpoint's statistics hold, nulls at every level, nested
/// structs, values at the edges: non-finite floats, wide decimals, dates and timestamps outside years 1 to 9999,
/// strings the encoder escapes), INT96 instants. Rows of unsliced arrays only: the oracle read a sliced struct wrongly
/// (it added the struct's offset to fields Apache.Arrow slices too); slices are <see cref="CheckpointSliceTests"/>'.
/// </summary>
public class ParsedStatsTextDifferentialTests
{
    private static readonly StructType s_bounds = new([
        new("i8", Int8Type.Default, true), new("i16", Int16Type.Default, true), new("i32", Int32Type.Default, true),
        new("i64", Int64Type.Default, true), new("f", FloatType.Default, true), new("d", DoubleType.Default, true),
        new("dec", new Decimal128Type(10, 2), true), new("dec38", new Decimal128Type(38, 4), true), new("dec0", new Decimal128Type(5, 0), true),
        new("dt", Date32Type.Default, true), new("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), true),
        new("tsn", new TimestampType(TimeUnit.Microsecond, (string?)null), true), new("tms", new TimestampType(TimeUnit.Millisecond, (string?)null), true),
        new("tsec", new TimestampType(TimeUnit.Second, "UTC"), true), new("tns", new TimestampType(TimeUnit.Nanosecond, (string?)null), true),
        new("b", BooleanType.Default, true), new("s", StringType.Default, true), new("bin", BinaryType.Default, true),
        new("n\"é", new StructType([new("x", Int32Type.Default, true), new("s", StringType.Default, true)]), true),
    ]);

    private static readonly StructType s_nullCounts = new([
        new("i32", Int64Type.Default, true), new("s", Int64Type.Default, true), new("arr", Int64Type.Default, true),
        new("n\"é", new StructType([new("x", Int64Type.Default, true)]), true),
    ]);

    private static readonly StructType s_stats = new([
        new("numRecords", Int64Type.Default, true), new("minValues", s_bounds, true), new("maxValues", s_bounds, true),
        new("nullCount", s_nullCounts, true), new("tightBounds", BooleanType.Default, true),
    ]);

    [Theory]
    [InlineData(1, false)]
    [InlineData(2, true)]
    public void EveryRow_IsWrittenAsTheUtf8JsonWriterWroteIt(int seed, bool instants)
    {
        var random = new Random(seed);
        const int Rows = 3000;
        StructArray stats = (StructArray)Make(random, s_stats, Rows, 0);
        HashSet<string> instantLeaves = instants ? ["minValues\0tsn", "maxValues\0tms"] : [];
        var oracle = new JsonWriterParsedStatsText(instantLeaves);
        using var text = new ParsedStatsText(instantLeaves);
        var arena = new ByteArena();
        Same(stats, Rows);

        void Same(StructArray array, int rows)
        {
            for (int i = 0; i < rows; i++)
            {
                byte[]? expected = oracle.Utf8(array, i);
                ReadOnlyMemory<byte>? actual = text.Utf8(array, i, arena);
                Assert.Equal(expected is null, actual is null);
                if (expected is not null)
                    Assert.True(expected.AsSpan().SequenceEqual(actual!.Value.Span),
                        $"row {i}\n oracle: {System.Text.Encoding.UTF8.GetString(expected)}\n ours:   {System.Text.Encoding.UTF8.GetString(actual!.Value.Span)}");
            }
        }
    }

    // ------------------------------------------------------------------ random arrays

    private static IArrowArray Make(Random random, IArrowType type, int rows, int depth)
    {
        bool Null() => random.Next(7) == 0;
        switch (type)
        {
            case StructType s:
            {
                IArrowArray[] children = [.. s.Fields.Select(f => Make(random, f.DataType, rows, depth + 1))];
                var validity = new ArrowBuffer.BitmapBuilder();
                int nulls = 0;
                for (int i = 0; i < rows; i++)
                {
                    bool valid = depth == 0 ? random.Next(20) != 0 : !Null();
                    validity.Append(valid);
                    if (!valid) nulls++;
                }
                return new StructArray(s, rows, children, validity.Build(), nulls);
            }
            case Int8Type:
            {
                var b = new Int8Array.Builder();
                for (int i = 0; i < rows; i++) { if (Null()) b.AppendNull(); else b.Append((sbyte)random.Next(-128, 128)); }
                return b.Build();
            }
            case Int16Type:
            {
                var b = new Int16Array.Builder();
                for (int i = 0; i < rows; i++) { if (Null()) b.AppendNull(); else b.Append((short)random.Next(-32768, 32768)); }
                return b.Build();
            }
            case Int32Type:
            {
                var b = new Int32Array.Builder();
                for (int i = 0; i < rows; i++) { if (Null()) b.AppendNull(); else b.Append(random.Next(4) == 0 ? int.MinValue + random.Next(2) : random.Next(int.MinValue, int.MaxValue)); }
                return b.Build();
            }
            case Int64Type:
            {
                var b = new Int64Array.Builder();
                for (int i = 0; i < rows; i++) { if (Null()) b.AppendNull(); else b.Append(random.Next(5) == 0 ? long.MinValue + random.Next(2) : random.NextInt64(long.MinValue, long.MaxValue) >> random.Next(64)); }
                return b.Build();
            }
            case FloatType:
            {
                float[] edges = [float.NaN, float.PositiveInfinity, float.NegativeInfinity, -0f, float.Epsilon, float.MaxValue, float.MinValue, 1e-7f, 0.1f, 3.4028235e38f, 16777217f];
                var b = new FloatArray.Builder();
                for (int i = 0; i < rows; i++)
                {
                    if (Null()) b.AppendNull();
                    else b.Append(random.Next(3) == 0 ? edges[random.Next(edges.Length)] : (float)(random.NextDouble() * Math.Pow(10, random.Next(-20, 20)) * (random.Next(2) * 2 - 1)));
                }
                return b.Build();
            }
            case DoubleType:
            {
                double[] edges = [double.NaN, double.PositiveInfinity, double.NegativeInfinity, -0.0, double.Epsilon, double.MaxValue, double.MinValue, 1e-300, 0.1, 1e21, 123456789012345678];
                var b = new DoubleArray.Builder();
                for (int i = 0; i < rows; i++)
                {
                    if (Null()) b.AppendNull();
                    else b.Append(random.Next(3) == 0 ? edges[random.Next(edges.Length)] : random.NextDouble() * Math.Pow(10, random.Next(-300, 300)) * (random.Next(2) * 2 - 1));
                }
                return b.Build();
            }
            case Decimal128Type d:
            {
                var b = new Decimal128Array.Builder(d);
                for (int i = 0; i < rows; i++)
                {
                    if (Null()) { b.AppendNull(); continue; }
                    // Values within decimal's range and the type's precision.
                    decimal scaleDown = (decimal)Math.Pow(10, d.Scale);
                    decimal value = d.Precision <= 18
                        ? random.NextInt64(-(long)Math.Pow(10, d.Precision) + 1, (long)Math.Pow(10, d.Precision)) / scaleDown
                        : random.NextInt64(long.MinValue, long.MaxValue) * (decimal)random.Next(1, 1000) / scaleDown;
                    b.Append(value);
                }
                return b.Build();
            }
            case Date32Type:
            {
                var b = new Date32Array.Builder();
                for (int i = 0; i < rows; i++)
                {
                    if (Null()) b.AppendNull();
                    else b.Append(DateOnly.FromDayNumber(random.Next(0, 3_652_059)));
                }
                // Days outside years 1 to 9999 cannot be appended as dates: a second array holds raw day numbers.
                return random.Next(2) == 0 ? b.Build() : RawDays(random, rows);
            }
            case TimestampType t:
            {
                // Raw values in the type's unit, some far outside years 1 to 9999.
                long span = t.Unit switch { TimeUnit.Second => 400_000_000_000L, TimeUnit.Millisecond => 400_000_000_000_000L, TimeUnit.Microsecond => long.MaxValue / 4, _ => long.MaxValue };
                var values = new ArrowBuffer.Builder<long>();
                var validity = new ArrowBuffer.BitmapBuilder();
                int nulls = 0;
                for (int i = 0; i < rows; i++)
                {
                    bool valid = !Null();
                    // Around the edges of years 1 to 9999 (in milliseconds -62 135 596 800 000 and 253 402 300 799 999) or anywhere.
                    long perMilli = t.Unit switch { TimeUnit.Second => 0, TimeUnit.Millisecond => 1, TimeUnit.Microsecond => 1000, _ => 1_000_000 };
                    long edge = s_edgeMillis[random.Next(s_edgeMillis.Length)];
                    values.Append(random.Next(5) == 0 && perMilli > 0 ? edge * perMilli
                        : random.Next(10) == 0 ? (random.Next(2) == 0 ? long.MaxValue : long.MinValue + 1) : random.NextInt64(-span, span));
                    validity.Append(valid);
                    if (!valid) nulls++;
                }
                return new TimestampArray(t, values.Build(), validity.Build(), rows, nulls, 0);
            }
            case BooleanType:
            {
                var b = new BooleanArray.Builder();
                for (int i = 0; i < rows; i++) { if (Null()) b.AppendNull(); else b.Append(random.Next(2) == 0); }
                return b.Build();
            }
            case StringType:
            {
                string[] pieces = ["a", "Z", "0", " ", "\"", "\\", "\n", "\t", "\u0001", "\u007f", "é", "中", "😀", "<", ">", "&", "'", "+", "/", "\u2028"];
                var b = new StringArray.Builder();
                for (int i = 0; i < rows; i++)
                {
                    if (Null()) { b.AppendNull(); continue; }
                    b.Append(string.Concat(Enumerable.Range(0, random.Next(0, 6)).Select(_ => random.Next(3) == 0 ? pieces[random.Next(pieces.Length)] : "abc")));
                }
                return b.Build();
            }
            case BinaryType:
            {
                var b = new BinaryArray.Builder();
                for (int i = 0; i < rows; i++) { if (Null()) b.AppendNull(); else b.Append([1, 2, 3]); }
                return b.Build();
            }
            default:
                throw new NotSupportedException(type.Name);
        }
    }

    private static readonly int[] s_edgeDays = [-719_163, -719_162, 2_932_896, 2_932_897];
    private static readonly long[] s_edgeMillis = [-62_135_596_800_001, -62_135_596_800_000, 253_402_300_799_999, 253_402_300_800_000];

    // Day numbers from far before year 1 to far after 9999.
    private static Date32Array RawDays(Random random, int rows)
    {
        var values = new ArrowBuffer.Builder<int>();
        var validity = new ArrowBuffer.BitmapBuilder();
        int nulls = 0;
        for (int i = 0; i < rows; i++)
        {
            bool valid = random.Next(7) != 0;
            // Around the edges of years 1 to 9999 (days from 1970: -719 162 is 0001-01-01, 2 932 896 is 9999-12-31) or anywhere.
            values.Append(random.Next(3) == 0 ? s_edgeDays[random.Next(s_edgeDays.Length)] : random.Next(-1_000_000, 4_000_000));
            validity.Append(valid);
            if (!valid) nulls++;
        }
        return new Date32Array(values.Build(), validity.Build(), rows, nulls, 0);
    }
}
