using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Kaminari.Sazanami.Buffers;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Kaminari.Seseragi.Lakehouse.Delta.Actions;

/// <summary>
/// The JSON text of a checkpoint's <c>add.stats_parsed</c> struct, in the form delta-spark writes <c>add.stats</c>
/// in a commit, for checkpoints that keep the statistics only as a struct (<c>delta.checkpoint.writeStatsAsJson</c>
/// false). The text is the file's statistics from then on, and is written back to the log.
/// </summary>
/// <remarks>
/// A null field is left out (as Spark's <c>to_json</c> does). Numbers stay numbers, decimals keep their scale, non-finite
/// floats become the strings <c>NaN</c>, <c>Infinity</c>, <c>-Infinity</c>; dates are <c>yyyy-MM-dd</c>; timestamps
/// are cut to milliseconds, as Spark writes them: an instant ends in <c>Z</c>, a local date-time (timestamp_ntz) has
/// no zone. Types that have no bounds (binaries, lists, maps) are left out.
/// </remarks>
/// <param name="instantLeaves">
/// The paths (inside <c>stats_parsed</c>, joined with <c>\0</c>) of leaves stored as INT96: zoneless in Arrow, yet
/// instants (Spark's timestamps).
/// </param>
internal sealed class ParsedStatsText(IReadOnlySet<string> instantLeaves) : IDisposable
{
    private static readonly int EpochDay = new DateOnly(1970, 1, 1).DayNumber;

    /// <summary>The bytes a string value cannot be copied with (the log's encoder may escape them).</summary>
    private static readonly SearchValues<byte> Escaped =
        SearchValues.Create([.. Enumerable.Range(0, 0x20).Select(static b => (byte)b), (byte)'"', (byte)'\\', .. Enumerable.Range(0x7F, 0x81).Select(static b => (byte)b)]);

    private readonly NativeBufferWriter _out = new();
    private StructArray? _planned;
    private Column[] _plan = [];

    /// <summary>How a field is written.</summary>
    private enum Kind : byte { Struct, Boolean, Int8, Int16, Int32, Int64, Float, Double, Decimal, Date, Timestamp, String, None }

    /// <summary>A field of the struct as written: its name ready for JSON (<c>"name":</c>), its values, its own fields.</summary>
    private sealed record Column(byte[] Name, Kind Kind, IArrowArray Values, bool Instant, Column[] Children);

    /// <summary>
    /// The statistics text of row <paramref name="index"/> of the struct as UTF-8, kept in <paramref name="texts"/>; null
    /// when the struct is null there. Written byte by byte, as a <see cref="Utf8JsonWriter"/> with the log's options
    /// writes it; the fields are resolved once per struct array (a batch).
    /// </summary>
    public ReadOnlyMemory<byte>? Utf8(StructArray stats, int index, ByteArena texts)
    {
        if (stats.IsNull(index)) return null;
        if (!ReferenceEquals(stats, _planned))
        {
            _plan = Plan(stats, "");
            _planned = stats;
        }
        _out.ResetWrittenCount();
        WriteStruct(_plan, index); // a struct's fields are sliced as it is: the row is their index too
        return texts.Add(_out.WrittenSpan);
    }

    public void Dispose() => _out.Dispose();

    private Column[] Plan(StructArray array, string path)
    {
        var type = (StructType)array.Data.DataType;
        var columns = new Column[type.Fields.Count];
        for (int f = 0; f < columns.Length; f++)
        {
            Apache.Arrow.Field field = type.Fields[f];
            string at = path.Length == 0 ? field.Name : $"{path}\0{field.Name}";
            IArrowArray values = array.Fields[f];
            Kind kind = values switch
            {
                StructArray => Kind.Struct,
                BooleanArray => Kind.Boolean,
                Int8Array => Kind.Int8,
                Int16Array => Kind.Int16,
                Int32Array => Kind.Int32,
                Int64Array => Kind.Int64,
                FloatArray => Kind.Float,
                DoubleArray => Kind.Double,
                Decimal128Array => Kind.Decimal,
                Date32Array => Kind.Date,
                TimestampArray => Kind.Timestamp,
                StringArray => Kind.String,
                _ => Kind.None, // binaries, lists and maps have no bounds; a null count of theirs is an Int64 above
            };
            byte[] encoded = JsonEncodedText.Encode(field.Name, ActionWriter.JsonOptions.Encoder).EncodedUtf8Bytes.ToArray();
            columns[f] = new Column([(byte)'"', .. encoded, (byte)'"', (byte)':'], kind, values, instantLeaves.Contains(at),
                values is StructArray nested ? Plan(nested, at) : []);
        }
        return columns;
    }

    private void WriteStruct(Column[] columns, int child)
    {
        Put("{"u8);
        bool first = true;
        Span<byte> text = stackalloc byte[48];
        foreach (Column column in columns)
        {
            if (column.Kind == Kind.None || column.Values.IsNull(child)) continue;
            int length = 0;
            // A date or timestamp outside years 1 to 9999 writes nothing, its name neither.
            if (column.Kind == Kind.Date)
            {
                int days = ((Date32Array)column.Values).GetValue(child)!.Value;
                if ((long)EpochDay + days is not (>= 0 and <= 3_652_058)) continue; // DateOnly's range: 0001-01-01 to 9999-12-31
                DateOnly.FromDayNumber(EpochDay + days).Deconstruct(out int year, out int month, out int day);
                length = FormatDate(year, month, day, text);
            }
            else if (column.Kind == Kind.Timestamp && (length = TimestampText((TimestampArray)column.Values, child, column.Instant, text)) == 0) continue;
            if (!first) Put(","u8);
            first = false;
            Put(column.Name);
            switch (column.Kind)
            {
                case Kind.Struct:
                    var nested = (StructArray)column.Values;
                    WriteStruct(column.Children, child);
                    break;
                case Kind.Boolean:
                    Put(((BooleanArray)column.Values).GetValue(child)!.Value ? "true"u8 : "false"u8);
                    break;
                case Kind.Int8:
                    Number(((Int8Array)column.Values).GetValue(child)!.Value);
                    break;
                case Kind.Int16:
                    Number(((Int16Array)column.Values).GetValue(child)!.Value);
                    break;
                case Kind.Int32:
                    Number(((Int32Array)column.Values).GetValue(child)!.Value);
                    break;
                case Kind.Int64:
                    Number(((Int64Array)column.Values).GetValue(child)!.Value);
                    break;
                case Kind.Float:
                    float f = ((FloatArray)column.Values).GetValue(child)!.Value;
                    if (float.IsFinite(f)) Number(f);
                    else Put(NonFinite(f));
                    break;
                case Kind.Double:
                    double d = ((DoubleArray)column.Values).GetValue(child)!.Value;
                    if (double.IsFinite(d)) Number(d);
                    else Put(NonFinite(d));
                    break;
                case Kind.Decimal:
                    Put(text[..DecimalText((Decimal128Array)column.Values, child, text)]);
                    break;
                case Kind.Date or Kind.Timestamp:
                    Put("\""u8);
                    Put(text[..length]);
                    Put("\""u8);
                    break;
                case Kind.String:
                    String(((StringArray)column.Values).GetBytes(child));
                    break;
            }
        }
        Put("}"u8);
    }

    private void Put(scoped ReadOnlySpan<byte> bytes)
    {
        bytes.CopyTo(_out.GetSpan(bytes.Length));
        _out.Advance(bytes.Length);
    }

    private void Number<T>(T value) where T : IUtf8SpanFormattable
    {
        Span<byte> span = _out.GetSpan(64);
        if (!value.TryFormat(span, out int written, default, CultureInfo.InvariantCulture)) throw new InvalidOperationException("A number did not fit.");
        _out.Advance(written);
    }

    // A string value, quoted, escaped as the log's encoder escapes it (most values need no escape: copied).
    private void String(ReadOnlySpan<byte> utf8)
    {
        Put("\""u8);
        if (utf8.ContainsAny(Escaped)) Put(JsonEncodedText.Encode(utf8, ActionWriter.JsonOptions.Encoder).EncodedUtf8Bytes);
        else Put(utf8);
        Put("\""u8);
    }

    private static ReadOnlySpan<byte> NonFinite(double value) =>
        double.IsNaN(value) ? "\"NaN\""u8 : value > 0 ? "\"Infinity\""u8 : "\"-Infinity\""u8;

    /// <summary>The decimal as JSON number text with all its scale digits (<c>-1.50</c>), as Spark writes a BigDecimal.</summary>
    private static int DecimalText(Decimal128Array array, int i, Span<byte> text)
    {
        int scale = ((Decimal128Type)array.Data.DataType).Scale;
        Int128 unscaled = BinaryPrimitives.ReadInt128LittleEndian(array.ValueBuffer.Span.Slice((array.Offset + i) * 16, 16));
        UInt128 magnitude = unscaled < 0 ? (UInt128)(-unscaled) : (UInt128)unscaled;
        int sign = unscaled < 0 ? 1 : 0;
        Span<byte> digits = stackalloc byte[40];
        magnitude.TryFormat(digits, out int count, default, CultureInfo.InvariantCulture);
        // Leading zeros so that one digit stands before the point.
        int pad = Math.Max(0, scale + 1 - count);
        int whole = count + pad - scale;
        int at = 0;
        if (sign == 1) text[at++] = (byte)'-';
        for (int k = 0; k < count + pad; k++)
        {
            if (k == whole) text[at++] = (byte)'.';
            text[at++] = k < pad ? (byte)'0' : digits[k - pad];
        }
        return at;
    }

    /// <summary>Writes <c>yyyy-MM-dd</c>; returns its length.</summary>
    private static int FormatDate(int year, int month, int day, Span<byte> text)
    {
        int at = Digits(text, 0, year, 4);
        text[at++] = (byte)'-';
        at = Digits(text, at, month, 2);
        text[at++] = (byte)'-';
        return Digits(text, at, day, 2);
    }

    /// <summary>Writes a non-negative number in exactly <paramref name="count"/> digits at <paramref name="at"/>; returns the end.</summary>
    private static int Digits(Span<byte> text, int at, int value, int count)
    {
        for (int k = at + count - 1; k >= at; k--)
        {
            text[k] = (byte)('0' + value % 10);
            value /= 10;
        }
        return at + count;
    }

    /// <summary>A timestamp cut to milliseconds as Spark writes it; 0 when it is outside years 1 to 9999.</summary>
    private static int TimestampText(TimestampArray array, int i, bool int96, Span<byte> text)
    {
        var type = (TimestampType)array.Data.DataType;
        long raw = array.GetValue(i)!.Value;
        long millis = type.Unit switch
        {
            TimeUnit.Second => raw is > long.MaxValue / 1000 or < long.MinValue / 1000 ? long.MinValue : raw * 1000,
            TimeUnit.Millisecond => raw,
            TimeUnit.Microsecond => FloorDiv(raw, 1000),
            _ => FloorDiv(raw, 1_000_000),
        };
        const long Min = -62_135_596_800_000; // 0001-01-01T00:00:00Z
        const long Max = 253_402_300_799_999; // 9999-12-31T23:59:59.999Z
        if (millis is < Min or > Max) return 0;
        DateTime time = DateTime.UnixEpoch.AddTicks(millis * TimeSpan.TicksPerMillisecond);
        time.Deconstruct(out int year, out int month, out int day);
        // yyyy-MM-ddTHH:mm:ss.fff, then Z for an instant.
        int at = FormatDate(year, month, day, text);
        long ofDay = millis - FloorDiv(millis, 86_400_000) * 86_400_000;
        text[at++] = (byte)'T';
        at = Digits(text, at, (int)(ofDay / 3_600_000), 2);
        text[at++] = (byte)':';
        at = Digits(text, at, (int)(ofDay / 60_000 % 60), 2);
        text[at++] = (byte)':';
        at = Digits(text, at, (int)(ofDay / 1000 % 60), 2);
        text[at++] = (byte)'.';
        at = Digits(text, at, (int)(ofDay % 1000), 3);
        if (!string.IsNullOrEmpty(type.Timezone) || int96) text[at++] = (byte)'Z';
        return at;

        static long FloorDiv(long value, long divisor) => Math.DivRem(value, divisor) is var (q, r) && r < 0 ? q - 1 : q;
    }
}
