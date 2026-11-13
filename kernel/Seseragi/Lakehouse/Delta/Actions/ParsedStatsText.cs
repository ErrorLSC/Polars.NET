using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using System.Text.Json;
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
internal sealed class ParsedStatsText(IReadOnlySet<string> instantLeaves)
{
    private static readonly int EpochDay = new DateOnly(1970, 1, 1).DayNumber;

    private readonly ArrayBufferWriter<byte> _buffer = new();
    private Utf8JsonWriter? _json;
    private StructType? _planType;
    private Field[] _plan = [];

    /// <summary>A struct field to write: its name ready for JSON, whether a timestamp in it is an instant, its own fields.</summary>
    private sealed record Field(JsonEncodedText Name, bool Instant, Field[] Children);

    /// <summary>The statistics text of row <paramref name="index"/> of the struct; null when the struct is null there.</summary>
    public string? Text(StructArray stats, int index)
    {
        if (stats.IsNull(index)) return null;
        var type = (StructType)stats.Data.DataType;
        if (!ReferenceEquals(type, _planType))
        {
            _plan = Plan(type, "");
            _planType = type;
        }
        _buffer.ResetWrittenCount();
        if (_json is null) _json = new Utf8JsonWriter(_buffer, ActionWriter.JsonOptions);
        else _json.Reset(_buffer);
        WriteStruct(_json, _plan, stats, index);
        _json.Flush();
        return Encoding.UTF8.GetString(_buffer.WrittenSpan);
    }

    private Field[] Plan(StructType type, string path) =>
    [
        .. type.Fields.Select(f =>
        {
            string at = path.Length == 0 ? f.Name : $"{path}\0{f.Name}";
            return new Field(JsonEncodedText.Encode(f.Name, ActionWriter.JsonOptions.Encoder), instantLeaves.Contains(at),
                f.DataType is StructType nested ? Plan(nested, at) : []);
        }),
    ];

    private static void WriteStruct(Utf8JsonWriter json, Field[] fields, StructArray array, int index)
    {
        int child = array.Offset + index;
        json.WriteStartObject();
        for (int f = 0; f < fields.Length; f++)
        {
            IArrowArray values = array.Fields[f];
            if (!values.IsNull(child)) WriteField(json, fields[f], values, child);
        }
        json.WriteEndObject();
    }

    private static void WriteField(Utf8JsonWriter json, Field field, IArrowArray values, int i)
    {
        JsonEncodedText name = field.Name;
        Span<char> text = stackalloc char[48];
        switch (values)
        {
            case StructArray s:
                json.WritePropertyName(name);
                WriteStruct(json, field.Children, s, i);
                break;
            case BooleanArray b:
                json.WriteBoolean(name, b.GetValue(i)!.Value);
                break;
            case Int8Array n:
                json.WriteNumber(name, n.GetValue(i)!.Value);
                break;
            case Int16Array n:
                json.WriteNumber(name, n.GetValue(i)!.Value);
                break;
            case Int32Array n:
                json.WriteNumber(name, n.GetValue(i)!.Value);
                break;
            case Int64Array n:
                json.WriteNumber(name, n.GetValue(i)!.Value);
                break;
            case FloatArray n:
                float f = n.GetValue(i)!.Value;
                if (float.IsFinite(f)) json.WriteNumber(name, f);
                else json.WriteString(name, NonFinite(f));
                break;
            case DoubleArray n:
                double d = n.GetValue(i)!.Value;
                if (double.IsFinite(d)) json.WriteNumber(name, d);
                else json.WriteString(name, NonFinite(d));
                break;
            case Decimal128Array n:
                json.WritePropertyName(name);
                json.WriteRawValue(text[..DecimalText(n, i, text)], skipInputValidation: true);
                break;
            case Date32Array n:
                int days = n.GetValue(i)!.Value;
                if ((long)EpochDay + days is >= 0 and <= 3_652_058) // DateOnly's range: 0001-01-01 to 9999-12-31
                {
                    DateOnly.FromDayNumber(EpochDay + days).Deconstruct(out int year, out int month, out int day);
                    json.WriteString(name, text[..FormatDate(year, month, day, text)]);
                }
                break;
            case TimestampArray n:
                int length = TimestampText(n, i, field.Instant, text);
                if (length > 0) json.WriteString(name, text[..length]);
                break;
            case StringArray n:
                json.WriteString(name, n.GetBytes(i));
                break;
            // Binaries, lists and maps have no bounds; a null count of theirs is an Int64 above.
        }
    }

    private static string NonFinite(double value) => double.IsNaN(value) ? "NaN" : value > 0 ? "Infinity" : "-Infinity";

    /// <summary>The decimal as JSON number text with all its scale digits (<c>-1.50</c>), as Spark writes a BigDecimal.</summary>
    private static int DecimalText(Decimal128Array array, int i, Span<char> text)
    {
        int scale = ((Decimal128Type)array.Data.DataType).Scale;
        Int128 unscaled = BinaryPrimitives.ReadInt128LittleEndian(array.ValueBuffer.Span.Slice((array.Offset + i) * 16, 16));
        UInt128 magnitude = unscaled < 0 ? (UInt128)(-unscaled) : (UInt128)unscaled;
        int sign = unscaled < 0 ? 1 : 0;
        Span<char> digits = stackalloc char[40];
        magnitude.TryFormat(digits, out int count, default, CultureInfo.InvariantCulture);
        // Leading zeros so that one digit stands before the point.
        int pad = Math.Max(0, scale + 1 - count);
        int whole = count + pad - scale;
        int at = 0;
        if (sign == 1) text[at++] = '-';
        for (int k = 0; k < count + pad; k++)
        {
            if (k == whole) text[at++] = '.';
            text[at++] = k < pad ? '0' : digits[k - pad];
        }
        return at;
    }

    /// <summary>Writes <c>yyyy-MM-dd</c>; returns its length.</summary>
    private static int FormatDate(int year, int month, int day, Span<char> text)
    {
        int at = Digits(text, 0, year, 4);
        text[at++] = '-';
        at = Digits(text, at, month, 2);
        text[at++] = '-';
        return Digits(text, at, day, 2);
    }

    /// <summary>Writes a non-negative number in exactly <paramref name="count"/> digits at <paramref name="at"/>; returns the end.</summary>
    private static int Digits(Span<char> text, int at, int value, int count)
    {
        for (int k = at + count - 1; k >= at; k--)
        {
            text[k] = (char)('0' + value % 10);
            value /= 10;
        }
        return at + count;
    }

    /// <summary>A timestamp cut to milliseconds as Spark writes it; 0 when it is outside years 1 to 9999.</summary>
    private static int TimestampText(TimestampArray array, int i, bool int96, Span<char> text)
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
        text[at++] = 'T';
        at = Digits(text, at, (int)(ofDay / 3_600_000), 2);
        text[at++] = ':';
        at = Digits(text, at, (int)(ofDay / 60_000 % 60), 2);
        text[at++] = ':';
        at = Digits(text, at, (int)(ofDay / 1000 % 60), 2);
        text[at++] = '.';
        at = Digits(text, at, (int)(ofDay % 1000), 3);
        if (!string.IsNullOrEmpty(type.Timezone) || int96) text[at++] = 'Z';
        return at;

        static long FloorDiv(long value, long divisor) => Math.DivRem(value, divisor) is var (q, r) && r < 0 ? q - 1 : q;
    }
}
