using System.Globalization;
using System.Numerics;
using System.Text;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;

namespace Kaminari.Seseragi.Files.Excel.Reading;

/// <summary>What the appenders need besides the cell: the date system and where the cell is, for errors.</summary>
internal sealed class AppendContext(string sheet, bool date1904)
{
    public string Sheet { get; } = sheet;

    public bool Date1904 { get; } = date1904;

    public ExcelValueException Mismatch(int row, in Cell cell, string what, bool inferred = false) =>
        new($"Cell {ExcelCellReference.Format(row, cell.Column)} of sheet '{Sheet}' {what}"
            + (inferred ? "; the column type was inferred from the first rows: read with a larger InferSchemaLength or a schema override." : "."),
            Sheet, ExcelCellReference.Format(row, cell.Column));
}

/// <summary>
/// Appends cells to one Arrow column of a fixed type. Error and empty cells are nulls; a value the type cannot hold
/// raises <see cref="ExcelValueException"/>.
/// </summary>
internal abstract class ColumnAppender(NativeColumnBuilder builder)
{
    public NativeColumnBuilder Builder { get; } = builder;

    /// <summary>The type was inferred (text is not parsed into it, and errors suggest a remedy), not given.</summary>
    public bool Inferred { get; set; }

    public void AppendNull() => Builder.AppendNull();

    /// <summary>Appends a cell of type Number, String, SharedString, Boolean or Temporal.</summary>
    public abstract void Append(RowBuffer row, in Cell cell, AppendContext context);

    /// <summary>The appender for an Arrow type, or null when Excel values cannot be read as it.</summary>
    public static ColumnAppender? Create(IArrowType type) => type switch
    {
        BooleanType => new BooleanAppender(),
        Int8Type => new IntegerAppender<sbyte>(type),
        Int16Type => new IntegerAppender<short>(type),
        Int32Type => new IntegerAppender<int>(type),
        Int64Type => new IntegerAppender<long>(type),
        UInt8Type => new IntegerAppender<byte>(type),
        UInt16Type => new IntegerAppender<ushort>(type),
        UInt32Type => new IntegerAppender<uint>(type),
        UInt64Type => new IntegerAppender<ulong>(type),
        FloatType => new FloatAppender<float>(type),
        DoubleType => new FloatAppender<double>(type),
        StringType or LargeStringType or StringViewType => new StringAppender(),
        Decimal128Type d => new DecimalAppender(d),
        Date32Type => new DateAppender(type, 1),
        Date64Type => new DateAppender(type, 86_400_000),
        TimestampType t => new TimestampAppender(t),
        Time32Type t => new TimeAppender(t, t.Unit),
        Time64Type t => new TimeAppender(t, t.Unit),
        DurationType t => new DurationAppender(t),
        NullType => new NullAppender(),
        _ => null,
    };

    /// <summary>Microseconds per unit (seconds 1e6, milliseconds 1e3, microseconds 1); nanoseconds give -1000.</summary>
    protected static long PerUnit(TimeUnit unit) => unit switch
    {
        TimeUnit.Second => 1_000_000,
        TimeUnit.Millisecond => 1_000,
        TimeUnit.Microsecond => 1,
        _ => -1_000,
    };

    /// <summary>Microseconds in a unit, exactly; false when the value has a finer part than the unit holds.</summary>
    protected static bool TryToUnit(long micros, long perUnit, out long value)
    {
        if (perUnit < 0)
        {
            value = micros * -perUnit;
            return true;
        }
        value = Math.DivRem(micros, perUnit, out long rem);
        return rem == 0;
    }

    protected ExcelValueException Fail(AppendContext context, RowBuffer row, in Cell cell, string what) =>
        context.Mismatch(row.Row, cell, what, Inferred);

    /// <summary>Text into a non-string column: parsed for a given type, refused for an inferred one.</summary>
    protected void RequireParse(AppendContext context, RowBuffer row, in Cell cell)
    {
        if (Inferred) throw Fail(context, row, cell, $"holds text '{Text(row, cell)}' in a {Builder.Type.Name} column");
    }

    protected static string Text(RowBuffer row, in Cell cell) => Encoding.UTF8.GetString(row.GetText(cell));

    protected static string Describe(in Cell cell) => cell.Type switch
    {
        CellType.Boolean => "holds a boolean",
        CellType.String or CellType.SharedString => "holds text",
        CellType.Temporal => cell.Kind == ExcelValueKind.Duration ? "holds a duration" : "holds a date or time",
        _ => cell.Kind == ExcelValueKind.Number ? "holds a number" : "holds a date or time",
    };
}

internal sealed class NullAppender() : ColumnAppender(new NativeNullBuilder())
{
    public override void Append(RowBuffer row, in Cell cell, AppendContext context) => Builder.AppendNull();
}

internal sealed class BooleanAppender() : ColumnAppender(new NativeBooleanBuilder())
{
    public override void Append(RowBuffer row, in Cell cell, AppendContext context)
    {
        var b = (NativeBooleanBuilder)Builder;
        switch (cell.Type)
        {
            case CellType.Boolean:
                b.Append(cell.Number != 0);
                return;
            case CellType.String or CellType.SharedString:
                RequireParse(context, row, cell);
                var text = Xml.XmlText.Trim(row.GetText(cell));
                if (Ascii.EqualsIgnoreCase(text, "true"u8)) b.Append(true);
                else if (Ascii.EqualsIgnoreCase(text, "false"u8)) b.Append(false);
                else throw Fail(context, row, cell, $"holds '{Text(row, cell)}', not a boolean");
                return;
            default:
                throw Fail(context, row, cell, $"{Describe(cell)}, not a boolean");
        }
    }
}

internal sealed class IntegerAppender<T>(IArrowType type) : ColumnAppender(NativeColumnBuilder.Create(type))
    where T : unmanaged, IBinaryInteger<T>, IMinMaxValue<T>
{
    private static readonly double Min = double.CreateTruncating(T.MinValue);
    private static readonly double MaxExclusive = double.CreateTruncating(T.MaxValue) + 1.0;

    public override void Append(RowBuffer row, in Cell cell, AppendContext context)
    {
        var b = (NativeFixedWidthBuilder<T>)Builder;
        switch (cell.Type)
        {
            case CellType.Number:
                double v = cell.Number;
                if (v != Math.Floor(v) || !(v >= Min && v < MaxExclusive))
                    throw Fail(context, row, cell, $"holds {v.ToString(CultureInfo.InvariantCulture)}, which is not a {Builder.Type.Name} integer");
                b.Append(T.CreateTruncating(v));
                return;
            case CellType.String or CellType.SharedString:
                RequireParse(context, row, cell);
                if (!T.TryParse(Xml.XmlText.Trim(row.GetText(cell)), NumberStyles.Integer, CultureInfo.InvariantCulture, out T parsed))
                    throw Fail(context, row, cell, $"holds '{Text(row, cell)}', not a {Builder.Type.Name} integer");
                b.Append(parsed);
                return;
            default:
                throw Fail(context, row, cell, $"{Describe(cell)}, not an integer");
        }
    }
}

internal sealed class FloatAppender<T>(IArrowType type) : ColumnAppender(NativeColumnBuilder.Create(type))
    where T : unmanaged, IBinaryFloatingPointIeee754<T>
{
    public override void Append(RowBuffer row, in Cell cell, AppendContext context)
    {
        var b = (NativeFixedWidthBuilder<T>)Builder;
        switch (cell.Type)
        {
            case CellType.Number:
                b.Append(T.CreateTruncating(cell.Number));
                return;
            case CellType.String or CellType.SharedString:
                RequireParse(context, row, cell);
                if (!T.TryParse(Xml.XmlText.Trim(row.GetText(cell)), NumberStyles.Float, CultureInfo.InvariantCulture, out T parsed))
                    throw Fail(context, row, cell, $"holds '{Text(row, cell)}', not a number");
                b.Append(parsed);
                return;
            default:
                throw Fail(context, row, cell, $"{Describe(cell)}, not a number");
        }
    }
}

internal sealed class StringAppender() : ColumnAppender(new NativeBinaryBuilder(StringType.Default))
{
    public override void Append(RowBuffer row, in Cell cell, AppendContext context)
    {
        var b = (NativeBinaryBuilder)Builder;
        if (cell.Type is CellType.String or CellType.SharedString)
        {
            b.Append(row.GetText(cell));
            return;
        }
        Span<byte> text = stackalloc byte[CellText.MaxLength];
        if (CellText.TryFormat(cell, context.Date1904, text, out int n)) b.Append(text[..n]);
        else b.AppendNull();
    }
}

/// <summary>Decimal128: numbers through their shortest round-trip text, strings parsed; exact at the scale or refused.</summary>
internal sealed class DecimalAppender(Decimal128Type type) : ColumnAppender(NativeColumnBuilder.Create(type))
{
    public override void Append(RowBuffer row, in Cell cell, AppendContext context)
    {
        var b = (NativeFixedWidthBuilder<Int128>)Builder;
        Span<byte> buffer = stackalloc byte[CellText.MaxLength];
        scoped ReadOnlySpan<byte> text;
        switch (cell.Type)
        {
            case CellType.Number:
                cell.Number.TryFormat(buffer, out int n, default, CultureInfo.InvariantCulture);
                text = buffer[..n];
                break;
            case CellType.String or CellType.SharedString:
                RequireParse(context, row, cell);
                text = Xml.XmlText.Trim(row.GetText(cell));
                break;
            default:
                throw Fail(context, row, cell, $"{Describe(cell)}, not a decimal");
        }
        if (!DecimalText.TryParse(text, type.Precision, type.Scale, out Int128 unscaled))
            throw Fail(context, row, cell, $"holds '{Encoding.UTF8.GetString(text)}', which decimal({type.Precision}, {type.Scale}) cannot hold exactly");
        b.Append(unscaled);
    }
}

/// <summary>Date32 (days) or Date64 (milliseconds at midnight): a time part other than midnight is refused.</summary>
internal sealed class DateAppender(IArrowType type, long perDay) : ColumnAppender(NativeColumnBuilder.Create(type))
{
    public override void Append(RowBuffer row, in Cell cell, AppendContext context)
    {
        long micros;
        switch (cell.Type)
        {
            case CellType.Number:
                if (!ExcelDates.TryToUnixMicros(cell.Number, context.Date1904, out micros))
                    throw Fail(context, row, cell, $"holds {cell.Number.ToString(CultureInfo.InvariantCulture)}, outside Excel's dates");
                break;
            case CellType.Temporal when cell.Kind is ExcelValueKind.Date or ExcelValueKind.DateTime:
                micros = cell.Micros;
                break;
            case CellType.String or CellType.SharedString:
                RequireParse(context, row, cell);
                if (!DateOnly.TryParseExact(Text(row, cell).Trim(), "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var d))
                    throw Fail(context, row, cell, $"holds '{Text(row, cell)}', not a yyyy-mm-dd date");
                micros = (d.DayNumber - 719_162L) * ExcelDates.MicrosPerDay;
                break;
            default:
                throw Fail(context, row, cell, $"{Describe(cell)}, not a date");
        }
        var (days, ofDay) = ExcelDates.SplitMicros(micros);
        if (ofDay != 0) throw Fail(context, row, cell, "holds a time of day besides the date; read the column as a datetime");
        if (perDay == 1) ((NativeFixedWidthBuilder<int>)Builder).Append((int)days);
        else ((NativeFixedWidthBuilder<long>)Builder).Append(days * perDay);
    }
}

/// <summary>
/// Timestamps: the workbook's wall-clock value; with a time zone it is that zone's local time (stored as the UTC
/// instant). A part finer than the unit is refused.
/// </summary>
internal sealed class TimestampAppender(TimestampType type) : ColumnAppender(NativeColumnBuilder.Create(type))
{
    private readonly long _perUnit = PerUnit(type.Unit);
    private readonly TimeZoneInfo? _zone = string.IsNullOrEmpty(type.Timezone) ? null : TimeZones.Find(type.Timezone);

    public override void Append(RowBuffer row, in Cell cell, AppendContext context)
    {
        long micros;
        switch (cell.Type)
        {
            case CellType.Number:
                if (!ExcelDates.TryToUnixMicros(cell.Number, context.Date1904, out micros))
                    throw Fail(context, row, cell, $"holds {cell.Number.ToString(CultureInfo.InvariantCulture)}, outside Excel's dates");
                break;
            case CellType.Temporal when cell.Kind != ExcelValueKind.Duration:
                micros = cell.Kind == ExcelValueKind.Time
                    ? ExcelDates.UnixDays(0, context.Date1904) * ExcelDates.MicrosPerDay + cell.Micros
                    : cell.Micros;
                break;
            case CellType.String or CellType.SharedString:
                RequireParse(context, row, cell);
                if (!DateTime.TryParse(Text(row, cell), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var dt))
                    throw Fail(context, row, cell, $"holds '{Text(row, cell)}', not a datetime");
                micros = (dt.Ticks - DateTime.UnixEpoch.Ticks) / 10;
                break;
            default:
                throw Fail(context, row, cell, $"{Describe(cell)}, not a datetime");
        }
        if (_zone is not null) micros = ToUtc(micros, row, cell, context);
        if (!TryToUnit(micros, _perUnit, out long value))
            throw Fail(context, row, cell, $"holds a time finer than the column's unit ({type.Unit})");
        ((NativeFixedWidthBuilder<long>)Builder).Append(value);
    }

    private long ToUtc(long wallMicros, RowBuffer row, in Cell cell, AppendContext context)
    {
        var wall = new DateTime(DateTime.UnixEpoch.Ticks + wallMicros * 10, DateTimeKind.Unspecified);
        if (_zone!.IsInvalidTime(wall))
            throw Fail(context, row, cell, $"holds {wall:yyyy-MM-dd HH:mm:ss}, which does not exist in time zone {type.Timezone}");
        var utc = TimeZoneInfo.ConvertTimeToUtc(wall, _zone);
        return (utc.Ticks - DateTime.UnixEpoch.Ticks) / 10;
    }
}

/// <summary>Time of day: values in [0, 1) days only.</summary>
internal sealed class TimeAppender(IArrowType type, TimeUnit unit) : ColumnAppender(NativeColumnBuilder.Create(type))
{
    private readonly long _perUnit = PerUnit(unit);

    public override void Append(RowBuffer row, in Cell cell, AppendContext context)
    {
        long micros;
        switch (cell.Type)
        {
            case CellType.Number:
                if (cell.Number is not (>= 0 and < 1))
                    throw Fail(context, row, cell, $"holds {cell.Number.ToString(CultureInfo.InvariantCulture)} days, not a time of day");
                micros = (long)Math.Round(cell.Number * ExcelDates.MicrosPerDay, MidpointRounding.AwayFromZero);
                if (micros == ExcelDates.MicrosPerDay)
                    throw Fail(context, row, cell, "holds 24:00:00, not a time of day");
                break;
            case CellType.Temporal when cell.Kind == ExcelValueKind.Time:
                micros = cell.Micros;
                break;
            case CellType.String or CellType.SharedString:
                RequireParse(context, row, cell);
                if (!TimeOnly.TryParse(Text(row, cell), CultureInfo.InvariantCulture, out var t))
                    throw Fail(context, row, cell, $"holds '{Text(row, cell)}', not a time");
                micros = t.Ticks / 10;
                break;
            default:
                throw Fail(context, row, cell, $"{Describe(cell)}, not a time of day");
        }
        if (!TryToUnit(micros, _perUnit, out long value))
            throw Fail(context, row, cell, $"holds a time finer than the column's unit ({unit})");
        if (Builder is NativeFixedWidthBuilder<int> b32) b32.Append((int)value);
        else ((NativeFixedWidthBuilder<long>)Builder).Append(value);
    }
}

/// <summary>Durations: the serial number counts days (any sign).</summary>
internal sealed class DurationAppender(DurationType type) : ColumnAppender(NativeColumnBuilder.Create(type))
{
    private readonly long _perUnit = PerUnit(type.Unit);

    public override void Append(RowBuffer row, in Cell cell, AppendContext context)
    {
        long micros;
        switch (cell.Type)
        {
            case CellType.Number:
                if (!ExcelDates.TryToDurationMicros(cell.Number, out micros))
                    throw Fail(context, row, cell, $"holds {cell.Number.ToString(CultureInfo.InvariantCulture)}, not a duration");
                break;
            case CellType.Temporal when cell.Kind is ExcelValueKind.Time or ExcelValueKind.Duration:
                micros = cell.Micros;
                break;
            case CellType.String or CellType.SharedString:
                RequireParse(context, row, cell);
                if (!TimeSpan.TryParse(Text(row, cell), CultureInfo.InvariantCulture, out var ts))
                    throw Fail(context, row, cell, $"holds '{Text(row, cell)}', not a duration");
                micros = ts.Ticks / 10;
                break;
            default:
                throw Fail(context, row, cell, $"{Describe(cell)}, not a duration");
        }
        if (!TryToUnit(micros, _perUnit, out long value))
            throw Fail(context, row, cell, $"holds a duration finer than the column's unit ({type.Unit})");
        ((NativeFixedWidthBuilder<long>)Builder).Append(value);
    }
}

/// <summary>IANA or Windows time zone ids.</summary>
internal static class TimeZones
{
    public static TimeZoneInfo Find(string id)
    {
        if (id is "UTC" or "Etc/UTC" or "Z" or "+00:00") return TimeZoneInfo.Utc;
        if (TimeSpanOffset(id) is { } offset)
            return TimeZoneInfo.CreateCustomTimeZone(id, offset, id, id);
        try
        {
            return TimeZoneInfo.FindSystemTimeZoneById(id);
        }
        catch (Exception e) when (e is TimeZoneNotFoundException or InvalidTimeZoneException)
        {
            throw new ArgumentException($"Unknown time zone '{id}'.", e);
        }
    }

    // Fixed offsets as Arrow allows them: "+09:00", "-05:30".
    private static TimeSpan? TimeSpanOffset(string id) =>
        id.Length == 6 && id[0] is '+' or '-' && id[3] == ':'
        && int.TryParse(id.AsSpan(1, 2), CultureInfo.InvariantCulture, out int h) && int.TryParse(id.AsSpan(4, 2), CultureInfo.InvariantCulture, out int m)
            ? (id[0] == '-' ? -1 : 1) * new TimeSpan(h, m, 0)
            : null;
}

/// <summary>Exact decimal text to a Decimal128 unscaled value.</summary>
internal static class DecimalText
{
    /// <summary>
    /// Parses <c>[-+]digits[.digits][e[-+]digits]</c> as an unscaled integer at <paramref name="scale"/>; false when
    /// the value has more fraction digits than the scale holds (non-zero ones) or more than
    /// <paramref name="precision"/> digits in all.
    /// </summary>
    public static bool TryParse(ReadOnlySpan<byte> text, int precision, int scale, out Int128 unscaled)
    {
        unscaled = 0;
        int i = 0;
        bool negative = false;
        if (i < text.Length && text[i] is (byte)'-' or (byte)'+') negative = text[i++] == (byte)'-';
        int start = i;
        int pointAt = -1;
        int end = i;
        for (; end < text.Length; end++)
        {
            byte c = text[end];
            if (c == (byte)'.' && pointAt < 0) pointAt = end;
            else if ((uint)(c - '0') > 9) break;
        }
        if (end == start || (end == start + 1 && pointAt == start)) return false;
        int exponent = 0;
        if (end < text.Length)
        {
            if (text[end] is not ((byte)'e' or (byte)'E')) return false;
            if (!int.TryParse(text[(end + 1)..], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out exponent)) return false;
            if (Math.Abs(exponent) > 400) return false;
        }
        // Digits d1..dn with the decimal point after position p: value = digits * 10^(p - n + exponent).
        Int128 digits = 0;
        int count = 0, fractionDigits = 0;
        bool afterPoint = false;
        for (int k = start; k < end; k++)
        {
            if (k == pointAt)
            {
                afterPoint = true;
                continue;
            }
            int d = text[k] - '0';
            if (afterPoint) fractionDigits++;
            if (count == 0 && d == 0)
            {
                continue; // leading zeros
            }
            if (++count > 38) return false;
            digits = digits * 10 + d;
        }
        // Strip trailing zeros of the fraction (they do not need scale).
        int shift = scale - fractionDigits + exponent; // multiply digits by 10^shift
        while (shift < 0 && digits != 0 && digits % 10 == 0)
        {
            digits /= 10;
            shift++;
        }
        if (digits == 0)
        {
            unscaled = 0;
            return true;
        }
        if (shift < 0) return false;
        Int128 limit = Pow10(precision);
        for (int k = 0; k < shift; k++)
        {
            digits *= 10;
            if (digits >= limit) return false;
        }
        if (digits >= limit) return false;
        unscaled = negative ? -digits : digits;
        return true;
    }

    private static Int128 Pow10(int n)
    {
        Int128 r = 1;
        for (int i = 0; i < n; i++) r *= 10;
        return r;
    }
}
