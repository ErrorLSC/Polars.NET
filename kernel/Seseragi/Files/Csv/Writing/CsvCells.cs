using System.Buffers;
using System.Globalization;
using System.Numerics;
using System.Text.Unicode;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Sazanami.Text;
using Kaminari.Seseragi.Files.IO.Writing;

namespace Kaminari.Seseragi.Files.Csv.Writing;

/// <summary>The bytes a writer's fields are written with, and which of them are quoted.</summary>
internal sealed class CsvSyntax
{
    public CsvSyntax(CsvWriteOptions options)
    {
        Separator = (byte)options.Separator;
        Quote = (byte)options.Quote;
        Style = options.QuoteStyle;
        Special = SearchValues.Create([Separator, Quote, (byte)'\r', (byte)'\n']);
        byte[] text = System.Text.Encoding.UTF8.GetBytes(options.NullValue);
        Null = Style == CsvQuoteStyle.Always ? Quoted(text) : text;
        LineTerminator = System.Text.Encoding.UTF8.GetBytes(options.LineTerminator);
    }

    public byte Separator { get; }

    public byte Quote { get; }

    public CsvQuoteStyle Style { get; }

    /// <summary>The bytes that make a field need quotes: the separator, the quote, CR, LF.</summary>
    public SearchValues<byte> Special { get; }

    /// <summary>A null as written.</summary>
    public byte[] Null { get; }

    public byte[] LineTerminator { get; }

    /// <summary>How a column of numbers (<paramref name="numeric"/>) or of other values written by format is quoted.</summary>
    public CsvQuoting ForFormatted(bool numeric) => Style switch
    {
        CsvQuoteStyle.Always => CsvQuoting.Always,
        CsvQuoteStyle.NonNumeric when !numeric => CsvQuoting.Always,
        _ => CsvQuoting.None,
    };

    /// <summary><paramref name="text"/> in quotes, the quotes in it doubled.</summary>
    public byte[] Quoted(ReadOnlySpan<byte> text)
    {
        using var buffer = new Kaminari.Sazanami.Buffers.NativeBufferWriter(text.Length + 16);
        var o = new TextOut(buffer);
        WriteQuoted(ref o, text);
        o.Commit();
        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>A field as text: quoted as the style says (<see cref="CsvQuoteStyle.Necessary"/>: when it holds a special byte or is empty).</summary>
    public void WriteText(ref TextOut o, scoped ReadOnlySpan<byte> text)
    {
        switch (Style)
        {
            case CsvQuoteStyle.Never:
                o.Bytes(text);
                break;
            case CsvQuoteStyle.Necessary when !text.IsEmpty && text.IndexOfAny(Special) < 0:
                o.Bytes(text);
                break;
            default:
                WriteQuoted(ref o, text);
                break;
        }
    }

    /// <summary><paramref name="text"/> in quotes, a quote in it doubled.</summary>
    public void WriteQuoted(ref TextOut o, scoped ReadOnlySpan<byte> text)
    {
        o.Byte(Quote);
        while (true)
        {
            int i = text.IndexOf(Quote);
            if (i < 0)
            {
                o.Bytes(text);
                break;
            }
            o.Bytes(text[..(i + 1)]);
            o.Byte(Quote);
            text = text[(i + 1)..];
        }
        o.Byte(Quote);
    }

    /// <summary>A column name as the header writes it (quoted as needed; every name with <see cref="CsvQuoteStyle.NonNumeric"/>).</summary>
    public void WriteName(ref TextOut o, string name)
    {
        byte[] text = System.Text.Encoding.UTF8.GetBytes(name);
        if (Style == CsvQuoteStyle.NonNumeric) WriteQuoted(ref o, text);
        else WriteText(ref o, text);
    }
}

/// <summary>Which fields of a column written by format are quoted.</summary>
internal enum CsvQuoting
{
    None,
    Always,
}

/// <summary>
/// Writes the values of one Arrow array as CSV fields, row by row (a row is the array's own index, before its offset). A
/// value that cannot be written (a string that is not UTF-8) is a <see cref="CsvWriteException"/> (its row added by
/// the writer).
/// </summary>
internal abstract unsafe class CsvCell
{
    private readonly byte* _validity;
    private readonly bool _allNull;

    protected CsvCell(ArrayData data, Pins pins, string name)
    {
        Name = name;
        Offset = data.Offset;
        _allNull = data.DataType.TypeId == ArrowTypeId.Null;
        if (!_allNull && data.NullCount != 0 && data.Buffers.Length > 0) _validity = pins.Pin(data.Buffers[0]);
    }

    /// <summary>The column's name, for errors.</summary>
    public string Name { get; }

    /// <summary>The array's offset (added to rows to reach its buffers).</summary>
    protected int Offset { get; }

    public bool IsNull(long row)
    {
        if (_validity == null) return _allNull;
        long bit = row + Offset;
        return (_validity[bit >> 3] & (1 << (int)(bit & 7))) == 0;
    }

    /// <summary>The field of a row that is not null.</summary>
    public abstract void Write(ref TextOut o, long row);

    /// <summary>Why a column of <paramref name="field"/>'s type cannot be written, or null when it can.</summary>
    public static string? Refusal(Field field) => field.DataType switch
    {
        TimestampType { Timezone: { Length: > 0 } zone } when TimeZoneOffsets.For(zone) is null =>
            $"Column '{field.Name}': the time zone '{zone}' is not known (convert the column to UTC or to a known zone).",
        NullType or BooleanType or Int8Type or Int16Type or Int32Type or Int64Type or UInt8Type or UInt16Type or UInt32Type or UInt64Type
            or HalfFloatType or FloatType or DoubleType or Decimal32Type or Decimal64Type or Decimal128Type or StringType or LargeStringType
            or StringViewType or Date32Type or Date64Type or TimestampType or Time32Type or Time64Type or DurationType => null,
        DictionaryType d => Refusal(new Field(field.Name, d.ValueType, true)),
        BinaryType or LargeBinaryType or BinaryViewType or FixedSizeBinaryType =>
            $"Column '{field.Name}' is binary, which CSV cannot hold as it is: encode it as text first (base64 or hex), or leave it out.",
        NestedType => $"Column '{field.Name}' is {field.DataType.Name}: CSV holds no nested values (flatten it into columns, or write it as JSON text).",
        _ => $"Column '{field.Name}': {field.DataType.Name} cannot be written to CSV.",
    };

    /// <summary>The cell of <paramref name="data"/> (of <paramref name="field"/>'s type, checked by <see cref="Refusal"/>).</summary>
    public static CsvCell For(ArrayData data, Field field, Pins pins, CsvSyntax syntax) => field.DataType switch
    {
        NullType => new NullCell(data, pins, field.Name),
        BooleanType => new BooleanCell(data, pins, field.Name, syntax),
        Int8Type => new IntegerCell<sbyte>(data, pins, field.Name, syntax),
        Int16Type => new IntegerCell<short>(data, pins, field.Name, syntax),
        Int32Type => new IntegerCell<int>(data, pins, field.Name, syntax),
        Int64Type => new IntegerCell<long>(data, pins, field.Name, syntax),
        UInt8Type => new IntegerCell<byte>(data, pins, field.Name, syntax),
        UInt16Type => new IntegerCell<ushort>(data, pins, field.Name, syntax),
        UInt32Type => new IntegerCell<uint>(data, pins, field.Name, syntax),
        UInt64Type => new IntegerCell<ulong>(data, pins, field.Name, syntax),
        HalfFloatType => new FloatCell<Half>(data, pins, field.Name, syntax),
        FloatType => new FloatCell<float>(data, pins, field.Name, syntax),
        DoubleType => new FloatCell<double>(data, pins, field.Name, syntax),
        Decimal32Type d => new DecimalCell(data, pins, field.Name, syntax, d.Scale, null, width: 4),
        Decimal64Type d => new DecimalCell(data, pins, field.Name, syntax, d.Scale, null, width: 8),
        Decimal128Type d => new DecimalCell(data, pins, field.Name, syntax, d.Scale, WideIntegers.KindOf(field), width: 16),
        StringType => new BytesCell(data, pins, field.Name, syntax, large: false),
        LargeStringType => new BytesCell(data, pins, field.Name, syntax, large: true),
        StringViewType => new ViewCell(data, pins, field.Name, syntax),
        Date32Type => new DateCell(data, pins, field.Name, syntax, millis: false),
        Date64Type => new DateCell(data, pins, field.Name, syntax, millis: true),
        TimestampType t => new TimestampCell(data, pins, field.Name, syntax, t.Unit, t.Timezone is { Length: > 0 } zone ? TimeZoneOffsets.For(zone) : null),
        Time32Type t => new TimeCell(data, pins, field.Name, syntax, UnitNanos(t.Unit), wide: false),
        Time64Type t => new TimeCell(data, pins, field.Name, syntax, UnitNanos(t.Unit), wide: true),
        DurationType t => new DurationCell(data, pins, field.Name, syntax, UnitNanos(t.Unit)),
        DictionaryType d => new DictionaryCell(data, pins, field.Name, d.IndexType, For(data.Dictionary, new Field(field.Name, d.ValueType, true), pins, syntax)),
        _ => throw new NotSupportedException(Refusal(field) ?? $"Column '{field.Name}': {field.DataType.Name} cannot be written to CSV."),
    };

    private static long UnitNanos(TimeUnit unit) => unit switch
    {
        TimeUnit.Second => 1_000_000_000,
        TimeUnit.Millisecond => 1_000_000,
        TimeUnit.Microsecond => 1_000,
        _ => 1,
    };
}

/// <summary>A column of the null type: every field null (never written: the writer writes nulls).</summary>
internal sealed class NullCell(ArrayData data, Pins pins, string name) : CsvCell(data, pins, name)
{
    public override void Write(ref TextOut o, long row) { }
}

/// <summary>Values written by format (numbers, booleans, dates, times): at most <see cref="MaxBytes"/> each, quoted as the column says.</summary>
internal abstract class FormattedCell(ArrayData data, Pins pins, string name, CsvSyntax syntax, bool numeric) : CsvCell(data, pins, name)
{
    private readonly CsvQuoting _quoting = syntax.ForFormatted(numeric);
    private readonly byte _quote = syntax.Quote;

    /// <summary>The most bytes a value's text takes.</summary>
    protected abstract int MaxBytes { get; }

    /// <summary>A row's value as text into <paramref name="destination"/> (at least <see cref="MaxBytes"/> long): its length.</summary>
    protected abstract int Format(long row, Span<byte> destination);

    public sealed override void Write(ref TextOut o, long row)
    {
        if (_quoting == CsvQuoting.None)
        {
            o.Advance(Format(row, o.Room(MaxBytes)));
            return;
        }
        Span<byte> room = o.Room(MaxBytes + 2);
        room[0] = _quote;
        int length = Format(row, room[1..]);
        room[length + 1] = _quote;
        o.Advance(length + 2);
    }
}

internal sealed unsafe class BooleanCell(ArrayData data, Pins pins, string name, CsvSyntax syntax) : FormattedCell(data, pins, name, syntax, numeric: false)
{
    private readonly byte* _values = pins.Pin(data.Buffers[1]);

    protected override int MaxBytes => 5;

    protected override int Format(long row, Span<byte> destination)
    {
        long bit = row + Offset;
        ReadOnlySpan<byte> text = (_values[bit >> 3] & (1 << (int)(bit & 7))) != 0 ? "true"u8 : "false"u8;
        text.CopyTo(destination);
        return text.Length;
    }
}

internal sealed unsafe class IntegerCell<T>(ArrayData data, Pins pins, string name, CsvSyntax syntax) : FormattedCell(data, pins, name, syntax, numeric: true)
    where T : unmanaged, IBinaryInteger<T>, IUtf8SpanFormattable
{
    private readonly T* _values = (T*)pins.Pin(data.Buffers[1]);

    protected override int MaxBytes => 21;

    protected override int Format(long row, Span<byte> destination)
    {
        _values[row + Offset].TryFormat(destination, out int written, default, CultureInfo.InvariantCulture);
        return written;
    }
}

/// <summary>
/// Floats: their shortest round-trip digits as the reference engine writes them (<see cref="FloatText.Shortest{T}"/>),
/// <c>NaN</c>, <c>inf</c>, <c>-inf</c>. Half floats are written as the float of their value.
/// </summary>
internal sealed unsafe class FloatCell<T>(ArrayData data, Pins pins, string name, CsvSyntax syntax) : FormattedCell(data, pins, name, syntax, numeric: true)
    where T : unmanaged, IFloatingPointIeee754<T>, IUtf8SpanFormattable
{
    private readonly T* _values = (T*)pins.Pin(data.Buffers[1]);

    protected override int MaxBytes => FloatText.MaxShortestBytes;

    protected override int Format(long row, Span<byte> destination)
    {
        T value = _values[row + Offset];
        if (T.IsNaN(value)) return Copy("NaN"u8, destination);
        if (T.IsInfinity(value)) return Copy(T.IsNegative(value) ? "-inf"u8 : "inf"u8, destination);
        return FloatText.Shortest(value, destination);
    }

    private static int Copy(ReadOnlySpan<byte> text, Span<byte> destination)
    {
        text.CopyTo(destination);
        return text.Length;
    }
}

/// <summary>Decimals (32, 64 or 128 bits) with every digit of their scale; 128-bit integers (decimal(38, 0) so marked) as integers.</summary>
internal sealed unsafe class DecimalCell(ArrayData data, Pins pins, string name, CsvSyntax syntax, int scale, string? integer, int width)
    : FormattedCell(data, pins, name, syntax, numeric: true)
{
    private readonly byte* _values = pins.Pin(data.Buffers[1]);

    protected override int MaxBytes => 44 + Math.Abs(scale);

    protected override int Format(long row, Span<byte> destination)
    {
        long index = row + Offset;
        Int128 value = width switch
        {
            4 => ((int*)_values)[index],
            8 => ((long*)_values)[index],
            _ => ((Int128*)_values)[index],
        };
        int written;
        if (integer == WideIntegers.Unsigned)
        {
            ((UInt128)value).TryFormat(destination, out written, default, CultureInfo.InvariantCulture);
            return written;
        }
        if (integer is not null)
        {
            value.TryFormat(destination, out written, default, CultureInfo.InvariantCulture);
            return written;
        }
        Span<byte> digits = stackalloc byte[48];
        bool negative = value < 0;
        ((UInt128)(negative ? -value : value)).TryFormat(digits, out int length, default, CultureInfo.InvariantCulture);
        int at = 0;
        if (negative) destination[at++] = (byte)'-';
        if (scale <= 0)
        {
            digits[..length].CopyTo(destination[at..]);
            at += length;
            if (value != 0)
                for (int i = 0; i < -scale; i++) destination[at++] = (byte)'0';
            return at;
        }
        int whole = length - scale;
        if (whole <= 0) destination[at++] = (byte)'0';
        else
        {
            digits[..whole].CopyTo(destination[at..]);
            at += whole;
        }
        destination[at++] = (byte)'.';
        for (int i = whole; i < 0; i++) destination[at++] = (byte)'0';
        ReadOnlySpan<byte> fraction = digits[Math.Max(0, whole)..length];
        fraction.CopyTo(destination[at..]);
        return at + fraction.Length;
    }
}

/// <summary>Strings (checked to be UTF-8), quoted as the style says.</summary>
internal abstract class TextCell(ArrayData data, Pins pins, string name, CsvSyntax syntax) : CsvCell(data, pins, name)
{
    protected void WriteValue(ref TextOut o, scoped ReadOnlySpan<byte> bytes)
    {
        if (!System.Text.Ascii.IsValid(bytes) && !Utf8.IsValid(bytes))
            throw new CsvWriteException($"Column '{Name}': a string that is not UTF-8.", Name, -1);
        syntax.WriteText(ref o, bytes);
    }
}

internal sealed unsafe class BytesCell(ArrayData data, Pins pins, string name, CsvSyntax syntax, bool large) : TextCell(data, pins, name, syntax)
{
    private readonly byte* _offsets = pins.Pin(data.Buffers[1]);
    private readonly byte* _data = pins.Pin(data.Buffers[2]);

    public override void Write(ref TextOut o, long row)
    {
        long i = row + Offset, start, end;
        if (large)
        {
            start = ((long*)_offsets)[i];
            end = ((long*)_offsets)[i + 1];
        }
        else
        {
            start = ((int*)_offsets)[i];
            end = ((int*)_offsets)[i + 1];
        }
        WriteValue(ref o, new ReadOnlySpan<byte>(_data + start, checked((int)(end - start))));
    }
}

/// <summary>String views: inline up to 12 bytes, else in one of the data buffers.</summary>
internal sealed unsafe class ViewCell : TextCell
{
    private readonly byte* _views;
    private readonly byte*[] _buffers;

    public ViewCell(ArrayData data, Pins pins, string name, CsvSyntax syntax) : base(data, pins, name, syntax)
    {
        _views = pins.Pin(data.Buffers[1]);
        _buffers = new byte*[Math.Max(0, data.Buffers.Length - 2)];
        for (int i = 2; i < data.Buffers.Length; i++) _buffers[i - 2] = pins.Pin(data.Buffers[i]);
    }

    public override void Write(ref TextOut o, long row)
    {
        byte* view = _views + (row + Offset) * 16;
        int length = *(int*)view;
        ReadOnlySpan<byte> bytes = length <= 12
            ? new ReadOnlySpan<byte>(view + 4, length)
            : new ReadOnlySpan<byte>(_buffers[*(int*)(view + 8)] + *(int*)(view + 12), length);
        WriteValue(ref o, bytes);
    }
}

/// <summary>Dictionary-encoded values (categoricals, enums): the dictionary's value at the row's index.</summary>
internal sealed unsafe class DictionaryCell(ArrayData data, Pins pins, string name, IArrowType indexType, CsvCell values) : CsvCell(data, pins, name)
{
    private readonly byte* _indices = pins.Pin(data.Buffers[1]);
    private readonly ArrowTypeId _index = indexType.TypeId;

    public override void Write(ref TextOut o, long row)
    {
        long i = row + Offset;
        long index = _index switch
        {
            ArrowTypeId.Int8 => ((sbyte*)_indices)[i],
            ArrowTypeId.UInt8 => _indices[i],
            ArrowTypeId.Int16 => ((short*)_indices)[i],
            ArrowTypeId.UInt16 => ((ushort*)_indices)[i],
            ArrowTypeId.Int32 => ((int*)_indices)[i],
            ArrowTypeId.UInt32 => ((uint*)_indices)[i],
            _ => ((long*)_indices)[i],
        };
        values.Write(ref o, index);
    }
}

/// <summary>Dates: <c>yyyy-MM-dd</c>.</summary>
internal sealed unsafe class DateCell(ArrayData data, Pins pins, string name, CsvSyntax syntax, bool millis) : FormattedCell(data, pins, name, syntax, numeric: false)
{
    private readonly byte* _values = pins.Pin(data.Buffers[1]);

    protected override int MaxBytes => IsoTemporal.MaxDateBytes;

    protected override int Format(long row, Span<byte> destination)
    {
        long days = millis ? Math.DivRem(((long*)_values)[row + Offset], 86_400_000L, out long rest) - (rest < 0 ? 1 : 0) : ((int*)_values)[row + Offset];
        return IsoTemporal.FormatDate(days, destination);
    }
}

/// <summary>
/// Timestamps: <c>yyyy-MM-ddTHH:mm:ss</c> and the fraction of their unit (3, 6 or 9 digits; none for seconds); with a
/// zone, its local time and offset (<c>+0900</c>).
/// </summary>
internal sealed unsafe class TimestampCell(ArrayData data, Pins pins, string name, CsvSyntax syntax, TimeUnit unit, TimeZoneOffsets? zone)
    : FormattedCell(data, pins, name, syntax, numeric: false)
{
    private readonly long* _values = (long*)pins.Pin(data.Buffers[1]);
    private readonly long _perSecond = unit switch
    {
        TimeUnit.Second => 1,
        TimeUnit.Millisecond => 1_000,
        TimeUnit.Microsecond => 1_000_000,
        _ => 1_000_000_000,
    };
    private readonly int _digits = unit switch
    {
        TimeUnit.Second => 0,
        TimeUnit.Millisecond => 3,
        TimeUnit.Microsecond => 6,
        _ => 9,
    };

    protected override int MaxBytes => IsoTemporal.MaxDateBytes + 26;

    protected override int Format(long row, Span<byte> destination)
    {
        long seconds = Math.DivRem(_values[row + Offset], _perSecond, out long rest);
        if (rest < 0)
        {
            seconds--;
            rest += _perSecond;
        }
        int offset = zone?.OffsetAt(seconds) ?? 0;
        seconds += offset;
        long days = Math.DivRem(seconds, 86_400L, out long secondOfDay);
        if (secondOfDay < 0)
        {
            days--;
            secondOfDay += 86_400;
        }
        int at = IsoTemporal.FormatDate(days, destination);
        destination[at++] = (byte)'T';
        at += CsvTemporal.Clock((int)secondOfDay, destination[at..]);
        if (_digits > 0)
        {
            destination[at++] = (byte)'.';
            at += CsvTemporal.Digits(rest, _digits, destination[at..]);
        }
        if (zone is not null) at += CsvTemporal.Offset(offset, destination[at..]);
        return at;
    }
}

/// <summary>Times of day: <c>HH:mm:ss.fffffffff</c> (always nine fraction digits).</summary>
internal sealed unsafe class TimeCell(ArrayData data, Pins pins, string name, CsvSyntax syntax, long unitNanos, bool wide)
    : FormattedCell(data, pins, name, syntax, numeric: false)
{
    private readonly byte* _values = pins.Pin(data.Buffers[1]);

    protected override int MaxBytes => 18;

    protected override int Format(long row, Span<byte> destination)
    {
        long value = wide ? ((long*)_values)[row + Offset] : ((int*)_values)[row + Offset];
        long nanos = value * unitNanos;
        if (nanos is < 0 or >= 86_400_000_000_000L)
            throw new CsvWriteException($"Column '{Name}': {value} is not a time of day.", Name, -1);
        int at = CsvTemporal.Clock((int)(nanos / 1_000_000_000), destination);
        destination[at++] = (byte)'.';
        return at + CsvTemporal.Digits(nanos % 1_000_000_000, 9, destination[at..]);
    }
}

/// <summary>
/// Durations in ISO 8601 seconds (<c>PT1.5S</c>, <c>-PT90S</c>; zero <c>P0D</c>), as the reference engine writes them
/// to JSON (its CSV writer refuses durations).
/// </summary>
internal sealed unsafe class DurationCell(ArrayData data, Pins pins, string name, CsvSyntax syntax, long unitNanos)
    : FormattedCell(data, pins, name, syntax, numeric: false)
{
    private readonly long* _values = (long*)pins.Pin(data.Buffers[1]);

    protected override int MaxBytes => 48;

    protected override int Format(long row, Span<byte> destination)
    {
        long value = _values[row + Offset];
        if (value == 0)
        {
            "P0D"u8.CopyTo(destination);
            return 3;
        }
        long perSecond = 1_000_000_000 / unitNanos;
        UInt128 magnitude = value < 0 ? (UInt128)(-(Int128)value) : (UInt128)value;
        UInt128 seconds = magnitude / (ulong)perSecond;
        long nanos = (long)(magnitude % (ulong)perSecond) * unitNanos;
        int at = 0;
        if (value < 0) destination[at++] = (byte)'-';
        destination[at++] = (byte)'P';
        destination[at++] = (byte)'T';
        seconds.TryFormat(destination[at..], out int written, default, CultureInfo.InvariantCulture);
        at += written;
        if (nanos != 0)
        {
            destination[at++] = (byte)'.';
            int digits = 9;
            while (nanos % 10 == 0)
            {
                nanos /= 10;
                digits--;
            }
            at += CsvTemporal.Digits(nanos, digits, destination[at..]);
        }
        destination[at++] = (byte)'S';
        return at;
    }
}

/// <summary>Pieces of temporal text.</summary>
internal static class CsvTemporal
{
    /// <summary><c>HH:mm:ss</c> of a second of the day.</summary>
    public static int Clock(int secondOfDay, Span<byte> destination)
    {
        Two(secondOfDay / 3600, destination);
        destination[2] = (byte)':';
        Two(secondOfDay / 60 % 60, destination[3..]);
        destination[5] = (byte)':';
        Two(secondOfDay % 60, destination[6..]);
        return 8;
    }

    /// <summary><paramref name="value"/> as exactly <paramref name="count"/> digits (leading zeros).</summary>
    public static int Digits(long value, int count, Span<byte> destination)
    {
        for (int i = count - 1; i >= 0; i--)
        {
            destination[i] = (byte)('0' + value % 10);
            value /= 10;
        }
        return count;
    }

    /// <summary>An offset from UTC as <c>±HHMM</c>, to the nearest minute (local mean times have seconds: +09:18:59 is +0919).</summary>
    public static int Offset(int seconds, Span<byte> destination)
    {
        destination[0] = seconds < 0 ? (byte)'-' : (byte)'+';
        int minutes = (Math.Abs(seconds) + 30) / 60;
        Two(minutes / 60, destination[1..]);
        Two(minutes % 60, destination[3..]);
        return 5;
    }

    private static void Two(int value, Span<byte> destination)
    {
        destination[0] = (byte)('0' + value / 10);
        destination[1] = (byte)('0' + value % 10);
    }
}
