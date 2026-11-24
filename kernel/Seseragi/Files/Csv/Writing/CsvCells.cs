using System.Buffers;
using System.Globalization;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
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
        SeparatorVector = Vector128.Create(Separator);
        QuoteVector = Vector128.Create(Quote);
        byte[] text = System.Text.Encoding.UTF8.GetBytes(options.NullValue);
        Null = Style == CsvQuoteStyle.Always ? Quoted(text) : text;
        LineTerminator = System.Text.Encoding.UTF8.GetBytes(options.LineTerminator);
        DecimalMark = options.DecimalComma ? (byte)',' : (byte)'.';
        Group = options.ThousandsSeparator is { } group ? System.Text.Encoding.UTF8.GetBytes(group) : [];
        Floats = (options.FloatScientific, options.FloatPrecision) switch
        {
            (true, null) => FloatLayout.Scientific,
            (false, null) => FloatLayout.Positional,
            (true, _) => FloatLayout.FixedScientific,
            (_, { }) => FloatLayout.Fixed,
            _ => FloatLayout.Shortest,
        };
        FloatPrecision = options.FloatPrecision ?? 0;
        _dateFormat = options.DateFormat;
        _timeFormat = options.TimeFormat;
        _datetimeFormat = options.DatetimeFormat;
        // The reference engine quotes every float and decimal when the decimal comma is the separator too; a group that
        // holds a byte that needs quotes makes every number quoted.
        bool groupNeedsQuotes = Group.AsSpan().IndexOfAny(Special) >= 0;
        _quoteFractional = options.DecimalComma && Separator == ',' || groupNeedsQuotes;
        _quoteIntegral = groupNeedsQuotes;
    }

    private readonly bool _quoteFractional, _quoteIntegral;
    private readonly string? _dateFormat, _timeFormat, _datetimeFormat;
    private readonly Dictionary<TemporalTarget, TemporalFormat?> _formats = [];
    private readonly object _lock = new();

    /// <summary>
    /// The format of values of <paramref name="target"/> (null: the default text), compiled once.
    /// </summary>
    /// <exception cref="FormatException">The format does not fit the target.</exception>
    public TemporalFormat? FormatFor(TemporalTarget target)
    {
        lock (_lock)
        {
            if (_formats.TryGetValue(target, out TemporalFormat? made)) return made;
            string? text = target switch
            {
                TemporalTarget.Date => _dateFormat,
                TemporalTarget.Time => _timeFormat,
                _ => _datetimeFormat,
            };
            return _formats[target] = text is null ? null : TemporalFormat.Compile(text, target);
        }
    }

    /// <summary>The decimal point of floats and decimals.</summary>
    public byte DecimalMark { get; }

    /// <summary>What goes between each three digits of a number's integer part (empty: nothing).</summary>
    public byte[] Group { get; }

    /// <summary>How floats are laid out.</summary>
    public FloatLayout Floats { get; }

    /// <summary>The decimals of <see cref="FloatLayout.Fixed"/> and <see cref="FloatLayout.FixedScientific"/>.</summary>
    public int FloatPrecision { get; }

    public byte Separator { get; }

    public byte Quote { get; }

    /// <summary><see cref="Separator"/> in every lane (string views compare 16 bytes at once).</summary>
    public Vector128<byte> SeparatorVector { get; }

    /// <summary><see cref="Quote"/> in every lane.</summary>
    public Vector128<byte> QuoteVector { get; }

    public CsvQuoteStyle Style { get; }

    /// <summary>The bytes that make a field need quotes: the separator, the quote, CR, LF.</summary>
    public SearchValues<byte> Special { get; }

    /// <summary>A null as written.</summary>
    public byte[] Null { get; }

    public byte[] LineTerminator { get; }

    /// <summary>How a column of values written by format is quoted.</summary>
    public CsvQuoting ForFormatted(CsvValueKind kind) => (Style, kind) switch
    {
        (CsvQuoteStyle.Always, _) => CsvQuoting.Always,
        (CsvQuoteStyle.Never, _) => CsvQuoting.None,
        (CsvQuoteStyle.NonNumeric, CsvValueKind.Other) => CsvQuoting.Always,
        (_, CsvValueKind.Fractional) when _quoteFractional => CsvQuoting.Always,
        (_, CsvValueKind.Integral) when _quoteIntegral => CsvQuoting.Always,
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

/// <summary>What a column written by format holds, for its quoting.</summary>
internal enum CsvValueKind
{
    /// <summary>Integers.</summary>
    Integral,

    /// <summary>Floats and decimals.</summary>
    Fractional,

    /// <summary>Booleans, dates, times, durations.</summary>
    Other,
}

/// <summary>How a writer lays floats out (<see cref="CsvWriteOptions.FloatScientific"/>, <see cref="CsvWriteOptions.FloatPrecision"/>).</summary>
internal enum FloatLayout
{
    Shortest,
    Scientific,
    Positional,
    Fixed,
    FixedScientific,
}

/// <summary>
/// Writes the values of one Arrow array as CSV fields, row by row (a row is the array's own index, before its offset). A
/// value that cannot be written is a <see cref="CsvWriteException"/> (its row added by the writer).
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

    /// <summary>Why a column's values cannot be written with the writer's formats, or null when they can.</summary>
    public static string? FormatRefusal(Field field, CsvSyntax syntax)
    {
        IArrowType type = field.DataType is DictionaryType d ? d.ValueType : field.DataType;
        TemporalTarget? target = type switch
        {
            Date32Type or Date64Type => TemporalTarget.Date,
            Time32Type or Time64Type => TemporalTarget.Time,
            TimestampType { Timezone: { Length: > 0 } } => TemporalTarget.Zoned,
            TimestampType => TemporalTarget.Naive,
            _ => null,
        };
        if (target is not { } t) return null;
        try
        {
            if (syntax.FormatFor(t) is { WritesZone: true } && type is TimestampType { Timezone: { } zone } && TimeZoneOffsets.For(zone) is { HasAbbreviations: false })
                return $"Column '{field.Name}': the time zone '{zone}' has no abbreviations here (no time zone database files) for %Z: write its offset (%z) instead.";
            return null;
        }
        catch (FormatException e)
        {
            return $"Column '{field.Name}': {e.Message}";
        }
    }

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
internal abstract class FormattedCell(ArrayData data, Pins pins, string name, CsvSyntax syntax, CsvValueKind kind) : CsvCell(data, pins, name)
{
    private readonly CsvQuoting _quoting = syntax.ForFormatted(kind);
    private readonly byte _quote = syntax.Quote;
    private int _maxBytes = -1; // MaxBytes, read once (a property of the column)

    /// <summary>The most bytes a value's text takes.</summary>
    protected abstract int MaxBytes { get; }

    /// <summary>A row's value as text into <paramref name="destination"/> (at least <see cref="MaxBytes"/> long): its length.</summary>
    protected abstract int Format(long row, Span<byte> destination);

    public override void Write(ref TextOut o, long row)
    {
        int most = _maxBytes >= 0 ? _maxBytes : _maxBytes = MaxBytes;
        if (_quoting == CsvQuoting.None)
        {
            o.Advance(Format(row, o.Room(most)));
            return;
        }
        Span<byte> room = o.Room(most + 2);
        room[0] = _quote;
        int length = Format(row, room[1..]);
        room[length + 1] = _quote;
        o.Advance(length + 2);
    }
}

/// <summary>
/// Numbers: their text (ASCII digits, '-', a '.' point) then its integer digits grouped by three and its point made the
/// writer's (<see cref="CsvWriteOptions.ThousandsSeparator"/>, <see cref="CsvWriteOptions.DecimalComma"/>).
/// </summary>
internal abstract class NumberCell(ArrayData data, Pins pins, string name, CsvSyntax syntax, CsvValueKind kind) : FormattedCell(data, pins, name, syntax, kind)
{
    private readonly byte _mark = syntax.DecimalMark;
    private readonly byte[] _group = syntax.Group;

    /// <summary>The most bytes of a value's plain text (before grouping).</summary>
    protected abstract int MaxPlainBytes { get; }

    /// <summary>A row's value as plain text (a '.' point, no groups).</summary>
    protected abstract int FormatPlain(long row, Span<byte> destination);

    protected sealed override int MaxBytes => MaxPlainBytes + (_group.Length == 0 ? 0 : (MaxPlainBytes / 3 + 1) * _group.Length);

    protected sealed override int Format(long row, Span<byte> destination)
    {
        int length = FormatPlain(row, destination);
        if (_mark == '.' && _group.Length == 0) return length;
        int start = length > 0 && destination[0] == '-' ? 1 : 0, end = start;
        while (end < length && char.IsAsciiDigit((char)destination[end])) end++;
        if (end == start) return length; // NaN, inf
        if (end < length && destination[end] == '.') destination[end] = _mark;
        int run = end - start;
        if (_group.Length == 0 || run <= 3) return length;
        int grow = (run - 1) / 3 * _group.Length;
        destination[end..length].CopyTo(destination[(end + grow)..]);
        // The integer digits moved right, a group put before each three from the right.
        int from = end - 1, to = end + grow - 1, k = 0;
        while (from >= start)
        {
            destination[to--] = destination[from--];
            if (++k % 3 == 0 && from >= start)
            {
                to -= _group.Length;
                _group.CopyTo(destination[(to + 1)..]);
            }
        }
        return length + grow;
    }
}

internal sealed unsafe class BooleanCell(ArrayData data, Pins pins, string name, CsvSyntax syntax) : FormattedCell(data, pins, name, syntax, CsvValueKind.Other)
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

internal sealed unsafe class IntegerCell<T>(ArrayData data, Pins pins, string name, CsvSyntax syntax) : NumberCell(data, pins, name, syntax, CsvValueKind.Integral)
    where T : unmanaged, IBinaryInteger<T>, IUtf8SpanFormattable
{
    private readonly T* _values = (T*)pins.Pin(data.Buffers[1]);
    // Plain text alone (no quotes, a '.' point, no groups): written here in one call, not through FormattedCell.Write.
    private readonly bool _plain = syntax.DecimalMark == '.' && syntax.Group.Length == 0 && syntax.ForFormatted(CsvValueKind.Integral) == CsvQuoting.None;

    protected override int MaxPlainBytes => 21;

    public override void Write(ref TextOut o, long row)
    {
        if (!_plain)
        {
            base.Write(ref o, row);
            return;
        }
        _values[row + Offset].TryFormat(o.Room(21), out int written, default, CultureInfo.InvariantCulture);
        o.Advance(written);
    }

    protected override int FormatPlain(long row, Span<byte> destination)
    {
        _values[row + Offset].TryFormat(destination, out int written, default, CultureInfo.InvariantCulture);
        return written;
    }
}

/// <summary>
/// Floats: their shortest round-trip digits as the reference engine writes them (<see cref="FloatText.Shortest{T}"/>),
/// <c>NaN</c>, <c>inf</c>, <c>-inf</c>. Half floats are written as the float of their value.
/// </summary>
internal sealed unsafe class FloatCell<T>(ArrayData data, Pins pins, string name, CsvSyntax syntax) : NumberCell(data, pins, name, syntax, CsvValueKind.Fractional)
    where T : unmanaged, IFloatingPointIeee754<T>, IUtf8SpanFormattable
{
    private readonly T* _values = (T*)pins.Pin(data.Buffers[1]);
    private readonly FloatLayout _layout = syntax.Floats;
    private readonly int _decimals = syntax.FloatPrecision;
    // Plain text alone (no quotes, a '.' point, no groups): written here in one call, not through FormattedCell.Write.
    private readonly bool _plain = syntax.DecimalMark == '.' && syntax.Group.Length == 0 && syntax.ForFormatted(CsvValueKind.Fractional) == CsvQuoting.None;
    private int _maxPlain = -1;

    public override void Write(ref TextOut o, long row)
    {
        if (!_plain)
        {
            base.Write(ref o, row);
            return;
        }
        int most = _maxPlain >= 0 ? _maxPlain : _maxPlain = MaxPlainBytes;
        o.Advance(FormatPlain(row, o.Room(most)));
    }

    protected override int MaxPlainBytes => _layout switch
    {
        FloatLayout.Positional => FloatText.MaxPositionalBytes,
        FloatLayout.Fixed => FloatText.MaxFixedBytes(_decimals),
        FloatLayout.FixedScientific => _decimals + 32,
        _ => FloatText.MaxShortestBytes,
    };

    protected override int FormatPlain(long row, Span<byte> destination)
    {
        T value = _values[row + Offset];
        if (T.IsNaN(value)) return Copy("NaN"u8, destination);
        if (T.IsInfinity(value)) return Copy(T.IsNegative(value) ? "-inf"u8 : "inf"u8, destination);
        return _layout switch
        {
            FloatLayout.Scientific => FloatText.Scientific(value, destination),
            FloatLayout.Positional => FloatText.Positional(value, destination),
            FloatLayout.Fixed => FloatText.Fixed(double.CreateChecked(value), _decimals, destination),
            FloatLayout.FixedScientific => FloatText.FixedScientific(double.CreateChecked(value), _decimals, destination),
            _ => FloatText.Shortest(value, destination),
        };
    }

    private static int Copy(ReadOnlySpan<byte> text, Span<byte> destination)
    {
        text.CopyTo(destination);
        return text.Length;
    }
}

/// <summary>Decimals (32, 64 or 128 bits) with every digit of their scale; 128-bit integers (decimal(38, 0) so marked) as integers.</summary>
internal sealed unsafe class DecimalCell(ArrayData data, Pins pins, string name, CsvSyntax syntax, int scale, string? integer, int width)
    : NumberCell(data, pins, name, syntax, integer is null && scale > 0 ? CsvValueKind.Fractional : CsvValueKind.Integral)
{
    private readonly byte* _values = pins.Pin(data.Buffers[1]);

    protected override int MaxPlainBytes => 44 + Math.Abs(scale);

    protected override int FormatPlain(long row, Span<byte> destination)
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
        // The digits written in place, then moved apart for the point (no buffer between).
        bool negative = value < 0;
        int at = negative ? 1 : 0;
        if (negative) destination[0] = (byte)'-';
        ((UInt128)(negative ? -value : value)).TryFormat(destination[at..], out int length, default, CultureInfo.InvariantCulture);
        if (scale <= 0)
        {
            at += length;
            if (value != 0)
                for (int i = 0; i < -scale; i++) destination[at++] = (byte)'0';
            return at;
        }
        int whole = length - scale;
        if (whole > 0)
        {
            // ddd.ff: the fraction one place right, the point before it.
            destination.Slice(at + whole, scale).CopyTo(destination[(at + whole + 1)..]);
            destination[at + whole] = (byte)'.';
            return at + length + 1;
        }
        // 0.00ff: the digits moved right past "0." and the zeros.
        int lead = 2 - whole;
        destination.Slice(at, length).CopyTo(destination[(at + lead)..]);
        destination[at] = (byte)'0';
        destination[at + 1] = (byte)'.';
        destination.Slice(at + 2, -whole).Fill((byte)'0');
        return at + lead + length;
    }
}

/// <summary>
/// Strings, quoted as the style says. Their bytes are written as they are: Arrow strings are UTF-8 (the format's rule,
/// not checked here, as the reference engine does not); an encoding's transcoder refuses bytes that are not.
/// </summary>
internal abstract class TextCell(ArrayData data, Pins pins, string name, CsvSyntax syntax) : CsvCell(data, pins, name)
{
    protected void WriteValue(ref TextOut o, scoped ReadOnlySpan<byte> bytes) => syntax.WriteText(ref o, bytes);
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

/// <summary>
/// String views: inline up to 12 bytes, else in one of the data buffers. An inline one with quoting as needed is checked
/// for bytes that need quotes with one vector compare of its view and written by two stores (no calls per value).
/// </summary>
internal sealed unsafe class ViewCell : TextCell
{
    private readonly byte* _views;
    private readonly byte*[] _buffers;
    private readonly bool _inline;              // quoting as needed, vectors at hand: the inline path
    private readonly CsvSyntax _syntax;

    public ViewCell(ArrayData data, Pins pins, string name, CsvSyntax syntax) : base(data, pins, name, syntax)
    {
        _views = pins.Pin(data.Buffers[1]);
        _buffers = data.Buffers.Length > 2 ? new byte*[data.Buffers.Length - 2] : [];
        for (int i = 2; i < data.Buffers.Length; i++) _buffers[i - 2] = pins.Pin(data.Buffers[i]);
        _inline = syntax.Style == CsvQuoteStyle.Necessary && Vector128.IsHardwareAccelerated;
        _syntax = syntax;
    }

    public override void Write(ref TextOut o, long row)
    {
        byte* view = _views + (row + Offset) * 16;
        int length = *(int*)view;
        if (_inline && (uint)(length - 1) < 12)
        {
            // The view's 16 bytes at once (its own memory): the inline bytes are lanes 4 to 4 + length.
            Vector128<byte> v = Vector128.Load(view);
            Vector128<byte> special = Vector128.Equals(v, _syntax.SeparatorVector) | Vector128.Equals(v, _syntax.QuoteVector)
                | Vector128.Equals(v, Vector128.Create((byte)'\r')) | Vector128.Equals(v, Vector128.Create((byte)'\n'));
            if ((special.ExtractMostSignificantBits() >> 4 & ((1u << length) - 1)) == 0)
            {
                // Twelve bytes stored whatever the length (the room holds them), the length kept.
                Span<byte> room = o.Room(12);
                Unsafe.WriteUnaligned(ref room[0], Unsafe.ReadUnaligned<ulong>(view + 4));
                Unsafe.WriteUnaligned(ref room[8], Unsafe.ReadUnaligned<uint>(view + 12));
                o.Advance(length);
                return;
            }
        }
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

/// <summary>Dates: <c>yyyy-MM-dd</c>, or the writer's format.</summary>
internal sealed unsafe class DateCell(ArrayData data, Pins pins, string name, CsvSyntax syntax, bool millis) : FormattedCell(data, pins, name, syntax, CsvValueKind.Other)
{
    private readonly byte* _values = pins.Pin(data.Buffers[1]);
    private readonly TemporalFormat? _format = syntax.FormatFor(TemporalTarget.Date);

    protected override int MaxBytes => _format?.MaxBytes ?? IsoTemporal.MaxDateBytes;

    protected override int Format(long row, Span<byte> destination)
    {
        long days = millis ? Math.DivRem(((long*)_values)[row + Offset], 86_400_000L, out long rest) - (rest < 0 ? 1 : 0) : ((int*)_values)[row + Offset];
        return _format is null ? IsoTemporal.FormatDate(days, destination) : _format.Format(new TemporalValue { Days = days }, destination);
    }
}

/// <summary>
/// Timestamps: <c>yyyy-MM-ddTHH:mm:ss</c> and the fraction of their unit (3, 6 or 9 digits; none for seconds); with a
/// zone, its local time and offset (<c>+0900</c>).
/// </summary>
internal sealed unsafe class TimestampCell(ArrayData data, Pins pins, string name, CsvSyntax syntax, TimeUnit unit, TimeZoneOffsets? zone)
    : FormattedCell(data, pins, name, syntax, CsvValueKind.Other)
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
    private readonly TemporalFormat? _format = syntax.FormatFor(zone is null ? TemporalTarget.Naive : TemporalTarget.Zoned);

    protected override int MaxBytes => _format?.MaxBytes ?? IsoTemporal.MaxDateBytes + 26;

    protected override int Format(long row, Span<byte> destination)
    {
        long value = _values[row + Offset], rest;
        // Constant divisors (multiplications), not a division by a variable per value.
        long seconds = _digits switch
        {
            6 => Math.DivRem(value, 1_000_000L, out rest),
            3 => Math.DivRem(value, 1_000L, out rest),
            9 => Math.DivRem(value, 1_000_000_000L, out rest),
            _ => Math.DivRem(value, 1L, out rest),
        };
        if (rest < 0)
        {
            seconds--;
            rest += _perSecond;
        }
        if (_format is not null) return Formatted(seconds, (int)(rest * (1_000_000_000 / _perSecond)), destination);
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

    // In the writer's format: the zone's local time (its UTC time for .NET's u and r), its offset and abbreviation.
    private int Formatted(long seconds, int nanos, Span<byte> destination)
    {
        int offset = zone is null || _format!.Utc ? 0 : zone.OffsetAt(seconds);
        long local = seconds + offset;
        long days = Math.DivRem(local, 86_400L, out long secondOfDay);
        if (secondOfDay < 0)
        {
            days--;
            secondOfDay += 86_400;
        }
        return _format!.Format(new TemporalValue
        {
            Days = days, SecondOfDay = (int)secondOfDay, Nanosecond = nanos, Offset = offset, UnixSeconds = seconds,
            Zone = _format.WritesZone ? zone?.AbbreviationAt(seconds) : null,
        }, destination);
    }
}

/// <summary>Times of day: <c>HH:mm:ss.fffffffff</c> (always nine fraction digits).</summary>
internal sealed unsafe class TimeCell(ArrayData data, Pins pins, string name, CsvSyntax syntax, long unitNanos, bool wide)
    : FormattedCell(data, pins, name, syntax, CsvValueKind.Other)
{
    private readonly byte* _values = pins.Pin(data.Buffers[1]);
    private readonly TemporalFormat? _format = syntax.FormatFor(TemporalTarget.Time);

    protected override int MaxBytes => _format?.MaxBytes ?? 18;

    protected override int Format(long row, Span<byte> destination)
    {
        long value = wide ? ((long*)_values)[row + Offset] : ((int*)_values)[row + Offset];
        long nanos = value * unitNanos;
        if (nanos is < 0 or >= 86_400_000_000_000L)
            throw new CsvWriteException($"Column '{Name}': {value} is not a time of day.", Name, -1);
        if (_format is not null)
            return _format.Format(new TemporalValue { SecondOfDay = (int)(nanos / 1_000_000_000), Nanosecond = (int)(nanos % 1_000_000_000) }, destination);
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
    : FormattedCell(data, pins, name, syntax, CsvValueKind.Other)
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

    private static ReadOnlySpan<byte> Pairs => "00010203040506070809101112131415161718192021222324252627282930313233343536373839404142434445464748495051525354555657585960616263646566676869707172737475767778798081828384858687888990919293949596979899"u8;

    /// <summary><paramref name="value"/> (not negative) as exactly <paramref name="count"/> digits (leading zeros), two at a time.</summary>
    public static int Digits(long value, int count, Span<byte> destination)
    {
        int i = count;
        ulong v = (ulong)value;
        while (i >= 2)
        {
            ulong q = v / 100;
            int pair = (int)(v - q * 100) * 2;
            v = q;
            i -= 2;
            destination[i] = Pairs[pair];
            destination[i + 1] = Pairs[pair + 1];
        }
        if (i == 1) destination[0] = (byte)('0' + (int)(v % 10));
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
