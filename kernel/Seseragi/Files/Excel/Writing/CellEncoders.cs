using System.Buffers.Text;
using System.Globalization;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Kaminari.Seseragi.Files.Excel.Writing;

/// <summary>
/// Writes the values of one Arrow column as cells. <see cref="Bind"/> takes each batch's array;
/// <see cref="Encode"/> writes one value (nothing for null) and returns its width in characters for autofit (-1 for
/// null).
/// </summary>
internal abstract class CellEncoder
{
    protected IArrowArray Array = null!;

    public virtual void Bind(IArrowArray array) => Array = array;

    public abstract int Encode(int index, CellSink sink);

    protected static ReadOnlySpan<T> Values<T>(IArrowArray a) where T : unmanaged =>
        MemoryMarshal.Cast<byte, T>(a.Data.Buffers[1].Span)[a.Offset..];

    /// <summary>The encoder for an Arrow type, or null when Excel cannot hold the type.</summary>
    public static CellEncoder? Create(IArrowType type, ExcelStyles styles) => type switch
    {
        NullType => new NullEncoder(),
        BooleanType => new BooleanEncoder(),
        Int8Type => new SmallIntegerEncoder<sbyte>(),
        Int16Type => new SmallIntegerEncoder<short>(),
        Int32Type => new SmallIntegerEncoder<int>(),
        UInt8Type => new SmallIntegerEncoder<byte>(),
        UInt16Type => new SmallIntegerEncoder<ushort>(),
        UInt32Type => new SmallIntegerEncoder<uint>(),
        Int64Type => new Int64Encoder(),
        UInt64Type => new UInt64Encoder(),
        HalfFloatType => new HalfEncoder(),
        FloatType => new FloatEncoder(),
        DoubleType => new DoubleEncoder(),
        Decimal32Type d => new DecimalEncoder(4, d.Scale),
        Decimal64Type d => new DecimalEncoder(8, d.Scale),
        Decimal128Type d => new DecimalEncoder(16, d.Scale),
        Decimal256Type d => new DecimalEncoder(32, d.Scale),
        StringType => new StringEncoder(),
        LargeStringType => new LargeStringEncoder(),
        StringViewType => new StringViewEncoder(),
        DictionaryType { ValueType: StringType or LargeStringType or StringViewType } d when Create(d.ValueType, styles) is { } values
            => DictionaryEncoder.For(d.IndexType, values),
        Date32Type => new DateEncoder(styles.Date, 1),
        Date64Type => new DateEncoder(styles.Date, 86_400_000),
        TimestampType t => new TimestampEncoder(styles.DateTime, t),
        Time32Type t => new TimeEncoder(styles.Time, t.Unit, 4),
        Time64Type t => new TimeEncoder(styles.Time, t.Unit, 8),
        DurationType t => new DurationEncoder(styles.Duration, t.Unit),
        _ => null,
    };

    /// <summary>Microseconds of a value in <paramref name="unit"/>; false when a nanosecond value has a sub-microsecond part.</summary>
    protected static bool TryMicros(long value, TimeUnit unit, out long micros)
    {
        switch (unit)
        {
            case TimeUnit.Second: micros = checked(value * 1_000_000); return true;
            case TimeUnit.Millisecond: micros = checked(value * 1_000); return true;
            case TimeUnit.Microsecond: micros = value; return true;
            default:
                micros = Math.DivRem(value, 1000, out long rem);
                return rem == 0;
        }
    }

    protected static int Digits(long v)
    {
        ulong u = v < 0 ? (ulong)(-(v + 1)) + 1 : (ulong)v;
        int n = v < 0 ? 2 : 1;
        while (u >= 10)
        {
            u /= 10;
            n++;
        }
        return n;
    }
}

internal sealed class NullEncoder : CellEncoder
{
    public override int Encode(int index, CellSink sink) => -1;
}

internal sealed class BooleanEncoder : CellEncoder
{
    public override int Encode(int index, CellSink sink)
    {
        if (Array.IsNull(index)) return -1;
        bool v = ((BooleanArray)Array).GetValue(index)!.Value;
        sink.Boolean(v);
        return v ? 4 : 5;
    }
}

internal sealed class SmallIntegerEncoder<T> : CellEncoder where T : unmanaged, IBinaryInteger<T>
{
    public override int Encode(int index, CellSink sink)
    {
        if (Array.IsNull(index)) return -1;
        long v = long.CreateTruncating(Values<T>(Array)[index]);
        sink.Integer(v);
        return Digits(v);
    }
}

/// <summary>Int64 by value: a number up to 2^53 in magnitude (exact as a double), text beyond.</summary>
internal sealed class Int64Encoder : CellEncoder
{
    private const long Exact = 1L << 53;

    public override int Encode(int index, CellSink sink)
    {
        if (Array.IsNull(index)) return -1;
        long v = Values<long>(Array)[index];
        if (v is >= -Exact and <= Exact)
        {
            sink.Integer(v);
            return Digits(v);
        }
        Span<byte> text = stackalloc byte[20];
        Utf8Formatter.TryFormat(v, text, out int n);
        sink.String(text[..n]);
        return n;
    }
}

internal sealed class UInt64Encoder : CellEncoder
{
    public override int Encode(int index, CellSink sink)
    {
        if (Array.IsNull(index)) return -1;
        ulong v = Values<ulong>(Array)[index];
        Span<byte> text = stackalloc byte[20];
        Utf8Formatter.TryFormat(v, text, out int n);
        if (v <= 1UL << 53) sink.UnsignedInteger(v);
        else sink.String(text[..n]);
        return n;
    }
}

internal sealed class HalfEncoder : CellEncoder
{
    public override int Encode(int index, CellSink sink)
    {
        if (Array.IsNull(index)) return -1;
        float v = (float)Values<Half>(Array)[index];
        if (!float.IsFinite(v))
        {
            sink.NumError();
            return 5;
        }
        sink.Single(v);
        return Math.Abs(v) < 1e15f ? Math.Min(12, Digits((long)v) + (v == MathF.Floor(v) ? 0 : 7)) : 12;
    }
}

internal sealed class FloatEncoder : CellEncoder
{
    public override int Encode(int index, CellSink sink)
    {
        if (Array.IsNull(index)) return -1;
        float v = Values<float>(Array)[index];
        if (!float.IsFinite(v))
        {
            sink.NumError();
            return 5;
        }
        sink.Single(v);
        return Math.Abs(v) < 1e15f ? Math.Min(12, Digits((long)v) + (v == MathF.Floor(v) ? 0 : 7)) : 12;
    }
}

internal sealed class DoubleEncoder : CellEncoder
{
    public override int Encode(int index, CellSink sink)
    {
        if (Array.IsNull(index)) return -1;
        double v = Values<double>(Array)[index];
        if (!double.IsFinite(v))
        {
            sink.NumError();
            return 5;
        }
        sink.Number(v);
        return Math.Abs(v) < 1e15 ? Math.Min(12, Digits((long)v) + (v == Math.Floor(v) ? 0 : 7)) : 12;
    }
}

/// <summary>
/// Decimals by value: a number when the shortest round-trip text of the nearest double is the same decimal value
/// (0.1, 123.45), text otherwise (more digits than a double holds).
/// </summary>
internal sealed class DecimalEncoder(int width, int scale) : CellEncoder
{
    public override int Encode(int index, CellSink sink)
    {
        if (Array.IsNull(index)) return -1;
        var bytes = Array.Data.Buffers[1].Span.Slice((Array.Offset + index) * width, width);
        BigInteger unscaled = new(bytes, isUnsigned: false, isBigEndian: false);
        Span<byte> text = stackalloc byte[100];
        int n = Format(unscaled, scale, text);
        var exact = text[..n];
        if (double.TryParse(exact, NumberStyles.Float, CultureInfo.InvariantCulture, out double d) && double.IsFinite(d))
        {
            Span<byte> shortest = stackalloc byte[32];
            d.TryFormat(shortest, out int m, default, CultureInfo.InvariantCulture);
            if (SameValue(exact, shortest[..m]))
            {
                sink.Number(d);
                return m;
            }
        }
        sink.String(exact);
        return n;
    }

    private static int Format(BigInteger unscaled, int scale, Span<byte> dest)
    {
        bool negative = unscaled.Sign < 0;
        string digits = BigInteger.Abs(unscaled).ToString(CultureInfo.InvariantCulture);
        int n = 0;
        if (negative) dest[n++] = (byte)'-';
        if (scale <= 0)
        {
            n += Encoding.ASCII.GetBytes(digits, dest[n..]);
            for (int i = 0; i < -scale && digits != "0"; i++) dest[n++] = (byte)'0';
            return n;
        }
        if (digits.Length <= scale) digits = new string('0', scale - digits.Length + 1) + digits;
        n += Encoding.ASCII.GetBytes(digits.AsSpan(0, digits.Length - scale), dest[n..]);
        dest[n++] = (byte)'.';
        n += Encoding.ASCII.GetBytes(digits.AsSpan(digits.Length - scale), dest[n..]);
        return n;
    }

    // Whether two decimal texts (optionally with an exponent) denote the same value.
    internal static bool SameValue(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => Normalize(a) == Normalize(b);

    private static (bool Negative, string Digits, int Exponent) Normalize(ReadOnlySpan<byte> text)
    {
        bool negative = false;
        int i = 0;
        if (i < text.Length && text[i] is (byte)'-' or (byte)'+') negative = text[i++] == (byte)'-';
        var digits = new StringBuilder();
        int exponent = 0;
        bool afterPoint = false;
        for (; i < text.Length; i++)
        {
            byte c = text[i];
            if (c == (byte)'.') afterPoint = true;
            else if (c is (byte)'e' or (byte)'E')
            {
                exponent += int.Parse(text[(i + 1)..], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
                break;
            }
            else
            {
                digits.Append((char)c);
                if (afterPoint) exponent--;
            }
        }
        string d = digits.ToString().TrimStart('0');
        int trailing = d.Length - d.TrimEnd('0').Length;
        d = d.TrimEnd('0');
        if (d.Length == 0) return (false, "0", 0);
        return (negative, d, exponent + trailing);
    }
}

internal sealed class StringEncoder : CellEncoder
{
    public override int Encode(int index, CellSink sink)
    {
        if (Array.IsNull(index)) return -1;
        var offsets = Values<int>(Array);
        int start = offsets[index];
        var bytes = Array.Data.Buffers[2].Span.Slice(start, offsets[index + 1] - start);
        sink.String(bytes);
        return bytes.Length;
    }
}

internal sealed class LargeStringEncoder : CellEncoder
{
    public override int Encode(int index, CellSink sink)
    {
        if (Array.IsNull(index)) return -1;
        var offsets = Values<long>(Array);
        long start = offsets[index];
        var bytes = Array.Data.Buffers[2].Span.Slice(checked((int)start), checked((int)(offsets[index + 1] - start)));
        sink.String(bytes);
        return bytes.Length;
    }
}

internal sealed class StringViewEncoder : CellEncoder
{
    public override int Encode(int index, CellSink sink)
    {
        if (Array.IsNull(index)) return -1;
        var bytes = ((BinaryViewArray)Array).GetBytes(index);
        sink.String(bytes);
        return bytes.Length;
    }
}

internal sealed class DictionaryEncoder<TIndex>(CellEncoder values) : CellEncoder where TIndex : unmanaged, IBinaryInteger<TIndex>
{
    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        values.Bind(((DictionaryArray)array).Dictionary);
    }

    public override int Encode(int index, CellSink sink)
    {
        if (Array.IsNull(index)) return -1;
        var indices = ((DictionaryArray)Array).Indices;
        int at = int.CreateChecked(MemoryMarshal.Cast<byte, TIndex>(indices.Data.Buffers[1].Span)[indices.Offset + index]);
        return values.Encode(at, sink);
    }
}

internal static class DictionaryEncoder
{
    public static CellEncoder? For(IArrowType indexType, CellEncoder values) => indexType switch
    {
        Int8Type => new DictionaryEncoder<sbyte>(values),
        Int16Type => new DictionaryEncoder<short>(values),
        Int32Type => new DictionaryEncoder<int>(values),
        Int64Type => new DictionaryEncoder<long>(values),
        UInt8Type => new DictionaryEncoder<byte>(values),
        UInt16Type => new DictionaryEncoder<ushort>(values),
        UInt32Type => new DictionaryEncoder<uint>(values),
        UInt64Type => new DictionaryEncoder<ulong>(values),
        _ => null,
    };
}

/// <summary>Date32 (days) or Date64 (milliseconds, midnight only) as a serial number with the date format.</summary>
internal sealed class DateEncoder(ExcelStyle style, long perDay) : CellEncoder
{
    public override int Encode(int index, CellSink sink)
    {
        if (Array.IsNull(index)) return -1;
        long days;
        if (perDay == 1)
        {
            days = Values<int>(Array)[index];
        }
        else
        {
            days = Math.DivRem(Values<long>(Array)[index], perDay, out long rem);
            if (rem != 0) throw sink.Refuse("a Date64 value is not at midnight");
        }
        if (!ExcelDates.TryToSerial(days, 0, out double serial))
            throw sink.Refuse($"the date {DateOnly.FromDayNumber((int)(days + 719_162)):yyyy-MM-dd} is outside Excel's dates (1900-01-01 to 9999-12-31)");
        sink.Integer((long)serial, style.Index); // whole days: integer formatting is much cheaper than a double's
        return style.Width;
    }
}

/// <summary>Timestamps of any unit; a zoned value is written as the wall-clock time in its zone.</summary>
internal sealed class TimestampEncoder(ExcelStyle style, TimestampType type) : CellEncoder
{
    private readonly TimeZoneInfo? _zone = string.IsNullOrEmpty(type.Timezone) ? null : Reading.TimeZones.Find(type.Timezone);

    public override int Encode(int index, CellSink sink)
    {
        if (Array.IsNull(index)) return -1;
        if (!TryMicros(Values<long>(Array)[index], type.Unit, out long micros))
            throw sink.Refuse("a timestamp has a sub-microsecond part, which Excel cannot hold");
        if (_zone is not null && _zone != TimeZoneInfo.Utc)
        {
            var utc = new DateTime(DateTime.UnixEpoch.Ticks + micros * 10, DateTimeKind.Utc);
            micros = (TimeZoneInfo.ConvertTimeFromUtc(utc, _zone).Ticks - DateTime.UnixEpoch.Ticks) / 10;
        }
        var (days, ofDay) = ExcelDates.SplitMicros(micros);
        if (!ExcelDates.TryToSerial(days, ofDay, out double serial))
            throw sink.Refuse("a timestamp is outside Excel's dates (1900-01-01 to 9999-12-31)");
        if (ofDay == 0) sink.Integer((long)serial, style.Index);
        else sink.Number(serial, style.Index);
        return style.Width;
    }
}

internal sealed class TimeEncoder(ExcelStyle style, TimeUnit unit, int width) : CellEncoder
{
    public override int Encode(int index, CellSink sink)
    {
        if (Array.IsNull(index)) return -1;
        long raw = width == 4 ? Values<int>(Array)[index] : Values<long>(Array)[index];
        if (!TryMicros(raw, unit, out long micros))
            throw sink.Refuse("a time has a sub-microsecond part, which Excel cannot hold");
        sink.Number(micros / (double)ExcelDates.MicrosPerDay, style.Index);
        return style.Width;
    }
}

internal sealed class DurationEncoder(ExcelStyle style, TimeUnit unit) : CellEncoder
{
    public override int Encode(int index, CellSink sink)
    {
        if (Array.IsNull(index)) return -1;
        if (!TryMicros(Values<long>(Array)[index], unit, out long micros))
            throw sink.Refuse("a duration has a sub-microsecond part, which Excel cannot hold");
        long days = Math.DivRem(micros, ExcelDates.MicrosPerDay, out long rem);
        sink.Number(days + rem / (double)ExcelDates.MicrosPerDay, style.Index);
        return style.Width;
    }
}
