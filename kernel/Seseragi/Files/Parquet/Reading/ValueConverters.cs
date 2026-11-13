using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.InteropServices;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Seseragi.Files.Parquet.Schema;

namespace Kaminari.Seseragi.Files.Parquet.Reading;

/// <summary>
/// Turns PLAIN-encoded fixed-width physical values into the values of an Arrow column. Values that do not fit the
/// Arrow type (a narrowed integer out of range, a decimal wider than its precision, a timestamp out of 64 bits) are
/// an error, never truncated.
/// </summary>
internal interface IValueConverter<T> where T : unmanaged
{
    /// <summary>The bytes of one PLAIN value.</summary>
    static abstract int Width(LeafType type);

    /// <summary>Converts <paramref name="destination"/>.Length values from <paramref name="plain"/>.</summary>
    static abstract void Convert(ReadOnlySpan<byte> plain, Span<T> destination, LeafType type);
}

/// <summary>Values whose Arrow storage is the physical value itself (INT32, INT64, FLOAT, DOUBLE, FLOAT16).</summary>
internal readonly struct SameBits<T> : IValueConverter<T> where T : unmanaged
{
    public static unsafe int Width(LeafType type) => sizeof(T);

    public static void Convert(ReadOnlySpan<byte> plain, Span<T> destination, LeafType type) =>
        MemoryMarshal.Cast<byte, T>(plain[..(destination.Length * Width(type))]).CopyTo(destination);
}

/// <summary>INT32 narrowed to a smaller integer (INT(8|16, signed|unsigned)), checked.</summary>
internal readonly struct NarrowInt32<T> : IValueConverter<T> where T : unmanaged, IBinaryInteger<T>, IMinMaxValue<T>
{
    public static int Width(LeafType type) => 4;

    public static void Convert(ReadOnlySpan<byte> plain, Span<T> destination, LeafType type)
    {
        ReadOnlySpan<int> values = MemoryMarshal.Cast<byte, int>(plain[..(destination.Length * 4)]);
        long min = long.CreateTruncating(T.MinValue), max = long.CreateTruncating(T.MaxValue);
        for (int i = 0; i < destination.Length; i++)
        {
            int v = values[i];
            if (v < min || v > max) throw new ParquetFormatException($"The value {v} does not fit the column's {type.ArrowType.Name} type.");
            destination[i] = T.CreateTruncating(v);
        }
    }
}

/// <summary>DECIMAL in INT32 or INT64, to Decimal128.</summary>
internal readonly struct IntegerDecimal : IValueConverter<Int128>
{
    public static int Width(LeafType type) => type.Kind == ValueKind.DecimalFromInt32 ? 4 : 8;

    public static void Convert(ReadOnlySpan<byte> plain, Span<Int128> destination, LeafType type)
    {
        Int128 bound = Decimals.Bound(type.Precision);
        bool wide = type.Kind == ValueKind.DecimalFromInt64;
        for (int i = 0; i < destination.Length; i++)
        {
            Int128 v = wide ? BinaryPrimitives.ReadInt64LittleEndian(plain[(i * 8)..]) : BinaryPrimitives.ReadInt32LittleEndian(plain[(i * 4)..]);
            if (v >= bound || v <= -bound) throw Decimals.TooWide(v.ToString(System.Globalization.CultureInfo.InvariantCulture), type);
            destination[i] = v;
        }
    }
}

/// <summary>DECIMAL in a FIXED_LEN_BYTE_ARRAY (big-endian two's complement) to Decimal128.</summary>
internal readonly struct FixedBytesDecimal128 : IValueConverter<Int128>
{
    public static int Width(LeafType type) => type.TypeLength;

    public static void Convert(ReadOnlySpan<byte> plain, Span<Int128> destination, LeafType type)
    {
        int width = type.TypeLength;
        for (int i = 0; i < destination.Length; i++) destination[i] = Decimals.ToInt128(plain.Slice(i * width, width), type);
    }
}

/// <summary>DECIMAL in a FIXED_LEN_BYTE_ARRAY to Decimal256 (precision above 38).</summary>
internal readonly struct FixedBytesDecimal256 : IValueConverter<Int256Bits>
{
    public static int Width(LeafType type) => type.TypeLength;

    public static void Convert(ReadOnlySpan<byte> plain, Span<Int256Bits> destination, LeafType type)
    {
        int width = type.TypeLength;
        for (int i = 0; i < destination.Length; i++) destination[i] = Decimals.ToInt256(plain.Slice(i * width, width), type);
    }
}

/// <summary>
/// INT96 (8 bytes of nanoseconds in the day, 4 bytes of Julian day) to a timestamp. In microseconds this is Spark's
/// own conversion, 64-bit arithmetic that wraps as Spark's does: Spark writes timestamps beyond the nanosecond range
/// wrapped, and its reader gets them back this way (apache/parquet-testing int96_from_spark). In nanoseconds a
/// value outside the 64-bit range is an error.
/// </summary>
internal readonly struct Int96Timestamp : IValueConverter<long>
{
    private const long JulianEpoch = 2_440_588; // the Julian day of 1970-01-01
    private const long NanosPerDay = 86_400_000_000_000;
    private const long MicrosPerDay = 86_400_000_000;

    public static int Width(LeafType type) => 12;

    public static void Convert(ReadOnlySpan<byte> plain, Span<long> destination, LeafType type)
    {
        bool nanos = type.Unit == TimeUnit.Nanosecond, micros = type.Unit == TimeUnit.Microsecond;
        long factor = LeafTypes.Nanos(type.Unit);
        for (int i = 0; i < destination.Length; i++)
        {
            ReadOnlySpan<byte> v = plain.Slice(i * 12, 12);
            long nanosOfDay = BinaryPrimitives.ReadInt64LittleEndian(v);
            long days = BinaryPrimitives.ReadInt32LittleEndian(v[8..]) - JulianEpoch;
            if (micros)
            {
                destination[i] = unchecked(days * MicrosPerDay + nanosOfDay / 1000);
                continue;
            }
            Int128 total = (Int128)days * NanosPerDay + nanosOfDay;
            if (total % factor != 0)
                throw new ParquetFormatException($"An INT96 timestamp (Julian day {days + JulianEpoch}) has a finer part than the column's {type.Unit} unit.");
            total /= factor;
            if (total < long.MinValue || total > long.MaxValue)
                throw new ParquetFormatException($"An INT96 timestamp (Julian day {days + JulianEpoch}) does not fit a 64-bit timestamp in {(nanos ? "nanoseconds" : type.Unit.ToString())}.");
            destination[i] = (long)total;
        }
    }
}

/// <summary>INT64 timestamps in another unit than the column's: multiplied (checked) or divided (exactly).</summary>
internal readonly struct RescaledInt64 : IValueConverter<long>
{
    public static int Width(LeafType type) => 8;

    public static void Convert(ReadOnlySpan<byte> plain, Span<long> destination, LeafType type)
    {
        ReadOnlySpan<long> values = MemoryMarshal.Cast<byte, long>(plain[..(destination.Length * 8)]);
        for (int i = 0; i < destination.Length; i++)
        {
            long v = values[i];
            if (type.Multiplier != 1)
            {
                long scaled = v * type.Multiplier;
                if (scaled / type.Multiplier != v) throw new ParquetFormatException($"The timestamp {v} does not fit 64 bits in the column's {type.Unit} unit.");
                v = scaled;
            }
            else if (v % type.Divisor != 0) throw new ParquetFormatException($"The timestamp {v} has a finer part than the column's {type.Unit} unit.");
            else v /= type.Divisor;
            destination[i] = v;
        }
    }
}

/// <summary>Unannotated INT64 narrowed to a smaller integer (the file's Arrow schema says so), checked.</summary>
internal readonly struct NarrowInt64<T> : IValueConverter<T> where T : unmanaged, IBinaryInteger<T>, IMinMaxValue<T>
{
    public static int Width(LeafType type) => 8;

    public static void Convert(ReadOnlySpan<byte> plain, Span<T> destination, LeafType type)
    {
        ReadOnlySpan<long> values = MemoryMarshal.Cast<byte, long>(plain[..(destination.Length * 8)]);
        long min = long.CreateTruncating(T.MinValue), max = long.CreateTruncating(T.MaxValue);
        for (int i = 0; i < destination.Length; i++)
        {
            long v = values[i];
            if (v < min || v > max) throw new ParquetFormatException($"The value {v} does not fit the column's {type.ArrowType.Name} type.");
            destination[i] = T.CreateTruncating(v);
        }
    }
}

/// <summary>A TIMESTAMP in FIXED_LEN_BYTE_ARRAY(12): a signed 96-bit little-endian count of units, narrowed to 64 bits.</summary>
internal readonly struct WideTimestamp : IValueConverter<long>
{
    public static int Width(LeafType type) => 12;

    public static void Convert(ReadOnlySpan<byte> plain, Span<long> destination, LeafType type)
    {
        for (int i = 0; i < destination.Length; i++)
        {
            ReadOnlySpan<byte> v = plain.Slice(i * 12, 12);
            long low = BinaryPrimitives.ReadInt64LittleEndian(v);
            int high = BinaryPrimitives.ReadInt32LittleEndian(v[8..]);
            if (high != (low < 0 ? -1 : 0)) throw new ParquetFormatException("A 96-bit TIMESTAMP value does not fit a 64-bit timestamp.");
            destination[i] = low;
        }
    }
}

/// <summary>INTERVAL (unsigned months, days, milliseconds) to month-day-nanosecond.</summary>
internal readonly struct IntervalValue : IValueConverter<MonthDayNanos>
{
    public static int Width(LeafType type) => 12;

    public static void Convert(ReadOnlySpan<byte> plain, Span<MonthDayNanos> destination, LeafType type)
    {
        for (int i = 0; i < destination.Length; i++)
        {
            ReadOnlySpan<byte> v = plain.Slice(i * 12, 12);
            uint months = BinaryPrimitives.ReadUInt32LittleEndian(v), days = BinaryPrimitives.ReadUInt32LittleEndian(v[4..]);
            uint millis = BinaryPrimitives.ReadUInt32LittleEndian(v[8..]);
            if (months > int.MaxValue || days > int.MaxValue) throw new ParquetFormatException($"An INTERVAL of {months} months and {days} days does not fit Arrow's signed fields.");
            destination[i] = new MonthDayNanos((int)months, (int)days, millis * 1_000_000L);
        }
    }
}

/// <summary>Decimal helpers: precision bounds and big-endian two's complement bytes.</summary>
internal static class Decimals
{
    private static readonly Int128[] Powers = [.. Enumerable.Range(0, 39).Select(p => (Int128)BigInteger.Pow(10, p))];

    /// <summary>10^precision: values must lie strictly between its negation and it.</summary>
    public static Int128 Bound(int precision) => Powers[precision];

    public static ParquetFormatException TooWide(string value, LeafType type) =>
        new($"The decimal value {value} (unscaled) has more digits than the column's precision {type.Precision}.");

    /// <summary>Big-endian two's complement (any length) to Int128, checked against the precision.</summary>
    public static Int128 ToInt128(ReadOnlySpan<byte> bigEndian, LeafType type)
    {
        if (bigEndian.IsEmpty) return 0;
        Int128 value = (sbyte)bigEndian[0] < 0 ? -1 : 0;
        for (int i = 0; i < bigEndian.Length; i++)
        {
            if (i < bigEndian.Length - 16 && bigEndian[i] != ((sbyte)bigEndian[0] < 0 ? 0xFF : 0)) throw TooWide(new BigInteger(bigEndian, isBigEndian: true).ToString(System.Globalization.CultureInfo.InvariantCulture), type);
            value = (value << 8) | bigEndian[i];
        }
        if (bigEndian.Length > 16 && ((sbyte)bigEndian[^16] < 0) != ((sbyte)bigEndian[0] < 0)) throw TooWide(new BigInteger(bigEndian, isBigEndian: true).ToString(System.Globalization.CultureInfo.InvariantCulture), type);
        Int128 bound = Bound(type.Precision);
        if (value >= bound || value <= -bound) throw TooWide(value.ToString(System.Globalization.CultureInfo.InvariantCulture), type);
        return value;
    }

    /// <summary>Big-endian two's complement to a 256-bit little-endian value, checked against the precision.</summary>
    public static Int256Bits ToInt256(ReadOnlySpan<byte> bigEndian, LeafType type)
    {
        var value = new BigInteger(bigEndian, isBigEndian: true);
        if (BigInteger.Abs(value) >= BigInteger.Pow(10, type.Precision)) throw TooWide(value.ToString(System.Globalization.CultureInfo.InvariantCulture), type);
        Span<byte> bytes = stackalloc byte[32];
        bytes.Fill(value.Sign < 0 ? (byte)0xFF : (byte)0);
        value.TryWriteBytes(bytes, out _, isUnsigned: false, isBigEndian: false);
        return MemoryMarshal.Read<Int256Bits>(bytes);
    }
}
