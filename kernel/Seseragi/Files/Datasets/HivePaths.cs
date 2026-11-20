using System.Globalization;
using System.Numerics;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Excel.Reading;

namespace Kaminari.Seseragi.Files;

/// <summary>
/// Hive partition directories as writers make them: <c>key=value/</c> per key, names and values escaped as Hive and
/// Spark escape them, a null value <see cref="NullValue"/>. Values are rendered as Polars casts them to strings (the
/// partitions a dataset written here reads back, through Polars or the dataset readers, with the same values): every
/// integer, float, decimal, boolean, string, date, time and timestamp type, and dictionaries of them.
/// </summary>
/// <remarks>
/// Floats: the shortest digits that read back exactly, plainly from 1e-5 to below 1e16 (whole numbers end in
/// <c>.0</c>), else as <c>1e+16</c>, <c>1.5e-6</c>; <c>NaN</c>, <c>inf</c>, <c>-inf</c>; -0.0 is 0.0 (they are one
/// partition). Timestamps: <c>2024-01-02 03:04:05</c> with the unit's digits after the second (3, 6 or 9), in their
/// zone's local time followed by its offset (<c>+09:00</c>) when they have a zone. Times: <c>03:04:05</c>, then the
/// fraction when there is one (Polars leaves it out, so that distinct times would share a directory). Decimals with
/// all their scale's digits.
/// </remarks>
internal static class HivePaths
{
    /// <summary>The value of a null partition key.</summary>
    public const string NullValue = "__HIVE_DEFAULT_PARTITION__";

    /// <summary>Escapes a partition directory's key or value the way Hive and Spark do (control characters and <c>"#%'*/:=?\{[]^</c>).</summary>
    public static string Escape(string value)
    {
        if (!value.AsSpan().ContainsAny(Escaped)) return value;
        var text = new StringBuilder(value.Length + 8);
        foreach (char c in value)
        {
            if (Escaped.Contains(c)) text.Append('%').Append(((int)c).ToString("X2", CultureInfo.InvariantCulture));
            else text.Append(c);
        }
        return text.ToString();
    }

    private static readonly System.Buffers.SearchValues<char> Escaped = System.Buffers.SearchValues.Create(
        "\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008\u0009\u000A\u000B\u000C\u000D\u000E\u000F\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001A\u001B\u001C\u001D\u001E\u001F\u0000\u007F\"#%'*/:=?\\{[]^");

    /// <summary>The directories of a partition: <c>key=value/</c> for each key, in order (values unescaped; null: <see cref="NullValue"/>).</summary>
    public static string Directory(IReadOnlyList<string> keys, IReadOnlyList<string?> values)
    {
        var path = new StringBuilder();
        for (int k = 0; k < keys.Count; k++) path.Append(Escape(keys[k])).Append('=').Append(values[k] is { } value ? Escape(value) : NullValue).Append('/');
        return path.ToString();
    }

    /// <summary>Whether values of <paramref name="type"/> can be partition keys.</summary>
    public static bool Renders(IArrowType type) => type switch
    {
        DictionaryType d => Renders(d.ValueType),
        NullType or BooleanType or Int8Type or Int16Type or Int32Type or Int64Type or UInt8Type or UInt16Type or UInt32Type or UInt64Type => true,
        HalfFloatType or FloatType or DoubleType => true,
        Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type => true,
        StringType or LargeStringType or StringViewType => true,
        Date32Type or Date64Type or Time32Type or Time64Type or TimestampType => true,
        _ => false,
    };

    /// <summary>Row <paramref name="row"/> of <paramref name="array"/> as a partition value (null for a null).</summary>
    /// <exception cref="NotSupportedException">A type <see cref="Renders"/> refuses.</exception>
    public static string? Render(IArrowArray array, int row)
    {
        if (array.IsNull(row)) return null;
        return array switch
        {
            DictionaryArray d => Render(d.Dictionary, Index(d.Indices, row)),
            BooleanArray b => b.GetValue(row)!.Value ? "true" : "false",
            Int8Array a => Integer(a.GetValue(row)!.Value),
            Int16Array a => Integer(a.GetValue(row)!.Value),
            Int32Array a => Integer(a.GetValue(row)!.Value),
            Int64Array a => Integer(a.GetValue(row)!.Value),
            UInt8Array a => Integer(a.GetValue(row)!.Value),
            UInt16Array a => Integer(a.GetValue(row)!.Value),
            UInt32Array a => Integer(a.GetValue(row)!.Value),
            UInt64Array a => Integer(a.GetValue(row)!.Value),
            HalfFloatArray a => Float((float)a.GetValue(row)!.Value, single: true),
            FloatArray a => Float(a.GetValue(row)!.Value, single: true),
            DoubleArray a => Float(a.GetValue(row)!.Value, single: false),
            Decimal32Array or Decimal64Array or Decimal128Array or Decimal256Array => Decimal(array, row),
            StringArray s => s.GetString(row),
            LargeStringArray s => s.GetString(row),
            StringViewArray s => s.GetString(row),
            Date32Array d => Date(d.GetValue(row)!.Value),
            Date64Array d => Date(checked((int)Math.Floor(d.GetValue(row)!.Value / 86_400_000.0))),
            Time32Array t => Time(t.GetValue(row)!.Value * (((Time32Type)t.Data.DataType).Unit == TimeUnit.Second ? 1_000_000_000L : 1_000_000L)),
            Time64Array t => Time(t.GetValue(row)!.Value * (((Time64Type)t.Data.DataType).Unit == TimeUnit.Microsecond ? 1_000L : 1L)),
            TimestampArray t => Timestamp(t.GetValue(row)!.Value, (TimestampType)t.Data.DataType),
            _ => throw new NotSupportedException($"Values of {array.Data.DataType.Name} cannot be partition keys."),
        };
    }

    private static int Index(IArrowArray indices, int row) => indices switch
    {
        Int8Array a => a.GetValue(row)!.Value,
        Int16Array a => a.GetValue(row)!.Value,
        Int32Array a => a.GetValue(row)!.Value,
        Int64Array a => checked((int)a.GetValue(row)!.Value),
        UInt8Array a => a.GetValue(row)!.Value,
        UInt16Array a => a.GetValue(row)!.Value,
        UInt32Array a => checked((int)a.GetValue(row)!.Value),
        UInt64Array a => checked((int)a.GetValue(row)!.Value),
        _ => throw new NotSupportedException($"Dictionary indices of {indices.Data.DataType.Name}."),
    };

    private static string Integer<T>(T value) where T : IFormattable => value.ToString(null, CultureInfo.InvariantCulture);

    /// <summary>A float as Polars writes it (see the remarks).</summary>
    internal static string Float(double value, bool single)
    {
        if (double.IsNaN(value)) return "NaN";
        if (double.IsInfinity(value)) return value > 0 ? "inf" : "-inf";
        if (value == 0) return "0.0";
        // The shortest digits that read back exactly, and the decimal exponent of the first.
        string shortest = single ? ((float)value).ToString("R", CultureInfo.InvariantCulture) : value.ToString("R", CultureInfo.InvariantCulture);
        bool negative = shortest[0] == '-';
        ReadOnlySpan<char> text = negative ? shortest.AsSpan(1) : shortest;
        int e = text.IndexOfAny('E', 'e');
        int exponent = e < 0 ? 0 : int.Parse(text[(e + 1)..], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
        ReadOnlySpan<char> mantissa = e < 0 ? text : text[..e];
        int point = mantissa.IndexOf('.');
        string digits = point < 0 ? mantissa.ToString() : string.Concat(mantissa[..point], mantissa[(point + 1)..]);
        int first = (point < 0 ? mantissa.Length : point) + exponent - 1; // the exponent of the first digit
        int leading = digits.Length - digits.TrimStart('0').Length;
        digits = digits.Trim('0');
        first -= leading;

        var result = new StringBuilder(negative ? "-" : "");
        if (first is >= -5 and < 16)
        {
            if (first >= 0)
            {
                result.Append(digits.Length > first ? digits[..(first + 1)] : digits.PadRight(first + 1, '0'));
                result.Append('.').Append(digits.Length > first + 1 ? digits[(first + 1)..] : "0");
            }
            else result.Append("0.").Append('0', -first - 1).Append(digits);
        }
        else
        {
            result.Append(digits[0]);
            if (digits.Length > 1) result.Append('.').Append(digits.AsSpan(1));
            result.Append('e').Append(first >= 0 ? "+" : "-").Append(Math.Abs(first).ToString(CultureInfo.InvariantCulture));
        }
        return result.ToString();
    }

    /// <summary>A decimal with all its scale's digits (<c>0.00</c>, <c>-0.05</c>, <c>12</c>).</summary>
    private static string Decimal(IArrowArray array, int row)
    {
        var (scale, width) = array.Data.DataType switch
        {
            Decimal32Type d => (d.Scale, 4),
            Decimal64Type d => (d.Scale, 8),
            Decimal128Type d => (d.Scale, 16),
            Decimal256Type d => (d.Scale, 32),
            _ => throw new NotSupportedException(array.Data.DataType.Name),
        };
        ReadOnlySpan<byte> bytes = array.Data.Buffers[1].Span.Slice((array.Offset + row) * width, width);
        var unscaled = new BigInteger(bytes, isUnsigned: false, isBigEndian: false);
        string digits = BigInteger.Abs(unscaled).ToString(CultureInfo.InvariantCulture);
        string sign = unscaled.Sign < 0 ? "-" : "";
        if (scale <= 0) return sign + digits + new string('0', -scale);
        digits = digits.PadLeft(scale + 1, '0');
        return $"{sign}{digits[..^scale]}.{digits[^scale..]}";
    }

    private static string Date(int days)
    {
        long epochDay = DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber + (long)days;
        if (epochDay is < 0 or > 3_652_058) throw new NotSupportedException($"The date {days} days from 1970-01-01 lies outside the years 1 to 9999.");
        return DateOnly.FromDayNumber((int)epochDay).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
    }

    /// <summary>A time of day: <c>HH:mm:ss</c>, then the fraction of a second (its digits up to the last that is not 0) when there is one.</summary>
    private static string Time(long nanoseconds)
    {
        long seconds = nanoseconds / 1_000_000_000, fraction = nanoseconds % 1_000_000_000;
        string text = $"{seconds / 3600:D2}:{seconds / 60 % 60:D2}:{seconds % 60:D2}";
        return fraction == 0 ? text : $"{text}.{fraction.ToString("D9", CultureInfo.InvariantCulture).TrimEnd('0')}";
    }

    private static string Timestamp(long value, TimestampType type)
    {
        (long perSecond, int digits) = type.Unit switch
        {
            TimeUnit.Second => (1L, 0),
            TimeUnit.Millisecond => (1_000L, 3),
            TimeUnit.Microsecond => (1_000_000L, 6),
            _ => (1_000_000_000L, 9),
        };
        long seconds = Math.DivRem(value, perSecond, out long fraction);
        if (fraction < 0)
        {
            seconds--;
            fraction += perSecond;
        }
        DateTime utc = DateTime.UnixEpoch.AddSeconds(seconds);
        string suffix = "";
        DateTime local = utc;
        if (!string.IsNullOrEmpty(type.Timezone))
        {
            TimeSpan offset = TimeZones.Find(type.Timezone).GetUtcOffset(utc);
            local = utc + offset;
            suffix = (offset < TimeSpan.Zero ? "-" : "+") + offset.Duration().ToString(@"hh\:mm", CultureInfo.InvariantCulture);
        }
        string text = local.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        if (digits > 0) text += "." + fraction.ToString(CultureInfo.InvariantCulture).PadLeft(digits, '0');
        return text + suffix;
    }
}
