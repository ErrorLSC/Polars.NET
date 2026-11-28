using System.Buffers.Binary;
using System.Globalization;
using System.Numerics;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Sazanami.Text;

namespace Kaminari.Seseragi.Files;

/// <summary>
/// The value a column or struct field reads as in the rows of a file that lacks it (a column added to a table after
/// the file was written: Iceberg's <c>initial-default</c>, a schema's default value), instead of nulls. A field with a
/// default may be missing from files whether or not <see cref="DatasetOptions.AllowMissingColumns"/> is set; a null
/// default declares just that (the rows read nulls). Only a field a file lacks takes its default: a file's own nulls
/// stay null.
/// </summary>
/// <param name="Path">
/// The field: a column of the dataset by name, then the struct fields below it by name; a list's elements are gone
/// through (the path goes on with their fields), a map's keys and values are <c>key</c> and <c>value</c>.
/// </param>
/// <param name="Value">
/// The value, of the field's type: <see cref="bool"/>; integers (any CLR integer, in range); floats (float, double,
/// Half, decimal or an integer); decimals (decimal or an integer, of the scale or fewer digits after the point);
/// <see cref="DateOnly"/> (or a <see cref="DateTime"/> at midnight) for dates; <see cref="TimeOnly"/> or a
/// <see cref="TimeSpan"/> within a day for times; <see cref="DateTime"/> (a local one converted to UTC),
/// <see cref="DateOnly"/>, or for timestamps of a time zone <see cref="DateTimeOffset"/>, for timestamps;
/// <see cref="TimeSpan"/> for durations; a string for strings; <see cref="byte"/>[] or <see cref="ReadOnlyMemory{T}"/>
/// of bytes for binaries; for fixed-size binaries those of the width or a <see cref="Guid"/> (16 bytes, big-endian as
/// UUIDs are written); <see cref="Struct"/> for a struct; a dictionary's value for a dictionary. A string for a type
/// that is not a string is its text: <c>true</c>/<c>false</c>, an integer, a decimal number (an exponent allowed, as
/// <c>2E+20</c>), ISO 8601 dates, times, timestamps (<c>2017-11-16T22:31:08.123456</c>, an offset for timestamps of a
/// time zone only) and durations (<c>PT1.5S</c>). Values are exact (floats aside, which take the nearest): a value the
/// type cannot hold (out of range, finer than its unit or scale) is an error. Lists and maps take no default but null (their elements' fields may have
/// theirs). Null: the field reads nulls.
/// </param>
public sealed record ColumnDefault(IReadOnlyList<string> Path, object? Value)
{
    /// <summary>The default of a top-level column.</summary>
    public ColumnDefault(string column, object? value) : this([column], value) { }

    /// <summary>
    /// A struct's default that is not null: a struct whose fields take their own defaults (null where they have none),
    /// as Iceberg's <c>{}</c>. A struct whose default is null (or none) reads as a null struct where a file lacks it.
    /// </summary>
    public static object Struct { get; } = new StructDefault();

    private sealed class StructDefault
    {
        public override string ToString() => "ColumnDefault.Struct";
    }
}

/// <summary>
/// <see cref="ColumnDefault"/>s checked against a dataset's columns and their values converted to the fields' types,
/// as <see cref="ArrowDefaults"/> (a tree from the top-level columns down).
/// </summary>
internal static class ColumnDefaults
{
    /// <summary>
    /// Checks the defaults' paths (each a field the dataset has, named once; none on a column whose values come from
    /// elsewhere) and converts their values; null when there are none.
    /// </summary>
    /// <param name="defaults">The defaults given (null: none).</param>
    /// <param name="columns">The dataset's columns from files.</param>
    /// <param name="notFromFiles">Why a column takes no default (a partition column, the file path column), or null.</param>
    /// <param name="where">Who gives them, in messages ("The dataset's defaults", "The defaults of 'path'").</param>
    /// <param name="onlyColumns">Whether defaults of top-level columns <paramref name="columns"/> lacks are passed over (else an error).</param>
    /// <exception cref="ArgumentException">A path or value is wrong (the message names the field).</exception>
    public static ArrowDefaults? Build(IReadOnlyList<ColumnDefault>? defaults, Schema columns, Func<string, string?> notFromFiles, string where,
        bool onlyColumns = false)
    {
        if (defaults is null || defaults.Count == 0) return null;
        var root = new ArrowDefaults();
        foreach (ColumnDefault given in defaults)
        {
            CheckPath(given, where);
            string column = given.Path[0];
            if (notFromFiles(column) is { } why) throw new ArgumentException($"{where}: '{column}' takes no default: {why}.", "options");
            int at = columns.GetFieldIndex(column);
            if (at < 0 && onlyColumns) continue;
            if (at < 0) throw new ArgumentException($"{where}: the dataset has no column '{column}' to give a default to.", "options");
            IArrowType type = columns.FieldsList[at].DataType;
            ArrowDefaults node = root.Below(column);
            for (int i = 1; i < given.Path.Count; i++)
            {
                string name = given.Path[i];
                type = Elements(type);
                IArrowType? below = type switch
                {
                    StructType s => s.GetFieldIndex(name, StringComparer.Ordinal) is int f and >= 0 ? s.Fields[f].DataType : null,
                    MapType m => name switch { "key" => m.KeyField.DataType, "value" => m.ValueField.DataType, _ => null },
                    _ => null,
                };
                if (below is null)
                {
                    string parent = string.Join('.', given.Path.Take(i));
                    throw new ArgumentException(type switch
                    {
                        StructType => $"{where}: '{parent}' has no field '{name}' to give a default to.",
                        MapType => $"{where}: '{parent}' is a map: the fields below it are 'key' and 'value', not '{name}'.",
                        _ => $"{where}: '{parent}' is {ArrowTypes.Describe(type)}: it has no field '{name}' below it.",
                    }, "options");
                }
                type = below;
                node = node.Below(name);
            }
            string path = string.Join('.', given.Path);
            if (node.Declared) throw new ArgumentException($"{where}: '{path}' is given two defaults.", "options");
            if (given.Value is null) node.Declare(null, isStruct: false);
            else if (type is StructType)
            {
                if (!ReferenceEquals(given.Value, ColumnDefault.Struct))
                    throw new ArgumentException($"{where}: '{path}' is a struct: its default is null or ColumnDefault.Struct (its fields then take their own defaults), not {Show(given.Value)}.", "options");
                node.Declare(null, isStruct: true);
            }
            else node.Declare(Scalar(type, given.Value, path, where), isStruct: false);
        }
        return root;
    }

    /// <summary>Checks that a default names a field: a path of names, none of them null.</summary>
    internal static void CheckPath(ColumnDefault given, string where)
    {
        if (given is null) throw new ArgumentException($"{where}: a default is null.", "options");
        if (given.Path is null || given.Path.Count == 0) throw new ArgumentException($"{where}: a default has an empty path (it names no column).", "options");
        if (given.Path.Any(string.IsNullOrEmpty)) throw new ArgumentException($"{where}: the default of '{string.Join('.', given.Path)}' has an empty name in its path.", "options");
    }

    /// <summary>Lists' elements gone through: the type of the values below the lists.</summary>
    private static IArrowType Elements(IArrowType type)
    {
        while (type is ListType or LargeListType or ListViewType or LargeListViewType or FixedSizeListType) type = ArrowWidening.ListElement(type);
        return type;
    }

    /// <summary>The value as a value of <paramref name="type"/> (not a struct), exactly.</summary>
    private static ArrowScalar Scalar(IArrowType type, object value, string path, string where)
    {
        if (type is ListType or LargeListType or ListViewType or LargeListViewType or FixedSizeListType or MapType)
            throw new ArgumentException($"{where}: '{path}' is {ArrowTypes.Describe(type)}: lists and maps take no default but null (the fields of their elements may have theirs).", "options");
        if (type is DictionaryType d) return new ArrowScalar(d, Scalar(d.ValueType, value, path, where));
        object? converted = type switch
        {
            BooleanType => value switch
            {
                bool b => b,
                string text when TextValues.TryParseBoolean(text, out bool b) => b,
                _ => null,
            },
            Int8Type or Int16Type or Int32Type or Int64Type or UInt8Type or UInt16Type or UInt32Type or UInt64Type =>
                Integer(value) is { } n && Fits(n, (IntegerType)type) ? Bytes(n, ((FixedWidthType)type).BitWidth / 8) : null,
            HalfFloatType or FloatType or DoubleType => Real(value) is { } real ? Float(real, type) : null,
            Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type => Decimal(value, type),
            Date32Type or Date64Type => Date(value) is { } days ? (type is Date32Type ? Bytes(days, 4) : Bytes(days * 86_400_000L, 8)) : null,
            Time32Type or Time64Type => Time(value) is { } ofDay ? Scaled(ofDay, ((TimeType)type).Unit, type is Time32Type ? 4 : 8) : null,
            TimestampType t => Timestamp(value, zoned: !string.IsNullOrEmpty(t.Timezone)) is { } nanos ? Scaled(nanos, t.Unit, 8) : null,
            DurationType t => Duration(value) is { } nanos ? Scaled(nanos, t.Unit, 8) : null,
            StringType or LargeStringType or StringViewType => value is string s ? Encoding.UTF8.GetBytes(s) : null,
            BinaryType or LargeBinaryType or BinaryViewType => Binary(value),
            FixedSizeBinaryType f => value is Guid g && f.ByteWidth == 16 ? g.ToByteArray(bigEndian: true) : Binary(value) is { Length: var n } bytes && n == f.ByteWidth ? bytes : null,
            _ => throw new ArgumentException($"{where}: '{path}' is of type {ArrowTypes.Describe(type)}, which takes no default but null.", "options"),
        };
        return converted is null
            ? throw new ArgumentException($"{where}: the default {Show(value)} of '{path}' is not a value of {ArrowTypes.Describe(type)}.", "options")
            : new ArrowScalar(type, converted);
    }

    private static string Show(object value) => value switch
    {
        string s => $"'{s}'",
        IFormattable f => $"{f.ToString(null, CultureInfo.InvariantCulture)} ({value.GetType().Name})",
        _ => $"{value} ({value.GetType().Name})",
    };

    // ---- values ---------------------------------------------------------------------------------------------------

    private static BigInteger? Integer(object value) => value switch
    {
        sbyte v => v,
        byte v => v,
        short v => v,
        ushort v => v,
        int v => v,
        uint v => v,
        long v => v,
        ulong v => v,
        Int128 v => v,
        UInt128 v => v,
        string text when TextValues.IsInteger(text) && BigInteger.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out BigInteger n) => n,
        _ => null,
    };

    private static bool Fits(BigInteger n, IntegerType type)
    {
        int bits = type.BitWidth;
        return type.IsSigned ? n >= -(BigInteger.One << (bits - 1)) && n < BigInteger.One << (bits - 1) : n >= 0 && n < BigInteger.One << bits;
    }

    /// <summary>The low <paramref name="width"/> bytes of the two's complement, little-endian.</summary>
    private static byte[] Bytes(BigInteger n, int width)
    {
        // The whole two's complement (one byte more than the width for the top unsigned values), its low bytes kept.
        var bytes = new byte[width];
        if (n.Sign < 0) bytes.AsSpan().Fill(0xFF);
        byte[] all = n.ToByteArray(isUnsigned: false, isBigEndian: false);
        all.AsSpan(0, Math.Min(all.Length, width)).CopyTo(bytes);
        return bytes;
    }

    private static double? Real(object value) => value switch
    {
        double d => d,
        float f => f,
        Half h => (double)h,
        decimal m => (double)m,
        string text when TextValues.TryParseFloat64(text, out double d) => d,
        _ => Integer(value) is { } n ? (double)n : null,
    };

    private static byte[] Float(double value, IArrowType type)
    {
        var bytes = new byte[((FixedWidthType)type).BitWidth / 8];
        switch (type)
        {
            case HalfFloatType: BinaryPrimitives.WriteHalfLittleEndian(bytes, (Half)value); break;
            case FloatType: BinaryPrimitives.WriteSingleLittleEndian(bytes, (float)value); break;
            default: BinaryPrimitives.WriteDoubleLittleEndian(bytes, value); break;
        }
        return bytes;
    }

    /// <summary>A decimal's unscaled value at the type's scale (exactly, within its precision), as its bytes.</summary>
    private static byte[]? Decimal(object value, IArrowType type)
    {
        var (precision, scale) = ArrowTypes.DecimalParameters(type);
        string? text = value switch
        {
            string s => s,
            decimal m => m.ToString(CultureInfo.InvariantCulture),
            _ => Integer(value)?.ToString(CultureInfo.InvariantCulture),
        };
        if (text is null || !Unscaled(text, scale, out BigInteger unscaled) || BigInteger.Abs(unscaled) >= BigInteger.Pow(10, precision)) return null;
        return Bytes(unscaled, ((FixedWidthType)type).BitWidth / 8);

        // [sign] digits [. digits] [e [sign] digits], exactly at the scale.
        static bool Unscaled(string text, int scale, out BigInteger unscaled)
        {
            unscaled = default;
            ReadOnlySpan<char> s = text.AsSpan().Trim();
            int e = s.IndexOfAny('e', 'E');
            int exponent = 0;
            if (e >= 0)
            {
                if (!int.TryParse(s[(e + 1)..], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out exponent) || Math.Abs(exponent) > 1000) return false;
                s = s[..e];
            }
            bool negative = s.Length > 0 && s[0] == '-';
            if (s.Length > 0 && s[0] is '-' or '+') s = s[1..];
            int dot = s.IndexOf('.');
            ReadOnlySpan<char> whole = dot < 0 ? s : s[..dot], fraction = dot < 0 ? [] : s[(dot + 1)..];
            if (whole.Length + fraction.Length == 0 || whole.ContainsAnyExceptInRange('0', '9') || fraction.ContainsAnyExceptInRange('0', '9')) return false;
            // digits × 10^(exponent − fraction digits), at the scale: 10^(scale + exponent − fraction digits).
            BigInteger digits = BigInteger.Parse(string.Concat(whole, fraction).PadLeft(1, '0'), CultureInfo.InvariantCulture);
            int shift = scale + exponent - fraction.Length;
            if (shift >= 0) unscaled = digits * BigInteger.Pow(10, shift);
            else
            {
                BigInteger divisor = BigInteger.Pow(10, -shift);
                if (!BigInteger.Remainder(digits, divisor).IsZero) return false; // finer than the scale
                unscaled = digits / divisor;
            }
            if (negative) unscaled = -unscaled;
            return true;
        }
    }

    private static long? Date(object value) => value switch
    {
        DateOnly day => day.DayNumber - DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber,
        DateTime time when time.TimeOfDay == TimeSpan.Zero => DateOnly.FromDateTime(time).DayNumber - DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber,
        string text when IsoTemporal.TryParseDate(Encoding.UTF8.GetBytes(text), out int days) => days,
        _ => null,
    };

    /// <summary>A time of day in nanoseconds.</summary>
    private static Int128? Time(object value) => value switch
    {
        TimeOnly clock => clock.Ticks * 100L,
        TimeSpan span when span >= TimeSpan.Zero && span < TimeSpan.FromDays(1) => span.Ticks * 100L,
        string text when IsoTemporal.TryParseTime(Encoding.UTF8.GetBytes(text), out long nanos) => nanos,
        _ => null,
    };

    /// <summary>A timestamp in nanoseconds since the epoch: an instant (UTC) for a zoned type, else the wall-clock time read as UTC.</summary>
    private static Int128? Timestamp(object value, bool zoned) => value switch
    {
        DateTimeOffset offset when zoned => Ticks(offset.UtcDateTime),
        DateTime time => Ticks(time.Kind == DateTimeKind.Local ? time.ToUniversalTime() : time),
        DateOnly day => Ticks(day.ToDateTime(TimeOnly.MinValue)),
        // An offset only for an instant: a wall-clock time has none.
        string text when IsoTemporal.TryParseDatetime(Encoding.UTF8.GetBytes(text), out Int128 nanos, out bool withOffset) && (!withOffset || zoned) => nanos,
        _ => null,
    };

    private static Int128 Ticks(DateTime time) => (Int128)(time.Ticks - DateTime.UnixEpoch.Ticks) * 100;

    private static Int128? Duration(object value) => value switch
    {
        TimeSpan span => (Int128)span.Ticks * 100,
        string text when IsoTemporal.TryParseDuration(Encoding.UTF8.GetBytes(text), out Int128 nanos) => nanos,
        _ => null,
    };

    /// <summary>Nanoseconds in <paramref name="unit"/> as <paramref name="width"/> bytes; null when finer than the unit or out of range.</summary>
    private static byte[]? Scaled(Int128 nanos, TimeUnit unit, int width)
    {
        long per = unit switch
        {
            TimeUnit.Second => 1_000_000_000L,
            TimeUnit.Millisecond => 1_000_000L,
            TimeUnit.Microsecond => 1_000L,
            _ => 1L,
        };
        if (nanos % per != 0) return null;
        Int128 count = nanos / per;
        if (width == 4 ? count < int.MinValue || count > int.MaxValue : count < long.MinValue || count > long.MaxValue) return null;
        return Bytes((BigInteger)count, width);
    }

    private static byte[]? Binary(object value) => value switch
    {
        byte[] bytes => [.. bytes],
        ReadOnlyMemory<byte> memory => memory.ToArray(),
        Memory<byte> memory => memory.ToArray(),
        _ => null,
    };
}
