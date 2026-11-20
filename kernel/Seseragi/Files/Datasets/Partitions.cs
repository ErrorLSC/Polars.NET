using Kaminari.Sazanami.Arrow.Builders;
using System.Buffers.Binary;
using System.Globalization;
using System.Numerics;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Text;

namespace Kaminari.Seseragi.Files;

/// <summary>
/// The partition values of a dataset's files, as a table of one row per file (typed columns, in the order of the
/// keys), from Hive paths (<c>key=value</c> directories) or from values given with the files.
/// </summary>
internal sealed class Partitions
{
    private const string NullMarker = "__HIVE_DEFAULT_PARTITION__";
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private Partitions(Field[] fields, IArrowArray[] columns)
    {
        Fields = fields;
        Columns = columns;
    }

    /// <summary>The partition columns, in the order of the keys (strings as <see cref="StringType"/>).</summary>
    public Field[] Fields { get; }

    /// <summary>Each column's values, one per file.</summary>
    public IArrowArray[] Columns { get; }

    public int IndexOf(string name) => System.Array.FindIndex(Fields, f => f.Name == name);

    /// <summary>
    /// The partitions of Hive paths: the <c>key=value</c> directories of each path after
    /// <paramref name="start"/> (the file name aside; a part with more than one '=' or a '*' in its value is no key),
    /// values percent-decoded, <c>__HIVE_DEFAULT_PARTITION__</c> null, an empty value null (an empty string for a
    /// string column). The keys are the first path's: every path must have them all, and no other. A key's type is
    /// <paramref name="given"/>'s, else the files' column of that name, else inferred over every value (CSV rules);
    /// temporal types are strings unless <paramref name="parseDates"/>. Null when the first path has no key.
    /// </summary>
    /// <exception cref="InvalidOperationException">The paths disagree on their keys, or a key is missing from <paramref name="given"/>.</exception>
    /// <exception cref="InvalidDataException">A value is not of its key's type.</exception>
    public static Partitions? FromHive(IReadOnlyList<string> paths, int start, Apache.Arrow.Schema? given, Apache.Arrow.Schema files, bool parseDates)
    {
        var parts = new List<(string Key, string Value)>[paths.Count];
        for (int i = 0; i < paths.Count; i++) parts[i] = Keys(paths[i], start);
        if (parts.Length == 0 || parts[0].Count == 0) return null;
        string[] keys = [.. parts[0].Select(p => p.Key).Distinct()];
        for (int i = 0; i < paths.Count; i++)
        {
            foreach (var (key, _) in parts[i])
            {
                if (System.Array.IndexOf(keys, key) < 0)
                    throw new InvalidOperationException($"'{paths[i]}' has the partition key '{key}', which the first file's path ('{paths[0]}') lacks.");
            }
            foreach (string key in keys)
            {
                if (!parts[i].Any(p => p.Key == key))
                    throw new InvalidOperationException($"'{paths[i]}' lacks the partition key '{key}' of the first file's path ('{paths[0]}').");
            }
        }

        var fields = new Field[keys.Length];
        var columns = new IArrowArray[keys.Length];
        for (int k = 0; k < keys.Length; k++)
        {
            string key = keys[k];
            string?[] values = [.. parts.Select(p => p.Last(x => x.Key == key).Value)];
            IArrowType type;
            if (given is not null)
            {
                Field field = given.GetFieldByName(key)
                    ?? throw new InvalidOperationException($"The partition key '{key}' (in '{paths[0]}') is not in the partition schema given.");
                type = Temporal(field.DataType) && !parseDates ? StringType.Default : field.DataType;
            }
            else if (files.GetFieldByName(key) is { } column)
                // Strings as plain strings: the batches give them in the reader's layout.
                type = Temporal(column.DataType) && !parseDates || column.DataType is StringViewType ? StringType.Default : column.DataType;
            else
                type = Inferred(values, parseDates);
            fields[k] = new Field(key, type, nullable: true);
            columns[k] = Build(type, values, (i, value) => $"The partition value '{value}' of '{key}' in '{paths[i]}' is not {Describe(type)}.");
        }
        return new Partitions(fields, columns);
    }

    /// <summary>
    /// The partitions given with the files (<see cref="DatasetFile.PartitionValues"/>), typed by
    /// <paramref name="schema"/>: every file gives every key (null allowed); a value is of the type's CLR kind, or a
    /// string read as a Hive value is (as a log of the files records them).
    /// </summary>
    /// <exception cref="ArgumentException">A file lacks a key or gives one the schema lacks, or a value does not fit its type.</exception>
    public static Partitions FromValues(IReadOnlyList<DatasetFile> files, Apache.Arrow.Schema schema)
    {
        var fields = new Field[schema.FieldsList.Count];
        var columns = new IArrowArray[fields.Length];
        foreach (DatasetFile file in files)
        {
            foreach (string key in (file.PartitionValues ?? new Dictionary<string, object?>()).Keys)
            {
                if (schema.GetFieldIndex(key) < 0) throw new ArgumentException($"'{file.Path}' gives the partition key '{key}', which the partition schema lacks.", nameof(files));
            }
        }
        for (int k = 0; k < fields.Length; k++)
        {
            Field field = schema.FieldsList[k];
            fields[k] = new Field(field.Name, field.DataType, nullable: true);
            object?[] values = new object?[files.Count];
            for (int i = 0; i < files.Count; i++)
            {
                if (files[i].PartitionValues is not { } given || !given.TryGetValue(field.Name, out values[i]))
                    throw new ArgumentException($"'{files[i].Path}' gives no value for the partition key '{field.Name}'.", nameof(files));
            }
            try
            {
                columns[k] = Build(field.DataType, values, (i, value) => $"The partition value {value} of '{field.Name}' for '{files[i].Path}' is not {Describe(field.DataType)}.");
            }
            catch (InvalidDataException e)
            {
                throw new ArgumentException(e.Message, nameof(files), e);
            }
        }
        return new Partitions(fields, columns);
    }

    /// <summary>The <c>key=value</c> parts of a path's directories after <paramref name="start"/>.</summary>
    internal static List<(string Key, string Value)> Keys(string path, int start)
    {
        List<(string, string)> keys = [];
        string[] segments = path[Math.Min(start, path.Length)..].Split('/', '\\');
        for (int s = 0; s < segments.Length - 1; s++)
        {
            string segment = segments[s];
            int equals = segment.IndexOf('=');
            if (equals < 0 || segment.IndexOf('=', equals + 1) >= 0) continue;
            string value = segment[(equals + 1)..];
            if (value.Contains('*')) continue;
            if (Decode(value) is { } decoded) keys.Add((segment[..equals], decoded));
        }
        return keys;
    }

    /// <summary>Percent-decoding (a '+' stays); null when the bytes are not UTF-8 or a '%' is not followed by two hex digits.</summary>
    private static string? Decode(string value)
    {
        if (!value.Contains('%')) return value;
        var bytes = new List<byte>(value.Length);
        Span<byte> utf8 = stackalloc byte[4];
        for (int i = 0; i < value.Length; i++)
        {
            if (value[i] == '%' && i + 2 < value.Length && byte.TryParse(value.AsSpan(i + 1, 2), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out byte b))
            {
                bytes.Add(b);
                i += 2;
            }
            else
            {
                int chars = char.IsHighSurrogate(value[i]) && i + 1 < value.Length ? 2 : 1;
                int n = Encoding.UTF8.GetBytes(value.AsSpan(i, chars), utf8);
                i += chars - 1;
                foreach (byte x in utf8[..n]) bytes.Add(x);
            }
        }
        try
        {
            return StrictUtf8.GetString([.. bytes]);
        }
        catch (DecoderFallbackException)
        {
            return null;
        }
    }

    private static bool Temporal(IArrowType type) => type is Date32Type or Date64Type or TimestampType or Time32Type or Time64Type or DurationType;

    /// <summary>The type the values hold, as a CSV column's is inferred (empty values and the null marker aside).</summary>
    private static IArrowType Inferred(string?[] values, bool parseDates)
    {
        HashSet<TextKind> kinds = [];
        foreach (string? value in values)
        {
            if (string.IsNullOrEmpty(value) || value == NullMarker) continue;
            kinds.Add(TextValues.Infer(value, parseDates));
        }
        return TextValues.Finish(kinds) switch
        {
            TextKind.Boolean => BooleanType.Default,
            TextKind.Int64 => Int64Type.Default,
            TextKind.Int128 => new Decimal128Type(38, 0),
            TextKind.Float64 => DoubleType.Default,
            TextKind.Date => Date32Type.Default,
            TextKind.Datetime => new TimestampType(TimeUnit.Microsecond, (string?)null),
            TextKind.DatetimeUtc => new TimestampType(TimeUnit.Microsecond, "UTC"),
            _ => StringType.Default, // strings, and keys with no value at all
        };
    }

    private static string Describe(IArrowType type) => type switch
    {
        TimestampType t => $"a timestamp ({t.Unit}{(t.Timezone is null ? "" : ", " + t.Timezone)})",
        Decimal128Type d => $"a decimal({d.Precision}, {d.Scale})",
        _ => $"a valid {type.Name}",
    };

    /// <summary>The partition types the values can be read as.</summary>
    internal static bool Supported(IArrowType type) => type is BooleanType or Int8Type or Int16Type or Int32Type or Int64Type or UInt8Type or UInt16Type
        or UInt32Type or UInt64Type or FloatType or DoubleType or Decimal128Type or Date32Type or Date64Type or Time32Type or Time64Type or TimestampType
        or StringType or StringViewType;

    /// <summary>One value per file: texts read as Hive values, or CLR values of the type's kind.</summary>
    private static IArrowArray Build(IArrowType type, IReadOnlyList<object?> values, Func<int, object, string> error)
    {
        if (!Supported(type)) throw new NotSupportedException($"A partition column cannot be of type {type.Name}.");
        var valid = new byte[(values.Count + 7) / 8];
        int width = type is FixedWidthType f ? f.BitWidth / 8 : 0;
        var data = new byte[type is BooleanType ? valid.Length : Math.Max(width, 1) * values.Count];
        List<string?> strings = [];
        int nulls = 0;
        for (int i = 0; i < values.Count; i++)
        {
            object? value = values[i];
            if (value is NullMarker || value is null || value is "" && type is not (StringType or StringViewType))
            {
                nulls++;
                strings.Add(null);
                continue;
            }
            valid[i / 8] |= (byte)(1 << (i % 8));
            if (type is StringType or StringViewType)
            {
                strings.Add(value as string ?? throw new InvalidDataException(error(i, value)));
                continue;
            }
            strings.Add(null);
            if (!Write(type, value, data, i)) throw new InvalidDataException(error(i, value));
        }
        if (type is StringType or StringViewType)
        {
            if (type is StringType)
            {
                var builder = new StringArray.Builder();
                foreach (string? s in strings) builder.Append(s);
                return builder.Build(PooledArrowAllocator.Instance);
            }
            var views = new StringViewArray.Builder();
            foreach (string? s in strings) views.Append(s);
            return views.Build(PooledArrowAllocator.Instance);
        }
        return ArrowArrayFactory.BuildArray(new ArrayData(type, values.Count, nulls, 0, [new ArrowBuffer(valid), new ArrowBuffer(data)]));
    }

    /// <summary>Writes value <paramref name="i"/> into the data buffer; false when it does not fit the type.</summary>
    private static bool Write(IArrowType type, object value, byte[] data, int i)
    {
        string? text = value as string;
        switch (type)
        {
            case BooleanType:
            {
                bool flag;
                if (text is not null) { if (!TextValues.TryParseBoolean(text, out flag)) return false; }
                else if (value is bool b) flag = b;
                else return false;
                if (flag) data[i / 8] |= (byte)(1 << (i % 8));
                return true;
            }
            case Int8Type or Int16Type or Int32Type or Int64Type:
            {
                if (!Integer(value, text, out BigInteger n)) return false;
                int bytes = ((FixedWidthType)type).BitWidth / 8;
                if (n < -(BigInteger.One << (bytes * 8 - 1)) || n >= BigInteger.One << (bytes * 8 - 1)) return false;
                Span<byte> whole = stackalloc byte[8];
                BinaryPrimitives.WriteInt64LittleEndian(whole, (long)n);
                whole[..bytes].CopyTo(data.AsSpan(i * bytes, bytes));
                return true;
            }
            case UInt8Type or UInt16Type or UInt32Type or UInt64Type:
            {
                if (!Integer(value, text, out BigInteger n)) return false;
                int bytes = ((FixedWidthType)type).BitWidth / 8;
                if (n < 0 || n >= BigInteger.One << (bytes * 8)) return false;
                Span<byte> whole = stackalloc byte[8];
                BinaryPrimitives.WriteUInt64LittleEndian(whole, (ulong)n);
                whole[..bytes].CopyTo(data.AsSpan(i * bytes, bytes));
                return true;
            }
            case FloatType:
                if (!Real(value, text, out double single)) return false;
                BinaryPrimitives.WriteSingleLittleEndian(data.AsSpan(i * 4), (float)single);
                return true;
            case DoubleType:
                if (!Real(value, text, out double real)) return false;
                BinaryPrimitives.WriteDoubleLittleEndian(data.AsSpan(i * 8), real);
                return true;
            case Decimal128Type d:
            {
                if (!Scaled(value, text, d.Scale, out BigInteger unscaled) || BigInteger.Abs(unscaled) >= BigInteger.Pow(10, d.Precision)) return false;
                BinaryPrimitives.WriteInt128LittleEndian(data.AsSpan(i * 16), (Int128)unscaled);
                return true;
            }
            case Date32Type or Date64Type:
            {
                DateOnly date;
                if (text is not null) { if (!TextValues.TryParseDate(text, out date)) return false; }
                else if (value is DateOnly day) date = day;
                else if (value is DateTime time && time.TimeOfDay == TimeSpan.Zero) date = DateOnly.FromDateTime(time);
                else return false;
                int days = date.DayNumber - DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber;
                if (type is Date32Type) BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(i * 4), days);
                else BinaryPrimitives.WriteInt64LittleEndian(data.AsSpan(i * 8), days * 86_400_000L);
                return true;
            }
            case TimestampType t:
            {
                DateTime instant;
                if (text is not null) { if (!TextValues.TryParseDatetime(text, out instant, out _)) return false; }
                else if (value is DateTimeOffset offset) instant = offset.UtcDateTime;
                else if (value is DateTime time) instant = time.Kind == DateTimeKind.Local ? time.ToUniversalTime() : time;
                else if (value is DateOnly day) instant = day.ToDateTime(TimeOnly.MinValue);
                else return false;
                long ticks = instant.Ticks - DateTime.UnixEpoch.Ticks;
                long scaled = t.Unit switch
                {
                    TimeUnit.Second => ticks / TimeSpan.TicksPerSecond,
                    TimeUnit.Millisecond => ticks / TimeSpan.TicksPerMillisecond,
                    TimeUnit.Microsecond => ticks / 10,
                    _ => checked(ticks * 100),
                };
                BinaryPrimitives.WriteInt64LittleEndian(data.AsSpan(i * 8), scaled);
                return true;
            }
            case Time32Type or Time64Type:
            {
                long nanoseconds;
                if (text is not null) { if (!TextValues.TryParseTime(text, out nanoseconds)) return false; }
                else if (value is TimeOnly clock) nanoseconds = clock.Ticks * 100;
                else if (value is TimeSpan span && span >= TimeSpan.Zero && span < TimeSpan.FromDays(1)) nanoseconds = span.Ticks * 100;
                else return false;
                long per = ((TimeType)type).Unit switch
                {
                    TimeUnit.Second => 1_000_000_000L,
                    TimeUnit.Millisecond => 1_000_000L,
                    TimeUnit.Microsecond => 1_000L,
                    _ => 1L,
                };
                if (nanoseconds % per != 0) return false; // finer than the type holds
                if (type is Time32Type) BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(i * 4), (int)(nanoseconds / per));
                else BinaryPrimitives.WriteInt64LittleEndian(data.AsSpan(i * 8), nanoseconds / per);
                return true;
            }
            default:
                return false;
        }

        static bool Integer(object value, string? text, out BigInteger n)
        {
            n = default;
            if (text is not null) return TextValues.IsInteger(text) && BigInteger.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out n);
            switch (value)
            {
                case sbyte or short or int or long or byte or ushort or uint or ulong:
                    n = value switch { ulong u => u, _ => Convert.ToInt64(value, CultureInfo.InvariantCulture) };
                    return true;
                case Int128 big:
                    n = big;
                    return true;
                default:
                    return false;
            }
        }

        static bool Real(object value, string? text, out double real)
        {
            real = default;
            if (text is not null) return TextValues.TryParseFloat64(text, out real);
            switch (value)
            {
                case float f: real = f; return true;
                case double d: real = d; return true;
                case decimal m: real = (double)m; return true;
                case sbyte or short or int or long or byte or ushort or uint or ulong: real = Convert.ToDouble(value, CultureInfo.InvariantCulture); return true;
                default: return false;
            }
        }

        static bool Scaled(object value, string? text, int scale, out BigInteger unscaled)
        {
            unscaled = default;
            string? digits = text ?? value switch
            {
                decimal m => m.ToString(CultureInfo.InvariantCulture),
                sbyte or short or int or long or byte or ushort or uint or ulong or Int128 => Convert.ToString(value, CultureInfo.InvariantCulture),
                _ => null,
            };
            if (digits is null) return false;
            int dot = digits.IndexOf('.');
            string whole = dot < 0 ? digits : digits[..dot], fraction = dot < 0 ? "" : digits[(dot + 1)..];
            if (fraction.Length > scale && fraction[scale..].Any(c => c != '0')) return false;
            fraction = fraction.Length > scale ? fraction[..scale] : fraction.PadRight(scale, '0');
            string number = whole + fraction;
            return TextValues.IsInteger(number) && BigInteger.TryParse(number, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out unscaled);
        }
    }
}
