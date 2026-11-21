using System.Buffers;
using System.Buffers.Text;
using System.Numerics;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Sazanami.Encodings;
using Kaminari.Sazanami.Text;

namespace Kaminari.Seseragi.Files.Json.Reading;

/// <summary>
/// Decodes JSON values into one Arrow column (in native memory, see <see cref="NativeColumnBuilder"/>). A value that
/// does not fit the column's type is a <see cref="JsonValueException"/>, or a null with <c>IgnoreErrors</c>.
/// </summary>
internal abstract class JsonColumn(NativeColumnBuilder builder, string path, bool ignoreErrors) : IDisposable
{
    /// <summary>Unescaped strings of up to this many bytes are copied on the stack.</summary>
    protected const int StackBytes = 256;

    public NativeColumnBuilder Builder { get; } = builder;

    /// <summary>The column's name (dotted through struct fields) for errors.</summary>
    public string Path { get; } = path;

    protected bool IgnoreErrors { get; } = ignoreErrors;

    /// <summary>
    /// Reads the value the reader stands on (its first token) and leaves the reader on its last token;
    /// <paramref name="line"/> is the reader's input (for raw value text).
    /// </summary>
    public abstract void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line);

    /// <summary>A record without the field.</summary>
    public virtual void Missing() => Builder.AppendNull();

    /// <summary>
    /// Expects <paramref name="rows"/> more values (and about <paramref name="dataBytes"/> of variable-length data), so
    /// that buffers grow once instead of doubling up as they fill.
    /// </summary>
    public virtual void Reserve(long rows, long dataBytes) => Builder.Reserve(rows, dataBytes);

    /// <summary>After each exported batch (columns with per-batch state reset it).</summary>
    public virtual void EndBatch() { }

    /// <summary>Frees what the decoder holds (not its builder).</summary>
    public virtual void Dispose() { }

    /// <summary>A value that does not fit: a null (its tokens passed over) or an error.</summary>
    protected void Mismatch(ref Utf8JsonReader reader, ReadOnlySpan<byte> line, string type, string? why = null)
    {
        if (IgnoreErrors)
        {
            if (reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray) reader.Skip();
            Builder.AppendNull();
            return;
        }
        throw new JsonValueException($"Column '{Path}': {Describe(ref reader, line)} is not {type}{(why is null ? "" : $" ({why})")}.");
    }

    /// <summary>The value the reader stands on, for errors: its token kind and (shortened) text.</summary>
    protected static string Describe(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        string kind = reader.TokenType switch
        {
            JsonTokenType.String => "the string",
            JsonTokenType.Number => "the number",
            JsonTokenType.True or JsonTokenType.False => "the boolean",
            JsonTokenType.StartObject => "an object",
            JsonTokenType.StartArray => "an array",
            _ => reader.TokenType.ToString(),
        };
        if (reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray) return kind;
        ReadOnlySpan<byte> text = reader.ValueSpan;
        string shown = Encoding.UTF8.GetString(text[..Math.Min(text.Length, 60)]) + (text.Length > 60 ? "…" : "");
        return reader.TokenType == JsonTokenType.String ? $"{kind} \"{shown}\"" : $"{kind} {shown}";
    }

    /// <summary>
    /// The unescaped UTF-8 of the string (or number) the reader stands on: its bytes in the input when nothing is
    /// escaped, else a copy in <paramref name="rented"/>, to be returned to the pool.
    /// </summary>
    protected static ReadOnlySpan<byte> Text(in Utf8JsonReader reader, out byte[]? rented)
    {
        rented = null;
        if (!reader.ValueIsEscaped) return reader.ValueSpan;
        rented = ArrayPool<byte>.Shared.Rent(reader.ValueSpan.Length);
        return rented.AsSpan(0, reader.CopyString(rented));
    }

    /// <summary>
    /// The unescaped UTF-8 of the string the reader stands on: its bytes in the input when nothing is escaped, else a
    /// copy in <paramref name="scratch"/> (or in <paramref name="rented"/>, to be returned to the pool).
    /// </summary>
    protected static ReadOnlySpan<byte> Unescaped(in Utf8JsonReader reader, Span<byte> scratch, out byte[]? rented)
    {
        rented = null;
        if (!reader.ValueIsEscaped) return reader.ValueSpan;
        int max = reader.ValueSpan.Length;
        Span<byte> buffer = scratch;
        if (max > scratch.Length) buffer = rented = ArrayPool<byte>.Shared.Rent(max);
        return buffer[..reader.CopyString(buffer)];
    }

    /// <summary>The decoder of a column of <paramref name="field"/>'s type over <paramref name="builder"/>.</summary>
    public static JsonColumn For(Field field, NativeColumnBuilder builder, string path, bool ignoreErrors) => field.DataType switch
    {
        NullType => new NullColumn(builder, path, ignoreErrors),
        BooleanType => new BooleanColumn(builder, path, ignoreErrors),
        Int8Type => new IntegerColumn<sbyte>(builder, path, ignoreErrors),
        Int16Type => new IntegerColumn<short>(builder, path, ignoreErrors),
        Int32Type => new IntegerColumn<int>(builder, path, ignoreErrors),
        Int64Type => new IntegerColumn<long>(builder, path, ignoreErrors),
        UInt8Type => new IntegerColumn<byte>(builder, path, ignoreErrors),
        UInt16Type => new IntegerColumn<ushort>(builder, path, ignoreErrors),
        UInt32Type => new IntegerColumn<uint>(builder, path, ignoreErrors),
        UInt64Type => new UInt64Column(builder, path, ignoreErrors),
        HalfFloatType => new FloatColumn<Half>(builder, path, ignoreErrors),
        FloatType => new FloatColumn<float>(builder, path, ignoreErrors),
        DoubleType => new FloatColumn<double>(builder, path, ignoreErrors),
        Decimal128Type when WideIntegers.KindOf(field) is not null => new WideIntegerColumn(builder, path, ignoreErrors, WideIntegers.KindOf(field) == WideIntegers.Unsigned),
        Decimal128Type d => new DecimalColumn(d, builder, path, ignoreErrors),
        StringType or LargeStringType or StringViewType => new StringColumn(builder, path, ignoreErrors),
        BinaryType or LargeBinaryType or BinaryViewType => new BinaryColumn(builder, path, ignoreErrors),
        Date32Type => new DateColumn(builder, path, ignoreErrors, millis: false),
        Date64Type => new DateColumn(builder, path, ignoreErrors, millis: true),
        TimestampType t => new TemporalColumn(builder, path, ignoreErrors, TemporalKind.Timestamp, UnitNanos(t.Unit)),
        Time32Type t => new TemporalColumn(builder, path, ignoreErrors, TemporalKind.Time32, UnitNanos(t.Unit)),
        Time64Type t => new TemporalColumn(builder, path, ignoreErrors, TemporalKind.Time64, UnitNanos(t.Unit)),
        DurationType t => new TemporalColumn(builder, path, ignoreErrors, TemporalKind.Duration, UnitNanos(t.Unit)),
        DictionaryType { ValueType: StringType or BinaryType } d => new DictionaryColumn((NativeDictionaryBuilder)builder, path, ignoreErrors, d.ValueType is BinaryType),
        MapType m => new MapColumn(m, (NativeListBuilder)builder, path, ignoreErrors),
        ListType l => new ListColumn(builder, ((NativeListBuilder)builder).Values, l.ValueField, path, ignoreErrors, fixedSize: -1),
        LargeListType l => new ListColumn(builder, ((NativeLargeListBuilder)builder).Values, l.ValueField, path, ignoreErrors, fixedSize: -1),
        FixedSizeListType l => new ListColumn(builder, ((NativeFixedSizeListBuilder)builder).Values, l.ValueField, path, ignoreErrors, l.ListSize),
        StructType s => new StructColumn(s, (NativeStructBuilder)builder, path, ignoreErrors),
        _ => throw new NotSupportedException($"Column '{path}': JSON cannot be read as {field.DataType.Name}."),
    };

    private static long UnitNanos(TimeUnit unit) => unit switch
    {
        TimeUnit.Second => 1_000_000_000,
        TimeUnit.Millisecond => 1_000_000,
        TimeUnit.Microsecond => 1_000,
        _ => 1,
    };
}

internal sealed class NullColumn(NativeColumnBuilder b, string path, bool ignore) : JsonColumn(b, path, ignore)
{
    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        if (reader.TokenType == JsonTokenType.Null) Builder.AppendNull();
        else Mismatch(ref reader, line, "null", "the column held only nulls in the records its type was inferred from");
    }
}

internal sealed class BooleanColumn(NativeColumnBuilder b, string path, bool ignore) : JsonColumn(b, path, ignore)
{
    private readonly NativeBooleanBuilder _b = (NativeBooleanBuilder)b;

    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.True: _b.Append(true); break;
            case JsonTokenType.False: _b.Append(false); break;
            case JsonTokenType.Null: _b.AppendNull(); break;
            default: Mismatch(ref reader, line, "a boolean"); break;
        }
    }
}

/// <summary>Integers up to 64 bits (signed, or unsigned below 2^63): whole numbers in range only, never fractions or exponents.</summary>
internal sealed class IntegerColumn<T>(NativeColumnBuilder b, string path, bool ignore) : JsonColumn(b, path, ignore)
    where T : unmanaged, IBinaryInteger<T>, IMinMaxValue<T>
{
    private static readonly long Min = long.CreateSaturating(T.MinValue), Max = long.CreateSaturating(T.MaxValue);
    private readonly NativeFixedWidthBuilder<T> _b = (NativeFixedWidthBuilder<T>)b;

    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        if (reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out long v) && v >= Min && v <= Max) _b.Append(T.CreateTruncating(v));
        else if (reader.TokenType == JsonTokenType.Null) _b.AppendNull();
        else Mismatch(ref reader, line, $"an {typeof(T).Name} integer", reader.TokenType == JsonTokenType.Number ? "out of range, or not a whole number" : null);
    }
}

internal sealed class UInt64Column(NativeColumnBuilder b, string path, bool ignore) : JsonColumn(b, path, ignore)
{
    private readonly NativeFixedWidthBuilder<ulong> _b = (NativeFixedWidthBuilder<ulong>)b;

    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        if (reader.TokenType == JsonTokenType.Number && reader.TryGetUInt64(out ulong v)) _b.Append(v);
        else if (reader.TokenType == JsonTokenType.Null) _b.AppendNull();
        else Mismatch(ref reader, line, "a UInt64 integer", reader.TokenType == JsonTokenType.Number ? "out of range, or not a whole number" : null);
    }
}

/// <summary>128-bit integers (Sazanami's <see cref="WideIntegers"/> layout): whole numbers in range.</summary>
internal sealed class WideIntegerColumn(NativeColumnBuilder b, string path, bool ignore, bool unsigned) : JsonColumn(b, path, ignore)
{
    private readonly NativeFixedWidthBuilder<Int128> _b = (NativeFixedWidthBuilder<Int128>)b;

    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            _b.AppendNull();
            return;
        }
        if (reader.TokenType == JsonTokenType.Number)
        {
            if (reader.TryGetInt64(out long small) && (!unsigned || small >= 0))
            {
                _b.Append(small);
                return;
            }
            if (unsigned && UInt128.TryParse(reader.ValueSpan, System.Globalization.NumberStyles.None, null, out UInt128 u))
            {
                _b.Append((Int128)u);
                return;
            }
            if (!unsigned && Int128.TryParse(reader.ValueSpan, System.Globalization.NumberStyles.AllowLeadingSign, null, out Int128 wide))
            {
                _b.Append(wide);
                return;
            }
        }
        Mismatch(ref reader, line, unsigned ? "a UInt128 integer" : "an Int128 integer", reader.TokenType == JsonTokenType.Number ? "out of range, or not a whole number" : null);
    }
}

/// <summary>Floating-point numbers (finite; rounded to the type as any decimal text is).</summary>
internal sealed class FloatColumn<T>(NativeColumnBuilder b, string path, bool ignore) : JsonColumn(b, path, ignore)
    where T : unmanaged, IFloatingPoint<T>
{
    private readonly NativeFixedWidthBuilder<T> _b = (NativeFixedWidthBuilder<T>)b;

    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        if (reader.TokenType == JsonTokenType.Number && reader.TryGetDouble(out double v) && double.IsFinite(v) && T.IsFinite(T.CreateSaturating(v)))
            _b.Append(T.CreateSaturating(v));
        else if (reader.TokenType == JsonTokenType.Null) _b.AppendNull();
        else Mismatch(ref reader, line, $"a {typeof(T).Name} number", reader.TokenType == JsonTokenType.Number ? "out of range" : null);
    }
}

/// <summary>Decimal128: numbers and strings of decimal text, exact at the scale (more fraction digits or precision is refused).</summary>
internal sealed class DecimalColumn(Decimal128Type type, NativeColumnBuilder b, string path, bool ignore) : JsonColumn(b, path, ignore)
{
    private readonly NativeFixedWidthBuilder<Int128> _b = (NativeFixedWidthBuilder<Int128>)b;

    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            _b.AppendNull();
            return;
        }
        if (reader.TokenType is JsonTokenType.Number or JsonTokenType.String)
        {
            ReadOnlySpan<byte> text = Text(in reader, out byte[]? rented);
            bool ok = DecimalText.TryParse(text, type.Precision, type.Scale, out Int128 unscaled);
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
            if (ok)
            {
                _b.Append(unscaled);
                return;
            }
        }
        Mismatch(ref reader, line, $"a decimal({type.Precision}, {type.Scale})", "not decimal text, or more digits than the type holds");
    }
}

/// <summary>
/// Strings: JSON strings unescaped; any other value as its text in the input (numbers and booleans as written,
/// objects and arrays as their source text).
/// </summary>
internal sealed class StringColumn(NativeColumnBuilder b, string path, bool ignore) : JsonColumn(b, path, ignore)
{
    private readonly IBytesBuilder _b = (IBytesBuilder)b;

    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.String:
            {
                Span<byte> scratch = stackalloc byte[StackBytes];
            ReadOnlySpan<byte> text = Unescaped(in reader, scratch, out byte[]? rented);
                _b.Append(text);
                if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
                break;
            }
            case JsonTokenType.Null:
                Builder.AppendNull();
                break;
            case JsonTokenType.StartObject or JsonTokenType.StartArray:
            {
                int start = (int)reader.TokenStartIndex;
                reader.Skip();
                _b.Append(line[start..(int)reader.BytesConsumed]);
                break;
            }
            default:
                _b.Append(reader.ValueSpan);
                break;
        }
    }

    private void AppendEscaped(in Utf8JsonReader reader)
    {
        Span<byte> scratch = stackalloc byte[StackBytes];
        ReadOnlySpan<byte> text = Unescaped(in reader, scratch, out byte[]? rented);
        _b.Append(text);
        if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
    }
}

/// <summary>Binaries: strings of base64 text.</summary>
internal sealed class BinaryColumn(NativeColumnBuilder b, string path, bool ignore) : JsonColumn(b, path, ignore)
{
    private readonly IBytesBuilder _b = (IBytesBuilder)b;

    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            Builder.AppendNull();
            return;
        }
        if (reader.TokenType == JsonTokenType.String)
        {
            ReadOnlySpan<byte> text = Text(in reader, out byte[]? rented);
            int max = Base64.GetMaxDecodedFromUtf8Length(text.Length);
            byte[]? decoded = max > StackBytes ? ArrayPool<byte>.Shared.Rent(max) : null;
            Span<byte> bytes = decoded ?? stackalloc byte[StackBytes];
            bool ok = Base64.DecodeFromUtf8(text, bytes, out _, out int written) == OperationStatus.Done;
            if (ok) _b.Append(bytes[..written]);
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
            if (decoded is not null) ArrayPool<byte>.Shared.Return(decoded);
            if (ok) return;
        }
        Mismatch(ref reader, line, "a binary", reader.TokenType == JsonTokenType.String ? "not base64 text" : "binaries are base64 strings");
    }
}

/// <summary>Dates: <c>yyyy-MM-dd</c> strings.</summary>
internal sealed class DateColumn(NativeColumnBuilder b, string path, bool ignore, bool millis) : JsonColumn(b, path, ignore)
{
    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            Builder.AppendNull();
            return;
        }
        if (reader.TokenType == JsonTokenType.String)
        {
            ReadOnlySpan<byte> text = Text(in reader, out byte[]? rented);
            bool ok = IsoTemporal.TryParseDate(text, out int days);
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
            if (ok)
            {
                if (millis) ((NativeFixedWidthBuilder<long>)Builder).Append(days * 86_400_000L);
                else ((NativeFixedWidthBuilder<int>)Builder).Append(days);
                return;
            }
        }
        Mismatch(ref reader, line, "a date", "dates are yyyy-MM-dd strings");
    }
}

internal enum TemporalKind { Timestamp, Time32, Time64, Duration }

/// <summary>
/// Timestamps (ISO 8601 date-times; with an offset or Z the instant, else the wall-clock time read as UTC), times of
/// day (<c>HH:mm[:ss[.f]]</c>) and durations (ISO 8601, <c>PT1.5S</c>), in the column's unit exactly: a finer
/// fraction than the unit holds is refused, never cut.
/// </summary>
internal sealed class TemporalColumn(NativeColumnBuilder b, string path, bool ignore, TemporalKind kind, long unitNanos) : JsonColumn(b, path, ignore)
{
    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            Builder.AppendNull();
            return;
        }
        string? why = null;
        if (reader.TokenType == JsonTokenType.String)
        {
            ReadOnlySpan<byte> text = Text(in reader, out byte[]? rented);
            Int128 nanos = 0;
            bool parsed = kind switch
            {
                TemporalKind.Timestamp => IsoTemporal.TryParseDatetime(text, out nanos, out _),
                TemporalKind.Duration => IsoTemporal.TryParseDuration(text, out nanos),
                _ => TryTime(text, out nanos),
            };
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
            if (parsed)
            {
                Int128 value = Int128.DivRem(nanos, unitNanos).Quotient;
                if (value * unitNanos != nanos) why = "a finer fraction of a second than the column's unit holds";
                else if (kind == TemporalKind.Time32)
                {
                    ((NativeFixedWidthBuilder<int>)Builder).Append((int)value);
                    return;
                }
                else if (value >= long.MinValue && value <= long.MaxValue)
                {
                    ((NativeFixedWidthBuilder<long>)Builder).Append((long)value);
                    return;
                }
                else why = "out of the column's range";
            }
        }
        string type = kind switch
        {
            TemporalKind.Timestamp => "a date-time",
            TemporalKind.Duration => "a duration",
            _ => "a time of day",
        };
        Mismatch(ref reader, line, type, why ?? (kind switch
        {
            TemporalKind.Timestamp => "date-times are ISO 8601 strings",
            TemporalKind.Duration => "durations are ISO 8601 strings such as PT1.5S",
            _ => "times are HH:mm:ss[.f] strings",
        }));

        static bool TryTime(ReadOnlySpan<byte> text, out Int128 nanos)
        {
            bool ok = IsoTemporal.TryParseTime(text, out long n);
            nanos = n;
            return ok;
        }
    }
}

/// <summary>Dictionary-encoded strings (or base64 binaries): each batch's distinct values in order of first appearance.</summary>
internal sealed class DictionaryColumn(NativeDictionaryBuilder b, string path, bool ignore, bool binary) : JsonColumn(b, path, ignore)
{
    private readonly ByteArrayDictionary _values = new();

    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            b.AppendNull();
            return;
        }
        if (reader.TokenType != JsonTokenType.String || binary)
        {
            Mismatch(ref reader, line, "a dictionary-encoded string", binary ? "dictionary-encoded binaries are not read" : null);
            return;
        }
        Span<byte> scratch = stackalloc byte[StackBytes];
            ReadOnlySpan<byte> text = Unescaped(in reader, scratch, out byte[]? rented);
        int count = _values.Count;
        int index = _values.GetOrAdd(text);
        if (index == count) b.Entries.Append(text);
        if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        b.Append(index);
    }

    public override void EndBatch() => _values.Clear();

    public override void Dispose() => _values.Dispose();
}

/// <summary>Lists (and fixed-size lists, whose arrays must have exactly their size): JSON arrays.</summary>
internal sealed class ListColumn : JsonColumn
{
    private readonly JsonColumn _items;
    private readonly int _fixedSize;

    public ListColumn(NativeColumnBuilder builder, NativeColumnBuilder values, Field item, string path, bool ignore, int fixedSize)
        : base(builder, path, ignore)
    {
        _items = For(item, values, path + "[]", ignore);
        _fixedSize = fixedSize;
    }

    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            Builder.AppendNull();
            return;
        }
        if (reader.TokenType != JsonTokenType.StartArray)
        {
            Mismatch(ref reader, line, "a list");
            return;
        }
        int count = 0;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
        {
            _items.Read(ref reader, line);
            count++;
        }
        switch (Builder)
        {
            case NativeListBuilder list: list.EndList(); break;
            case NativeLargeListBuilder large: large.EndList(); break;
            case NativeFixedSizeListBuilder fixedList:
                if (count != _fixedSize)
                    throw new JsonValueException($"Column '{Path}': an array of {count} values is not an array of {_fixedSize}.");
                fixedList.AppendSlots(1);
                break;
        }
    }

    public override void Reserve(long rows, long dataBytes) => Builder.Reserve(rows);

    public override void EndBatch() => _items.EndBatch();

    public override void Dispose() => _items.Dispose();
}

/// <summary>Maps: JSON objects, their names the keys (strings) and their values the map's values.</summary>
internal sealed class MapColumn : JsonColumn
{
    private readonly NativeListBuilder _list;
    private readonly NativeStructBuilder _entries;
    private readonly IBytesBuilder _keys;
    private readonly JsonColumn _values;

    public MapColumn(MapType type, NativeListBuilder builder, string path, bool ignore) : base(builder, path, ignore)
    {
        if (type.KeyField.DataType is not (StringType or LargeStringType or StringViewType))
            throw new NotSupportedException($"Column '{path}': the keys of a map read from JSON are strings, not {type.KeyField.DataType.Name}.");
        _list = builder;
        _entries = (NativeStructBuilder)builder.Values;
        _keys = (IBytesBuilder)_entries.Children[0];
        _values = For(type.ValueField, _entries.Children[1], path + "{}", ignore);
    }

    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            _list.AppendNull();
            return;
        }
        if (reader.TokenType != JsonTokenType.StartObject)
        {
            Mismatch(ref reader, line, "a map (an object)");
            return;
        }
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            ReadOnlySpan<byte> key = Text(in reader, out byte[]? rented);
            _keys.Append(key);
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
            reader.Read();
            _values.Read(ref reader, line);
            _entries.EndStruct();
        }
        _list.EndList();
    }

    public override void Reserve(long rows, long dataBytes) => Builder.Reserve(rows);

    public override void EndBatch() => _values.EndBatch();

    public override void Dispose() => _values.Dispose();
}

/// <summary>
/// Structs: JSON objects, fields matched by name (the first of repeated names kept), fields the struct does not have
/// passed over, fields an object lacks null. The field after the one just read is tried first (objects mostly keep
/// their fields in one order).
/// </summary>
internal sealed class StructColumn : JsonColumn
{
    private const int LinearFields = 12;

    private readonly NativeStructBuilder _b;
    private readonly byte[][] _names;
    private readonly JsonColumn[] _children;
    private readonly bool[] _seen;
    private readonly ByteArrayDictionary? _lookup;

    public StructColumn(StructType type, NativeStructBuilder builder, string path, bool ignore) : base(builder, path, ignore)
    {
        _b = builder;
        _names = [.. type.Fields.Select(f => Encoding.UTF8.GetBytes(f.Name))];
        _children = [.. type.Fields.Select((f, i) => For(f, builder.Children[i], path.Length == 0 ? f.Name : path + "." + f.Name, ignore))];
        _seen = new bool[_names.Length];
        if (_names.Length > LinearFields)
        {
            _lookup = new ByteArrayDictionary();
            foreach (byte[] name in _names) _lookup.GetOrAdd(name);
        }
    }

    public new NativeStructBuilder Builder => _b;

    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        if (reader.TokenType == JsonTokenType.StartObject) ReadObject(ref reader, line);
        else if (reader.TokenType == JsonTokenType.Null) _b.AppendNull();
        else Mismatch(ref reader, line, "a struct (an object)");
    }

    /// <summary>One object (the reader on its start), its fields into the children, then the struct's slot.</summary>
    public void ReadObject(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        Span<bool> seen = _seen;
        seen.Clear();
        int expected = 0, found = 0;
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            int index = Find(in reader, expected);
            if (index < 0 || seen[index])
            {
                reader.Skip();
                continue;
            }
            reader.Read();
            _children[index].Read(ref reader, line);
            seen[index] = true;
            found++;
            expected = index + 1;
        }
        if (found < _children.Length)
            for (int i = 0; i < _children.Length; i++)
                if (!seen[i]) _children[i].Missing();
        _b.EndStruct();
    }

    private int Find(in Utf8JsonReader reader, int expected)
    {
        if (!reader.ValueIsEscaped)
        {
            ReadOnlySpan<byte> name = reader.ValueSpan;
            if (expected < _names.Length && name.SequenceEqual(_names[expected])) return expected;
            if (_lookup is not null) return _lookup.IndexOf(name);
            for (int i = 0; i < _names.Length; i++)
                if (name.SequenceEqual(_names[i])) return i;
            return -1;
        }
        return FindEscaped(in reader);
    }

    private int FindEscaped(in Utf8JsonReader reader)
    {
        Span<byte> scratch = stackalloc byte[StackBytes];
        ReadOnlySpan<byte> unescaped = Unescaped(in reader, scratch, out byte[]? rented);
        int found = -1;
        if (_lookup is not null) found = _lookup.IndexOf(unescaped);
        else
            for (int i = 0; i < _names.Length && found < 0; i++)
                if (unescaped.SequenceEqual(_names[i])) found = i;
        if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        return found;
    }

    public override void Reserve(long rows, long dataBytes)
    {
        _b.Reserve(rows);
        long each = dataBytes / Math.Max(1, _children.Length);
        foreach (JsonColumn child in _children) child.Reserve(rows, each);
    }

    public override void EndBatch()
    {
        foreach (JsonColumn child in _children) child.EndBatch();
    }

    public override void Dispose()
    {
        _lookup?.Dispose();
        foreach (JsonColumn child in _children) child.Dispose();
    }
}
