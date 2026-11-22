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

    /// <summary>
    /// The decoder of a column of <paramref name="field"/>'s type over <paramref name="builder"/>; with
    /// <paramref name="strict"/>, an object's field its struct type lacks is an error (in structs, also in lists); with
    /// <paramref name="keysIgnoreCase"/>, fields match struct members ignoring case when none matches exactly. A field
    /// that is not nullable takes no nulls (unless <paramref name="ignoreErrors"/>).
    /// </summary>
    public static JsonColumn For(Field field, NativeColumnBuilder builder, string path, bool ignoreErrors, bool strict = false, bool keysIgnoreCase = false)
    {
        JsonColumn column = Of(field, builder, path, ignoreErrors, strict, keysIgnoreCase);
        return field.IsNullable || ignoreErrors ? column : new RequiredColumn(column);
    }

    /// <summary>The metadata key of a binary column of 16-byte GUIDs (as .NET lays them out) written and read as GUID strings.</summary>
    public const string GuidKey = "kaminari.json.guid";

    /// <summary>The metadata key of a string column of an enum's member names, the members as <see cref="EnumNameColumn.Members"/> lists them.</summary>
    public const string EnumKey = "kaminari.json.enum";

    /// <summary>The metadata key of a large list of key / value structs (string keys) written as JSON objects.</summary>
    public const string ObjectKey = "kaminari.json.object";

    private static JsonColumn Of(Field field, NativeColumnBuilder builder, string path, bool ignoreErrors, bool strict, bool keysIgnoreCase) => field.DataType switch
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
        StringType or LargeStringType or StringViewType when field.Metadata?.TryGetValue(EnumKey, out string? members) == true
            => new EnumNameColumn(builder, path, ignoreErrors, members),
        StringType or LargeStringType or StringViewType => new StringColumn(builder, path, ignoreErrors),
        BinaryType or LargeBinaryType or BinaryViewType when field.Metadata?.ContainsKey(GuidKey) == true => new GuidColumn(builder, path, ignoreErrors),
        BinaryType or LargeBinaryType or BinaryViewType => new BinaryColumn(builder, path, ignoreErrors),
        Date32Type => new DateColumn(builder, path, ignoreErrors, millis: false),
        Date64Type => new DateColumn(builder, path, ignoreErrors, millis: true),
        TimestampType t => new TemporalColumn(builder, path, ignoreErrors, TemporalKind.Timestamp, UnitNanos(t.Unit)),
        Time32Type t => new TemporalColumn(builder, path, ignoreErrors, TemporalKind.Time32, UnitNanos(t.Unit)),
        Time64Type t => new TemporalColumn(builder, path, ignoreErrors, TemporalKind.Time64, UnitNanos(t.Unit)),
        DurationType t => new TemporalColumn(builder, path, ignoreErrors, TemporalKind.Duration, UnitNanos(t.Unit)),
        DictionaryType { ValueType: StringType or BinaryType } d => new DictionaryColumn((NativeDictionaryBuilder)builder, path, ignoreErrors, d.ValueType is BinaryType),
        MapType m => new MapColumn(m, (NativeListBuilder)builder, path, ignoreErrors, keysIgnoreCase),
        ListType l => new ListColumn(builder, ((NativeListBuilder)builder).Values, l.ValueField, path, ignoreErrors, fixedSize: -1, strict, keysIgnoreCase),
        LargeListType l => new ListColumn(builder, ((NativeLargeListBuilder)builder).Values, l.ValueField, path, ignoreErrors, fixedSize: -1, strict, keysIgnoreCase),
        FixedSizeListType l => new ListColumn(builder, ((NativeFixedSizeListBuilder)builder).Values, l.ValueField, path, ignoreErrors, l.ListSize, strict, keysIgnoreCase),
        StructType s => new StructColumn(s, (NativeStructBuilder)builder, path, ignoreErrors, strict, keysIgnoreCase: keysIgnoreCase),
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

/// <summary>GUIDs: strings such as <c>0f8fad5b-d9cb-469f-a165-70867728950e</c>, kept as .NET's 16 bytes.</summary>
internal sealed class GuidColumn(NativeColumnBuilder b, string path, bool ignore) : JsonColumn(b, path, ignore)
{
    private readonly IBytesBuilder _b = (IBytesBuilder)b;

    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            Builder.AppendNull();
            return;
        }
        if (reader.TokenType == JsonTokenType.String && !reader.ValueIsEscaped
            && Utf8Parser.TryParse(reader.ValueSpan, out Guid value, out int used, 'D') && used == reader.ValueSpan.Length)
        {
            Span<byte> bytes = stackalloc byte[16];
            value.TryWriteBytes(bytes);
            _b.Append(bytes);
            return;
        }
        Mismatch(ref reader, line, "a GUID", "GUIDs are strings such as 0f8fad5b-d9cb-469f-a165-70867728950e");
    }
}

/// <summary>
/// An enum's member names (a string column marked <see cref="JsonColumn.EnumKey"/>): a member's name (exactly, else
/// ignoring case) or number (a JSON number or numeric text), written as the member's name, so that the column holds
/// member names exactly (EnumCategories), as readers' filters compare them and the rows read them. Any other value,
/// a number no member has too, is a mismatch.
/// </summary>
internal sealed class EnumNameColumn : JsonColumn
{
    private readonly IBytesBuilder _b;
    // The members' names, as given (exact matches) and folded to lower case; the member of each folded name.
    private readonly ByteArrayDictionary _names = new(), _folded = new();
    private readonly List<int> _foldedMember = [];
    // The member named for each value (its first member: the canonical name).
    private readonly Dictionary<Int128, int> _byValue = [];

    public EnumNameColumn(NativeColumnBuilder b, string path, bool ignore, string members) : base(b, path, ignore)
    {
        _b = (IBytesBuilder)b;
        Span<byte> scratch = stackalloc byte[StackBytes];
        foreach ((string name, Int128 value) in Parse(members))
        {
            int member = _names.GetOrAdd(Encoding.UTF8.GetBytes(name));
            if (_folded.GetOrAdd(Fold(Encoding.UTF8.GetBytes(name), scratch, out byte[]? rented)) == _foldedMember.Count) _foldedMember.Add(member);
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
            _byValue.TryAdd(value, member);
        }
    }

    /// <summary>
    /// The members of <paramref name="enumType"/> as the column's metadata lists them: canonical names first (each
    /// value's), then aliases, each as its name's length, ';', the name, its value, ';'.
    /// </summary>
    public static string Members(Type enumType)
    {
        StringBuilder text = new();
        string[] categories = Kaminari.Sazanami.Marshalling.EnumCategories.GetCategories(enumType);
        (string Name, object Value)[] members = Kaminari.Sazanami.Marshalling.EnumCategories.GetMembers(enumType);
        foreach ((string name, object value) in members.Where(m => categories.Contains(m.Name)).Concat(members.Where(m => !categories.Contains(m.Name))))
            text.Append(name.Length).Append(';').Append(name).Append(Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)).Append(';');
        return text.ToString();
    }

    private static IEnumerable<(string Name, Int128 Value)> Parse(string members)
    {
        for (int at = 0; at < members.Length;)
        {
            int semicolon = members.IndexOf(';', at);
            int length = int.Parse(members.AsSpan(at, semicolon - at), System.Globalization.CultureInfo.InvariantCulture);
            string name = members.Substring(semicolon + 1, length);
            int end = members.IndexOf(';', semicolon + 1 + length);
            yield return (name, Int128.Parse(members.AsSpan(semicolon + 1 + length, end - semicolon - 1 - length), System.Globalization.CultureInfo.InvariantCulture));
            at = end + 1;
        }
    }

    // Text in lower case (ASCII in place, any other through .NET's invariant lower case).
    private static ReadOnlySpan<byte> Fold(ReadOnlySpan<byte> text, Span<byte> scratch, out byte[]? rented)
    {
        rented = null;
        if (Ascii.IsValid(text))
        {
            Span<byte> lower = text.Length <= scratch.Length ? scratch : rented = ArrayPool<byte>.Shared.Rent(text.Length);
            Ascii.ToLower(text, lower, out int written);
            return lower[..written];
        }
        return Encoding.UTF8.GetBytes(Encoding.UTF8.GetString(text).ToLowerInvariant());
    }

    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.Null:
                Builder.AppendNull();
                return;
            case JsonTokenType.Number:
                if (reader.TryGetInt64(out long signed) && Append(signed)) return;
                if (reader.TryGetUInt64(out ulong unsigned) && Append(unsigned)) return;
                break;
            case JsonTokenType.String:
            {
                Span<byte> scratch = stackalloc byte[StackBytes];
                ReadOnlySpan<byte> text = Unescaped(in reader, scratch, out byte[]? rented);
                bool done = AppendText(text.Trim(" \t\r\n"u8));
                if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
                if (done) return;
                break;
            }
        }
        Mismatch(ref reader, line, "a member of the enum", "an enum is read from a member's name, in any case, or its number");
    }

    private bool AppendText(ReadOnlySpan<byte> text)
    {
        int member = _names.IndexOf(text);
        if (member < 0)
        {
            Span<byte> scratch = stackalloc byte[StackBytes];
            int folded = _folded.IndexOf(Fold(text, scratch, out byte[]? rented));
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
            if (folded >= 0) member = _foldedMember[folded];
        }
        if (member >= 0)
        {
            _b.Append(_names[member]);
            return true;
        }
        if (Utf8Parser.TryParse(text, out long signed, out int used) && used == text.Length) return Append(signed);
        return Utf8Parser.TryParse(text, out ulong unsigned, out used) && used == text.Length && Append(unsigned);
    }

    private bool Append(Int128 value)
    {
        if (!_byValue.TryGetValue(value, out int member)) return false;
        _b.Append(_names[member]);
        return true;
    }

    public override void Dispose()
    {
        _names.Dispose();
        _folded.Dispose();
    }
}

/// <summary>A column that takes no nulls: a null or a missing field is an error.</summary>
internal sealed class RequiredColumn(JsonColumn inner) : JsonColumn(inner.Builder, inner.Path, ignoreErrors: false)
{
    public override void Read(ref Utf8JsonReader reader, ReadOnlySpan<byte> line)
    {
        if (reader.TokenType == JsonTokenType.Null) throw new JsonValueException($"Column '{Path}': null, but the column takes no nulls.");
        inner.Read(ref reader, line);
    }

    public override void Missing() => throw new JsonValueException($"Column '{Path}': the field is missing, but the column takes no nulls.");

    public override void Reserve(long rows, long dataBytes) => inner.Reserve(rows, dataBytes);

    public override void EndBatch() => inner.EndBatch();

    public override void Dispose() => inner.Dispose();
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
                TemporalKind.Duration => IsoTemporal.TryParseDuration(text, out nanos) || TryClock(text, out nanos),
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
            TemporalKind.Duration => "durations are ISO 8601 strings such as PT1.5S, or [-][d.]hh:mm:ss[.f]",
            _ => "times are HH:mm:ss[.f] strings",
        }));

        // .NET's constant time span text, [-][d.]hh:mm:ss[.fffffff] (never a bare number: hours and minutes are there).
        static bool TryClock(ReadOnlySpan<byte> text, out Int128 nanos)
        {
            nanos = 0;
            if (text.IndexOf((byte)':') < 0 || !Utf8Parser.TryParse(text, out TimeSpan span, out int used, 'c') || used != text.Length) return false;
            nanos = (Int128)span.Ticks * 100;
            return true;
        }

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

    public ListColumn(NativeColumnBuilder builder, NativeColumnBuilder values, Field item, string path, bool ignore, int fixedSize, bool strict,
        bool keysIgnoreCase)
        : base(builder, path, ignore)
    {
        _items = For(item, values, path + "[]", ignore, strict, keysIgnoreCase);
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

    public MapColumn(MapType type, NativeListBuilder builder, string path, bool ignore, bool keysIgnoreCase) : base(builder, path, ignore)
    {
        if (type.KeyField.DataType is not (StringType or LargeStringType or StringViewType))
            throw new NotSupportedException($"Column '{path}': the keys of a map read from JSON are strings, not {type.KeyField.DataType.Name}.");
        _list = builder;
        _entries = (NativeStructBuilder)builder.Values;
        _keys = (IBytesBuilder)_entries.Children[0];
        _values = For(type.ValueField, _entries.Children[1], path + "{}", ignore, keysIgnoreCase: keysIgnoreCase);
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
    private readonly bool _strict;
    private readonly byte[][] _passedOver;
    // Ignoring case: the members by name, and the field names met so far with the member each matches (-1: none).
    private readonly Dictionary<string, int>? _folded;
    private readonly ByteArrayDictionary? _met;
    private readonly List<int>? _metMember;
    private const int MostMet = 1024;

    /// <param name="type">The struct type (the records' columns at the top).</param>
    /// <param name="builder">Its builder.</param>
    /// <param name="path">The struct's path ("" for the records).</param>
    /// <param name="ignore">Values that do not fit their type are nulls.</param>
    /// <param name="strict">An object's field the type lacks is an error (the type was inferred from other objects).</param>
    /// <param name="passedOver">Fields passed over without an error even when strict (columns left out of a read).</param>
    /// <param name="keysIgnoreCase">A field matches a member ignoring case when none matches exactly (here and below).</param>
    public StructColumn(StructType type, NativeStructBuilder builder, string path, bool ignore, bool strict = false, IEnumerable<string>? passedOver = null,
        bool keysIgnoreCase = false)
        : base(builder, path, ignore)
    {
        _b = builder;
        _strict = strict;
        _passedOver = [.. (passedOver ?? []).Select(Encoding.UTF8.GetBytes)];
        _names = [.. type.Fields.Select(f => Encoding.UTF8.GetBytes(f.Name))];
        _children = [.. type.Fields.Select((f, i) => For(f, builder.Children[i], path.Length == 0 ? f.Name : path + "." + f.Name, ignore, strict, keysIgnoreCase))];
        if (keysIgnoreCase)
        {
            _folded = new(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < type.Fields.Count; i++) _folded.TryAdd(type.Fields[i].Name, i);
            _met = new ByteArrayDictionary();
            _metMember = [];
        }
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
                if (index < 0 && _strict) Unknown(in reader);
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

    // A field the inferred type lacks: an error, unless it is a column left out of the read.
    private void Unknown(in Utf8JsonReader reader)
    {
        string name = reader.GetString()!;
        if (_passedOver.Length > 0)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(name);
            foreach (byte[] passed in _passedOver)
                if (bytes.AsSpan().SequenceEqual(passed)) return;
        }
        throw Extra(Path, name);
    }

    /// <summary>The error of a field first seen after the records the columns were inferred from.</summary>
    public static JsonValueException Extra(string path, string name) =>
        new($"{(path.Length == 0 ? "The" : $"Column '{path}': the")} field '{name}' is not in the records the columns were inferred from (infer from more records, or give the schema to pass such fields over).");

    private int Find(in Utf8JsonReader reader, int expected)
    {
        if (!reader.ValueIsEscaped)
        {
            ReadOnlySpan<byte> name = reader.ValueSpan;
            if (expected < _names.Length && name.SequenceEqual(_names[expected])) return expected;
            int found = Exact(name);
            return found < 0 && _folded is not null ? IgnoringCase(name) : found;
        }
        return FindEscaped(in reader);
    }

    private int Exact(ReadOnlySpan<byte> name)
    {
        if (_lookup is not null) return _lookup.IndexOf(name);
        for (int i = 0; i < _names.Length; i++)
            if (name.SequenceEqual(_names[i])) return i;
        return -1;
    }

    private int FindEscaped(in Utf8JsonReader reader)
    {
        Span<byte> scratch = stackalloc byte[StackBytes];
        ReadOnlySpan<byte> unescaped = Unescaped(in reader, scratch, out byte[]? rented);
        int found = Exact(unescaped);
        if (found < 0 && _folded is not null) found = IgnoringCase(unescaped);
        if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        return found;
    }

    // The member a field matches ignoring case (-1: none), remembered for the field names met (records repeat them).
    private int IgnoringCase(ReadOnlySpan<byte> name)
    {
        int met = _met!.IndexOf(name);
        if (met >= 0) return _metMember![met];
        int found = _folded!.TryGetValue(Encoding.UTF8.GetString(name), out int member) ? member : -1;
        if (_metMember!.Count < MostMet)
        {
            _met.GetOrAdd(name);
            _metMember.Add(found);
        }
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
        _met?.Dispose();
        foreach (JsonColumn child in _children) child.Dispose();
    }
}
