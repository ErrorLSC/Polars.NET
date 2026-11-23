using System.Globalization;
using System.Numerics;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Sazanami.Encodings;
using Kaminari.Sazanami.Marshalling;
using Kaminari.Sazanami.Text;

namespace Kaminari.Seseragi.Files.Csv.Reading;

/// <summary>A value its column refused: the column, the field's text and why (the reader adds the row and line).</summary>
internal sealed record CsvValueError(string Column, string Value, string Message);

/// <summary>How fields become values, the same for every column: nulls, booleans and number forms.</summary>
internal sealed class CsvValueRules(CsvTokenizer tokenizer, bool missingIsNull, bool ignoreErrors, byte[][] nullValues, byte[][] trueValues,
    byte[][] falseValues, bool decimalComma, byte[]? thousands)
{
    public CsvTokenizer Tokenizer { get; } = tokenizer;
    public bool MissingIsNull { get; } = missingIsNull;
    public bool IgnoreErrors { get; } = ignoreErrors;
    public byte[][] NullValues { get; } = nullValues;
    public byte[][] TrueValues { get; } = trueValues;
    public byte[][] FalseValues { get; } = falseValues;
    public bool DecimalComma { get; } = decimalComma;

    /// <summary>The thousands separator's UTF-8 bytes, or null.</summary>
    public byte[]? Thousands { get; } = thousands;

    /// <summary>Whether numbers need rewriting before they are parsed (a decimal comma, groups).</summary>
    public bool RewritesNumbers => DecimalComma || Thousands is not null;

    public bool IsNullValue(ReadOnlySpan<byte> text)
    {
        foreach (byte[] value in NullValues)
            if (text.SequenceEqual(value)) return true;
        return false;
    }

    public static bool In(byte[][] values, ReadOnlySpan<byte> text)
    {
        foreach (byte[] value in values)
            if (text.SequenceEqual(value)) return true;
        return false;
    }

    /// <summary>
    /// A number's text as the parsers read it, in <paramref name="destination"/> (as long as the text at least):
    /// groups of <see cref="Thousands"/> checked and removed (one to three digits, then groups of exactly three, before
    /// the decimal point), a decimal comma made a point. False when the groups are wrong.
    /// </summary>
    public bool Rewrite(ReadOnlySpan<byte> text, Span<byte> destination, out int written)
    {
        written = 0;
        byte point = DecimalComma ? (byte)',' : (byte)'.';
        ReadOnlySpan<byte> sep = Thousands;
        int i = 0;
        if (sep.Length > 0 && text.IndexOf(sep) >= 0)
        {
            if (i < text.Length && text[i] is (byte)'-' or (byte)'+') destination[written++] = text[i++];
            int run = 0, groups = 0;
            while (i < text.Length)
            {
                byte c = text[i];
                if (c - (uint)'0' <= 9)
                {
                    destination[written++] = c;
                    run++;
                    i++;
                    continue;
                }
                if (text[i..].StartsWith(sep))
                {
                    if (groups == 0 ? run is < 1 or > 3 : run != 3) return false;
                    groups++;
                    run = 0;
                    i += sep.Length;
                    continue;
                }
                break;
            }
            if (groups > 0 && run != 3) return false;
        }
        for (; i < text.Length; i++)
        {
            byte c = text[i];
            destination[written++] = DecimalComma && c == point ? (byte)'.' : c;
        }
        return true;
    }
}

/// <summary>
/// Decodes one column's fields of a chunk of records into its builder (native memory). A refused value is a
/// <see cref="CsvValueError"/> (or a null with IgnoreErrors).
/// </summary>
internal abstract class CsvColumn(NativeColumnBuilder builder, string name, CsvValueRules rules) : IDisposable
{
    /// <summary>Texts of up to this many bytes are rewritten on the stack.</summary>
    protected const int StackBytes = 128;

    private NativeByteBuffer? _scratch;

    public NativeColumnBuilder Builder { get; } = builder;

    public string Name { get; } = name;

    protected CsvValueRules Rules { get; } = rules;

    /// <summary>
    /// Decodes rows [0, <paramref name="count"/>) of <paramref name="fields"/>; returns -1, or the first row refused
    /// (<paramref name="error"/> then says why; rows after it are not decoded).
    /// </summary>
    public abstract int Decode(ReadOnlySpan<byte> unit, ReadOnlySpan<CsvField> fields, int count, out CsvValueError? error);

    /// <summary>After each exported batch (columns with per-batch state reset it).</summary>
    public virtual void EndBatch() { }

    /// <summary>Expects <paramref name="rows"/> more values, <paramref name="sample"/> the first fields of them (to size variable-length data).</summary>
    public virtual void Reserve(long rows, ReadOnlySpan<CsvField> sample) => Builder.Reserve(rows);

    public virtual void Dispose() => _scratch?.Release();

    /// <summary>A native scratch buffer of at least <paramref name="bytes"/>.</summary>
    protected Span<byte> Scratch(int bytes)
    {
        _scratch ??= new NativeByteBuffer(Math.Max(bytes, 4096), pooled: true);
        _scratch.EnsureCapacity(bytes);
        return _scratch.Span;
    }

    /// <summary>
    /// The field's text (unescaped when quotes are doubled in it, into scratch); false when it is a null: absent, empty
    /// (for columns other than strings), or a null value.
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    protected bool Text(ReadOnlySpan<byte> unit, CsvField field, out ReadOnlySpan<byte> text)
    {
        text = default;
        if (field.Bits <= 0) return false;
        text = unit.Slice(field.Start, field.Length);
        if ((field.Bits & CsvField.Escaped) != 0)
        {
            Span<byte> scratch = Scratch(text.Length);
            text = scratch[..Rules.Tokenizer.Unescape(text, scratch)];
        }
        if (text.IsEmpty) return false;
        return !_nulls || !Rules.IsNullValue(text);
    }

    private readonly bool _nulls = rules.NullValues.Length > 0;

    /// <summary>
    /// The rows the column's type was inferred from (a value past them that the type cannot hold names them and the
    /// fixes); 0 when the type was given (or inferred from the whole input).
    /// </summary>
    public int InferredFrom { get; set; }

    /// <summary>A refused value: a null with IgnoreErrors (true), else the error (false).</summary>
    /// <param name="text">The field's text.</param>
    /// <param name="why">The reason, "{value}" standing for the text.</param>
    /// <param name="error">The error.</param>
    /// <param name="hint">Whether an inferred column's reason is followed by the fixes (false when it names them itself).</param>
    protected bool Refuse(ReadOnlySpan<byte> text, string why, out CsvValueError? error, bool hint = true)
    {
        if (Rules.IgnoreErrors)
        {
            Builder.AppendNull();
            error = null;
            return true;
        }
        string shown = Shown(text);
        string fixes = hint && InferredFrom > 0
            ? $" (the column's type was inferred from the first {InferredFrom} rows: give the column its type in SchemaOverrides, or a larger InferSchemaLength)"
            : "";
        error = new CsvValueError(Name, shown, $"column '{Name}': {why.Replace("{value}", $"'{shown}'")}{fixes}");
        return false;
    }

    /// <summary>A value's text for errors (shortened).</summary>
    public static string Shown(ReadOnlySpan<byte> text)
    {
        ReadOnlySpan<byte> head = text[..Math.Min(text.Length, 60)];
        // Cut whole UTF-8 sequences only.
        while (head.Length < text.Length && head.Length > 0 && (head[^1] & 0xC0) == 0x80) head = head[..^1];
        if (head.Length < text.Length && head.Length > 0 && head[^1] >= 0xC0) head = head[..^1];
        return Encoding.UTF8.GetString(head) + (head.Length < text.Length ? "…" : "");
    }

    /// <summary>The text after leading spaces and tabs (typed numbers skip them, as the engine's builders do).</summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    protected static ReadOnlySpan<byte> TrimStart(ReadOnlySpan<byte> text) => text[0] is (byte)' ' or (byte)'\t' ? Trimmed(text) : text;

    private static ReadOnlySpan<byte> Trimmed(ReadOnlySpan<byte> text)
    {
        int i = 0;
        while (i < text.Length && text[i] is (byte)' ' or (byte)'\t') i++;
        return text[i..];
    }

    /// <summary>A number's text rewritten for the parsers (see <see cref="CsvValueRules.Rewrite"/>), on the stack or in scratch; false: wrong groups.</summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    protected bool Number(ReadOnlySpan<byte> text, Span<byte> stack, out ReadOnlySpan<byte> number)
    {
        number = text;
        return !_rewrites || Rewritten(text, stack, out number);
    }

    private readonly bool _rewrites = rules.RewritesNumbers;

    private bool Rewritten(ReadOnlySpan<byte> text, Span<byte> stack, out ReadOnlySpan<byte> number)
    {
        number = text;
        Span<byte> destination = text.Length <= stack.Length ? stack : Scratch(text.Length);
        if (!Rules.Rewrite(text, destination, out int written)) return false;
        number = destination[..written];
        return true;
    }

    /// <summary>The decoder of a column of <paramref name="field"/>'s type.</summary>
    public static CsvColumn For(Field field, NativeColumnBuilder builder, CsvValueRules rules, TemporalPattern? group)
    {
        string name = field.Name;
        return field.DataType switch
        {
            BooleanType => new BooleanColumn(builder, name, rules),
            Int8Type => new IntegerColumn<sbyte>(builder, name, rules, "an Int8 integer"),
            Int16Type => new IntegerColumn<short>(builder, name, rules, "an Int16 integer"),
            Int32Type => new IntegerColumn<int>(builder, name, rules, "an Int32 integer"),
            Int64Type => new IntegerColumn<long>(builder, name, rules, "an Int64 integer"),
            UInt8Type => new IntegerColumn<byte>(builder, name, rules, "a UInt8 integer"),
            UInt16Type => new IntegerColumn<ushort>(builder, name, rules, "a UInt16 integer"),
            UInt32Type => new IntegerColumn<uint>(builder, name, rules, "a UInt32 integer"),
            UInt64Type => new IntegerColumn<ulong>(builder, name, rules, "a UInt64 integer"),
            Decimal128Type when WideIntegers.KindOf(field) is { } kind => new WideIntegerColumn(builder, name, rules, kind == WideIntegers.Unsigned),
            Decimal128Type d => new DecimalColumn(builder, name, rules, d),
            DoubleType => new DoubleColumn(builder, name, rules),
            FloatType => new FloatColumn(builder, name, rules),
            Date32Type => new DateColumn(builder, name, rules, group),
            TimestampType t => new DatetimeColumn(builder, name, rules, t, group),
            Time64Type t => new TimeColumn(builder, name, rules, t.Unit),
            StringType or LargeStringType or StringViewType => new StringColumn(builder, name, rules),
            DictionaryType { ValueType: StringType } when field.Metadata?.GetValueOrDefault(EnumCategories.EnumMetadataKey) is { } encoded =>
                new EnumColumn((NativeDictionaryBuilder)builder, name, rules, EnumValues(encoded)),
            DictionaryType { ValueType: StringType } => new DictionaryColumn((NativeDictionaryBuilder)builder, name, rules),
            _ => throw new NotSupportedException($"Column '{name}': CSV reads no values of type {field.DataType}."),
        };
    }

    /// <summary>The categories of an Enum's metadata (each prefixed with its UTF-8 byte length and ';').</summary>
    public static string[] EnumValues(string encoded)
    {
        List<string> values = [];
        byte[] bytes = Encoding.UTF8.GetBytes(encoded);
        int at = 0;
        while (at < bytes.Length)
        {
            int semicolon = System.Array.IndexOf(bytes, (byte)';', at);
            if (semicolon < 0 || !int.TryParse(bytes.AsSpan(at, semicolon - at), NumberStyles.None, CultureInfo.InvariantCulture, out int length)
                || semicolon + 1 + length > bytes.Length)
                throw new ArgumentException($"The Enum categories '{encoded}' are not length-prefixed.");
            values.Add(Encoding.UTF8.GetString(bytes, semicolon + 1, length));
            at = semicolon + 1 + length;
        }
        return [.. values];
    }
}

/// <summary>Booleans: "true" / "false" in any (ASCII) case, and the true and false values given.</summary>
internal sealed class BooleanColumn(NativeColumnBuilder b, string name, CsvValueRules rules) : CsvColumn(b, name, rules)
{
    private readonly NativeBooleanBuilder _b = (NativeBooleanBuilder)b;

    public override int Decode(ReadOnlySpan<byte> unit, ReadOnlySpan<CsvField> fields, int count, out CsvValueError? error)
    {
        error = null;
        for (int r = 0; r < count; r++)
        {
            if (!Text(unit, fields[r], out ReadOnlySpan<byte> text))
            {
                _b.AppendNull();
                continue;
            }
            if (Ascii.EqualsIgnoreCase(text, "true"u8) || (Rules.TrueValues.Length > 0 && CsvValueRules.In(Rules.TrueValues, text))) _b.Append(true);
            else if (Ascii.EqualsIgnoreCase(text, "false"u8) || (Rules.FalseValues.Length > 0 && CsvValueRules.In(Rules.FalseValues, text))) _b.Append(false);
            else if (!Refuse(text, "{value} is not a boolean (true or false" + (Rules.TrueValues.Length + Rules.FalseValues.Length > 0 ? ", or a true or false value given" : "") + ")", out error))
                return r;
        }
        return -1;
    }
}

/// <summary>Integers of up to 64 bits: optional leading spaces, a sign ('-' for signed types only), decimal digits.</summary>
internal sealed class IntegerColumn<T>(NativeColumnBuilder b, string name, CsvValueRules rules, string kind) : CsvColumn(b, name, rules)
    where T : unmanaged, IBinaryInteger<T>, IMinMaxValue<T>
{
    private readonly NativeFixedWidthBuilder<T> _b = (NativeFixedWidthBuilder<T>)b;
    private static readonly bool Signed = T.IsNegative(T.MinValue);

    public override int Decode(ReadOnlySpan<byte> unit, ReadOnlySpan<CsvField> fields, int count, out CsvValueError? error)
    {
        error = null;
        Span<byte> stack = stackalloc byte[StackBytes];
        for (int r = 0; r < count; r++)
        {
            if (!Text(unit, fields[r], out ReadOnlySpan<byte> text))
            {
                _b.AppendNull();
                continue;
            }
            ReadOnlySpan<byte> trimmed = TrimStart(text);
            if (Number(trimmed, stack, out ReadOnlySpan<byte> number) && Parse(number, out T value)) _b.Append(value);
            else if (!Refuse(text, $"{{value}} is not {kind}{Why(number)}", out error)) return r;
        }
        return -1;
    }

    private static string Why(ReadOnlySpan<byte> number)
    {
        int digits = 0;
        int i = number.Length > 0 && number[0] is (byte)'-' or (byte)'+' ? 1 : 0;
        for (; i < number.Length && number[i] - (uint)'0' <= 9; i++) digits++;
        return digits > 0 && i == number.Length ? " (out of range: give the column a wider type in the schema)" : "";
    }

    private static readonly long Min = long.CreateSaturating(T.MinValue), Max = long.CreateSaturating(T.MaxValue);

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static bool Parse(ReadOnlySpan<byte> text, out T value)
    {
        // Up to 18 digits cannot overflow a long: no check per digit.
        int i = text.Length > 0 && text[0] is (byte)'-' or (byte)'+' ? 1 : 0;
        int digits = text.Length - i;
        if (digits is > 0 and <= 18)
        {
            long acc = 0;
            for (int k = i; k < text.Length; k++)
            {
                uint d = text[k] - (uint)'0';
                if (d > 9)
                {
                    value = default;
                    return false;
                }
                acc = acc * 10 + d;
            }
            if (i == 1 && text[0] == '-') acc = -acc;
            value = T.CreateTruncating(acc);
            return acc >= Min && acc <= Max && (Signed || i == 0 || text[0] != '-');
        }
        return Wide(text, out value);
    }

    private static bool Wide(ReadOnlySpan<byte> text, out T value)
    {
        value = default;
        int i = 0;
        bool negative = false;
        if (text.Length > 0 && text[0] is (byte)'-' or (byte)'+')
        {
            negative = text[0] == '-';
            i = 1;
            if (negative && !Signed) return false;
        }
        if (i == text.Length) return false;
        ulong magnitude = 0;
        for (; i < text.Length; i++)
        {
            uint d = text[i] - (uint)'0';
            if (d > 9) return false;
            if (magnitude > (ulong.MaxValue - d) / 10) return false;
            magnitude = magnitude * 10 + d;
        }
        if (Signed)
        {
            ulong limit = negative ? ulong.CreateTruncating(T.MaxValue) + 1 : ulong.CreateTruncating(T.MaxValue);
            if (magnitude > limit) return false;
            long signedValue = negative ? (long)(0 - magnitude) : (long)magnitude;
            value = T.CreateTruncating(signedValue);
            return true;
        }
        if (magnitude > ulong.CreateTruncating(T.MaxValue)) return false;
        value = T.CreateTruncating(magnitude);
        return true;
    }
}

/// <summary>128-bit integers (Sazanami's <see cref="WideIntegers"/> layout).</summary>
internal sealed class WideIntegerColumn(NativeColumnBuilder b, string name, CsvValueRules rules, bool unsigned) : CsvColumn(b, name, rules)
{
    private readonly NativeFixedWidthBuilder<Int128> _b = (NativeFixedWidthBuilder<Int128>)b;

    public override int Decode(ReadOnlySpan<byte> unit, ReadOnlySpan<CsvField> fields, int count, out CsvValueError? error)
    {
        error = null;
        Span<byte> stack = stackalloc byte[StackBytes];
        for (int r = 0; r < count; r++)
        {
            if (!Text(unit, fields[r], out ReadOnlySpan<byte> text))
            {
                _b.AppendNull();
                continue;
            }
            if (Number(TrimStart(text), stack, out ReadOnlySpan<byte> number))
            {
                if (unsigned && UInt128.TryParse(number, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out UInt128 u))
                {
                    _b.Append((Int128)u);
                    continue;
                }
                if (!unsigned && Int128.TryParse(number, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out Int128 v))
                {
                    _b.Append(v);
                    continue;
                }
            }
            if (!Refuse(text, unsigned ? "{value} is not a UInt128 integer" : "{value} is not an Int128 integer", out error)) return r;
        }
        return -1;
    }
}

/// <summary>Doubles: the fast_float grammar (inf, nan, exponents), after leading spaces.</summary>
internal sealed class DoubleColumn(NativeColumnBuilder b, string name, CsvValueRules rules) : CsvColumn(b, name, rules)
{
    private readonly NativeFixedWidthBuilder<double> _b = (NativeFixedWidthBuilder<double>)b;

    public override int Decode(ReadOnlySpan<byte> unit, ReadOnlySpan<CsvField> fields, int count, out CsvValueError? error)
    {
        error = null;
        Span<byte> stack = stackalloc byte[StackBytes];
        for (int r = 0; r < count; r++)
        {
            if (!Text(unit, fields[r], out ReadOnlySpan<byte> text))
            {
                _b.AppendNull();
                continue;
            }
            if (Number(TrimStart(text), stack, out ReadOnlySpan<byte> number) && FastDouble.TryParse(number, out double value)) _b.Append(value);
            else if (!Refuse(text, "{value} is not a floating-point number", out error)) return r;
        }
        return -1;
    }
}

/// <summary>Floats, parsed straight to single precision.</summary>
internal sealed class FloatColumn(NativeColumnBuilder b, string name, CsvValueRules rules) : CsvColumn(b, name, rules)
{
    private readonly NativeFixedWidthBuilder<float> _b = (NativeFixedWidthBuilder<float>)b;

    public override int Decode(ReadOnlySpan<byte> unit, ReadOnlySpan<CsvField> fields, int count, out CsvValueError? error)
    {
        error = null;
        Span<byte> stack = stackalloc byte[StackBytes];
        for (int r = 0; r < count; r++)
        {
            if (!Text(unit, fields[r], out ReadOnlySpan<byte> text))
            {
                _b.AppendNull();
                continue;
            }
            if (Number(TrimStart(text), stack, out ReadOnlySpan<byte> number) && FastDouble.TryParse(number, out float value)) _b.Append(value);
            else if (!Refuse(text, "{value} is not a floating-point number", out error)) return r;
        }
        return -1;
    }
}

/// <summary>Decimals: exact, a value with more fraction digits than the scale refused (rule R2).</summary>
internal sealed class DecimalColumn(NativeColumnBuilder b, string name, CsvValueRules rules, Decimal128Type type) : CsvColumn(b, name, rules)
{
    private readonly NativeFixedWidthBuilder<Int128> _b = (NativeFixedWidthBuilder<Int128>)b;

    public override int Decode(ReadOnlySpan<byte> unit, ReadOnlySpan<CsvField> fields, int count, out CsvValueError? error)
    {
        error = null;
        Span<byte> stack = stackalloc byte[StackBytes];
        for (int r = 0; r < count; r++)
        {
            if (!Text(unit, fields[r], out ReadOnlySpan<byte> text))
            {
                _b.AppendNull();
                continue;
            }
            if (Number(TrimStart(text), stack, out ReadOnlySpan<byte> number))
            {
                if (DecimalText.TryParse(number, type.Precision, type.Scale, out Int128 unscaled))
                {
                    _b.Append(unscaled);
                    continue;
                }
                int places = FractionDigits(number);
                if (places > type.Scale)
                {
                    int scale = Math.Min(38, places);
                    int precision = Math.Min(38, Math.Max(type.Precision, type.Precision - type.Scale + scale));
                    if (!Refuse(text, $"{{value}} has {places} decimal places, more than the column's scale of {type.Scale} (decimal({type.Precision}, {type.Scale})) " +
                        $"holds: give the column a larger scale in the schema, e.g. decimal({precision}, {scale})", out error)) return r;
                    continue;
                }
                if (places >= 0)
                {
                    if (!Refuse(text, $"{{value}} has more digits than decimal({type.Precision}, {type.Scale}) holds: give the column a larger precision in the schema", out error))
                        return r;
                    continue;
                }
            }
            if (!Refuse(text, $"{{value}} is not a decimal number", out error)) return r;
        }
        return -1;
    }

    // The fraction digits of a plain decimal number ([-+]digits[.digits]), trailing zeros left out; -1 when it is no such number.
    private static int FractionDigits(ReadOnlySpan<byte> text)
    {
        int i = text.Length > 0 && text[0] is (byte)'-' or (byte)'+' ? 1 : 0;
        int start = i;
        while (i < text.Length && text[i] - (uint)'0' <= 9) i++;
        if (i == text.Length) return i > start ? 0 : -1;
        if (text[i] != '.') return -1;
        int point = i++;
        while (i < text.Length && text[i] - (uint)'0' <= 9) i++;
        if (i != text.Length || (point == start && i == point + 1)) return -1;
        int end = i;
        while (end > point + 1 && text[end - 1] == '0') end--;
        return end - point - 1;
    }
}

/// <summary>
/// A temporal column's pattern group: given (from the sample or the reader's order), or taken from the first value that
/// reads in the unit (reported, so the reader checks it against the file's group).
/// </summary>
internal abstract class TemporalColumnBase(NativeColumnBuilder b, string name, CsvValueRules rules, TemporalPattern? group) : CsvColumn(b, name, rules)
{
    protected TemporalColumn Patterns = new() { Group = group };

    /// <summary>Whether the group was given when the unit started.</summary>
    public bool Seeded { get; } = group is not null;

    /// <summary>The group the column took (null while no value read).</summary>
    public TemporalPattern? Group => Patterns.Group;

    /// <summary>Whether a value was refused (or nulled) before the column's group was known.</summary>
    public bool RefusedUnseeded { get; protected set; }

    /// <summary>A refused value; noted when it came before the group was known.</summary>
    protected bool Refused(ReadOnlySpan<byte> text, string why, out CsvValueError? error, bool hint = true)
    {
        if (Patterns.Group is null) RefusedUnseeded = true;
        return Refuse(text, why, out error, hint);
    }
}

/// <summary>Dates (days since the epoch), in the column's pattern group.</summary>
internal sealed class DateColumn(NativeColumnBuilder b, string name, CsvValueRules rules, TemporalPattern? group) : TemporalColumnBase(b, name, rules, group)
{
    private readonly NativeFixedWidthBuilder<int> _b = (NativeFixedWidthBuilder<int>)b;

    public override int Decode(ReadOnlySpan<byte> unit, ReadOnlySpan<CsvField> fields, int count, out CsvValueError? error)
    {
        error = null;
        for (int r = 0; r < count; r++)
        {
            if (!Text(unit, fields[r], out ReadOnlySpan<byte> text))
            {
                _b.AppendNull();
                continue;
            }
            if (Patterns.ReadDate(text, out int days) == TemporalOutcome.Parsed) _b.Append(days);
            else if (!Refused(text, Patterns.Group is { } g ? $"{{value}} is not a date in the column's format ({Describe(g)})" : "{value} is not a date", out error))
                return r;
        }
        return -1;
    }

    public static string Describe(TemporalPattern group) => group switch
    {
        TemporalPattern.DateDmy or TemporalPattern.DatetimeDmy => "day-month-year",
        TemporalPattern.DateYmd or TemporalPattern.DatetimeYmd => "year-month-day",
        TemporalPattern.DatetimeYmdZ => "year-month-day with an offset",
        _ => group.ToString(),
    };
}

/// <summary>Datetimes in the column's unit (an offset or 'Z' read as the instant, UTC), in the column's pattern group.</summary>
internal sealed class DatetimeColumn(NativeColumnBuilder b, string name, CsvValueRules rules, TimestampType type, TemporalPattern? group)
    : TemporalColumnBase(b, name, rules, group)
{
    private readonly NativeFixedWidthBuilder<long> _b = (NativeFixedWidthBuilder<long>)b;

    public override int Decode(ReadOnlySpan<byte> unit, ReadOnlySpan<CsvField> fields, int count, out CsvValueError? error)
    {
        error = null;
        for (int r = 0; r < count; r++)
        {
            if (!Text(unit, fields[r], out ReadOnlySpan<byte> text))
            {
                _b.AppendNull();
                continue;
            }
            switch (Patterns.ReadDatetime(text, type.Unit, out long value))
            {
                case TemporalOutcome.Parsed:
                    _b.Append(value);
                    break;
                case TemporalOutcome.TooFine:
                    // R3: a precision problem, named as one, with the fix (for the schema given, or the type inferred).
                    if (!Refused(text, InferredFrom > 0
                        ? $"{{value}} is more precise than the column's unit ({UnitName(type.Unit)}), inferred from the first {InferredFrom} rows: " +
                          $"give the column a finer unit in SchemaOverrides, e.g. {Finer(type)}, or a larger InferSchemaLength"
                        : $"{{value}} is more precise than the column's unit ({UnitName(type.Unit)}): change the given schema to a finer unit, e.g. {Finer(type)}",
                        out error, hint: false)) return r;
                    break;
                case TemporalOutcome.OutOfRange:
                    if (!Refused(text, InferredFrom > 0
                        ? $"{{value}} is outside the range datetime[{UnitName(type.Unit)}] holds (1677-09-21 to 2262-04-11 in nanoseconds), the unit inferred " +
                          $"from the first {InferredFrom} rows: give the column a coarser unit in SchemaOverrides, e.g. datetime[μs]"
                        : $"{{value}} is outside the range datetime[{UnitName(type.Unit)}] holds (1677-09-21 to 2262-04-11 in nanoseconds): " +
                          "change the given schema to a coarser unit, e.g. datetime[μs]", out error, hint: false)) return r;
                    break;
                default:
                    if (!Refused(text, Patterns.Group is { } g ? $"{{value}} is not a datetime in the column's format ({DateColumn.Describe(g)})" : "{value} is not a datetime", out error))
                        return r;
                    break;
            }
        }
        return -1;
    }

    public static string UnitName(TimeUnit unit) => unit switch
    {
        TimeUnit.Second => "s",
        TimeUnit.Millisecond => "ms",
        TimeUnit.Microsecond => "μs",
        _ => "ns",
    };

    private static string Finer(TimestampType type) => type.Unit switch
    {
        TimeUnit.Second => "datetime[ms]",
        TimeUnit.Millisecond => "datetime[μs]",
        _ => "datetime[ns]",
    } is var finer && type.Timezone is { } tz ? $"{finer[..^1]}, {tz}]" : finer;
}

/// <summary>Times of day: H:M or H:M:S[.f], each value as it is written (rule R4).</summary>
internal sealed class TimeColumn(NativeColumnBuilder b, string name, CsvValueRules rules, TimeUnit timeUnit) : CsvColumn(b, name, rules)
{
    private readonly NativeFixedWidthBuilder<long> _b = (NativeFixedWidthBuilder<long>)b;

    public override int Decode(ReadOnlySpan<byte> unit, ReadOnlySpan<CsvField> fields, int count, out CsvValueError? error)
    {
        error = null;
        long divisor = timeUnit == TimeUnit.Microsecond ? 1000 : 1;
        for (int r = 0; r < count; r++)
        {
            if (!Text(unit, fields[r], out ReadOnlySpan<byte> text))
            {
                _b.AppendNull();
                continue;
            }
            TemporalOutcome outcome = TemporalPatterns.ParseTime(text, out long nanos);
            if (outcome == TemporalOutcome.Parsed && nanos % divisor != 0) outcome = TemporalOutcome.TooFine;
            if (outcome == TemporalOutcome.Parsed) _b.Append(nanos / divisor);
            else if (!Refuse(text, outcome == TemporalOutcome.TooFine
                ? $"{{value}} is more precise than the column's unit ({DatetimeColumn.UnitName(timeUnit)}): change the given schema to a finer unit, e.g. time (nanoseconds)"
                : "{value} is not a time of day (H:M or H:M:S)", out error)) return r;
        }
        return -1;
    }
}

/// <summary>Strings (Utf8, LargeUtf8, Utf8View): the field's text, quotes undoubled; an empty quoted field is "".</summary>
internal sealed class StringColumn(NativeColumnBuilder b, string name, CsvValueRules rules) : CsvColumn(b, name, rules)
{
    private readonly IBytesBuilder _b = (IBytesBuilder)b;

    public override void Reserve(long rows, ReadOnlySpan<CsvField> sample)
    {
        long bytes = 0;
        foreach (CsvField field in sample)
            if (field.Bits > 0) bytes += field.Length;
        Builder.Reserve(rows, sample.IsEmpty ? 0 : (long)(bytes * (double)rows / sample.Length * 1.0625));
    }

    public override int Decode(ReadOnlySpan<byte> unit, ReadOnlySpan<CsvField> fields, int count, out CsvValueError? error)
    {
        error = null;
        bool nulls = Rules.NullValues.Length > 0;
        for (int r = 0; r < count; r++)
        {
            CsvField field = fields[r];
            if (field.Bits <= 0)
            {
                // A blank line is a null row; an empty or missing field a null unless MissingIsNull is off (then "").
                if (field.Bits != CsvField.Blank && !Rules.MissingIsNull) _b.Append([]);
                else Builder.AppendNull();
                continue;
            }
            ReadOnlySpan<byte> text = unit.Slice(field.Start, field.Length);
            if (field.IsEscaped)
            {
                Span<byte> scratch = Scratch(text.Length);
                text = scratch[..Rules.Tokenizer.Unescape(text, scratch)];
            }
            if (nulls && Rules.IsNullValue(text)) Builder.AppendNull();
            else _b.Append(text);
        }
        return -1;
    }
}

/// <summary>Categoricals: each batch's distinct strings in order of first appearance.</summary>
internal sealed class DictionaryColumn(NativeDictionaryBuilder b, string name, CsvValueRules rules) : CsvColumn(b, name, rules)
{
    private readonly ByteArrayDictionary _values = new();

    public override int Decode(ReadOnlySpan<byte> unit, ReadOnlySpan<CsvField> fields, int count, out CsvValueError? error)
    {
        error = null;
        bool nulls = Rules.NullValues.Length > 0;
        for (int r = 0; r < count; r++)
        {
            CsvField field = fields[r];
            ReadOnlySpan<byte> text;
            if (field.Bits <= 0)
            {
                if (field.Bits == CsvField.Blank || Rules.MissingIsNull)
                {
                    b.AppendNull();
                    continue;
                }
                text = [];
            }
            else
            {
                text = unit.Slice(field.Start, field.Length);
                if (field.IsEscaped)
                {
                    Span<byte> scratch = Scratch(text.Length);
                    text = scratch[..Rules.Tokenizer.Unescape(text, scratch)];
                }
                if (nulls && Rules.IsNullValue(text))
                {
                    b.AppendNull();
                    continue;
                }
            }
            int known = _values.Count;
            int index = _values.GetOrAdd(text);
            if (index == known) b.Entries.Append(text);
            b.Append(index);
        }
        return -1;
    }

    public override void EndBatch() => _values.Clear();

    public override void Dispose()
    {
        _values.Dispose();
        base.Dispose();
    }
}

/// <summary>Enums: the fixed categories (every batch's dictionary); a value not among them is refused.</summary>
internal sealed class EnumColumn : CsvColumn
{
    private readonly NativeDictionaryBuilder _b;
    private readonly ByteArrayDictionary _categories = new();
    private readonly string _listed;

    public EnumColumn(NativeDictionaryBuilder b, string name, CsvValueRules rules, string[] categories) : base(b, name, rules)
    {
        _b = b;
        b.Symbols = categories;
        foreach (string category in categories) _categories.GetOrAdd(Encoding.UTF8.GetBytes(category));
        _listed = string.Join(", ", categories.Take(10).Select(c => $"'{c}'")) + (categories.Length > 10 ? ", …" : "");
    }

    public override int Decode(ReadOnlySpan<byte> unit, ReadOnlySpan<CsvField> fields, int count, out CsvValueError? error)
    {
        error = null;
        for (int r = 0; r < count; r++)
        {
            if (!Text(unit, fields[r], out ReadOnlySpan<byte> text))
            {
                _b.AppendNull();
                continue;
            }
            int index = _categories.IndexOf(text);
            if (index >= 0) _b.Append(index);
            else if (!Refuse(text, $"{{value}} is not one of the Enum's categories ({_listed}): add it to the Enum in the schema, or read the column as a Categorical", out error))
                return r;
        }
        return -1;
    }

    public override void Dispose()
    {
        _categories.Dispose();
        base.Dispose();
    }
}
