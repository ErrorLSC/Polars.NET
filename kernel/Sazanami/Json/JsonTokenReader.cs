using System.Buffers.Text;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Unicode;
using Kaminari.Sazanami.Text;

namespace Kaminari.Sazanami.Json;

/// <summary>
/// The tokens of UTF-8 JSON held whole in one span, with <see cref="Utf8JsonReader"/>'s surface and meaning (the same
/// tokens, positions, values and refusals, under its default options with a maximum depth of 512) for the parts a
/// reader of records uses; made for speed on whole inputs: one pass, no segments, no options.
/// </summary>
/// <remarks>
/// <para>
/// Two framings: <see cref="Lines"/> reads newline-delimited records, one value per line (<see cref="NextRecord"/>
/// moves to the next one, blank lines passed over; a newline inside a value or anything after it on its line is an
/// error); <see cref="Value"/> reads the one value at the span's start and nothing after it (a document's element).
/// </para>
/// <para>
/// The input is checked as JSON's grammar asks: strict numbers and literals, escapes, no control bytes in strings, no
/// comments or trailing commas. UTF-8 is checked inside strings, only those holding non-ASCII bytes; outside strings
/// any non-ASCII byte breaks the grammar anyway. Failures are <see cref="JsonSyntaxException"/>s at a byte offset.
/// A string's escapes are checked as it is read, but an escaped surrogate without its pair is refused only when the
/// string is unescaped (<see cref="CopyString"/>, <see cref="GetString"/>), as Utf8JsonReader does.
/// </para>
/// </remarks>
internal ref struct JsonTokenReader
{
    /// <summary>The deepest nesting read: objects and arrays inside one another, the outermost at depth 1.</summary>
    public const int MaxDepth = 512;

    private readonly ReadOnlySpan<byte> _bytes;
    private readonly bool _lines;
    private int _position;      // the next byte to read
    private int _depth;
    private bool _inObject;     // the innermost open value is an object
    private Expect _expect;
    private int _newlines;      // newlines passed (lines)
    private int _tokenStart, _valueStart, _valueLength;
    private JsonTokenType _type;
    private bool _escaped;
    private Levels _levels;     // a bit per depth: 1 for an object

    private enum Expect : byte { Value, ValueOrEndArray, NameOrEndObject, Name, After }

    [InlineArray(MaxDepth / 64)]
    private struct Levels
    {
        private ulong _word;
    }

    private JsonTokenReader(ReadOnlySpan<byte> bytes, bool lines)
    {
        _bytes = bytes;
        _lines = lines;
        _expect = lines ? Expect.After : Expect.Value;
    }

    /// <summary>A reader of newline-delimited records in <paramref name="bytes"/>, before the first one.</summary>
    public static JsonTokenReader Lines(ReadOnlySpan<byte> bytes) => new(bytes, lines: true);

    /// <summary>A reader of the one value at the start of <paramref name="bytes"/> (white space before it passed over).</summary>
    public static JsonTokenReader Value(ReadOnlySpan<byte> bytes) => new(bytes, lines: false);

    /// <summary>The bytes read.</summary>
    public readonly ReadOnlySpan<byte> Source => _bytes;

    public readonly JsonTokenType TokenType => _type;

    /// <summary>The token's bytes: a string's or name's content without its quotes (escaped as written), a number's or literal's text, a bracket.</summary>
    public readonly ReadOnlySpan<byte> ValueSpan => _bytes.Slice(_valueStart, _valueLength);

    /// <summary>Whether the string or name holds an escape (its <see cref="ValueSpan"/> is then not its value).</summary>
    public readonly bool ValueIsEscaped => _escaped;

    /// <summary>The offset of the token's first byte (a string's opening quote).</summary>
    public readonly int TokenStartIndex => _tokenStart;

    /// <summary>The offset after the token read (after a name's colon).</summary>
    public readonly int BytesConsumed => _position;

    /// <summary>The depth of the token: 0 for a record's own value, its start and its end.</summary>
    public readonly int CurrentDepth => _type is JsonTokenType.StartObject or JsonTokenType.StartArray ? _depth - 1 : _depth;

    /// <summary>
    /// Lines: the line the reader is on, from 1 (a failure inside a record is on the record's line); the lines entered
    /// so far once the records end (a last line without its newline counted).
    /// </summary>
    public readonly int Line => _newlines + (_position < _bytes.Length || (_bytes.Length > 0 && _bytes[^1] != '\n') ? 1 : 0);

    /// <summary>
    /// Lines: moves past the end of the record read (if any) and the blank lines after it, to the next record (its first
    /// <see cref="Read"/> gives its first token); false at the end of the bytes.
    /// </summary>
    public bool NextRecord()
    {
        ReadOnlySpan<byte> b = _bytes;
        int p = _position;
        while (p < b.Length)
        {
            byte c = b[p];
            if (c == '\n') _newlines++;
            else if (c is not ((byte)' ' or (byte)'\t' or (byte)'\r')) break;
            p++;
        }
        _position = p;
        _expect = Expect.Value;
        _type = JsonTokenType.None;
        return p < b.Length;
    }

    /// <summary>The next token; false after the value (lines: the record) is read.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Read()
    {
        ReadOnlySpan<byte> b = _bytes;
        int p = _position;
        if (_expect == Expect.After && _depth == 0)
        {
            if (!_lines) return false;
            if (p < b.Length && b[p] <= ' ') p = White(p);
            if (p < b.Length && b[p] != '\n') throw Fail("A line of newline-delimited JSON holds one object.", p);
            _position = p;
            return false;
        }
        if (p < b.Length && b[p] <= ' ') p = White(p);
        switch (_expect)
        {
            case Expect.After:
            {
                if (p >= b.Length) throw NotClosed(p);
                byte c = b[p];
                if (c == ',')
                {
                    p++;
                    if (p < b.Length && b[p] <= ' ') p = White(p);
                    return _inObject ? Name(p) : Value(p);
                }
                if (c == '}' && _inObject) return End(p, JsonTokenType.EndObject);
                if (c == ']' && !_inObject) return End(p, JsonTokenType.EndArray);
                throw Fail(_inObject ? $"Expected ',' or '}}' after a property value, found {Shown(p)}." : $"Expected ',' or ']' after an array element, found {Shown(p)}.", p);
            }
            case Expect.NameOrEndObject:
                if (p < b.Length && b[p] == '}') return End(p, JsonTokenType.EndObject);
                return Name(p);
            case Expect.ValueOrEndArray:
                if (p < b.Length && b[p] == ']') return End(p, JsonTokenType.EndArray);
                return Value(p);
            default:
                return Value(p);
        }
    }

    /// <summary>
    /// On a property name, passes over its value; on an object's or array's start, passes to its end; on anything else,
    /// does nothing (the reader then stands on the value's last token).
    /// </summary>
    public void Skip()
    {
        if (_type == JsonTokenType.PropertyName) Read();
        if (_type is not (JsonTokenType.StartObject or JsonTokenType.StartArray)) return;
        int depth = _depth;
        while (_depth >= depth) Read();
    }

    /// <summary>The number read as a 64-bit integer (no fraction, no exponent, in range); false otherwise.</summary>
    public readonly bool TryGetInt64(out long value)
    {
        RequireNumber();
        ReadOnlySpan<byte> text = ValueSpan;
        if (JsonNumbers.TryInt64(text, out value)) return true;
        if (Utf8Parser.TryParse(text, out value, out int consumed) && consumed == text.Length) return true;
        value = 0;
        return false;
    }

    /// <summary>The number read as an unsigned 64-bit integer (no sign, fraction or exponent, in range); false otherwise.</summary>
    public readonly bool TryGetUInt64(out ulong value)
    {
        RequireNumber();
        ReadOnlySpan<byte> text = ValueSpan;
        if (Utf8Parser.TryParse(text, out value, out int consumed) && consumed == text.Length) return true;
        value = 0;
        return false;
    }

    /// <summary>The number read as the nearest double (an infinity beyond the doubles' range, as Utf8JsonReader gives it).</summary>
    public readonly bool TryGetDouble(out double value)
    {
        RequireNumber();
        ReadOnlySpan<byte> text = ValueSpan;
        if (JsonNumbers.TryExactDouble(text, out value)) return true;
        if (Utf8Parser.TryParse(text, out value, out int consumed) && consumed == text.Length) return true;
        value = 0;
        return false;
    }

    /// <summary>The unescaped UTF-8 of the string or name read, written to <paramref name="destination"/> (at least <see cref="ValueSpan"/>'s length): its length.</summary>
    public readonly int CopyString(Span<byte> destination)
    {
        if (_type is not (JsonTokenType.String or JsonTokenType.PropertyName))
            throw new InvalidOperationException($"Cannot get the value of a token type '{_type}' as a string.");
        if (!_escaped)
        {
            ValueSpan.CopyTo(destination);
            return _valueLength;
        }
        return JsonStrings.Unescape(ValueSpan, destination, _valueStart);
    }

    /// <summary>The string or name read; null for a null.</summary>
    public readonly string? GetString()
    {
        if (_type == JsonTokenType.Null) return null;
        if (!_escaped && _type is JsonTokenType.String or JsonTokenType.PropertyName) return Encoding.UTF8.GetString(ValueSpan);
        byte[] buffer = new byte[_valueLength];
        return Encoding.UTF8.GetString(buffer, 0, CopyString(buffer));
    }

    private readonly void RequireNumber()
    {
        if (_type != JsonTokenType.Number) throw new InvalidOperationException($"Cannot get the value of a token type '{_type}' as a number.");
    }

    // Past white space from p (b[p] <= ' '): its end. A newline ends a record (lines) and must come after its value.
    private int White(int p)
    {
        ReadOnlySpan<byte> b = _bytes;
        for (; p < b.Length; p++)
        {
            byte c = b[p];
            if (c > ' ') break;
            if (c == '\n')
            {
                if (!_lines) continue;
                if (_depth == 0 && _expect == Expect.After) break;
                throw NotClosed(p);
            }
            if (c is not ((byte)' ' or (byte)'\t' or (byte)'\r')) throw Fail($"Unexpected control character (0x{c:X2}).", p);
        }
        return p;
    }

    private bool End(int p, JsonTokenType type)
    {
        int depth = --_depth;
        _inObject = depth > 0 && (_levels[(depth - 1) >> 6] >> ((depth - 1) & 63) & 1) != 0;
        _type = type;
        _tokenStart = _valueStart = p;
        _valueLength = 1;
        _escaped = false;
        _position = p + 1;
        _expect = Expect.After;
        return true;
    }

    private void Start(int p, bool isObject)
    {
        int depth = _depth;
        if (depth >= MaxDepth) throw Fail($"Nesting deeper than {MaxDepth} levels.", p);
        ref ulong word = ref _levels[depth >> 6];
        ulong bit = 1UL << (depth & 63);
        word = isObject ? word | bit : word & ~bit;
        _depth = depth + 1;
        _inObject = isObject;
        _type = isObject ? JsonTokenType.StartObject : JsonTokenType.StartArray;
        _tokenStart = _valueStart = p;
        _valueLength = 1;
        _escaped = false;
        _position = p + 1;
        _expect = isObject ? Expect.NameOrEndObject : Expect.ValueOrEndArray;
    }

    private bool Value(int p)
    {
        ReadOnlySpan<byte> b = _bytes;
        if (p >= b.Length || (_lines && b[p] == '\n')) throw _depth == 0 && _lines ? Fail("Expected a value, found the line's end.", p) : NotClosed(p);
        switch (b[p])
        {
            case (byte)'"':
                _tokenStart = p;
                String(p);
                _type = JsonTokenType.String;
                _expect = Expect.After;
                return true;
            case (byte)'{':
                Start(p, isObject: true);
                return true;
            case (byte)'[':
                Start(p, isObject: false);
                return true;
            case (byte)'-' or (>= (byte)'0' and <= (byte)'9'):
                Scalar(p, Number(p), JsonTokenType.Number);
                return true;
            case (byte)'t':
                Scalar(p, Literal(p, "true"u8), JsonTokenType.True);
                return true;
            case (byte)'f':
                Scalar(p, Literal(p, "false"u8), JsonTokenType.False);
                return true;
            case (byte)'n':
                Scalar(p, Literal(p, "null"u8), JsonTokenType.Null);
                return true;
            default:
                throw Fail($"Expected a value, found {Shown(p)}.", p);
        }
    }

    private void Scalar(int p, int end, JsonTokenType type)
    {
        _type = type;
        _tokenStart = _valueStart = p;
        _valueLength = end - p;
        _escaped = false;
        _position = end;
        _expect = Expect.After;
    }

    private bool Name(int p)
    {
        ReadOnlySpan<byte> b = _bytes;
        if (p >= b.Length || b[p] != '"')
        {
            if (p >= b.Length || (_lines && b[p] == '\n')) throw NotClosed(p);
            throw Fail($"Expected a property name (a string), found {Shown(p)}.", p);
        }
        _tokenStart = p;
        String(p);
        _type = JsonTokenType.PropertyName;
        p = _position;
        if (p < b.Length && b[p] <= ' ') p = White(p);
        if (p >= b.Length || b[p] != ':')
        {
            if (p >= b.Length || (_lines && b[p] == '\n')) throw NotClosed(p);
            throw Fail($"Expected ':' after a property name, found {Shown(p)}.", p);
        }
        _position = p + 1;
        _expect = Expect.Value;
        return true;
    }

    // The string whose opening quote is at p: its content, the position after its closing quote.
    private void String(int p)
    {
        ReadOnlySpan<byte> b = _bytes;
        int i = JsonStrings.Stop(b, p + 1);
        bool escaped = false, wide = false;
        while (true)
        {
            if (i >= b.Length) throw Fail("Unterminated string.", p);
            byte c = b[i];
            if (c == '"') break;
            if (c == '\\')
            {
                escaped = true;
                i = Escape(i);
            }
            else if (c >= 0x80)
            {
                wide = true;
                i++;
            }
            else if (c == '\n' && _lines) throw Fail("Unterminated string.", p);
            else throw Fail($"A control character (0x{c:X2}) in a string must be escaped.", i);
            // Once a string holds non-ASCII, the rest is searched for quotes, backslashes and controls only, and checked whole at its end.
            i = wide ? JsonStrings.StopNarrow(b, i) : JsonStrings.Stop(b, i);
        }
        if (wide && !Utf8.IsValid(b[(p + 1)..i])) throw Fail("The input is not UTF-8.", p);
        _valueStart = p + 1;
        _valueLength = i - p - 1;
        _escaped = escaped;
        _position = i + 1;
    }

    // The escape whose backslash is at i: the position after it.
    private readonly int Escape(int i)
    {
        ReadOnlySpan<byte> b = _bytes;
        if (i + 1 < b.Length)
        {
            switch (b[i + 1])
            {
                case (byte)'"' or (byte)'\\' or (byte)'/' or (byte)'b' or (byte)'f' or (byte)'n' or (byte)'r' or (byte)'t':
                    return i + 2;
                case (byte)'u':
                    if (i + 6 <= b.Length && IsHex(b[i + 2]) && IsHex(b[i + 3]) && IsHex(b[i + 4]) && IsHex(b[i + 5])) return i + 6;
                    break;
            }
        }
        // The escape as written: its backslash and the byte after it, or "\u" and the hex digits that follow (never past a
        // control byte or the record, so that the text does not depend on where the input was cut).
        int end = i + 1;
        if (end < b.Length && b[end] >= ' ' && b[end] != '"')
        {
            end++;
            if (b[i + 1] == 'u')
                while (end < b.Length && end < i + 6 && IsHex(b[end])) end++;
        }
        throw Fail($"Invalid escape '{Encoding.UTF8.GetString(b[i..end])}' in a string.", i);

        static bool IsHex(byte c) => (uint)(c - '0') <= 9 || (uint)((c | 0x20) - 'a') <= 5;
    }

    // A number in JSON's grammar at p (b[p] is '-' or a digit), followed by a delimiter: its end.
    private readonly int Number(int p)
    {
        ReadOnlySpan<byte> b = _bytes;
        int i = p;
        if (b[i] == '-') i++;
        if (i >= b.Length) throw Fail($"Invalid number {Shown(p)}.", p);
        uint d = (uint)(b[i] - '0');
        if (d == 0) i++;
        else if (d <= 9)
            for (i++; i < b.Length && (uint)(b[i] - '0') <= 9; i++) { }
        else throw Fail($"Invalid number {Shown(p)}.", p);
        if (i < b.Length && b[i] == '.')
        {
            if (++i >= b.Length || (uint)(b[i] - '0') > 9) throw Fail($"Invalid number {Shown(p)}.", p);
            for (i++; i < b.Length && (uint)(b[i] - '0') <= 9; i++) { }
        }
        if (i < b.Length && (b[i] | 0x20) == 'e')
        {
            if (++i < b.Length && b[i] is (byte)'+' or (byte)'-') i++;
            if (i >= b.Length || (uint)(b[i] - '0') > 9) throw Fail($"Invalid number {Shown(p)}.", p);
            for (i++; i < b.Length && (uint)(b[i] - '0') <= 9; i++) { }
        }
        if (i < b.Length && !IsDelimiter(b[i])) throw Fail($"Invalid number {Shown(p)}.", p);
        return i;
    }

    // The literal at p: its end. Where more is read after it (inside a value, or on a record's line) it must be followed
    // by a delimiter (else the next token fails anyway: this failure names the literal); a document's value is not.
    private readonly int Literal(int p, ReadOnlySpan<byte> literal)
    {
        ReadOnlySpan<byte> b = _bytes;
        int end = p + literal.Length;
        if (!b[p..].StartsWith(literal) || (end < b.Length && (_depth > 0 || _lines) && !IsDelimiter(b[end])))
            throw Fail($"{Shown(p)} is not a JSON literal (true, false or null).", p);
        return end;
    }

    // What may follow a number or literal (Utf8JsonReader's delimiters: white space, a separator, an end, a comment's
    // start, refused next).
    private static bool IsDelimiter(byte c) => c is (byte)' ' or (byte)'\t' or (byte)'\r' or (byte)'\n' or (byte)',' or (byte)']' or (byte)'}' or (byte)'/';

    // The text at p for an error: up to a delimiter, at most 40 bytes, quoted; or the end it stands at. It never runs past
    // the record or element it is in (control bytes, separators and brackets end it), so that it does not depend on
    // where the input was cut into units.
    private readonly string Shown(int p)
    {
        ReadOnlySpan<byte> b = _bytes;
        if (p >= b.Length) return _lines ? "the line's end" : "the end of the input";
        if (_lines && b[p] == '\n') return "the line's end";
        // A string up to its closing quote; anything else up to a delimiter or the start of a string or a nested value.
        bool quoted = b[p] == '"';
        int end = p + 1;
        while (end < b.Length && end - p < 40 && b[end] >= ' ' && !(quoted
            ? (b[end - 1] == '"' && end - 1 > p) || b[end] is (byte)'{' or (byte)'}' or (byte)'[' or (byte)']' or (byte)',' or (byte)':'
            : IsDelimiter(b[end]) || b[end] is (byte)'"' or (byte)'{' or (byte)'[')) end++;
        string text = Encoding.UTF8.GetString(b[p..end]);
        return end - p == 40 && end < b.Length && !IsDelimiter(b[end]) ? $"'{text}…'" : $"'{text}'";
    }

    private readonly JsonSyntaxException NotClosed(int p) =>
        Fail(_lines ? "The value is not closed at its line's end." : "The input ends inside a value.", p);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static JsonSyntaxException Fail(string message, int position) => new(message, position);
}

/// <summary>A failure to read JSON at a byte offset of the reader's input.</summary>
internal sealed class JsonSyntaxException(string message, int position) : Exception(message)
{
    /// <summary>The offset of the failing byte in the reader's input.</summary>
    public int Position { get; } = position;
}
