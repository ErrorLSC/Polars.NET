namespace Kaminari.Seseragi.Files.Excel.Xml;

/// <summary>Token kinds of <see cref="XmlSpanScanner"/>.</summary>
internal enum XmlToken : byte
{
    None,
    StartElement,
    EndElement,
    Text,
    EndOfInput,
}

/// <summary>
/// A forward-only, allocation-free XML tokenizer over complete UTF-8 text in memory. Element and attribute names
/// come without their namespace prefix (OOXML parts may be written with any prefix, and strict OOXML uses other
/// namespace URIs, so matching by local name covers both). Comments, processing instructions and a DOCTYPE are
/// skipped; entities are never expanded by the scanner (see <see cref="XmlText"/>); CDATA sections are text tokens
/// with <see cref="IsCData"/> set.
/// </summary>
internal ref struct XmlSpanScanner(ReadOnlySpan<byte> input)
{
    private readonly ReadOnlySpan<byte> _input = input;
    private int _pos;
    private int _nameStart, _nameLength, _attrStart, _attrLength, _textStart, _textLength;

    /// <summary>The current token.</summary>
    public XmlToken Token { get; private set; }

    /// <summary>For a start element: it closes itself (<c>&lt;x/&gt;</c>).</summary>
    public bool IsEmptyElement { get; private set; }

    /// <summary>For a text token: it came from a CDATA section (no entities to decode).</summary>
    public bool IsCData { get; private set; }

    /// <summary>The position after the current token.</summary>
    public readonly int Position => _pos;

    /// <summary>The local name of the current element (start or end).</summary>
    public readonly ReadOnlySpan<byte> LocalName => _input.Slice(_nameStart, _nameLength);

    /// <summary>The raw attribute text of the current start element.</summary>
    public readonly ReadOnlySpan<byte> Attributes => _input.Slice(_attrStart, _attrLength);

    /// <summary>The raw text (entities not decoded) of the current text token.</summary>
    public readonly ReadOnlySpan<byte> Text => _input.Slice(_textStart, _textLength);

    /// <summary>Whether the current element's local name is <paramref name="name"/>.</summary>
    public readonly bool Is(ReadOnlySpan<byte> name) => LocalName.SequenceEqual(name);

    /// <summary>Advances to the next token; false at the end of the input.</summary>
    public bool Read()
    {
        var s = _input;
        while (true)
        {
            if (_pos >= s.Length)
            {
                Token = XmlToken.EndOfInput;
                return false;
            }
            if (s[_pos] != (byte)'<')
            {
                int end = IndexOfOrEnd(s, _pos, (byte)'<');
                _textStart = _pos;
                _textLength = end - _pos;
                IsCData = false;
                _pos = end;
                Token = XmlToken.Text;
                return true;
            }
            if (_pos + 1 >= s.Length) throw Malformed("an unterminated tag");
            byte next = s[_pos + 1];
            switch (next)
            {
                case (byte)'/':
                {
                    int gt = IndexOfOrEnd(s, _pos + 2, (byte)'>');
                    if (gt >= s.Length) throw Malformed("an unterminated end tag");
                    int nameEnd = gt;
                    while (nameEnd > _pos + 2 && IsSpace(s[nameEnd - 1])) nameEnd--;
                    SetName(_pos + 2, nameEnd - _pos - 2);
                    _pos = gt + 1;
                    Token = XmlToken.EndElement;
                    return true;
                }
                case (byte)'?':
                {
                    int end = s[_pos..].IndexOf("?>"u8);
                    if (end < 0) throw Malformed("an unterminated processing instruction");
                    _pos += end + 2;
                    continue;
                }
                case (byte)'!':
                {
                    var rest = s[_pos..];
                    if (rest.StartsWith("<!--"u8))
                    {
                        int end = rest.IndexOf("-->"u8);
                        if (end < 0) throw Malformed("an unterminated comment");
                        _pos += end + 3;
                        continue;
                    }
                    if (rest.StartsWith("<![CDATA["u8))
                    {
                        int end = rest.IndexOf("]]>"u8);
                        if (end < 0) throw Malformed("an unterminated CDATA section");
                        _textStart = _pos + 9;
                        _textLength = end - 9;
                        IsCData = true;
                        _pos += end + 3;
                        Token = XmlToken.Text;
                        return true;
                    }
                    // <!DOCTYPE …> with an optional internal subset in brackets.
                    int bracket = rest.IndexOf((byte)'[');
                    int gt = rest.IndexOf((byte)'>');
                    if (gt < 0) throw Malformed("an unterminated declaration");
                    if (bracket >= 0 && bracket < gt)
                    {
                        int close = rest.IndexOf("]>"u8);
                        if (close < 0) throw Malformed("an unterminated declaration");
                        gt = close + 1;
                    }
                    _pos += gt + 1;
                    continue;
                }
                default:
                {
                    // One scalar pass over the tag (tags are short: vector searches cost more than they save here).
                    int start = _pos + 1;
                    int i = start, colon = -1;
                    for (; i < s.Length; i++)
                    {
                        byte b = s[i];
                        if (b == (byte)':') colon = i;
                        else if (IsSpace(b) || b == (byte)'/' || b == (byte)'>') break;
                    }
                    int nameEnd = i;
                    for (; i < s.Length; i++)
                    {
                        byte b = s[i];
                        if (b == (byte)'>') break;
                        if (b is (byte)'"' or (byte)'\'')
                        {
                            i = IndexOfOrEnd(s, i + 1, b);
                            if (i >= s.Length) break;
                        }
                    }
                    if (i >= s.Length) throw Malformed("an unterminated start tag");
                    int gt = i;
                    bool empty = s[gt - 1] == (byte)'/';
                    int contentEnd = empty ? gt - 1 : gt;
                    _nameStart = colon < 0 ? start : colon + 1;
                    _nameLength = nameEnd - _nameStart;
                    _attrStart = nameEnd;
                    _attrLength = Math.Max(0, contentEnd - nameEnd);
                    IsEmptyElement = empty;
                    _pos = gt + 1;
                    Token = XmlToken.StartElement;
                    return true;
                }
            }
        }
    }

    /// <summary>Skips the rest of the current start element (its content and end tag).</summary>
    public void SkipElement()
    {
        if (Token != XmlToken.StartElement || IsEmptyElement) return;
        int depth = 1;
        while (Read())
        {
            if (Token == XmlToken.StartElement && !IsEmptyElement) depth++;
            else if (Token == XmlToken.EndElement && --depth == 0) return;
        }
        throw Malformed("an element without its end tag");
    }

    /// <summary>The value of an attribute of the current start element, by local name (raw, entities not decoded).</summary>
    public readonly bool TryGetAttribute(ReadOnlySpan<byte> localName, out ReadOnlySpan<byte> value) =>
        XmlText.TryGetAttribute(Attributes, localName, out value);

    private void SetName(int start, int length)
    {
        var name = _input.Slice(start, length);
        int colon = name.IndexOf((byte)':');
        _nameStart = start + colon + 1;
        _nameLength = length - colon - 1;
    }

    /// <summary>The attributes of the current start element, in order (names without prefix, raw values).</summary>
    public readonly XmlAttributeEnumerator EnumerateAttributes() => new(Attributes);

    private static bool IsSpace(byte b) => b is (byte)' ' or (byte)'\t' or (byte)'\r' or (byte)'\n';

    // The index of value at or after from, or s.Length: a short scalar look first (most runs between markup are a
    // few bytes), then a vectorized search for long text.
    private static int IndexOfOrEnd(ReadOnlySpan<byte> s, int from, byte value)
    {
        int limit = Math.Min(s.Length, from + 32);
        for (int i = from; i < limit; i++)
            if (s[i] == value) return i;
        if (limit == s.Length) return s.Length;
        int k = s[limit..].IndexOf(value);
        return k < 0 ? s.Length : limit + k;
    }

    private static ExcelFormatException Malformed(string what) => new($"Malformed XML: {what}.");
}

/// <summary>
/// Walks raw attribute text once (no allocation): <see cref="LocalName"/> without its prefix (xmlns declarations
/// skipped) and the raw <see cref="Value"/> (entities not decoded). Malformed text ends the walk.
/// </summary>
internal ref struct XmlAttributeEnumerator(ReadOnlySpan<byte> attributes)
{
    private readonly ReadOnlySpan<byte> _s = attributes;
    private int _pos;

    public ReadOnlySpan<byte> LocalName { get; private set; }

    public ReadOnlySpan<byte> Value { get; private set; }

    public readonly XmlAttributeEnumerator GetEnumerator() => this;

    public readonly XmlAttributeEnumerator Current => this;

    public bool MoveNext()
    {
        var s = _s;
        int i = _pos;
        while (true)
        {
            while (i < s.Length && s[i] is (byte)' ' or (byte)'\t' or (byte)'\r' or (byte)'\n') i++;
            if (i >= s.Length) return Stop();
            int nameStart = i, colon = -1;
            while (i < s.Length && s[i] is not ((byte)'=' or (byte)' ' or (byte)'\t' or (byte)'\r' or (byte)'\n'))
            {
                if (s[i] == (byte)':') colon = i;
                i++;
            }
            int nameEnd = i;
            while (i < s.Length && s[i] != (byte)'=') i++;
            i++;
            while (i < s.Length && s[i] is (byte)' ' or (byte)'\t' or (byte)'\r' or (byte)'\n') i++;
            if (i >= s.Length || s[i] is not ((byte)'"' or (byte)'\'')) return Stop();
            byte quote = s[i];
            int valueStart = ++i;
            while (i < s.Length && s[i] != quote) i++;
            if (i >= s.Length) return Stop();
            _pos = i + 1;
            if (colon >= 0 && s[nameStart..colon].SequenceEqual("xmlns"u8))
            {
                i = _pos;
                continue; // a namespace declaration, not an attribute
            }
            LocalName = s[(colon < 0 ? nameStart : colon + 1)..nameEnd];
            Value = s[valueStart..i];
            return true;
        }
    }

    private bool Stop()
    {
        _pos = _s.Length;
        return false;
    }
}
