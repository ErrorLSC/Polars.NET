using System.Buffers;
using System.Buffers.Text;
using System.Globalization;
using System.Text;

namespace Kaminari.Seseragi.Files.Excel.Xml;

/// <summary>Attribute lookup and text decoding for <see cref="XmlSpanScanner"/>.</summary>
internal static class XmlText
{
    /// <summary>XML whitespace.</summary>
    public static readonly SearchValues<byte> Whitespace = SearchValues.Create(" \t\r\n"u8);

    /// <summary>Finds an attribute by local name in raw attribute text; the value is raw (entities not decoded).</summary>
    public static bool TryGetAttribute(ReadOnlySpan<byte> attributes, ReadOnlySpan<byte> localName, out ReadOnlySpan<byte> value)
    {
        foreach (var a in new XmlAttributeEnumerator(attributes))
        {
            if (a.LocalName.SequenceEqual(localName))
            {
                value = a.Value;
                return true;
            }
        }
        value = default;
        return false;
    }

    /// <summary>
    /// Decodes XML entities and character references from <paramref name="raw"/> into <paramref name="destination"/>
    /// (which must be at least as long: decoding never grows text). Returns the decoded length.
    /// </summary>
    public static int DecodeEntities(ReadOnlySpan<byte> raw, Span<byte> destination)
    {
        int amp = raw.IndexOf((byte)'&');
        if (amp < 0)
        {
            raw.CopyTo(destination);
            return raw.Length;
        }
        int written = 0;
        while (true)
        {
            raw[..amp].CopyTo(destination[written..]);
            written += amp;
            raw = raw[amp..];
            int semi = raw.IndexOf((byte)';');
            if (semi < 0) throw new ExcelFormatException("Malformed XML: an unterminated entity reference.");
            var entity = raw[1..semi];
            if (entity.SequenceEqual("lt"u8)) destination[written++] = (byte)'<';
            else if (entity.SequenceEqual("gt"u8)) destination[written++] = (byte)'>';
            else if (entity.SequenceEqual("amp"u8)) destination[written++] = (byte)'&';
            else if (entity.SequenceEqual("quot"u8)) destination[written++] = (byte)'"';
            else if (entity.SequenceEqual("apos"u8)) destination[written++] = (byte)'\'';
            else if (entity.Length > 1 && entity[0] == (byte)'#')
            {
                bool hex = entity[1] is (byte)'x' or (byte)'X';
                var digits = hex ? entity[2..] : entity[1..];
                if (!Utf8Parser.TryParse(digits, out uint cp, out int used, hex ? 'X' : default) || used != digits.Length
                    || !Rune.TryCreate(cp, out var rune))
                    throw new ExcelFormatException("Malformed XML: an invalid character reference.");
                written += rune.EncodeToUtf8(destination[written..]);
            }
            else throw new ExcelFormatException($"Malformed XML: unknown entity '&{Encoding.UTF8.GetString(entity)};'.");
            raw = raw[(semi + 1)..];
            amp = raw.IndexOf((byte)'&');
            if (amp < 0)
            {
                raw.CopyTo(destination[written..]);
                return written + raw.Length;
            }
        }
    }

    /// <summary>
    /// Decodes OOXML's <c>_xHHHH_</c> escapes (UTF-16 code units, for characters XML cannot carry) in place.
    /// Returns the new length. Unpaired surrogates become U+FFFD.
    /// </summary>
    public static int DecodeEscapes(Span<byte> text)
    {
        int at = text.IndexOf("_x"u8);
        if (at < 0) return text.Length;
        int read = at, written = at;
        while (read < text.Length)
        {
            if (TryEscape(text[read..], out char unit))
            {
                read += 7;
                int scalar = unit;
                if (char.IsHighSurrogate(unit) && TryEscape(text[read..], out char low) && char.IsLowSurrogate(low))
                {
                    read += 7;
                    scalar = char.ConvertToUtf32(unit, low);
                }
                else if (char.IsSurrogate(unit))
                {
                    scalar = 0xFFFD;
                }
                written += new Rune(scalar).EncodeToUtf8(text[written..]);
                continue;
            }
            text[written++] = text[read++];
        }
        return written;

        static bool TryEscape(ReadOnlySpan<byte> s, out char unit)
        {
            unit = '\0';
            if (s.Length < 7 || s[0] != (byte)'_' || s[1] != (byte)'x' || s[6] != (byte)'_') return false;
            if (!Utf8Parser.TryParse(s.Slice(2, 4), out ushort v, out int used, 'X') || used != 4) return false;
            unit = (char)v;
            return true;
        }
    }

    /// <summary>Decodes text (entities, then <c>_xHHHH_</c> escapes) into a new string. For small values (names).</summary>
    public static string DecodeString(ReadOnlySpan<byte> raw, bool escapes = true)
    {
        if (raw.IsEmpty) return "";
        byte[]? rented = raw.Length > 512 ? ArrayPool<byte>.Shared.Rent(raw.Length) : null;
        Span<byte> buffer = rented ?? stackalloc byte[512];
        try
        {
            int n = DecodeEntities(raw, buffer);
            if (escapes) n = DecodeEscapes(buffer[..n]);
            return Encoding.UTF8.GetString(buffer[..n]);
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Text without leading and trailing ASCII whitespace.</summary>
    public static ReadOnlySpan<byte> Trim(ReadOnlySpan<byte> text) => text[Ascii.Trim(text)];

    /// <summary>An xsd:boolean (<c>1</c>, <c>true</c>, <c>0</c>, <c>false</c>, <c>on</c> in some writers).</summary>
    public static bool ParseBoolean(ReadOnlySpan<byte> raw)
    {
        var v = Trim(raw);
        return v.SequenceEqual("1"u8) || v.SequenceEqual("true"u8) || v.SequenceEqual("on"u8);
    }

    /// <summary>A non-negative integer attribute; false when absent or malformed.</summary>
    public static bool TryParseInt(ReadOnlySpan<byte> raw, out int value) =>
        Utf8Parser.TryParse(Trim(raw), out value, out int used) && used == Trim(raw).Length;

    /// <summary>
    /// A cell's number: plain integers of up to 15 digits (most cell values) converted directly, which is exact;
    /// anything else as an xsd:double.
    /// </summary>
    public static bool TryParseNumber(ReadOnlySpan<byte> raw, out double value)
    {
        int i = raw.Length > 0 && raw[0] == (byte)'-' ? 1 : 0;
        int digits = raw.Length - i;
        if (digits is > 0 and <= 15)
        {
            long n = 0;
            for (; i < raw.Length; i++)
            {
                uint d = (uint)(raw[i] - '0');
                if (d > 9) return TryParseDouble(raw, out value);
                n = n * 10 + d;
            }
            value = raw[0] == (byte)'-' ? -n : n;
            return true;
        }
        return TryParseDouble(raw, out value);
    }

    /// <summary>An xsd:double (also <c>INF</c>, <c>-INF</c>, <c>NaN</c>).</summary>
    public static bool TryParseDouble(ReadOnlySpan<byte> raw, out double value)
    {
        var v = Trim(raw);
        if (Utf8Parser.TryParse(v, out value, out int used) && used == v.Length) return true;
        return double.TryParse(v, NumberStyles.Float, CultureInfo.InvariantCulture, out value);
    }
}
