using System.Buffers;
using System.Text;
using System.Text.Unicode;
using Kaminari.Sazanami.Text;

namespace Kaminari.Seseragi.Files.Csv.Reading;

/// <summary>
/// How a unit's bytes become text: as they are (UTF-8, read strictly or already transcoded), or, for a field holding a
/// byte from 0x80 up, decoded first (a legacy code page read on its bytes, or UTF-8 read lossily: <see cref="FieldDecoder"/>);
/// and how a strict read checks a unit (its first invalid unit fails its record, <see cref="Invalid"/> the message).
/// </summary>
internal sealed class CsvCharset
{
    private CsvCharset(TextDecoder? fieldDecoder, bool strict, bool utf8Bytes, string invalid)
    {
        FieldDecoder = fieldDecoder;
        Strict = strict;
        Utf8Bytes = utf8Bytes;
        Invalid = invalid;
    }

    /// <summary>The decoder of fields holding non-ASCII bytes, or null when the bytes are UTF-8 to be used as they are.</summary>
    public TextDecoder? FieldDecoder { get; }

    /// <summary>Whether invalid text fails its record (checked per unit, whatever columns are read).</summary>
    public bool Strict { get; }

    /// <summary>Whether the raw bytes are UTF-8 (a valid unit is used as it is).</summary>
    public bool Utf8Bytes { get; }

    /// <summary>The failure of invalid text.</summary>
    public string Invalid { get; }

    /// <summary>The failure of text that is not UTF-8 (strict UTF-8, the default).</summary>
    public const string NotUtf8 = "the text is not UTF-8 (give the input's encoding)";

    /// <summary>UTF-8 bytes read strictly (or text transcoded to UTF-8 from <paramref name="encoding"/>, invalid units marked by 0xFF).</summary>
    public static CsvCharset Utf8Text(Encoding? encoding) => new(null, strict: true, utf8Bytes: true, encoding is null || encoding.CodePage == 65001 ? NotUtf8 : NotValid(encoding));

    /// <summary>The raw bytes of <paramref name="encoding"/> decoded per field by <paramref name="decoder"/>.</summary>
    public static CsvCharset Fields(Encoding encoding, TextDecoder decoder) =>
        new(decoder, strict: !decoder.Replace, utf8Bytes: encoding.CodePage == 65001, encoding.CodePage == 65001 ? NotUtf8 : NotValid(encoding));

    private static string NotValid(Encoding encoding) =>
        $"the text is not valid {encoding.WebName} (check the encoding, or read with InvalidText = Replace)";

    /// <summary>Whether a unit's fields are used as they are (no field needs decoding).</summary>
    public bool Clean(ReadOnlySpan<byte> unit) => FieldDecoder is null || Ascii.IsValid(unit) || (Utf8Bytes && Utf8.IsValid(unit));

    /// <summary>The first invalid byte of a unit (strict reads), or -1.</summary>
    public int FirstInvalid(ReadOnlySpan<byte> unit) => !Strict ? -1 : FieldDecoder is null ? CsvDecode.FirstInvalidUtf8(unit) : FieldDecoder.FirstInvalid(unit);

    /// <summary>The bytes a field's text may take once decoded.</summary>
    public static int Room(int bytes) => bytes * 3 + 4;

    /// <summary>A field's text decoded into <paramref name="destination"/> (at least <see cref="Room"/> bytes).</summary>
    public int Decode(ReadOnlySpan<byte> text, Span<byte> destination)
    {
        OperationStatus status = FieldDecoder!.Decode(text, destination, final: true, out int consumed, out int written);
        // Strict units were checked whole before their fields are decoded: a field is valid text.
        if (status != OperationStatus.Done || consumed != text.Length)
            throw new InvalidOperationException($"A field checked valid did not decode ({status}).");
        return written;
    }
}
