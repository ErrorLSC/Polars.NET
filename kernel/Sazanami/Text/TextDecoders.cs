using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Unicode;

namespace Kaminari.Sazanami.Text;

/// <summary>What a reader may do with an encoding's bytes before decoding them.</summary>
internal enum TextLayout : byte
{
    /// <summary>
    /// Bytes 0x00–0x7F are ASCII wherever they appear and decoding is stateless (UTF-8, the table code pages): the
    /// source may be cut at '\n' and tokenized on structural bytes below 0x40 (no multi-byte unit contains one).
    /// </summary>
    AsciiCompatible,
    /// <summary>UTF-16: cut and tokenize only after decoding (or on aligned code units).</summary>
    Utf16,
    /// <summary>Anything else (EBCDIC, stateful or 4-byte forms): decoded in order, from the start.</summary>
    InOrder,
}

/// <summary>
/// Decodes a byte stream into UTF-8. Strict decoders stop at an invalid unit (<see cref="OperationStatus.InvalidData"/>,
/// <c>consumed</c> = its offset); lossy ones write U+FFFD for it and go on. A unit cut by the end of the source is
/// <see cref="OperationStatus.NeedMoreData"/> (stopping before it) unless <c>final</c>, then it is invalid. Three
/// destination bytes per source byte always suffice.
/// </summary>
internal abstract class TextDecoder(bool replace)
{
    /// <summary>Invalid units become U+FFFD instead of stopping the decoding.</summary>
    public bool Replace { get; } = replace;

    public abstract TextLayout Layout { get; }

    public abstract OperationStatus Decode(ReadOnlySpan<byte> source, Span<byte> destination, bool final, out int consumed, out int written);

    private protected static ReadOnlySpan<byte> ReplacementUtf8 => "�"u8;
}

/// <summary>UTF-8 checked (strict) or repaired (lossy: U+FFFD per maximal invalid subpart, as .NET and Rust replace).</summary>
internal sealed class Utf8TextDecoder(bool replace) : TextDecoder(replace)
{
    public override TextLayout Layout => TextLayout.AsciiCompatible;

    public override OperationStatus Decode(ReadOnlySpan<byte> source, Span<byte> destination, bool final, out int consumed, out int written)
    {
        int at = 0, w = 0;
        bool whole = Utf8.IsValid(source); // the common case, vectorized; else valid runs are found one by one
        while (at < source.Length)
        {
            // Valid runs are copied as they are; only an invalid or cut sequence stops the run.
            int valid = whole ? source.Length : ValidPrefix(source[at..]);
            if (destination.Length - w < valid)
            {
                consumed = at;
                written = w;
                return OperationStatus.DestinationTooSmall;
            }
            source.Slice(at, valid).CopyTo(destination[w..]);
            at += valid;
            w += valid;
            if (at == source.Length) break;
            OperationStatus status = Rune.DecodeFromUtf8(source[at..], out _, out int length);
            if (status == OperationStatus.NeedMoreData && !final)
            {
                consumed = at;
                written = w;
                return OperationStatus.NeedMoreData;
            }
            if (!Replace)
            {
                consumed = at;
                written = w;
                return OperationStatus.InvalidData;
            }
            if (destination.Length - w < 3)
            {
                consumed = at;
                written = w;
                return OperationStatus.DestinationTooSmall;
            }
            ReplacementUtf8.CopyTo(destination[w..]);
            w += 3;
            at += length;
        }
        consumed = at;
        written = w;
        return OperationStatus.Done;

        static int ValidPrefix(ReadOnlySpan<byte> text)
        {
            int p = 0;
            while (p < text.Length)
            {
                int ascii = text[p..].IndexOfAnyExceptInRange((byte)0, (byte)0x7F);
                if (ascii < 0) return text.Length;
                p += ascii;
                if (Rune.DecodeFromUtf8(text[p..], out _, out int length) != OperationStatus.Done) return p;
                p += length;
            }
            return p;
        }
    }
}

/// <summary>UTF-16 (little or big endian) to UTF-8; lone surrogates and an odd last byte are invalid.</summary>
internal sealed class Utf16TextDecoder(bool bigEndian, bool replace) : TextDecoder(replace)
{
    public bool BigEndian { get; } = bigEndian;

    public override TextLayout Layout => TextLayout.Utf16;

    public override OperationStatus Decode(ReadOnlySpan<byte> source, Span<byte> destination, bool final, out int consumed, out int written)
    {
        consumed = 0;
        written = 0;
        Span<char> chunk = stackalloc char[1024];
        while (true)
        {
            int units = Math.Min(chunk.Length, (source.Length - consumed) / 2);
            ReadOnlySpan<byte> bytes = source.Slice(consumed, units * 2);
            Span<char> chars = chunk[..units];
            if (BigEndian) BinaryPrimitives.ReverseEndianness(MemoryMarshal.Cast<byte, ushort>(bytes), MemoryMarshal.Cast<char, ushort>(chars));
            else MemoryMarshal.Cast<byte, char>(bytes).CopyTo(chars);
            bool last = consumed + units * 2 >= source.Length - 1; // the whole source but an odd byte
            OperationStatus status = Utf8.FromUtf16(chars, destination[written..], out int read, out int wrote,
                replaceInvalidSequences: Replace, isFinalBlock: last && final);
            consumed += read * 2;
            written += wrote;
            // A chunk ending in a high surrogate leaves it to the next chunk.
            if (!last && status is OperationStatus.Done or OperationStatus.NeedMoreData) continue;
            if (status != OperationStatus.Done) return status;
            if (consumed == source.Length) return OperationStatus.Done;
            // One byte left: half a code unit.
            if (!final) return OperationStatus.NeedMoreData;
            if (!Replace) return OperationStatus.InvalidData;
            if (destination.Length - written < 3) return OperationStatus.DestinationTooSmall;
            ReplacementUtf8.CopyTo(destination[written..]);
            written += 3;
            consumed++;
            return OperationStatus.Done;
        }
    }
}

/// <summary>
/// Any other .NET encoding, decoded in order by its <see cref="Decoder"/>: stateful (a cut sequence waits inside it for
/// the next call, and characters the destination had no room for are written by the next call), so <c>consumed</c> is
/// the whole source unless the destination fills up. Strict = the exception fallback, lossy = U+FFFD; the encoding's
/// own fallback is never used.
/// </summary>
internal sealed class SequentialTextDecoder : TextDecoder
{
    private readonly Decoder _decoder;
    private readonly char[] _chars = new char[4096];
    private int _start, _end; // decoded characters not written yet
    private bool _flushed;

    public SequentialTextDecoder(Encoding encoding, bool replace) : base(replace)
    {
        var e = (Encoding)encoding.Clone();
        e.DecoderFallback = replace ? new DecoderReplacementFallback("\uFFFD") : DecoderFallback.ExceptionFallback;
        _decoder = e.GetDecoder();
        Encoding = encoding;
    }

    public Encoding Encoding { get; }

    public override TextLayout Layout => TextLayout.InOrder;

    public override OperationStatus Decode(ReadOnlySpan<byte> source, Span<byte> destination, bool final, out int consumed, out int written)
    {
        consumed = 0;
        written = 0;
        while (true)
        {
            bool input = consumed < source.Length;
            if (_end > _start)
            {
                OperationStatus status = Utf8.FromUtf16(_chars.AsSpan(_start, _end - _start), destination[written..], out int read, out int wrote,
                    replaceInvalidSequences: true, isFinalBlock: !input && final && _flushed);
                written += wrote;
                _start += read;
                if (status == OperationStatus.DestinationTooSmall) return status;
            }
            // A high surrogate whose low half is still to come stays, at the front.
            _chars.AsSpan(_start, _end - _start).CopyTo(_chars);
            (_start, _end) = (0, _end - _start);
            if (!input && (!final || _flushed))
            {
                _flushed = false;
                return OperationStatus.Done;
            }
            int take = Math.Min(source.Length - consumed, 2048);
            bool flush = final && consumed + take == source.Length;
            int used, count;
            try
            {
                _decoder.Convert(source.Slice(consumed, take), _chars.AsSpan(_end), flush, out used, out count, out _);
            }
            catch (DecoderFallbackException e)
            {
                consumed += Math.Max(0, e.Index);
                _decoder.Reset();
                (_start, _end, _flushed) = (0, 0, false);
                return OperationStatus.InvalidData;
            }
            _end += count;
            consumed += used;
            _flushed = flush && consumed == source.Length;
        }
    }
}
