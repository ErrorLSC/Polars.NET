using System.Buffers;
using System.Globalization;
using System.Numerics;
using System.Text.Unicode;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Seseragi.Files.Json.Writing;

/// <summary>
/// Where encoded JSON goes: spans of a <see cref="NativeBufferWriter"/> filled in place, committed to it as they fill
/// (and by <see cref="Commit"/>).
/// </summary>
internal ref struct JsonOut(NativeBufferWriter writer)
{
    private const int Step = 1 << 16;

    private Span<byte> _span;
    private int _at;

    /// <summary>The bytes written into this piece so far (committed or not).</summary>
    public readonly long Written => writer.WrittenCount + _at;

    /// <summary>At least <paramref name="bytes"/> free bytes to write into (then <see cref="Advance"/>).</summary>
    public Span<byte> Room(int bytes)
    {
        if (_span.Length - _at < bytes) Grow(bytes);
        return _span[_at..];
    }

    public void Advance(int bytes) => _at += bytes;

    public void Byte(byte value)
    {
        Room(1)[0] = value;
        _at++;
    }

    public void Bytes(scoped ReadOnlySpan<byte> bytes)
    {
        bytes.CopyTo(Room(bytes.Length));
        _at += bytes.Length;
    }

    /// <summary>Writes a value in its invariant UTF-8 text (integers).</summary>
    public void Format<T>(T value) where T : IUtf8SpanFormattable
    {
        Span<byte> room = Room(48);
        if (!value.TryFormat(room, out int written, default, CultureInfo.InvariantCulture)) throw new InvalidOperationException("A number's text does not fit.");
        _at += written;
    }

    private void Grow(int bytes)
    {
        writer.Advance(_at);
        _at = 0;
        _span = writer.GetSpan(Math.Max(bytes, Step));
    }

    /// <summary>Hands what was written to the buffer writer.</summary>
    public void Commit()
    {
        writer.Advance(_at);
        _at = 0;
        _span = default;
    }
}

/// <summary>JSON text of numbers and strings, as the reference dataframe engine writes them.</summary>
internal static class JsonText
{
    // What a string must escape: quotes, backslashes, control characters.
    private static readonly SearchValues<byte> Escaped = SearchValues.Create([.. Enumerable.Range(0, 0x20).Select(b => (byte)b), (byte)'"', (byte)'\\']);

    /// <summary>
    /// A floating-point number (finite): its shortest round-trip digits, in decimal notation when its exponent is
    /// from −5 to 15 (a whole number ends <c>.0</c>), else as <c>d.ddde±x</c>.
    /// </summary>
    public static void Float<T>(ref JsonOut o, T value) where T : IFloatingPointIeee754<T>, IUtf8SpanFormattable
    {
        Span<byte> digits = stackalloc byte[32];
        bool negative = T.IsNegative(value);
        if (T.IsZero(value))
        {
            o.Bytes(negative ? "-0.0"u8 : "0.0"u8);
            return;
        }
        int count, exponent;
        if (typeof(T) == typeof(double))
        {
            Sazanami.Text.ShortestDouble.Digits(double.Abs(System.Runtime.CompilerServices.Unsafe.As<T, double>(ref value)), out ulong mantissa, out int e10);
            mantissa.TryFormat(digits, out count, default, CultureInfo.InvariantCulture);
            exponent = e10 + count - 1;
        }
        else (count, exponent) = NetDigits(T.Abs(value), digits);
        Layout(ref o, negative, digits[..count].TrimEnd((byte)'0'), exponent);
    }

    // The shortest digits of a positive value as .NET writes them ("1.5E-06", "123", "0.0001"): its significant digits
    // and the exponent of the first.
    private static (int Count, int Exponent) NetDigits<T>(T value, Span<byte> digits) where T : IUtf8SpanFormattable
    {
        Span<byte> text = stackalloc byte[48];
        if (!value.TryFormat(text, out int length, default, CultureInfo.InvariantCulture)) throw new InvalidOperationException("A number's text does not fit.");
        text = text[..length];
        int e = text.IndexOfAny((byte)'E', (byte)'e');
        ReadOnlySpan<byte> mantissa = e < 0 ? text : text[..e];
        int point = mantissa.IndexOf((byte)'.');
        int integerDigits = point < 0 ? mantissa.Length : point, count = 0;
        foreach (byte b in mantissa)
            if (b != '.') digits[count++] = b;
        int lead = 0;
        while (lead < count - 1 && digits[lead] == '0') lead++;
        digits[lead..count].CopyTo(digits);
        int exponent = integerDigits - 1 - lead;
        if (e >= 0) exponent += int.Parse(text[(e + 1)..], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
        return (count - lead, exponent);
    }

    // Significant digits (no trailing zeros) d₀d₁… × 10^exponent laid out.
    private static void Layout(ref JsonOut o, bool negative, scoped ReadOnlySpan<byte> significant, int exponent)
    {
        Span<byte> room = o.Room(48);
        int at = 0;
        if (negative) room[at++] = (byte)'-';
        if (exponent is >= -5 and < 16)
        {
            if (exponent >= 0)
            {
                for (int i = 0; i <= exponent; i++) room[at++] = i < significant.Length ? significant[i] : (byte)'0';
                room[at++] = (byte)'.';
                if (significant.Length > exponent + 1)
                    foreach (byte b in significant[(exponent + 1)..]) room[at++] = b;
                else room[at++] = (byte)'0';
            }
            else
            {
                room[at++] = (byte)'0';
                room[at++] = (byte)'.';
                for (int i = 0; i < -exponent - 1; i++) room[at++] = (byte)'0';
                foreach (byte b in significant) room[at++] = b;
            }
        }
        else
        {
            room[at++] = significant[0];
            if (significant.Length > 1)
            {
                room[at++] = (byte)'.';
                foreach (byte b in significant[1..]) room[at++] = b;
            }
            room[at++] = (byte)'e';
            room[at++] = exponent < 0 ? (byte)'-' : (byte)'+';
            Math.Abs(exponent).TryFormat(room[at..], out int written, default, CultureInfo.InvariantCulture);
            at += written;
        }
        o.Advance(at);
    }

    /// <summary>
    /// A string in quotes: quotes, backslashes and control characters escaped (<c>\b \t \n \f \r</c> by name, the
    /// others as <c>\u00xx</c>), everything else as it is. False (nothing written) when it is not UTF-8.
    /// </summary>
    public static bool String(ref JsonOut o, scoped ReadOnlySpan<byte> utf8)
    {
        // ASCII is UTF-8 (the common case, checked fastest); anything else is checked whole.
        if (!System.Text.Ascii.IsValid(utf8) && !Utf8.IsValid(utf8)) return false;
        o.Byte((byte)'"');
        while (true)
        {
            int i = utf8.IndexOfAny(Escaped);
            if (i < 0)
            {
                o.Bytes(utf8);
                break;
            }
            o.Bytes(utf8[..i]);
            o.Advance(Escape(utf8[i], o.Room(6)));
            utf8 = utf8[(i + 1)..];
        }
        o.Byte((byte)'"');
        return true;
    }

    // One escaped byte: its two-character name, or \u00xx.
    private static int Escape(byte b, Span<byte> room)
    {
        room[0] = (byte)'\\';
        switch (b)
        {
            case (byte)'"': room[1] = (byte)'"'; return 2;
            case (byte)'\\': room[1] = (byte)'\\'; return 2;
            case (byte)'\b': room[1] = (byte)'b'; return 2;
            case (byte)'\t': room[1] = (byte)'t'; return 2;
            case (byte)'\n': room[1] = (byte)'n'; return 2;
            case (byte)'\f': room[1] = (byte)'f'; return 2;
            case (byte)'\r': room[1] = (byte)'r'; return 2;
            default:
                room[1] = (byte)'u';
                room[2] = (byte)'0';
                room[3] = (byte)'0';
                room[4] = Hex(b >> 4);
                room[5] = Hex(b & 0xF);
                return 6;
        }

        static byte Hex(int nibble) => (byte)(nibble < 10 ? '0' + nibble : 'a' + nibble - 10);
    }

    /// <summary>A name as its quoted JSON string followed by a colon (<c>"name":</c>).</summary>
    public static byte[] Name(string name)
    {
        using var buffer = new NativeBufferWriter(64);
        var o = new JsonOut(buffer);
        if (!String(ref o, System.Text.Encoding.UTF8.GetBytes(name))) throw new ArgumentException($"The name '{name}' is not UTF-8.");
        o.Byte((byte)':');
        o.Commit();
        return buffer.WrittenSpan.ToArray();
    }
}
