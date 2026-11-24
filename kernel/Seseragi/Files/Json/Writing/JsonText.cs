using System.Buffers;
using System.Globalization;
using System.Numerics;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Text;
using Kaminari.Seseragi.Files.IO.Writing;

namespace Kaminari.Seseragi.Files.Json.Writing;

/// <summary>JSON text of numbers and strings, as the reference dataframe engine writes them.</summary>
internal static class JsonText
{
    // What a string must escape: quotes, backslashes, control characters.
    private static readonly SearchValues<byte> Escaped = SearchValues.Create([.. Enumerable.Range(0, 0x20).Select(b => (byte)b), (byte)'"', (byte)'\\']);

    /// <summary>
    /// A floating-point number (finite): its shortest round-trip digits, in decimal notation when its exponent is
    /// from −5 to 15 (a whole number ends <c>.0</c>), else as <c>d.ddde±x</c> (<see cref="FloatText.Shortest{T}"/>).
    /// </summary>
    public static void Float<T>(ref TextOut o, T value) where T : IFloatingPointIeee754<T>, IUtf8SpanFormattable =>
        o.Advance(FloatText.Shortest(value, o.Room(FloatText.MaxShortestBytes)));

    /// <summary>
    /// A string in quotes: quotes, backslashes and control characters escaped (<c>\b \t \n \f \r</c> by name, the
    /// others as <c>\u00xx</c>), everything else as it is. Its bytes are UTF-8 (Arrow's rule for strings; not checked, as
    /// the reference engine does not).
    /// </summary>
    public static void String(ref TextOut o, scoped ReadOnlySpan<byte> utf8)
    {
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
        var o = new TextOut(buffer);
        String(ref o, System.Text.Encoding.UTF8.GetBytes(name));
        o.Byte((byte)':');
        o.Commit();
        return buffer.WrittenSpan.ToArray();
    }
}
