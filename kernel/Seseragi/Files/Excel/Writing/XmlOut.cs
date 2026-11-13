using System.Buffers;
using System.Buffers.Text;
using System.Globalization;
using System.Text;
using Kaminari.Sazanami.Arrow;

namespace Kaminari.Seseragi.Files.Excel.Writing;

/// <summary>
/// UTF-8 XML output in native memory: raw markup, numbers, and text escaped for XML and for OOXML's
/// <c>_xHHHH_</c> convention (characters XML 1.0 cannot carry, and literal <c>_xHHHH_</c> sequences).
/// </summary>
internal sealed unsafe class XmlOut : IDisposable
{
    // Bytes that need a look: markup characters, control characters, '_' (a literal escape sequence), 0xEF (U+FFFE/U+FFFF).
    private static readonly SearchValues<byte> Special = SearchValues.Create(
        [.. Enumerable.Range(0, 0x20).Select(i => (byte)i).Where(b => b is not ((byte)'\t' or (byte)'\n')), (byte)'&', (byte)'<', (byte)'>', (byte)'"', (byte)'_', 0xEF]);

    private NativeGrowBuffer _buffer;

    public long Length => _buffer.Length;

    public ReadOnlySpan<byte> Written => new(_buffer.Pointer, checked((int)_buffer.Length));

    /// <summary>The written bytes as memory for asynchronous stream writes (valid until the next append).</summary>
    public ReadOnlyMemory<byte> WrittenMemory => new PointerMemory(_buffer.Pointer, checked((int)_buffer.Length)).Memory;

    private sealed class PointerMemory(byte* pointer, int length) : MemoryManager<byte>
    {
        public override Span<byte> GetSpan() => new(pointer, length);

        public override MemoryHandle Pin(int elementIndex = 0) => new(pointer + elementIndex);

        public override void Unpin() { }

        protected override void Dispose(bool disposing) { }
    }

    public void Clear() => _buffer.Clear();

    public void Raw(ReadOnlySpan<byte> bytes) => _buffer.Append(bytes);

    public void Byte(byte b) => _buffer.Append(b);

    /// <summary>A .NET string as escaped text.</summary>
    public void Text(string text)
    {
        int max = Encoding.UTF8.GetMaxByteCount(text.Length);
        byte[]? rented = max > 1024 ? ArrayPool<byte>.Shared.Rent(max) : null;
        Span<byte> utf8 = rented ?? stackalloc byte[1024];
        try
        {
            int n = Encoding.UTF8.GetBytes(text, utf8);
            Text(utf8[..n]);
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    public void Int(long value)
    {
        byte* at = _buffer.Reserve(20);
        Utf8Formatter.TryFormat(value, new Span<byte>(at, 20), out int n);
        _buffer.SetLength(_buffer.Length - 20 + n);
    }

    public void UInt(ulong value)
    {
        byte* at = _buffer.Reserve(20);
        Utf8Formatter.TryFormat(value, new Span<byte>(at, 20), out int n);
        _buffer.SetLength(_buffer.Length - 20 + n);
    }

    /// <summary>A finite double in its shortest round-trip form.</summary>
    public void Double(double value)
    {
        byte* at = _buffer.Reserve(32);
        value.TryFormat(new Span<byte>(at, 32), out int n, default, CultureInfo.InvariantCulture);
        _buffer.SetLength(_buffer.Length - 32 + n);
    }

    /// <summary>A finite float in its shortest round-trip form (as single precision).</summary>
    public void Single(float value)
    {
        byte* at = _buffer.Reserve(32);
        value.TryFormat(new Span<byte>(at, 32), out int n, default, CultureInfo.InvariantCulture);
        _buffer.SetLength(_buffer.Length - 32 + n);
    }

    /// <summary>A1 reference: column letters then the row number (both precomputed by the caller).</summary>
    public void CellRef(ReadOnlySpan<byte> column, ReadOnlySpan<byte> row)
    {
        _buffer.Append(column);
        _buffer.Append(row);
    }

    /// <summary>
    /// UTF-8 text escaped for element content: <c>&amp;</c>, <c>&lt;</c>, <c>&gt;</c>, <c>&quot;</c> (so it is also
    /// safe in attributes); control characters, CR, U+FFFE and U+FFFF as <c>_xHHHH_</c>; a literal <c>_xHHHH_</c>
    /// with its underscore escaped (<c>_x005F_</c>).
    /// </summary>
    public void Text(ReadOnlySpan<byte> utf8)
    {
        while (true)
        {
            int i = utf8.IndexOfAny(Special);
            if (i < 0)
            {
                _buffer.Append(utf8);
                return;
            }
            _buffer.Append(utf8[..i]);
            byte b = utf8[i];
            int consumed = 1;
            switch (b)
            {
                case (byte)'&': _buffer.Append("&amp;"u8); break;
                case (byte)'<': _buffer.Append("&lt;"u8); break;
                case (byte)'>': _buffer.Append("&gt;"u8); break;
                case (byte)'"': _buffer.Append("&quot;"u8); break;
                case (byte)'_':
                    if (IsEscapeSequence(utf8[i..])) _buffer.Append("_x005F_"u8);
                    else _buffer.Append(b);
                    break;
                case 0xEF:
                    if (utf8.Length - i >= 3 && utf8[i + 1] == 0xBF && utf8[i + 2] is 0xBE or 0xBF)
                    {
                        _buffer.Append(utf8[i + 2] == 0xBE ? "_xFFFE_"u8 : "_xFFFF_"u8);
                        consumed = 3;
                    }
                    else
                    {
                        _buffer.Append(b);
                    }
                    break;
                default: // control character
                    _buffer.Append("_x00"u8);
                    _buffer.Append(Hex(b >> 4));
                    _buffer.Append(Hex(b & 0xF));
                    _buffer.Append((byte)'_');
                    break;
            }
            utf8 = utf8[(i + consumed)..];
        }

        static byte Hex(int v) => (byte)(v < 10 ? '0' + v : 'A' + v - 10);
    }

    private static bool IsEscapeSequence(ReadOnlySpan<byte> s) =>
        s.Length >= 7 && s[1] == (byte)'x' && s[6] == (byte)'_'
        && Utf8Parser.TryParse(s.Slice(2, 4), out ushort _, out int used, 'X') && used == 4;

    /// <summary>Whether text needs <c>xml:space="preserve"</c> (leading or trailing whitespace).</summary>
    public static bool NeedsPreserve(ReadOnlySpan<byte> utf8) =>
        !utf8.IsEmpty && (IsSpace(utf8[0]) || IsSpace(utf8[^1]));

    private static bool IsSpace(byte b) => b is (byte)' ' or (byte)'\t' or (byte)'\n' or (byte)'\r';

    public void Dispose() => _buffer.Free();
}
