using System.Globalization;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Seseragi.Files.IO.Writing;

/// <summary>
/// Where encoded text goes (JSON, CSV): spans of a <see cref="NativeBufferWriter"/> filled in place, committed to it as they fill
/// (and by <see cref="Commit"/>).
/// </summary>
internal ref struct TextOut(NativeBufferWriter writer)
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
