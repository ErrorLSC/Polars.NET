using Kaminari.Sazanami.Helpers;

namespace Kaminari.Seseragi.Files.Excel.Xml;

/// <summary>
/// Buffers a (decompressing) stream of XML in native memory so that the parser can work on complete chunks (one
/// row, one shared string) with <see cref="XmlSpanScanner"/>. Only refilling touches the stream, synchronously or
/// asynchronously; parsing never does. The buffer grows when one chunk does not fit.
/// </summary>
internal sealed class XmlChunkReader(Stream stream, int initialSize = 64 * 1024) : IDisposable
{
    private readonly NativeByteBuffer _buffer = new(initialSize);
    private int _start;
    private int _end;
    private bool _bomChecked;

    /// <summary>The stream is exhausted (what is buffered is all there is).</summary>
    public bool EndOfStream { get; private set; }

    /// <summary>The bytes buffered and not yet consumed. Invalid after a refill.</summary>
    public ReadOnlySpan<byte> Buffered => _buffer.Span[_start.._end];

    /// <summary>Marks <paramref name="count"/> buffered bytes consumed.</summary>
    public void Consume(int count) => _start += count;

    /// <summary>Reads more of the stream; false when the stream had nothing left.</summary>
    public bool Fill()
    {
        if (EndOfStream) return false;
        Prepare();
        int n = stream.Read(_buffer.Span[_end..]);
        return Advance(n);
    }

    /// <inheritdoc cref="Fill"/>
    public async ValueTask<bool> FillAsync(CancellationToken cancellationToken)
    {
        if (EndOfStream) return false;
        Prepare();
        int n = await stream.ReadAsync(_buffer.Memory[_end..], cancellationToken).ConfigureAwait(false);
        return Advance(n);
    }

    // Moves unconsumed bytes to the front and makes room for at least a quarter of the buffer.
    private void Prepare()
    {
        if (_start > 0)
        {
            int live = _end - _start;
            _buffer.Span.Slice(_start, live).CopyTo(_buffer.Span);
            _start = 0;
            _end = live;
        }
        if (_buffer.Capacity - _end < _buffer.Capacity / 4) _buffer.EnsureCapacity((long)_buffer.Capacity * 2);
    }

    private bool Advance(int n)
    {
        if (n <= 0)
        {
            EndOfStream = true;
            return false;
        }
        _end += n;
        if (!_bomChecked && _end - _start >= 3)
        {
            _bomChecked = true;
            var b = _buffer.Span[_start.._end];
            if (b[0] == 0xEF && b[1] == 0xBB && b[2] == 0xBF) _start += 3;
            else if (b[0] is 0xFE or 0xFF && b[1] is 0xFE or 0xFF)
                throw new ExcelFormatException("UTF-16 encoded workbook parts are not supported.");
        }
        return true;
    }

    public void Dispose() => _buffer.Release();
}
