using System.Buffers;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Sazanami.Text;

/// <summary>
/// A stream of text in any encoding read as UTF-8: the source decoded in order by a <see cref="TextDecoder"/> (readers
/// that cut and tokenize UTF-8 read text of other layouts through it). A strict decoder's invalid unit becomes
/// <see cref="InvalidMark"/> (0xFF, never in UTF-8) and decoding goes on after it, so a UTF-8 check downstream finds the
/// failure at its place in the text (its line, its record); a lossy decoder replaces it as it does. Input blocks are
/// decoded into an output buffer that always holds their text (three bytes per byte and a few more), then copied to the
/// reader's buffers: any destination size makes progress. Buffers are native.
/// </summary>
internal sealed class TextDecodingStream : Stream
{
    /// <summary>The byte standing for an invalid unit in the output of a strict decoder.</summary>
    public const byte InvalidMark = 0xFF;

    private const int Block = 64 << 10;

    private readonly Stream _source;
    private readonly TextDecoder _decoder;
    private readonly bool _leaveOpen;
    private readonly int _unit; // the bytes an invalid unit is skipped by (a UTF-16 code unit: 2)
    private readonly NativeByteBuffer _input = new(Block, pooled: true);
    private readonly NativeByteBuffer _output = new(3 * Block + 64, pooled: true);
    private int _start, _end, _skip, _outStart, _outEnd;
    private bool _ended, _finished;

    /// <param name="source">The encoded text.</param>
    /// <param name="decoder">Its decoder.</param>
    /// <param name="skip">Bytes of the source passed over first (a byte order mark).</param>
    /// <param name="leaveOpen">Leave the source open when the stream is disposed.</param>
    public TextDecodingStream(Stream source, TextDecoder decoder, int skip, bool leaveOpen)
    {
        _source = source;
        _decoder = decoder;
        _leaveOpen = leaveOpen;
        _skip = skip;
        _unit = decoder.Layout == TextLayout.Utf16 ? 2 : 1;
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        if (buffer.IsEmpty) return 0;
        while (_outStart == _outEnd && !_finished)
        {
            if (!_ended) Fill(useAsync: false, default).GetAwaiter().GetResult();
            Decode();
        }
        return Copy(buffer);
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (buffer.IsEmpty) return 0;
        while (_outStart == _outEnd && !_finished)
        {
            if (!_ended) await Fill(useAsync: true, cancellationToken).ConfigureAwait(false);
            Decode();
        }
        return Copy(buffer.Span);
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    private int Copy(Span<byte> destination)
    {
        int n = Math.Min(destination.Length, _outEnd - _outStart);
        _output.Span.Slice(_outStart, n).CopyTo(destination);
        _outStart += n;
        return n;
    }

    // Decodes the buffered input into the (empty) output buffer; at the end, the decoder's last characters too.
    private void Decode()
    {
        (_outStart, _outEnd) = (0, 0);
        Span<byte> output = _output.Span;
        while (true)
        {
            OperationStatus status = _decoder.Decode(_input.Span[_start.._end], output[_outEnd..], _ended, out int consumed, out int written);
            _start += consumed;
            _outEnd += written;
            if (status == OperationStatus.InvalidData)
            {
                // Strict: the invalid unit becomes the mark, decoding goes on after it.
                output[_outEnd++] = InvalidMark;
                _start = Math.Min(_end, _start + _unit);
                continue;
            }
            // A stateful decoder's held characters filled the output: the rest comes with the next read.
            if (status == OperationStatus.DestinationTooSmall && _outEnd > 0) return;
            if (_ended && _start == _end) _finished = true;
            return;
        }
    }

    // Moves the undecoded rest to the front and reads the source until the buffer is full or the source ends.
    private async ValueTask Fill(bool useAsync, CancellationToken ct)
    {
        if (_start > 0)
        {
            _input.Span[_start.._end].CopyTo(_input.Span);
            (_start, _end) = (0, _end - _start);
        }
        while (_end < _input.Capacity)
        {
            int n = useAsync
                ? await _source.ReadAsync(_input.Memory[_end..], ct).ConfigureAwait(false)
                : _source.Read(_input.Span[_end..]);
            if (n == 0)
            {
                _ended = true;
                return;
            }
            if (_skip > 0)
            {
                int dropped = Math.Min(_skip, n);
                _input.Span.Slice(_end + dropped, n - dropped).CopyTo(_input.Span[_end..]);
                n -= dropped;
                _skip -= dropped;
            }
            _end += n;
        }
    }

    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _input.Release();
            _output.Release();
            if (!_leaveOpen) _source.Dispose();
        }
        base.Dispose(disposing);
    }
}
