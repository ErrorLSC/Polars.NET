using System.Buffers;
using System.Buffers.Binary;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Sazanami.Text;

/// <summary>
/// A stream of text read as UTF-8, its blocks decoded side by side: for encodings whose characters can be found from any
/// block boundary (UTF-16 at an even offset outside a surrogate pair, single-byte pages anywhere), the source is read in
/// order in blocks, each decoded by its own decoder on the pool (a window of them ahead of the reader), and the text
/// handed over in order — as <see cref="TextDecodingStream"/> gives it (invalid units of a strict decoder become
/// <see cref="TextDecodingStream.InvalidMark"/>). Buffers are native.
/// </summary>
internal sealed class ParallelTextDecodingStream : Stream
{

    private readonly Stream _source;
    private readonly Func<TextDecoder> _decoder;
    private readonly bool _utf16, _bigEndian, _leaveOpen;
    private readonly int _window, _block;
    private readonly Queue<Task<(NativeByteBuffer Text, int Length)>> _ahead = new();
    private NativeByteBuffer? _carry; // raw bytes held back for the next block (half a code unit or pair)
    private int _carryLength, _skip;
    private bool _ended;
    private NativeByteBuffer? _current;
    private int _at, _length;

    /// <summary>Whether text of <paramref name="decoder"/>'s encoding can be cut into blocks decoded apart.</summary>
    public static bool Splits(TextDecoder decoder) => decoder switch
    {
        Utf16TextDecoder => true,
        LegacyText legacy => !legacy.DoubleByte,
        SequentialTextDecoder sequential => sequential.Encoding.IsSingleByte,
        _ => false,
    };

    /// <param name="source">The encoded text.</param>
    /// <param name="decoder">A new decoder of its encoding (one per block).</param>
    /// <param name="skip">Bytes of the source passed over first (a byte order mark).</param>
    /// <param name="window">The most blocks decoded ahead of the reader.</param>
    /// <param name="leaveOpen">Leave the source open when the stream is disposed.</param>
    /// <param name="block">The raw bytes of a block (at least 16).</param>
    public ParallelTextDecodingStream(Stream source, Func<TextDecoder> decoder, int skip, int window, bool leaveOpen, int block = 1 << 20)
    {
        _block = Math.Max(16, block);
        _source = source;
        _decoder = decoder;
        _skip = skip;
        _window = Math.Max(1, window);
        _leaveOpen = leaveOpen;
        TextDecoder sample = decoder();
        if (!Splits(sample)) throw new ArgumentException($"{sample.GetType().Name} text cannot be cut into blocks.", nameof(decoder));
        _utf16 = sample is Utf16TextDecoder;
        _bigEndian = sample is Utf16TextDecoder { BigEndian: true };
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
        while (_at == _length)
        {
            if (!Next(useAsync: false, default).GetAwaiter().GetResult()) return 0;
        }
        return Copy(buffer);
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (buffer.IsEmpty) return 0;
        while (_at == _length)
        {
            if (!await Next(useAsync: true, cancellationToken).ConfigureAwait(false)) return 0;
        }
        return Copy(buffer.Span);
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    private int Copy(Span<byte> destination)
    {
        int n = Math.Min(destination.Length, _length - _at);
        _current!.Span.Slice(_at, n).CopyTo(destination);
        _at += n;
        return n;
    }

    // The next block's text: the window filled with blocks read and started, the first one taken; false at the end.
    private async ValueTask<bool> Next(bool useAsync, CancellationToken ct)
    {
        _current?.Release();
        _current = null;
        (_at, _length) = (0, 0);
        while (!_ended && _ahead.Count < _window) await StartBlock(useAsync, ct).ConfigureAwait(false);
        if (_ahead.Count == 0) return false;
        Task<(NativeByteBuffer Text, int Length)> next = _ahead.Dequeue();
        (_current, _length) = useAsync ? await next.ConfigureAwait(false) : next.GetAwaiter().GetResult();
        return true;
    }

    // Reads a block (the carried bytes first), cuts it where a character starts, and starts its decoding.
    private async ValueTask StartBlock(bool useAsync, CancellationToken ct)
    {
        var raw = new NativeByteBuffer(_block + 8, pooled: true);
        try
        {
            int length = 0;
            if (_carryLength > 0)
            {
                _carry!.Span[.._carryLength].CopyTo(raw.Span);
                length = _carryLength;
                _carryLength = 0;
            }
            while (length < _block)
            {
                int n = useAsync
                    ? await _source.ReadAsync(raw.Memory[length.._block], ct).ConfigureAwait(false)
                    : _source.Read(raw.Span[length.._block]);
                if (n == 0)
                {
                    _ended = true;
                    break;
                }
                if (_skip > 0)
                {
                    int dropped = Math.Min(_skip, n);
                    raw.Span.Slice(length + dropped, n - dropped).CopyTo(raw.Span[length..]);
                    n -= dropped;
                    _skip -= dropped;
                }
                length += n;
            }
            int cut = _ended ? length : Boundary(raw.Span[..length]);
            if (cut < length)
            {
                _carry ??= new NativeByteBuffer(16, pooled: true);
                raw.Span[cut..length].CopyTo(_carry.Span);
                _carryLength = length - cut;
            }
            if (cut == 0 && _ended)
            {
                raw.Release();
                return;
            }
            NativeByteBuffer block = raw;
            _ahead.Enqueue(Task.Run(() => DecodeBlock(block, cut), ct));
        }
        catch
        {
            raw.Release();
            throw;
        }
    }

    // Where a block of whole characters ends: UTF-16 at an even offset, not after a high surrogate.
    private int Boundary(ReadOnlySpan<byte> raw)
    {
        if (!_utf16) return raw.Length;
        int cut = raw.Length & ~1;
        if (cut >= 2)
        {
            ushort last = _bigEndian ? BinaryPrimitives.ReadUInt16BigEndian(raw[(cut - 2)..]) : BinaryPrimitives.ReadUInt16LittleEndian(raw[(cut - 2)..]);
            if (last is >= 0xD800 and <= 0xDBFF) cut -= 2;
        }
        return cut;
    }

    // A block decoded on its own (whole characters: decoded as a final block), strict invalid units marked.
    private (NativeByteBuffer, int) DecodeBlock(NativeByteBuffer raw, int length)
    {
        var text = new NativeByteBuffer(3 * length + 64, pooled: true);
        try
        {
            return (text, Decode(raw.Span[..length], _decoder(), _utf16 ? 2 : 1, text.Span));
        }
        catch
        {
            text.Release();
            throw;
        }
        finally
        {
            raw.Release();
        }
    }

    // Whole characters decoded into output (three bytes per byte and 64 more), strict invalid units marked.
    private static int Decode(ReadOnlySpan<byte> source, TextDecoder decoder, int unit, Span<byte> output)
    {
        int at = 0, written = 0;
        while (true)
        {
            OperationStatus status = decoder.Decode(source[at..], output[written..], final: true, out int consumed, out int wrote);
            at += consumed;
            written += wrote;
            if (status == OperationStatus.InvalidData)
            {
                output[written++] = TextDecodingStream.InvalidMark;
                at = Math.Min(source.Length, at + unit);
                continue;
            }
            if (status != OperationStatus.Done) throw new InvalidOperationException($"A block of whole characters did not decode ({status}).");
            return written;
        }
    }

    /// <summary>
    /// The whole of <paramref name="data"/> (no byte order mark) decoded into one native buffer of UTF-8, its blocks side by
    /// side; null when the text could exceed 2 GB once decoded (or its encoding cannot be cut, see <see cref="Splits"/>).
    /// </summary>
    public static unsafe NativeByteBuffer? DecodeAll(byte* data, long length, Func<TextDecoder> decoder, int threads, out int written, int block = 1 << 20)
    {
        written = 0;
        block = Math.Max(16, block); // a block always holds a character and moves on
        TextDecoder sample = decoder();
        if (!Splits(sample) || 3 * length + 64 > int.MaxValue) return null;
        bool utf16 = sample is Utf16TextDecoder, bigEndian = sample is Utf16TextDecoder { BigEndian: true };
        int unit = utf16 ? 2 : 1;
        // Block bounds where characters start.
        List<long> bounds = [0];
        for (long at = 0; at < length;)
        {
            long end = Math.Min(length, at + block);
            if (end < length && utf16)
            {
                end = at + ((end - at) & ~1L);
                ushort last = bigEndian ? BinaryPrimitives.ReadUInt16BigEndian(new ReadOnlySpan<byte>(data + end - 2, 2))
                    : BinaryPrimitives.ReadUInt16LittleEndian(new ReadOnlySpan<byte>(data + end - 2, 2));
                if (last is >= 0xD800 and <= 0xDBFF) end -= 2;
            }
            bounds.Add(end);
            at = end;
        }
        int blocks = bounds.Count - 1;
        if (utf16 && !bigEndian == BitConverter.IsLittleEndian && Direct(data, bounds, decoder, threads, out written) is { } direct) return direct;
        var texts = new NativeByteBuffer?[blocks];
        var lengths = new int[blocks];
        try
        {
            Parallel.For(0, blocks, new ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, threads) }, b =>
            {
                int size = (int)(bounds[b + 1] - bounds[b]);
                var text = new NativeByteBuffer(3 * size + 64, pooled: true);
                texts[b] = text;
                lengths[b] = Decode(new ReadOnlySpan<byte>(data + bounds[b], size), decoder(), unit, text.Span);
            });
            long total = 0;
            var starts = new long[blocks];
            for (int b = 0; b < blocks; b++)
            {
                starts[b] = total;
                total += lengths[b];
            }
            var all = new NativeByteBuffer((int)Math.Max(1, total), pooled: true);
            try
            {
                Parallel.For(0, blocks, new ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, threads) },
                    b => texts[b]!.Span[..lengths[b]].CopyTo(all.Span.Slice((int)starts[b], lengths[b])));
            }
            catch
            {
                all.Release();
                throw;
            }
            written = (int)total;
            return all;
        }
        finally
        {
            foreach (NativeByteBuffer? text in texts) text?.Release();
        }
    }

    // UTF-16 in the machine's order: each block's UTF-8 length counted side by side, then each block decoded straight into
    // its place in one buffer; null when a block's text differs from its count (invalid units: marked, not replaced).
    private static unsafe NativeByteBuffer? Direct(byte* data, List<long> bounds, Func<TextDecoder> decoder, int threads, out int written)
    {
        written = 0;
        int blocks = bounds.Count - 1;
        var sizes = new long[blocks];
        var parallel = new ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, threads) };
        Parallel.For(0, blocks, parallel, b =>
        {
            long from = bounds[b], to = bounds[b + 1];
            var units = new ReadOnlySpan<char>(data + from, (int)((to - from) / 2));
            sizes[b] = System.Text.Encoding.UTF8.GetByteCount(units) + ((to - from) & 1) * 3;
        });
        var starts = new long[blocks + 1];
        for (int b = 0; b < blocks; b++) starts[b + 1] = starts[b] + sizes[b];
        if (starts[blocks] > int.MaxValue - 64) return null;
        var all = new NativeByteBuffer((int)Math.Max(1, starts[blocks]), pooled: true);
        int mismatched = 0;
        try
        {
            Parallel.For(0, blocks, parallel, b =>
            {
                long from = bounds[b];
                int size = (int)(bounds[b + 1] - from);
                Span<byte> place = all.Span.Slice((int)starts[b], (int)sizes[b]);
                OperationStatus status = decoder().Decode(new ReadOnlySpan<byte>(data + from, size), place, final: true, out int consumed, out int wrote);
                if (status != OperationStatus.Done || consumed != size || wrote != sizes[b]) Interlocked.Increment(ref mismatched);
            });
        }
        catch
        {
            all.Release();
            throw;
        }
        if (mismatched > 0)
        {
            all.Release();
            return null;
        }
        written = (int)starts[blocks];
        return all;
    }

    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _current?.Release();
            while (_ahead.Count > 0)
            {
                Task<(NativeByteBuffer Text, int Length)> task = _ahead.Dequeue();
                try { task.GetAwaiter().GetResult().Text.Release(); }
                catch (Exception) { /* a block that failed holds nothing */ }
            }
            _carry?.Release();
            if (!_leaveOpen) _source.Dispose();
        }
        base.Dispose(disposing);
    }
}
