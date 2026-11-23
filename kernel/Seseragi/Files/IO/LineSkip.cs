namespace Kaminari.Seseragi.Files.IO;

/// <summary>
/// Raw lines passed over at the start of a text input (CSV SkipLines): line ends counted as they are — LF, CR or CRLF
/// (one), or the chosen byte — quotes and comment prefixes ignored, nothing decoded or checked.
/// </summary>
internal sealed class LineSkip(long lines, byte? lineEnd)
{
    private const int Chunk = 1 << 20;

    private readonly long _lines = lines;
    private long _remaining = lines;
    private bool _pendingCr, _tail;

    /// <summary>The lines found: line ends, and a last line without one at the end of the input.</summary>
    public long Found => _lines - _remaining + (_remaining > 0 && _tail ? 1 : 0);

    /// <summary>Whether every line asked was passed over (a last line without a line end counts).</summary>
    public bool Done => _remaining == 0 || (_remaining == 1 && _tail);

    /// <summary>
    /// Takes the next bytes of the input; returns where the text after the lines starts in them, or -1 when they are
    /// passed over whole (more may be needed, or the input ended: see <see cref="Done"/>).
    /// </summary>
    public int Feed(ReadOnlySpan<byte> chunk)
    {
        int i = 0;
        if (_pendingCr)
        {
            // A CR last in the previous bytes: an LF after it belongs to it.
            _pendingCr = false;
            if (chunk.Length > 0 && chunk[0] == '\n') i = 1;
        }
        if (_remaining == 0) return i;
        while (true)
        {
            int k = lineEnd is { } e ? chunk[i..].IndexOf(e) : chunk[i..].IndexOfAny((byte)'\n', (byte)'\r');
            if (k < 0)
            {
                if (i < chunk.Length) _tail = true;
                return -1;
            }
            int at = i + k;
            _tail = false;
            i = at + 1;
            _remaining--;
            if (lineEnd is null && chunk[at] == '\r')
            {
                if (i == chunk.Length)
                {
                    _pendingCr = true;
                    return -1;
                }
                if (chunk[i] == '\n') i++;
            }
            if (_remaining == 0) return i;
        }
    }

    /// <summary>Where the text after the lines starts in <paramref name="source"/> (from <paramref name="from"/>), or -1 when it holds fewer.</summary>
    public async ValueTask<long> SkipAsync(IRandomAccess source, long from, bool useAsync, CancellationToken ct)
    {
        if (_lines == 0) return from;
        // Memory and a mapped file are scanned in place, other sources read in pieces into native memory.
        if (source is ILocalSource local && local.TryGetMemory(out ReadOnlyMemory<byte> memory))
            return Scanned(Feed(memory.Span[(int)Math.Min(from, memory.Length)..]), from, memory.Length);
        if (source is ILocalSource mappable && mappable.Map(out long start) is { } mapped && mapped.TryAcquire())
        {
            try { return InPlace(mapped, start, from, source.Length); }
            finally { mapped.Release(); }
        }
        var buffer = new Sazanami.Buffers.NativeByteBuffer((int)Math.Min(Chunk, Math.Max(1, source.Length - from)), pooled: true);
        try
        {
            long at = from;
            while (at < source.Length)
            {
                int n = useAsync ? await source.ReadAtAsync(at, buffer.Memory, ct).ConfigureAwait(false) : source.ReadAt(at, buffer.Span);
                if (n <= 0) break;
                int found = Feed(buffer.Span[..n]);
                if (found >= 0) return at + found;
                at += n;
            }
            return Done ? source.Length : -1;
        }
        finally
        {
            buffer.Release();
        }
    }

    // A mapped file scanned in place, in pieces a span holds.
    private unsafe long InPlace(Sazanami.Buffers.MappedFile mapped, long start, long from, long length)
    {
        byte* data = mapped.At(start, length);
        for (long at = from; at < length;)
        {
            int n = (int)Math.Min(length - at, 1 << 30);
            int found = Feed(new ReadOnlySpan<byte>(data + at, n));
            if (found >= 0) return at + found;
            at += n;
        }
        return Done ? length : -1;
    }

    // Where the text starts after a scan of everything from `from` (found: in it), or the end, or -1.
    private long Scanned(int found, long from, long length) => found >= 0 ? from + found : Done ? length : -1;

    /// <summary>The rest of <paramref name="stream"/> after the lines (owned: disposed with it), or null when it holds fewer.</summary>
    public async ValueTask<Stream?> SkipAsync(Stream stream, bool useAsync, CancellationToken ct)
    {
        if (_lines == 0) return stream;
        var buffer = new Sazanami.Buffers.NativeByteBuffer(Chunk, pooled: true);
        try
        {
            while (true)
            {
                int n = useAsync ? await stream.ReadAsync(buffer.Memory, ct).ConfigureAwait(false) : stream.Read(buffer.Span);
                if (n <= 0) break;
                int found = Feed(buffer.Span[..n]);
                if (found < 0) continue;
                // The rest of this piece comes first (from the native buffer, released with the stream).
                var rest = new PrefixedStream(buffer.Memory[found..n], stream, leaveOpen: false, new Releaser(buffer));
                buffer = null;
                return rest;
            }
            return Done ? new PrefixedStream(ReadOnlyMemory<byte>.Empty, stream, leaveOpen: false) : null;
        }
        finally
        {
            buffer?.Release();
        }
    }
}
