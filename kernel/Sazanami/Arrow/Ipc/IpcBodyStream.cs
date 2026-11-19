using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Compression;

namespace Kaminari.Sazanami.Arrow.Ipc;

/// <summary>
/// A block of a streamed body's output, packed by one compressing thread with the units it compresses: it holds a
/// reference of its own while it packs, each unit placed one more; freed when the last is dropped (units written, or
/// not used), or with the stream it belongs to.
/// </summary>
internal sealed unsafe class IpcChunk(nint pointer, long bytes)
{
    private nint _pointer = pointer;
    private int _references = 1;

    public byte* Pointer => (byte*)_pointer;

    public long Bytes => bytes;

    public void Ref() => Interlocked.Increment(ref _references);

    public void Unref()
    {
        if (Interlocked.Decrement(ref _references) == 0) Free();
    }

    /// <summary>Frees the block (once, whatever the references: the stream it belongs to is done).</summary>
    public void Free()
    {
        nint p = Interlocked.Exchange(ref _pointer, 0);
        if (p != 0) NativeMemoryPool.Free((void*)p);
    }
}

/// <summary>The bytes of one unit of a buffer, in a chunk (holding a reference of it).</summary>
internal readonly record struct IpcChunkPiece(IpcChunk Chunk, nint Pointer, int Length);

/// <summary>
/// A buffer of a streamed body as written: <see cref="Piece"/> (its prefix, its length in the body) and its bytes, in
/// <see cref="Pieces"/> (compressed: one unit's bytes after another, for LZ4 blocks between the frame's header and end
/// mark when <see cref="Framed"/>) or at <see cref="IpcPiece.Pointer"/> (stored as it is: in <see cref="Owned"/>, a
/// block of the stream's, or the caller's).
/// </summary>
internal readonly record struct IpcStreamedBuffer(IpcPiece Piece, IpcChunkPiece[]? Pieces = null, bool Framed = false, nint Owned = 0);

/// <summary>
/// The buffers of a body handed over one by one as their compression finishes (in any order), so that a writer can
/// write them in order while the rest are still compressed: it waits for each (<see cref="Ready"/>), writes it and
/// lets its memory go (<see cref="Release"/>), which later units take again. Every chunk and block left is freed with
/// this, once the compression has ended.
/// </summary>
internal sealed unsafe class IpcBodyStream(int count) : IDisposable
{
    private readonly TaskCompletionSource<IpcStreamedBuffer>[] _ready =
        [.. Enumerable.Range(0, count).Select(_ => new TaskCompletionSource<IpcStreamedBuffer>(TaskCreationOptions.RunContinuationsAsynchronously))];

    private readonly IpcStreamedBuffer[] _buffers = new IpcStreamedBuffer[count];
    private readonly int[] _released = new int[count];
    private readonly List<IpcChunk> _chunks = [];
    private readonly object _lock = new();

    /// <summary>The LZ4 frame header written before the blocks of a split buffer (<see cref="IpcStreamedBuffer.Framed"/>).</summary>
    public static readonly byte[] Lz4FrameHeader = MakeHeader();

    /// <summary>The LZ4 frame's end mark, after the blocks.</summary>
    public static readonly byte[] Lz4FrameEnd = new byte[Lz4Frame.EndLength];

    private static byte[] MakeHeader()
    {
        byte[] header = new byte[Lz4Frame.HeaderLength];
        Lz4Frame.WriteHeader(header);
        return header;
    }

    /// <summary>The number of buffers.</summary>
    public int Count => _ready.Length;

    /// <summary>The chunks not freed yet (tests: none once every buffer is released and the compression has ended).</summary>
    internal int LiveChunks
    {
        get
        {
            lock (_lock) return _chunks.Count(c => c.Pointer != null);
        }
    }

    /// <summary>A chunk of <paramref name="bytes"/> (its packer's reference taken), freed with this at the latest.</summary>
    public IpcChunk NewChunk(long bytes)
    {
        var chunk = new IpcChunk((nint)NativeMemoryPool.Alloc(bytes), bytes);
        lock (_lock) _chunks.Add(chunk);
        return chunk;
    }

    /// <summary>Buffer <paramref name="i"/> is final (its pieces' references and its owned block now this one's).</summary>
    public void Publish(int i, IpcStreamedBuffer buffer)
    {
        _buffers[i] = buffer;
        _ready[i].TrySetResult(buffer);
    }

    /// <summary>The compression failed: every buffer not handed over fails with it.</summary>
    public void Fail(Exception error)
    {
        foreach (TaskCompletionSource<IpcStreamedBuffer> ready in _ready)
        {
            if (ready.TrySetException(error)) _ = ready.Task.Exception; // observed: a writer may have stopped earlier
        }
    }

    /// <summary>Buffer <paramref name="i"/> once final.</summary>
    public Task<IpcStreamedBuffer> Ready(int i) => _ready[i].Task;

    /// <summary>Lets buffer <paramref name="i"/>'s memory go (written).</summary>
    public void Release(int i)
    {
        if (Interlocked.Exchange(ref _released[i], 1) != 0) return;
        IpcStreamedBuffer buffer = _buffers[i];
        foreach (IpcChunkPiece piece in buffer.Pieces ?? []) piece.Chunk.Unref();
        if (buffer.Owned != 0) NativeMemoryPool.Free((void*)buffer.Owned);
    }

    /// <summary>Frees what is left (never while the body is compressed).</summary>
    public void Dispose()
    {
        for (int i = 0; i < _buffers.Length; i++)
        {
            if (_ready[i].Task.IsCompletedSuccessfully) Release(i);
        }
        lock (_lock)
        {
            foreach (IpcChunk chunk in _chunks) chunk.Free();
            _chunks.Clear();
        }
    }
}
