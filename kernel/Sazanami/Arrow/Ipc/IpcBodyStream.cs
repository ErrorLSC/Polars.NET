using Kaminari.Sazanami.Buffers;

namespace Kaminari.Sazanami.Arrow.Ipc;

/// <summary>
/// The buffers of a body handed over one by one as their compression finishes (in any order), so that a writer can
/// write them in order while the rest are still compressed: it waits for each (<see cref="Ready"/>), writes it and
/// frees its output (<see cref="Release"/>). What is not released is freed with this, once the compression has ended.
/// </summary>
internal sealed unsafe class IpcBodyStream(int count) : IDisposable
{
    private readonly TaskCompletionSource<IpcPiece>[] _ready =
        [.. Enumerable.Range(0, count).Select(_ => new TaskCompletionSource<IpcPiece>(TaskCreationOptions.RunContinuationsAsynchronously))];

    private readonly nint[] _owned = new nint[count]; // per buffer, the block holding its bytes (0: none of ours)

    /// <summary>The number of buffers.</summary>
    public int Count => _ready.Length;

    /// <summary>Buffer <paramref name="i"/> is final: its bytes, in <paramref name="owned"/> (a <see cref="NativeMemoryPool"/> block, now this one's) or elsewhere (0).</summary>
    public void Publish(int i, IpcPiece piece, nint owned)
    {
        _owned[i] = owned;
        _ready[i].TrySetResult(piece);
    }

    /// <summary>The compression failed: every buffer not handed over fails with it.</summary>
    public void Fail(Exception error)
    {
        foreach (TaskCompletionSource<IpcPiece> ready in _ready)
        {
            if (ready.TrySetException(error)) _ = ready.Task.Exception; // observed: a writer may have stopped earlier
        }
    }

    /// <summary>Buffer <paramref name="i"/> once final.</summary>
    public Task<IpcPiece> Ready(int i) => _ready[i].Task;

    /// <summary>Frees buffer <paramref name="i"/>'s bytes (written).</summary>
    public void Release(int i)
    {
        nint p = Interlocked.Exchange(ref _owned[i], 0);
        if (p != 0) NativeMemoryPool.Free((void*)p);
    }

    public void Dispose()
    {
        for (int i = 0; i < _owned.Length; i++) Release(i);
    }
}
