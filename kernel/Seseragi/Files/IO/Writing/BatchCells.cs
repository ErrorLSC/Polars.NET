using System.Buffers;
using Apache.Arrow;

namespace Kaminari.Seseragi.Files.IO.Writing;

/// <summary>The buffers of a batch pinned while its cells are written (native memory stays put; managed memory is pinned).</summary>
internal sealed unsafe class Pins : IDisposable
{
    private readonly List<MemoryHandle> _handles = [];

    public byte* Pin(ArrowBuffer buffer)
    {
        if (buffer.IsEmpty) return null;
        MemoryHandle handle = buffer.Memory.Pin();
        _handles.Add(handle);
        return (byte*)handle.Pointer;
    }

    public void Dispose()
    {
        foreach (MemoryHandle handle in _handles) handle.Dispose();
        _handles.Clear();
    }
}

/// <summary>
/// The cells of a batch's columns (one <typeparamref name="TCell"/> per column, made by the writer's format), its buffers
/// pinned until its last piece is encoded (an owned batch then disposed).
/// </summary>
internal sealed class BatchCells<TCell>
{
    private readonly Pins _pins = new();
    private readonly RecordBatch? _owned;
    private int _pending;

    public BatchCells(RecordBatch batch, Schema schema, bool owned, Func<ArrayData, Field, Pins, TCell> cell)
    {
        try
        {
            Cells = [.. schema.FieldsList.Select((f, i) => cell(batch.Column(i).Data, f, _pins))];
        }
        catch
        {
            _pins.Dispose();
            throw;
        }
        _owned = owned ? batch : null;
    }

    public TCell[] Cells { get; }

    /// <summary>The pieces that will each <see cref="Release"/> the batch.</summary>
    public void Expect(int pieces) => _pending = pieces;

    public void Release()
    {
        if (Interlocked.Decrement(ref _pending) != 0) return;
        _pins.Dispose();
        _owned?.Dispose();
    }

    /// <summary>Unpins the batch's buffers (cells of a caller's batch appended as rows, not encoded by pieces).</summary>
    public void Dispose() => _pins.Dispose();
}
