using System.Buffers;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Seseragi.Files.IO.Writing;

/// <summary>
/// The buffers of a batch held while its cells are written: native memory (<see cref="INativeMemory"/>) by its address,
/// managed memory pinned.
/// </summary>
/// <param name="capacity">The buffers expected (handles of pinned ones kept in one array of this size, made at the first pin).</param>
internal sealed unsafe class Pins(int capacity = 8) : IDisposable
{
    private MemoryHandle[] _handles = [];
    private int _count;

    public byte* Pin(ArrowBuffer buffer)
    {
        if (buffer.IsEmpty) return null;
        // Native memory stays put: its address alone (the batch keeps it until its pieces are encoded), no handle.
        if (MemoryMarshal.TryGetMemoryManager(buffer.Memory, out MemoryManager<byte>? manager, out int start, out _) && manager is INativeMemory native)
            return native.Pointer + start;
        MemoryHandle handle = buffer.Memory.Pin();
        if (_count == _handles.Length) System.Array.Resize(ref _handles, _count == 0 ? Math.Max(1, capacity) : _count * 2);
        _handles[_count++] = handle;
        return (byte*)handle.Pointer;
    }

    /// <summary>The buffers of <paramref name="data"/>, its children and dictionary (what its cell may pin).</summary>
    public static int Count(ArrayData data)
    {
        int count = data.Buffers.Length;
        foreach (ArrayData child in data.Children ?? []) count += Count(child);
        return data.Dictionary is { } dictionary ? count + Count(dictionary) : count;
    }

    public void Dispose()
    {
        for (int i = 0; i < _count; i++) _handles[i].Dispose();
        System.Array.Clear(_handles, 0, _count);
        _count = 0;
    }
}

/// <summary>
/// The cells of a batch's columns (one <typeparamref name="TCell"/> per column, made by the writer's format), its buffers
/// pinned until its last piece is encoded (an owned batch then disposed).
/// </summary>
internal sealed class BatchCells<TCell>
{
    private readonly Pins _pins;
    private readonly RecordBatch? _owned;
    private int _pending;

    public BatchCells(RecordBatch batch, Schema schema, bool owned, Func<ArrayData, Field, Pins, TCell> cell)
    {
        IReadOnlyList<Field> fields = schema.FieldsList;
        int buffers = 0;
        for (int i = 0; i < fields.Count; i++) buffers += Pins.Count(batch.Column(i).Data);
        _pins = new Pins(buffers);
        try
        {
            var cells = new TCell[fields.Count];
            for (int i = 0; i < cells.Length; i++) cells[i] = cell(batch.Column(i).Data, fields[i], _pins);
            Cells = cells;
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
