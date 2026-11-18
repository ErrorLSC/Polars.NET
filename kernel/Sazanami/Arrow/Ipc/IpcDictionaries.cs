using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;

namespace Kaminari.Sazanami.Arrow.Ipc;

/// <summary>
/// The dictionaries of an IPC stream or file by id, as dictionary batches define them: a batch replaces the
/// dictionary of its id, or (a delta) appends to it. Each dictionary holds its memory until it is replaced or this is
/// disposed. Not thread-safe.
/// </summary>
internal sealed class IpcDictionaries : IDisposable
{
    private readonly Dictionary<long, IpcArray> _byId = [];

    /// <summary>How many dictionaries are defined.</summary>
    public int Count => _byId.Count;

    /// <summary>The dictionary <paramref name="id"/> for <paramref name="field"/>.</summary>
    /// <param name="allNull">The field's values are all null: a dictionary not defined yet reads as empty (the format allows a
    /// stream to send it after such a batch).</param>
    /// <exception cref="InvalidDataException">The dictionary is not defined (and the values are not all null).</exception>
    public IpcArray Get(long id, IpcField field, bool allNull, string where)
    {
        if (_byId.TryGetValue(id, out IpcArray? dictionary)) return dictionary;
        if (allNull) return IpcBatchDecoder.Empty(((DictionaryType)field.Field.DataType).ValueType);
        throw IpcValidation.Invalid(where, $"dictionary {id} is used before it is defined");
    }

    /// <summary>Whether dictionary <paramref name="id"/> is defined.</summary>
    public bool Contains(long id) => _byId.ContainsKey(id);

    /// <summary>Sets dictionary <paramref name="id"/> to <paramref name="values"/> (a replacement), holding its memory.</summary>
    public void Set(long id, IpcArray values)
    {
        values.Hold();
        if (_byId.Remove(id, out IpcArray? old)) old.Unhold();
        _byId[id] = values;
    }

    /// <summary>
    /// Appends <paramref name="delta"/> to dictionary <paramref name="id"/> (or defines it): the values are concatenated
    /// into memory of their own.
    /// </summary>
    public void Append(long id, IpcArray delta, string where)
    {
        if (!_byId.TryGetValue(id, out IpcArray? existing) || existing.Length == 0)
        {
            Set(id, delta);
            return;
        }
        if (delta.Length == 0) return;
        if ((long)existing.Length + delta.Length > int.MaxValue) throw IpcValidation.Invalid(where, "a dictionary grows past 2^31 values");
        using IArrowArray a = ArrowArrayFactory.BuildArray(existing.ToArrayData());
        using IArrowArray b = ArrowArrayFactory.BuildArray(delta.ToArrayData());
        IArrowArray joined;
        try
        {
            joined = ArrowArrayConcatenator.Concatenate([a, b], PooledArrowAllocator.Instance);
        }
        catch (Exception e) when (e is NotSupportedException or NotImplementedException or ArgumentException)
        {
            throw new InvalidDataException($"Arrow IPC delta dictionaries of type {delta.Type.Name} ({where}) are not supported: {e.Message}", e);
        }
        using (joined)
        {
            var memory = new IpcBodyMemory();
            IpcArray adopted = Adopt(joined.Data, memory);
            Set(id, adopted);
            memory.Sealed(); // the dictionary holds it now
        }
    }

    /// <summary>A copy of <paramref name="data"/> (offset 0) in <paramref name="memory"/>.</summary>
    private static unsafe IpcArray Adopt(ArrayData data, IpcBodyMemory memory)
    {
        if (data.Offset != 0) throw new InvalidOperationException("A concatenated dictionary has an offset.");
        var buffers = new IpcSlice[data.Buffers.Length];
        for (int i = 0; i < buffers.Length; i++)
        {
            ReadOnlySpan<byte> bytes = data.Buffers[i].Span;
            if (bytes.IsEmpty) continue;
            byte* p = memory.Allocate(bytes.Length);
            bytes.CopyTo(new Span<byte>(p, bytes.Length));
            buffers[i] = new IpcSlice(memory, (nint)p, bytes.Length);
        }
        IpcArray[] children = [.. (data.Children ?? []).Select(c => Adopt(c, memory))];
        IpcArray? dictionary = data.Dictionary is { } d ? Adopt(d, memory) : null;
        return new IpcArray(data.DataType, data.Length, data.GetNullCount(), buffers, children, dictionary);
    }

    public void Dispose()
    {
        foreach (IpcArray dictionary in _byId.Values) dictionary.Unhold();
        _byId.Clear();
    }
}
