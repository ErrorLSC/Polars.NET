using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Data.Building;

namespace Kaminari.Sazanami.Encodings;

/// <summary>
/// The distinct values of a fixed-width type in order of first appearance, and the index of each (dictionary
/// encoding). Values are equal when their bytes are: two NaNs of different payloads, or -0 and +0, are distinct.
/// The values and the hash table are in native memory, freed by <see cref="Dispose"/>.
/// </summary>
internal sealed class FixedValueDictionary<T> : IDisposable where T : unmanaged
{
    private readonly NativeArray<T> _values = new(64);
    private readonly NativeArray<int> _slots = new(); // index + 1 of the value in each slot, 0 when empty
    private int _slotCount; // a power of two (the array may hold more)
    private int _count;

    public FixedValueDictionary() => DictionarySlots.Empty(_slots, ref _slotCount, 128);

    public int Count => _count;

    /// <summary>The values, by index.</summary>
    public ReadOnlySpan<T> Values => _values.AsSpan(0, _count);

    /// <summary>The index of <paramref name="value"/>, added at the end when new.</summary>
    public int GetOrAdd(T value)
    {
        Span<int> slots = _slots.AsSpan(0, _slotCount);
        int mask = slots.Length - 1;
        for (int slot = (int)Hash(value) & mask; ; slot = (slot + 1) & mask)
        {
            int entry = slots[slot];
            if (entry == 0)
            {
                if (_count == _values.Capacity) _values.Grow(_count + 1, _count);
                _values.At(_count) = value;
                slots[slot] = ++_count;
                if (_count * 2 > slots.Length) Rehash();
                return _count - 1;
            }
            if (Same(_values.At(entry - 1), value)) return entry - 1;
        }
    }

    public void Clear()
    {
        _count = 0;
        _slots.AsSpan(0, _slotCount).Clear();
    }

    /// <summary>The native bytes it holds (values and table).</summary>
    public long ScratchBytes => (long)_values.Capacity * Unsafe.SizeOf<T>() + (long)_slots.Capacity * sizeof(int);

    /// <summary>Empties it and returns its memory but a small table (grown again as values come).</summary>
    public void Trim()
    {
        _values.Dispose();
        _count = 0;
        _slots.Dispose();
        DictionarySlots.Empty(_slots, ref _slotCount, 128);
    }

    public void Dispose()
    {
        _values.Dispose();
        _slots.Dispose();
        _count = _slotCount = 0;
    }

    private void Rehash()
    {
        DictionarySlots.Empty(_slots, ref _slotCount, _slotCount * 2);
        Span<int> slots = _slots.AsSpan(0, _slotCount);
        ReadOnlySpan<T> values = _values.AsSpan(0, _count);
        int mask = slots.Length - 1;
        for (int i = 0; i < values.Length; i++)
        {
            int slot = (int)Hash(values[i]) & mask;
            while (slots[slot] != 0) slot = (slot + 1) & mask;
            slots[slot] = i + 1;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool Same(T a, T b) => Unsafe.SizeOf<T>() switch
    {
        4 => Unsafe.As<T, uint>(ref a) == Unsafe.As<T, uint>(ref b),
        8 => Unsafe.As<T, ulong>(ref a) == Unsafe.As<T, ulong>(ref b),
        _ => MemoryMarshal.AsBytes(new ReadOnlySpan<T>(ref a)).SequenceEqual(MemoryMarshal.AsBytes(new ReadOnlySpan<T>(ref b))),
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint Hash(T value) => Unsafe.SizeOf<T>() switch
    {
        4 => ByteHash.Mix(Unsafe.As<T, uint>(ref value)),
        8 => ByteHash.Mix(Unsafe.As<T, ulong>(ref value)),
        _ => ByteHash.Of(MemoryMarshal.AsBytes(new ReadOnlySpan<T>(ref value))),
    };
}

/// <summary>
/// The distinct byte strings in order of first appearance, and the index of each (dictionary encoding of strings,
/// binary and fixed-length byte arrays). The strings are copied into an arena of their own; the arena, offsets, hashes
/// and table are in native memory, freed by <see cref="Dispose"/>.
/// </summary>
internal sealed class ByteArrayDictionary : IDisposable
{
    private readonly NativeArray<byte> _arena = new(4096);
    private int _arenaLength;
    private readonly NativeArray<int> _offsets = new(65); // _offsets[i]..[i+1] is value i
    private readonly NativeArray<uint> _hashes = new(64);
    private readonly NativeArray<int> _slots = new();
    private int _slotCount; // a power of two (the array may hold more)
    private int _count;

    public ByteArrayDictionary() => DictionarySlots.Empty(_slots, ref _slotCount, 128);

    public int Count => _count;

    /// <summary>The bytes of all values together.</summary>
    public long ByteSize => _arenaLength;

    /// <summary>The value at <paramref name="index"/>.</summary>
    public ReadOnlySpan<byte> this[int index]
    {
        get
        {
            int start = _offsets.At(index);
            return _arena.AsSpan(start, _offsets.At(index + 1) - start);
        }
    }

    /// <summary>The index of <paramref name="value"/>, added at the end when new.</summary>
    public int GetOrAdd(ReadOnlySpan<byte> value)
    {
        uint hash = ByteHash.Of(value);
        Span<int> slots = _slots.AsSpan(0, _slotCount);
        int mask = slots.Length - 1;
        for (int slot = (int)hash & mask; ; slot = (slot + 1) & mask)
        {
            int entry = slots[slot];
            if (entry == 0)
            {
                Append(value, hash);
                slots[slot] = _count;
                if (_count * 2 > slots.Length) Rehash();
                return _count - 1;
            }
            if (_hashes.At(entry - 1) == hash && this[entry - 1].SequenceEqual(value)) return entry - 1;
        }
    }

    public void Clear()
    {
        _count = 0;
        _arenaLength = 0;
        _slots.AsSpan(0, _slotCount).Clear();
    }

    /// <summary>The native bytes it holds (values, offsets, hashes and table).</summary>
    public long ScratchBytes => _arena.Capacity + 4L * _offsets.Capacity + 4L * _hashes.Capacity + 4L * _slots.Capacity;

    /// <summary>Empties it and returns its memory but a small table (grown again as values come).</summary>
    public void Trim()
    {
        _arena.Dispose();
        _hashes.Dispose();
        _offsets.Dispose();
        _offsets.Reserve(1); // the first offset is kept as values are added
        _slots.Dispose();
        DictionarySlots.Empty(_slots, ref _slotCount, 128);
        _count = _arenaLength = 0;
    }

    public void Dispose()
    {
        _arena.Dispose();
        _offsets.Dispose();
        _hashes.Dispose();
        _slots.Dispose();
        _count = _arenaLength = _slotCount = 0;
    }

    private void Append(ReadOnlySpan<byte> value, uint hash)
    {
        _arena.Grow(checked(_arenaLength + value.Length), _arenaLength);
        value.CopyTo(_arena.AsSpan(_arenaLength));
        if (_count == _hashes.Capacity) _hashes.Grow(_count + 1, _count);
        _offsets.Grow(_count + 2, _count + 1);
        _offsets.At(_count) = _arenaLength;
        _arenaLength += value.Length;
        _hashes.At(_count) = hash;
        _offsets.At(++_count) = _arenaLength;
    }

    private void Rehash()
    {
        DictionarySlots.Empty(_slots, ref _slotCount, _slotCount * 2);
        Span<int> slots = _slots.AsSpan(0, _slotCount);
        ReadOnlySpan<uint> hashes = _hashes.AsSpan(0, _count);
        int mask = slots.Length - 1;
        for (int i = 0; i < hashes.Length; i++)
        {
            int slot = (int)hashes[i] & mask;
            while (slots[slot] != 0) slot = (slot + 1) & mask;
            slots[slot] = i + 1;
        }
    }
}

/// <summary>The open-addressing tables of the dictionaries: slots holding index + 1 of an entry, 0 when empty.</summary>
internal static class DictionarySlots
{
    /// <summary>Gives the table <paramref name="count"/> (a power of two) empty slots: native memory is not zeroed, so they are cleared here.</summary>
    public static void Empty(NativeArray<int> slots, ref int slotCount, int count)
    {
        slots.Reserve(count);
        slots.AsSpan(0, count).Clear();
        slotCount = count;
    }
}

/// <summary>A fast non-cryptographic hash of bytes (multiply-rotate over 8-byte words, then a finalizer).</summary>
internal static class ByteHash
{
    private const ulong K = 0x9E3779B97F4A7C15UL;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Mix(ulong x)
    {
        x ^= x >> 33;
        x *= 0xFF51AFD7ED558CCDUL;
        x ^= x >> 33;
        x *= 0xC4CEB9FE1A85EC53UL;
        x ^= x >> 33;
        return (uint)x;
    }

    public static uint Of(ReadOnlySpan<byte> bytes)
    {
        ulong h = (ulong)bytes.Length * K;
        if (bytes.Length >= 8)
        {
            // Whole words, then the last 8 bytes (overlapping the words already taken: the length is in the hash).
            ReadOnlySpan<byte> last = bytes[^8..];
            while (bytes.Length > 8)
            {
                h = BitOperations.RotateLeft(h ^ BinaryPrimitives.ReadUInt64LittleEndian(bytes), 27) * K;
                bytes = bytes[8..];
            }
            return Mix(h ^ BinaryPrimitives.ReadUInt64LittleEndian(last));
        }
        ulong tail = 0;
        for (int i = 0; i < bytes.Length; i++) tail |= (ulong)bytes[i] << (8 * i);
        return Mix(h ^ tail);
    }
}
