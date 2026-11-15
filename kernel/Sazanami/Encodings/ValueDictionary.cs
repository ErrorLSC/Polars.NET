using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Kaminari.Sazanami.Encodings;

/// <summary>
/// The distinct values of a fixed-width type in order of first appearance, and the index of each (dictionary
/// encoding). Values are equal when their bytes are: two NaNs of different payloads, or -0 and +0, are distinct.
/// </summary>
internal sealed class FixedValueDictionary<T> where T : unmanaged
{
    private T[] _values = new T[64];
    private int[] _slots = new int[128]; // index + 1 of the value in each slot, 0 when empty
    private int _count;

    public int Count => _count;

    /// <summary>The values, by index.</summary>
    public ReadOnlySpan<T> Values => _values.AsSpan(0, _count);

    /// <summary>The index of <paramref name="value"/>, added at the end when new.</summary>
    public int GetOrAdd(T value)
    {
        int mask = _slots.Length - 1;
        for (int slot = (int)Hash(value) & mask; ; slot = (slot + 1) & mask)
        {
            int entry = _slots[slot];
            if (entry == 0)
            {
                if (_count == _values.Length) Array.Resize(ref _values, _values.Length * 2);
                _values[_count] = value;
                _slots[slot] = ++_count;
                if (_count * 2 > _slots.Length) Rehash();
                return _count - 1;
            }
            if (Same(_values[entry - 1], value)) return entry - 1;
        }
    }

    public void Clear()
    {
        _count = 0;
        Array.Clear(_slots);
    }

    private void Rehash()
    {
        _slots = new int[_slots.Length * 2];
        int mask = _slots.Length - 1;
        for (int i = 0; i < _count; i++)
        {
            int slot = (int)Hash(_values[i]) & mask;
            while (_slots[slot] != 0) slot = (slot + 1) & mask;
            _slots[slot] = i + 1;
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
/// binary and fixed-length byte arrays). The strings are copied into an arena of their own.
/// </summary>
internal sealed class ByteArrayDictionary
{
    private byte[] _arena = new byte[4096];
    private int _arenaLength;
    private int[] _offsets = new int[65]; // _offsets[i]..[i+1] is value i
    private uint[] _hashes = new uint[64];
    private int[] _slots = new int[128];
    private int _count;

    public int Count => _count;

    /// <summary>The bytes of all values together.</summary>
    public long ByteSize => _arenaLength;

    /// <summary>The value at <paramref name="index"/>.</summary>
    public ReadOnlySpan<byte> this[int index] => _arena.AsSpan(_offsets[index], _offsets[index + 1] - _offsets[index]);

    /// <summary>The index of <paramref name="value"/>, added at the end when new.</summary>
    public int GetOrAdd(ReadOnlySpan<byte> value)
    {
        uint hash = ByteHash.Of(value);
        int mask = _slots.Length - 1;
        for (int slot = (int)hash & mask; ; slot = (slot + 1) & mask)
        {
            int entry = _slots[slot];
            if (entry == 0)
            {
                Append(value, hash);
                _slots[slot] = _count;
                if (_count * 2 > _slots.Length) Rehash();
                return _count - 1;
            }
            if (_hashes[entry - 1] == hash && this[entry - 1].SequenceEqual(value)) return entry - 1;
        }
    }

    public void Clear()
    {
        _count = 0;
        _arenaLength = 0;
        Array.Clear(_slots);
    }

    private void Append(ReadOnlySpan<byte> value, uint hash)
    {
        if (_arenaLength + value.Length > _arena.Length)
            Array.Resize(ref _arena, (int)Math.Min(Array.MaxLength, Math.Max(_arena.Length * 2L, (long)_arenaLength + value.Length)));
        value.CopyTo(_arena.AsSpan(_arenaLength));
        if (_count == _hashes.Length)
        {
            Array.Resize(ref _hashes, _hashes.Length * 2);
            Array.Resize(ref _offsets, _hashes.Length + 1);
        }
        _offsets[_count] = _arenaLength;
        _arenaLength += value.Length;
        _hashes[_count] = hash;
        _offsets[++_count] = _arenaLength;
    }

    private void Rehash()
    {
        _slots = new int[_slots.Length * 2];
        int mask = _slots.Length - 1;
        for (int i = 0; i < _count; i++)
        {
            int slot = (int)_hashes[i] & mask;
            while (_slots[slot] != 0) slot = (slot + 1) & mask;
            _slots[slot] = i + 1;
        }
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
        while (bytes.Length >= 8)
        {
            h = BitOperations.RotateLeft(h ^ BinaryPrimitives.ReadUInt64LittleEndian(bytes), 27) * K;
            bytes = bytes[8..];
        }
        ulong tail = 0;
        for (int i = 0; i < bytes.Length; i++) tail |= (ulong)bytes[i] << (8 * i);
        return Mix(h ^ tail);
    }
}
