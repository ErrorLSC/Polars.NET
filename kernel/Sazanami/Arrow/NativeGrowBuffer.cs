using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Kaminari.Sazanami.Helpers;

namespace Kaminari.Sazanami.Arrow;

/// <summary>
/// A growable, 64-byte aligned buffer in native memory (from <see cref="NativeMemoryPool"/>). Column builders append
/// into it and hand the finished allocation to an exported Arrow array without copying (<see cref="Detach"/>).
/// </summary>
internal unsafe struct NativeGrowBuffer
{
    internal const int Alignment = NativeMemoryPool.Alignment;

    private byte* _ptr;
    private long _length;
    private long _capacity;
    private long _hint; // size of the last detached buffer: the next one starts there instead of doubling up again

    public readonly long Length => _length;
    public readonly byte* Pointer => _ptr;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte* Reserve(long bytes)
    {
        long needed = _length + bytes;
        if (needed > _capacity) Grow(needed);
        byte* at = _ptr + _length;
        _length = needed;
        return at;
    }

    /// <summary>Makes room for <paramref name="bytes"/> more bytes at once, so the appends that follow do not regrow.</summary>
    public void EnsureAdditional(long bytes)
    {
        long needed = _length + bytes;
        if (needed > _capacity) Grow(needed);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append<T>(T value) where T : unmanaged
    {
        long needed = _length + sizeof(T);
        if (needed > _capacity) Grow(needed);
        Unsafe.WriteUnaligned(_ptr + _length, value);
        _length = needed;
    }

    public void Append(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty) return;
        byte* at = Reserve(bytes.Length);
        bytes.CopyTo(new Span<byte>(at, bytes.Length));
    }

    /// <summary>Appends <paramref name="bytes"/> zero bytes.</summary>
    public void AppendZeros(long bytes)
    {
        if (bytes == 0) return;
        byte* at = Reserve(bytes);
        NativeMemory.Clear(at, (nuint)bytes);
    }

    /// <summary>Shrinks or grows the logical length; new bytes are zeroed.</summary>
    public void SetLength(long length)
    {
        if (length > _length) AppendZeros(length - _length);
        else _length = length;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Grow(long needed)
    {
        long capacity = Math.Max(Math.Max(needed, _capacity * 2), Math.Max(_hint, Alignment));
        byte* next = NativeMemoryPool.Alloc(capacity);
        if (_ptr != null)
        {
            Buffer.MemoryCopy(_ptr, next, capacity, _length);
            NativeMemoryPool.Free(_ptr);
        }
        _ptr = next;
        _capacity = NativeMemoryPool.UsableSize(next);
    }

    /// <summary>
    /// Hands over the allocation (never null: an empty buffer still owns a small block, since consumers may
    /// dereference it) and resets this builder to empty. The caller frees the pointer with
    /// <see cref="NativeMemoryPool.Free"/>.
    /// </summary>
    public byte* Detach() => Detach(out _);

    /// <inheritdoc cref="Detach()"/>
    /// <param name="length">The number of bytes written into the allocation.</param>
    public byte* Detach(out long length)
    {
        length = _length;
        if (_ptr == null) Grow(Alignment);
        byte* p = _ptr;
        // Batches of one column tend to have similar sizes; leave a little headroom so the next fill rarely reallocates.
        _hint = _length + (_length >> 3);
        _ptr = null;
        _length = 0;
        _capacity = 0;
        return p;
    }

    public void Clear() => _length = 0;

    public void Free()
    {
        NativeMemoryPool.Free(_ptr);
        _ptr = null;
        _length = 0;
        _capacity = 0;
    }
}

/// <summary>A validity (or boolean value) bitmap in native memory, LSB bit order as Arrow requires.</summary>
internal unsafe struct NativeBitmap
{
    private NativeGrowBuffer _bytes;
    private long _bitLength;
    private long _unsetCount;

    public readonly long Length => _bitLength;
    public readonly long UnsetCount => _unsetCount;

    /// <summary>Makes room for <paramref name="bits"/> more bits at once.</summary>
    public void EnsureAdditional(long bits) => _bytes.EnsureAdditional((bits + 8) >> 3);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(bool set)
    {
        long bit = _bitLength;
        if ((bit & 7) == 0) _bytes.Append<byte>(0);
        if (set) _bytes.Pointer[bit >> 3] |= (byte)(1 << (int)(bit & 7));
        else _unsetCount++;
        _bitLength = bit + 1;
    }

    public void AppendMany(bool set, long count)
    {
        if (count <= 0) return;
        // Finish the partial byte bit by bit, then fill whole bytes at once.
        while ((_bitLength & 7) != 0 && count > 0)
        {
            Append(set);
            count--;
        }
        long bytes = count >> 3;
        if (bytes > 0)
        {
            byte* at = _bytes.Reserve(bytes);
            new Span<byte>(at, checked((int)bytes)).Fill(set ? (byte)0xFF : (byte)0);
            _bitLength += bytes << 3;
            if (!set) _unsetCount += bytes << 3;
        }
        for (long i = 0; i < (count & 7); i++) Append(set);
    }

    /// <summary>Appends one bit per byte of <paramref name="mask"/>: set where the byte is not zero.</summary>
    public void AppendMask(ReadOnlySpan<byte> mask)
    {
        int i = 0;
        while ((_bitLength & 7) != 0 && i < mask.Length) Append(mask[i++] != 0);
        int whole = (mask.Length - i) >> 3;
        if (whole > 0)
        {
            byte* at = _bytes.Reserve(whole);
            long set = 0;
            int b = 0;
            fixed (byte* source = mask)
            {
                byte* p = source + i;
                // 32 or 16 mask bytes → 4 or 2 bitmap bytes: compare with zero, take the lanes' top bits.
                if (Vector256.IsHardwareAccelerated)
                {
                    for (; b + 4 <= whole; b += 4, p += 32)
                    {
                        uint bits = ~Vector256.ExtractMostSignificantBits(Vector256.Equals(Vector256.Load(p), Vector256<byte>.Zero));
                        Unsafe.WriteUnaligned(at + b, bits);
                        set += System.Numerics.BitOperations.PopCount(bits);
                    }
                }
                if (Vector128.IsHardwareAccelerated)
                {
                    for (; b + 2 <= whole; b += 2, p += 16)
                    {
                        uint bits = ~Vector128.ExtractMostSignificantBits(Vector128.Equals(Vector128.Load(p), Vector128<byte>.Zero)) & 0xFFFF;
                        Unsafe.WriteUnaligned(at + b, (ushort)bits);
                        set += System.Numerics.BitOperations.PopCount(bits);
                    }
                }
                for (; b < whole; b++, p += 8)
                {
                    int bits = 0;
                    for (int k = 0; k < 8; k++) bits |= (p[k] != 0 ? 1 : 0) << k;
                    at[b] = (byte)bits;
                    set += System.Numerics.BitOperations.PopCount((uint)bits);
                }
            }
            i += whole << 3;
            _bitLength += (long)whole << 3;
            _unsetCount += ((long)whole << 3) - set;
        }
        while (i < mask.Length) Append(mask[i++] != 0);
    }

    /// <summary>
    /// Appends <paramref name="count"/> bits of an LSB-first bitmap (as Arrow and Parquet's PLAIN booleans store them),
    /// starting at bit <paramref name="firstBit"/> of <paramref name="source"/>.
    /// </summary>
    public void AppendBits(ReadOnlySpan<byte> source, long firstBit, long count)
    {
        if (count <= 0) return;
        if ((firstBit + count + 7) >> 3 > source.Length) throw new ArgumentOutOfRangeException(nameof(count));
        // Up to a byte boundary of this bitmap bit by bit, then a byte at a time, then the rest.
        while ((_bitLength & 7) != 0 && count > 0)
        {
            Append((source[(int)(firstBit >> 3)] >> (int)(firstBit & 7) & 1) != 0);
            firstBit++;
            count--;
        }
        long whole = count >> 3;
        if (whole > 0)
        {
            byte* at = _bytes.Reserve(whole);
            int shift = (int)(firstBit & 7);
            ReadOnlySpan<byte> from = source[(int)(firstBit >> 3)..];
            if (shift == 0) from[..(int)whole].CopyTo(new Span<byte>(at, (int)whole));
            else
            {
                // Each output byte spans two input bytes; the last one may have no second byte in range.
                for (long b = 0; b < whole; b++)
                {
                    int lo = from[(int)b] >> shift;
                    int hi = b + 1 < from.Length ? from[(int)b + 1] << (8 - shift) : 0;
                    at[b] = (byte)(lo | hi);
                }
            }
            long set = 0;
            var bytes = new ReadOnlySpan<byte>(at, (int)whole);
            int k = 0;
            for (; k + 8 <= bytes.Length; k += 8) set += System.Numerics.BitOperations.PopCount(Unsafe.ReadUnaligned<ulong>(at + k));
            for (; k < bytes.Length; k++) set += System.Numerics.BitOperations.PopCount((uint)bytes[k]);
            _bitLength += whole << 3;
            _unsetCount += (whole << 3) - set;
            firstBit += whole << 3;
            count -= whole << 3;
        }
        for (; count > 0; count--, firstBit++) Append((source[(int)(firstBit >> 3)] >> (int)(firstBit & 7) & 1) != 0);
    }

    public byte* Detach() => Detach(out _);

    /// <inheritdoc cref="Detach()"/>
    /// <param name="length">The number of bytes of the bitmap.</param>
    public byte* Detach(out long length)
    {
        _bitLength = 0;
        _unsetCount = 0;
        return _bytes.Detach(out length);
    }

    public void Free()
    {
        _bytes.Free();
        _bitLength = 0;
        _unsetCount = 0;
    }
}
