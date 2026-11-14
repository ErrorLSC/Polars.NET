using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Kaminari.Sazanami.Encodings;

namespace Kaminari.Sazanami.Helpers;

/// <summary>
/// Which slots of a run are valid: all of them (the default: an empty mask), one byte per slot (zero = null), or the
/// bits of an LSB-first bitmap from a bit offset (Arrow validity and Parquet's bit-packed definition levels, which
/// can then be used as they come). Byte spans convert to it implicitly.
/// </summary>
internal readonly ref struct SlotMask
{
    private readonly ReadOnlySpan<byte> _data;  // the bytes, or the bitmap
    private readonly long _firstBit;
    private readonly int _length;
    private readonly bool _bits;                // false for bytes, so that the default is the empty mask

    private SlotMask(ReadOnlySpan<byte> data, long firstBit, int length, bool bits)
    {
        _data = data;
        _firstBit = firstBit;
        _length = length;
        _bits = bits;
    }

    /// <summary>A mask of one byte per slot (an empty span: every slot valid).</summary>
    public static SlotMask FromBytes(ReadOnlySpan<byte> bytes) => new(bytes, 0, bytes.Length, false);

    /// <summary><paramref name="length"/> slots, valid where their bits (from <paramref name="firstBit"/> of <paramref name="bitmap"/>) are set.</summary>
    public static SlotMask FromBits(ReadOnlySpan<byte> bitmap, long firstBit, int length)
    {
        if (firstBit < 0 || length < 0 || ((firstBit + length + 7) >> 3) > bitmap.Length) throw new ArgumentOutOfRangeException(nameof(length));
        return new(bitmap, firstBit, length, true);
    }

    public static implicit operator SlotMask(ReadOnlySpan<byte> bytes) => FromBytes(bytes);

    public static implicit operator SlotMask(Span<byte> bytes) => FromBytes(bytes);

    public static implicit operator SlotMask(byte[] bytes) => FromBytes(bytes);

    /// <summary>Whether every slot is valid (no mask at all).</summary>
    public bool IsEmpty => !_bits && _data.IsEmpty;

    /// <summary>Whether the mask is bits of a bitmap (else bytes).</summary>
    public bool IsBits => _bits;

    /// <summary>The number of slots.</summary>
    public int Length => _length;

    /// <summary>The bytes of a byte mask.</summary>
    public ReadOnlySpan<byte> Bytes => IsBits ? throw new InvalidOperationException("The mask is a bitmap.") : _data;

    /// <summary>The bitmap of a bit mask (slot 0 at <see cref="FirstBit"/>).</summary>
    public ReadOnlySpan<byte> Bitmap => IsBits ? _data : throw new InvalidOperationException("The mask is bytes.");

    public long FirstBit => _firstBit;

    /// <summary>Whether slot <paramref name="i"/> is valid.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsValid(int i) => IsBits ? Bits.Get(_data, _firstBit + i) : _data[i] != 0;

    /// <summary>The number of valid slots.</summary>
    public int CountValid() => IsBits ? (int)Bits.Count(_data, _firstBit, _length) : _length - _data.Count((byte)0);

    /// <summary>Slots [<paramref name="at"/>, +64) as a word, bit k set where slot at + k is valid.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong Word(int at) =>
        IsBits ? Bits.Word(_data, _firstBit + at) : MaskExpand.Word(ref Unsafe.Add(ref MemoryMarshal.GetReference(_data), at));

    /// <summary>Slots [<paramref name="at"/>, + <paramref name="count"/>) (fewer than 64) as a word.</summary>
    public ulong PartialWord(int at, int count) =>
        IsBits ? Bits.Word(_data, _firstBit + at) & ((1UL << count) - 1) : MaskExpand.PartialWord(ref Unsafe.Add(ref MemoryMarshal.GetReference(_data), at), count);

    /// <summary>
    /// The mask as one byte per slot (1 = valid): its own bytes, or the bits unpacked into <paramref name="scratch"/>
    /// (at least <see cref="Length"/> long).
    /// </summary>
    public ReadOnlySpan<byte> ToBytes(Span<byte> scratch)
    {
        if (!IsBits) return _data;
        Span<byte> bytes = scratch[.._length];
        BitUnpacking.Unpack(_data, _firstBit, 1, bytes);
        return bytes;
    }
}
