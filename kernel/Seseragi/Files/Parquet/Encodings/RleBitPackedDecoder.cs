using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using Kaminari.Sazanami.Encodings;
using Kaminari.Sazanami.Helpers;

namespace Kaminari.Seseragi.Files.Parquet.Encodings;

/// <summary>The deprecated BIT_PACKED encoding of levels (Encodings.md): values packed most significant bit first.</summary>
internal static class BitPacking
{
    /// <summary>Unpacks the deprecated BIT_PACKED encoding (levels only): values packed most significant bit first.</summary>
    public static void UnpackMsbFirst(ReadOnlySpan<byte> packed, long first, int bitWidth, Span<byte> destination)
    {
        long bit = first * bitWidth;
        for (int i = 0; i < destination.Length; i++)
        {
            int value = 0;
            for (int k = 0; k < bitWidth; k++, bit++)
                value = (value << 1) | ((packed[(int)(bit >> 3)] >> (7 - (int)(bit & 7))) & 1);
            destination[i] = (byte)value;
        }
    }
}

/// <summary>
/// The RLE / bit-packing hybrid (Encodings.md): runs of one repeated value, or bit-packed groups of 8 values. Used
/// for levels, dictionary indices and booleans. Reads stop at the end of the data; a last bit-packed run may be cut
/// short (writers pad it to 8 values but some write only the bytes the values need).
/// </summary>
internal ref struct RleBitPackedDecoder
{
    private readonly ReadOnlySpan<byte> _data;
    private readonly int _bitWidth;
    private readonly int _valueBytes;
    private int _position;

    private int _repeatLeft;
    private uint _repeatValue;
    private ReadOnlySpan<byte> _packed; // the data from the current bit-packed run on
    private int _packedIndex;
    private int _packedLeft;

    /// <exception cref="ParquetFormatException">The bit width is above 32.</exception>
    public RleBitPackedDecoder(ReadOnlySpan<byte> data, int bitWidth)
    {
        if ((uint)bitWidth > 32) throw new ParquetFormatException($"An RLE / bit-packed run has bit width {bitWidth} (at most 32).");
        _data = data;
        _bitWidth = bitWidth;
        _valueBytes = (bitWidth + 7) >> 3;
    }

    /// <summary>Fills <paramref name="destination"/>; returns how many values were there (fewer only at the end of the data).</summary>
    public int Read<T>(scoped Span<T> destination) where T : unmanaged, IBinaryInteger<T>
    {
        int done = 0;
        while (done < destination.Length)
        {
            if (_repeatLeft > 0)
            {
                int n = Math.Min(_repeatLeft, destination.Length - done);
                destination.Slice(done, n).Fill(T.CreateTruncating(_repeatValue));
                _repeatLeft -= n;
                done += n;
            }
            else if (_packedLeft > 0)
            {
                int n = Math.Min(_packedLeft, destination.Length - done);
                BitUnpacking.Unpack(_packed, _packedIndex, _bitWidth, destination.Slice(done, n));
                _packedIndex += n;
                _packedLeft -= n;
                done += n;
            }
            else if (!NextRun()) break;
        }
        return done;
    }

    /// <summary>
    /// For a bit width of 1: writes up to <paramref name="count"/> values as bits of an LSB-first bitmap from bit
    /// <paramref name="firstBit"/> of <paramref name="bitmap"/> (an RLE run fills its range; a bit-packed run is
    /// already such bits and is copied); returns how many there were.
    /// </summary>
    public int ReadBits(Span<byte> bitmap, long firstBit, int count)
    {
        if (_bitWidth != 1) throw new InvalidOperationException("Values are read as bits only at bit width 1.");
        int done = 0;
        while (done < count)
        {
            if (_repeatLeft > 0)
            {
                int n = Math.Min(_repeatLeft, count - done);
                Bits.Fill(bitmap, firstBit + done, n, _repeatValue != 0);
                _repeatLeft -= n;
                done += n;
            }
            else if (_packedLeft > 0)
            {
                int n = Math.Min(_packedLeft, count - done);
                Bits.Copy(_packed, _packedIndex, bitmap, firstBit + done, n);
                _packedIndex += n;
                _packedLeft -= n;
                done += n;
            }
            else if (!NextRun()) break;
        }
        return done;
    }

    /// <summary>
    /// Decodes up to <paramref name="count"/> values of bit width 1 from the start of <paramref name="data"/> into an
    /// LSB-first bitmap from bit 0 of <paramref name="output"/> (what a fresh decoder's <see cref="ReadBits"/> would
    /// write); returns how many there were and, in <paramref name="set"/>, how many of them are 1. One pass with all
    /// state in locals: runs are gathered in a register and written a word at a time; the last bits are merged,
    /// leaving the bits after them.
    /// </summary>
    /// <exception cref="ParquetFormatException">A run header or an RLE value is damaged.</exception>
    public static int ReadAllBits(ReadOnlySpan<byte> data, Span<byte> output, int count, out long set)
    {
        if (count < 0 || (count + 7L) >> 3 > output.Length) throw new ArgumentOutOfRangeException(nameof(count));
        // Locals only (none passed by reference outside inlined code), so they stay in registers.
        int position = 0, done = 0, at = 0, heldCount = 0;
        ulong held = 0;     // bits gathered, not yet written (fewer than 64)
        long ones = 0;
        while (done < count && position < data.Length)
        {
            ulong header = data[position++];
            if (header >= 0x80) (header, position) = VarintRest(data, position, header);
            if ((header & 1) == 0)
            {
                ulong length = header >> 1;
                if (length > int.MaxValue) throw new ParquetFormatException("An RLE run is longer than 2^31 - 1 values.");
                if (position >= data.Length) throw new ParquetFormatException("An RLE run is cut short.");
                byte value = data[position++];
                if (value > 1) throw new ParquetFormatException($"An RLE run repeats {value}, wider than 1 bits.");
                int n = (int)Math.Min(length, (ulong)(count - done));
                done += n;
                if (value != 0) ones += n;
                ulong fill = value != 0 ? ulong.MaxValue : 0;
                while (n > 0)
                {
                    int k = Math.Min(n, 64);
                    Emit(output, fill >> (64 - k), k, ref held, ref heldCount, ref at);
                    n -= k;
                }
            }
            else
            {
                ulong groups = header >> 1;
                if (groups > int.MaxValue / 8) throw new ParquetFormatException("A bit-packed run is longer than 2^31 - 1 values.");
                // Eight values a byte; a cut-short last run holds what its bytes hold.
                int available = (int)Math.Min(groups, (ulong)(data.Length - position));
                int n = Math.Min(available * 8, count - done);
                done += n;
                for (int from = position; n > 0; from += 8)
                {
                    int k = Math.Min(n, 64);
                    ulong bits = from + 8 <= data.Length ? BinaryPrimitives.ReadUInt64LittleEndian(data[from..]) : Partial(data[from..]);
                    if (k < 64) bits &= (1UL << k) - 1;
                    ones += BitOperations.PopCount(bits);
                    Emit(output, bits, k, ref held, ref heldCount, ref at);
                    n -= k;
                }
                position += available;
            }
        }
        if (heldCount > 0) WriteLast(output[at..], held, heldCount);
        set = ones;
        return done;

        // Adds k bits (k ≤ 64, nothing above them) after the held ones; 64 complete bits are written (within the
        // output: they were all read, so at most count of them).
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static void Emit(Span<byte> output, ulong bits, int k, ref ulong held, ref int heldCount, ref int at)
        {
            held |= bits << heldCount;
            heldCount += k;
            if (heldCount < 64) return;
            BinaryPrimitives.WriteUInt64LittleEndian(output[at..], held);
            at += 8;
            heldCount -= 64;
            held = heldCount == 0 ? 0 : bits >> (k - heldCount);
        }

        // The last bits (fewer than 64): whole bytes, then the bits of a partial byte merged with the ones after them.
        // (No stackalloc in this method: it would keep the JIT from optimizing the loop above in tiers.)
        static void WriteLast(Span<byte> output, ulong held, int count)
        {
            int bytes = count >> 3;
            for (int i = 0; i < bytes; i++) output[i] = (byte)(held >> (8 * i));
            int rest = count & 7;
            if (rest == 0) return;
            int keep = (1 << rest) - 1;
            output[bytes] = (byte)((output[bytes] & ~keep) | ((int)(held >> (8 * bytes)) & keep));
        }

        static ulong Partial(ReadOnlySpan<byte> rest)
        {
            ulong bits = 0;
            for (int i = 0; i < rest.Length && i < 8; i++) bits |= (ulong)rest[i] << (8 * i);
            return bits;
        }

        // The rest of a varint whose first byte (already read) has its high bit set; with the position after it.
        static (ulong Value, int Position) VarintRest(ReadOnlySpan<byte> data, int position, ulong first)
        {
            ulong result = first & 0x7F;
            for (int shift = 7; shift < 64; shift += 7)
            {
                if (position >= data.Length) throw new ParquetFormatException("An RLE / bit-packed run header is cut short.");
                byte b = data[position++];
                result |= (ulong)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) return (result, position);
            }
            throw new ParquetFormatException("An RLE / bit-packed run header is too long.");
        }
    }

    /// <summary>Skips up to <paramref name="count"/> values; returns how many there were.</summary>
    public int Skip(int count)
    {
        int done = 0;
        while (done < count)
        {
            if (_repeatLeft > 0)
            {
                int n = Math.Min(_repeatLeft, count - done);
                _repeatLeft -= n;
                done += n;
            }
            else if (_packedLeft > 0)
            {
                int n = Math.Min(_packedLeft, count - done);
                _packedIndex += n;
                _packedLeft -= n;
                done += n;
            }
            else if (!NextRun()) break;
        }
        return done;
    }

    private bool NextRun()
    {
        if (_position >= _data.Length) return false;
        ulong header = ReadVarint();
        if ((header & 1) == 0)
        {
            ulong count = header >> 1;
            if (count > int.MaxValue) throw new ParquetFormatException("An RLE run is longer than 2^31 - 1 values.");
            if (_data.Length - _position < _valueBytes) throw new ParquetFormatException("An RLE run is cut short.");
            uint value = 0;
            for (int k = 0; k < _valueBytes; k++) value |= (uint)_data[_position + k] << (8 * k);
            _position += _valueBytes;
            if (_bitWidth < 32 && value >> _bitWidth != 0) throw new ParquetFormatException($"An RLE run repeats {value}, wider than {_bitWidth} bits.");
            _repeatValue = value;
            _repeatLeft = (int)count;
        }
        else
        {
            ulong groups = header >> 1;
            if (groups > int.MaxValue / 8) throw new ParquetFormatException("A bit-packed run is longer than 2^31 - 1 values.");
            long bytes = BitUnpacking.Bytes((long)groups * 8, _bitWidth);
            int available = (int)Math.Min(bytes, _data.Length - _position);
            // The run reaches to the end of the data (_packedLeft bounds what is read): the unpacker's fast path
            // needs 8 readable bytes past a group, which a short run alone never has.
            _packed = _data[_position..];
            _position += available;
            _packedIndex = 0;
            // A cut-short last run holds what its bytes hold.
            _packedLeft = available == bytes ? (int)groups * 8 : _bitWidth == 0 ? (int)groups * 8 : (int)((long)available * 8 / _bitWidth);
        }
        return true;
    }

    private ulong ReadVarint()
    {
        ulong result = 0;
        for (int shift = 0; shift < 64; shift += 7)
        {
            if (_position >= _data.Length) throw new ParquetFormatException("An RLE / bit-packed run header is cut short.");
            byte b = _data[_position++];
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
        }
        throw new ParquetFormatException("An RLE / bit-packed run header is too long.");
    }
}
