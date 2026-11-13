using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Kaminari.Seseragi.Files.Parquet.Encodings;

/// <summary>Unpacking of bit-packed values (Encodings.md): LSB first for the hybrid, MSB first for the deprecated BIT_PACKED.</summary>
internal static class BitPacking
{
    /// <summary>
    /// Unpacks <paramref name="destination"/>.Length values of <paramref name="bitWidth"/> bits (at most 32), the
    /// first at value index <paramref name="first"/> of <paramref name="packed"/>, packed least significant bit first.
    /// </summary>
    public static void Unpack<T>(ReadOnlySpan<byte> packed, long first, int bitWidth, Span<T> destination) where T : unmanaged, IBinaryInteger<T>
    {
        if (bitWidth == 0)
        {
            destination.Clear();
            return;
        }
        ulong mask = (1UL << bitWidth) - 1;
        long bit = first * bitWidth;
        for (int i = 0; i < destination.Length; i++, bit += bitWidth)
        {
            int at = (int)(bit >> 3);
            ulong word = at + 8 <= packed.Length ? BinaryPrimitives.ReadUInt64LittleEndian(packed[at..]) : Tail(packed, at);
            destination[i] = T.CreateTruncating((word >> (int)(bit & 7)) & mask);
        }
    }

    /// <summary>The bytes from <paramref name="at"/> on, little-endian, when fewer than 8 remain.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static ulong Tail(ReadOnlySpan<byte> packed, int at)
    {
        ulong word = 0;
        for (int k = 0; at + k < packed.Length && k < 8; k++) word |= (ulong)packed[at + k] << (8 * k);
        return word;
    }

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

    /// <summary>The bytes <paramref name="count"/> values of <paramref name="bitWidth"/> bits take.</summary>
    public static long Bytes(long count, int bitWidth) => (count * bitWidth + 7) >> 3;
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
    private ReadOnlySpan<byte> _packed; // the current bit-packed run
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
                BitPacking.Unpack(_packed, _packedIndex, _bitWidth, destination.Slice(done, n));
                _packedIndex += n;
                _packedLeft -= n;
                done += n;
            }
            else if (!NextRun()) break;
        }
        return done;
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
            long bytes = BitPacking.Bytes((long)groups * 8, _bitWidth);
            int available = (int)Math.Min(bytes, _data.Length - _position);
            _packed = _data.Slice(_position, available);
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
