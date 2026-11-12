using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Kaminari.Sazanami.Helpers;

namespace Kaminari.Seseragi.Files.Avro;

/// <summary>The input is not valid Avro (a corrupt or truncated file, or a value outside the schema).</summary>
public sealed class AvroFormatException(string message, Exception? inner = null) : FormatException(message, inner);

/// <summary>Reads the Avro binary encoding from a span. All reads are bounds-checked; malformed input throws
/// <see cref="AvroFormatException"/>.</summary>
internal ref struct AvroSpanReader(ReadOnlySpan<byte> data)
{
    private readonly ReadOnlySpan<byte> _data = data;
    private int _position;

    public readonly int Position => _position;
    public readonly int Remaining => _data.Length - _position;
    public readonly bool IsAtEnd => _position >= _data.Length;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long ReadLong()
    {
        // Fast path: one-byte varints are the common case for lengths, counts and branch indexes.
        if ((uint)_position < (uint)_data.Length)
        {
            byte b = _data[_position];
            if (b < 0x80)
            {
                _position++;
                return (long)((ulong)b >> 1) ^ -(long)(b & 1);
            }
        }
        return ReadLongSlow();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private long ReadLongSlow()
    {
        // Multi-byte varints up to 8 bytes (values below 2^55 after zigzag, e.g. microsecond timestamps) decode
        // branch-free from one 8-byte load: find the terminating byte, mask it off, compact the 7-bit groups.
        if (_data.Length - _position >= 8)
        {
            ulong word = BinaryPrimitives.ReadUInt64LittleEndian(_data[_position..]);
            ulong stops = ~word & 0x8080808080808080UL;
            if (stops != 0)
            {
                int length = (System.Numerics.BitOperations.TrailingZeroCount(stops) >> 3) + 1;
                _position += length;
                ulong raw = length == 8 ? word : word & ((1UL << (length * 8)) - 1);
                ulong value = CompactSevenBitGroups(raw);
                return (long)(value >> 1) ^ -(long)(value & 1);
            }
        }
        return ReadLongBytewise();
    }

    /// <summary>Packs the low 7 bits of each of 8 little-endian bytes into 56 contiguous bits (SWAR).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ulong CompactSevenBitGroups(ulong x)
    {
        x &= 0x7F7F7F7F7F7F7F7FUL;
        x = (x & 0x007F007F007F007FUL) | ((x & 0x7F007F007F007F00UL) >> 1);
        x = (x & 0x00003FFF00003FFFUL) | ((x & 0x3FFF00003FFF0000UL) >> 2);
        return (x & 0x000000000FFFFFFFUL) | ((x & 0x0FFFFFFF00000000UL) >> 4);
    }

    private long ReadLongBytewise()
    {
        ulong value = 0;
        int shift = 0;
        while (true)
        {
            if ((uint)_position >= (uint)_data.Length) throw Truncated();
            byte b = _data[_position++];
            value |= (ulong)(b & 0x7F) << shift;
            if (b < 0x80) break;
            shift += 7;
            if (shift > 63) throw new AvroFormatException("Avro varint is longer than 10 bytes.");
        }
        return (long)(value >> 1) ^ -(long)(value & 1);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ReadInt()
    {
        long v = ReadLong();
        if (v != (int)v) throw new AvroFormatException($"Avro int value {v} is out of the 32-bit range.");
        return (int)v;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ReadBoolean()
    {
        if ((uint)_position >= (uint)_data.Length) throw Truncated();
        return _data[_position++] switch
        {
            0 => false,
            1 => true,
            var b => throw new AvroFormatException($"Invalid Avro boolean byte 0x{b:X2}."),
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public float ReadFloat() => BinaryPrimitives.ReadSingleLittleEndian(ReadFixed(4));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public double ReadDouble() => BinaryPrimitives.ReadDoubleLittleEndian(ReadFixed(8));

    /// <summary>Reads a length-prefixed <c>bytes</c> or <c>string</c> value without copying.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> ReadBytes()
    {
        long length = ReadLong();
        if ((ulong)length > (ulong)Remaining)
            throw length < 0
                ? new AvroFormatException($"Negative Avro bytes length {length}.")
                : Truncated();
        var span = _data.Slice(_position, (int)length);
        _position += (int)length;
        return span;
    }

    /// <summary>Reads a <c>string</c> value and checks that it is well-formed UTF-8.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> ReadUtf8()
    {
        var bytes = ReadBytes();
        if (!System.Text.Unicode.Utf8.IsValid(bytes)) ThrowInvalidUtf8();
        return bytes;
    }

    private static void ThrowInvalidUtf8() => throw new AvroFormatException("An Avro string is not valid UTF-8.");

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> ReadFixed(int size)
    {
        if ((uint)size > (uint)Remaining) throw Truncated();
        var span = _data.Slice(_position, size);
        _position += size;
        return span;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Skip(long bytes)
    {
        if ((ulong)bytes > (ulong)Remaining)
            throw bytes < 0 ? new AvroFormatException($"Negative Avro skip length {bytes}.") : Truncated();
        _position += (int)bytes;
    }

    public void SkipBytes() => Skip(ReadLong());

    /// <summary>Reads an array or map block count. A negative count is followed by the block's byte size,
    /// which is consumed here; the absolute count is returned.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long ReadBlockCount()
    {
        long count = ReadLong();
        if (count < 0)
        {
            if (count == long.MinValue) throw new AvroFormatException("Invalid Avro block count.");
            long size = ReadLong();
            if (size < 0) throw new AvroFormatException($"Negative Avro block size {size}.");
            count = -count;
        }
        return count;
    }

    /// <summary>Like <see cref="ReadBlockCount"/>, but when the writer recorded the block's byte size the
    /// whole block is skipped at once and 0 items are returned for it (<paramref name="skipped"/> is true).</summary>
    public long ReadBlockCountForSkip(out bool skipped)
    {
        long count = ReadLong();
        skipped = false;
        if (count < 0)
        {
            if (count == long.MinValue) throw new AvroFormatException("Invalid Avro block count.");
            Skip(ReadLong());
            skipped = true;
            return 0;
        }
        return count;
    }

    internal static AvroFormatException Truncated() => new("Unexpected end of Avro data.");
}

/// <summary>Encodes Avro binary values into a growable native buffer.</summary>
internal sealed class AvroBufferWriter(int initialCapacity = 64 * 1024) : IDisposable
{
    private readonly NativeByteBuffer _buffer = new(initialCapacity);
    private int _length;

    public int Length => _length;
    public ReadOnlySpan<byte> WrittenSpan => _buffer.Span[.._length];
    public ReadOnlyMemory<byte> WrittenMemory => _buffer.Memory[.._length];

    public void Clear() => _length = 0;

    /// <summary>Truncates back to an earlier length (used to undo a partly written row).</summary>
    public void Truncate(int length) => _length = length;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Span<byte> GetSpan(int size)
    {
        if (_buffer.Capacity - _length < size) Grow(size);
        return _buffer.Span[_length..];
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Grow(int size)
    {
        if ((long)_length + size > Array.MaxLength) throw new InvalidOperationException("An Avro block exceeds 2 GB.");
        _buffer.EnsureCapacity((long)_length + size);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteLong(long value)
    {
        ulong n = (ulong)((value << 1) ^ (value >> 63));
        var span = GetSpan(10);
        if (n < 0x80)
        {
            span[0] = (byte)n;
            _length++;
            return;
        }
        if (n < 1UL << 56)
        {
            // Up to 8 bytes in one store: spread the 7-bit groups into bytes, set the continuation bits (SWAR).
            int length = (64 - System.Numerics.BitOperations.LeadingZeroCount(n) + 6) / 7;
            ulong continuation = 0x8080808080808080UL & ((1UL << ((length - 1) * 8)) - 1);
            BinaryPrimitives.WriteUInt64LittleEndian(span, SpreadSevenBitGroups(n) | continuation);
            _length += length;
            return;
        }
        int i = 0;
        while (n >= 0x80)
        {
            span[i++] = (byte)(n | 0x80);
            n >>= 7;
        }
        span[i++] = (byte)n;
        _length += i;
    }

    /// <summary>Spreads 56 bits into the low 7 bits of 8 little-endian bytes (inverse of
    /// <see cref="AvroSpanReader.CompactSevenBitGroups"/>).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ulong SpreadSevenBitGroups(ulong x)
    {
        x = (x & 0x000000000FFFFFFFUL) | ((x & 0x00FFFFFFF0000000UL) << 4);
        x = (x & 0x00003FFF00003FFFUL) | ((x & 0x0FFFC0000FFFC000UL) << 2);
        return (x & 0x007F007F007F007FUL) | ((x & 0x3F803F803F803F80UL) << 1);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteInt(int value) => WriteLong(value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteBoolean(bool value)
    {
        GetSpan(1)[0] = value ? (byte)1 : (byte)0;
        _length++;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteFloat(float value)
    {
        BinaryPrimitives.WriteSingleLittleEndian(GetSpan(4), value);
        _length += 4;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteDouble(double value)
    {
        BinaryPrimitives.WriteDoubleLittleEndian(GetSpan(8), value);
        _length += 8;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        WriteLong(value.Length);
        WriteFixed(value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteFixed(ReadOnlySpan<byte> value)
    {
        value.CopyTo(GetSpan(value.Length));
        _length += value.Length;
    }

    public void Dispose()
    {
        _buffer.Release();
        _length = 0;
    }
}
