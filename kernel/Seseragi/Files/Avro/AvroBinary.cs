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
        int i = 0;
        while (n >= 0x80)
        {
            span[i++] = (byte)(n | 0x80);
            n >>= 7;
        }
        span[i++] = (byte)n;
        _length += i;
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
