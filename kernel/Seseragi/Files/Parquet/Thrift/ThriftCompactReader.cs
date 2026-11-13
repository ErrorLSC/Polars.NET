using System.Buffers.Binary;
using System.Text;

namespace Kaminari.Seseragi.Files.Parquet.Thrift;

/// <summary>The type nibbles of the Thrift compact protocol.</summary>
internal static class ThriftType
{
    public const byte Stop = 0, BoolTrue = 1, BoolFalse = 2, Byte = 3, I16 = 4, I32 = 5, I64 = 6, Double = 7, Binary = 8,
        List = 9, Set = 10, Map = 11, Struct = 12;
}

/// <summary>Reads one element of a list of structs.</summary>
internal delegate T ThriftElementReader<T>(ref ThriftCompactReader reader);

/// <summary>
/// Reads the Thrift compact protocol from a span. A struct is read as
/// <c>r.EnterStruct(); short last = 0; while (r.NextField(ref last, out id, out type)) …; r.ExitStruct();</c>, each
/// field either read with the method of its type or skipped with <see cref="Skip"/>. Every length and count is checked
/// against the bytes left, so damaged input fails with <see cref="ParquetFormatException"/>, never with an
/// out-of-range read or a huge allocation.
/// </summary>
internal ref struct ThriftCompactReader(ReadOnlySpan<byte> data)
{
    private const int MaxDepth = 64;

    private readonly ReadOnlySpan<byte> _data = data;
    private int _position;
    private int _depth;
    private bool _pendingBool; // a bool field's value lives in its header

    /// <summary>How many bytes were read.</summary>
    public readonly int Position => _position;

    /// <summary>How many bytes are left.</summary>
    public readonly int Remaining => _data.Length - _position;

    public void EnterStruct()
    {
        if (++_depth > MaxDepth) throw new ParquetFormatException("The Parquet metadata nests too deeply.");
    }

    public void ExitStruct() => _depth--;

    /// <summary>Reads the next field header of the current struct; false at its stop byte.</summary>
    /// <param name="last">The id of the previous field of this struct (0 at its start), updated.</param>
    /// <param name="id">The field id.</param>
    /// <param name="type">The field's wire type (<see cref="ThriftType"/>).</param>
    public bool NextField(ref short last, out short id, out byte type)
    {
        byte header = ReadRawByte();
        if (header == ThriftType.Stop)
        {
            id = 0;
            type = 0;
            return false;
        }
        type = (byte)(header & 0x0F);
        int delta = header >> 4;
        id = delta != 0 ? (short)(last + delta) : ReadI16();
        last = id;
        if (type is ThriftType.BoolTrue or ThriftType.BoolFalse) _pendingBool = type == ThriftType.BoolTrue;
        return true;
    }

    /// <summary>Whether a field of wire type <paramref name="type"/> holds a bool.</summary>
    public static bool IsBool(byte type) => type is ThriftType.BoolTrue or ThriftType.BoolFalse;

    /// <summary>A bool field's value (from its header).</summary>
    public readonly bool BoolField() => _pendingBool;

    /// <summary>A bool element of a list: one byte, 1 for true (0 or 2 for false, as writers differ).</summary>
    public bool ReadBoolElement() => ReadRawByte() == 1;

    public sbyte ReadI8() => (sbyte)ReadRawByte();

    public short ReadI16()
    {
        long value = ZigZag(ReadVarint());
        return value is >= short.MinValue and <= short.MaxValue ? (short)value : throw new ParquetFormatException("A 16-bit value in the Parquet metadata is out of range.");
    }

    public int ReadI32()
    {
        long value = ZigZag(ReadVarint());
        return value is >= int.MinValue and <= int.MaxValue ? (int)value : throw new ParquetFormatException("A 32-bit value in the Parquet metadata is out of range.");
    }

    public long ReadI64() => ZigZag(ReadVarint());

    public double ReadDouble()
    {
        double value = BinaryPrimitives.ReadDoubleLittleEndian(Take(8));
        return value;
    }

    /// <summary>A binary value, as a view of the input.</summary>
    public ReadOnlySpan<byte> ReadBinary()
    {
        ulong length = ReadVarint();
        if (length > (ulong)Remaining) throw Truncated();
        return Take((int)length);
    }

    public byte[] ReadBytes() => ReadBinary().ToArray();

    public string ReadString()
    {
        ReadOnlySpan<byte> bytes = ReadBinary();
        return bytes.IsEmpty ? "" : Encoding.UTF8.GetString(bytes);
    }

    /// <summary>A list (or set) header: the element count, at most the bytes left (every element takes one at least).</summary>
    public int ReadListHeader(out byte elementType)
    {
        byte header = ReadRawByte();
        elementType = (byte)(header & 0x0F);
        ulong count = (ulong)(header >> 4);
        if (count == 15) count = ReadVarint();
        if (count > (ulong)Remaining) throw Truncated();
        return (int)count;
    }

    // ---- lists ----

    public T[] ReadStructList<T>(ThriftElementReader<T> read)
    {
        int count = ReadListHeader(out byte type);
        CheckElement(type, ThriftType.Struct, count);
        var result = new T[count];
        for (int i = 0; i < count; i++) result[i] = read(ref this);
        return result;
    }

    public int[] ReadI32List()
    {
        int count = ReadListHeader(out byte type);
        CheckInteger(type, count);
        var result = new int[count];
        for (int i = 0; i < count; i++) result[i] = ReadI32();
        return result;
    }

    public long[] ReadI64List()
    {
        int count = ReadListHeader(out byte type);
        CheckInteger(type, count);
        var result = new long[count];
        for (int i = 0; i < count; i++) result[i] = ReadI64();
        return result;
    }

    public string[] ReadStringList()
    {
        int count = ReadListHeader(out byte type);
        CheckElement(type, ThriftType.Binary, count);
        var result = new string[count];
        for (int i = 0; i < count; i++) result[i] = ReadString();
        return result;
    }

    public byte[][] ReadBinaryList()
    {
        int count = ReadListHeader(out byte type);
        CheckElement(type, ThriftType.Binary, count);
        var result = new byte[count][];
        for (int i = 0; i < count; i++) result[i] = ReadBytes();
        return result;
    }

    public bool[] ReadBoolList()
    {
        int count = ReadListHeader(out byte type);
        if (count > 0 && !IsBool(type)) throw new ParquetFormatException($"A list in the Parquet metadata holds Thrift type {type}, not bool.");
        var result = new bool[count];
        for (int i = 0; i < count; i++) result[i] = ReadBoolElement();
        return result;
    }

    /// <summary>
    /// Integer lists take any varint element type (i16, i32, i64 encode alike), as the generated Thrift code of the
    /// reference readers reads them by the declared type whatever the list header says.
    /// </summary>
    private static void CheckInteger(byte type, int count)
    {
        if (count > 0 && type is not (ThriftType.I16 or ThriftType.I32 or ThriftType.I64))
            throw new ParquetFormatException($"A list in the Parquet metadata holds Thrift type {type}, not integers.");
    }

    private static void CheckElement(byte type, byte expected, int count)
    {
        if (count > 0 && type != expected)
            throw new ParquetFormatException($"A list in the Parquet metadata holds Thrift type {type}, not {expected}.");
    }

    /// <summary>Skips a value of wire type <paramref name="type"/> (a field's value, after its header).</summary>
    public void Skip(byte type)
    {
        switch (type)
        {
            case ThriftType.BoolTrue or ThriftType.BoolFalse: // the value is in the field header
                return;
            case ThriftType.Byte:
                ReadRawByte();
                return;
            case ThriftType.I16 or ThriftType.I32 or ThriftType.I64:
                ReadVarint();
                return;
            case ThriftType.Double:
                Take(8);
                return;
            case ThriftType.Binary:
                ReadBinary();
                return;
            case ThriftType.List or ThriftType.Set:
            {
                int count = ReadListHeader(out byte element);
                for (int i = 0; i < count; i++) SkipElement(element);
                return;
            }
            case ThriftType.Map:
            {
                ulong count = ReadVarint();
                if (count == 0) return;
                if (count > (ulong)Remaining) throw Truncated();
                byte types = ReadRawByte();
                for (ulong i = 0; i < count; i++)
                {
                    SkipElement((byte)(types >> 4));
                    SkipElement((byte)(types & 0x0F));
                }
                return;
            }
            case ThriftType.Struct:
            {
                EnterStruct();
                short last = 0;
                while (NextField(ref last, out _, out byte fieldType)) Skip(fieldType);
                ExitStruct();
                return;
            }
            default:
                throw new ParquetFormatException($"Unknown Thrift compact type {type} in the Parquet metadata.");
        }
    }

    /// <summary>Skips an element of a list, set or map (a bool takes a byte there).</summary>
    private void SkipElement(byte type)
    {
        if (IsBool(type)) ReadRawByte();
        else Skip(type);
    }

    private byte ReadRawByte()
    {
        if ((uint)_position >= (uint)_data.Length) throw Truncated();
        return _data[_position++];
    }

    private ReadOnlySpan<byte> Take(int count)
    {
        if (count > Remaining) throw Truncated();
        ReadOnlySpan<byte> slice = _data.Slice(_position, count);
        _position += count;
        return slice;
    }

    private ulong ReadVarint()
    {
        ulong result = 0;
        for (int shift = 0; shift < 64; shift += 7)
        {
            byte b = ReadRawByte();
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
        }
        throw new ParquetFormatException("A varint in the Parquet metadata is too long.");
    }

    private static long ZigZag(ulong n) => (long)(n >> 1) ^ -(long)(n & 1);

    private static ParquetFormatException Truncated() => new("The Parquet metadata is truncated or damaged.");
}
