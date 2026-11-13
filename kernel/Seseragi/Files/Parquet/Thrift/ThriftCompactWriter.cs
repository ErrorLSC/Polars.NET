using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Kaminari.Seseragi.Files.Parquet.Thrift;

/// <summary>
/// Writes the Thrift compact protocol. A struct is written as <c>BeginStruct()</c> (or <c>BeginStructField(id)</c>
/// inside another struct), its fields in id order, then <c>EndStruct()</c>; a list as <c>BeginListField(id, type,
/// count)</c> followed by exactly <c>count</c> elements (<c>Write*</c> values, or <c>BeginStruct()</c> …
/// <c>EndStruct()</c> per struct element).
/// </summary>
internal sealed class ThriftCompactWriter
{
    private readonly ArrayBufferWriter<byte> _out = new(256);
    private readonly Stack<short> _outer = new();
    private short _last;

    /// <summary>The bytes written so far.</summary>
    public ReadOnlySpan<byte> WrittenSpan => _out.WrittenSpan;

    public byte[] ToArray() => _out.WrittenSpan.ToArray();

    public void Clear()
    {
        _out.Clear();
        _outer.Clear();
        _last = 0;
    }

    // ---- structs ----

    /// <summary>Starts a struct at the top level or as a list element.</summary>
    public void BeginStruct()
    {
        _outer.Push(_last);
        _last = 0;
    }

    /// <summary>Starts a struct field of the current struct.</summary>
    public void BeginStructField(short id)
    {
        FieldHeader(id, ThriftType.Struct);
        BeginStruct();
    }

    /// <summary>Writes the stop byte and returns to the enclosing struct.</summary>
    public void EndStruct()
    {
        WriteByte(ThriftType.Stop);
        _last = _outer.Count > 0 ? _outer.Pop() : (short)0;
    }

    /// <summary>An empty struct field (a member of a union of empty structs).</summary>
    public void EmptyStructField(short id)
    {
        BeginStructField(id);
        EndStruct();
    }

    // ---- fields ----

    public void BoolField(short id, bool value) => FieldHeader(id, value ? ThriftType.BoolTrue : ThriftType.BoolFalse);

    public void I8Field(short id, sbyte value)
    {
        FieldHeader(id, ThriftType.Byte);
        WriteByte((byte)value);
    }

    public void I16Field(short id, short value)
    {
        FieldHeader(id, ThriftType.I16);
        WriteVarint(ZigZag(value));
    }

    public void I32Field(short id, int value)
    {
        FieldHeader(id, ThriftType.I32);
        WriteI32(value);
    }

    public void I64Field(short id, long value)
    {
        FieldHeader(id, ThriftType.I64);
        WriteI64(value);
    }

    public void DoubleField(short id, double value)
    {
        FieldHeader(id, ThriftType.Double);
        WriteDouble(value);
    }

    public void BinaryField(short id, ReadOnlySpan<byte> value)
    {
        FieldHeader(id, ThriftType.Binary);
        WriteBinary(value);
    }

    public void StringField(short id, string value)
    {
        FieldHeader(id, ThriftType.Binary);
        WriteString(value);
    }

    /// <summary>Starts a list field of <paramref name="count"/> elements of wire type <paramref name="elementType"/>.</summary>
    public void BeginListField(short id, byte elementType, int count)
    {
        FieldHeader(id, ThriftType.List);
        if (count < 15) WriteByte((byte)((count << 4) | elementType));
        else
        {
            WriteByte((byte)(0xF0 | elementType));
            WriteVarint((ulong)count);
        }
    }

    // ---- values (list elements) ----

    /// <summary>A bool list element: 1 for true, 2 for false, as the Java and C++ Thrift libraries write it.</summary>
    public void WriteBool(bool value) => WriteByte(value ? ThriftType.BoolTrue : ThriftType.BoolFalse);

    public void WriteI32(int value) => WriteVarint(ZigZag(value));

    public void WriteI64(long value) => WriteVarint(ZigZag(value));

    public void WriteDouble(double value)
    {
        Span<byte> span = _out.GetSpan(8);
        BinaryPrimitives.WriteDoubleLittleEndian(span, value);
        _out.Advance(8);
    }

    public void WriteBinary(ReadOnlySpan<byte> value)
    {
        WriteVarint((ulong)value.Length);
        _out.Write(value);
    }

    public void WriteString(string value)
    {
        int length = Encoding.UTF8.GetByteCount(value);
        WriteVarint((ulong)length);
        Span<byte> span = _out.GetSpan(length);
        Encoding.UTF8.GetBytes(value, span);
        _out.Advance(length);
    }

    // ---- encoding ----

    private void FieldHeader(short id, byte type)
    {
        int delta = id - _last;
        if (delta is > 0 and <= 15) WriteByte((byte)((delta << 4) | type));
        else
        {
            WriteByte(type);
            WriteVarint(ZigZag(id));
        }
        _last = id;
    }

    private void WriteByte(byte value)
    {
        _out.GetSpan(1)[0] = value;
        _out.Advance(1);
    }

    private void WriteVarint(ulong value)
    {
        Span<byte> span = _out.GetSpan(10);
        int n = 0;
        while (value >= 0x80)
        {
            span[n++] = (byte)(value | 0x80);
            value >>= 7;
        }
        span[n++] = (byte)value;
        _out.Advance(n);
    }

    private static ulong ZigZag(long n) => (ulong)((n << 1) ^ (n >> 63));
}
