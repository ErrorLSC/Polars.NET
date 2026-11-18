using System.Buffers.Binary;
using System.Text;

namespace Kaminari.Sazanami.Arrow.Ipc;

/// <summary>
/// A FlatBuffer table read with every position checked against the buffer: the vtable, each slot, each offset and
/// each vector or string lies inside it, and vectors are no longer than the bytes left. Damage throws
/// <see cref="InvalidDataException"/> instead of reading outside the buffer or allocating for a forged length.
/// </summary>
internal readonly ref struct FlatBufferTable
{
    private readonly ReadOnlySpan<byte> _b;
    private readonly int _position;
    private readonly int _vtable;
    private readonly int _vtableSize;

    private FlatBufferTable(ReadOnlySpan<byte> buffer, int position)
    {
        _b = buffer;
        if (position < 0 || position > buffer.Length - 4) throw Damaged("a table outside the buffer");
        long vtable = position - (long)BinaryPrimitives.ReadInt32LittleEndian(buffer[position..]);
        if (vtable < 0 || vtable > buffer.Length - 4) throw Damaged("a vtable outside the buffer");
        int vtableSize = BinaryPrimitives.ReadUInt16LittleEndian(buffer[(int)vtable..]);
        int size = BinaryPrimitives.ReadUInt16LittleEndian(buffer[((int)vtable + 2)..]);
        if (vtableSize < 4 || (vtableSize & 1) != 0 || vtable + vtableSize > buffer.Length || size < 4 || position + (long)size > buffer.Length)
            throw Damaged("a table or vtable reaches past the buffer");
        _position = position;
        _vtable = (int)vtable;
        _vtableSize = vtableSize;
    }

    /// <summary>The root table of a FlatBuffer (its first word is the offset to it).</summary>
    public static FlatBufferTable Root(ReadOnlySpan<byte> buffer)
    {
        if (buffer.Length < 8) throw Damaged("a buffer too short for a root table");
        return new FlatBufferTable(buffer, Target(buffer, 0));
    }

    private int Offset(int slot)
    {
        int at = 4 + 2 * slot;
        if (at + 2 > _vtableSize) return 0; // vtables shorter than the slot leave it absent
        int offset = BinaryPrimitives.ReadUInt16LittleEndian(_b[(_vtable + at)..]);
        return offset;
    }

    public bool Has(int slot) => Offset(slot) != 0;

    private int Scalar(int slot, int size)
    {
        int offset = Offset(slot);
        if (offset == 0) return -1;
        // As the FlatBuffers verifier checks it: inside the buffer (not necessarily inside the size the vtable gives the table).
        if ((long)_position + offset + size > _b.Length) throw Damaged("a field reaches past the buffer");
        return _position + offset;
    }

    public byte UInt8(int slot, byte fallback = 0) => Scalar(slot, 1) is var at and >= 0 ? _b[at] : fallback;

    public bool Bool(int slot, bool fallback = false) => Scalar(slot, 1) is var at and >= 0 ? _b[at] != 0 : fallback;

    public short Int16(int slot, short fallback = 0) => Scalar(slot, 2) is var at and >= 0 ? BinaryPrimitives.ReadInt16LittleEndian(_b[at..]) : fallback;

    public int Int32(int slot, int fallback = 0) => Scalar(slot, 4) is var at and >= 0 ? BinaryPrimitives.ReadInt32LittleEndian(_b[at..]) : fallback;

    public long Int64(int slot, long fallback = 0) => Scalar(slot, 8) is var at and >= 0 ? BinaryPrimitives.ReadInt64LittleEndian(_b[at..]) : fallback;

    /// <summary>The position an offset slot points to, or -1 when the slot is absent.</summary>
    private int Child(int slot)
    {
        int at = Scalar(slot, 4);
        return at < 0 ? -1 : Target(_b, at);
    }

    /// <summary>The table a slot points to.</summary>
    public bool TryTable(int slot, out FlatBufferTable table)
    {
        int at = Child(slot);
        table = at < 0 ? default : new FlatBufferTable(_b, at);
        return at >= 0;
    }

    /// <summary>A vector of <paramref name="elementSize"/>-byte elements: where its elements start and how many (0 when absent).</summary>
    public int Vector(int slot, int elementSize, out int start)
    {
        start = 0;
        int at = Child(slot);
        if (at < 0) return 0;
        if (at > _b.Length - 4) throw Damaged("a vector outside the buffer");
        long count = BinaryPrimitives.ReadUInt32LittleEndian(_b[at..]);
        if (count * elementSize > _b.Length - at - 4) throw Damaged("a vector longer than the buffer");
        start = at + 4;
        return (int)count;
    }

    /// <summary>Element <paramref name="index"/> of a vector of tables starting at <paramref name="start"/>.</summary>
    public FlatBufferTable TableAt(int start, int index) => new(_b, Target(_b, start + 4 * index));

    /// <summary>The bytes of a vector of fixed-size elements (structs, scalars).</summary>
    public ReadOnlySpan<byte> Bytes(int start, int length) => _b.Slice(start, length);

    /// <summary>A string slot (null when absent); invalid UTF-8 is replaced.</summary>
    public string? String(int slot)
    {
        int count = Vector(slot, 1, out int start);
        if (start == 0) return null;
        return Encoding.UTF8.GetString(_b.Slice(start, count));
    }

    /// <summary>The position an unsigned offset stored at <paramref name="at"/> points to (forward, inside the buffer).</summary>
    private static int Target(ReadOnlySpan<byte> buffer, int at)
    {
        if (at < 0 || at > buffer.Length - 4) throw Damaged("an offset outside the buffer");
        long to = at + (long)BinaryPrimitives.ReadUInt32LittleEndian(buffer[at..]);
        if (to >= buffer.Length) throw Damaged("an offset pointing past the buffer");
        return (int)to;
    }

    public static InvalidDataException Damaged(string what) => new($"Arrow IPC metadata is damaged: {what}.");
}
