using System.Buffers.Binary;
using System.Text;
using Kaminari.Sazanami.Arrow.Builders;

namespace Kaminari.Sazanami.Arrow.Ipc;

/// <summary>A field of a FlatBuffer table being written: a scalar of 1, 2, 4 or 8 bytes, or an offset patched later.</summary>
internal readonly record struct FlatBufferField(int Slot, int Size, long Value, bool IsOffset)
{
    public static FlatBufferField Byte(int slot, byte value) => new(slot, 1, value, false);

    public static FlatBufferField Bool(int slot, bool value) => new(slot, 1, value ? 1 : 0, false);

    public static FlatBufferField Short(int slot, short value) => new(slot, 2, value, false);

    public static FlatBufferField Int(int slot, int value) => new(slot, 4, value, false);

    public static FlatBufferField Long(int slot, long value) => new(slot, 8, value, false);

    /// <summary>An offset to a string, vector or table written after this table (<see cref="FlatBufferWriter.Patch"/>).</summary>
    public static FlatBufferField Offset(int slot) => new(slot, 4, 0, true);
}

/// <summary>
/// Writes a FlatBuffer front to back into native memory: the root offset first, then each table before what it points
/// to (FlatBuffer offsets point forward), each vtable just before its table. Fields are laid out largest first;
/// every value is aligned to its size from the buffer's start, which the caller places at an 8-byte boundary.
/// </summary>
internal sealed unsafe class FlatBufferWriter : IDisposable
{
    private NativeGrowBuffer _b;

    public FlatBufferWriter() => _b.Append(0u); // the root offset, patched by Root

    public int Position => (int)_b.Length;

    /// <summary>The bytes written so far.</summary>
    public ReadOnlySpan<byte> Written => new(_b.Pointer, (int)_b.Length);

    /// <summary>Points the root at <paramref name="table"/>.</summary>
    public void Root(int table) => Patch(0, table);

    /// <summary>Writes, at <paramref name="at"/> (an offset field or a vector element), the offset to <paramref name="target"/>.</summary>
    public void Patch(int at, int target)
    {
        if (target <= at) throw new InvalidOperationException("FlatBuffer offsets point forward.");
        BinaryPrimitives.WriteUInt32LittleEndian(new Span<byte>(_b.Pointer + at, 4), (uint)(target - at));
    }

    /// <summary>Pads with zeros to a multiple of <paramref name="alignment"/>.</summary>
    public void Pad(int alignment)
    {
        int over = Position % alignment;
        if (over != 0) _b.AppendZeros(alignment - over);
    }

    /// <summary>A string (UTF-8, its length first, a zero after it); returns where it is.</summary>
    public int String(string value)
    {
        int length = Encoding.UTF8.GetByteCount(value);
        Pad(4);
        int at = Position;
        _b.Append((uint)length);
        Encoding.UTF8.GetBytes(value, new Span<byte>(_b.Reserve(length), length));
        _b.Append((byte)0);
        return at;
    }

    /// <summary>A vector of <paramref name="count"/> offsets (patched later: element i at <paramref name="first"/> + 4i); returns where it is.</summary>
    public int OffsetVector(int count, out int first)
    {
        Pad(4);
        int at = Position;
        _b.Append((uint)count);
        first = Position;
        _b.AppendZeros(4L * count);
        return at;
    }

    /// <summary>A vector of <paramref name="count"/> fixed-size elements (structs or scalars) aligned to <paramref name="alignment"/>; returns where it is.</summary>
    public int ValueVector(ReadOnlySpan<byte> elements, int count, int alignment)
    {
        // The length comes just before the elements: pad so that the elements land aligned.
        while ((Position + 4) % Math.Max(alignment, 4) != 0) _b.Append((byte)0);
        int at = Position;
        _b.Append((uint)count);
        _b.Append(elements);
        return at;
    }

    /// <summary>
    /// A table of <paramref name="fields"/> (its vtable just before it); returns where it is and, by slot, where each
    /// field lies (for offsets to patch).
    /// </summary>
    public int Table(ReadOnlySpan<FlatBufferField> fields, out int[] fieldAt)
    {
        int slots = 0, largest = 4;
        foreach (FlatBufferField f in fields)
        {
            slots = Math.Max(slots, f.Slot + 1);
            largest = Math.Max(largest, f.Size);
        }
        // Layout: the vtable offset (4 bytes), then the fields from the largest down, each at a multiple of its size.
        Span<int> offsets = stackalloc int[fields.Length];
        int end = 4;
        foreach (int size in (ReadOnlySpan<int>)[8, 4, 2, 1])
        {
            for (int i = 0; i < fields.Length; i++)
            {
                if (fields[i].Size != size) continue;
                end = (end + size - 1) / size * size;
                offsets[i] = end;
                end += size;
            }
        }
        int tableSize = (end + 3) / 4 * 4;

        // The vtable: its size, the table's size, each slot's offset (0: absent).
        Pad(2);
        int vtable = Position;
        _b.Append((ushort)(4 + 2 * slots));
        _b.Append((ushort)tableSize);
        int slotsAt = Position;
        _b.AppendZeros(2L * slots);
        for (int i = 0; i < fields.Length; i++)
            BinaryPrimitives.WriteUInt16LittleEndian(new Span<byte>(_b.Pointer + slotsAt + 2 * fields[i].Slot, 2), (ushort)offsets[i]);

        Pad(largest);
        int table = Position;
        _b.AppendZeros(tableSize);
        byte* t = _b.Pointer + table;
        BinaryPrimitives.WriteInt32LittleEndian(new Span<byte>(t, 4), table - vtable);
        fieldAt = new int[slots];
        for (int i = 0; i < fields.Length; i++)
        {
            FlatBufferField f = fields[i];
            var span = new Span<byte>(t + offsets[i], f.Size);
            switch (f.Size)
            {
                case 1: span[0] = (byte)f.Value; break;
                case 2: BinaryPrimitives.WriteInt16LittleEndian(span, (short)f.Value); break;
                case 4: BinaryPrimitives.WriteInt32LittleEndian(span, (int)f.Value); break;
                default: BinaryPrimitives.WriteInt64LittleEndian(span, f.Value); break;
            }
            fieldAt[f.Slot] = table + offsets[i];
        }
        return table;
    }

    public void Dispose() => _b.Free();
}
