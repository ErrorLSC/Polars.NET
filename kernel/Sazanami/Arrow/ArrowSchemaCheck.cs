using System.Buffers.Binary;

namespace Kaminari.Sazanami.Arrow;

/// <summary>
/// Checks that an Arrow IPC schema message (as stored in Parquet's <c>ARROW:schema</c>, or at the head of an IPC
/// stream) is a sound FlatBuffer before the Apache.Arrow reader sees it: every table, vtable, offset, string and vector
/// lies inside the message, vectors are no longer than the bytes they take, nesting is bounded. The reader trusts
/// these, and a damaged length otherwise makes it allocate gigabytes or read garbage. Only the tables a schema message
/// uses are walked (Message, Schema, Field, KeyValue, DictionaryEncoding, Int and the type tables); a sound buffer may
/// still hold values the reader refuses (an unknown bit width), which it reports itself.
/// </summary>
internal static class ArrowSchemaCheck
{
    private const int MaxDepth = 64;

    /// <summary>Whether <paramref name="message"/> (an optional continuation marker, a length, the FlatBuffer) is sound.</summary>
    public static bool IsSound(ReadOnlySpan<byte> message)
    {
        if (message.Length < 4) return false;
        int at = 0;
        if (BinaryPrimitives.ReadUInt32LittleEndian(message) == 0xFFFFFFFF)
        {
            if (message.Length < 8) return false;
            at = 4;
        }
        int length = BinaryPrimitives.ReadInt32LittleEndian(message[at..]);
        if (length < 8 || length > message.Length - at - 4) return false;
        return new Walker(message.Slice(at + 4, length)).Message();
    }

    /// <summary>The checks over one FlatBuffer; positions are offsets into it.</summary>
    private readonly ref struct Walker(ReadOnlySpan<byte> buffer)
    {
        private readonly ReadOnlySpan<byte> _b = buffer;

        public bool Message()
        {
            // The root: an offset to the Message table. Message: 0 version, 1 header_type, 2 header, 3 bodyLength, 4 custom_metadata.
            if (!Target(0, out int message) || !Table(message, out Table m)) return false;
            if (!Scalar(m, 0, 2) || !Scalar(m, 1, 1) || !Scalar(m, 3, 8) || !KeyValues(m, 4)) return false;
            // Header type 1 is Schema; a schema message must have one.
            if (!m.Has(1) || _b[m.At(1)] != 1 || !Child(m, 2, out int schema) || schema < 0 || !Table(schema, out Table s)) return false;
            // Schema: 0 endianness, 1 fields, 2 custom_metadata, 3 features (longs).
            return Scalar(s, 0, 2) && Fields(s, 1, 0) && KeyValues(s, 2) && Scalars(s, 3, 8);
        }

        private bool Fields(Table table, int slot, int depth)
        {
            if (depth > MaxDepth || !Vector(table, slot, 4, out int start, out int count)) return false;
            for (int i = 0; i < count; i++)
            {
                if (!Target(start + 4 * i, out int field) || !Field(field, depth)) return false;
            }
            return true;
        }

        /// <summary>Field: 0 name, 1 nullable, 2 type_type, 3 type, 4 dictionary, 5 children, 6 custom_metadata.</summary>
        private bool Field(int position, int depth)
        {
            if (!Table(position, out Table f) || !String(f, 0) || !Scalar(f, 1, 1) || !Scalar(f, 2, 1) || !KeyValues(f, 6)) return false;
            int type = f.Has(2) ? _b[f.At(2)] : 0;
            if (!Child(f, 3, out int typeTable) || typeTable >= 0 && !TypeTable(typeTable, type)) return false;
            if (!Child(f, 4, out int dictionary)) return false;
            if (dictionary >= 0)
            {
                // DictionaryEncoding: 0 id, 1 indexType (Int), 2 isOrdered, 3 dictionaryKind.
                if (!Table(dictionary, out Table d) || !Scalar(d, 0, 8) || !Scalar(d, 2, 1) || !Scalar(d, 3, 2)) return false;
                if (!Child(d, 1, out int index) || index >= 0 && !TypeTable(index, 2)) return false;
            }
            return Fields(f, 5, depth + 1);
        }

        /// <summary>The table of a type (Schema.fbs's Type union), by its tag: the sizes of its scalars, its offsets.</summary>
        private bool TypeTable(int position, int type)
        {
            if (!Table(position, out Table t)) return false;
            return type switch
            {
                2 => Scalar(t, 0, 4) && Scalar(t, 1, 1),                    // Int: bitWidth, is_signed
                3 or 8 or 11 or 18 => Scalar(t, 0, 2),                      // FloatingPoint, Date, Interval, Duration: a unit
                7 => Scalar(t, 0, 4) && Scalar(t, 1, 4) && Scalar(t, 2, 4), // Decimal: precision, scale, bitWidth
                9 => Scalar(t, 0, 2) && Scalar(t, 1, 4),                    // Time: unit, bitWidth
                10 => Scalar(t, 0, 2) && String(t, 1),                      // Timestamp: unit, timezone
                14 => Scalar(t, 0, 2) && Scalars(t, 1, 4),                  // Union: mode, typeIds
                15 or 16 => Scalar(t, 0, 4),                                // FixedSizeBinary, FixedSizeList: a width
                17 => Scalar(t, 0, 1),                                      // Map: keysSorted
                1 or 4 or 5 or 6 or 12 or 13 or 19 or 20 or 21 or 22 or 23 or 24 or 25 or 26 => true, // no fields
                _ => false,                                                 // NONE or a type from the future
            };
        }

        private bool KeyValues(Table table, int slot)
        {
            if (!Vector(table, slot, 4, out int start, out int count)) return false;
            for (int i = 0; i < count; i++)
            {
                // KeyValue: 0 key, 1 value.
                if (!Target(start + 4 * i, out int pair) || !Table(pair, out Table kv) || !String(kv, 0) || !String(kv, 1)) return false;
            }
            return true;
        }

        // ------------------------------------------------------------------
        // FlatBuffer primitives
        // ------------------------------------------------------------------

        /// <summary>A table at <paramref name="position"/>: its vtable (found through the leading signed offset) and its extent lie in the buffer.</summary>
        private bool Table(int position, out Table table)
        {
            table = default;
            if (position < 0 || position > _b.Length - 4) return false;
            long vtable = position - (long)BinaryPrimitives.ReadInt32LittleEndian(_b[position..]);
            if (vtable < 0 || vtable > _b.Length - 4) return false;
            int vtableSize = BinaryPrimitives.ReadUInt16LittleEndian(_b[(int)vtable..]);
            int tableSize = BinaryPrimitives.ReadUInt16LittleEndian(_b[((int)vtable + 2)..]);
            if (vtableSize < 4 || (vtableSize & 1) != 0 || vtable + vtableSize > _b.Length || tableSize < 4 || position + (long)tableSize > _b.Length) return false;
            table = new Table(position, (int)vtable, vtableSize, tableSize, _b);
            return true;
        }

        /// <summary>A scalar slot, absent or of <paramref name="size"/> bytes inside its table.</summary>
        private static bool Scalar(Table table, int slot, int size) => !table.Has(slot) || table.Offset(slot) + size <= table.Size;

        /// <summary>An offset slot (string, table, vector): absent (<paramref name="target"/> -1), or pointing inside the buffer.</summary>
        private bool Child(Table table, int slot, out int target)
        {
            target = -1;
            if (!table.Has(slot)) return true;
            if (table.Offset(slot) + 4 > table.Size) return false;
            return Target(table.At(slot), out target);
        }

        /// <summary>The position an unsigned offset stored at <paramref name="at"/> points to (forward, inside the buffer).</summary>
        private bool Target(int at, out int target)
        {
            target = -1;
            if (at < 0 || at > _b.Length - 4) return false;
            long to = at + (long)BinaryPrimitives.ReadUInt32LittleEndian(_b[at..]);
            if (to >= _b.Length) return false;
            target = (int)to;
            return true;
        }

        /// <summary>A vector slot: absent (count 0), or a count of <paramref name="size"/>-byte elements that fit after it.</summary>
        private bool Vector(Table table, int slot, int size, out int start, out int count)
        {
            start = count = 0;
            if (!Child(table, slot, out int vector)) return false;
            if (vector < 0) return true;
            if (vector > _b.Length - 4) return false;
            long n = BinaryPrimitives.ReadUInt32LittleEndian(_b[vector..]);
            if (n * size > _b.Length - vector - 4) return false;
            start = vector + 4;
            count = (int)n;
            return true;
        }

        private bool String(Table table, int slot) => Vector(table, slot, 1, out _, out _);

        private bool Scalars(Table table, int slot, int size) => Vector(table, slot, size, out _, out _);
    }

    /// <summary>A table: its position, its vtable and the size it claims.</summary>
    private readonly ref struct Table(int position, int vtable, int vtableSize, int size, ReadOnlySpan<byte> buffer)
    {
        private readonly ReadOnlySpan<byte> _b = buffer;

        public int Size => size;

        /// <summary>The slot's offset in the table (0: absent; vtables shorter than the slot leave it absent).</summary>
        public int Offset(int slot) => 4 + 2 * slot + 2 <= vtableSize ? BinaryPrimitives.ReadUInt16LittleEndian(_b[(vtable + 4 + 2 * slot)..]) : 0;

        public bool Has(int slot) => Offset(slot) != 0;

        /// <summary>The slot's position in the buffer.</summary>
        public int At(int slot) => position + Offset(slot);
    }
}
