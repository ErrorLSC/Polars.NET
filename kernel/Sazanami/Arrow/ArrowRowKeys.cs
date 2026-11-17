using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Kaminari.Sazanami.Arrow;

/// <summary>
/// The identity of a row's values in a few columns as bytes, equal for equal values whatever array (or batch) holds
/// them: the key of a row's group across batches (a partition, a bucket). Per column a tag (0 null, 1 a value) and the
/// value: fixed-width values by their bytes, booleans as one byte, strings and binary by their length and bytes (in
/// every layout: plain, large, view), dictionary entries by the value they stand for. Columns of one key must keep
/// their types from batch to batch (an int32 and an int64 of equal value are other keys). The bytes go to a buffer the
/// caller reuses, and are looked up as a span (<see cref="Encodings.ByteArrayDictionary.GetOrAdd"/>): no allocation per row.
/// </summary>
internal static class ArrowRowKeys
{
    /// <summary>
    /// Appends the key bytes of row <paramref name="row"/> of the arrays to <paramref name="key"/>.
    /// <paramref name="emptyIsNull"/> keys an empty string or binary value as null (as Hive partitions take it).
    /// </summary>
    public static void Append(IReadOnlyList<IArrowArray> columns, int row, IBufferWriter<byte> key, bool emptyIsNull)
    {
        foreach (IArrowArray column in columns) Append(column, row, key, emptyIsNull);
    }

    private static void Append(IArrowArray array, int row, IBufferWriter<byte> key, bool emptyIsNull)
    {
        if (array.Data.DataType is NullType || array.IsNull(row))
        {
            Tag(key, 0);
            return;
        }
        switch (array)
        {
            case DictionaryArray dictionary:
                Append(dictionary.Dictionary, Index(dictionary, row), key, emptyIsNull);
                return;
            case BinaryViewArray views:
                Bytes(key, views.GetBytes(row), emptyIsNull);
                return;
            case BinaryArray binary:
                Bytes(key, binary.GetBytes(row), emptyIsNull);
                return;
            case LargeBinaryArray large:
                Bytes(key, large.GetBytes(row), emptyIsNull);
                return;
            case BooleanArray booleans:
                Tag(key, 1);
                Tag(key, booleans.GetValue(row) == true ? (byte)1 : (byte)0);
                return;
        }
        if (array.Data.DataType is not FixedWidthType { BitWidth: >= 8 } fixedWidth)
            throw new NotSupportedException($"Rows of {array.Data.DataType.Name} arrays have no key.");
        int width = fixedWidth.BitWidth / 8;
        Tag(key, 1);
        key.Write(array.Data.Buffers[1].Span.Slice((array.Data.Offset + row) * width, width));
    }

    private static void Tag(IBufferWriter<byte> key, byte tag)
    {
        key.GetSpan(1)[0] = tag;
        key.Advance(1);
    }

    private static void Bytes(IBufferWriter<byte> key, ReadOnlySpan<byte> value, bool emptyIsNull)
    {
        if (value.IsEmpty && emptyIsNull)
        {
            Tag(key, 0);
            return;
        }
        Tag(key, 1);
        BinaryPrimitives.WriteInt32LittleEndian(key.GetSpan(4), value.Length);
        key.Advance(4);
        key.Write(value);
    }

    private static int Index(DictionaryArray dictionary, int row)
    {
        ArrayData indices = dictionary.Indices.Data;
        ReadOnlySpan<byte> raw = indices.Buffers[1].Span;
        int at = indices.Offset + row;
        return indices.DataType.TypeId switch
        {
            ArrowTypeId.Int8 => (sbyte)raw[at],
            ArrowTypeId.UInt8 => raw[at],
            ArrowTypeId.Int16 => MemoryMarshal.Cast<byte, short>(raw)[at],
            ArrowTypeId.UInt16 => MemoryMarshal.Cast<byte, ushort>(raw)[at],
            ArrowTypeId.Int32 => MemoryMarshal.Cast<byte, int>(raw)[at],
            ArrowTypeId.UInt32 => checked((int)MemoryMarshal.Cast<byte, uint>(raw)[at]),
            ArrowTypeId.Int64 => checked((int)MemoryMarshal.Cast<byte, long>(raw)[at]),
            _ => checked((int)MemoryMarshal.Cast<byte, ulong>(raw)[at]),
        };
    }
}
