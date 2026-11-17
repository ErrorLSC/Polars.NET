using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Sazanami.Grouping;

/// <summary>How a key column's values are compared (and stored): by a normalized 64-bit value, 128 bits, or bytes.</summary>
internal enum KeyKind : byte { Null, Bool, Fixed8, Fixed16, Fixed32, Fixed64, Float16, Float32, Float64, Fixed128, Bytes }

/// <summary>Where the bytes of a byte-string key column are.</summary>
internal enum BytesLayout : byte { None, Offsets32, Offsets64, Views, FixedWidth }

internal static class KeyKinds
{
    /// <summary>
    /// The kind of a column of <paramref name="type"/> (a dictionary's value type for dictionary arrays) and its family:
    /// the type a later batch's column must have (strings and binaries in any layout are one family each).
    /// </summary>
    /// <exception cref="NotSupportedException">A type without a key kind here (nested types).</exception>
    public static KeyKind Of(IArrowType type, out IArrowType family)
    {
        family = type;
        switch (type)
        {
            case NullType: return KeyKind.Null;
            case BooleanType: return KeyKind.Bool;
            case HalfFloatType: return KeyKind.Float16;
            case FloatType: return KeyKind.Float32;
            case DoubleType: return KeyKind.Float64;
            case StringType or LargeStringType or StringViewType:
                family = StringType.Default;
                return KeyKind.Bytes;
            case BinaryType or LargeBinaryType or BinaryViewType:
                family = BinaryType.Default;
                return KeyKind.Bytes;
            case FixedSizeBinaryType f:
                return f.ByteWidth switch { 1 => KeyKind.Fixed8, 2 => KeyKind.Fixed16, 4 => KeyKind.Fixed32, 8 => KeyKind.Fixed64, 16 => KeyKind.Fixed128, _ => KeyKind.Bytes };
            case FixedWidthType { BitWidth: 8 }: return KeyKind.Fixed8;
            case FixedWidthType { BitWidth: 16 }: return KeyKind.Fixed16;
            case FixedWidthType { BitWidth: 32 }: return KeyKind.Fixed32;
            case FixedWidthType { BitWidth: 64 }: return KeyKind.Fixed64;
            case FixedWidthType { BitWidth: 128 }: return KeyKind.Fixed128;
            default: throw new NotSupportedException($"Grouping by {type.Name} keys is not supported.");
        }
    }
}

/// <summary>
/// One key column of the batch being grouped, read in place: pointers into its (pinned) buffers. A dictionary array
/// reads its rows' indices, then the dictionary's values. Values are read normalized (see <see cref="Fixed"/>).
/// </summary>
internal unsafe struct BatchKey
{
    public KeyKind Kind;
    public BytesLayout Layout;
    public bool EmptyIsNull;

    // The array's own nulls (the indices' for a dictionary array).
    public byte* RowValidity;
    public long RowBit;

    // A dictionary array's indices (null: the rows are the values).
    public byte* Indices;
    public int IndexWidth;
    public bool IndexUnsigned;

    // The values (a dictionary's): their nulls when apart from the rows', from their offset.
    public byte* ValueValidity;
    public long ValueBit;
    public byte* Values;   // fixed values from the offset; plain bytes' data; fixed-size binaries from the offset
    public long BoolBit;   // booleans: the bit of value 0
    public byte* Offsets;  // plain and large bytes: offsets from the offset
    public int Width;      // fixed-size binaries
    public byte** Buffers; // views' data buffers
    public int BufferCount;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool Bit(byte* bits, long bit) => (bits[bit >> 3] >> (int)(bit & 7) & 1) != 0;

    /// <summary>The value index of row <paramref name="row"/>, or -1 for a null key.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly int Resolve(int row)
    {
        if (Kind == KeyKind.Null || RowValidity != null && !Bit(RowValidity, RowBit + row)) return -1;
        int at = Indices == null ? row : Index(row);
        if (ValueValidity != null && !Bit(ValueValidity, ValueBit + at)) return -1;
        if (EmptyIsNull && Kind == KeyKind.Bytes && Bytes(at).IsEmpty) return -1;
        return at;
    }

    private readonly int Index(int row) => (IndexWidth, IndexUnsigned) switch
    {
        (1, false) => ((sbyte*)Indices)[row],
        (1, true) => Indices[row],
        (2, false) => ((short*)Indices)[row],
        (2, true) => ((ushort*)Indices)[row],
        (4, false) => ((int*)Indices)[row],
        (4, true) => checked((int)((uint*)Indices)[row]),
        (8, false) => checked((int)((long*)Indices)[row]),
        _ => checked((int)((ulong*)Indices)[row]),
    };

    /// <summary>
    /// A value of at most 64 bits, normalized: floating NaNs as one NaN, -0.0 as 0.0 (SEMANTICS.md, Grouping keys);
    /// booleans 0 or 1; other values by their bits.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly ulong Fixed(int at) => Kind switch
    {
        KeyKind.Fixed8 => Values[at],
        KeyKind.Fixed16 => Unsafe.ReadUnaligned<ushort>(Values + 2L * at),
        KeyKind.Fixed32 => Unsafe.ReadUnaligned<uint>(Values + 4L * at),
        KeyKind.Fixed64 => Unsafe.ReadUnaligned<ulong>(Values + 8L * at),
        KeyKind.Bool => Bit(Values, BoolBit + at) ? 1UL : 0UL,
        KeyKind.Float64 => Normal(Unsafe.ReadUnaligned<double>(Values + 8L * at)),
        KeyKind.Float32 => Normal(Unsafe.ReadUnaligned<float>(Values + 4L * at)),
        KeyKind.Float16 => Normal(Unsafe.ReadUnaligned<Half>(Values + 2L * at)),
        _ => throw new InvalidOperationException($"A {Kind} key has no 64-bit value."),
    };

    private static ulong Normal(double d) => double.IsNaN(d) ? 0x7FF8_0000_0000_0000UL : d == 0 ? 0UL : BitConverter.DoubleToUInt64Bits(d);

    private static ulong Normal(float f) => float.IsNaN(f) ? 0x7FC0_0000U : f == 0 ? 0U : BitConverter.SingleToUInt32Bits(f);

    private static ulong Normal(Half h) => Half.IsNaN(h) ? 0x7E00U : h == Half.Zero ? 0U : BitConverter.HalfToUInt16Bits(h);

    public readonly Int128 Wide(int at) => Unsafe.ReadUnaligned<Int128>(Values + 16L * at);

    /// <summary>The bytes of a byte-string value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly ReadOnlySpan<byte> Bytes(int at)
    {
        switch (Layout)
        {
            case BytesLayout.Offsets32:
            {
                int start = ((int*)Offsets)[at];
                return new ReadOnlySpan<byte>(Values + start, ((int*)Offsets)[at + 1] - start);
            }
            case BytesLayout.Offsets64:
            {
                long start = ((long*)Offsets)[at];
                return new ReadOnlySpan<byte>(Values + start, checked((int)(((long*)Offsets)[at + 1] - start)));
            }
            case BytesLayout.FixedWidth:
                return new ReadOnlySpan<byte>(Values + (long)at * Width, Width);
            default:
            {
                byte* view = Values + 16L * at;
                int length = *(int*)view;
                if (length <= 12) return new ReadOnlySpan<byte>(view + 4, length);
                int buffer = *(int*)(view + 8), offset = *(int*)(view + 12);
                if ((uint)buffer >= (uint)BufferCount) throw new InvalidDataException($"A view points into data buffer {buffer} of {BufferCount}.");
                return new ReadOnlySpan<byte>(Buffers[buffer] + offset, length);
            }
        }
    }

    /// <summary>
    /// Points <paramref name="key"/> at <paramref name="array"/>'s buffers, pinned into <paramref name="pins"/> (released
    /// by the caller once the batch is grouped); views' data buffers listed in <paramref name="buffers"/>.
    /// </summary>
    public static void Prepare(ref BatchKey key, IArrowArray array, KeyKind kind, bool emptyIsNull, List<MemoryHandle> pins, NativeArray<nint> buffers)
    {
        key = new BatchKey { Kind = kind, EmptyIsNull = emptyIsNull };
        if (kind == KeyKind.Null) return;
        ArrayData data = array.Data;
        if (data.NullCount != 0 && !data.Buffers[0].IsEmpty)
        {
            key.RowValidity = Pin(data.Buffers[0], pins);
            key.RowBit = data.Offset;
        }
        ArrayData values = data;
        if (data.DataType is DictionaryType { IndexType: IntegerType index })
        {
            int width = index.BitWidth / 8;
            key.Indices = Pin(data.Buffers[1], pins) + (long)data.Offset * width;
            key.IndexWidth = width;
            key.IndexUnsigned = !index.IsSigned;
            values = data.Dictionary!;
            if (values.NullCount != 0 && !values.Buffers[0].IsEmpty)
            {
                key.ValueValidity = Pin(values.Buffers[0], pins);
                key.ValueBit = values.Offset;
            }
        }
        int offset = values.Offset;
        switch (values.DataType)
        {
            case BooleanType:
                key.Values = Pin(values.Buffers[1], pins);
                key.BoolBit = offset;
                break;
            case StringType or BinaryType:
                key.Layout = BytesLayout.Offsets32;
                key.Offsets = Pin(values.Buffers[1], pins) + 4L * offset;
                key.Values = Pin(values.Buffers[2], pins);
                break;
            case LargeStringType or LargeBinaryType:
                key.Layout = BytesLayout.Offsets64;
                key.Offsets = Pin(values.Buffers[1], pins) + 8L * offset;
                key.Values = Pin(values.Buffers[2], pins);
                break;
            case StringViewType or BinaryViewType:
            {
                key.Layout = BytesLayout.Views;
                key.Values = Pin(values.Buffers[1], pins) + 16L * offset;
                int count = values.Buffers.Length - 2;
                buffers.Reserve(Math.Max(count, 1));
                for (int b = 0; b < count; b++) buffers[b] = (nint)Pin(values.Buffers[2 + b], pins);
                key.Buffers = (byte**)Unsafe.AsPointer(ref buffers[0]);
                key.BufferCount = count;
                break;
            }
            case FixedSizeBinaryType f when kind == KeyKind.Bytes:
                key.Layout = BytesLayout.FixedWidth;
                key.Width = f.ByteWidth;
                key.Values = Pin(values.Buffers[1], pins) + (long)offset * f.ByteWidth;
                break;
            case FixedSizeBinaryType f:
                key.Values = Pin(values.Buffers[1], pins) + (long)offset * f.ByteWidth;
                break;
            case FixedWidthType f:
                key.Values = Pin(values.Buffers[1], pins) + (long)offset * (f.BitWidth / 8);
                break;
        }
    }

    private static byte* Pin(ArrowBuffer buffer, List<MemoryHandle> pins)
    {
        if (buffer.IsEmpty) return null;
        MemoryHandle handle = buffer.Memory.Pin();
        pins.Add(handle);
        return (byte*)handle.Pointer;
    }
}

/// <summary>The keys of one column of every group seen, in native memory: by group id, normalized as read.</summary>
internal sealed class StoredKeys : IDisposable
{
    private readonly NativeArray<byte> _nulls = new();
    private readonly NativeArray<ulong> _fixed = new();
    private readonly NativeArray<Int128> _wide = new();
    private readonly NativeArray<int> _offsets = new(); // _offsets[g].._offsets[g + 1]: group g's bytes
    private readonly NativeArray<byte> _arena = new();
    private int _arenaLength;

    /// <summary>The column's kind and family, from the first batch (null before it).</summary>
    public KeyKind Kind { get; private set; }

    public IArrowType? Family { get; private set; }

    public void Start(KeyKind kind, IArrowType family)
    {
        Kind = kind;
        Family = family;
        _arenaLength = 0;
        _offsets.Reserve(1);
        _offsets[0] = 0;
    }

    public void Reset()
    {
        Family = null;
        _arenaLength = 0;
    }

    /// <summary>Stores group <paramref name="group"/>'s key: value <paramref name="at"/> of the batch column (-1: null).</summary>
    public void Append(int group, in BatchKey key, int at)
    {
        _nulls.Grow(group + 1, group);
        _nulls.At(group) = at < 0 ? (byte)1 : (byte)0;
        switch (Kind)
        {
            case KeyKind.Null:
                break;
            case KeyKind.Fixed128:
                _wide.Grow(group + 1, group);
                _wide.At(group) = at < 0 ? default : key.Wide(at);
                break;
            case KeyKind.Bytes:
            {
                ReadOnlySpan<byte> bytes = at < 0 ? default : key.Bytes(at);
                int end = checked(_arenaLength + bytes.Length);
                _arena.Grow(end, _arenaLength);
                bytes.CopyTo(_arena.AsSpan(_arenaLength, bytes.Length));
                _arenaLength = end;
                _offsets.Grow(group + 2, group + 1);
                _offsets.At(group + 1) = end;
                break;
            }
            default:
                _fixed.Grow(group + 1, group);
                _fixed.At(group) = at < 0 ? 0 : key.Fixed(at);
                break;
        }
    }

    /// <summary>Whether group <paramref name="group"/>'s key is value <paramref name="at"/> of the batch column (-1: null).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Matches(int group, in BatchKey key, int at)
    {
        bool isNull = _nulls.At(group) != 0;
        if (at < 0 || isNull) return at < 0 && isNull;
        return Kind switch
        {
            KeyKind.Fixed128 => _wide.At(group) == key.Wide(at),
            KeyKind.Bytes => _arena.AsSpan(_offsets.At(group), _offsets.At(group + 1) - _offsets.At(group)).SequenceEqual(key.Bytes(at)),
            _ => _fixed.At(group) == key.Fixed(at),
        };
    }

    public long ScratchBytes => _nulls.Capacity + 8L * _fixed.Capacity + 16L * _wide.Capacity + 4L * _offsets.Capacity + _arena.Capacity;

    public void Dispose()
    {
        _nulls.Dispose();
        _fixed.Dispose();
        _wide.Dispose();
        _offsets.Dispose();
        _arena.Dispose();
    }
}
