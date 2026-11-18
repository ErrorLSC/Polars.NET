using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Encodings;

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
        if (EmptyIsNull && Kind == KeyKind.Bytes && Length(at) == 0) return -1;
        return at;
    }

    /// <summary>Whether every row is its own value, never null: no validity, no dictionary, no empty taken as null.</summary>
    public readonly bool Plain => RowValidity == null && Indices == null && ValueValidity == null && !(EmptyIsNull && Kind == KeyKind.Bytes);

    /// <summary>The length of a byte-string value (without reading its bytes where the layout tells it).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly int Length(int at) => Layout switch
    {
        BytesLayout.Views => *(int*)(Values + 16L * at),
        BytesLayout.Offsets32 => ((int*)Offsets)[at + 1] - ((int*)Offsets)[at],
        BytesLayout.Offsets64 => checked((int)(((long*)Offsets)[at + 1] - ((long*)Offsets)[at])),
        _ => Width,
    };

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

/// <summary>
/// The keys of one column of every group seen, in native memory, by group id: each as its image (see
/// <see cref="KeyImage"/>), byte strings also by their bytes.
/// </summary>
internal sealed class StoredKeys : IDisposable
{
    private readonly NativeArray<byte> _nulls = new();
    private readonly NativeArray<UInt128> _images = new();
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

    /// <summary>
    /// Stores group <paramref name="group"/>'s key: value <paramref name="at"/> of the batch column (-1: null), whose
    /// image is <paramref name="image"/>.
    /// </summary>
    public void Append(int group, in BatchKey key, int at, UInt128 image)
    {
        _nulls.Grow(group + 1, group);
        _nulls.At(group) = at < 0 ? (byte)1 : (byte)0;
        _images.Grow(group + 1, group);
        _images.At(group) = image;
        if (Kind != KeyKind.Bytes) return;
        ReadOnlySpan<byte> bytes = at < 0 ? default : key.Bytes(at);
        int end = checked(_arenaLength + bytes.Length);
        _arena.Grow(end, _arenaLength);
        bytes.CopyTo(_arena.AsSpan(_arenaLength, bytes.Length));
        _arenaLength = end;
        _offsets.Grow(group + 2, group + 1);
        _offsets.At(group + 1) = end;
    }

    /// <summary>
    /// Whether group <paramref name="group"/>'s key is value <paramref name="at"/> of the batch column (-1: null), whose
    /// image is <paramref name="image"/>: the images decide but for byte strings longer than an image holds.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Matches(int group, in BatchKey key, int at, UInt128 image)
    {
        bool isNull = _nulls.At(group) != 0;
        if (at < 0 || isNull) return at < 0 && isNull;
        if (_images.At(group) != image) return false;
        if (Kind != KeyKind.Bytes || KeyImage.Whole(image)) return true;
        return _arena.AsSpan(_offsets.At(group), _offsets.At(group + 1) - _offsets.At(group)).SequenceEqual(key.Bytes(at));
    }

    public long ScratchBytes => _nulls.Capacity + 16L * _images.Capacity + 4L * _offsets.Capacity + _arena.Capacity;

    public void Dispose()
    {
        _nulls.Dispose();
        _images.Dispose();
        _offsets.Dispose();
        _arena.Dispose();
    }
}

/// <summary>
/// A key value as 128 bits, read once per row, and its hash: what the rows' hashes fold and what stored keys are
/// compared to. Values of at most 64 bits are their normalized value (<see cref="BatchKey.Fixed"/>), which is also their
/// hash; 128-bit values themselves. A byte string's image is its length (32 bits) and its first 12 bytes, zero-padded:
/// the layout of an inline Arrow view, read from a view without its bytes. A string of at most 12 bytes is whole in its
/// image (equal images, equal strings) and hashed from it; a longer one is hashed from all its bytes, and compared by
/// them once the images agree. Images and hashes do not depend on the layout (views, offsets, large offsets).
/// </summary>
internal static unsafe class KeyImage
{
    private const ulong K = 0xFF51_AFD7_ED55_8CCDUL;

    /// <summary>The longest byte string an image holds whole.</summary>
    public const int InlineBytes = 12;

    /// <summary>Whether a byte string's image holds it whole.</summary>
    public static bool Whole(UInt128 image) => (uint)(ulong)image <= InlineBytes;

    /// <summary>The hash of a 128-bit value (or of a whole byte string's image).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Hash128(ulong low, ulong high) => low ^ BitOperations.RotateLeft(high * K, 29);

    /// <summary>The image of a byte string, and its hash.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static UInt128 OfBytes(ReadOnlySpan<byte> bytes, out ulong hash)
    {
        UInt128 image = 0;
        Span<byte> parts = MemoryMarshal.AsBytes(new Span<UInt128>(ref image));
        BinaryPrimitives.WriteInt32LittleEndian(parts, bytes.Length);
        bytes[..Math.Min(bytes.Length, InlineBytes)].CopyTo(parts[4..]);
        hash = bytes.Length <= InlineBytes ? Hash128((ulong)image, (ulong)(image >> 64)) : ByteHash.Of(bytes);
        return image;
    }

    /// <summary>The image of view <paramref name="at"/>, and its hash (an inline view's from the view alone).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static UInt128 OfView(in BatchKey key, int at, out ulong hash)
    {
        byte* view = key.Values + 16L * at;
        int length = *(int*)view;
        if (length > InlineBytes) return OfBytes(key.Bytes(at), out hash);
        ulong low = Unsafe.ReadUnaligned<ulong>(view), high = Unsafe.ReadUnaligned<ulong>(view + 8);
        // The bytes past the string are padding: zero whatever the writer left there.
        if (length < 4)
        {
            low &= (1UL << (32 + 8 * length)) - 1;
            high = 0;
        }
        else if (length < InlineBytes) high &= (1UL << (8 * (length - 4))) - 1;
        hash = Hash128(low, high);
        return new UInt128(high, low);
    }
}

/// <summary>Reads value <c>at</c> of a batch column of one kind and layout as its image and hash (a type per kind: loops specialized by it).</summary>
internal interface IKeyReader
{
    static abstract UInt128 Read(in BatchKey key, int at, out ulong hash);
}

internal unsafe struct Fixed8Reader : IKeyReader
{
    public static UInt128 Read(in BatchKey key, int at, out ulong hash) => hash = key.Values[at];
}

internal unsafe struct Fixed16Reader : IKeyReader
{
    public static UInt128 Read(in BatchKey key, int at, out ulong hash) => hash = Unsafe.ReadUnaligned<ushort>(key.Values + 2L * at);
}

internal unsafe struct Fixed32Reader : IKeyReader
{
    public static UInt128 Read(in BatchKey key, int at, out ulong hash) => hash = Unsafe.ReadUnaligned<uint>(key.Values + 4L * at);
}

internal unsafe struct Fixed64Reader : IKeyReader
{
    public static UInt128 Read(in BatchKey key, int at, out ulong hash) => hash = Unsafe.ReadUnaligned<ulong>(key.Values + 8L * at);
}

/// <summary>Booleans and floats: <see cref="BatchKey.Fixed"/> normalizes them.</summary>
internal struct NormalizedReader : IKeyReader
{
    public static UInt128 Read(in BatchKey key, int at, out ulong hash) => hash = key.Fixed(at);
}

internal struct Fixed128Reader : IKeyReader
{
    public static UInt128 Read(in BatchKey key, int at, out ulong hash)
    {
        var value = (UInt128)key.Wide(at);
        hash = KeyImage.Hash128((ulong)value, (ulong)(value >> 64));
        return value;
    }
}

internal struct ViewReader : IKeyReader
{
    public static UInt128 Read(in BatchKey key, int at, out ulong hash) => KeyImage.OfView(in key, at, out hash);
}

/// <summary>Byte strings with offsets (plain, large) or of a fixed width.</summary>
internal struct BytesReader : IKeyReader
{
    public static UInt128 Read(in BatchKey key, int at, out ulong hash) => KeyImage.OfBytes(key.Bytes(at), out hash);
}
