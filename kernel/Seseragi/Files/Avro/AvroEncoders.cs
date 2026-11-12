using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;

namespace Kaminari.Seseragi.Files.Avro;

/// <summary>Encodes the value at a logical index of a bound Arrow array as Avro binary. Hot encoders pin the
/// array's buffers at <see cref="Bind"/> and read through raw pointers; <see cref="Release"/> unpins them.</summary>
internal abstract unsafe class AvroEncoder
{
    protected IArrowArray Array = null!;
    private List<MemoryHandle>? _pins;

    public virtual void Bind(IArrowArray array)
    {
        Release();
        Array = array;
    }

    /// <summary>Unpins what <see cref="Bind"/> pinned (children included).</summary>
    public virtual void Release()
    {
        if (_pins is null) return;
        foreach (var h in _pins) h.Dispose();
        _pins.Clear();
    }

    /// <summary>Pins buffer <paramref name="index"/> of the bound array; null when the buffer is empty.</summary>
    protected byte* Pin(int index)
    {
        var buffers = Array.Data.Buffers;
        if (index >= buffers.Length || buffers[index].IsEmpty) return null;
        var handle = buffers[index].Memory.Pin();
        (_pins ??= []).Add(handle);
        return (byte*)handle.Pointer;
    }

    public abstract void Encode(int index, AvroBufferWriter w);

    /// <summary>Encodes the values at [start, end) one after another (list items); plain numeric encoders
    /// override it with a loop that has no virtual call per value.</summary>
    public virtual void EncodeRange(int start, int end, AvroBufferWriter w)
    {
        for (int i = start; i < end; i++) Encode(i, w);
    }

    /// <summary>The array's value buffer as <typeparamref name="T"/>, already sliced to the array's offset.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static ReadOnlySpan<T> Values<T>(IArrowArray a) where T : unmanaged =>
        MemoryMarshal.Cast<byte, T>(a.Data.Buffers[1].Span)[a.Offset..];

    protected static NotSupportedException Lossy(string what) =>
        new($"Cannot write {what} to Avro without losing data.");
}

/// <summary><c>["null", T]</c>: branch 0 for null, branch 1 followed by the value.</summary>
internal sealed unsafe class NullableEncoder(AvroEncoder inner) : AvroEncoder
{
    private byte* _validity;
    private int _offset;

    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        inner.Bind(array);
        _offset = array.Offset;
        _validity = array.NullCount > 0 ? Pin(0) : null;
    }

    public override void Release()
    {
        base.Release();
        inner.Release();
    }

    public override void Encode(int index, AvroBufferWriter w)
    {
        int bit = _offset + index;
        if (_validity != null && (_validity[bit >> 3] & (1 << (bit & 7))) == 0)
        {
            w.WriteLong(0);
            return;
        }
        w.WriteLong(1);
        inner.Encode(index, w);
    }
}

/// <summary>A non-nullable field: refuses a null slot (it has no Avro encoding) instead of writing garbage.</summary>
internal sealed unsafe class RequiredEncoder(AvroEncoder inner, string what) : AvroEncoder
{
    private byte* _validity;
    private int _offset;

    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        inner.Bind(array);
        _offset = array.Offset;
        _validity = array.NullCount > 0 ? Pin(0) : null;
    }

    public override void Release()
    {
        base.Release();
        inner.Release();
    }

    public override void Encode(int index, AvroBufferWriter w)
    {
        int bit = _offset + index;
        if (_validity != null && (_validity[bit >> 3] & (1 << (bit & 7))) == 0)
            throw new NotSupportedException($"Cannot write a null to the non-nullable {what}.");
        inner.Encode(index, w);
    }

    public override void EncodeRange(int start, int end, AvroBufferWriter w)
    {
        if (_validity == null) inner.EncodeRange(start, end, w);
        else base.EncodeRange(start, end, w);
    }
}

internal sealed class NullEncoder : AvroEncoder
{
    public override void Encode(int index, AvroBufferWriter w) { }
}

internal sealed unsafe class BooleanEncoder : AvroEncoder
{
    private byte* _bits;
    private int _offset;

    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        _bits = Pin(1);
        _offset = array.Offset;
    }

    public override void Encode(int index, AvroBufferWriter w)
    {
        int bit = _offset + index;
        w.WriteBoolean((_bits[bit >> 3] & (1 << (bit & 7))) != 0);
    }
}

/// <summary>
/// An integer-valued column written as Avro int/long, optionally rescaled (time units). Values that do not fit,
/// or that a division would truncate, are refused rather than changed.
/// </summary>
internal sealed unsafe class IntegerEncoder<T>(long multiply, long divide, bool toInt, string what) : AvroEncoder
    where T : unmanaged, IBinaryInteger<T>
{
    private T* _values;

    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        _values = (T*)Pin(1) + array.Offset;
    }

    private bool Plain => multiply == 1 && divide == 1 && (typeof(T) == typeof(int) || (typeof(T) == typeof(long) && !toInt));

    public override void EncodeRange(int start, int end, AvroBufferWriter w)
    {
        if (!Plain)
        {
            base.EncodeRange(start, end, w);
            return;
        }
        for (int i = start; i < end; i++)
        {
            T raw = _values[i];
            w.WriteLong(typeof(T) == typeof(int) ? Unsafe.As<T, int>(ref raw) : Unsafe.As<T, long>(ref raw));
        }
    }

    public override void Encode(int index, AvroBufferWriter w)
    {
        T raw = _values[index];
        if (typeof(T) == typeof(int) && multiply == 1 && divide == 1)
        {
            w.WriteLong(Unsafe.As<T, int>(ref raw)); // fast path: plain int
            return;
        }
        if (typeof(T) == typeof(long) && multiply == 1 && divide == 1 && !toInt)
        {
            w.WriteLong(Unsafe.As<T, long>(ref raw)); // fast path: plain long
            return;
        }
        long v;
        try
        {
            v = long.CreateChecked(raw); // only UInt64 beyond long.MaxValue can fail
        }
        catch (OverflowException)
        {
            throw Lossy($"{what} value {raw} (beyond the Avro long range)");
        }
        if (multiply != 1)
        {
            long scaled = v * multiply;
            if (scaled / multiply != v) throw Lossy($"{what} value {v} (overflows when converted)");
            v = scaled;
        }
        if (divide != 1)
        {
            if (v % divide != 0) throw Lossy($"{what} value {v} (its precision exceeds what Avro stores)");
            v /= divide;
        }
        if (toInt && v != (int)v) throw Lossy($"{what} value {v} (beyond the Avro int range)");
        w.WriteLong(v);
    }
}

internal sealed class HalfEncoder : AvroEncoder
{
    public override void Encode(int index, AvroBufferWriter w) => w.WriteFloat((float)Values<Half>(Array)[index]);
}

internal sealed unsafe class FloatEncoder : AvroEncoder
{
    private float* _values;

    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        _values = (float*)Pin(1) + array.Offset;
    }

    public override void Encode(int index, AvroBufferWriter w) => w.WriteFloat(_values[index]);

    public override void EncodeRange(int start, int end, AvroBufferWriter w) =>
        w.WriteFixed(new ReadOnlySpan<byte>(_values + start, (end - start) * sizeof(float)));
}

internal sealed unsafe class DoubleEncoder : AvroEncoder
{
    private double* _values;

    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        _values = (double*)Pin(1) + array.Offset;
    }

    public override void Encode(int index, AvroBufferWriter w) => w.WriteDouble(_values[index]);

    public override void EncodeRange(int start, int end, AvroBufferWriter w) =>
        w.WriteFixed(new ReadOnlySpan<byte>(_values + start, (end - start) * sizeof(double)));
}

/// <summary>String or Binary (32-bit offsets).</summary>
internal sealed unsafe class BinaryEncoder : AvroEncoder
{
    private int* _offsets;
    private byte* _data;

    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        _offsets = (int*)Pin(1) + array.Offset;
        _data = Pin(2);
    }

    public override void Encode(int index, AvroBufferWriter w)
    {
        int start = _offsets[index];
        w.WriteBytes(new ReadOnlySpan<byte>(_data + start, _offsets[index + 1] - start));
    }
}

internal sealed unsafe class LargeBinaryEncoder : AvroEncoder
{
    private long* _offsets;
    private byte* _data;

    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        _offsets = (long*)Pin(1) + array.Offset;
        _data = Pin(2);
    }

    public override void Encode(int index, AvroBufferWriter w)
    {
        long start = _offsets[index];
        w.WriteBytes(new ReadOnlySpan<byte>(_data + start, checked((int)(_offsets[index + 1] - start))));
    }
}

internal sealed class BinaryViewEncoder : AvroEncoder
{
    public override void Encode(int index, AvroBufferWriter w) => w.WriteBytes(((BinaryViewArray)Array).GetBytes(index));
}

internal sealed unsafe class FixedEncoder(int size) : AvroEncoder
{
    private byte* _data;

    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        _data = Pin(1) + (long)array.Offset * size;
    }

    public override void Encode(int index, AvroBufferWriter w) => w.WriteFixed(new ReadOnlySpan<byte>(_data + (long)index * size, size));
}

/// <summary>A little-endian two's-complement decimal of <paramref name="width"/> bytes written as Avro
/// <c>bytes</c> decimal: the shortest big-endian two's complement.</summary>
internal sealed unsafe class DecimalEncoder(int width) : AvroEncoder
{
    private byte* _data;

    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        _data = Pin(1) + (long)array.Offset * width;
    }

    public override void Encode(int index, AvroBufferWriter w)
    {
        var le = new ReadOnlySpan<byte>(_data + (long)index * width, width);
        byte sign = (le[width - 1] & 0x80) != 0 ? (byte)0xFF : (byte)0;
        int length = width;
        // Drop leading sign bytes while the next byte still carries the sign bit.
        while (length > 1 && le[length - 1] == sign && ((le[length - 2] ^ sign) & 0x80) == 0) length--;
        Span<byte> be = stackalloc byte[length];
        for (int i = 0; i < length; i++) be[i] = le[length - 1 - i];
        w.WriteBytes(be);
    }
}

/// <summary>Arrow day-time interval (days, milliseconds) as Avro <c>duration</c>.</summary>
internal sealed class DayTimeDurationEncoder : AvroEncoder
{
    public override void Encode(int index, AvroBufferWriter w)
    {
        var raw = MemoryMarshal.Cast<byte, int>(Array.Data.Buffers[1].Span.Slice((Array.Offset + index) * 8, 8));
        if (raw[0] < 0 || raw[1] < 0) throw Lossy($"the negative interval ({raw[0]} days, {raw[1]} ms) as an Avro duration");
        AvroDurations.Write(w, 0, (uint)raw[0], (uint)raw[1]);
    }
}

/// <summary>Arrow month-day-nano interval as Avro <c>duration</c> (unsigned months, days, milliseconds).</summary>
internal sealed class MonthDayNanoDurationEncoder : AvroEncoder
{
    public override void Encode(int index, AvroBufferWriter w)
    {
        var bytes = Array.Data.Buffers[1].Span.Slice((Array.Offset + index) * 16, 16);
        int months = BinaryPrimitives.ReadInt32LittleEndian(bytes);
        int days = BinaryPrimitives.ReadInt32LittleEndian(bytes[4..]);
        long nanos = BinaryPrimitives.ReadInt64LittleEndian(bytes[8..]);
        if (months < 0 || days < 0 || nanos < 0 || nanos % 1_000_000 != 0 || nanos / 1_000_000 > uint.MaxValue)
            throw Lossy($"the interval ({months} months, {days} days, {nanos} ns) as an Avro duration (unsigned, whole milliseconds)");
        AvroDurations.Write(w, (uint)months, (uint)days, (uint)(nanos / 1_000_000));
    }
}

internal static class AvroDurations
{
    public static void Write(AvroBufferWriter w, uint months, uint days, uint millis)
    {
        Span<byte> b = stackalloc byte[12];
        BinaryPrimitives.WriteUInt32LittleEndian(b, months);
        BinaryPrimitives.WriteUInt32LittleEndian(b[4..], days);
        BinaryPrimitives.WriteUInt32LittleEndian(b[8..], millis);
        w.WriteFixed(b);
    }
}

internal sealed class StructEncoder(AvroEncoder[] fields) : AvroEncoder
{
    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        var s = (StructArray)array;
        for (int i = 0; i < fields.Length; i++) fields[i].Bind(s.Fields[i]);
    }

    public override void Release()
    {
        base.Release();
        foreach (var f in fields) f.Release();
    }

    public override void Encode(int index, AvroBufferWriter w)
    {
        // StructArray.Fields are already sliced to the parent's offset and length.
        foreach (var f in fields) f.Encode(index, w);
    }
}

internal enum ListLayout { Int32Offsets, Int64Offsets, FixedSize }

/// <summary>List, LargeList or FixedSizeList as an Avro array (one block, then the 0 terminator).</summary>
internal sealed unsafe class ListEncoder(ListLayout layout, int fixedSize, AvroEncoder items) : AvroEncoder
{
    private byte* _offsets;
    private int _offset;

    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        _offset = array.Offset;
        _offsets = layout == ListLayout.FixedSize ? null : Pin(1);
        items.Bind(array switch
        {
            ListArray l => l.Values,
            LargeListArray l => l.Values,
            FixedSizeListArray l => l.Values,
            _ => throw new InvalidOperationException($"Unexpected list array {array.GetType().Name}."),
        });
    }

    public override void Release()
    {
        base.Release();
        items.Release();
    }

    public override void Encode(int index, AvroBufferWriter w)
    {
        int at = _offset + index;
        var (start, end) = layout switch
        {
            ListLayout.Int32Offsets => (((int*)_offsets)[at], ((int*)_offsets)[at + 1]),
            ListLayout.Int64Offsets => (checked((int)((long*)_offsets)[at]), checked((int)((long*)_offsets)[at + 1])),
            _ => (at * fixedSize, at * fixedSize + fixedSize),
        };
        if (end > start)
        {
            w.WriteLong(end - start);
            items.EncodeRange(start, end, w);
        }
        w.WriteLong(0);
    }
}

/// <summary>Arrow map with string keys as an Avro map.</summary>
internal sealed class MapEncoder(AvroEncoder keys, AvroEncoder values) : AvroEncoder
{
    private IArrowArray _keys = null!;

    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        // Map offsets index the entries struct, whose Fields are sliced to its own offset.
        var entries = ((MapArray)array).KeyValues;
        _keys = entries.Fields[0];
        keys.Bind(entries.Fields[0]);
        values.Bind(entries.Fields[1]);
    }

    public override void Release()
    {
        base.Release();
        keys.Release();
        values.Release();
    }

    public override void Encode(int index, AvroBufferWriter w)
    {
        var offsets = MemoryMarshal.Cast<byte, int>(Array.Data.Buffers[1].Span);
        int at = Array.Offset + index;
        int start = offsets[at], end = offsets[at + 1];
        if (end > start)
        {
            w.WriteLong(end - start);
            for (int i = start; i < end; i++)
            {
                if (_keys.IsNull(i)) throw new NotSupportedException("Cannot write a null map key to Avro.");
                keys.Encode(i, w);
                values.Encode(i, w);
            }
        }
        w.WriteLong(0);
    }
}

/// <summary>A dictionary-encoded column written as its decoded values.</summary>
internal sealed class DictionaryEncoder<TIndex>(AvroEncoder values) : AvroEncoder where TIndex : unmanaged, IBinaryInteger<TIndex>
{
    private IArrowArray _indices = null!;

    public override void Bind(IArrowArray array)
    {
        base.Bind(array);
        var d = (DictionaryArray)array;
        _indices = d.Indices;
        values.Bind(d.Dictionary);
    }

    public override void Release()
    {
        base.Release();
        values.Release();
    }

    public override void Encode(int index, AvroBufferWriter w) =>
        values.Encode(int.CreateChecked(Values<TIndex>(_indices)[index]), w);
}

/// <summary>Encodes one row of a record batch.</summary>
internal sealed class RowEncoder(AvroEncoder[] columns)
{
    public void Bind(RecordBatch batch)
    {
        for (int i = 0; i < columns.Length; i++) columns[i].Bind(batch.Column(i));
    }

    public void Encode(int row, AvroBufferWriter w)
    {
        foreach (var c in columns) c.Encode(row, w);
    }

    public void Release()
    {
        foreach (var c in columns) c.Release();
    }
}
