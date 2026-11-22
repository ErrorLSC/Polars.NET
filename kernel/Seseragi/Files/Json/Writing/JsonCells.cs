using System.Buffers;
using System.Buffers.Text;
using System.Numerics;
using System.Text.Unicode;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Sazanami.Text;

namespace Kaminari.Seseragi.Files.Json.Writing;

/// <summary>The buffers of a batch pinned while its cells are written (native memory stays put; managed memory is pinned).</summary>
internal sealed unsafe class Pins : IDisposable
{
    private readonly List<MemoryHandle> _handles = [];

    public byte* Pin(ArrowBuffer buffer)
    {
        if (buffer.IsEmpty) return null;
        MemoryHandle handle = buffer.Memory.Pin();
        _handles.Add(handle);
        return (byte*)handle.Pointer;
    }

    public void Dispose()
    {
        foreach (MemoryHandle handle in _handles) handle.Dispose();
        _handles.Clear();
    }
}

/// <summary>
/// Writes the values of one Arrow array as JSON, row by row (a row is the array's own index, before its offset). A
/// value JSON cannot hold as it is (a string that is not UTF-8, a map key that is null) is a
/// <see cref="JsonValueException"/>.
/// </summary>
internal abstract unsafe class JsonCell
{
    private readonly byte* _validity;
    private readonly bool _allNull;

    protected JsonCell(ArrayData data, Pins pins, string path)
    {
        Path = path;
        Offset = data.Offset;
        _allNull = data.DataType.TypeId == ArrowTypeId.Null;
        if (!_allNull && data.NullCount != 0 && data.Buffers.Length > 0) _validity = pins.Pin(data.Buffers[0]);
    }

    /// <summary>The column's name (dotted through struct fields) for errors.</summary>
    public string Path { get; }

    /// <summary>The array's offset (added to rows to reach its buffers).</summary>
    protected int Offset { get; }

    public bool IsNull(long row)
    {
        if (_validity == null) return _allNull;
        long bit = row + Offset;
        return (_validity[bit >> 3] & (1 << (int)(bit & 7))) == 0;
    }

    /// <summary>Writes a row's value (not null).</summary>
    public abstract void Write(ref JsonOut o, long row);

    public void WriteOrNull(ref JsonOut o, long row)
    {
        if (IsNull(row)) o.Bytes("null"u8);
        else Write(ref o, row);
    }

    /// <summary>
    /// Fails with <see cref="NotSupportedException"/> when <paramref name="field"/>'s type cannot be written as JSON (or
    /// its time zone is not known).
    /// </summary>
    public static void Check(Field field, string path)
    {
        switch (field.DataType)
        {
            case TimestampType { Timezone: { Length: > 0 } zone } when TimeZoneOffsets.For(zone) is null:
                throw new NotSupportedException($"Column '{path}': the time zone '{zone}' is not known.");
            case Decimal256Type:
                throw new NotSupportedException($"Column '{path}': {field.DataType.Name} cannot be written as JSON.");
            case MapType m:
                if (m.KeyField.DataType is not (StringType or LargeStringType or StringViewType))
                    throw new NotSupportedException($"Column '{path}': the keys of a map written as JSON are strings, not {m.KeyField.DataType.Name}.");
                Check(m.ValueField, path + "{}");
                break;
            case DictionaryType d:
                Check(new Field(field.Name, d.ValueType, true), path);
                break;
            case NestedType n and (StructType or ListType or LargeListType or FixedSizeListType or ListViewType or LargeListViewType):
                foreach (Field child in n.Fields) Check(child, n is StructType ? Child(path, child.Name) : path + "[]");
                break;
            case NullType or BooleanType or Int8Type or Int16Type or Int32Type or Int64Type or UInt8Type or UInt16Type or UInt32Type or UInt64Type
                or HalfFloatType or FloatType or DoubleType or Decimal32Type or Decimal64Type or Decimal128Type or StringType or LargeStringType or StringViewType
                or BinaryType or LargeBinaryType or BinaryViewType or FixedSizeBinaryType or Date32Type or Date64Type
                or TimestampType or Time32Type or Time64Type or DurationType:
                break;
            default:
                throw new NotSupportedException($"Column '{path}': {field.DataType.Name} cannot be written as JSON.");
        }
    }

    public static string Child(string path, string name) => path.Length == 0 ? name : path + "." + name;

    /// <summary>The cell of <paramref name="data"/> (of <paramref name="field"/>'s type, checked) over <paramref name="pins"/>.</summary>
    public static JsonCell For(ArrayData data, Field field, Pins pins, string path) => field.DataType switch
    {
        NullType => new NullCell(data, pins, path),
        BooleanType => new BooleanCell(data, pins, path),
        Int8Type => new IntegerCell<sbyte>(data, pins, path),
        Int16Type => new IntegerCell<short>(data, pins, path),
        Int32Type => new IntegerCell<int>(data, pins, path),
        Int64Type => new IntegerCell<long>(data, pins, path),
        UInt8Type => new IntegerCell<byte>(data, pins, path),
        UInt16Type => new IntegerCell<ushort>(data, pins, path),
        UInt32Type => new IntegerCell<uint>(data, pins, path),
        UInt64Type => new IntegerCell<ulong>(data, pins, path),
        HalfFloatType => new FloatCell<Half>(data, pins, path),
        FloatType => new FloatCell<float>(data, pins, path),
        DoubleType => new FloatCell<double>(data, pins, path),
        Decimal32Type d => new DecimalCell(data, pins, path, d.Scale, null, width: 4),
        Decimal64Type d => new DecimalCell(data, pins, path, d.Scale, null, width: 8),
        Decimal128Type d => new DecimalCell(data, pins, path, d.Scale, WideIntegers.KindOf(field), width: 16),
        BinaryType or LargeBinaryType when IsGuid(field) => new BytesCell(data, pins, path, large: field.DataType is LargeBinaryType, binary: true, guid: true),
        BinaryViewType when IsGuid(field) => new ViewCell(data, pins, path, binary: true, guid: true),
        StringType or BinaryType => new BytesCell(data, pins, path, large: false, binary: field.DataType is BinaryType),
        LargeStringType or LargeBinaryType => new BytesCell(data, pins, path, large: true, binary: field.DataType is LargeBinaryType),
        StringViewType or BinaryViewType => new ViewCell(data, pins, path, binary: field.DataType is BinaryViewType),
        FixedSizeBinaryType f => new FixedBinaryCell(data, pins, path, f.ByteWidth),
        Date32Type => new DateCell(data, pins, path, millis: false),
        Date64Type => new DateCell(data, pins, path, millis: true),
        TimestampType t => new TimestampCell(data, pins, path, UnitNanos(t.Unit), t.Timezone is { Length: > 0 } zone ? TimeZoneOffsets.For(zone) : null),
        Time32Type t => new TimeCell(data, pins, path, UnitNanos(t.Unit), wide: false),
        Time64Type t => new TimeCell(data, pins, path, UnitNanos(t.Unit), wide: true),
        DurationType t => new DurationCell(data, pins, path, UnitNanos(t.Unit)),
        ListType l => new ListCell(data, pins, path, For(data.Children[0], l.ValueField, pins, path + "[]"), ListLayout.Offsets32),
        LargeListType { ValueField.DataType: StructType entry } when field.Metadata?.ContainsKey(Reading.JsonColumn.ObjectKey) == true =>
            new MapCell(data, pins, path, For(data.Children[0].Children[0], entry.Fields[0], pins, path + "{}"),
                For(data.Children[0].Children[1], entry.Fields[1], pins, path + "{}"), large: true),
        LargeListType l => new ListCell(data, pins, path, For(data.Children[0], l.ValueField, pins, path + "[]"), ListLayout.Offsets64),
        ListViewType l => new ListCell(data, pins, path, For(data.Children[0], l.ValueField, pins, path + "[]"), ListLayout.Views32),
        LargeListViewType l => new ListCell(data, pins, path, For(data.Children[0], l.ValueField, pins, path + "[]"), ListLayout.Views64),
        FixedSizeListType l => new FixedListCell(data, pins, path, For(data.Children[0], l.ValueField, pins, path + "[]"), l.ListSize),
        StructType s => new StructCell(data, pins, path, [.. s.Fields.Select((f, i) => (JsonText.Name(f.Name), For(data.Children[i], f, pins, Child(path, f.Name))))]),
        MapType m => new MapCell(data, pins, path, For(data.Children[0].Children[0], m.KeyField, pins, path + "{}"),
            For(data.Children[0].Children[1], m.ValueField, pins, path + "{}")),
        DictionaryType d => new DictionaryCell(data, pins, path, d.IndexType, For(data.Dictionary, new Field(field.Name, d.ValueType, true), pins, path)),
        _ => throw new NotSupportedException($"Column '{path}': {field.DataType.Name} cannot be written as JSON."),
    };

    private static bool IsGuid(Field field) => field.Metadata?.ContainsKey(Reading.JsonColumn.GuidKey) == true;

    private static long UnitNanos(TimeUnit unit) => unit switch
    {
        TimeUnit.Second => 1_000_000_000,
        TimeUnit.Millisecond => 1_000_000,
        TimeUnit.Microsecond => 1_000,
        _ => 1,
    };
}

internal sealed class NullCell(ArrayData data, Pins pins, string path) : JsonCell(data, pins, path)
{
    public override void Write(ref JsonOut o, long row) => o.Bytes("null"u8);
}

internal sealed unsafe class BooleanCell(ArrayData data, Pins pins, string path) : JsonCell(data, pins, path)
{
    private readonly byte* _values = pins.Pin(data.Buffers[1]);

    public override void Write(ref JsonOut o, long row)
    {
        long bit = row + Offset;
        o.Bytes((_values[bit >> 3] & (1 << (int)(bit & 7))) != 0 ? "true"u8 : "false"u8);
    }
}

internal sealed unsafe class IntegerCell<T>(ArrayData data, Pins pins, string path) : JsonCell(data, pins, path)
    where T : unmanaged, IBinaryInteger<T>, IUtf8SpanFormattable
{
    private readonly T* _values = (T*)pins.Pin(data.Buffers[1]);

    public override void Write(ref JsonOut o, long row) => o.Format(_values[row + Offset]);
}

/// <summary>Floats: NaN and infinities are null (as the reference engine writes them).</summary>
internal sealed unsafe class FloatCell<T>(ArrayData data, Pins pins, string path) : JsonCell(data, pins, path)
    where T : unmanaged, IFloatingPointIeee754<T>, IUtf8SpanFormattable
{
    private readonly T* _values = (T*)pins.Pin(data.Buffers[1]);

    public override void Write(ref JsonOut o, long row)
    {
        T value = _values[row + Offset];
        if (T.IsFinite(value)) JsonText.Float(ref o, value);
        else o.Bytes("null"u8);
    }
}

/// <summary>
/// Decimals (32, 64 or 128 bits) as strings with every digit of their scale; 128-bit integers (decimal(38, 0) so
/// marked) as numbers.
/// </summary>
internal sealed unsafe class DecimalCell(ArrayData data, Pins pins, string path, int scale, string? integer, int width) : JsonCell(data, pins, path)
{
    private readonly byte* _values = pins.Pin(data.Buffers[1]);

    public override void Write(ref JsonOut o, long row)
    {
        long index = row + Offset;
        Int128 value = width switch
        {
            4 => ((int*)_values)[index],
            8 => ((long*)_values)[index],
            _ => ((Int128*)_values)[index],
        };
        if (integer is not null)
        {
            if (integer == WideIntegers.Unsigned) o.Format((UInt128)value);
            else o.Format(value);
            return;
        }
        Span<byte> digits = stackalloc byte[48];
        bool negative = value < 0;
        ((UInt128)(negative ? -value : value)).TryFormat(digits, out int length, default, System.Globalization.CultureInfo.InvariantCulture);
        Span<byte> room = o.Room(length + Math.Abs(scale) + 5);
        int at = 0;
        room[at++] = (byte)'"';
        if (negative) room[at++] = (byte)'-';
        if (scale <= 0)
        {
            digits[..length].CopyTo(room[at..]);
            at += length;
            if (value != 0)
                for (int i = 0; i < -scale; i++) room[at++] = (byte)'0';
        }
        else
        {
            int whole = length - scale;
            if (whole <= 0) room[at++] = (byte)'0';
            else
            {
                digits[..whole].CopyTo(room[at..]);
                at += whole;
            }
            room[at++] = (byte)'.';
            for (int i = whole; i < 0; i++) room[at++] = (byte)'0';
            ReadOnlySpan<byte> fraction = digits[Math.Max(0, whole)..length];
            fraction.CopyTo(room[at..]);
            at += fraction.Length;
        }
        room[at++] = (byte)'"';
        o.Advance(at);
    }
}

/// <summary>Strings (checked to be UTF-8) and binaries (as base64 strings).</summary>
internal abstract class TextCell(ArrayData data, Pins pins, string path, bool binary, bool guid = false) : JsonCell(data, pins, path)
{
    protected void WriteValue(ref JsonOut o, scoped ReadOnlySpan<byte> bytes)
    {
        if (guid)
        {
            if (bytes.Length != 16) throw new JsonValueException($"Column '{Path}': {bytes.Length} bytes are not a GUID.");
            Span<byte> text = o.Room(38);
            text[0] = (byte)'"';
            new Guid(bytes).TryFormat(text[1..], out int length, "D");
            text[length + 1] = (byte)'"';
            o.Advance(length + 2);
            return;
        }
        if (binary)
        {
            Span<byte> room = o.Room(Base64.GetMaxEncodedToUtf8Length(bytes.Length) + 2);
            room[0] = (byte)'"';
            Base64.EncodeToUtf8(bytes, room[1..], out _, out int written);
            room[written + 1] = (byte)'"';
            o.Advance(written + 2);
            return;
        }
        if (!JsonText.String(ref o, bytes)) throw new JsonValueException($"Column '{Path}': a string that is not UTF-8.");
    }
}

internal sealed unsafe class BytesCell(ArrayData data, Pins pins, string path, bool large, bool binary, bool guid = false) : TextCell(data, pins, path, binary, guid)
{
    private readonly byte* _offsets = pins.Pin(data.Buffers[1]);
    private readonly byte* _data = pins.Pin(data.Buffers[2]);

    public override void Write(ref JsonOut o, long row)
    {
        long i = row + Offset, start, end;
        if (large)
        {
            start = ((long*)_offsets)[i];
            end = ((long*)_offsets)[i + 1];
        }
        else
        {
            start = ((int*)_offsets)[i];
            end = ((int*)_offsets)[i + 1];
        }
        WriteValue(ref o, new ReadOnlySpan<byte>(_data + start, checked((int)(end - start))));
    }
}

/// <summary>String and binary views: inline up to 12 bytes, else in one of the data buffers.</summary>
internal sealed unsafe class ViewCell : TextCell
{
    private readonly byte* _views;
    private readonly byte*[] _buffers;

    public ViewCell(ArrayData data, Pins pins, string path, bool binary, bool guid = false) : base(data, pins, path, binary, guid)
    {
        _views = pins.Pin(data.Buffers[1]);
        _buffers = new byte*[Math.Max(0, data.Buffers.Length - 2)];
        for (int i = 2; i < data.Buffers.Length; i++) _buffers[i - 2] = pins.Pin(data.Buffers[i]);
    }

    public override void Write(ref JsonOut o, long row)
    {
        byte* view = _views + (row + Offset) * 16;
        int length = *(int*)view;
        ReadOnlySpan<byte> bytes = length <= 12
            ? new ReadOnlySpan<byte>(view + 4, length)
            : new ReadOnlySpan<byte>(_buffers[*(int*)(view + 8)] + *(int*)(view + 12), length);
        WriteValue(ref o, bytes);
    }
}

internal sealed unsafe class FixedBinaryCell(ArrayData data, Pins pins, string path, int width) : TextCell(data, pins, path, binary: true)
{
    private readonly byte* _values = pins.Pin(data.Buffers[1]);

    public override void Write(ref JsonOut o, long row) => WriteValue(ref o, new ReadOnlySpan<byte>(_values + (row + Offset) * width, width));
}

internal sealed unsafe class DateCell(ArrayData data, Pins pins, string path, bool millis) : JsonCell(data, pins, path)
{
    private readonly byte* _values = pins.Pin(data.Buffers[1]);

    public override void Write(ref JsonOut o, long row)
    {
        long days = millis ? Math.DivRem(((long*)_values)[row + Offset], 86_400_000L, out long rest) - (rest < 0 ? 1 : 0) : ((int*)_values)[row + Offset];
        Span<byte> room = o.Room(IsoTemporal.MaxDateBytes + 2);
        room[0] = (byte)'"';
        int at = 1 + IsoTemporal.FormatDate(days, room[1..]);
        room[at++] = (byte)'"';
        o.Advance(at);
    }
}

/// <summary>
/// Timestamps: without a zone the wall clock (<c>yyyy-MM-dd HH:mm:ss.f</c>), with one the zone's local time and
/// offset (<c>yyyy-MM-ddTHH:mm:ss.f±HH:MM</c>).
/// </summary>
internal sealed unsafe class TimestampCell(ArrayData data, Pins pins, string path, long unitNanos, TimeZoneOffsets? zone) : JsonCell(data, pins, path)
{
    private readonly long* _values = (long*)pins.Pin(data.Buffers[1]);

    public override void Write(ref JsonOut o, long row)
    {
        long value = _values[row + Offset];
        long perSecond = 1_000_000_000 / unitNanos;
        long seconds = Math.DivRem(value, perSecond, out long rest);
        if (rest < 0)
        {
            seconds--;
            rest += perSecond;
        }
        int nanos = (int)(rest * unitNanos);
        int offset = zone?.OffsetAt(seconds) ?? 0;
        seconds += offset;
        long days = Math.DivRem(seconds, 86_400L, out long secondOfDay);
        if (secondOfDay < 0)
        {
            days--;
            secondOfDay += 86_400;
        }
        Span<byte> room = o.Room(IsoTemporal.MaxDateBytes + 32);
        room[0] = (byte)'"';
        int at = 1 + IsoTemporal.FormatDate(days, room[1..]);
        room[at++] = zone is null ? (byte)' ' : (byte)'T';
        at += IsoTemporal.FormatTime((int)secondOfDay, nanos, room[at..]);
        if (zone is not null) at += IsoTemporal.FormatOffset(offset, room[at..]);
        room[at++] = (byte)'"';
        o.Advance(at);
    }
}

internal sealed unsafe class TimeCell(ArrayData data, Pins pins, string path, long unitNanos, bool wide) : JsonCell(data, pins, path)
{
    private readonly byte* _values = pins.Pin(data.Buffers[1]);

    public override void Write(ref JsonOut o, long row)
    {
        long value = wide ? ((long*)_values)[row + Offset] : ((int*)_values)[row + Offset];
        long nanos = value * unitNanos;
        if (nanos is < 0 or >= 86_400_000_000_000L) throw new JsonValueException($"Column '{Path}': {value} is not a time of day.");
        Span<byte> room = o.Room(24);
        room[0] = (byte)'"';
        int at = 1 + IsoTemporal.FormatTime((int)(nanos / 1_000_000_000), (int)(nanos % 1_000_000_000), room[1..]);
        room[at++] = (byte)'"';
        o.Advance(at);
    }
}

/// <summary>Durations in ISO 8601 seconds (<c>PT1.5S</c>, <c>-PT90S</c>; zero <c>P0D</c>), as the reference engine writes them.</summary>
internal sealed unsafe class DurationCell(ArrayData data, Pins pins, string path, long unitNanos) : JsonCell(data, pins, path)
{
    private readonly long* _values = (long*)pins.Pin(data.Buffers[1]);

    public override void Write(ref JsonOut o, long row)
    {
        long value = _values[row + Offset];
        if (value == 0)
        {
            o.Bytes("\"P0D\""u8);
            return;
        }
        long perSecond = 1_000_000_000 / unitNanos;
        UInt128 magnitude = value < 0 ? (UInt128)(-(Int128)value) : (UInt128)value;
        UInt128 seconds = magnitude / (ulong)perSecond;
        long nanos = (long)(magnitude % (ulong)perSecond) * unitNanos;
        Span<byte> room = o.Room(48);
        int at = 0;
        room[at++] = (byte)'"';
        if (value < 0) room[at++] = (byte)'-';
        room[at++] = (byte)'P';
        room[at++] = (byte)'T';
        seconds.TryFormat(room[at..], out int written, default, System.Globalization.CultureInfo.InvariantCulture);
        at += written;
        if (nanos != 0)
        {
            room[at++] = (byte)'.';
            Span<byte> fraction = stackalloc byte[9];
            for (int i = 8; i >= 0; i--)
            {
                fraction[i] = (byte)('0' + nanos % 10);
                nanos /= 10;
            }
            ReadOnlySpan<byte> kept = fraction.TrimEnd((byte)'0');
            kept.CopyTo(room[at..]);
            at += kept.Length;
        }
        room[at++] = (byte)'S';
        room[at++] = (byte)'"';
        o.Advance(at);
    }
}

internal enum ListLayout { Offsets32, Offsets64, Views32, Views64 }

/// <summary>Lists, large lists and list views: arrays of their items.</summary>
internal sealed unsafe class ListCell(ArrayData data, Pins pins, string path, JsonCell items, ListLayout layout) : JsonCell(data, pins, path)
{
    private readonly byte* _offsets = pins.Pin(data.Buffers[1]);
    private readonly byte* _sizes = layout is ListLayout.Views32 or ListLayout.Views64 ? pins.Pin(data.Buffers[2]) : null;

    public override void Write(ref JsonOut o, long row)
    {
        long i = row + Offset, start, end;
        switch (layout)
        {
            case ListLayout.Offsets32: start = ((int*)_offsets)[i]; end = ((int*)_offsets)[i + 1]; break;
            case ListLayout.Offsets64: start = ((long*)_offsets)[i]; end = ((long*)_offsets)[i + 1]; break;
            case ListLayout.Views32: start = ((int*)_offsets)[i]; end = start + ((int*)_sizes)[i]; break;
            default: start = ((long*)_offsets)[i]; end = start + ((long*)_sizes)[i]; break;
        }
        o.Byte((byte)'[');
        for (long item = start; item < end; item++)
        {
            if (item > start) o.Byte((byte)',');
            items.WriteOrNull(ref o, item);
        }
        o.Byte((byte)']');
    }
}

internal sealed class FixedListCell(ArrayData data, Pins pins, string path, JsonCell items, int size) : JsonCell(data, pins, path)
{
    public override void Write(ref JsonOut o, long row)
    {
        long start = (row + Offset) * size;
        o.Byte((byte)'[');
        for (int k = 0; k < size; k++)
        {
            if (k > 0) o.Byte((byte)',');
            items.WriteOrNull(ref o, start + k);
        }
        o.Byte((byte)']');
    }
}

/// <summary>Structs: objects of every field, null ones included.</summary>
internal sealed class StructCell(ArrayData data, Pins pins, string path, (byte[] Name, JsonCell Cell)[] fields) : JsonCell(data, pins, path)
{
    public override void Write(ref JsonOut o, long row) => WriteObject(ref o, row + Offset);

    /// <summary>The object of the children's row <paramref name="row"/> (the records themselves have no offset).</summary>
    public void WriteObject(ref JsonOut o, long row)
    {
        o.Byte((byte)'{');
        for (int k = 0; k < fields.Length; k++)
        {
            if (k > 0) o.Byte((byte)',');
            o.Bytes(fields[k].Name);
            fields[k].Cell.WriteOrNull(ref o, row);
        }
        o.Byte((byte)'}');
    }
}

/// <summary>Maps: objects, their (string) keys the names.</summary>
internal sealed unsafe class MapCell(ArrayData data, Pins pins, string path, JsonCell keys, JsonCell values, bool large = false) : JsonCell(data, pins, path)
{
    private readonly byte* _offsets = pins.Pin(data.Buffers[1]);
    private readonly int _entriesOffset = data.Children[0].Offset;

    public override void Write(ref JsonOut o, long row)
    {
        long start = large ? ((long*)_offsets)[row + Offset] : ((int*)_offsets)[row + Offset];
        long end = large ? ((long*)_offsets)[row + Offset + 1] : ((int*)_offsets)[row + Offset + 1];
        o.Byte((byte)'{');
        for (long entry = start + (long)_entriesOffset; entry < end + (long)_entriesOffset; entry++)
        {
            if (entry > start + (long)_entriesOffset) o.Byte((byte)',');
            if (keys.IsNull(entry)) throw new JsonValueException($"Column '{Path}': a map key is null.");
            keys.Write(ref o, entry);
            o.Byte((byte)':');
            values.WriteOrNull(ref o, entry);
        }
        o.Byte((byte)'}');
    }
}

/// <summary>Dictionary-encoded values: the dictionary's value at each index.</summary>
internal sealed unsafe class DictionaryCell(ArrayData data, Pins pins, string path, IArrowType indexType, JsonCell values) : JsonCell(data, pins, path)
{
    private readonly byte* _indices = pins.Pin(data.Buffers[1]);
    private readonly ArrowTypeId _index = indexType.TypeId;

    public override void Write(ref JsonOut o, long row)
    {
        long i = row + Offset;
        long index = _index switch
        {
            ArrowTypeId.Int8 => ((sbyte*)_indices)[i],
            ArrowTypeId.UInt8 => _indices[i],
            ArrowTypeId.Int16 => ((short*)_indices)[i],
            ArrowTypeId.UInt16 => ((ushort*)_indices)[i],
            ArrowTypeId.Int32 => ((int*)_indices)[i],
            ArrowTypeId.UInt32 => ((uint*)_indices)[i],
            ArrowTypeId.Int64 => ((long*)_indices)[i],
            _ => (long)((ulong*)_indices)[i],
        };
        values.WriteOrNull(ref o, index);
    }
}
