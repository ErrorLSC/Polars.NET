using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Kernels;

namespace Kaminari.Sazanami.Arrow.Ipc;

/// <summary>
/// Turns the nodes and buffers of a record batch (or of a dictionary batch's data) into arrays, walking the schema in
/// pre-order as the format flattens it (Columnar.rst, "RecordBatch message"; variadic buffer counts per view field).
/// Only the selected top-level fields are decoded; the others are walked past without touching their buffers.
/// Every array is checked before it is returned (<see cref="IpcValidation"/>): buffer sizes against lengths, null
/// counts against validity bitmaps, offsets, views, union type ids, run ends, dictionary indices.
/// </summary>
internal sealed unsafe class IpcBatchDecoder(IpcRecordBatch header, IpcMetadataVersion version, IpcBody? body, IpcDictionaries? dictionaries,
    bool validateUtf8, bool[]? plan)
{
    private readonly IpcRecordBatch _header = header;
    private readonly IpcMetadataVersion _version = version;
    private readonly IpcBody? _body = body;
    private readonly IpcDictionaries? _dictionaries = dictionaries;
    private readonly bool _validateUtf8 = validateUtf8;
    private readonly bool[]? _plan = plan;
    private int _node, _buffer, _variadic;

    /// <summary>Which buffers of <paramref name="header"/> the <paramref name="selected"/> fields need (null: all fields).</summary>
    /// <exception cref="InvalidDataException">The batch has fewer nodes or buffers than the schema needs, or a buffer lies outside the body.</exception>
    public static bool[] Plan(IpcField[] fields, bool[]? selected, IpcRecordBatch header, IpcMetadataVersion version, long bodyLength)
    {
        foreach (IpcBuffer b in header.Buffers)
        {
            if (b.Offset < 0 || b.Length < 0 || b.Offset > bodyLength - b.Length)
                throw IpcValidation.Invalid("record batch", $"buffer ({b.Offset}, {b.Length}) lies outside the {bodyLength}-byte body");
        }
        var plan = new bool[header.Buffers.Length];
        var walker = new IpcBatchDecoder(header, version, null, null, false, plan);
        for (int i = 0; i < fields.Length; i++) walker.Visit(fields[i], selected is null || selected[i], top: true, fields[i].Field.Name);
        walker.CheckConsumed();
        return plan;
    }

    /// <summary>
    /// The arrays of the <paramref name="selected"/> top-level fields (null for the others), from a body that holds at
    /// least the buffers <see cref="Plan"/> named. Dictionary-encoded fields take their dictionaries from
    /// <paramref name="dictionaries"/>.
    /// </summary>
    /// <exception cref="InvalidDataException">The data is invalid.</exception>
    public static IpcArray?[] Decode(IpcField[] fields, bool[]? selected, IpcRecordBatch header, IpcMetadataVersion version, IpcBody body,
        IpcDictionaries dictionaries, bool validateUtf8)
    {
        var decoder = new IpcBatchDecoder(header, version, body, dictionaries, validateUtf8, null);
        var arrays = new IpcArray?[fields.Length];
        for (int i = 0; i < fields.Length; i++) arrays[i] = decoder.Visit(fields[i], selected is null || selected[i], top: true, fields[i].Field.Name);
        decoder.CheckConsumed();
        return arrays;
    }

    private void CheckConsumed()
    {
        if (_node != _header.Nodes.Length || _buffer != _header.Buffers.Length || _variadic != _header.VariadicCounts.Length)
            throw IpcValidation.Invalid("record batch", $"it has {_header.Nodes.Length} nodes, {_header.Buffers.Length} buffers and "
                + $"{_header.VariadicCounts.Length} variadic counts; its schema takes {_node}, {_buffer} and {_variadic}");
    }

    /// <summary>The number of buffers a field's own layout takes (its children's come after).</summary>
    private int OwnBuffers(IArrowType stored, string where) => stored switch
    {
        NullType or RunEndEncodedType => 0,
        UnionType u => (_version < IpcMetadataVersion.V5 ? 1 : 0) + (u.Mode == UnionMode.Dense ? 2 : 1),
        StructType or FixedSizeListType => 1,
        MapType or ListType or LargeListType => 2,
        ListViewType or LargeListViewType => 3,
        BinaryType or StringType or LargeBinaryType or LargeStringType => 3,
        BinaryViewType or StringViewType => 2 + Variadic(where),
        _ => 2,
    };

    private int Variadic(string where)
    {
        if (_variadic >= _header.VariadicCounts.Length) throw IpcValidation.Invalid(where, "the batch has no variadic buffer count for it");
        long count = _header.VariadicCounts[_variadic++];
        if (count < 0 || count > _header.Buffers.Length) throw IpcValidation.Invalid(where, $"{count} variadic buffers");
        return (int)count;
    }

    private IpcArray? Visit(IpcField field, bool needed, bool top, string where)
    {
        if (_node >= _header.Nodes.Length) throw IpcValidation.Invalid(where, "the batch has no node for it");
        IpcFieldNode node = _header.Nodes[_node++];
        if (node.Length < 0 || node.Length > int.MaxValue) throw IpcValidation.Invalid(where, $"a length of {node.Length}");
        if (node.NullCount < 0 || node.NullCount > node.Length) throw IpcValidation.Invalid(where, $"{node.NullCount} nulls in {node.Length} values");
        if (top && node.Length < _header.Length) throw IpcValidation.Invalid(where, $"{node.Length} values in a batch of {_header.Length} rows");

        IArrowType stored = field.StoredType;
        int first = _buffer;
        int own = OwnBuffers(stored, where);
        if (own > _header.Buffers.Length - first) throw IpcValidation.Invalid(where, "the batch has too few buffers for it");
        _buffer += own;

        // Dictionary-encoded fields hold only their indices here; the values' children live in the dictionary batches.
        IpcField[] children = field.DictionaryId is null ? field.Children : [];
        if (_plan is not null || !needed)
        {
            if (_plan is not null && needed) _plan.AsSpan(first, own).Fill(true);
            foreach (IpcField child in children) Visit(child, needed, false, where + "." + child.Field.Name);
            return null;
        }
        return Decode(field, stored, (int)node.Length, (int)node.NullCount, first, own, children, where);
    }

    private IpcSlice Buffer(int index, string where) => _body!.Slice(index, _header.Buffers[index], _header.Compression, where);

    /// <summary>The validity bitmap: none when there are no nulls; else long enough, its unset bits counting the nulls.</summary>
    private IpcSlice Validity(int index, int length, int nulls, string where)
    {
        if (nulls == 0) return IpcSlice.Empty;
        IpcSlice v = Buffer(index, where);
        if (v.Length < Bytes(length)) throw IpcValidation.Invalid(where, $"a validity bitmap of {v.Length} bytes for {length} values");
        long set = Bits.Count(v.Span, 0, length);
        if (length - set != nulls) throw IpcValidation.Invalid(where, $"its null count {nulls} does not match its validity bitmap ({length - set})");
        return v;
    }

    private IpcSlice Sized(int index, long bytes, string where)
    {
        IpcSlice s = Buffer(index, where);
        if (s.Length < bytes) throw IpcValidation.Invalid(where, $"a buffer of {s.Length} bytes where {bytes} are needed");
        return s;
    }

    /// <summary>Offsets of <paramref name="length"/> + 1 entries (an empty array may leave them out: zeros then).</summary>
    private IpcSlice Offsets(int index, int length, int width, string where)
    {
        IpcSlice s = Buffer(index, where);
        if (s.Length == 0 && length == 0) return new IpcSlice(IpcZeros.Instance, (nint)IpcZeros.Instance.Pointer, width);
        if (s.Length < ((long)length + 1) * width) throw IpcValidation.Invalid(where, $"{s.Length} bytes of offsets for {length} values");
        return s;
    }

    private static long Bytes(long bits) => (bits + 7) >> 3;

    private IpcArray Decode(IpcField field, IArrowType stored, int length, int nulls, int first, int own, IpcField[] childFields, string where)
    {
        IpcArray[] Children() => [.. childFields.Select(c => Visit(c, true, false, where + "." + c.Field.Name)!)];

        switch (stored)
        {
            case NullType:
                return new IpcArray(field.Field.DataType, length, length, [], []);

            case UnionType u:
            {
                int at = first;
                if (_version < IpcMetadataVersion.V5)
                {
                    // Before V5 a union had a validity bitmap of its own; a union with nulls there cannot be read as V5.
                    if (nulls != 0) throw new InvalidDataException($"Arrow IPC data has a union with top-level nulls (metadata {_version}) at {where}, which is not supported.");
                    at++;
                }
                IpcSlice typeIds = Sized(at, length, where);
                IpcSlice offsets = u.Mode == UnionMode.Dense ? Sized(at + 1, 4L * length, where) : IpcSlice.Empty;
                IpcArray[] kids = Children();
                if (u.Mode == UnionMode.Sparse)
                {
                    foreach (IpcArray kid in kids)
                    {
                        if (kid.Length < length) throw IpcValidation.Invalid(where, $"a sparse union child of {kid.Length} values for {length}");
                    }
                }
                IpcValidation.Union(typeIds.Span[..length], u.Mode == UnionMode.Dense ? MemoryMarshal.Cast<byte, int>(offsets.Span)[..length] : default,
                    u.TypeIds, [.. kids.Select(k => k.Length)], u.Mode == UnionMode.Dense, where);
                return new IpcArray(stored, length, 0, u.Mode == UnionMode.Dense ? [typeIds, offsets] : [typeIds], kids);
            }

            case RunEndEncodedType:
            {
                if (nulls != 0) throw IpcValidation.Invalid(where, $"a run-end encoded array with {nulls} nulls of its own");
                IpcArray[] kids = Children();
                IpcArray ends = kids[0];
                if (ends.NullCount != 0) throw IpcValidation.Invalid(where, "its run ends hold nulls");
                if (kids[1].Length < ends.Length) throw IpcValidation.Invalid(where, $"{ends.Length} runs and {kids[1].Length} values");
                ReadOnlySpan<byte> raw = ends.Buffers[1].Span;
                switch (ends.Type)
                {
                    case Int16Type: IpcValidation.RunEnds(MemoryMarshal.Cast<byte, short>(raw)[..ends.Length], length, where); break;
                    case Int32Type: IpcValidation.RunEnds(MemoryMarshal.Cast<byte, int>(raw)[..ends.Length], length, where); break;
                    default: IpcValidation.RunEnds(MemoryMarshal.Cast<byte, long>(raw)[..ends.Length], length, where); break;
                }
                return new IpcArray(stored, length, 0, [], kids);
            }
        }

        IpcSlice validity = Validity(first, length, nulls, where);
        switch (stored)
        {
            case StructType:
            {
                IpcArray[] kids = Children();
                foreach (IpcArray kid in kids)
                {
                    if (kid.Length < length) throw IpcValidation.Invalid(where, $"a struct child of {kid.Length} values for {length}");
                }
                return new IpcArray(stored, length, nulls, [validity], kids);
            }
            case FixedSizeListType f:
            {
                IpcArray[] kids = Children();
                if (kids[0].Length < (long)length * f.ListSize) throw IpcValidation.Invalid(where, $"{kids[0].Length} items for {length} lists of {f.ListSize}");
                return new IpcArray(stored, length, nulls, [validity], kids);
            }
            case MapType or ListType:
            {
                IpcSlice offsets = Offsets(first + 1, length, 4, where);
                IpcArray[] kids = Children();
                IpcValidation.Offsets(MemoryMarshal.Cast<byte, int>(offsets.Span)[..(length + 1)], kids[0].Length, where);
                if (stored is MapType && kids[0].Children[0].NullCount != 0) throw IpcValidation.Invalid(where, "a map key is null");
                return new IpcArray(stored, length, nulls, [validity, offsets], kids);
            }
            case LargeListType:
            {
                IpcSlice offsets = Offsets(first + 1, length, 8, where);
                IpcArray[] kids = Children();
                IpcValidation.Offsets(MemoryMarshal.Cast<byte, long>(offsets.Span)[..(length + 1)], kids[0].Length, where);
                return new IpcArray(stored, length, nulls, [validity, offsets], kids);
            }
            case ListViewType or LargeListViewType:
            {
                int width = stored is ListViewType ? 4 : 8;
                IpcSlice offsets = Sized(first + 1, (long)width * length, where), sizes = Sized(first + 2, (long)width * length, where);
                IpcArray[] kids = Children();
                if (width == 4) IpcValidation.ListViews(MemoryMarshal.Cast<byte, int>(offsets.Span)[..length], MemoryMarshal.Cast<byte, int>(sizes.Span)[..length], validity.Span, kids[0].Length, where);
                else IpcValidation.ListViews(MemoryMarshal.Cast<byte, long>(offsets.Span)[..length], MemoryMarshal.Cast<byte, long>(sizes.Span)[..length], validity.Span, kids[0].Length, where);
                return new IpcArray(stored, length, nulls, [validity, offsets, sizes], kids);
            }
            case BinaryType or StringType:
            {
                IpcSlice offsets = Offsets(first + 1, length, 4, where);
                IpcSlice data = Buffer(first + 2, where);
                ReadOnlySpan<int> o = MemoryMarshal.Cast<byte, int>(offsets.Span)[..(length + 1)];
                IpcValidation.Offsets(o, data.Length, where);
                if (_validateUtf8 && stored is StringType) IpcValidation.Utf8Strings(o, data.Span, validity.Span, where);
                return new IpcArray(stored, length, nulls, [validity, offsets, data], []);
            }
            case LargeBinaryType or LargeStringType:
            {
                IpcSlice offsets = Offsets(first + 1, length, 8, where);
                IpcSlice data = Buffer(first + 2, where);
                ReadOnlySpan<long> o = MemoryMarshal.Cast<byte, long>(offsets.Span)[..(length + 1)];
                IpcValidation.Offsets(o, data.Length, where);
                if (_validateUtf8 && stored is LargeStringType) IpcValidation.Utf8Strings(o, data.Span, validity.Span, where);
                return new IpcArray(stored, length, nulls, [validity, offsets, data], []);
            }
            case BinaryViewType or StringViewType:
            {
                IpcSlice views = Sized(first + 1, 16L * length, where);
                var data = new IpcSlice[own - 2];
                for (int i = 0; i < data.Length; i++) data[i] = Buffer(first + 2 + i, where);
                IpcValidation.Views(views.Span, length, validity.Span, data, where);
                if (_validateUtf8 && stored is StringViewType) IpcValidation.Utf8Views(views.Span, length, validity.Span, data, where);
                return new IpcArray(stored, length, nulls, [validity, views, .. data], []);
            }
        }

        // Fixed width: booleans, numbers, temporal values, intervals, decimals, fixed-size binary, dictionary indices.
        if (stored is BooleanType) return new IpcArray(stored, length, nulls, [validity, Sized(first + 1, Bytes(length), where)], []);
        int valueWidth = stored switch
        {
            _ when field.Conversion == IpcConversion.UInt128AsDecimal256 => 16, // stored as 128-bit integers
            FixedSizeBinaryType b => b.ByteWidth,
            FixedWidthType w when w.BitWidth % 8 == 0 => w.BitWidth / 8,
            _ => throw new InvalidDataException($"Arrow IPC type {stored.Name} at {where} is not supported."),
        };
        IpcSlice values = Sized(first + 1, (long)valueWidth * length, where);

        if (field.DictionaryId is long id)
        {
            IpcArray dictionary = _dictionaries!.Get(id, field, nulls == length, where);
            ReadOnlySpan<byte> raw = values.Span;
            ReadOnlySpan<byte> valid = validity.Span;
            switch (stored)
            {
                case Int8Type: IpcValidation.Indices(MemoryMarshal.Cast<byte, sbyte>(raw)[..length], valid, dictionary.Length, where); break;
                case Int16Type: IpcValidation.Indices(MemoryMarshal.Cast<byte, short>(raw)[..length], valid, dictionary.Length, where); break;
                case Int32Type: IpcValidation.Indices(MemoryMarshal.Cast<byte, int>(raw)[..length], valid, dictionary.Length, where); break;
                case Int64Type: IpcValidation.Indices(MemoryMarshal.Cast<byte, long>(raw)[..length], valid, dictionary.Length, where); break;
                case UInt8Type: IpcValidation.Indices(raw[..length], valid, dictionary.Length, where); break;
                case UInt16Type: IpcValidation.Indices(MemoryMarshal.Cast<byte, ushort>(raw)[..length], valid, dictionary.Length, where); break;
                case UInt32Type: IpcValidation.Indices(MemoryMarshal.Cast<byte, uint>(raw)[..length], valid, dictionary.Length, where); break;
                default: IpcValidation.Indices(MemoryMarshal.Cast<byte, ulong>(raw)[..length], valid, dictionary.Length, where); break;
            }
            return new IpcArray(field.Field.DataType, length, nulls, [validity, values], [], dictionary);
        }

        if (field.Conversion == IpcConversion.UInt128AsDecimal256)
        {
            // Zero-extended into 32-byte decimals.
            byte* wide = _body!.Memory.Allocate(32L * length);
            var target = new Span<byte>(wide, 32 * length);
            target.Clear();
            ReadOnlySpan<byte> source = values.Span;
            for (int i = 0; i < length; i++) source.Slice(16 * i, 16).CopyTo(target[(32 * i)..]);
            values = new IpcSlice(_body.Memory, (nint)wide, 32 * length);
        }
        return new IpcArray(field.Field.DataType, length, nulls, [validity, values], []);
    }

    /// <summary>An empty array of <paramref name="type"/> (a dictionary that never came, for a column that is all null).</summary>
    public static IpcArray Empty(IArrowType type)
    {
        var zeros = new IpcSlice(IpcZeros.Instance, (nint)IpcZeros.Instance.Pointer, 8);
        return type switch
        {
            NullType => new IpcArray(type, 0, 0, [], []),
            UnionType u => new IpcArray(type, 0, 0, u.Mode == UnionMode.Dense ? [IpcSlice.Empty, IpcSlice.Empty] : [IpcSlice.Empty],
                [.. u.Fields.Select(f => Empty(f.DataType))]),
            RunEndEncodedType r => new IpcArray(type, 0, 0, [], [Empty(r.RunEndsDataType), Empty(r.ValuesDataType)]),
            StructType s => new IpcArray(type, 0, 0, [IpcSlice.Empty], [.. s.Fields.Select(f => Empty(f.DataType))]),
            FixedSizeListType f => new IpcArray(type, 0, 0, [IpcSlice.Empty], [Empty(f.ValueDataType)]),
            MapType m => new IpcArray(type, 0, 0, [IpcSlice.Empty, zeros], [Empty(m.KeyValueType)]),
            ListType l => new IpcArray(type, 0, 0, [IpcSlice.Empty, zeros], [Empty(l.ValueDataType)]),
            LargeListType l => new IpcArray(type, 0, 0, [IpcSlice.Empty, zeros], [Empty(l.ValueDataType)]),
            ListViewType l => new IpcArray(type, 0, 0, [IpcSlice.Empty, IpcSlice.Empty, IpcSlice.Empty], [Empty(l.ValueDataType)]),
            LargeListViewType l => new IpcArray(type, 0, 0, [IpcSlice.Empty, IpcSlice.Empty, IpcSlice.Empty], [Empty(l.ValueDataType)]),
            BinaryType or StringType or LargeBinaryType or LargeStringType => new IpcArray(type, 0, 0, [IpcSlice.Empty, zeros, IpcSlice.Empty], []),
            BinaryViewType or StringViewType => new IpcArray(type, 0, 0, [IpcSlice.Empty, IpcSlice.Empty], []),
            DictionaryType d => new IpcArray(type, 0, 0, [IpcSlice.Empty, IpcSlice.Empty], [], Empty(d.ValueType)),
            _ => new IpcArray(type, 0, 0, [IpcSlice.Empty, IpcSlice.Empty], []),
        };
    }

    /// <summary>The first <paramref name="rows"/> rows of a top-level array (the null count of the prefix recounted).</summary>
    public static IpcArray Prefix(IpcArray array, int rows)
    {
        if (rows >= array.Length) return array;
        int nulls = array.Type switch
        {
            NullType => rows,
            UnionType or RunEndEncodedType => 0,
            _ when array.NullCount == 0 => 0,
            _ => rows - (int)Bits.Count(array.Buffers[0].Span, 0, rows),
        };
        return array.Prefix(rows, nulls);
    }
}
