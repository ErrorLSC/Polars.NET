using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Filters;
using Kaminari.Sazanami.Kernels;

namespace Kaminari.Sazanami.Arrow.Builders;

/// <summary>
/// Rows of record batches of one schema appended as ranges, copied into growable native buffers, then taken as one
/// batch over native memory of its own (<see cref="Take"/>): small runs of rows (a partition's rows of each batch a
/// partitioned writer gets) gathered into batches of a useful size, each row copied once. The inputs may be disposed
/// once <see cref="TryAppend"/> returns.
/// </summary>
/// <remarks>
/// Layouts: flat values (fixed widths, booleans, fixed-size binaries, strings and binaries plain, large and as views:
/// long view values copied into a data buffer of the appender's), structs, lists, large lists, maps, fixed-size lists
/// and dictionaries (the dictionary of the rows appended so far kept; rows of another dictionary are refused: take the
/// batch first). Other types (unions, run-end encoded, list views) are not supported (<see cref="Supports"/>).
/// </remarks>
internal sealed unsafe class ArrowBatchAppender : IDisposable
{
    private readonly Schema _schema;
    private readonly Column[] _columns;

    public ArrowBatchAppender(Schema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        if (!Supports(schema)) throw new NotSupportedException("The schema has columns of a type the appender does not take.");
        _schema = schema;
        _columns = [.. schema.FieldsList.Select(f => Column.For(f.DataType))];
    }

    /// <summary>The rows appended since the last <see cref="Take"/>.</summary>
    public int Rows { get; private set; }

    /// <summary>The bytes the appended rows hold (their buffers' lengths).</summary>
    public long Bytes
    {
        get
        {
            long bytes = 0;
            foreach (Column column in _columns) bytes += column.Bytes;
            return bytes;
        }
    }

    /// <summary>Whether every column of <paramref name="schema"/> is of a type the appender takes.</summary>
    public static bool Supports(Schema schema) => schema.FieldsList.All(f => Supports(f.DataType));

    private static bool Supports(IArrowType type) => type switch
    {
        NullType or BooleanType or FixedSizeBinaryType or FixedWidthType => type is not DictionaryType || Supports(((DictionaryType)type).ValueType),
        StringType or BinaryType or LargeStringType or LargeBinaryType or StringViewType or BinaryViewType => true,
        StructType s => s.Fields.All(f => Supports(f.DataType)),
        ListType l => Supports(l.ValueDataType),
        LargeListType l => Supports(l.ValueDataType),
        MapType m => Supports(m.KeyValueType),
        FixedSizeListType f => Supports(f.ValueDataType),
        _ => false,
    };

    /// <summary>
    /// Appends rows <paramref name="start"/> .. +<paramref name="count"/> of <paramref name="batch"/> (of the schema's
    /// columns); false, appending nothing, when they cannot join the rows appended (a dictionary other than theirs, or
    /// more bytes than a buffer takes): take the batch first.
    /// </summary>
    public bool TryAppend(RecordBatch batch, int start, int count)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentOutOfRangeException.ThrowIfNegative(start);
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, batch.Length - start);
        if (batch.ColumnCount != _columns.Length) throw new ArgumentException($"A batch of {batch.ColumnCount} columns; the appender's schema has {_columns.Length}.", nameof(batch));
        if (count == 0) return true;
        if ((long)Rows + count > int.MaxValue) return false;
        for (int c = 0; c < _columns.Length; c++)
        {
            if (!_columns[c].Accepts(batch.Column(c).Data, start, count)) return false;
        }
        for (int c = 0; c < _columns.Length; c++) _columns[c].Append(batch.Column(c).Data, start, count);
        Rows += count;
        return true;
    }

    /// <summary>The rows appended, as a batch over memory of its own (the appender starts empty again).</summary>
    public RecordBatch Take()
    {
        var export = new NativeBatchExport();
        try
        {
            var children = new nint[_columns.Length];
            for (int c = 0; c < children.Length; c++) children[c] = (nint)_columns[c].Build(export);
            return export.Import(export.NewArray(Rows, 0, [], children), _schema);
        }
        catch
        {
            export.FreeAll();
            throw;
        }
        finally
        {
            foreach (Column column in _columns) column.Reset();
            Rows = 0;
        }
    }

    /// <summary>Whether row <paramref name="i"/> (logical) of <paramref name="data"/> is valid.</summary>
    private static bool ValidAt(ArrayData data, int i) =>
        data.NullCount == 0 || data.Buffers[0].IsEmpty || (data.Buffers[0].Span[(data.Offset + i) >> 3] >> ((data.Offset + i) & 7) & 1) != 0;

    public void Dispose()
    {
        foreach (Column column in _columns) column.Reset();
        Rows = 0;
    }

    /// <summary>A column's appended rows: its validity, then what its layout holds.</summary>
    private abstract class Column
    {
        private NativeGrowBuffer _validity; // materialized at the first null (every row before valid)
        private bool _hasValidity;
        private long _nulls;

        /// <summary>The rows appended.</summary>
        protected long Length { get; private set; }

        public static Column For(IArrowType type) => type switch
        {
            NullType => new Nulls(),
            DictionaryType d => new Dictionaries(((FixedWidthType)d.IndexType).BitWidth / 8, For(d.ValueType)),
            BooleanType => new Booleans(),
            FixedSizeBinaryType f => new Fixed(f.ByteWidth),
            FixedWidthType f => new Fixed(f.BitWidth / 8),
            StringType or BinaryType => new Offsets<int>(),
            LargeStringType or LargeBinaryType => new Offsets<long>(),
            StringViewType or BinaryViewType => new Views(),
            StructType s => new Structs([.. s.Fields.Select(f => For(f.DataType))]),
            ListType l => new Lists<int>(For(l.ValueDataType)),
            MapType m => new Lists<int>(For(m.KeyValueType)),
            LargeListType l => new Lists<long>(For(l.ValueDataType)),
            FixedSizeListType f => new FixedSizeLists(f.ListSize, For(f.ValueDataType)),
            _ => throw new NotSupportedException($"Appending rows of {type.Name} arrays is not supported."),
        };

        public virtual long Bytes => _validity.Length;

        /// <summary>Whether rows <paramref name="start"/> .. +<paramref name="count"/> of <paramref name="data"/> can join those appended.</summary>
        public virtual bool Accepts(ArrayData data, int start, int count) => true;

        public void Append(ArrayData data, int start, int count)
        {
            AppendValidity(data, start, count);
            AppendValues(data, start, count);
            Length += count;
        }

        protected abstract void AppendValues(ArrayData data, int start, int count);

        /// <summary>The column's array (its buffers detached into <paramref name="export"/>).</summary>
        public NativeCArray* Build(NativeBatchExport export)
        {
            nint validity = 0;
            if (_hasValidity && _nulls > 0) validity = export.Ptr(ref _validity);
            return BuildValues(export, validity, _nulls);
        }

        protected abstract NativeCArray* BuildValues(NativeBatchExport export, nint validity, long nulls);

        /// <summary>Frees what is held (nothing once built) and starts empty.</summary>
        public virtual void Reset()
        {
            _validity.Free();
            _hasValidity = false;
            _nulls = 0;
            Length = 0;
        }

        private void AppendValidity(ArrayData data, int start, int count)
        {
            if (data.DataType is NullType) return; // no buffers at all: every row null
            bool bitmap = data.NullCount != 0 && !data.Buffers[0].IsEmpty;
            if (!bitmap && !_hasValidity) return;
            if (!_hasValidity)
            {
                _hasValidity = true;
                Bitmaps.AppendOnes(ref _validity, 0, Length);
            }
            if (bitmap)
            {
                Bitmaps.AppendBits(ref _validity, Length, data.Buffers[0].Span, data.Offset + start, count);
                _nulls += count - Bits.Count(data.Buffers[0].Span, data.Offset + start, count);
            }
            else Bitmaps.AppendOnes(ref _validity, Length, count);
        }
    }

    /// <summary>Bitmaps in grow buffers: their bytes cover the bits (with 8 bytes of room past them for word-wide merges).</summary>
    private static class Bitmaps
    {
        private static ReadOnlySpan<byte> Ones => [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];

        /// <summary>Appends <paramref name="count"/> bits of <paramref name="source"/> from <paramref name="sourceBit"/>, after the <paramref name="bits"/> there.</summary>
        public static void AppendBits(ref NativeGrowBuffer buffer, long bits, ReadOnlySpan<byte> source, long sourceBit, long count)
        {
            Grow(ref buffer, bits + count);
            Bits.Copy(source, sourceBit, new Span<byte>(buffer.Pointer, (int)buffer.Length + 8), bits, count);
        }

        /// <summary>Appends <paramref name="count"/> set bits after the <paramref name="bits"/> there.</summary>
        public static void AppendOnes(ref NativeGrowBuffer buffer, long bits, long count)
        {
            Grow(ref buffer, bits + count);
            var target = new Span<byte>(buffer.Pointer, (int)buffer.Length + 8);
            for (long at = bits, left = count; left > 0;)
            {
                int n = (int)Math.Min(left, 56);
                Bits.Copy(Ones, 0, target, at, n);
                at += n;
                left -= n;
            }
        }

        private static void Grow(ref NativeGrowBuffer buffer, long bits)
        {
            long bytes = (bits + 7) >> 3;
            buffer.EnsureAdditional(bytes - buffer.Length + 8);
            buffer.SetLength(bytes);
        }
    }

    private sealed class Nulls : Column
    {
        protected override void AppendValues(ArrayData data, int start, int count)
        {
        }

        protected override NativeCArray* BuildValues(NativeBatchExport export, nint validity, long nulls) => export.NewArray(Length, Length, [], []);
    }

    /// <summary>Values of a fixed width, in bytes.</summary>
    private sealed class Fixed(int width) : Column
    {
        private NativeGrowBuffer _values;

        public override long Bytes => base.Bytes + _values.Length;

        protected override void AppendValues(ArrayData data, int start, int count) =>
            _values.Append(data.Buffers[1].Span.Slice(checked((data.Offset + start) * width), checked(count * width)));

        protected override NativeCArray* BuildValues(NativeBatchExport export, nint validity, long nulls) =>
            export.NewArray(Length, nulls, [validity, export.Ptr(ref _values)], []);

        public override void Reset()
        {
            base.Reset();
            _values.Free();
        }
    }

    private sealed class Booleans : Column
    {
        private NativeGrowBuffer _values;

        public override long Bytes => base.Bytes + _values.Length;

        protected override void AppendValues(ArrayData data, int start, int count) =>
            Bitmaps.AppendBits(ref _values, Length, data.Buffers[1].Span, data.Offset + start, count);

        protected override NativeCArray* BuildValues(NativeBatchExport export, nint validity, long nulls)
        {
            if (Length == 0) _values.Append<ulong>(0);
            return export.NewArray(Length, nulls, [validity, export.Ptr(ref _values)], []);
        }

        public override void Reset()
        {
            base.Reset();
            _values.Free();
        }
    }

    /// <summary>Strings and binaries with int or long offsets: offsets rebased, values copied.</summary>
    private sealed class Offsets<TOffset> : Column where TOffset : unmanaged, IBinaryInteger<TOffset>
    {
        private NativeGrowBuffer _offsets, _bytes;

        public override long Bytes => base.Bytes + _offsets.Length + _bytes.Length;

        public override bool Accepts(ArrayData data, int start, int count)
        {
            ReadOnlySpan<TOffset> offsets = data.Buffers[1].Span.CastTo<TOffset>().Slice(data.Offset + start, count + 1);
            long size = long.CreateTruncating(offsets[count] - offsets[0]);
            return _bytes.Length + size <= (typeof(TOffset) == typeof(int) ? int.MaxValue : long.MaxValue);
        }

        protected override void AppendValues(ArrayData data, int start, int count)
        {
            ReadOnlySpan<TOffset> offsets = data.Buffers[1].Span.CastTo<TOffset>().Slice(data.Offset + start, count + 1);
            if (_offsets.Length == 0) _offsets.Append(TOffset.Zero);
            TOffset shift = TOffset.CreateTruncating(_bytes.Length) - offsets[0];
            var target = (TOffset*)_offsets.Reserve((long)count * sizeof(TOffset));
            for (int i = 1; i <= count; i++) target[i - 1] = offsets[i] + shift;
            long first = long.CreateTruncating(offsets[0]), size = long.CreateTruncating(offsets[count]) - first;
            _bytes.Append(data.Buffers[2].Span.Slice((int)first, (int)size));
        }

        protected override NativeCArray* BuildValues(NativeBatchExport export, nint validity, long nulls)
        {
            if (_offsets.Length == 0) _offsets.Append(TOffset.Zero);
            return export.NewArray(Length, nulls, [validity, export.Ptr(ref _offsets), export.Ptr(ref _bytes)], []);
        }

        public override void Reset()
        {
            base.Reset();
            _offsets.Free();
            _bytes.Free();
        }
    }

    /// <summary>String and binary views: inline views copied as they are, long values copied into one data buffer of the appender's.</summary>
    private sealed class Views : Column
    {
        private NativeGrowBuffer _views, _data;

        public override long Bytes => base.Bytes + _views.Length + _data.Length;

        public override bool Accepts(ArrayData data, int start, int count)
        {
            long size = 0;
            ReadOnlySpan<Int128> views = data.Buffers[1].Span.CastTo<Int128>().Slice(data.Offset + start, count);
            for (int i = 0; i < count; i++)
            {
                int length = (int)(views[i] & uint.MaxValue);
                if (length > BytesTests.MaxInline && ValidAt(data, start + i)) size += length;
            }
            return _data.Length + size <= int.MaxValue;
        }

        protected override void AppendValues(ArrayData data, int start, int count)
        {
            ReadOnlySpan<Int128> views = data.Buffers[1].Span.CastTo<Int128>().Slice(data.Offset + start, count);
            var buffers = new ReadOnlyMemory<byte>[data.Buffers.Length - 2];
            for (int b = 0; b < buffers.Length; b++) buffers[b] = data.Buffers[2 + b].Memory;
            var target = (Int128*)_views.Reserve(16L * count);
            for (int i = 0; i < count; i++)
            {
                Int128 view = views[i];
                int length = (int)(view & uint.MaxValue);
                if (!ValidAt(data, start + i)) view = default; // nulls point nowhere: a foreign null view may name any buffer
                else if (length > BytesTests.MaxInline)
                {
                    var bytes = new Span<byte>(&view, 16);
                    int offset = (int)_data.Length;
                    _data.Append(BinaryKernels.Long(bytes, buffers));
                    BinaryPrimitives.WriteInt32LittleEndian(bytes[8..], 0);
                    BinaryPrimitives.WriteInt32LittleEndian(bytes[12..], offset);
                }
                target[i] = view;
            }
        }

        protected override NativeCArray* BuildValues(NativeBatchExport export, nint validity, long nulls)
        {
            nint views = export.Ptr(ref _views);
            return _data.Length == 0
                ? export.NewArray(Length, nulls, [validity, views], [])
                : export.NewArray(Length, nulls, [validity, views, export.Ptr(ref _data)], []);
        }

        public override void Reset()
        {
            base.Reset();
            _views.Free();
            _data.Free();
        }
    }

    private sealed class Structs(Column[] children) : Column
    {
        public override long Bytes => base.Bytes + children.Sum(c => c.Bytes);

        public override bool Accepts(ArrayData data, int start, int count)
        {
            for (int c = 0; c < children.Length; c++)
            {
                if (!children[c].Accepts(data.Children[c], data.Offset + start, count)) return false;
            }
            return true;
        }

        protected override void AppendValues(ArrayData data, int start, int count)
        {
            for (int c = 0; c < children.Length; c++) children[c].Append(data.Children[c], data.Offset + start, count);
        }

        protected override NativeCArray* BuildValues(NativeBatchExport export, nint validity, long nulls)
        {
            var built = new nint[children.Length];
            for (int c = 0; c < built.Length; c++) built[c] = (nint)children[c].Build(export);
            return export.NewArray(Length, nulls, [validity], built);
        }

        public override void Reset()
        {
            base.Reset();
            foreach (Column child in children) child.Reset();
        }
    }

    /// <summary>Lists, large lists and maps: offsets rebased, the elements of the rows appended to the child.</summary>
    private sealed class Lists<TOffset>(Column child) : Column where TOffset : unmanaged, IBinaryInteger<TOffset>
    {
        private NativeGrowBuffer _offsets;
        private long _elements;

        public override long Bytes => base.Bytes + _offsets.Length + child.Bytes;

        public override bool Accepts(ArrayData data, int start, int count)
        {
            ReadOnlySpan<TOffset> offsets = data.Buffers[1].Span.CastTo<TOffset>().Slice(data.Offset + start, count + 1);
            long first = long.CreateTruncating(offsets[0]), size = long.CreateTruncating(offsets[count]) - first;
            return _elements + size <= int.MaxValue && child.Accepts(data.Children[0], (int)first, (int)size);
        }

        protected override void AppendValues(ArrayData data, int start, int count)
        {
            ReadOnlySpan<TOffset> offsets = data.Buffers[1].Span.CastTo<TOffset>().Slice(data.Offset + start, count + 1);
            if (_offsets.Length == 0) _offsets.Append(TOffset.Zero);
            TOffset shift = TOffset.CreateTruncating(_elements) - offsets[0];
            var target = (TOffset*)_offsets.Reserve((long)count * sizeof(TOffset));
            for (int i = 1; i <= count; i++) target[i - 1] = offsets[i] + shift;
            int first = int.CreateTruncating(offsets[0]), size = int.CreateTruncating(offsets[count]) - first;
            child.Append(data.Children[0], first, size);
            _elements += size;
        }

        protected override NativeCArray* BuildValues(NativeBatchExport export, nint validity, long nulls)
        {
            if (_offsets.Length == 0) _offsets.Append(TOffset.Zero);
            return export.NewArray(Length, nulls, [validity, export.Ptr(ref _offsets)], [(nint)child.Build(export)]);
        }

        public override void Reset()
        {
            base.Reset();
            _offsets.Free();
            _elements = 0;
            child.Reset();
        }
    }

    private sealed class FixedSizeLists(int size, Column child) : Column
    {
        public override long Bytes => base.Bytes + child.Bytes;

        public override bool Accepts(ArrayData data, int start, int count) =>
            (Length + count) * (long)size <= int.MaxValue && child.Accepts(data.Children[0], (data.Offset + start) * size, count * size);

        protected override void AppendValues(ArrayData data, int start, int count) =>
            child.Append(data.Children[0], (data.Offset + start) * size, count * size);

        protected override NativeCArray* BuildValues(NativeBatchExport export, nint validity, long nulls) =>
            export.NewArray(Length, nulls, [validity], [(nint)child.Build(export)]);

        public override void Reset()
        {
            base.Reset();
            child.Reset();
        }
    }

    /// <summary>Dictionary indices appended; the dictionary of the first rows kept, and required of the rows after.</summary>
    private sealed class Dictionaries(int width, Column none) : Column
    {
        private NativeGrowBuffer _indices;
        private ArrayData? _dictionary; // retained (its memory held) until the batch is taken

        public override long Bytes => base.Bytes + _indices.Length;

        public override bool Accepts(ArrayData data, int start, int count) => _dictionary is null || SameValues(_dictionary, data.Dictionary!);

        protected override void AppendValues(ArrayData data, int start, int count)
        {
            _dictionary ??= data.Dictionary!.Retain();
            _indices.Append(data.Buffers[1].Span.Slice(checked((data.Offset + start) * width), checked(count * width)));
        }

        protected override NativeCArray* BuildValues(NativeBatchExport export, nint validity, long nulls)
        {
            // No rows appended: an empty dictionary.
            NativeCArray* dictionary = _dictionary is null ? none.Build(export) : ArrowFilter.Dictionary(export, _dictionary);
            return export.NewArray(Length, nulls, [validity, export.Ptr(ref _indices)], [], dictionary);
        }

        public override void Reset()
        {
            base.Reset();
            _indices.Free();
            _dictionary?.Dispose();
            _dictionary = null;
            none.Reset();
        }
    }

    /// <summary>
    /// Whether two dictionaries hold the same values: the same memory at once, else flat values compared (fixed widths,
    /// booleans, strings and binaries plain, large and as views, nulls at the same rows); nested ones only when the same.
    /// </summary>
    internal static bool SameValues(ArrayData a, ArrayData b)
    {
        if (ReferenceEquals(a, b)) return true;
        if (a.Length != b.Length || a.NullCount != b.NullCount || !SameType(a.DataType, b.DataType)) return false;
        if (a.Offset == b.Offset && a.Buffers.Length == b.Buffers.Length && a.Buffers.Zip(b.Buffers).All(static p => Same(p.First, p.Second))
            && a.Children is null or { Length: 0 } && a.Dictionary is null) return true;
        int n = a.Length;
        for (int i = 0; i < n; i++)
        {
            bool valid = ValidAt(a, i);
            if (valid != ValidAt(b, i)) return false;
        }
        switch (a.DataType)
        {
            case BooleanType:
                for (int i = 0; i < n; i++)
                {
                    if (ValidAt(a, i) && Bit(a.Buffers[1].Span, a.Offset + i) != Bit(b.Buffers[1].Span, b.Offset + i)) return false;
                }
                return true;
            case FixedSizeBinaryType or FixedWidthType when a.DataType is not DictionaryType:
            {
                int width = a.DataType is FixedSizeBinaryType f ? f.ByteWidth : ((FixedWidthType)a.DataType).BitWidth / 8;
                ReadOnlySpan<byte> x = a.Buffers[1].Span.Slice(a.Offset * width, n * width), y = b.Buffers[1].Span.Slice(b.Offset * width, n * width);
                for (int i = 0; i < n; i++)
                {
                    if (ValidAt(a, i) && !x.Slice(i * width, width).SequenceEqual(y.Slice(i * width, width))) return false;
                }
                return true;
            }
            case StringType or BinaryType or LargeStringType or LargeBinaryType or StringViewType or BinaryViewType:
                for (int i = 0; i < n; i++)
                {
                    if (ValidAt(a, i) && !Value(a, i).SequenceEqual(Value(b, i))) return false;
                }
                return true;
            default:
                return false;
        }

        static bool Same(ArrowBuffer x, ArrowBuffer y) => x.Length == y.Length && (x.IsEmpty || x.Span == y.Span);
        static bool Bit(ReadOnlySpan<byte> bits, int i) => (bits[i >> 3] >> (i & 7) & 1) != 0;
        static bool SameType(IArrowType x, IArrowType y) => Kaminari.Sazanami.Arrow.TypeSystem.ArrowTypes.SameType(x, y);

        static ReadOnlySpan<byte> Value(ArrayData data, int i)
        {
            int at = data.Offset + i;
            switch (data.DataType)
            {
                case StringType or BinaryType:
                {
                    ReadOnlySpan<int> offsets = data.Buffers[1].Span.CastTo<int>();
                    return data.Buffers[2].Span[offsets[at]..offsets[at + 1]];
                }
                case LargeStringType or LargeBinaryType:
                {
                    ReadOnlySpan<long> offsets = data.Buffers[1].Span.CastTo<long>();
                    return data.Buffers[2].Span[(int)offsets[at]..(int)offsets[at + 1]];
                }
                default:
                {
                    ReadOnlySpan<byte> view = data.Buffers[1].Span.Slice(at * 16, 16);
                    int length = BinaryPrimitives.ReadInt32LittleEndian(view);
                    if (length <= BytesTests.MaxInline) return view.Slice(4, length);
                    var buffers = new ReadOnlyMemory<byte>[data.Buffers.Length - 2];
                    for (int b = 0; b < buffers.Length; b++) buffers[b] = data.Buffers[2 + b].Memory;
                    return BinaryKernels.Long(view, buffers);
                }
            }
        }
    }
}
