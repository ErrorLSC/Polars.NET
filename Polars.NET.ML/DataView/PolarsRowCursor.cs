using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.ML;
using Microsoft.ML.Data;
using Polars.NET.Core.Data;

namespace Polars.NET.ML.DataView;

/// <summary>
/// A high-performance RowCursor that streams Apache Arrow RecordBatches directly into ML.NET.
/// </summary>
/// <remarks>
/// Scalar columns are read through Polars.NET.Core's pointer-based column accessors: buffer pointers and the
/// null bitmap are bound once per batch, so a getter call is a null-bit test plus a raw load. Each getter
/// closure captures the concrete sealed accessor type, letting the JIT devirtualize and inline the read.
/// </remarks>
internal sealed class PolarsRowCursor : DataViewRowCursor
{
    private readonly IEnumerator<RecordBatch> _batchEnumerator;

    private RecordBatch? _currentBatch;
    private long _position = -1;
    private int _batchRowIndex = -1;
    private readonly bool[] _activeColumns;
    private readonly int[] _neededOriginalIndices;
    private readonly IDisposable? _resourceToDispose;
    private readonly long _baseRowOffset;

    // Per schema column (null for inactive or unsupported columns); rebound on every batch switch.
    private readonly ColumnAccessor?[] _accessors;
    private readonly VectorColumn?[] _vectors;

    // ==========================================
    // Metadata & State Properties
    // ==========================================
    public override DataViewSchema Schema { get; }
    public override long Position => _position;
    public override long Batch => 0;

    public override bool IsColumnActive(DataViewSchema.Column column)
        => _activeColumns[column.Index];

    public override ValueGetter<DataViewRowId> GetIdGetter() =>
        (ref id) => id = new DataViewRowId((ulong)(_baseRowOffset + _position), 0);

    public PolarsRowCursor(
        DataViewSchema schema,
        IEnumerable<RecordBatch> batches,
        IEnumerable<DataViewSchema.Column>? columnsNeeded,
        IDisposable? resourceToDispose = null,
        long baseRowOffset = 0)
    {
        Schema = schema;
        _batchEnumerator = batches.GetEnumerator();
        _resourceToDispose = resourceToDispose;
        _baseRowOffset = baseRowOffset;

        _activeColumns = new bool[schema.Count];
        _accessors = new ColumnAccessor?[schema.Count];
        _vectors = new VectorColumn?[schema.Count];

        var neededList = new List<int>();

        if (columnsNeeded != null)
        {
            foreach (var col in columnsNeeded)
            {
                _activeColumns[col.Index] = true;
                neededList.Add(col.Index);
            }
        }
        else
        {
            for (int i = 0; i < schema.Count; i++)
            {
                _activeColumns[i] = true;
                neededList.Add(i);
            }
        }

        _neededOriginalIndices = [.. neededList];

        foreach (int idx in _neededOriginalIndices)
        {
            DataViewType type = schema[idx].Type;
            if (type is VectorDataViewType vt)
                _vectors[idx] ??= VectorColumn.Create(vt.ItemType.RawType);
            else
                _accessors[idx] ??= CreateScalarAccessor(type);
        }
    }

    /// <summary>
    /// The accessor is chosen from the DataViewType alone: <see cref="ArrowDataViewMapper"/> maps each supported
    /// Arrow type to exactly one DataViewType, and every accessor here handles all Arrow encodings of its type.
    /// </summary>
    private static ColumnAccessor? CreateScalarAccessor(DataViewType type)
    {
        if (type is NumberDataViewType)
        {
            Type raw = type.RawType;
            if (raw == typeof(sbyte)) return new Int8Accessor();
            if (raw == typeof(short)) return new Int16Accessor();
            if (raw == typeof(int)) return new Int32Accessor();
            if (raw == typeof(long)) return new Int64Accessor();
            if (raw == typeof(byte)) return new UInt8Accessor();
            if (raw == typeof(ushort)) return new UInt16Accessor();
            if (raw == typeof(uint)) return new UInt32Accessor();
            if (raw == typeof(ulong)) return new UInt64Accessor();
            if (raw == typeof(float)) return new FloatAccessor();
            if (raw == typeof(double)) return new DoubleAccessor();
            return null;
        }

        return type switch
        {
            BooleanDataViewType => new BooleanAccessor(),
            TextDataViewType => new StringViewAccessor(),
            DateTimeDataViewType => new TimestampAccessor(typeof(DateTime)),
            TimeSpanDataViewType => new DurationAccessor(),
            // KeyDataViewType (Arrow Dictionary) is not supported yet.
            _ => null
        };
    }

    // ==========================================
    // Iteration Logic
    // ==========================================
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _batchEnumerator.Dispose();
            _currentBatch?.Dispose();
            _currentBatch = null;
            foreach (var accessor in _accessors) accessor?.Dispose();
            _resourceToDispose?.Dispose();
        }
        base.Dispose(disposing);
    }

    public override bool MoveNext()
    {
        while (true)
        {
            _batchRowIndex++;

            if (_currentBatch != null && _batchRowIndex < _currentBatch.Length)
            {
                _position++;
                return true;
            }

            if (!_batchEnumerator.MoveNext())
            {
                return false;
            }

            _currentBatch?.Dispose();
            _currentBatch = _batchEnumerator.Current;
            _batchRowIndex = -1;

            if (_currentBatch == null || _currentBatch.Length == 0)
            {
                continue;
            }

            for (int i = 0; i < _neededOriginalIndices.Length; i++)
            {
                int originalIdx = _neededOriginalIndices[i];
                IArrowArray array = _currentBatch.Column(i);
                _accessors[originalIdx]?.SetBatch(array);
                _vectors[originalIdx]?.SetBatch(array);
            }
        }
    }

    // ==========================================
    // Value Extraction
    // ==========================================
    public override ValueGetter<TValue> GetGetter<TValue>(DataViewSchema.Column column)
    {
        int colIndex = column.Index;
        if (!_activeColumns[colIndex])
            throw new InvalidOperationException($"Column '{column.Name}' is not active in this cursor.");

        Type type = typeof(TValue);
        if (type != column.Type.RawType)
            throw new InvalidOperationException(
                $"Invalid TValue '{type.Name}' for column '{column.Name}': expected '{column.Type.RawType.Name}'.");

        if (_vectors[colIndex] is { } vector)
            return vector.CreateGetter<TValue>(this);

        // Each branch closes over the concrete sealed accessor so the read is devirtualized and inlined.
        return _accessors[colIndex] switch
        {
            // ------------------------------------------
            // Integers (null -> 0)
            // ------------------------------------------
            Int8Accessor acc => Cast<TValue, sbyte>((ref v) => { int i = _batchRowIndex; v = acc.IsNullFast(i) ? (sbyte)0 : acc.GetTypedValue(i); }),
            Int16Accessor acc => Cast<TValue, short>((ref v) => { int i = _batchRowIndex; v = acc.IsNullFast(i) ? (short)0 : acc.GetTypedValue(i); }),
            Int32Accessor acc => Cast<TValue, int>((ref v) => { int i = _batchRowIndex; v = acc.IsNullFast(i) ? 0 : acc.GetTypedValue(i); }),
            Int64Accessor acc => Cast<TValue, long>((ref v) => { int i = _batchRowIndex; v = acc.IsNullFast(i) ? 0L : acc.GetTypedValue(i); }),
            UInt8Accessor acc => Cast<TValue, byte>((ref v) => { int i = _batchRowIndex; v = acc.IsNullFast(i) ? (byte)0 : acc.GetTypedValue(i); }),
            UInt16Accessor acc => Cast<TValue, ushort>((ref v) => { int i = _batchRowIndex; v = acc.IsNullFast(i) ? (ushort)0 : acc.GetTypedValue(i); }),
            UInt32Accessor acc => Cast<TValue, uint>((ref v) => { int i = _batchRowIndex; v = acc.IsNullFast(i) ? 0u : acc.GetTypedValue(i); }),
            UInt64Accessor acc => Cast<TValue, ulong>((ref v) => { int i = _batchRowIndex; v = acc.IsNullFast(i) ? 0UL : acc.GetTypedValue(i); }),
            // ------------------------------------------
            // Floats (null -> NaN, ML.NET's missing value)
            // ------------------------------------------
            FloatAccessor acc => Cast<TValue, float>((ref v) => { int i = _batchRowIndex; v = acc.IsNullFast(i) ? float.NaN : acc.GetTypedValue(i); }),
            DoubleAccessor acc => Cast<TValue, double>((ref v) => { int i = _batchRowIndex; v = acc.IsNullFast(i) ? double.NaN : acc.GetTypedValue(i); }),
            // ------------------------------------------
            // Boolean (null -> false)
            // ------------------------------------------
            BooleanAccessor acc => Cast<TValue, bool>((ref v) => { int i = _batchRowIndex; v = !acc.IsNullFast(i) && acc.GetTypedValue(i); }),
            // ------------------------------------------
            // Temporal (null -> default)
            // ------------------------------------------
            TimestampAccessor acc => Cast<TValue, DateTime>((ref v) => { int i = _batchRowIndex; v = acc.IsNullFast(i) ? default : acc.GetTypedValue(i); }),
            DurationAccessor acc => Cast<TValue, TimeSpan>((ref v) => { int i = _batchRowIndex; v = acc.IsNullFast(i) ? default : acc.GetTypedValue(i); }),
            // ------------------------------------------
            // String (UTF-8 view -> ReadOnlyMemory<char>)
            // ------------------------------------------
            StringViewAccessor acc => Cast<TValue, ReadOnlyMemory<char>>(CreateTextGetter(acc)),
            _ => throw new NotSupportedException(
                        $"Column '{column.Name}' of type '{column.Type}' is not currently supported for zero-copy column extraction."),
        };
    }

    private static ValueGetter<TValue> Cast<TValue, T>(ValueGetter<T> getter)
        => (ValueGetter<TValue>)(object)getter;

    /// <summary>
    /// Decodes UTF-8 into a per-getter char arena. Each returned ReadOnlyMemory owns its slice: a full arena is
    /// replaced rather than reused, so values handed out earlier stay valid.
    /// </summary>
    private ValueGetter<ReadOnlyMemory<char>> CreateTextGetter(StringViewAccessor acc)
    {
        const int ArenaSize = 65536;
        char[] arena = new char[ArenaSize];
        int arenaOffset = 0;

        return (ref value) =>
        {
            int i = _batchRowIndex;
            if (acc.IsNullFast(i))
            {
                value = default;
                return;
            }

            ReadOnlySpan<byte> utf8Bytes = acc.GetBytesSpan(i);
            if (utf8Bytes.IsEmpty)
            {
                value = ReadOnlyMemory<char>.Empty;
                return;
            }

            int maxChars = System.Text.Encoding.UTF8.GetMaxCharCount(utf8Bytes.Length);
            if (arenaOffset + maxChars > arena.Length)
            {
                arena = new char[Math.Max(ArenaSize, maxChars)];
                arenaOffset = 0;
            }

            int actualChars = System.Text.Encoding.UTF8.GetChars(utf8Bytes, arena.AsSpan(arenaOffset));
            value = new ReadOnlyMemory<char>(arena, arenaOffset, actualChars);
            arenaOffset += actualChars;
        };
    }

    // ==========================================
    // Vector / Tensor
    // ==========================================

    private abstract class VectorColumn
    {
        public abstract void SetBatch(IArrowArray array);
        public abstract ValueGetter<TValue> CreateGetter<TValue>(PolarsRowCursor cursor);

        public static VectorColumn? Create(Type itemType)
        {
            if (itemType == typeof(float)) return new VectorColumn<float>(float.NaN);
            if (itemType == typeof(double)) return new VectorColumn<double>(double.NaN);
            if (itemType == typeof(sbyte)) return new VectorColumn<sbyte>(0);
            if (itemType == typeof(short)) return new VectorColumn<short>(0);
            if (itemType == typeof(int)) return new VectorColumn<int>(0);
            if (itemType == typeof(long)) return new VectorColumn<long>(0);
            if (itemType == typeof(byte)) return new VectorColumn<byte>(0);
            if (itemType == typeof(ushort)) return new VectorColumn<ushort>(0);
            if (itemType == typeof(uint)) return new VectorColumn<uint>(0);
            if (itemType == typeof(ulong)) return new VectorColumn<ulong>(0);
            return null;
        }
    }

    /// <summary>
    /// Copies one list row into a dense VBuffer. Handles FixedSizeList (row start includes the list offset),
    /// List and LargeList (offsets resolved through Arrow, which applies the list offset).
    /// A null fixed-size row yields a full-length vector of the missing value (NaN for floats, 0 otherwise);
    /// a null variable-length row yields an empty vector.
    /// </summary>
    private sealed class VectorColumn<T>(T missingValue) : VectorColumn where T : unmanaged, IEquatable<T>
    {
        private enum ListKind { FixedSize, List, LargeList }

        private IArrowArray? _array;
        private PrimitiveArray<T>? _child;
        private ListKind _kind;
        private int _fixedSize;
        private int _fixedOffset;

        public override void SetBatch(IArrowArray array)
        {
            _array = array;
            switch (array)
            {
                case FixedSizeListArray fs:
                    _kind = ListKind.FixedSize;
                    _fixedSize = ((FixedSizeListType)fs.Data.DataType).ListSize;
                    _fixedOffset = fs.Offset;
                    _child = ChildValues(fs.Values);
                    break;
                case LargeListArray large:
                    _kind = ListKind.LargeList;
                    _child = ChildValues(large.Values);
                    break;
                case ListArray list:
                    _kind = ListKind.List;
                    _child = ChildValues(list.Values);
                    break;
                default:
                    throw new NotSupportedException($"Arrow array '{array.GetType().Name}' cannot be read as a vector column.");
            }
        }

        private static PrimitiveArray<T> ChildValues(IArrowArray child)
            => child as PrimitiveArray<T>
               ?? throw new NotSupportedException(
                   $"Vector item array '{child.GetType().Name}' does not hold '{typeof(T).Name}' values.");

        public override ValueGetter<TValue> CreateGetter<TValue>(PolarsRowCursor cursor)
            => (ValueGetter<TValue>)(object)(ValueGetter<VBuffer<T>>)((ref value) => Fill(cursor._batchRowIndex, ref value));

        private void Fill(int row, ref VBuffer<T> value)
        {
            IArrowArray array = _array!;
            ReadOnlySpan<T> allValues = _child!.Values;

            if (_kind == ListKind.FixedSize)
            {
                var fixedEditor = VBufferEditor.Create(ref value, _fixedSize);
                if (array.IsNull(row))
                    fixedEditor.Values.Fill(missingValue);
                else
                    allValues.Slice((_fixedOffset + row) * _fixedSize, _fixedSize).CopyTo(fixedEditor.Values);
                value = fixedEditor.Commit();
                return;
            }

            if (array.IsNull(row))
            {
                value = VBufferEditor.Create(ref value, 0).Commit();
                return;
            }

            int start, length;
            if (_kind == ListKind.LargeList)
            {
                ReadOnlySpan<long> offsets = ((LargeListArray)array).ValueOffsets;
                start = checked((int)offsets[row]);
                length = checked((int)(offsets[row + 1] - offsets[row]));
            }
            else
            {
                ReadOnlySpan<int> offsets = ((ListArray)array).ValueOffsets;
                start = offsets[row];
                length = offsets[row + 1] - start;
            }

            var editor = VBufferEditor.Create(ref value, length);
            allValues.Slice(start, length).CopyTo(editor.Values);
            value = editor.Commit();
        }
    }
}
