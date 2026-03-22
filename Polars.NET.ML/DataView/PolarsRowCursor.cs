using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.ML;
using Microsoft.ML.Data;

namespace Polars.NET.ML.DataView;

/// <summary>
/// A high-performance RowCursor that streams Apache Arrow RecordBatches directly into ML.NET.
/// </summary>
internal sealed class PolarsRowCursor : DataViewRowCursor
{
    private readonly DataViewSchema _schema;
    private readonly IEnumerator<RecordBatch> _batchEnumerator;
    
    private RecordBatch? _currentBatch;
    private long _position = -1;       
    private int _batchRowIndex = -1;
    private readonly bool[] _activeColumns;
    private readonly int[] _neededOriginalIndices;
    private readonly IArrowArray[] _currentArrays;

    // ==========================================
    // Metadata & State Properties
    // ==========================================
    public override DataViewSchema Schema => _schema;
    public override long Position => _position;
    public override long Batch => 0; 

    public override bool IsColumnActive(DataViewSchema.Column column) 
        => _activeColumns[column.Index];

    public override ValueGetter<DataViewRowId> GetIdGetter() => 
        (ref DataViewRowId id) => id = new DataViewRowId((ulong)_position, 0);

    // ==========================================
    // Iteration Logic
    // ==========================================
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _batchEnumerator.Dispose();
            _currentBatch?.Dispose();
        }
    }

    public override bool MoveNext()
    {
        _position++;
        _batchRowIndex++;

        if (_currentBatch == null || _batchRowIndex >= _currentBatch.Length)
        {
            if (_batchEnumerator.MoveNext())
            {
                _currentBatch?.Dispose(); 
                _currentBatch = _batchEnumerator.Current;
                _batchRowIndex = 0;

                for (int i = 0; i < _neededOriginalIndices.Length; i++)
                {
                    int originalIdx = _neededOriginalIndices[i];
                    _currentArrays[originalIdx] = _currentBatch.Column(i); 
                }
                
                return true;
            }
            return false;
        }
        
        return true;
    }

    public PolarsRowCursor(
        DataViewSchema schema, 
        IEnumerable<RecordBatch> batches, 
        IEnumerable<DataViewSchema.Column>? columnsNeeded)
    {
        _schema = schema;
        _batchEnumerator = batches.GetEnumerator();
        
        _activeColumns = new bool[schema.Count];
        _currentArrays = new IArrowArray[schema.Count];
        
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
    }

    // ==========================================
    // Value Extraction
    // ==========================================
    public override ValueGetter<TValue> GetGetter<TValue>(DataViewSchema.Column column)
    {
        Type type = typeof(TValue);
        int colIndex = column.Index;

        // ------------------------------------------
        // Integers
        // ------------------------------------------
        if (type == typeof(sbyte))
        {
            void getter(ref sbyte value)
            {
                var array = (Int8Array)_currentArrays[colIndex];

                if (array.IsNull(_batchRowIndex))
                {
                    value = 0;
                }
                else
                {
                    ref sbyte r = ref MemoryMarshal.GetReference(array.Values);
                    value = Unsafe.Add(ref r, _batchRowIndex);
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<sbyte>)getter;
        }

        if (type == typeof(short))
        {
            void getter(ref short value)
            {
                var array = (Int16Array)_currentArrays[colIndex];

                if (array.IsNull(_batchRowIndex))
                {
                    value = 0;
                }
                else
                {
                    ref short r = ref MemoryMarshal.GetReference(array.Values);
                    value = Unsafe.Add(ref r, _batchRowIndex);
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<short>)getter;
        }

        if (type == typeof(int))
        {
            void getter(ref int value)
            {
                var array = (Int32Array)_currentArrays[colIndex];

                if (array.IsNull(_batchRowIndex))
                {
                    value = 0;
                }
                else
                {
                    ref int r = ref MemoryMarshal.GetReference(array.Values);
                    value = Unsafe.Add(ref r, _batchRowIndex);
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<int>)getter;
        }

        if (type == typeof(long))
        {
            void getter(ref long value)
            {
                var array = (Int64Array)_currentArrays[colIndex];

                if (array.IsNull(_batchRowIndex))
                {
                    value = 0;
                }
                else
                {
                    ref long r = ref MemoryMarshal.GetReference(array.Values);
                    value = Unsafe.Add(ref r, _batchRowIndex);
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<long>)getter;
        }
        // ------------------------------------------
        // Unsigned Integers
        // ------------------------------------------
        if (type == typeof(byte))
        {
            void getter(ref byte value)
            {
                var array = (UInt8Array)_currentArrays[colIndex];

                if (array.IsNull(_batchRowIndex))
                {
                    value = 0;
                }
                else
                {
                    ref byte r = ref MemoryMarshal.GetReference(array.Values);
                    value = Unsafe.Add(ref r, _batchRowIndex);
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<byte>)getter;
        }

        if (type == typeof(ushort))
        {
            void getter(ref ushort value)
            {
                var array = (UInt16Array)_currentArrays[colIndex];

                if (array.IsNull(_batchRowIndex))
                {
                    value = 0;
                }
                else
                {
                    ref ushort r = ref MemoryMarshal.GetReference(array.Values);
                    value = Unsafe.Add(ref r, _batchRowIndex);
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<ushort>)getter;
        }

        if (type == typeof(uint))
        {
            void getter(ref uint value)
            {
                var array = (UInt32Array)_currentArrays[colIndex];

                if (array.IsNull(_batchRowIndex))
                {
                    value = 0;
                }
                else
                {
                    ref uint r = ref MemoryMarshal.GetReference(array.Values);
                    value = Unsafe.Add(ref r, _batchRowIndex);
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<uint>)getter;
        }

        if (type == typeof(ulong))
        {
            void getter(ref ulong value)
            {
                var array = (UInt64Array)_currentArrays[colIndex];

                if (array.IsNull(_batchRowIndex))
                {
                    value = 0;
                }
                else
                {
                    ref ulong r = ref MemoryMarshal.GetReference(array.Values);
                    value = Unsafe.Add(ref r, _batchRowIndex);
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<ulong>)getter;
        }

        // ------------------------------------------
        // Floats
        // ------------------------------------------
        if (type == typeof(Half))
        {
            void getter(ref Half value)
            {
                var array = (HalfFloatArray)_currentArrays[colIndex];

                if (array.IsNull(_batchRowIndex))
                {
                    value = Half.NaN;
                }
                else
                {
                    ref Half r = ref MemoryMarshal.GetReference(array.Values);
                    value = Unsafe.Add(ref r, _batchRowIndex);
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<Half>)getter;
        }

        if (type == typeof(float))
        {
            void getter(ref float value)
            {
                var array = (FloatArray)_currentArrays[colIndex];

                if (array.IsNull(_batchRowIndex))
                {
                    value = float.NaN;
                }
                else
                {
                    ref float r = ref MemoryMarshal.GetReference(array.Values);
                    value = Unsafe.Add(ref r, _batchRowIndex);
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<float>)getter;
        }        

        if (type == typeof(double))
        {
            void getter(ref double value)
            {
                var array = (DoubleArray)_currentArrays[colIndex];

                if (array.IsNull(_batchRowIndex))
                {
                    value = double.NaN;
                }
                else
                {
                    ref double r = ref MemoryMarshal.GetReference(array.Values);
                    value = Unsafe.Add(ref r, _batchRowIndex);
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<double>)getter;
        }        
        // ------------------------------------------
        // Boolean
        // ------------------------------------------
        if (type == typeof(bool))
        {
            void getter(ref bool value)
            {
                var array = (BooleanArray)_currentArrays[colIndex];

                if (array.IsNull(_batchRowIndex))
                {
                    value = false;
                }
                else
                {
                    ref byte r = ref MemoryMarshal.GetReference(array.ValueBuffer.Span);
                    
                    int absoluteIndex = _batchRowIndex + array.Offset;
                    
                    int byteOffset = absoluteIndex >> 3; // absoluteIndex / 8
                    int bitOffset  = absoluteIndex & 7;  // absoluteIndex % 8

                    byte b = Unsafe.Add(ref r, byteOffset);
                    
                    value = (b & (1 << bitOffset)) != 0;
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<bool>)getter;
        }
        // ----------------------------------------------------------------
        // String (Polars StringViewArray -> ML.NET ReadOnlyMemory<char>)
        // ----------------------------------------------------------------
        if (type == typeof(ReadOnlyMemory<char>))
        {
            char[] charBuffer = new char[256];

            void getter(ref ReadOnlyMemory<char> value)
            {
                var array = (StringViewArray)_currentArrays[colIndex];

                if (array.IsNull(_batchRowIndex))
                {
                    value = default;
                }
                else
                {
                    ReadOnlySpan<byte> utf8Bytes = array.GetBytes(_batchRowIndex);

                    if (utf8Bytes.IsEmpty)
                    {
                        value = ReadOnlyMemory<char>.Empty;
                        return;
                    }

                    // GetMaxCharCount is O(1) 
                    int maxCharCount = System.Text.Encoding.UTF8.GetMaxCharCount(utf8Bytes.Length);
                    
                    // Amortized O(1)
                    if (charBuffer.Length < maxCharCount)
                    {
                        int newSize = Math.Max(charBuffer.Length * 2, maxCharCount);
                        charBuffer = new char[newSize];
                    }

                    int actualCharCount = System.Text.Encoding.UTF8.GetChars(utf8Bytes, charBuffer);

                    value = new ReadOnlyMemory<char>(charBuffer, 0, actualCharCount);
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<ReadOnlyMemory<char>>)getter;
        }
        // ------------------------------------------
        // DateTime
        // ------------------------------------------

        if (type == typeof(DateTime))
        {
            const long UnixEpochTicks = 621355968000000000L; 
            const long TicksPerDay = 864000000000L;

            void getter(ref DateTime value)
            {
                var columnArray = _currentArrays[colIndex];

                if (columnArray.IsNull(_batchRowIndex))
                {
                    value = default;
                    return;
                }

                if (columnArray is TimestampArray tsArray)
                {
                    ref long r = ref MemoryMarshal.GetReference(tsArray.Values);
                    long val = Unsafe.Add(ref r, _batchRowIndex);
                    
                    var unit = ((TimestampType)tsArray.Data.DataType).Unit;
                    long ticks = unit switch {
                        TimeUnit.Nanosecond => val / 100,
                        TimeUnit.Microsecond => val * 10,
                        TimeUnit.Millisecond => val * 10_000,
                        TimeUnit.Second => val * 10_000_000,
                        _ => val
                    };
                    value = new DateTime(UnixEpochTicks + ticks);
                }
                else if (columnArray is Date32Array d32Array)
                {
                    ref int r = ref MemoryMarshal.GetReference(d32Array.Values);
                    int val = Unsafe.Add(ref r, _batchRowIndex);
                    
                    value = new DateTime(UnixEpochTicks + val * TicksPerDay);
                }
                else if (columnArray is Date64Array d64Array)
                {
                    ref long r = ref MemoryMarshal.GetReference(d64Array.Values);
                    long val = Unsafe.Add(ref r, _batchRowIndex);
                    
                    value = new DateTime(UnixEpochTicks + val * 10_000L);
                }
                else
                {
                    value = default; 
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<DateTime>)getter;
        }

        // ------------------------------------------
        // TimeSpan
        // ------------------------------------------
        if (type == typeof(TimeSpan))
        {
            void getter(ref TimeSpan value)
            {
                var columnArray = _currentArrays[colIndex];

                if (columnArray.IsNull(_batchRowIndex))
                {
                    value = default;
                    return;
                }

                if (columnArray is DurationArray durArray)
                {
                    ref long r = ref MemoryMarshal.GetReference(durArray.Values);
                    long val = Unsafe.Add(ref r, _batchRowIndex);
                    
                    var unit = ((DurationType)durArray.Data.DataType).Unit;
                    long ticks = unit switch {
                        TimeUnit.Nanosecond => val / 100,
                        TimeUnit.Microsecond => val * 10,
                        TimeUnit.Millisecond => val * 10_000,
                        TimeUnit.Second => val * 10_000_000,
                        _ => val
                    };
                    value = new TimeSpan(ticks);
                }
                else if (columnArray is Time64Array t64Array)
                {
                    ref long r = ref MemoryMarshal.GetReference(t64Array.Values);
                    long val = Unsafe.Add(ref r, _batchRowIndex);
                    
                    var unit = ((Time64Type)t64Array.Data.DataType).Unit;
                    long ticks = unit switch {
                        TimeUnit.Nanosecond => val / 100,
                        TimeUnit.Microsecond => val * 10,
                        _ => val
                    };
                    value = new TimeSpan(ticks);
                }
                else if (columnArray is Time32Array t32Array)
                {
                    ref int r = ref MemoryMarshal.GetReference(t32Array.Values);
                    int val = Unsafe.Add(ref r, _batchRowIndex);
                    
                    var unit = ((Time32Type)t32Array.Data.DataType).Unit;
                    long ticks = unit switch {
                        TimeUnit.Millisecond => val * 10_000,
                        TimeUnit.Second => val * 10_000_000,
                        _ => val
                    };
                    value = new TimeSpan(ticks);
                }
                else
                {
                    value = default;
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<TimeSpan>)getter;
        }

        // ------------------------------------------
        // Vector / Tensor
        // ------------------------------------------
        
        if (type == typeof(VBuffer<float>))
        {
            void getter(ref VBuffer<float> value) 
                => FillVBuffer(colIndex, ref value, array => ((FloatArray)array).Values);
            return (ValueGetter<TValue>)(object)(ValueGetter<VBuffer<float>>)getter;
        }

        if (type == typeof(VBuffer<int>))
        {
            void getter(ref VBuffer<int> value) 
                => FillVBuffer(colIndex, ref value, array => ((Int32Array)array).Values);
            return (ValueGetter<TValue>)(object)(ValueGetter<VBuffer<int>>)getter;
        }

        if (type == typeof(VBuffer<double>))
        {
            void getter(ref VBuffer<double> value) 
                => FillVBuffer(colIndex, ref value, array => ((DoubleArray)array).Values);
            return (ValueGetter<TValue>)(object)(ValueGetter<VBuffer<double>>)getter;
        }

        if (type == typeof(VBuffer<short>))
        {
            void getter(ref VBuffer<short> value) 
                => FillVBuffer(colIndex, ref value, array => ((Int16Array)array).Values);
            return (ValueGetter<TValue>)(object)(ValueGetter<VBuffer<short>>)getter;
        }

        throw new NotSupportedException($"Type '{type.Name}' is not currently supported for zero-copy column extraction.");
    }

    private void FillVBuffer<TPrimitive>(
        int colIndex, 
        ref VBuffer<TPrimitive> value, 
        SpanExtractor<TPrimitive> getValuesSpan)
        where TPrimitive : unmanaged
    {
        var columnArray = _currentArrays[colIndex];

        if (columnArray.IsNull(_batchRowIndex))
        {
            value = default;
            return;
        }

        ReadOnlySpan<TPrimitive> span = default;
        int length = 0;

        if (columnArray is FixedSizeListArray fsList)
        {
            // Dense Tensor
            length = ((FixedSizeListType)fsList.Data.DataType).ListSize;
            ReadOnlySpan<TPrimitive> allValues = getValuesSpan(fsList.Values); 
            
            int offset = _batchRowIndex * length;
            span = allValues.Slice(offset, length);
        }
        else if (columnArray is ListArray list)
        {
            ref int offsetsRef = ref MemoryMarshal.GetReference(list.ValueOffsets);
            
            int offset = Unsafe.Add(ref offsetsRef, _batchRowIndex);
            length = Unsafe.Add(ref offsetsRef, _batchRowIndex + 1) - offset;

            ReadOnlySpan<TPrimitive> allValues = getValuesSpan(list.Values);
            span = allValues.Slice(offset, length);
        }

        var editor = VBufferEditor.Create(ref value, length);
        span.CopyTo(editor.Values);
        value = editor.Commit();
    }

    private delegate ReadOnlySpan<T> SpanExtractor<T>(IArrowArray array) where T : unmanaged;
}