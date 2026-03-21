using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.ML;
using Microsoft.ML.Data;

namespace Polars.NET.ML.DataView;

/// <summary>
/// A high-performance RowCursor that streams Apache Arrow RecordBatches directly into ML.NET.
/// </summary>
internal sealed class PolarsRowCursor(DataViewSchema schema, IEnumerable<RecordBatch> batches) : DataViewRowCursor
{
    private readonly IEnumerator<RecordBatch> _batchEnumerator = batches.GetEnumerator();
    
    private RecordBatch? _currentBatch;
    private long _position = -1;       
    private int _batchRowIndex = -1;

    // ==========================================
    // Metadata & State Properties
    // ==========================================
    public override DataViewSchema Schema => schema;
    public override long Position => _position;
    public override long Batch => 0; 
    /// <summary>
    /// ML.NET requires a unique RowId for tracking and shuffling.
    /// </summary>
    public override ValueGetter<DataViewRowId> GetIdGetter()
    {
        return (ref DataViewRowId id) => 
        {
            id = new DataViewRowId((ulong)_position, 0); 
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
                _currentBatch = _batchEnumerator.Current;
                _batchRowIndex = 0;
                return true;
            }
            return false;
        }
        
        return true;
    }

    // ==========================================
    // Value Extraction
    // ==========================================
    public override ValueGetter<TValue> GetGetter<TValue>(DataViewSchema.Column column)
    {
        int colIndex = column.Index;
        var type = typeof(TValue);

        // ------------------------------------------
        // Integers
        // ------------------------------------------
        if (type == typeof(sbyte))
        {
            void getter(ref sbyte value)
            {
                var array = (Int8Array)_currentBatch!.Column(colIndex);

                if (array.IsNull(_batchRowIndex))
                {
                    value = 0;
                }
                else
                {
                    value = array.Values[_batchRowIndex];
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<sbyte>)getter;
        }

        if (type == typeof(short))
        {
            void getter(ref short value)
            {
                var array = (Int16Array)_currentBatch!.Column(colIndex);

                if (array.IsNull(_batchRowIndex))
                {
                    value = 0;
                }
                else
                {
                    value = array.Values[_batchRowIndex];
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<short>)getter;
        }

        if (type == typeof(int))
        {
            void getter(ref int value)
            {
                var array = (Int32Array)_currentBatch!.Column(colIndex);

                if (array.IsNull(_batchRowIndex))
                {
                    value = 0;
                }
                else
                {
                    value = array.Values[_batchRowIndex];
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<int>)getter;
        }

        if (type == typeof(long))
        {
            void getter(ref long value)
            {
                var array = (Int64Array)_currentBatch!.Column(colIndex);

                if (array.IsNull(_batchRowIndex))
                {
                    value = 0;
                }
                else
                {
                    value = array.Values[_batchRowIndex];
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
                var array = (UInt8Array)_currentBatch!.Column(colIndex);

                if (array.IsNull(_batchRowIndex))
                {
                    value = 0;
                }
                else
                {
                    value = array.Values[_batchRowIndex];
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<byte>)getter;
        }

        if (type == typeof(ushort))
        {
            void getter(ref ushort value)
            {
                var array = (UInt16Array)_currentBatch!.Column(colIndex);

                if (array.IsNull(_batchRowIndex))
                {
                    value = 0;
                }
                else
                {
                    value = array.Values[_batchRowIndex];
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<ushort>)getter;
        }

        if (type == typeof(uint))
        {
            void getter(ref uint value)
            {
                var array = (UInt32Array)_currentBatch!.Column(colIndex);

                if (array.IsNull(_batchRowIndex))
                {
                    value = 0;
                }
                else
                {
                    value = array.Values[_batchRowIndex];
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<uint>)getter;
        }

        if (type == typeof(ulong))
        {
            void getter(ref ulong value)
            {
                var array = (UInt64Array)_currentBatch!.Column(colIndex);

                if (array.IsNull(_batchRowIndex))
                {
                    value = 0;
                }
                else
                {
                    value = array.Values[_batchRowIndex];
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
                var array = (HalfFloatArray)_currentBatch!.Column(colIndex);

                if (array.IsNull(_batchRowIndex))
                {
                    value = Half.NaN;
                }
                else
                {
                    value = array.Values[_batchRowIndex];
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<Half>)getter;
        }

        if (type == typeof(float))
        {
            void getter(ref float value)
            {
                var array = (FloatArray)_currentBatch!.Column(colIndex);

                if (array.IsNull(_batchRowIndex))
                {
                    value = float.NaN;
                }
                else
                {
                    value = array.Values[_batchRowIndex];
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<float>)getter;
        }
        
        if (type == typeof(double))
        {
            void getter(ref double value)
            {
                var array = (DoubleArray)_currentBatch!.Column(colIndex);

                if (array.IsNull(_batchRowIndex))
                {
                    value = double.NaN;
                }
                else
                {
                    value = array.Values[_batchRowIndex];
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
                var array = (BooleanArray)_currentBatch!.Column(colIndex);

                if (array.IsNull(_batchRowIndex))
                {
                    value = false;
                }
                else
                {
                    value = array.GetValue(_batchRowIndex).GetValueOrDefault();
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
                var array = (StringViewArray)_currentBatch!.Column(colIndex);

                if (array.IsNull(_batchRowIndex))
                {
                    value = default;
                }
                else
                {
                    // Get UTF-8 ReadOnlySpan<byte>
                    ReadOnlySpan<byte> utf8Bytes = array.GetBytes(_batchRowIndex);

                    if (utf8Bytes.IsEmpty)
                    {
                        value = ReadOnlyMemory<char>.Empty;
                        return;
                    }

                    // Check Char Buffer Length
                    int maxCharCount = System.Text.Encoding.UTF8.GetMaxCharCount(utf8Bytes.Length);
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
            void getter(ref DateTime value)
            {
                var columnArray = _currentBatch!.Column(colIndex);

                if (columnArray.IsNull(_batchRowIndex))
                {
                    value = default;
                    return;
                }

                if (columnArray is TimestampArray tsArray)
                {
                    value = tsArray.GetTimestamp(_batchRowIndex)?.UtcDateTime ?? default;
                }
                else if (columnArray is Date32Array d32Array)
                {
                    value = d32Array.GetDateTime(_batchRowIndex) ?? default;
                }
                else if (columnArray is Date64Array d64Array)
                {
                    value = d64Array.GetDateTime(_batchRowIndex) ?? default;
                }
                else
                {
                    value = default; 
                }
            }
            return (ValueGetter<TValue>)(object)(ValueGetter<DateTime>)getter;
        }

        if (type == typeof(TimeSpan))
        {
            void getter(ref TimeSpan value)
            {
                var columnArray = _currentBatch!.Column(colIndex);

                if (columnArray.IsNull(_batchRowIndex))
                {
                    value = default;
                    return;
                }

                if (columnArray is DurationArray durArray)
                {
                    long val = durArray.Values[_batchRowIndex];
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
                    long val = t64Array.Values[_batchRowIndex];
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
                    long val = t32Array.Values[_batchRowIndex];
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
        var columnArray = _currentBatch!.Column(colIndex);

        if (columnArray.IsNull(_batchRowIndex))
        {
            value = default;
            return;
        }

        ReadOnlySpan<TPrimitive> span = default;
        int length = 0;

        if (columnArray is FixedSizeListArray fsList)
        {
            length = ((FixedSizeListType)fsList.Data.DataType).ListSize;
            ReadOnlySpan<TPrimitive> allValues = getValuesSpan(fsList.Values); 
            int offset = _batchRowIndex * length;
            span = allValues.Slice(offset, length);
        }
        else if (columnArray is ListArray list)
        {
            int offset = list.ValueOffsets[_batchRowIndex];
            length = list.ValueOffsets[_batchRowIndex + 1] - offset;
            ReadOnlySpan<TPrimitive> allValues = getValuesSpan(list.Values);
            span = allValues.Slice(offset, length);
        }

        var editor = VBufferEditor.Create(ref value, length);
        span.CopyTo(editor.Values);
        value = editor.Commit();
    }

    private delegate ReadOnlySpan<T> SpanExtractor<T>(IArrowArray array) where T : unmanaged;

    /// <summary>
    /// ML.NET API checking if a column is active
    /// </summary>
    public override bool IsColumnActive(DataViewSchema.Column column) => true;
}