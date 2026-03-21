using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.ML;
using Microsoft.ML.Data;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;

namespace Polars.NET.ML.DataView;

internal static class DataViewToPolarsExtensions
{
    /// <summary>
    /// Convert IDataView to Polars DataFrame
    /// </summary>
    public static DataFrameHandle ToPolarsDataFrameHandle(this IDataView dataView, int batchSize = 64_000)
    {
        // Create Pumpers
        var pumpers = CreatePumpers(dataView).ToArray();

        // Build Arrow Schema
        var arrowFields = pumpers.Select(p => p.ArrowField).ToList();
        var arrowSchema = new Schema(arrowFields, null);

        // Pump to Arrow
        IEnumerable<RecordBatch> recordBatches = PumpToArrowBatches(dataView, arrowSchema, pumpers, batchSize);
        
        return ArrowStreamInterop.ImportEager(recordBatches, arrowSchema);
    }

    private static IEnumerable<RecordBatch> PumpToArrowBatches(
        IDataView dataView, 
        Schema schema, 
        IColumnPumper[] pumpers, 
        int batchSize)
    {
        using var cursor = dataView.GetRowCursor(dataView.Schema);
        int rowCount = 0;

        while (cursor.MoveNext())
        {
            for (int i = 0; i < pumpers.Length; i++)
            {
                pumpers[i].Pump();
            }

            rowCount++;

            if (rowCount >= batchSize)
            {
                yield return BuildBatch(schema, pumpers, rowCount);
                rowCount = 0;
            }
        }

        if (rowCount > 0)
        {
            yield return BuildBatch(schema, pumpers, rowCount);
        }
    }

    private static RecordBatch BuildBatch(Schema schema, IColumnPumper[] pumpers, int length)
    {
        var arrays = new IArrowArray[pumpers.Length];
        for (int i = 0; i < pumpers.Length; i++)
        {
            arrays[i] = pumpers[i].BuildArrayAndClear();
        }
        return new RecordBatch(schema, arrays, length);
    }

    // --- Pumper Factory ---
    private static IEnumerable<IColumnPumper> CreatePumpers(IDataView dataView)
    {
        using var cursor = dataView.GetRowCursor(dataView.Schema);

        foreach (var col in dataView.Schema)
        {
            var type = col.Type;

            if (type is NumberDataViewType numType)
            {
                if (numType == NumberDataViewType.Single) yield return new FloatPumper(cursor, col);
                else if (numType == NumberDataViewType.Double) yield return new DoublePumper(cursor, col);
                else if (numType == NumberDataViewType.SByte) yield return new Int8Pumper(cursor, col);
                else if (numType == NumberDataViewType.Int16) yield return new Int16Pumper(cursor, col);
                else if (numType == NumberDataViewType.Int32) yield return new Int32Pumper(cursor, col);
                else if (numType == NumberDataViewType.Int64) yield return new Int64Pumper(cursor, col);
                else if (numType == NumberDataViewType.Byte) yield return new UInt8Pumper(cursor, col);
                else if (numType == NumberDataViewType.UInt16) yield return new UInt16Pumper(cursor, col);
                else if (numType == NumberDataViewType.UInt32) yield return new UInt32Pumper(cursor, col);
                else if (numType == NumberDataViewType.UInt64) yield return new UInt64Pumper(cursor, col);
                else throw new NotSupportedException($"Numeric type {numType.RawType.Name} is currently missing a pumper.");
            }
            else if (type is TextDataViewType)
            {
                yield return new StringPumper(cursor, col);
            }
            else if (type is BooleanDataViewType)
            {
                yield return new BooleanPumper(cursor, col);
            }
            else if (type is DateTimeDataViewType)
            {
                yield return new DateTimePumper(cursor, col);
            }
            else if (type is TimeSpanDataViewType)
            {
                yield return new TimeSpanPumper(cursor, col);
            }
            else if (type is VectorDataViewType vecType)
            {
                if (vecType.ItemType == NumberDataViewType.Single)
                {
                    yield return new FloatVectorPumper(cursor, col, vecType.Size);
                }
                else if (vecType.ItemType == NumberDataViewType.Int32)
                {
                    yield return new Int32VectorPumper(cursor, col, vecType.Size);
                }
                else
                {
                    throw new NotSupportedException($"Vector of type '{vecType.ItemType.RawType.Name}' is not supported for reverse pumping yet.");
                }
            }
            else
            {
                throw new NotSupportedException($"DataView type {type} is not supported for reverse pumping yet.");
            }
        }
    }
}

internal interface IColumnPumper
{
    Field ArrowField { get; }
    void Pump();
    IArrowArray BuildArrayAndClear();
}

// ==========================================================
// Float Pumper
// ==========================================================
internal sealed class FloatPumper(DataViewRowCursor cursor, DataViewSchema.Column col) : IColumnPumper
{
    private readonly ValueGetter<float> _getter = cursor.GetGetter<float>(col);
    private readonly FloatArray.Builder _builder = new();
    private float _val;

    public Field ArrowField { get; } = new Field(col.Name, FloatType.Default, true);

    public void Pump()
    {
        _getter(ref _val);
        
        if (float.IsNaN(_val)) _builder.AppendNull();
        else _builder.Append(_val);
    }

    public IArrowArray BuildArrayAndClear()
    {
        var arr = _builder.Build();
        _builder.Clear();
        return arr;
    }
}

internal sealed class DoublePumper(DataViewRowCursor cursor, DataViewSchema.Column col) : IColumnPumper
{
    private readonly ValueGetter<double> _getter = cursor.GetGetter<double>(col);
    private readonly DoubleArray.Builder _builder = new();
    private double _val;

    public Field ArrowField { get; } = new Field(col.Name, DoubleType.Default, true);

    public void Pump()
    {
        _getter(ref _val);
        
        if (double.IsNaN(_val)) _builder.AppendNull();
        else _builder.Append(_val);
    }

    public IArrowArray BuildArrayAndClear()
    {
        var arr = _builder.Build();
        _builder.Clear();
        return arr;
    }
}

// ==========================================================
// Integer Pumper
// ==========================================================

internal sealed class Int8Pumper(DataViewRowCursor cursor, DataViewSchema.Column col) : IColumnPumper
{
    private readonly ValueGetter<sbyte> _getter = cursor.GetGetter<sbyte>(col);
    private readonly Int8Array.Builder _builder = new();
    private sbyte _val;

    public Field ArrowField { get; } = new Field(col.Name, Int8Type.Default, true);

    public void Pump()
    {
        _getter(ref _val);
        
        _builder.Append(_val);
    }

    public IArrowArray BuildArrayAndClear()
    {
        var arr = _builder.Build();
        _builder.Clear();
        return arr;
    }
}

internal sealed class UInt16Pumper(DataViewRowCursor cursor, DataViewSchema.Column col) : IColumnPumper
{
    private readonly ValueGetter<byte> _getter = cursor.GetGetter<byte>(col);
    private readonly UInt16Array.Builder _builder = new();
    private byte _val;

    public Field ArrowField { get; } = new Field(col.Name, UInt16Type.Default, true);

    public void Pump()
    {
        _getter(ref _val);
        
        _builder.Append(_val);
    }

    public IArrowArray BuildArrayAndClear()
    {
        var arr = _builder.Build();
        _builder.Clear();
        return arr;
    }
}

internal sealed class UInt32Pumper(DataViewRowCursor cursor, DataViewSchema.Column col) : IColumnPumper
{
    private readonly ValueGetter<uint> _getter = cursor.GetGetter<uint>(col);
    private readonly UInt32Array.Builder _builder = new();
    private uint _val;

    public Field ArrowField { get; } = new Field(col.Name, UInt32Type.Default, true);

    public void Pump()
    {
        _getter(ref _val);
        
        _builder.Append(_val);
    }

    public IArrowArray BuildArrayAndClear()
    {
        var arr = _builder.Build();
        _builder.Clear();
        return arr;
    }
}

internal sealed class Int64Pumper(DataViewRowCursor cursor, DataViewSchema.Column col) : IColumnPumper
{
    private readonly ValueGetter<long> _getter = cursor.GetGetter<long>(col);
    private readonly Int64Array.Builder _builder = new();
    private long _val;

    public Field ArrowField { get; } = new Field(col.Name, Int64Type.Default, true);

    public void Pump()
    {
        _getter(ref _val);
        
        _builder.Append(_val);
    }

    public IArrowArray BuildArrayAndClear()
    {
        var arr = _builder.Build();
        _builder.Clear();
        return arr;
    }
}

// ==========================================================
// Unsigned Integer Pumper
// ==========================================================

internal sealed class UInt8Pumper(DataViewRowCursor cursor, DataViewSchema.Column col) : IColumnPumper
{
    private readonly ValueGetter<byte> _getter = cursor.GetGetter<byte>(col);
    private readonly UInt8Array.Builder _builder = new();
    private byte _val;

    public Field ArrowField { get; } = new Field(col.Name, UInt8Type.Default, true);

    public void Pump()
    {
        _getter(ref _val);
        
        _builder.Append(_val);
    }

    public IArrowArray BuildArrayAndClear()
    {
        var arr = _builder.Build();
        _builder.Clear();
        return arr;
    }
}

internal sealed class Int16Pumper(DataViewRowCursor cursor, DataViewSchema.Column col) : IColumnPumper
{
    private readonly ValueGetter<short> _getter = cursor.GetGetter<short>(col);
    private readonly Int16Array.Builder _builder = new();
    private short _val;

    public Field ArrowField { get; } = new Field(col.Name, Int16Type.Default, true);

    public void Pump()
    {
        _getter(ref _val);
        
        _builder.Append(_val);
    }

    public IArrowArray BuildArrayAndClear()
    {
        var arr = _builder.Build();
        _builder.Clear();
        return arr;
    }
}

internal sealed class Int32Pumper(DataViewRowCursor cursor, DataViewSchema.Column col) : IColumnPumper
{
    private readonly ValueGetter<int> _getter = cursor.GetGetter<int>(col);
    private readonly Int32Array.Builder _builder = new();
    private int _val;

    public Field ArrowField { get; } = new Field(col.Name, Int32Type.Default, true);

    public void Pump()
    {
        _getter(ref _val);
        
        _builder.Append(_val);
    }

    public IArrowArray BuildArrayAndClear()
    {
        var arr = _builder.Build();
        _builder.Clear();
        return arr;
    }
}

internal sealed class UInt64Pumper(DataViewRowCursor cursor, DataViewSchema.Column col) : IColumnPumper
{
    private readonly ValueGetter<ulong> _getter = cursor.GetGetter<ulong>(col);
    private readonly UInt64Array.Builder _builder = new();
    private ulong _val;

    public Field ArrowField { get; } = new Field(col.Name, UInt64Type.Default, true);

    public void Pump()
    {
        _getter(ref _val);
        
        _builder.Append(_val);
    }

    public IArrowArray BuildArrayAndClear()
    {
        var arr = _builder.Build();
        _builder.Clear();
        return arr;
    }
}

// ==========================================================
// Boolean Pumper
// ==========================================================

internal sealed class BooleanPumper(DataViewRowCursor cursor, DataViewSchema.Column col) : IColumnPumper
{
    private readonly ValueGetter<bool> _getter = cursor.GetGetter<bool>(col);
    private readonly BooleanArray.Builder _builder = new();
    private bool _val;

    public Field ArrowField { get; } = new Field(col.Name, BooleanType.Default, true);

    public void Pump()
    {
        _getter(ref _val);

        _builder.Append(_val);
    }

    public IArrowArray BuildArrayAndClear()
    {
        var arr = _builder.Build();
        _builder.Clear();
        return arr;
    }
}

// ==========================================================
// String Pumper
// ==========================================================
internal sealed class StringPumper(DataViewRowCursor cursor, DataViewSchema.Column col) : IColumnPumper
{
    private readonly ValueGetter<ReadOnlyMemory<char>> _getter = cursor.GetGetter<ReadOnlyMemory<char>>(col);
    private readonly StringViewArray.Builder _builder = new();
    private ReadOnlyMemory<char> _val;

    public Field ArrowField { get; } = new Field(col.Name, Apache.Arrow.Types.StringViewType.Default, true);

    public void Pump()
    {
        _getter(ref _val);
        
        if (_val.IsEmpty) 
        {
            _builder.AppendNull(); 
            return;
        }

        ReadOnlySpan<char> charSpan = _val.Span;
        
        int maxByteCount = System.Text.Encoding.UTF8.GetMaxByteCount(charSpan.Length);

        if (maxByteCount <= 1024)
        {
            Span<byte> byteSpan = stackalloc byte[maxByteCount];
            int actualByteCount = System.Text.Encoding.UTF8.GetBytes(charSpan, byteSpan);
            
            _builder.Append(byteSpan[..actualByteCount]);
        }
        else
        {
            byte[] rentedBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(maxByteCount);
            try
            {
                int actualByteCount = System.Text.Encoding.UTF8.GetBytes(charSpan, rentedBuffer);
                _builder.Append(new ReadOnlySpan<byte>(rentedBuffer, 0, actualByteCount));
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(rentedBuffer);
            }
        }
    }

    public IArrowArray BuildArrayAndClear()
    {
        var arr = _builder.Build();
        _builder.Clear();
        return arr;
    }
}

// ==========================================================
// DateTime Pumper
// ==========================================================

internal sealed class DateTimePumper(DataViewRowCursor cursor, DataViewSchema.Column col) : IColumnPumper
{
    private readonly ValueGetter<DateTime> _getter = cursor.GetGetter<DateTime>(col);
    
    private static readonly TimestampType _arrowType = new(TimeUnit.Microsecond, timezone: null as string);
    
    private readonly TimestampArray.Builder _builder = new(_arrowType);
    private DateTime _val;

    public Field ArrowField { get; } = new Field(col.Name, _arrowType, true);

    public void Pump()
    {
        _getter(ref _val);
        
        if (_val == DateTime.MinValue) 
        {
            _builder.AppendNull();
        }
        else 
        {
            _builder.Append(new DateTimeOffset(_val.Ticks, TimeSpan.Zero));
        }
    }

    public IArrowArray BuildArrayAndClear()
    {
        var arr = _builder.Build();
        _builder.Clear();
        return arr;
    }
}

internal sealed class TimeSpanPumper(DataViewRowCursor cursor, DataViewSchema.Column col) : IColumnPumper
{
    private readonly ValueGetter<TimeSpan> _getter = cursor.GetGetter<TimeSpan>(col);
    
    private static readonly DurationType _arrowType = DurationType.Microsecond;
    
    private readonly DurationArray.Builder _builder = new(_arrowType);
    private TimeSpan _val;

    public Field ArrowField { get; } = new Field(col.Name, _arrowType, true);

    public void Pump()
    {
        _getter(ref _val);
        
        if (_val == TimeSpan.MinValue) 
        {
            _builder.AppendNull();
        }
        else 
        {
            long microSeconds = _val.Ticks / 10;
            _builder.Append(microSeconds);
        }
    }

    public IArrowArray BuildArrayAndClear()
    {
        var arr = _builder.Build();
        _builder.Clear();
        return arr;
    }
}

// ==========================================================
// Tensor/Vector Pumper
// ==========================================================
internal sealed class FloatVectorPumper(DataViewRowCursor cursor, DataViewSchema.Column col, int vectorSize) : IColumnPumper
{
    private readonly ValueGetter<VBuffer<float>> _getter = cursor.GetGetter<VBuffer<float>>(col);
    private readonly FloatArray.Builder _valueBuilder = new();

    private readonly float[] _denseBuffer = new float[vectorSize]; 
    private VBuffer<float> _val;

    public Field ArrowField { get; } = new Field(col.Name, new FixedSizeListType(FloatType.Default, vectorSize), true);

    public void Pump()
    {
        _getter(ref _val);

        _val.CopyTo(_denseBuffer);

        _valueBuilder.Append(_denseBuffer);
    }

    public IArrowArray BuildArrayAndClear()
    {
        var flatArray = _valueBuilder.Build();
        _valueBuilder.Clear();

        int length = flatArray.Length / vectorSize;
        return new FixedSizeListArray(
            (FixedSizeListType)ArrowField.DataType, 
            length, 
            flatArray, 
            ArrowBuffer.Empty); 
    }
}

internal sealed class Int32VectorPumper : IColumnPumper
{
    private readonly ValueGetter<VBuffer<int>> _getter;
    private readonly Int32Array.Builder _valueBuilder = new();
    private readonly int _vectorSize;

    private readonly int[] _denseBuffer; 
    private VBuffer<int> _val;

    public Field ArrowField { get; }

    public Int32VectorPumper(DataViewRowCursor cursor, DataViewSchema.Column col, int vectorSize)
    {
        _getter = cursor.GetGetter<VBuffer<int>>(col);
        _vectorSize = vectorSize;
        _denseBuffer = new int[vectorSize]; 

        ArrowField = new Field(col.Name, new FixedSizeListType(Int32Type.Default, vectorSize), true);
    }

    public void Pump()
    {
        _getter(ref _val);
        _val.CopyTo(_denseBuffer);
        _valueBuilder.Append(_denseBuffer);
    }

    public IArrowArray BuildArrayAndClear()
    {
        var flatArray = (Int32Array)_valueBuilder.Build();
        _valueBuilder.Clear();

        int length = flatArray.Length / _vectorSize;
        
        return new FixedSizeListArray(
            (FixedSizeListType)ArrowField.DataType, 
            length, 
            flatArray, 
            ArrowBuffer.Empty); 
    }
}