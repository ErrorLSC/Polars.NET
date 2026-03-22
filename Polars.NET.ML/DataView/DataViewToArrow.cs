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
        var (recordBatches,arrowSchema) = DataViewToArrow.ToArrowBatches(dataView,batchSize); 
        return ArrowStreamInterop.ImportEager(recordBatches, arrowSchema);
    }  
}

/// <summary>
/// Provide Methods to convert DataView to ArrowBatches
/// </summary>
public static class DataViewToArrow
{
    /// <summary>
    /// Convert DataView to ArrowBatches, return RecordBatchs,and its schema
    /// </summary>
    public static (IEnumerable<RecordBatch>, Schema) ToArrowBatches(this IDataView dataView, int batchSize = 64_000)
    {
        var arrowSchema = BuildArrowSchema(dataView.Schema);

        var batches = PumpLazily(dataView, arrowSchema, batchSize);

        return (batches, arrowSchema);  
    }
    private static IEnumerable<RecordBatch> PumpLazily(
        IDataView dataView, 
        Schema arrowSchema, 
        int batchSize)
    {
        using var cursor = dataView.GetRowCursor(dataView.Schema);
        
        var pumpers = CreatePumpers(cursor, dataView.Schema).ToArray();

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
                yield return BuildBatch(arrowSchema, pumpers, rowCount);
                rowCount = 0;
            }
        }

        if (rowCount > 0)
        {
            yield return BuildBatch(arrowSchema, pumpers, rowCount);
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
    private static Schema BuildArrowSchema(DataViewSchema schema)
    {
        var fields = new Field[schema.Count];

        for (int i = 0; i < schema.Count; i++)
        {
            var col = schema[i];
            
            IArrowType arrowType = ArrowDataViewMapper.GetArrowType(col.Type);
            
            fields[i] = new Field(col.Name, arrowType, nullable: true);
        }

        return new Schema(fields, null);
    }

    // --- Pumper Factory ---
    private static IEnumerable<IColumnPumper> CreatePumpers(DataViewRowCursor cursor, DataViewSchema schema)
    {
        foreach (var col in schema)
        {
            Type rawType = col.Type.RawType;

            if (col.Type is VectorDataViewType vecType)
            {
                if (vecType.ItemType == NumberDataViewType.Single) 
                {
                    if (vecType.Size > 0) yield return new FloatVectorPumper(cursor, col, vecType.Size);
                    else yield return new VarLenFloatVectorPumper(cursor, col);
                }
                else if (vecType.ItemType == NumberDataViewType.Int32) 
                {
                    if (vecType.Size > 0) yield return new Int32VectorPumper(cursor, col, vecType.Size);
                    else yield return new VarLenInt32VectorPumper(cursor, col);
                }
                else throw new NotSupportedException($"Vector of type '{vecType.ItemType.RawType.Name}' is not supported.");
                continue;
            }

            if (rawType == typeof(float)) yield return new FloatPumper(cursor, col);
            else if (rawType == typeof(double)) yield return new DoublePumper(cursor, col);
            else if (rawType == typeof(int)) yield return new Int32Pumper(cursor, col);
            else if (rawType == typeof(long)) yield return new Int64Pumper(cursor, col);
            else if (rawType == typeof(bool)) yield return new BooleanPumper(cursor, col);
            else if (rawType == typeof(ReadOnlyMemory<char>)) yield return new StringPumper(cursor, col);
            else if (rawType == typeof(DateTime)) yield return new DateTimePumper(cursor, col);
            else if (rawType == typeof(TimeSpan)) yield return new TimeSpanPumper(cursor, col);
            
            else if (rawType == typeof(sbyte)) yield return new Int8Pumper(cursor, col);
            else if (rawType == typeof(short)) yield return new Int16Pumper(cursor, col);
            else if (rawType == typeof(byte)) yield return new UInt8Pumper(cursor, col);
            else if (rawType == typeof(ushort)) yield return new UInt16Pumper(cursor, col);
            else if (rawType == typeof(uint)) yield return new UInt32Pumper(cursor, col);
            else if (rawType == typeof(ulong)) yield return new UInt64Pumper(cursor, col);
            
            else throw new NotSupportedException($"DataView type {col.Type} (RawType: {rawType.Name}) is not supported for reverse pumping yet.");
        }
    }
}

internal interface IColumnPumper
{
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
    private readonly ValueGetter<ushort> _getter = cursor.GetGetter<ushort>(col);
    private readonly UInt16Array.Builder _builder = new();
    private ushort _val;

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

    public void Pump()
    {
        _getter(ref _val);
        
        if (_val.IsEmpty) 
        {
            _builder.Append([]);
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

    private readonly FixedSizeListType _arrowType = new(FloatType.Default, vectorSize);

    private readonly float[] _denseBuffer = new float[vectorSize]; 
    private VBuffer<float> _val;

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
            _arrowType, 
            length, 
            flatArray, 
            ArrowBuffer.Empty); 
    }
}

internal sealed class Int32VectorPumper(DataViewRowCursor cursor, DataViewSchema.Column col, int vectorSize) : IColumnPumper
{
    private readonly ValueGetter<VBuffer<int>> _getter = cursor.GetGetter<VBuffer<int>>(col);
    private readonly Int32Array.Builder _valueBuilder = new();
    
    private readonly FixedSizeListType _arrowType = new(Int32Type.Default, vectorSize);
    
    private readonly int[] _denseBuffer = new int[vectorSize]; 
    private VBuffer<int> _val;

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
            _arrowType, 
            length, 
            flatArray, 
            ArrowBuffer.Empty); 
    }
}

// ==========================================================
// Variable-Length Tensor Pumpers (LargeListArray)
// ==========================================================

internal sealed class VarLenFloatVectorPumper : IColumnPumper
{
    private readonly ValueGetter<VBuffer<float>> _getter;
    private readonly FloatArray.Builder _valueBuilder = new();
    
    private readonly ArrowBuffer.Builder<long> _offsetsBuilder = new();
    private readonly LargeListType _arrowType = new(FloatType.Default);
    
    private VBuffer<float> _val;
    private long _currentOffset = 0L; 

    public VarLenFloatVectorPumper(DataViewRowCursor cursor, DataViewSchema.Column col) 
    { 
        _getter = cursor.GetGetter<VBuffer<float>>(col);
        _offsetsBuilder.Append(0L); 
    }

    public void Pump()
    {
        _getter(ref _val);
        int len = _val.Length; 

        if (len > 0)
        {
            var rented = System.Buffers.ArrayPool<float>.Shared.Rent(len);
            Span<float> span = rented.AsSpan(0, len);
            _val.CopyTo(span);
            _valueBuilder.Append(span);
            System.Buffers.ArrayPool<float>.Shared.Return(rented);
        }

        _currentOffset += len;
        _offsetsBuilder.Append(_currentOffset);
    }

    public IArrowArray BuildArrayAndClear()
    {
        var valuesArray = _valueBuilder.Build();
        var offsetsBuffer = _offsetsBuilder.Build();
        
        _valueBuilder.Clear();
        _offsetsBuilder.Clear();

        int length = (offsetsBuffer.Length / sizeof(long)) - 1; 

        _currentOffset = 0L;
        _offsetsBuilder.Append(0L);

        var data = new ArrayData(
            _arrowType, length, 0, 0,
            [ArrowBuffer.Empty, offsetsBuffer], 
            [valuesArray.Data]                  
        );
        return new LargeListArray(data); 
    }
}

internal sealed class VarLenInt32VectorPumper : IColumnPumper
{
    private readonly ValueGetter<VBuffer<int>> _getter;
    private readonly Int32Array.Builder _valueBuilder = new();
    
    private readonly ArrowBuffer.Builder<long> _offsetsBuilder = new();
    private readonly LargeListType _arrowType = new(Int32Type.Default);
    
    private VBuffer<int> _val;
    private long _currentOffset = 0L;

    public VarLenInt32VectorPumper(DataViewRowCursor cursor, DataViewSchema.Column col) 
    { 
        _getter = cursor.GetGetter<VBuffer<int>>(col);
        _offsetsBuilder.Append(0L);
    }

    public void Pump()
    {
        _getter(ref _val);
        int len = _val.Length;

        if (len > 0)
        {
            var rented = System.Buffers.ArrayPool<int>.Shared.Rent(len);
            Span<int> span = rented.AsSpan(0, len);
            _val.CopyTo(span);
            _valueBuilder.Append(span);
            System.Buffers.ArrayPool<int>.Shared.Return(rented);
        }

        _currentOffset += len;
        _offsetsBuilder.Append(_currentOffset);
    }

    public IArrowArray BuildArrayAndClear()
    {
        var valuesArray = _valueBuilder.Build();
        var offsetsBuffer = _offsetsBuilder.Build();
        
        _valueBuilder.Clear();
        _offsetsBuilder.Clear();

        int length = (offsetsBuffer.Length / sizeof(long)) - 1; 
        
        _currentOffset = 0L;
        _offsetsBuilder.Append(0L);

        var data = new ArrayData(
            _arrowType, length, 0, 0,
            [ArrowBuffer.Empty, offsetsBuffer], 
            [valuesArray.Data]
        );
        return new LargeListArray(data); 
    }
}