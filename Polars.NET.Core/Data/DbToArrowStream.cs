using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Polars.NET.Core.Arrow;

namespace Polars.NET.Core.Data;
public static class DbToArrowStream
{
    /// <summary>
    /// High-Performance: IDataReader -> Arrow RecordBatch Stream
    /// Zero-Boxing on Hot Paths.
    /// </summary>
    public static IEnumerable<RecordBatch> ToArrowBatches(IDataReader reader, int batchSize = 50_000)
    {
        // 1. Resolve Schema
        var schema = ArrowTypeResolver.GetSchemaFromDataReader(reader);
        int fieldCount = reader.FieldCount;
        
        if (schema.FieldsList.Count != fieldCount)
        {
            throw new InvalidOperationException(
                $"Schema mismatch! Reader has {fieldCount} fields, but Schema has {schema.FieldsList.Count} fields.");
        }

        // 2. Initialize Builders
        bool isDbReader = reader is DbDataReader;

        var builders = new ColumnBuilder[fieldCount];
        for (int i = 0; i < fieldCount; i++)
        {
            var field = schema.FieldsList[i];
            var netType = reader.GetFieldType(i);
            
            builders[i] = ColumnBuilderFactory.Create(field, netType, isDbReader, batchSize);
        }

        // 3. Pump Loop
        int rowCount = 0;
        while (reader.Read())
        {
            for (int i = 0; i < fieldCount; i++)
            {
                builders[i].Add(reader, i);
            }

            rowCount++;

            if (rowCount >= batchSize)
            {
                yield return BuildBatch(schema, builders, rowCount);
                rowCount = 0;
            }
        }

        if (rowCount > 0)
        {
            yield return BuildBatch(schema, builders, rowCount);
        }
    }
    
    private static RecordBatch BuildBatch(Schema schema, ColumnBuilder[] builders, int length)
    {
        var arrays = new IArrowArray[builders.Length];
        for (int i = 0; i < builders.Length; i++)
        {
            arrays[i] = builders[i].Build();
        }
        return new RecordBatch(schema, arrays, length);
    }
}

// ==================================================================================
// Column Builder System (Polymorphic & Zero-Boxing)
// ==================================================================================

internal abstract class ColumnBuilder
{
    // For DataReader
    public abstract void Add(IDataReader reader, int ordinal);
    
    // Universal Path (UDF / Buffer)
    public abstract void AddObject(object? value);

    public abstract IArrowArray Build();
}

internal static class ColumnBuilderFactory
{
   public static ColumnBuilder Create(Field field, Type netType, bool isDbReader, int capacity = 50000)
    {
        var typeId = field.DataType.TypeId;

        // =====================================
        // Primitives
        // =====================================
        
        if (typeId == ArrowTypeId.Int8) return new Int8ColumnBuilder(capacity, netType, isDbReader);
        if (typeId == ArrowTypeId.Int16) return new Int16ColumnBuilder(capacity, netType, isDbReader);
        if (typeId == ArrowTypeId.UInt16) return new UInt16ColumnBuilder(capacity, netType, isDbReader);
        if (typeId == ArrowTypeId.UInt32) return new UInt32ColumnBuilder(capacity, netType, isDbReader);
        if (typeId == ArrowTypeId.UInt64) return new UInt64ColumnBuilder(capacity, netType, isDbReader);

        if (typeId == ArrowTypeId.Int32) return new Int32ColumnBuilder(capacity);
        if (typeId == ArrowTypeId.Int64) return new Int64ColumnBuilder(capacity);
        if (typeId == ArrowTypeId.Double) return new DoubleColumnBuilder(capacity);
        if (typeId == ArrowTypeId.Boolean) return new BooleanColumnBuilder(capacity);
        if (typeId == ArrowTypeId.Float) return new FloatColumnBuilder(capacity);
        if (typeId == ArrowTypeId.HalfFloat) return new HalfFloatColumnBuilder(capacity);
        if (typeId == ArrowTypeId.UInt8)  return new UInt8ColumnBuilder(capacity);

        // String -> StringView
        if (typeId == ArrowTypeId.String || typeId == ArrowTypeId.LargeString || typeId == ArrowTypeId.StringView) 
            return new StringViewColumnBuilder(capacity);

        // =====================================
        // Date & Time
        // =====================================
        if (typeId == ArrowTypeId.Timestamp) return new TimestampColumnBuilder((TimestampType)field.DataType, capacity);
        
        if (typeId == ArrowTypeId.Date32) return new Date32ColumnBuilder(capacity, netType, isDbReader);
        
        if (typeId == ArrowTypeId.Time64) return new Time64ColumnBuilder(capacity);
        if (typeId == ArrowTypeId.Duration) return new DurationColumnBuilder(capacity);

        // Decimal 
        if (typeId == ArrowTypeId.Decimal128) return new DecimalColumnBuilder((Decimal128Type)field.DataType, capacity);

        if (typeId == ArrowTypeId.List || typeId == ArrowTypeId.LargeList ||
            typeId == ArrowTypeId.Struct || typeId == ArrowTypeId.FixedSizeList ||
            typeId == ArrowTypeId.Map ||
            typeId == ArrowTypeId.Dictionary ||
            typeId == ArrowTypeId.Union)
        {
            return new ComplexTypeColumnBuilder(capacity); 
        }
        if (netType != typeof(string) && !netType.IsValueType && netType != typeof(byte[]))
        {
            return new ComplexTypeColumnBuilder(capacity);
        }

        // =====================================
        // Binary
        // =====================================
        if (typeId == ArrowTypeId.Binary || 
            typeId == ArrowTypeId.LargeBinary || 
            typeId == ArrowTypeId.BinaryView) 
        {
            return new BinaryColumnBuilder(capacity, netType, isDbReader);
        }
        if (typeId == ArrowTypeId.FixedSizedBinary) 
        {
            return new FixedSizeBinaryColumnBuilder((FixedSizeBinaryType)field.DataType, capacity, netType, isDbReader);
        }

        // Fallback
        return new FallbackColumnBuilder(capacity);
    }
}

// ==================================================================================
// Concrete Implementations
// ==================================================================================

internal sealed class ConcreteFixedSizeBinaryBuilder(FixedSizeBinaryType type) : 
    FixedSizeBinaryArray.BuilderBase<FixedSizeBinaryArray, ConcreteFixedSizeBinaryBuilder>(type, type.ByteWidth)
{

    protected override FixedSizeBinaryArray Build(ArrayData data)
    {
        return new FixedSizeBinaryArray(data);
    }
}

internal sealed class FixedSizeBinaryColumnBuilder : ColumnBuilder
{
    private readonly ConcreteFixedSizeBinaryBuilder _builder;
    private readonly int _byteWidth;
    
    private readonly bool _isGuidFastPath;
    private readonly bool _isByteArrayFastPath;

    public FixedSizeBinaryColumnBuilder(FixedSizeBinaryType type, int capacity, Type netType, bool isDbReader)
    {
        _builder = new ConcreteFixedSizeBinaryBuilder(type);
        _builder.Reserve(capacity);
        _byteWidth = type.ByteWidth;

        _isGuidFastPath = isDbReader && _byteWidth == 16 && netType == typeof(Guid);
        
        _isByteArrayFastPath = isDbReader && netType == typeof(byte[]);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override void Add(IDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            _builder.AppendNull();
            return;
        }

        if (_isGuidFastPath)
        {
            Guid guid = ((DbDataReader)reader).GetGuid(ordinal);
            
            Span<byte> guidBytes = stackalloc byte[16];
            guid.TryWriteBytes(guidBytes);
            
            _builder.Append(guidBytes);
            return;
        }

        if (_isByteArrayFastPath)
        {
            _builder.Append(((DbDataReader)reader).GetFieldValue<byte[]>(ordinal));
            return;
        }

        AddObject(reader.GetValue(ordinal));
    }

    public override void AddObject(object? v)
    {
        if (v == null || v == DBNull.Value)
        {
            _builder.AppendNull();
        }
        else if (v is Guid guid)
        {
            Span<byte> guidBytes = stackalloc byte[16];
            guid.TryWriteBytes(guidBytes);
            _builder.Append(guidBytes);
        }
        else if (v is byte[] bytes)
        {
            if (bytes.Length != _byteWidth)
            {
                throw new ArgumentException($"FixedSizeBinary expected {_byteWidth} bytes, but got {bytes.Length} bytes.");
            }
            _builder.Append(bytes);
        }
        else
        {
            _builder.Append((byte[])v);
        }
    }

    public override IArrowArray Build()
    {
        var arr = _builder.Build();
        _builder.Clear();
        return arr;
    }
}
internal sealed class BinaryColumnBuilder : ColumnBuilder
{
    private readonly BinaryViewArray.Builder _builder = new();
    
    private readonly bool _isGuidFastPath;
    private readonly bool _isByteArrayFastPath;

    public BinaryColumnBuilder(int capacity, Type netType, bool isDbReader) 
    { 
        _builder.Reserve(capacity); 
        
        _isGuidFastPath = isDbReader && netType == typeof(Guid);
        _isByteArrayFastPath = isDbReader && netType == typeof(byte[]);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override void Add(IDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            _builder.AppendNull();
            return;
        }

        // Guid Zero Allocation
        if (_isGuidFastPath)
        {
            Guid guid = ((DbDataReader)reader).GetGuid(ordinal);
            Span<byte> guidBytes = stackalloc byte[16];
            guid.TryWriteBytes(guidBytes);
            
            _builder.Append(guidBytes);
            return;
        }

        // byte[]
        if (_isByteArrayFastPath)
        {
            _builder.Append(((DbDataReader)reader).GetFieldValue<byte[]>(ordinal));
            return;
        }

        AddObject(reader.GetValue(ordinal));
    }

    public override void AddObject(object? v) 
    { 
        if (v == null || v == DBNull.Value) 
        {
            _builder.AppendNull(); 
        }
        else if (v is Guid guid)
        {
            Span<byte> guidBytes = stackalloc byte[16];
            guid.TryWriteBytes(guidBytes);
            _builder.Append(guidBytes);
        }
        else if (v is byte[] bytes)
        {
            _builder.Append(bytes);
        }
        else
        {
            _builder.Append((byte[])v);
        }
    }

    public override IArrowArray Build() 
    { 
        var arr = _builder.Build(); 
        _builder.Clear(); 
        return arr; 
    }
}

internal sealed class Int8ColumnBuilder : ColumnBuilder
{
    private readonly Int8Array.Builder _builder = new();
    
    private readonly bool _useFastPath;

    public Int8ColumnBuilder(int capacity, Type netType, bool isDbReader) 
    { 
        _builder.Reserve(capacity); 
        
        _useFastPath = isDbReader && netType == typeof(sbyte);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override void Add(IDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            _builder.AppendNull();
            return;
        }

        if (_useFastPath)
        {
            _builder.Append(((DbDataReader)reader).GetFieldValue<sbyte>(ordinal));
        }
        else
        {
            _builder.Append(Convert.ToSByte(reader.GetValue(ordinal)));
        }
    }
    public override void AddObject(object? v) 
    { 
        if (v == null) _builder.AppendNull(); 
        else _builder.Append(Convert.ToSByte(v)); 
    }
    
    public override IArrowArray Build() 
    { 
        var arr = _builder.Build(); 
        _builder.Clear(); 
        return arr; 
    }
}

internal sealed class Int16ColumnBuilder : ColumnBuilder
{
    private readonly Int16Array.Builder _builder = new();
    private readonly bool _useFastPath;

    public Int16ColumnBuilder(int capacity, Type netType, bool isDbReader) 
    { 
        _builder.Reserve(capacity); 
        
        _useFastPath = isDbReader && netType == typeof(short);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override void Add(IDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            _builder.AppendNull();
            return;
        }

        if (_useFastPath)
        {
            _builder.Append(((DbDataReader)reader).GetFieldValue<short>(ordinal));
        }
        else
        {
            _builder.Append(Convert.ToInt16(reader.GetValue(ordinal)));
        }
    }
    public override void AddObject(object? v) { if (v == null) _builder.AppendNull(); else _builder.Append(Convert.ToInt16(v)); }
    public override IArrowArray Build() { var arr = _builder.Build(); _builder.Clear(); return arr; }
}


internal sealed class UInt8ColumnBuilder : ColumnBuilder
{
    private readonly UInt8Array.Builder _builder = new();
    public UInt8ColumnBuilder(int capacity) { _builder.Reserve(capacity); }

    public override void Add(IDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal)) _builder.AppendNull();
        else _builder.Append(reader.GetByte(ordinal));
    }

    public override void AddObject(object? v) { if (v == null) _builder.AppendNull(); else _builder.Append(Convert.ToByte(v)); }
    public override IArrowArray Build() { var arr = _builder.Build(); _builder.Clear(); return arr; }
}


internal sealed class UInt16ColumnBuilder : ColumnBuilder
{
    private readonly UInt16Array.Builder _builder = new();
    private readonly bool _useFastPath;

    public UInt16ColumnBuilder(int capacity, Type netType, bool isDbReader) 
    { 
        _builder.Reserve(capacity); 
        
        _useFastPath = isDbReader && netType == typeof(ushort);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override void Add(IDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            _builder.AppendNull();
            return;
        }

        if (_useFastPath)
        {
            _builder.Append(((DbDataReader)reader).GetFieldValue<UInt16>(ordinal));
        }
        else
        {
            _builder.Append(Convert.ToUInt16(reader.GetValue(ordinal)));
        }
    }

    public override void AddObject(object? v) { if (v == null) _builder.AppendNull(); else _builder.Append(Convert.ToUInt16(v)); }
    public override IArrowArray Build() { var arr = _builder.Build(); _builder.Clear(); return arr; }
}

internal sealed class UInt32ColumnBuilder : ColumnBuilder
{
    private readonly UInt32Array.Builder _builder = new();
    private readonly bool _useFastPath;

    public UInt32ColumnBuilder(int capacity, Type netType, bool isDbReader) 
    { 
        _builder.Reserve(capacity); 
        
        _useFastPath = isDbReader && netType == typeof(uint);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override void Add(IDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            _builder.AppendNull();
            return;
        }

        if (_useFastPath)
        {
            _builder.Append(((DbDataReader)reader).GetFieldValue<uint>(ordinal));
        }
        else
        {
            _builder.Append(Convert.ToUInt32(reader.GetValue(ordinal)));
        }
    }

    public override void AddObject(object? v) { if (v == null) _builder.AppendNull(); else _builder.Append(Convert.ToUInt32(v)); }
    public override IArrowArray Build() { var arr = _builder.Build(); _builder.Clear(); return arr; }
}

internal sealed class UInt64ColumnBuilder : ColumnBuilder
{
    private readonly UInt64Array.Builder _builder = new();
   private readonly bool _useFastPath;

    public UInt64ColumnBuilder(int capacity, Type netType, bool isDbReader) 
    { 
        _builder.Reserve(capacity); 
        
        _useFastPath = isDbReader && netType == typeof(ulong);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override void Add(IDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            _builder.AppendNull();
            return;
        }

        if (_useFastPath)
        {
            _builder.Append(((DbDataReader)reader).GetFieldValue<ulong>(ordinal));
        }
        else
        {
            _builder.Append(Convert.ToUInt64(reader.GetValue(ordinal)));
        }
    }

    public override void AddObject(object? v) { if (v == null) _builder.AppendNull(); else _builder.Append(Convert.ToUInt64(v)); }
    public override IArrowArray Build() { var arr = _builder.Build(); _builder.Clear(); return arr; }
}
internal sealed class Int32ColumnBuilder : ColumnBuilder
{
    public Int32ColumnBuilder(int capacity) { _builder.Reserve(capacity); }
    public override void AddObject(object? v) { if (v == null) _builder.AppendNull(); else _builder.Append((int)v); }
    private readonly Int32Array.Builder _builder = new();
    public override void Add(IDataReader reader, int ordinal) {
        if (reader.IsDBNull(ordinal)) _builder.AppendNull();
        else _builder.Append(reader.GetInt32(ordinal));
    }
    public override IArrowArray Build() { var arr = _builder.Build(); _builder.Clear(); return arr; }
}

internal sealed class Int64ColumnBuilder : ColumnBuilder
{
    public Int64ColumnBuilder(int capacity) { _builder.Reserve(capacity); }
    public override void AddObject(object? v) { if (v == null) _builder.AppendNull(); else _builder.Append((long)v); }
    private readonly Int64Array.Builder _builder = new();
    public override void Add(IDataReader reader, int ordinal) {
        if (reader.IsDBNull(ordinal)) _builder.AppendNull();
        else _builder.Append(reader.GetInt64(ordinal));
    }
    public override IArrowArray Build() { var arr = _builder.Build(); _builder.Clear(); return arr; }
}

internal sealed class DoubleColumnBuilder : ColumnBuilder
{
    public DoubleColumnBuilder(int capacity) { _builder.Reserve(capacity); }
    public override void AddObject(object? v) { if (v == null) _builder.AppendNull(); else _builder.Append((double)v); }
    private readonly DoubleArray.Builder _builder = new();
    public override void Add(IDataReader reader, int ordinal) {
        if (reader.IsDBNull(ordinal)) _builder.AppendNull();
        else _builder.Append(reader.GetDouble(ordinal));
    }
    public override IArrowArray Build() { var arr = _builder.Build(); _builder.Clear(); return arr; }
}
internal sealed class HalfFloatColumnBuilder : ColumnBuilder
{
    private readonly HalfFloatArray.Builder _builder = new();

    public HalfFloatColumnBuilder(int capacity) 
    { 
        _builder.Reserve(capacity); 
    }

    public override void AddObject(object? v) 
    { 
        if (v == null || v == DBNull.Value) 
        {
            _builder.AppendNull(); 
        }
        else 
        {
            if (v is float f) 
            {
                _builder.Append((Half)f);
            }
            else if (v is double d) 
            {
                _builder.Append((Half)d);
            }
            else if (v is Half h)
            {
                _builder.Append(h);
            }
            else
            {
                try 
                {
                    _builder.Append((Half)Convert.ChangeType(v, typeof(float)));
                }
                catch
                {
                    throw new InvalidCastException($"Cannot convert value of type {v.GetType()} to System.Half.");
                }
            }
        }
    }

    public override void Add(IDataReader reader, int ordinal) 
    {
        if (reader.IsDBNull(ordinal)) 
        {
            _builder.AppendNull();
        }
        else 
        {
            _builder.Append((Half)reader.GetFloat(ordinal));
        }
    }

    public override IArrowArray Build() 
    { 
        var arr = _builder.Build(); 
        _builder.Clear(); 
        return arr; 
    }
}
internal sealed class FloatColumnBuilder : ColumnBuilder
{
    public FloatColumnBuilder(int capacity) { _builder.Reserve(capacity); }
    public override void AddObject(object? v) { if (v == null) _builder.AppendNull(); else _builder.Append((float)v); }
    private readonly FloatArray.Builder _builder = new();
    public override void Add(IDataReader reader, int ordinal) {
        if (reader.IsDBNull(ordinal)) _builder.AppendNull();
        else _builder.Append(reader.GetFloat(ordinal));
    }
    public override IArrowArray Build() { var arr = _builder.Build(); _builder.Clear(); return arr; }
}

internal sealed class BooleanColumnBuilder : ColumnBuilder
{
    public BooleanColumnBuilder(int capacity) { _builder.Reserve(capacity); }
    public override void AddObject(object? v) { if (v == null) _builder.AppendNull(); else _builder.Append((bool)v); }
    private readonly BooleanArray.Builder _builder = new();
    public override void Add(IDataReader reader, int ordinal) {
        if (reader.IsDBNull(ordinal)) _builder.AppendNull();
        else _builder.Append(reader.GetBoolean(ordinal));
    }
    public override IArrowArray Build() { var arr = _builder.Build(); _builder.Clear(); return arr; }
}

internal sealed class DecimalColumnBuilder : ColumnBuilder
{
    public DecimalColumnBuilder(Decimal128Type type, int capacity)
    {
        _builder = new Decimal128Array.Builder(type);
        _builder.Reserve(capacity);
    }
    public override void AddObject(object? v) { if (v == null) _builder.AppendNull(); else _builder.Append((decimal)v); }
    private readonly Decimal128Array.Builder _builder;

    // Accept Decimal128Type to ensure Builder Scale matches Schema Scale
    public DecimalColumnBuilder(Decimal128Type type)
    {
        _builder = new Decimal128Array.Builder(type);
    }

    public override void Add(IDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal)) 
            _builder.AppendNull();
        else 
            _builder.Append(reader.GetDecimal(ordinal)); // Builder handles rescaling
    }

    public override IArrowArray Build() { var arr = _builder.Build(); _builder.Clear(); return arr; }
}

internal sealed class StringViewColumnBuilder : ColumnBuilder
{
    public StringViewColumnBuilder(int capacity) { _builder.Reserve(capacity); }
    public override void AddObject(object? v) { if (v == null) _builder.AppendNull(); else _builder.Append((string)v); }
    private readonly StringViewArray.Builder _builder = new();

    public override void Add(IDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
            _builder.AppendNull();
        else
            _builder.Append(reader.GetString(ordinal));
    }

    public override IArrowArray Build() { var arr = _builder.Build(); _builder.Clear(); return arr; }
}

// ------------------------------------------------------------------------
// Date & Time
// ------------------------------------------------------------------------

internal sealed class Date32ColumnBuilder : ColumnBuilder
{
    private readonly Date32Array.Builder _builder = new();
    
    private readonly bool _isDateOnlyFastPath;
    private readonly bool _isDateTimeFastPath;

    public Date32ColumnBuilder(int capacity, Type netType, bool isDbReader) 
    { 
        _builder.Reserve(capacity); 
        
        _isDateOnlyFastPath = isDbReader && netType == typeof(DateOnly);
        
        _isDateTimeFastPath = netType == typeof(DateTime);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override void Add(IDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            _builder.AppendNull();
            return;
        }

        if (_isDateOnlyFastPath)
        {
            var date = ((DbDataReader)reader).GetFieldValue<DateOnly>(ordinal);
            _builder.Append(date.ToDateTime(TimeOnly.MinValue));
            return;
        }

        if (_isDateTimeFastPath)
        {
            _builder.Append(reader.GetDateTime(ordinal));
            return;
        }

        var val = reader.GetValue(ordinal);
        if (val is DateOnly d)
        {
            _builder.Append(d.ToDateTime(TimeOnly.MinValue));
        }
        else if (val is DateTime dt)
        {
            _builder.Append(dt);
        }
        else
        {
            var convertedDt = Convert.ToDateTime(val);
            _builder.Append(convertedDt);
        }
    }

    public override void AddObject(object? v) 
    {
        if (v == null || v == DBNull.Value) 
        { 
            _builder.AppendNull(); 
            return; 
        }
        
        if (v is DateOnly d) _builder.Append(d.ToDateTime(TimeOnly.MinValue));
        else if (v is DateTime dt) _builder.Append(dt);
        else _builder.Append(Convert.ToDateTime(v));
    }

    public override IArrowArray Build() 
    { 
        var arr = _builder.Build(); 
        _builder.Clear(); 
        return arr; 
    }
}
internal sealed class TimestampColumnBuilder : ColumnBuilder
{
    public TimestampColumnBuilder(TimestampType type, int capacity) { 
        _builder = new TimestampArray.Builder(type); 
        _builder.Reserve(capacity);
    }
    public override void AddObject(object? v)
    {
        if (v == null)
        {
            _builder.AppendNull();
        }
        else
        {
            DateTime dt;
            if (v is DateTime d) dt = d;
            else if (v is DateTimeOffset dto) dt = dto.DateTime; 
            else 
            {
                dt = Convert.ToDateTime(v);
            }

            var wallClockDto = new DateTimeOffset(dt.Ticks, TimeSpan.Zero);
            _builder.Append(wallClockDto);
        }
    }
    private readonly TimestampArray.Builder _builder = new();
    public TimestampColumnBuilder(TimestampType type)
    {
        _builder = new TimestampArray.Builder(type);
    }
    public override void Add(IDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            _builder.AppendNull();
        }
        else
        {
            var dt = reader.GetDateTime(ordinal);
            
            var wallClockDto = new DateTimeOffset(dt.Ticks, TimeSpan.Zero);
            _builder.Append(wallClockDto);
        }
    }

    public override IArrowArray Build() { var arr = _builder.Build(); _builder.Clear(); return arr; }
}

internal sealed class Time64ColumnBuilder : ColumnBuilder
{
    private readonly Time64Array.Builder _builder;
    public Time64ColumnBuilder(int capacity) { _builder = new Time64Array.Builder(TimeUnit.Nanosecond); _builder.Reserve(capacity); }
    public override void Add(IDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal)) { _builder.AppendNull(); return; }
        var val = reader.GetValue(ordinal);
        AddObject(val);
    }
    public override void AddObject(object? v)
    {
        if (v == null || v == DBNull.Value)
        {
            _builder.AppendNull();
            return;
        }


        long arrowTimeValue;

        if (v is TimeOnly timeOnly)
        {
            arrowTimeValue = timeOnly.Ticks * 100;
        }
        else
        {
            arrowTimeValue = Convert.ToInt64(v);
        }

        _builder.Append(arrowTimeValue);
    }
    public override IArrowArray Build() { var arr = _builder.Build(); _builder.Clear(); return arr; }
}

internal sealed class DurationColumnBuilder : ColumnBuilder
{
    private readonly DurationArray.Builder _builder;

    public DurationColumnBuilder(int capacity) 
    { 
        _builder = new DurationArray.Builder(DurationType.Microsecond); 
        _builder.Reserve(capacity); 
    }

    public override void Add(IDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal)) { _builder.AppendNull(); return; }
        var val = reader.GetValue(ordinal);
        AddObject(val);
    }

    public override void AddObject(object? v)
    {
        if (v == null || v == DBNull.Value)
        {
            _builder.AppendNull();
            return;
        }

        long arrowDurationValue;

        if (v is TimeSpan timeSpan)
        {
            arrowDurationValue = timeSpan.Ticks / 10;
        }
        else
        {
            arrowDurationValue = Convert.ToInt64(v);
        }

        _builder.Append(arrowDurationValue);
    }

    public override IArrowArray Build() 
    { 
        var arr = _builder.Build(); 
        _builder.Clear(); 
        return arr; 
    }
}

internal sealed class ComplexTypeColumnBuilder : ColumnBuilder
{
    private readonly List<object?> _buffer;
    
    private Type? _cachedRuntimeType;
    private Func<List<object?>, IArrowArray>? _fastBuildDelegate;

    public ComplexTypeColumnBuilder(int capacity)
    {
        _buffer = new List<object?>(capacity);
    }

    public override void AddObject(object? v) 
    {
        _buffer.Add(v); 
    }

    public override void Add(IDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            _buffer.Add(null);
        }
        else
        {
            var val = reader.GetValue(ordinal);
            _buffer.Add(val);
        }
    }

    public override IArrowArray Build()
    {           
        if (_buffer.Count == 0) return new NullArray(0);

        var firstItem = _buffer.FirstOrDefault(x => x != null);
        if (firstItem == null) 
        {
            var nullArray = new NullArray(_buffer.Count);
            _buffer.Clear(); 
            return nullArray;
        }

        Type runtimeType = firstItem.GetType();

        if (_fastBuildDelegate == null || _cachedRuntimeType != runtimeType)
        {
            _cachedRuntimeType = runtimeType;
            _fastBuildDelegate = CreateFastBuildDelegate(runtimeType);
        }
        
        try 
        {
            var array = _fastBuildDelegate(_buffer);
            _buffer.Clear();
            return array;
        }
        catch(Exception ex)
        {
            var innerMsg = ex.InnerException?.Message ?? ex.Message;
            throw new InvalidOperationException($"[DbToArrowStream] Failed to build complex array for type '{runtimeType.Name}'. Error: {innerMsg}", ex);
        }
    }

    private static Func<List<object?>, IArrowArray> CreateFastBuildDelegate(Type elementType)
    {
        var castMethod = typeof(Enumerable)
            .GetMethod(nameof(Enumerable.Cast), BindingFlags.Public | BindingFlags.Static)!
            .MakeGenericMethod(elementType);
            
        var buildMethod = typeof(ArrowConverter)
            .GetMethod(nameof(ArrowConverter.Build), BindingFlags.Public | BindingFlags.Static)!
            .MakeGenericMethod(elementType);

        // (List<object?> buffer) => ArrowConverter.Build(Enumerable.Cast<T>(buffer))
        var bufferParam = Expression.Parameter(typeof(List<object?>), "buffer");
        
        var ienumerableCast = Expression.Convert(bufferParam, typeof(System.Collections.IEnumerable));
        
        var castCall = Expression.Call(null, castMethod, ienumerableCast);
        var buildCall = Expression.Call(null, buildMethod, castCall);
        
        var lambda = Expression.Lambda<Func<List<object?>, IArrowArray>>(buildCall, bufferParam);
        return lambda.Compile();
    }
}
// ------------------------------------------------------------------------
// Fallback
// ------------------------------------------------------------------------

internal sealed class FallbackColumnBuilder : ColumnBuilder
{
    private readonly StringViewArray.Builder _builder = new();

    public FallbackColumnBuilder(int capacity) 
    { 
        _builder.Reserve(capacity); 
    }

    public override void Add(IDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            _builder.AppendNull();
        }
        else
        {
            var val = reader.GetValue(ordinal);
            _builder.Append(val?.ToString());
        }
    }

    public override void AddObject(object? v)
    {
        if (v == null) _builder.AppendNull();
        else _builder.Append(v.ToString());
    }

    public override IArrowArray Build() 
    { 
        var arr = _builder.Build(); 
        _builder.Clear(); 
        return arr; 
    }
}
