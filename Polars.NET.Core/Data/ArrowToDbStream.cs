using System.Collections;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Polars.NET.Core.Arrow;

namespace Polars.NET.Core.Data;
/// <summary>
/// Convert Arrow RecordBatch Stream to IDataReader。
/// For Polars/Arrow Sink to Database streamingly (With SqlBulkCopy, etc.)
/// Zero-Allocation on Hot Paths.
/// </summary>
public sealed class ArrowToDbStream : DbDataReader
{
    private readonly IEnumerator<RecordBatch> _batchEnumerator;
    private Schema? _schema; 
    private RecordBatch? _currentBatch;
    private int _currentRowIndex;
    private bool _isClosed;
    private readonly Dictionary<string, Type>? _typeOverrides;

    private ColumnAccessor[] _accessors = [];

    public ArrowToDbStream(IEnumerable<RecordBatch> stream, Dictionary<string, Type>? typeOverrides = null)
    {
        ArgumentNullException.ThrowIfNull(stream);
        _batchEnumerator = stream.GetEnumerator();
        _currentRowIndex = -1;
        _typeOverrides = typeOverrides ?? new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);
    }

    // ==================================================================================
    // Lifecycle
    // ==================================================================================

    private bool EnsureSchema()
    {
        if (_schema != null) return true;
        if (LoadNextBatch()) return true;
        _isClosed = true;
        return false;
    }

    private bool LoadNextBatch()
    {
        if (_batchEnumerator.MoveNext())
        {
            _currentBatch?.Dispose();
            _currentBatch = _batchEnumerator.Current;

            if (_schema == null || _schema != _currentBatch.Schema)
            {
                _schema = _currentBatch.Schema;
                InitializeAccessors();
                

                for(int i=0; i<_accessors.Length; i++)
                {
                    _ = _schema.GetFieldByIndex(i);
                }
            }

            UpdateAccessorData();
            _currentRowIndex = -1;
            
            if (_currentBatch.Length == 0) 
            {
                return LoadNextBatch();
            }
            return true;
        }
        return false;
    }

    private void InitializeAccessors()
    {
        int count = _schema!.FieldsList.Count;
        if (_accessors.Length != count)
        {
            _accessors = new ColumnAccessor[count];
        }

        for (int i = 0; i < count; i++)
        {
            var field = _schema.GetFieldByIndex(i);
            Type targetType;

            if (_typeOverrides != null && _typeOverrides.TryGetValue(field.Name, out var overrideType))
            {
                targetType = overrideType;
            }
            else
            {
                var arrowType = field.DataType;
                targetType = arrowType switch
                {
                    Time64Type => typeof(TimeSpan), 
                    
                    Date32Type => typeof(DateOnly),
                    
                    TimestampType => typeof(DateTime),
                    _ => ArrowTypeResolver.GetNetTypeFromArrowType(arrowType)
                };
            }

            _accessors[i] = ColumnAccessorFactory.Create(field, targetType);
        }
    }

    private void UpdateAccessorData()
    {
        for (int i = 0; i < _accessors.Length; i++)
        {
            _accessors[i].SetBatch(_currentBatch!.Column(i));
        }
    }

    public override bool Read()
    {
        if (_isClosed) 
        {
            return false;
        }

        if (_currentBatch == null)
        {
            if (!LoadNextBatch())
            {
                _isClosed = true;
                return false;
            }
        }

        _currentRowIndex++;

        if (_currentBatch != null && _currentRowIndex < _currentBatch.Length)
        {
            return true;
        }

        if (LoadNextBatch())
        {
            _currentRowIndex++; 
            return true;
        }

        _currentBatch = null!;
        _isClosed = true;
        return false;
    }

    // ==================================================================================
    // IDataReader Interface Implementation (Hot Path)
    // ==================================================================================

    public override bool IsDBNull(int ordinal) => _accessors[ordinal].IsNull(_currentRowIndex);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool GetBoolean(int ordinal)
    {
        ref var accessor = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_accessors), ordinal);
        return accessor.GetBoolean(_currentRowIndex);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override byte GetByte(int ordinal)
    {
        ref var accessor = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_accessors), ordinal);
        return accessor.GetByte(_currentRowIndex);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]    
    public override short GetInt16(int ordinal)
    {
        ref var accessor = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_accessors), ordinal);
        return accessor.GetInt16(_currentRowIndex);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override int GetInt32(int ordinal) 
    {
        ref var accessor = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_accessors), ordinal);
        return accessor.GetInt32(_currentRowIndex);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override long GetInt64(int ordinal)
    {
        ref var accessor = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_accessors), ordinal);
        return accessor.GetInt64(_currentRowIndex);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override float GetFloat(int ordinal)
    {
        ref var accessor = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_accessors), ordinal);
        return accessor.GetFloat(_currentRowIndex);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override double GetDouble(int ordinal)
    {    
        ref var accessor = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_accessors), ordinal);
        return accessor.GetDouble(_currentRowIndex);
    }
    public override decimal GetDecimal(int ordinal) => _accessors[ordinal].GetDecimal(_currentRowIndex);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override string GetString(int ordinal) 
    {
        ref var accessor = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_accessors), ordinal);
        return accessor.GetString(_currentRowIndex);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override DateTime GetDateTime(int ordinal)
    {
        ref var accessor = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_accessors), ordinal);
        return accessor.GetDateTime(_currentRowIndex);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override char GetChar(int ordinal)
    {
        ref var accessor = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_accessors), ordinal);
        return accessor.GetChar(_currentRowIndex);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Guid GetGuid(int ordinal)
    {
        ref var accessor = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_accessors), ordinal);
        return accessor.GetGuid(_currentRowIndex);
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        => _accessors[ordinal].GetBytes(_currentRowIndex, dataOffset, buffer, bufferOffset, length);

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        => _accessors[ordinal].GetChars(_currentRowIndex, dataOffset, buffer, bufferOffset, length);

    public override object GetValue(int ordinal)
    {
        if (IsDBNull(ordinal)) 
        {
            return DBNull.Value;
        }

        return _accessors[ordinal].GetValue(_currentRowIndex);
    }

    public override int GetValues(object[] values)
    {
        int count = Math.Min(values.Length, FieldCount);
        for (int i = 0; i < count; i++) values[i] = GetValue(i);
        return count;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override T GetFieldValue<T>(int ordinal)
    {
        if (IsDBNull(ordinal))
        {
            return default!; 
        }

        ref var accessor = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_accessors), ordinal);

        if (typeof(T) == typeof(DateOnly))
        {
            if (accessor is Date32ToDateOnlyAccessor date32Accessor)
            {
                var val = date32Accessor.GetDateOnlyFast(_currentRowIndex);
                return Unsafe.As<DateOnly, T>(ref val);
            }
        }
        else if (typeof(T) == typeof(TimeOnly))
        {
            if (accessor is Time64Accessor t64Accessor)
            {
                var val = t64Accessor.GetTimeOnlyFast(_currentRowIndex);
                return Unsafe.As<TimeOnly, T>(ref val);
            }
            
            if (accessor is Int64ToTimeOnlyAccessor i64ToTimeOnly)
            {
                var val = i64ToTimeOnly.GetTimeOnlyFast(_currentRowIndex);
                return Unsafe.As<TimeOnly, T>(ref val);
            }
        }
        else if (typeof(T) == typeof(TimeSpan))
        {
            if (accessor is DurationAccessor durAccessor)
            {
                var val = durAccessor.GetTimeSpanFast(_currentRowIndex);
                return Unsafe.As<TimeSpan, T>(ref val);
            }
            
            if (accessor is Int64ToTimeSpanAccessor i64ToTimeSpan)
            {
                var val = i64ToTimeSpan.GetTimeSpanFast(_currentRowIndex);
                return Unsafe.As<TimeSpan, T>(ref val);
            }
        }
        else if (typeof(T) == typeof(Guid))
        {
            if (accessor is FixedSizeBinaryToGuidAccessor fixedAccessor)
            {
                var val = fixedAccessor.GetGuidFast(_currentRowIndex);
                return Unsafe.As<Guid, T>(ref val);
            }
            if (accessor is BinaryToGuidAccessor binaryGuidAccessor)
            {
                var val = binaryGuidAccessor.GetGuidFast(_currentRowIndex);
                return Unsafe.As<Guid, T>(ref val);
            }
        }

        return (T)accessor.GetValue(_currentRowIndex);
    }

    // --- Schema / Metadata ---
    public override int FieldCount 
    {
        get 
        {
            EnsureSchema();
            return _schema?.FieldsList.Count ?? 0;
        }
    }
    public override string GetName(int ordinal) { EnsureSchema(); return _schema!.GetFieldByIndex(ordinal).Name; }
    public override int GetOrdinal(string name) { EnsureSchema(); return _schema!.GetFieldIndex(name); }
    public override Type GetFieldType(int ordinal)
    {
        EnsureSchema();
        var type = _accessors[ordinal].TargetType;
        
        return type;
    }
    
    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));
    public override bool HasRows => EnsureSchema();
    public override bool NextResult() => false;
    public override int Depth => 0;
    public override int RecordsAffected => -1;
    public override bool IsClosed => _isClosed;
    public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;

    public override void Close()
    {
        _isClosed = true;
        _batchEnumerator.Dispose();
        _currentBatch?.Dispose();
        _currentBatch = null;
    }
    protected override void Dispose(bool disposing) { if (disposing) Close(); base.Dispose(disposing); }
    public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

    public override DataTable GetSchemaTable()
    {
        EnsureSchema();
        if (_schema == null) return null!;

        var table = new DataTable("SchemaTable");
        table.Columns.Add("ColumnName", typeof(string));
        table.Columns.Add("ColumnOrdinal", typeof(int));
        table.Columns.Add("ColumnSize", typeof(int));
        table.Columns.Add("NumericPrecision", typeof(short));
        table.Columns.Add("NumericScale", typeof(short));
        table.Columns.Add("DataType", typeof(Type));
        table.Columns.Add("ProviderType", typeof(Type));
        table.Columns.Add("IsLong", typeof(bool));
        table.Columns.Add("AllowDBNull", typeof(bool));
        table.Columns.Add("IsReadOnly", typeof(bool));
        table.Columns.Add("IsRowVersion", typeof(bool));
        table.Columns.Add("IsUnique", typeof(bool));
        table.Columns.Add("IsKey", typeof(bool));
        table.Columns.Add("IsAutoIncrement", typeof(bool));
        table.Columns.Add("BaseSchemaName", typeof(string));
        table.Columns.Add("BaseCatalogName", typeof(string));
        table.Columns.Add("BaseTableName", typeof(string));
        table.Columns.Add("BaseColumnName", typeof(string));

        for (int i = 0; i < _schema.FieldsList.Count; i++)
        {
            var field = _schema.GetFieldByIndex(i);
            var row = table.NewRow();
            row["ColumnName"] = field.Name;
            row["ColumnOrdinal"] = i;
            row["DataType"] = GetFieldType(i); 
            row["ColumnSize"] = -1;
            row["AllowDBNull"] = field.IsNullable;
            row["IsReadOnly"] = true;
            row["IsLong"] = false;
            row["IsKey"] = false;
            row["BaseColumnName"] = field.Name;

            table.Rows.Add(row);
        }
        return table;
    }

    // ==================================================================================
    // Column Accessor System (Optimized)
    // ==================================================================================

    internal abstract class ColumnAccessor
    {
        public abstract Type TargetType { get; }
        public abstract void SetBatch(IArrowArray array);
        public abstract bool IsNull(int index);
        public abstract object GetValue(int index);

        public virtual bool GetBoolean(int index) => throw new InvalidCastException();
        public virtual byte GetByte(int index) => throw new InvalidCastException();
        public virtual char GetChar(int index) => throw new InvalidCastException();
        public virtual short GetInt16(int index) => throw new InvalidCastException();
        public virtual int GetInt32(int index) => throw new InvalidCastException();
        public virtual long GetInt64(int index) => throw new InvalidCastException();
        public virtual float GetFloat(int index) => throw new InvalidCastException();
        public virtual double GetDouble(int index) => throw new InvalidCastException();
        public virtual decimal GetDecimal(int index) => throw new InvalidCastException();
        public virtual DateTime GetDateTime(int index) => throw new InvalidCastException();
        public virtual Guid GetGuid(int index) => throw new InvalidCastException();
        public virtual string GetString(int index) => throw new InvalidCastException();
        public virtual long GetBytes(int index, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0;
        public virtual long GetChars(int index, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0;
    }

    internal static class ColumnAccessorFactory
    {
        public static ColumnAccessor Create(Field field, Type targetType)
    {
            return field.DataType switch
        {
            // 1. Primitives
            BooleanType => new BooleanAccessor(),
            Int8Type => new Int8Accessor(),
            Int16Type => new Int16Accessor(),
            Int32Type => new Int32Accessor(),
            UInt8Type => new UInt8Accessor(),
            UInt16Type => new UInt16Accessor(),
            UInt32Type => new UInt32Accessor(),
            UInt64Type => new UInt64Accessor(),
            
            // 2. Int64 & Magic Types
            Int64Type => targetType switch
            {
                { } t when t == typeof(DateTime) => new Int64ToDateTimeAccessor(),
                { } t when t == typeof(TimeSpan) => new Int64ToTimeSpanAccessor(),
                { } t when t == typeof(TimeOnly) => new Int64ToTimeOnlyAccessor(),
                _ => new Int64Accessor()
            },

            HalfFloatType => new HalfFloatAccessor(),
            FloatType => new FloatAccessor(),
            DoubleType => new DoubleAccessor(),

            // 3. String & Binary
            StringViewType or StringType or LargeStringType => new StringViewAccessor(),
            BinaryViewType or BinaryType or LargeBinaryType => targetType switch
            {
                { } t when t == typeof(Guid) => new BinaryToGuidAccessor(),
                _ => new BinaryAccessor()
            },
            

            // 4. Time
            TimestampType => new TimestampAccessor(),
            Time64Type => new Time64Accessor(),
            DurationType => new DurationAccessor(),
            
            Date32Type => new Date32ToDateOnlyAccessor(),
            Date64Type => new Date64Accessor(),

            // 5. Decimal 
            Decimal128Type => new DecimalAccessor(), 
            Decimal256Type => new DecimalAccessor(),
            FixedSizeBinaryType => targetType switch
            {
                { } t when t == typeof(Guid) => new FixedSizeBinaryToGuidAccessor(),
                _ => new FixedSizeBinaryAccessor()
            },

            // 6. Fallback
            _ => new JsonFallbackAccessor(targetType)
        };
    }
    }

    // ==================================================================================
    // Concrete Accessors
    // ==================================================================================
    internal sealed class FixedSizeBinaryToGuidAccessor : ColumnAccessor {
        private FixedSizeBinaryArray? _array;
        
        public override Type TargetType => typeof(Guid);
        
        public override void SetBatch(IArrowArray array) {
            _array = (FixedSizeBinaryArray)array;
        }
        
        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Guid GetGuidFast(int index) {

            return new Guid(_array!.GetBytes(index));
        }

        public override object GetValue(int index) {
            if (IsNull(index)) return DBNull.Value;

            return GetGuidFast(index); 
        }
    }
    internal sealed class FixedSizeBinaryAccessor : ColumnAccessor {
        private FixedSizeBinaryArray? _array;
        
        public override Type TargetType => typeof(byte[]);
        
        public override void SetBatch(IArrowArray array) {
            _array = (FixedSizeBinaryArray)array;
        }
        
        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ReadOnlySpan<byte> GetBytesFast(int index) {
            return _array!.GetBytes(index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Guid GetGuidFast(int index) {
            return new Guid(_array!.GetBytes(index));
        }

        public override object GetValue(int index) {
            if (IsNull(index)) return DBNull.Value;

            return GetBytesFast(index).ToArray(); 
        }

        public override long GetBytes(int index, long dataOffset, byte[]? buffer, int bufferOffset, int length) {
            ReadOnlySpan<byte> bytes = GetBytesFast(index);
            if (buffer == null) return bytes.Length;
            
            int count = Math.Min(bytes.Length - (int)dataOffset, length);
            if (count > 0) {
                bytes.Slice((int)dataOffset, count).CopyTo(buffer.AsSpan(bufferOffset));
            }
            return count;
        }
    }
    internal sealed class BinaryToGuidAccessor : ColumnAccessor {
        private IArrowArray? _array;
        
        public override Type TargetType => typeof(Guid);
        
        public override void SetBatch(IArrowArray array) {
            _array = array;
        }
        
        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ReadOnlySpan<byte> GetBytesFast(int index) {
            if (_array is BinaryViewArray bv) return bv.GetBytes(index);
            if (_array is LargeBinaryArray lba) return lba.GetBytes(index);
            if (_array is BinaryArray ba) return ba.GetBytes(index);
            
            throw new InvalidCastException($"Expected Binary-like array, got {_array?.GetType()}");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Guid GetGuidFast(int index) {
            ReadOnlySpan<byte> bytes = GetBytesFast(index);
            if (bytes.Length != 16) {
                throw new InvalidDataException($"Cannot convert binary of length {bytes.Length} to Guid.");
            }
            return new Guid(bytes);
        }

        public override object GetValue(int index) {
            if (IsNull(index)) return DBNull.Value;

            return GetGuidFast(index); 
        }
    }
    internal sealed class BooleanAccessor : ColumnAccessor {
        private BooleanArray? _array;
        public override Type TargetType => typeof(bool);
        public override void SetBatch(IArrowArray array) => _array = (BooleanArray)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override bool GetBoolean(int index) => _array!.GetValue(index)!.Value;
        public override object GetValue(int index) => GetBoolean(index);
    }

    internal sealed class Int8Accessor : ColumnAccessor {
        private Int8Array? _array;
        public override Type TargetType => typeof(sbyte);
        public override void SetBatch(IArrowArray array) => _array = (Int8Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override byte GetByte(int index) => (byte)_array!.Values[index];
        public override short GetInt16(int index) => _array!.Values[index];
        public override int GetInt32(int index) => _array!.Values[index];
        public override object GetValue(int index) => _array!.Values[index];
    }

    internal sealed class Int16Accessor : ColumnAccessor {
        private Int16Array? _array;
        public override Type TargetType => typeof(short);
        public override void SetBatch(IArrowArray array) => _array = (Int16Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override short GetInt16(int index) => _array!.Values[index];
        public override int GetInt32(int index) => _array!.Values[index];
        public override object GetValue(int index) => GetInt16(index);
    }

    internal sealed class Int32Accessor : ColumnAccessor {
        private Int32Array? _array;
        public override Type TargetType => typeof(int);
        public override void SetBatch(IArrowArray array) => _array = (Int32Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override int GetInt32(int index) => _array!.Values[index];
        public override long GetInt64(int index) => _array!.Values[index];
        public override double GetDouble(int index) => _array!.Values[index];
        public override decimal GetDecimal(int index) => _array!.Values[index];
        public override object GetValue(int index) => GetInt32(index);
    }

    internal sealed class Int64Accessor : ColumnAccessor {
        private Int64Array? _array;
        public override Type TargetType => typeof(long);
        public override void SetBatch(IArrowArray array) => _array = (Int64Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override long GetInt64(int index) => _array!.Values[index];
        public override decimal GetDecimal(int index) => _array!.Values[index];
        public override object GetValue(int index) => GetInt64(index);
    }
    internal sealed class UInt8Accessor : ColumnAccessor {
        private UInt8Array? _array;
        public override Type TargetType => typeof(byte);
        public override void SetBatch(IArrowArray array) => _array = (UInt8Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override byte GetByte(int index) => _array!.Values[index];
        public override short GetInt16(int index) => _array!.Values[index];
        public override int GetInt32(int index) => _array!.Values[index];
        public override object GetValue(int index) => GetByte(index);
    }

    internal sealed class UInt16Accessor : ColumnAccessor {
        private UInt16Array? _array;
        public override Type TargetType => typeof(ushort);
        public override void SetBatch(IArrowArray array) => _array = (UInt16Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override int GetInt32(int index) => _array!.Values[index];
        public override long GetInt64(int index) => _array!.Values[index];
        public override object GetValue(int index) => _array!.Values[index]; // returns ushort
    }

    internal sealed class UInt32Accessor : ColumnAccessor {
        private UInt32Array? _array;
        public override Type TargetType => typeof(uint);
        public override void SetBatch(IArrowArray array) => _array = (UInt32Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override long GetInt64(int index) => _array!.Values[index];
        public override object GetValue(int index) => _array!.Values[index]; // returns uint
    }

    internal sealed class UInt64Accessor : ColumnAccessor {
        private UInt64Array? _array;
        public override Type TargetType => typeof(ulong);
        public override void SetBatch(IArrowArray array) => _array = (UInt64Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override decimal GetDecimal(int index) => _array!.Values[index];
        public override object GetValue(int index) => _array!.Values[index]; // returns ulong
    }
    internal sealed class HalfFloatAccessor : ColumnAccessor
    {
        private HalfFloatArray? _array;

        public override Type TargetType => typeof(Half);

        public override void SetBatch(IArrowArray array) => _array = (HalfFloatArray)array;

        public override bool IsNull(int index) => _array!.IsNull(index);

        public override float GetFloat(int index) => (float)_array!.Values[index];

        public override double GetDouble(int index) => (double)_array!.Values[index];

        public override decimal GetDecimal(int index) => (decimal)(float)_array!.Values[index];

        public override object GetValue(int index) => _array!.Values[index];

        public override string GetString(int index) => _array!.Values[index].ToString();
    }
    internal sealed class FloatAccessor : ColumnAccessor {
        private FloatArray? _array;
        public override Type TargetType => typeof(float);
        public override void SetBatch(IArrowArray array) => _array = (FloatArray)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override float GetFloat(int index) => _array!.Values[index];
        public override double GetDouble(int index) => _array!.Values[index];
        public override object GetValue(int index) => GetFloat(index);
    }

    internal sealed class DoubleAccessor : ColumnAccessor {
        private DoubleArray? _array;
        public override Type TargetType => typeof(double);
        public override void SetBatch(IArrowArray array) => _array = (DoubleArray)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override double GetDouble(int index) => _array!.Values[index];
        public override object GetValue(int index) => GetDouble(index);
    }

    internal sealed class DecimalAccessor : ColumnAccessor {
        private Decimal128Array? _array;
        public override Type TargetType => typeof(decimal);
        public override void SetBatch(IArrowArray array) => _array = (Decimal128Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override decimal GetDecimal(int index) => _array!.GetValue(index) ?? 0m; // Fix: GetValue returns decimal?
        public override double GetDouble(int index) => (double)GetDecimal(index);
        public override object GetValue(int index) => GetDecimal(index);
    }

    // Support StringView and fallback to String
    internal sealed class StringViewAccessor : ColumnAccessor {
        private IArrowArray? _array;
        private bool _isView;
        public override Type TargetType => typeof(string);
        
        public override void SetBatch(IArrowArray array) {
            _array = array;
            _isView = array is StringViewArray;
        }
        
        public override bool IsNull(int index) => _array!.IsNull(index);

        public override string GetString(int index) {
            if (_isView) return ((StringViewArray)_array!).GetString(index);
            if (_array is LargeStringArray lsa) return lsa.GetString(index);
            if (_array is StringArray sa) return sa.GetString(index);
            throw new InvalidCastException($"Expected String or StringView, got {_array?.GetType()}");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ReadOnlySpan<byte> GetBytesFast(int index) {
            if (_isView) return ((StringViewArray)_array!).GetBytes(index);
            if (_array is LargeStringArray lsa) return lsa.GetBytes(index);
            if (_array is StringArray sa) return sa.GetBytes(index);
            return default;
        }

        public override object GetValue(int index) => GetString(index);

        public override long GetChars(int index, long dataOffset, char[]? buffer, int bufferOffset, int length) {
            // Get original UTF8 bytes
            ReadOnlySpan<byte> utf8Bytes = GetBytesFast(index);

            // If only query length
            if (buffer == null) {
                return System.Text.Encoding.UTF8.GetCharCount(utf8Bytes);
            }

            int totalChars = System.Text.Encoding.UTF8.GetCharCount(utf8Bytes);
            int count = Math.Min(totalChars - (int)dataOffset, length);
            if (count <= 0) return 0;

            if (dataOffset == 0) {
                System.Text.Encoding.UTF8.GetChars(utf8Bytes, buffer.AsSpan(bufferOffset, count));
                return count;
            }

            string val = GetString(index);
            val.CopyTo((int)dataOffset, buffer, bufferOffset, count);
            return count;
        }
    }

    internal sealed class BinaryAccessor : ColumnAccessor {
        private IArrowArray? _array; 
        
        public override Type TargetType => typeof(byte[]);
        
        public override void SetBatch(IArrowArray array) {
            _array = array;
        }
        
        public override bool IsNull(int index) => _array!.IsNull(index);


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ReadOnlySpan<byte> GetBytesFast(int index) {
            if (_array is BinaryArray ba) return ba.GetBytes(index);
            if (_array is LargeBinaryArray lba) return lba.GetBytes(index);
            if (_array is BinaryViewArray bv) return bv.GetBytes(index);
            
            throw new InvalidCastException($"Expected BinaryArray or LargeBinaryArray, got {_array?.GetType()}");
        }

        public override object GetValue(int index) {
            if (IsNull(index)) return DBNull.Value;
            return GetBytesFast(index).ToArray(); 
        }

        public override long GetBytes(int index, long dataOffset, byte[]? buffer, int bufferOffset, int length) {
            ReadOnlySpan<byte> bytes = GetBytesFast(index);
            
            if (buffer == null) return bytes.Length;
            
            int count = Math.Min(bytes.Length - (int)dataOffset, length);
            
            if (count > 0) {
                bytes.Slice((int)dataOffset, count).CopyTo(buffer.AsSpan(bufferOffset));
            }
            
            return count;
        }
    }

    // --- Magic Time Types ---
    internal sealed class Int64ToDateTimeAccessor : ColumnAccessor {
        private Int64Array? _array;
        public override Type TargetType => typeof(DateTime);
        public override void SetBatch(IArrowArray array) => _array = (Int64Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override DateTime GetDateTime(int index)
        {
            long val = _array!.Values[index];

            return DateTime.UnixEpoch.AddMicroseconds(val);
        }
        public override object GetValue(int index) => GetDateTime(index);
    }
    internal sealed class Int64ToTimeSpanAccessor : ColumnAccessor {
        private Int64Array? _array;
        public override Type TargetType => typeof(TimeSpan);
        public override void SetBatch(IArrowArray array) => _array = (Int64Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal TimeSpan GetTimeSpanFast(int index) 
        {
            return new TimeSpan(_array!.Values[index] * 10);
        }

        public override object GetValue(int index) 
        {
            if (IsNull(index)) return DBNull.Value;
            return GetTimeSpanFast(index);
        }
    }

    // --- Standard Date/Time ---
    internal sealed class TimestampAccessor : ColumnAccessor {
        private TimestampArray? _array;
        public override Type TargetType => typeof(DateTime);
        public override void SetBatch(IArrowArray array) => _array = (TimestampArray)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override DateTime GetDateTime(int index)
        {
            var dto = _array!.GetTimestamp(index);

            return dto.HasValue ? dto.Value.DateTime : default; 
        }
        public override object GetValue(int index) => GetDateTime(index);
    }

    // Date32 -> DateOnly
    internal sealed class Date32ToDateOnlyAccessor : ColumnAccessor 
    {
        private Date32Array? _array;
        
        private const int UnixEpochDayNumber = 719162; 

        public override Type TargetType => typeof(DateOnly);
        
        public override void SetBatch(IArrowArray array) => _array = (Date32Array)array;
        
        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal DateOnly GetDateOnlyFast(int index)
        {
            return DateOnly.FromDayNumber(_array!.Values[index] + UnixEpochDayNumber);
        }

        public override object GetValue(int index) 
        {
            if (IsNull(index)) return DBNull.Value;
            
            return GetDateOnlyFast(index);
        }

        public override DateTime GetDateTime(int index) 
        {
            return GetDateOnlyFast(index).ToDateTime(TimeOnly.MinValue);
        }
    }

    internal sealed class Date64Accessor : ColumnAccessor {
        private Date64Array? _array;
        public override Type TargetType => typeof(DateTime);
        public override void SetBatch(IArrowArray array) => _array = (Date64Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override DateTime GetDateTime(int index) => _array!.GetDateTime(index)!.Value;
        public override object GetValue(int index) => GetDateTime(index);
    }
    internal sealed class DurationAccessor : ColumnAccessor 
    {
        private DurationArray? _array;
        private TimeUnit _unit;

        public override Type TargetType => typeof(TimeSpan);
        
        public override void SetBatch(IArrowArray array) 
        {
            _array = (DurationArray)array;
            _unit = ((DurationType)_array.Data.DataType).Unit;
        }

        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal TimeSpan GetTimeSpanFast(int index) 
        {
            long val = _array!.Values[index];
            
            long ticks = _unit switch {
                TimeUnit.Nanosecond => val / 100,
                TimeUnit.Microsecond => val * 10,
                TimeUnit.Millisecond => val * 10_000,
                TimeUnit.Second => val * 10_000_000,
                _ => val
            };
            
            return new TimeSpan(ticks);
        }

        public override object GetValue(int index) 
        {
            if (IsNull(index)) return DBNull.Value;
            return GetTimeSpanFast(index);
        }
    }
    internal sealed class Int64ToTimeOnlyAccessor : ColumnAccessor {
        private Int64Array? _array;
        public override Type TargetType => typeof(TimeOnly);
        public override void SetBatch(IArrowArray array) => _array = (Int64Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal TimeOnly GetTimeOnlyFast(int index) 
        {
            long ticks = _array!.Values[index] * 10;
            return new TimeOnly(ticks);
        }

        public override object GetValue(int index) 
        {
            if (IsNull(index)) return DBNull.Value;
            return GetTimeOnlyFast(index);
        }
    }
    internal sealed class Time64Accessor : ColumnAccessor {
        private Time64Array? _array;
        private long _divisor; 

        public override Type TargetType => typeof(TimeOnly);
        
        public override void SetBatch(IArrowArray array) {
            _array = (Time64Array)array;
            var unit = ((Time64Type)_array.Data.DataType).Unit;
            
            _divisor = unit == TimeUnit.Nanosecond ? 100 : 0; 
        }

        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal TimeOnly GetTimeOnlyFast(int index) 
        {
            long val = _array!.Values[index];
            
            long ticks = _divisor == 100 ? (val / 100) : (val * 10);
            
            return new TimeOnly(ticks);
        }

        public override object GetValue(int index) {
            if (IsNull(index)) return DBNull.Value;
            
            return GetTimeOnlyFast(index); 
        }
    }
    
    // --- Fallback ---
    internal sealed class JsonFallbackAccessor : ColumnAccessor {
        private IArrowArray? _array;
        private readonly Type _targetType;
        public JsonFallbackAccessor(Type targetType) { _targetType = targetType; }
        public override Type TargetType => _targetType;
        public override void SetBatch(IArrowArray array) => _array = array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override string GetString(int index) {
            var value = ExtractValue(_array!, index);
            return JsonSerializer.Serialize(value);
        }
        public override object GetValue(int index) {
            var val = ExtractValue(_array!, index);
            if (val is not string && val != null) return JsonSerializer.Serialize(val);
            return val ?? DBNull.Value;
        }
        private static object? ExtractValue(IArrowArray array, int index) {
            return array switch {
                Int32Array i32 => i32.Values[index],
                Int64Array i64 => i64.Values[index],
                DoubleArray dbl => dbl.Values[index],
                FloatArray flt => flt.Values[index],
                BooleanArray b => b.Values[index],
                StringArray s => s.GetString(index),
                StringViewArray sv => sv.GetString(index), 
                Date32Array d32 => d32.GetDateTime(index),
                Date64Array d64 => d64.GetDateTime(index),
                Time64Array t64 => t64.GetDateTime(index),
                TimestampArray ts => ts.GetTimestamp(index)?.DateTime,
                _ => null 
            };
        }
    }
}
