using System.Collections;
using System.Data;
using System.Data.Common;
using System.Text.Json;
using Apache.Arrow;
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

    private ColumnAccessor[] _accessors = System.Array.Empty<ColumnAccessor>();

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
                    var field = _schema.GetFieldByIndex(i);
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

    public override bool GetBoolean(int ordinal) => _accessors[ordinal].GetBoolean(_currentRowIndex);
    public override byte GetByte(int ordinal) => _accessors[ordinal].GetByte(_currentRowIndex);
    public override short GetInt16(int ordinal) => _accessors[ordinal].GetInt16(_currentRowIndex);
    public override int GetInt32(int ordinal) => _accessors[ordinal].GetInt32(_currentRowIndex);
    public override long GetInt64(int ordinal) => _accessors[ordinal].GetInt64(_currentRowIndex);
    public override float GetFloat(int ordinal) => _accessors[ordinal].GetFloat(_currentRowIndex);
    public override double GetDouble(int ordinal) => _accessors[ordinal].GetDouble(_currentRowIndex);
    public override decimal GetDecimal(int ordinal) => _accessors[ordinal].GetDecimal(_currentRowIndex);
    public override string GetString(int ordinal) => _accessors[ordinal].GetString(_currentRowIndex);
    public override DateTime GetDateTime(int ordinal) => _accessors[ordinal].GetDateTime(_currentRowIndex);
    public override char GetChar(int ordinal) => _accessors[ordinal].GetChar(_currentRowIndex);
    public override Guid GetGuid(int ordinal) => _accessors[ordinal].GetGuid(_currentRowIndex);

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
        public static ColumnAccessor Create(Apache.Arrow.Field field, Type targetType)
        {
            var typeId = field.DataType.TypeId;

            // 1. Primitives
            if (typeId == ArrowTypeId.Boolean) return new BooleanAccessor();
            if (typeId == ArrowTypeId.Int8) return new Int8Accessor();
            if (typeId == ArrowTypeId.Int16) return new Int16Accessor();
            if (typeId == ArrowTypeId.Int32) return new Int32Accessor();
            if (typeId == ArrowTypeId.UInt8) return new UInt8Accessor();
            if (typeId == ArrowTypeId.UInt16) return new UInt16Accessor();
            if (typeId == ArrowTypeId.UInt32) return new UInt32Accessor();
            if (typeId == ArrowTypeId.UInt64) return new UInt64Accessor();
            // 2. Int64 & Magic Types
            if (typeId == ArrowTypeId.Int64)
            {
                if (targetType == typeof(DateTime)) return new Int64ToDateTimeAccessor();
                if (targetType == typeof(TimeSpan)) return new Int64ToTimeSpanAccessor();
                if (targetType == typeof(TimeOnly)) return new Int64ToTimeOnlyAccessor();
                return new Int64Accessor();
            }

            if (typeId == ArrowTypeId.HalfFloat) return new HalfFloatAccessor();
            if (typeId == ArrowTypeId.Float) return new FloatAccessor();
            if (typeId == ArrowTypeId.Double) return new DoubleAccessor();
            
            // 3. String & StringView
            if (typeId == ArrowTypeId.StringView) return new StringViewAccessor();
            if (typeId == ArrowTypeId.String || typeId == ArrowTypeId.LargeString) return new StringViewAccessor();

            if (typeId == ArrowTypeId.Binary || typeId == ArrowTypeId.LargeBinary) return new BinaryAccessor();

            // 4. Time
            if (typeId == ArrowTypeId.Timestamp) return new TimestampAccessor();
            if (typeId == ArrowTypeId.Time64) return new Time64Accessor();
            
            // Date32 -> DateOnly
            if (typeId == ArrowTypeId.Date32) return new Date32ToDateOnlyAccessor();
            if (typeId == ArrowTypeId.Date64) return new Date64Accessor();

            // 5. Decimal
            if (typeId == ArrowTypeId.Decimal128 || typeId == ArrowTypeId.Decimal256) return new DecimalAccessor();

            // 6. Fallback
            return new JsonFallbackAccessor(targetType);
        }
    }

    // ==================================================================================
    // Concrete Accessors
    // ==================================================================================

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
            if (_array is StringArray sa) return sa.GetString(index);
            throw new InvalidCastException($"Expected String or StringView, got {_array?.GetType()}");
        }
        public override object GetValue(int index) => GetString(index);
        public override long GetChars(int index, long dataOffset, char[]? buffer, int bufferOffset, int length) {
            string val = GetString(index);
            if (buffer == null) return val.Length;
            int count = Math.Min(val.Length - (int)dataOffset, length);
            if (count > 0) val.CopyTo((int)dataOffset, buffer, bufferOffset, count);
            return count;
        }
    }

    internal sealed class BinaryAccessor : ColumnAccessor {
        private BinaryArray? _array;
        public override Type TargetType => typeof(byte[]);
        public override void SetBatch(IArrowArray array) => _array = (BinaryArray)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override object GetValue(int index) => _array!.GetBytes(index).ToArray();
        public override long GetBytes(int index, long dataOffset, byte[]? buffer, int bufferOffset, int length) {
            var bytes = _array!.GetBytes(index);
            if (buffer == null) return bytes.Length;
            int count = Math.Min(bytes.Length - (int)dataOffset, length);
            if (count > 0) bytes.Slice((int)dataOffset, count).CopyTo(buffer.AsSpan(bufferOffset));
            return count;
        }
    }

    // --- Magic Time Types ---
    internal sealed class Int64ToDateTimeAccessor : ColumnAccessor {
        private Int64Array? _array;
        public override Type TargetType => typeof(DateTime);
        public override void SetBatch(IArrowArray array) => _array = (Int64Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        // public override DateTime GetDateTime(int index) => DateTime.UnixEpoch.AddTicks(_array!.Values[index] * 10);
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
        public override object GetValue(int index) => new TimeSpan(_array!.Values[index] * 10);
    }
    internal sealed class Int64ToTimeOnlyAccessor : ColumnAccessor {
        private Int64Array? _array;
        public override Type TargetType => typeof(TimeOnly);
        public override void SetBatch(IArrowArray array) => _array = (Int64Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override object GetValue(int index) {
            return new TimeOnly(_array!.Values[index] / 100); 
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
    internal sealed class Date32ToDateOnlyAccessor : ColumnAccessor {
        private Date32Array? _array;
        public override Type TargetType => typeof(DateOnly);
        public override void SetBatch(IArrowArray array) => _array = (Date32Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override object GetValue(int index) {
            if (IsNull(index)) return DBNull.Value;
            // Values[index] returns int (days), not int?
            int daysSinceUnixEpoch = _array!.Values[index];
            return DateOnly.FromDateTime(DateTime.UnixEpoch.AddDays(daysSinceUnixEpoch));
        }
        public override DateTime GetDateTime(int index) {
            return DateOnly.FromDayNumber(_array!.Values[index]).ToDateTime(TimeOnly.MinValue);
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

        public override object GetValue(int index) {
            long val = _array!.Values[index];
            
            if (_divisor == 100) return new TimeSpan(val / 100);
            
            return new TimeSpan(val * 10);
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
