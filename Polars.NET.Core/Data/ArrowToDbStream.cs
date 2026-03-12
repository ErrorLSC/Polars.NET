using System.Collections;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Microsoft.FSharp.Core;
using Polars.NET.Core.Arrow;

namespace Polars.NET.Core.Data;
internal interface ITypedAccessor<T>
{
    T GetTypedValue(int index);
}

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
    // IDataReader Interface Implementation
    // ==================================================================================

    public override bool IsDBNull(int ordinal) => _accessors[ordinal].IsNull(_currentRowIndex);

    // =========================================================
    // Hot Path
    // =========================================================
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool GetBoolean(int ordinal) => GetFieldValue<bool>(ordinal);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override byte GetByte(int ordinal) => GetFieldValue<byte>(ordinal);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override short GetInt16(int ordinal) => GetFieldValue<short>(ordinal);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override int GetInt32(int ordinal) => GetFieldValue<int>(ordinal);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override long GetInt64(int ordinal) => GetFieldValue<long>(ordinal);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override float GetFloat(int ordinal) => GetFieldValue<float>(ordinal);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override double GetDouble(int ordinal) => GetFieldValue<double>(ordinal);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override decimal GetDecimal(int ordinal) => GetFieldValue<decimal>(ordinal);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override DateTime GetDateTime(int ordinal) => GetFieldValue<DateTime>(ordinal);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Guid GetGuid(int ordinal) => GetFieldValue<Guid>(ordinal);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override string GetString(int ordinal) => GetFieldValue<string>(ordinal);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override char GetChar(int ordinal) => GetFieldValue<char>(ordinal);

    // =========================================================
    // NotSupported
    // =========================================================
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) 
        => throw new NotSupportedException("Polars.NET uses zero-copy memory. Byte streaming is not supported. Use GetFieldValue<byte[]>() instead.");

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        => throw new NotSupportedException();

    // =========================================================
    // Fallback
    // =========================================================
    public override object GetValue(int ordinal)
    {
        if (IsDBNull(ordinal)) return DBNull.Value;
        return _accessors[ordinal].GetValue(_currentRowIndex)!; 
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
        if (IsDBNull(ordinal)) return default!; 

        ref var accessor = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_accessors), ordinal);

        if (accessor is ITypedAccessor<T> typed)
        {
            return typed.GetTypedValue(_currentRowIndex);
        }

        // 终极兜底
        return (T)accessor.GetValue(_currentRowIndex)!;
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
        public abstract object? GetValue(int index);

    }

    internal abstract class TypedColumnAccessor<T> : ColumnAccessor, ITypedAccessor<T>
    {
        public abstract T GetTypedValue(int index);

        public override object GetValue(int index)
        {
            if (IsNull(index)) return DBNull.Value;
            return GetTypedValue(index)!;
        }
    }

    internal static class ColumnAccessorFactory
    {
        public static ColumnAccessor Create(Field field, Type targetType)
        {
            if (targetType.IsGenericType)
            {
                var genericDef = targetType.GetGenericTypeDefinition();
                bool isOption = genericDef == typeof(FSharpOption<>);
                bool isValueOption = genericDef == typeof(FSharpValueOption<>);

                if (isOption || isValueOption)
                {
                    Type innerType = targetType.GetGenericArguments()[0];
                    var innerAccessor = Create(field, innerType);
                    
                    Type accessorClass = isOption ? typeof(FSharpOptionAccessor<>) : typeof(FSharpValueOptionAccessor<>);
                    Type concreteAccessorType = accessorClass.MakeGenericType(innerType);
                    
                    return (ColumnAccessor)Activator.CreateInstance(concreteAccessorType, innerAccessor, targetType)!;
                }
            }
            return field.DataType switch
            {
                
                ListType or LargeListType => CreateListAccessor(field.DataType, targetType),
                StructType => (ColumnAccessor)Activator.CreateInstance(typeof(StructAccessor<>).MakeGenericType(targetType), targetType, field.DataType)!,
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
                _ => (ColumnAccessor)Activator.CreateInstance(
            typeof(JsonFallbackAccessor<>).MakeGenericType(targetType))!
            };
        }
    }
    private static ColumnAccessor CreateListAccessor(IArrowType arrowType, Type targetType)
    {
        Type elementType;
        if (targetType.IsArray) 
        {
            elementType = targetType.GetElementType()!;
        }
        else if (targetType.IsGenericType && targetType.GetGenericArguments().Length == 1)
        {
            elementType = targetType.GetGenericArguments()[0];
        }
        else 
        {
            IArrowType inner = arrowType is ListType lt ? lt.ValueDataType : ((LargeListType)arrowType).ValueDataType;
            elementType = ArrowTypeResolver.GetNetTypeFromArrowType(inner);
        }

        Type accessorClass = typeof(ListAccessor<>).MakeGenericType(elementType);
        return (ColumnAccessor)Activator.CreateInstance(accessorClass, targetType, arrowType)!;
    }
    // ==================================================================================
    // Concrete Accessors
    // ==================================================================================
    internal sealed class FixedSizeBinaryToGuidAccessor : TypedColumnAccessor<Guid> {
        private FixedSizeBinaryArray? _array;
        
        public override Type TargetType => typeof(Guid);
        
        public override void SetBatch(IArrowArray array) {
            _array = (FixedSizeBinaryArray)array;
        }
        
        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override Guid GetTypedValue(int index) {

            return new Guid(_array!.GetBytes(index));
        }
    }
  // ============================================================
    // 1. FixedSizeBinaryAccessor (byte[])
    // ============================================================
    internal sealed class FixedSizeBinaryAccessor : TypedColumnAccessor<byte[]>,
        ITypedAccessor<byte[]>, 
        ITypedAccessor<Guid>, ITypedAccessor<Guid?>
    {
        private FixedSizeBinaryArray? _array;
        
        public override Type TargetType => typeof(byte[]);
        public override void SetBatch(IArrowArray array) => _array = (FixedSizeBinaryArray)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override byte[] GetTypedValue(int index) => _array!.GetBytes(index).ToArray();

        Guid ITypedAccessor<Guid>.GetTypedValue(int index) => new Guid(_array!.GetBytes(index));
        Guid? ITypedAccessor<Guid?>.GetTypedValue(int index) => new Guid(_array!.GetBytes(index));

        byte[] ITypedAccessor<byte[]>.GetTypedValue(int index) => GetTypedValue(index);
    }

    // ============================================================
    // BinaryToGuidAccessor (Binary -> Guid)
    // ============================================================
    internal sealed class BinaryToGuidAccessor : TypedColumnAccessor<Guid>,
        ITypedAccessor<Guid>, ITypedAccessor<Guid?>
    {
        private IArrowArray? _array;
        
        public override Type TargetType => typeof(Guid);
        public override void SetBatch(IArrowArray array) => _array = array;
        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ReadOnlySpan<byte> GetBytesFast(int index) {
            if (_array is BinaryViewArray bv) return bv.GetBytes(index);
            if (_array is LargeBinaryArray lba) return lba.GetBytes(index);
            if (_array is BinaryArray ba) return ba.GetBytes(index);
            throw new InvalidCastException($"Expected Binary-like array, got {_array?.GetType()}");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override Guid GetTypedValue(int index) {
            ReadOnlySpan<byte> bytes = GetBytesFast(index);
            if (bytes.Length != 16) {
                throw new InvalidDataException($"Cannot convert binary of length {bytes.Length} to Guid.");
            }
            return new Guid(bytes);
        }

        Guid ITypedAccessor<Guid>.GetTypedValue(int index) => GetTypedValue(index);
        Guid? ITypedAccessor<Guid?>.GetTypedValue(int index) => GetTypedValue(index);
    }

    // ============================================================
    // BooleanAccessor (bool)
    // ============================================================
    internal sealed class BooleanAccessor : TypedColumnAccessor<bool>,
        ITypedAccessor<bool>, ITypedAccessor<bool?>
    {
        private BooleanArray? _array;
        public override Type TargetType => typeof(bool);
        public override void SetBatch(IArrowArray array) => _array = (BooleanArray)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool GetTypedValue(int index) => _array!.GetValue(index)!.Value;
        bool ITypedAccessor<bool>.GetTypedValue(int index) => GetTypedValue(index);
        bool? ITypedAccessor<bool?>.GetTypedValue(int index) => GetTypedValue(index);
    }
    // ============================================================
    // Int8Accessor (sbyte)
    // ============================================================
    internal sealed class Int8Accessor : TypedColumnAccessor<sbyte>, 
        ITypedAccessor<byte>, 
        ITypedAccessor<short>, 
        ITypedAccessor<int>, 
        ITypedAccessor<long>,
        ITypedAccessor<sbyte?>,
        ITypedAccessor<byte?>,
        ITypedAccessor<short?>,
        ITypedAccessor<int?>,
        ITypedAccessor<long?>
    {
        private Int8Array? _array;
        public override Type TargetType => typeof(sbyte);
        public override void SetBatch(IArrowArray array) => _array = (Int8Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override sbyte GetTypedValue(int index) => _array!.Values[index];
        byte ITypedAccessor<byte>.GetTypedValue(int index) => (byte)_array!.Values[index];
        short ITypedAccessor<short>.GetTypedValue(int index) => _array!.Values[index];
        int ITypedAccessor<int>.GetTypedValue(int index) => _array!.Values[index];
        long ITypedAccessor<long>.GetTypedValue(int index) => _array!.Values[index];
        sbyte? ITypedAccessor<sbyte?>.GetTypedValue(int index) => _array!.Values[index];
        byte? ITypedAccessor<byte?>.GetTypedValue(int index) => (byte)_array!.Values[index];
        short? ITypedAccessor<short?>.GetTypedValue(int index) => _array!.Values[index];
        int? ITypedAccessor<int?>.GetTypedValue(int index) => _array!.Values[index];
        long? ITypedAccessor<long?>.GetTypedValue(int index) => _array!.Values[index];
    }
    // ============================================================
    // Int16Accessor (short)
    // ============================================================
    internal sealed class Int16Accessor : TypedColumnAccessor<short>,
        ITypedAccessor<int>, ITypedAccessor<long>, ITypedAccessor<float>, ITypedAccessor<double>, ITypedAccessor<decimal>,
        ITypedAccessor<short?>, ITypedAccessor<int?>, ITypedAccessor<long?>, ITypedAccessor<float?>, ITypedAccessor<double?>, ITypedAccessor<decimal?>
    {
        private Int16Array? _array;
        public override Type TargetType => typeof(short);
        public override void SetBatch(IArrowArray array) => _array = (Int16Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);

        public override short GetTypedValue(int index) => _array!.Values[index];

        int ITypedAccessor<int>.GetTypedValue(int index) => _array!.Values[index];
        long ITypedAccessor<long>.GetTypedValue(int index) => _array!.Values[index];
        float ITypedAccessor<float>.GetTypedValue(int index) => _array!.Values[index];
        double ITypedAccessor<double>.GetTypedValue(int index) => _array!.Values[index];
        decimal ITypedAccessor<decimal>.GetTypedValue(int index) => _array!.Values[index];

        short? ITypedAccessor<short?>.GetTypedValue(int index) => _array!.Values[index];
        int? ITypedAccessor<int?>.GetTypedValue(int index) => _array!.Values[index];
        long? ITypedAccessor<long?>.GetTypedValue(int index) => _array!.Values[index];
        float? ITypedAccessor<float?>.GetTypedValue(int index) => _array!.Values[index];
        double? ITypedAccessor<double?>.GetTypedValue(int index) => _array!.Values[index];
        decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => _array!.Values[index];
    }
    // ============================================================
    // Int32Accessor (int)
    // ============================================================

    internal sealed class Int32Accessor :
        TypedColumnAccessor<int>,
        ITypedAccessor<long>,
        ITypedAccessor<double>,
        ITypedAccessor<float>,
        ITypedAccessor<decimal>,
        ITypedAccessor<int?>,
        ITypedAccessor<long?>,
        ITypedAccessor<double?>,
        ITypedAccessor<float?>,
        ITypedAccessor<decimal?>{
        private Int32Array? _array;
        public override Type TargetType => typeof(int);
        public override void SetBatch(IArrowArray array) => _array = (Int32Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override int GetTypedValue(int index) => _array!.Values[index];
        int? ITypedAccessor<int?>.GetTypedValue(int index) => _array!.Values[index];
        long ITypedAccessor<long>.GetTypedValue(int index) => _array!.Values[index];
        long? ITypedAccessor<long?>.GetTypedValue(int index) => _array!.Values[index];
        decimal ITypedAccessor<decimal>.GetTypedValue(int index) => _array!.Values[index];
        float ITypedAccessor<float>.GetTypedValue(int index) => _array!.Values[index];
        double ITypedAccessor<double>.GetTypedValue(int index) => _array!.Values[index];
        decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => _array!.Values[index];
        float? ITypedAccessor<float?>.GetTypedValue(int index) => _array!.Values[index];
        double? ITypedAccessor<double?>.GetTypedValue(int index) => _array!.Values[index];
    }
    // ============================================================
    // Int64Accessor (long)
    // ============================================================

    internal sealed class Int64Accessor : TypedColumnAccessor<long>,
        ITypedAccessor<long>, ITypedAccessor<decimal>, ITypedAccessor<double>,
        ITypedAccessor<long?>, ITypedAccessor<decimal?>, ITypedAccessor<double?>
    {
        private Int64Array? _array;
        public override Type TargetType => typeof(long);
        public override void SetBatch(IArrowArray array) => _array = (Int64Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override long GetTypedValue(int index) => _array!.Values[index];

        long ITypedAccessor<long>.GetTypedValue(int index) => GetTypedValue(index);
        decimal ITypedAccessor<decimal>.GetTypedValue(int index) => GetTypedValue(index);
        double ITypedAccessor<double>.GetTypedValue(int index) => GetTypedValue(index);

        long? ITypedAccessor<long?>.GetTypedValue(int index) => GetTypedValue(index);
        decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => GetTypedValue(index);
        double? ITypedAccessor<double?>.GetTypedValue(int index) => GetTypedValue(index);
    }
    // ============================================================
    // UInt8Accessor (byte)
    // ============================================================
    internal sealed class UInt8Accessor : 
        TypedColumnAccessor<byte>,
        ITypedAccessor<short>,
        ITypedAccessor<int>,
        ITypedAccessor<short?>,
        ITypedAccessor<int?>,
        ITypedAccessor<byte?>
     {
        private UInt8Array? _array;
        public override Type TargetType => typeof(byte);
        public override void SetBatch(IArrowArray array) => _array = (UInt8Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override byte GetTypedValue(int index) => _array!.Values[index];
        short ITypedAccessor<short>.GetTypedValue(int index) => _array!.Values[index];
        int ITypedAccessor<int>.GetTypedValue(int index) => _array!.Values[index];
        int? ITypedAccessor<int?>.GetTypedValue(int index) => _array!.Values[index];
        short? ITypedAccessor<short?>.GetTypedValue(int index) => _array!.Values[index];
        byte? ITypedAccessor<byte?>.GetTypedValue(int index) => _array!.Values[index];
    }

    // ============================================================
    // UInt16Accessor (ushort)
    // ============================================================
    internal sealed class UInt16Accessor : TypedColumnAccessor<ushort>,
        ITypedAccessor<int>, ITypedAccessor<uint>, ITypedAccessor<long>, ITypedAccessor<ulong>,
        ITypedAccessor<ushort?>, ITypedAccessor<int?>, ITypedAccessor<uint?>, ITypedAccessor<long?>, ITypedAccessor<ulong?>
    {
        private UInt16Array? _array;
        public override Type TargetType => typeof(ushort);
        public override void SetBatch(IArrowArray array) => _array = (UInt16Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);

        public override ushort GetTypedValue(int index) => _array!.Values[index];

        int ITypedAccessor<int>.GetTypedValue(int index) => _array!.Values[index];
        uint ITypedAccessor<uint>.GetTypedValue(int index) => _array!.Values[index];
        long ITypedAccessor<long>.GetTypedValue(int index) => _array!.Values[index];
        ulong ITypedAccessor<ulong>.GetTypedValue(int index) => _array!.Values[index];

        ushort? ITypedAccessor<ushort?>.GetTypedValue(int index) => _array!.Values[index];
        int? ITypedAccessor<int?>.GetTypedValue(int index) => _array!.Values[index];
        uint? ITypedAccessor<uint?>.GetTypedValue(int index) => _array!.Values[index];
        long? ITypedAccessor<long?>.GetTypedValue(int index) => _array!.Values[index];
        ulong? ITypedAccessor<ulong?>.GetTypedValue(int index) => _array!.Values[index];
    }

    // ============================================================
    // UInt32Accessor (uint)
    // ============================================================
    internal sealed class UInt32Accessor : TypedColumnAccessor<uint>,
        ITypedAccessor<long>, ITypedAccessor<ulong>,
        ITypedAccessor<uint?>, ITypedAccessor<long?>, ITypedAccessor<ulong?>
    {
        private UInt32Array? _array;
        public override Type TargetType => typeof(uint);
        public override void SetBatch(IArrowArray array) => _array = (UInt32Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);

        public override uint GetTypedValue(int index) => _array!.Values[index];

        long ITypedAccessor<long>.GetTypedValue(int index) => _array!.Values[index];
        ulong ITypedAccessor<ulong>.GetTypedValue(int index) => _array!.Values[index];

        uint? ITypedAccessor<uint?>.GetTypedValue(int index) => _array!.Values[index];
        long? ITypedAccessor<long?>.GetTypedValue(int index) => _array!.Values[index];
        ulong? ITypedAccessor<ulong?>.GetTypedValue(int index) => _array!.Values[index];
    }

    // ============================================================
    // UInt64Accessor (ulong)
    // ============================================================
    internal sealed class UInt64Accessor : TypedColumnAccessor<ulong>,
        ITypedAccessor<decimal>,
        ITypedAccessor<ulong?>, ITypedAccessor<decimal?>
    {
        private UInt64Array? _array;
        public override Type TargetType => typeof(ulong);
        public override void SetBatch(IArrowArray array) => _array = (UInt64Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override ulong GetTypedValue(int index) => _array!.Values[index];
        decimal ITypedAccessor<decimal>.GetTypedValue(int index) => _array!.Values[index];

        ulong? ITypedAccessor<ulong?>.GetTypedValue(int index) => _array!.Values[index];
        decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => _array!.Values[index];
    }
    // ============================================================
    // HalfFloatAccessor (Float16)
    // ============================================================
    internal sealed class HalfFloatAccessor : TypedColumnAccessor<Half>,
        ITypedAccessor<float>, ITypedAccessor<double>, ITypedAccessor<decimal>, ITypedAccessor<string>,
        ITypedAccessor<Half?>, ITypedAccessor<float?>, ITypedAccessor<double?>, ITypedAccessor<decimal?>
    {
        private HalfFloatArray? _array;
        public override Type TargetType => typeof(Half);
        public override void SetBatch(IArrowArray array) => _array = (HalfFloatArray)array;
        public override bool IsNull(int index) => _array!.IsNull(index);

        public override Half GetTypedValue(int index) => _array!.Values[index];

        float ITypedAccessor<float>.GetTypedValue(int index) => (float)_array!.Values[index];
        double ITypedAccessor<double>.GetTypedValue(int index) => (double)_array!.Values[index];
        decimal ITypedAccessor<decimal>.GetTypedValue(int index) => (decimal)(float)_array!.Values[index];
        string ITypedAccessor<string>.GetTypedValue(int index) => _array!.Values[index].ToString();

        Half? ITypedAccessor<Half?>.GetTypedValue(int index) => _array!.Values[index];
        float? ITypedAccessor<float?>.GetTypedValue(int index) => (float)_array!.Values[index];
        double? ITypedAccessor<double?>.GetTypedValue(int index) => (double)_array!.Values[index];
        decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => (decimal)(float)_array!.Values[index];
    }
    // ============================================================
    // FloatAccessor (float / Single)
    // ============================================================
    internal sealed class FloatAccessor : TypedColumnAccessor<float>,
        ITypedAccessor<double>, ITypedAccessor<decimal>,
        ITypedAccessor<float?>, ITypedAccessor<double?>, ITypedAccessor<decimal?>
    {
        private FloatArray? _array;
        public override Type TargetType => typeof(float);
        public override void SetBatch(IArrowArray array) => _array = (FloatArray)array;
        public override bool IsNull(int index) => _array!.IsNull(index);

        public override float GetTypedValue(int index) => _array!.Values[index];

        double ITypedAccessor<double>.GetTypedValue(int index) => _array!.Values[index];
        decimal ITypedAccessor<decimal>.GetTypedValue(int index) => (decimal)_array!.Values[index];

        float? ITypedAccessor<float?>.GetTypedValue(int index) => _array!.Values[index];
        double? ITypedAccessor<double?>.GetTypedValue(int index) => _array!.Values[index];
        decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => (decimal)_array!.Values[index];
    }

    // ============================================================
    // DoubleAccessor (double)
    // ============================================================
    internal sealed class DoubleAccessor : TypedColumnAccessor<double>,
        ITypedAccessor<decimal>,
        ITypedAccessor<double?>, ITypedAccessor<decimal?>
    {
        private DoubleArray? _array;
        public override Type TargetType => typeof(double);
        public override void SetBatch(IArrowArray array) => _array = (DoubleArray)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override double GetTypedValue(int index) => _array!.Values[index];

        decimal ITypedAccessor<decimal>.GetTypedValue(int index) => (decimal)_array!.Values[index];

        double? ITypedAccessor<double?>.GetTypedValue(int index) => _array!.Values[index];
        decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => (decimal)_array!.Values[index];
    }

    // ============================================================
    // DecimalAccessor (decimal)
    // ============================================================
    internal sealed class DecimalAccessor : TypedColumnAccessor<decimal>,
        ITypedAccessor<double>,
        ITypedAccessor<decimal?>, ITypedAccessor<double?>
    {
        private Decimal128Array? _array;
        public override Type TargetType => typeof(decimal);
        public override void SetBatch(IArrowArray array) => _array = (Decimal128Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);
        public override decimal GetTypedValue(int index) => _array!.GetValue(index)!.Value;
        double ITypedAccessor<double>.GetTypedValue(int index) => (double)_array!.GetValue(index)!.Value;
        decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => _array!.GetValue(index);
        double? ITypedAccessor<double?>.GetTypedValue(int index) => (double?)_array!.GetValue(index);
    }

    // Support StringView and fallback to String
    // ============================================================
    // 1. StringViewAccessor (string)
    // ============================================================
    internal sealed class StringViewAccessor : TypedColumnAccessor<string>,
        ITypedAccessor<string>
    {
        private IArrowArray? _array;
        private bool _isView;

        public override Type TargetType => typeof(string);

        public override void SetBatch(IArrowArray array)
        {
            _array = array;
            _isView = array is StringViewArray;
        }

        public override bool IsNull(int index) => _array!.IsNull(index);

        public override string GetTypedValue(int index)
        {
            if (_isView) return ((StringViewArray)_array!).GetString(index);
            if (_array is LargeStringArray lsa) return lsa.GetString(index);
            if (_array is StringArray sa) return sa.GetString(index);
            
            throw new InvalidCastException($"Expected String or StringView, got {_array?.GetType()}");
        }

    }

    // ============================================================
    // 2. BinaryAccessor (byte[])
    // ============================================================
    internal sealed class BinaryAccessor : TypedColumnAccessor<byte[]>,
        ITypedAccessor<byte[]>
    {
        private IArrowArray? _array;

        public override Type TargetType => typeof(byte[]);

        public override void SetBatch(IArrowArray array) => _array = array;

        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ReadOnlySpan<byte> GetBytesFast(int index)
        {
            if (_array is BinaryArray ba) return ba.GetBytes(index);
            if (_array is LargeBinaryArray lba) return lba.GetBytes(index);
            if (_array is BinaryViewArray bv) return bv.GetBytes(index);
            
            throw new InvalidCastException($"Expected BinaryArray or LargeBinaryArray, got {_array?.GetType()}");
        }

        public override byte[] GetTypedValue(int index)
        {
            // 注意：byte[] 是引用类型，这里会产生一次数组拷贝分配。
            // 真正高性能场景建议用户使用 GetFieldValue<ReadOnlySpan<byte>> (如果你的框架支持)
            return GetBytesFast(index).ToArray();
        }

    }

    // --- Magic Time Types ---
    // ============================================================
    // Int64ToDateTimeAccessor (Int64 to DateTime)
    // ============================================================
    internal sealed class Int64ToDateTimeAccessor : TypedColumnAccessor<DateTime>,
        ITypedAccessor<DateTime>, ITypedAccessor<DateTime?>, ITypedAccessor<DateTimeOffset>, ITypedAccessor<DateTimeOffset?>
    {
        private Int64Array? _array;
        public override Type TargetType => typeof(DateTime);
        public override void SetBatch(IArrowArray array) => _array = (Int64Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override DateTime GetTypedValue(int index) 
            => DateTime.UnixEpoch.AddMicroseconds(_array!.Values[index]);

        DateTime ITypedAccessor<DateTime>.GetTypedValue(int index) => GetTypedValue(index);
        DateTimeOffset ITypedAccessor<DateTimeOffset>.GetTypedValue(int index) => new DateTimeOffset(GetTypedValue(index));

        DateTime? ITypedAccessor<DateTime?>.GetTypedValue(int index) => GetTypedValue(index);
        DateTimeOffset? ITypedAccessor<DateTimeOffset?>.GetTypedValue(int index) => new DateTimeOffset(GetTypedValue(index));
    }

    // ============================================================
    // Int64ToTimeSpanAccessor (Int64 to TimeSpan)
    // ============================================================
    internal sealed class Int64ToTimeSpanAccessor : TypedColumnAccessor<TimeSpan>,
        ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>, ITypedAccessor<long>, ITypedAccessor<long?>
    {
        private Int64Array? _array;
        public override Type TargetType => typeof(TimeSpan);
        public override void SetBatch(IArrowArray array) => _array = (Int64Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override TimeSpan GetTypedValue(int index) 
            => new(_array!.Values[index] * 10);

        TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index);
        long ITypedAccessor<long>.GetTypedValue(int index) => _array!.Values[index] * 10;
        TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => GetTypedValue(index);
        long? ITypedAccessor<long?>.GetTypedValue(int index) => _array!.Values[index] * 10;
    }

    // ============================================================
    // TimestampAccessor
    // ============================================================
    internal sealed class TimestampAccessor : TypedColumnAccessor<DateTime>,
        ITypedAccessor<DateTime>, ITypedAccessor<DateTime?>, 
        ITypedAccessor<DateTimeOffset>, ITypedAccessor<DateTimeOffset?>
    {
        private TimestampArray? _array;
        public override Type TargetType => typeof(DateTime);
        public override void SetBatch(IArrowArray array) => _array = (TimestampArray)array;
        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override DateTime GetTypedValue(int index) 
            => _array!.GetTimestamp(index)!.Value.DateTime;

        DateTime ITypedAccessor<DateTime>.GetTypedValue(int index) => GetTypedValue(index);
        
        DateTimeOffset ITypedAccessor<DateTimeOffset>.GetTypedValue(int index) 
            => _array!.GetTimestamp(index)!.Value;

        DateTime? ITypedAccessor<DateTime?>.GetTypedValue(int index) => GetTypedValue(index);
        DateTimeOffset? ITypedAccessor<DateTimeOffset?>.GetTypedValue(int index) => _array!.GetTimestamp(index);
    }
    // ============================================================
    // Date32ToDateOnlyAccessor (int -> DateOnly)
    // ============================================================
    internal sealed class Date32ToDateOnlyAccessor : TypedColumnAccessor<DateOnly>,
        ITypedAccessor<DateOnly>, ITypedAccessor<DateTime>,
        ITypedAccessor<DateOnly?>, ITypedAccessor<DateTime?>
    {
        private Date32Array? _array;
        private const int UnixEpochDayNumber = 719162; 

        public override Type TargetType => typeof(DateOnly);
        public override void SetBatch(IArrowArray array) => _array = (Date32Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);

        // 🌟 本命转换
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override DateOnly GetTypedValue(int index)
            => DateOnly.FromDayNumber(_array!.Values[index] + UnixEpochDayNumber);

        DateOnly ITypedAccessor<DateOnly>.GetTypedValue(int index) => GetTypedValue(index);
        DateTime ITypedAccessor<DateTime>.GetTypedValue(int index) 
            => GetTypedValue(index).ToDateTime(TimeOnly.MinValue);
        DateOnly? ITypedAccessor<DateOnly?>.GetTypedValue(int index) => GetTypedValue(index);
        DateTime? ITypedAccessor<DateTime?>.GetTypedValue(int index) 
            => GetTypedValue(index).ToDateTime(TimeOnly.MinValue);
    }

    // ============================================================
    // Date64Accessor (long -> DateTime)
    // ============================================================
    internal sealed class Date64Accessor : TypedColumnAccessor<DateTime>,
        ITypedAccessor<DateTime>, ITypedAccessor<DateTimeOffset>,
        ITypedAccessor<DateTime?>, ITypedAccessor<DateTimeOffset?>
    {
        private Date64Array? _array;
        public override Type TargetType => typeof(DateTime);
        public override void SetBatch(IArrowArray array) => _array = (Date64Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override DateTime GetTypedValue(int index) 
            => _array!.GetDateTime(index)!.Value;
        DateTime ITypedAccessor<DateTime>.GetTypedValue(int index) => GetTypedValue(index);
        DateTimeOffset ITypedAccessor<DateTimeOffset>.GetTypedValue(int index) => new DateTimeOffset(GetTypedValue(index));

        DateTime? ITypedAccessor<DateTime?>.GetTypedValue(int index) => GetTypedValue(index);
        DateTimeOffset? ITypedAccessor<DateTimeOffset?>.GetTypedValue(int index) => new DateTimeOffset(GetTypedValue(index));
    }

    // ============================================================
    // DurationAccessor
    // ============================================================
    internal sealed class DurationAccessor : TypedColumnAccessor<TimeSpan>,
        ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>, ITypedAccessor<long>, ITypedAccessor<long?>
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
        public override TimeSpan GetTypedValue(int index) 
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

        TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index);
        long ITypedAccessor<long>.GetTypedValue(int index) => GetTypedValue(index).Ticks;

        TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => GetTypedValue(index);
        long? ITypedAccessor<long?>.GetTypedValue(int index) => GetTypedValue(index).Ticks;
    }
    // ============================================================
    // Int64ToTimeOnlyAccessor
    // ============================================================
    internal sealed class Int64ToTimeOnlyAccessor : TypedColumnAccessor<TimeOnly>,
        ITypedAccessor<TimeOnly>, ITypedAccessor<TimeOnly?>, 
        ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>
    {
        private Int64Array? _array;
        public override Type TargetType => typeof(TimeOnly);
        public override void SetBatch(IArrowArray array) => _array = (Int64Array)array;
        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override TimeOnly GetTypedValue(int index) 
        {
            long ticks = _array!.Values[index] * 10;
            return new TimeOnly(ticks);
        }

        TimeOnly ITypedAccessor<TimeOnly>.GetTypedValue(int index) => GetTypedValue(index);
        TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index).ToTimeSpan();
        TimeOnly? ITypedAccessor<TimeOnly?>.GetTypedValue(int index) => GetTypedValue(index);
        TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => GetTypedValue(index).ToTimeSpan();
    }

    // ============================================================
    // Time64Accessor
    // ============================================================
    internal sealed class Time64Accessor : TypedColumnAccessor<TimeOnly>,
        ITypedAccessor<TimeOnly>, ITypedAccessor<TimeOnly?>,
        ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>
    {
        private Time64Array? _array;
        private bool _isNanosecond; 

        public override Type TargetType => typeof(TimeOnly);
        
        public override void SetBatch(IArrowArray array) {
            _array = (Time64Array)array;
            var unit = ((Time64Type)_array.Data.DataType).Unit;
            _isNanosecond = unit == TimeUnit.Nanosecond;
        }

        public override bool IsNull(int index) => _array!.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override TimeOnly GetTypedValue(int index) 
        {
            long val = _array!.Values[index];
            long ticks = _isNanosecond ? (val / 100) : (val * 10);
            return new TimeOnly(ticks);
        }

        TimeOnly ITypedAccessor<TimeOnly>.GetTypedValue(int index) => GetTypedValue(index);
        TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index).ToTimeSpan();

        TimeOnly? ITypedAccessor<TimeOnly?>.GetTypedValue(int index) => GetTypedValue(index);
        TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => GetTypedValue(index).ToTimeSpan();
    }
    // ============================================================
    // ListAccessor<TElement>
    // ============================================================
    internal sealed class ListAccessor<TElement> : TypedColumnAccessor<TElement[]>, 
        ITypedAccessor<TElement[]>, 
        ITypedAccessor<List<TElement>>
    {
        private IArrowArray? _array;
        private readonly Type _targetType;
        private readonly ColumnAccessor _childAccessor; 
        private readonly ITypedAccessor<TElement>? _typedChildAccessor; 

        public ListAccessor(Type targetType, IArrowType arrowType) 
        {
            _targetType = targetType;

            IArrowType innerArrowType = arrowType switch
            {
                ListType lt => lt.ValueDataType,
                LargeListType llt => llt.ValueDataType,
                _ => throw new InvalidOperationException("Not a list type")
            };

            var dummyField = new Field("item", innerArrowType, true);
            _childAccessor = ColumnAccessorFactory.Create(dummyField, typeof(TElement));
            
            _typedChildAccessor = _childAccessor as ITypedAccessor<TElement>;
        }

        public override Type TargetType => _targetType;

        public override void SetBatch(IArrowArray array) 
        {
            _array = array;
            if (_array is ListArray list) _childAccessor.SetBatch(list.Values);
            else if (_array is LargeListArray largeList) _childAccessor.SetBatch(largeList.Values);
        }

        public override bool IsNull(int index) => _array!.IsNull(index);

        public override TElement[] GetTypedValue(int index) 
        {
            IArrowArray valuesArray;
            int offset, length;

            if (_array is ListArray list) 
            {
                valuesArray = list.Values;
                offset = list.ValueOffsets[index];
                length = list.ValueOffsets[index + 1] - offset;
            } 
            else if (_array is LargeListArray largeList) 
            {
                valuesArray = largeList.Values;
                offset = (int)largeList.ValueOffsets[index];
                length = (int)(largeList.ValueOffsets[index + 1] - largeList.ValueOffsets[index]);
            } 
            else throw new InvalidDataException("Invalid List Array.");

            if (length == 0) return [];

            // =====================================================================
            // Fast-Path
            // =====================================================================
            if (typeof(TElement) == typeof(int) && valuesArray is Int32Array i32) 
                return (TElement[])(object)i32.Values.Slice(offset, length).ToArray();
            
            if (typeof(TElement) == typeof(long) && valuesArray is Int64Array i64) 
                return (TElement[])(object)i64.Values.Slice(offset, length).ToArray();
            
            if (typeof(TElement) == typeof(double) && valuesArray is DoubleArray dbl) 
                return (TElement[])(object)dbl.Values.Slice(offset, length).ToArray();
            
            if (typeof(TElement) == typeof(float) && valuesArray is FloatArray flt) 
                return (TElement[])(object)flt.Values.Slice(offset, length).ToArray();

            // =====================================================================
            // String Fast-Path
            // =====================================================================
            if (typeof(TElement) == typeof(string))
            {
                var res = new string[length];
                if (valuesArray is StringViewArray sv) {
                    for (int i = 0; i < length; i++) res[i] = sv.IsNull(offset + i) ? null! : sv.GetString(offset + i);
                    return (TElement[])(object)res;
                }
                if (valuesArray is StringArray sa) {
                    for (int i = 0; i < length; i++) res[i] = sa.IsNull(offset + i) ? null! : sa.GetString(offset + i);
                    return (TElement[])(object)res;
                }
            }

            // =====================================================================
            // 🌟 终极强类型 Fallback：彻底消灭装箱！
            // =====================================================================
            var result = new TElement[length];
            
            if (_typedChildAccessor != null)
            {
                for (int i = 0; i < length; i++) 
                {
                    result[i] = _childAccessor.IsNull(offset + i) 
                        ? default! 
                        : _typedChildAccessor.GetTypedValue(offset + i);
                }
            }
            else
            {
                for (int i = 0; i < length; i++) 
                {
                    result[i] = _childAccessor.IsNull(offset + i) 
                        ? default! 
                        : (TElement)_childAccessor.GetValue(offset + i)!;
                }
            }

            return result;
        }

        List<TElement> ITypedAccessor<List<TElement>>.GetTypedValue(int index) => GetTypedValue(index).ToList();
    }

    // ============================================================
    // StructAccessor<T>：Object mapper
    // ============================================================
    internal sealed class StructAccessor<T> : TypedColumnAccessor<T>, ITypedAccessor<T>
    {
        private StructArray? _array;
        private readonly Type _targetType;
        private readonly ColumnAccessor[] _childAccessors;
        private readonly int[] _arrowFieldIndexes;

        private readonly Func<int, T> _rowInstantiator;

        public StructAccessor(Type targetType, IArrowType arrowType)
        {
            _targetType = targetType;

            if (arrowType is not StructType structType)
                throw new InvalidOperationException($"Expected StructType, got {arrowType.Name}");

            var childAccessorsList = new List<ColumnAccessor>();
            var arrowFieldIndexesList = new List<int>();

            var members = ArrowTypeResolver.GetReadableMembers(targetType);
            var bindings = new List<MemberAssignment>();

            var indexParam = Expression.Parameter(typeof(int), "index");

            for (int i = 0; i < structType.Fields.Count; i++)
            {
                var arrowField = structType.Fields[i];
                var member = members.FirstOrDefault(m => string.Equals(m.Name, arrowField.Name, StringComparison.OrdinalIgnoreCase));

                if (member != null)
                {
                    Type memberType = ArrowTypeResolver.GetMemberType(member);
                    var childAccessor = ColumnAccessorFactory.Create(arrowField, memberType);

                    int accessorIndex = childAccessorsList.Count;
                    childAccessorsList.Add(childAccessor);
                    arrowFieldIndexesList.Add(i);

                    // ==========================================================
                    // Generate as _childAccessors[0] as ITypedAccessor<TMember>
                    // ==========================================================
                    var accessorInstanceExpr = Expression.Constant(childAccessor);

                    Expression getValueExpr;

                    Type typedInterface = typeof(ITypedAccessor<>).MakeGenericType(memberType);
                    if (typedInterface.IsAssignableFrom(childAccessor.GetType()))
                    {
                        // ((ITypedAccessor<TMember>)accessor).GetTypedValue(index)
                        var typedAccessor = Expression.Convert(accessorInstanceExpr, typedInterface);
                        var getMethod = typedInterface.GetMethod("GetTypedValue")!;
                        getValueExpr = Expression.Call(typedAccessor, getMethod, indexParam);
                    }
                    else
                    {
                        // (TMember)accessor.GetValue(index)
                        var getMethod = typeof(ColumnAccessor).GetMethod("GetValue")!;
                        var callGet = Expression.Call(accessorInstanceExpr, getMethod, indexParam);
                        getValueExpr = Expression.Convert(callGet, memberType);
                    }

                    bindings.Add(Expression.Bind(member, getValueExpr));
                }
            }

            _childAccessors = [.. childAccessorsList];
            _arrowFieldIndexes = [.. arrowFieldIndexesList];

            // ==========================================================
            // Generate：new TTarget { Prop1 = ..., Prop2 = ... }
            // ==========================================================
            var newExpr = Expression.New(targetType);
            var initExpr = Expression.MemberInit(newExpr, bindings);

            _rowInstantiator = Expression.Lambda<Func<int, T>>(initExpr, indexParam).Compile();
        }

        public override Type TargetType => _targetType;

        public override void SetBatch(IArrowArray array)
        {
            _array = (StructArray)array;
            for (int i = 0; i < _childAccessors.Length; i++)
            {
                _childAccessors[i].SetBatch(_array.Fields[_arrowFieldIndexes[i]]);
            }
        }

        public override bool IsNull(int index) => _array!.IsNull(index);

        // ==========================================================
        // Hot Path
        // ==========================================================
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override T GetTypedValue(int index)
            => _rowInstantiator(index);

        T ITypedAccessor<T>.GetTypedValue(int index) => GetTypedValue(index);
    }
    // ========================================================
    // 1. FSharpOption<T> (Ref Type Class)
    // ========================================================
    internal sealed class FSharpOptionAccessor<T>(ColumnAccessor innerAccessor, Type targetType) : TypedColumnAccessor<FSharpOption<T>>, ITypedAccessor<FSharpOption<T>>
    {

        private readonly ITypedAccessor<T>? _typedInnerAccessor = innerAccessor as ITypedAccessor<T>;

        public override Type TargetType => targetType;
        public override void SetBatch(IArrowArray array) => innerAccessor.SetBatch(array);
        public override bool IsNull(int index) => innerAccessor.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override FSharpOption<T> GetTypedValue(int index)
        {
            // F# Option None is null
            if (innerAccessor.IsNull(index)) return null!; 

            if (_typedInnerAccessor != null)
            {
                T rawValue = _typedInnerAccessor.GetTypedValue(index);
                return FSharpOption<T>.Some(rawValue);
            }

            var boxedValue = innerAccessor.GetValue(index);
            return FSharpOption<T>.Some((T)boxedValue!);
        }
        
        FSharpOption<T> ITypedAccessor<FSharpOption<T>>.GetTypedValue(int index) => GetTypedValue(index);
    }

    // ========================================================
    // FSharpValueOption<T> (Struct)
    // ========================================================
    internal sealed class FSharpValueOptionAccessor<T>(ColumnAccessor innerAccessor, Type targetType) : TypedColumnAccessor<FSharpValueOption<T>>, ITypedAccessor<FSharpValueOption<T>>
    {
        private readonly ITypedAccessor<T>? _typedInnerAccessor = innerAccessor as ITypedAccessor<T>;

        public override Type TargetType => targetType;
        public override void SetBatch(IArrowArray array) => innerAccessor.SetBatch(array);
        public override bool IsNull(int index) => innerAccessor.IsNull(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override FSharpValueOption<T> GetTypedValue(int index)
        {
            if (innerAccessor.IsNull(index)) return FSharpValueOption<T>.ValueNone;

            if (_typedInnerAccessor != null)
            {
                T rawValue = _typedInnerAccessor.GetTypedValue(index);
                return FSharpValueOption<T>.NewValueSome(rawValue);
            }

            var boxedValue = innerAccessor.GetValue(index);
            if (boxedValue == DBNull.Value || boxedValue == null) return FSharpValueOption<T>.ValueNone;

            return FSharpValueOption<T>.NewValueSome((T)boxedValue);
        }

        FSharpValueOption<T> ITypedAccessor<FSharpValueOption<T>>.GetTypedValue(int index) => GetTypedValue(index);
    }
    // --- Fallback ---

    // ============================================================
    // JsonFallbackAccessor<T>
    // ============================================================
    internal sealed class JsonFallbackAccessor<T> : TypedColumnAccessor<T>, ITypedAccessor<T>
    {
        private IArrowArray? _array;

        public override Type TargetType => typeof(T);
        public override void SetBatch(IArrowArray array) => _array = array;
        public override bool IsNull(int index) => _array!.IsNull(index);

        public override T GetTypedValue(int index)
        {
            var rawValue = ExtractValue(_array!, index);

            if (typeof(T) == typeof(string))
            {
                var json = rawValue is string s ? s : JsonSerializer.Serialize(rawValue);
                return (T)(object)json;
            }

            if (rawValue is T typedValue)
            {
                return typedValue;
            }

            if (rawValue != null)
            {
                var json = JsonSerializer.Serialize(rawValue);
                return (T)(object)json;
            }

            return default!;
        }

        T ITypedAccessor<T>.GetTypedValue(int index) => GetTypedValue(index);

        private static object? ExtractValue(IArrowArray array, int index)
        {
            return array switch
            {
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
