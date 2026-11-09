using System.Diagnostics.CodeAnalysis;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Data;

namespace Polars.NET.Core.Data;

/// <summary>
/// Convert Arrow RecordBatch Stream to DataReader
/// For Polars/Arrow Sink to Database streamingly (With SqlBulkCopy, etc.)
/// Zero-Allocation on Hot Paths.
/// </summary>
/// <remarks>
/// <para>
/// <b>Ownership:</b> the reader takes ownership of every <see cref="RecordBatch"/> it pulls from the stream.
/// Each batch is disposed as soon as the reader advances to the next one, and the current batch is disposed
/// when the stream is exhausted or the reader is closed/disposed. This keeps memory bounded while streaming
/// (batches imported through the Arrow C Data Interface hold native memory).
/// </para>
/// <para>
/// Do not reuse or access a batch after handing it to this reader, and do not share one batch between readers.
/// To keep a batch alive, pass a copy (e.g. one re-imported or rebuilt from the source data) instead.
/// </para>
/// </remarks>
public sealed class ArrowToDbStream : DbDataReader
{
    private readonly IEnumerator<RecordBatch> _batchEnumerator;
    private Schema? _schema;
    private RecordBatch? _currentBatch;
    private int _currentRowIndex;
    private bool _isClosed;
    private readonly Dictionary<string, Type>? _typeOverrides;

    private ColumnAccessor[] _accessors = [];

    /// <summary>
    /// Creates a reader over a stream of record batches. The reader takes ownership of the batches
    /// and disposes them as it reads (see the class remarks).
    /// </summary>
    /// <param name="stream">The batches to read. Enumerated lazily, once.</param>
    /// <param name="typeOverrides">Optional per-column .NET type overrides, keyed by column name.</param>
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
            }

            UpdateAccessorData();
            _currentRowIndex = -1;

            if (_currentBatch.Length == 0)
            {
                return LoadNextBatch();
            }
            return true;
        }
        DisposeAccessors();
        _currentBatch?.Dispose();
        _currentBatch = null;
        return false;
    }

    private void DisposeAccessors()
    {
        for (int i = 0; i < _accessors.Length; i++)
        {
            _accessors[i]?.Dispose();
        }
    }

    private void InitializeAccessors()
    {
        DisposeAccessors();
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
                    Time64Type => typeof(TimeOnly),
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> GetBytesSpan(int ordinal)
    {
        if (IsDBNull(ordinal)) return default;

        ref readonly var accessor = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_accessors), ordinal);

        if (accessor is IBinarySpanAccessor spanAccessor)
        {
            return spanAccessor.GetBytesSpan(_currentRowIndex);
        }

        throw new InvalidCastException($"Column '{GetName(ordinal)}' does not support zero-copy Span access.");
    }

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

        ref readonly var accessor = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_accessors), ordinal);

        if (accessor is ITypedAccessor<T> typed)
        {
            return typed.GetTypedValue(_currentRowIndex);
        }

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
    public override int GetOrdinal(string name)
    {
        EnsureSchema();
        int idx = _schema!.GetFieldIndex(name);
        if (idx < 0)
        {
            idx = _schema.GetFieldIndex(name, StringComparer.OrdinalIgnoreCase);
        }
        if (idx < 0)
        {
            throw new IndexOutOfRangeException($"Column '{name}' does not exist in current Arrow schema.");
        }
        return idx;
    }
    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
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
        DisposeAccessors();
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
            row["ColumnSize"] = field.DataType is FixedSizeBinaryType fsb ? fsb.ByteWidth : -1;
            if (field.DataType is Decimal128Type d128)
            {
                row["NumericPrecision"] = (short)d128.Precision;
                row["NumericScale"] = (short)d128.Scale;
            }
            else if (field.DataType is Decimal256Type d256)
            {
                row["NumericPrecision"] = (short)d256.Precision;
                row["NumericScale"] = (short)d256.Scale;
            }
            row["AllowDBNull"] = field.IsNullable;
            row["IsReadOnly"] = true;
            row["IsLong"] = false;
            row["IsKey"] = false;
            row["BaseColumnName"] = field.Name;

            table.Rows.Add(row);
        }
        return table;
    }
}
