using System.Collections.Concurrent;
using System.Data;
using Apache.Arrow;

namespace Polars.NET.Core.Data;
public sealed class DataReaderLifecycleWrapper : IDataReader
{
    private readonly ArrowToDbStream _innerReader;
    private readonly BlockingCollection<RecordBatch> _buffer;
    private readonly CancellationTokenSource _cts;
    private readonly Task _producerTask;
    private bool _disposed;

    public DataReaderLifecycleWrapper(
        ArrowToDbStream innerReader, 
        BlockingCollection<RecordBatch> buffer, 
        CancellationTokenSource cts, 
        Task producerTask)
    {
        _innerReader = innerReader;
        _buffer = buffer;
        _cts = cts;
        _producerTask = producerTask;
    }

    // ==========================================================
    // 💀 核心绞杀逻辑：极其优雅的 Dispose
    // ==========================================================
    public void Dispose()
    {
        if (_disposed) return;

        // 1. 触发令牌：瞬间解锁后台线程的 buffer.Add() 阻塞
        _cts.Cancel();

        // 2. 释放内部的 ArrowToDbStream 
        // 这一步会调用你的 Close()，进而 Dispose 掉 _currentBatch 和 IEnumerator
        _innerReader.Dispose();

        // 3. 优雅等待后台线程退出 (给 500ms 宽限期，防止僵尸线程)
        try
        {
            _producerTask.Wait(TimeSpan.FromMilliseconds(500));
        }
        catch { /* Task 可能会抛出 AggregateException，直接忽略 */ }

        // 4. 清理通信管道
        _buffer.Dispose();
        _cts.Dispose();

        _disposed = true;
    }

    // ==========================================================
    // 🚀 极速转发层 (Hot Path Relay)
    // 直接调用 _innerReader 的方法，享受你的 Zero-Allocation 优化
    // ==========================================================
    
    // IDataReader 专属
    public void Close() => _innerReader.Close();
    public DataTable GetSchemaTable() => _innerReader.GetSchemaTable(); // 完美复用你写的 GetSchemaTable
    public bool NextResult() => _innerReader.NextResult();
    public bool Read() => _innerReader.Read();
    public int Depth => _innerReader.Depth;
    public bool IsClosed => _innerReader.IsClosed;
    public int RecordsAffected => _innerReader.RecordsAffected;

    // IDataRecord 专属
    public int FieldCount => _innerReader.FieldCount;
    public bool GetBoolean(int i) => _innerReader.GetBoolean(i);
    public byte GetByte(int i) => _innerReader.GetByte(i);
    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => _innerReader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
    public char GetChar(int i) => _innerReader.GetChar(i);
    public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => _innerReader.GetChars(i, fieldoffset, buffer, bufferoffset, length);
    public IDataReader GetData(int i) => throw new NotSupportedException();
    public string GetDataTypeName(int i) => _innerReader.GetDataTypeName(i);
    public DateTime GetDateTime(int i) => _innerReader.GetDateTime(i);
    public decimal GetDecimal(int i) => _innerReader.GetDecimal(i);
    public double GetDouble(int i) => _innerReader.GetDouble(i);
    public Type GetFieldType(int i) => _innerReader.GetFieldType(i);
    public float GetFloat(int i) => _innerReader.GetFloat(i);
    public Guid GetGuid(int i) => _innerReader.GetGuid(i);
    public short GetInt16(int i) => _innerReader.GetInt16(i);
    public int GetInt32(int i) => _innerReader.GetInt32(i);
    public long GetInt64(int i) => _innerReader.GetInt64(i);
    public string GetName(int i) => _innerReader.GetName(i);
    public int GetOrdinal(string name) => _innerReader.GetOrdinal(name);
    public string GetString(int i) => _innerReader.GetString(i);
    public object GetValue(int i) => _innerReader.GetValue(i);
    public int GetValues(object[] values) => _innerReader.GetValues(values);
    public bool IsDBNull(int i) => _innerReader.IsDBNull(i);
    public object this[int i] => _innerReader[i];
    public object this[string name] => _innerReader[name];
}