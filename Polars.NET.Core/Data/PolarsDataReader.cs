using System.Collections;
using System.Data;
using System.Data.Common;

namespace Polars.NET.Core.Data;
/// <summary>
/// Convert Polars DataFrame to DataReader with CancellationToken Control
/// </summary>
public sealed class PolarsDataReader(
    ArrowToDbStream innerReader,
    CancellationTokenSource cts,
    Task producerTask) : DbDataReader
{
    private bool _disposed;
    public override T GetFieldValue<T>(int ordinal) => innerReader.GetFieldValue<T>(ordinal);
    public ReadOnlySpan<byte> GetBytesSpan(int ordinal) => innerReader.GetBytesSpan(ordinal);
    protected override void Dispose(bool disposing)
    {
        if (_disposed) return;
        
        if (disposing)
        {
            cts.Cancel();
            innerReader.Dispose();

            try
            {
                producerTask.Wait(TimeSpan.FromMilliseconds(500));
            }
            catch { /* */ }

            cts.Dispose();
        }

        _disposed = true;
        base.Dispose(disposing);
    }

    // ==========================================================
    // Hot Path
    // ==========================================================
    public override bool HasRows => innerReader.HasRows;
    public override bool IsClosed => innerReader.IsClosed;
    public override int RecordsAffected => innerReader.RecordsAffected;
    public override int Depth => innerReader.Depth;

    public override int FieldCount => innerReader.FieldCount;

    public override void Close() => innerReader.Close();
    public override DataTable GetSchemaTable() => innerReader.GetSchemaTable(); 
    public override bool NextResult() => innerReader.NextResult();
    public override bool Read() => innerReader.Read();

    public override bool GetBoolean(int i) => innerReader.GetBoolean(i);
    public override byte GetByte(int i) => innerReader.GetByte(i);
    public override long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => innerReader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
    public override char GetChar(int i) => innerReader.GetChar(i);
    public override long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => innerReader.GetChars(i, fieldoffset, buffer, bufferoffset, length);
    public override string GetDataTypeName(int i) => innerReader.GetDataTypeName(i);
    public override DateTime GetDateTime(int i) => innerReader.GetDateTime(i);
    public override decimal GetDecimal(int i) => innerReader.GetDecimal(i);
    public override double GetDouble(int i) => innerReader.GetDouble(i);
    public override Type GetFieldType(int i) => innerReader.GetFieldType(i);
    public override float GetFloat(int i) => innerReader.GetFloat(i);
    public override Guid GetGuid(int i) => innerReader.GetGuid(i);
    public override short GetInt16(int i) => innerReader.GetInt16(i);
    public override int GetInt32(int i) => innerReader.GetInt32(i);
    public override long GetInt64(int i) => innerReader.GetInt64(i);
    public override string GetName(int i) => innerReader.GetName(i);
    public override int GetOrdinal(string name) => innerReader.GetOrdinal(name);
    public override string GetString(int i) => innerReader.GetString(i);
    public override object GetValue(int i) => innerReader.GetValue(i);
    public override int GetValues(object[] values) => innerReader.GetValues(values);
    public override bool IsDBNull(int i) => innerReader.IsDBNull(i);
    public override IEnumerator GetEnumerator() => innerReader.GetEnumerator();
    public override object this[int i] => innerReader[i];
    public override object this[string name] => innerReader[name];
}