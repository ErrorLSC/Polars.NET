#pragma warning disable CS8765 
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
using Apache.Arrow;

namespace Polars.NET.Core.Data;

/// <summary>
/// Represents a virtual ADO.NET connection to the Polars SQL engine.
/// </summary>
/// <remarks>
/// This class acts as a bridge between standard .NET data access abstractions and the 
/// Rust-backed Polars <see cref="IPolarsSqlContext"/>. 
/// <para>
/// Unlike traditional connections, <see cref="PolarsDbConnection"/> does not establish 
/// a network socket. Instead, it provides the necessary metadata and command-routing 
/// logic to allow ORM to treat an in-memory Polars context as a relational database.
/// </para>
/// </remarks>
/// <param name="sqlContext">The underlying Polars SQL context where tables are registered and queries are executed.</param>
public class PolarsDbConnection(IPolarsSqlContext sqlContext) : DbConnection
{
    private readonly IPolarsSqlContext _sqlContext = sqlContext;
    private ConnectionState _state = ConnectionState.Closed;

    public override string ConnectionString { get; set; } = "";
    public override string Database => "Polars";
    public override string DataSource => "Memory";
    public override string ServerVersion => "1.0";
    public override ConnectionState State => _state;

    public override void ChangeDatabase(string databaseName) { }
    public override void Close() => _state = ConnectionState.Closed;
    public override void Open() => _state = ConnectionState.Open;
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
    protected override DbCommand CreateDbCommand() => new PolarsDbCommand(_sqlContext) { Connection = this };
}

internal partial class PolarsDbCommand(IPolarsSqlContext sqlContext) : DbCommand
{
    private readonly IPolarsSqlContext _sqlContext = sqlContext;

    public override string CommandText { get; set; } = "";
    public override int CommandTimeout { get; set; }
    public override CommandType CommandType { get; set; }
    public override bool DesignTimeVisible { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }
    protected override DbConnection? DbConnection { get; set; }
    protected override DbTransaction? DbTransaction { get; set; }
    
    protected override DbParameterCollection DbParameterCollection => new PolarsDbParameterCollection();
    protected override DbParameter CreateDbParameter() => new PolarsDbParameter();

    [GeneratedRegex(@"(?:DELETE\s+FROM|UPDATE)\s+""?([a-zA-Z0-9_]+)""?", RegexOptions.IgnoreCase | RegexOptions.Singleline, "en-US")]
    private static partial Regex DmlTableRegex();
    
    public override void Cancel() { }
    public override int ExecuteNonQuery()
    {
        var match = DmlTableRegex().Match(CommandText.Trim());
        string? tableName = match.Success ? match.Groups[1].Value : null;

        long oldHeight = -1;

        if (tableName != null)
        {
            try
            {
                using var oldLf = _sqlContext.Execute($"SELECT * FROM {tableName}");
                using var oldDf = oldLf.Collect(useStreaming: false);
                oldHeight = oldDf.Height;
            }
            catch
            {
                // ignored here
            }
        }

        using var lazyFrame = _sqlContext.Execute(CommandText);
        var newDf = lazyFrame.Collect(useStreaming: false);

        int affectedRows = 0;

        if (tableName != null)
        {
            _sqlContext.Register(tableName, newDf);

            if (oldHeight >= 0)
            {
                affectedRows = (int)(oldHeight - newDf.Height);
                
                if (affectedRows < 0) affectedRows = 0; 
            }
        }
        else
        {
            newDf.Dispose();
        }

        return affectedRows > 0 ? affectedRows : 0; 
    }
    public override object? ExecuteScalar()
    {
        using var reader = ExecuteDbDataReader(CommandBehavior.Default);
        
        if (reader.Read() && reader.FieldCount > 0)
        {
            return reader.GetValue(0);
        }
        
        return null;
    }
    public override void Prepare() {}

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        var stream = ExecuteAndYieldBatches();
        return new ArrowToDbStream(stream);
    }
 
    private IEnumerable<RecordBatch> ExecuteAndYieldBatches()
    {
        using var lazyFrame = _sqlContext.Execute(CommandText);
        using var df = lazyFrame.Collect(useStreaming:false);
        using var batch = df.ToArrow();
        yield return batch;
    }
    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        return await Task.Run(() => 
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ExecuteDbDataReader(behavior); 
        }, cancellationToken);
    }
}

internal class PolarsDbParameter : DbParameter
{
    public override DbType DbType { get; set; }
    public override ParameterDirection Direction { get; set; }
    public override bool IsNullable { get; set; }
    public override string ParameterName { get; set; } = "";
    public override int Size { get; set; }
    public override string SourceColumn { get; set; } = "";
    public override bool SourceColumnNullMapping { get; set; }
    public override object? Value { get; set; }
    public override void ResetDbType() { }
}

internal class PolarsDbParameterCollection : DbParameterCollection
{
    private readonly List<DbParameter> _parameters = [];
    public override int Count => _parameters.Count;
    public override object SyncRoot => this;
    public override int Add(object value) { _parameters.Add((DbParameter)value); return Count - 1; }
    public override void AddRange(System.Array values) { foreach (var v in values) Add(v); }
    public override void Clear() => _parameters.Clear();
    public override bool Contains(object value) => _parameters.Contains(value);
    public override bool Contains(string value) => false;
    public override void CopyTo(System.Array array, int index) => ((ICollection)_parameters).CopyTo(array, index);
    public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();
    protected override DbParameter GetParameter(int index) => _parameters[index];
    protected override DbParameter GetParameter(string parameterName) => null!;
    public override int IndexOf(object value) => _parameters.IndexOf((DbParameter)value);
    public override int IndexOf(string parameterName) => -1;
    public override void Insert(int index, object value) => _parameters.Insert(index, (DbParameter)value);
    public override void Remove(object value) => _parameters.Remove((DbParameter)value);
    public override void RemoveAt(int index) => _parameters.RemoveAt(index);
    public override void RemoveAt(string parameterName) { }
    protected override void SetParameter(int index, DbParameter value) => _parameters[index] = value;
    protected override void SetParameter(string parameterName, DbParameter value) { }

}