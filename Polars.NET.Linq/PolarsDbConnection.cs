#pragma warning disable CS8765 
using System.Collections;
using System.Data;
using System.Data.Common;
using Apache.Arrow;
using Polars.NET.Core;
using Polars.NET.Core.Data;

namespace Polars.NET.Linq;

public class PolarsDbConnection : DbConnection
{
    private readonly IPolarsSqlContext _sqlContext;
    private ConnectionState _state = ConnectionState.Closed;

    public PolarsDbConnection(IPolarsSqlContext sqlContext) { _sqlContext = sqlContext; }

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

internal partial class PolarsDbCommand : DbCommand
{
    private readonly IPolarsSqlContext _sqlContext;
    public PolarsDbCommand(IPolarsSqlContext sqlContext) { _sqlContext = sqlContext; }

    public override string CommandText { get; set; } = "";
    public override int CommandTimeout { get; set; }
    public override CommandType CommandType { get; set; }
    public override bool DesignTimeVisible { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }
    protected override DbConnection? DbConnection { get; set; }
    protected override DbTransaction? DbTransaction { get; set; }
    
    protected override DbParameterCollection DbParameterCollection => new PolarsDbParameterCollection();
    protected override DbParameter CreateDbParameter() => new PolarsDbParameter();

    public override void Cancel() { }
    public override int ExecuteNonQuery()
    {
        // Console.WriteLine($"[Polars.NET.Linq] Native DML Execution: {CommandText}");
        // 强行把 UPDATE 语句塞给 Polars
        using var lazyFrame = _sqlContext.Execute(CommandText);
        using var df = lazyFrame.Collect(useStreaming:true);
        return (int)df.Height; 
    }
    public override object? ExecuteScalar()
    {
        // 1. 复用已经写好的 DataReader 逻辑
        using var reader = ExecuteDbDataReader(CommandBehavior.Default);
        
        // 2. 如果成功读取到了 Polars 传回来的数据，并且至少有一列
        if (reader.Read() && reader.FieldCount > 0)
        {
            // 3. 返回第一行第一列的标量值
            return reader.GetValue(0);
        }
        
        return null;
    }
    public override void Prepare() { }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        Console.WriteLine($"[Polars.NET.Linq] Native Execution: {CommandText}");
        var stream = ExecuteAndYieldBatches(CommandText);
        return new ArrowToDbStream(stream);
    }

    private IEnumerable<RecordBatch> ExecuteAndYieldBatches(string rawSql)
    {
        var sanitizedSql = MyRegex().Replace(rawSql, "");
        using var lazyFrame = _sqlContext.Execute(sanitizedSql);
        using var df = lazyFrame.Collect(true);
        using var batch = df.ToArrow();
        yield return batch;
    }

    [System.Text.RegularExpressions.GeneratedRegex(@"\s+ESCAPE\s+'.'")]
    private static partial System.Text.RegularExpressions.Regex MyRegex();
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
    private readonly List<DbParameter> _parameters = new();
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