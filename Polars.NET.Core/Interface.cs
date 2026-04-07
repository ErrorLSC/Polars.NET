using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Polars.NET.Core;

public interface IPolarsDataFrame : IDisposable
{
    long Height{ get; }
    RecordBatch ToArrow();
    void Show();
    IPolarsSchema Schema{get;}
    UpdateResult WriteToAdbc(AdbcStatement statement);
    IPolarsSeries Column(int index);
    IArrowArrayStream ToArrowStream(ReadOnlySpan<int> columnIndices = default,ulong? seed = null);

}

public interface IPolarsSeries : IDisposable
{
    IPolarsDataFrame ToFrame();
    void Rename(string newName)
    {
        this.Name = newName;
    }
    IPolarsDataType DataType {get;}
    string Name{get;set;}
}
public interface IPolarsDataType: IDisposable
{
    IArrowType GetArrowType();
}
public interface IPolarsLazyFrame : IDisposable
{
    IPolarsDataFrame Collect(PlEngine engine=PlEngine.Auto,bool useStreaming=false);
    IPolarsSchema Schema{get;}
    string Explain(bool optimized=true);
    Task<IPolarsDataFrame> CollectAsync(PlEngine engine=PlEngine.Auto,bool useStreaming = false, CancellationToken cancellationToken = default);
}

public interface IPolarsSqlContext : IDisposable
{
    void Register(string tableName, IPolarsDataFrame df);
    void Register(string tableName, IPolarsLazyFrame lf);
    
    IPolarsLazyFrame Execute(string sql);
}

public interface IPolarsSchema : IDisposable
{
    int Length { get; }
    List<string> ColumnNames { get; }
    
    IPolarsDataType this[string name] { get; } 
    Dictionary<string,IPolarsDataType> ToDictionary();
}
