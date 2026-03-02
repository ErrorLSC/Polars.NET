using Apache.Arrow;
using Apache.Arrow.Types;

namespace Polars.NET.Core
{
    public interface IPolarsDataFrame : IDisposable
    {
        long Height{ get; }
        RecordBatch ToArrow();

        IPolarsSchema Schema{get;}
    }

    public interface IPolarsSeries : IDisposable
    {
        IPolarsDataFrame ToFrame();
        void Rename(string newName)
        {
            this.Name = newName;
        }
        IPolarsDataType DataType {get;}
        // IArrowType GetArrowType();
        string Name{get;set;}
    }
    public interface IPolarsDataType: IDisposable
    {
        IArrowType GetArrowType();
    }
    public interface IPolarsLazyFrame : IDisposable
    {
        IPolarsDataFrame Collect(bool useStreaming=false);
        IPolarsSchema Schema{get;}
        string Explain(bool optimized=true);
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
}