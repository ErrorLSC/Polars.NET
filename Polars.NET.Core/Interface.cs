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

public interface IPolarsSchema : IDisposable, IReadOnlyDictionary<string, IPolarsDataType>
{

}

public interface IDataFrameMaterializer
{
    IEnumerable<T> Materialize<T>(DataFrameHandle handle);
    T MaterializeScalar<T>(DataFrameHandle handle);
    /// <summary>
    /// Folds the entire DataFrame into a single scalar value using the zero-allocation row enumerator.
    /// </summary>
    TAccum Aggregate<TSource, TAccum>(
        DataFrameHandle handle,
        TAccum seed,
        Func<TAccum, TSource, TAccum> folder);
    IEnumerable<KeyValuePair<TKey, TAccum>> AggregateBy<TSource, TKey, TAccum>(
        DataFrameHandle handle,
        Func<TSource, TKey> keySelector,
        TAccum seed,
        Func<TAccum, TSource, TAccum> folder,
        IEqualityComparer<TKey>? comparer = null)
        where TKey : notnull;
        
    IEnumerable<KeyValuePair<TKey, TAccum>> AggregateBy<TSource, TKey, TAccum>(
        DataFrameHandle handle,
        Func<TSource, TKey> keySelector,
        Func<TKey, TAccum> seedSelector,
        Func<TAccum, TSource, TAccum> folder,
        IEqualityComparer<TKey>? comparer = null)
        where TKey : notnull;

    /// <summary>
    /// Slices the DataFrame into chunks of the specified size using a zero-allocation stack row enumerator.
    /// </summary>
    /// <typeparam name="T">The row model type.</typeparam>
    /// <param name="handle">The native DataFrame handle.</param>
    /// <param name="chunkSize">The maximum number of rows in each chunk.</param>
    /// <returns>A sequence of chunks represented as arrays of T.</returns>
    IEnumerable<T[]> Chunk<T>(DataFrameHandle handle, int chunkSize);

    bool Any<TSource>(DataFrameHandle handle, Func<TSource, bool>? predicate = null);
    bool All<TSource>(DataFrameHandle handle, Func<TSource, bool> predicate);
}

public static class DataFrameMaterializerRegistry
{
    internal static IDataFrameMaterializer? Default { get; set; }
   
}
