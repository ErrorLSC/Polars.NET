using Apache.Arrow.Adbc;
using LinqToDB;
using LinqToDB.Internal.Linq;
using Polars.CSharp;
using Polars.NET.Core;

namespace Polars.NET.Linq.CSharpExtensions;

/// <summary>
/// Extension method for Polars.CSharp
/// </summary>
public static class CSharpLinqExtensions
{

    /// <summary>
    /// Translates the LINQ query into a Polars LazyFrame. 
    /// This builds the logical execution plan (LINQ -> SQL -> Polars) without eagerly materializing the data.
    /// </summary>
    public static Polars.CSharp.LazyFrame ToLazyFrame<T>(this IQueryable<T> query)
    {
        IPolarsLazyFrame coreInterface = PolarsQueryableExtensions.ToILazyFrame(query);
        return coreInterface.AsLazyFrame();
    }

    /// <summary>
    /// Translates and eagerly executes the LINQ query, materializing the results into a fully computed Polars DataFrame.
    /// </summary>
    public static Polars.CSharp.DataFrame ToDataFrame<T>(this IQueryable<T> query)
        => query.ToLazyFrame().Collect();

    /// <summary>
    /// Asynchronously translates the LINQ query into a Polars LazyFrame. 
    /// This builds the logical execution plan without eagerly materializing the data.
    /// </summary>
    public static async Task<Polars.CSharp.LazyFrame> ToLazyFrameAsync<T>(
        this IQueryable<T> query, 
        CancellationToken cancellationToken = default)
    {
        IPolarsLazyFrame coreInterface = await PolarsQueryableExtensions
            .ToILazyFrameAsync(query, cancellationToken)
            .ConfigureAwait(false);
            
        return coreInterface.AsLazyFrame();
    }

    /// <summary>
    /// Asynchronously translates and eagerly executes the LINQ query, 
    /// materializing the results into a fully computed Polars DataFrame.
    /// </summary>
    public static async Task<Polars.CSharp.DataFrame> ToDataFrameAsync<T>(
        this IQueryable<T> query, 
        bool useStreaming = false,
        CancellationToken cancellationToken = default)
    {
        IPolarsDataFrame coreInterface = await PolarsQueryableExtensions
            .ToIDataFrameAsync(query, useStreaming, cancellationToken)
            .ConfigureAwait(false);
            
        return coreInterface.AsDataFrame();
    }
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="query"></param>
    /// <param name="connection"></param>
    /// <returns></returns>
    public static Polars.CSharp.DataFrame ToDataFrameAdbc<T>(this IQueryable<T> query, AdbcConnection connection)
    {
        // 1. 获取 linq2db 为目标数据库生成的纯正方言 SQL
        var rawSql = query.ToSqlQuery().Sql;
        
        // 2. 核心：只做别名恢复，绝对不碰方言语法！
        Type originalType = typeof(T);
        var pushdownSql = PolarsSqlTranslator.InjectAliases(rawSql, originalType);
        
        // 3. 将注入了正确别名的 SQL 扔给外部数据库
        var dfInterface = DataFrame.ReadAdbc(connection, pushdownSql);
        
        // 转回 C# 实体
        return dfInterface.AsDataFrame();
    }
}
