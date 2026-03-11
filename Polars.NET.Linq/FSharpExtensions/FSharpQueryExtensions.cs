using Apache.Arrow.Adbc;
using LinqToDB;
using Polars.FSharp;
using Polars.NET.Core;

namespace Polars.NET.Linq.FSharpExtensions;

/// <summary>
/// Extension method for Polars.FSharp
/// </summary>
public static class FSharpLinqExtensions
{

    /// <summary>
    /// Translates the LINQ query into a Polars LazyFrame. 
    /// This builds the logical execution plan (LINQ -> SQL -> Polars) without eagerly materializing the data.
    /// </summary>
    public static Polars.FSharp.LazyFrame ToLazyFrame<T>(this IQueryable<T> query)
    {
        IPolarsLazyFrame coreInterface = PolarsQueryableExtensions.ToILazyFrame(query);
        return InterfaceUnwrapperExtensions.asLazyFrame(coreInterface);
    }

    /// <summary>
    /// Translates and eagerly executes the LINQ query, materializing the results into a fully computed Polars DataFrame.
    /// </summary>
    public static Polars.FSharp.DataFrame ToDataFrame<T>(this IQueryable<T> query)
        => query.ToLazyFrame().Collect(false);

    /// <summary>
    /// Asynchronously translates the LINQ query into a Polars LazyFrame. 
    /// This builds the logical execution plan without eagerly materializing the data.
    /// </summary>
    public static async Task<Polars.FSharp.LazyFrame> ToLazyFrameAsync<T>(
        this IQueryable<T> query,
        CancellationToken cancellationToken = default)
    {
        IPolarsLazyFrame coreInterface = await PolarsQueryableExtensions
            .ToILazyFrameAsync(query, cancellationToken)
            .ConfigureAwait(false);
            
        return InterfaceUnwrapperExtensions.asLazyFrame(coreInterface);
    }

    /// <summary>
    /// Asynchronously translates and eagerly executes the LINQ query, 
    /// materializing the results into a fully computed Polars DataFrame.
    /// </summary>
    public static async Task<Polars.FSharp.DataFrame> ToDataFrameAsync<T>(
        this IQueryable<T> query, 
        bool useStreaming = false,
        CancellationToken cancellationToken = default)
    {
        IPolarsDataFrame coreInterface = await PolarsQueryableExtensions
            .ToIDataFrameAsync(query, useStreaming, cancellationToken)
            .ConfigureAwait(false);
            
        return InterfaceUnwrapperExtensions.asDataFrame(coreInterface);
    }
        /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="query"></param>
    /// <param name="connection"></param>
    /// <returns></returns>
    public static Polars.FSharp.DataFrame ToDataFrameAdbc<T>(this IQueryable<T> query, AdbcConnection connection)
    {
        // 1. 获取 linq2db 为目标数据库生成的纯正方言 SQL
        var rawSql = query.ToSqlQuery().Sql;
        Type originalType = typeof(T);
        // 2. 核心：只做别名恢复，绝对不碰方言语法！
        // Type elementType = ((IQueryable)query).ElementType;
        var pushdownSql = PolarsSqlTranslator.InjectAliases(rawSql, originalType);
        
        // 3. 将注入了正确别名的 SQL 扔给外部数据库
        var dfInterface = DataFrame.ReadAdbc(connection, pushdownSql);
        
        // 转回 F# 实体
        return InterfaceUnwrapperExtensions.asDataFrame(dfInterface);
    }
}
