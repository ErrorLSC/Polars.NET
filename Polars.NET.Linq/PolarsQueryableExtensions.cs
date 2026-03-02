using Polars.NET.Core;
using LinqToDB.Internal.Linq; // 必须引入，为了使用 IExpressionQuery

namespace Polars.NET.Linq
{
    public static class PolarsQueryableExtensions
    {
        public static IPolarsLazyFrame ToLazyFrame<T>(this IQueryable<T> query)
        {
            // 1. 扒掉外衣，露出你发现的 IExpressionQuery 接口
            if (query is IExpressionQuery exprQuery && 
                exprQuery.DataContext is PolarsDataContext polarsDb)
            {
                // 2. 调用你亲自从源码里挖出来的宝藏方法！
                // 传 null 使用默认的生成选项，此时 linq2db 会在内存中把 LINQ 翻译成 SQL，但绝不执行！
                var sqlQueries = exprQuery.GetSqlQueries(null);

                if (sqlQueries == null || sqlQueries.Count == 0)
                    throw new InvalidOperationException("[Polars.NET] linq2db SQL generation failed");

                // 3. 提取纯净的 SQL 文本！
                // 注意：在某些版本的 linq2db 中，QuerySql 的属性名可能叫 CommandText
                var rawSql = sqlQueries[0].Sql; // 或者是 sqlQueries[0].CommandText;

                // 4. 完美截胡！交给 Polars 引擎生成底层逻辑计划
                var sanitizedSql = SqlSanitizer.Clean(rawSql); // 如果你加了那个正则清理类
                return polarsDb.ExecuteToLazyFrame(sanitizedSql);
            }

            throw new InvalidOperationException("无法提取 PolarsDataContext。");
        }

        /// <summary>
        /// 快捷方法：LINQ -> SQL -> LazyFrame -> DataFrame
        /// </summary>
        public static IPolarsDataFrame ToDataFrame<T>(this IQueryable<T> query, bool useStreaming = false)
        {
            // 依赖上面的 ToLazyFrame，最后补一脚 Collect
            var lf = query.ToLazyFrame();
            return lf.Collect(useStreaming);
        }
        /// <summary>
        /// 打印该 LINQ 查询在 Polars 底层的逻辑执行计划
        /// </summary>
        public static string Explain<T>(this IQueryable<T> query, bool optimized = true)
        {
            var lf = query.ToLazyFrame();

            return lf.Explain(optimized);
        }
    }
}