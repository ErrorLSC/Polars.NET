using Polars.CSharp;

namespace Polars.NET.Linq;

public static class PolarsLinqExtensions
{
    // =========================================================================
    // 1. SqlContext 扩展 (推荐用法，支持多表 Join)
    // =========================================================================

    /// <summary>
    /// 为已在 SqlContext 中注册的表生成 IQueryable 接口
    /// </summary>
    public static IQueryable<T> GetTable<T>(this SqlContext context, string tableName)
    {
        var provider = new PolarsQueryProvider(context, tableName);
        return new PolarsQueryable<T>(provider, tableName);
    }

    /// <summary>
    /// 将 DataFrame 注册到上下文中，并直接返回可查询的 IQueryable
    /// </summary>
    public static IQueryable<T> RegisterTable<T>(this SqlContext context, string tableName, DataFrame df)
    {
        context.Register(tableName, df);
        return context.GetTable<T>(tableName);
    }

    /// <summary>
    /// 将 LazyFrame 注册到上下文中，并直接返回可查询的 IQueryable
    /// </summary>
    public static IQueryable<T> RegisterTable<T>(this SqlContext context, string tableName, LazyFrame lf)
    {
        context.Register(tableName, lf);
        return context.GetTable<T>(tableName);
    }

    // 增加一个重载，利用 source 数据集合来推断泛型 T
    public static IQueryable<T> RegisterTable<T>(
        this SqlContext context, 
        string tableName, 
        DataFrame df, 
        IEnumerable<T> _sourceTypeInferencer) // 仅用于让编译器推断 T
    {
        context.Register(tableName, df);
        return context.GetTable<T>(tableName);
    }

    // =========================================================================
    // 2. 单表快捷扩展 (注意：生成孤立的 SqlContext，无法与其他表 Join)
    // =========================================================================

    /// <summary>
    /// Convert DataFrame to IQueryable (Isolated Context)
    /// </summary>
    public static IQueryable<T> AsPolarsQueryable<T>(this DataFrame df, string tableName = "df")
    {
        // 注意：因为这里隐式持有了 ctx，如果你的 SqlContext 底层需要显式 Dispose，
        // 在这种快捷方法中可能会引发内存泄漏。建议对于原生资源保持警惕。
        var ctx = new SqlContext();
        ctx.Register(tableName, df);

        var provider = new PolarsQueryProvider(ctx, tableName);
        return new PolarsQueryable<T>(provider, tableName);
    }
    
    /// <summary>
    /// Convert LazyFrame to IQueryable (Isolated Context)
    /// </summary>
    public static IQueryable<T> AsPolarsQueryable<T>(this LazyFrame lf, string tableName = "lf")
    {
        var ctx = new SqlContext();
        ctx.Register(tableName, lf);

        var provider = new PolarsQueryProvider(ctx, tableName);
        return new PolarsQueryable<T>(provider, tableName);
    }
}