using Polars.NET.Core;
using LinqToDB.Internal.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Collections.Concurrent;

namespace Polars.NET.Linq;
/// <summary>
/// Provides a set of high-level extension methods for <see cref="IQueryable{T}"/> to bridge 
/// the gap between .NET LINQ expressions and the high-performance Polars execution engine.
/// </summary>
/// <remarks>
/// This class serves as the final integration layer of the Polars.NET.LINQ provider. 
/// It leverages <c>linq2db</c> to intercept expression trees, translates them into 
/// optimized SQL, and then injects that logic directly into the Polars Rust core 
/// via <see cref="IPolarsLazyFrame"/>. 
/// </remarks>
public static class PolarsQueryableExtensions
{
    private static IPolarsLazyFrame Execute(PolarsDataContext db,string finalSql)
       => db.ExecuteToLazyFrame(finalSql);

    private static string Translate(IExpressionQuery exprQuery,Type originalType)
    {
        var sqlQueries = exprQuery.GetSqlQueries(null);

        if (sqlQueries == null || sqlQueries.Count == 0)
            throw new InvalidOperationException("[Polars.NET] linq2db SQL generation failed");
        var rawSql = sqlQueries[0].Sql;
        var sanitizedSql = SqlSanitizer.Clean(rawSql); 
        // Type elementType = ((IQueryable)exprQuery).ElementType;
        var finalSql = PolarsSqlTranslator.InjectAliases(sanitizedSql, originalType);
        // Console.WriteLine(rawSql);
        return finalSql;
    }
    /// <summary>
    /// Translates the LINQ expression tree into a Polars execution plan without executing the query.
    /// This represents the "Logic-to-Logic" phase: LINQ -> SQL -> <see cref="IPolarsLazyFrame"/>.
    /// </summary>
    internal static IPolarsLazyFrame ToILazyFrame<T>(this IQueryable<T> query)
    {
        var (db, sql) = GetTranslatedQuery(query);
        return Execute(db, sql);
    }

    /// <summary>
    /// Translates the LINQ expression tree into SQL string
    /// This represents the "Logic-to-Logic" phase: LINQ -> SQL.
    /// </summary>
    public static string ToSqlString<T>(this IQueryable<T> query)
        => GetTranslatedQuery(query).Sql;

    // ==========================================
    // Core Engine
    // ==========================================
    private static (PolarsDataContext Db, string Sql) GetTranslatedQuery<T>(IQueryable<T> query)
    {
        Type originalType = typeof(T);

        // ==========================================
        // Fast Path: Method Chain
        // ==========================================
        if (query is IExpressionQuery fastQuery && fastQuery.DataContext is PolarsDataContext fastDb)
        {
            return (fastDb, Translate(fastQuery,originalType));
        }

        // ==========================================
        // Hacker Path：AST Rewrite ( F# query { } )
        // ==========================================
        var rewriter = new FSharpAstRewriter();
        var cleanExpression = rewriter.Visit(query.Expression); 

        if (rewriter.Context != null && rewriter.Provider != null)
        {
            var pureQuery = rewriter.Provider.CreateQuery(cleanExpression);
            
            if (pureQuery is IExpressionQuery pureExprQuery)
            {
                return (rewriter.Context, Translate(pureExprQuery,originalType));
            }
        }

        // ==========================================
        // Fallback
        // ==========================================
        throw new InvalidOperationException(
            $"[Polars.NET] Failed to unwrap query of type '{query.GetType().Name}'. " +
            "Ensure the query originates from a PolarsDataContext (e.g., db.RegisterTable).");
    }
    /// <summary>
    /// Materializes the LINQ query results into an in-memory <see cref="IPolarsDataFrame"/>.
    /// This triggers the actual data processing: LINQ -> SQL -> LazyFrame -> <see cref="IPolarsDataFrame"/>.
    /// </summary>
    /// <typeparam name="T">The record or class type being queried.</typeparam>
    /// <param name="query">The <see cref="IQueryable{T}"/> source.</param>
    /// <param name="useStreaming">
    /// If set to <see langword="true"/>, enables Polars' streaming execution engine for memory-intensive computations.
    /// </param>
    /// <returns>A materialized <see cref="IPolarsDataFrame"/> containing the query results.</returns>
    public static IPolarsDataFrame ToIDataFrame<T>(this IQueryable<T> query, bool useStreaming = false)
    {
        var lf = query.ToILazyFrame();
        return lf.Collect(useStreaming);
    }
    /// <summary>
    /// Asynchronously executes the Polars query and materializes the result into a DataFrame.
    /// This will offload the heavy Rust execution to the ThreadPool.
    /// </summary>
    public static async Task<IPolarsDataFrame> ToIDataFrameAsync<T>(
        this IQueryable<T> query, 
        bool useStreaming = false,
        CancellationToken cancellationToken = default)
    {
        var lazyFrame = query.ToILazyFrame();

        try
        {
            return await lazyFrame.CollectAsync(useStreaming, cancellationToken)
                                  .ConfigureAwait(false);
        }
        finally
        {
            // lazyFrame.Dispose(); 
        }
    }
    /// <summary>
    /// Asynchronously generates the logical plan (LazyFrame) from the query.
    /// Note: This operation is very fast as it only builds the plan without executing it.
    /// </summary>
    public static Task<IPolarsLazyFrame> ToILazyFrameAsync<T>(
        this IQueryable<T> query, 
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        
        try
        {
            var lf = query.ToILazyFrame();
            return Task.FromResult(lf);
        }
        catch (Exception ex)
        {
            return Task.FromException<IPolarsLazyFrame>(ex);
        }
    }
    /// <summary>
    /// Print Execuation Plan generated by Polars
    /// </summary>
    public static string Explain<T>(this IQueryable<T> query, bool optimized = true)
    {
        var lf = query.ToILazyFrame();

        return lf.Explain(optimized);
    }
}

internal class FSharpAstRewriter : ExpressionVisitor
{
    public IQueryProvider? Provider { get; private set; }
    public PolarsDataContext? Context { get; private set; }
    private readonly struct CachedField(FieldInfo? info)
    {
        public readonly FieldInfo? Info = info;
    }

    private static readonly ConcurrentDictionary<Type, Func<object, object?>?> _compiledGetters = new();
    protected override Expression VisitConstant(ConstantExpression node)
    {
        if (node.Value != null && node.Value.GetType().IsGenericType && 
            node.Value.GetType().GetGenericTypeDefinition() == typeof(EnumerableQuery<>))
        {
            var bedrockQuery = DeepUnwrap(node.Value);
            
            if (bedrockQuery is IQueryable q && q is IExpressionQuery eq && eq.DataContext is PolarsDataContext db)
            {
                Provider = q.Provider;
                Context = db;
                
                return Expression.Constant(bedrockQuery, bedrockQuery.GetType());
            }
        }
        return base.VisitConstant(node);
    }

    private static object? DeepUnwrap(object? obj)
    {
        if (obj == null) return null;
        if (obj is IExpressionQuery) return obj; 

        if (obj is System.Collections.IEnumerable)
        {
            var type = obj.GetType();

            var getter = _compiledGetters.GetOrAdd(type, CreateCompiledGetter);

            if (getter != null)
            {
                return DeepUnwrap(getter(obj)); 
            }
        }
        return obj;
    }

    private static Func<object, object?>? CreateCompiledGetter(Type targetType)
    {
        var flags = BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public;
        var sourceField = targetType.GetField("source", flags) ?? 
                          targetType.GetField("_source", flags) ??
                          targetType.GetField("enumerable", flags) ?? 
                          targetType.GetField("_enumerable", flags);

        if (sourceField == null) return null;

        var objParam = Expression.Parameter(typeof(object), "obj");
        var castExpr = Expression.Convert(objParam, targetType);
        var fieldExpr = Expression.Field(castExpr, sourceField);
        var resultCastExpr = Expression.Convert(fieldExpr, typeof(object));

        return Expression.Lambda<Func<object, object?>>(resultCastExpr, objParam).Compile();
    }
}