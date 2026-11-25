// The LINQ provider builds generic code at run time (rows of element types known only then): its public entry points
// carry RequiresDynamicCode / RequiresUnreferencedCode, so the analyzer's warnings inside it are silenced here.
#pragma warning disable IL2026, IL2060, IL2067, IL2070, IL2072, IL2075, IL2080, IL2087, IL2090, IL2091, IL3050, IL3051

using System.Linq.Expressions;
using Kaminari.Samidare;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Marshalling;
using Polars.NET.Core.Linq.Backend;

namespace Polars.NET.Core.Linq.Execution;

/// <summary>
/// Executing a pipeline that needs the full LINQ-to-Objects fallback: the longest natively compilable prefix runs in
/// Polars, and the rest of the pipeline runs via EnumerableQuery over its materialized rows.
/// </summary>
internal static class InMemoryExecution
{
    /// <summary>
    /// For a pipeline that needs full LINQ-to-Objects fallback, finds the longest prefix of its method-call chain that
    /// compiles without it. Returns the call at which LINQ-to-Objects takes over and the prefix expression that call
    /// consumes, or null when no non-trivial prefix runs natively. <paramref name="effectiveExpr"/> must be the tree the
    /// caller later rewrites, since the prefix is identified by reference.
    /// </summary>
    public static (MethodCallExpression Consumer, Expression Prefix)? TryFindNativePrefix(IQueryProvider provider, Expression effectiveExpr)
    {
        static bool IsQueryable(Type t) => typeof(IQueryable).IsAssignableFrom(t);

        bool CompilesNatively(Expression prefix)
        {
            try
            {
                return provider.CreateQuery(prefix) is IInternalQueryExplainable q && q.IsNativelyExecutable();
            }
            catch
            {
                return false;
            }
        }

        // Method calls along the source spine, outermost first.
        for (var e = effectiveExpr;
             e is MethodCallExpression call && call.Object is null && call.Arguments.Count > 0 && IsQueryable(call.Arguments[0].Type);
             e = call.Arguments[0])
        {
            var consumer = (MethodCallExpression)e;
            var prefix = consumer.Arguments[0];
            // ThenBy needs the ordered sequence its OrderBy produced, so the two are never split.
            bool splitsOrdering = consumer.Method.Name is "ThenBy" or "ThenByDescending";
            if (prefix is MethodCallExpression && !splitsOrdering && CompilesNatively(prefix)) return (consumer, prefix);
        }
        return null;
    }

    /// <summary>Executes the full LINQ expression pipeline in-memory via .NET EnumerableQuery LINQ-to-Objects engine.</summary>
    public static IEnumerable<T> ExecuteRootInMemory<T>(Expression effectiveExpr)
    {
        if (!QueryCompiler.TryFindRootQueryable(effectiveExpr, out var rootExpr, out var rootType))
            throw new InvalidOperationException($"Failed to locate root queryable for in-memory execution: {effectiveExpr}");
        var rootConst = (ConstantExpression)rootExpr;
        var rootQueryable = (IQueryable)rootConst.Value!;
        var rootElemType = ClrTypeHelper.TryGetEnumerableElementType(rootType) ?? rootQueryable.ElementType;
        var casted = LinqReflectionCache.GetEnumerableMethod("Cast", 1, [rootElemType]).Invoke(null, [rootQueryable]);
        var inMemoryArray = LinqReflectionCache.GetEnumerableMethod("ToArray", 1, [rootElemType]).Invoke(null, [casted]);
        var inMemoryQueryable = (IQueryable)LinqReflectionCache.GetQueryableMethod("AsQueryable", 1, [rootElemType]).Invoke(null, [inMemoryArray])!;
        var replacement = Expression.Constant(inMemoryQueryable, typeof(IQueryable<>).MakeGenericType(rootElemType));
        var inMemoryExpr = new RootReplacer(rootConst.Value!, replacement).Visit(effectiveExpr);
        return inMemoryQueryable.Provider.CreateQuery<T>(inMemoryExpr);
    }

    private sealed class RootReplacer(object root, ConstantExpression replacement) : ExpressionVisitor
    {
        protected override Expression VisitConstant(ConstantExpression node) =>
            node.Value is not null && (ReferenceEquals(node.Value, root) || typeof(IPolarsPlanSource).IsAssignableFrom(node.Type))
                ? replacement
                : base.VisitConstant(node);
    }

    private sealed class NodeReplacer(Expression target, Expression replacement) : ExpressionVisitor
    {
        public override Expression? Visit(Expression? node) => ReferenceEquals(node, target) ? replacement : base.Visit(node);
    }

    /// <summary>
    /// Executes a pipeline that needs LINQ-to-Objects: the longest natively compilable prefix runs in Polars, and the
    /// rest of the pipeline runs via EnumerableQuery over the materialized prefix rows. Falls back to replaying the
    /// whole pipeline over the in-memory root when no prefix runs natively.
    /// </summary>
    public static IEnumerable<T> Execute<T>(IQueryProvider provider, Expression effectiveExpr)
    {
        if (TryFindNativePrefix(provider, effectiveExpr) is { } split
            && ClrTypeHelper.TryGetEnumerableElementType(split.Prefix.Type) is { } rowType)
        {
            var prefixQuery = provider.CreateQuery(split.Prefix);
            var rows = LinqReflectionCache.GetEnumerableMethod("ToArray", 1, [rowType]).Invoke(null, [prefixQuery]);
            var localRows = (IQueryable)LinqReflectionCache.GetQueryableMethod("AsQueryable", 1, [rowType]).Invoke(null, [rows])!;
            if (split.Prefix.Type.IsAssignableFrom(localRows.GetType()))
            {
                var suffixExpr = new NodeReplacer(split.Prefix, Expression.Constant(localRows, split.Prefix.Type)).Visit(effectiveExpr)!;
                return localRows.Provider.CreateQuery<T>(suffixExpr);
            }
        }
        return ExecuteRootInMemory<T>(effectiveExpr);
    }
}
