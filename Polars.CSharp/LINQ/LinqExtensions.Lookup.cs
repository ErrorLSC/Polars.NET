using System.Linq.Expressions;
using Microsoft.FSharp.Core;
using Polars.NET.Core;
using Polars.NET.Core.Helpers;
using Polars.NET.Linq;
using Polars.NET.Linq.Backend.Polars;
using Polars.NET.Linq.IR;

namespace Polars.CSharp.Linq;

public static partial class LinqExtensions
{
    /// <inheritdoc cref="System.Linq.Enumerable.ToLookup{TSource, TKey}(IEnumerable{TSource}, Func{TSource, TKey}, IEqualityComparer{TKey})"/>
    public static PolarsLookup<TKey, TSource> ToLookup<TSource, TKey>(
        this IQueryable<TSource> source,
        Expression<Func<TSource, TKey>> keySelector,
        IEqualityComparer<TKey>? comparer = null)
        where TKey : notnull
        => source.ToLookup(keySelector, e => e, comparer);

    /// <summary>
    /// Converts a queryable sequence into a PolarsLookup, grouping elements by a specified key selector and selecting elements with a specified selector.
    /// </summary>
    /// <typeparam name="TSource">The type of the source elements.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TElement">The type of the element.</typeparam>
    /// <param name="source">The queryable sequence to convert.</param>
    /// <param name="keySelector">The function to extract the key from each element.</param>
    /// <param name="elementSelector">The function to extract the element from each element.</param>
    /// <param name="comparer">The equality comparer to use for the keys.</param>
    /// <returns>The resulting PolarsLookup.</returns>
    public static PolarsLookup<TKey, TElement> ToLookup<TSource, TKey, TElement>(
        this IQueryable<TSource> source,
        Expression<Func<TSource, TKey>> keySelector,
        Expression<Func<TSource, TElement>> elementSelector,
        IEqualityComparer<TKey>? comparer = null)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(elementSelector);

        // 1. Evaluate preceding LINQ pipeline down to a DataFrame
        var df = source.ToDataFrame();

        // 2. Resolve whether TSource is a scalar column or composite entity
        bool isScalarSource = PolarsTypeHelper.IsScalarType(typeof(TSource)) || typeof(TSource) == typeof(string);
        string? scalarCol = isScalarSource && df.Columns.Length == 1 ? df.Columns[0] : null;

        LogicalSchema schema;
        using (var lazy = df.Lazy())
        {
            schema = PolarsExprCompiler.schemaOf(lazy.Handle);
        }

        TranslationContext CreateContext(string paramName) =>
            scalarCol is not null
                ? TranslationContext.NewScalar(paramName, scalarCol)
                : TranslationContext.NewEntity(paramName);

        // 3. Fold closures and push down key selector using TranslationContext
        var evaluatedKeyBody = PartialEvaluator.eval(keySelector.Body);
        var keyParam = keySelector.Parameters[0].Name ?? "x";
        var keyCtx = CreateContext(keyParam);

        var keyOpt = PolarsExprCompiler.tryCompile(keyCtx, schema, evaluatedKeyBody);
        if (!FSharpOption<ExprHandle>.get_IsSome(keyOpt))
        {
            throw new NotSupportedException($"Key selector '{keySelector}' could not be translated to a Polars expression.");
        }
        var keyExpr = new Expr(keyOpt.Value);

        // 4. Native partition via Rust engine directly to DataFrame[]
        var partitions = df.PartitionBy(keyExpr, maintainOrder: true, includeKey: true);

        // 5. Translate element selector if not identity
        var isIdentity = typeof(TSource) == typeof(TElement) && elementSelector.Body is ParameterExpression;
        Expr? elemExpr = null;
        if (!isIdentity)
        {
            var evaluatedElemBody = PartialEvaluator.eval(elementSelector.Body);
            var elemParam = elementSelector.Parameters[0].Name ?? "x";
            var elemCtx = CreateContext(elemParam);

            var elemOpt = PolarsExprCompiler.tryCompile(elemCtx, schema, evaluatedElemBody);
            if (FSharpOption<ExprHandle>.get_IsSome(elemOpt))
            {
                elemExpr = new Expr(elemOpt.Value);
            }
        }

        // 6. Pre-allocate exactly one dictionary
        var groupDict = new Dictionary<TKey, PolarsGrouping<TKey, TElement>>(
            partitions.Length,
            comparer ?? EqualityComparer<TKey>.Default);

        // Fast-path: Resolve key column name from simple member access or scalar parameter
        var body = keySelector.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
        {
            body = u.Operand;
        }

        string? singleKeyCol = body switch
        {
            MemberExpression me when me.Expression is ParameterExpression => me.Member.Name,
            ParameterExpression when scalarCol is not null => scalarCol, // Direct scalar key mapping
            _ => null
        };

        foreach (var partDf in partitions)
        {
            if (partDf.Height == 0L) continue;

            TKey key;
            if (singleKeyCol is not null)
            {
                // O(1) direct scalar read from Row 0 without object[] allocations
                using var col = partDf[singleKeyCol];
                key = ConvertKey<TKey>(col.GetValue<object>(0L, uncheck: true)!);
            }
            else
            {
                // Composite key fallback: hydrate row 0 via ArrowRowMapper
                key = RowMapper<TKey>.ReadRow(partDf, 0L);
            }

            var finalDf = elemExpr is not null ? partDf.Select(elemExpr) : partDf;
            groupDict[key] = new PolarsGrouping<TKey, TElement>(key, finalDf);
        }

        return new PolarsLookup<TKey, TElement>(groupDict);
    }

    private static TKey ConvertKey<TKey>(object rawKey)
    {
        if (rawKey is TKey exact)
        {
            return exact;
        }

        var targetType = typeof(TKey);
        var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;

        // Guarantee enum safety (e.g. DayOfWeek, StringComparison): a Polars Enum key arrives as its category
        // (the member name), an integer key as the underlying value.
        if (underlying.IsEnum)
        {
            return rawKey is string name
                ? (TKey)Enum.Parse(underlying, name)
                : (TKey)Enum.ToObject(underlying, rawKey);
        }

        return (TKey)Convert.ChangeType(rawKey, underlying);
    }
}
