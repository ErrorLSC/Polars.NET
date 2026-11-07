using System.Diagnostics.CodeAnalysis;
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
    public static PolarsLookup<TKey, TSource> ToLookup<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TSource, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TKey>(
        this IQueryable<TSource> source,
        Expression<Func<TSource, TKey>> keySelector,
        IEqualityComparer<TKey>? comparer = null)
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
    public static PolarsLookup<TKey, TElement> ToLookup<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TSource, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TKey, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TElement>(
        this IQueryable<TSource> source,
        Expression<Func<TSource, TKey>> keySelector,
        Expression<Func<TSource, TElement>> elementSelector,
        IEqualityComparer<TKey>? comparer = null)
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

        Expr Compile(TranslationContext ctx, Expression body, LambdaExpression selector, string role)
        {
            var compiled = PolarsExprCompiler.tryCompile(ctx, schema, body);
            if (!FSharpOption<ExprHandle>.get_IsSome(compiled))
            {
                throw new NotSupportedException($"{role} selector '{selector}' could not be translated to a Polars expression.");
            }
            return new Expr(compiled.Value);
        }

        // 3. Key columns: a plain column is partitioned on directly; a computed key, or each member of a composite
        //    key (anonymous type, tuple), is first added as a temporary column.
        var evaluatedKeyBody = PartialEvaluator.eval(keySelector.Body);
        var keyParam = keySelector.Parameters[0].Name ?? "x";
        var keyCtx = CreateContext(keyParam);

        var body = evaluatedKeyBody;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u && u.Type == u.Operand.Type)
        {
            body = u.Operand;
        }

        string? singleKeyCol = body switch
        {
            MemberExpression me when me.Expression is ParameterExpression && me.Type == typeof(TKey) && schema.Contains(me.Member.Name) => me.Member.Name,
            ParameterExpression when scalarCol is not null => scalarCol, // Direct scalar key mapping
            _ => null
        };

        var compositeMembers = singleKeyCol is null ? TryCompositeKeyMembers(evaluatedKeyBody) : null;
        var tempKeyCols = new List<string>();
        var keyed = df;

        if (singleKeyCol is null)
        {
            var keyExprs = new List<Expr>();
            string TempName(int i)
            {
                var name = $"__polars_lookup_key_{i}";
                while (schema.Contains(name)) name = "_" + name;
                return name;
            }

            if (compositeMembers is not null)
            {
                for (int i = 0; i < compositeMembers.Count; i++)
                {
                    tempKeyCols.Add(TempName(i));
                    keyExprs.Add(Compile(keyCtx, compositeMembers[i].Expr, keySelector, "Key").Alias(tempKeyCols[i]));
                }
            }
            else
            {
                tempKeyCols.Add(TempName(0));
                keyExprs.Add(Compile(keyCtx, evaluatedKeyBody, keySelector, "Key").Alias(tempKeyCols[0]));
            }

            keyed = df.WithColumns([.. keyExprs]);
        }

        // 4. Native partition via Rust engine directly to DataFrame[]
        var partitionColumns = singleKeyCol is not null ? [singleKeyCol] : tempKeyCols;
        var partitions = keyed.PartitionBy(
            partitionColumns.Select(c => (IntoSelector)c),
            maintainOrder: true,
            includeKey: true);

        // 5. Translate element selector if not identity: a composite element (anonymous type, tuple) is selected
        //    as one column per member, named as the element type's members.
        var isIdentity = typeof(TSource) == typeof(TElement) && elementSelector.Body is ParameterExpression;
        IntoExprColumn[]? elemExprs = null;
        if (!isIdentity)
        {
            var evaluatedElemBody = PartialEvaluator.eval(elementSelector.Body);
            var elemCtx = CreateContext(elementSelector.Parameters[0].Name ?? "x");
            elemExprs = TryCompositeKeyMembers(evaluatedElemBody) is { } elemMembers
                ? [.. elemMembers.Select(m => (IntoExprColumn)Compile(elemCtx, m.Expr, elementSelector, "Element").Alias(m.Name))]
                : [Compile(elemCtx, evaluatedElemBody, elementSelector, "Element")];
        }

        // 6. Groups in the order their keys first appear (partitions keep that order)
        var groups = new List<PolarsGrouping<TKey, TElement>>(partitions.Length);

        foreach (var partDf in partitions)
        {
            if (partDf.Height == 0L) continue;

            TKey key;
            if (compositeMembers is not null)
            {
                // Rebuild the composite key from its member columns, named as the key type's members.
                using var keyRow = partDf.Head(1);
                using var keyDf = keyRow.Select(
                    [.. compositeMembers.Select((m, i) => (IntoExprColumn)Polars.Col(tempKeyCols[i]).Alias(m.Name))]);
                // The row mapper folds an all-null row to a null object (outer-join semantics); a composite key
                // is never null, so such a row is the key whose members are all null.
                key = RowMapper<TKey>.ReadRow(keyDf, 0L) ?? AllNullCompositeKey<TKey>(compositeMembers.Count);
            }
            else
            {
                // O(1) direct scalar read from Row 0 without object[] allocations
                using var col = partDf[singleKeyCol ?? tempKeyCols[0]];
                key = col.GetValue<object>(0L, uncheck: true) is { } rawKey ? ConvertKey<TKey>(rawKey) : default!;
            }

            var finalDf = elemExprs is not null
                ? partDf.Select(elemExprs)
                : tempKeyCols.Count > 0 ? partDf.Drop([.. tempKeyCols]) : partDf;
            groups.Add(new PolarsGrouping<TKey, TElement>(key, finalDf));
        }

        return new PolarsLookup<TKey, TElement>(groups, comparer);
    }

    /// <summary>
    /// A composite key (anonymous type, tuple, member initializer) all of whose members are null.
    /// </summary>
    private static TKey AllNullCompositeKey<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] TKey>(int memberCount)
    {
        var type = typeof(TKey);
        var ctor = type.GetConstructors().FirstOrDefault(c => c.GetParameters().Length == memberCount);
        if (ctor is not null)
        {
            return (TKey)ctor.Invoke([.. ctor.GetParameters().Select(p => p.ParameterType.IsValueType ? Activator.CreateInstance(p.ParameterType) : null)]);
        }

        return (TKey)Activator.CreateInstance(type)!;
    }

    /// <summary>
    /// The members of a composite value (an anonymous type, a Tuple / ValueTuple, or a member initializer) with the
    /// names its type is read back by (tuple elements as Item1..ItemN), or null for any other expression.
    /// </summary>
    private static List<(string Name, Expression Expr)>? TryCompositeKeyMembers(Expression body)
    {
        static bool IsTuple(Type t) =>
            t.IsGenericType && (t.FullName?.StartsWith("System.Tuple`", StringComparison.Ordinal) == true
                || t.FullName?.StartsWith("System.ValueTuple`", StringComparison.Ordinal) == true)
            && t.GetGenericArguments().Length <= 7;

        static List<(string, Expression)> Items(IReadOnlyList<Expression> args) =>
            [.. args.Select((a, i) => ($"Item{i + 1}", a))];

        switch (body)
        {
            case NewExpression ne when IsTuple(ne.Type):
                return Items(ne.Arguments);
            case NewExpression { Members: { } members } ne when members.Count == ne.Arguments.Count:
                return [.. members.Select((m, i) => (m.Name, ne.Arguments[i]))];
            case MethodCallExpression mc when mc.Method.Name == "Create"
                && (mc.Method.DeclaringType == typeof(Tuple) || mc.Method.DeclaringType == typeof(ValueTuple))
                && IsTuple(mc.Type):
                return Items(mc.Arguments);
            case MemberInitExpression mi when mi.NewExpression.Arguments.Count == 0
                && mi.Bindings.All(b => b is MemberAssignment):
                return [.. mi.Bindings.Select(b => (b.Member.Name, ((MemberAssignment)b).Expression))];
            default:
                return null;
        }
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
