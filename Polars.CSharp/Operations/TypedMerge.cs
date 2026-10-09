using System.Linq.Expressions;
using Microsoft.FSharp.Core;
using Polars.NET.Core;
using Polars.CSharp.Linq;
using Polars.NET.Linq;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

/// <summary>
/// Provides strongly-typed setters for updating Target columns during a Merge operation.
/// </summary>
/// <typeparam name="TTarget">The target entity or record type.</typeparam>
/// <typeparam name="TSource">The source entity or record type.</typeparam>
public class TypedMergeUpdateSetterBuilder<TTarget, TSource>
{
    private readonly string _sourceSuffix;
    internal Dictionary<string, IntoExprColumn> Setters { get; } = [];

    internal TypedMergeUpdateSetterBuilder(string sourceSuffix)
    {
        _sourceSuffix = sourceSuffix;
    }

    /// <summary>
    /// Updates a specific column using an expression referencing Target and Source.
    /// </summary>
    public TypedMergeUpdateSetterBuilder<TTarget, TSource> Set<TProp>(
        Expression<Func<TTarget, TProp>> targetProperty,
        Expression<Func<TTarget, TSource, TProp>> valueExpression)
    {
        string colName = ExtractPropertyName(targetProperty);
        var exprHandleOpt = ExprTranslator.tryTranslateMergeLambda(_sourceSuffix, valueExpression);
        if (FSharpOption<ExprHandle>.get_IsNone(exprHandleOpt))
        {
            throw new NotSupportedException($"Expression '{valueExpression}' could not be translated to a native Polars expression.");
        }

        Setters[colName] = new Expr(exprHandleOpt.Value);
        return this;
    }

    /// <summary>
    /// Updates a specific target column using an expression directly over the Source entity.
    /// </summary>
    public TypedMergeUpdateSetterBuilder<TTarget, TSource> Set<TProp>(
        Expression<Func<TTarget, TProp>> targetProperty,
        Expression<Func<TSource, TProp>> valueExpression)
    {
        string colName = ExtractPropertyName(targetProperty);
        var exprHandleOpt = ExprTranslator.tryTranslateSourceOnlyLambda(_sourceSuffix, valueExpression);
        if (FSharpOption<ExprHandle>.get_IsNone(exprHandleOpt))
        {
            throw new NotSupportedException($"Expression '{valueExpression}' could not be translated to a native Polars expression.");
        }

        Setters[colName] = new Expr(exprHandleOpt.Value);
        return this;
    }

    /// <summary>
    /// Updates a specific column with a constant value.
    /// </summary>
    public TypedMergeUpdateSetterBuilder<TTarget, TSource> Set<TProp>(
        Expression<Func<TTarget, TProp>> targetProperty,
        TProp value)
    {
        string colName = ExtractPropertyName(targetProperty);
        var handle = ExprTranslator.toLiteralHandle(value!, typeof(TProp));
        Setters[colName] = new Expr(handle);
        return this;
    }

    private static string ExtractPropertyName<TProp>(Expression<Func<TTarget, TProp>> propertyLambda)
    {
        if (propertyLambda.Body is MemberExpression member)
            return member.Member.Name;
        if (propertyLambda.Body is UnaryExpression { Operand: MemberExpression innerMember })
            return innerMember.Member.Name;

        throw new ArgumentException($"Expression '{propertyLambda}' must refer directly to a property of {typeof(TTarget).Name}.");
    }
}

/// <summary>
/// Provides strongly-typed setters for inserting new rows during a Merge operation.
/// </summary>
/// <typeparam name="TTarget">The target entity or record type.</typeparam>
/// <typeparam name="TSource">The source entity or record type.</typeparam>
public class TypedMergeInsertSetterBuilder<TTarget, TSource>
{
    private readonly string _sourceSuffix;
    internal Dictionary<string, IntoExprColumn> Setters { get; } = [];

    internal TypedMergeInsertSetterBuilder(string sourceSuffix)
    {
        _sourceSuffix = sourceSuffix;
    }

    /// <summary>
    /// Sets a target column value from an expression over the Source entity.
    /// </summary>
    public TypedMergeInsertSetterBuilder<TTarget, TSource> Set<TProp>(
        Expression<Func<TTarget, TProp>> targetProperty,
        Expression<Func<TSource, TProp>> valueExpression)
    {
        string colName = ExtractPropertyName(targetProperty);
        var exprHandleOpt = ExprTranslator.tryTranslateSourceOnlyLambda(_sourceSuffix, valueExpression);
        if (FSharpOption<ExprHandle>.get_IsNone(exprHandleOpt))
        {
            throw new NotSupportedException($"Expression '{valueExpression}' could not be translated to a native Polars expression.");
        }

        Setters[colName] = new Expr(exprHandleOpt.Value);
        return this;
    }

    /// <summary>
    /// Sets a target column value to a constant value.
    /// </summary>
    public TypedMergeInsertSetterBuilder<TTarget, TSource> Set<TProp>(
        Expression<Func<TTarget, TProp>> targetProperty,
        TProp value)
    {
        string colName = ExtractPropertyName(targetProperty);
        var handle = ExprTranslator.toLiteralHandle(value!, typeof(TProp));
        Setters[colName] = new Expr(handle);
        return this;
    }

    private static string ExtractPropertyName<TProp>(Expression<Func<TTarget, TProp>> propertyLambda)
    {
        if (propertyLambda.Body is MemberExpression member)
            return member.Member.Name;
        if (propertyLambda.Body is UnaryExpression { Operand: MemberExpression innerMember })
            return innerMember.Member.Name;

        throw new ArgumentException($"Expression '{propertyLambda}' must refer directly to a property of {typeof(TTarget).Name}.");
    }
}

/// <summary>
/// Action builder for the matched row condition in a typed merge.
/// </summary>
public class MergeMatchedAction<TTarget, TSource>
{
    private readonly string _sourceSuffix;
    internal MergeActionType ActionType { get; private set; } = MergeActionType.MatchedUpdate;
    internal Dictionary<string, IntoExprColumn>? Setters { get; private set; }

    internal MergeMatchedAction(string sourceSuffix)
    {
        _sourceSuffix = sourceSuffix;
    }

    /// <summary>
    /// Updates matched target rows. Optionally configures column-level overrides.
    /// </summary>
    public void Update(Action<TypedMergeUpdateSetterBuilder<TTarget, TSource>>? set = null)
    {
        ActionType = MergeActionType.MatchedUpdate;
        if (set != null)
        {
            var sb = new TypedMergeUpdateSetterBuilder<TTarget, TSource>(_sourceSuffix);
            set(sb);
            Setters = sb.Setters;
        }
    }

    /// <summary>
    /// Deletes matched target rows.
    /// </summary>
    public void Delete()
    {
        ActionType = MergeActionType.MatchedDelete;
        Setters = null;
    }
}

/// <summary>
/// Action builder for rows not matched in the target table.
/// </summary>
public class MergeNotMatchedByTargetAction<TTarget, TSource>
{
    private readonly string _sourceSuffix;
    internal Dictionary<string, IntoExprColumn>? Setters { get; private set; }

    internal MergeNotMatchedByTargetAction(string sourceSuffix)
    {
        _sourceSuffix = sourceSuffix;
    }

    /// <summary>
    /// Inserts a new row with optional column overrides.
    /// </summary>
    public void Insert(Action<TypedMergeInsertSetterBuilder<TTarget, TSource>>? set = null)
    {
        if (set != null)
        {
            var sb = new TypedMergeInsertSetterBuilder<TTarget, TSource>(_sourceSuffix);
            set(sb);
            Setters = sb.Setters;
        }
    }
}

/// <summary>
/// Action builder for target rows not matched by any source row.
/// </summary>
public class MergeNotMatchedBySourceAction<TTarget>
{
    internal MergeActionType ActionType { get; private set; } = MergeActionType.NotMatchedBySourceDelete;

    /// <summary>
    /// Deletes target rows that do not exist in the incoming source.
    /// </summary>
    public void Delete()
    {
        ActionType = MergeActionType.NotMatchedBySourceDelete;
    }
}

/// <summary>
/// A strongly-typed builder for Polars Merge operations.
/// </summary>
/// <typeparam name="TTarget">Type representing target entities/rows.</typeparam>
/// <typeparam name="TSource">Type representing incoming source delta entities/rows.</typeparam>
public class TypedMergeBuilder<TTarget, TSource> : MergeBuilderBase<TypedMergeBuilder<TTarget, TSource>>
{
    internal TypedMergeBuilder(LazyFrame target, LazyFrame source, string[] on)
        : base(target, source, on)
    {
    }

    /// <summary>
    /// Applies actions (Update or Delete) when a row is matched between Target and Source.
    /// </summary>
    public TypedMergeBuilder<TTarget, TSource> WhenMatched(
        Expression<Func<TTarget, TSource, bool>> condition,
        Action<MergeMatchedAction<TTarget, TSource>> then)
    {
        var exprHandleOpt = ExprTranslator.tryTranslateMergeLambda(_tmpSfx, condition);
        if (FSharpOption<ExprHandle>.get_IsNone(exprHandleOpt))
        {
            throw new NotSupportedException($"Condition expression '{condition}' could not be translated natively.");
        }

        Expr condExpr = new(exprHandleOpt.Value);
        var act = new MergeMatchedAction<TTarget, TSource>(_tmpSfx);
        then(act);

        _actions.Add((_actions.Count + 1, act.ActionType, condExpr, act.Setters));
        return this;
    }

    /// <summary>
    /// Unconditional matched action (Update or Delete).
    /// </summary>
    public TypedMergeBuilder<TTarget, TSource> WhenMatched(
        Action<MergeMatchedAction<TTarget, TSource>> then)
    {
        var act = new MergeMatchedAction<TTarget, TSource>(_tmpSfx);
        then(act);

        _actions.Add((_actions.Count + 1, act.ActionType, Pl.Lit(true), act.Setters));
        return this;
    }

    /// <summary>
    /// Inserts a new row when a source row does not match any target row, filtered by condition.
    /// </summary>
    public TypedMergeBuilder<TTarget, TSource> WhenNotMatchedByTarget(
        Expression<Func<TSource, bool>> condition,
        Action<MergeNotMatchedByTargetAction<TTarget, TSource>> then)
    {
        var exprHandleOpt = ExprTranslator.tryTranslateSourceOnlyLambda(_tmpSfx, condition);
        if (FSharpOption<ExprHandle>.get_IsNone(exprHandleOpt))
        {
            throw new NotSupportedException($"Source condition expression '{condition}' could not be translated natively.");
        }

        Expr condExpr = new(exprHandleOpt.Value);
        var act = new MergeNotMatchedByTargetAction<TTarget, TSource>(_tmpSfx);
        then(act);

        _actions.Add((_actions.Count + 1, MergeActionType.NotMatchedInsert, condExpr, act.Setters));
        return this;
    }

    /// <summary>
    /// Unconditionally inserts a new row when a source row does not match any target row.
    /// </summary>
    public TypedMergeBuilder<TTarget, TSource> WhenNotMatchedByTarget(
        Action<MergeNotMatchedByTargetAction<TTarget, TSource>> then)
    {
        var act = new MergeNotMatchedByTargetAction<TTarget, TSource>(_tmpSfx);
        then(act);

        _actions.Add((_actions.Count + 1, MergeActionType.NotMatchedInsert, Pl.Lit(true), act.Setters));
        return this;
    }

    /// <summary>
    /// Deletes target rows when they do not match any row in the incoming source.
    /// </summary>
    public TypedMergeBuilder<TTarget, TSource> WhenNotMatchedBySource(
        Expression<Func<TTarget, bool>> condition,
        Action<MergeNotMatchedBySourceAction<TTarget>> then)
    {
        var exprHandleOpt = ExprTranslator.tryTranslate(condition.Parameters[0].Name, condition.Body);
        if (FSharpOption<ExprHandle>.get_IsNone(exprHandleOpt))
        {
            throw new NotSupportedException($"Target condition expression '{condition}' could not be translated natively.");
        }

        Expr condExpr = new(exprHandleOpt.Value);
        var act = new MergeNotMatchedBySourceAction<TTarget>();
        then(act);

        _actions.Add((_actions.Count + 1, act.ActionType, condExpr, null));
        return this;
    }

    /// <summary>
    /// Unconditionally deletes target rows when they do not match any row in the incoming source.
    /// </summary>
    public TypedMergeBuilder<TTarget, TSource> WhenNotMatchedBySource(
        Action<MergeNotMatchedBySourceAction<TTarget>> then)
    {
        var act = new MergeNotMatchedBySourceAction<TTarget>();
        then(act);

        _actions.Add((_actions.Count + 1, act.ActionType, Pl.Lit(true), null));
        return this;
    }

    /// <summary>
    /// Returns the compiled Polars LazyFrame execution plan for this merge.
    /// </summary>
    public LazyFrame ToLazyFrame()
    {
        _srcSchemaCache = _source.CollectSchema().ToFrozenDictionary();
        _tgtSchemaCache = _target.Schema.ToFrozenDictionary();
        ValidateMergePhase();
        return BuildAst();
    }

    /// <summary>
    /// Executes the merge operation eagerly and materializes as a DataFrame.
    /// </summary>
    public DataFrame ToDataFrame(Engine engine = Engine.Auto)
    {
        return ToLazyFrame().Collect(engine);
    }

    /// <summary>
    /// Executes the merge operation and materializes directly into strongly-typed target entities.
    /// </summary>
    public List<TTarget> ToList(Engine engine = Engine.Auto)
    {
        using var df = ToDataFrame(engine);
        var cursor = df.Rows<TTarget>();                       
        return cursor.ToList();
    }
    /// <summary>
    /// Compiles the merge plan into an IQueryable pipeline for further native LINQ queries.
    /// </summary>
    public IQueryable<TTarget> AsQueryable()
    {
        var lf = ToLazyFrame();
        return new PolarsQuery<TTarget>(lf.Handle, CSharpRowCursorMaterializer.Instance);
    }
}

/// <summary>
/// Extension methods enabling strongly-typed Merge operations on IQueryable and LazyFrames.
/// </summary>
public static class TypedMergeExtensions
{
    /// <summary>
    /// Uniformly extracts a LazyFrame from an IQueryable, converting in-memory sequences when necessary.
    /// </summary>
    private static LazyFrame ResolveLazyFrame<T>(IEnumerable<T> sequence)
    {
        // Branch 1: Native Polars-backed queryable
        if (sequence is IPolarsPlanSource planSource)
        {
            return new LazyFrame(planSource.GetCompiledLazyFrameHandle());
        }

        // Branch 2: In-memory sequence (e.g., EnumerableQuery<T> from list.AsQueryable())
        if (sequence is IEnumerable<T> inMemoryRows)
        {
            var df = DataFrame.FromRows(inMemoryRows);
            return df.Lazy();
        }

        throw new NotSupportedException(
            $"Queryable of type '{sequence.GetType().Name}' cannot be converted to a Polars LazyFrame. " +
            $"Ensure it implements IPolarsPlanSource or IEnumerable<{typeof(T).Name}>.");
    }
    
    /// <summary>
    /// Initiates a strongly-typed Merge builder between two queries.
    /// Supports both native Polars queries and in-memory collections.
    /// </summary>
    public static TypedMergeBuilder<TTarget, TSource> Merge<TTarget, TSource, TKey>(
        this IEnumerable<TTarget> target,
        IEnumerable<TSource> source,
        Expression<Func<TTarget, TKey>> targetKey,
        Expression<Func<TSource, TKey>> sourceKey)
    {
        var targetLf = ResolveLazyFrame(target);
        var sourceLf = ResolveLazyFrame(source);

        string[] onKeys = ExtractKeyNames(targetKey, sourceKey);
        return new TypedMergeBuilder<TTarget, TSource>(targetLf, sourceLf, onKeys);
    }

    /// <summary>
    /// Overload when target is queryable but source is directly a DataFrame/LazyFrame.
    /// </summary>
    public static TypedMergeBuilder<TTarget, TSource> Merge<TTarget, TSource, TKey>(
        this IEnumerable<TTarget> target,
        DataFrame source,
        Expression<Func<TTarget, TKey>> targetKey,
        Expression<Func<TSource, TKey>> sourceKey)
    {
        return target.Merge(source.AsQueryable<TSource>(), targetKey, sourceKey);
    }

    private static string[] ExtractKeyNames<TTarget, TSource, TKey>(
        Expression<Func<TTarget, TKey>> targetKey,
        Expression<Func<TSource, TKey>> sourceKey)
    {
        static string[] GetNames(LambdaExpression expr)
        {
            var cleanBody = expr.Body is UnaryExpression u ? u.Operand : expr.Body;

            // Single Column: t => t.Id
            if (cleanBody is MemberExpression m)
                return [m.Member.Name];

            // Anonymous object or ValueTuple: t => new { t.A, t.B } or t => (t.A, t.B)
            if (cleanBody is NewExpression ne)
            {
                // Anonymous object: Member.Name
                if (ne.Members != null && ne.Members.Count > 0)
                {
                    return [.. ne.Members.Select(mem => mem.Name)];
                }

                // ValueTuple (t.A, t.B)
                var argNames = ne.Arguments.Select(arg =>
                {
                    var cleanArg = arg is UnaryExpression ua ? ua.Operand : arg;
                    if (cleanArg is MemberExpression ma)
                        return ma.Member.Name;
                    throw new ArgumentException($"Cannot resolve column name from key argument: '{arg}'.");
                }).ToArray();

                if (argNames.Length > 0)
                    return argNames;
            }

            throw new ArgumentException($"Unsupported key selector expression: '{expr}'. Use single property (x => x.Id), anonymous object (x => new {{ x.A, x.B }}), or tuple (x => (x.A, x.B)).");
        }

        var targetCols = GetNames(targetKey);
        var sourceCols = GetNames(sourceKey);

        if (targetCols.Length != sourceCols.Length)
        {
            throw new ArgumentException("Target key and Source key must have the same number of columns.");
        }

        return targetCols;
    }
    /// <summary>
    /// Initiates a strongly-typed Merge operation between two DataFrames.
    /// </summary>
    public static TypedMergeBuilder<TTarget, TSource> Merge<TTarget, TSource, TKey>(
        this DataFrame target,
        DataFrame source,
        Expression<Func<TTarget, TKey>> targetKey,
        Expression<Func<TSource, TKey>> sourceKey)
    {
        return target.AsQueryable<TTarget>().Merge(source.AsQueryable<TSource>(), targetKey, sourceKey);
    }
}