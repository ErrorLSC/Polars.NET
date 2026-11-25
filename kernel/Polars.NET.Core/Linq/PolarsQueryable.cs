// The LINQ provider builds generic code at run time (queries over element types known only then): its public entry
// points carry RequiresDynamicCode / RequiresUnreferencedCode, so the analyzer's warnings inside it are silenced here.
#pragma warning disable IL2026, IL2060, IL2067, IL2070, IL2072, IL2075, IL2080, IL2087, IL2090, IL2091, IL3050, IL3051

using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using Kaminari.Samidare;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Marshalling;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;
using Polars.NET.Core.Helpers;
using Polars.NET.Core.Linq.Backend;
using Polars.NET.Core.Linq.Execution;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Linq;

/// <summary>
/// The language whose compiler built a query's expression trees. F# query expressions are rewritten (quotations, curried
/// closures, AnonymousObject tuples, nested SelectMany chains) before translation; C# expression trees are translated as
/// they are.
/// </summary>
public enum QueryLanguage
{
    CSharp = 0,
    FSharp = 1,
}

/// <summary>
/// A natively compiled query whose execution is split into its two remaining phases, collect and materialize, so that
/// benchmarks can time translation, native execution and row materialization separately. Together the phases do what
/// enumerating the query does (see PolarsQuery.ExecuteQuery).
/// </summary>
internal sealed class PreparedQuery<T>(LazyFrameHandle nativeLf, ClientResidual residual, FSharpList<LinqStage> stages, IDataFrameMaterializer materializer)
    : IDisposable
{
    /// <summary>Runs the native plan. Collecting consumes a plan, so each call collects a clone and the query can run again.</summary>
    public DataFrameHandle Collect() => RowMaterialization.Collect(PolarsWrapper.LazyClone(nativeLf), stages);

    /// <summary>
    /// Materializes a collected frame as the query's elements, applying any residual row operations. The materializer
    /// takes ownership of the frame, so pass a clone to materialize the same frame repeatedly.
    /// </summary>
    public List<T> Materialize(DataFrameHandle df) => [.. RowMaterialization.Materialize<T>(materializer, stages, residual, df)];

    public void Dispose() => nativeLf.Dispose();
}

/// <summary>Strongly-typed IQueryProvider backed by Polars.NET.Core.</summary>
internal sealed class PolarsQueryProvider : IQueryProvider
{
    private readonly Lazy<ILogicalTableSource> _rootSource;

    public PolarsQueryProvider(Lazy<ILogicalTableSource> rootSource, IDataFrameMaterializer materializer, QueryLanguage language)
    {
        PolarsRuntimeConfig.EnsureImported();
        _rootSource = rootSource;
        Materializer = materializer;
        Language = language;
    }

    // The root frame's schema is read once per provider. Lowering clones the handle, so the binding never hands it out.
    public PolarsQueryProvider(LazyFrameHandle initialLazyFrame, IDataFrameMaterializer materializer, QueryLanguage language)
        : this(new Lazy<ILogicalTableSource>(() => DeltaKernelSources.OfLazyFrame(initialLazyFrame, initialLazyFrame)), materializer, language)
    {
    }

    /// <summary>Root table source, bound to slot 0 of every query this provider compiles.</summary>
    public ILogicalTableSource RootSource => _rootSource.Value;

    public IDataFrameMaterializer Materializer { get; }

    /// <summary>Shared by every query derived from this provider's root.</summary>
    public QueryLanguage Language { get; }

    public IQueryable CreateQuery(Expression expression)
    {
        var elemType = ClrTypeHelper.TryGetEnumerableElementType(expression.Type) ?? typeof(object);
        var queryType = typeof(PolarsQuery<>).MakeGenericType(elemType);
        return (IQueryable)Activator.CreateInstance(
            queryType, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public, null, [this, expression], null)!;
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression) => new PolarsQuery<TElement>(this, expression);

    public object? Execute(Expression expression) =>
        LinqReflectionCache.GetQueryProviderExecuteMethod(expression.Type).Invoke(this, [expression]);

    public TResult Execute<TResult>(Expression expression)
    {
        var targetType = typeof(TResult);
        // An element of an upcast sequence: taken from the sequence without the upcasts, then converted.
        if (ElementUpcast.TryStripTerminal(expression) is { } narrowed) return (TResult)((IQueryProvider)this).Execute(narrowed)!;
        if ((expression is MethodCallExpression call && QueryCompiler.IsScalarReductionMethod(call.Method)) || ClrTypeHelper.IsScalarType(targetType))
            return PolarsScalarRunner.Execute(Materializer, QueryCompiler.PlanScalar<TResult>(PolarsQueryBackend.Instance, this, expression));

        var query = ((IQueryProvider)this).CreateQuery(expression);
        if (query is TResult matched) return matched;
        var elemType = ClrTypeHelper.TryGetEnumerableElementType(targetType) ?? typeof(object);
        var list = LinqReflectionCache.GetEnumerableMethod("ToList", 1, [elemType]).Invoke(null, [query])!;
        if (targetType.IsArray) return (TResult)LinqReflectionCache.GetEnumerableMethod("ToArray", 1, [elemType]).Invoke(null, [query])!;
        if (targetType.IsAssignableFrom(list.GetType())) return (TResult)list;
        throw new InvalidOperationException($"Cannot convert query sequence to requested terminal type '{targetType.FullName}'");
    }
}

/// <summary>A LINQ query over Polars: compiled to the IR by Samidare, lowered to a native plan, materialized as <typeparamref name="T"/>.</summary>
public sealed class PolarsQuery<T>
    : IOrderedQueryable<T>, IAsyncEnumerable<T>, IPolarsPlanSource, IInternalPlanSource, IQueryExplainable, IInternalQueryExplainable
{
    private readonly PolarsQueryProvider _provider;
    private readonly Expression? _source;
    private readonly IDataFrameMaterializer _materializer;
    private readonly Type _targetType = typeof(T);
    private readonly bool _isScalar = ClrTypeHelper.IsScalarType(typeof(T));
    private readonly bool _isGrouping = QueryCompiler.IsGroupingType(typeof(T));
    private readonly Expression _expression;

    /// <summary>
    /// When the query upcasts its elements (to object, an interface, a base class), the query narrowed to their actual
    /// type: it runs in its place, and its elements are converted to T (see ElementUpcast).
    /// </summary>
    private readonly Lazy<IQueryable?> _upcastSource;

    internal PolarsQuery(PolarsQueryProvider provider, Expression? expression)
    {
        _provider = provider;
        _source = expression;
        _materializer = provider.Materializer;
        _expression = expression ?? Expression.Constant(this, typeof(IQueryable<T>));
        _upcastSource = new Lazy<IQueryable?>(() =>
            _source is not null
            && ElementUpcast.TryStrip(_source) is { } inner
            && ClrTypeHelper.TryGetEnumerableElementType(inner.Type) is { } rowType
            && _targetType.IsAssignableFrom(rowType)
                ? ((IQueryProvider)_provider).CreateQuery(inner)
                : null);
    }

    [RequiresDynamicCode(AotMessages.LinqProvider)]
    [RequiresUnreferencedCode(AotMessages.LinqProvider)]
    public PolarsQuery(LazyFrameHandle lazyFrameHandle, IDataFrameMaterializer materializer, QueryLanguage language, Expression expression)
        : this(new PolarsQueryProvider(lazyFrameHandle, materializer, language), expression)
    {
    }

    [RequiresDynamicCode(AotMessages.LinqProvider)]
    [RequiresUnreferencedCode(AotMessages.LinqProvider)]
    public PolarsQuery(LazyFrameHandle lazyFrameHandle, IDataFrameMaterializer materializer, QueryLanguage language)
        : this(new PolarsQueryProvider(lazyFrameHandle, materializer, language), null)
    {
    }

    /// <summary>A query over any table source the backend lowers (for instance a Delta table read by the managed kernel).</summary>
    [RequiresDynamicCode(AotMessages.LinqProvider)]
    [RequiresUnreferencedCode(AotMessages.LinqProvider)]
    internal PolarsQuery(ILogicalTableSource source, IDataFrameMaterializer materializer, QueryLanguage language)
        : this(new PolarsQueryProvider(new Lazy<ILogicalTableSource>(source), materializer, language), null)
    {
    }

    private Expression EffectiveExpression() =>
        _provider.Language == QueryLanguage.FSharp ? QueryCompiler.RewriteFSharpQuery(_expression) : _expression;

    /// <summary>Compiles the pipeline to optimized IR (see QueryCompiler.Compile).</summary>
    private CompiledPipeline CompilePipeline(bool mergeDiagnostics) =>
        QueryCompiler.Compile(PolarsQueryBackend.Instance, _provider.RootSource, _targetType, EffectiveExpression(), mergeDiagnostics);

    /// <summary>The compiled pipeline with its join scopes packed for materializing this query's elements.</summary>
    private (CompiledPipeline Pipeline, CompiledLogicalQuery Compiled) CompileForMaterialization()
    {
        var pipeline = CompilePipeline(false);
        return (pipeline, RowMaterialization.ForMaterialization(_targetType, pipeline.Stages, pipeline.Compiled));
    }

    /// <summary>Compiles pipeline into a pure symbolic CompiledLogicalQuery IR.</summary>
    internal CompiledLogicalQuery CompileLogicalQuery() => CompilePipeline(false).Compiled;

    /// <summary>
    /// For a pipeline that needs full LINQ-to-Objects fallback, finds the longest natively compilable prefix (see
    /// InMemoryExecution.TryFindNativePrefix).
    /// </summary>
    internal (MethodCallExpression Consumer, Expression Prefix)? TryFindNativePrefix(Expression effectiveExpr) =>
        InMemoryExecution.TryFindNativePrefix(_provider, effectiveExpr);

    /// <summary>
    /// Attempts to extract a pure symbolic LogicalPlan (with its source bindings) for grafting into another query,
    /// merging its diagnostics into the outer ones and never lowering to native handles.
    /// </summary>
    internal (LogicalPlan Plan, SourceBinding[] Sources)? TryGetPureLogicalPlan()
    {
        var pipeline = CompilePipeline(true);
        var compiled = pipeline.Compiled;
        var schema = IrOps.InferSchema(compiled.Plan);
        bool canUsePurePlan =
            ClientResidualOps.isNative(compiled.Residual)
            && !(_isScalar && schema.Fields.Length > 1)
            && !_isGrouping
            && !pipeline.Stages.Any(QueryCompiler.IsPostCollect);
        return canUsePurePlan ? (compiled.Plan, compiled.Sources) : null;
    }

    /// <summary>
    /// Compiles the pipeline into the native LazyFrameHandle, the managed residual and the normalized stages, enforcing
    /// the client evaluation policy.
    /// </summary>
    private (LazyFrameHandle NativeLf, ClientResidual Residual, FSharpList<LinqStage> Stages) GetCompiledPlan()
    {
        var (pipeline, compiled) = CompileForMaterialization();
        var nativeLf = PolarsPhysicalLowerer.LowerPlan(compiled.Sources, compiled.Plan);
        // The query's text only goes into a warning or an exception; rendering a large query costs.
        QueryCompiler.EnforcePipelinePolicy(
            PolarsQueryBackend.Instance, "QueryExecution", compiled.Residual.IsLinqToObjects, compiled.Residual, () => _expression.ToString());
        return (nativeLf, compiled.Residual, pipeline.Stages);
    }

    /// <summary>
    /// Translates and lowers the query, leaving collection and materialization to the returned PreparedQuery. For
    /// benchmarks; only natively executable queries can be prepared.
    /// </summary>
    internal PreparedQuery<T> Prepare()
    {
        if (_upcastSource.Value is not null) throw new InvalidOperationException("Prepare does not support queries that upcast their elements.");
        var (nativeLf, residual, stages) = GetCompiledPlan();
        if (residual.IsLinqToObjects)
        {
            nativeLf.Dispose();
            throw new InvalidOperationException("Prepare needs a natively executable query; this one falls back to LINQ-to-Objects.");
        }
        return new PreparedQuery<T>(nativeLf, residual, stages, _materializer);
    }

    /// <summary>Compiles pipeline and builds the target LogicalPlan IR tree.</summary>
    public LogicalPlan GetLogicalPlan() => CompileLogicalQuery().Plan;

    /// <summary>Explains the intermediate representation (IR) LogicalPlan tree.</summary>
    public string ExplainLogicalPlan() => IrOps.Format(GetLogicalPlan());

    private bool CanCollectNativeDirectly(LazyFrameHandle nativeLf, ClientResidual residual)
    {
        var width = PolarsWrapper.GetSchemaLen(PolarsWrapper.GetLazySchema(nativeLf));
        return ClientResidualOps.isNative(residual) && !(_isScalar && width > 1UL) && !_isGrouping;
    }

    /// <summary>Evaluates the complete pipeline, executing native plan first, then materializing.</summary>
    private IEnumerable<T> ExecuteQuery()
    {
        if (_upcastSource.Value is { } rows) return ((IEnumerable)rows).Cast<T>();
        var (nativeLf, residual, stages) = GetCompiledPlan();
        return residual.IsLinqToObjects
            ? InMemoryExecution.Execute<T>(_provider, EffectiveExpression())
            : RowMaterialization.Materialize<T>(_materializer, stages, residual, RowMaterialization.Collect(nativeLf, stages));
    }

    /// <summary>Compiles pipeline to LazyFrameHandle. If client fallback, predicates or projections exist, materializes and transposes back.</summary>
    public LazyFrameHandle CompileToLazyFrameHandle()
    {
        if (_upcastSource.Value is { } rows) return ((IPolarsPlanSource)rows).GetCompiledLazyFrameHandle();
        var (nativeLf, residual, _) = GetCompiledPlan();
        return CanCollectNativeDirectly(nativeLf, residual)
            ? nativeLf
            : PolarsWrapper.DataFrameToLazy(DataFrameBuilder.FromRows(ExecuteQuery()));
    }

    /// <summary>Compiles the query and materializes directly into a native DataFrameHandle.</summary>
    public DataFrameHandle CompileToDataFrameHandle()
    {
        if (_upcastSource.Value is { } rows)
            return (DataFrameHandle)rows.GetType().GetMethod(nameof(CompileToDataFrameHandle))!.Invoke(rows, [])!;
        var (nativeLf, residual, stages) = GetCompiledPlan();
        if (stages.Any(s => s is LinqStage.Chunk))
            throw new InvalidOperationException(
                "Cannot materialize a chunked sequence (IEnumerable<T[]>) into a single flat DataFrame. "
                + "If you want to iterate over DataFrames in batches, use 'ToBatchesAsync(chunkSize)' or 'CollectBatchesStream'. "
                + "If you want in-memory array chunks, enumerate the query directly via ToList() / foreach.");
        return CanCollectNativeDirectly(nativeLf, residual)
            ? RowMaterialization.Collect(nativeLf, stages)
            : DataFrameBuilder.FromRows(ExecuteQuery());
    }

    Tuple<CompiledLogicalQuery, bool, FSharpOption<Tuple<Type, Func<object, object>>>> IInternalPlanSource.GetLogicalPlanInfo()
    {
        // No policy check here: sources needing client evaluation are enumerated by the scalar planner, and enumeration
        // (GetCompiledPlan) enforces the pipeline policy; for pure native sources it is a no-op.
        var compiled = CompileLogicalQuery();
        return ClientResidualOps.tryProjectionOnly(compiled.Residual).OrNull() is { } projection
            ? Tuple.Create(compiled, false, FSharpOption<Tuple<Type, Func<object, object>>>.Some(projection))
            : Tuple.Create(compiled, !ClientResidualOps.isNative(compiled.Residual), (FSharpOption<Tuple<Type, Func<object, object>>>)null!);
    }

    public IEnumerator<T> GetEnumerator() => ExecuteQuery().GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public Type ElementType => _targetType;

    public Expression Expression => _expression;

    public IQueryProvider Provider => _provider;

    LazyFrameHandle IPolarsPlanSource.GetCompiledLazyFrameHandle() => CompileToLazyFrameHandle();

    public PlanInspection GetPlanInspection(bool optimized)
    {
        var (pipeline, compiled) = CompileForMaterialization();
        var nativeLf = PolarsPhysicalLowerer.LowerPlan(compiled.Sources, compiled.Plan);
        string? fallbackBoundary = compiled.Residual.IsLinqToObjects ? TryFindNativePrefix(EffectiveExpression())?.Consumer.Method.Name : null;
        return PlanInspectionReport.Build(pipeline, compiled, PolarsWrapper.Explain(nativeLf, optimized), _isGrouping, fallbackBoundary);
    }

    PlanInspection IQueryExplainable.InspectPlan(bool optimized) =>
        _upcastSource.Value is { } rows ? ((IQueryExplainable)rows).InspectPlan(optimized) : GetPlanInspection(optimized);

    string IQueryExplainable.Explain(bool optimized) => GetPlanInspection(optimized).FormatReport();

    string IQueryExplainable.ExplainLogicalPlan() => ExplainLogicalPlan();

    LogicalPlan IQueryExplainable.GetLogicalPlan() => GetLogicalPlan();

    CompiledLogicalQuery IInternalQueryExplainable.CompileLogicalQuery() => CompileLogicalQuery();

    FSharpOption<Tuple<LogicalPlan, SourceBinding[]>> IInternalQueryExplainable.TryGetPureLogicalPlan() =>
        TryGetPureLogicalPlan() is { } pure ? FSharpOption<Tuple<LogicalPlan, SourceBinding[]>>.Some(Tuple.Create(pure.Plan, pure.Sources)) : null!;

    bool IInternalQueryExplainable.IsNativelyExecutable() => !CompileForMaterialization().Compiled.Residual.IsLinqToObjects;

    ILogicalTableSource IInternalQueryExplainable.RootTable => _provider.RootSource;

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var (nativeLf, residual, stages) = GetCompiledPlan();
        // Residual row operations are row-local, so they can be applied batch by batch.
        bool canStreamBatches =
            _upcastSource.Value is null
            && !residual.IsLinqToObjects
            && !_isGrouping
            && !stages.Any(QueryCompiler.IsPostCollect);
        return canStreamBatches
            ? AsyncQueryEnumerator.OfBatchStream(cancellationToken, nativeLf, df => RowMaterialization.MaterializeFlatRows<T>(_materializer, df, residual))
            : AsyncQueryEnumerator.OfDeferredRows(cancellationToken, ExecuteQuery);
    }
}
