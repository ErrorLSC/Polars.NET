namespace Polars.NET.Linq

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open Polars.NET.Core
open Polars.NET.Core.Wrappers
open Polars.NET.Core.Enums
open Polars.NET.Core.Helpers
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
open Polars.NET.Linq.IR
open Polars.NET.Linq.Backend.Polars

/// The language whose compiler built a query's expression trees. F# query expressions are rewritten
/// (quotations, curried closures, AnonymousObject tuples, nested SelectMany chains) before translation;
/// C# expression trees are translated as they are.
type QueryLanguage =
    | CSharp = 0
    | FSharp = 1

/// Strongly-typed IQueryProvider backed by Polars.NET.Core
type internal PolarsQueryProvider(initialLazyFrame: LazyFrameHandle, materializer: IDataFrameMaterializer, language: QueryLanguage) =
    // The root frame's schema is read once per provider. Lowering clones the handle, so the binding never hands it out.
    let rootSource = lazy (PolarsPhysicalLowerer.tableSourceOfLazyFrame initialLazyFrame)

    /// Root table source, bound to slot 0 of every query this provider compiles.
    member _.RootSource : ILogicalTableSource = rootSource.Value
    member _.Materializer = materializer
    /// Shared by every query derived from this provider's root.
    member _.Language = language

    interface IQueryProvider with
        member this.CreateQuery(expression: Expression) : IQueryable =
            let elemType =
                match PolarsTypeHelper.TryGetEnumerableElementType expression.Type with
                | null -> typedefof<obj>
                | t -> t

            let queryType = typedefof<PolarsQuery<_>>.MakeGenericType elemType
            Activator.CreateInstance(
                queryType,
                Reflection.BindingFlags.Instance ||| Reflection.BindingFlags.NonPublic ||| Reflection.BindingFlags.Public,
                null,
                [| box this; box (Some expression) |],
                null
            )
            :?> IQueryable

        member this.CreateQuery<'TElement>(expression: Expression) : IQueryable<'TElement> =
            PolarsQuery<'TElement>(this, Some expression) :> IQueryable<'TElement>

        member this.Execute(expression: Expression) : obj =
            let executeMethod = LinqReflectionCache.GetQueryProviderExecuteMethod expression.Type
            executeMethod.Invoke(this, [| expression |])

        member this.Execute<'TResult>(expression: Expression) : 'TResult =
            let targetType = typeof<'TResult>

            match expression with
            | MethodCall(m, _, _) when ScalarPlanner.isScalarReductionMethod m ->
                ScalarPlanner.plan<'TResult> PolarsQueryBackend.Instance (this :> IQueryProvider) expression
                |> PolarsScalarRunner.execute materializer
            | _ when PolarsTypeHelper.IsScalarType targetType ->
                ScalarPlanner.plan<'TResult> PolarsQueryBackend.Instance (this :> IQueryProvider) expression
                |> PolarsScalarRunner.execute materializer
            | _ ->
                let query = (this :> IQueryProvider).CreateQuery expression

                match box query with
                | :? 'TResult as matched -> matched
                | _ ->
                    let elemType =
                        match PolarsTypeHelper.TryGetEnumerableElementType targetType with
                        | null -> typeof<obj>
                        | t -> t

                    let toListMethod =
                        LinqReflectionCache.GetEnumerableMethod("ToList", 1, [| elemType |])

                    let listObj = toListMethod.Invoke(null, [| box query |])

                    if targetType.IsArray then
                        let toArrayMethod =
                            LinqReflectionCache.GetEnumerableMethod("ToArray", 1, [| elemType |])

                        toArrayMethod.Invoke(null, [| box query |]) :?> 'TResult
                    elif targetType.IsAssignableFrom(listObj.GetType()) then
                        listObj :?> 'TResult
                    else
                        failwithf "Cannot convert query sequence to requested terminal type '%s'" targetType.FullName

and PolarsQuery<'T>
    internal (provider: PolarsQueryProvider, exprOpt: Expression option) as this
    =
    let materializer = provider.Materializer
    let targetType = typeof<'T>
    let isScalar = PolarsTypeHelper.IsScalarType targetType
    let isGrouping = isGroupingType targetType

    let expression =
        match exprOpt with
        | Some e -> e
        | None -> Expression.Constant(this, typeof<IQueryable<'T>>) :> Expression

    let getEffectiveExpr () =
        if provider.Language = QueryLanguage.FSharp && FSharpAst.needsRewrite expression then
            FSharpAst.rewrite expression
        else
            expression

    new(lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer, language: QueryLanguage, expression: Expression) =
        PolarsQuery<'T>(PolarsQueryProvider(lazyFrameHandle, materializer, language), Some expression)

    new(lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer, language: QueryLanguage) =
        PolarsQuery<'T>(PolarsQueryProvider(lazyFrameHandle, materializer, language), None)

    /// Compiles the pipeline to optimized IR (see PipelineCompilation.compile).
    member private _.CompilePipeline(mergeDiagnostics: bool) : CompiledPipeline =
        PipelineCompilation.compile provider.RootSource targetType (getEffectiveExpr ()) mergeDiagnostics

    /// The compiled pipeline with its join scopes packed for materializing this query's elements.
    member private this.CompileForMaterialization() : CompiledPipeline * CompiledLogicalQuery =
        let pipeline = this.CompilePipeline false
        pipeline, RowMaterialization.forMaterialization targetType pipeline.Stages pipeline.Compiled

    /// Compiles pipeline into a pure symbolic CompiledLogicalQuery IR.
    member internal this.CompileLogicalQuery() : CompiledLogicalQuery =
        (this.CompilePipeline false).Compiled

    /// For a pipeline that needs full LINQ-to-Objects fallback, finds the longest natively compilable prefix
    /// (see InMemoryExecution.tryFindNativePrefix).
    member internal _.TryFindNativePrefix(effectiveExpr: Expression) : (MethodCallExpression * Expression) option =
        InMemoryExecution.tryFindNativePrefix provider effectiveExpr

    /// Attempts to extract a pure symbolic LogicalPlan (with its source bindings) for grafting into another
    /// query, merging its diagnostics into the outer ones and never lowering to native handles.
    member internal this.TryGetPureLogicalPlan() : (LogicalPlan * SourceBinding array) option =
        let pipeline = this.CompilePipeline true
        let compiled = pipeline.Compiled
        let stages = pipeline.Stages
        let optimizedPlan = compiled.Plan
        let schema = SchemaInference.inferPlanSchema optimizedPlan
        let canUsePurePlan =
            ClientResidualOps.isNative compiled.Residual
            && not (isScalar && schema.Fields.Length > 1)
            && not isGrouping
            && not (stages |> List.exists LinqStageTraits.isPostCollect)
        if canUsePurePlan then
            Some(optimizedPlan, compiled.Sources)
        else
            None

    /// Compiles the pipeline into the native LazyFrameHandle, the managed residual and the normalized stages,
    /// enforcing the client evaluation policy.
    member private this.GetCompiledPlan() : LazyFrameHandle * ClientResidual * LinqStage list =
        let pipeline, compiled = this.CompileForMaterialization()
        let nativeLf = PolarsPhysicalLowerer.lowerPlan compiled.Sources compiled.Plan

        enforcePipelinePolicy
            "QueryExecution"
            (compiled.Residual = ClientResidual.LinqToObjects)
            (ClientResidualOps.filters compiled.Residual)
            (expression.ToString())

        nativeLf, compiled.Residual, pipeline.Stages

    /// Compiles pipeline and builds the target LogicalPlan IR tree.
    member this.GetLogicalPlan() : LogicalPlan =
        (this.CompileLogicalQuery()).Plan

    /// Explains the intermediate representation (IR) LogicalPlan tree.
    member this.ExplainLogicalPlan() : string =
        LogicalPlanOps.formatPlan (this.GetLogicalPlan())

    member private _.CanCollectNativeDirectly(nativeLf: LazyFrameHandle, residual: ClientResidual) : bool =
        let schema = PolarsWrapper.GetLazySchema nativeLf
        let width = PolarsWrapper.GetSchemaLen schema

        ClientResidualOps.isNative residual
        && not (isScalar && width > 1UL)
        && not isGrouping

    /// Evaluates the complete pipeline, executing native plan first, then materializing
    member private this.ExecuteQuery() : IEnumerable<'T> =
        let nativeLf, residual, stages = this.GetCompiledPlan()

        if residual = ClientResidual.LinqToObjects then
            InMemoryExecution.execute<'T> (provider :> IQueryProvider) (getEffectiveExpr ())
        else
            RowMaterialization.collect nativeLf stages
            |> RowMaterialization.materialize<'T> materializer stages residual

    /// Compiles pipeline to LazyFrameHandle. If client fallback, predicates or projections exist, materializes and transposes back.
    member this.CompileToLazyFrameHandle() : LazyFrameHandle =
        let nativeLf, residual, _ = this.GetCompiledPlan()

        if this.CanCollectNativeDirectly(nativeLf, residual) then
            nativeLf
        else
            let filteredRows = this.ExecuteQuery()
            let newDfHandle = DataFrameBuilder.FromRows<'T> filteredRows
            PolarsWrapper.DataFrameToLazy newDfHandle

    /// Compiles the query and materializes directly into a native DataFrameHandle
    member this.CompileToDataFrameHandle() : DataFrameHandle =
        let nativeLf, residual, stages = this.GetCompiledPlan()

        if stages |> List.exists (function LinqStage.Chunk _ -> true | _ -> false) then
            raise (
                InvalidOperationException(
                    "Cannot materialize a chunked sequence (IEnumerable<T[]>) into a single flat DataFrame. "
                    + "If you want to iterate over DataFrames in batches, use 'ToBatchesAsync(chunkSize)' or 'CollectBatchesStream'. "
                    + "If you want in-memory array chunks, enumerate the query directly via ToList() / foreach."
                )
            )

        if this.CanCollectNativeDirectly(nativeLf, residual) then
            RowMaterialization.collect nativeLf stages
        else
            let rows = this.ExecuteQuery()
            DataFrameBuilder.FromRows<'T>(rows)

    interface IInternalPlanSource with
        member this.GetLogicalPlanInfo() =
            // No policy check here: sources needing client evaluation are enumerated by the scalar planner,
            // and enumeration (GetCompiledPlan) enforces the pipeline policy; for pure native sources it is a no-op.
            let compiled = this.CompileLogicalQuery()

            match ClientResidualOps.tryProjectionOnly compiled.Residual with
            | Some projection -> compiled, false, Some projection
            | None -> compiled, not (ClientResidualOps.isNative compiled.Residual), None

    interface IEnumerable<'T> with
        member this.GetEnumerator() : IEnumerator<'T> = (this.ExecuteQuery()).GetEnumerator()

    interface IEnumerable with
        member this.GetEnumerator() : IEnumerator =
            (this :> IEnumerable<'T>).GetEnumerator() :> IEnumerator

    interface IQueryable with
        member _.ElementType = targetType
        member _.Expression = expression
        member _.Provider = provider :> IQueryProvider

    interface IQueryable<'T>

    interface IOrderedQueryable<'T>

    interface IPolarsPlanSource with
        member this.GetCompiledLazyFrameHandle() = this.CompileToLazyFrameHandle()

    member this.GetPlanInspection(optimized: bool) : PlanInspection =
        let pipeline, compiled = this.CompileForMaterialization()
        let nativeLf = PolarsPhysicalLowerer.lowerPlan compiled.Sources compiled.Plan

        let fallbackBoundary =
            if compiled.Residual = ClientResidual.LinqToObjects then
                this.TryFindNativePrefix(getEffectiveExpr ()) |> Option.map (fun (consumer, _) -> consumer.Method.Name)
            else
                None

        PlanInspectionReport.build pipeline compiled (PolarsWrapper.Explain(nativeLf, optimized)) isGrouping fallbackBoundary

    interface IPolarsExplainable with
        member this.InspectPlan(optimized: bool) = this.GetPlanInspection optimized

        member this.Explain(optimized: bool) =
            (this.GetPlanInspection optimized).FormatReport()

        member this.ExplainLogicalPlan() = this.ExplainLogicalPlan()

        member this.GetLogicalPlan() = this.GetLogicalPlan()

    interface IPolarsInternalExplainable with
        member this.CompileLogicalQuery() = this.CompileLogicalQuery()
        member this.TryGetPureLogicalPlan() = this.TryGetPureLogicalPlan()

        member this.IsNativelyExecutable() =
            (snd (this.CompileForMaterialization())).Residual <> ClientResidual.LinqToObjects

        member _.RootTable = provider.RootSource

    interface IAsyncEnumerable<'T> with
        member this.GetAsyncEnumerator(cancellationToken: CancellationToken) : IAsyncEnumerator<'T> =
            cancellationToken.ThrowIfCancellationRequested()

            let nativeLf, residual, stages = this.GetCompiledPlan()

            // Residual row operations are row-local, so they can be applied batch by batch.
            let canStreamBatches =
                residual <> ClientResidual.LinqToObjects
                && not isGrouping
                && not (stages |> List.exists LinqStageTraits.isPostCollect)

            if canStreamBatches then
                AsyncQueryEnumerator.ofBatchStream<'T> cancellationToken nativeLf (fun df ->
                    RowMaterialization.materializeFlatRows<'T> materializer df residual)
            else
                AsyncQueryEnumerator.ofDeferredRows<'T> cancellationToken this.ExecuteQuery
