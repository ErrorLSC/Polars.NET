namespace Kaminari.Samidare

open System
open Kaminari.Samidare.IR
open Kaminari.Samidare.IR.Passes
open System.Linq.Expressions

/// The IR's operations for C# callers: schema and type inference, formatting, JSON types. Part of Samidare's facade:
/// public for other assemblies, with no compatibility promise; signatures shaped for C# (no F# options, functions or
/// sets; nulls for absent values).
[<AbstractClass; Sealed>]
type IrOps =
    /// The expression as text (diagnostics, error messages).
    static member Format(expr: ScalarExpr) : string = LogicalPlanOps.formatScalar expr

    /// The plan as an indented tree of text (diagnostics, explain output).
    static member Format(plan: LogicalPlan) : string = LogicalPlanOps.formatPlan plan

    /// The columns (names and types) a plan yields.
    static member InferSchema(plan: LogicalPlan) : LogicalSchema = SchemaInference.inferPlanSchema plan

    /// The type of an expression over `schema` (Unknown where the IR cannot tell).
    static member InferType(schema: LogicalSchema, expr: ScalarExpr) : LogicalType = SchemaInference.inferScalarType schema expr

    /// The JSON decoding schema of a .NET type, or null when it has none.
    static member TryInferJsonType(t: Type) : JsonDataType =
        match JsonDataTypeInference.tryInfer t with
        | Some json -> json
        | None -> Unchecked.defaultof<_>

/// The optimizer's passes and entry points for C# callers (part of Samidare's facade; see IrOps).
[<AbstractClass; Sealed>]
type PlanOptimizer =
    /// Enum conversions resolved in a plan: every IR plan goes through this before a backend lowers it.
    static member ResolveEnums(plan: LogicalPlan) : LogicalPlan = EnumResolution.resolvePlan plan

    /// Enum conversions resolved in an expression over `schema` (aggregations taken per group).
    static member ResolveEnums(schema: LogicalSchema, expr: ScalarExpr) : ScalarExpr = EnumResolution.resolveScalar schema expr

    /// Enum conversions resolved in a value assigned to a column of `columnType` (an enum constant takes the column's
    /// representation).
    static member ResolveEnumAssignment(schema: LogicalSchema, columnType: LogicalType, expr: ScalarExpr) : ScalarExpr =
        EnumResolution.resolveAssignment schema columnType expr

    /// The plan a scalar terminal collects, prepared for a backend: enum conversions resolved in `backend`'s compilation
    /// scope (a checked sum asserts by its capabilities), the plan's rows freed from an order its terminal does not read
    /// and, with column statistics on, the joins that frees reordered.
    static member PrepareScalarPlan(backend: IQueryBackend, sources: SourceBinding array, plan: LogicalPlan) : LogicalPlan =
        use _scope = CompilationScope.Enter backend
        let resolved = EnumResolution.resolvePlan plan |> RaisingGuards.ofPlan
        if Kaminari.Samidare.Config.SamidareConfig.CollectColumnStatistics then
            // The freed joins can be reordered too, now that nothing observes their order.
            let relaxed, unorderedJoins = GroupOrderRelaxation.applyCollectingUnorderedJoins false true resolved
            JoinReordering.apply sources unorderedJoins relaxed
        else
            GroupOrderRelaxation.apply resolved

/// The LINQ frontend for C# backends: compiling a query's expression into an optimized pipeline, translating a lone
/// expression (part of Samidare's facade; see IrOps).
[<AbstractClass; Sealed>]
type QueryCompiler =
    /// Inlines a composed query that roots the operator chain as a constant (`Constant(o.Where(..)).Select(..)`, built by
    /// re-rooting a captured subquery or by hand) as its own expression, repeatedly. Its operators are then compiled with
    /// the chain; the stage parser stops at a constant and would read only the root table.
    static member private InlineComposedRoot(e: Expression) : Expression =
        match e with
        | :? ConstantExpression as c ->
            match c.Value with
            | :? IInternalQueryExplainable as q ->
                match (q :?> System.Linq.IQueryable).Expression with
                | :? ConstantExpression as own when obj.ReferenceEquals(own.Value, q) -> e
                | composed when c.Type.IsAssignableFrom composed.Type -> QueryCompiler.InlineComposedRoot composed
                | composed -> Expression.Convert(QueryCompiler.InlineComposedRoot composed, c.Type) :> Expression
            | _ -> e
        | :? MethodCallExpression as mc when mc.Arguments.Count > 0 && isNull mc.Object ->
            let source = QueryCompiler.InlineComposedRoot mc.Arguments.[0]
            if obj.ReferenceEquals(source, mc.Arguments.[0]) then e
            else mc.Update(null, Seq.append [ source ] (Seq.skip 1 mc.Arguments)) :> Expression
        | _ -> e

    /// Compiles a query's pipeline to optimized IR for `backend`, reusing the cached compilation of an identical query
    /// (same expression shape, parameter values, root tables' schemas and configuration) when one exists, bound to this
    /// query's own roots. `effectiveExpr` is the query's expression after any language-specific rewrite; `targetType` is
    /// the query's element type. The pipeline carries its own fallback and UDF diagnostics. With `mergeDiagnostics`
    /// (this query is grafted into an outer one being compiled), the outer compilation also takes them.
    static member Compile
        (backend: IQueryBackend, root: ILogicalTableSource, targetType: Type, effectiveExpr: Expression, mergeDiagnostics: bool)
        : CompiledPipeline =
        let effectiveExpr = QueryCompiler.InlineComposedRoot effectiveExpr
        use _schemas = PlanSchemaCache.Enter()
        let isGrouping = isGroupingType targetType
        let rootBinding = SourceBinding.Table root

        // The cache key covers composed query constants (query values embedded in the expression, such as a subquery)
        // by their full expression; compilation itself keeps resolving them as values (see
        // QueryPlanCache.InlineComposedQueries). With the cache disabled no key is computed.
        let keyed =
            if QueryPlanCache.Enabled then
                let keyExpr = QueryPlanCache.InlineComposedQueries effectiveExpr
                let keyExtraction = PartialEvaluator.extract keyExpr
                Some(keyExpr, keyExtraction)
            else
                None

        // A subquery's sequence reading the enclosing query's row compiles for that row (its scope numbers and
        // columns): it is not cached.
        let readsOuterRows = CompilationScope.TryCurrent |> Option.exists (fun s -> s.ReadsOuterRows)
        let probe =
            keyed
            |> Option.filter (fun _ -> not readsOuterRows)
            |> Option.bind (fun (_, keyExtraction) ->
                QueryPlanCache.TryCreateProbe(targetType, keyExtraction.NormalizedExpression, keyExtraction.Parameters, mergeDiagnostics))

        let pipeline =
            match probe |> Option.bind QueryPlanCache.TryGetBound with
            | Some cached -> cached
            | None ->
                let extraction =
                    match keyed with
                    | Some(keyExpr, keyExtraction) when obj.ReferenceEquals(keyExpr, effectiveExpr) -> keyExtraction
                    | _ -> PartialEvaluator.extract effectiveExpr
                let bound =
                    if extraction.Parameters.Count = 0 then extraction.NormalizedExpression
                    else PartialEvaluator.substituteExtracted extraction.NormalizedExpression extraction.Parameters
                let stages = flattenStages bound []
                use scope = CompilationScope.Enter backend
                let compiled = TreeBuilder.buildCompiledQuery rootBinding root.Schema isGrouping stages
                let pipeline =
                    {
                        Compiled =
                            { compiled with
                                Plan =
                                    // Short-circuit evaluation for the nodes that may fail the query, before the
                                    // optimizer reorders anything (SEMANTICS.md, Guards).
                                    let resolved = EnumResolution.resolvePlan compiled.Plan |> RaisingGuards.ofPlan
                                    // A grafted plan is optimized with the query it joins: optimized on its own, it would
                                    // no longer equal another reference to the same query in that query.
                                    if mergeDiagnostics then
                                        resolved
                                    else
                                        resolved
                                        |> CommonSubplanSharing.apply scope.Backend compiled.Sources
                                        |> LogicalOptimizer.optimizeComplete
                                            Kaminari.Samidare.Config.SamidareConfig.AllowUnstableOrdering
                                            Kaminari.Samidare.Config.SamidareConfig.CollectColumnStatistics
                                            compiled.Sources
                            }
                        Stages = stages
                        FallbackDiagnostic = scope.FallbackReason
                        Udfs = scope.Udfs
                        EvaluatedAtCompile = scope.EvaluatedAtCompile
                    }
                probe |> Option.iter (fun p -> QueryPlanCache.Offer(p, pipeline, isGrouping))
                pipeline

        if mergeDiagnostics then
            CompilationScope.TryCurrent
            |> Option.iter (fun outer -> outer.Absorb(pipeline.FallbackDiagnostic, pipeline.Udfs, pipeline.EvaluatedAtCompile))
        pipeline

    /// A lone LINQ expression (outside a query pipeline: a merge condition, a key selector) as an IR expression over
    /// `schema` (the columns it reads), its enum conversions resolved and its raising nodes guarded (RaisingGuards), all
    /// in `backend`'s compilation scope; null when it does not translate.
    static member TryTranslateScalar(backend: IQueryBackend, context: TranslationContext, schema: LogicalSchema, expr: Expression) : ScalarExpr =
        use _scope = CompilationScope.Enter backend
        match tryTranslateToScalar context expr with
        | Some translated -> EnumResolution.resolveScalar schema translated |> RaisingGuards.ofExpr
        | None -> Unchecked.defaultof<_>

    /// Like TryTranslateScalar, for an expression whose value is stored into a column of `columnType` (an enum constant
    /// takes the column's representation).
    static member TryTranslateAssignment
        (backend: IQueryBackend, context: TranslationContext, schema: LogicalSchema, columnType: LogicalType, expr: Expression)
        : ScalarExpr =
        use _scope = CompilationScope.Enter backend
        match tryTranslateToScalar context expr with
        | Some translated -> EnumResolution.resolveAssignment schema columnType translated |> RaisingGuards.ofExpr
        | None -> Unchecked.defaultof<_>

    /// The value of an expression that reads no query data (a captured variable, a closure), evaluated now.
    static member TryEvaluate(expr: Expression, [<Runtime.InteropServices.Out>] value: byref<obj>) : bool =
        match PartialEvaluator.tryEvaluate expr with
        | Some v ->
            value <- v
            true
        | None -> false

    /// The seed, folder and finisher of a numeric LINQ fold (Sum, Average, Min, Max over materialized rows): the
    /// finisher takes the folded accumulator and the row count.
    static member CreateNumericFolder(sourceType: Type, accumType: Type, methodName: string, selector: Delegate)
        : struct (obj * Delegate * Func<obj, int64, obj>) =
        let seed, folder, finish = LinqReflectionCache.CreateNumericFolder(sourceType, accumType, methodName, Option.ofObj selector)
        struct (seed, folder, Func<obj, int64, obj>(fun acc rows -> finish acc rows))

    /// The constant queryable a query's operator chain starts from, and its type; false when there is none.
    static member TryFindRootQueryable(expr: Expression, [<Runtime.InteropServices.Out>] root: byref<Expression>, [<Runtime.InteropServices.Out>] rootType: byref<Type>) : bool =
        match tryFindRootQueryable expr with
        | Some(r, t) ->
            root <- r
            rootType <- t
            true
        | None -> false

    /// Whether a query's element type is a group (IGrouping, or what a GroupJoin yields).
    static member IsGroupingType(t: Type) : bool = isGroupingType t

    /// The plan with the entities of the join scopes still in effect over its rows packed into struct columns, so the
    /// rows materialize as `rowType`.
    static member PackRow(rowShape: RowShape, plan: LogicalPlan, rowType: Type) : RowPacking = EntityPacking.packRow rowShape plan rowType

    /// Enforces the client evaluation policy on a compiled query (`stage` names the step for messages): a full
    /// LINQ-to-Objects fallback or client predicates warn or throw as SamidareConfig.ClientEvaluationPolicy says.
    /// `expressionText` is called only for a message.
    static member EnforcePipelinePolicy
        (backend: IQueryBackend, stage: string, requiresClientFallback: bool, residual: ClientResidual, expressionText: Func<string>)
        : unit =
        enforcePipelinePolicy backend stage requiresClientFallback (ClientResidualOps.filters residual) (lazy (expressionText.Invoke()))

    /// A scalar LINQ terminal (Count, Any, First, Sum, ...) planned for `backend`: what to run and how its outcome
    /// becomes the result.
    static member PlanScalar<'TResult>(backend: IQueryBackend, provider: System.Linq.IQueryProvider, expr: Expression) : PlannedScalar<'TResult> =
        ScalarPlanner.plan<'TResult> backend provider expr

    /// Whether a method is a scalar LINQ terminal the scalar planner plans.
    static member IsScalarReductionMethod(m: Reflection.MethodInfo) : bool = ScalarPlanner.isScalarReductionMethod m

    /// An F# query expression's tree rewritten for translation (quotations, curried closures, AnonymousObject tuples,
    /// nested SelectMany chains); any other tree as it is.
    static member RewriteFSharpQuery(expr: Expression) : Expression =
        if FSharpAst.needsRewrite expr then FSharpAst.rewrite expr else expr

    /// Whether a stage is applied after the native plan is collected (shuffle, chunk, ...), so that the plan's rows are not
    /// yet the query's.
    static member IsPostCollect(stage: LinqStage) : bool = LinqStageTraits.isPostCollect stage

    /// The expression with its closure variables and local computations (everything that reads no query data)
    /// evaluated into constants.
    static member EvaluateCapturedValues(expr: Expression) : Expression = PartialEvaluator.eval expr

    /// The expression under its quotes and conversions.
    static member Unwrap(expr: Expression) : Expression = ExpressionPatterns.unwrap expr

/// The SQL frontend for C# backends (part of Samidare's facade; see IrOps).
[<AbstractClass; Sealed>]
type SqlFrontend =
    /// The optimized IR plan of a SQL query over the catalog's tables for `backend`, and the sources its Scan slots
    /// read: the SQL frontend's plan optimized as a LINQ query's is.
    static member Plan(catalog: Kaminari.Samidare.Sql.ISqlCatalog, backend: IQueryBackend, text: string) : struct (LogicalPlan * SourceBinding array) =
        // Each pass infers schemas of the nodes it meets: once per node for the whole translation, as a LINQ compile does.
        use _schemas = PlanSchemaCache.Enter()
        let sql = Kaminari.Samidare.Sql.SqlQueries.translate catalog backend.SupportsAssertions text
        // The frontend shares a common table expression's plan among its references; their filters move down to it
        // first (a shared plan is a barrier to pushdown), constant ones drop the branches of unions they empty, and the
        // shared plan is filtered by what its references read. The rest of the optimization comes after.
        let optimized =
            EnumResolution.resolvePlan sql.Plan
            |> RaisingGuards.ofPlan
            |> FilterNullSimplification.apply
            |> PredicatePushdown.apply
            |> ConstantFilterFolding.apply
            |> SharedConsumerFilters.apply
            |> LogicalOptimizer.optimizeComplete
                Kaminari.Samidare.Config.SamidareConfig.AllowUnstableOrdering
                Kaminari.Samidare.Config.SamidareConfig.CollectColumnStatistics
                sql.Sources
        struct (optimized, sql.Sources)

    /// Schemas inferred once per plan node until the returned scope is disposed (wrap a lowering that infers them).
    static member EnterSchemaCache() : IDisposable = PlanSchemaCache.Enter()
