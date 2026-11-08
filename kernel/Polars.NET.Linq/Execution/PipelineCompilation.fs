namespace Polars.NET.Linq

open System
open System.Linq.Expressions
open Polars.NET.Linq.IR
open Polars.NET.Linq.Backend.Polars

/// Compiling a query's expression into an optimized, possibly cached, pipeline.
module internal PipelineCompilation =

    /// Compiles the pipeline to optimized IR, reusing the cached compilation of an identical query
    /// (same expression shape, parameter values, root tables' schemas and configuration) when one exists,
    /// bound to this query's own roots.
    /// `effectiveExpr` is the query's expression after any language-specific rewrite; `targetType` is the
    /// query's element type. The pipeline carries its own fallback and UDF diagnostics. With
    /// `mergeDiagnostics` (this query is grafted into an outer one being compiled), the outer compilation
    /// also takes them.
    /// Inlines a composed query that roots the operator chain as a constant (`Constant(o.Where(..)).Select(..)`,
    /// built by re-rooting a captured subquery or by hand) as its own expression, repeatedly. Its operators are
    /// then compiled with the chain; the stage parser stops at a constant and would read only the root table.
    let rec private inlineComposedRoot (e: Expression) : Expression =
        match e with
        | :? ConstantExpression as c ->
            match c.Value with
            | :? IPolarsInternalExplainable as q ->
                match (q :?> System.Linq.IQueryable).Expression with
                | :? ConstantExpression as own when obj.ReferenceEquals(own.Value, q) -> e
                | composed when c.Type.IsAssignableFrom composed.Type -> inlineComposedRoot composed
                | composed -> Expression.Convert(inlineComposedRoot composed, c.Type) :> Expression
            | _ -> e
        | :? MethodCallExpression as mc when mc.Arguments.Count > 0 && isNull mc.Object ->
            let source = inlineComposedRoot mc.Arguments.[0]
            if obj.ReferenceEquals(source, mc.Arguments.[0]) then e
            else mc.Update(null, Seq.append [ source ] (Seq.skip 1 mc.Arguments)) :> Expression
        | _ -> e

    let compile
        (root: ILogicalTableSource)
        (targetType: Type)
        (effectiveExpr: Expression)
        (mergeDiagnostics: bool)
        : CompiledPipeline =
        let effectiveExpr = inlineComposedRoot effectiveExpr
        use _schemas = PlanSchemaCache.Enter()
        let isGrouping = isGroupingType targetType
        let rootBinding = SourceBinding.Table root

        // The cache key covers composed query constants (query values embedded in the expression, such as a
        // subquery) by their full expression; compilation itself keeps resolving them as values
        // (see QueryPlanCache.InlineComposedQueries).
        // With the cache disabled no key is computed.
        let keyed =
            if QueryPlanCache.Enabled then
                let keyExpr = QueryPlanCache.InlineComposedQueries effectiveExpr
                let keyExtraction = PartialEvaluator.extract keyExpr
                Some(keyExpr, keyExtraction)
            else
                None

        let probe =
            keyed
            |> Option.bind (fun (_, keyExtraction) ->
                QueryPlanCache.TryCreateProbe(targetType, keyExtraction.NormalizedExpression, keyExtraction.Parameters))

        let pipeline =
            match probe |> Option.bind QueryPlanCache.TryGetBound with
            | Some cached -> cached
            | None ->
                let extraction =
                    match keyed with
                    | Some(keyExpr, keyExtraction) when obj.ReferenceEquals(keyExpr, effectiveExpr) -> keyExtraction
                    | _ -> PartialEvaluator.extract effectiveExpr

                let bound =
                    if extraction.Parameters.Count = 0 then
                        extraction.NormalizedExpression
                    else
                        PartialEvaluator.substituteExtracted extraction.NormalizedExpression extraction.Parameters

                let stages = flattenStages bound []
                use scope = CompilationScope.Enter PolarsQueryBackend.Instance
                let compiled = TreeBuilder.buildCompiledQuery rootBinding root.Schema isGrouping stages

                let pipeline =
                    {
                        Compiled =
                            { compiled with
                                Plan =
                                    EnumResolution.resolvePlan compiled.Plan
                                    |> Passes.CommonSubplanSharing.apply compiled.Sources
                                    |> (if mergeDiagnostics then LogicalOptimizer.optimize else LogicalOptimizer.optimizeComplete compiled.Sources)
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
