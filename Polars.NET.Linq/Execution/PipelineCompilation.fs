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
    let compile
        (root: ILogicalTableSource)
        (targetType: Type)
        (effectiveExpr: Expression)
        (mergeDiagnostics: bool)
        : CompiledPipeline =
        let isGrouping = isGroupingType targetType
        let extraction = PartialEvaluator.extract effectiveExpr
        let rootBinding = SourceBinding.Table root

        let probe = QueryPlanCache.TryCreateProbe(targetType, extraction.NormalizedExpression, extraction.Parameters)

        let pipeline =
            match probe |> Option.bind QueryPlanCache.TryGetBound with
            | Some cached -> cached
            | None ->
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
                        Compiled = { compiled with Plan = LogicalOptimizer.optimize (EnumResolution.resolvePlan compiled.Plan) }
                        Stages = stages
                        FallbackDiagnostic = scope.FallbackReason
                        Udfs = scope.Udfs
                    }

                probe |> Option.iter (fun p -> QueryPlanCache.Offer(p, pipeline, isGrouping))
                pipeline

        if mergeDiagnostics then
            CompilationScope.TryCurrent
            |> Option.iter (fun outer -> outer.Absorb(pipeline.FallbackDiagnostic, pipeline.Udfs))

        pipeline
