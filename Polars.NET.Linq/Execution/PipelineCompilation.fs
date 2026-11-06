namespace Polars.NET.Linq

open System
open System.Linq.Expressions
open Polars.NET.Linq.IR
open Polars.NET.Linq.Backend.Polars

/// Compiling a query's expression into an optimized, possibly cached, pipeline.
module internal PipelineCompilation =

    /// Compiles the pipeline to optimized IR, reusing the cached compilation of an identical query
    /// (same expression shape, parameter values, root columns and configuration) when one exists.
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

        let keyOpt =
            QueryPlanCache.TryCreateKey(targetType, extraction.NormalizedExpression, extraction.Parameters, root.Schema.ColumnNames)

        let pipeline =
            match keyOpt |> Option.bind QueryPlanCache.TryGet with
            | Some cached ->
                { cached.Pipeline with Compiled = { cached.Pipeline.Compiled with Sources = [| rootBinding |] } }
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
                        Compiled = { compiled with Plan = LogicalOptimizer.optimize compiled.Plan }
                        Stages = stages
                        FallbackDiagnostic = scope.FallbackReason
                        Udfs = scope.Udfs
                    }

                // Only root-only pipelines are cached: any other source is bound from this query's values.
                match keyOpt with
                | Some key when compiled.Sources.Length = 1 ->
                    QueryPlanCache.Admit
                        {
                            Key = key
                            Pipeline = { pipeline with Compiled = { pipeline.Compiled with Sources = [||] } }
                            LastAccessTick = 0L
                        }
                | _ -> ()

                pipeline

        if mergeDiagnostics then
            CompilationScope.TryCurrent
            |> Option.iter (fun outer -> outer.Absorb(pipeline.FallbackDiagnostic, pipeline.Udfs))

        pipeline
