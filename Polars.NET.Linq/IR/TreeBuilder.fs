namespace Polars.NET.Linq.IR

open System
open System.Linq.Expressions
open Polars.NET.Core
open Polars.NET.Linq

/// Constructs LogicalPlan relational trees from LINQ query stages or QueryOp streams.
module internal TreeBuilder =

    /// Extracts projection bindings (name * ScalarExpr) from a projection LambdaExpression.
    let private extractProjections (ctx: TranslationContext) (body: Expression) : (string * ScalarExpr) list option =
        match ExpressionPatterns.unwrap body with
        | :? MemberInitExpression as mie ->
            let bindings =
                mie.Bindings
                |> Seq.choose (function
                    | :? MemberAssignment as ma ->
                        ExprTranslator.tryTranslateToScalar ctx ma.Expression
                        |> Option.map (fun s -> ma.Member.Name, s)
                    | _ -> None)
                |> Seq.toList
            if bindings.Length = mie.Bindings.Count then Some bindings else None

        | :? NewExpression as ne ->
            let names =
                if not (isNull ne.Members) && ne.Members.Count = ne.Arguments.Count then
                    ne.Members |> Seq.map (fun m -> m.Name) |> Seq.toList
                elif not (isNull ne.Constructor) then
                    ne.Constructor.GetParameters() |> Seq.map (fun p -> p.Name) |> Seq.toList
                else
                    []

            let bindings =
                ne.Arguments
                |> Seq.mapi (fun i arg ->
                    let name = if i < names.Length then names.[i] else sprintf "col_%d" i
                    ExprTranslator.tryTranslateToScalar ctx arg |> Option.map (fun s -> name, s))
                |> Seq.toList

            if bindings |> List.forall Option.isSome then
                Some(bindings |> List.map Option.get)
            else
                None

        | other ->
            ExprTranslator.tryTranslateToScalar ctx other
            |> Option.map (fun s -> [ ("value", s) ])

    /// Constructs a LogicalPlan tree by applying a sequence of LinqStages to an initial LogicalPlan.
    let fromStages (initialPlan: LogicalPlan) (stages: LinqStage list) : LogicalPlan =
        stages
        |> List.fold (fun currentPlan stage ->
            match stage with
            | LinqStage.Filter lam ->
                match SubqueryDecorrelator.tryDecorrelateFilter currentPlan lam Map.empty with
                | Some (decorPlan, residualOpt) ->
                    match residualOpt with
                    | Some resLam ->
                        let p = resLam.Parameters.[0]
                        let ctx = TranslationContext.Entity p.Name
                        match ExprTranslator.tryTranslateToScalar ctx resLam.Body with
                        | Some pred -> LogicalPlan.Filter(decorPlan, pred)
                        | None -> decorPlan
                    | None -> decorPlan
                | None ->
                    let p = lam.Parameters.[0]
                    let ctx = TranslationContext.Entity p.Name
                    match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                    | Some pred -> LogicalPlan.Filter(currentPlan, pred)
                    | None -> currentPlan

            | LinqStage.Project lam ->
                match SubqueryDecorrelator.tryDecorrelateProjection currentPlan lam Map.empty with
                | Some (decorPlan, rewrittenLam) ->
                    let p = rewrittenLam.Parameters.[0]
                    let ctx = TranslationContext.Entity p.Name
                    match extractProjections ctx rewrittenLam.Body with
                    | Some projs -> LogicalPlan.Project(decorPlan, projs)
                    | None -> decorPlan
                | None ->
                    let p = lam.Parameters.[0]
                    let ctx = TranslationContext.Entity p.Name
                    match extractProjections ctx lam.Body with
                    | Some projs -> LogicalPlan.Project(currentPlan, projs)
                    | None -> currentPlan

            | LinqStage.Sort(lam, isDesc, nullsLast) ->
                let p = lam.Parameters.[0]
                let ctx = TranslationContext.Entity p.Name
                match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                | Some s ->
                    let newOrder = { SortOrder.Expr = s; Descending = isDesc; NullsLast = nullsLast }
                    match currentPlan with
                    | LogicalPlan.Sort(input, prevOrders) ->
                        LogicalPlan.Sort(input, prevOrders @ [ newOrder ])
                    | _ ->
                        LogicalPlan.Sort(currentPlan, [ newOrder ])
                | None -> currentPlan

            | LinqStage.SortDefault(isDesc, nullsLast) ->
                let newOrder = { SortOrder.Expr = ScalarExpr.Column ""; Descending = isDesc; NullsLast = nullsLast }
                match currentPlan with
                | LogicalPlan.Sort(input, prevOrders) ->
                    LogicalPlan.Sort(input, prevOrders @ [ newOrder ])
                | _ ->
                    LogicalPlan.Sort(currentPlan, [ newOrder ])

            | LinqStage.Take count ->
                LogicalPlan.Limit(currentPlan, 0L, count)

            | LinqStage.Skip count ->
                LogicalPlan.Limit(currentPlan, int64 count, UInt32.MaxValue)

            | LinqStage.SkipLast count ->
                LogicalPlan.SkipLast(currentPlan, count)

            | LinqStage.Distinct ->
                LogicalPlan.Distinct(currentPlan, None, PlUniqueKeepStrategy.First)

            | LinqStage.DistinctBy lam ->
                let p = lam.Parameters.[0]
                let ctx = TranslationContext.Entity p.Name
                match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                | Some (ScalarExpr.Column col) -> LogicalPlan.Distinct(currentPlan, Some [ col ], PlUniqueKeepStrategy.First)
                | _ -> currentPlan

            | LinqStage.Reverse ->
                LogicalPlan.Reverse currentPlan

            | LinqStage.Index ->
                LogicalPlan.WithRowIndex(currentPlan, "index", None)

            | LinqStage.CountBy lam ->
                let p = lam.Parameters.[0]
                let ctx = TranslationContext.Entity p.Name
                match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                | Some keyExpr -> LogicalPlan.GroupBy(currentPlan, [ keyExpr ], [ ("Count", ScalarExpr.Aggregation(AggKind.Len, None, false)) ], None, false)
                | None -> currentPlan

            | LinqStage.GroupByKey lam ->
                let p = lam.Parameters.[0]
                let ctx = TranslationContext.Entity p.Name
                match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                | Some keyExpr -> LogicalPlan.GroupBy(currentPlan, [ keyExpr ], [], None, false)
                | None -> currentPlan

            | LinqStage.GroupByWithResult(keyLam, resLam) ->
                let keyP = keyLam.Parameters.[0]
                let keyCtx = TranslationContext.Entity keyP.Name
                let keyExpr = ExprTranslator.tryTranslateToScalar keyCtx keyLam.Body |> Option.defaultValue (ScalarExpr.Column "Key")
                let resP = resLam.Parameters.[0]
                let resCtx = TranslationContext.Entity resP.Name
                match extractProjections resCtx resLam.Body with
                | Some aggs -> LogicalPlan.GroupBy(currentPlan, [ keyExpr ], aggs, None, false)
                | None -> currentPlan

            | LinqStage.Explode(colLam, _) ->
                let p = colLam.Parameters.[0]
                let ctx = TranslationContext.Entity p.Name
                match ColumnResolver.tryResolveColumnNameWithContext ctx colLam.Body with
                | Some col -> LogicalPlan.Explode(currentPlan, [ col ], false, false)
                | None -> currentPlan

            | LinqStage.Cast _
            | LinqStage.OfType _
            | LinqStage.DefaultIfEmpty _ ->
                currentPlan

            | _ ->
                currentPlan)
            initialPlan