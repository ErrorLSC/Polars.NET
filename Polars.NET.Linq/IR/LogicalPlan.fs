namespace Polars.NET.Linq.IR

open System
open System.Text
open Polars.NET.Core

/// Construction helpers, visitors, and graph visualization for LogicalPlan
module internal LogicalPlanOps =

    // =========================================================================
    // 1. Direct Child & Subplan Navigation
    // =========================================================================

    /// Extracts the immediate child inputs of a logical plan node.
    let getInputs (plan: LogicalPlan) : LogicalPlan list =
        match plan with
        | LogicalPlan.Scan _
        | LogicalPlan.Empty _ -> []
        | LogicalPlan.Filter(input, _)
        | LogicalPlan.Project(input, _)
        | LogicalPlan.WithColumns(input, _)
        | LogicalPlan.GroupBy(input, _, _, _, _)
        | LogicalPlan.Sort(input, _)
        | LogicalPlan.Limit(input, _, _)
        | LogicalPlan.Distinct(input, _, _)
        | LogicalPlan.Explode(input, _, _, _)
        | LogicalPlan.Unpivot(input, _, _, _, _)
        | LogicalPlan.WithRowIndex(input, _, _)
        | LogicalPlan.Rename(input, _, _)
        | LogicalPlan.Reverse input
        | LogicalPlan.SkipLast(input, _) -> [ input ]
        | LogicalPlan.HorizontalConcat inputs -> inputs
        | LogicalPlan.Join(left, right, _, _, _, _, _, _)
        | LogicalPlan.JoinAsOf(left, right, _)
        | LogicalPlan.Union(left, right, _) -> [ left; right ]

    /// Transforms immediate child subplans of a logical plan node.
    let mapInputs (f: LogicalPlan -> LogicalPlan) (plan: LogicalPlan) : LogicalPlan =
        match plan with
        | LogicalPlan.Scan _
        | LogicalPlan.Empty _ -> plan
        | LogicalPlan.Filter(input, pred) -> LogicalPlan.Filter(f input, pred)
        | LogicalPlan.Project(input, projs) -> LogicalPlan.Project(f input, projs)
        | LogicalPlan.WithColumns(input, cols) -> LogicalPlan.WithColumns(f input, cols)
        | LogicalPlan.Join(left, right, lOn, rOn, how, sfx, coal, nullEq) ->
            LogicalPlan.Join(f left, f right, lOn, rOn, how, sfx, coal, nullEq)
        | LogicalPlan.JoinAsOf(left, right, spec) -> LogicalPlan.JoinAsOf(f left, f right, spec)
        | LogicalPlan.GroupBy(input, keys, aggs, having, ord) ->
            LogicalPlan.GroupBy(f input, keys, aggs, having, ord)
        | LogicalPlan.Sort(input, orderings) -> LogicalPlan.Sort(f input, orderings)
        | LogicalPlan.Limit(input, off, cnt) -> LogicalPlan.Limit(f input, off, cnt)
        | LogicalPlan.Distinct(input, sub, keep) -> LogicalPlan.Distinct(f input, sub, keep)
        | LogicalPlan.Explode(input, cols, emAsNull, kpNulls) ->
            LogicalPlan.Explode(f input, cols, emAsNull, kpNulls)
        | LogicalPlan.Unpivot(input, idx, on, varN, valN) ->
            LogicalPlan.Unpivot(f input, idx, on, varN, valN)
        | LogicalPlan.Union(left, right, isAll) -> LogicalPlan.Union(f left, f right, isAll)
        | LogicalPlan.WithRowIndex(input, name, off) -> LogicalPlan.WithRowIndex(f input, name, off)
        | LogicalPlan.Rename(input, existing, newN) -> LogicalPlan.Rename(f input, existing, newN)
        | LogicalPlan.Reverse input -> LogicalPlan.Reverse(f input)
        | LogicalPlan.SkipLast(input, count) -> LogicalPlan.SkipLast(f input, count)
        | LogicalPlan.HorizontalConcat inputs -> LogicalPlan.HorizontalConcat(inputs |> List.map f)

    /// Transforms all scalar expressions embedded in the current logical plan node (non-recursively).
    let mapExpressions (f: ScalarExpr -> ScalarExpr) (plan: LogicalPlan) : LogicalPlan =
        match plan with
        | LogicalPlan.Scan _
        | LogicalPlan.Empty _ -> plan
        | LogicalPlan.Filter(input, pred) -> LogicalPlan.Filter(input, f pred)
        | LogicalPlan.Project(input, projs) ->
            LogicalPlan.Project(input, projs |> List.map (fun (name, e) -> name, f e))
        | LogicalPlan.WithColumns(input, cols) ->
            LogicalPlan.WithColumns(input, cols |> List.map (fun (name, e) -> name, f e))
        | LogicalPlan.Join(left, right, lOn, rOn, how, sfx, coal, nullEq) ->
            LogicalPlan.Join(left, right, lOn |> List.map f, rOn |> List.map f, how, sfx, coal, nullEq)
        | LogicalPlan.JoinAsOf(left, right, spec) ->
            LogicalPlan.JoinAsOf(
                left,
                right,
                { spec with
                    LeftOn = f spec.LeftOn
                    RightOn = f spec.RightOn
                    LeftBy = spec.LeftBy |> List.map f
                    RightBy = spec.RightBy |> List.map f
                }
            )
        | LogicalPlan.GroupBy(input, keys, aggs, having, ord) ->
            LogicalPlan.GroupBy(
                input,
                keys |> List.map f,
                aggs |> List.map (fun (name, e) -> name, f e),
                having |> Option.map f,
                ord
            )
        | LogicalPlan.Sort(input, orderings) ->
            LogicalPlan.Sort(
                input,
                orderings
                |> List.map (fun o ->
                    { o with
                        Expr = f o.Expr
                    })
            )
        | LogicalPlan.Limit _
        | LogicalPlan.Distinct _
        | LogicalPlan.Explode _
        | LogicalPlan.Unpivot _
        | LogicalPlan.Union _
        | LogicalPlan.WithRowIndex _
        | LogicalPlan.Rename _
        | LogicalPlan.Reverse _
        | LogicalPlan.SkipLast _
        | LogicalPlan.HorizontalConcat _ -> plan

    // =========================================================================
    // 2. Tree Folding & Transformation
    // =========================================================================

    /// Post-order recursive traversal over the entire logical plan tree.
    let rec rewrite (rule: LogicalPlan -> LogicalPlan option) (plan: LogicalPlan) : LogicalPlan =
        let rewrittenChildren = mapInputs (rewrite rule) plan

        match rule rewrittenChildren with
        | Some newPlan -> newPlan
        | None -> rewrittenChildren

    /// Collects all column names required by operators at or above this plan node.
    let rec collectReferencedColumns (plan: LogicalPlan) : Set<string> =
        let currentCols =
            match plan with
            | LogicalPlan.Scan _
            | LogicalPlan.Empty _ -> Set.empty
            | LogicalPlan.Filter(_, pred) -> ScalarExprOps.collectColumns pred
            | LogicalPlan.Project(_, projs) ->
                projs |> List.map (snd >> ScalarExprOps.collectColumns) |> Set.unionMany
            | LogicalPlan.WithColumns(_, cols) ->
                cols |> List.map (snd >> ScalarExprOps.collectColumns) |> Set.unionMany
            | LogicalPlan.Join(_, _, lOn, rOn, _, _, _, _) ->
                let lCols = lOn |> List.map ScalarExprOps.collectColumns |> Set.unionMany
                let rCols = rOn |> List.map ScalarExprOps.collectColumns |> Set.unionMany
                Set.union lCols rCols
            | LogicalPlan.JoinAsOf(_, _, spec) ->
                let lCols = ScalarExprOps.collectColumns spec.LeftOn
                let rCols = ScalarExprOps.collectColumns spec.RightOn
                let lBy = spec.LeftBy |> List.map ScalarExprOps.collectColumns |> Set.unionMany
                let rBy = spec.RightBy |> List.map ScalarExprOps.collectColumns |> Set.unionMany
                Set.unionMany [ lCols; rCols; lBy; rBy ]
            | LogicalPlan.GroupBy(_, keys, aggs, having, _) ->
                let kCols = keys |> List.map ScalarExprOps.collectColumns |> Set.unionMany
                let aCols = aggs |> List.map (snd >> ScalarExprOps.collectColumns) |> Set.unionMany
                let hCols = having |> Option.map ScalarExprOps.collectColumns |> Option.defaultValue Set.empty
                Set.unionMany [ kCols; aCols; hCols ]
            | LogicalPlan.Sort(_, orderings) ->
                orderings |> List.map (fun o -> ScalarExprOps.collectColumns o.Expr) |> Set.unionMany
            | LogicalPlan.Distinct(_, subsetOpt, _) ->
                subsetOpt |> Option.map Set.ofList |> Option.defaultValue Set.empty
            | LogicalPlan.Explode(_, cols, _, _) -> Set.ofList cols
            | LogicalPlan.Unpivot(_, idx, onOpt, _, _) ->
                let idxCols = Set.ofList idx
                let onCols = onOpt |> Option.map Set.ofList |> Option.defaultValue Set.empty
                Set.union idxCols onCols
            | LogicalPlan.Rename(_, existing, _) -> Set.ofList existing
            | LogicalPlan.Limit _
            | LogicalPlan.Union _
            | LogicalPlan.WithRowIndex _
            | LogicalPlan.Reverse _
            | LogicalPlan.SkipLast _
            | LogicalPlan.HorizontalConcat _ -> Set.empty

        let childCols =
            getInputs plan |> List.map collectReferencedColumns |> Set.unionMany

        Set.union currentCols childCols

    // =========================================================================
    // 3. Plan Graph Formatter / Pretty Printer
    // =========================================================================

    let formatPlan (root: LogicalPlan) : string =
        let sb = StringBuilder()

        let rec formatNode (indent: string) (isLast: bool) (plan: LogicalPlan) =
            let marker = if isLast then "└── " else "├── "
            sb.Append(indent).Append(marker) |> ignore

            let nextIndent = indent + (if isLast then "    " else "│   ")

            match plan with
            | LogicalPlan.Scan(source, schema) ->
                let srcDesc =
                    match source with
                    | LogicalPlanSource.LazyFrame _ -> "LazyFrame"
                    | LogicalPlanSource.SubqueryRef id -> sprintf "SubqueryRef(%s)" id
                    | LogicalPlanSource.InMemoryTable(name, _) -> sprintf "InMemoryTable(%s)" name
                    | LogicalPlanSource.Empty -> "Empty"

                sb.AppendLine(sprintf "Scan: %s columns=[%s]" srcDesc (String.concat ", " schema.ColumnNames))
                |> ignore

            | LogicalPlan.Filter(input, pred) ->
                sb.AppendLine(sprintf "Filter: %A" pred) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Project(input, projs) ->
                let projStr =
                    projs |> List.map (fun (n, e) -> sprintf "%s=%A" n e) |> String.concat ", "

                sb.AppendLine(sprintf "Project: [%s]" projStr) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.WithColumns(input, cols) ->
                let colStr =
                    cols |> List.map (fun (n, e) -> sprintf "%s=%A" n e) |> String.concat ", "

                sb.AppendLine(sprintf "WithColumns: [%s]" colStr) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Join(left, right, lOn, rOn, how, sfx, _, _) ->
                sb.AppendLine(sprintf "Join: how=%A suffix=%A leftOn=%A rightOn=%A" how sfx lOn rOn) |> ignore
                formatNode nextIndent false left
                formatNode nextIndent true right

            | LogicalPlan.JoinAsOf(left, right, spec) ->
                sb.AppendLine(sprintf "JoinAsOf: strategy=%A leftOn=%A rightOn=%A" spec.Strategy spec.LeftOn spec.RightOn)
                |> ignore
                formatNode nextIndent false left
                formatNode nextIndent true right

            | LogicalPlan.GroupBy(input, keys, aggs, having, ord) ->
                let aggStr =
                    aggs |> List.map (fun (n, e) -> sprintf "%s=%A" n e) |> String.concat ", "

                sb.AppendLine(sprintf "GroupBy: keys=%A aggs=[%s] having=%A maintainOrder=%b" keys aggStr having ord)
                |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Sort(input, orderings) ->
                sb.AppendLine(sprintf "Sort: orderings=%A" orderings) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Limit(input, off, cnt) ->
                sb.AppendLine(sprintf "Limit: offset=%d count=%u" off cnt) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Distinct(input, subOpt, keep) ->
                sb.AppendLine(sprintf "Distinct: subset=%A keep=%A" subOpt keep) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Explode(input, cols, _, _) ->
                sb.AppendLine(sprintf "Explode: columns=%A" cols) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Unpivot(input, idx, onOpt, varN, valN) ->
                sb.AppendLine(sprintf "Unpivot: index=%A on=%A var=%s val=%s" idx onOpt varN valN) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Union(left, right, isAll) ->
                sb.AppendLine(sprintf "Union: isAll=%b" isAll) |> ignore
                formatNode nextIndent false left
                formatNode nextIndent true right

            | LogicalPlan.WithRowIndex(input, name, off) ->
                sb.AppendLine(sprintf "WithRowIndex: name=%s offset=%A" name off) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Rename(input, existing, newN) ->
                sb.AppendLine(sprintf "Rename: existing=%A new=%A" existing newN) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.Reverse input ->
                sb.AppendLine "Reverse" |> ignore
                formatNode nextIndent true input

            | LogicalPlan.SkipLast(input, count) ->
                sb.AppendLine(sprintf "SkipLast: count=%u" count) |> ignore
                formatNode nextIndent true input

            | LogicalPlan.HorizontalConcat inputs ->
                sb.AppendLine(sprintf "HorizontalConcat: inputs=%d" inputs.Length) |> ignore
                inputs |> List.iteri (fun i inp -> formatNode nextIndent (i = inputs.Length - 1) inp)

            | LogicalPlan.Empty schema ->
                sb.AppendLine(sprintf "Empty: columns=[%s]" (String.concat ", " schema.ColumnNames))
                |> ignore

        formatNode "" true root
        sb.ToString()

