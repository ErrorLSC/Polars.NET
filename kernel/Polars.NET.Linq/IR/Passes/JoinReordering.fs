namespace Polars.NET.Linq.IR.Passes

open System.Collections.Generic
open Polars.NET.Linq
open Polars.NET.Linq.IR

/// Chooses the order of inner equi-joins whose output order nothing observes.
///
/// A query's joins run in the order they were written, which is usually the order of a SQL FROM clause: a fact table
/// can meet an unfiltered dimension long before the dimension that removes most of its rows. This pass:
/// 1. Flattens a tree of such joins, through row-local filters and plain column projections, into relations, equality
///    edges between them and residual predicates. Anything else (outer, semi and anti joins, groupings, shared
///    subplans, ...) is one opaque relation, whose own inside is reordered separately.
/// 2. Builds a left-deep order greedily: from each starting relation it repeatedly adds the connected relation giving
///    the smallest estimated result, and keeps the order whose intermediate results sum lowest. Starting from the
///    single smallest join would pair two small dimensions and then have to meet the fact table with nothing to
///    reduce it. Estimates come from source row counts
///    (ISourceStatistics) and fixed filter selectivities, treating the smaller side of an edge as its unique side.
///    With no statistics every source counts the same, leaving the filters to decide.
/// 3. Places each residual predicate at the first join that has all its columns, and restores the original columns.
///
/// The order is only changed when it differs from the written one. A query that observes join order keeps it: IR
/// joins are ordered, so only joins reported by GroupOrderRelaxation are candidates.
module internal JoinReordering =

    /// The flattened query cannot be expressed as relations and edges.
    exception private NotFlattenable

    let private defaultSourceRows = 1_000_000.0

    /// The fraction of rows a predicate is assumed to keep.
    let rec private selectivity (predicate: ScalarExpr) : float =
        let isLiteral (e: ScalarExpr) =
            match e with
            | ScalarExpr.Literal _
            | ScalarExpr.Cast(ScalarExpr.Literal _, _, _) -> true
            | _ -> false
        match predicate with
        | ScalarExpr.Binary(BinaryOp.And, l, r) -> selectivity l * selectivity r
        | ScalarExpr.Binary(BinaryOp.Or, l, r) ->
            let a, b = selectivity l, selectivity r
            min 1.0 (a + b - a * b)
        | ScalarExpr.Binary((BinaryOp.Eq | BinaryOp.EqMissing), l, r) when isLiteral l || isLiteral r -> 0.1
        | ScalarExpr.Binary((BinaryOp.Lt | BinaryOp.LtEq | BinaryOp.Gt | BinaryOp.GtEq), l, r) when isLiteral l || isLiteral r -> 0.33
        | ScalarExpr.Binary((BinaryOp.Like | BinaryOp.Contains | BinaryOp.StartsWith | BinaryOp.EndsWith | BinaryOp.RegexMatch), _, _)
        | ScalarExpr.InList(_, _, false, _) -> 0.25
        | ScalarExpr.Unary(UnaryOp.IsNotNull, _) -> 0.9
        | ScalarExpr.Unary(UnaryOp.IsNull, _) -> 0.1
        | ScalarExpr.Unary(UnaryOp.Not, inner) -> 1.0 - selectivity inner
        | _ -> 0.5

    /// (estimated rows, rows of the table beneath before its filters).
    let rec estimate (sourceRows: int -> float) (plan: LogicalPlan) : float * float =
        let recurse = estimate sourceRows
        match plan with
        | LogicalPlan.Scan(LogicalPlanSource.Slot index, _) ->
            let rows = sourceRows index
            rows, rows
        | LogicalPlan.Scan _ -> defaultSourceRows, defaultSourceRows
        | LogicalPlan.Empty _ -> 1.0, 1.0
        | LogicalPlan.Filter(input, predicate) ->
            let rows, tableRows = recurse input
            max 1.0 (rows * selectivity predicate), tableRows
        | LogicalPlan.Limit(input, _, count) ->
            let rows, tableRows = recurse input
            min rows (max 1.0 (float count)), tableRows
        | LogicalPlan.Distinct(input, _, _, _) ->
            let rows, tableRows = recurse input
            max 1.0 (rows * 0.5), tableRows
        | LogicalPlan.GroupBy(input, _, _, _, _)
        | LogicalPlan.GroupingSets(input, _) ->
            let rows, tableRows = recurse input
            max 1.0 (rows * 0.1), tableRows
        | LogicalPlan.Join(left, right, _, _, how, _, _, _) ->
            let leftRows, leftTable = recurse left
            let rightRows, rightTable = recurse right
            match how with
            | JoinKind.Semi
            | JoinKind.Anti -> max 1.0 (leftRows * 0.5), leftTable
            | JoinKind.Left -> leftRows, leftTable
            | JoinKind.Cross -> leftRows * rightRows, leftTable
            | _ -> max 1.0 (leftRows * rightRows / max 1.0 (min leftTable rightTable)), max leftTable rightTable
        | LogicalPlan.Union(left, right, _) ->
            let leftRows, leftTable = recurse left
            let rightRows, rightTable = recurse right
            leftRows + rightRows, leftTable + rightTable
        | other ->
            match LogicalPlanOps.getInputs other with
            | [] -> defaultSourceRows, defaultSourceRows
            | input :: _ -> recurse input

    let private isRowLocal (expr: ScalarExpr) =
        not (
            ScalarExprOps.containsAggregation expr
            || ScalarExprOps.containsWindow expr
            || ScalarExprOps.containsSubquery expr
            || ScalarExprOps.dependsOnOtherRows expr
        )

    /// A column of a relation, as written in flattened expressions.
    let private columnToken (relation: int) (column: string) = $"{relation}:{column}"

    let private tokenRelation (token: string) = int (token.Substring(0, token.IndexOf ':'))
    let private tokenColumn (token: string) = token.Substring(token.IndexOf ':' + 1)

    let private relationsOf (expr: ScalarExpr) =
        ScalarExprOps.collectColumns expr |> Set.map tokenRelation

    type private Edge =
        {
            A: int
            B: int
            KeyA: ScalarExpr
            KeyB: ScalarExpr
        }

    /// The row count of a source slot, read from its statistics once.
    let sourceRowCounts (sources: SourceBinding array) : int -> float =
        let knownRows = Dictionary<int, float>()
        fun (slot: int) ->
            match knownRows.TryGetValue slot with
            | true, rows -> rows
            | _ ->
                let rows =
                    if slot < 0 || slot >= sources.Length then
                        defaultSourceRows
                    else
                        match sources.[slot] with
                        | SourceBinding.InMemory(rows, _) -> float rows.Length
                        | SourceBinding.Table table ->
                            match table with
                            | :? ISourceStatistics as statistics ->
                                statistics.EstimatedRows |> Option.map float |> Option.defaultValue defaultSourceRows
                            | _ -> defaultSourceRows
                knownRows.[slot] <- rows
                rows

    let apply (sources: SourceBinding array) (unorderedJoins: HashSet<LogicalPlan>) (plan: LogicalPlan) : LogicalPlan =
        let sourceRows = sourceRowCounts sources

        let isFlattenableJoin (node: LogicalPlan) =
            match node with
            | LogicalPlan.Join(_, _, _, _, JoinKind.Inner, _, _, false) -> unorderedJoins.Contains node
            | _ -> false

        let isPlainColumn (expr: ScalarExpr) =
            match expr with
            | ScalarExpr.Column _ -> true
            | _ -> false

        /// Whether the node, under its filters and plain projections, is a flattenable join.
        let rec reachesJoin (node: LogicalPlan) =
            match node with
            | LogicalPlan.Filter(input, predicate) when isRowLocal predicate -> reachesJoin input
            | LogicalPlan.Project(input, projections) when projections |> List.forall (snd >> isPlainColumn) -> reachesJoin input
            | other -> isFlattenableJoin other

        let rec go (node: LogicalPlan) : LogicalPlan =
            if isFlattenableJoin node then
                match tryReorder node with
                | Some reordered -> reordered
                | None -> LogicalPlanOps.mapInputs go node
            else
                LogicalPlanOps.mapInputs go node

        and tryReorder (root: LogicalPlan) : LogicalPlan option =
            let relations = ResizeArray<LogicalPlan>()
            let edges = ResizeArray<Edge>()
            let residuals = ResizeArray<ScalarExpr>()

            /// The columns the node outputs, as (name, token).
            let rec flatten (node: LogicalPlan) : (string * string) list =
                let qualify (scope: (string * string) list) (expr: ScalarExpr) =
                    let names = Map.ofList scope
                    ScalarExprOps.substitute
                        (fun column ->
                            match names.TryFind column with
                            | Some token -> Some(ScalarExpr.Column token)
                            | None -> raise NotFlattenable)
                        expr

                match node with
                | LogicalPlan.Join(left, right, leftOn, rightOn, _, suffix, coalesce, _) when isFlattenableJoin node ->
                    let leftScope = flatten left
                    let rightScope = flatten right
                    for leftKey, rightKey in List.zip leftOn rightOn do
                        let a, b = qualify leftScope leftKey, qualify rightScope rightKey
                        match Set.toList (relationsOf a), Set.toList (relationsOf b) with
                        | [ ra ], [ rb ] -> edges.Add { A = ra; B = rb; KeyA = a; KeyB = b }
                        | _ -> raise NotFlattenable

                    let dropped =
                        match coalesce with
                        | JoinKeyCoalescing.KeepBoth -> Set.empty
                        | _ ->
                            rightOn
                            |> List.choose (function
                                | ScalarExpr.Column name -> Some name
                                | _ -> None)
                            |> Set.ofList

                    let leftNames = leftScope |> List.map fst |> Set.ofList
                    let rightSuffix = defaultArg suffix LinqConstants.DefaultRightSuffix
                    leftScope
                    @ (rightScope
                       |> List.filter (fun (name, _) -> not (dropped.Contains name))
                       |> List.map (fun (name, token) ->
                           (if leftNames.Contains name then name + rightSuffix else name), token))

                | LogicalPlan.Filter(input, predicate) when isRowLocal predicate && reachesJoin input ->
                    let scope = flatten input
                    residuals.Add(qualify scope predicate)
                    scope

                | LogicalPlan.Project(input, projections) when
                    projections |> List.forall (snd >> isPlainColumn) && reachesJoin input
                    ->
                    let scope = flatten input |> Map.ofList
                    projections
                    |> List.map (fun (name, expr) ->
                        match expr with
                        | ScalarExpr.Column source when scope.ContainsKey source -> name, scope.[source]
                        | _ -> raise NotFlattenable)

                | leaf ->
                    let index = relations.Count
                    relations.Add leaf
                    (SchemaInference.inferPlanSchema leaf).ColumnNames
                    |> List.map (fun column -> column, columnToken index column)

            try
                let outputScope = flatten root
                let n = relations.Count
                if n < 3 then
                    None
                else
                    let measures = relations |> Seq.map (estimate sourceRows) |> Array.ofSeq
                    let rows i = fst measures.[i]
                    let tableRows i = snd measures.[i]

                    /// The estimated rows of joining `current` rows with relation `i` through its edges to `joined`.
                    let joinedRows (current: float) (joined: HashSet<int>) (i: int) =
                        let denominators =
                            edges
                            |> Seq.choose (fun e ->
                                if e.A = i && joined.Contains e.B then Some(min (tableRows i) (tableRows e.B))
                                elif e.B = i && joined.Contains e.A then Some(min (tableRows i) (tableRows e.A))
                                else None)
                            |> List.ofSeq
                        match denominators with
                        | [] -> None
                        | _ -> Some(max 1.0 (current * rows i / max 1.0 (List.max denominators)))

                    /// The greedy order from one starting relation with its cost, the sum of its intermediate
                    /// results; None when some relation cannot be reached.
                    let greedyFrom (start: int) =
                        let joined = HashSet<int>([ start ])
                        let order = ResizeArray [ start ]
                        let mutable current = rows start
                        let mutable cost = 0.0
                        let mutable connected = true
                        while connected && joined.Count < n do
                            let candidates =
                                [ for i in 0 .. n - 1 do
                                      if not (joined.Contains i) then
                                          match joinedRows current joined i with
                                          | Some result -> yield result, rows i, i
                                          | None -> () ]
                            match candidates with
                            | [] -> connected <- false
                            | _ ->
                                let result, _, next = List.min candidates
                                order.Add next
                                joined.Add next |> ignore
                                current <- result
                                cost <- cost + result
                        if connected then Some(cost, start, List.ofSeq order) else None

                    let best = [ 0 .. n - 1 ] |> List.choose greedyFrom

                    // The larger relation of the first pair goes on the left.
                    let chosen =
                        match best with
                        | [] -> None
                        | _ ->
                            match List.min best with
                            | _, _, a :: b :: rest when rows b > rows a -> Some(b :: a :: rest)
                            | _, _, order -> Some order

                    match chosen with
                    | None -> None
                    | Some order when order = [ 0 .. n - 1 ] -> None
                    | Some order ->
                        // A column name used by several relations gets a name of its own, per relation.
                        let usage =
                            relations
                            |> Seq.collect (fun r -> (SchemaInference.inferPlanSchema r).ColumnNames)
                            |> Seq.countBy id
                            |> Map.ofSeq
                        let columnName (relation: int) (column: string) =
                            if usage.[column] > 1 then $"__jr{relation}_{column}" else column
                        let finalName (token: string) = columnName (tokenRelation token) (tokenColumn token)
                        let finalize (expr: ScalarExpr) =
                            ScalarExprOps.substitute (fun token -> Some(ScalarExpr.Column(finalName token))) expr

                        let relationPlan (i: int) =
                            let body = go relations.[i]
                            let renames =
                                (SchemaInference.inferPlanSchema relations.[i]).ColumnNames
                                |> List.filter (fun column -> columnName i column <> column)
                            if renames.IsEmpty then
                                body
                            else
                                LogicalPlan.Rename(body, renames, renames |> List.map (columnName i))

                        let pending = List<ScalarExpr>(residuals)
                        let placed = HashSet<int>()
                        let applyResiduals (input: LogicalPlan) =
                            let ready = pending |> Seq.filter (fun p -> (relationsOf p) |> Set.forall placed.Contains) |> List.ofSeq
                            for p in ready do
                                pending.Remove p |> ignore
                            ready |> List.fold (fun acc p -> LogicalPlan.Filter(acc, finalize p)) input

                        placed.Add order.Head |> ignore
                        let mutable built = applyResiduals (relationPlan order.Head)
                        for next in order.Tail do
                            let leftKeys, rightKeys =
                                edges
                                |> Seq.choose (fun e ->
                                    if e.A = next && placed.Contains e.B then Some(finalize e.KeyB, finalize e.KeyA)
                                    elif e.B = next && placed.Contains e.A then Some(finalize e.KeyA, finalize e.KeyB)
                                    else None)
                                |> List.ofSeq
                                |> List.unzip
                            built <-
                                LogicalPlan.Join(built, relationPlan next, leftKeys, rightKeys, JoinKind.Inner, None, JoinKeyCoalescing.KeepBoth, false)
                            placed.Add next |> ignore
                            built <- applyResiduals built

                        Some(LogicalPlan.Project(built, outputScope |> List.map (fun (name, token) -> name, ScalarExpr.Column(finalName token))))
            with NotFlattenable ->
                None

        go plan
