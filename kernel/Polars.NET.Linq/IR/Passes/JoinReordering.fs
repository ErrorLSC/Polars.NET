namespace Polars.NET.Linq.IR.Passes

open System
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
///    reduce it. Estimates come from source row counts, column statistics when a source has them
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

    /// What the estimator reads about the plan's source slots.
    type SourceEstimates =
        {
            Rows: int -> float
            Column: int -> string -> ColumnStatistics option
            /// The (lowest, highest) values of a column, numbers and dates as doubles.
            KeyRange: int -> string -> (float * float) option
            /// The rows a subplan was seen to produce, when an engine has reported them.
            Observed: LogicalPlan -> float option
        }

    let private unixEpochDayNumber = DateOnly(1970, 1, 1).DayNumber

    /// A number or date as the double that statistics compare it by.
    let private literalNumber (value: LiteralValue) =
        match value with
        | LiteralValue.Int8 v -> Some(float v)
        | LiteralValue.Int16 v -> Some(float v)
        | LiteralValue.Int32 v -> Some(float v)
        | LiteralValue.Int64 v -> Some(float v)
        | LiteralValue.UInt8 v -> Some(float v)
        | LiteralValue.UInt16 v -> Some(float v)
        | LiteralValue.UInt32 v -> Some(float v)
        | LiteralValue.UInt64 v -> Some(float v)
        | LiteralValue.Float32 v -> Some(float v)
        | LiteralValue.Float64 v -> Some v
        | LiteralValue.Decimal v -> Some(float v)
        | LiteralValue.Date v -> Some(float (v.DayNumber - unixEpochDayNumber))
        | _ -> None

    let rec private literalOf (expr: ScalarExpr) =
        match expr with
        | ScalarExpr.Literal(LiteralValue.Null _) -> None
        | ScalarExpr.Literal value -> Some value
        | ScalarExpr.Cast(inner, _, _) -> literalOf inner
        | _ -> None

    let rec private columnOf (expr: ScalarExpr) =
        match expr with
        | ScalarExpr.Column name -> Some name
        | ScalarExpr.Cast(inner, _, _) -> columnOf inner
        | _ -> None

    let private sameValue (a: LiteralValue) (b: LiteralValue) =
        match literalNumber a, literalNumber b, a, b with
        | Some x, Some y, _, _ -> x = y
        | _, _, LiteralValue.String x, LiteralValue.String y -> x = y
        | _ -> false

    /// The share of rows equal to `value`, from the column's frequent values and its distinct count.
    let private equalityShare (statistics: ColumnStatistics) (value: LiteralValue) =
        match statistics.FrequentValues |> List.tryFind (fun (frequent, _) -> sameValue frequent value) with
        | Some(_, share) -> Some share
        | None ->
            statistics.DistinctCount
            |> Option.map (fun distinct ->
                let nonNull = 1.0 - defaultArg statistics.NullFraction 0.0
                let listed = statistics.FrequentValues |> List.sumBy snd
                max 0.0 (nonNull - listed) / max 1.0 (distinct - float statistics.FrequentValues.Length))

    [<RequireQualifiedAccess>]
    type private Bound =
        | Lower
        | Upper

    /// A comparison of a column with a literal number, as (column, side, value).
    let private tryBound (predicate: ScalarExpr) =
        let bound side column literal =
            match columnOf column, literalOf literal |> Option.bind literalNumber with
            | Some name, Some value -> Some(name, side, value)
            | _ -> None
        match predicate with
        | ScalarExpr.Binary((BinaryOp.Gt | BinaryOp.GtEq), left, right) ->
            bound Bound.Lower left right |> Option.orElseWith (fun () -> bound Bound.Upper right left)
        | ScalarExpr.Binary((BinaryOp.Lt | BinaryOp.LtEq), left, right) ->
            bound Bound.Upper left right |> Option.orElseWith (fun () -> bound Bound.Lower right left)
        | _ -> None

    /// The share of the histogram's values at or above the start of `value`'s position (`atStart`), or up to its end.
    let private histogramPosition (boundaries: float[]) (atStart: bool) (value: float) =
        let buckets = boundaries.Length - 1
        if value <= boundaries.[0] && atStart then
            0.0
        elif value >= boundaries.[buckets] && not atStart then
            1.0
        elif value < boundaries.[0] then
            0.0
        elif value > boundaries.[buckets] then
            1.0
        else
            let inBucket (i: int) = boundaries.[i] <= value && value <= boundaries.[i + 1]
            let index =
                if atStart then
                    Seq.find inBucket (seq { 0 .. buckets - 1 })
                else
                    Seq.find inBucket (seq { buckets - 1 .. -1 .. 0 })
            let width = boundaries.[index + 1] - boundaries.[index]
            let within = if width > 0.0 then (value - boundaries.[index]) / width elif atStart then 0.0 else 1.0
            (float index + within) / float buckets

    /// The fraction of rows kept by a conjunction of predicates, reading a column's statistics where it has them.
    let rec private conjunctionShare (column: string -> ColumnStatistics option) (predicates: ScalarExpr list) : float =
        let rec split (predicate: ScalarExpr) =
            match predicate with
            | ScalarExpr.Binary(BinaryOp.And, left, right) -> split left @ split right
            | other -> [ other ]
        let conjuncts = predicates |> List.collect split
        let spans (name: string) =
            column name
            |> Option.bind (fun statistics ->
                match statistics.Min |> Option.bind literalNumber, statistics.Max |> Option.bind literalNumber with
                | Some low, Some high -> Some(statistics, low, high)
                | _ -> None)
        let ranged = conjuncts |> List.choose tryBound |> List.filter (fun (name, _, _) -> (spans name).IsSome)
        let rangeShares =
            ranged
            |> List.groupBy (fun (name, _, _) -> name)
            |> List.map (fun (name, bounds) ->
                let statistics, low, high = (spans name).Value
                let lowest = bounds |> List.choose (fun (_, side, v) -> if side = Bound.Lower then Some v else None) |> List.fold max low
                let highest = bounds |> List.choose (fun (_, side, v) -> if side = Bound.Upper then Some v else None) |> List.fold min high
                let nonNull = 1.0 - defaultArg statistics.NullFraction 0.0
                match statistics.Histogram with
                | Some boundaries when boundaries.Length >= 2 ->
                    nonNull * max 0.0 (histogramPosition boundaries false highest - histogramPosition boundaries true lowest)
                | _ when high <= low ->
                    (if lowest <= highest then nonNull else 0.0)
                | _ -> nonNull * max 0.0 (min 1.0 ((highest - lowest) / (high - low))))
        let rangedColumns = ranged |> List.map (fun (name, _, _) -> name) |> Set.ofList
        let others =
            conjuncts
            |> List.filter (fun predicate ->
                match tryBound predicate with
                | Some(name, _, _) -> not (rangedColumns.Contains name)
                | None -> true)
        (rangeShares @ (others |> List.map (leafShare column))) |> List.fold (*) 1.0

    and private leafShare (column: string -> ColumnStatistics option) (predicate: ScalarExpr) : float =
        let equality target literal =
            match columnOf target, literalOf literal with
            | Some name, Some value -> column name |> Option.bind (fun statistics -> equalityShare statistics value)
            | _ -> None
        let known =
            match predicate with
            | ScalarExpr.Binary((BinaryOp.Eq | BinaryOp.EqMissing), left, right) ->
                equality left right |> Option.orElseWith (fun () -> equality right left)
            | ScalarExpr.Binary(BinaryOp.Or, left, right) ->
                let a, b = leafShare column left, leafShare column right
                Some(min 1.0 (a + b - a * b))
            | ScalarExpr.Binary(BinaryOp.And, _, _) -> Some(conjunctionShare column [ predicate ])
            | ScalarExpr.Unary(UnaryOp.Not, inner) -> Some(1.0 - leafShare column inner)
            | ScalarExpr.Unary(UnaryOp.IsNull, target) ->
                columnOf target |> Option.bind column |> Option.bind (fun statistics -> statistics.NullFraction)
            | ScalarExpr.Unary(UnaryOp.IsNotNull, target) ->
                columnOf target |> Option.bind column |> Option.bind (fun statistics -> statistics.NullFraction |> Option.map (fun nulls -> 1.0 - nulls))
            | ScalarExpr.InList(target, source, false, _) ->
                let values =
                    match source with
                    | InListSource.Literals literals -> literals
                    | InListSource.Collection(values, _) -> values |> Seq.cast<obj> |> Seq.choose LiteralValue.TryOfObject |> List.ofSeq
                columnOf target
                |> Option.bind column
                |> Option.bind (fun statistics ->
                    let shares = values |> List.map (equalityShare statistics)
                    if shares |> List.forall Option.isSome then Some(min 1.0 (shares |> List.sumBy Option.get)) else None)
            | _ -> None
        known |> Option.defaultWith (fun () -> selectivity predicate)

    /// The source slot a plan reads through filters and shared subplans.
    let rec private sourceSlot (plan: LogicalPlan) =
        match plan with
        | LogicalPlan.Scan(LogicalPlanSource.Slot index, _) -> Some index
        | LogicalPlan.Filter(input, _)
        | LogicalPlan.Shared(_, input) -> sourceSlot input
        | _ -> None

    /// (estimated rows, rows of the table beneath before its filters).
    let rec estimate (sources: SourceEstimates) (plan: LogicalPlan) : float * float =
        let rows, tableRows = estimateFromInputs sources plan
        match sources.Observed plan with
        | Some observed -> max 1.0 observed, tableRows
        | None -> rows, tableRows

    and private estimateFromInputs (sources: SourceEstimates) (plan: LogicalPlan) : float * float =
        let recurse = estimate sources
        match plan with
        | LogicalPlan.Scan(LogicalPlanSource.Slot index, _) ->
            let rows = sources.Rows index
            rows, rows
        | LogicalPlan.Scan _ -> defaultSourceRows, defaultSourceRows
        | LogicalPlan.Empty _ -> 1.0, 1.0
        | LogicalPlan.Filter _ ->
            let rec gather (node: LogicalPlan) (predicates: ScalarExpr list) =
                match node with
                | LogicalPlan.Filter(input, predicate) -> gather input (predicate :: predicates)
                | other -> other, predicates
            let input, predicates = gather plan []
            let rows, tableRows = recurse input
            let measuredColumn =
                match sourceSlot input with
                | Some slot when
                    predicates
                    |> List.exists (fun predicate -> ScalarExprOps.collectColumns predicate |> Seq.exists (fun column -> (sources.Column slot column).IsSome))
                    ->
                    Some(sources.Column slot)
                | _ -> None
            match measuredColumn with
            | Some column -> max 1.0 (rows * conjunctionShare column predicates), tableRows
            | None -> (predicates |> List.fold (fun kept predicate -> max 1.0 (kept * selectivity predicate)) rows), tableRows
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
    let sourceEstimates (sources: SourceBinding array) : SourceEstimates =
        let knownRows = Dictionary<int, float>()
        let knownColumns = Dictionary<struct (int * string), ColumnStatistics option>()
        let table slot =
            if slot < 0 || slot >= sources.Length then
                None
            else
                match sources.[slot] with
                | SourceBinding.Table table -> Some table
                | SourceBinding.InMemory _ -> None
        {
            Rows =
                fun slot ->
                    match knownRows.TryGetValue slot with
                    | true, rows -> rows
                    | _ ->
                        let rows =
                            match table slot with
                            | Some source -> QueryStatistics.RowCount source |> Option.map float |> Option.defaultValue defaultSourceRows
                            | None ->
                                if slot >= 0 && slot < sources.Length then
                                    match sources.[slot] with
                                    | SourceBinding.InMemory(rows, _) -> float rows.Length
                                    | SourceBinding.Table _ -> defaultSourceRows
                                else
                                    defaultSourceRows
                        knownRows.[slot] <- rows
                        rows
            Observed =
                fun plan ->
                    if QueryStatistics.HasFeedback then
                        CardinalityFeedback.tryKey sources plan
                        |> Option.bind QueryStatistics.Observed
                        |> Option.map (fun observed -> observed.Rows)
                    else
                        None
            KeyRange =
                fun slot column ->
                    table slot
                    |> Option.bind (fun source -> QueryStatistics.ColumnRange(source, column))
                    |> Option.bind (fun (low, high) ->
                        match literalNumber low, literalNumber high with
                        | Some lowest, Some highest -> Some(lowest, highest)
                        | _ -> None)
            Column =
                fun slot column ->
                    let key = struct (slot, column)
                    match knownColumns.TryGetValue key with
                    | true, statistics -> statistics
                    | _ ->
                        let statistics = table slot |> Option.bind (fun source -> QueryStatistics.Column(source, column))
                        knownColumns.[key] <- statistics
                        statistics
        }

    /// The estimates of `sourceEstimates` without column statistics, so that the fixed selectivities decide.
    let tableEstimates (sources: SourceBinding array) : SourceEstimates =
        { sourceEstimates sources with Column = (fun _ _ -> None); KeyRange = (fun _ _ -> None); Observed = (fun _ -> None) }

    let apply (sources: SourceBinding array) (unorderedJoins: HashSet<LogicalPlan>) (plan: LogicalPlan) : LogicalPlan =
        let estimates = sourceEstimates sources

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
                    let measures = relations |> Seq.map (estimate estimates) |> Array.ofSeq
                    let rows i = fst measures.[i]
                    let tableRows i = snd measures.[i]

                    let knownRanges = Dictionary<struct (int * string), (float * float) option>()

                    /// The range of a join key, when the relation reads it straight from a table.
                    let keyRange (relation: int) (key: ScalarExpr) =
                        match columnOf key with
                        | None -> None
                        | Some token ->
                            let cacheKey = struct (relation, token)
                            match knownRanges.TryGetValue cacheKey with
                            | true, range -> range
                            | _ ->
                                let range =
                                    sourceSlot relations.[relation]
                                    |> Option.bind (fun slot -> estimates.KeyRange slot (tokenColumn token))
                                knownRanges.[cacheKey] <- range
                                range

                    /// The rows of relation `x` whose key can match relation `y`: a dimension can span far more values
                    /// than the fact table uses (200 years of dates for 5 years of sales).
                    let matchingDomain (x: int) (keyX: ScalarExpr) (y: int) (keyY: ScalarExpr) =
                        match keyRange x keyX, keyRange y keyY with
                        | Some(lowX, highX), Some(lowY, highY) when highX > lowX ->
                            tableRows x * max 1e-6 (min 1.0 ((min highX highY - max lowX lowY) / (highX - lowX)))
                        | _ -> tableRows x

                    let edgeDomain (e: Edge) = min (matchingDomain e.A e.KeyA e.B e.KeyB) (matchingDomain e.B e.KeyB e.A e.KeyA)

                    /// The estimated rows of joining `current` rows with relation `i` through its edges to `joined`.
                    let joinedRows (current: float) (joined: HashSet<int>) (i: int) =
                        let matches =
                            edges
                            |> Seq.choose (fun e ->
                                if e.A = i && joined.Contains e.B then Some(edgeDomain e, matchingDomain e.A e.KeyA e.B e.KeyB)
                                elif e.B = i && joined.Contains e.A then Some(edgeDomain e, matchingDomain e.B e.KeyB e.A e.KeyA)
                                else None)
                            |> List.ofSeq
                        match matches with
                        | [] -> None
                        | _ ->
                            let denominator = matches |> List.map fst |> List.max
                            // Only the rows whose key can match take part: an unfiltered wide dimension does not fan out.
                            let taking = min (rows i) (matches |> List.map snd |> List.min)
                            Some(max 1.0 (current * taking / max 1.0 denominator))

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
