namespace Kaminari.Samidare.IR.Passes

open System.Collections.Generic
open Kaminari.Samidare.IR

/// Moves what one side of an equi-join is known to hold on a join key to the other side's key.
///
/// In `l JOIN r ON l.k = r.k` a row of `r` whose key fails a test every row of `l` passes on its key matches no row of
/// `l`, so `r` can be filtered by the test first. TPC-DS Q78 joins store sales of the year 2000 to web and catalog sales
/// of every year, on (year, item, customer): the web and catalog sides then aggregate one year instead of all.
///
/// A test is known on a plan's output when every row passes it: a filter's conjuncts, carried through renames,
/// grouping keys, inner joins (both sides), and the left side of left, semi and anti joins. Only tests of one column
/// against literals are carried (comparisons, IN, and their disjunctions), which reject nulls, so they move whatever
/// the join's null equality.
///
/// The test moves to the right side of inner, left, semi and anti joins (a left row's partner has the same key), and
/// from the right to the left of inner joins. It goes in only where the other side does not already hold it, by
/// predicate pushdown, as far down as it allows.
module internal JoinKeyPredicateTransfer =

    /// The column a test of one column against literals reads, if the predicate is one.
    let rec private testedColumn (predicate: ScalarExpr) : string option =
        let isLiteral (expr: ScalarExpr) =
            match expr with
            | ScalarExpr.Literal(LiteralValue.Null _) -> false
            | ScalarExpr.Literal _
            | ScalarExpr.Cast(ScalarExpr.Literal _, _, _) -> true
            | _ -> false
        match predicate with
        | ScalarExpr.Binary((BinaryOp.Eq | BinaryOp.Lt | BinaryOp.LtEq | BinaryOp.Gt | BinaryOp.GtEq), ScalarExpr.Column column, bound)
        | ScalarExpr.Binary((BinaryOp.Eq | BinaryOp.Lt | BinaryOp.LtEq | BinaryOp.Gt | BinaryOp.GtEq), bound, ScalarExpr.Column column) when isLiteral bound ->
            Some column
        | ScalarExpr.InList(ScalarExpr.Column column, _, false, false) -> Some column
        | ScalarExpr.Binary(BinaryOp.Or, left, right) ->
            match testedColumn left, testedColumn right with
            | Some l, Some r when l = r -> Some l
            | _ -> None
        | _ -> None

    let apply (plan: LogicalPlan) : LogicalPlan =
        let memo = Dictionary<LogicalPlan, (string * ScalarExpr) list>(HashIdentity.Reference)
        let schemas = Dictionary<LogicalPlan, LogicalSchema>(HashIdentity.Reference)
        let schemaOf (p: LogicalPlan) =
            match schemas.TryGetValue p with
            | true, schema -> schema
            | _ ->
                let schema = SchemaInference.inferPlanSchema p
                schemas.[p] <- schema
                schema
        let renamed (mapping: (string * string) list) (tests: (string * ScalarExpr) list) =
            [ for (source, target) in mapping do
                  for (column, test) in tests do
                      if column = source then
                          target, ScalarExprOps.renameColumns (Map.ofList [ source, target ]) test ]
        /// The tests every output row of the plan passes, with the column each reads.
        let rec known (p: LogicalPlan) : (string * ScalarExpr) list =
            match memo.TryGetValue p with
            | true, tests -> tests
            | _ ->
                let tests =
                    match p with
                    | LogicalPlan.Filter(input, predicate) ->
                        let own = FilterShapes.conjuncts predicate |> List.choose (fun c -> testedColumn c |> Option.map (fun column -> column, c))
                        own @ known input
                    | LogicalPlan.Project(input, columns) ->
                        let mapping = columns |> List.choose (fun (name, expr) -> match expr with ScalarExpr.Column source -> Some(source, name) | _ -> None)
                        renamed mapping (known input)
                    | LogicalPlan.Rename(input, existing, newNames) ->
                        let renames = List.zip existing newNames
                        match known input with
                        | [] -> []
                        | tests ->
                            let untouched = (schemaOf input).ColumnNames |> List.filter (fun c -> not (List.contains c existing))
                            renamed (renames @ (untouched |> List.map (fun c -> c, c))) tests
                    | LogicalPlan.WithColumns(input, columns) ->
                        let replaced = columns |> List.map fst |> Set.ofList
                        known input |> List.filter (fun (column, _) -> not (replaced.Contains column))
                    | LogicalPlan.GroupBy(input, keys, _, _, _) ->
                        let mapping =
                            keys
                            |> List.choose (function
                                | ScalarExpr.Column name -> Some(name, name)
                                | ScalarExpr.Alias(ScalarExpr.Column source, name) -> Some(source, name)
                                | _ -> None)
                        renamed mapping (known input)
                    | LogicalPlan.Join(left, right, _, _, JoinKind.Inner, _, _, _, joinOrdered) ->
                        match known right with
                        | [] -> known left
                        | fromRight ->
                            let leftColumns = Set.ofList (schemaOf left).ColumnNames
                            known left @ (fromRight |> List.filter (fun (column, _) -> not (leftColumns.Contains column)))
                    | LogicalPlan.Join(left, _, _, _, (JoinKind.Left | JoinKind.Semi | JoinKind.Anti), _, _, _, joinOrdered) -> known left
                    | LogicalPlan.Distinct(input, _, _, _)
                    | LogicalPlan.Sort(input, _)
                    | LogicalPlan.Limit(input, _, _)
                    | LogicalPlan.Shared(_, input) -> known input
                    | _ -> []
                memo.[p] <- tests
                tests
        let typeOf (p: LogicalPlan) (column: string) =
            (schemaOf p).Fields |> List.tryFind (fun f -> f.Name = column) |> Option.map (fun f -> f.DataType)
        /// The tests of `from` on its keys, as tests of the other side's partner keys that side does not hold yet.
        let moved (from: LogicalPlan) (fromKeys: ScalarExpr list) (into: LogicalPlan) (intoKeys: ScalarExpr list) =
            let tests = known from
            // Most joins' inputs hold no test on a key: their schemas are not needed.
            let onKeys =
                List.zip fromKeys intoKeys
                |> List.choose (function
                    | ScalarExpr.Column a, ScalarExpr.Column b when tests |> List.exists (fst >> (=) a) -> Some(a, b)
                    | _ -> None)
            if onKeys.IsEmpty then
                []
            else
                let pairs = onKeys |> List.filter (fun (a, b) -> (typeOf from a).IsSome && typeOf from a = typeOf into b)
                let held = known into |> List.map snd
                renamed pairs tests |> List.map snd |> List.distinct |> List.filter (fun test -> not (List.contains test held))
        let rec rewrite (p: LogicalPlan) : LogicalPlan =
            match LogicalPlanOps.mapInputs rewrite p with
            | LogicalPlan.Join(left, right, leftOn, rightOn, (JoinKind.Inner | JoinKind.Left | JoinKind.Semi | JoinKind.Anti as kind), suffix, coalescing, nullsEqual, joinOrdered) as join ->
                let toRight = moved left leftOn right rightOn
                let toLeft = if kind = JoinKind.Inner then moved right rightOn left leftOn else []
                if toRight.IsEmpty && toLeft.IsEmpty then
                    join
                else
                    let push tests side = tests |> List.fold (fun acc test -> PredicatePushdown.pushdownConjunct test acc) side
                    LogicalPlan.Join(push toLeft left, push toRight right, leftOn, rightOn, kind, suffix, coalescing, nullsEqual, joinOrdered)
            | other -> other
        rewrite plan
