/// Contract: aggregates skip nulls. Sum over no values is 0; Mean, Min, Max, First and Std over no values are
/// null; Count counts the non-null values and Len the rows. Std / Var are null with no more than ddof values.
/// GroupBy with maintainOrder lists groups in the order their keys first appear; `having` keeps the groups
/// whose aggregate predicate is true. Grouping compares strings by their bytes (ordinal, case-sensitive), decimals by
/// value and date-times by the instant they store.
module Polars.NET.Linq.Conformance.Tests.AggregationConformance

open System
open Xunit
open Kaminari.Samidare.IR

type KV = { K: int64; V: Nullable<int64> }

let private aggregates =
    [
        "sum", agg AggKind.Sum (col "V")
        "mean", agg AggKind.Mean (col "V")
        "min", agg AggKind.Min (col "V")
        "max", agg AggKind.Max (col "V")
        "count", agg AggKind.Count (col "V")
        "len", len
        "first", agg AggKind.First (col "V")
        "std", agg (AggKind.Std 1uy) (col "V")
    ]

[<Fact>]
let ``Aggregates skip nulls`` () =
    let t = Tables()
    let rows = [| { K = 1L; V = n 1L }; { K = 1L; V = nul }; { K = 1L; V = n 3L } |]
    Conformance.rowsInOrder t (project (t.Scan rows) aggregates) [ [ 4L; 2.0; 1L; 3L; 2L; 3L; 1L; Math.Sqrt 2.0 ] ]

[<Fact>]
let ``Aggregates over only nulls`` () =
    let t = Tables()
    let rows = [| { K = 1L; V = nul }; { K = 1L; V = nul } |]
    Conformance.rowsInOrder t (project (t.Scan rows) aggregates) [ [ 0L; null; null; null; 0L; 2L; null; null ] ]

[<Fact>]
let ``Aggregates over no rows`` () =
    let t = Tables()
    let rows = [| { K = 1L; V = n 1L } |]
    let none = filter (t.Scan rows) (bin BinaryOp.Gt (col "K") (i64 5L))
    Conformance.rowsInOrder t (project none aggregates) [ [ 0L; null; null; null; 0L; 0L; null; null ] ]

[<Fact>]
let ``Std is null with no more than ddof values`` () =
    let t = Tables()
    let rows = [| { K = 1L; V = n 4L }; { K = 2L; V = n 4L }; { K = 2L; V = n 6L } |]
    let plan = LogicalPlan.GroupBy(t.Scan rows, [ col "K" ], [ "std", agg (AggKind.Std 1uy) (col "V") ], None, true)
    Conformance.rowsInOrder t plan [ [ 1L; null ]; [ 2L; Math.Sqrt 2.0 ] ]

[<Fact>]
let ``Ordered groups appear in first-key order and having filters groups`` () =
    let t = Tables()
    let rows = [| { K = 3L; V = n 1L }; { K = 1L; V = n 2L }; { K = 3L; V = n 3L }; { K = 2L; V = n 4L }; { K = 1L; V = nul } |]
    let grouped having = LogicalPlan.GroupBy(t.Scan rows, [ col "K" ], [ "n", len; "s", agg AggKind.Sum (col "V") ], having, true)
    Conformance.rowsInOrder t (grouped None) [ [ 3L; 2L; 4L ]; [ 1L; 2L; 2L ]; [ 2L; 1L; 4L ] ]
    Conformance.rowsInOrder t (grouped (Some(bin BinaryOp.Gt len (i64 1L)))) [ [ 3L; 2L; 4L ]; [ 1L; 2L; 2L ] ]

/// First and Last read the order of their input, so the optimizer keeps every order guarantee beneath them
/// (here, a GroupBy's first-key order), while order-free aggregates let it go.
[<Fact>]
let ``The optimizer keeps the order beneath order-reading aggregates`` () =
    let t = Tables()
    let rows = [| { K = 3L; V = n 1L }; { K = 1L; V = n 2L } |]
    let groups = LogicalPlan.GroupBy(t.Scan rows, [ col "K" ], [ "n", len ], None, true)
    let keepsOrder plan =
        match LogicalOptimizer.optimize plan with
        | LogicalPlan.Project(LogicalPlan.GroupBy(_, _, _, _, maintainOrder), _) -> maintainOrder
        | other -> failwithf "unexpected plan %s" (LogicalPlanOps.formatPlan other)
    Assert.True(keepsOrder (project groups [ "last", agg AggKind.Last (col "K") ]))
    Assert.True(keepsOrder (project groups [ "first", agg AggKind.First (col "K") ]))
    Assert.False(keepsOrder (project groups [ "total", agg AggKind.Sum (col "K") ]))
    Conformance.rowsInOrder t (project groups [ "last", agg AggKind.Last (col "K") ]) [ [ 1L ] ]

type KI = { K: int64; I: Nullable<int> }

/// CheckedSum is .NET's checked integer sum: the exact total in the summed type (Int32 stays Int32), nulls skipped
/// and 0 for no values; the total decides, so partial sums may leave the range.
[<Fact>]
let ``CheckedSum is the exact total of the summed type`` () =
    let t = Tables()
    let longs = [| { K = 1L; V = n Int64.MaxValue }; { K = 1L; V = n 1L }; { K = 1L; V = nul }; { K = 1L; V = n -1L } |]
    Conformance.rowsInOrder t (project (t.Scan longs) [ "s", agg AggKind.CheckedSum (col "V") ]) [ [ Int64.MaxValue ] ]
    let ints = [| { K = 1L; I = Nullable Int32.MinValue }; { K = 1L; I = Nullable -1 }; { K = 1L; I = Nullable 1 } |]
    Conformance.rowsInOrder t (project (t.Scan ints) [ "s", agg AggKind.CheckedSum (col "I") ]) [ [ Int32.MinValue ] ]
    let none = filter (t.Scan longs) (bin BinaryOp.Gt (col "K") (i64 5L))
    Conformance.rowsInOrder t (project none [ "s", agg AggKind.CheckedSum (col "V") ]) [ [ 0L ] ]

[<Fact>]
let ``CheckedSum fails with Overflow when the total leaves the range`` () =
    let t = Tables()
    let longs = [| { K = 1L; V = n Int64.MaxValue }; { K = 1L; V = n Int64.MaxValue } |]
    Conformance.raises<OverflowException> t (project (t.Scan longs) [ "s", agg AggKind.CheckedSum (col "V") ]) "Arithmetic operation resulted in an overflow."
    let ints = [| { K = 1L; I = Nullable Int32.MinValue }; { K = 1L; I = Nullable -1 } |]
    Conformance.raises<OverflowException> t (project (t.Scan ints) [ "s", agg AggKind.CheckedSum (col "I") ]) "Arithmetic operation resulted in an overflow."

/// A value as an integer type, checked (the function's result type).
let private narrow (target: Apache.Arrow.Types.IArrowType) e = ScalarExpr.Function(ScalarFunction.CheckedNarrow, [ e ], LogicalType.Arrow target)

/// A long column's exact sum: widened to Decimal(38, 0) (cast to Int64 first, as the checked sum per group is built).
let private exactSum e =
    agg AggKind.Sum (ScalarExpr.Cast(ScalarExpr.Cast(e, LogicalTypes.Int64, false), LogicalTypes.decimalOf 38 0, false))

[<Fact>]
let ``CheckedNarrow takes an exact sum back per group`` () =
    let t = Tables()
    let rows = [| { K = 1L; V = n Int64.MaxValue }; { K = 2L; V = nul }; { K = 1L; V = n -1L }; { K = 1L; V = n 1L }; { K = 3L; V = n Int64.MinValue } |]
    let plan = LogicalPlan.GroupBy(t.Scan rows, [ col "K" ], [ "s", narrow Apache.Arrow.Types.Int64Type.Default (exactSum (col "V")) ], None, true)
    Conformance.rowsInOrder t plan [ [ 1L; Int64.MaxValue ]; [ 2L; 0L ]; [ 3L; Int64.MinValue ] ]
    let over = [| { K = 1L; V = n 3L }; { K = 2L; V = n Int64.MaxValue }; { K = 2L; V = n 1L } |]
    let t2 = Tables()
    let overflowing = LogicalPlan.GroupBy(t2.Scan over, [ col "K" ], [ "s", narrow Apache.Arrow.Types.Int64Type.Default (exactSum (col "V")) ], None, true)
    Conformance.raises<OverflowException> t2 overflowing "Arithmetic operation resulted in an overflow."

[<Fact>]
let ``CheckedNarrow converts row by row, failing with Overflow outside the range`` () =
    let t = Tables()
    let rows = [| { K = 1L; V = n (int64 Int32.MaxValue) }; { K = 2L; V = nul }; { K = 3L; V = n (int64 Int32.MinValue) } |]
    Conformance.rowsInOrder t (project (t.Scan rows) [ "i", narrow Apache.Arrow.Types.Int32Type.Default (col "V") ])
        [ [ Int32.MaxValue ]; [ null ]; [ Int32.MinValue ] ]
    let t2 = Tables()
    let beyond = [| { K = 1L; V = n 1L }; { K = 2L; V = n (int64 Int32.MaxValue + 1L) } |]
    Conformance.raises<OverflowException> t2 (project (t2.Scan beyond) [ "i", narrow Apache.Arrow.Types.Int32Type.Default (col "V") ])
        "Arithmetic operation resulted in an overflow."

type NX = { Id: int64; L: Nullable<int64>; X: Nullable<float>; M: Nullable<decimal> }

/// NarrowOrNull is a row conversion .NET checks: truncated toward zero, null outside the range or for NaN.
[<Fact>]
let ``NarrowOrNull truncates, null outside the range`` () =
    let t = Tables()
    let rows: NX array =
        [|
            { Id = 1L; L = n (int64 Int32.MaxValue); X = Nullable -2.9; M = Nullable -488023.5m }
            { Id = 2L; L = n (int64 Int32.MaxValue + 1L); X = Nullable nan; M = Nullable 3e9m }
            { Id = 3L; L = nul; X = Nullable 2147483648.0; M = Nullable() }
        |]
    let narrowOrNull e = ScalarExpr.Function(ScalarFunction.NarrowOrNull, [ e ], LogicalTypes.Int32)
    Conformance.rowsInOrder t (project (t.Scan rows) [ "l", narrowOrNull (col "L"); "x", narrowOrNull (col "X"); "m", narrowOrNull (col "M") ])
        [ [ Int32.MaxValue; -2; -488023 ]; [ null; null; null ]; [ null; null; null ] ]

type GV = { Id: int64; S: string; D: decimal; T: DateTimeOffset }

let private keyed: GV array =
    [|
        { Id = 1L; S = "a"; D = 1.50m; T = DateTimeOffset(2024, 1, 1, 1, 0, 0, TimeSpan.FromHours 1.0) }
        { Id = 2L; S = "A"; D = 1.5m; T = DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero) } // the same instant as row 1
        { Id = 3L; S = "a"; D = 2.00m; T = DateTimeOffset(2024, 1, 2, 0, 0, 0, TimeSpan.Zero) }
    |]

[<Fact>]
let ``Grouping compares strings ordinally, decimals by value, date-times by instant`` () =
    let groupsBy key =
        let t = Tables()
        let grouped = LogicalPlan.GroupBy(t.Scan keyed, [ col key ], [ "n", len; "first", agg AggKind.First (col "Id") ], None, true)
        t, project grouped [ "n", col "n"; "first", col "first" ]
    let t, byS = groupsBy "S"
    Conformance.rowsInOrder t byS [ [ 2L; 1L ]; [ 1L; 2L ] ]
    let t, byD = groupsBy "D"
    Conformance.rowsInOrder t byD [ [ 2L; 1L ]; [ 1L; 3L ] ]
    let t, byT = groupsBy "T"
    Conformance.rowsInOrder t byT [ [ 2L; 1L ]; [ 1L; 3L ] ]

/// SumOrNull is SQL's SUM: the total of the non-null values, null when there are none (no rows, only nulls), per
/// group and per window partition too; a distinct one sums the distinct values.
[<Fact>]
let ``SumOrNull is null without values`` () =
    let t = Tables()
    let rows = [| { K = 1L; V = n 2L }; { K = 1L; V = nul }; { K = 2L; V = nul }; { K = 3L; V = n 2L }; { K = 3L; V = n 5L } |]
    let sumOrNull e = agg AggKind.SumOrNull e
    let distinctSum e = ScalarExpr.Aggregation(AggKind.SumOrNull, Some e, true)
    let scan = t.Scan rows
    Conformance.rowsInOrder t (project scan [ "s", sumOrNull (col "V"); "d", distinctSum (col "V") ]) [ [ 9L; 7L ] ]
    let onlyNulls = filter scan (bin BinaryOp.Eq (col "K") (i64 2L))
    Conformance.rowsInOrder t (project onlyNulls [ "s", sumOrNull (col "V"); "d", distinctSum (col "V") ]) [ [ null; null ] ]
    let none = filter scan (bin BinaryOp.Gt (col "K") (i64 5L))
    Conformance.rowsInOrder t (project none [ "s", sumOrNull (col "V"); "sum", agg AggKind.Sum (col "V") ]) [ [ null; 0L ] ]
    Conformance.rowsInOrder t (LogicalPlan.GroupBy(scan, [ col "K" ], [ "s", sumOrNull (col "V") ], None, true))
        [ [ 1L; 2L ]; [ 2L; null ]; [ 3L; 7L ] ]
    Conformance.rowsInOrder t (project scan [ "K", col "K"; "w", ScalarExpr.Window(sumOrNull (col "V"), [ col "K" ], []) ])
        [ [ 1L; 2L ]; [ 1L; 2L ]; [ 2L; null ]; [ 3L; 7L ]; [ 3L; 7L ] ]

type RCA = { R: string; C: string; A: Nullable<int64> }

/// A grouping-sets level computed from a finer one (GroupingSetsCascade) keeps SumOrNull's nulls: a coarser group is
/// null exactly when every finer one is.
[<Fact>]
let ``SumOrNull levels merge from finer levels`` () =
    let t = Tables()
    let rows =
        [| { R = "east"; C = "x"; A = n 1L }; { R = "east"; C = "x"; A = nul }; { R = "east"; C = "y"; A = nul }
           { R = "west"; C = "y"; A = nul } |]
    let spec =
        {
            KeyAliases = [ "R", col "R"; "C", col "C" ]
            Aggs = [ "total", agg AggKind.SumOrNull (col "A") ]
            Levels = [ [ 0; 1 ], Map.empty; [ 0 ], Map.empty; [], Map.empty ]
            OutputColumns = [ "R"; "C"; "total" ]
            LevelMerges = None
        }
    let plan = LogicalPlan.GroupingSets(t.Scan rows, spec)
    let cascaded = Kaminari.Samidare.IR.Passes.GroupingSetsCascade.apply plan
    match cascaded with
    | LogicalPlan.GroupingSets(_, { LevelMerges = Some [ "total", ScalarExpr.Aggregation(AggKind.SumOrNull, _, false) ] }) -> ()
    | other -> failwithf "not cascaded: %s" (LogicalPlanOps.formatPlan other)
    let expected =
        [ [ box "east"; box "x"; box 1L ]; [ "east"; "y"; null ]; [ "west"; "y"; null ]
          [ "east"; null; 1L ]; [ "west"; null; null ]
          [ null; null; 1L ] ]
    for p in [ plan; cascaded ] do
        Conformance.rowsInOrder t p expected

type LKV = { LK: int64; LV: Nullable<int64> }
type RKV = { RK: int64; RV: Nullable<int64> }

/// A SumOrNull of a UNION ALL of groupings flattened into one grouping (UnionAggregateFlattening): a branch's 0 and
/// COALESCE(SUM_OR_NULL(x), 0) still reach their groups as 0, and groups only nulls reach stay null.
[<Fact>]
let ``SumOrNull over a union of groupings flattens into one grouping`` () =
    let t = Tables()
    let left = t.Scan [| { LK = 1L; LV = nul }; { LK = 2L; LV = n 5L } |]
    let right = t.Scan [| { RK = 1L; RV = nul }; { RK = 3L; RV = nul } |]
    let sumOrNull name = agg AggKind.SumOrNull (col name)
    let zero = i64 0L
    let inner input key aggs = LogicalPlan.GroupBy(input, [ ScalarExpr.Alias(col key, "k") ], aggs, None, true)
    let plan =
        LogicalPlan.GroupBy(
            LogicalPlan.Union(
                inner left "LK" [ "a", sumOrNull "LV"; "b", zero; "c", sumOrNull "LV" ],
                inner right "RK" [ "a", zero; "b", ScalarExpr.Coalesce [ sumOrNull "RV"; zero ]; "c", sumOrNull "RV" ],
                true),
            [ col "k" ],
            [ "a", sumOrNull "a"; "b", sumOrNull "b"; "c", sumOrNull "c" ],
            None,
            true)
    let flattened = Kaminari.Samidare.IR.Passes.UnionAggregateFlattening.apply plan
    match flattened with
    | LogicalPlan.GroupBy(LogicalPlan.Union(LogicalPlan.Project _, LogicalPlan.Project _, true), _, _, None, true) -> ()
    | other -> failwithf "not flattened: %s" (LogicalPlanOps.formatPlan other)
    for p in [ plan; flattened ] do
        Conformance.rowsInOrder t p [ [ 1L; 0L; 0L; null ]; [ 2L; 5L; 0L; 5L ]; [ 3L; 0L; 0L; null ] ]

type FKV = { FK: int64; FV: Nullable<int64> }
type DKA = { DK: int64; DA: string; DT: int64 }

/// SumOrNull aggregated on the join key beneath its dimension (EagerAggregation), plainly and guarded by a test on
/// dimension columns: a group is null when none of its rows has a value (or passes the test).
[<Fact>]
let ``SumOrNull is aggregated before the join of its dimension`` () =
    let t = Tables()
    let fact =
        [| for v in [ n 1L; nul; n 2L; nul ] -> { FK = 1L; FV = v }
           for v in [ n 5L; nul; nul; nul ] -> { FK = 2L; FV = v }
           for _ in 1..8 -> { FK = 3L; FV = nul }
           for _ in 1..4 -> { FK = 4L; FV = n 7L } |]
    let dims = [| { DK = 1L; DA = "a"; DT = 1L }; { DK = 2L; DA = "a"; DT = 0L }; { DK = 3L; DA = "b"; DT = 1L }; { DK = 4L; DA = "c"; DT = 0L } |]
    let factScan = t.Scan fact
    let dimScan = t.Scan dims
    let joined = LogicalPlan.Join(factScan, dimScan, [ col "FK" ], [ col "DK" ], JoinKind.Inner, None, JoinKeyCoalescing.Default, false)
    let guarded =
        ScalarExpr.Conditional(bin BinaryOp.Eq (col "DT") (i64 1L), col "FV", ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Int64))
    let plan =
        LogicalPlan.GroupBy(joined, [ col "DA" ], [ "s", agg AggKind.SumOrNull (col "FV"); "g", agg AggKind.SumOrNull guarded ], None, true)
    let eager = Kaminari.Samidare.IR.Passes.EagerAggregation.apply t.Sources plan
    match eager with
    | LogicalPlan.GroupBy(LogicalPlan.Join(LogicalPlan.GroupBy _, _, _, _, _, _, _, _), _, _, _, _) -> ()
    | other -> failwithf "not aggregated eagerly: %s" (LogicalPlanOps.formatPlan other)
    for p in [ plan; eager ] do
        Conformance.rowsInOrder t p [ [ "a"; 8L; 3L ]; [ "b"; null; null ]; [ "c"; 28L; null ] ]

/// A projection over an aggregating projection (one row) counts and reads that one row: the optimizer does not collapse
/// the two when the outer one aggregates or reads no inner column.
[<Fact>]
let ``A projection over aggregates reads their one row`` () =
    let t = Tables()
    let rows = [| { K = 1L; V = n 2L }; { K = 2L; V = n 3L }; { K = 3L; V = nul } |]
    let totals = project (t.Scan rows) [ "s", agg AggKind.Sum (col "V") ]
    Conformance.rowsInOrder t (project totals [ "rows", len; "first", agg AggKind.First (col "s") ]) [ [ 1L; 5L ] ]
    Conformance.rowsInOrder t (project totals [ "one", i64 1L ]) [ [ 1L ] ]
    Conformance.rowsInOrder t (project totals [ "twice", bin BinaryOp.Multiply (col "s") (i64 2L) ]) [ [ 10L ] ]
