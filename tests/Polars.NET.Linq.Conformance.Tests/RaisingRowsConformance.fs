/// Contract (SEMANTICS.md, Rows a raising expression sees): an expression that may fail the query is evaluated only on
/// rows its plan position reaches, never on rows a filter or a join beneath it removes, as written and after the
/// optimizer; it still fails on a row that does reach it.
module Polars.NET.Linq.Conformance.Tests.RaisingRowsConformance

open System
open Xunit
open Kaminari.Samidare.IR

type RR = { K: int64; V: Nullable<int64>; Z: Nullable<int64> }
type RU = { RK: int64; W: int64 }

/// Row 2 makes every raising kind fail: V leaves Int32, is not listed, and Z is zero.
let private bad = int64 Int32.MaxValue + 1L
let private rows: RR array = [| { K = 1L; V = n 1L; Z = n 1L }; { K = 2L; V = n bad; Z = n 0L }; { K = 3L; V = n 5L; Z = n 1L } |]
let private others: RU array = [| { RK = 1L; W = 10L }; { RK = 3L; W = 30L } |]

let private kinds: (string * ScalarExpr) list =
    [
        "CheckedNarrow", ScalarExprOps.checkedNarrow LogicalTypes.Int32 ScalarExprOps.unguarded (col "V")
        "CheckedDivide", ScalarExprOps.checkedDivide DivisionKind.IntegerDivide LogicalTypes.Int64 ScalarExprOps.unguarded (col "V") (col "Z")
        "RequireListed",
        ScalarExpr.Function(
            ScalarFunction.RequireListed([ LiteralValue.Int64 1L; LiteralValue.Int64 5L ], "Value '{}' is not listed."),
            [ col "V"; ScalarExprOps.unguarded ],
            LogicalTypes.Int64
        )
    ]

let private keep = bin BinaryOp.NotEq (col "K") (i64 2L)
let private positive e = bin BinaryOp.Gt e (i64 0L)
let private join left right = LogicalPlan.Join(left, right, [ col "K" ], [ col "RK" ], JoinKind.Inner, None, JoinKeyCoalescing.Default, false)

/// The plan shapes whose lower filter or join removes row 2 before the raising expression `e` is reached.
let private shapes (e: ScalarExpr) : (string * (Tables -> LogicalPlan)) list =
    [
        "a filter over a filter", fun t -> filter (filter (t.Scan rows) keep) (positive e)
        "a filter over a projection of it", fun t -> filter (project (filter (t.Scan rows) keep) [ "K", col "K"; "i", e ]) (positive (col "i"))
        "a filter over a join of a filtered side", fun t -> filter (join (filter (t.Scan rows) keep) (t.Scan others)) (positive e)
        "a filter over a join the row does not match", fun t -> filter (join (t.Scan rows) (t.Scan others)) (positive e)
        "a guarded conjunct after the test", fun t -> filter (t.Scan rows) (RaisingGuards.ofExpr (bin BinaryOp.And keep (positive e)))
    ]

[<Fact>]
let ``A raising expression never sees rows a filter or a join beneath it removes`` () =
    for kind, e in kinds do
        for shape, build in shapes e do
            let t = Tables()
            let plan = project (build t) [ "K", col "K" ]
            try
                Conformance.rowsInOrder t plan [ [ 1L ]; [ 3L ] ]
            with ex ->
                Assert.Fail($"{kind}, {shape}: {ex.GetType().Name}: {ex.Message}")

[<Fact>]
let ``A filter on a raising grouping key never sees rows a filter beneath the grouping removes`` () =
    for kind, e in kinds do
        let t = Tables()
        let grouped = LogicalPlan.GroupBy(filter (t.Scan rows) keep, [ ScalarExpr.Alias(e, "i") ], [ "c", len ], None, true)
        try
            Conformance.rowsInOrder t (project (filter grouped (positive (col "i"))) [ "c", col "c" ]) [ [ 1L ]; [ 1L ] ]
        with ex ->
            Assert.Fail($"{kind}: {ex.GetType().Name}: {ex.Message}")

/// A raising conjunct that reads other rows (here the filter's row count) sees every row of the filter's input, not
/// only those its other conjuncts keep.
[<Fact>]
let ``A raising conjunct over other rows sees the whole input of its filter`` () =
    let t = Tables()
    let counted = bin BinaryOp.Eq (ScalarExprOps.checkedNarrow LogicalTypes.Int32 ScalarExprOps.unguarded len) (i64 3L)
    Conformance.rowsInOrder t (project (filter (t.Scan rows) (bin BinaryOp.And keep counted)) [ "K", col "K" ]) [ [ 1L ]; [ 3L ] ]

/// Fails, as written and after the optimizer.
let private fails (t: Tables) (plan: LogicalPlan) =
    for p in [ plan; LogicalOptimizer.optimize plan ] do
        let failed =
            try
                (PolarsEngine() :> IIrEngine).Execute(p, t.Sources) |> ignore
                false
            with _ -> true
        Assert.True(failed, sprintf "expected a failure:\n%s" (LogicalPlanOps.formatPlan p))

[<Fact>]
let ``A raising expression still fails on a row that reaches it`` () =
    let lets = bin BinaryOp.NotEq (col "K") (i64 1L)
    for _, e in kinds do
        let t = Tables()
        fails t (filter (filter (t.Scan rows) lets) (positive e))
        let t2 = Tables()
        fails t2 (filter (project (filter (t2.Scan rows) lets) [ "K", col "K"; "i", e ]) (positive (col "i")))
        let t3 = Tables()
        fails t3 (filter (join (t3.Scan rows) (t3.Scan [| { RK = 2L; W = 20L } |])) (positive e))

type RD = { RK: int64; G: string }
type RE = { RK2: int64; H: string }

/// The rows of the plan, in any order, after the whole optimizer (join reordering and eager aggregation included).
let private rowsAfterComplete (t: Tables) (plan: LogicalPlan) (expected: obj list list) =
    let optimized = LogicalOptimizer.optimizeComplete true true t.Sources plan
    let normalize (v: obj) = match v with :? int64 | :? int32 | :? uint32 | :? uint64 -> box (Convert.ToInt64 v) | v -> v
    let actual = (PolarsEngine() :> IIrEngine).Execute(optimized, t.Sources).Rows |> List.map (List.map normalize) |> List.sortBy string
    let expected = expected |> List.sortBy string
    Assert.True((actual = expected), sprintf "expected %A\nactual   %A\n%s" expected actual (LogicalPlanOps.formatPlan optimized))

/// A fact table whose last row matches no dimension row and divides by zero.
let private fact: RR array = [| for i in 1L .. 40L -> { K = (if i = 40L then 9L else i % 2L + 1L); V = n i; Z = n (if i = 40L then 0L else 1L) } |]
let private dims: RD array = [| { RK = 1L; G = "a" }; { RK = 2L; G = "b" } |]
let private vDivZ = ScalarExprOps.checkedDivide DivisionKind.IntegerDivide LogicalTypes.Int64 ScalarExprOps.unguarded (col "V") (col "Z")

[<Fact>]
let ``Eager aggregation does not aggregate a raising argument before the join`` () =
    let t = Tables()
    let j = LogicalPlan.Join(t.Scan fact, t.Scan dims, [ col "K" ], [ col "RK" ], JoinKind.Inner, None, JoinKeyCoalescing.Default, false)
    rowsAfterComplete t (LogicalPlan.GroupBy(j, [ col "G" ], [ "s", agg AggKind.Sum vDivZ ], None, true)) [ [ "b"; 400L ]; [ "a"; 380L ] ]

/// Row 40 matches the last joined table but not the dimension: a filter between the joins must stay above the
/// dimension's join.
[<Fact>]
let ``Join reordering keeps a raising filter above the joins beneath it`` () =
    let t = Tables()
    let others: RE array = [| { RK2 = 1L; H = "x" }; { RK2 = 2L; H = "y" }; { RK2 = 9L; H = "z" } |]
    // A dimension larger than the other table, so the other table is joined first once the joins are reordered.
    let bigDims: RD array = [| yield! dims; for k in 10L .. 30L -> { RK = k; G = "-" } |]
    let inner = LogicalPlan.Join(t.Scan fact, t.Scan bigDims, [ col "K" ], [ col "RK" ], JoinKind.Inner, None, JoinKeyCoalescing.Default, false)
    let outer = LogicalPlan.Join(filter inner (positive vDivZ), t.Scan others, [ col "K" ], [ col "RK2" ], JoinKind.Inner, None, JoinKeyCoalescing.Default, false)
    rowsAfterComplete t (LogicalPlan.GroupBy(outer, [ col "G" ], [ "c", len ], None, false)) [ [ "a"; 19L ]; [ "b"; 20L ] ]

