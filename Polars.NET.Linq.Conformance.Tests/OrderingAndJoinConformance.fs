/// Contract: Filter, Project and WithColumns keep row order. Sort is stable and places nulls by NullsLast.
/// Union lists the left rows, then the right. Distinct with maintainOrder keeps the kept rows in input order.
/// Limit takes `count` rows after `offset`. Inner and Left joins list rows in left order, then right order
/// among matches; Semi and Anti keep left order. Null keys match exactly when `nullsEqual`.
module Polars.NET.Linq.Conformance.Tests.OrderingAndJoinConformance

open System
open Xunit
open Polars.NET.Linq.IR

type KI = { Id: int64; K: Nullable<int64> }
type RK = { RK: Nullable<int64>; RV: string }

let private left: KI array = [| { Id = 1L; K = n 2L }; { Id = 2L; K = nul }; { Id = 3L; K = n 1L }; { Id = 4L; K = n 2L }; { Id = 5L; K = nul } |]

let private ids plan = project plan [ "Id", col "Id" ]

[<Fact>]
let ``Sort is stable and places nulls by NullsLast`` () =
    let t = Tables()
    let scan = t.Scan left
    Conformance.rowsInOrder t (ids (LogicalPlan.Sort(scan, [ asc (col "K") ]))) [ [ 2L ]; [ 5L ]; [ 3L ]; [ 1L ]; [ 4L ] ]
    Conformance.rowsInOrder t
        (ids (LogicalPlan.Sort(scan, [ { asc (col "K") with Descending = true; NullsLast = true } ])))
        [ [ 1L ]; [ 4L ]; [ 3L ]; [ 2L ]; [ 5L ] ]

[<Fact>]
let ``Union lists left rows then right rows`` () =
    let t = Tables()
    let scan = t.Scan left
    let plan = LogicalPlan.Union(ids (filter scan (bin BinaryOp.Gt (col "Id") (i64 3L))), ids scan, true)
    Conformance.rowsInOrder t plan [ [ 4L ]; [ 5L ]; [ 1L ]; [ 2L ]; [ 3L ]; [ 4L ]; [ 5L ] ]

[<Fact>]
let ``Ordered Distinct keeps the first row per key in input order`` () =
    let t = Tables()
    let plan = ids (LogicalPlan.Distinct(t.Scan left, Some [ "K" ], DistinctKeep.First, true))
    Conformance.rowsInOrder t plan [ [ 1L ]; [ 2L ]; [ 3L ] ]

[<Fact>]
let ``Limit takes count rows after offset`` () =
    let t = Tables()
    Conformance.rowsInOrder t (ids (LogicalPlan.Limit(t.Scan left, 1L, 2u))) [ [ 2L ]; [ 3L ] ]

let private right: RK array = [| { RK = n 2L; RV = "a" }; { RK = nul; RV = "n" }; { RK = n 2L; RV = "b" }; { RK = n 9L; RV = "z" } |]

let private join how nullsEqual (t: Tables) =
    LogicalPlan.Join(t.Scan left, t.Scan right, [ col "K" ], [ col "RK" ], how, None, JoinKeyCoalescing.Default, nullsEqual)

[<Fact>]
let ``Left join keeps left order and right order among matches`` () =
    let t = Tables()
    let plan = project (join JoinKind.Left false t) [ "Id", col "Id"; "RV", col "RV" ]
    Conformance.rowsInOrder t plan [ [ 1L; "a" ]; [ 1L; "b" ]; [ 2L; null ]; [ 3L; null ]; [ 4L; "a" ]; [ 4L; "b" ]; [ 5L; null ] ]

[<Fact>]
let ``Null keys match exactly when nullsEqual`` () =
    let t = Tables()
    Conformance.rowsInOrder t (project (join JoinKind.Inner false t) [ "Id", col "Id"; "RV", col "RV" ]) [
        [ 1L; "a" ]; [ 1L; "b" ]; [ 4L; "a" ]; [ 4L; "b" ]
    ]
    Conformance.rowsInOrder t (project (join JoinKind.Inner true t) [ "Id", col "Id"; "RV", col "RV" ]) [
        [ 1L; "a" ]; [ 1L; "b" ]; [ 2L; "n" ]; [ 4L; "a" ]; [ 4L; "b" ]; [ 5L; "n" ]
    ]

[<Fact>]
let ``Semi and Anti joins keep left order`` () =
    let t = Tables()
    Conformance.rowsInOrder t (ids (join JoinKind.Semi false t)) [ [ 1L ]; [ 4L ] ]
    Conformance.rowsInOrder t (ids (join JoinKind.Anti false t)) [ [ 2L ]; [ 3L ]; [ 5L ] ]
    Conformance.rowsInOrder t (ids (join JoinKind.Anti true t)) [ [ 3L ] ]
