/// Contract: aggregates skip nulls. Sum over no values is 0; Mean, Min, Max, First and Std over no values are
/// null; Count counts the non-null values and Len the rows. Std / Var are null with no more than ddof values.
/// GroupBy with maintainOrder lists groups in the order their keys first appear; `having` keeps the groups
/// whose aggregate predicate is true.
module Polars.NET.Linq.Conformance.Tests.AggregationConformance

open System
open Xunit
open Polars.NET.Linq.IR

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
