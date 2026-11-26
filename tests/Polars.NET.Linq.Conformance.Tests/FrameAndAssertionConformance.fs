/// Contract: an expression that reads other rows (a window, rank, cumulative function) sees the input of the
/// node that computes it, so a Filter above that node never changes its values. AssertCount yields its value
/// when the count lies in range, and otherwise fails the query with the check's error.
module Polars.NET.Linq.Conformance.Tests.FrameAndAssertionConformance

open System
open Xunit
open Kaminari.Samidare.IR

type GV = { Id: int64; G: int64; V: int64 }

let private rows: GV array =
    [| { Id = 1L; G = 1L; V = 30L }; { Id = 2L; G = 1L; V = 10L }; { Id = 3L; G = 2L; V = 20L }; { Id = 4L; G = 2L; V = 40L } |]

[<Fact>]
let ``A filter above a rank does not change the ranks`` () =
    let t = Tables()
    let ranked =
        project (t.Scan rows) [
            "Id", col "Id"
            "r", fn (ScalarFunction.Rank(RankMethod.Ordinal, None)) [ col "V"; bool false ]
        ]
    Conformance.rowsInOrder t (filter ranked (bin BinaryOp.GtEq (col "Id") (i64 3L))) [ [ 3L; 2L ]; [ 4L; 4L ] ]

[<Fact>]
let ``A filter above a window does not change the window values`` () =
    let t = Tables()
    let windowed =
        project (t.Scan rows) [
            "Id", col "Id"
            "s", ScalarExpr.Window(agg AggKind.Sum (col "V"), [ col "G" ], [])
        ]
    Conformance.rowsInOrder t (filter windowed (bin BinaryOp.NotEq (col "Id") (i64 2L))) [ [ 1L; 40L ]; [ 3L; 60L ]; [ 4L; 60L ] ]

type GOV = { Id: int64; G: int64; O: int64; V: Nullable<int64> }

/// ForwardFill gives the last non-null value at or before each row, in a window's order within its partition (null
/// before the first): over a cumulative function, SQL's running aggregate that skips nulls.
[<Fact>]
let ``ForwardFill over a cumulative maximum skips nulls in the window's order`` () =
    let t = Tables()
    let data =
        [| { Id = 1L; G = 1L; O = 3L; V = n 5L }; { Id = 2L; G = 1L; O = 1L; V = nul }; { Id = 3L; G = 1L; O = 2L; V = n 7L }
           { Id = 4L; G = 1L; O = 4L; V = nul }; { Id = 5L; G = 2L; O = 1L; V = n 2L }; { Id = 6L; G = 2L; O = 2L; V = nul } |]
    let running = fn ScalarFunction.ForwardFill [ fn ScalarFunction.CumMax [ col "V"; bool false ] ]
    let plan = project (t.Scan data) [ "Id", col "Id"; "m", ScalarExpr.Window(running, [ col "G" ], [ asc (col "O") ]) ]
    Conformance.rowsInOrder t plan [ [ 1L; 7L ]; [ 2L; null ]; [ 3L; 7L ]; [ 4L; 7L ]; [ 5L; 2L ]; [ 6L; 2L ] ]

/// RollingWindow(offset, length) aggregates, for each row, the rows of its partition whose index lies in
/// (index + offset, index + offset + length]: equal indexes share a frame, a frame past the partition's ends holds
/// the rows inside it, and a frame holding no values gives the aggregate over none (Sum 0, Max null).
[<Fact>]
let ``RollingWindow aggregates the rows of a frame by an index`` () =
    let t = Tables()
    let data =
        [| { Id = 1L; G = 1L; O = 1L; V = n 1L }; { Id = 2L; G = 1L; O = 2L; V = n 2L }; { Id = 3L; G = 1L; O = 2L; V = nul }
           { Id = 4L; G = 1L; O = 4L; V = n 8L }; { Id = 5L; G = 2L; O = 1L; V = n 16L }; { Id = 6L; G = 2L; O = 9L; V = nul } |]
    // The frame [O - 1, O]: offset -2, length 2.
    let framed (aggregate: ScalarExpr) = ScalarExpr.Window(fn (ScalarFunction.RollingWindow(-2L, 2L)) [ aggregate; col "O" ], [ col "G" ], [ asc (col "O") ])
    let plan = project (t.Scan data) [ "Id", col "Id"; "s", framed (agg AggKind.Sum (col "V")); "n", framed len; "m", framed (agg AggKind.Max (col "V")) ]
    Conformance.rowsInAnyOrder t plan
        [ [ 1L; 1L; 1L; 1L ]; [ 2L; 3L; 3L; 2L ]; [ 3L; 3L; 3L; 2L ]; [ 4L; 8L; 1L; 8L ]; [ 5L; 16L; 1L; 16L ]; [ 6L; 0L; 1L; null ] ]

let private exactlyOne =
    {
        Min = 1L
        Max = Some 1L
        TooFew = { Kind = QueryErrorKind.InvalidOperation; Message = "Sequence contains no elements" }
        TooMany = { Kind = QueryErrorKind.InvalidOperation; Message = "Sequence contains more than one element" }
    }

let private single (t: Tables) (pred: ScalarExpr) (check: CountCheck) =
    project (filter (t.Scan rows) pred) [ "v", fn (ScalarFunction.AssertCount check) [ len; agg AggKind.First (col "V") ] ]

[<Fact>]
let ``AssertCount yields its value when the count is in range`` () =
    let t = Tables()
    Conformance.rowsInOrder t (single t (bin BinaryOp.Eq (col "Id") (i64 3L)) exactlyOne) [ [ 20L ] ]

[<Fact>]
let ``AssertCount fails the query with the check's error`` () =
    let t = Tables()
    Conformance.raises<InvalidOperationException> t (single t (bin BinaryOp.Gt (col "Id") (i64 2L)) exactlyOne) "Sequence contains more than one element"
    Conformance.raises<InvalidOperationException> t (single t (bin BinaryOp.Gt (col "Id") (i64 9L)) exactlyOne) "Sequence contains no elements"
    let outOfRange = { Kind = QueryErrorKind.ArgumentOutOfRange; Message = "" }
    Conformance.raises<ArgumentOutOfRangeException> t
        (single t (bin BinaryOp.Gt (col "Id") (i64 9L)) { exactlyOne with Max = None; TooFew = outOfRange })
        "Specified argument was out of the range of valid values. (Parameter 'index')"

/// Expressions are evaluated whole, without short-circuit: an AssertCount is checked once whenever its plan runs,
/// even where the logic around it (`false AND ...`, an empty outer relation) would never read its value. This is
/// stricter than LINQ to Objects, never laxer.
[<Fact>]
let ``AssertCount is checked without short-circuit`` () =
    let t = Tables()
    let failing = single t (bin BinaryOp.Gt (col "Id") (i64 2L)) exactlyOne
    Conformance.raises<InvalidOperationException> t
        (project failing [ "x", bin BinaryOp.And (bool false) (bin BinaryOp.Gt (col "v") (i64 0L)) ])
        "Sequence contains more than one element"

type FK = { K: int64 }

/// Every conjunct of a filter sees the filter's whole input: one reading other rows (here the row count) is not
/// evaluated on only the rows the other conjuncts keep.
[<Fact>]
let ``A filter's conjunct over other rows sees the filter's whole input`` () =
    let t = Tables()
    let rows = [| { K = 1L }; { K = 2L }; { K = 3L } |]
    let keep = bin BinaryOp.NotEq (col "K") (i64 2L)
    Conformance.rowsInOrder t (filter (t.Scan rows) (bin BinaryOp.And keep (bin BinaryOp.Eq len (i64 3L)))) [ [ 1L ]; [ 3L ] ]
    let t2 = Tables()
    Conformance.rowsInOrder t2 (filter (t2.Scan rows) (bin BinaryOp.And (bin BinaryOp.Eq len (i64 3L)) keep)) [ [ 1L ]; [ 3L ] ]
