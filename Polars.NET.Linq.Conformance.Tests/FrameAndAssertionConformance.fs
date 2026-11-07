/// Contract: an expression that reads other rows (a window, rank, cumulative function) sees the input of the
/// node that computes it, so a Filter above that node never changes its values. AssertCount yields its value
/// when the count lies in range, and otherwise fails the query with the check's error.
module Polars.NET.Linq.Conformance.Tests.FrameAndAssertionConformance

open System
open Xunit
open Polars.NET.Linq.IR

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
