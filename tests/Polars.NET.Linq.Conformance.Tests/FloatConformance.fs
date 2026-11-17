/// Contract: IR comparisons over floating values follow the engine's total order, in which NaN equals NaN and is
/// greater than every number; the frontend compensates for .NET semantics (see NaNSemantics). IsNan is true for
/// NaN, false for a number, null for null. Min is .NET Min (NaN as soon as one value is NaN); Max skips NaN unless
/// every value is NaN. Float16 literals compare with Half columns exactly. Grouping and Distinct compare floating keys
/// as .NET Equals does: every NaN is one value (whatever its payload or sign) and -0.0 equals 0.0.
module Polars.NET.Linq.Conformance.Tests.FloatConformance

open System
open Xunit
open Kaminari.Samidare.IR

type FX = { Id: int64; X: Nullable<float>; Y: Nullable<float> }

let private rows: FX array =
    [|
        { Id = 1L; X = Nullable nan; Y = Nullable nan }
        { Id = 2L; X = Nullable nan; Y = Nullable 1.0 }
        { Id = 3L; X = Nullable 1.0; Y = Nullable() }
        { Id = 4L; X = Nullable(); Y = Nullable 2.0 }
    |]

let private f64 v = ScalarExpr.Literal(LiteralValue.Float64 v)

[<Fact>]
let ``Comparisons follow the total order, NaN equal to NaN and greatest`` () =
    let t = Tables()
    let plan =
        project (t.Scan rows) [
            "eq", bin BinaryOp.Eq (col "X") (col "Y")
            "gt", bin BinaryOp.Gt (col "X") (f64 5.0)
            "lt", bin BinaryOp.Lt (col "X") (f64 5.0)
            "isNan", ScalarExpr.Unary(UnaryOp.IsNan, col "X")
        ]
    Conformance.rowsInOrder t plan [
        [ true; true; false; true ]
        [ false; true; false; true ]
        [ null; false; true; false ]
        [ null; null; null; null ]
    ]

[<Fact>]
let ``Min is NaN as soon as one value is NaN, Max skips NaN`` () =
    let t = Tables()
    let plan = project (t.Scan rows) [ "min", agg AggKind.Min (col "X"); "max", agg AggKind.Max (col "X"); "maxY", agg AggKind.Max (col "Y") ]
    Conformance.rowsInOrder t plan [ [ nan; 1.0; 2.0 ] ]
    let t2 = Tables()
    let onlyNaN = filter (t2.Scan rows) (bin BinaryOp.Eq (col "Id") (i64 1L))
    Conformance.rowsInOrder t2 (project onlyNaN [ "min", agg AggKind.Min (col "X"); "max", agg AggKind.Max (col "X") ]) [ [ nan; nan ] ]

type HX = { Id: int64; H: Nullable<Half> }

[<Fact>]
let ``Float16 literals compare with Half values exactly`` () =
    let t = Tables()
    let hs: HX array = [| { Id = 1L; H = Nullable(Half.op_Explicit 0.5) }; { Id = 2L; H = Nullable(Half.op_Explicit 2.0) }; { Id = 3L; H = Nullable() } |]
    let plan = project (t.Scan hs) [ ("gt", bin BinaryOp.Gt (col "H") (ScalarExpr.Literal(LiteralValue.Float16(Half.op_Explicit 1.0)))) ]
    Conformance.rowsInOrder t plan [ [ false ]; [ true ]; [ null ] ]

type GF = { Id: int64; X: Nullable<float> }

/// NaNs of other payloads and signs than Double.NaN's.
let private otherNaN = BitConverter.Int64BitsToDouble 0x7FF8000000000001L
let private negativeNaN = BitConverter.Int64BitsToDouble 0xFFF8000000000000L

let private floatKeys: GF array =
    [|
        { Id = 1L; X = Nullable 0.0 }
        { Id = 2L; X = Nullable -0.0 }
        { Id = 3L; X = Nullable nan }
        { Id = 4L; X = Nullable otherNaN }
        { Id = 5L; X = Nullable 1.0 }
        { Id = 6L; X = Nullable negativeNaN }
        { Id = 7L; X = Nullable -0.0 }
    |]

[<Fact>]
let ``Grouping takes -0.0 as 0.0 and every NaN as one value`` () =
    let t = Tables()
    let plan = LogicalPlan.GroupBy(t.Scan floatKeys, [ col "X" ], [ "n", len; "first", agg AggKind.First (col "Id") ], None, true)
    Conformance.rowsInOrder t plan [ [ 0.0; 3L; 1L ]; [ nan; 3L; 3L ]; [ 1.0; 1L; 5L ] ]

[<Fact>]
let ``Distinct takes -0.0 as 0.0 and every NaN as one value`` () =
    let t = Tables()
    let plan = project (LogicalPlan.Distinct(t.Scan floatKeys, Some [ "X" ], DistinctKeep.First, true)) [ "Id", col "Id" ]
    Conformance.rowsInOrder t plan [ [ 1L ]; [ 3L ]; [ 5L ] ]
