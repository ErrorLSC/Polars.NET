/// Contract: IR comparisons over floating values follow the engine's total order, in which NaN equals NaN and is
/// greater than every number; the frontend compensates for .NET semantics (see NaNSemantics). IsNan is true for
/// NaN, false for a number, null for null. Min is .NET Min (NaN as soon as one value is NaN); Max skips NaN unless
/// every value is NaN. Float16 literals compare with Half columns exactly. Grouping and Distinct compare floating keys
/// as .NET Equals does: every NaN is one value (whatever its payload or sign) and -0.0 equals 0.0.
/// DecimalToFloating is .NET 11's (double)decimal / (float)decimal: correctly rounded, once. FloatingToDecimal is .NET
/// 11's (decimal)x (the exact binary value, rounded half to even where System.Decimal's digits end) rounded half to even
/// at the target scale; NaN, an infinity or a value beyond the precision raise OverflowException.
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

type DX = { Id: int64; M: Nullable<decimal>; X: Nullable<float>; F: Nullable<float32> }

let private conversions: DX array =
    [|
        { Id = 1L; M = Nullable 0.1m; X = Nullable 0.1; F = Nullable 0.1f }
        // M's nearest double is the midpoint of 1 and the next float (which ties to 1); System.Decimal keeps X to 19
        // fractional digits, its last a 5 that ties to even at 18.
        { Id = 2L; M = Nullable 1.000000059604644776m; X = Nullable 945001529.969741; F = Nullable 16777216.0f }
        { Id = 3L; M = Nullable(); X = Nullable(); F = Nullable() }
    |]

[<Fact>]
let ``DecimalToFloating rounds once, FloatingToDecimal takes the exact value to the scale`` () =
    let t = Tables()
    let toFloat target e = ScalarExpr.Function(ScalarFunction.DecimalToFloating, [ e ], target)
    let toDecimal e = ScalarExpr.Function(ScalarFunction.FloatingToDecimal, [ e ], LogicalTypes.Decimal)
    let plan =
        project (t.Scan conversions) [
            "d", toFloat LogicalTypes.Float64 (col "M")
            "f", toFloat LogicalTypes.Float32 (col "M")
            "m", toDecimal (col "X")
            "mf", toDecimal (col "F")
        ]
    Conformance.rowsInOrder t plan [
        [ 0.1; float 0.1f; 0.100000000000000006m; 0.100000001490116119m ]
        [ 1.0000000596046448; float 1.0000001f; 945001529.969740986824035644m; 16777216m ]
        [ null; null; null; null ]
    ]

[<Fact>]
let ``FloatingToDecimal raises Overflow on NaN and beyond the precision`` () =
    for x in [ nan; infinity; 1e20 ] do
        let t = Tables()
        let rows: DX array = [| { Id = 1L; M = Nullable(); X = Nullable x; F = Nullable() } |]
        let plan = project (t.Scan rows) [ "m", ScalarExpr.Function(ScalarFunction.FloatingToDecimal, [ col "X" ], LogicalTypes.Decimal) ]
        Conformance.raises<OverflowException> t plan "Value was either too large or too small for a Decimal."
