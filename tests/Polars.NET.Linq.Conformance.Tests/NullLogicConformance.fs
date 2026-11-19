/// Contract: three-valued (Kleene) logic. A comparison with a null operand is null, except EqMissing /
/// NotEqMissing, which are two-valued. Filter keeps only the rows whose predicate is true. A conditional whose
/// test is null takes its else branch. Grouping and Distinct take null as one key value: rows with a null key form one
/// group, and a composite key matches a null component with a null component.
module Polars.NET.Linq.Conformance.Tests.NullLogicConformance

open System
open Xunit
open Kaminari.Samidare.IR

type NV = { Id: int64; V: Nullable<int64> }
type AB = { Id: int64; A: Nullable<bool>; B: Nullable<bool> }
type XY = { Id: int64; X: Nullable<int64>; Y: Nullable<int64> }

let private values: NV array = [| { Id = 1L; V = n 1L }; { Id = 2L; V = nul }; { Id = 3L; V = n 3L } |]

[<Fact>]
let ``A comparison with a null operand is null`` () =
    let t = Tables()
    let plan =
        project (t.Scan values) [
            "Id", col "Id"
            "gt", bin BinaryOp.Gt (col "V") (i64 2L)
            "eq", bin BinaryOp.Eq (col "V") (i64 1L)
            "ne", bin BinaryOp.NotEq (col "V") (i64 1L)
        ]
    Conformance.rowsInOrder t plan [
        [ 1L; false; true; false ]
        [ 2L; null; null; null ]
        [ 3L; true; false; true ]
    ]

[<Fact>]
let ``And, Or and Not follow Kleene logic`` () =
    let t = Tables()
    let vs = [ Nullable true; Nullable false; Nullable() ]
    let rows = [| for a in vs do for b in vs -> { AB.Id = 0L; A = a; B = b } |] |> Array.mapi (fun i r -> { r with Id = int64 i })
    let plan =
        project (t.Scan rows) [
            "and", bin BinaryOp.And (col "A") (col "B")
            "or", bin BinaryOp.Or (col "A") (col "B")
            "notA", not' (col "A")
        ]
    let T, F, N = box true, box false, null
    Conformance.rowsInOrder t plan [
        [ T; T; F ]; [ F; T; F ]; [ N; T; F ]
        [ F; T; T ]; [ F; F; T ]; [ F; N; T ]
        [ N; T; N ]; [ F; N; N ]; [ N; N; N ]
    ]

[<Fact>]
let ``EqMissing and NotEqMissing are two-valued, null equal to null`` () =
    let t = Tables()
    let rows: XY array =
        [| { Id = 1L; X = n 1L; Y = n 1L }; { Id = 2L; X = n 1L; Y = nul }; { Id = 3L; X = nul; Y = nul }; { Id = 4L; X = n 1L; Y = n 2L } |]
    let plan =
        project (t.Scan rows) [
            "eq", bin BinaryOp.EqMissing (col "X") (col "Y")
            "ne", bin BinaryOp.NotEqMissing (col "X") (col "Y")
        ]
    Conformance.rowsInOrder t plan [ [ true; false ]; [ false; true ]; [ true; false ]; [ false; true ] ]

[<Fact>]
let ``Filter keeps only the rows whose predicate is true`` () =
    let t = Tables()
    let plan = project (filter (t.Scan values) (not' (bin BinaryOp.Gt (col "V") (i64 2L)))) [ "Id", col "Id" ]
    Conformance.rowsInOrder t plan [ [ 1L ] ]

[<Fact>]
let ``Null and false agree in a filter, so a lifted comparison made two-valued filters the same`` () =
    let t = Tables()
    let twoValued = ScalarExpr.Coalesce [ bin BinaryOp.Gt (col "V") (i64 2L); bool false ]
    let keep = project (filter (t.Scan values) twoValued) [ "Id", col "Id" ]
    let negated = project (filter (t.Scan values) (not' twoValued)) [ "Id", col "Id" ]
    Conformance.rowsInOrder t keep [ [ 3L ] ]
    Conformance.rowsInOrder t negated [ [ 1L ]; [ 2L ] ]

[<Fact>]
let ``A conditional with a null test takes the else branch`` () =
    let t = Tables()
    let plan =
        project (t.Scan values) [ "c", ScalarExpr.Conditional(bin BinaryOp.Gt (col "V") (i64 2L), str "big", str "small") ]
    Conformance.rowsInOrder t plan [ [ "small" ]; [ "small" ]; [ "big" ] ]

[<Fact>]
let ``Coalesce takes the first non-null value`` () =
    let t = Tables()
    let plan = project (t.Scan values) [ "c", ScalarExpr.Coalesce [ col "V"; i64 0L ] ]
    Conformance.rowsInOrder t plan [ [ 1L ]; [ 0L ]; [ 3L ] ]

type IJ = { Id: int64; I: int64; J: int64 }

/// .NET arithmetic: Modulo is the truncated remainder (the dividend's sign) and TruncatedDivide truncates toward
/// zero, where the engine floors.
[<Fact>]
let ``Modulo and TruncatedDivide truncate toward zero`` () =
    let t = Tables()
    let rows: IJ array = [| { Id = 1L; I = -7L; J = 2L }; { Id = 2L; I = 7L; J = -2L }; { Id = 3L; I = -8L; J = -3L }; { Id = 4L; I = 7L; J = 0L } |]
    let plan = project (t.Scan rows) [ "div", bin BinaryOp.TruncatedDivide (col "I") (col "J"); "rem", bin BinaryOp.Modulo (col "I") (col "J") ]
    Conformance.rowsInOrder t plan [ [ -3L; -1L ]; [ -3L; 1L ]; [ 2L; -2L ]; [ null; null ] ]

type IJ32 = { Id: int64; I: int; J: int }

/// Integer overflow wraps in the operands' type: MinValue / -1 is MinValue and MinValue % -1 is 0; Add and Multiply
/// wrap (SEMANTICS.md, Arithmetic).
[<Fact>]
let ``Integer overflow wraps, MinValue over -1 too`` () =
    let t = Tables()
    let rows: IJ32 array = [| { Id = 1L; I = Int32.MinValue; J = -1 }; { Id = 2L; I = Int32.MaxValue; J = 2 } |]
    let plan =
        project (t.Scan rows)
            [ "div", bin BinaryOp.TruncatedDivide (col "I") (col "J")
              "rem", bin BinaryOp.Modulo (col "I") (col "J")
              "sum", bin BinaryOp.Add (col "I") (col "I")
              "prod", bin BinaryOp.Multiply (col "I") (col "J") ]
    Conformance.rowsInOrder t plan
        [ [ int64 Int32.MinValue; 0L; 0L; int64 Int32.MinValue ]
          [ int64 (Int32.MaxValue / 2); 1L; -2L; -2L ] ]

type Widths = { Id: int64; I: int; L: int64; U: uint32; F: float32; D: float }

/// Numbers of two widths compare by value: Int32 with Int64, UInt32 with Int32, Float32 widened against Float64.
[<Fact>]
let ``Comparisons across widths compare values`` () =
    let t = Tables()
    let rows: Widths array =
        [| { Id = 1L; I = -1; L = 4294967295L; U = 4294967295u; F = 0.1f; D = 0.1 }
           { Id = 2L; I = 7; L = 7L; U = 7u; F = 0.5f; D = 0.5 } |]
    let plan =
        project (t.Scan rows)
            [ "il", bin BinaryOp.Lt (col "I") (col "L")
              "ui", bin BinaryOp.Gt (col "U") (col "I")
              "fd", bin BinaryOp.Eq (col "F") (col "D") ]
    // (double)0.1f is not 0.1; 0.5f is 0.5 exactly.
    Conformance.rowsInOrder t plan [ [ true; true; false ]; [ false; false; true ] ]

type GK = { Id: int64; A: Nullable<int64>; B: string }

let private nullKeys: GK array =
    [|
        { Id = 1L; A = nul; B = "x" }
        { Id = 2L; A = n 1L; B = null }
        { Id = 3L; A = nul; B = "x" }
        { Id = 4L; A = nul; B = null }
        { Id = 5L; A = n 1L; B = null }
        { Id = 6L; A = nul; B = null }
        { Id = 7L; A = n 1L; B = "x" }
    |]

[<Fact>]
let ``Null keys group together`` () =
    let t = Tables()
    let byA = LogicalPlan.GroupBy(t.Scan nullKeys, [ col "A" ], [ "n", len; "first", agg AggKind.First (col "Id") ], None, true)
    Conformance.rowsInOrder t byA [ [ null; 4L; 1L ]; [ 1L; 3L; 2L ] ]
    let t2 = Tables()
    let byAB = LogicalPlan.GroupBy(t2.Scan nullKeys, [ col "A"; col "B" ], [ "n", len; "first", agg AggKind.First (col "Id") ], None, true)
    Conformance.rowsInOrder t2 byAB [ [ null; "x"; 2L; 1L ]; [ 1L; null; 2L; 2L ]; [ null; null; 2L; 4L ]; [ 1L; "x"; 1L; 7L ] ]

[<Fact>]
let ``Distinct takes null as one key value`` () =
    let t = Tables()
    let plan = project (LogicalPlan.Distinct(t.Scan nullKeys, Some [ "A"; "B" ], DistinctKeep.First, true)) [ "Id", col "Id" ]
    Conformance.rowsInOrder t plan [ [ 1L ]; [ 2L ]; [ 4L ]; [ 7L ] ]
