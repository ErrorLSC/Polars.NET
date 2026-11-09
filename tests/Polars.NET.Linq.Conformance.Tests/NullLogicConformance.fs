/// Contract: three-valued (Kleene) logic. A comparison with a null operand is null, except EqMissing /
/// NotEqMissing, which are two-valued. Filter keeps only the rows whose predicate is true. A conditional whose
/// test is null takes its else branch.
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
