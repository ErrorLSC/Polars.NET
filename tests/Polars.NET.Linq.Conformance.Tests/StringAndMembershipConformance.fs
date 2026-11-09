/// Contract: .NET string building reads a null as the empty string (StringConcat, ConcatString, FormatString,
/// ListJoin). Membership tests (InList, ListContains) match a null to a null exactly when `nullsEqual`.
module Polars.NET.Linq.Conformance.Tests.StringAndMembershipConformance

open System
open System.Collections.Generic
open Xunit
open Kaminari.Samidare.IR

type SR = { Id: int64; A: string; B: string; Tags: List<string> }

let private rows: SR array =
    [|
        { Id = 1L; A = "x"; B = null; Tags = List [ "a"; null; "b" ] }
        { Id = 2L; A = null; B = "y"; Tags = List [ null ] }
        { Id = 3L; A = null; B = null; Tags = List [ "c" ] }
        { Id = 4L; A = "z"; B = "w"; Tags = List [] }
    |]

[<Fact>]
let ``String building reads null as empty`` () =
    let t = Tables()
    let plan =
        project (t.Scan rows) [
            "plus", bin BinaryOp.StringConcat (col "A") (col "B")
            "concat", fn ScalarFunction.ConcatString [ str ","; col "A"; col "B" ]
            "format", fn ScalarFunction.FormatString [ str "{}|{}"; col "A"; col "B" ]
            "join", fn ScalarFunction.ListJoin [ col "Tags"; str "," ]
        ]
    Conformance.rowsInOrder t plan [
        [ "x"; "x,"; "x|"; "a,,b" ]
        [ "y"; ",y"; "|y"; "" ]
        [ ""; ","; "|"; "c" ]
        [ "zw"; "z,w"; "z|w"; "" ]
    ]

[<Fact>]
let ``InList matches null to null exactly when nullsEqual`` () =
    let t = Tables()
    let source = InListSource.Collection([| "x"; null |], typeof<string>)
    let plan =
        project (t.Scan rows) [
            "csharp", ScalarExpr.InList(col "A", source, false, true)
            "sql", ScalarExpr.InList(col "A", source, false, false)
            "notIn", ScalarExpr.InList(col "A", source, true, true)
        ]
    Conformance.rowsInOrder t plan [
        [ true; true; false ]
        [ true; null; false ]
        [ true; null; false ]
        [ false; false; true ]
    ]

[<Fact>]
let ``ListContains matches a null item to a null element exactly when nullsEqual`` () =
    let t = Tables()
    let nullItem = ScalarExpr.Literal(LiteralValue.Null LogicalTypes.String)
    let plan =
        project (t.Scan rows) [
            "csharp", fn (ScalarFunction.ListContains true) [ col "Tags"; nullItem ]
            "value", fn (ScalarFunction.ListContains true) [ col "Tags"; str "c" ]
        ]
    Conformance.rowsInOrder t plan [ [ true; false ]; [ true; false ]; [ false; true ]; [ false; false ] ]
