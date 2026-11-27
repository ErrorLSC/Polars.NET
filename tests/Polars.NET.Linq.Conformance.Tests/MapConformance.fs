/// Contract (SEMANTICS.md, Maps): a map holds entries in order, keys never null. MapGet is null where the map lacks the
/// key or is null; MapContainsKey, MapLen, MapKeys and MapValues are null for a null map; MapOf keeps a repeated key's
/// first position and its last value; maps are equal entry by entry, in entry order.
module Polars.NET.Linq.Conformance.Tests.MapConformance

open System
open System.Collections.Generic
open Xunit
open Kaminari.Samidare.IR

type MR = { Id: int64; M: Dictionary<string, Nullable<int64>> }

let private rows: MR array =
    [|
        { Id = 1L; M = Dictionary(dict [ "a", Nullable 1L; "b", Nullable 2L ]) }
        { Id = 2L; M = null }
        { Id = 3L; M = Dictionary() }
        { Id = 4L; M = Dictionary(dict [ "c", Nullable() ]) }
    |]

let private mapOf (entries: (string * int64) list) =
    let m = Apache.Arrow.Types.MapType(Apache.Arrow.Field("key", Apache.Arrow.Types.StringViewType.Default, false), Apache.Arrow.Field("value", Apache.Arrow.Types.Int64Type.Default, true))
    ScalarExpr.Function(ScalarFunction.MapOf, entries |> List.collect (fun (k, v) -> [ str k; i64 v ]), LogicalType.Arrow m)

[<Fact>]
let ``MapGet is null where the key is missing or the map is null`` () =
    let t = Tables()
    let plan =
        project (t.Scan rows) [
            "a", fn ScalarFunction.MapGet [ col "M"; str "a" ]
            "c", fn ScalarFunction.MapGet [ col "M"; str "c" ]
            "has", fn ScalarFunction.MapContainsKey [ col "M"; str "c" ]
            "len", fn ScalarFunction.MapLen [ col "M" ]
            "firstKey", fn ScalarFunction.ListGet [ fn ScalarFunction.MapKeys [ col "M" ]; i64 0L ]
            "values", fn ScalarFunction.ListLen [ fn ScalarFunction.MapValues [ col "M" ] ]
        ]
    Conformance.rowsInOrder t plan [
        [ 1L; null; false; 2L; "a"; 2L ]
        [ null; null; null; null; null; null ]
        [ null; null; false; 0L; null; 0L ]
        [ null; null; true; 1L; "c"; 1L ]
    ]

[<Fact>]
let ``MapOf keeps a repeated key's first position and its last value`` () =
    let t = Tables()
    let built = mapOf [ "x", 1L; "y", 2L; "x", 5L ]
    let plan =
        project (filter (t.Scan rows) (bin BinaryOp.Eq (col "Id") (i64 1L))) [
            "x", fn ScalarFunction.MapGet [ built; str "x" ]
            "first", fn ScalarFunction.ListGet [ fn ScalarFunction.MapKeys [ built ]; i64 0L ]
            "len", fn ScalarFunction.MapLen [ built ]
        ]
    Conformance.rowsInOrder t plan [ [ 5L; "x"; 2L ] ]

[<Fact>]
let ``Maps are equal entry by entry in entry order`` () =
    let t = Tables()
    let plan =
        project (filter (t.Scan rows) (bin BinaryOp.Eq (col "Id") (i64 1L))) [
            "same", bin BinaryOp.Eq (col "M") (mapOf [ "a", 1L; "b", 2L ])
            "reordered", bin BinaryOp.Eq (col "M") (mapOf [ "b", 2L; "a", 1L ])
        ]
    Conformance.rowsInOrder t plan [ [ true; false ] ]
