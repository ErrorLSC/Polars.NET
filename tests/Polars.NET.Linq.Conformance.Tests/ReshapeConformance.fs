/// Contract for the reshaping nodes.
/// - Explode: one row per list element, rows in input order and elements in list order. An empty list gives a
///   null row with `emptyAsNull` (otherwise none); a null list gives a null row with `keepNulls` (otherwise none).
/// - Unpivot: columns `index..., variable, value`; one block of rows per `on` column, in `on` order, each block
///   in input row order.
/// - JoinAsOf: every left row once, in left order, with the right row whose `on` key is nearest by the strategy
///   (Backward: greatest key <= the left key; Forward: least >= ; Nearest: closest), among right rows with
///   equal `by` keys; `<=` / `>=` become `<` / `>` without AllowEq. No such row gives nulls. Both sides are
///   sorted by their `on` keys.
/// - GroupingSets: one block per level, in Levels order. A level groups by its subset of keys (groups in
///   first-key order; an empty subset is one grand-total row); keys outside the subset are null, and the level's
///   own literals set their columns.
module Polars.NET.Linq.Conformance.Tests.ReshapeConformance

open System
open System.Collections.Generic
open Xunit
open Kaminari.Samidare.IR

type LR = { Id: int64; Xs: List<Nullable<int64>> }

[<Fact>]
let ``Explode lists elements in order and handles empty and null lists by its flags`` () =
    let t = Tables()
    let rows: LR array =
        [| { Id = 1L; Xs = List [ n 10L; n 11L ] }; { Id = 2L; Xs = List() }; { Id = 3L; Xs = null }; { Id = 4L; Xs = List [ nul; n 40L ] } |]
    let explode emptyAsNull keepNulls = LogicalPlan.Explode(t.Scan rows, [ "Xs" ], emptyAsNull, keepNulls)
    Conformance.rowsInOrder t (explode true true) [ [ 1L; 10L ]; [ 1L; 11L ]; [ 2L; null ]; [ 3L; null ]; [ 4L; null ]; [ 4L; 40L ] ]
    Conformance.rowsInOrder t (explode false false) [ [ 1L; 10L ]; [ 1L; 11L ]; [ 4L; null ]; [ 4L; 40L ] ]

type WIDE = { Id: int64; A: int64; B: int64 }

[<Fact>]
let ``Unpivot lists one block per on column`` () =
    let t = Tables()
    let rows: WIDE array = [| { Id = 1L; A = 10L; B = 20L }; { Id = 2L; A = 11L; B = 21L } |]
    let plan = LogicalPlan.Unpivot(t.Scan rows, [ "Id" ], Some [ "B"; "A" ], "variable", "value")
    Conformance.columns t plan [ "Id"; "variable"; "value" ]
    Conformance.rowsInOrder t plan [ [ 1L; "B"; 20L ]; [ 2L; "B"; 21L ]; [ 1L; "A"; 10L ]; [ 2L; "A"; 11L ] ]

type LT = { Id: int64; G: int64; T: int64 }
type RT = { RG: int64; RT: int64; RV: string }

let private asOf strategy allowEq (by: bool) (t: Tables) =
    let left: LT array = [| { Id = 1L; G = 1L; T = 1L }; { Id = 2L; G = 2L; T = 4L }; { Id = 3L; G = 1L; T = 5L }; { Id = 4L; G = 1L; T = 10L } |]
    let right: RT array = [| { RG = 1L; RT = 2L; RV = "a" }; { RG = 2L; RT = 4L; RV = "b" }; { RG = 1L; RT = 8L; RV = "c" } |]
    let spec =
        {
            LeftOn = col "T"
            RightOn = col "RT"
            LeftBy = (if by then [ col "G" ] else [])
            RightBy = (if by then [ col "RG" ] else [])
            Strategy = strategy
            Tolerance = None
            AllowEq = allowEq
            CheckSorted = false
            Suffix = None
            Coalesce = JoinKeyCoalescing.Default
        }
    project (LogicalPlan.JoinAsOf(t.Scan left, t.Scan right, spec)) [ "Id", col "Id"; "RV", col "RV" ]

[<Fact>]
let ``AsOf join matches each left row once, in left order, by its strategy`` () =
    let t = Tables()
    Conformance.rowsInOrder t (asOf AsofStrategy.Backward true false t) [ [ 1L; null ]; [ 2L; "b" ]; [ 3L; "b" ]; [ 4L; "c" ] ]
    Conformance.rowsInOrder t (asOf AsofStrategy.Forward true false t) [ [ 1L; "a" ]; [ 2L; "b" ]; [ 3L; "c" ]; [ 4L; null ] ]
    Conformance.rowsInOrder t (asOf AsofStrategy.Nearest true false t) [ [ 1L; "a" ]; [ 2L; "b" ]; [ 3L; "b" ]; [ 4L; "c" ] ]

[<Fact>]
let ``AsOf join without AllowEq skips equal keys and by keys partition the match`` () =
    let t = Tables()
    Conformance.rowsInOrder t (asOf AsofStrategy.Backward false false t) [ [ 1L; null ]; [ 2L; "a" ]; [ 3L; "b" ]; [ 4L; "c" ] ]
    Conformance.rowsInOrder t (asOf AsofStrategy.Backward true true t) [ [ 1L; null ]; [ 2L; "b" ]; [ 3L; "a" ]; [ 4L; "c" ] ]

type SALE = { R: string; C: string; Amount: int64 }

let private sales: SALE array =
    [| { R = "east"; C = "x"; Amount = 1L }; { R = "west"; C = "y"; Amount = 2L }; { R = "east"; C = "y"; Amount = 4L }; { R = "east"; C = "x"; Amount = 8L } |]

let private groupingSets (t: Tables) levels =
    let spec =
        {
            KeyAliases = [ "R", col "R"; "C", col "C" ]
            Aggs = [ "total", agg AggKind.Sum (col "Amount") ]
            Levels = levels
            OutputColumns = [ "R"; "C"; "total"; "level" ]
            LevelMerges = None
        }
    LogicalPlan.GroupingSets(t.Scan sales, spec)

[<Fact>]
let ``GroupingSets lists one block per level, each with its own literals`` () =
    let t = Tables()
    let plan = groupingSets t [ [ 0; 1 ], Map [ "level", i64 7L ]; [ 0 ], Map [ "level", i64 1L ]; [], Map [ "level", i64 2L ] ]
    Conformance.rowsInOrder t plan [
        [ "east"; "x"; 9L; 7L ]; [ "west"; "y"; 2L; 7L ]; [ "east"; "y"; 4L; 7L ]
        [ "east"; null; 13L; 1L ]; [ "west"; null; 2L; 1L ]
        [ null; null; 15L; 2L ]
    ]

[<Fact>]
let ``GroupingSets without a level of every key`` () =
    let t = Tables()
    let plan = groupingSets t [ [ 1 ], Map [ "level", i64 1L ]; [], Map [ "level", i64 3L ] ]
    Conformance.rowsInOrder t plan [ [ null; "x"; 9L; 1L ]; [ null; "y"; 6L; 1L ]; [ null; null; 15L; 3L ] ]

type LT2 = { Id: int64; G: Nullable<int64>; T: Nullable<int64> }
type RT2 = { RG: Nullable<int64>; RT: Nullable<int64>; RV: string }

[<Fact>]
let ``AsOf join never matches a null on or by key`` () =
    let left: LT2 array = [| { Id = 1L; G = n 1L; T = nul }; { Id = 2L; G = nul; T = n 5L }; { Id = 3L; G = n 1L; T = n 5L } |]
    let right: RT2 array = [| { RG = n 1L; RT = n 2L; RV = "a" }; { RG = nul; RT = n 3L; RV = "n" }; { RG = n 1L; RT = nul; RV = "z" } |]
    let plan by (t: Tables) =
        let spec =
            {
                LeftOn = col "T"; RightOn = col "RT"
                LeftBy = (if by then [ col "G" ] else []); RightBy = (if by then [ col "RG" ] else [])
                Strategy = AsofStrategy.Backward; Tolerance = None
                AllowEq = true; CheckSorted = false; Suffix = None; Coalesce = JoinKeyCoalescing.Default
            }
        project (LogicalPlan.JoinAsOf(t.Scan left, t.Scan right, spec)) [ "Id", col "Id"; "RV", col "RV" ]
    let t1 = Tables()
    Conformance.rowsInOrder t1 (plan false t1) [ [ 1L; null ]; [ 2L; "n" ]; [ 3L; "n" ] ]
    let t2 = Tables()
    Conformance.rowsInOrder t2 (plan true t2) [ [ 1L; null ]; [ 2L; null ]; [ 3L; "a" ] ]

type L2 = { Id: int64; A: List<int64>; B: List<int64> }

[<Fact>]
let ``Explode of several columns pairs elements and fails on unequal lengths`` () =
    let t = Tables()
    let equal: L2 array = [| { Id = 1L; A = List [ 1L; 2L ]; B = List [ 10L; 20L ] }; { Id = 2L; A = List [ 3L ]; B = List [ 30L ] } |]
    Conformance.rowsInOrder t (LogicalPlan.Explode(t.Scan equal, [ "A"; "B" ], true, true)) [ [ 1L; 1L; 10L ]; [ 1L; 2L; 20L ]; [ 2L; 3L; 30L ] ]
    let t2 = Tables()
    let unequal: L2 array = [| { Id = 1L; A = List [ 1L; 2L ]; B = List [ 10L ] } |]
    for engine in Engines.all do
        Assert.ThrowsAny<exn>(fun () -> engine.Execute(LogicalPlan.Explode(t2.Scan unequal, [ "A"; "B" ], true, true), t2.Sources) |> ignore) |> ignore

[<Fact>]
let ``Over no rows a grouping has no groups, and a grand-total level still has its row`` () =
    let t = Tables()
    let none = filter (t.Scan sales) (bin BinaryOp.Eq (col "R") (str "north"))
    Conformance.rowsInOrder t (LogicalPlan.GroupBy(none, [ col "R" ], [ "n", len ], None, true)) []
    Conformance.rowsInOrder t (project none [ "n", len; "total", agg AggKind.Sum (col "Amount") ]) [ [ 0L; 0L ] ]
    let spec =
        {
            KeyAliases = [ "R", col "R" ]
            Aggs = [ "n", len; "total", agg AggKind.Sum (col "Amount") ]
            Levels = [ [ 0 ], Map.empty; [], Map.empty ]
            OutputColumns = [ "R"; "n"; "total" ]
            LevelMerges = None
        }
    Conformance.rowsInOrder t (LogicalPlan.GroupingSets(none, spec)) [ [ null; 0L; 0L ] ]
