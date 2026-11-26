/// Contract (SEMANTICS.md, Casts): a non-strict Cast of text to a date, a timestamp or a time of day reads the ISO 8601
/// forms of that type after trimming the text (a date; a date and a time after `T` or a space, seconds and their
/// fraction optional; a time, seconds optional), and gives null for other text. A date is the date of a timestamp's
/// text.
module Polars.NET.Linq.Conformance.Tests.TextCastConformance

open System
open Xunit
open Kaminari.Samidare.IR

type TR = { Id: int64; S: string }

let private rows: TR array =
    [|
        { Id = 1L; S = "2001-02-03" }
        { Id = 2L; S = " 2001-02-03 04:05:06.25 " }
        { Id = 3L; S = "2001-02-03T04:05" }
        { Id = 4L; S = " 04:05:06.5 " }
        { Id = 5L; S = "x" }
        { Id = 6L; S = null }
    |]

[<Fact>]
let ``Text casts to dates, timestamps and times by the ISO forms`` () =
    let t = Tables()
    let cast target = ScalarExpr.Cast(col "S", target, false)
    let plan = project (t.Scan rows) [ "date", cast LogicalTypes.Date; "timestamp", cast LogicalTypes.Datetime; "time", cast LogicalTypes.Time ]
    let at h m s ms = DateTime(2001, 2, 3, h, m, s).AddMilliseconds(float ms)
    Conformance.rowsInOrder t plan [
        [ box (DateOnly(2001, 2, 3)); box (at 0 0 0 0); null ]
        [ box (DateOnly(2001, 2, 3)); box (at 4 5 6 250); null ]
        [ box (DateOnly(2001, 2, 3)); box (at 4 5 0 0); null ]
        [ null; null; box (TimeOnly(4, 5, 6, 500)) ]
        [ null; null; null ]
        [ null; null; null ]
    ]
