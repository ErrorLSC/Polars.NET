/// Contract (SEMANTICS.md, Checked rescale; Date and time): the nodes the Spark expression profile binds to.
/// CheckedRescale is ANSI SQL's CAST to a decimal: rounded half away from zero at the target's scale, failing with
/// Overflow beyond its precision on a guarded row, null elsewhere. DtInZone reads an instant as a zone's wall clock.
module Polars.NET.Linq.Conformance.Tests.SparkFunctionConformance

open System
open Xunit
open Kaminari.Samidare.IR

type RD = { K: int64; D: Nullable<decimal> }

let private overflow = "Arithmetic operation resulted in an overflow."
let private rescale p s guard value = ScalarExprOps.checkedRescale (LogicalTypes.decimalOf p s) guard value
let private asDecimal p s e = ScalarExpr.Cast(e, LogicalTypes.decimalOf p s, false)

let private rows: RD array =
    [|
        { K = 1L; D = Nullable 2.5m }
        { K = 2L; D = Nullable -2.5m }
        { K = 3L; D = Nullable 2.4999m }
        { K = 4L; D = Nullable() }
        { K = 5L; D = Nullable 99.9949m }
    |]

[<Fact>]
let ``CheckedRescale rounds half away from zero at the target's scale`` () =
    let t = Tables()
    let d = asDecimal 6 4 (col "D")
    let plan = project (t.Scan rows) [ "r0", rescale 3 0 ScalarExprOps.unguarded d; "r2", rescale 4 2 ScalarExprOps.unguarded d; "up", rescale 9 6 ScalarExprOps.unguarded d ]
    Conformance.rowsInOrder t plan [
        [ 3m; 2.50m; 2.500000m ]
        [ -3m; -2.50m; -2.500000m ]
        [ 2m; 2.50m; 2.499900m ]
        [ null; null; null ]
        [ 100m; 99.99m; 99.994900m ]
    ]

[<Fact>]
let ``CheckedRescale fails beyond the target's precision on a guarded row only`` () =
    let d = asDecimal 6 4 (col "D")
    for plan in [ (fun (t: Tables) -> project (t.Scan rows) [ "r", rescale 2 0 ScalarExprOps.unguarded d ])
                  // 99.9949 rounds to 99.995 at scale 3: five digits, beyond DECIMAL(4, 3).
                  (fun (t: Tables) -> project (t.Scan rows) [ "r", rescale 4 3 ScalarExprOps.unguarded d ]) ] do
        let t = Tables()
        Conformance.raises<OverflowException> t (plan t) overflow
        let t2 = Tables()
        Conformance.raises<OverflowException> t2 (LogicalOptimizer.optimize (plan t2)) overflow
    let t = Tables()
    let small = bin BinaryOp.Lt (ScalarExpr.Unary(UnaryOp.Abs, col "D")) (ScalarExpr.Literal(LiteralValue.Decimal 10m))
    let guarded = RaisingGuards.ofExpr (ScalarExpr.Conditional(small, rescale 2 0 ScalarExprOps.unguarded d, ScalarExpr.Literal(LiteralValue.Null(LogicalTypes.decimalOf 2 0))))
    Conformance.rowsInOrder t (project (t.Scan rows) [ "r", guarded ]) [ [ 3m ]; [ -3m ]; [ 2m ]; [ null ]; [ null ] ]

type TZ = { K: int64; T: Nullable<DateTimeOffset> }

[<Fact>]
let ``DtInZone reads an instant as the zone's wall clock`` () =
    let t = Tables()
    let instants: TZ array =
        [|
            { K = 1L; T = Nullable(DateTimeOffset(2021, 3, 5, 4, 0, 0, TimeSpan.Zero)) }
            // Daylight saving time in Los Angeles: UTC-7.
            { K = 2L; T = Nullable(DateTimeOffset(2021, 7, 1, 6, 30, 0, TimeSpan.Zero)) }
            { K = 3L; T = Nullable() }
        |]
    let inZone zone = ScalarExpr.Function(ScalarFunction.DtInZone zone, [ col "T" ], LogicalTypes.Datetime)
    let la = inZone "America/Los_Angeles"
    let plan =
        project (t.Scan instants) [
            "d", ScalarExpr.Function(ScalarFunction.DtDay, [ la ], LogicalTypes.Int32)
            "h", ScalarExpr.Function(ScalarFunction.DtHour, [ la ], LogicalTypes.Int32)
            "utc", ScalarExpr.Function(ScalarFunction.DtHour, [ inZone "UTC" ], LogicalTypes.Int32)
        ]
    Conformance.rowsInOrder t plan [ [ 4; 20; 4 ]; [ 30; 23; 6 ]; [ null; null; null ] ]
