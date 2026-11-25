/// Contract (SEMANTICS.md, Checked division and Guards): CheckedDivide is ANSI SQL's division. Null for a null operand
/// and off the guard's rows; on a guarded row a zero divisor of a non-null dividend fails with DivideByZero, a quotient
/// out of range with Overflow. IntegerDivide and Remainder truncate (the remainder has the dividend's sign); a decimal
/// quotient is rounded half away from zero at the result's scale. RaisingGuards gives each raising node the conditions
/// under which CASE, AND, OR and COALESCE reach it, so a short-circuited row never fails.
module Polars.NET.Linq.Conformance.Tests.CheckedDivisionConformance

open System
open Xunit
open Kaminari.Samidare.IR

type DZ = { K: int64; A: Nullable<int64>; Z: Nullable<int64>; X: Nullable<float>; Y: Nullable<float> }

let private rows: DZ array =
    [|
        { K = 1L; A = n 7L; Z = n 2L; X = Nullable 7.5; Y = Nullable 2.0 }
        { K = 2L; A = n -7L; Z = n 2L; X = Nullable -7.5; Y = Nullable 2.0 }
        { K = 3L; A = nul; Z = n 0L; X = Nullable(); Y = Nullable 0.0 }
        { K = 4L; A = n 7L; Z = n -3L; X = Nullable 1.0; Y = Nullable nan }
        { K = 5L; A = n 5L; Z = nul; X = Nullable 5.0; Y = Nullable() }
    |]

let private div kind t guard a b = ScalarExprOps.checkedDivide kind t guard a b
let private unguarded = ScalarExprOps.unguarded
let private divideByZero = "Attempted to divide by zero."
let private overflow = "Arithmetic operation resulted in an overflow."
let private f64 (v: float) = ScalarExpr.Literal(LiteralValue.Float64 v)

[<Fact>]
let ``IntegerDivide and Remainder truncate, null for a null operand`` () =
    let t = Tables()
    let plan =
        project (t.Scan rows) [
            "q", div DivisionKind.IntegerDivide LogicalTypes.Int64 unguarded (col "A") (col "Z")
            "r", div DivisionKind.Remainder LogicalTypes.Int64 unguarded (col "A") (col "Z")
        ]
    Conformance.rowsInOrder t plan [ [ 3L; 1L ]; [ -3L; -1L ]; [ null; null ]; [ -2L; 1L ]; [ null; null ] ]

[<Fact>]
let ``Floating Divide and Remainder are IEEE, a NaN divisor divides`` () =
    let t = Tables()
    let plan =
        project (t.Scan rows) [
            "q", div DivisionKind.Divide LogicalTypes.Float64 unguarded (col "X") (col "Y")
            "r", div DivisionKind.Remainder LogicalTypes.Float64 unguarded (col "X") (col "Y")
        ]
    Conformance.rowsInOrder t plan [ [ 3.75; 1.5 ]; [ -3.75; -1.5 ]; [ null; null ]; [ nan; nan ]; [ null; null ] ]

type DD = { K: int64; D: Nullable<decimal>; E: Nullable<decimal> }

[<Fact>]
let ``A decimal quotient is rounded half away from zero at the result's scale`` () =
    let t = Tables()
    let ds: DD array =
        [|
            { K = 1L; D = Nullable 1m; E = Nullable 3m }
            { K = 2L; D = Nullable -2m; E = Nullable 3m }
            { K = 3L; D = Nullable 5m; E = Nullable 2000m }
            { K = 4L; D = Nullable(); E = Nullable 0m }
        |]
    let plan =
        project (t.Scan ds) [
            "q", div DivisionKind.Divide (LogicalTypes.decimalOf 38 2) unguarded (col "D") (col "E")
            "r", div DivisionKind.Remainder LogicalTypes.Decimal unguarded (col "D") (col "E")
        ]
    Conformance.rowsInOrder t plan [ [ 0.33m; 1m ]; [ -0.67m; -2m ]; [ 0.00m; 5m ]; [ null; null ] ]

[<Fact>]
let ``A zero divisor of a value fails with DivideByZero, for every kind`` () =
    let zeroRow = [| { K = 1L; A = n 1L; Z = n 0L; X = Nullable 1.0; Y = Nullable -0.0 } |]
    for kind in [ DivisionKind.IntegerDivide; DivisionKind.Remainder ] do
        let t = Tables()
        Conformance.raises<DivideByZeroException> t (project (t.Scan zeroRow) [ "q", div kind LogicalTypes.Int64 unguarded (col "A") (col "Z") ]) divideByZero
    for kind in [ DivisionKind.Divide; DivisionKind.Remainder ] do
        let t = Tables()
        Conformance.raises<DivideByZeroException> t (project (t.Scan zeroRow) [ "q", div kind LogicalTypes.Float64 unguarded (col "X") (col "Y") ]) divideByZero
    // A literal zero divisor, too.
    let t = Tables()
    Conformance.raises<DivideByZeroException> t (project (t.Scan zeroRow) [ "q", div DivisionKind.IntegerDivide LogicalTypes.Int64 unguarded (col "A") (i64 0L) ]) divideByZero

[<Fact>]
let ``MinValue div -1 fails with Overflow, MinValue % -1 is 0`` () =
    let minRow = [| { K = 1L; A = n Int64.MinValue; Z = n -1L; X = Nullable(); Y = Nullable() } |]
    let t = Tables()
    Conformance.rowsInOrder t (project (t.Scan minRow) [ "r", div DivisionKind.Remainder LogicalTypes.Int64 unguarded (col "A") (col "Z") ]) [ [ 0L ] ]
    let t2 = Tables()
    Conformance.raises<OverflowException> t2 (project (t2.Scan minRow) [ "q", div DivisionKind.IntegerDivide LogicalTypes.Int64 unguarded (col "A") (col "Z") ]) overflow

[<Fact>]
let ``CheckedNarrow off its guard is null and never fails`` () =
    let t = Tables()
    let wide = [| { K = 1L; A = n (int64 Int32.MaxValue + 1L); Z = n 0L; X = Nullable(); Y = Nullable() }; { K = 2L; A = n 5L; Z = n 1L; X = Nullable(); Y = Nullable() } |]
    let guard = bin BinaryOp.Eq (col "Z") (i64 1L)
    Conformance.rowsInOrder t (project (t.Scan wide) [ "i", ScalarExprOps.checkedNarrow LogicalTypes.Int32 guard (col "A") ]) [ [ null ]; [ 5L ] ]
    let t2 = Tables()
    Conformance.raises<OverflowException> t2 (project (t2.Scan wide) [ "i", ScalarExprOps.checkedNarrow LogicalTypes.Int32 unguarded (col "A") ]) overflow

/// Fails with `'E` as written and after the optimizer (a pass must never weaken a guard into "never raises").
let private raisesAlways<'E when 'E :> exn> (t: Tables) (plan: LogicalPlan) (message: string) =
    Conformance.raises<'E> t plan message
    Conformance.raises<'E> t (LogicalOptimizer.optimize plan) message

/// The expression as the frontend hands it over: guarded by RaisingGuards.
let private guarded (e: ScalarExpr) = RaisingGuards.ofExpr e
let private aDivZ = div DivisionKind.IntegerDivide LogicalTypes.Int64 unguarded (col "A") (col "Z")
let private zIsZero = bin BinaryOp.Eq (col "Z") (i64 0L)

type GZ = { K: int64; A: Nullable<int64>; Z: Nullable<int64> }

let private zeros: GZ array =
    [|
        { K = 1L; A = n 8L; Z = n 2L }
        { K = 2L; A = n 9L; Z = n 0L }
        { K = 3L; A = n 4L; Z = nul }
    |]

[<Fact>]
let ``Rows off the guard are null and never fail`` () =
    let t = Tables()
    let twoZeros: GZ array = [| { K = 1L; A = n 8L; Z = n 2L }; { K = 2L; A = n 9L; Z = n 0L }; { K = 3L; A = n 4L; Z = n 0L } |]
    // False off the zeros; null on row 2 and false on row 3 (a null guard is not true).
    let nonZero = bin BinaryOp.NotEq (col "Z") (i64 0L)
    let nullGuard = ScalarExpr.Conditional(bin BinaryOp.Eq (col "K") (i64 2L), nullBool, bin BinaryOp.Eq (col "K") (i64 1L))
    let plan =
        project (t.Scan twoZeros) [
            "q", div DivisionKind.IntegerDivide LogicalTypes.Int64 nonZero (col "A") (col "Z")
            "n", div DivisionKind.Remainder LogicalTypes.Int64 nullGuard (col "A") (col "Z")
        ]
    Conformance.rowsInOrder t plan [ [ 4L; 0L ]; [ null; null ]; [ null; null ] ]

[<Fact>]
let ``RaisingGuards: an untaken CASE branch never fails`` () =
    let t = Tables()
    let caseThen = ScalarExpr.Conditional(zIsZero, ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Int64), aDivZ)
    // A null test takes the else branch (-1): a null Z never reaches the division.
    let caseElse = ScalarExpr.Conditional(bin BinaryOp.NotEq (col "Z") (i64 0L), aDivZ, i64 -1L)
    Conformance.rowsInOrder t (project (t.Scan zeros) [ "a", guarded caseThen; "b", guarded caseElse ]) [ [ 4L; 4L ]; [ null; -1L ]; [ null; -1L ] ]

[<Fact>]
let ``RaisingGuards: the right of AND and OR is reached only when it decides`` () =
    let t = Tables()
    let gt1 = bin BinaryOp.Gt aDivZ (i64 1L)
    let andPred = bin BinaryOp.And (bin BinaryOp.NotEq (col "Z") (i64 0L)) gt1
    let orPred = bin BinaryOp.Or zIsZero gt1
    Conformance.rowsInOrder t (project (t.Scan zeros) [ "and", guarded andPred; "or", guarded orPred ]) [ [ true; true ]; [ false; true ]; [ null; null ] ]
    // As a filter predicate over the scan itself.
    let t2 = Tables()
    Conformance.rowsInOrder t2 (project (filter (t2.Scan zeros) (guarded andPred)) [ "K", col "K" ]) [ [ 1L ] ]

[<Fact>]
let ``RaisingGuards: a later COALESCE argument is reached only after nulls`` () =
    let t = Tables()
    let firstNonNull = ScalarExpr.Coalesce [ ScalarExpr.Conditional(zIsZero, i64 0L, ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Int64)); aDivZ ]
    Conformance.rowsInOrder t (project (t.Scan zeros) [ "c", guarded firstNonNull ]) [ [ 4L ]; [ 0L ]; [ null ] ]
    // Without the guard, the same expression fails on row 2.
    let t2 = Tables()
    raisesAlways<DivideByZeroException> t2 (project (t2.Scan zeros) [ "c", firstNonNull ]) divideByZero

[<Fact>]
let ``RaisingGuards: a reached division still fails`` () =
    let t = Tables()
    let reached = ScalarExpr.Conditional(bin BinaryOp.Gt (col "A") (i64 0L), aDivZ, i64 0L)
    raisesAlways<DivideByZeroException> t (project (t.Scan zeros) [ "c", guarded reached ]) divideByZero
    // An aggregation's argument: only the conditions inside it apply. The aggregate is computed whatever the
    // condition above it (as Spark aggregates before the projection that reads it), so it fails here.
    let t2 = Tables()
    let summed = ScalarExpr.Conditional(bin BinaryOp.Gt len (i64 100L), agg AggKind.Sum aDivZ, i64 0L)
    raisesAlways<DivideByZeroException> t2 (project (t2.Scan zeros) [ "s", guarded summed ]) divideByZero
    let t3 = Tables()
    let inside = agg AggKind.Sum (ScalarExpr.Conditional(zIsZero, i64 0L, aDivZ))
    Conformance.rowsInOrder t3 (project (t3.Scan zeros) [ "s", guarded inside ]) [ [ 4L ] ]

type NG = { K: int64; N: Nullable<int64>; A: Nullable<int64>; Z: Nullable<int64> }

/// Three-valued tests: a null left of AND / OR, or a null CASE test, still reaches the division.
let private nullLeftAnd = bin BinaryOp.And (bin BinaryOp.NotEq (col "N") (i64 0L)) (bin BinaryOp.Gt aDivZ (i64 5L))
let private nullLeftOr = bin BinaryOp.Or (bin BinaryOp.Eq (col "N") (i64 0L)) (bin BinaryOp.Gt aDivZ (i64 1L))
let private nullTestCase = ScalarExpr.Conditional(bin BinaryOp.Eq (col "N") (i64 0L), i64 0L, aDivZ)

[<Fact>]
let ``RaisingGuards: a null left side or CASE test reaches the division`` () =
    let t = Tables()
    let row = [| { K = 1L; N = nul; A = n 8L; Z = n 2L } |]
    // null AND false is false, null OR true is true, a null test takes the else branch.
    Conformance.rowsInOrder t (project (t.Scan row) [ "and", guarded nullLeftAnd; "or", guarded nullLeftOr; "case", guarded nullTestCase ])
        [ [ false; true; 4L ] ]
    for e in [ nullLeftAnd; nullLeftOr; nullTestCase ] do
        let t = Tables()
        let zero = [| { K = 1L; N = nul; A = n 8L; Z = n 0L } |]
        raisesAlways<DivideByZeroException> t (project (t.Scan zero) [ "c", guarded e ]) divideByZero

[<Fact>]
let ``RaisingGuards: an OR filter keeps a null left with a true right, skips the division after a true left`` () =
    let t = Tables()
    let rs =
        [|
            { K = 1L; N = nul; A = n 8L; Z = n 2L }
            { K = 2L; N = n 0L; A = n 1L; Z = n 0L }
            { K = 3L; N = n 5L; A = n 1L; Z = n 1L }
        |]
    Conformance.rowsInOrder t (project (filter (t.Scan rs) (guarded nullLeftOr)) [ "K", col "K" ]) [ [ 1L ]; [ 2L ] ]
