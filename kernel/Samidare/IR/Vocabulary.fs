namespace Kaminari.Samidare.IR

/// Relational join semantics
[<RequireQualifiedAccess>]
type JoinKind =
    | Inner
    | Left
    | Full
    | Cross
    | Semi
    | Anti

/// How equi-join key columns appear in the join output
[<RequireQualifiedAccess>]
type JoinKeyCoalescing =
    /// Backend default for the join kind
    | Default
    /// Merge left and right key columns into one
    | Coalesce
    /// Keep both key columns (right side suffixed)
    | KeepBoth

/// Which row to keep among duplicates in Distinct
[<RequireQualifiedAccess>]
type DistinctKeep =
    | First
    | Last
    | Any
    | None

/// Matching direction of an AsOf join
[<RequireQualifiedAccess>]
type AsofStrategy =
    | Backward
    | Forward
    | Nearest

/// Midpoint rounding behavior for Round
[<RequireQualifiedAccess>]
type RoundMode =
    | HalfToEven
    | HalfAwayFromZero
    | ToZero

/// What a ScalarFunction.CheckedDivide computes. Every kind truncates toward zero where it truncates.
[<RequireQualifiedAccess>]
type DivisionKind =
    /// The quotient: over floating values the IEEE quotient, over decimals the exact quotient rounded half away from
    /// zero at the result's scale (SQL `/`).
    | Divide
    /// The integer quotient truncated toward zero (-7 div 2 = -3; Spark's `div`).
    | IntegerDivide
    /// The remainder of the truncated division: it carries the dividend's sign (-7 % 2 = -1, 7 % -3 = 1).
    | Remainder

/// Tie-breaking method for Rank
[<RequireQualifiedAccess>]
type RankMethod =
    | Average
    | Min
    | Max
    | Dense
    | Ordinal
    | Random

/// Fill method for Interpolate
[<RequireQualifiedAccess>]
type InterpolationMethod =
    | Linear
    | Nearest

/// Interpolation method for Quantile
[<RequireQualifiedAccess>]
type QuantileMethod =
    | Nearest
    | Higher
    | Lower
    | Midpoint
    | Linear
    | Equiprobable

/// Null handling for Diff
[<RequireQualifiedAccess>]
type NullBehavior =
    | Ignore
    | Drop

/// The largest distance at which an as-of join still matches.
[<RequireQualifiedAccess>]
type AsofTolerance =
    | Duration of System.TimeSpan
    | Integer of int64
    | Float of double
    /// Text in the syntax of the execution engine. The IR does not read it; a backend may reject it.
    | BackendText of string
