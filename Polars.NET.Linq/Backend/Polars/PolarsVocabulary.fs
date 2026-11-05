namespace Polars.NET.Linq.Backend.Polars

open Polars.NET.Core
open Polars.NET.Core.Enums
open Polars.NET.Core.Wrappers
open Polars.NET.Linq.IR

/// Maps IR vocabulary (logical types and relational / function enums) to native Polars counterparts.
module internal PolarsVocabulary =

    /// Materializes a LogicalType as a native Polars DataTypeHandle (Arrow types via the shared Arrow mapping).
    let rec toDataTypeHandle (t: LogicalType) : DataTypeHandle =
        match t with
        | LogicalType.Arrow arrowType -> PolarsWrapper.MapArrowToDataTypeHandle arrowType
        | LogicalType.ListOf element -> PolarsWrapper.NewListType(toDataTypeHandle element)
        | LogicalType.Unknown -> PolarsWrapper.NewPrimitiveType(int PlDataType.Unknown)

    let joinType (kind: JoinKind) : PlJoinType =
        match kind with
        | JoinKind.Inner -> PlJoinType.Inner
        | JoinKind.Left -> PlJoinType.Left
        | JoinKind.Full -> PlJoinType.Outer
        | JoinKind.Cross -> PlJoinType.Cross
        | JoinKind.Semi -> PlJoinType.Semi
        | JoinKind.Anti -> PlJoinType.Anti

    /// LINQ joins yield rows in outer order, and the matches of one outer row in inner order. A right join
    /// reaches the IR with its sides swapped, so this is also the right join's order. Semi and anti joins
    /// filter the left side and keep its order.
    ///
    /// A cross join is left unconstrained: it is produced in outer-then-inner order anyway, and requiring
    /// the order would stop Polars from rewriting a cross join filtered by inequalities into an IEJoin.
    /// Such a range join is therefore the one join whose row order is not guaranteed.
    let joinMaintainOrder (kind: JoinKind) : PlJoinMaintainOrder =
        match kind with
        | JoinKind.Semi
        | JoinKind.Anti -> PlJoinMaintainOrder.Left
        | JoinKind.Inner
        | JoinKind.Left
        | JoinKind.Full -> PlJoinMaintainOrder.LeftRight
        | JoinKind.Cross -> PlJoinMaintainOrder.None

    let joinCoalesce (coalescing: JoinKeyCoalescing) : PlJoinCoalesce =
        match coalescing with
        | JoinKeyCoalescing.Default -> PlJoinCoalesce.JoinSpecific
        | JoinKeyCoalescing.Coalesce -> PlJoinCoalesce.CoalesceColumns
        | JoinKeyCoalescing.KeepBoth -> PlJoinCoalesce.KeepColumns

    let uniqueKeep (keep: DistinctKeep) : PlUniqueKeepStrategy =
        match keep with
        | DistinctKeep.First -> PlUniqueKeepStrategy.First
        | DistinctKeep.Last -> PlUniqueKeepStrategy.Last
        | DistinctKeep.Any -> PlUniqueKeepStrategy.Any
        | DistinctKeep.None -> PlUniqueKeepStrategy.None

    let asofStrategy (strategy: AsofStrategy) : PlAsofStrategy =
        match strategy with
        | AsofStrategy.Backward -> PlAsofStrategy.Backward
        | AsofStrategy.Forward -> PlAsofStrategy.Forward
        | AsofStrategy.Nearest -> PlAsofStrategy.Nearest

    let roundMode (mode: RoundMode) : PlRoundMode =
        match mode with
        | RoundMode.HalfToEven -> PlRoundMode.HalfToEven
        | RoundMode.HalfAwayFromZero -> PlRoundMode.HalfAwayFromZero
        | RoundMode.ToZero -> PlRoundMode.ToZero

    let rankMethod (m: RankMethod) : PlRankMethod =
        match m with
        | RankMethod.Average -> PlRankMethod.Average
        | RankMethod.Min -> PlRankMethod.Min
        | RankMethod.Max -> PlRankMethod.Max
        | RankMethod.Dense -> PlRankMethod.Dense
        | RankMethod.Ordinal -> PlRankMethod.Ordinal
        | RankMethod.Random -> PlRankMethod.Random

    let interpolationMethod (m: InterpolationMethod) : PlInterpolationMethod =
        match m with
        | InterpolationMethod.Linear -> PlInterpolationMethod.Linear
        | InterpolationMethod.Nearest -> PlInterpolationMethod.Nearest

    let quantileMethod (m: QuantileMethod) : PlQuantileMethod =
        match m with
        | QuantileMethod.Nearest -> PlQuantileMethod.Nearest
        | QuantileMethod.Higher -> PlQuantileMethod.Higher
        | QuantileMethod.Lower -> PlQuantileMethod.Lower
        | QuantileMethod.Midpoint -> PlQuantileMethod.Midpoint
        | QuantileMethod.Linear -> PlQuantileMethod.Linear
        | QuantileMethod.Equiprobable -> PlQuantileMethod.Equiprobable

    let nullBehavior (nb: NullBehavior) : PlNullBehavior =
        match nb with
        | NullBehavior.Ignore -> PlNullBehavior.Ignore
        | NullBehavior.Drop -> PlNullBehavior.Drop
