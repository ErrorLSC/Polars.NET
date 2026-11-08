namespace Polars.NET.Linq.Backend.Polars

open System
open Polars.NET.Core
open Polars.NET.Core.Helpers
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
        | LogicalType.StructOf fields ->
            PolarsWrapper.NewStructType(fields |> List.map fst |> Array.ofList, fields |> List.map (snd >> toDataTypeHandle) |> Array.ofList)
        | LogicalType.Unknown -> PolarsWrapper.NewPrimitiveType(int PlDataType.Unknown)
        | LogicalType.Enum categories ->
            use frozen = PolarsWrapper.FrozenCategoriesNew(Array.ofList categories)
            PolarsWrapper.NewEnumType frozen

    /// The logical type of a native Polars data type: Enum types with their categories, every other type
    /// through its Arrow export, and Unknown for a type Arrow cannot express.
    /// The categories Polars records in the metadata of a nested Enum field: each one prefixed with its UTF-8
    /// byte length and ';'.
    let private tryEnumCategories (field: Apache.Arrow.Field) : string list option =
        match field.Metadata with
        | null -> None
        | metadata ->
            match metadata.TryGetValue EnumCategories.PolarsEnumMetadataKey with
            | true, encoded ->
                let bytes = Text.Encoding.UTF8.GetBytes(encoded: string)
                let rec read pos acc =
                    if pos >= bytes.Length then
                        List.rev acc
                    else
                        let sep = Array.IndexOf(bytes, byte ';', pos)
                        let len = int (Text.Encoding.ASCII.GetString(bytes, pos, sep - pos))
                        read (sep + 1 + len) (Text.Encoding.UTF8.GetString(bytes, sep + 1, len) :: acc)
                Some(read 0 [])
            | _ -> None

    let rec private hasEnum (t: LogicalType) =
        match t with
        | LogicalType.Enum _ -> true
        | LogicalType.ListOf element -> hasEnum element
        | LogicalType.StructOf fields -> fields |> List.exists (snd >> hasEnum)
        | _ -> false

    /// An Arrow export as an IR type: an Enum nested in a list or struct (field metadata in Arrow) becomes
    /// ListOf / StructOf of LogicalType.Enum; any other type stays Arrow.
    let rec private ofArrowType (t: Apache.Arrow.Types.IArrowType) : LogicalType =
        let nested =
            match t with
            | :? Apache.Arrow.Types.StructType as st ->
                let fields = st.Fields |> Seq.map (fun f -> f.Name, ofArrowField f) |> List.ofSeq
                if fields |> List.exists (snd >> hasEnum) then Some(LogicalType.StructOf fields) else None
            | :? Apache.Arrow.Types.ListType as lt ->
                let element = ofArrowField lt.ValueField
                if hasEnum element then Some(LogicalType.ListOf element) else None
            | :? Apache.Arrow.Types.LargeListType as lt ->
                let element = ofArrowField lt.ValueField
                if hasEnum element then Some(LogicalType.ListOf element) else None
            | _ -> None
        nested |> Option.defaultValue (LogicalType.Arrow t)

    and private ofArrowField (field: Apache.Arrow.Field) : LogicalType =
        match tryEnumCategories field with
        | Some categories -> LogicalType.Enum categories
        | None -> ofArrowType field.DataType

    let ofDataTypeHandle (dataType: DataTypeHandle) : LogicalType =
        match PolarsWrapper.GetDataTypeKind dataType with
        | PlDataType.Enum -> LogicalType.Enum(List.ofArray (PolarsSchemaExtractor.GetEnumCategories dataType))
        | _ ->
            try
                ofArrowType (Polars.NET.Core.Arrow.ArrowFfiBridge.ImportDataType dataType)
            with
            | :? NotSupportedException
            | :? ArgumentException
            | :? Polars.NET.Core.Exceptions.PolarsException -> LogicalType.Unknown

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
