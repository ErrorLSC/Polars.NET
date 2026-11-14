namespace Kaminari.Samidare.IR

open System
open Apache.Arrow.Types

/// Shapes of row filters that the optimizer and the readers deriving skip predicates from a filter (statistics
/// pruning, file skipping) recognise alike: conjunctions and disjunctions, the front end's NaN guards, and casts that
/// keep every value.
[<RequireQualifiedAccess>]
module FilterShapes =

    /// The conjuncts of an AND tree, left to right (the expression itself when it is no AND).
    let conjuncts (expr: ScalarExpr) : ScalarExpr list =
        let rec go expr acc =
            match expr with
            | ScalarExpr.Binary(BinaryOp.And, left, right) -> go left (go right acc)
            | other -> other :: acc
        go expr []

    /// The disjuncts of an OR tree, left to right (the expression itself when it is no OR).
    let disjuncts (expr: ScalarExpr) : ScalarExpr list =
        let rec go expr acc =
            match expr with
            | ScalarExpr.Binary(BinaryOp.Or, left, right) -> go left (go right acc)
            | other -> other :: acc
        go expr []

    /// Whether a literal is positive infinity (of any floating width).
    let isPositiveInfinity (value: LiteralValue) : bool =
        match value with
        | LiteralValue.Float64 d -> Double.IsPositiveInfinity d
        | LiteralValue.Float32 f -> Single.IsPositiveInfinity f
        | LiteralValue.Float16 h -> Half.IsPositiveInfinity h
        | _ -> false

    /// The operand a conjunct keeps away from NaN, when it is one of the NaN guards (SEMANTICS.md, NaNSemantics):
    /// `x <= +inf`, `+inf >= x`, `NOT isnan(x)` or `NOT coalesce(isnan(x), false)`. A row the conjunction keeps is a
    /// number (or null) there, so its other conjuncts need not consider NaN for that operand.
    let tryNaNGuardOperand (conjunct: ScalarExpr) : ScalarExpr option =
        match conjunct with
        | ScalarExpr.Binary(BinaryOp.LtEq, operand, ScalarExpr.Literal bound)
        | ScalarExpr.Binary(BinaryOp.GtEq, ScalarExpr.Literal bound, operand) when isPositiveInfinity bound -> Some operand
        | ScalarExpr.Unary(UnaryOp.Not, ScalarExpr.Unary(UnaryOp.IsNan, operand))
        | ScalarExpr.Unary(UnaryOp.Not, ScalarExpr.Coalesce [ ScalarExpr.Unary(UnaryOp.IsNan, operand); _ ]) -> Some operand
        | _ -> None

    let private integerWidth (id: ArrowTypeId) =
        match id with
        | ArrowTypeId.Int8 -> Some(8, true)
        | ArrowTypeId.Int16 -> Some(16, true)
        | ArrowTypeId.Int32 -> Some(32, true)
        | ArrowTypeId.Int64 -> Some(64, true)
        | ArrowTypeId.UInt8 -> Some(8, false)
        | ArrowTypeId.UInt16 -> Some(16, false)
        | ArrowTypeId.UInt32 -> Some(32, false)
        | ArrowTypeId.UInt64 -> Some(64, false)
        | _ -> None

    /// Whether a cast from `source` to `target` keeps every value as it is (so comparisons on the cast are
    /// comparisons on the source): the same type, Float32 to Float64, an integer to an integer that holds all of its
    /// values, a decimal to one of the same scale and no less precision. Casts that round (timestamps to a coarser
    /// unit, narrowing) are not: `cast(x) <= c` then holds on values above `c`.
    let castKeepsValues (source: IArrowType) (target: IArrowType) : bool =
        if LogicalType.Arrow source = LogicalType.Arrow target then
            true
        else
            match source, target with
            | (:? FloatType), (:? DoubleType) -> true
            | (:? Decimal128Type as s), (:? Decimal128Type as t) -> s.Scale = t.Scale && t.Precision >= s.Precision
            | _ ->
                match integerWidth source.TypeId, integerWidth target.TypeId with
                | Some(fromBits, fromSigned), Some(toBits, toSigned) ->
                    if fromSigned = toSigned then toBits >= fromBits else not fromSigned && toSigned && toBits > fromBits
                | _ -> false

    /// As castKeepsValues, for IR types (false when either is not an Arrow type).
    let castKeepsValuesOf (source: LogicalType) (target: LogicalType) : bool =
        match source, target with
        | LogicalType.Arrow s, LogicalType.Arrow t -> castKeepsValues s t
        | _ -> false
