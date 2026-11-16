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

    /// <summary>
    /// A filter with the shapes the LINQ translator gives rewritten as the readers' skip predicates and filter
    /// lowering read them: a starts-with call as the binary operator, an IN over a collection of values as an IN over
    /// their literals (kept as it is when a value has no plain literal). The other nodes are left as they are; the
    /// semantics are unchanged.
    /// </summary>
    let rec canonical (expr: ScalarExpr) : ScalarExpr =
        match expr with
        | ScalarExpr.Function(ScalarFunction.StartsWith, [ operand; ScalarExpr.Literal _ as prefix ], _) ->
            ScalarExpr.Binary(BinaryOp.StartsWith, canonical operand, prefix)
        | ScalarExpr.InList(target, InListSource.Collection(values, elementType), negated, nullsEqual) ->
            let literals =
                [ for value in Seq.cast<obj> values ->
                      if isNull value then Some(LiteralValue.Null LogicalType.Unknown)
                      else
                          match ScalarExprOps.tryToLiteral value elementType with
                          | Some(ScalarExpr.Literal literal) -> Some literal
                          | _ -> None ]
            let source =
                if literals |> List.forall Option.isSome then InListSource.Literals(literals |> List.map Option.get)
                else InListSource.Collection(values, elementType)
            ScalarExpr.InList(canonical target, source, negated, nullsEqual)
        | ScalarExpr.InList(target, source, negated, nullsEqual) -> ScalarExpr.InList(canonical target, source, negated, nullsEqual)
        | ScalarExpr.Binary(op, left, right) -> ScalarExpr.Binary(op, canonical left, canonical right)
        | ScalarExpr.Unary(op, operand) -> ScalarExpr.Unary(op, canonical operand)
        | ScalarExpr.Cast(operand, target, strict) -> ScalarExpr.Cast(canonical operand, target, strict)
        | ScalarExpr.Coalesce operands -> ScalarExpr.Coalesce(operands |> List.map canonical)
        | _ -> expr

    /// The order of two literals of one kind, where the IR's order and the CLR's agree: booleans, integers, decimals,
    /// dates and times of one kind; strings only for equality (the IR orders them by UTF-8 bytes); floats never (NaN
    /// is ordered in the IR). None for anything else, null literals included.
    let private compareLiterals (equalityOnly: bool) (a: LiteralValue) (b: LiteralValue) : int option =
        match a, b with
        | LiteralValue.Bool x, LiteralValue.Bool y -> Some(compare x y)
        | LiteralValue.Int8 x, LiteralValue.Int8 y -> Some(compare x y)
        | LiteralValue.Int16 x, LiteralValue.Int16 y -> Some(compare x y)
        | LiteralValue.Int32 x, LiteralValue.Int32 y -> Some(compare x y)
        | LiteralValue.Int64 x, LiteralValue.Int64 y -> Some(compare x y)
        | LiteralValue.UInt8 x, LiteralValue.UInt8 y -> Some(compare x y)
        | LiteralValue.UInt16 x, LiteralValue.UInt16 y -> Some(compare x y)
        | LiteralValue.UInt32 x, LiteralValue.UInt32 y -> Some(compare x y)
        | LiteralValue.UInt64 x, LiteralValue.UInt64 y -> Some(compare x y)
        | LiteralValue.Decimal x, LiteralValue.Decimal y -> Some(compare x y)
        | LiteralValue.Date x, LiteralValue.Date y -> Some(compare x y)
        | LiteralValue.TimeOnly x, LiteralValue.TimeOnly y -> Some(compare x y)
        | LiteralValue.String x, LiteralValue.String y when equalityOnly -> Some(if String.Equals(x, y, StringComparison.Ordinal) then 0 else 1)
        | _ -> None

    /// <summary>
    /// A filter with comparisons between two literals decided (where <c>compareLiterals</c> can order them), null
    /// literals carried through (a comparison with a null is null, a null-safe one decided; IS NULL and IS NOT NULL of
    /// a literal decided; an IN of a null), and the constants left folded into AND, OR and NOT, in Kleene logic (false
    /// wins an AND, true an OR; a null stays beside a column, two nulls make a null). A filter whose columns were
    /// replaced by values (a file's partition values, nulls for columns the file lacks) then reads only the columns it
    /// still names. The semantics are unchanged.
    /// </summary>
    let rec foldConstants (expr: ScalarExpr) : ScalarExpr =
        let boolean value = ScalarExpr.Literal(LiteralValue.Bool value)
        let nullValue = ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Boolean)
        let isNullLiteral e = match e with ScalarExpr.Literal(LiteralValue.Null _) -> true | _ -> false
        match expr with
        | ScalarExpr.Binary(BinaryOp.And, left, right) ->
            match foldConstants left, foldConstants right with
            | ScalarExpr.Literal(LiteralValue.Bool false), _
            | _, ScalarExpr.Literal(LiteralValue.Bool false) -> boolean false
            | ScalarExpr.Literal(LiteralValue.Bool true), other
            | other, ScalarExpr.Literal(LiteralValue.Bool true) -> other
            | l, r when isNullLiteral l && isNullLiteral r -> nullValue
            | l, r -> ScalarExpr.Binary(BinaryOp.And, l, r)
        | ScalarExpr.Binary(BinaryOp.Or, left, right) ->
            match foldConstants left, foldConstants right with
            | ScalarExpr.Literal(LiteralValue.Bool true), _
            | _, ScalarExpr.Literal(LiteralValue.Bool true) -> boolean true
            | ScalarExpr.Literal(LiteralValue.Bool false), other
            | other, ScalarExpr.Literal(LiteralValue.Bool false) -> other
            | l, r when isNullLiteral l && isNullLiteral r -> nullValue
            | l, r -> ScalarExpr.Binary(BinaryOp.Or, l, r)
        | ScalarExpr.Unary(UnaryOp.Not, operand) ->
            match foldConstants operand with
            | ScalarExpr.Literal(LiteralValue.Bool value) -> boolean (not value)
            | ScalarExpr.Literal(LiteralValue.Null _) -> nullValue
            | other -> ScalarExpr.Unary(UnaryOp.Not, other)
        | ScalarExpr.Unary(UnaryOp.IsNull, ScalarExpr.Literal value) ->
            boolean (match value with LiteralValue.Null _ -> true | _ -> false)
        | ScalarExpr.Unary(UnaryOp.IsNotNull, ScalarExpr.Literal value) ->
            boolean (match value with LiteralValue.Null _ -> false | _ -> true)
        | ScalarExpr.Binary((BinaryOp.EqMissing | BinaryOp.NotEqMissing) as op, ScalarExpr.Literal a, ScalarExpr.Literal b)
            when (match a, b with LiteralValue.Null _, _ | _, LiteralValue.Null _ -> true | _ -> false) ->
            // Null-safe: equal when both are null, unequal when one is.
            let equal = (match a, b with LiteralValue.Null _, LiteralValue.Null _ -> true | _ -> false)
            boolean (if op = BinaryOp.EqMissing then equal else not equal)
        | ScalarExpr.Binary((BinaryOp.EqMissing | BinaryOp.NotEqMissing) as op, ScalarExpr.Literal(LiteralValue.Null _), other)
        | ScalarExpr.Binary((BinaryOp.EqMissing | BinaryOp.NotEqMissing) as op, other, ScalarExpr.Literal(LiteralValue.Null _)) ->
            ScalarExpr.Unary((if op = BinaryOp.EqMissing then UnaryOp.IsNull else UnaryOp.IsNotNull), other)
        | ScalarExpr.Binary((BinaryOp.Eq | BinaryOp.NotEq | BinaryOp.Lt | BinaryOp.LtEq | BinaryOp.Gt | BinaryOp.GtEq), left, right)
            when isNullLiteral left || isNullLiteral right ->
            nullValue
        | ScalarExpr.InList(ScalarExpr.Literal(LiteralValue.Null _), InListSource.Literals values, negated, nullsEqual) ->
            if nullsEqual then
                let found = values |> List.exists (fun v -> match v with LiteralValue.Null _ -> true | _ -> false)
                boolean (found <> negated)
            else nullValue
        | ScalarExpr.Binary((BinaryOp.Eq | BinaryOp.NotEq | BinaryOp.EqMissing | BinaryOp.NotEqMissing | BinaryOp.Lt | BinaryOp.LtEq | BinaryOp.Gt | BinaryOp.GtEq) as op,
                            ScalarExpr.Literal a,
                            ScalarExpr.Literal b) ->
            let equality = op = BinaryOp.Eq || op = BinaryOp.NotEq || op = BinaryOp.EqMissing || op = BinaryOp.NotEqMissing
            match compareLiterals equality a b with
            | Some order ->
                boolean (
                    match op with
                    | BinaryOp.Eq | BinaryOp.EqMissing -> order = 0
                    | BinaryOp.NotEq | BinaryOp.NotEqMissing -> order <> 0
                    | BinaryOp.Lt -> order < 0
                    | BinaryOp.LtEq -> order <= 0
                    | BinaryOp.Gt -> order > 0
                    | _ -> order >= 0)
            | None -> expr
        | _ -> expr
