namespace Polars.NET.Linq.IR

open System
open Polars.NET.Core

/// Lowers pure symbolic ScalarExpr AST nodes to unmanaged Polars ExprHandle pointers.
module internal ScalarLowerer =

    /// Strongly-typed zero-boxing conversion from LiteralValue to Polars ExprHandle
    let lowerLiteral (value: LiteralValue) : ExprHandle =
        match value with
        | LiteralValue.Null _ -> PolarsWrapper.LitNull()
        | LiteralValue.Bool b -> PolarsWrapper.Lit b
        | LiteralValue.Int8 sb -> PolarsWrapper.Lit sb
        | LiteralValue.Int16 s -> PolarsWrapper.Lit s
        | LiteralValue.Int32 i -> PolarsWrapper.Lit i
        | LiteralValue.Int64 l -> PolarsWrapper.Lit l
        | LiteralValue.UInt8 b -> PolarsWrapper.Lit b
        | LiteralValue.UInt16 us -> PolarsWrapper.Lit us
        | LiteralValue.UInt32 ui -> PolarsWrapper.Lit ui
        | LiteralValue.UInt64 ul -> PolarsWrapper.Lit ul
        | LiteralValue.Float32 f -> PolarsWrapper.Lit f
        | LiteralValue.Float64 d -> PolarsWrapper.Lit d
        | LiteralValue.Decimal m -> PolarsWrapper.Lit m
        | LiteralValue.String s -> PolarsWrapper.Lit s
        | LiteralValue.Date d -> PolarsWrapper.Lit d
        | LiteralValue.DateTime dt -> PolarsWrapper.Lit dt
        | LiteralValue.DateTimeOffset dto -> PolarsWrapper.Lit dto
        | LiteralValue.TimeSpan ts -> PolarsWrapper.Lit ts
        | LiteralValue.TimeOnly to' -> PolarsWrapper.Lit to'

    /// Lowers a SortOrder into a tuple of (ExprHandle, isDescending, nullsLast)
    let rec lowerSortOrder (order: SortOrder) : ExprHandle * bool * bool =
        lower order.Expr, order.Descending, order.NullsLast

    /// Lowers a ScalarExpr recursively to an ExprHandle
    and lower (expr: ScalarExpr) : ExprHandle =
        match expr with
        | ScalarExpr.Column name -> PolarsWrapper.Col name

        | ScalarExpr.Literal lit -> lowerLiteral lit

        | ScalarExpr.Binary(op, left, right) ->
            let leftH = lower left
            let rightH = lower right
            match op with
            | BinaryOp.Eq -> PolarsWrapper.Eq(leftH, rightH)
            | BinaryOp.NotEq -> PolarsWrapper.Neq(leftH, rightH)
            | BinaryOp.Lt -> PolarsWrapper.Lt(leftH, rightH)
            | BinaryOp.LtEq -> PolarsWrapper.LtEq(leftH, rightH)
            | BinaryOp.Gt -> PolarsWrapper.Gt(leftH, rightH)
            | BinaryOp.GtEq -> PolarsWrapper.GtEq(leftH, rightH)
            | BinaryOp.And -> PolarsWrapper.And(leftH, rightH)
            | BinaryOp.Or -> PolarsWrapper.Or(leftH, rightH)
            | BinaryOp.Xor -> PolarsWrapper.Xor(leftH, rightH)
            | BinaryOp.Add -> PolarsWrapper.Add(leftH, rightH)
            | BinaryOp.Subtract -> PolarsWrapper.Sub(leftH, rightH)
            | BinaryOp.Multiply -> PolarsWrapper.Mul(leftH, rightH)
            | BinaryOp.Divide -> PolarsWrapper.Div(leftH, rightH)
            | BinaryOp.Modulo -> PolarsWrapper.Rem(leftH, rightH)
            | BinaryOp.FloorDivide -> PolarsWrapper.FloorDiv(leftH, rightH)
            | BinaryOp.StringConcat -> PolarsWrapper.Add(leftH, rightH)
            | BinaryOp.Contains -> PolarsWrapper.StrContains(leftH, rightH, literal = false, strict = false)
            | BinaryOp.StartsWith -> PolarsWrapper.StrStartsWith(leftH, rightH)
            | BinaryOp.EndsWith -> PolarsWrapper.StrEndsWith(leftH, rightH)
            | BinaryOp.Like -> PolarsWrapper.StrContains(leftH, rightH, literal = true, strict = false)
            | BinaryOp.RegexMatch -> PolarsWrapper.StrContains(leftH, rightH, literal = false, strict = false)

        | ScalarExpr.Unary(op, inner) ->
            let innerH = lower inner
            match op with
            | UnaryOp.Not -> PolarsWrapper.Not innerH
            | UnaryOp.Negate -> PolarsWrapper.Neg innerH
            | UnaryOp.IsNull -> PolarsWrapper.IsNull innerH
            | UnaryOp.IsNotNull -> PolarsWrapper.IsNotNull innerH
            | UnaryOp.Abs -> PolarsWrapper.Abs innerH

        | ScalarExpr.Cast(inner, targetType, isStrict) ->
            let innerH = lower inner
            let dtHandle = PolarsWrapper.NewPrimitiveType(int targetType)
            let dtExpr = PolarsWrapper.DataTypeExprFromDataType dtHandle
            PolarsWrapper.ExprCast(innerH, dtExpr, strict = isStrict, wrapNumerical = false)

        | ScalarExpr.Conditional(test, ifTrue, ifFalse) ->
            let testH = lower test
            let trueH = lower ifTrue
            let falseH = lower ifFalse
            PolarsWrapper.IfElse(testH, trueH, falseH)

        | ScalarExpr.Coalesce exprs ->
            let handles = exprs |> List.map lower |> List.toArray
            PolarsWrapper.Coalesce handles

        | ScalarExpr.ItemAccess(target, index) ->
            let targetH = lower target
            let indexH = lower index
            PolarsWrapper.ListGet(targetH, indexH, nullOnOob = true)

        | ScalarExpr.Slice(target, offset, length) ->
            let targetH = lower target
            PolarsWrapper.ExprSlice(targetH, PolarsWrapper.Lit offset, PolarsWrapper.Lit length)

        | ScalarExpr.Alias(inner, alias) ->
            let innerH = lower inner
            PolarsWrapper.Alias(innerH, alias)

        | ScalarExpr.Aggregation(kind, exprOpt, isDistinct) ->
            let baseH =
                match exprOpt with
                | Some e ->
                    let h = lower e
                    if isDistinct && kind <> AggKind.Count then PolarsWrapper.ExprUnique h else h
                | None -> PolarsWrapper.Col "*"

            match kind with
            | AggKind.Sum -> PolarsWrapper.Sum baseH
            | AggKind.Mean -> PolarsWrapper.Mean baseH
            | AggKind.Min -> PolarsWrapper.Min baseH
            | AggKind.Max -> PolarsWrapper.Max baseH
            | AggKind.Count -> if isDistinct then PolarsWrapper.NUnique baseH else PolarsWrapper.Count baseH
            | AggKind.First -> PolarsWrapper.First(baseH, false)
            | AggKind.Last -> PolarsWrapper.Last(baseH, false)
            | AggKind.Median -> PolarsWrapper.Median baseH
            | AggKind.Std -> PolarsWrapper.Std(baseH, 1uy)
            | AggKind.Var -> PolarsWrapper.Var(baseH, 1uy)
            | AggKind.Quantile prob -> PolarsWrapper.Quantile(baseH, prob, PlQuantileMethod.Nearest)
            | AggKind.List -> PolarsWrapper.Implode(baseH, false)
            | AggKind.Len -> PolarsWrapper.Len()

        | ScalarExpr.Window(inner, partitionBy, orderBy) ->
            let innerH = lower inner
            let partHs = partitionBy |> List.map lower |> List.toArray
            let orderHs = orderBy |> List.map (fun o -> lower o.Expr) |> List.toArray
            let isDesc = orderBy |> List.tryHead |> Option.map (fun o -> o.Descending) |> Option.defaultValue false
            let nullsLast = orderBy |> List.tryHead |> Option.map (fun o -> o.NullsLast) |> Option.defaultValue false
            PolarsWrapper.Over(
                innerH,
                partHs,
                orderHs,
                descending = isDesc,
                nullsLast = nullsLast,
                multithreaded = true,
                maintainOrder = true,
                mappingCode = PlWindowMapping.GroupsToRows
            )

        | ScalarExpr.Function(name, args, _) ->
            match name.ToLowerInvariant(), args with
            | "abs", [ a ] -> PolarsWrapper.Abs(lower a)
            | "round", [ a ] -> PolarsWrapper.Round(lower a, 0u, PlRoundMode.HalfToEven)
            | "round", [ a; ScalarExpr.Literal(LiteralValue.Int32 decimals) ] ->
                PolarsWrapper.Round(lower a, uint32 decimals, PlRoundMode.HalfToEven)
            | "floor", [ a ] -> PolarsWrapper.Floor(lower a)
            | "ceil", [ a ] -> PolarsWrapper.Ceil(lower a)
            | "sqrt", [ a ] -> PolarsWrapper.Sqrt(lower a)
            | "exp", [ a ] -> PolarsWrapper.Exp(lower a)
            | "log", [ a; b ] -> PolarsWrapper.Log(lower a, lower b)
            | "to_upper", [ a ] -> PolarsWrapper.StrToUpper(lower a)
            | "to_lower", [ a ] -> PolarsWrapper.StrToLower(lower a)
            | "len_chars", [ a ] -> PolarsWrapper.StrLenChars(lower a)
            | "len_bytes", [ a ] -> PolarsWrapper.StrLenBytes(lower a)
            | "reverse", [ a ] -> PolarsWrapper.StrReverse(lower a)
            | _ -> failwithf "Unsupported function in ScalarLowerer: %s" name

        | ScalarExpr.Subquery(_, kind) ->
            failwithf "Subquery scalar expression must be decorrelated into relational join before physical lowering: %A" kind

        | ScalarExpr.Opaque(handle, _, _) ->
            PolarsWrapper.CloneExpr handle
