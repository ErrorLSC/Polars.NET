namespace Polars.NET.Linq.Provider

open Polars.NET.Core
open Polars.NET.Core.Arrow
open System
open System.Linq.Expressions
open Polars.NET.Core.Helpers
open Apache.Arrow.Types

module ExprTranslator =

    let timeUnitMap (unit:Apache.Arrow.Types.TimeUnit): byte=
        match unit with
        | TimeUnit.Second -> PlTimeUnit.Second |> byte
        | TimeUnit.Millisecond -> PlTimeUnit.Milliseconds |> byte
        | TimeUnit.Microsecond -> PlTimeUnit.Microseconds |> byte
        | TimeUnit.Nanosecond -> PlTimeUnit.Nanoseconds |> byte
        | _ -> invalidArg (nameof unit) "Unknown TimeUnit"

    // Maps an Arrow IArrowType to a Polars Native DataTypeHandle
    let private arrowTypeToPolarsDataType (arrowType: IArrowType) : DataTypeHandle option =
        match arrowType with
        | :? BooleanType   -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Boolean))
        | :? Int8Type      -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Int8))
        | :? UInt8Type     -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.UInt8))
        | :? Int16Type     -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Int16))
        | :? UInt16Type    -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.UInt16))
        | :? Int32Type     -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Int32))
        | :? UInt32Type    -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.UInt32))
        | :? Int64Type     -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Int64))
        | :? UInt64Type    -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.UInt64))
        | :? HalfFloatType    -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Float16))
        | :? FloatType     -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Float32))
        | :? DoubleType    -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Float64))
        | :? StringType
        | :? StringViewType
        | :? LargeStringType -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.String))
        | :? Date32Type    -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Date))
        | :? Time64Type    -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Time))
        | :? DurationType as du  -> Some (PolarsWrapper.NewDurationType(timeUnitMap du.Unit))
        | :? TimestampType as ts ->
            let tz = if String.IsNullOrEmpty ts.Timezone then null else ts.Timezone
            Some (PolarsWrapper.NewDateTimeType(timeUnitMap ts.Unit, tz))
        | :? Decimal128Type as dec ->
            Some (PolarsWrapper.NewDecimalType(int dec.Precision, int dec.Scale))
        | _ ->
            // Unsupported complex/nested types for direct scalar cast
            None

    /// Maps a CLR Type directly to native DataTypeHandle via existing ArrowTypeResolver
    let private tryGetPolarsDataType (t: Type) : DataTypeHandle option =
        try
            (ArrowTypeResolver.GetArrowTypeFromNetType >> arrowTypeToPolarsDataType) t
        with _ ->
            None

    /// Translates constant values to native literal ExprHandle
    let rec private toLiteralHandle (value: obj) (t: Type) : ExprHandle =
        match value with
        | null -> PolarsWrapper.LitNull()
        | _ ->
            let coreType = PolarsTypeHelper.UnwrapCoreType(t)
            if coreType.IsEnum then
                let underlyingType = Enum.GetUnderlyingType(coreType)
                let underlyingVal = Convert.ChangeType(value, underlyingType)
                toLiteralHandle underlyingVal underlyingType
            else
                match value with
                | :? bool as b -> PolarsWrapper.Lit(b)
                | :? sbyte as sb -> PolarsWrapper.Lit(sb)
                | :? byte as b -> PolarsWrapper.Lit(b)
                | :? int16 as s -> PolarsWrapper.Lit(s)
                | :? uint16 as us -> PolarsWrapper.Lit(us)
                | :? int as i -> PolarsWrapper.Lit(i)
                | :? uint32 as ui -> PolarsWrapper.Lit(ui)
                | :? int64 as l -> PolarsWrapper.Lit(l)
                | :? uint64 as ul -> PolarsWrapper.Lit(ul)
                | :? Int128 as i128 -> PolarsWrapper.Lit(i128)
                | :? decimal as d -> PolarsWrapper.Lit(d)
                | :? Half as h -> PolarsWrapper.Lit(h)
                | :? float32 as f -> PolarsWrapper.Lit(f)
                | :? double as d -> PolarsWrapper.Lit(d)
                | :? string as s -> PolarsWrapper.Lit(s)
                | :? DateTime as dt -> PolarsWrapper.Lit(dt)
                | :? DateTimeOffset as doff -> PolarsWrapper.Lit(doff)
                | :? TimeSpan as ts -> PolarsWrapper.Lit(ts)
                | :? DateOnly as dof -> PolarsWrapper.Lit(dof)
                | :? TimeOnly as tof -> PolarsWrapper.Lit(tof)
                | other ->
                    failwithf "Type '%s' is not supported as a constant literal in Polars LINQ" (other.GetType().FullName)

    /// Translates binary operators to native Expr binary expressions
    let private translateBinary (nodeType: ExpressionType) (left: ExprHandle) (right: ExprHandle) : ExprHandle =
        match nodeType with
        // Arithmetic operators
        | ExpressionType.Equal -> PolarsWrapper.Eq(left, right)
        | ExpressionType.NotEqual -> PolarsWrapper.Neq(left, right)
        | ExpressionType.GreaterThan -> PolarsWrapper.Gt(left, right)
        | ExpressionType.GreaterThanOrEqual -> PolarsWrapper.GtEq(left, right)
        | ExpressionType.LessThan -> PolarsWrapper.Lt(left, right)
        | ExpressionType.LessThanOrEqual -> PolarsWrapper.LtEq(left, right)
        | ExpressionType.Add -> PolarsWrapper.Add(left, right)
        | ExpressionType.Subtract -> PolarsWrapper.Sub(left, right)
        | ExpressionType.Multiply -> PolarsWrapper.Mul(left, right)
        | ExpressionType.Divide -> PolarsWrapper.Div(left, right)
        | ExpressionType.Modulo -> PolarsWrapper.Rem(left, right)
        | ExpressionType.Power -> PolarsWrapper.Pow(left, right)
        // Binary Logic
        | ExpressionType.And -> PolarsWrapper.And(left, right)
        | ExpressionType.AndAlso -> PolarsWrapper.And(left, right)
        | ExpressionType.Or -> PolarsWrapper.Or(left, right)
        | ExpressionType.OrElse -> PolarsWrapper.Or(left, right)
        | ExpressionType.ExclusiveOr -> PolarsWrapper.Xor(left, right)

        | other ->
            failwithf "Binary operator '%A' is not currently supported in Polars LINQ" other

    /// Translates unary operators to native Expr unary expressions
    let private translateUnary (nodeType: ExpressionType) (operand: ExprHandle) : ExprHandle option =
        match nodeType with
        | ExpressionType.Not ->
            Some (PolarsWrapper.Not operand)
        | ExpressionType.Negate
        | ExpressionType.NegateChecked ->
            Some (PolarsWrapper.Neg operand)
        | ExpressionType.UnaryPlus ->
            Some operand
        | _ ->
            None

    /// Translates explicit and implicit casting nodes into native Polars ExprCast
    let private tryTranslateCast (targetType: Type) (operand: ExprHandle) : ExprHandle option =
        // 1. Boxing / Reference passthrough
        if targetType = typeof<obj> then
            Some operand
        else
            // 2. Map CLR type to native DataTypeHandle and assemble ExprCast
            match tryGetPolarsDataType targetType with
            | Some dtypeHandle ->
                let dtypeExpr = PolarsWrapper.DataTypeExprFromDataType(dtypeHandle)
                Some (PolarsWrapper.ExprCast(operand, dtypeExpr, strict = false, wrapNumerical = false))
            | None ->
                None

    /// Attempts to translate an Expression AST node to a native Polars ExprHandle.
    /// Returns None if the node contains unsupported logic (e.g. custom C#/F# methods).
    let rec tryTranslate (paramName: string) (expr: Expression) : ExprHandle option =
        try
            match expr with
            | MemberAccess(paramExpr, memberInfo) when paramExpr <> null && paramExpr.NodeType = ExpressionType.Parameter ->
                Some (PolarsWrapper.Col(memberInfo.Name))

            | MemberAccess _ as memberExpr ->
                match tryEvaluate memberExpr with
                | Some value -> Some (toLiteralHandle value memberExpr.Type)
                | None -> None

            | Constant(value, t) ->
                Some (toLiteralHandle value t)

            | Binary(op, left, right) ->
                match tryTranslate paramName left, tryTranslate paramName right with
                | Some l, Some r -> Some (translateBinary op l r)
                | _ -> None

            // 1. Type Casting & Conversions
            | Unary(ExpressionType.Convert, operandExpr)
            | Unary(ExpressionType.ConvertChecked, operandExpr) ->
                match tryTranslate paramName operandExpr with
                | Some inner -> tryTranslateCast expr.Type inner
                | None -> None

            // 2. Pure Unary Operations (Clean signature, zero type baggage)
            | Unary(op, operandExpr) ->
                match tryTranslate paramName operandExpr with
                | Some inner -> translateUnary op inner
                | None -> None

            | _ -> None
        with _ ->
            None