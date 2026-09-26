namespace Polars.NET.Linq.Provider

open Polars.NET.Core
open Polars.NET.Core.Arrow
open System
open System.Reflection
open System.Linq.Expressions
open Polars.NET.Core.Helpers
open Apache.Arrow.Types

module ExprTranslator =

    let rec private isNullConstantExpr (e: Expression) =
        match e with
        | null -> true
        | :? ConstantExpression as c -> isNull c.Value
        | :? UnaryExpression as u when u.NodeType = ExpressionType.Convert || 
                                       u.NodeType = ExpressionType.ConvertChecked || 
                                       u.NodeType = ExpressionType.Quote ->
            isNullConstantExpr u.Operand
        | _ -> false

    let timeUnitMap (unit: Apache.Arrow.Types.TimeUnit) : byte =
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
        | :? HalfFloatType -> Some (PolarsWrapper.NewPrimitiveType(int PlDataType.Float16))
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
        | ExpressionType.And
        | ExpressionType.AndAlso -> PolarsWrapper.And(left, right)
        | ExpressionType.Or
        | ExpressionType.OrElse -> PolarsWrapper.Or(left, right)
        | ExpressionType.ExclusiveOr -> PolarsWrapper.Xor(left, right)
        | other ->
            failwithf "Binary operator '%A' is not currently supported in Polars LINQ" other

    /// Translates unary operators to native Expr unary expressions
    let private translateUnary (nodeType: ExpressionType) (operand: ExprHandle) : ExprHandle option =
        match nodeType with
        | ExpressionType.Not -> Some (PolarsWrapper.Not operand)
        | ExpressionType.Negate
        | ExpressionType.NegateChecked -> Some (PolarsWrapper.Neg operand)
        | ExpressionType.UnaryPlus -> Some operand
        | _ -> None

    /// Translates explicit and implicit casting nodes into native Polars ExprCast
    let private tryTranslateCast (targetType: Type) (operand: ExprHandle) : ExprHandle option =
        if targetType = typeof<obj> then
            Some operand
        else
            match tryGetPolarsDataType targetType with
            | Some dtypeHandle ->
                let dtypeExpr = PolarsWrapper.DataTypeExprFromDataType(dtypeHandle)
                Some (PolarsWrapper.ExprCast(operand, dtypeExpr, strict = false, wrapNumerical = false))
            | None -> None

    /// 判断是否为 F# 闭包/元组的中间包装成员 (Item1, Item2 等)
    let private isFSharpAnonymousMember (m: MemberInfo) =
        m.Name.StartsWith("Item") || 
        m.DeclaringType.Name.StartsWith("AnonymousObject") || 
        m.DeclaringType.Name.StartsWith("Tuple") ||
        m.DeclaringType.Name.Contains("TransparentIdentifier")

    /// 精准解析列名：忽略中间的 Item1/Item2 嵌套壳，直接抓取底层的真实列名 (如 DeptName, Name, Latency)
    let rec private tryResolveColumnName (paramName: string) (expr: Expression) : string option =
        match expr with
        // Case 1: 访问的是元组壳自身 (例如 _arg1.Item1) ➔ 不是独立的物理列，返回 None
        | MemberAccess(_, m) when isFSharpAnonymousMember m ->
            None

        // Case 2: 访问真实的实体属性 (例如 e.Name、_arg1.Item1.DeptName、tupledArg.Item3.Name)
        | MemberAccess(inner, m) ->
            let rec isRootedInParamOrClosure (e: Expression) =
                match e with
                | :? ParameterExpression as p ->
                    p.Name = paramName || 
                    p.Name.StartsWith("_arg") || 
                    p.Name.StartsWith("tupled") ||
                    p.Type.Name.StartsWith("AnonymousObject") ||
                    p.Type.Name.StartsWith("Tuple")
                | MemberAccess(nextInner, nextM) when isFSharpAnonymousMember nextM ->
                    isRootedInParamOrClosure nextInner
                | _ -> false

            if isRootedInParamOrClosure inner then
                Some m.Name
            else
                None

        | _ -> None

    /// Attempts to translate an Expression AST node to a native Polars ExprHandle.
    let rec tryTranslate (paramName: string) (expr: Expression) : ExprHandle option =
        try
            match expr with
            // 1. Column Reference: 自动解析直接访问 (e.Name) 和 F# 闭包/嵌套元组 (tupledArg.Item1.DeptName / _arg1.Item1.DeptId)
            | MemberAccess _ as memberExpr when (tryResolveColumnName paramName memberExpr).IsSome ->
                let actualCol = (tryResolveColumnName paramName memberExpr).Value
                Some (PolarsWrapper.Col(actualCol))

            // 2. Closure / Captured Local Variables evaluation
            | MemberAccess _ as memberExpr ->
                match tryEvaluate memberExpr with
                | Some value -> Some (toLiteralHandle value memberExpr.Type)
                | None -> None

            // 3. Constant Literals
            | Constant(value, t) ->
                Some (toLiteralHandle value t)

            // 4. Binary Expressions (Arithmetic, Logical, Equality, Null checks)
            | Binary(op, left, right) ->
                // Check if this is an equality/inequality comparison with null
                if op = ExpressionType.NotEqual && isNullConstantExpr right then
                    tryTranslate paramName left |> Option.map PolarsWrapper.IsNotNull
                elif op = ExpressionType.NotEqual && isNullConstantExpr left then
                    tryTranslate paramName right |> Option.map PolarsWrapper.IsNotNull
                elif op = ExpressionType.Equal && isNullConstantExpr right then
                    tryTranslate paramName left |> Option.map PolarsWrapper.IsNull
                elif op = ExpressionType.Equal && isNullConstantExpr left then
                    tryTranslate paramName right |> Option.map PolarsWrapper.IsNull
                else
                    match tryTranslate paramName left, tryTranslate paramName right with
                    | Some l, Some r -> Some (translateBinary op l r)
                    | _ -> None

            // 5. Type Casting & Conversions
            | Unary(ExpressionType.Convert, operandExpr)
            | Unary(ExpressionType.ConvertChecked, operandExpr) ->
                match tryTranslate paramName operandExpr with
                | Some inner -> tryTranslateCast expr.Type inner
                | None -> None

            // 6. Pure Unary Operations (Not, Negate, etc.)
            | Unary(op, operandExpr) ->
                match tryTranslate paramName operandExpr with
                | Some inner -> translateUnary op inner
                | None -> None

            // 7. Ternary Conditional Operator (test ? ifTrue : ifFalse) -> when/then/otherwise
            | :? ConditionalExpression as condExpr ->
                let rec unwrapConvert (e: Expression) =
                    match e with
                    | :? UnaryExpression as u when u.NodeType = ExpressionType.Convert || u.NodeType = ExpressionType.ConvertChecked ->
                        unwrapConvert u.Operand
                    | other -> other

                match condExpr.Test with
                | :? BinaryExpression as b when b.NodeType = ExpressionType.Equal ->
                    let isNullCheck =
                        isNullConstantExpr b.Right && (unwrapConvert b.Left) <> null ||
                        isNullConstantExpr b.Left && (unwrapConvert b.Right) <> null

                    if isNullCheck then
                        match tryTranslate paramName condExpr.IfFalse, tryTranslate paramName condExpr.IfTrue with
                        | Some colExpr, Some fallbackExpr ->
                            Some (PolarsWrapper.FillNull(colExpr, fallbackExpr))
                        | _ -> None
                    else
                        match tryTranslate paramName condExpr.Test,
                              tryTranslate paramName condExpr.IfTrue,
                              tryTranslate paramName condExpr.IfFalse with
                        | Some testHandle, Some trueHandle, Some falseHandle ->
                            Some (PolarsWrapper.IfElse(testHandle, trueHandle, falseHandle))
                        | _ -> None

                | _ ->
                    match tryTranslate paramName condExpr.Test,
                          tryTranslate paramName condExpr.IfTrue,
                          tryTranslate paramName condExpr.IfFalse with
                    | Some testHandle, Some trueHandle, Some falseHandle ->
                        Some (PolarsWrapper.IfElse(testHandle, trueHandle, falseHandle))
                    | _ -> None

            | _ -> None
        with _ ->
            None