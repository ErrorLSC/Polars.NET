namespace Polars.NET.Linq.Provider

open Polars.NET.Core
open System
open System.Linq.Expressions
open Polars.NET.Core.Helpers

module ExprTranslator =
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
    let private translateUnary (nodeType: ExpressionType) (operand: ExprHandle) : ExprHandle =
        match nodeType with
        | ExpressionType.Not ->
            PolarsWrapper.Not(operand)
        | ExpressionType.Negate
        | ExpressionType.NegateChecked ->
            // Arithmetic negation: -x is equivalent to 0 - x in Polars Expr
            PolarsWrapper.Sub(PolarsWrapper.Lit(0), operand)
        | ExpressionType.UnaryPlus
        | ExpressionType.Convert
        | ExpressionType.ConvertChecked ->
            // Passthrough for basic type promotions
            operand
        | other ->
            failwithf "Unary operator '%A' is not currently supported in Polars LINQ" other

    /// Attempts to translate an Expression AST node to a native Polars ExprHandle.
    /// Returns None if the node contains unsupported logic (e.g. custom C#/F# methods).
    let rec tryTranslate (paramName: string) (expr: Expression) : ExprHandle option =
        try
            match expr with
            // 1. Parameter column access: x.Column
            | MemberAccess(paramExpr, memberInfo) when paramExpr <> null && paramExpr.NodeType = ExpressionType.Parameter ->
                Some(PolarsWrapper.Col(memberInfo.Name))

            // 2. Evaluatable member access: local props, static fields, captured variables
            | MemberAccess _ as memberExpr ->
                match tryEvaluate memberExpr with
                | Some value -> Some(toLiteralHandle value memberExpr.Type)
                | None -> None

            // 3. Constant literal
            | Constant(value, t) ->
                Some(toLiteralHandle value t)

            // 4. Binary operations (arithmetic, comparisons, logic)
            | Binary(op, left, right) ->
                match tryTranslate paramName left, tryTranslate paramName right with
                | Some l, Some r -> Some(translateBinary op l r)
                | _ -> None

            // 5. Unary operations
            | Unary(op, operand) ->
                match tryTranslate paramName operand with
                | Some inner -> Some(translateUnary op inner)
                | None -> None

            | _ -> None
        with _ ->
            None