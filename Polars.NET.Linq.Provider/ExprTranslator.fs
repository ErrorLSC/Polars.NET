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

    /// Translates LINQ Expressions into native Polars ExprHandle
    let rec translate (paramName: string) (expr: Expression) : ExprHandle =
        match expr with
        // Property / Field on row parameter (e.g., x.Age -> expr_col("Age"))
        | MemberAccess(paramExpr, memberInfo) when paramExpr <> null && paramExpr.NodeType = ExpressionType.Parameter ->
            PolarsWrapper.Col memberInfo.Name

        // Captured variables or local constants (e.g., x.Age > threshold)
        | MemberAccess _ as memberExpr ->
            match tryEvaluate memberExpr with
            | Some value -> toLiteralHandle value memberExpr.Type
            | None -> failwithf "Failed to evaluate expression: %A" memberExpr

        // Direct constant
        | Constant(value, t) ->
            toLiteralHandle value t

        // Binary operations (e.g., a > b, a == b)
        | Binary(op, left, right) ->
            let l = translate paramName left
            let r = translate paramName right
            translateBinary op l r

        // Unary NOT (e.g., !x.IsActive)
        | Unary(ExpressionType.Not, operand) ->
            let op = translate paramName operand
            PolarsWrapper.Not op

        // Type conversions (e.g., (int)x.Foo)
        | Unary(ExpressionType.Convert, operand) ->
            translate paramName operand

        | other ->
            failwithf "Expression '%A' is not supported for translation to Polars Expr" other
