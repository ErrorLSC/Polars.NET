namespace Polars.NET.Linq.Provider

open System
open System.Linq.Expressions
open System.Reflection

[<AutoOpen>]
module ExpressionPatterns =

    /// Active pattern for MethodCallExpression
    let (|MethodCall|_|) (expr: Expression) =
        match expr with
        | :? MethodCallExpression as m -> Some(m.Method, m.Object, m.Arguments |> Seq.toList)
        | _ -> None

    /// Active pattern for LambdaExpression
    let (|Lambda|_|) (expr: Expression) =
        match expr with
        | :? LambdaExpression as l -> Some(l.Parameters |> Seq.toList, l.Body)
        | _ -> None

    /// Active pattern for UnaryExpression
    let (|Unary|_|) (expr: Expression) =
        match expr with
        | :? UnaryExpression as u -> Some(u.NodeType, u.Operand)
        | _ -> None

    /// Active pattern for BinaryExpression
    let (|Binary|_|) (expr: Expression) =
        match expr with
        | :? BinaryExpression as b -> Some(b.NodeType, b.Left, b.Right)
        | _ -> None

    /// Active pattern for MemberExpression
    let (|MemberAccess|_|) (expr: Expression) =
        match expr with
        | :? MemberExpression as m -> Some(m.Expression, m.Member)
        | _ -> None

    /// Active pattern for ConstantExpression
    let (|Constant|_|) (expr: Expression) =
        match expr with
        | :? ConstantExpression as c -> Some(c.Value, c.Type)
        | _ -> None

    /// Total Active pattern to strip quotation wrappers added by LINQ provider expression tree building
    let rec (|StripQuotes|) (expr: Expression) =
        match expr with
        | Unary(ExpressionType.Quote, operand) -> (|StripQuotes|) operand
        | _ -> expr

    /// Evaluates expressions that resolve to values (e.g., closures, captured variables, local props)
    let rec tryEvaluate (expr: Expression) : obj option =
        match expr with
        | Constant(value, _) -> Some value
        | MemberAccess(instanceExpr, memberInfo) ->
            let targetObj =
                match instanceExpr with
                | null -> None // Static member
                | exp -> tryEvaluate exp

            match memberInfo with
            | :? PropertyInfo as prop ->
                let inst = match targetObj with Some o -> o | None -> null
                Some (prop.GetValue(inst))
            | :? FieldInfo as field ->
                let inst = match targetObj with Some o -> o | None -> null
                Some (field.GetValue(inst))
            | _ -> None
        | _ -> None
