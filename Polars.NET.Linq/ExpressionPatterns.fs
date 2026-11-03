namespace Polars.NET.Linq

open System
open System.Linq.Expressions
open Polars.NET.Core.Helpers

[<AutoOpen>]
module internal OptionComputationExpression =
    type OptionBuilder() =
        member inline _.Bind(m: 'T option, [<InlineIfLambda>] f: 'T -> 'U option) : 'U option = Option.bind f m
        member inline _.Return(x: 'T) : 'T option = Some x
        member inline _.ReturnFrom(m: 'T option) : 'T option = m
        member inline _.Zero() : unit option = Some()

        member inline _.Combine(a: unit option, b: 'T option) : 'T option =
            match a with
            | Some() -> b
            | None -> None

        member inline _.Delay([<InlineIfLambda>] f: unit -> 'T option) : 'T option = f ()

    let option = OptionBuilder()

    let inline guard (cond: bool) : unit option = if cond then Some() else None

[<AutoOpen>]
module internal ExpressionPatterns =

    /// Evaluates expressions that resolve to values (e.g., closures, captured variables, local props, method calls)
    let internal tryEvaluate (expr: Expression) : obj option =
        PartialEvaluator.tryEvaluate expr

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

    /// Active pattern to recursively unwrap Quotations, Converts, and other Unary wrappers
    /// to cleanly extract an underlying LambdaExpression
    let rec (|CleanLambda|_|) (expr: Expression) : LambdaExpression option =
        match expr with
        | null -> None
        | :? LambdaExpression as l -> Some l
        | Unary(ExpressionType.Quote, inner)
        | Unary(ExpressionType.Convert, inner)
        | Unary(ExpressionType.ConvertChecked, inner) -> (|CleanLambda|_|) inner
        | _ -> None

    /// Recursively strips Quote, Convert, and ConvertChecked unary wrappers from an Expression.
    let rec internal unwrap (expr: Expression) : Expression =
        match expr with
        | null -> null
        | Unary(ExpressionType.Convert, inner)
        | Unary(ExpressionType.ConvertChecked, inner)
        | Unary(ExpressionType.Quote, inner) -> unwrap inner
        | other -> other

    /// Checks if an expression represents a null literal (or wrapped null constant).
    let internal isNullConstantExpr (e: Expression) : bool =
        match unwrap e with
        | null -> true
        | :? ConstantExpression as c -> isNull c.Value
        | _ -> false

    let (|ArrayIndexOp|_|) (expr: Expression) : (Expression * Expression) option =
        match expr with
        | :? BinaryExpression as bin when bin.NodeType = ExpressionType.ArrayIndex -> Some(bin.Left, bin.Right)
        | :? MethodCallExpression as m when
            not (isNull m.Object)
            && m.Object.Type.IsArray
            && (m.Method.Name = "Get" || m.Method.Name = "Address")
            && m.Arguments.Count = 1
            ->
            Some(m.Object, m.Arguments.[0])
        | :? IndexExpression as idx when idx.Arguments.Count = 1 && (idx.Object.Type.IsArray || typeof<System.Collections.IEnumerable>.IsAssignableFrom idx.Object.Type) ->
            Some(idx.Object, idx.Arguments.[0])
        | _ -> None
