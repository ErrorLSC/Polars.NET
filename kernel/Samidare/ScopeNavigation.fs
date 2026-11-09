namespace Kaminari.Samidare

open System
open System.Reflection
open System.Linq.Expressions
open Kaminari.Sazanami.Helpers

[<AutoOpen>]
module internal ScopeNavigation =

    /// Checks if a Type is a compiler-generated or runtime tuple/anonymous/transparent wrapper
    let internal isTupleOrAnonType (t: Type) : bool =
        ClrTypeHelper.IsTupleOrAnonymousType t

    /// Checks if a Type is a closed IGrouping<TKey, TElement>.
    let internal isGroupingType (t: Type) : bool =
        not (isNull t) && t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<System.Linq.IGrouping<_, _>>

    /// Checks if a member access is for Nullable<T>.Value or F# option.Value
    let internal isNullableValue (m: MemberInfo) : bool =
        not (isNull m)
        && m.Name = "Value"
        && not (isNull m.DeclaringType)
        && (Nullable.GetUnderlyingType m.DeclaringType <> null
            || let struct (_, kind) = ClrTypeHelper.GetOptionalInfo m.DeclaringType in kind <> OptionalKind.None)

    /// Checks if a member access is for Nullable<T>.HasValue
    let internal isNullableHasValue (m: MemberInfo) : bool =
        not (isNull m)
        && m.Name = "HasValue"
        && not (isNull m.DeclaringType)
        && Nullable.GetUnderlyingType m.DeclaringType <> null

    /// Checks if a member belongs to F#'s internal AnonymousObject wrapper
    let internal isFSharpAnonymousMember (m: MemberInfo) : bool =
        not (isNull m)
        && not (isNull m.DeclaringType)
        && not (isNull m.DeclaringType.FullName)
        && m.DeclaringType.FullName.Contains "AnonymousObject"
        && m.Name = "Item"

    /// Unified check for whether a member access is a transparent navigation/scope wrapper
    /// (such as Nullable<T>.Value, Roslyn TransparentIdentifier, Zip/Join tuple item,
    /// or F#/C# anonymous scope holder) rather than a physical scalar column.
    let internal isNavigationMember (m: MemberInfo) : bool =
        if isNull m || isNull m.DeclaringType then
            false
        elif isNullableValue m then
            true
        else
            let memberName = m.Name
            let isNonScalarMember =
                match m with
                | :? PropertyInfo as p -> not (ClrTypeHelper.IsScalarType p.PropertyType)
                | :? FieldInfo as f -> not (ClrTypeHelper.IsScalarType f.FieldType)
                | _ -> false

            memberName.Contains "TransparentIdentifier"
            || memberName.StartsWith "<>h__"
            || (isTupleOrAnonType m.DeclaringType
                || memberName = "First"
                || memberName = "Second"
                || memberName = "Third"
                || memberName = "Rest")
               && isNonScalarMember

    /// Checks if a ParameterExpression belongs to the active query context or a synthetic tuple/transparent scope
    let internal isSyntheticScopeParam (ctxContainsParam: string -> bool) (p: ParameterExpression) : bool =
        not (isNull p)
        && (ctxContainsParam p.Name
            || not (isNull p.Name)
               && (p.Name.StartsWith "_arg"
                   || p.Name.StartsWith "tupled"
                   || p.Name.Contains "TransparentIdentifier"
                   || p.Name.StartsWith "<>h__")
            || isTupleOrAnonType p.Type)

    /// Member path from the parameter `rootName`: Some ["t"; "x"] for root.t.x, Some [] for root itself.
    let rec internal tryMemberPath (rootName: string) (expr: Expression) : string list option =
        match unwrap expr with
        | :? ParameterExpression as p when p.Name = rootName -> Some []
        | MemberAccess(target, m) when not (isNull target) -> tryMemberPath rootName target |> Option.map (fun path -> path @ [ m.Name ])
        | _ -> None

    let (|StructFieldAccess|_|) (expr: Expression) : (Expression * string) option =
        match expr with
        | MemberAccess(target, m) when
            not (isNull target)
            && target.NodeType = ExpressionType.MemberAccess
            && ClrTypeHelper.IsStructType target.Type
            ->
            match target with
            | :? MemberExpression as parentMe when isNavigationMember parentMe.Member ->
                None
            | _ -> Some(target, m.Name)
        | _ -> None

    [<TailCall>]
    let rec internal isTransparentChain (expr: Expression) : bool =
        match expr with
        | null -> false
        | :? ParameterExpression -> true
        | MemberAccess(target, m) when isNavigationMember m ->
            isTransparentChain target
        | _ -> false

    /// Active pattern to strip convert/quote wrappers and resolve root column names,
    /// seamlessly unwrapping F# AnonymousObject tuple chains
    let rec (|ExtractColumnName|_|) (expr: Expression) : string option =
        match expr with
        | null -> None
        | Unary(ExpressionType.Convert, inner)
        | Unary(ExpressionType.ConvertChecked, inner)
        | Unary(ExpressionType.Quote, inner) -> (|ExtractColumnName|_|) inner
        // Direct member: e.Name
        | MemberAccess(p, m) when not (isNull p) && p.NodeType = ExpressionType.Parameter -> Some m.Name
        // Arbitrary F# AnonymousObject / Tuple nesting: tuple.Item1.Name, tuple.Item8.Item1.Name, etc.
        | MemberAccess(target, m) when not (isNull target) && isTransparentChain target -> Some m.Name
        | _ -> None
