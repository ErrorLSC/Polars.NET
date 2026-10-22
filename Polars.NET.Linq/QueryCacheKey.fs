namespace Polars.NET.Linq

open System
open System.Collections.Generic
open System.Collections.ObjectModel
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core.Helpers

module internal ExpressionEquality =

    [<TailCall>]
    let rec private tryFindIndex (pe: ParameterExpression) (scope: (ParameterExpression * int) list) : int option =
        match scope with
        | [] -> None
        | (p, idx) :: rest ->
            if obj.ReferenceEquals(p, pe) then
                Some idx
            else
                tryFindIndex pe rest

    [<TailCall>]
    let rec private tryFindMapped
        (pe: ParameterExpression)
        (scope: (ParameterExpression * ParameterExpression) list)
        : ParameterExpression option =
        match scope with
        | [] -> None
        | (pA, pB) :: rest ->
            if obj.ReferenceEquals(pA, pe) then
                Some pB
            else
                tryFindMapped pe rest

    /// Computes structural hash code of an expression tree into a rolling HashCode accumulator.
    let rec computeHash (hc: byref<HashCode>) (scope: (ParameterExpression * int) list) (expr: Expression) : unit =
        if isNull expr then
            hc.Add 0
        else
            hc.Add(int expr.NodeType)
            hc.Add(expr.Type.GetHashCode())

            match expr with
            | :? ConstantExpression as ce ->
                if isNull ce.Value then
                    hc.Add 0
                elif
                    typeof<IQueryable>.IsAssignableFrom ce.Type
                    || typeof<IPolarsPlanSource>.IsAssignableFrom ce.Type
                then
                    // Treat queryable root sources by their element type rather than object identity
                    let elemType =
                        match PolarsTypeHelper.TryGetEnumerableElementType ce.Type with
                        | null -> ce.Type
                        | t -> t

                    hc.Add(elemType.GetHashCode())
                else
                    hc.Add(ce.Value.GetHashCode())

            | :? ParameterExpression as (pe: ParameterExpression) ->
                if not (isNull pe.Name) && pe.Name.StartsWith "@p" then
                    // Extracted query parameter: hash its normalized parameter name
                    hc.Add(pe.Name.GetHashCode())
                else
                    // Lambda parameter: hash by its index in the enclosing scope
                    match tryFindIndex pe scope with
                    | Some idx -> hc.Add idx
                    | None ->
                        if not (isNull pe.Name) then
                            hc.Add(pe.Name.GetHashCode())

            | :? BinaryExpression as be ->
                if not (isNull be.Method) then
                    hc.Add(be.Method.GetHashCode())

                computeHash &hc scope be.Left
                computeHash &hc scope be.Right

                if not (isNull be.Conversion) then
                    computeHash &hc scope be.Conversion

            | :? UnaryExpression as ue ->
                if not (isNull ue.Method) then
                    hc.Add(ue.Method.GetHashCode())

                computeHash &hc scope ue.Operand

            | :? MethodCallExpression as me ->
                hc.Add(me.Method.GetHashCode())

                if not (isNull me.Object) then
                    computeHash &hc scope me.Object

                for arg in me.Arguments do
                    computeHash &hc scope arg

            | :? MemberExpression as me ->
                hc.Add(me.Member.GetHashCode())

                if not (isNull me.Expression) then
                    computeHash &hc scope me.Expression

            | :? LambdaExpression as (le: LambdaExpression) ->
                hc.Add le.Parameters.Count

                let newScope =
                    let baseIdx = scope.Length
                    let mutable s = scope

                    for i = 0 to le.Parameters.Count - 1 do
                        s <- (le.Parameters.[i], baseIdx + i) :: s

                    s

                computeHash &hc newScope le.Body

            | :? NewExpression as ne ->
                if not (isNull ne.Constructor) then
                    hc.Add(ne.Constructor.GetHashCode())

                for arg in ne.Arguments do
                    computeHash &hc scope arg

            | :? MemberInitExpression as mie ->
                computeHash &hc scope mie.NewExpression

                for b in mie.Bindings do
                    hc.Add(b.Member.GetHashCode())
                    hc.Add(int b.BindingType)

                    match b with
                    | :? MemberAssignment as ma -> computeHash &hc scope ma.Expression
                    | _ -> ()

            | :? ConditionalExpression as ce ->
                computeHash &hc scope ce.Test
                computeHash &hc scope ce.IfTrue
                computeHash &hc scope ce.IfFalse

            | :? InvocationExpression as ie ->
                computeHash &hc scope ie.Expression

                for arg in ie.Arguments do
                    computeHash &hc scope arg

            | :? TypeBinaryExpression as tbe ->
                hc.Add(tbe.TypeOperand.GetHashCode())
                computeHash &hc scope tbe.Expression

            | :? NewArrayExpression as nae ->
                for elem in nae.Expressions do
                    computeHash &hc scope elem

            | _ -> ()

    let private compareMembers (a: ReadOnlyCollection<MemberInfo>) (b: ReadOnlyCollection<MemberInfo>) : bool =
        if isNull a && isNull b then
            true
        elif isNull a || isNull b then
            false
        elif a.Count <> b.Count then
            false
        else
            let rec loop i =
                if i >= a.Count then true
                elif a.[i] = b.[i] then loop (i + 1)
                else false

            loop 0

    /// Recursively checks semantic structural equality between two expression trees.
    let rec compareExpr
        (scopeA: (ParameterExpression * ParameterExpression) list)
        (a: Expression)
        (b: Expression)
        : bool =
        if obj.ReferenceEquals(a, b) then
            true
        elif isNull a || isNull b then
            false
        elif a.NodeType <> b.NodeType || a.Type <> b.Type then
            false
        else
            match a, b with
            | (:? ConstantExpression as ca), (:? ConstantExpression as cb) ->
                if isNull ca.Value && isNull cb.Value then
                    true
                elif isNull ca.Value || isNull cb.Value then
                    false
                elif
                    (typeof<IQueryable>.IsAssignableFrom ca.Type
                     || typeof<IPolarsPlanSource>.IsAssignableFrom ca.Type)
                    && (typeof<IQueryable>.IsAssignableFrom cb.Type
                        || typeof<IPolarsPlanSource>.IsAssignableFrom cb.Type)
                then
                    let elemA =
                        match PolarsTypeHelper.TryGetEnumerableElementType ca.Type with
                        | null -> ca.Type
                        | t -> t

                    let elemB =
                        match PolarsTypeHelper.TryGetEnumerableElementType cb.Type with
                        | null -> cb.Type
                        | t -> t

                    elemA = elemB
                else
                    Object.Equals(ca.Value, cb.Value)

            | (:? ParameterExpression as (pa: ParameterExpression)),
              (:? ParameterExpression as (pb: ParameterExpression)) ->
                if not (isNull pa.Name) && pa.Name.StartsWith "@p" then
                    pa.Name = pb.Name && pa.Type = pb.Type
                else
                    match tryFindMapped pa scopeA with
                    | Some mappedB -> obj.ReferenceEquals(mappedB, pb)
                    | None -> pa.Name = pb.Name && pa.Type = pb.Type

            | (:? BinaryExpression as ba), (:? BinaryExpression as bb) ->
                ba.Method = bb.Method
                && compareExpr scopeA ba.Left bb.Left
                && compareExpr scopeA ba.Right bb.Right
                && compareExpr scopeA ba.Conversion bb.Conversion

            | (:? UnaryExpression as ua), (:? UnaryExpression as ub) ->
                ua.Method = ub.Method && compareExpr scopeA ua.Operand ub.Operand

            | (:? MethodCallExpression as ma), (:? MethodCallExpression as mb) ->
                ma.Method = mb.Method
                && compareExpr scopeA ma.Object mb.Object
                && compareSeq scopeA ma.Arguments mb.Arguments

            | (:? MemberExpression as ma), (:? MemberExpression as mb) ->
                ma.Member = mb.Member && compareExpr scopeA ma.Expression mb.Expression

            | (:? LambdaExpression as (la: LambdaExpression)), (:? LambdaExpression as (lb: LambdaExpression)) ->
                if la.Parameters.Count <> lb.Parameters.Count then
                    false
                else
                    let mutable newScope = scopeA
                    let mutable paramsOk = true

                    for i = 0 to la.Parameters.Count - 1 do
                        let pA = la.Parameters.[i]
                        let pB = lb.Parameters.[i]

                        if pA.Type <> pB.Type then
                            paramsOk <- false
                        else
                            newScope <- (pA, pB) :: newScope

                    paramsOk && compareExpr newScope la.Body lb.Body

            | (:? NewExpression as na), (:? NewExpression as nb) ->
                na.Constructor = nb.Constructor
                && compareSeq scopeA na.Arguments nb.Arguments
                && compareMembers na.Members nb.Members

            | (:? MemberInitExpression as mia), (:? MemberInitExpression as mib) ->
                compareExpr scopeA mia.NewExpression mib.NewExpression
                && compareBindings scopeA mia.Bindings mib.Bindings

            | (:? ConditionalExpression as ca), (:? ConditionalExpression as cb) ->
                compareExpr scopeA ca.Test cb.Test
                && compareExpr scopeA ca.IfTrue cb.IfTrue
                && compareExpr scopeA ca.IfFalse cb.IfFalse

            | (:? InvocationExpression as ia), (:? InvocationExpression as ib) ->
                compareExpr scopeA ia.Expression ib.Expression
                && compareSeq scopeA ia.Arguments ib.Arguments

            | (:? TypeBinaryExpression as ta), (:? TypeBinaryExpression as tb) ->
                ta.TypeOperand = tb.TypeOperand
                && compareExpr scopeA ta.Expression tb.Expression

            | (:? NewArrayExpression as na), (:? NewArrayExpression as nb) ->
                compareSeq scopeA na.Expressions nb.Expressions

            | _ -> false

    and compareSeq
        (scopeA: (ParameterExpression * ParameterExpression) list)
        (a: ReadOnlyCollection<Expression>)
        (b: ReadOnlyCollection<Expression>)
        : bool =
        if a.Count <> b.Count then
            false
        else
            let rec loop i =
                if i >= a.Count then true
                elif compareExpr scopeA a.[i] b.[i] then loop (i + 1)
                else false

            loop 0

    and compareBindings
        (scopeA: (ParameterExpression * ParameterExpression) list)
        (a: ReadOnlyCollection<MemberBinding>)
        (b: ReadOnlyCollection<MemberBinding>)
        : bool =
        if a.Count <> b.Count then
            false
        else
            let rec loop i =
                if i >= a.Count then
                    true
                else
                    let ba: MemberBinding = a.[i]
                    let bb: MemberBinding = b.[i]

                    if ba.BindingType <> bb.BindingType || ba.Member <> bb.Member then
                        false
                    else
                        match ba, bb with
                        | (:? MemberAssignment as maa), (:? MemberAssignment as mab) ->
                            if compareExpr scopeA maa.Expression mab.Expression then
                                loop (i + 1)
                            else
                                false
                        | _ -> false

            loop 0

/// Structural equality and hashing engine for LINQ Expression Trees.
/// Implements alpha-equivalence for lambda parameters and semantic comparison for query ASTs.
type internal ExpressionEqualityComparer private () =

    static let instance = ExpressionEqualityComparer()
    static member Instance = instance

    interface IEqualityComparer<Expression> with
        member _.Equals(x: Expression, y: Expression) = ExpressionEquality.compareExpr [] x y

        member _.GetHashCode(obj: Expression) =
            let mutable hc = HashCode()
            ExpressionEquality.computeHash &hc [] obj
            hc.ToHashCode()

    member this.Equals(x: Expression, y: Expression) =
        (this :> IEqualityComparer<Expression>).Equals(x, y)

    member this.GetHashCode(obj: Expression) =
        (this :> IEqualityComparer<Expression>).GetHashCode(obj)

/// Strongly-typed cache key representing a normalized query pipeline.
[<CustomEquality; CustomComparison>]
type internal QueryCacheKey =
    {
        SourceType: Type
        NormalizedExpression: Expression
        HashCode: int
    }

    static member Create(sourceType: Type, normalizedExpr: Expression) : QueryCacheKey =
        let exprHash = ExpressionEqualityComparer.Instance.GetHashCode(normalizedExpr)
        let combinedHash = HashCode.Combine(sourceType.GetHashCode(), exprHash)

        {
            SourceType = sourceType
            NormalizedExpression = normalizedExpr
            HashCode = combinedHash
        }

    interface IEquatable<QueryCacheKey> with
        member this.Equals(other: QueryCacheKey) =
            if isNull (box other) then
                false
            elif obj.ReferenceEquals(this, other) then
                true
            elif this.HashCode <> other.HashCode then
                false
            elif this.SourceType <> other.SourceType then
                false
            else
                ExpressionEqualityComparer.Instance.Equals(this.NormalizedExpression, other.NormalizedExpression)

    override this.Equals(obj: obj) =
        match obj with
        | :? QueryCacheKey as other -> (this :> IEquatable<QueryCacheKey>).Equals(other)
        | _ -> false

    override this.GetHashCode() = this.HashCode

    interface IComparable with
        member this.CompareTo(obj: obj) =
            match obj with
            | :? QueryCacheKey as other ->
                let c = this.HashCode.CompareTo other.HashCode

                if c <> 0 then
                    c
                else
                    String.Compare(this.SourceType.FullName, other.SourceType.FullName, StringComparison.Ordinal)
            | _ -> -1
