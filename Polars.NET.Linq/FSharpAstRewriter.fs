namespace Polars.NET.Linq

open System
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Microsoft.FSharp.Quotations

module internal FSharpAst =

    let private unwrapQuote (e: Expression) =
        match e with
        | null -> null
        | :? UnaryExpression as u when u.NodeType = ExpressionType.Quote || u.NodeType = ExpressionType.Convert ->
            u.Operand
        | other -> other

    let rec private areExpressionsEqual (e1: Expression) (e2: Expression) : bool =
        if obj.ReferenceEquals(e1, e2) then true
        elif isNull e1 || isNull e2 then false
        elif e1.NodeType <> e2.NodeType || e1.Type <> e2.Type then false
        else
            match e1, e2 with
            | (:? ParameterExpression as p1), (:? ParameterExpression as p2) ->
                p1 = p2 || (p1.Name = p2.Name && p1.Type = p2.Type)
            | (:? ConstantExpression as c1), (:? ConstantExpression as c2) ->
                obj.Equals(c1.Value, c2.Value)
            | (:? MemberExpression as m1), (:? MemberExpression as m2) ->
                m1.Member = m2.Member && areExpressionsEqual m1.Expression m2.Expression
            | (:? MethodCallExpression as mc1), (:? MethodCallExpression as mc2) ->
                mc1.Method = mc2.Method
                && areExpressionsEqual mc1.Object mc2.Object
                && mc1.Arguments.Count = mc2.Arguments.Count
                && Seq.forall2 areExpressionsEqual mc1.Arguments mc2.Arguments
            | (:? BinaryExpression as b1), (:? BinaryExpression as b2) ->
                b1.Method = b2.Method
                && areExpressionsEqual b1.Left b2.Left
                && areExpressionsEqual b1.Right b2.Right
            | (:? UnaryExpression as u1), (:? UnaryExpression as u2) ->
                u1.Method = u2.Method && areExpressionsEqual u1.Operand u2.Operand
            | (:? NewExpression as n1), (:? NewExpression as n2) ->
                n1.Constructor = n2.Constructor
                && n1.Arguments.Count = n2.Arguments.Count
                && Seq.forall2 areExpressionsEqual n1.Arguments n2.Arguments
            | (:? ConditionalExpression as c1), (:? ConditionalExpression as c2) ->
                areExpressionsEqual c1.Test c2.Test
                && areExpressionsEqual c1.IfTrue c2.IfTrue
                && areExpressionsEqual c1.IfFalse c2.IfFalse
            | (:? InvocationExpression as inv1), (:? InvocationExpression as inv2) ->
                areExpressionsEqual inv1.Expression inv2.Expression
                && inv1.Arguments.Count = inv2.Arguments.Count
                && Seq.forall2 areExpressionsEqual inv1.Arguments inv2.Arguments
            | (:? LambdaExpression as l1), (:? LambdaExpression as l2) ->
                l1.Parameters.Count = l2.Parameters.Count
                && Seq.forall2 (fun (p1: ParameterExpression) (p2: ParameterExpression) -> p1.Type = p2.Type && p1.Name = p2.Name) l1.Parameters l2.Parameters
                && areExpressionsEqual l1.Body l2.Body
            | (:? MemberInitExpression as mi1), (:? MemberInitExpression as mi2) ->
                areExpressionsEqual mi1.NewExpression mi2.NewExpression
                && mi1.Bindings.Count = mi2.Bindings.Count
                && Seq.forall2 (fun (b1: MemberBinding) (b2: MemberBinding) ->
                    b1.BindingType = b2.BindingType && b1.Member = b2.Member
                    && (match b1, b2 with
                        | (:? MemberAssignment as ma1), (:? MemberAssignment as ma2) -> areExpressionsEqual ma1.Expression ma2.Expression
                        | _ -> false)) mi1.Bindings mi2.Bindings
            | (:? NewArrayExpression as na1), (:? NewArrayExpression as na2) ->
                na1.Expressions.Count = na2.Expressions.Count
                && Seq.forall2 areExpressionsEqual na1.Expressions na2.Expressions
            | _ -> false

    let rec private computeStructuralHash (e: Expression) : int =
        if isNull e then 0
        else
            let h = int e.NodeType * 397 ^^^ (if isNull e.Type then 0 else e.Type.GetHashCode())
            match e with
            | :? ParameterExpression as p ->
                h * 31 ^^^ (if isNull p.Name then 0 else p.Name.GetHashCode())
            | :? ConstantExpression as c ->
                h * 31 ^^^ (if isNull c.Value then 0 else c.Value.GetHashCode())
            | :? MemberExpression as m ->
                (h * 31 ^^^ m.Member.GetHashCode()) * 31 ^^^ computeStructuralHash m.Expression
            | :? MethodCallExpression as mc ->
                let hMethod = (h * 31 ^^^ mc.Method.GetHashCode()) * 31 ^^^ computeStructuralHash mc.Object
                mc.Arguments |> Seq.fold (fun acc a -> acc * 31 ^^^ computeStructuralHash a) hMethod
            | :? BinaryExpression as b ->
                (h * 31 ^^^ computeStructuralHash b.Left) * 31 ^^^ computeStructuralHash b.Right
            | :? UnaryExpression as u ->
                h * 31 ^^^ computeStructuralHash u.Operand
            | :? NewExpression as ne ->
                let hCtor = h * 31 ^^^ (if isNull ne.Constructor then 0 else ne.Constructor.GetHashCode())
                ne.Arguments |> Seq.fold (fun acc a -> acc * 31 ^^^ computeStructuralHash a) hCtor
            | :? MemberInitExpression as mie ->
                let hNew = computeStructuralHash mie.NewExpression
                mie.Bindings |> Seq.fold (fun acc b ->
                    match b with
                    | :? MemberAssignment as ma -> acc * 31 ^^^ ma.Member.GetHashCode() * 31 ^^^ computeStructuralHash ma.Expression
                    | _ -> acc * 31 ^^^ b.Member.GetHashCode()) hNew
            | :? NewArrayExpression as nae ->
                nae.Expressions |> Seq.fold (fun acc a -> acc * 31 ^^^ computeStructuralHash a) h
            | _ -> h

    type private ExpressionStructuralEqualityComparer() =
        interface IEqualityComparer<Expression> with
            member this.Equals(x, y) = areExpressionsEqual x y
            member this.GetHashCode(obj) = computeStructuralHash obj

        static member val Instance = ExpressionStructuralEqualityComparer() :> IEqualityComparer<Expression>

    /// Visitor that replaces parameter expressions by reference
    type private ParameterReplacer(paramMap: Dictionary<ParameterExpression, Expression>) =
        inherit ExpressionVisitor()

        override this.VisitParameter(node: ParameterExpression) =
            match paramMap.TryGetValue(node) with
            | true, replacement -> replacement
            | false, _ -> base.VisitParameter(node)

    /// Visitor that replaces expressions matching keys structurally
    type private StructuralExpressionReplacer(exprMap: Dictionary<Expression, Expression>) =
        inherit ExpressionVisitor()

        override this.Visit(node: Expression) =
            if isNull node then
                null
            else
                match exprMap.TryGetValue(node) with
                | true, replacement -> replacement
                | false, _ ->
                    let unwrapped = unwrapQuote node
                    if not (obj.ReferenceEquals(unwrapped, node)) then
                        match exprMap.TryGetValue(unwrapped) with
                        | true, replacement -> replacement
                        | false, _ -> base.Visit node
                    else
                        base.Visit node

    let private queryableGroupBy3Method =
        typeof<Queryable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
        |> Array.find (fun m ->
            m.Name = "GroupBy"
            && m.GetGenericArguments().Length = 3
            && m.GetParameters().Length = 3)

    let private queryableGroupJoin5Method =
        typeof<Queryable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
        |> Array.find (fun m ->
            m.Name = "GroupJoin"
            && m.GetGenericArguments().Length = 4
            && m.GetParameters().Length = 5)

    let private queryableGroupJoin6Method =
        typeof<Queryable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
        |> Array.find (fun m ->
            m.Name = "GroupJoin"
            && m.GetGenericArguments().Length = 4
            && m.GetParameters().Length = 6)

    let private leafExpressionConverterType =
        typeof<int option>.Assembly.GetType("Microsoft.FSharp.Linq.RuntimeHelpers.LeafExpressionConverter")

    let private convEnvType =
        typeof<int option>.Assembly.GetType("Microsoft.FSharp.Linq.RuntimeHelpers.LeafExpressionConverter+ConvEnv")

    let private convEnvCtor =
        if isNull convEnvType then null
        else
            let ctors = convEnvType.GetConstructors(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
            if ctors.Length > 0 then ctors.[0] else null

    let private convExprMethod =
        if isNull leafExpressionConverterType then null
        else
            leafExpressionConverterType.GetMethod(
                "ConvExprToLinqInContext",
                BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Static
            )

    let private evalExpr (e: Expression) : obj =
        match e with
        | :? ConstantExpression as c -> c.Value
        | _ ->
            let lam = Expression.Lambda(e)
            lam.Compile().DynamicInvoke()

    let private tryRewriteQuotationToLambda (mc: MethodCallExpression) : Expression option =
        if mc.Method.Name = "QuotationToLambdaExpression" && mc.Arguments.Count = 1 then
            let arg = unwrapQuote mc.Arguments.[0]
            match arg with
            | :? MethodCallExpression as subMc when (subMc.Method.Name = "SubstHelper" || subMc.Method.Name = "SubstHelperRaw") && subMc.Arguments.Count = 3 ->
                try
                    let qExpr = subMc.Arguments.[0]
                    let varsExpr = subMc.Arguments.[1]
                    let argsExpr = subMc.Arguments.[2]

                    let q = evalExpr qExpr :?> Expr
                    let vars = evalExpr varsExpr :?> Var array

                    let argExpressions: Expression list =
                        match argsExpr with
                        | :? NewArrayExpression as nae ->
                            nae.Expressions
                            |> Seq.map (fun el ->
                                match unwrapQuote el with
                                | :? UnaryExpression as u when u.NodeType = ExpressionType.Convert || u.NodeType = ExpressionType.ConvertChecked ->
                                    u.Operand
                                | other -> other)
                            |> Seq.toList
                        | _ -> []

                    if vars.Length = argExpressions.Length && not (isNull convEnvCtor) && not (isNull convExprMethod) then
                        let varMap =
                            Seq.zip vars argExpressions
                            |> Seq.map (fun (v, e) -> (v, e))
                            |> Map.ofSeq

                        let env = convEnvCtor.Invoke [| box varMap |]
                        let converted = convExprMethod.Invoke(null, [| env; box q |]) :?> Expression
                        match converted with
                        | :? LambdaExpression as lam ->
                            Some (Expression.Quote lam :> Expression)
                        | other ->
                            Some other
                    else
                        None
                with _ ->
                    None
            | _ ->
                None
        else
            None

    let private isIdentityPairing (lam: LambdaExpression) : bool =
        if lam.Parameters.Count <> 2 then false
        else
            let p1 = lam.Parameters.[0]
            let p2 = lam.Parameters.[1]
            let rec unwrapAll (e: Expression) =
                match e with
                | null -> null
                | :? UnaryExpression as u when
                    u.NodeType = ExpressionType.Quote
                    || u.NodeType = ExpressionType.Convert
                    || u.NodeType = ExpressionType.ConvertChecked ->
                    unwrapAll u.Operand
                | _ -> e

            let isP1 (e: Expression) =
                match unwrapAll e with
                | :? ParameterExpression as p -> p = p1
                | _ -> false

            let isP2 (e: Expression) =
                match unwrapAll e with
                | :? ParameterExpression as p -> p = p2
                | _ -> false

            match unwrapAll lam.Body with
            | :? NewExpression as ne when ne.Arguments.Count = 2 ->
                isP1 ne.Arguments.[0] && isP2 ne.Arguments.[1]
            | :? MemberInitExpression as mie when mie.Bindings.Count = 2 ->
                let b1 = mie.Bindings.[0]
                let b2 = mie.Bindings.[1]
                match b1, b2 with
                | (:? MemberAssignment as ma1), (:? MemberAssignment as ma2) ->
                    (ma1.Member.Name = "Item1" && isP1 ma1.Expression && ma2.Member.Name = "Item2" && isP2 ma2.Expression)
                    || (ma1.Member.Name = "Item2" && isP2 ma1.Expression && ma2.Member.Name = "Item1" && isP1 ma2.Expression)
                | _ -> false
            | _ -> false

    type private GroupJoinSelectorSubstitutor(pTuple: ParameterExpression, pOuter: ParameterExpression, pInner: ParameterExpression, rawPairExpr: Expression) =
        inherit ExpressionVisitor()

        override this.VisitParameter(node: ParameterExpression) =
            if node = pTuple then
                rawPairExpr
            else
                base.VisitParameter node

        override this.VisitMember(node: MemberExpression) =
            let visitedExpr = this.Visit node.Expression
            match visitedExpr with
            | :? ParameterExpression as p when p = pTuple ->
                if node.Member.Name = "Item1" || node.Member.Name = "Fst" then
                    pOuter :> Expression
                elif node.Member.Name = "Item2" || node.Member.Name = "Snd" then
                    pInner :> Expression
                else
                    base.VisitMember node
            | _ ->
                if visitedExpr = node.Expression then node :> Expression
                else Expression.MakeMemberAccess(visitedExpr, node.Member) :> Expression

        override this.VisitMethodCall(mc: MethodCallExpression) =
            if (mc.Method.Name = "Fst" || mc.Method.Name = "Snd") && mc.Arguments.Count = 1 then
                match this.Visit mc.Arguments.[0] with
                | :? ParameterExpression as p when p = pTuple ->
                    if mc.Method.Name = "Fst" then pOuter :> Expression else pInner :> Expression
                | _ -> base.VisitMethodCall mc
            elif (mc.Method.Name = "get_Item1" || mc.Method.Name = "get_Item2") && not (isNull mc.Object) && mc.Arguments.Count = 0 then
                match this.Visit mc.Object with
                | :? ParameterExpression as p when p = pTuple ->
                    if mc.Method.Name = "get_Item1" then pOuter :> Expression else pInner :> Expression
                | _ -> base.VisitMethodCall mc
            else
                base.VisitMethodCall mc

    let private extractMemberMap (ne: NewExpression) =
        if isNull ne.Members || ne.Members.Count = 0 then
            ne.Arguments
            |> Seq.mapi (fun i arg ->
                let name =
                    if i = 7 && not (isNull ne.Type) && (ne.Type.Name.StartsWith "Tuple`8" || ne.Type.Name.StartsWith "ValueTuple`8") then
                        "Rest"
                    else
                        sprintf "Item%d" (i + 1)
                name, arg)
            |> Map.ofSeq
        else
            Seq.zip ne.Members ne.Arguments
            |> Seq.map (fun (m, arg) -> m.Name, arg)
            |> Map.ofSeq

    let private isTupleOrAnon (t: Type) =
        not (isNull t)
        && (t.Name.StartsWith "AnonymousObject"
            || t.Name.StartsWith "Tuple"
            || t.Name.StartsWith "ValueTuple"
            || t.Name.Contains "TransparentIdentifier")

    /// 1. Beta-reduction: reduce curried F# closure .Invoke(...), tuple operators, and structural expressions
    let rec betaReduce (expr: Expression) : Expression =
        match expr with
        | null -> null
        | :? MethodCallExpression as mc when mc.Method.Name = "Invoke" && not (isNull mc.Object) ->
            let reducedObj = betaReduce mc.Object
            let reducedArgs = mc.Arguments |> Seq.map betaReduce |> Seq.toList

            let rec unwrapLambda (e: Expression) : LambdaExpression option =
                match e with
                | null -> None
                | :? LambdaExpression as l -> Some l
                | :? UnaryExpression as u when u.NodeType = ExpressionType.Quote || u.NodeType = ExpressionType.Convert ->
                    unwrapLambda u.Operand
                | _ -> None

            match unwrapLambda reducedObj with
            | Some lambda when lambda.Parameters.Count = reducedArgs.Length ->
                let dict = Dictionary<ParameterExpression, Expression>(HashIdentity.Reference)

                List.zip (lambda.Parameters |> Seq.toList) reducedArgs
                |> List.iter (fun (p, arg) -> dict.[p] <- arg)

                let substituted = ParameterReplacer(dict).Visit(lambda.Body)
                betaReduce substituted
            | _ ->
                let newArgs = reducedArgs |> List.toArray

                if isNull mc.Object then
                    Expression.Call(mc.Method, newArgs) :> Expression
                else
                    Expression.Call(mc.Object, mc.Method, newArgs) :> Expression

        | :? InvocationExpression as inv ->
            let reducedTarget = betaReduce inv.Expression
            let reducedArgs = inv.Arguments |> Seq.map betaReduce |> Seq.toList

            match reducedTarget with
            | :? LambdaExpression as lambda when lambda.Parameters.Count = reducedArgs.Length ->
                let dict = Dictionary<ParameterExpression, Expression>(HashIdentity.Reference)

                List.zip (lambda.Parameters |> Seq.toList) reducedArgs
                |> List.iter (fun (p, arg) -> dict.[p] <- arg)

                let substituted = ParameterReplacer(dict).Visit(lambda.Body)
                betaReduce substituted
            | _ -> Expression.Invoke(reducedTarget, reducedArgs |> List.toArray) :> Expression

        // Normalization of Operators.Fst and Operators.Snd
        | :? MethodCallExpression as mc when
            not (isNull mc.Method.DeclaringType)
            && (mc.Method.DeclaringType.FullName = "Microsoft.FSharp.Core.Operators"
                || mc.Method.DeclaringType.Name = "Operators")
            && (mc.Method.Name = "Fst" || mc.Method.Name = "Snd")
            && mc.Arguments.Count = 1 ->

            let target = betaReduce mc.Arguments.[0]
            let propName = if mc.Method.Name = "Fst" then "Item1" else "Item2"
            Expression.PropertyOrField(target, propName) :> Expression

        | :? MethodCallExpression as mc ->
            let newObj = if isNull mc.Object then null else betaReduce mc.Object
            let newArgs = mc.Arguments |> Seq.map betaReduce |> Seq.toArray

            if isNull newObj then
                Expression.Call(mc.Method, newArgs) :> Expression
            else
                Expression.Call(newObj, mc.Method, newArgs) :> Expression

        | :? LambdaExpression as l ->
            let newBody = betaReduce l.Body
            Expression.Lambda(l.Type, newBody, l.Parameters) :> Expression

        | :? UnaryExpression as u ->
            let newOp = betaReduce u.Operand

            if newOp = u.Operand then
                u :> Expression
            else
                Expression.MakeUnary(u.NodeType, newOp, u.Type, u.Method) :> Expression

        | :? BinaryExpression as b ->
            let newLeft = betaReduce b.Left
            let newRight = betaReduce b.Right

            if newLeft = b.Left && newRight = b.Right then
                b :> Expression
            else
                Expression.MakeBinary(b.NodeType, newLeft, newRight, b.IsLiftedToNull, b.Method) :> Expression

        | :? ConditionalExpression as cond ->
            let newTest = betaReduce cond.Test
            let newIfTrue = betaReduce cond.IfTrue
            let newIfFalse = betaReduce cond.IfFalse
            if newTest = cond.Test && newIfTrue = cond.IfTrue && newIfFalse = cond.IfFalse then
                cond :> Expression
            else
                Expression.Condition(newTest, newIfTrue, newIfFalse) :> Expression

        | :? MemberExpression as me ->
            let newTarget = if isNull me.Expression then null else betaReduce me.Expression
            if newTarget = me.Expression then me :> Expression
            else Expression.MakeMemberAccess(newTarget, me.Member) :> Expression

        | :? NewExpression as ne ->
            let newArgs = ne.Arguments |> Seq.map betaReduce |> Seq.toList
            let changed = Seq.zip ne.Arguments newArgs |> Seq.exists (fun (oldA, newA) -> oldA <> newA)
            if not changed then
                ne :> Expression
            elif isNull ne.Members || ne.Members.Count = 0 then
                Expression.New(ne.Constructor, newArgs) :> Expression
            else
                Expression.New(ne.Constructor, newArgs, ne.Members) :> Expression

        | :? MemberInitExpression as mie ->
            let newNew = betaReduce mie.NewExpression :?> NewExpression
            let newBindings =
                mie.Bindings
                |> Seq.map (fun b ->
                    match b with
                    | :? MemberAssignment as ma ->
                        let newExpr = betaReduce ma.Expression
                        if newExpr = ma.Expression then ma :> MemberBinding
                        else Expression.Bind(ma.Member, newExpr) :> MemberBinding
                    | other -> other)
                |> Seq.toList
            Expression.MemberInit(newNew, newBindings) :> Expression

        | :? NewArrayExpression as nae when nae.NodeType = ExpressionType.NewArrayInit ->
            let newElems = nae.Expressions |> Seq.map betaReduce |> Seq.toList
            let changed = Seq.zip nae.Expressions newElems |> Seq.exists (fun (oldA, newA) -> oldA <> newA)
            if not changed then nae :> Expression
            else Expression.NewArrayInit(nae.Type.GetElementType(), newElems) :> Expression

        | :? TypeBinaryExpression as tbe ->
            let newExpr = betaReduce tbe.Expression
            if newExpr = tbe.Expression then tbe :> Expression
            else Expression.TypeIs(newExpr, tbe.TypeOperand) :> Expression

        | other -> other

    /// Helper checking whether a lambda is an identity projection copying properties of an anonymous object or tuple
    let private isIdentityProjection (lambda: LambdaExpression) : bool =
        if lambda.Parameters.Count <> 1 then
            false
        else
            let param = lambda.Parameters.[0]
            let unwrapped = unwrapQuote lambda.Body

            let rec isIdentityMember (p: ParameterExpression) (expectedMember: string) (expr: Expression) =
                match unwrapQuote expr with
                | :? MemberExpression as me ->
                    match unwrapQuote me.Expression with
                    | :? ParameterExpression as pe ->
                        pe = p
                        && (me.Member.Name = expectedMember
                            || (expectedMember = "Rest" && me.Member.Name = "Item8")
                            || (expectedMember = "Item8" && me.Member.Name = "Rest"))
                    | _ -> false
                | :? NewExpression as innerNe when isTupleOrAnon innerNe.Type ->
                    let innerMap = extractMemberMap innerNe
                    innerMap
                    |> Map.forall (fun innerMember innerArg ->
                        match unwrapQuote innerArg with
                        | :? MemberExpression as me ->
                            match unwrapQuote me.Expression with
                            | :? MemberExpression as parentMe ->
                                match unwrapQuote parentMe.Expression with
                                | :? ParameterExpression as pe ->
                                    pe = p
                                    && (parentMe.Member.Name = expectedMember
                                        || (expectedMember = "Rest" && parentMe.Member.Name = "Item8")
                                        || (expectedMember = "Item8" && parentMe.Member.Name = "Rest"))
                                    && me.Member.Name = innerMember
                                | _ -> false
                            | _ -> false
                        | _ -> false)
                | _ -> false

            match unwrapped with
            | :? ParameterExpression as p when p = param -> true
            | :? MemberInitExpression as mie when isTupleOrAnon mie.Type && mie.Type = param.Type ->
                mie.Bindings.Count > 0
                && mie.Bindings
                   |> Seq.forall (function
                       | :? MemberAssignment as ma -> isIdentityMember param ma.Member.Name ma.Expression
                       | _ -> false)
            | :? NewExpression as ne when isTupleOrAnon ne.Type && ne.Type = param.Type && ne.Arguments.Count > 0 ->
                let memberMap = extractMemberMap ne
                memberMap
                |> Map.forall (fun memberName argExpr -> isIdentityMember param memberName argExpr)
            | _ -> false

    /// 2. PipelineNormalizer:
    /// - Strips redundant identity projections (e.g. .Select(_arg => new AnonymousObject(_arg.Item1, ...)))
    /// - Flattens F# synthetic Grouping wrappers (e.g. .GroupBy(...).Select(g => new Grouping(g.Key, g.Select(...))))
    type private PipelineNormalizer() =
        inherit ExpressionVisitor()

        override this.VisitMethodCall(mc: MethodCallExpression) =
            match tryRewriteQuotationToLambda mc with
            | Some rewritten -> this.Visit rewritten
            | None ->
                let visitedMc = base.VisitMethodCall(mc) :?> MethodCallExpression
                this.NormalizeVisitedCall visitedMc

        member private this.NormalizeVisitedCall(visitedMc: MethodCallExpression) : Expression =
            // Check if this method call is Queryable.Select or Enumerable.Select
            if (visitedMc.Method.DeclaringType = typeof<Queryable> || visitedMc.Method.DeclaringType = typeof<Enumerable>)
               && visitedMc.Method.Name = "Select"
               && visitedMc.Arguments.Count = 2 then

                let source = visitedMc.Arguments.[0]
                let selArg = unwrapQuote visitedMc.Arguments.[1]

                match selArg with
                // 1. Redundant Identity Projection Stripper
                | :? LambdaExpression as l when isIdentityProjection l ->
                    source

                // 2. F# Grouping Wrapper Flattener
                | :? LambdaExpression as l when l.Parameters.Count = 1 ->
                    let gParam = l.Parameters.[0]
                    let bodyUnwrapped = unwrapQuote l.Body

                    match bodyUnwrapped with
                    | :? NewExpression as ne when
                        not (isNull ne.Type.FullName) && ne.Type.FullName.Contains "Grouping"
                        && ne.Arguments.Count = 2 ->

                        let keyArg = unwrapQuote ne.Arguments.[0]
                        let isKeyArgMatching =
                            match keyArg with
                            | :? MemberExpression as me ->
                                match unwrapQuote me.Expression with
                                | :? ParameterExpression as p -> p = gParam && me.Member.Name = "Key"
                                | _ -> false
                            | _ -> false

                        if isKeyArgMatching then
                            let elemArg = unwrapQuote ne.Arguments.[1]
                            let sourceUnwrapped = unwrapQuote source

                            match sourceUnwrapped with
                            | :? MethodCallExpression as groupMc when
                                (groupMc.Method.DeclaringType = typeof<Queryable> || groupMc.Method.DeclaringType = typeof<Enumerable>)
                                && groupMc.Method.Name = "GroupBy"
                                && (groupMc.Arguments.Count = 2 || groupMc.Arguments.Count = 3) ->

                                let innerSource = groupMc.Arguments.[0]
                                let keySelector = groupMc.Arguments.[1]

                                match elemArg with
                                // Case A: g.Select(v => elemProjection) -> Queryable.GroupBy(source, keySelector, elemSelector)
                                | :? MethodCallExpression as elemMc when
                                    elemMc.Method.Name = "Select"
                                    && elemMc.Arguments.Count = 2
                                    && (match unwrapQuote elemMc.Arguments.[0] with
                                        | :? ParameterExpression as p -> p = gParam
                                        | _ -> false) ->

                                    let elemSelector = elemMc.Arguments.[1]
                                    let tSource = groupMc.Method.GetGenericArguments().[0]
                                    let tKey = groupMc.Method.GetGenericArguments().[1]
                                    let tElement = elemMc.Method.GetGenericArguments().[1]

                                    let gm = queryableGroupBy3Method.MakeGenericMethod(tSource, tKey, tElement)

                                    Expression.Call(null, gm, innerSource, keySelector, elemSelector) :> Expression

                                // Case B: second arg is just gParam (identity grouping wrapper)
                                | :? ParameterExpression as p when p = gParam ->
                                    sourceUnwrapped

                                | _ -> visitedMc :> Expression
                            | _ ->
                                visitedMc :> Expression
                        else
                            visitedMc :> Expression
                    | _ ->
                        let pTuple = l.Parameters.[0]
                        let sourceUnwrapped = unwrapQuote source

                        match sourceUnwrapped with
                        | :? MethodCallExpression as gjMc when
                            (gjMc.Method.DeclaringType = typeof<Queryable> || gjMc.Method.DeclaringType = typeof<Enumerable>)
                            && gjMc.Method.Name = "GroupJoin"
                            && (gjMc.Arguments.Count = 5 || gjMc.Arguments.Count = 6) ->

                            let joinResSel = unwrapQuote gjMc.Arguments.[4]
                            match joinResSel with
                            | :? LambdaExpression as joinResLambda when isIdentityPairing joinResLambda ->
                                let pOuter = joinResLambda.Parameters.[0]
                                let pInner = joinResLambda.Parameters.[1]
                                let fusedBody = GroupJoinSelectorSubstitutor(pTuple, pOuter, pInner, joinResLambda.Body).Visit(l.Body)
                                let fusedBodyReduced = betaReduce fusedBody
                                let fusedLambda = Expression.Lambda(fusedBodyReduced, joinResLambda.Parameters)

                                let tOuter = gjMc.Method.GetGenericArguments().[0]
                                let tInner = gjMc.Method.GetGenericArguments().[1]
                                let tKey = gjMc.Method.GetGenericArguments().[2]
                                let tResult = fusedLambda.ReturnType

                                if gjMc.Arguments.Count = 5 then
                                    let gm = queryableGroupJoin5Method.MakeGenericMethod(tOuter, tInner, tKey, tResult)
                                    Expression.Call(null, gm, gjMc.Arguments.[0], gjMc.Arguments.[1], gjMc.Arguments.[2], gjMc.Arguments.[3], fusedLambda) :> Expression
                                else
                                    let gm = queryableGroupJoin6Method.MakeGenericMethod(tOuter, tInner, tKey, tResult)
                                    Expression.Call(null, gm, gjMc.Arguments.[0], gjMc.Arguments.[1], gjMc.Arguments.[2], gjMc.Arguments.[3], fusedLambda, gjMc.Arguments.[5]) :> Expression
                            | _ -> visitedMc :> Expression
                        | _ -> visitedMc :> Expression
                | _ -> visitedMc :> Expression
            else
                visitedMc :> Expression

    /// 3. StreamBindingCollector：Track parameters by reference, with fallback to ParamName and TypeName
    type private StreamBindingCollector(bindings: Dictionary<string, NewExpression>, paramBindings: Dictionary<ParameterExpression, NewExpression>) =
        inherit ExpressionVisitor()

        let mutable lastStreamNewExpr: NewExpression option = None

        let registerBinding (p: ParameterExpression) (ne: NewExpression) =
            if isTupleOrAnon p.Type then
                paramBindings.[p] <- ne

                if not (String.IsNullOrEmpty p.Name) then
                    bindings.[p.Name] <- ne

                if not (String.IsNullOrEmpty p.Type.Name) then
                    bindings.[p.Type.Name] <- ne

        override this.VisitMethodCall(mc: MethodCallExpression) =
            if mc.Arguments.Count >= 1 then
                this.Visit(mc.Arguments.[0]) |> ignore

            let currentUpstreamNew = lastStreamNewExpr

            for i in 1 .. mc.Arguments.Count - 1 do
                let argUnwrapped = unwrapQuote mc.Arguments.[i]

                match argUnwrapped with
                | :? LambdaExpression as l ->
                    match currentUpstreamNew with
                    | Some ne ->
                        for p in l.Parameters do
                            registerBinding p ne
                    | None -> ()

                    let rec findNestedNewExpr (e: Expression) : NewExpression option =
                        match unwrapQuote e with
                        | :? NewExpression as ne when isTupleOrAnon ne.Type -> Some ne
                        | :? MethodCallExpression as subMc ->
                            subMc.Arguments
                            |> Seq.rev
                            |> Seq.map unwrapQuote
                            |> Seq.tryPick (function
                                | :? LambdaExpression as subL -> findNestedNewExpr subL.Body
                                | subE -> findNestedNewExpr subE)
                        | _ -> None

                    let bodyUnwrapped = unwrapQuote l.Body

                    let isJoinMethod =
                        mc.Method.Name = "Join"
                        || mc.Method.Name = "GroupJoin"
                        || mc.Method.Name = "LeftJoin"
                        || mc.Method.Name = "RightJoin"
                        || mc.Method.Name = "FullJoin"

                    match findNestedNewExpr bodyUnwrapped with
                    | Some newNe when not isJoinMethod ->
                        lastStreamNewExpr <- Some newNe
                    | _ ->
                        if isJoinMethod then
                            lastStreamNewExpr <- None
                | _ -> ()

            mc :> Expression

    /// 4. LetInliner: Flatten tupledArg.Item8.Item1 or tupledArg.Rest.Item1 into inlined expressions
    type private LetInliner(bindings: Dictionary<string, NewExpression>, paramBindings: Dictionary<ParameterExpression, NewExpression>) =
        inherit ExpressionVisitor()

        let visitingMembers = HashSet<Expression>(HashIdentity.Reference)

        let tryGetBinding (p: ParameterExpression) =
            match paramBindings.TryGetValue(p) with
            | true, ne -> Some ne
            | false, _ ->
                if not (String.IsNullOrEmpty p.Name) && bindings.ContainsKey(p.Name) then
                    Some bindings.[p.Name]
                elif not (String.IsNullOrEmpty p.Type.Name) && bindings.ContainsKey(p.Type.Name) then
                    Some bindings.[p.Type.Name]
                else
                    None

        let tryFindMember (memberMap: Map<string, Expression>) (name: string) =
            match memberMap.TryFind name with
            | Some childExpr -> Some childExpr
            | None when name = "Item8" -> memberMap.TryFind "Rest"
            | None when name = "Rest" -> memberMap.TryFind "Item8"
            | None -> None

        override this.VisitMember(node: MemberExpression) =
            if visitingMembers.Contains node then
                base.VisitMember node
            else
                visitingMembers.Add node |> ignore
                try
                    let visitedExpr = this.Visit node.Expression
                    let unwrappedExpr = unwrapQuote visitedExpr

                    match unwrappedExpr with
                    | :? NewExpression as ne when isTupleOrAnon ne.Type ->
                        let memberMap = extractMemberMap ne

                        match tryFindMember memberMap node.Member.Name with
                        | Some childExpr -> this.Visit childExpr
                        | None -> Expression.MakeMemberAccess(visitedExpr, node.Member) :> Expression

                    | :? ParameterExpression as p when (tryGetBinding p).IsSome ->
                        let ne = (tryGetBinding p).Value
                        let memberMap = extractMemberMap ne

                        match tryFindMember memberMap node.Member.Name with
                        | Some childExpr ->
                            let unwrappedChild = unwrapQuote childExpr

                            match unwrappedChild with
                            | :? NewExpression as childNe when isTupleOrAnon childNe.Type ->
                                let paramMap = Dictionary<Expression, Expression>(ExpressionStructuralEqualityComparer.Instance)
                                let parentMap = extractMemberMap ne

                                for KeyValue(itemName, itemArg) in parentMap do
                                    if itemName <> node.Member.Name && not (itemName = "Rest" && node.Member.Name = "Item8") && not (itemName = "Item8" && node.Member.Name = "Rest") then
                                        let pAccess = Expression.PropertyOrField(p, itemName) :> Expression
                                        paramMap.[itemArg] <- pAccess
                                        let unwrappedArg = unwrapQuote itemArg
                                        if not (obj.ReferenceEquals(unwrappedArg, itemArg)) then
                                            paramMap.[unwrappedArg] <- pAccess

                                let remappedChildNe = StructuralExpressionReplacer(paramMap).Visit(childNe)
                                this.Visit remappedChildNe
                            | _ -> Expression.MakeMemberAccess(visitedExpr, node.Member) :> Expression
                        | None -> Expression.MakeMemberAccess(visitedExpr, node.Member) :> Expression

                    | _ ->
                        if visitedExpr = node.Expression then
                            node :> Expression
                        else
                            Expression.MakeMemberAccess(visitedExpr, node.Member) :> Expression
                finally
                    visitingMembers.Remove node |> ignore

    /// Main rewrite entry point for F# LINQ expressions
    let rewrite (expr: Expression) : Expression =
        let reduced = betaReduce expr
        let normalized = PipelineNormalizer().Visit reduced

        let bindings = Dictionary<string, NewExpression>()
        let paramBindings = Dictionary<ParameterExpression, NewExpression>(HashIdentity.Reference)
        let collector = StreamBindingCollector(bindings, paramBindings)
        collector.Visit normalized |> ignore

        let inliner = LetInliner(bindings, paramBindings)
        let inlined = inliner.Visit normalized

        let postReduced = betaReduce inlined
        PipelineNormalizer().Visit postReduced
