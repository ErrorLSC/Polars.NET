namespace Polars.NET.Linq

open System
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection

module internal FSharpAst =

    type private StructuralExpressionReplacer(paramMap: Dictionary<string, Expression>) =
        inherit ExpressionVisitor()

        override this.Visit(node: Expression) =
            if not (isNull node) then
                match paramMap.TryGetValue(node.ToString()) with
                | true, replacement -> replacement
                | false, _ -> base.Visit node
            else
                base.Visit node

    let private unwrapQuote (e: Expression) =
        match e with
        | null -> null
        | :? UnaryExpression as u when u.NodeType = ExpressionType.Quote || u.NodeType = ExpressionType.Convert ->
            u.Operand
        | other -> other

    let private extractMemberMap (ne: NewExpression) =
        if isNull ne.Members || ne.Members.Count = 0 then
            ne.Arguments
            |> Seq.mapi (fun i arg -> sprintf "Item%d" (i + 1), arg)
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
            | Some lambda ->
                let dict = Dictionary<string, Expression>()

                List.zip (lambda.Parameters |> Seq.toList) reducedArgs
                |> List.iter (fun (p, arg) -> dict.[p.ToString()] <- arg)

                let substituted = StructuralExpressionReplacer(dict).Visit(lambda.Body)
                betaReduce substituted
            | None ->
                let newArgs = reducedArgs |> List.toArray

                if isNull mc.Object then
                    Expression.Call(mc.Method, newArgs) :> Expression
                else
                    Expression.Call(mc.Object, mc.Method, newArgs) :> Expression

        | :? InvocationExpression as inv ->
            let reducedTarget = betaReduce inv.Expression
            let reducedArgs = inv.Arguments |> Seq.map betaReduce |> Seq.toList

            match reducedTarget with
            | :? LambdaExpression as lambda ->
                let dict = Dictionary<string, Expression>()

                List.zip (lambda.Parameters |> Seq.toList) reducedArgs
                |> List.iter (fun (p, arg) -> dict.[p.ToString()] <- arg)

                let substituted = StructuralExpressionReplacer(dict).Visit(lambda.Body)
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
            match unwrapped with
            | :? ParameterExpression as p when p = param -> true
            | :? MemberInitExpression as mie when isTupleOrAnon mie.Type && mie.Type = param.Type ->
                mie.Bindings.Count > 0
                && mie.Bindings
                   |> Seq.forall (function
                       | :? MemberAssignment as ma ->
                           match unwrapQuote ma.Expression with
                           | :? MemberExpression as me ->
                               match unwrapQuote me.Expression with
                               | :? ParameterExpression as p -> p = param && me.Member.Name = ma.Member.Name
                               | _ -> false
                           | _ -> false
                       | _ -> false)
            | :? NewExpression as ne when isTupleOrAnon ne.Type && ne.Type = param.Type && ne.Arguments.Count > 0 ->
                let memberMap = extractMemberMap ne
                memberMap
                |> Map.forall (fun memberName argExpr ->
                    match unwrapQuote argExpr with
                    | :? MemberExpression as me ->
                        match unwrapQuote me.Expression with
                        | :? ParameterExpression as p -> p = param && me.Member.Name = memberName
                        | _ -> false
                    | _ -> false)
            | _ -> false

    /// 2. PipelineNormalizer:
    /// - Strips redundant identity projections (e.g. .Select(_arg => new AnonymousObject(_arg.Item1, ...)))
    /// - Flattens F# synthetic Grouping wrappers (e.g. .GroupBy(...).Select(g => new Grouping(g.Key, g.Select(...))))
    type private PipelineNormalizer() =
        inherit ExpressionVisitor()

        override this.VisitMethodCall(mc: MethodCallExpression) =
            let visitedMc = base.VisitMethodCall(mc) :?> MethodCallExpression

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
                                && groupMc.Arguments.Count = 2 ->

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

                                    let gm =
                                        typeof<Queryable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                                        |> Array.find (fun m ->
                                            m.Name = "GroupBy"
                                            && m.GetGenericArguments().Length = 3
                                            && m.GetParameters().Length = 3)
                                        |> fun m -> m.MakeGenericMethod(tSource, tKey, tElement)

                                    Expression.Call(null, gm, innerSource, keySelector, elemSelector) :> Expression

                                // Case B: second arg is just gParam (identity grouping wrapper)
                                | :? ParameterExpression as p when p = gParam ->
                                    sourceUnwrapped

                                | _ -> visitedMc :> Expression
                            | _ ->
                                visitedMc :> Expression
                        else
                            visitedMc :> Expression
                    | _ -> visitedMc :> Expression
                | _ -> visitedMc :> Expression
            else
                visitedMc :> Expression

    /// 3. StreamBindingCollector：Use ParamName and TypeName as index
    type private StreamBindingCollector(bindings: Dictionary<string, NewExpression>) =
        inherit ExpressionVisitor()

        let mutable lastStreamNewExpr: NewExpression option = None

        let registerBinding (p: ParameterExpression) (ne: NewExpression) =
            if isTupleOrAnon p.Type then
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

                    match findNestedNewExpr bodyUnwrapped with
                    | Some newNe ->
                        lastStreamNewExpr <- Some newNe

                        for p in l.Parameters do
                            registerBinding p newNe
                    | None -> ()
                | _ -> ()

            mc :> Expression

    /// 4. LetInliner: Flatten tupledArg.Item8.Item1 into (tupledArg.Item1.DeptId * 100)
    type private LetInliner(bindings: Dictionary<string, NewExpression>) =
        inherit ExpressionVisitor()

        let tryGetBinding (p: ParameterExpression) =
            if not (String.IsNullOrEmpty p.Name) && bindings.ContainsKey(p.Name) then
                Some bindings.[p.Name]
            elif not (String.IsNullOrEmpty p.Type.Name) && bindings.ContainsKey(p.Type.Name) then
                Some bindings.[p.Type.Name]
            else
                None

        override this.VisitMember(node: MemberExpression) =
            let visitedExpr = this.Visit node.Expression
            let unwrappedExpr = unwrapQuote visitedExpr

            match unwrappedExpr with
            | :? NewExpression as ne when isTupleOrAnon ne.Type ->
                let memberMap = extractMemberMap ne

                match memberMap.TryFind node.Member.Name with
                | Some childExpr -> this.Visit childExpr
                | None -> Expression.MakeMemberAccess(visitedExpr, node.Member) :> Expression

            | :? ParameterExpression as p when (tryGetBinding p).IsSome ->
                let ne = (tryGetBinding p).Value
                let memberMap = extractMemberMap ne

                match memberMap.TryFind node.Member.Name with
                | Some childExpr ->
                    let unwrappedChild = unwrapQuote childExpr

                    match unwrappedChild with
                    | :? NewExpression as childNe when isTupleOrAnon childNe.Type ->
                        let paramMap = Dictionary<string, Expression>()
                        let parentMap = extractMemberMap ne

                        for KeyValue(itemName, itemArg) in parentMap do
                            if itemName <> node.Member.Name then
                                let pAccess = Expression.PropertyOrField(p, itemName) :> Expression
                                paramMap.[itemArg.ToString()] <- pAccess
                                paramMap.[(unwrapQuote itemArg).ToString()] <- pAccess

                        let remappedChildNe = StructuralExpressionReplacer(paramMap).Visit(childNe)
                        this.Visit remappedChildNe
                    | _ -> Expression.MakeMemberAccess(visitedExpr, node.Member) :> Expression
                | None -> Expression.MakeMemberAccess(visitedExpr, node.Member) :> Expression

            | _ ->
                if visitedExpr = node.Expression then
                    node :> Expression
                else
                    Expression.MakeMemberAccess(visitedExpr, node.Member) :> Expression

    /// Main rewrite entry point for F# LINQ expressions
    let rewrite (expr: Expression) : Expression =
        let reduced = betaReduce expr
        let normalized = PipelineNormalizer().Visit reduced

        let bindings = Dictionary<string, NewExpression>()
        let collector = StreamBindingCollector bindings
        collector.Visit normalized |> ignore

        let inliner = LetInliner bindings
        let inlined = inliner.Visit normalized

        let postReduced = betaReduce inlined
        PipelineNormalizer().Visit postReduced
