namespace Polars.NET.Linq.Provider

open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Reflection

module FSharpAst =

    /// 基于 AST 表达式结构 String 签名的替换器
    type private StructuralExpressionReplacer(paramMap: Dictionary<string, Expression>) =
        inherit ExpressionVisitor()
        override this.Visit(node: Expression) =
            if not (isNull node) then
                match paramMap.TryGetValue(node.ToString()) with
                | true, replacement -> replacement
                | false, _ -> base.Visit(node)
            else
                base.Visit(node)

    let private unwrapQuote (e: Expression) =
        match e with
        | null -> null
        | :? UnaryExpression as u when u.NodeType = ExpressionType.Quote || u.NodeType = ExpressionType.Convert -> u.Operand
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
        not (isNull t) &&
        (t.Name.StartsWith("AnonymousObject") ||
         t.Name.StartsWith("Tuple") ||
         t.Name.Contains("TransparentIdentifier"))

    /// 1. 标准 Beta-reduction: 规约 F# 闭包 .Invoke(...)
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
                if isNull mc.Object then Expression.Call(mc.Method, newArgs) :> Expression
                else Expression.Call(mc.Object, mc.Method, newArgs) :> Expression

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
            | _ ->
                Expression.Invoke(reducedTarget, reducedArgs |> List.toArray) :> Expression

        | :? MethodCallExpression as mc ->
            let newObj = if isNull mc.Object then null else betaReduce mc.Object
            let newArgs = mc.Arguments |> Seq.map betaReduce |> Seq.toArray
            if isNull newObj then Expression.Call(mc.Method, newArgs) :> Expression
            else Expression.Call(newObj, mc.Method, newArgs) :> Expression

        | :? LambdaExpression as l ->
            let newBody = betaReduce l.Body
            Expression.Lambda(l.Type, newBody, l.Parameters) :> Expression

        | :? UnaryExpression as u ->
            let newOp = betaReduce u.Operand
            if newOp = u.Operand then u :> Expression
            else Expression.MakeUnary(u.NodeType, newOp, u.Type, u.Method) :> Expression

        | :? BinaryExpression as b ->
            let newLeft = betaReduce b.Left
            let newRight = betaReduce b.Right
            if newLeft = b.Left && newRight = b.Right then b :> Expression
            else Expression.MakeBinary(b.NodeType, newLeft, newRight, b.IsLiftedToNull, b.Method) :> Expression

        | other -> other

    /// 2. 流式绑定收集器：使用 ParamName 和 TypeName 双重索引，彻底解决 Null FullName 问题
    type private StreamBindingCollector(bindings: Dictionary<string, NewExpression>) =
        inherit ExpressionVisitor()

        let mutable lastStreamNewExpr : NewExpression option = None

        let registerBinding (p: ParameterExpression) (ne: NewExpression) =
            if isTupleOrAnon p.Type then
                if not (String.IsNullOrEmpty p.Name) then bindings.[p.Name] <- ne
                if not (String.IsNullOrEmpty p.Type.Name) then bindings.[p.Type.Name] <- ne

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
                            |> Seq.tryPick (function :? LambdaExpression as subL -> findNestedNewExpr subL.Body | subE -> findNestedNewExpr subE)
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

    /// 3. LetInliner：将 tupledArg.Item8.Item1 展平为 (tupledArg.Item1.DeptId * 100)，作用域 100% 封闭
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
            let visitedExpr = this.Visit(node.Expression)
            let unwrappedExpr = unwrapQuote visitedExpr

            match unwrappedExpr with
            | :? NewExpression as ne when isTupleOrAnon ne.Type ->
                let memberMap = extractMemberMap ne
                match memberMap.TryFind node.Member.Name with
                | Some childExpr -> this.Visit(childExpr)
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
                        this.Visit(remappedChildNe)
                    | _ ->
                        Expression.MakeMemberAccess(visitedExpr, node.Member) :> Expression
                | None -> Expression.MakeMemberAccess(visitedExpr, node.Member) :> Expression

            | _ ->
                if visitedExpr = node.Expression then node :> Expression
                else Expression.MakeMemberAccess(visitedExpr, node.Member) :> Expression

    /// Main rewrite entry point for F# LINQ expressions
    let rewrite (expr: Expression) : Expression =
        let reduced = betaReduce expr

        let bindings = Dictionary<string, NewExpression>()
        let collector = StreamBindingCollector(bindings)
        collector.Visit(reduced) |> ignore

        let inliner = LetInliner(bindings)
        let inlined = inliner.Visit(reduced)

        betaReduce inlined