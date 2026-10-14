namespace Polars.NET.Linq

open System
open System.Collections.Generic
open System.Linq.Expressions
open System.Reflection
open System.Linq

/// Performs client-side partial evaluation to fold closure references 
/// and local computations into ConstantExpressions before AST translation.
module internal PartialEvaluator =

    /// Evaluates a closed expression subtree to its boxed value.
    /// Uses reflection fast-path for compiler-generated closures to avoid Expression.Compile overhead.
    let private evaluateSubtree (expr: Expression) : obj =
        match expr with
        | :? ConstantExpression as ce -> ce.Value
        | :? MemberExpression as me ->
            match me.Expression with
            | :? ConstantExpression as targetConst when not (isNull targetConst.Value) ->
                match me.Member with
                | :? FieldInfo as fi -> fi.GetValue(targetConst.Value)
                | :? PropertyInfo as pi -> pi.GetValue(targetConst.Value)
                | _ -> Expression.Lambda<Func<obj>>(Expression.Convert(expr, typeof<obj>)).Compile().Invoke()
            | _ -> Expression.Lambda<Func<obj>>(Expression.Convert(expr, typeof<obj>)).Compile().Invoke()
        | _ ->
            let boxed = Expression.Convert(expr, typeof<obj>)
            Expression.Lambda<Func<obj>>(boxed).Compile().Invoke()

    /// Phase 1: Traverses the AST to nominate all subtrees independent of query parameters.
    type private Nominator(canBeEvaluated: Expression -> bool) =
        inherit ExpressionVisitor()
        let candidates = HashSet<Expression>()
        let mutable cannotBeEvaluated = false

        member _.Nominate(expr: Expression) : HashSet<Expression> =
            candidates.Clear()
            cannotBeEvaluated <- false
            ignore (base.Visit expr)
            candidates

        override this.Visit(node: Expression) =
            if isNull node then null
            else
                let saveCannotBeEvaluated = cannotBeEvaluated
                cannotBeEvaluated <- false
                ignore (base.Visit node)
                if not cannotBeEvaluated then
                    if canBeEvaluated node then
                        candidates.Add node |> ignore
                    else
                        cannotBeEvaluated <- true
                cannotBeEvaluated <- cannotBeEvaluated || saveCannotBeEvaluated
                node

        /// Overrides MemberInit visiting: prevents the underlying NewExpression node from being
        /// nominated as an independent candidate when the parent MemberInit depends on query parameters.
        override this.VisitMemberInit(node: MemberInitExpression) =
            // 1. Visit arguments of NewExpression directly so nested closures inside constructor arguments
            // are nominated, without nominating the NewExpression container itself.
            for arg in node.NewExpression.Arguments do
                ignore (this.Visit arg)

            // 2. Visit member bindings
            for binding in node.Bindings do
                match binding with
                | :? MemberAssignment as ma ->
                    ignore (this.Visit ma.Expression)
                | :? MemberMemberBinding as mmb ->
                    for b in mmb.Bindings do
                        match b with
                        | :? MemberAssignment as a -> ignore (this.Visit a.Expression)
                        | _ -> ()
                | :? MemberListBinding as mlb ->
                    for init in mlb.Initializers do
                        for arg in init.Arguments do
                            ignore (this.Visit arg)
                | _ -> ()
            node :> Expression

    /// Phase 2: Rewrites maximal nominated subtrees into constant literals.
    type private SubtreeReplacer(candidates: HashSet<Expression>) =
        inherit ExpressionVisitor()

        override this.Visit(node: Expression) =
            if isNull node then null
            elif candidates.Contains node then
                try
                    let value = evaluateSubtree node
                    Expression.Constant(value, node.Type) :> Expression
                with _ ->
                    // Fall back to original AST if evaluation throws (e.g. null dereference)
                    base.Visit node
            else
                base.Visit node

    /// Evaluates all closure variables and local computations within the expression tree.
    let eval (expr: Expression) : Expression =
        if isNull expr then null
        else
            let canBeEvaluated (node: Expression) =
                node.NodeType <> ExpressionType.Parameter &&
                node.NodeType <> ExpressionType.Lambda &&
                not (typeof<IQueryable>.IsAssignableFrom node.Type)

            let nominator = Nominator canBeEvaluated
            let candidates = nominator.Nominate expr
            let replacer = SubtreeReplacer candidates
            replacer.Visit expr