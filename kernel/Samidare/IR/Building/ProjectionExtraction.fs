namespace Kaminari.Samidare.IR

open System
open System.Reflection
open System.Linq
open System.Linq.Expressions
open Kaminari.Samidare
open Kaminari.Sazanami.Helpers

/// Extraction of key and projection expressions from selector lambdas.
module internal ProjectionExtraction =

    /// Extracts a sequence of key ScalarExprs from a key selector lambda (supporting scalar columns or anonymous key objects).
    let tryExtractKeyScalars (ctx: TranslationContext) (keyLambda: LambdaExpression) : ScalarExpr list option =
        match tryExtractMemberBindings keyLambda.Body with
        | Some bindings ->
            let exprs =
                bindings
                |> List.choose (fun (_, e) -> ExprTranslator.tryTranslateToScalar ctx e)
            if exprs.Length = bindings.Length then
                Some exprs
            else
                None
        | None ->
            match ExprTranslator.tryTranslateToScalar ctx keyLambda.Body with
            | Some key -> Some [ key ]
            // A whole anonymous object as the key (`sq => sq`): one key per member, in declaration order, as the
            // anonymous object expression on the other side lists them.
            | None when EntityPacking.isAnonymousType keyLambda.Body.Type ->
                let members =
                    keyLambda.Body.Type.GetProperties()
                    |> Array.toList
                    |> List.map (fun p -> ExprTranslator.tryTranslateToScalar ctx (Expression.Property(keyLambda.Body, p)))
                if members |> List.forall Option.isSome then Some(members |> List.map Option.get) else None
            | None -> None

    /// Extracts projection bindings (name * ScalarExpr) from a projection LambdaExpression.
    /// Whole entities among the members (a join scope, or the row parameter itself) become struct columns.
    let extractProjections (ctx: TranslationContext) (plan: LogicalPlan) (body: Expression) : (string * ScalarExpr) list option =
        match tryExtractMemberBindings (ExpressionPatterns.unwrap body) with
        | Some bindings ->
            let splitMaxMap =
                bindings
                |> List.choose (fun (_, argExpr) ->
                    match argExpr with
                    | SplitIndexAccess(target, sep, idx) -> Some((target.ToString(), sep), idx)
                    | _ -> None)
                |> List.groupBy fst
                |> List.map (fun (k, g) -> k, g |> List.map snd |> List.max)
                |> Map.ofList

            let scope = CompilationScope.Current
            scope.SplitIndexHints <- if splitMaxMap.IsEmpty then None else Some splitMaxMap
            try
                let exprOpts =
                    bindings
                    |> List.map (fun (name, expr) ->
                        name, expr, EntityPacking.tryTranslateMember ctx plan expr)
                let failed = exprOpts |> List.filter (fun (_, _, opt) -> opt.IsNone)
                if failed.IsEmpty then
                    Some(exprOpts |> List.map (fun (name, _, opt) -> name, opt.Value))
                else
                    let reasons = failed |> List.map (fun (n, e, _) -> sprintf "%s = %s (%s)" n (e.ToString()) (e.GetType().Name))
                    scope.FallbackReason <- Some(sprintf "Failed projection bindings: %s" (String.concat "; " reasons))
                    None
            finally
                scope.SplitIndexHints <- None
        | None ->
            let unwrappedBody = ExpressionPatterns.unwrap body
            match unwrappedBody with
            | MemberAccess _ as ma when not (ClrTypeHelper.IsScalarType ma.Type) && (ColumnResolver.tryResolveColumnNameWithContext ctx ma).IsNone ->
                let entityProps = ma.Type.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                let matchedProjs =
                    entityProps
                    |> Array.choose (fun prop ->
                        let propAccess = Expression.Property(ma, prop) :> Expression
                        match ColumnResolver.tryResolveColumnNameWithContext ctx propAccess with
                        | Some physCol -> Some(prop.Name, ScalarExpr.Column physCol)
                        | None -> None)
                    |> Array.toList
                if not matchedProjs.IsEmpty then
                    Some matchedProjs
                else
                    None
            | _ ->
                // The body itself, not its unwrapped form: a conversion such as (int)e is part of the value.
                ExprTranslator.tryTranslateToScalar ctx body
                |> Option.map (fun s ->
                    let colName =
                        match s with
                        | ScalarExpr.Column col -> col
                        | _ ->
                            match ctx with
                            | TranslationContext.Scalar(_, physCol) -> physCol
                            | _ -> "value"
                    [ (colName, s) ])

    let tryExtractJoinProjections
        (ctx: TranslationContext)
        (plan: LogicalPlan)
        (pLeft: ParameterExpression)
        (pRight: ParameterExpression)
        (leftCols: string list)
        (rightCols: string list)
        (uniqueSuffix: string)
        (invertSides: bool)
        (body: Expression)
        : (string * ScalarExpr) list option =
        let firstParam, secondParam, firstCols, secondCols, secondSuffix =
            if invertSides then
                pRight, pLeft, rightCols, leftCols, LinqConstants.DefaultLeftSuffix
            else
                pLeft, pRight, leftCols, rightCols, uniqueSuffix

        let firstColSet = Set.ofList firstCols
        let unwrapped = ExpressionPatterns.unwrap body
        match unwrapped with
        | :? ParameterExpression as p when p.Name = firstParam.Name ->
            Some(firstCols |> List.map (fun c -> c, ScalarExpr.Column c))
        | :? ParameterExpression as p when p.Name = secondParam.Name ->
            Some(secondCols |> List.map (fun c ->
                let physCol = if firstColSet.Contains c then c + secondSuffix else c
                c, ScalarExpr.Column physCol))
        | _ ->
            extractProjections ctx plan unwrapped
