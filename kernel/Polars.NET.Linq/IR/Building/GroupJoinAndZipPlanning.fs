namespace Polars.NET.Linq.IR

open System
open System.Linq.Expressions
open Polars.NET.Linq

/// Plans combining a second sequence row by row: a GroupJoin with a result selector, and Zip.
module internal GroupJoinAndZipPlanning =

    /// Disambiguates duplicate column names in a LogicalPlan against an existing set of columns by appending a suffix.
    let private disambiguatePlanColumns
        (existingCols: Set<string>)
        (plan: LogicalPlan)
        (suffix: string)
        : LogicalPlan * Set<string> * Map<string, string> =
        let cols = (SchemaInference.inferPlanSchema plan).ColumnNames
        let duplicates = cols |> List.filter existingCols.Contains
        if not duplicates.IsEmpty then
            let newNames = duplicates |> List.map (fun c -> c + suffix)
            let suffixMap = List.zip duplicates newNames |> Map.ofList
            let renamedPlan = LogicalPlan.Rename(plan, duplicates, newNames)
            renamedPlan, existingCols |> Set.union (Set.ofList cols), suffixMap
        else
            plan, existingCols |> Set.union (Set.ofList cols), Map.empty

    /// Replaces parameters by expressions (by reference).
    let private substitute (map: (ParameterExpression * Expression) list) (body: Expression) : Expression =
        let visitor =
            { new ExpressionVisitor() with
                override _.VisitParameter(node: ParameterExpression) =
                    match map |> List.tryFind (fun (p, _) -> obj.ReferenceEquals(p, node)) with
                    | Some(_, replacement) -> replacement
                    | None -> node :> Expression
            }
        visitor.Visit body

    /// The components of a key: the members of an anonymous / tuple key built in place, else the key itself.
    let private keyComponents (key: Expression) : Expression list =
        match key with
        | :? NewExpression
        | :? MemberInitExpression ->
            match tryExtractMemberBindings key with
            | Some bindings -> bindings |> List.map snd
            | None -> [ key ]
        | _ -> [ key ]

    let private canBeNull (t: Type) = not t.IsValueType || not (isNull (Nullable.GetUnderlyingType t))

    /// GroupJoin(inner, outerKey, innerKey, (a, g) => body) as the projection
    /// `a => body[g := inner.Where(b => innerKey(b) == outerKey(a))]`, whose group is a correlated subquery that
    /// decorrelation plans (an aggregate, Any, a filtered Count, ...). As in Enumerable.GroupJoin, a null key
    /// matches nothing: each key component of the inner rows is required non-null. Also returns the group
    /// expression, which the body may hold as a value.
    let groupJoinAsProjection
        (innerExpr: Expression)
        (outerKey: LambdaExpression)
        (innerKey: LambdaExpression)
        (resSel: LambdaExpression)
        : LambdaExpression * Expression =
        let outerParam = resSel.Parameters.[0]
        let groupParam = resSel.Parameters.[1]
        let innerType = innerKey.Parameters.[0].Type
        let b = Expression.Parameter(innerType, "__gj_inner")
        let outerKeyBody = substitute [ outerKey.Parameters.[0], outerParam :> Expression ] outerKey.Body
        let innerKeyBody = substitute [ innerKey.Parameters.[0], b :> Expression ] innerKey.Body
        let outerParts = keyComponents outerKeyBody
        let innerParts = keyComponents innerKeyBody
        let pairs =
            if outerParts.Length = innerParts.Length then List.zip outerParts innerParts
            else [ outerKeyBody, innerKeyBody ]
        // The guard is on the inner side: with no null inner key, a null outer key matches nothing either, and an
        // inner-only filter keeps the subquery a plain equi-join.
        let guards =
            pairs
            |> List.choose (fun (_, i) ->
                if canBeNull i.Type then Some(Expression.NotEqual(i, Expression.Constant(null, i.Type)) :> Expression)
                else None)
        let equalities = pairs |> List.map (fun (o, i) -> Expression.Equal(i, o) :> Expression)
        let predicate = (guards @ equalities) |> List.reduce (fun l r -> Expression.AndAlso(l, r) :> Expression)
        let matchLambda = Expression.Lambda(typedefof<Func<_, _>>.MakeGenericType(innerType, typeof<bool>), predicate, [| b |])
        let group =
            if typedefof<System.Linq.IQueryable<_>>.MakeGenericType(innerType).IsAssignableFrom innerExpr.Type then
                Expression.Call(typeof<System.Linq.Queryable>, "Where", [| innerType |], innerExpr, Expression.Quote matchLambda) :> Expression
            else
                Expression.Call(typeof<System.Linq.Enumerable>, "Where", [| innerType |], innerExpr, matchLambda) :> Expression
        let body = substitute [ groupParam, group ] resSel.Body
        Expression.Lambda(typedefof<Func<_, _>>.MakeGenericType(outerParam.Type, resSel.ReturnType), body, [| outerParam |]), group

    /// The lambda `l` (over the rows `projection` produces) composed with the pure projection: each read of a
    /// projected member becomes the member's expression. None when `l` reads its row other than by member.
    let tryComposeWithProjection (projection: LambdaExpression) (l: LambdaExpression) : LambdaExpression option =
        match tryExtractMemberBindings projection.Body with
        | Some bindings when (projection.Body :? NewExpression || projection.Body :? MemberInitExpression) ->
            let row = l.Parameters.[0]
            let mutable readsWholeRow = false
            let visitor =
                { new ExpressionVisitor() with
                    override this.VisitMember(node: MemberExpression) =
                        match node.Expression with
                        | :? ParameterExpression as p when obj.ReferenceEquals(p, row) ->
                            match bindings |> List.tryFind (fun (name, _) -> name = node.Member.Name) with
                            | Some(_, e) -> e
                            | None ->
                                readsWholeRow <- true
                                node :> Expression
                        | _ -> base.VisitMember node
                    override _.VisitParameter(node: ParameterExpression) =
                        if obj.ReferenceEquals(node, row) then readsWholeRow <- true
                        node :> Expression
                }
            let body = visitor.Visit l.Body
            if readsWholeRow then
                None
            else
                let parameters = Seq.append [ projection.Parameters.[0] ] (Seq.skip 1 l.Parameters) |> Seq.toArray
                Some(Expression.Lambda(body, parameters))
        | _ -> None

    /// Pure symbolic 2-way Zip builder into HorizontalConcat and optional column renames.
    let buildZipPlan
        (resolveSequence: Expression -> LogicalPlan option)
        (basePlan: LogicalPlan)
        (secondExpr: Expression)
        (resLambdaOpt: LambdaExpression option)
        : LogicalPlan option =
        resolveSequence secondExpr
        |> Option.map (fun otherPlan ->
            let baseCols = (SchemaInference.inferPlanSchema basePlan).ColumnNames |> Set.ofList
            let safeSecondPlan, _, suffixMap = disambiguatePlanColumns baseCols otherPlan "_second"
            let zipPlan = LogicalPlan.HorizontalConcat [ basePlan; safeSecondPlan ]
            match resLambdaOpt with
            | Some resLambda ->
                let renames =
                    extractBinaryResultRenames resLambda None
                    |> List.map (fun (srcCol, destCol) ->
                        suffixMap.TryFind srcCol |> Option.defaultValue srcCol, destCol)
                if renames.IsEmpty then zipPlan
                else LogicalPlan.Rename(zipPlan, renames |> List.map fst, renames |> List.map snd)
            | None -> zipPlan)

    /// Pure symbolic 3-way Zip builder into HorizontalConcat.
    let buildZip3Plan
        (resolveSequence: Expression -> LogicalPlan option)
        (basePlan: LogicalPlan)
        (secondExpr: Expression)
        (thirdExpr: Expression)
        : LogicalPlan option =
        match
            resolveSequence secondExpr,
            resolveSequence thirdExpr
        with
        | Some secondPlan, Some thirdPlan ->
            let baseCols = (SchemaInference.inferPlanSchema basePlan).ColumnNames |> Set.ofList
            let safeSecondPlan, colsAfter2, _ = disambiguatePlanColumns baseCols secondPlan "_second"
            let safeThirdPlan, _, _ = disambiguatePlanColumns colsAfter2 thirdPlan "_third"
            Some(LogicalPlan.HorizontalConcat [ basePlan; safeSecondPlan; safeThirdPlan ])
        | _ -> None
