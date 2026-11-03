namespace Polars.NET.Linq

open System
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core
open Polars.NET.Core.Helpers

module internal LinqPipelineCompiler =

    let addSort (spec: SortSpec) (ops: QueryOp list) =
        match ops with
        | QueryOp.Sort specs :: tail -> QueryOp.Sort(specs @ [ spec ]) :: tail
        | _ -> QueryOp.Sort [ spec ] :: ops

    let updateJoinScopeMap
        (existingScopeMap: Map<string * string, string>)
        (leftScope: string)
        (leftCols: string array)
        (rightScope: string)
        (rightCols: string array)
        (suffix: string)
        (invertSides: bool)
        =
        if invertSides then
            let rightColSet = Set.ofArray rightCols
            let baseMap =
                if existingScopeMap.IsEmpty then
                    rightCols |> Array.fold (fun m col -> Map.add (rightScope, col) col m) existingScopeMap
                else
                    existingScopeMap
            leftCols
            |> Array.fold (fun m col ->
                let physicalCol = if rightColSet.Contains col then col + suffix else col
                Map.add (leftScope, col) physicalCol m) baseMap
        else
            let leftColSet = Set.ofArray leftCols
            let baseMap =
                if existingScopeMap.IsEmpty then
                    leftCols |> Array.fold (fun m col -> Map.add (leftScope, col) col m) existingScopeMap
                else
                    existingScopeMap
            rightCols
            |> Array.fold (fun m col ->
                let physicalCol = if leftColSet.Contains col then col + suffix else col
                Map.add (rightScope, col) physicalCol m) baseMap

    let compileStages
        (currentLf: LazyFrameHandle)
        (stages: LinqStage list)
        (targetType: Type)
        (isScalar: bool)
        (isGrouping: bool)
        (bound: Expression)
        : LazyFrameHandle * QueryOp list * (Func<obj, bool> * LambdaExpression) list * (Type * Func<obj, obj>) option * bool =

        let rec fuse
            (currentLf: LazyFrameHandle)
            (remaining: LinqStage list)
            (opsAcc: QueryOp list)
            (clientPreds: (Func<obj, bool> * LambdaExpression) list)
            (clientProjOpt: (Type * Func<obj, obj>) option)
            (scopeMap: Map<string * string, string>)
            : LazyFrameHandle * QueryOp list * (Func<obj, bool> * LambdaExpression) list * (Type * Func<obj, obj>) option * bool =
            let canPushdown = clientPreds.IsEmpty && clientProjOpt.IsNone
            let inline pushOp op tail = fuse currentLf tail (op :: opsAcc) clientPreds clientProjOpt scopeMap
            let inline pushOps ops tail = fuse currentLf tail (ops @ opsAcc) clientPreds clientProjOpt scopeMap

            let pushOpOrFallback (opOpt: QueryOp option) tail =
                match opOpt with
                | Some op -> pushOp op tail
                | None -> currentLf, [], [], None, true

            let pushOpsOrFallback (opsOpt: QueryOp list option) tail =
                match opsOpt with
                | Some ops -> pushOps ops tail
                | None -> currentLf, [], [], None, true

            let tryDispatchGroupBy
                (ctx: GroupFoldContext)
                (projBody: Expression)
                (groupParamName: string)
                (isKeyArg: Expression -> bool)
                (nextTail: LinqStage list)
                =
                if not clientPreds.IsEmpty then
                    currentLf, [], [], None, true
                else
                    let activeLf = if opsAcc.IsEmpty then currentLf else applyNativeOps currentLf (List.rev opsAcc)
                    match compileUnifiedGroupBy activeLf ctx projBody groupParamName isKeyArg scopeMap with
                    | Some compiledOps ->
                        if nextTail.IsEmpty then
                            fuse activeLf nextTail (compiledOps @ []) clientPreds clientProjOpt Map.empty
                        else
                            let groupedLf = applyNativeOps activeLf (List.rev compiledOps)
                            fuse groupedLf nextTail [] clientPreds clientProjOpt Map.empty
                    | None when isGrouping -> fuse currentLf nextTail opsAcc clientPreds clientProjOpt scopeMap
                    | None -> currentLf, [], [], None, true

            let slurpPipelineGroupBy (stages: LinqStage list) (initCtx: GroupFoldContext) =
                let rec slurp (s: LinqStage list) (ctx: GroupFoldContext) (letBindings: Map<string, Expression>) =
                    let substituteLet (expr: Expression) =
                        if letBindings.IsEmpty then expr
                        else
                            { new ExpressionVisitor() with
                                override self.VisitMember(node: MemberExpression) =
                                    let visitedInner = self.Visit(node.Expression)
                                    let rec isTransparentTarget (e: Expression) =
                                        match ExprTranslator.unwrap e with
                                        | :? ParameterExpression as p ->
                                            isTupleOrAnonType p.Type || (not (isNull p.Name) && p.Name.Contains "TransparentIdentifier")
                                        | MemberAccess(target, m) when isNavigationMember m || isTupleOrAnonType m.DeclaringType ->
                                            true
                                        | MemberAccess(target, _) ->
                                            isTransparentTarget target
                                        | _ -> false
                                    if isTransparentTarget visitedInner then
                                        match letBindings.TryFind node.Member.Name with
                                        | Some boundExpr -> boundExpr
                                        | None -> node.Update(visitedInner) :> Expression
                                    else
                                        node.Update(visitedInner) :> Expression
                            }.Visit(expr)

                    match s with
                    | LinqStage.Filter pred :: next ->
                        let newBody = substituteLet pred.Body
                        let newPred = Expression.Lambda(newBody, pred.Parameters)
                        slurp next { ctx with HavingPreds = ctx.HavingPreds @ [ newPred ] } letBindings
                    | LinqStage.Sort(sortLambda, isDesc, nullsLast) :: next ->
                        let newBody = substituteLet sortLambda.Body
                        let newSort = Expression.Lambda(newBody, sortLambda.Parameters)
                        slurp next { ctx with SortStages = ctx.SortStages @ [ (newSort, isDesc, nullsLast) ] } letBindings
                    | LinqStage.Project projLambda :: next ->
                        let param = projLambda.Parameters.[0]
                        let isTransparentGroupLet =
                            if not (isNull projLambda.Body.Type) && not (isNull projLambda.Body.Type.FullName) && projLambda.Body.Type.FullName.StartsWith "Microsoft.FSharp." then
                                None
                            else
                                match tryExtractMemberBindings projLambda.Body with
                                | Some bindings when not next.IsEmpty ->
                                    let hasGroupingNav =
                                        bindings
                                        |> List.exists (fun (propName, expr) ->
                                            if propName.Contains "TransparentIdentifier" then
                                                true
                                            else
                                                match ExprTranslator.unwrap expr with
                                                | :? ParameterExpression as p when
                                                    (p.Type.IsGenericType && p.Type.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>)
                                                    || isTupleOrAnonType p.Type
                                                    || (not (isNull p.Name) && p.Name.Contains "TransparentIdentifier") ->
                                                    true
                                                | MemberAccess(_, m) when isNavigationMember m ->
                                                    true
                                                | _ -> false)
                                    if hasGroupingNav then
                                        Some bindings
                                    else
                                        None
                                | _ -> None

                        match isTransparentGroupLet with
                        | Some bindings ->
                            let gParamOpt =
                                bindings
                                |> List.tryPick (fun (name, expr) ->
                                    match ExprTranslator.unwrap expr with
                                    | :? ParameterExpression as p when p.Name = param.Name && p.Type.IsGenericType && p.Type.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>> ->
                                        Some(name, p :> Expression)
                                    | _ -> None)
                            let updatedBindings =
                                bindings
                                |> List.fold (fun m (name, expr) ->
                                    let subExpr = substituteLet expr
                                    let normalizedExpr =
                                        match gParamOpt with
                                        | Some(gPropName, gParamExpr) ->
                                            { new ExpressionVisitor() with
                                                override _.VisitParameter(p: ParameterExpression) =
                                                    if p.Name = param.Name then gParamExpr
                                                    else base.VisitParameter p
                                            }.Visit(subExpr)
                                        | None -> subExpr
                                    match ExprTranslator.unwrap normalizedExpr with
                                    | :? ParameterExpression as p when p.Name = param.Name && isTupleOrAnonType p.Type ->
                                        m
                                    | _ ->
                                        Map.add name normalizedExpr m) letBindings
                            slurp next ctx updatedBindings
                        | None ->
                            let finalProjBody = substituteLet projLambda.Body
                            let isKeyArg (e: Expression) =
                                match ExprTranslator.unwrap e with
                                | MemberAccess(p, m) when
                                    not (isNull p) && (p.NodeType = ExpressionType.Parameter || isNavigationMember (match p with :? MemberExpression as me -> me.Member | _ -> null)) && m.Name = "Key"
                                    ->
                                    true
                                | :? ParameterExpression as p when p.Name = projLambda.Parameters.[0].Name ->
                                    true
                                | _ -> false

                            let gParamName =
                                match letBindings |> Map.tryPick (fun _ v -> match ExprTranslator.unwrap v with :? ParameterExpression as p when p.Type.IsGenericType && p.Type.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>> -> Some p.Name | _ -> None) with
                                | Some name -> name
                                | None -> projLambda.Parameters.[0].Name

                            tryDispatchGroupBy ctx finalProjBody gParamName isKeyArg next
                    | [] when isGrouping ->
                        let clientHavingPreds =
                            ctx.HavingPreds |> List.map (fun p -> compileClientPredicate p, p)

                        fuse currentLf [] opsAcc (clientHavingPreds @ clientPreds) clientProjOpt scopeMap
                    | _ -> currentLf, [], [], None, true

                slurp stages initCtx Map.empty

            let dispatchCSharpGroupBy
                (keyLambda: LambdaExpression)
                (elemLambdaOpt: LambdaExpression option)
                (resLambda: LambdaExpression)
                (tail: LinqStage list)
                =
                let havingPreds, nextTail =
                    match tail with
                    | LinqStage.Filter havingPred :: rest -> [ havingPred ], rest
                    | _ -> [], tail

                let ctx =
                    {
                        KeyLambda = keyLambda
                        ElemLambdaOpt = elemLambdaOpt
                        HavingPreds = havingPreds
                        SortStages = []
                    }

                let resKeyParamName = resLambda.Parameters.[0].Name

                let isKeyArg (e: Expression) =
                    match e with
                    | :? ParameterExpression as p when p.Name = resKeyParamName -> true
                    | MemberAccess(:? ParameterExpression as p, _) when p.Name = resKeyParamName -> true
                    | _ -> false

                tryDispatchGroupBy ctx resLambda.Body resLambda.Parameters.[1].Name isKeyArg nextTail

            match remaining with
            | [] -> currentLf, List.rev opsAcc, List.rev clientPreds, clientProjOpt, false

            // -----------------------------------------------------------------
            // 1. GroupBy Stages (F# Pipeline & C# 3/4-arg Overloads)
            // -----------------------------------------------------------------
            | LinqStage.GroupByKey keyLambda :: tail ->
                slurpPipelineGroupBy
                    tail
                    {
                        KeyLambda = keyLambda
                        ElemLambdaOpt = None
                        HavingPreds = []
                        SortStages = []
                    }

            | LinqStage.GroupByWithResult(keyLambda, secondLambda) :: tail ->
                if secondLambda.Parameters.Count >= 2 then
                    dispatchCSharpGroupBy keyLambda None secondLambda tail
                else
                    slurpPipelineGroupBy
                        tail
                        {
                            KeyLambda = keyLambda
                            ElemLambdaOpt = Some secondLambda
                            HavingPreds = []
                            SortStages = []
                        }

            | LinqStage.GroupByWithElementAndResult(keyLambda, elemLambda, resLambda) :: tail ->
                dispatchCSharpGroupBy keyLambda (Some elemLambda) resLambda tail

            | LinqStage.GroupByRollup(keyLambda, resLambda) :: tail ->
                if not clientPreds.IsEmpty then
                    currentLf, [], [], None, true
                else
                    let activeLf = if opsAcc.IsEmpty then currentLf else applyNativeOps currentLf (List.rev opsAcc)
                    match compileUnifiedGroupingSets activeLf GroupingKind.Rollup keyLambda resLambda scopeMap with
                    | Some groupedLf ->
                        fuse groupedLf tail [] clientPreds clientProjOpt Map.empty
                    | None -> currentLf, [], [], None, true

            | LinqStage.GroupByCube(keyLambda, resLambda) :: tail ->
                if not clientPreds.IsEmpty then
                    currentLf, [], [], None, true
                else
                    let activeLf = if opsAcc.IsEmpty then currentLf else applyNativeOps currentLf (List.rev opsAcc)
                    match compileUnifiedGroupingSets activeLf GroupingKind.Cube keyLambda resLambda scopeMap with
                    | Some groupedLf ->
                        fuse groupedLf tail [] clientPreds clientProjOpt Map.empty
                    | None -> currentLf, [], [], None, true

            | LinqStage.GroupByGroupingSets(keyLambda, setsExpr, resLambda) :: tail ->
                if not clientPreds.IsEmpty then
                    currentLf, [], [], None, true
                else
                    let activeLf = if opsAcc.IsEmpty then currentLf else applyNativeOps currentLf (List.rev opsAcc)
                    match compileUnifiedGroupingSets activeLf (GroupingKind.GroupingSets setsExpr) keyLambda resLambda scopeMap with
                    | Some groupedLf ->
                        fuse groupedLf tail [] clientPreds clientProjOpt Map.empty
                    | None -> currentLf, [], [], None, true

            // -----------------------------------------------------------------
            // 2. Filtering & Window Masking (Filter, TakeWhile, SkipWhile)
            // -----------------------------------------------------------------
            | LinqStage.Filter l :: tail ->
                match if canPushdown then SubqueryDecorrelator.tryDecorrelateFilter currentLf l scopeMap else None with
                | Some decorResult ->
                    let newOpsAcc = (decorResult.GeneratedOps |> List.rev) @ opsAcc
                    let nextStages =
                        match decorResult.ResidualLambda with
                        | Some residualLam -> LinqStage.Filter residualLam :: tail
                        | None -> tail
                    fuse currentLf nextStages newOpsAcc clientPreds clientProjOpt scopeMap
                | None ->
                    let p = l.Parameters.[0]
                    let ctx = createContextWithScope scopeMap p.Type currentLf p.Name

                    let nativeExprOpt =
                        if canPushdown then
                            ExprTranslator.tryTranslateWithContext ctx l.Body
                            |> Option.orElseWith (fun () -> tryTranslateTupleFilter ctx l.Body)
                        else
                            None

                    match nativeExprOpt with
                    | Some exprHandle -> pushOp (QueryOp.Filter exprHandle) tail
                    | None ->
                        if isTupleOrAnonType p.Type || clientProjOpt.IsSome then
                            FallbackTracker.Diagnostic <-
                                Some(
                                    sprintf
                                        "Filter on transparent identifier %s: %s\nRewritten body: %s"
                                        p.Type.Name
                                        (l.ToString())
                                        (bound.ToString())
                                )
                            currentLf, [], [], None, true
                        else
                            fuse currentLf tail opsAcc ((compileClientPredicate l, l) :: clientPreds) clientProjOpt scopeMap

            | LinqStage.TakeWhile l | LinqStage.SkipWhile l as stage :: tail ->
                let isSkip =
                    match stage with
                    | LinqStage.SkipWhile _ -> true
                    | _ -> false

                let ctx = createContextWithScope scopeMap targetType currentLf l.Parameters.[0].Name

                match if canPushdown then ExprTranslator.tryTranslateWithContext ctx l.Body else None with
                | Some boolExpr ->
                    let maskExpr =
                        if isSkip then
                            PolarsWrapper.CumMax(PolarsWrapper.Not boolExpr, false)
                        else
                            PolarsWrapper.CumMin(boolExpr, false)

                    pushOp (QueryOp.Filter maskExpr) tail
                | None ->
                    fuse currentLf tail opsAcc ((compileClientPredicate l, l) :: clientPreds) clientProjOpt scopeMap

            // -----------------------------------------------------------------
            // 3. Projection (Select, Transparent Let, Subquery Decorrelation)
            // -----------------------------------------------------------------
            | LinqStage.Project projLambda :: tail ->
                match if canPushdown then SubqueryDecorrelator.tryDecorrelateProjection currentLf projLambda scopeMap else None with
                | Some(subqueryOps, rewrittenProjLambda) ->
                    fuse currentLf (LinqStage.Project rewrittenProjLambda :: tail) ((subqueryOps |> List.rev) @ opsAcc) clientPreds None scopeMap
                | None ->
                    let nativeProjOpOpt =
                        if canPushdown then
                            tryCompileTransparentLet currentLf projLambda scopeMap
                            |> Option.orElseWith (fun () ->
                                compileSelectProjection currentLf projLambda scopeMap |> Option.map QueryOp.Select)
                        else
                            None

                    match nativeProjOpOpt with
                    | Some(QueryOp.WithColumns _ as op) ->
                        if tail.IsEmpty then
                            fuse currentLf tail (op :: opsAcc) clientPreds None scopeMap
                        else
                            let activeLf = applyNativeOps currentLf (List.rev (op :: opsAcc))
                            fuse activeLf tail [] clientPreds None scopeMap
                    | Some op ->
                        if tail.IsEmpty then
                            fuse currentLf tail (op :: opsAcc) clientPreds None Map.empty
                        else
                            let activeLf = applyNativeOps currentLf (List.rev (op :: opsAcc))
                            fuse activeLf tail [] clientPreds None Map.empty
                    | None ->
                        let sourceType = projLambda.Parameters.[0].Type
                        let currentFunc = compileToBoxedFunc projLambda

                        let nextProj =
                            match clientProjOpt with
                            | None -> Some(sourceType, currentFunc)
                            | Some(prevSrcType, prevFunc) ->
                                Some(prevSrcType, Func<obj, obj>(fun rawItem -> currentFunc.Invoke(prevFunc.Invoke rawItem)))

                        fuse currentLf tail (QueryOp.SelectPassthrough :: opsAcc) clientPreds nextProj scopeMap

            // -----------------------------------------------------------------
            // 4. Sorting (Sort, SortDefault)
            // -----------------------------------------------------------------
            | LinqStage.Sort(l, isDesc, nullsLast) :: tail ->
                let param = l.Parameters.[0]

                let isParamGrouping =
                    param.Type.IsGenericType
                    && param.Type.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

                if not isParamGrouping && canPushdown then
                    let ctx = createContextWithScope scopeMap param.Type currentLf param.Name

                    let sortExprOpt =
                        ExprTranslator.tryTranslateWithContext ctx l.Body
                        |> Option.orElseWith (fun () -> tryTranslateSelectArg ctx l.Body)

                    match sortExprOpt with
                    | Some exprHandle ->
                        let spec = { SortSpec.Expr = exprHandle; Descending = isDesc; NullsLast = nullsLast }
                        fuse currentLf tail (addSort spec opsAcc) clientPreds clientProjOpt scopeMap
                    | None -> fuse currentLf tail opsAcc clientPreds clientProjOpt scopeMap
                else
                    fuse currentLf tail opsAcc clientPreds clientProjOpt scopeMap

            // -----------------------------------------------------------------
            // 5. Pure Pushdown Stages (requires canPushdown = true)
            // -----------------------------------------------------------------
            | _ when canPushdown ->
                match remaining with
                | LinqStage.Index :: tail ->
                    let indexOp = QueryOp.WithRowIndex("Index", Some 0u)
                    let int32Dtype =
                        PolarsWrapper.DataTypeExprFromDataType(PolarsWrapper.NewPrimitiveType(int PlDataType.Int32))
                    let castIndexExpr =
                        PolarsWrapper.ExprCast(PolarsWrapper.Col "Index", int32Dtype, strict = false, wrapNumerical = false)
                    pushOps [ QueryOp.WithColumns [| castIndexExpr |]; indexOp ] tail

                | LinqStage.SortDefault(isDesc, nullsLast) :: tail ->
                    let colNames = PolarsSchemaExtractor.GetColumnNames currentLf
                    let targetCol = if colNames.Length > 0 then colNames.[0] else ""
                    let spec = { SortSpec.Expr = PolarsWrapper.Col targetCol; Descending = isDesc; NullsLast = nullsLast }
                    fuse currentLf tail (addSort spec opsAcc) clientPreds clientProjOpt scopeMap

                // Slicing & Reordering
                | LinqStage.Skip s :: LinqStage.Take t :: tail -> pushOp (QueryOp.Slice(int64 s, t)) tail
                | LinqStage.Take count :: tail -> pushOp (QueryOp.Slice(0L, count)) tail
                | LinqStage.Skip count :: tail -> pushOp (QueryOp.Slice(int64 count, UInt32.MaxValue)) tail
                | LinqStage.TakeLast count :: tail -> pushOp (QueryOp.Slice(-int64 count, count)) tail
                | LinqStage.SkipLast count :: tail -> pushOp (QueryOp.SkipLast count) tail
                | LinqStage.Reverse :: tail -> pushOp QueryOp.Reverse tail

                // Deduplication & Set / Concat Operations
                | LinqStage.Distinct :: tail ->
                    pushOp (QueryOp.Unique { SubsetCols = None; Keep = PlUniqueKeepStrategy.First; MaintainOrder = false }) tail

                | LinqStage.DistinctBy keyLambda :: tail ->
                    let opOpt =
                        extractDistinctColumns keyLambda
                        |> Option.map (fun cols ->
                            QueryOp.Unique { SubsetCols = Some cols; Keep = PlUniqueKeepStrategy.First; MaintainOrder = false })
                    pushOpOrFallback opOpt tail

                | LinqStage.SetOp(m, second, k1Opt, k2Opt) :: tail ->
                    let activeBaseLf = if opsAcc.IsEmpty then currentLf else applyNativeOps currentLf (List.rev opsAcc)
                    match compileSetOp m second k1Opt k2Opt activeBaseLf |> Option.map QueryOp.Join with
                    | Some joinOp ->
                        if tail.IsEmpty then
                            fuse activeBaseLf tail [ joinOp ] clientPreds clientProjOpt scopeMap
                        else
                            let joinedLf = applyNativeOps activeBaseLf [ joinOp ]
                            fuse joinedLf tail [] clientPreds clientProjOpt scopeMap
                    | None -> currentLf, [], [], None, true

                | LinqStage.Concat otherExpr :: tail ->
                    let opOpt =
                        SubqueryDecorrelator.resolveSequenceLazyFrame otherExpr
                        |> Option.map (fun otherLf -> QueryOp.Concat { OtherLf = otherLf; Prepend = false })
                    pushOpOrFallback opOpt tail

                | LinqStage.Union(secondExpr, keyLambdaOpt) :: tail ->
                    let opsOpt =
                        SubqueryDecorrelator.resolveSequenceLazyFrame secondExpr
                        |> Option.map (fun otherLf ->
                            let concatOp = QueryOp.Concat { OtherLf = otherLf; Prepend = false }
                            let uniqueOp =
                                QueryOp.Unique
                                    {
                                        SubsetCols = keyLambdaOpt |> Option.bind extractDistinctColumns
                                        Keep = PlUniqueKeepStrategy.First
                                        MaintainOrder = false
                                    }
                            [ uniqueOp; concatOp ])
                    pushOpsOrFallback opsOpt tail

                | LinqStage.Append elemExpr | LinqStage.Prepend elemExpr as stage :: tail ->
                    let isPrepend =
                        match stage with
                        | LinqStage.Prepend _ -> true
                        | _ -> false
                    pushOpOrFallback (tryCompileAppendOrPrepend elemExpr isPrepend) tail

                // Joins, GroupJoin, Zip, Explode, CrossJoin
                | LinqStage.Join(m, inner, outerKey, innerKey, resSelOpt) :: tail ->
                    let activeBaseLf = if opsAcc.IsEmpty then currentLf else applyNativeOps currentLf (List.rev opsAcc)
                    match compileJoin activeBaseLf m inner outerKey innerKey scopeMap with
                    | Some spec ->
                        let leftCols = getLazySchemaFields activeBaseLf |> Array.map fst
                        let rightCols = getLazySchemaFields spec.RightLf |> Array.map fst
                        let suffix = spec.Suffix |> Option.defaultValue (if spec.InvertSides then LinqConstants.DefaultLeftSuffix else LinqConstants.DefaultRightSuffix)
                        let leftScope = outerKey.Parameters.[0].Name
                        let rightScope =
                            match resSelOpt with
                            | Some resSel when resSel.Parameters.Count >= 2 -> resSel.Parameters.[1].Name
                            | _ -> innerKey.Parameters.[0].Name

                        let updatedScopeMap =
                            updateJoinScopeMap scopeMap leftScope leftCols rightScope rightCols suffix spec.InvertSides

                        let pushJoinOp op =
                            if tail.IsEmpty then
                                fuse activeBaseLf tail [ op ] clientPreds clientProjOpt updatedScopeMap
                            else
                                let joinedLf = applyNativeOps activeBaseLf [ op ]
                                fuse joinedLf tail [] clientPreds clientProjOpt updatedScopeMap

                        match resSelOpt with
                        | TransparentOrNoResSelector -> pushJoinOp (QueryOp.Join spec)
                        | ConcreteResSelector resSel ->
                            let hasSubquery = canPushdown && SubqueryDecorrelator.containsSubquery resSel.Body
                            if hasSubquery then
                                let joinedBaseLf = applyNativeOps activeBaseLf [ QueryOp.Join spec ]
                                match SubqueryDecorrelator.tryDecorrelateProjection joinedBaseLf resSel updatedScopeMap with
                                | Some(subqueryOps, rewrittenResSel) ->
                                    let lfAfterSubqueries =
                                        if subqueryOps.IsEmpty then joinedBaseLf else applyNativeOps joinedBaseLf subqueryOps
                                    let postJoinLeftCols = getLazySchemaFields lfAfterSubqueries |> Array.map fst
                                    match compileJoinResultSelector postJoinLeftCols rightCols spec rewrittenResSel updatedScopeMap with
                                    | Some selectOp ->
                                        if tail.IsEmpty then
                                            fuse lfAfterSubqueries tail [ selectOp ] clientPreds clientProjOpt Map.empty
                                        else
                                            let finalLf = applyNativeOps lfAfterSubqueries [ selectOp ]
                                            fuse finalLf tail [] clientPreds clientProjOpt Map.empty
                                    | None ->
                                        FallbackTracker.Diagnostic <-
                                            Some(
                                                sprintf
                                                    "CompileJoinResultSelector with subquery failed: %s\nLeftCols: %A\nRightCols: %A"
                                                    (rewrittenResSel.ToString())
                                                    postJoinLeftCols
                                                    rightCols
                                            )
                                        currentLf, [], [], None, true
                                | None ->
                                    currentLf, [], [], None, true
                            else
                                match compileJoinResultSelector leftCols rightCols spec resSel updatedScopeMap with
                                | Some selectOp ->
                                    if tail.IsEmpty then
                                        fuse activeBaseLf tail [ selectOp; QueryOp.Join spec ] clientPreds clientProjOpt Map.empty
                                    else
                                        let joinedLf = applyNativeOps activeBaseLf [ QueryOp.Join spec; selectOp ]
                                        fuse joinedLf tail [] clientPreds clientProjOpt Map.empty
                                | None ->
                                    FallbackTracker.Diagnostic <-
                                        Some(
                                            sprintf
                                                "CompileJoinResultSelector failed: %s\nLeftCols: %A\nRightCols: %A"
                                                (resSel.ToString())
                                                leftCols
                                                rightCols
                                        )
                                    currentLf, [], [], None, true
                    | None ->
                        FallbackTracker.Diagnostic <-
                            Some(
                                sprintf
                                    "CompileJoin failed for inner: %s, outerKey: %s, innerKey: %s"
                                    (inner.ToString())
                                    (outerKey.ToString())
                                    (innerKey.ToString())
                            )
                        currentLf, [], [], None, true

                | LinqStage.JoinAsOf(methodInfo, inner, outerKey, innerKey, outerByOpt, innerByOpt, resSelOpt, tolStr, tolInt, tolFloat, strategy) :: tail ->
                    let activeBaseLf = if opsAcc.IsEmpty then currentLf else applyNativeOps currentLf (List.rev opsAcc)
                    match compileJoinAsOf activeBaseLf methodInfo inner outerKey innerKey outerByOpt innerByOpt tolStr tolInt tolFloat strategy scopeMap with
                    | Some spec ->
                        let leftCols = getLazySchemaFields activeBaseLf |> Array.map fst
                        let rightCols = getLazySchemaFields spec.RightLf |> Array.map fst
                        let leftColSet = Set.ofArray leftCols
                        let suffix = spec.Suffix |> Option.defaultValue LinqConstants.DefaultRightSuffix
                        let leftScope = outerKey.Parameters.[0].Name
                        let rightScope =
                            match resSelOpt with
                            | Some resSel when resSel.Parameters.Count >= 2 -> resSel.Parameters.[1].Name
                            | _ -> innerKey.Parameters.[0].Name

                        let extractColNames (lambda: LambdaExpression) : string list =
                            match tryExtractMemberBindings lambda.Body with
                            | Some bindings -> bindings |> List.map fst
                            | None ->
                                match ExprTranslator.unwrap lambda.Body with
                                | MemberAccess(_, m) -> [ m.Name ]
                                | _ -> []

                        let rightKeyCols =
                            extractColNames innerKey
                            @ (innerByOpt |> Option.map extractColNames |> Option.defaultValue [])
                            |> Set.ofList

                        let initialLeftMap =
                            if scopeMap.IsEmpty then
                                leftCols |> Array.fold (fun m col -> Map.add (leftScope, col) col m) scopeMap
                            else
                                scopeMap

                        let updatedScopeMap =
                            rightCols
                            |> Array.fold (fun m col ->
                                let physicalCol =
                                    if rightKeyCols.Contains col then col
                                    elif leftColSet.Contains col then col + suffix
                                    else col
                                Map.add (rightScope, col) physicalCol m) initialLeftMap

                        let availableCols =
                            let rightAddedCols =
                                rightCols
                                |> Array.choose (fun c ->
                                    if rightKeyCols.Contains c then None
                                    elif leftColSet.Contains c then Some(c + suffix)
                                    else Some c)
                            Array.append leftCols rightAddedCols

                        let pushJoinOp op =
                            if tail.IsEmpty then
                                fuse activeBaseLf tail [ op ] clientPreds clientProjOpt updatedScopeMap
                            else
                                let joinedLf = applyNativeOps activeBaseLf [ op ]
                                fuse joinedLf tail [] clientPreds clientProjOpt updatedScopeMap

                        match resSelOpt with
                        | TransparentOrNoResSelector -> pushJoinOp (QueryOp.JoinAsOf spec)
                        | ConcreteResSelector resSel ->
                            let hasSubquery = canPushdown && SubqueryDecorrelator.containsSubquery resSel.Body
                            if hasSubquery then
                                let joinedBaseLf = applyNativeOps activeBaseLf [ QueryOp.JoinAsOf spec ]
                                match SubqueryDecorrelator.tryDecorrelateProjection joinedBaseLf resSel updatedScopeMap with
                                | Some(subqueryOps, rewrittenResSel) ->
                                    let lfAfterSubqueries =
                                        if subqueryOps.IsEmpty then joinedBaseLf else applyNativeOps joinedBaseLf subqueryOps
                                    let postSubqueryCols = getLazySchemaFields lfAfterSubqueries |> Array.map fst
                                    match compileJoinResultSelectorWithParams postSubqueryCols rightCols false spec.Suffix rewrittenResSel updatedScopeMap with
                                    | Some selectOp ->
                                        if tail.IsEmpty then
                                            fuse lfAfterSubqueries tail [ selectOp ] clientPreds clientProjOpt Map.empty
                                        else
                                            let finalLf = applyNativeOps lfAfterSubqueries [ selectOp ]
                                            fuse finalLf tail [] clientPreds clientProjOpt Map.empty
                                    | None ->
                                        FallbackTracker.Diagnostic <-
                                            Some(
                                                sprintf
                                                    "CompileJoinAsOfResultSelector with subquery failed: %s\nPostSubqueryCols: %A\nRightCols: %A"
                                                    (rewrittenResSel.ToString())
                                                    postSubqueryCols
                                                    rightCols
                                            )
                                        currentLf, [], [], None, true
                                | None ->
                                    currentLf, [], [], None, true
                            else
                                match compileJoinResultSelectorWithParams availableCols rightCols false spec.Suffix resSel updatedScopeMap with
                                | Some selectOp ->
                                    if tail.IsEmpty then
                                        fuse activeBaseLf tail [ selectOp; QueryOp.JoinAsOf spec ] clientPreds clientProjOpt Map.empty
                                    else
                                        let joinedLf = applyNativeOps activeBaseLf [ QueryOp.JoinAsOf spec; selectOp ]
                                        fuse joinedLf tail [] clientPreds clientProjOpt Map.empty
                                | None ->
                                    FallbackTracker.Diagnostic <-
                                        Some(
                                            sprintf
                                                "CompileJoinAsOfResultSelector failed: %s\nAvailableCols: %A\nRightCols: %A"
                                                (resSel.ToString())
                                                availableCols
                                                rightCols
                                        )
                                    currentLf, [], [], None, true
                    | None ->
                        FallbackTracker.Diagnostic <-
                            Some(
                                sprintf
                                    "CompileJoinAsOf failed for inner: %s, outerKey: %s, innerKey: %s"
                                    (inner.ToString())
                                    (outerKey.ToString())
                                    (innerKey.ToString())
                            )
                        currentLf, [], [], None, true

                | LinqStage.GroupJoin(_, inner, outerKey, innerKey, _) :: LinqStage.Explode(collSelector, resSelOpt) :: tail when
                    (tryExtractDefaultIfEmpty collSelector.Body).IsSome
                    ->
                    let activeBaseLf = if opsAcc.IsEmpty then currentLf else applyNativeOps currentLf (List.rev opsAcc)
                    match compileGroupJoinLeftOuterSpec activeBaseLf inner outerKey innerKey scopeMap with
                    | Some spec ->
                        let joinOp = QueryOp.Join spec
                        let leftCols = getLazySchemaFields activeBaseLf |> Array.map fst
                        let rightCols = getLazySchemaFields spec.RightLf |> Array.map fst
                        let suffix = spec.Suffix |> Option.defaultValue LinqConstants.DefaultRightSuffix
                        let leftScope = outerKey.Parameters.[0].Name
                        let rightScope =
                            match resSelOpt with
                            | Some resSel when resSel.Parameters.Count >= 2 -> resSel.Parameters.[1].Name
                            | _ -> innerKey.Parameters.[0].Name

                        let updatedScopeMap =
                            updateJoinScopeMap scopeMap leftScope leftCols rightScope rightCols suffix false

                        let pushJoinOp op =
                            if tail.IsEmpty then
                                fuse activeBaseLf tail [ op ] clientPreds clientProjOpt updatedScopeMap
                            else
                                let joinedLf = applyNativeOps activeBaseLf [ op ]
                                fuse joinedLf tail [] clientPreds clientProjOpt updatedScopeMap

                        match resSelOpt with
                        | TransparentOrNoResSelector -> pushJoinOp joinOp
                        | ConcreteResSelector resSel ->
                            match compileJoinResultSelector leftCols rightCols spec resSel updatedScopeMap with
                            | Some selectOp ->
                                if tail.IsEmpty then
                                    fuse activeBaseLf tail [ selectOp; joinOp ] clientPreds clientProjOpt Map.empty
                                else
                                    let joinedLf = applyNativeOps activeBaseLf [ joinOp; selectOp ]
                                    fuse joinedLf tail [] clientPreds clientProjOpt Map.empty
                            | None ->
                                let joinedLf = applyNativeOps activeBaseLf [ joinOp ]
                                fuse joinedLf tail [] clientPreds clientProjOpt updatedScopeMap
                    | None -> currentLf, [], [], None, true

                | LinqStage.GroupJoin(m, innerExpr, outerKey, innerKey, None) :: LinqStage.Project projLambda :: tail ->
                    let outerType, innerType = outerKey.Parameters.[0].Type, innerKey.Parameters.[0].Type
                    let desugaredResSel = desugarGroupJoinProject projLambda outerType innerType
                    fuse currentLf (LinqStage.GroupJoin(m, innerExpr, outerKey, innerKey, Some desugaredResSel) :: tail) opsAcc clientPreds clientProjOpt scopeMap

                | LinqStage.GroupJoin(_, innerExpr, outerKey, innerKey, Some resSel) :: tail ->
                    let activeBaseLf = if opsAcc.IsEmpty then currentLf else applyNativeOps currentLf (List.rev opsAcc)
                    match compileGroupJoinWithResult activeBaseLf innerExpr outerKey innerKey resSel with
                    | Some joinOps ->
                        if tail.IsEmpty then
                            fuse activeBaseLf tail joinOps clientPreds clientProjOpt scopeMap
                        else
                            let joinedLf = applyNativeOps activeBaseLf (List.rev joinOps)
                            fuse joinedLf tail [] clientPreds clientProjOpt scopeMap
                    | None -> currentLf, [], [], None, true

                | LinqStage.GroupJoin(m, innerExpr, outerKey, innerKey, None) :: tail ->
                    let activeBaseLf = if opsAcc.IsEmpty then currentLf else applyNativeOps currentLf (List.rev opsAcc)
                    match compileJoin activeBaseLf m innerExpr outerKey innerKey scopeMap |> Option.map QueryOp.Join with
                    | Some joinOp ->
                        if tail.IsEmpty then
                            fuse activeBaseLf tail [ joinOp ] clientPreds clientProjOpt scopeMap
                        else
                            let joinedLf = applyNativeOps activeBaseLf [ joinOp ]
                            fuse joinedLf tail [] clientPreds clientProjOpt scopeMap
                    | None -> currentLf, [], [], None, true

                | LinqStage.Zip(secondExpr, resLambdaOpt) :: tail ->
                    pushOpsOrFallback (compileZip currentLf secondExpr resLambdaOpt) tail

                | LinqStage.Zip3(secondExpr, thirdExpr) :: tail ->
                    pushOpOrFallback (compileZip3 currentLf secondExpr thirdExpr) tail

                | LinqStage.Explode(colLambda, resLambdaOpt) :: tail ->
                    pushOpsOrFallback (compileExplode colLambda resLambdaOpt) tail

                | LinqStage.Unpivot(colLambda, resLambdaOpt) :: tail ->
                    let activeBaseLf = if opsAcc.IsEmpty then currentLf else applyNativeOps currentLf (List.rev opsAcc)
                    let availableCols = getLazySchemaFields activeBaseLf |> Array.map fst
                    match tryExtractUnpivotPattern colLambda resLambdaOpt with
                    | Some pattern ->
                        let explicitIndexCols = pattern.ExplicitIndexCols
                        let onCols = pattern.OnCols
                        let indexCols =
                            if not explicitIndexCols.IsEmpty then explicitIndexCols |> List.toArray
                            else availableCols |> Array.filter (fun c -> not (Array.contains c onCols))

                        if indexCols.Length = 0 then
                            FallbackTracker.Diagnostic <- Some "Unpivot requires at least one index column"
                            currentLf, [], [], None, true
                        else
                            let spec =
                                {
                                    Index = indexCols
                                    On = Some onCols
                                    VariableName = pattern.VariableName
                                    ValueName = pattern.ValueName
                                }
                            let unpivotOp = QueryOp.Unpivot spec
                            let postUnpivotCols = Array.append indexCols [| pattern.VariableName; pattern.ValueName |]
                            let leftScope = colLambda.Parameters.[0].Name
                            let updatedScopeMap =
                                let outerMappings = indexCols |> Array.map (fun c -> (leftScope, c), c)
                                let innerScope =
                                    match resLambdaOpt with
                                    | Some resSel when resSel.Parameters.Count >= 2 -> resSel.Parameters.[1].Name
                                    | _ -> "q"
                                let innerMappings =
                                    [
                                        (innerScope, pattern.VariableName), pattern.VariableName
                                        (innerScope, pattern.ValueName), pattern.ValueName
                                        (innerScope, pattern.VarPropName), pattern.VariableName
                                        (innerScope, pattern.ValPropName), pattern.ValueName
                                        (innerScope, "Item1"), pattern.VariableName
                                        (innerScope, "Item2"), pattern.ValueName
                                    ]
                                scopeMap
                                |> Map.fold (fun m k v -> Map.add k v m) (Map.ofArray (Array.append outerMappings (List.toArray innerMappings)))

                            let pushJoinOp op =
                                if tail.IsEmpty then
                                    fuse activeBaseLf tail [ op ] clientPreds clientProjOpt updatedScopeMap
                                else
                                    let unpivotedLf = applyNativeOps activeBaseLf [ op ]
                                    fuse unpivotedLf tail [] clientPreds clientProjOpt updatedScopeMap

                            match resLambdaOpt with
                            | TransparentOrNoResSelector -> pushJoinOp unpivotOp
                            | ConcreteResSelector resSel ->
                                match compileJoinResultSelectorWithParams postUnpivotCols [||] false None resSel updatedScopeMap with
                                | Some selectOp ->
                                    if tail.IsEmpty then
                                        fuse activeBaseLf tail [ selectOp; unpivotOp ] clientPreds clientProjOpt Map.empty
                                    else
                                        let unpivotedLf = applyNativeOps activeBaseLf [ unpivotOp; selectOp ]
                                        fuse unpivotedLf tail [] clientPreds clientProjOpt Map.empty
                                | None ->
                                    FallbackTracker.Diagnostic <- Some (sprintf "CompileUnpivotResultSelector failed: %s" (resSel.ToString()))
                                    currentLf, [], [], None, true
                    | None ->
                        FallbackTracker.Diagnostic <- Some "ExtractUnpivotPattern failed"
                        currentLf, [], [], None, true

                | LinqStage.CrossJoin(innerExpr, resLambdaOpt) :: tail ->
                    pushOpsOrFallback (compileCrossJoin innerExpr resLambdaOpt) tail

                | LinqStage.GroupPartitionSlice(keySelector, innerStages, resSelOpt) :: tail ->
                    let activeBaseLf = if opsAcc.IsEmpty then currentLf else applyNativeOps currentLf (List.rev opsAcc)
                    match compileGroupPartitionSlice activeBaseLf keySelector innerStages resSelOpt with
                    | Some sliceOps ->
                        if tail.IsEmpty then
                            fuse activeBaseLf tail sliceOps clientPreds clientProjOpt scopeMap
                        else
                            let slicedLf = applyNativeOps activeBaseLf (List.rev sliceOps)
                            fuse slicedLf tail [] clientPreds clientProjOpt scopeMap
                    | None -> currentLf, [], [], None, true

                // Type Filtering, CountBy & Passthrough Stages
                | LinqStage.OfType(sourceType, targetType) :: tail ->
                    if not (targetType.IsAssignableFrom sourceType || sourceType.IsAssignableFrom targetType) then
                        pushOp (QueryOp.Filter(PolarsWrapper.Lit false)) tail
                    else
                        fuse currentLf tail opsAcc clientPreds clientProjOpt scopeMap

                | LinqStage.CountBy keyLambda :: tail ->
                    pushOpOrFallback (compileCountBy targetType currentLf keyLambda) tail

                | (LinqStage.DefaultIfEmpty _ | LinqStage.Cast _ | LinqStage.AggregateBy _) :: tail ->
                    fuse currentLf tail opsAcc clientPreds clientProjOpt scopeMap

                | _ :: tail -> fuse currentLf tail opsAcc clientPreds clientProjOpt scopeMap
                | [] -> currentLf, List.rev opsAcc, List.rev clientPreds, clientProjOpt, false

            | _ :: tail -> fuse currentLf tail opsAcc clientPreds clientProjOpt scopeMap

        let finalLf, ops, clientPreds, clientProjOpt, requiresClientFallback = fuse currentLf stages [] [] None Map.empty
        if requiresClientFallback && FallbackTracker.Diagnostic.IsNone then
            FallbackTracker.Diagnostic <- Some (sprintf "Stages failed to fuse. stages count: %d\nbound: %s\nstages: %A" stages.Length (bound.ToString()) stages)
        finalLf, ops, clientPreds, clientProjOpt, requiresClientFallback

