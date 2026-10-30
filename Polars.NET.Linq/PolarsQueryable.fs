namespace Polars.NET.Linq

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open Polars.NET.Core
open Polars.NET.Core.Helpers
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks

/// Strongly-typed IQueryProvider backed by Polars.NET.Core
type internal PolarsQueryProvider(initialLazyFrame: LazyFrameHandle, materializer: IDataFrameMaterializer) =
    let rootTemplate = PolarsWrapper.LazyClone initialLazyFrame

    member _.RootLazyFrame = PolarsWrapper.LazyClone rootTemplate
    member _.Materializer = materializer

    interface IQueryProvider with
        member this.CreateQuery(expression: Expression) : IQueryable =
            let elemType =
                match PolarsTypeHelper.TryGetEnumerableElementType expression.Type with
                | null -> typedefof<obj>
                | t -> t

            let queryType = typedefof<PolarsQuery<_>>.MakeGenericType elemType
            Activator.CreateInstance(
                queryType,
                Reflection.BindingFlags.Instance ||| Reflection.BindingFlags.NonPublic ||| Reflection.BindingFlags.Public,
                null,
                [| box this; box (Some expression) |],
                null
            )
            :?> IQueryable

        member this.CreateQuery<'TElement>(expression: Expression) : IQueryable<'TElement> =
            PolarsQuery<'TElement>(this, Some expression) :> IQueryable<'TElement>

        member this.Execute(expression: Expression) : obj =
            let executeMethod =
                typeof<PolarsQueryProvider>.GetInterface("IQueryProvider").GetMethods()
                |> Array.find (fun m -> m.Name = "Execute" && m.IsGenericMethod)
                |> fun m -> m.MakeGenericMethod expression.Type

            executeMethod.Invoke(this, [| expression |])

        member this.Execute<'TResult>(expression: Expression) : 'TResult =
            let targetType = typeof<'TResult>

            match expression with
            | MethodCall(m, _, _) when ScalarExecutor.isScalarReductionMethod m ->
                ScalarExecutor.executeScalar<'TResult> (this :> IQueryProvider) materializer expression
            | _ when PolarsTypeHelper.IsScalarType targetType || targetType = typeof<string> ->
                ScalarExecutor.executeScalar<'TResult> (this :> IQueryProvider) materializer expression
            | _ ->
                let query = (this :> IQueryProvider).CreateQuery expression

                match box query with
                | :? 'TResult as matched -> matched
                | _ ->
                    let elemType =
                        match PolarsTypeHelper.TryGetEnumerableElementType targetType with
                        | null -> typeof<obj>
                        | t -> t

                    let toListMethod =
                        LinqReflectionCache.GetEnumerableMethod("ToList", 1, [| elemType |])

                    let listObj = toListMethod.Invoke(null, [| box query |])

                    if targetType.IsArray then
                        let toArrayMethod =
                            LinqReflectionCache.GetEnumerableMethod("ToArray", 1, [| elemType |])

                        toArrayMethod.Invoke(null, [| box query |]) :?> 'TResult
                    elif targetType.IsAssignableFrom(listObj.GetType()) then
                        listObj :?> 'TResult
                    else
                        failwithf "Cannot convert query sequence to requested terminal type '%s'" targetType.FullName

and PolarsQuery<'T>
    internal (provider: PolarsQueryProvider, exprOpt: Expression option) as this
    =
    let materializer = provider.Materializer
    let targetType = typeof<'T>
    let isScalar = PolarsTypeHelper.IsScalarType targetType || targetType = typeof<string>
    let isGrouping =
        targetType.IsGenericType
        && targetType.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

    let expression =
        match exprOpt with
        | Some e -> e
        | None -> Expression.Constant(this, typeof<IQueryable<'T>>) :> Expression

    let getEffectiveExpr () =
        let isCSharp = not (isNull materializer) && materializer.GetType().Name.StartsWith "CSharp"
        if not isCSharp && FSharpAst.needsRewrite expression then
            FSharpAst.rewrite expression
        else
            expression

    new(lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer, expression: Expression) =
        PolarsQuery<'T>(PolarsQueryProvider(lazyFrameHandle, materializer), Some expression)

    new(lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer) =
        PolarsQuery<'T>(PolarsQueryProvider(lazyFrameHandle, materializer), None)

    /// Parses the LINQ expression tree into native ops, client predicates, optional projection, and fallback signal
    member private _.CompilePipeline
        ()
        : LazyFrameHandle * QueryOp list * ((Func<obj, bool> * LambdaExpression) list) * ((Type * Func<obj, obj>) option) * bool =
        let lfCloned = provider.RootLazyFrame
        let rewrittenExpr = getEffectiveExpr ()
        let extraction = PartialEvaluator.extract rewrittenExpr
        let cacheKey = QueryCacheKey.Create(typeof<'T>, extraction.NormalizedExpression)

        let cached =
            QueryPlanCache.GetOrAdd(
                cacheKey,
                fun _ ->
                    let normalizedStages =
                        try
                            let s = flattenStages extraction.NormalizedExpression []
                            let hasSecondarySource =
                                s |> List.exists (function
                                    | LinqStage.Join _
                                    | LinqStage.JoinAsOf _
                                    | LinqStage.GroupJoin _
                                    | LinqStage.CrossJoin _
                                    | LinqStage.Zip _
                                    | LinqStage.Zip3 _
                                    | LinqStage.SetOp _
                                    | LinqStage.Union _
                                    | LinqStage.Concat _ -> true
                                    | _ -> false)
                            if hasSecondarySource then [] else s
                        with _ ->
                            []
                    {
                        Key = cacheKey
                        NormalizedExpression = extraction.NormalizedExpression
                        ParamNames = extraction.Parameters.Keys |> Seq.toArray
                        NormalizedStages = normalizedStages
                        LastAccessTick = Environment.TickCount64
                    }
            )

        let bound =
            if extraction.Parameters.Count = 0 then
                extraction.NormalizedExpression
            else
                PartialEvaluator.substituteExtracted extraction.NormalizedExpression extraction.Parameters

        let stages =
            if not cached.NormalizedStages.IsEmpty then
                if extraction.Parameters.Count = 0 then
                    cached.NormalizedStages
                else
                    cached.NormalizedStages
                    |> List.map (QueryPlanCache.SubstituteStage extraction.Parameters)
            else
                flattenStages bound []
        FallbackTracker.Diagnostic <- None

        let addSort (spec: SortSpec) (ops: QueryOp list) =
            match ops with
            | QueryOp.Sort specs :: tail -> QueryOp.Sort(specs @ [ spec ]) :: tail
            | _ -> QueryOp.Sort [ spec ] :: ops

        let fullFallback = lfCloned, [], [], None, true

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
                    match compileUnifiedGroupBy activeLf ctx projBody groupParamName isKeyArg with
                    | Some compiledOps ->
                        if nextTail.IsEmpty then
                            fuse activeLf nextTail (compiledOps @ []) clientPreds clientProjOpt Map.empty
                        else
                            let groupedLf = applyNativeOps activeLf (List.rev compiledOps)
                            fuse groupedLf nextTail [] clientPreds clientProjOpt Map.empty
                    | None when isGrouping -> fuse currentLf nextTail opsAcc clientPreds clientProjOpt scopeMap
                    | None -> currentLf, [], [], None, true

            let slurpPipelineGroupBy (stages: LinqStage list) (initCtx: GroupFoldContext) =
                let rec slurp (s: LinqStage list) (ctx: GroupFoldContext) =
                    match s with
                    | LinqStage.Filter pred :: next ->
                        slurp next { ctx with HavingPreds = ctx.HavingPreds @ [ pred ] }
                    | LinqStage.Sort(sortLambda, isDesc) :: next ->
                        slurp next { ctx with SortStages = ctx.SortStages @ [ (sortLambda, isDesc) ] }
                    | LinqStage.Project projLambda :: next ->
                        let isKeyArg (e: Expression) =
                            match ExprTranslator.unwrap e with
                            | MemberAccess(p, m) when
                                not (isNull p) && p.NodeType = ExpressionType.Parameter && m.Name = "Key"
                                ->
                                true
                            | :? ParameterExpression as p when p.Name = projLambda.Parameters.[0].Name ->
                                true
                            | _ -> false

                        tryDispatchGroupBy ctx projLambda.Body projLambda.Parameters.[0].Name isKeyArg next
                    | [] when isGrouping ->
                        let clientHavingPreds =
                            ctx.HavingPreds |> List.map (fun p -> compileClientPredicate p, p)

                        fuse currentLf [] opsAcc (clientHavingPreds @ clientPreds) clientProjOpt scopeMap
                    | _ -> currentLf, [], [], None, true

                slurp stages initCtx

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
                    match compileUnifiedGroupingSets activeLf GroupingKind.Rollup keyLambda resLambda with
                    | Some groupedLf ->
                        fuse groupedLf tail [] clientPreds clientProjOpt Map.empty
                    | None -> currentLf, [], [], None, true

            | LinqStage.GroupByCube(keyLambda, resLambda) :: tail ->
                if not clientPreds.IsEmpty then
                    currentLf, [], [], None, true
                else
                    let activeLf = if opsAcc.IsEmpty then currentLf else applyNativeOps currentLf (List.rev opsAcc)
                    match compileUnifiedGroupingSets activeLf GroupingKind.Cube keyLambda resLambda with
                    | Some groupedLf ->
                        fuse groupedLf tail [] clientPreds clientProjOpt Map.empty
                    | None -> currentLf, [], [], None, true

            | LinqStage.GroupByGroupingSets(keyLambda, setsExpr, resLambda) :: tail ->
                if not clientPreds.IsEmpty then
                    currentLf, [], [], None, true
                else
                    let activeLf = if opsAcc.IsEmpty then currentLf else applyNativeOps currentLf (List.rev opsAcc)
                    match compileUnifiedGroupingSets activeLf (GroupingKind.GroupingSets setsExpr) keyLambda resLambda with
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
                        if
                            p.Type.Name.StartsWith "AnonymousObject"
                            || p.Type.Name.Contains "TransparentIdentifier"
                            || clientProjOpt.IsSome
                        then
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
            | LinqStage.Sort(l, isDesc) :: tail ->
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
                        let spec = { Expr = exprHandle; Descending = isDesc; NullsLast = false }
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

                | LinqStage.SortDefault isDesc :: tail ->
                    let colNames = PolarsSchemaExtractor.GetColumnNames currentLf
                    let targetCol = if colNames.Length > 0 then colNames.[0] else ""
                    let spec = { Expr = PolarsWrapper.Col targetCol; Descending = isDesc; NullsLast = false }
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
                        let suffix = spec.Suffix |> Option.defaultValue (if spec.InvertSides then "_left" else "_right")
                        let leftScope = outerKey.Parameters.[0].Name
                        let rightScope = innerKey.Parameters.[0].Name

                        let updatedScopeMap =
                            if spec.InvertSides then
                                let rightColSet = Set.ofArray rightCols
                                let initialRightMap =
                                    if scopeMap.IsEmpty then
                                        rightCols |> Array.fold (fun m col -> Map.add (rightScope, col) col m) scopeMap
                                    else
                                        scopeMap
                                leftCols
                                |> Array.fold (fun m col ->
                                    let physicalCol = if rightColSet.Contains col then col + suffix else col
                                    Map.add (leftScope, col) physicalCol m) initialRightMap
                            else
                                let leftColSet = Set.ofArray leftCols
                                let initialLeftMap =
                                    if scopeMap.IsEmpty then
                                        leftCols |> Array.fold (fun m col -> Map.add (leftScope, col) col m) scopeMap
                                    else
                                        scopeMap
                                rightCols
                                |> Array.fold (fun m col ->
                                    let physicalCol = if leftColSet.Contains col then col + suffix else col
                                    Map.add (rightScope, col) physicalCol m) initialLeftMap

                        match resSelOpt with
                        | None ->
                            if tail.IsEmpty then
                                fuse activeBaseLf tail [ QueryOp.Join spec ] clientPreds clientProjOpt updatedScopeMap
                            else
                                let joinedLf = applyNativeOps activeBaseLf [ QueryOp.Join spec ]
                                fuse joinedLf tail [] clientPreds clientProjOpt updatedScopeMap
                        | Some resSel when isTransparentJoinResSelector resSel ->
                            if tail.IsEmpty then
                                fuse activeBaseLf tail [ QueryOp.Join spec ] clientPreds clientProjOpt updatedScopeMap
                            else
                                let joinedLf = applyNativeOps activeBaseLf [ QueryOp.Join spec ]
                                fuse joinedLf tail [] clientPreds clientProjOpt updatedScopeMap
                        | Some resSel ->
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
                        let suffix = spec.Suffix |> Option.defaultValue "_right"
                        let leftScope = outerKey.Parameters.[0].Name
                        let rightScope = innerKey.Parameters.[0].Name

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

                        match resSelOpt with
                        | None ->
                            if tail.IsEmpty then
                                fuse activeBaseLf tail [ QueryOp.JoinAsOf spec ] clientPreds clientProjOpt updatedScopeMap
                            else
                                let joinedLf = applyNativeOps activeBaseLf [ QueryOp.JoinAsOf spec ]
                                fuse joinedLf tail [] clientPreds clientProjOpt updatedScopeMap
                        | Some resSel when isTransparentJoinResSelector resSel ->
                            if tail.IsEmpty then
                                fuse activeBaseLf tail [ QueryOp.JoinAsOf spec ] clientPreds clientProjOpt updatedScopeMap
                            else
                                let joinedLf = applyNativeOps activeBaseLf [ QueryOp.JoinAsOf spec ]
                                fuse joinedLf tail [] clientPreds clientProjOpt updatedScopeMap
                        | Some resSel ->
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
                    collSelector.ToString().Contains "DefaultIfEmpty"
                    ->
                    let activeBaseLf = if opsAcc.IsEmpty then currentLf else applyNativeOps currentLf (List.rev opsAcc)
                    match compileGroupJoinLeftOuterSpec activeBaseLf inner outerKey innerKey scopeMap with
                    | Some spec ->
                        let joinOp = QueryOp.Join spec
                        let leftCols = getLazySchemaFields activeBaseLf |> Array.map fst
                        let rightCols = getLazySchemaFields spec.RightLf |> Array.map fst
                        let suffix = spec.Suffix |> Option.defaultValue "_right"
                        let leftScope = outerKey.Parameters.[0].Name
                        let rightScope =
                            match resSelOpt with
                            | Some resSel when resSel.Parameters.Count >= 2 -> resSel.Parameters.[1].Name
                            | _ -> innerKey.Parameters.[0].Name

                        let leftColSet = Set.ofArray leftCols
                        let initialLeftMap =
                            if scopeMap.IsEmpty then
                                leftCols |> Array.fold (fun m col -> Map.add (leftScope, col) col m) scopeMap
                            else
                                scopeMap

                        let updatedScopeMap =
                            rightCols
                            |> Array.fold (fun m col ->
                                let physicalCol = if leftColSet.Contains col then col + suffix else col
                                Map.add (rightScope, col) physicalCol m) initialLeftMap

                        match resSelOpt with
                        | None ->
                            if tail.IsEmpty then
                                fuse activeBaseLf tail [ joinOp ] clientPreds clientProjOpt updatedScopeMap
                            else
                                let joinedLf = applyNativeOps activeBaseLf [ joinOp ]
                                fuse joinedLf tail [] clientPreds clientProjOpt updatedScopeMap
                        | Some resSel when isTransparentJoinResSelector resSel ->
                            if tail.IsEmpty then
                                fuse activeBaseLf tail [ joinOp ] clientPreds clientProjOpt updatedScopeMap
                            else
                                let joinedLf = applyNativeOps activeBaseLf [ joinOp ]
                                fuse joinedLf tail [] clientPreds clientProjOpt updatedScopeMap
                        | Some resSel ->
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

                            match resLambdaOpt with
                            | None ->
                                if tail.IsEmpty then
                                    fuse activeBaseLf tail [ unpivotOp ] clientPreds clientProjOpt updatedScopeMap
                                else
                                    let unpivotedLf = applyNativeOps activeBaseLf [ unpivotOp ]
                                    fuse unpivotedLf tail [] clientPreds clientProjOpt updatedScopeMap
                            | Some resSel when isTransparentJoinResSelector resSel ->
                                if tail.IsEmpty then
                                    fuse activeBaseLf tail [ unpivotOp ] clientPreds clientProjOpt updatedScopeMap
                                else
                                    let unpivotedLf = applyNativeOps activeBaseLf [ unpivotOp ]
                                    fuse unpivotedLf tail [] clientPreds clientProjOpt updatedScopeMap
                            | Some resSel ->
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
                    pushOpOrFallback (compileCountBy typeof<'T> currentLf keyLambda) tail

                | (LinqStage.DefaultIfEmpty _ | LinqStage.Cast _ | LinqStage.AggregateBy _) :: tail ->
                    fuse currentLf tail opsAcc clientPreds clientProjOpt scopeMap

                | _ :: tail -> fuse currentLf tail opsAcc clientPreds clientProjOpt scopeMap
                | [] -> currentLf, List.rev opsAcc, List.rev clientPreds, clientProjOpt, false

            | _ :: tail -> fuse currentLf tail opsAcc clientPreds clientProjOpt scopeMap

        let finalLf, ops, clientPreds, clientProjOpt, requiresClientFallback = fuse lfCloned stages [] [] None Map.empty
        if requiresClientFallback && FallbackTracker.Diagnostic.IsNone then
            FallbackTracker.Diagnostic <- Some (sprintf "Stages failed to fuse. stages count: %d\nbound: %s\nstages: %A" stages.Length (bound.ToString()) stages)
        finalLf, ops, clientPreds, clientProjOpt, requiresClientFallback

    /// Compiles pipeline and builds the target native LazyFrameHandle alongside client predicates, projection, and fallback signal
    member private this.GetCompiledPlan
        ()
        : LazyFrameHandle * (Func<obj, bool> list) * ((Type * Func<obj, obj>) option) * bool =
        let baseLf, ops, clientPredsWithAst, clientProjOpt, requiresClientFallback =
            this.CompilePipeline()

        enforcePipelinePolicy
            "QueryExecution"
            requiresClientFallback
            clientPredsWithAst
            (expression.ToString())

        let nativeLf = applyNativeOps baseLf ops
        let clientPredicates = clientPredsWithAst |> List.map fst
        nativeLf, clientPredicates, clientProjOpt, requiresClientFallback

    member private _.MaterializeFlatRows
        (dfHandle: DataFrameHandle, clientPredicates: Func<obj, bool> list, clientProjOpt: (Type * Func<obj, obj>) option)
        : IEnumerable<'T> =
        match clientProjOpt with
        | Some(sourceType, func) ->
            let invoker = LinqReflectionCache.GetMaterializerInvoker sourceType
            let rawSourceRows = invoker.Invoke(materializer, dfHandle)

            seq {
                for item in rawSourceRows do
                    if clientPredicates.IsEmpty || clientPredicates |> List.forall (fun p -> p.Invoke item) then
                        yield func.Invoke item :?> 'T
            }
        | None ->
            let rows = materializer.Materialize<'T> dfHandle

            if clientPredicates.IsEmpty then
                rows
            else
                rows
                |> Seq.filter (fun item -> clientPredicates |> List.forall (fun p -> p.Invoke(box item)))

    member private _.CanCollectNativeDirectly
        (
            nativeLf: LazyFrameHandle,
            clientPredicates: Func<obj, bool> list,
            clientProjOpt: (Type * Func<obj, obj>) option,
            requiresClientFallback: bool
        ) : bool =
        let schema = PolarsWrapper.GetLazySchema nativeLf
        let width = PolarsWrapper.GetSchemaLen schema

        not requiresClientFallback
        && clientPredicates.IsEmpty
        && clientProjOpt.IsNone
        && not (isScalar && width > 1UL)
        && not isGrouping

    /// Executes the full LINQ expression pipeline in-memory via .NET EnumerableQuery LINQ-to-Objects engine
    member private _.ExecuteInMemory() : IEnumerable<'T> =
        let effectiveExpr = getEffectiveExpr ()

        match tryFindRootQueryable effectiveExpr with
        | Some(rootExpr, rootType) ->
            let rootConst = rootExpr :?> ConstantExpression
            let rootQueryable = rootConst.Value :?> IQueryable

            let rootElemType =
                match PolarsTypeHelper.TryGetEnumerableElementType rootType with
                | null -> rootQueryable.ElementType
                | t -> t

            let toArrayMethod =
                LinqReflectionCache.GetEnumerableMethod("ToArray", 1, [| rootElemType |])

            let castMethod =
                LinqReflectionCache.GetEnumerableMethod("Cast", 1, [| rootElemType |])

            let casted = castMethod.Invoke(null, [| box rootQueryable |])
            let inMemoryArray = toArrayMethod.Invoke(null, [| casted |])

            let asQueryableMethod =
                typeof<Queryable>.GetMethods()
                |> Array.find (fun m -> m.Name = "AsQueryable" && m.IsGenericMethodDefinition)
                |> fun m -> m.MakeGenericMethod(rootElemType)

            let inMemoryQueryable =
                asQueryableMethod.Invoke(null, [| inMemoryArray |]) :?> IQueryable

            let targetQueryType = typedefof<IQueryable<_>>.MakeGenericType(rootElemType)
            let replacement = Expression.Constant(inMemoryQueryable, targetQueryType)

            let replacer =
                { new ExpressionVisitor() with
                    override _.VisitConstant(node: ConstantExpression) =
                        if
                            not (isNull node.Value)
                            && (obj.ReferenceEquals(node.Value, rootConst.Value)
                                || typeof<IPolarsPlanSource>.IsAssignableFrom(node.Type))
                        then
                            replacement
                        else
                            base.VisitConstant(node)
                }

            let inMemoryExpr = replacer.Visit(effectiveExpr)
            let resultQueryable = inMemoryQueryable.Provider.CreateQuery<'T>(inMemoryExpr)
            resultQueryable :> IEnumerable<'T>

        | None -> failwithf "Failed to locate root queryable for in-memory execution: %A" expression

    /// Evaluates the complete pipeline, executing native plan first, then materializing
    member private this.ExecuteQuery() : IEnumerable<'T> =
        let nativeLf, clientPredicates, clientProjOpt, requiresClientFallback =
            this.GetCompiledPlan()

        if requiresClientFallback then
            this.ExecuteInMemory()
        else
            let collectedDf = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

            let dfHandle =
                if hasShuffle expression then
                    applyShuffle collectedDf
                else
                    collectedDf

            if clientProjOpt.IsSome then
                this.MaterializeFlatRows(dfHandle, clientPredicates, clientProjOpt)
            else
                let chunkSizeOpt =
                    if targetType.IsArray then
                        tryExtractChunkSize expression
                    else
                        None

                let aggByInfoOpt = tryExtractAggregateBy expression

                // Case 1: GroupBy & GroupJoin materialization (produces IEnumerable<IGrouping<TKey, TElement>>)
                if isGrouping then
                    let genericArgs = targetType.GetGenericArguments()
                    let keyType, elemType = genericArgs.[0], genericArgs.[1]

                    let applyGroupSort (seqToSort: IEnumerable<'T>) : IEnumerable<'T> =
                        match tryExtractSortSelector expression with
                        | Some(sortLambda, isDesc) ->
                            let compiled = sortLambda.Compile()
                            let keySel (g: 'T) = compiled.DynamicInvoke([| box g |]) :?> IComparable
                            if isDesc then Seq.sortByDescending keySel seqToSort else Seq.sortBy keySel seqToSort
                        | None -> seqToSort

                    match tryExtractGroupByInfo expression with
                    | Some(keyLambda, elemLambdaOpt) ->
                        let compiledKeySelector = keyLambda.Compile()

                        let grouped =
                            match elemLambdaOpt with
                            | Some elemLambda ->
                                let sourceType = elemLambda.Parameters.[0].Type
                                let invoker = LinqReflectionCache.GetMaterializerInvoker sourceType
                                let rawRows = invoker.Invoke(materializer, dfHandle)
                                let compiledElemSelector = elemLambda.Compile()

                                let groupByMethod =
                                    typeof<Enumerable>.GetMethods()
                                    |> Array.find (fun m ->
                                        m.Name = "GroupBy"
                                        && m.GetParameters().Length = 3
                                        && m.GetGenericArguments().Length = 3)
                                    |> fun m -> m.MakeGenericMethod(sourceType, keyType, elemType)

                                groupByMethod.Invoke(
                                    null,
                                    [| box rawRows; box compiledKeySelector; box compiledElemSelector |]
                                )
                                :?> IEnumerable<'T>
                            | None ->
                                let invoker = LinqReflectionCache.GetMaterializerInvoker elemType
                                let rawRows = invoker.Invoke(materializer, dfHandle)

                                let groupByMethod =
                                    typeof<Enumerable>.GetMethods()
                                    |> Array.find (fun m ->
                                        m.Name = "GroupBy"
                                        && m.GetParameters().Length = 2
                                        && m.GetGenericArguments().Length = 2)
                                    |> fun m -> m.MakeGenericMethod(elemType, keyType)

                                groupByMethod.Invoke(null, [| box rawRows; box compiledKeySelector |])
                                :?> IEnumerable<'T>

                        let filtered =
                            if clientPredicates.IsEmpty then
                                grouped
                            else
                                grouped
                                |> Seq.filter (fun g -> clientPredicates |> List.forall (fun p -> p.Invoke(box g)))

                        applyGroupSort filtered

                    | None when hasGroupJoin expression ->
                        let innerNullableType =
                            if elemType.IsValueType then
                                typedefof<Nullable<_>>.MakeGenericType elemType
                            else
                                elemType

                        let pairType =
                            typedefof<ValueTuple<_, _>>.MakeGenericType(keyType, innerNullableType)

                        let invoker = LinqReflectionCache.GetMaterializerInvoker pairType
                        let rawPairs = invoker.Invoke(materializer, dfHandle)

                        let keyOrder = List<obj>()
                        let groupMap = Dictionary<obj, List<obj>>()

                        for pairObj in rawPairs do
                            let tuple = pairObj :?> ITuple
                            let outerObj = tuple.[0]
                            let innerObj = tuple.[1]

                            match groupMap.TryGetValue outerObj with
                            | true, list ->
                                if not (isNull innerObj) then
                                    list.Add innerObj
                            | false, _ ->
                                let list = List<obj>()

                                if not (isNull innerObj) then
                                    list.Add innerObj

                                groupMap.[outerObj] <- list
                                keyOrder.Add outerObj

                        let groupingType =
                            typedefof<MaterializedGrouping<_, _>>.MakeGenericType(keyType, elemType)

                        let ctor = groupingType.GetConstructors().[0]

                        let toArrayMethod =
                            LinqReflectionCache.GetEnumerableMethod("ToArray", 1, [| elemType |])

                        let castMethod =
                            LinqReflectionCache.GetEnumerableMethod("Cast", 1, [| elemType |])

                        let resultList = List<'T>()

                        for k in keyOrder do
                            let rawList = groupMap.[k]
                            let castedSeq = castMethod.Invoke(null, [| box rawList |])
                            let typedArray = toArrayMethod.Invoke(null, [| castedSeq |])
                            let grpInstance = ctor.Invoke [| k; typedArray |] :?> 'T
                            resultList.Add grpInstance

                        applyGroupSort resultList

                    | None -> failwith "Could not locate keySelector for GroupBy pipeline."

                // Case 2: Chunk slicing via stack-based cursor
                elif chunkSizeOpt.IsSome then
                    let chunkSize = chunkSizeOpt.Value
                    let elemType = targetType.GetElementType()

                    let invoker = LinqReflectionCache.GetMaterializerChunkInvoker elemType
                    let chunkedSeq = invoker.Invoke(materializer, dfHandle, chunkSize)
                    chunkedSeq.Cast<'T>()

                // Case 3: AggregateBy (produces IEnumerable<KeyValuePair<TKey, TAccum>>)
                elif aggByInfoOpt.IsSome then
                    let sourceExpr, keyLambda, seedExpr, seedLambdaOpt, funcLambda, comparerExprOpt =
                        aggByInfoOpt.Value

                    let genericArgs = targetType.GetGenericArguments()
                    let keyType, accumType = genericArgs.[0], genericArgs.[1]
                    let sourceElemType = keyLambda.Parameters.[0].Type

                    let compiledKey = keyLambda.Compile()
                    let compiledFunc = funcLambda.Compile()

                    let comparerObj =
                        match comparerExprOpt with
                        | Some ce -> tryEvaluate ce |> Option.toObj
                        | None -> null

                    let sourceQuery = (provider :> IQueryProvider).CreateQuery sourceExpr

                    let sourceLf =
                        match box sourceQuery with
                        | :? IPolarsPlanSource as ps -> ps.GetCompiledLazyFrameHandle()
                        | _ -> nativeLf

                    let sourceDfHandle = PolarsWrapper.LazyCollect(sourceLf, PlEngine.Auto)

                    let res =
                        match seedLambdaOpt with
                        | Some seedLambda ->
                            let invoker =
                                LinqReflectionCache.GetMaterializerAggByFactoryInvoker(sourceElemType, keyType, accumType)

                            invoker.Invoke(materializer, sourceDfHandle, compiledKey, seedLambda.Compile(), compiledFunc, comparerObj)
                        | None ->
                            let seedVal =
                                match tryEvaluate seedExpr with
                                | Some s -> s
                                | None -> failwith "Failed to evaluate static seed for AggregateBy."

                            let invoker =
                                LinqReflectionCache.GetMaterializerAggBySeedInvoker(sourceElemType, keyType, accumType)

                            invoker.Invoke(materializer, sourceDfHandle, compiledKey, seedVal, compiledFunc, comparerObj)

                    res.Cast<'T>()

                // Case 4: Flat row materialization (standard records, anonymous types, DTOs)
                else
                    let height = PolarsWrapper.DataFrameHeight dfHandle

                    match tryExtractDefaultIfEmpty expression with
                    | Some(true, customDefaultOpt) when height = 0L ->
                        let defaultItem =
                            match customDefaultOpt with
                            | Some v when not (isNull v) -> v :?> 'T
                            | _ -> Unchecked.defaultof<'T>

                        Seq.singleton defaultItem
                    | _ ->
                        this.MaterializeFlatRows(dfHandle, clientPredicates, None)

    /// Compiles pipeline to LazyFrameHandle. If client fallback, predicates or projections exist, materializes and transposes back.
    member this.CompileToLazyFrameHandle() : LazyFrameHandle =
        let nativeLf, clientPredicates, clientProjOpt, requiresClientFallback =
            this.GetCompiledPlan()

        if this.CanCollectNativeDirectly(nativeLf, clientPredicates, clientProjOpt, requiresClientFallback) then
            nativeLf
        else
            let filteredRows = this.ExecuteQuery()
            let newDfHandle = DataFrameBuilder.FromRows<'T> filteredRows
            PolarsWrapper.DataFrameToLazy newDfHandle

    /// Compiles the query and materializes directly into a native DataFrameHandle
    member this.CompileToDataFrameHandle() : DataFrameHandle =
        if (tryExtractChunkSize expression).IsSome then
            raise (
                InvalidOperationException(
                    "Cannot materialize a chunked sequence (IEnumerable<T[]>) into a single flat DataFrame. "
                    + "If you want to iterate over DataFrames in batches, use 'ToBatchesAsync(chunkSize)' or 'CollectBatchesStream'. "
                    + "If you want in-memory array chunks, enumerate the query directly via ToList() / foreach."
                )
            )

        let nativeLf, clientPredicates, clientProjOpt, requiresClientFallback =
            this.GetCompiledPlan()

        if this.CanCollectNativeDirectly(nativeLf, clientPredicates, clientProjOpt, requiresClientFallback) then
            let collectedDf = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

            if hasShuffle expression then
                applyShuffle collectedDf
            else
                collectedDf
        else
            let rows = this.ExecuteQuery()
            DataFrameBuilder.FromRows<'T>(rows)

    interface IInternalPlanSource with
        member this.GetPlanInfo() =
            let nativeLf, clientPreds, clientProjOpt, requiresClientFallback =
                this.GetCompiledPlan()

            nativeLf, requiresClientFallback || not clientPreds.IsEmpty, clientProjOpt

    interface IEnumerable<'T> with
        member this.GetEnumerator() : IEnumerator<'T> = (this.ExecuteQuery()).GetEnumerator()

    interface IEnumerable with
        member this.GetEnumerator() : IEnumerator =
            (this :> IEnumerable<'T>).GetEnumerator() :> IEnumerator

    interface IQueryable with
        member _.ElementType = targetType
        member _.Expression = expression
        member _.Provider = provider :> IQueryProvider

    interface IQueryable<'T>

    interface IOrderedQueryable<'T>

    interface IPolarsPlanSource with
        member this.GetCompiledLazyFrameHandle() = this.CompileToLazyFrameHandle()

    member this.GetPlanInspection(optimized: bool) : PlanInspection =
        let baseLf, ops, clientPreds, clientProjOpt, requiresClientFallback = this.CompilePipeline()
        let nativeLf = applyNativeOps baseLf ops
        let planStr = PolarsWrapper.Explain(nativeLf, optimized)
        let chunkSizeOpt = tryExtractChunkSize expression
        let shouldShuffle = hasShuffle expression
        let mutable postOps = []

        if shouldShuffle then
            postOps <-
                "[Eager Post-Processing]: Shuffle(fraction = 1.0, with_replacement = false)"
                :: postOps

        if isGrouping then
            postOps <-
                "[Client Post-Processing]: Materialize to IEnumerable<IGrouping<TKey, TElement>>"
                :: postOps

        match chunkSizeOpt with
        | Some size ->
            postOps <-
                sprintf "[Client Post-Processing]: Chunk(size = %d) -> Materialize as TSource[] batches" size
                :: postOps
        | None -> ()

        let planWithPostOps =
            if postOps.IsEmpty then
                planStr
            else
                planStr + "\n" + String.concat "\n" (List.rev postOps)

        {
            NativePlan = planWithPostOps
            HasClientPredicates = not clientPreds.IsEmpty
            ClientPredicateExpressions = clientPreds |> List.map snd
            HasClientProjection = clientProjOpt.IsSome || chunkSizeOpt.IsSome || shouldShuffle || isGrouping
            IsFullFallback = requiresClientFallback
            FallbackReason = FallbackTracker.Diagnostic
        }

    interface IPolarsExplainable with
        member this.InspectPlan(optimized: bool) = this.GetPlanInspection optimized

        member this.Explain(optimized: bool) =
            (this.GetPlanInspection optimized).FormatReport()

    interface IAsyncEnumerable<'T> with
        member this.GetAsyncEnumerator(cancellationToken: CancellationToken) : IAsyncEnumerator<'T> =
            cancellationToken.ThrowIfCancellationRequested()

            let nativeLf, clientPredicates, clientProjOpt, requiresClientFallback =
                this.GetCompiledPlan()

            let canStreamBatches =
                not requiresClientFallback
                && not (hasShuffle expression)
                && not isGrouping
                && (tryExtractDefaultIfEmpty expression).IsNone
                && (tryExtractChunkSize expression).IsNone
                && (tryExtractAggregateBy expression).IsNone

            if not canStreamBatches then
                let enumeratorRef = ref (None: IEnumerator<'T> option)

                { new IAsyncEnumerator<'T> with
                    member _.Current =
                        match enumeratorRef.Value with
                        | Some e -> e.Current
                        | None -> invalidOp "Enumeration has not started."

                    member _.MoveNextAsync() : ValueTask<bool> =
                        cancellationToken.ThrowIfCancellationRequested()

                        match enumeratorRef.Value with
                        | Some e -> ValueTask<bool>(e.MoveNext())
                        | None ->
                            ValueTask<bool>(
                                task {
                                    cancellationToken.ThrowIfCancellationRequested()
                                    let rows = this.ExecuteQuery()
                                    let e = rows.GetEnumerator()
                                    enumeratorRef.Value <- Some e
                                    return e.MoveNext()
                                }
                            )

                    member _.DisposeAsync() : ValueTask =
                        match enumeratorRef.Value with
                        | Some e -> e.Dispose()
                        | None -> ()

                        ValueTask()
                }
            else
                let batchStream =
                    PolarsWrapper.LazyCollectBatchesStreamAsync(
                        nativeLf,
                        PlEngine.Auto,
                        maintainOrder = false,
                        chunkSize = 0L,
                        cancellationToken = cancellationToken
                    )

                let batchEnumerator = batchStream.GetAsyncEnumerator cancellationToken
                let mutable currentBatchHandle: DataFrameHandle = null
                let mutable currentBatchRows: IEnumerator<'T> = null
                let mutable hasActiveRows = false
                let mutable isDisposed = false

                let materializeBatch (dfHandle: DataFrameHandle) : IEnumerable<'T> =
                    this.MaterializeFlatRows(dfHandle, clientPredicates, clientProjOpt)

                { new IAsyncEnumerator<'T> with
                    member _.Current =
                        if hasActiveRows && not (isNull currentBatchRows) then
                            currentBatchRows.Current
                        else
                            invalidOp "Enumeration has not started or is completed."

                    member _.MoveNextAsync() : ValueTask<bool> =
                        cancellationToken.ThrowIfCancellationRequested()

                        if hasActiveRows && currentBatchRows.MoveNext() then
                            ValueTask<bool>(true)
                        else
                            ValueTask<bool>(
                                task {
                                    cancellationToken.ThrowIfCancellationRequested()
                                    let mutable advanced = false
                                    let mutable doneStreaming = false

                                    while not (advanced || doneStreaming) do
                                        if hasActiveRows && currentBatchRows.MoveNext() then
                                            advanced <- true
                                        else
                                            if not (isNull currentBatchHandle) then
                                                currentBatchHandle.Dispose()
                                                currentBatchHandle <- null
                                                currentBatchRows <- null
                                                hasActiveRows <- false

                                            let! hasNextBatch = batchEnumerator.MoveNextAsync()

                                            if hasNextBatch then
                                                let dfHandle = batchEnumerator.Current
                                                let h = PolarsWrapper.DataFrameHeight dfHandle

                                                if h > 0L then
                                                    currentBatchHandle <- dfHandle
                                                    let rows = materializeBatch dfHandle
                                                    currentBatchRows <- rows.GetEnumerator()
                                                    hasActiveRows <- true
                                                else
                                                    dfHandle.Dispose()
                                            else
                                                doneStreaming <- true

                                    return advanced
                                }
                            )

                    member _.DisposeAsync() : ValueTask =
                        ValueTask(
                            task {
                                if not isDisposed then
                                    isDisposed <- true

                                    if not (isNull currentBatchHandle) then
                                        currentBatchHandle.Dispose()
                                        currentBatchHandle <- null
                                        currentBatchRows <- null
                                        hasActiveRows <- false

                                    do! batchEnumerator.DisposeAsync()
                            }
                        )
                }
