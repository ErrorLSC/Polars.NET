namespace Polars.NET.Linq

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open Polars.NET.Core
open Polars.NET.Core.Helpers
open System.Threading
open System.Threading.Tasks

/// Strongly-typed IQueryProvider backed by Polars.NET.Core
type internal PolarsQueryProvider(initialLazyFrame: LazyFrameHandle, materializer: IDataFrameMaterializer) =
    let clonedLf = PolarsWrapper.LazyClone initialLazyFrame

    member _.RootLazyFrame = clonedLf
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
                let query = (this :> IQueryProvider).CreateQuery(expression)

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
    let lfCloned = provider.RootLazyFrame
    let materializer = provider.Materializer

    let expression =
        match exprOpt with
        | Some e -> e
        | None -> Expression.Constant(this, typeof<IQueryable<'T>>) :> Expression

    new(lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer, expression: Expression) =
        PolarsQuery<'T>(PolarsQueryProvider(lazyFrameHandle, materializer), Some expression)

    new(lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer) =
        PolarsQuery<'T>(PolarsQueryProvider(lazyFrameHandle, materializer), None)

    /// Parses the LINQ expression tree into native ops, client predicates, optional projection, and fallback signal
    member private _.CompilePipeline
        ()
        : QueryOp list * ((Func<obj, bool> * LambdaExpression) list) * ((Type * Func<obj, obj>) option) * bool =
        let isCSharp = not (isNull materializer) && materializer.GetType().Name.StartsWith "CSharp"
        let rewrittenExpr =
            if not isCSharp && FSharpAst.needsRewrite expression then
                FSharpAst.rewrite expression
            else
                expression
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

        let isGrouping =
            typeof<'T>.IsGenericType
            && typeof<'T>.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

        let rec fuse
            (remaining: LinqStage list)
            (opsAcc: QueryOp list)
            (clientPreds: (Func<obj, bool> * LambdaExpression) list)
            (clientProjOpt: (Type * Func<obj, obj>) option)
            =
            let tryDispatchGroupBy
                (ctx: GroupFoldContext)
                (projBody: Expression)
                (groupParamName: string)
                (isKeyArg: Expression -> bool)
                (nextTail: LinqStage list)
                =
                if not clientPreds.IsEmpty then
                    [], [], None, true
                else
                    match compileUnifiedGroupBy lfCloned ctx projBody groupParamName isKeyArg with
                    | Some compiledOps -> fuse nextTail (compiledOps @ opsAcc) clientPreds clientProjOpt
                    | None ->
                        if isGrouping then
                            fuse nextTail opsAcc clientPreds clientProjOpt
                        else
                            [], [], None, true

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
                            | _ -> false

                        tryDispatchGroupBy ctx projLambda.Body projLambda.Parameters.[0].Name isKeyArg next
                    | [] when isGrouping ->
                        let clientHavingPreds =
                            ctx.HavingPreds
                            |> List.map (fun predLambda -> compileClientPredicate predLambda, predLambda)

                        fuse [] opsAcc (clientHavingPreds @ clientPreds) clientProjOpt
                    | _ -> [], [], None, true

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

                let resKeyParam = resLambda.Parameters.[0]

                let isKeyArg (e: Expression) =
                    match e with
                    | :? ParameterExpression as p when p.Name = resKeyParam.Name -> true
                    | MemberAccess(:? ParameterExpression as p, _) when p.Name = resKeyParam.Name -> true
                    | _ -> false

                tryDispatchGroupBy ctx resLambda.Body resLambda.Parameters.[1].Name isKeyArg nextTail

            match remaining with
            // 1. Universal Pipeline GroupBy (F# QueryBuilder / chained syntax)
            | LinqStage.GroupByKey keyLambda :: tail ->
                slurpPipelineGroupBy
                    tail
                    {
                        KeyLambda = keyLambda
                        ElemLambdaOpt = None
                        HavingPreds = []
                        SortStages = []
                    }

            // 2. C# 3-arg GroupBy: (keySelector, resultSelector) OR (keySelector, elementSelector)
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

            // 3. C# 4-arg GroupBy (keySelector, elementSelector, resultSelector)
            | LinqStage.GroupByWithElementAndResult(keyLambda, elemLambda, resLambda) :: tail ->
                dispatchCSharpGroupBy keyLambda (Some elemLambda) resLambda tail

            | LinqStage.Filter l :: tail ->
                match SubqueryDecorrelator.tryDecorrelateFilter lfCloned l with
                | Some decorResult when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                    let newOpsAcc = (decorResult.GeneratedOps |> List.rev) @ opsAcc

                    match decorResult.ResidualLambda with
                    | Some residualLam ->
                        fuse (LinqStage.Filter residualLam :: tail) newOpsAcc clientPreds clientProjOpt
                    | None -> fuse tail newOpsAcc clientPreds clientProjOpt
                | _ ->
                    let p = l.Parameters.[0]
                    let ctx = createContextForType p.Type lfCloned p.Name

                    match ExprTranslator.tryTranslateWithContext ctx l.Body with
                    | Some exprHandle when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                        fuse tail (QueryOp.Filter exprHandle :: opsAcc) clientPreds clientProjOpt
                    | _ ->
                        let rec tryTranslateTupleFilter (e: Expression) =
                            let isNullConstant (expr: Expression) =
                                match ExprTranslator.unwrap expr with
                                | :? ConstantExpression as c -> isNull c.Value
                                | _ -> false

                            match e with
                            | Binary(op, left, right) when op = ExpressionType.NotEqual && isNullConstant right ->
                                tryTranslateTupleFilter left |> Option.map PolarsWrapper.IsNotNull
                            | Binary(op, left, right) when op = ExpressionType.NotEqual && isNullConstant left ->
                                tryTranslateTupleFilter right |> Option.map PolarsWrapper.IsNotNull
                            | Binary(op, left, right) when op = ExpressionType.Equal && isNullConstant right ->
                                tryTranslateTupleFilter left |> Option.map PolarsWrapper.IsNull
                            | Binary(op, left, right) when op = ExpressionType.Equal && isNullConstant left ->
                                tryTranslateTupleFilter right |> Option.map PolarsWrapper.IsNull
                            | Binary(op, left, right) ->
                                match tryTranslateTupleFilter left, tryTranslateTupleFilter right with
                                | Some l, Some r ->
                                    try
                                        Some(ExprTranslator.translateBinary op l r)
                                    with _ ->
                                        None
                                | _ -> None
                            | MemberAccess _ as ma ->
                                ExprTranslator.tryResolveColumnNameWithContext ctx ma
                                |> Option.map PolarsWrapper.Col
                            | Constant _ -> ExprTranslator.tryTranslateWithContext TranslationContext.Empty e
                            | _ -> None

                        match tryTranslateTupleFilter l.Body with
                        | Some exprHandle when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                            fuse tail (QueryOp.Filter exprHandle :: opsAcc) clientPreds clientProjOpt
                        | _ ->
                            let paramType = l.Parameters.[0].Type
                            if
                                paramType.Name.StartsWith "AnonymousObject"
                                || paramType.Name.Contains "TransparentIdentifier"
                                || clientProjOpt.IsSome
                            then
                                FallbackTracker.Diagnostic <- Some (sprintf "Filter on transparent identifier %s: %s\nRewritten body: %s" paramType.Name (l.ToString()) (bound.ToString()))
                                [], [], None, true
                            else
                                let safePred = compileClientPredicate l
                                fuse tail opsAcc ((safePred, l) :: clientPreds) clientProjOpt

            | LinqStage.Index :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                let indexOp = QueryOp.WithRowIndex("Index", Some 0u)

                let int32Dtype =
                    PolarsWrapper.DataTypeExprFromDataType(PolarsWrapper.NewPrimitiveType(int PlDataType.Int32))

                let castIndexExpr =
                    PolarsWrapper.Col "Index"
                    |> fun c -> PolarsWrapper.ExprCast(c, int32Dtype, strict = false, wrapNumerical = false)

                let withCastOp = QueryOp.WithColumns [| castIndexExpr |]
                fuse tail (withCastOp :: indexOp :: opsAcc) clientPreds clientProjOpt

            | LinqStage.SortDefault isDesc :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                let colNames = PolarsSchemaExtractor.GetColumnNames lfCloned
                let targetCol = if colNames.Length > 0 then colNames.[0] else ""
                let colExpr = PolarsWrapper.Col targetCol

                let spec =
                    {
                        Expr = colExpr
                        Descending = isDesc
                        NullsLast = false
                    }

                fuse tail (addSort spec opsAcc) clientPreds clientProjOpt

            | LinqStage.Sort(l, isDesc) :: tail ->
                let param = l.Parameters.[0]

                let isParamGrouping =
                    param.Type.IsGenericType
                    && param.Type.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

                if not isParamGrouping && clientPreds.IsEmpty && clientProjOpt.IsNone then
                    let ctx = createContextForType param.Type lfCloned param.Name

                    let sortExprOpt =
                        match ExprTranslator.tryTranslateWithContext ctx l.Body with
                        | Some h -> Some h
                        | None -> tryTranslateSelectArg ctx l.Body

                    match sortExprOpt with
                    | Some exprHandle ->
                        let spec =
                            {
                                Expr = exprHandle
                                Descending = isDesc
                                NullsLast = false
                            }

                        fuse tail (addSort spec opsAcc) clientPreds clientProjOpt
                    | None -> fuse tail opsAcc clientPreds clientProjOpt
                else
                    fuse tail opsAcc clientPreds clientProjOpt

            | LinqStage.Join(m, inner, outerKey, innerKey, resSelOpt) :: tail when
                clientPreds.IsEmpty && clientProjOpt.IsNone
                ->
                match compileJoin lfCloned m inner outerKey innerKey with
                | Some spec ->
                    match resSelOpt with
                    | None ->
                        fuse tail (QueryOp.Join spec :: opsAcc) clientPreds clientProjOpt
                    | Some resSel ->
                        let isTransparent =
                            match tryExtractMemberBindings resSel.Body with
                            | Some bindings when resSel.Parameters.Count >= 2 ->
                                let p1, p2 = resSel.Parameters.[0].Name, resSel.Parameters.[1].Name

                                let rec isTransparentArg (e: Expression) =
                                    match ExprTranslator.unwrap e with
                                    | :? ParameterExpression as p -> p.Name = p1 || p.Name = p2
                                    | MemberAccess(target, m) when isNavigationMember m ->
                                        isTransparentArg target
                                    | :? NewExpression as ne when isTupleOrAnonType ne.Type ->
                                        ne.Arguments |> Seq.forall isTransparentArg
                                    | _ -> false

                                bindings |> List.forall (fun (_, expr) -> isTransparentArg expr)
                            | _ -> false

                        if isTransparent then
                            fuse tail (QueryOp.Join spec :: opsAcc) clientPreds clientProjOpt
                        else
                            let leftCols = getLazySchemaFields lfCloned |> Array.map fst
                            let rightCols = getLazySchemaFields spec.RightLf |> Array.map fst

                            match compileJoinResultSelector leftCols rightCols spec resSel with
                            | Some selectOp ->
                                fuse tail (selectOp :: QueryOp.Join spec :: opsAcc) clientPreds clientProjOpt
                            | None ->
                                FallbackTracker.Diagnostic <- Some (sprintf "CompileJoinResultSelector failed: %s\nLeftCols: %A\nRightCols: %A" (resSel.ToString()) leftCols rightCols)
                                [], [], None, true
                | None ->
                    FallbackTracker.Diagnostic <- Some (sprintf "CompileJoin failed for inner: %s, outerKey: %s, innerKey: %s" (inner.ToString()) (outerKey.ToString()) (innerKey.ToString()))
                    [], [], None, true

            | LinqStage.GroupJoin(_, inner, outerKey, innerKey, _) :: LinqStage.Explode(collSelector, _) :: tail when
                collSelector.ToString().Contains "DefaultIfEmpty"
                && clientPreds.IsEmpty
                && clientProjOpt.IsNone
                ->

                match SubqueryDecorrelator.resolveSequenceLazyFrame inner with
                | Some rightLf ->
                    let outerParam = outerKey.Parameters.[0]
                    let innerParam = innerKey.Parameters.[0]

                    let leftCtx = createContextForType outerParam.Type lfCloned outerParam.Name
                    let rightCtx = createContextForType innerParam.Type rightLf innerParam.Name

                    match
                        ExprTranslator.tryTranslateWithContext leftCtx outerKey.Body,
                        ExprTranslator.tryTranslateWithContext rightCtx innerKey.Body
                    with
                    | Some leftKey, Some rightKey ->
                        let joinSpec =
                            {
                                RightLf = rightLf
                                LeftOn = [| leftKey |]
                                RightOn = [| rightKey |]
                                How = PlJoinType.Left
                                Suffix = None
                                InvertSides = false
                                Coalesce = PlJoinCoalesce.KeepColumns
                            }

                        fuse tail (QueryOp.Join joinSpec :: opsAcc) clientPreds clientProjOpt
                    | _ -> [], [], None, true
                | None -> [], [], None, true

            | LinqStage.SetOp(m, second, k1Opt, k2Opt) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match compileSetOp m second k1Opt k2Opt lfCloned with
                | Some spec -> fuse tail (QueryOp.Join spec :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.Skip s :: LinqStage.Take t :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail (QueryOp.Slice(int64 s, t) :: opsAcc) clientPreds clientProjOpt

            | LinqStage.Take count :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail (QueryOp.Slice(0L, count) :: opsAcc) clientPreds clientProjOpt

            | LinqStage.Skip count :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail (QueryOp.Slice(int64 count, UInt32.MaxValue) :: opsAcc) clientPreds clientProjOpt

            | LinqStage.Distinct :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                let spec =
                    {
                        SubsetCols = None
                        Keep = PlUniqueKeepStrategy.First
                        MaintainOrder = false
                    }

                fuse tail (QueryOp.Unique spec :: opsAcc) clientPreds clientProjOpt

            | LinqStage.DistinctBy keyLambda :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match extractDistinctColumns keyLambda with
                | Some cols ->
                    let spec =
                        {
                            SubsetCols = Some cols
                            Keep = PlUniqueKeepStrategy.First
                            MaintainOrder = false
                        }

                    fuse tail (QueryOp.Unique spec :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.Concat otherExpr :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match SubqueryDecorrelator.resolveSequenceLazyFrame otherExpr with
                | Some otherCompiledLf ->
                    let spec =
                        {
                            OtherLf = otherCompiledLf
                            Prepend = false
                        }

                    fuse tail (QueryOp.Concat spec :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.Zip(secondExpr, resLambdaOpt) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match SubqueryDecorrelator.resolveSequenceLazyFrame secondExpr with
                | Some otherLf ->
                    let baseCols = PolarsSchemaExtractor.GetColumnNames lfCloned |> Set.ofArray

                    let safeSecondLf, _, suffixMap =
                        disambiguateColumns baseCols otherLf "_second"

                    let zipOp = QueryOp.HorizontalConcat [| safeSecondLf |]

                    let renameOps =
                        match resLambdaOpt with
                        | Some resLambda ->
                            let renames = extractBinaryResultRenames resLambda None

                            if not renames.IsEmpty then
                                let mappedRenames =
                                    renames
                                    |> List.map (fun (srcCol, destCol) ->
                                        match suffixMap.TryFind srcCol with
                                        | Some actualColName -> actualColName, destCol
                                        | None -> srcCol, destCol)

                                [
                                    QueryOp.Rename
                                        {
                                            ExistingNames = mappedRenames |> List.map fst |> List.toArray
                                            NewNames = mappedRenames |> List.map snd |> List.toArray
                                        }
                                ]
                            else
                                []
                        | None -> []

                    fuse tail (renameOps @ zipOp :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.Zip3(secondExpr, thirdExpr) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match
                    SubqueryDecorrelator.resolveSequenceLazyFrame secondExpr,
                    SubqueryDecorrelator.resolveSequenceLazyFrame thirdExpr
                with
                | Some secondLf, Some thirdLf ->
                    let baseCols = PolarsSchemaExtractor.GetColumnNames lfCloned |> Set.ofArray

                    let safeSecondLf, colsAfter2, _ =
                        disambiguateColumns baseCols secondLf "_second"

                    let safeThirdLf, _, _ =
                        disambiguateColumns colsAfter2 thirdLf "_third"

                    let zip3Op =
                        QueryOp.HorizontalConcat [| safeSecondLf; safeThirdLf |]

                    fuse tail (zip3Op :: opsAcc) clientPreds clientProjOpt
                | _ -> [], [], None, true

            | LinqStage.Union(secondExpr, keyLambdaOpt) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match SubqueryDecorrelator.resolveSequenceLazyFrame secondExpr with
                | Some otherLf ->
                    let concatOp = QueryOp.Concat { OtherLf = otherLf; Prepend = false }

                    let subsetCols =
                        keyLambdaOpt |> Option.bind extractDistinctColumns

                    let uniqueSpec =
                        {
                            SubsetCols = subsetCols
                            Keep = PlUniqueKeepStrategy.First
                            MaintainOrder = false
                        }

                    fuse tail (QueryOp.Unique uniqueSpec :: concatOp :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.Explode(colLambda, resLambdaOpt) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match extractExplodeColumn colLambda with
                | Some colName ->
                    let explodeOp =
                        QueryOp.Explode
                            {
                                ColumnNames = [| colName |]
                                EmptyAsNull = true
                                KeepNulls = true
                            }

                    let renameOps =
                        match resLambdaOpt with
                        | Some resLambda when resLambda.Parameters.Count >= 2 ->
                            let itemParam = resLambda.Parameters.[1]

                            let renames =
                                extractBinaryResultRenames resLambda (Some(itemParam.Name, colName))

                            if not renames.IsEmpty then
                                [
                                    QueryOp.Rename
                                        {
                                            ExistingNames = renames |> List.map fst |> List.toArray
                                            NewNames = renames |> List.map snd |> List.toArray
                                        }
                                ]
                            else
                                []
                        | _ -> []

                    fuse tail (renameOps @ explodeOp :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.CrossJoin(innerExpr, resLambdaOpt) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match SubqueryDecorrelator.resolveSequenceLazyFrame innerExpr with
                | Some rightLf ->
                    let joinSpec =
                        {
                            RightLf = rightLf
                            LeftOn = [||]
                            RightOn = [||]
                            How = PlJoinType.Cross
                            Suffix = Some "_right"
                            InvertSides = false
                            Coalesce = PlJoinCoalesce.KeepColumns
                        }

                    let joinOp = QueryOp.Join joinSpec

                    let renameOps =
                        match resLambdaOpt with
                        | Some resLambda ->
                            let renames = extractBinaryResultRenames resLambda None

                            if not renames.IsEmpty then
                                [
                                    QueryOp.Rename
                                        {
                                            ExistingNames = renames |> List.map fst |> List.toArray
                                            NewNames = renames |> List.map snd |> List.toArray
                                        }
                                ]
                            else
                                []
                        | None -> []

                    fuse tail (renameOps @ joinOp :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.TakeWhile l :: tail ->
                let param = l.Parameters.[0]
                let ctx = createContextForType typeof<'T> lfCloned param.Name

                match ExprTranslator.tryTranslateWithContext ctx l.Body with
                | Some boolExpr when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                    let cumMinExpr = PolarsWrapper.CumMin(boolExpr, false)
                    fuse tail (QueryOp.Filter cumMinExpr :: opsAcc) clientPreds clientProjOpt
                | _ ->
                    let safePred = compileClientPredicate l
                    fuse tail opsAcc ((safePred, l) :: clientPreds) clientProjOpt

            | LinqStage.SkipWhile l :: tail ->
                let param = l.Parameters.[0]
                let ctx = createContextForType typeof<'T> lfCloned param.Name

                match ExprTranslator.tryTranslateWithContext ctx l.Body with
                | Some boolExpr when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                    let notExpr = PolarsWrapper.Not boolExpr
                    let cumMaxExpr = PolarsWrapper.CumMax(notExpr, false)
                    fuse tail (QueryOp.Filter cumMaxExpr :: opsAcc) clientPreds clientProjOpt
                | _ ->
                    let safePred = compileClientPredicate l
                    fuse tail opsAcc ((safePred, l) :: clientPreds) clientProjOpt

            | LinqStage.GroupJoin(m, innerExpr, outerKey, innerKey, None) :: LinqStage.Project projLambda :: tail when
                clientPreds.IsEmpty && clientProjOpt.IsNone
                ->
                let outerType = outerKey.Parameters.[0].Type
                let innerType = innerKey.Parameters.[0].Type

                let desugaredResSel =
                    desugarGroupJoinProject projLambda outerType innerType

                fuse
                    (LinqStage.GroupJoin(m, innerExpr, outerKey, innerKey, Some desugaredResSel)
                     :: tail)
                    opsAcc
                    clientPreds
                    clientProjOpt

            | LinqStage.GroupJoin(_, innerExpr, outerKey, innerKey, Some resSel) :: tail when
                clientPreds.IsEmpty && clientProjOpt.IsNone
                ->
                match SubqueryDecorrelator.resolveSequenceLazyFrame innerExpr with
                | Some rightLf ->
                    let outerParam = outerKey.Parameters.[0]
                    let innerParam = innerKey.Parameters.[0]

                    let leftCtx = createContextForType outerParam.Type lfCloned outerParam.Name
                    let rightCtx = createContextForType innerParam.Type rightLf innerParam.Name

                    match
                        ExprTranslator.tryTranslateWithContext leftCtx outerKey.Body,
                        ExprTranslator.tryTranslateWithContext rightCtx innerKey.Body
                    with
                    | Some leftKey, Some rightKey ->
                        let groupParam = resSel.Parameters.[1]

                        let preAggRightLf, aggColOpt =
                            match tryExtractGroupAgg groupParam.Name resSel with
                            | Some(aggColName, aggExpr) ->
                                let aggKeyClone = PolarsWrapper.CloneExpr rightKey

                                let aggLf =
                                    PolarsWrapper.LazyGroupByAgg(
                                        rightLf,
                                        [| aggKeyClone |],
                                        [| aggExpr |],
                                        null,
                                        false
                                    )

                                aggLf, Some aggColName
                            | None -> rightLf, None

                        let leftKeyForJoin = PolarsWrapper.CloneExpr leftKey
                        let rightKeyForJoin = PolarsWrapper.CloneExpr rightKey

                        let joinSpec =
                            {
                                RightLf = preAggRightLf
                                LeftOn = [| leftKeyForJoin |]
                                RightOn = [| rightKeyForJoin |]
                                How = PlJoinType.Left
                                Suffix = Some "_right"
                                InvertSides = false
                                Coalesce = PlJoinCoalesce.JoinSpecific
                            }

                        let joinOp = QueryOp.Join joinSpec

                        let renames = extractBinaryResultRenames resSel None

                        let renameOps =
                            if not renames.IsEmpty then
                                [
                                    QueryOp.Rename
                                        {
                                            ExistingNames = renames |> List.map fst |> List.toArray
                                            NewNames = renames |> List.map snd |> List.toArray
                                        }
                                ]
                            else
                                []

                        let fillOps =
                            match aggColOpt with
                            | Some aggCol ->
                                let colExpr = PolarsWrapper.Col aggCol
                                let zeroLit = PolarsWrapper.Lit 0u
                                let filledExpr = PolarsWrapper.FillNull(colExpr, zeroLit)
                                let aliased = PolarsWrapper.Alias(filledExpr, aggCol)
                                [ QueryOp.WithColumns [| aliased |] ]
                            | None -> []

                        let nextOps = renameOps @ fillOps @ joinOp :: opsAcc
                        fuse tail nextOps clientPreds clientProjOpt
                    | _ -> [], [], None, true
                | None -> [], [], None, true

            | LinqStage.GroupJoin(m, innerExpr, outerKey, innerKey, None) :: tail when
                clientPreds.IsEmpty && clientProjOpt.IsNone
                ->
                match compileJoin lfCloned m innerExpr outerKey innerKey with
                | Some spec -> fuse tail (QueryOp.Join spec :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.TakeLast count :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                let sliceOp = QueryOp.Slice(-int64 count, count)
                fuse tail (sliceOp :: opsAcc) clientPreds clientProjOpt

            | LinqStage.SkipLast count :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail (QueryOp.SkipLast count :: opsAcc) clientPreds clientProjOpt

            | (LinqStage.Append elemExpr | LinqStage.Prepend elemExpr) as stage :: tail when
                clientPreds.IsEmpty && clientProjOpt.IsNone
                ->
                let isPrepend =
                    match stage with
                    | LinqStage.Prepend _ -> true
                    | _ -> false

                match tryCompileAppendOrPrepend elemExpr isPrepend with
                | Some concatOp -> fuse tail (concatOp :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.Reverse :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail (QueryOp.Reverse :: opsAcc) clientPreds clientProjOpt

            | LinqStage.DefaultIfEmpty _ :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail opsAcc clientPreds clientProjOpt

            | LinqStage.Project projLambda :: tail ->
                let decorrelatedOpt =
                    if clientPreds.IsEmpty && clientProjOpt.IsNone then
                        SubqueryDecorrelator.tryDecorrelateProjection lfCloned projLambda
                    else
                        None

                match decorrelatedOpt with
                | Some(subqueryOps, rewrittenProjLambda) ->
                    let newOpsAcc = (subqueryOps |> List.rev) @ opsAcc
                    fuse (LinqStage.Project rewrittenProjLambda :: tail) newOpsAcc clientPreds None
                | None ->
                    match tryCompileTransparentLet lfCloned projLambda with
                    | Some withColOp when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                        fuse tail (withColOp :: opsAcc) clientPreds None
                    | _ ->
                        match compileSelectProjection lfCloned projLambda with
                        | Some exprs when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                            fuse tail (QueryOp.Select exprs :: opsAcc) clientPreds None

                        | _ ->
                            let sourceType = projLambda.Parameters.[0].Type
                            let currentFunc = compileToBoxedFunc projLambda

                            let nextProj =
                                match clientProjOpt with
                                | None ->
                                    Some(sourceType, currentFunc)
                                | Some(prevSrcType, prevFunc) ->
                                    let composed =
                                        Func<obj, obj>(fun rawItem ->
                                            let mid = prevFunc.Invoke(rawItem)
                                            currentFunc.Invoke(mid))

                                    Some(prevSrcType, composed)

                            fuse tail (QueryOp.SelectPassthrough :: opsAcc) clientPreds nextProj

            | LinqStage.Cast _ :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail opsAcc clientPreds clientProjOpt

            | LinqStage.OfType(sourceType, targetType) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                if targetType.IsAssignableFrom(sourceType) then
                    fuse tail opsAcc clientPreds clientProjOpt
                elif not (sourceType.IsAssignableFrom(targetType)) then
                    let falseFilter = PolarsWrapper.Lit false
                    let filterOp = QueryOp.Filter falseFilter
                    fuse tail (filterOp :: opsAcc) clientPreds clientProjOpt
                else
                    fuse tail opsAcc clientPreds clientProjOpt

            | LinqStage.CountBy keyLambda :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                let keyParam = keyLambda.Parameters.[0]
                let ctx = createContextForType typeof<'T> lfCloned keyParam.Name

                match ExprTranslator.tryTranslateWithContext ctx keyLambda.Body with
                | Some keyExpr ->
                    let keyAliased = PolarsWrapper.Alias(keyExpr, "Key")
                    let lenExpr = PolarsWrapper.Len()
                    let valueAliased = PolarsWrapper.Alias(lenExpr, "Value")

                    let spec =
                        {
                            Keys = [| keyAliased |]
                            Aggs = [| valueAliased |]
                            Having = None
                            MaintainOrder = false
                        }

                    fuse tail (QueryOp.GroupBy spec :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.AggregateBy _ :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail opsAcc clientPreds clientProjOpt

            | [] -> List.rev opsAcc, List.rev clientPreds, clientProjOpt, false

            | _ :: tail -> fuse tail opsAcc clientPreds clientProjOpt

        let ops, clientPreds, clientProjOpt, requiresClientFallback = fuse stages [] [] None
        if requiresClientFallback && FallbackTracker.Diagnostic.IsNone then
            FallbackTracker.Diagnostic <- Some (sprintf "Stages failed to fuse. stages count: %d\nbound: %s\nstages: %A" stages.Length (bound.ToString()) stages)
        ops, clientPreds, clientProjOpt, requiresClientFallback

    /// Compiles pipeline and builds the target native LazyFrameHandle alongside client predicates, projection, and fallback signal
    member private this.GetCompiledPlan
        ()
        : LazyFrameHandle * (Func<obj, bool> list) * ((Type * Func<obj, obj>) option) * bool =
        let ops, clientPredsWithAst, clientProjOpt, requiresClientFallback =
            this.CompilePipeline()

        enforcePipelinePolicy
            "QueryExecution"
            requiresClientFallback
            clientPredsWithAst
            (expression.ToString())

        let nativeLf = applyNativeOps lfCloned ops
        let clientPredicates = clientPredsWithAst |> List.map fst
        nativeLf, clientPredicates, clientProjOpt, requiresClientFallback

    /// Executes the full LINQ expression pipeline in-memory via .NET EnumerableQuery LINQ-to-Objects engine
    member private _.ExecuteInMemory() : IEnumerable<'T> =
        let isCSharp = not (isNull materializer) && materializer.GetType().Name.StartsWith("CSharp")
        let effectiveExpr =
            if not isCSharp && FSharpAst.needsRewrite expression then
                FSharpAst.rewrite expression
            else
                expression

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
            let targetType = typeof<'T>

            let isGrouping =
                targetType.IsGenericType
                && targetType.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

            let shouldShuffle = hasShuffle expression

            let dfHandle =
                if shouldShuffle then
                    applyShuffle collectedDf
                else
                    collectedDf

            match clientProjOpt with
            | Some(sourceType, func) ->
                let mat = QueryMaterializerResolver.Resolve(Some materializer)
                let invoker = LinqReflectionCache.GetMaterializerInvoker sourceType
                let rawSourceRows = invoker.Invoke(mat, dfHandle)

                let filteredSource =
                    if clientPredicates.IsEmpty then
                        rawSourceRows
                    else
                        seq {
                            for item in rawSourceRows do
                                if clientPredicates |> List.forall (fun p -> p.Invoke item) then
                                    yield item
                        }

                seq {
                    for item in filteredSource do
                        yield func.Invoke item :?> 'T
                }

            | None ->
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
                    let mat = QueryMaterializerResolver.Resolve(Some materializer)

                    match tryExtractGroupByInfo expression with
                    | Some(keyLambda, elemLambdaOpt) ->
                        let compiledKeySelector = keyLambda.Compile()

                        let grouped =
                            match elemLambdaOpt with
                            | Some elemLambda ->
                                let sourceType = elemLambda.Parameters.[0].Type
                                let invoker = LinqReflectionCache.GetMaterializerInvoker sourceType
                                let rawRows = invoker.Invoke(mat, dfHandle)
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
                                let rawRows = invoker.Invoke(mat, dfHandle)

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

                        match tryExtractSortSelector expression with
                        | Some(sortLambda, false) ->
                            let compiled = sortLambda.Compile()

                            filtered
                            |> Seq.sortBy (fun g -> compiled.DynamicInvoke([| box g |]) :?> IComparable)
                        | Some(sortLambda, true) ->
                            let compiled = sortLambda.Compile()

                            filtered
                            |> Seq.sortByDescending (fun g -> compiled.DynamicInvoke([| box g |]) :?> IComparable)
                        | None -> filtered

                    | None when hasGroupJoin expression ->
                        let innerNullableType =
                            if elemType.IsValueType then
                                typedefof<Nullable<_>>.MakeGenericType elemType
                            else
                                elemType

                        let pairType =
                            typedefof<ValueTuple<_, _>>.MakeGenericType(keyType, innerNullableType)

                        let invoker = LinqReflectionCache.GetMaterializerInvoker pairType
                        let rawPairs = invoker.Invoke(mat, dfHandle)

                        let keyOrder = List<obj>()
                        let groupMap = Dictionary<obj, List<obj>>()

                        for pairObj in rawPairs do
                            let tuple = pairObj :?> System.Runtime.CompilerServices.ITuple
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
                            typedefof<PolarsGrouping<_, _>>.MakeGenericType(keyType, elemType)

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

                        match tryExtractSortSelector expression with
                        | Some(sortLambda, false) ->
                            let compiled = sortLambda.Compile()

                            resultList
                            |> Seq.sortBy (fun g -> compiled.DynamicInvoke [| box g |] :?> IComparable)
                        | Some(sortLambda, true) ->
                            let compiled = sortLambda.Compile()

                            resultList
                            |> Seq.sortByDescending (fun g -> compiled.DynamicInvoke [| box g |] :?> IComparable)
                        | None -> resultList :> IEnumerable<'T>

                    | None -> failwith "Could not locate keySelector for GroupBy pipeline."

                // Case 2: Chunk slicing via stack-based cursor
                elif chunkSizeOpt.IsSome then
                    let chunkSize = chunkSizeOpt.Value
                    let elemType = targetType.GetElementType()

                    let mat = QueryMaterializerResolver.Resolve(Some materializer)
                    let invoker = LinqReflectionCache.GetMaterializerChunkInvoker elemType
                    let chunkedSeq = invoker.Invoke(mat, dfHandle, chunkSize)
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

                    let mat = QueryMaterializerResolver.Resolve(Some materializer)
                    let sourceQuery = (provider :> IQueryProvider).CreateQuery(sourceExpr)

                    let sourceLf =
                        match box sourceQuery with
                        | :? IPolarsPlanSource as ps -> ps.GetCompiledLazyFrameHandle()
                        | _ -> nativeLf

                    let sourceDfHandle = PolarsWrapper.LazyCollect(sourceLf, PlEngine.Auto)

                    match seedLambdaOpt with
                    | Some seedLambda ->
                        let compiledSeed = seedLambda.Compile()

                        let invoker =
                            LinqReflectionCache.GetMaterializerAggByFactoryInvoker(sourceElemType, keyType, accumType)

                        let res =
                            invoker.Invoke(mat, sourceDfHandle, compiledKey, compiledSeed, compiledFunc, comparerObj)

                        res.Cast<'T>()

                    | None ->
                        let seedVal =
                            match tryEvaluate seedExpr with
                            | Some s -> s
                            | None -> failwith "Failed to evaluate static seed for AggregateBy."

                        let invoker =
                            LinqReflectionCache.GetMaterializerAggBySeedInvoker(sourceElemType, keyType, accumType)

                        let res =
                            invoker.Invoke(mat, sourceDfHandle, compiledKey, seedVal, compiledFunc, comparerObj)

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
                        let mat = QueryMaterializerResolver.Resolve(Some materializer)
                        let rows = mat.Materialize<'T>(dfHandle)

                        if clientPredicates.IsEmpty then
                            rows
                        else
                            rows
                            |> Seq.filter (fun item -> clientPredicates |> List.forall (fun p -> p.Invoke(box item)))

    /// Compiles pipeline to LazyFrameHandle. If client fallback, predicates or projections exist, materializes and transposes back.
    member this.CompileToLazyFrameHandle() : LazyFrameHandle =
        let nativeLf, clientPredicates, clientProjOpt, requiresClientFallback =
            this.GetCompiledPlan()

        let schema = PolarsWrapper.GetLazySchema nativeLf
        let width = PolarsWrapper.GetSchemaLen schema
        let targetType = typeof<'T>

        let isScalar =
            PolarsTypeHelper.IsScalarType targetType || targetType = typeof<string>

        let isGrouping =
            targetType.IsGenericType
            && targetType.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

        let requiresClientEvaluation =
            requiresClientFallback
            || not clientPredicates.IsEmpty
            || clientProjOpt.IsSome
            || isScalar && width > 1UL
            || isGrouping

        if not requiresClientEvaluation then
            nativeLf
        else
            let filteredRows = this.ExecuteQuery()
            let newDfHandle = DataFrameBuilder.FromRows<'T>(filteredRows)
            PolarsWrapper.DataFrameToLazy newDfHandle

    /// Compiles the query and materializes directly into a native DataFrameHandle
    member this.CompileToDataFrameHandle() : DataFrameHandle =
        let chunkSizeOpt = tryExtractChunkSize expression

        if chunkSizeOpt.IsSome then
            raise (
                InvalidOperationException(
                    "Cannot materialize a chunked sequence (IEnumerable<T[]>) into a single flat DataFrame. "
                    + "If you want to iterate over DataFrames in batches, use 'ToBatchesAsync(chunkSize)' or 'CollectBatchesStream'. "
                    + "If you want in-memory array chunks, enumerate the query directly via ToList() / foreach."
                )
            )

        let nativeLf, clientPredicates, clientProjOpt, requiresClientFallback =
            this.GetCompiledPlan()

        let schema = PolarsWrapper.GetLazySchema nativeLf
        let width = PolarsWrapper.GetSchemaLen schema

        let isScalar =
            PolarsTypeHelper.IsScalarType typeof<'T> || typeof<'T> = typeof<string>

        let isGrouping =
            typeof<'T>.IsGenericType
            && typeof<'T>.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

        let shouldShuffle = hasShuffle expression

        let canDirectlyCollectNative =
            not requiresClientFallback
            && clientPredicates.IsEmpty
            && clientProjOpt.IsNone
            && not (isScalar && width > 1UL)
            && not isGrouping

        if canDirectlyCollectNative then
            let collectedDf = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

            if shouldShuffle then
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
        member _.ElementType = typeof<'T>
        member _.Expression = expression
        member _.Provider = provider :> IQueryProvider

    interface IQueryable<'T>

    interface IOrderedQueryable<'T>

    interface IPolarsPlanSource with
        member this.GetCompiledLazyFrameHandle() = this.CompileToLazyFrameHandle()

    member this.GetPlanInspection(optimized: bool) : PlanInspection =
        let ops, clientPreds, clientProjOpt, requiresClientFallback = this.CompilePipeline()
        let nativeLf = applyNativeOps lfCloned ops
        let planStr = PolarsWrapper.Explain(nativeLf, optimized)
        let chunkSizeOpt = tryExtractChunkSize expression
        let shouldShuffle = hasShuffle expression

        let isGrouping =
            typeof<'T>.IsGenericType
            && typeof<'T>.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

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

            let targetType = typeof<'T>

            let isGrouping =
                targetType.IsGenericType
                && targetType.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

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
                    let mat = QueryMaterializerResolver.Resolve(Some materializer)

                    match clientProjOpt with
                    | Some(sourceType, func) ->
                        let invoker = LinqReflectionCache.GetMaterializerInvoker sourceType
                        let rawSourceRows = invoker.Invoke(mat, dfHandle)
                        seq {
                            for item in rawSourceRows do
                                if
                                    clientPredicates.IsEmpty
                                    || clientPredicates |> List.forall (fun p -> p.Invoke item)
                                then
                                    yield func.Invoke item :?> 'T
                        }
                    | None ->
                        let rows = mat.Materialize<'T>(dfHandle)

                        if clientPredicates.IsEmpty then
                            rows
                        else
                            rows
                            |> Seq.filter (fun item -> clientPredicates |> List.forall (fun p -> p.Invoke(box item)))

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

                                    while not advanced && not doneStreaming do
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
