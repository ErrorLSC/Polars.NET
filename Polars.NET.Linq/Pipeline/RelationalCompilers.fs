namespace Polars.NET.Linq

open System
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core
open Polars.NET.Core.Helpers

[<AutoOpen>]
module internal RelationalCompilers =

    let tryCompileTransparentLet
        (lf: LazyFrameHandle)
        (projLambda: LambdaExpression)
        (scopeMap: Map<string * string, string>)
        : QueryOp option =
        let param = projLambda.Parameters.[0]
        let ctx = createContextWithScope scopeMap param.Type lf param.Name

        match tryExtractMemberBindings projLambda.Body with
        | Some bindings ->
            let isNavigationBinding (_name: string, expr: Expression) =
                match ExpressionPatterns.unwrap expr with
                | :? ParameterExpression as p when p.Name = param.Name -> true
                | MemberAccess(_, m) when isNavigationMember m -> true
                | _ -> false

            let navBindings, computedBindings = bindings |> List.partition isNavigationBinding

            if not navBindings.IsEmpty && not computedBindings.IsEmpty then
                let translatedCols =
                    computedBindings
                    |> List.choose (fun (name, expr) ->
                        ExprTranslator.tryTranslateWithContext ctx expr
                        |> Option.map (fun h -> PolarsWrapper.Alias(h, name)))

                if translatedCols.Length = computedBindings.Length then
                    Some(QueryOp.WithColumns(List.toArray translatedCols))
                else
                    None
            else
                None
        | None -> None

    /// Translates a Select projection argument, supporting null-coalescing conditional fallbacks and member access.
    let tryTranslateSelectArg (ctx: TranslationContext) (argExpr: Expression) : ExprHandle option =
        match ExprTranslator.tryTranslateWithContext ctx argExpr with
        | Some h -> Some h
        | None ->
            match argExpr with
            | :? ConditionalExpression as cond ->
                let hasParamRef (e: Expression) =
                    let rec check expr =
                        match ExpressionPatterns.unwrap expr with
                        | null -> false
                        | :? ParameterExpression as p ->
                            isSyntheticScopeParam ctx.ContainsParam p
                        | MethodCall(_, target, args) ->
                            not (isNull target) && check target || args |> List.exists check
                        | MemberAccess(target, _) -> not (isNull target) && check target
                        | Unary(_, inner) -> check inner
                        | Binary(_, left, right) -> check left || check right
                        | Lambda(_, body) -> check body
                        | _ -> false

                    check e

                let colExprOpt, fallbackExprOpt =
                    if hasParamRef cond.IfTrue && not (hasParamRef cond.IfFalse) then
                        Some cond.IfTrue, Some cond.IfFalse
                    elif hasParamRef cond.IfFalse && not (hasParamRef cond.IfTrue) then
                        Some cond.IfFalse, Some cond.IfTrue
                    else
                        None, None

                match colExprOpt, fallbackExprOpt with
                | Some colExpr, Some fallbackExpr ->
                    let colHandleOpt =
                        match ColumnResolver.tryResolveColumnNameWithContext ctx colExpr with
                        | Some c -> Some(PolarsWrapper.Col c)
                        | None -> ExprTranslator.tryTranslateWithContext ctx colExpr

                    let litHandleOpt =
                        let cleanFallback = ExpressionPatterns.unwrap fallbackExpr

                        match cleanFallback with
                        | :? ConstantExpression as ce when not (isNull ce.Value) ->
                            Some(ExprTranslator.toLiteralHandle ce.Value cleanFallback.Type)
                        | other ->
                            match tryEvaluate other with
                            | Some v when not (isNull v) -> Some(ExprTranslator.toLiteralHandle v other.Type)
                            | _ -> None

                    match colHandleOpt, litHandleOpt with
                    | Some colH, Some litH -> Some(PolarsWrapper.FillNull(colH, litH))
                    | _ -> None

                | _ -> None

            | MemberAccess _ as ma ->
                ColumnResolver.tryResolveColumnNameWithContext ctx ma
                |> Option.map PolarsWrapper.Col

            | _ -> None

    /// Compiles a Select projection lambda into native Polars Select expressions
    let compileSelectProjection
        (lf: LazyFrameHandle)
        (projLambda: LambdaExpression)
        (scopeMap: Map<string * string, string>)
        : ExprHandle array option =
        let param = projLambda.Parameters.[0]
        let ctx = createContextWithScope scopeMap param.Type lf param.Name

        match tryExtractMemberBindings projLambda.Body with
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

            SplitOptContext.Current <- if splitMaxMap.IsEmpty then None else Some splitMaxMap
            try
                let exprOpts =
                    bindings
                    |> List.map (fun (name, argExpr) ->
                        name, argExpr, tryTranslateSelectArg ctx argExpr)

                let failed = exprOpts |> List.filter (fun (_, _, opt) -> opt.IsNone)
                if failed.IsEmpty then
                    Some(exprOpts |> List.map (fun (name, _, opt) -> PolarsWrapper.Alias(opt.Value, name)) |> List.toArray)
                else
                    let reasons = failed |> List.map (fun (n, e, _) -> sprintf "%s = %s (%s)" n (e.ToString()) (e.GetType().Name))
                    FallbackTracker.Diagnostic <- Some(sprintf "Failed projection bindings: %s" (String.concat "; " reasons))
                    None
            finally
                SplitOptContext.Current <- None

        | None ->
            let unwrappedBody = ExpressionPatterns.unwrap projLambda.Body
            match unwrappedBody with
            | MemberAccess _ as ma when not (PolarsTypeHelper.IsScalarType ma.Type) && (ColumnResolver.tryResolveColumnNameWithContext ctx ma).IsNone ->
                let lfCols = PolarsSchemaExtractor.GetColumnNames lf |> Set.ofArray
                let entityProps = ma.Type.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                let matchedExprs =
                    entityProps
                    |> Array.choose (fun prop ->
                        let propAccess = Expression.Property(ma, prop) :> Expression
                        match ColumnResolver.tryResolveColumnNameWithContext ctx propAccess with
                        | Some physCol -> Some(PolarsWrapper.Alias(PolarsWrapper.Col physCol, prop.Name))
                        | None when lfCols.Contains prop.Name -> Some(PolarsWrapper.Col prop.Name)
                        | None -> None)
                if matchedExprs.Length > 0 then
                    Some matchedExprs
                else
                    None
            | ExtractColumnName colName -> Some [| PolarsWrapper.Col colName |]
            | expr -> ExprTranslator.tryTranslateWithContext ctx expr |> Option.map (fun h -> [| h |])

    let internal extractKeyExprs (ctx: TranslationContext) (keyLambda: LambdaExpression) : ExprHandle array option =
        match tryExtractMemberBindings keyLambda.Body with
        | Some bindings ->
            let exprs =
                bindings
                |> List.choose (fun (_, e) -> ExprTranslator.tryTranslateWithContext ctx e)

            if exprs.Length = bindings.Length then
                Some(List.toArray exprs)
            else
                None
        | None ->
            ExprTranslator.tryTranslateWithContext ctx keyLambda.Body
            |> Option.map (fun h -> [| h |])

    /// Compiles a Join stage into a native JoinSpec (supporting single-column and multi-column keys across Inner, Left, Right, Full)
    let compileJoin
        (baseLf: LazyFrameHandle)
        (methodInfo: MethodInfo)
        (innerExpr: Expression)
        (outerKey: LambdaExpression)
        (innerKey: LambdaExpression)
        (scopeMap: Map<string * string, string>)
        : JoinSpec option =
        let isRightJoin = methodInfo.Name = "RightJoin"

        let how =
            match methodInfo.Name with
            | "RightJoin"
            | "LeftJoin"
            | "GroupJoin" -> PlJoinType.Left
            | "FullJoin" -> PlJoinType.Outer
            | _ -> PlJoinType.Inner

        match SubqueryDecorrelator.resolveSequenceLazyFrame innerExpr with
        | Some rightLf ->
            let outerParam = outerKey.Parameters.[0]
            let innerParam = innerKey.Parameters.[0]

            let leftCtx = createContextWithScope scopeMap outerParam.Type baseLf outerParam.Name
            let rightCtx = createContextForType innerParam.Type rightLf innerParam.Name

            match extractKeyExprs leftCtx outerKey, extractKeyExprs rightCtx innerKey with
            | Some leftKeys, Some rightKeys when leftKeys.Length = rightKeys.Length ->
                let leftCols = PolarsSchemaExtractor.GetColumnNames baseLf
                let rightCols = PolarsSchemaExtractor.GetColumnNames rightLf
                let leftColSet = Set.ofArray leftCols
                let baseSuffix = if isRightJoin then LinqConstants.DefaultLeftSuffix else LinqConstants.DefaultRightSuffix
                let rec findUniqueSuffix (cnt: int) =
                    let candidate = if cnt = 1 then baseSuffix else sprintf "%s%d" baseSuffix cnt
                    let hasConflict = rightCols |> Array.exists (fun rc -> leftColSet.Contains (rc + candidate))
                    if not hasConflict then candidate
                    else findUniqueSuffix (cnt + 1)
                let uniqueSuffix = findUniqueSuffix 1
                Some
                    {
                        RightLf = rightLf
                        LeftOn = leftKeys
                        RightOn = rightKeys
                        How = how
                        Suffix = Some uniqueSuffix
                        InvertSides = isRightJoin
                        Coalesce = PlJoinCoalesce.KeepColumns
                        NullsEqual = false
                    }
            | _ -> None
        | None -> None

    /// Compiles a JoinAsOf stage into a native AsOfJoinSpec
    let compileJoinAsOf
        (baseLf: LazyFrameHandle)
        (methodInfo: MethodInfo)
        (innerExpr: Expression)
        (outerKey: LambdaExpression)
        (innerKey: LambdaExpression)
        (outerByOpt: LambdaExpression option)
        (innerByOpt: LambdaExpression option)
        (tolStr: string option)
        (tolInt: int64 option)
        (tolFloat: double option)
        (strategy: PlAsofStrategy)
        (scopeMap: Map<string * string, string>)
        : AsOfJoinSpec option =
        let extractKeyExprs (ctx: TranslationContext) (keyLambda: LambdaExpression) : ExprHandle array option =
            match tryExtractMemberBindings keyLambda.Body with
            | Some bindings ->
                let exprs =
                    bindings
                    |> List.choose (fun (_, e) -> ExprTranslator.tryTranslateWithContext ctx e)

                if exprs.Length = bindings.Length then
                    Some(List.toArray exprs)
                else
                    None
            | None ->
                ExprTranslator.tryTranslateWithContext ctx keyLambda.Body
                |> Option.map (fun h -> [| h |])

        match SubqueryDecorrelator.resolveSequenceLazyFrame innerExpr with
        | Some rightLf ->
            let outerParam = outerKey.Parameters.[0]
            let innerParam = innerKey.Parameters.[0]

            let leftCtx = createContextWithScope scopeMap outerParam.Type baseLf outerParam.Name
            let rightCtx = createContextForType innerParam.Type rightLf innerParam.Name

            match extractKeyExprs leftCtx outerKey, extractKeyExprs rightCtx innerKey with
            | Some leftKeys, Some rightKeys when leftKeys.Length = rightKeys.Length ->
                let leftByHandlesOpt =
                    match outerByOpt with
                    | Some ob -> extractKeyExprs leftCtx ob
                    | None -> None

                let rightByHandlesOpt =
                    match innerByOpt with
                    | Some ib -> extractKeyExprs rightCtx ib
                    | None -> None

                let byValid =
                    match outerByOpt, innerByOpt with
                    | Some _, Some _ ->
                        leftByHandlesOpt.IsSome
                        && rightByHandlesOpt.IsSome
                        && leftByHandlesOpt.Value.Length = rightByHandlesOpt.Value.Length
                    | None, None -> true
                    | _ -> false

                if byValid then
                    Some
                        {
                            RightLf = rightLf
                            LeftOn = leftKeys
                            RightOn = rightKeys
                            LeftBy = leftByHandlesOpt
                            RightBy = rightByHandlesOpt
                            Strategy = strategy
                            ToleranceStr = tolStr
                            ToleranceInt = tolInt
                            ToleranceFloat = tolFloat
                            AllowEq = true
                            CheckSorted = true
                            Suffix = Some LinqConstants.DefaultRightSuffix
                            Validation = PlJoinValidation.ManyToMany
                            Coalesce = PlJoinCoalesce.JoinSpecific
                            MaintainOrder = PlJoinMaintainOrder.None
                            JoinSide = PlJoinSide.None
                            NullsEqual = false
                            SliceOffset = None
                            SliceLen = 0UL
                        }
                else
                    None
            | _ -> None
        | None -> None

    /// Compiles a two-parameter Join/LeftJoin/RightJoin/JoinAsOf result selector into a native Select projection operation
    let compileJoinResultSelectorWithParams
        (leftCols: string array)
        (rightCols: string array)
        (invertSides: bool)
        (suffixOpt: string option)
        (resLambda: LambdaExpression)
        (scopeMap: Map<string * string, string>)
        : QueryOp option =
        if resLambda.Parameters.Count < 2 then
            None
        else
            let pLeft = resLambda.Parameters.[0]
            let pRight = resLambda.Parameters.[1]

            let firstParam, secondParam, firstCols, secondCols, secondSuffix =
                if invertSides then
                    pRight, pLeft, rightCols, leftCols, suffixOpt |> Option.defaultValue LinqConstants.DefaultLeftSuffix
                else
                    pLeft, pRight, leftCols, rightCols, suffixOpt |> Option.defaultValue LinqConstants.DefaultRightSuffix

            let firstColSet = Set.ofArray firstCols
            let ctx =
                if not scopeMap.IsEmpty then
                    TranslationContext.ScopedEntity([ firstParam.Name; secondParam.Name ], scopeMap, Array.toList firstCols @ Array.toList secondCols)
                else
                    TranslationContext.BinaryJoin(firstParam.Name, secondParam.Name, firstColSet, secondSuffix)

            let unwrappedBody = ExpressionPatterns.unwrap resLambda.Body
            match unwrappedBody with
            | :? ParameterExpression as p when p.Name = firstParam.Name ->
                let selectExprs = firstCols |> Array.map PolarsWrapper.Col
                Some(QueryOp.Select selectExprs)

            | :? ParameterExpression as p when p.Name = secondParam.Name ->
                let selectExprs =
                    secondCols
                    |> Array.map (fun c ->
                        if firstColSet.Contains c then
                            PolarsWrapper.Alias(PolarsWrapper.Col(c + secondSuffix), c)
                        else
                            PolarsWrapper.Col c)
                Some(QueryOp.Select selectExprs)

            | MemberAccess _ as ma when not (PolarsTypeHelper.IsScalarType ma.Type) ->
                let entityProps = ma.Type.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                let matchedCols = entityProps |> Array.map (fun p -> p.Name) |> Array.filter firstColSet.Contains
                if matchedCols.Length > 0 then
                    let selectExprs = matchedCols |> Array.map PolarsWrapper.Col
                    Some(QueryOp.Select selectExprs)
                else
                    let secondColSet = Set.ofArray secondCols
                    let matchedSecond = entityProps |> Array.map (fun p -> p.Name) |> Array.filter secondColSet.Contains
                    if matchedSecond.Length > 0 then
                        let selectExprs =
                            matchedSecond
                            |> Array.map (fun c ->
                                if firstColSet.Contains c then
                                    PolarsWrapper.Alias(PolarsWrapper.Col(c + secondSuffix), c)
                                else
                                    PolarsWrapper.Col c)
                        Some(QueryOp.Select selectExprs)
                    else
                        None

            | _ ->
                match tryExtractMemberBindings unwrappedBody with
                | Some bindings ->
                    let exprOpts =
                        bindings
                        |> List.choose (fun (name, argExpr) ->
                            tryTranslateSelectArg ctx argExpr
                            |> Option.map (fun h -> PolarsWrapper.Alias(h, name)))

                    if exprOpts.Length = bindings.Length then
                        Some(QueryOp.Select(List.toArray exprOpts))
                    else
                        None

                | None ->
                    match unwrappedBody with
                    | MemberAccess(_, m) ->
                        ExprTranslator.tryTranslateWithContext ctx unwrappedBody
                        |> Option.map (fun h -> QueryOp.Select [| PolarsWrapper.Alias(h, m.Name) |])
                    | _ ->
                        ExprTranslator.tryTranslateWithContext ctx unwrappedBody
                        |> Option.map (fun h -> QueryOp.Select [| h |])

    let compileJoinResultSelector
        (leftCols: string array)
        (rightCols: string array)
        (spec: JoinSpec)
        (resLambda: LambdaExpression)
        (scopeMap: Map<string * string, string>)
        : QueryOp option =
        compileJoinResultSelectorWithParams leftCols rightCols spec.InvertSides spec.Suffix resLambda scopeMap

    /// Compiles Intersect/Except/IntersectBy/ExceptBy stages into a native JoinSpec (Semi/Anti Join)
    let compileSetOp
        (methodInfo: MethodInfo)
        (secondExpr: Expression)
        (firstKeyOpt: LambdaExpression option)
        (secondKeyOpt: LambdaExpression option)
        (baseLf: LazyFrameHandle)
        : JoinSpec option =
        let how =
            if methodInfo.Name.StartsWith "Intersect" then
                PlJoinType.Semi
            else
                PlJoinType.Anti

        SubqueryDecorrelator.resolveSequenceLazyFrame secondExpr
        |> Option.bind (fun rightLf ->
            let keysOpt =
                match firstKeyOpt, secondKeyOpt with
                | Some k1, Some k2 ->
                    let p1 = k1.Parameters.[0]
                    let p2 = k2.Parameters.[0]
                    let ctx1 = createContextForType p1.Type baseLf p1.Name
                    let ctx2 = createContextForType p2.Type rightLf p2.Name

                    match
                        ExprTranslator.tryTranslateWithContext ctx1 k1.Body,
                        ExprTranslator.tryTranslateWithContext ctx2 k2.Body
                    with
                    | Some lk, Some rk -> Some([| lk |], [| rk |])
                    | _ -> None
                | Some k1, None ->
                    let p1 = k1.Parameters.[0]
                    let ctx1 = createContextForType p1.Type baseLf p1.Name

                    ExprTranslator.tryTranslateWithContext ctx1 k1.Body
                    |> Option.bind (fun lk ->
                        let rightCols = PolarsSchemaExtractor.GetColumnNames rightLf

                        if rightCols.Length > 0 then
                            Some([| lk |], [| PolarsWrapper.Col rightCols.[0] |])
                        else
                            None)
                | None, None ->
                    let leftCols = PolarsSchemaExtractor.GetColumnNames baseLf
                    let rightCols = PolarsSchemaExtractor.GetColumnNames rightLf
                    if leftCols.Length = rightCols.Length && leftCols.Length > 0 then
                        Some(leftCols |> Array.map PolarsWrapper.Col, rightCols |> Array.map PolarsWrapper.Col)
                    else
                        Some(leftCols |> Array.map PolarsWrapper.Col, leftCols |> Array.map PolarsWrapper.Col)
                | _ -> None

            keysOpt
            |> Option.map (fun (leftOn, rightOn) ->
                {
                    RightLf = rightLf
                    LeftOn = leftOn
                    RightOn = rightOn
                    How = how
                    Suffix = None
                    InvertSides = false
                    Coalesce = PlJoinCoalesce.JoinSpecific
                    NullsEqual = true
                }))

    /// Compiles an Append or Prepend stage into a native QueryOp.Concat
    let tryCompileAppendOrPrepend (elemExpr: Expression) (prepend: bool) : QueryOp option =
        let evaluatedOpt =
            match tryEvaluate elemExpr with
            | Some elem -> Some elem
            | None ->
                try
                    let lambda = Expression.Lambda<Func<obj>>(Expression.Convert(elemExpr, typeof<obj>)).Compile()
                    Some(lambda.Invoke())
                with _ ->
                    None

        match evaluatedOpt with
        | Some elem when not (isNull elem) ->
            let elemType = elem.GetType()

            let fromRows =
                LinqReflectionCache.FromRowsMethod.MakeGenericMethod(elemType)

            let typedArray = Array.CreateInstance(elemType, 1)
            typedArray.SetValue(elem, 0)

            let singleDf = fromRows.Invoke(null, [| box typedArray |]) :?> DataFrameHandle
            let otherLf = PolarsWrapper.DataFrameToLazy singleDf
            Some(QueryOp.Concat { OtherLf = otherLf; Prepend = prepend })
        | _ -> None

    /// Converts a list of (sourceCol, targetCol) pairs into a single-element or empty QueryOp.Rename list
    let buildRenameOps (renames: (string * string) list) : QueryOp list =
        if renames.IsEmpty then
            []
        else
            [
                QueryOp.Rename
                    {
                        ExistingNames = renames |> List.map fst |> List.toArray
                        NewNames = renames |> List.map snd |> List.toArray
                    }
            ]

    /// Fallback translator for filters over tuple/transparent scopes (supporting null checks and binary comparisons)
    let rec tryTranslateTupleFilter (ctx: TranslationContext) (e: Expression) : ExprHandle option =
        let isNullConstant (expr: Expression) =
            match ExpressionPatterns.unwrap expr with
            | :? ConstantExpression as c -> isNull c.Value
            | _ -> false

        match e with
        | Binary(ExpressionType.NotEqual | ExpressionType.Equal as op, left, right) when
            isNullConstant left || isNullConstant right
            ->
            let target = if isNullConstant right then left else right
            let wrap = if op = ExpressionType.NotEqual then PolarsWrapper.IsNotNull else PolarsWrapper.IsNull
            tryTranslateTupleFilter ctx target |> Option.map wrap
        | Binary(op, left, right) ->
            match tryTranslateTupleFilter ctx left, tryTranslateTupleFilter ctx right with
            | Some l, Some r ->
                try
                    Some(ExprTranslator.translateBinary op l r)
                with _ ->
                    None
            | _ -> None
        | MemberAccess _ as ma ->
            ColumnResolver.tryResolveColumnNameWithContext ctx ma
            |> Option.map PolarsWrapper.Col
        | Constant _ -> ExprTranslator.tryTranslateWithContext TranslationContext.Empty e
        | _ -> None

    /// Checks whether a two-parameter Join result selector merely packages parameters into a transparent identifier/tuple
    let isTransparentJoinResSelector (resSel: LambdaExpression) : bool =
        match tryExtractMemberBindings resSel.Body with
        | Some bindings when resSel.Parameters.Count >= 2 ->
            let p1, p2 = resSel.Parameters.[0].Name, resSel.Parameters.[1].Name

            let rec isTransparentArg (e: Expression) =
                match ExpressionPatterns.unwrap e with
                | :? ParameterExpression as p -> p.Name = p1 || p.Name = p2
                | MemberAccess(target, m) when isNavigationMember m -> isTransparentArg target
                | :? NewExpression as ne when isTupleOrAnonType ne.Type ->
                    ne.Arguments |> Seq.forall isTransparentArg
                | _ -> false

            bindings |> List.forall (fun (_, expr) -> isTransparentArg expr)
        | _ -> false

    /// Active pattern categorizing an optional Join/Unpivot result selector into transparent (or absent) vs concrete
    let (|TransparentOrNoResSelector|ConcreteResSelector|) (resSelOpt: LambdaExpression option) =
        match resSelOpt with
        | Some resSel when not (isTransparentJoinResSelector resSel) -> ConcreteResSelector resSel
        | _ -> TransparentOrNoResSelector

    /// Compiles GroupJoin followed by SelectMany(DefaultIfEmpty) into a native Left JoinSpec
    let compileGroupJoinLeftOuterSpec
        (baseLf: LazyFrameHandle)
        (innerExpr: Expression)
        (outerKey: LambdaExpression)
        (innerKey: LambdaExpression)
        (scopeMap: Map<string * string, string>)
        : JoinSpec option =
        SubqueryDecorrelator.resolveSequenceLazyFrame innerExpr
        |> Option.bind (fun rightLf ->
            let outerParam = outerKey.Parameters.[0]
            let innerParam = innerKey.Parameters.[0]
            let leftCtx = createContextWithScope scopeMap outerParam.Type baseLf outerParam.Name
            let rightCtx = createContextForType innerParam.Type rightLf innerParam.Name

            match extractKeyExprs leftCtx outerKey, extractKeyExprs rightCtx innerKey with
            | Some leftKeys, Some rightKeys when leftKeys.Length = rightKeys.Length ->
                let leftCols = PolarsSchemaExtractor.GetColumnNames baseLf
                let rightCols = PolarsSchemaExtractor.GetColumnNames rightLf
                let leftColSet = Set.ofArray leftCols
                let baseSuffix = LinqConstants.DefaultRightSuffix
                let rec findUniqueSuffix (cnt: int) =
                    let candidate = if cnt = 1 then baseSuffix else sprintf "%s%d" baseSuffix cnt
                    let hasConflict = rightCols |> Array.exists (fun rc -> leftColSet.Contains (rc + candidate))
                    if not hasConflict then candidate
                    else findUniqueSuffix (cnt + 1)
                let uniqueSuffix = findUniqueSuffix 1
                Some
                    {
                        RightLf = rightLf
                        LeftOn = leftKeys
                        RightOn = rightKeys
                        How = PlJoinType.Left
                        Suffix = Some uniqueSuffix
                        InvertSides = false
                        Coalesce = PlJoinCoalesce.KeepColumns
                        NullsEqual = false
                    }
            | _ -> None)

    let compileGroupJoinLeftOuter
        (baseLf: LazyFrameHandle)
        (innerExpr: Expression)
        (outerKey: LambdaExpression)
        (innerKey: LambdaExpression)
        (scopeMap: Map<string * string, string>)
        : QueryOp option =
        compileGroupJoinLeftOuterSpec baseLf innerExpr outerKey innerKey scopeMap
        |> Option.map QueryOp.Join

    /// Compiles GroupJoin with an explicit result selector (supporting pre-aggregated group counts and null-filling)
    let compileGroupJoinWithResult
        (baseLf: LazyFrameHandle)
        (innerExpr: Expression)
        (outerKey: LambdaExpression)
        (innerKey: LambdaExpression)
        (resSel: LambdaExpression)
        : QueryOp list option =
        SubqueryDecorrelator.resolveSequenceLazyFrame innerExpr
        |> Option.bind (fun rightLf ->
            let outerParam = outerKey.Parameters.[0]
            let innerParam = innerKey.Parameters.[0]
            let leftCtx = createContextForType outerParam.Type baseLf outerParam.Name
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
                        let aggLf = PolarsWrapper.LazyGroupByAgg(rightLf, [| aggKeyClone |], [| aggExpr |], null, false)
                        aggLf, Some aggColName
                    | None -> rightLf, None

                let joinOp =
                    QueryOp.Join
                        {
                            RightLf = preAggRightLf
                            LeftOn = [| PolarsWrapper.CloneExpr leftKey |]
                            RightOn = [| PolarsWrapper.CloneExpr rightKey |]
                            How = PlJoinType.Left
                            Suffix = Some LinqConstants.DefaultRightSuffix
                            InvertSides = false
                            Coalesce = PlJoinCoalesce.JoinSpecific
                            NullsEqual = false
                        }

                let renameOps = extractBinaryResultRenames resSel None |> buildRenameOps

                let fillOps =
                    match aggColOpt with
                    | Some aggCol ->
                        let filledExpr = PolarsWrapper.FillNull(PolarsWrapper.Col aggCol, PolarsWrapper.Lit 0u)
                        [ QueryOp.WithColumns [| PolarsWrapper.Alias(filledExpr, aggCol) |] ]
                    | None -> []

                Some(renameOps @ fillOps @ [ joinOp ])
            | _ -> None)

    /// Compiles a 2-way or 3-way Zip stage into HorizontalConcat (and optional column renames)
    let compileZip
        (baseLf: LazyFrameHandle)
        (secondExpr: Expression)
        (resLambdaOpt: LambdaExpression option)
        : QueryOp list option =
        SubqueryDecorrelator.resolveSequenceLazyFrame secondExpr
        |> Option.map (fun otherLf ->
            let baseCols = PolarsSchemaExtractor.GetColumnNames baseLf |> Set.ofArray
            let safeSecondLf, _, suffixMap = disambiguateColumns baseCols otherLf "_second"
            let zipOp = QueryOp.HorizontalConcat [| safeSecondLf |]

            let renameOps =
                match resLambdaOpt with
                | Some resLambda ->
                    extractBinaryResultRenames resLambda None
                    |> List.map (fun (srcCol, destCol) ->
                        suffixMap.TryFind srcCol |> Option.defaultValue srcCol, destCol)
                    |> buildRenameOps
                | None -> []

            renameOps @ [ zipOp ])

    let compileZip3 (baseLf: LazyFrameHandle) (secondExpr: Expression) (thirdExpr: Expression) : QueryOp option =
        match
            SubqueryDecorrelator.resolveSequenceLazyFrame secondExpr,
            SubqueryDecorrelator.resolveSequenceLazyFrame thirdExpr
        with
        | Some secondLf, Some thirdLf ->
            let baseCols = PolarsSchemaExtractor.GetColumnNames baseLf |> Set.ofArray
            let safeSecondLf, colsAfter2, _ = disambiguateColumns baseCols secondLf "_second"
            let safeThirdLf, _, _ = disambiguateColumns colsAfter2 thirdLf "_third"
            Some(QueryOp.HorizontalConcat [| safeSecondLf; safeThirdLf |])
        | _ -> None

    /// Compiles group-scoped partition operations (e.g. from g in source.GroupBy(k) from top in g.OrderBy(s).Take(n) select ...)
    /// into native window ranking filters and projections.

    let compileExplode
        (colLambda: LambdaExpression)
        (resLambdaOpt: LambdaExpression option)
        : QueryOp list option =
        extractExplodeColumn colLambda
        |> Option.map (fun colName ->
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
                    extractBinaryResultRenames resLambda (Some(itemParam.Name, colName))
                    |> buildRenameOps
                | _ -> []

            renameOps @ [ explodeOp ])

    /// Compiles CrossJoin (uncorrelated SelectMany) into a Cross Join and optional renames
    let compileCrossJoin (innerExpr: Expression) (resLambdaOpt: LambdaExpression option) : QueryOp list option =
        SubqueryDecorrelator.resolveSequenceLazyFrame innerExpr
        |> Option.map (fun rightLf ->
            let joinOp =
                QueryOp.Join
                    {
                        RightLf = rightLf
                        LeftOn = [||]
                        RightOn = [||]
                        How = PlJoinType.Cross
                        Suffix = Some LinqConstants.DefaultRightSuffix
                        InvertSides = false
                        Coalesce = PlJoinCoalesce.KeepColumns
                        NullsEqual = false
                    }

            let renameOps =
                resLambdaOpt
                |> Option.map (fun r -> extractBinaryResultRenames r None |> buildRenameOps)
                |> Option.defaultValue []

            renameOps @ [ joinOp ])

