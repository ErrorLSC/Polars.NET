namespace Polars.NET.Linq.IR

open System
open System.Reflection
open System.Linq.Expressions
open Polars.NET.Linq
open Polars.NET.Linq.IR.ProjectionExtraction

/// GroupBy plans: aggregations over groups, per-group slices and result selector decomposition.
module internal GroupByPlanning =

    /// Helper to translate a single group-by reduction/aggregation into a pure ScalarExpr.
    let tryTranslateGroupByAggScalar
        (groupParamName: string)
        (targetColExprOpt: ScalarExpr option)
        (scalarColOpt: string option)
        (wholeRowStructOpt: ScalarExpr option)
        (scopeMap: Map<string * string, string>)
        (cols: string list)
        (rawArgExpr: Expression)
        : ScalarExpr option =

        let argExpr = ExpressionPatterns.unwrap rawArgExpr

        let isGroupParam (e: Expression) =
            match ExpressionPatterns.unwrap e with
            | :? ParameterExpression as p -> p.Name = groupParamName
            | _ -> false

        let isGroupReductionCall (targetObj: Expression) (callArgs: Expression list) =
            isNull targetObj && callArgs.Length = 1 && isGroupParam callArgs.Head
            || not (isNull targetObj) && callArgs.IsEmpty && isGroupParam targetObj

        let isGroupCountCall (m: MethodInfo) (targetObj: Expression) (callArgs: Expression list) =
            (m.Name = "Count" || m.Name = "LongCount") && isGroupReductionCall targetObj callArgs

        let tryMatchNestedListAgg (e: Expression) =
            match e with
            | MethodCall(mList, null, [ MethodCall(mSel, null, [ gArg; CleanLambda selLambda ]) ])
            | MethodCall(mList, MethodCall(mSel, null, [ gArg; CleanLambda selLambda ]), [])
            | MethodCall(mList, MethodCall(mSel, gArg, [ CleanLambda selLambda ]), []) when
                (mList.Name = "ToList" || mList.Name = "ToArray")
                && mSel.Name = "Select"
                && isGroupParam gArg
                ->
                Some selLambda
            | _ -> None

        match argExpr with
        | MethodCall(m, target, callArgs) when isGroupCountCall m target callArgs ->
            let core =
                if m.Name = "LongCount" then
                    ScalarExpr.Cast(ScalarExpr.Aggregation(AggKind.Len, None, false), LogicalTypes.Int64, false)
                else
                    ScalarExpr.Aggregation(AggKind.Len, None, false)
            Some core

        | MethodCall(m, target, callArgs) when
            isGroupReductionCall target callArgs && targetColExprOpt.IsSome
            ->
            ScalarExprOps.translateReductionOp m.Name targetColExprOpt.Value

        | _ ->
            match tryMatchNestedListAgg argExpr with
            | Some selLambda ->
                let selParam = selLambda.Parameters.[0]
                let selCtx = PipelineStageParser.createContextWithScopeAndCols scopeMap selParam.Type cols selParam.Name
                match ColumnResolver.tryResolveColumnNameWithContext selCtx selLambda.Body with
                | Some cName -> Some(ScalarExpr.Column cName)
                | None -> ExprTranslator.tryTranslateToScalar selCtx selLambda.Body
            | None ->
                AggTranslator.tryTranslateAggWithScalarColAndScope
                    groupParamName
                    scalarColOpt
                    wholeRowStructOpt
                    scopeMap
                    argExpr
                    ""

    /// Pure symbolic GroupBy builder powering both single-key and multi-key GroupBy pipelines (C# LINQ & F# QueryBuilder).
    let buildUnifiedGroupByPlan
        (resolveSequence: Expression -> LogicalPlan option)
        (basePlan: LogicalPlan)
        (ctx: GroupFoldContext)
        (projBody: Expression)
        (groupParamName: string)
        (isKeyArg: Expression -> bool)
        (scopeMap: Map<string * string, string>)
        : LogicalPlan option =

        let cols = (SchemaInference.inferPlanSchema basePlan).ColumnNames
        let keyParam = ctx.KeyLambda.Parameters.[0]
        let keyCtx = PipelineStageParser.createContextWithScopeAndCols scopeMap keyParam.Type cols keyParam.Name

        let elemExprOpt =
            ctx.ElemLambdaOpt
            |> Option.bind (fun el ->
                let elParam = el.Parameters.[0]
                let elCtx = PipelineStageParser.createContextWithScopeAndCols scopeMap elParam.Type cols elParam.Name
                ExprTranslator.tryTranslateToScalar elCtx el.Body)

        let elemScoreMapOpt =
            ctx.ElemLambdaOpt
            |> Option.bind (fun el ->
                let elParam = el.Parameters.[0]
                match tryExtractMemberBindings el.Body with
                | Some bindings ->
                    let elCtx = PipelineStageParser.createContextWithScopeAndCols scopeMap elParam.Type cols elParam.Name
                    let resolvedBindings =
                        bindings
                        |> List.choose (fun (propName, expr) ->
                            match ColumnResolver.tryResolveColumnNameWithContext elCtx expr with
                            | Some cName -> Some((propName, cName))
                            | None -> None)
                    let updatedFromEntities =
                        bindings
                        |> List.fold (fun m (propName, expr) ->
                            let subScopeOpt =
                                match ExpressionPatterns.unwrap expr with
                                | :? ParameterExpression as p -> Some p.Name
                                | MemberAccess(_, m) -> Some m.Name
                                | _ -> None
                            match subScopeOpt with
                            | Some s when s <> propName ->
                                scopeMap
                                |> Map.fold (fun acc (kScope, kCol) physCol ->
                                    if kScope = s then Map.add (propName, kCol) physCol acc else acc) m
                            | _ -> m) scopeMap
                    let combined =
                        resolvedBindings
                        |> List.fold (fun m (propName, colName) ->
                            m |> Map.add ("", propName) colName |> Map.add (elParam.Name, propName) colName) updatedFromEntities
                    Some combined
                | None ->
                    let subScopeOpt =
                        match ExpressionPatterns.unwrap el.Body with
                        | :? ParameterExpression as p -> Some p.Name
                        | MemberAccess(_, m) -> Some m.Name
                        | _ -> None
                    match subScopeOpt with
                    | Some s ->
                        let updated =
                            scopeMap
                            |> Map.fold (fun acc (kScope, kCol) physCol ->
                                if kScope = s then
                                    acc |> Map.add (elParam.Name, kCol) physCol |> Map.add ("", kCol) physCol
                                else acc) scopeMap
                        Some updated
                    | None -> None)

        let effectiveScopeMap = elemScoreMapOpt |> Option.defaultValue scopeMap
        let elemType = keyParam.Type

        let wholeRowStructOpt = EntityPacking.tryWholeRowStruct elemType cols

        let translateKeyPart (e: Expression) : ScalarExpr option =
            match ExprTranslator.tryTranslateToScalar keyCtx e with
            | Some s -> Some s
            | None ->
                match e with
                | MemberAccess(:? ParameterExpression as p, mem) when p.Name = keyParam.Name ->
                    Some(ScalarExpr.Column mem.Name)
                | _ -> None

        let keyPairsOpt: (string * Expression) list option =
            match ExpressionPatterns.unwrap ctx.KeyLambda.Body with
            | :? NewExpression as ne when
                ne.Type.IsGenericType && (ne.Type.Name.StartsWith "Tuple`" || ne.Type.Name.StartsWith "ValueTuple`") ->
                let names =
                    ne.Arguments
                    |> Seq.mapi (fun i arg ->
                        match ExpressionPatterns.unwrap arg with
                        | ExtractColumnName col -> col
                        | MemberAccess(_, m) -> m.Name
                        | _ -> sprintf "Item%d" (i + 1))
                    |> Seq.toList
                let args = ne.Arguments |> Seq.toList
                Some(List.zip names args)
            | _ ->
                match tryExtractMemberBindings ctx.KeyLambda.Body with
                | Some pairs -> Some pairs
                | None ->
                    let name =
                        match ctx.KeyLambda.Body with
                        | ExtractColumnName col -> col
                        | _ -> "Key"
                    Some [ (name, ctx.KeyLambda.Body) ]

        let keyDefinitionsOpt: (string * ScalarExpr) list option =
            keyPairsOpt
            |> Option.bind (fun pairs ->
                let translated =
                    pairs
                    |> List.choose (fun (name, expr) -> translateKeyPart expr |> Option.map (fun s -> (name, s)))
                if translated.Length = pairs.Length then
                    Some translated
                else
                    None)

        match keyDefinitionsOpt with
        | None -> None
        | Some keyDefs ->
            let isKeyExpression (e: Expression) : bool =
                let cleanE = ExpressionPatterns.unwrap e
                if isKeyArg cleanE then
                    true
                else
                    match cleanE with
                    | MemberAccess(target, _) when isKeyArg target -> true
                    | MemberAccess(MemberAccess(p, keyProp), _) when
                        not (isNull p) && p.NodeType = ExpressionType.Parameter && keyProp.Name = "Key"
                        ->
                        true
                    | _ -> false

            let memberBindingsOpt =
                match tryExtractMemberBindings projBody with
                | Some bindings -> Some bindings
                | None ->
                    let cleanProj = ExpressionPatterns.unwrap projBody
                    if isKeyExpression cleanProj then
                        let name =
                            match cleanProj with
                            | MemberAccess(_, m) when m.Name <> "Key" -> m.Name
                            | _ ->
                                if keyDefs.Length = 1 then fst keyDefs.[0] else "Key"
                        Some [ (name, projBody) ]
                    else
                        let defaultName =
                            match cleanProj with
                            | MethodCall(m, _, _) -> m.Name
                            | _ -> "Value"
                        Some [ (defaultName, projBody) ]

            match memberBindingsOpt with
            | None -> None
            | Some memberBindings ->
                let memberNames = memberBindings |> List.map fst
                let args = memberBindings |> List.map snd

                let keyNameToOutputAliasMap =
                    keyDefs
                    |> List.mapi (fun keyIdx (origKeyName, _) ->
                        let outputColName =
                            List.zip memberNames args
                            |> List.tryPick (fun (memName, rawArg) ->
                                let arg = ExpressionPatterns.unwrap rawArg
                                match arg with
                                | MemberAccess(MemberAccess(_, k), m) when k.Name = "Key" ->
                                    if m.Name = origKeyName then Some memName
                                    elif m.Name = sprintf "Item%d" (keyIdx + 1) || (m.Name = "Fst" && keyIdx = 0) || (m.Name = "Snd" && keyIdx = 1) then Some memName
                                    else None
                                | MemberAccess(p, m) when isKeyArg p ->
                                    if m.Name = origKeyName then Some memName
                                    elif m.Name = "Key" && keyDefs.Length = 1 then Some memName
                                    elif m.Name = sprintf "Item%d" (keyIdx + 1) || (m.Name = "Fst" && keyIdx = 0) || (m.Name = "Snd" && keyIdx = 1) then Some memName
                                    else None
                                | _ when isKeyArg arg && keyDefs.Length = 1 -> Some memName
                                | _ -> None)
                            |> Option.defaultValue origKeyName
                        origKeyName, outputColName)
                    |> Map.ofList

                let keyAliasedExprs =
                    keyDefs
                    |> List.map (fun (origKeyName, rawKeyExpr) ->
                        let outputColName = keyNameToOutputAliasMap.[origKeyName]
                        ScalarExpr.Alias(rawKeyExpr, outputColName))

                let nonKeyArgs =
                    List.zip memberNames args
                    |> List.filter (fun (_, argExpr) -> not (isKeyExpression argExpr))

                let scalarColOpt =
                    match ctx.ElemLambdaOpt with
                    | Some el ->
                        match el.Body with
                        | ExtractColumnName c -> Some c
                        | _ -> None
                    | None ->
                        PipelineStageParser.tryGetScalarColumnNameFromCols keyParam.Type cols

                let targetColExprOpt =
                    elemExprOpt |> Option.orElse (scalarColOpt |> Option.map ScalarExpr.Column)

                let tryResolveKeyPropertyCol (e: Expression) : string option =
                    let cleanE = ExpressionPatterns.unwrap e
                    match cleanE with
                    | MemberAccess(_, m) when m.Name = "Key" && keyDefs.Length = 1 ->
                        Some keyNameToOutputAliasMap.[fst keyDefs.[0]]
                    | MemberAccess(MemberAccess(_, k), m) when k.Name = "Key" ->
                        let idxOpt =
                            if m.Name = "Item1" || m.Name = "Fst" then Some 0
                            elif m.Name = "Item2" || m.Name = "Snd" then Some 1
                            elif m.Name.StartsWith "Item" then
                                match Int32.TryParse(m.Name.Substring 4) with
                                | true, i -> Some(i - 1)
                                | _ -> None
                            else None
                        match idxOpt with
                        | Some idx when idx >= 0 && idx < keyDefs.Length ->
                            Some keyNameToOutputAliasMap.[fst keyDefs.[idx]]
                        | _ ->
                            keyNameToOutputAliasMap.TryFind m.Name |> Option.orElse (Some m.Name)
                    | MemberAccess(p, m) when isKeyArg p ->
                        let idxOpt =
                            if m.Name = "Item1" || m.Name = "Fst" then Some 0
                            elif m.Name = "Item2" || m.Name = "Snd" then Some 1
                            elif m.Name.StartsWith "Item" then
                                match Int32.TryParse(m.Name.Substring 4) with
                                | true, i -> Some(i - 1)
                                | _ -> None
                            else None
                        match idxOpt with
                        | Some idx when idx >= 0 && idx < keyDefs.Length ->
                            Some keyNameToOutputAliasMap.[fst keyDefs.[idx]]
                        | _ ->
                            keyNameToOutputAliasMap.TryFind m.Name |> Option.orElse (Some m.Name)
                    | _ when isKeyArg cleanE && keyDefs.Length = 1 ->
                        Some keyNameToOutputAliasMap.[fst keyDefs.[0]]
                    | _ -> None

                /// A parameter that stands for the group key, rewritten as a reference to the key column.
                /// Inside a larger expression a grouping parameter is its rows (g.Distinct().Count()), never its key.
                let tryKeyColumnParameter (node: ParameterExpression) : Expression option =
                    if isGroupingType node.Type then
                        None
                    else
                        tryResolveKeyPropertyCol node
                        |> Option.map (fun colName -> Expression.Parameter(node.Type, sprintf "__key_col_%s" colName) :> Expression)

                let tryTranslateDerivedKeyExpr (rawArgExpr: Expression) : ScalarExpr option =
                    let rewritten =
                        { new ExpressionVisitor() with
                            override _.VisitMember(node: MemberExpression) =
                                match tryResolveKeyPropertyCol node with
                                | Some colName ->
                                    Expression.Parameter(node.Type, sprintf "__key_col_%s" colName) :> Expression
                                | None -> base.VisitMember node
                            override _.VisitParameter(node: ParameterExpression) =
                                match tryKeyColumnParameter node with
                                | Some rewritten -> rewritten
                                | None -> base.VisitParameter node
                        }.Visit(rawArgExpr)
                    ExprTranslator.tryTranslateToScalar (TranslationContext.Entity groupParamName) rewritten

                let tryTranslateSingleAgg (rawArgExpr: Expression) : ScalarExpr option =
                    tryTranslateGroupByAggScalar
                        groupParamName
                        targetColExprOpt
                        scalarColOpt
                        wholeRowStructOpt
                        effectiveScopeMap
                        cols
                        rawArgExpr

                let windowAggs = ResizeArray<Expression * string * ScalarExpr>()

                let findOrAddWindowAgg (node: Expression) : string option =
                    let unwrappedNode = ExpressionPatterns.unwrap node
                    let existingProjectedColOpt =
                        List.zip memberNames args
                        |> List.tryPick (fun (colName, argExpr) ->
                            if ExpressionEqualityComparer.Instance.Equals(ExpressionPatterns.unwrap argExpr, unwrappedNode) then
                                Some colName
                            else
                                None)
                    match existingProjectedColOpt with
                    | Some colName when (tryTranslateSingleAgg node).IsSome ->
                        Some colName
                    | _ ->
                        match windowAggs |> Seq.tryPick (fun (expr, c, _) -> if ExpressionEqualityComparer.Instance.Equals(expr, unwrappedNode) then Some c else None) with
                        | Some c -> Some c
                        | None ->
                            let colName = sprintf "__win_agg_%d" windowAggs.Count
                            match tryTranslateSingleAgg node with
                            | Some aggS ->
                                windowAggs.Add((unwrappedNode, colName, aggS))
                                Some colName
                            | None -> None

                let isGroupAggCall (node: MethodCallExpression) =
                    let targetOpt =
                        if not (isNull node.Object) then Some node.Object
                        elif node.Arguments.Count > 0 then Some node.Arguments.[0]
                        else None
                    match targetOpt with
                    | Some target ->
                        let unwrapped = ExpressionPatterns.unwrap target
                        let t = unwrapped.Type
                        isGroupingType t
                        || (match unwrapped with
                            | :? ParameterExpression as p -> p.Name = groupParamName
                            | _ -> false)
                    | None -> false

                let rec rewriteWindowExpr (derivedColsOpt: Map<string, ScalarExpr> option) (expr: Expression) : Expression =
                    { new ExpressionVisitor() with
                        override self.Visit(node: Expression) =
                            if isNull node then null
                            else
                                match derivedColsOpt with
                                | Some derivedMap ->
                                    let unwrappedNode = ExpressionPatterns.unwrap node
                                    let matchingColOpt =
                                        List.zip memberNames args
                                        |> List.tryPick (fun (colName, argExpr) ->
                                            if derivedMap.ContainsKey colName && ExpressionEqualityComparer.Instance.Equals(ExpressionPatterns.unwrap argExpr, unwrappedNode) then
                                                Some colName
                                            else
                                                None)
                                    match matchingColOpt with
                                    | Some colName ->
                                        Expression.Parameter(node.Type, sprintf "__key_col_%s" colName) :> Expression
                                    | None ->
                                        base.Visit node
                                | None ->
                                    base.Visit node
                        override _.VisitMethodCall(node: MethodCallExpression) =
                            if isGroupAggCall node then
                                match findOrAddWindowAgg node with
                                | Some colName ->
                                    Expression.Parameter(node.Type, sprintf "__key_col_%s" colName) :> Expression
                                | None ->
                                    base.VisitMethodCall node
                            else
                                base.VisitMethodCall node
                        override _.VisitMember(node: MemberExpression) =
                            match tryResolveKeyPropertyCol node with
                            | Some colName ->
                                Expression.Parameter(node.Type, sprintf "__key_col_%s" colName) :> Expression
                            | None ->
                                match findOrAddWindowAgg node with
                                | Some colName ->
                                    Expression.Parameter(node.Type, sprintf "__key_col_%s" colName) :> Expression
                                | None ->
                                    base.VisitMember node
                        override _.VisitParameter(node: ParameterExpression) =
                            match tryKeyColumnParameter node with
                            | Some rewritten -> rewritten
                            | None -> base.VisitParameter node
                    }.Visit(expr)

                let tryTranslatePostGroupExpr (derivedColsOpt: Map<string, ScalarExpr> option) (rawArgExpr: Expression) : ScalarExpr option =
                    let beforeCount = windowAggs.Count
                    let rewritten = rewriteWindowExpr derivedColsOpt rawArgExpr
                    match ExprTranslator.tryTranslateToScalar (TranslationContext.Entity groupParamName) rewritten with
                    | Some s -> Some s
                    | None ->
                        while windowAggs.Count > beforeCount do
                            windowAggs.RemoveAt(windowAggs.Count - 1)
                        None

                let classifiedNonKeyArgs =
                    nonKeyArgs
                    |> List.map (fun (colName, argExpr) ->
                        match tryTranslateSingleAgg argExpr with
                        | Some aggS -> Choice1Of3(colName, aggS)
                        | None ->
                            match tryTranslatePostGroupExpr None argExpr with
                            | Some derivedS -> Choice2Of3(colName, derivedS)
                            | None -> Choice3Of3 colName)

                if classifiedNonKeyArgs |> List.exists (function Choice3Of3 _ -> true | _ -> false) then
                    None
                else
                    let derivedKeyColMap =
                        classifiedNonKeyArgs
                        |> List.choose (function Choice2Of3(c, s) -> Some(c, s) | _ -> None)
                        |> Map.ofList

                    let resolveResultColName (e: Expression) : string option =
                        let cleanE = ExpressionPatterns.unwrap e
                        match tryResolveKeyPropertyCol cleanE with
                        | Some colName -> Some colName
                        | None ->
                            match cleanE with
                            | MemberAccess(p, m) when not (isNull p) && p.NodeType = ExpressionType.Parameter -> Some m.Name
                            | resultExpr ->
                                // Any expression equal to a result member (an aggregate call, or a composite
                                // such as `cond ? g.Sum() : null` inlined from a `let`) reuses that column.
                                let unwrappedAgg = ExpressionPatterns.unwrap resultExpr
                                List.zip memberNames args
                                |> List.tryPick (fun (colName, argExpr) ->
                                    if ExpressionEqualityComparer.Instance.Equals(ExpressionPatterns.unwrap argExpr, unwrappedAgg) then
                                        Some colName
                                    else
                                        None)

                    let extraHavingAggs = ResizeArray<Expression * string * ScalarExpr>()

                    let resolveHavingAggColName (aggCall: MethodCallExpression) : string option =
                        match resolveResultColName aggCall with
                        | Some colName -> Some colName
                        | None ->
                            let unwrappedAgg = ExpressionPatterns.unwrap aggCall
                            match extraHavingAggs |> Seq.tryPick (fun (expr, c, _) -> if ExpressionEqualityComparer.Instance.Equals(expr, unwrappedAgg) then Some c else None) with
                            | Some c -> Some c
                            | None ->
                                let colName = sprintf "__having_agg_%d" extraHavingAggs.Count
                                match tryTranslateSingleAgg aggCall with
                                | Some s ->
                                    extraHavingAggs.Add((unwrappedAgg, colName, s))
                                    Some colName
                                | None -> None

                    // Uncorrelated scalar subqueries in having predicates become one-row plans joined to the
                    // grouped rows before the having filters (see havingScalars below).
                    let havingScalars = ResizeArray<SubqueryDecorrelator.UncorrelatedScalar>()

                    let rec translateHaving (expr: Expression) : ScalarExpr option =
                        let foldedExpr =
                            if SubqueryDecorrelator.containsSubquery expr then
                                match SubqueryDecorrelator.tryRewriteUncorrelatedScalars resolveSequence (Set.ofList [ groupParamName ]) expr with
                                | Some(rewritten, scalars) ->
                                    havingScalars.AddRange scalars
                                    rewritten
                                | None -> expr
                            else
                                expr
                        match foldedExpr with
                        | Binary(op, left, right) ->
                            match translateHaving left, translateHaving right with
                            | Some l, Some r -> OperatorAndUdfTranslator.tryTranslateBinary op l r
                            | _ ->
                                tryTranslatePostGroupExpr (Some derivedKeyColMap) foldedExpr
                        | Unary(op, inner) when op = ExpressionType.Convert || op = ExpressionType.ConvertChecked ->
                            match translateHaving inner with
                            | Some innerS ->
                                match ScalarExprOps.tryTranslateCast (match foldedExpr with :? UnaryExpression as u -> u.Type | _ -> typeof<obj>) innerS with
                                | Some castS -> Some castS
                                | None -> Some innerS
                            | None -> tryTranslatePostGroupExpr (Some derivedKeyColMap) foldedExpr
                        | Unary(op, inner) ->
                            match translateHaving inner with
                            | Some innerS -> OperatorAndUdfTranslator.translateUnary op innerS
                            | None -> tryTranslatePostGroupExpr (Some derivedKeyColMap) foldedExpr
                        | Constant _ -> ExprTranslator.tryTranslateToScalar TranslationContext.Empty foldedExpr
                        | other ->
                            match resolveResultColName other with
                            | Some colName -> Some(ScalarExpr.Column colName)
                            | None ->
                                match ExpressionPatterns.unwrap other with
                                | :? MethodCallExpression as aggCall ->
                                    match resolveHavingAggColName aggCall with
                                    | Some c -> Some(ScalarExpr.Column c)
                                    | None -> tryTranslatePostGroupExpr (Some derivedKeyColMap) foldedExpr
                                | _ ->
                                    tryTranslatePostGroupExpr (Some derivedKeyColMap) foldedExpr

                    let havingFilterOpt =
                        ctx.HavingPreds
                        |> List.map (fun havingPred -> translateHaving havingPred.Body)

                    // A derived column whose value depends on other groups (rank, cumulative, window) is computed
                    // where LINQ evaluates it: a `let` bound before the first `where` sees all groups, while the
                    // final `select` (or a later `let`) sees only the groups that survive the having filters.
                    let isPreFilterLet (colName: string) =
                        List.zip memberNames args
                        |> List.exists (fun (memName, argExpr) ->
                            memName = colName
                            && ctx.PreFilterLets
                               |> List.exists (fun letExpr ->
                                   ExpressionEqualityComparer.Instance.Equals(ExpressionPatterns.unwrap letExpr, ExpressionPatterns.unwrap argExpr)))

                    let preFilterDerivedCols, postFilterDerivedCols =
                        derivedKeyColMap
                        |> Map.toList
                        |> List.partition (fun (c, s) -> not (ScalarExprOps.dependsOnOtherRows s) || isPreFilterLet c)

                    let postFilterDerivedNames = postFilterDerivedCols |> List.map fst |> Set.ofList

                    let havingReadsPostFilterCol =
                        havingFilterOpt
                        |> List.exists (function
                            | Some s -> not (Set.intersect (ScalarExprOps.collectColumns s) postFilterDerivedNames).IsEmpty
                            | None -> false)

                    if not (havingFilterOpt |> List.forall Option.isSome) || havingReadsPostFilterCol then
                        None
                    else
                        let havingFilterExprs = havingFilterOpt |> List.choose id
                        let sortStages = fuseSortPairs ctx.SortStages

                        let hasLateSort =
                            sortStages
                            |> List.exists (fun (sortLambda, _, _) ->
                                match resolveResultColName sortLambda.Body with
                                | Some colName when derivedKeyColMap.ContainsKey colName -> true
                                | None ->
                                    match tryTranslateDerivedKeyExpr sortLambda.Body with
                                    | Some _ -> false
                                    | None -> true
                                | _ -> false)

                        let allSortOrders =
                            sortStages
                            |> List.choose (fun (sortLambda, isDesc, nullsLast) ->
                                match resolveResultColName sortLambda.Body with
                                | Some colName ->
                                    Some
                                        {
                                            SortOrder.Expr = ScalarExpr.Column colName
                                            Descending = isDesc
                                            NullsLast = nullsLast
                                        }
                                | None ->
                                    tryTranslateDerivedKeyExpr sortLambda.Body
                                    |> Option.orElseWith (fun () -> tryTranslatePostGroupExpr (Some derivedKeyColMap) sortLambda.Body)
                                    |> Option.map (fun exprS ->
                                        {
                                            SortOrder.Expr = exprS
                                            Descending = isDesc
                                            NullsLast = nullsLast
                                        }))

                        let earlySortOrders =
                            if hasLateSort then
                                sortStages
                                |> List.choose (fun (sortLambda, isDesc, nullsLast) ->
                                    match resolveResultColName sortLambda.Body with
                                    | Some colName when not (derivedKeyColMap.ContainsKey colName) ->
                                        Some
                                            {
                                                SortOrder.Expr = ScalarExpr.Column colName
                                                Descending = isDesc
                                                NullsLast = nullsLast
                                            }
                                    | None ->
                                        tryTranslateDerivedKeyExpr sortLambda.Body
                                        |> Option.map (fun exprS ->
                                            {
                                                SortOrder.Expr = exprS
                                                Descending = isDesc
                                                NullsLast = nullsLast
                                            })
                                    | _ -> None)
                            else
                                allSortOrders

                        let lateSortOrders = if hasLateSort then allSortOrders else []

                        // Taken after the sort keys are translated: they can add window aggregates too.
                        let allAggs =
                            (classifiedNonKeyArgs |> List.choose (function Choice1Of3(c, s) -> Some(c, s) | _ -> None))
                            @ (windowAggs |> Seq.map (fun (_, c, s) -> c, s) |> Seq.toList)
                            @ (extraHavingAggs |> Seq.map (fun (_, c, s) -> c, s) |> Seq.toList)

                        let groupPlan = LogicalPlan.GroupBy(basePlan, keyAliasedExprs, allAggs, None, true)

                        let afterEarlySortPlan =
                            if earlySortOrders.IsEmpty then groupPlan
                            else LogicalPlan.Sort(groupPlan, earlySortOrders)

                        let afterDerivedColsPlan =
                            if preFilterDerivedCols.IsEmpty then afterEarlySortPlan
                            else LogicalPlan.WithColumns(afterEarlySortPlan, preFilterDerivedCols)

                        let afterFilterPlan =
                            havingFilterExprs
                            |> List.fold (fun p f -> LogicalPlan.Filter(p, f)) (SubqueryDecorrelator.attachUncorrelatedScalars afterDerivedColsPlan (List.ofSeq havingScalars))

                        let afterHavingPlan =
                            if postFilterDerivedCols.IsEmpty then afterFilterPlan
                            else LogicalPlan.WithColumns(afterFilterPlan, postFilterDerivedCols)

                        let afterLateSortPlan =
                            if lateSortOrders.IsEmpty then afterHavingPlan
                            else LogicalPlan.Sort(afterHavingPlan, lateSortOrders)

                        let orderedColProjs =
                            List.zip memberNames args
                            |> List.collect (fun (memName, rawArg) ->
                                let arg = ExpressionPatterns.unwrap rawArg
                                if isKeyArg arg then
                                    if keyDefs.Length = 1 then
                                        let c = keyNameToOutputAliasMap.[fst keyDefs.[0]]
                                        [ (c, ScalarExpr.Column c) ]
                                    else
                                        keyDefs |> List.map (fun (kName, _) ->
                                            let c = keyNameToOutputAliasMap.[kName]
                                            (c, ScalarExpr.Column c))
                                else
                                    [ (memName, ScalarExpr.Column memName) ])

                        Some(LogicalPlan.Project(afterLateSortPlan, orderedColProjs))

    /// Pure symbolic GroupPartitionSlice builder (window ranking over partitions + slice filter + projection).
    let buildGroupPartitionSlicePlan
        (basePlan: LogicalPlan)
        (keySelector: LambdaExpression)
        (innerStages: LinqStage list)
        (resSelOpt: LambdaExpression option)
        : LogicalPlan option =

        let cols = (SchemaInference.inferPlanSchema basePlan).ColumnNames
        let keyParam = keySelector.Parameters.[0]
        let keyCtx = PipelineStageParser.createContextWithScopeAndCols Map.empty keyParam.Type cols keyParam.Name

        match tryExtractKeyScalars keyCtx keySelector with
        | None -> None
        | Some partitionKeys ->
            let filteredPlan =
                innerStages
                |> List.fold (fun plan stage ->
                    match stage with
                    | LinqStage.Filter l ->
                        let ctx = PipelineStageParser.createContextWithScopeAndCols Map.empty l.Parameters.[0].Type cols l.Parameters.[0].Name
                        match ExprTranslator.tryTranslateToScalar ctx l.Body with
                        | Some pred -> LogicalPlan.Filter(plan, pred)
                        | None -> plan
                    | _ -> plan) basePlan

            let sortInfoOpt =
                innerStages
                |> List.tryPick (function
                    | LinqStage.Sort(l, isDesc, _) ->
                        let ctx = PipelineStageParser.createContextWithScopeAndCols Map.empty l.Parameters.[0].Type cols l.Parameters.[0].Name
                        ExprTranslator.tryTranslateToScalar ctx l.Body
                        |> Option.map (fun s -> s, isDesc)
                    | _ -> None)

            let takeOpt =
                innerStages
                |> List.tryPick (function
                    | LinqStage.Take n -> Some(int64 n)
                    | _ -> None)

            let skipOpt =
                innerStages
                |> List.tryPick (function
                    | LinqStage.Skip n -> Some(int64 n)
                    | _ -> None)

            let rankExprOpt =
                match sortInfoOpt with
                | Some(sortS, isDesc) ->
                    let rankS =
                        ScalarExpr.Function(
                            ScalarFunction.Rank(RankMethod.Ordinal, None),
                            [ sortS; ScalarExpr.Literal(LiteralValue.Bool isDesc) ],
                            LogicalTypes.UInt32
                        )
                    Some(ScalarExpr.Window(rankS, partitionKeys, []))
                | None when takeOpt.IsSome || skipOpt.IsSome ->
                    if not cols.IsEmpty then
                        let col0 = ScalarExpr.Column cols.[0]
                        let cumCountS =
                            ScalarExpr.Function(
                                ScalarFunction.CumCount,
                                [ col0; ScalarExpr.Literal(LiteralValue.Bool false) ],
                                LogicalTypes.UInt32
                            )
                        Some(ScalarExpr.Window(cumCountS, partitionKeys, []))
                    else
                        None
                | _ -> None

            let slicedPlanOpt =
                rankExprOpt
                |> Option.map (fun rankS ->
                    let cond =
                        match skipOpt, takeOpt with
                        | Some skip, Some take ->
                            ScalarExpr.Binary(
                                BinaryOp.And,
                                ScalarExpr.Binary(BinaryOp.Gt, rankS, ScalarExpr.Literal(LiteralValue.Int64 skip)),
                                ScalarExpr.Binary(BinaryOp.LtEq, rankS, ScalarExpr.Literal(LiteralValue.Int64(skip + take)))
                            )
                        | None, Some take ->
                            ScalarExpr.Binary(BinaryOp.LtEq, rankS, ScalarExpr.Literal(LiteralValue.Int64 take))
                        | Some skip, None ->
                            ScalarExpr.Binary(BinaryOp.Gt, rankS, ScalarExpr.Literal(LiteralValue.Int64 skip))
                        | None, None ->
                            ScalarExpr.Literal(LiteralValue.Bool true)
                    LogicalPlan.Filter(filteredPlan, cond))

            let singleKeyColOpt =
                match keySelector.Body with
                | ExtractColumnName col -> Some col
                | MemberAccess(_, m) -> Some m.Name
                | _ -> None

            let keyColBindings =
                match tryExtractMemberBindings keySelector.Body with
                | Some bindings ->
                    bindings
                    |> List.choose (fun (propName, expr) ->
                        match ExpressionPatterns.unwrap expr with
                        | ExtractColumnName col -> Some(propName, col)
                        | MemberAccess(_, m) -> Some(propName, m.Name)
                        | _ -> None)
                | None -> []

            let keyColMap = Map.ofList keyColBindings

            let selectProjsOpt =
                match resSelOpt with
                | Some resSel when resSel.Parameters.Count >= 2 ->
                    let topParam = resSel.Parameters.[1]
                    let baseCtx = PipelineStageParser.createContextWithScopeAndCols Map.empty topParam.Type cols topParam.Name

                    let translateArg (arg: Expression) : ScalarExpr option =
                        let cleanArg = ExpressionPatterns.unwrap arg
                        match cleanArg with
                        | MemberAccess(p, m) when p.NodeType = ExpressionType.Parameter && isGroupingType p.Type && m.Name = "Key" ->
                            singleKeyColOpt |> Option.map ScalarExpr.Column
                        | MemberAccess(MemberAccess(p, k), m) when p.NodeType = ExpressionType.Parameter && k.Name = "Key" ->
                            let colName =
                                if (m.Name = "Item1" || m.Name = "Fst") && keyColBindings.Length > 0 then
                                    snd keyColBindings.[0]
                                elif (m.Name = "Item2" || m.Name = "Snd") && keyColBindings.Length > 1 then
                                    snd keyColBindings.[1]
                                else
                                    keyColMap.TryFind m.Name |> Option.defaultValue m.Name
                            Some(ScalarExpr.Column colName)
                        | MethodCall(m, _, [ MemberAccess(_, k) ]) when (m.Name = "Fst" || m.Name = "fst") && k.Name = "Key" && keyColBindings.Length > 0 ->
                            Some(ScalarExpr.Column(snd keyColBindings.[0]))
                        | MethodCall(m, _, [ MemberAccess(_, k) ]) when (m.Name = "Snd" || m.Name = "snd") && k.Name = "Key" && keyColBindings.Length > 1 ->
                            Some(ScalarExpr.Column(snd keyColBindings.[1]))
                        | MemberAccess(p, m) when p.NodeType = ExpressionType.Parameter && (match p with :? ParameterExpression as pe -> pe.Name = topParam.Name | _ -> false) ->
                            Some(ScalarExpr.Column m.Name)
                        | _ ->
                            ExprTranslator.tryTranslateToScalar baseCtx cleanArg

                    match tryExtractMemberBindings resSel.Body with
                    | Some bindings ->
                        let exprOpts =
                            bindings
                            |> List.choose (fun (name, argExpr) ->
                                translateArg argExpr
                                |> Option.map (fun s -> name, s))
                        if exprOpts.Length = bindings.Length then
                            Some exprOpts
                        else
                            None
                    | None -> None
                | _ -> None

            match slicedPlanOpt, selectProjsOpt with
            | Some slicedPlan, Some projs -> Some(LogicalPlan.Project(slicedPlan, projs))
            | Some slicedPlan, None -> Some slicedPlan
            | None, Some projs -> Some(LogicalPlan.Project(filteredPlan, projs))
            | None, None -> None

    /// Extracts group aggregations and projections from a GroupBy result selector LambdaExpression.
    let tryExtractGroupByResult
        (currentPlan: LogicalPlan)
        (keyExprs: ScalarExpr list)
        (resLam: LambdaExpression)
        : LogicalPlan option =

        let keyParam = resLam.Parameters.[0]
        let groupParamOpt = if resLam.Parameters.Count >= 2 then Some resLam.Parameters.[1] else None

        let isKeyArg (e: Expression) =
            match ExpressionPatterns.unwrap e with
            | :? ParameterExpression as p when p.Name = keyParam.Name -> true
            | MemberAccess(target, _) when
                (match ExpressionPatterns.unwrap target with :? ParameterExpression as p -> p.Name = keyParam.Name | _ -> false) -> true
            | _ -> false

        let tryTranslateGroupExpr (expr: Expression) : (bool * ScalarExpr) option =
            let clean = ExpressionPatterns.unwrap expr
            if isKeyArg clean then
                match clean with
                | MemberAccess(_, m) -> Some(true, ScalarExpr.Column m.Name)
                | _ ->
                    if keyExprs.Length = 1 then Some(true, keyExprs.[0])
                    else None
            else
                match groupParamOpt with
                | Some gp ->
                    let isGroupTarget (e: Expression) =
                        match ExpressionPatterns.unwrap e with
                        | :? ParameterExpression as p -> p.Name = gp.Name
                        | MethodCall(innerM, _, innerArgs) when innerM.Name = "Select" && innerArgs.Length >= 1 ->
                            match ExpressionPatterns.unwrap innerArgs.[0] with
                            | :? ParameterExpression as p -> p.Name = gp.Name
                            | _ -> false
                        | _ -> false

                    match clean with
                    | MethodCall(m, _, args) when args.Length >= 1 && isGroupTarget args.[0] ->
                        match m.Name with
                        | "Count" | "LongCount" when args.Length = 1 ->
                            Some(false, ScalarExpr.Aggregation(AggKind.Len, None, false))
                        | "Count" | "LongCount" when args.Length = 2 ->
                            match args.[1] with
                            | CleanLambda lam ->
                                let ctx = TranslationContext.Entity lam.Parameters.[0].Name
                                ExprTranslator.tryTranslateToScalar ctx lam.Body
                                |> Option.map (fun s -> false, ScalarExpr.Aggregation(AggKind.Sum, Some (ScalarExpr.Cast(s, LogicalTypes.Int64, false)), false))
                            | _ -> None
                        | "Sum" | "Average" | "Mean" | "Min" | "Max" | "Median"
                        | "First" | "FirstOrDefault" | "Last" | "LastOrDefault"
                        | "Std" | "StdDev" | "Var" | "Variance" ->
                            if args.Length = 2 then
                                match args.[1] with
                                | CleanLambda lam ->
                                    let ctx = TranslationContext.Entity lam.Parameters.[0].Name
                                    ExprTranslator.tryTranslateToScalar ctx lam.Body
                                    |> Option.bind (fun s -> ScalarExprOps.translateReductionOp m.Name s)
                                    |> Option.map (fun s -> false, s)
                                | _ -> None
                            elif args.Length = 1 then
                                match args.[0] with
                                | MethodCall(sm, _, [ _; CleanLambda lam ]) when sm.Name = "Select" ->
                                    let ctx = TranslationContext.Entity lam.Parameters.[0].Name
                                    ExprTranslator.tryTranslateToScalar ctx lam.Body
                                    |> Option.bind (fun s -> ScalarExprOps.translateReductionOp m.Name s)
                                    |> Option.map (fun s -> false, s)
                                | _ -> None
                            else
                                None
                        | "ToList" | "ToArray" ->
                            Some(false, ScalarExpr.Aggregation(AggKind.List, None, false))
                        | _ -> None
                    | _ ->
                        let ctx = TranslationContext.Entity keyParam.Name
                        ExprTranslator.tryTranslateToScalar ctx clean |> Option.map (fun s -> false, s)
                | None ->
                    let ctx = TranslationContext.Entity keyParam.Name
                    ExprTranslator.tryTranslateToScalar ctx clean |> Option.map (fun s -> false, s)

        let bindingsOpt =
            match ExpressionPatterns.unwrap resLam.Body with
            | :? MemberInitExpression as mie ->
                let b =
                    mie.Bindings
                    |> Seq.choose (function
                        | :? MemberAssignment as ma ->
                            tryTranslateGroupExpr ma.Expression
                            |> Option.map (fun (isK, s) -> ma.Member.Name, isK, s)
                        | _ -> None)
                    |> Seq.toList
                if b.Length = mie.Bindings.Count then Some b else None

            | :? NewExpression as ne ->
                let names =
                    if not (isNull ne.Members) && ne.Members.Count = ne.Arguments.Count then
                        ne.Members |> Seq.map (fun m -> m.Name) |> Seq.toList
                    elif not (isNull ne.Constructor) then
                        ne.Constructor.GetParameters() |> Seq.map (fun p -> p.Name) |> Seq.toList
                    else
                        []

                let b =
                    ne.Arguments
                    |> Seq.mapi (fun i arg ->
                        let name = if i < names.Length then names.[i] else sprintf "col_%d" i
                        tryTranslateGroupExpr arg |> Option.map (fun (isK, s) -> name, isK, s))
                    |> Seq.toList

                if b |> List.forall Option.isSome then
                    Some(b |> List.map Option.get)
                else
                    None

            | other ->
                tryTranslateGroupExpr other
                |> Option.map (fun (isK, s) -> [ ("value", isK, s) ])

        match bindingsOpt with
        | Some bindings ->
            let aggs =
                bindings
                |> List.choose (fun (name, isKey, s) ->
                    if isKey then None
                    else Some(name, s))

            let groupPlan = LogicalPlan.GroupBy(currentPlan, keyExprs, aggs, None, true)

            let finalProjs =
                bindings
                |> List.map (fun (name, isKey, s) ->
                    if isKey then
                        match s with
                        | ScalarExpr.Column cName -> name, ScalarExpr.Column cName
                        | _ -> name, s
                    else
                        name, ScalarExpr.Column name)

            Some (LogicalPlan.Project(groupPlan, finalProjs))
        | None ->
            None
