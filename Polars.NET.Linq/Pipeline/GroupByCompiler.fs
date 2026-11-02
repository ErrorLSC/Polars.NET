namespace Polars.NET.Linq

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core
open Polars.NET.Core.Helpers

[<AutoOpen>]
module internal GroupByCompiler =

    let compileCountBy (elemType: Type) (baseLf: LazyFrameHandle) (keyLambda: LambdaExpression) : QueryOp option =
        let keyParam = keyLambda.Parameters.[0]
        let ctx = createContextForType elemType baseLf keyParam.Name

        ExprTranslator.tryTranslateWithContext ctx keyLambda.Body
        |> Option.map (fun keyExpr ->
            QueryOp.GroupBy
                {
                    Keys = [| PolarsWrapper.Alias(keyExpr, "Key") |]
                    Aggs = [| PolarsWrapper.Alias(PolarsWrapper.Len(), "Value") |]
                    Having = None
                    MaintainOrder = false
                })

    /// Helper to translate a single group-by reduction/aggregation into an ExprHandle

    let compileGroupPartitionSlice
        (lf: LazyFrameHandle)
        (keySelector: LambdaExpression)
        (innerStages: LinqStage list)
        (resSelOpt: LambdaExpression option)
        : QueryOp list option =

        let keyParam = keySelector.Parameters.[0]
        let keyCtx = createContextForType keyParam.Type lf keyParam.Name

        let partitionKeysOpt =
            match tryExtractMemberBindings keySelector.Body with
            | Some bindings ->
                let exprs =
                    bindings
                    |> List.choose (fun (_, e) -> ExprTranslator.tryTranslateWithContext keyCtx e)
                if exprs.Length = bindings.Length then Some(List.toArray exprs) else None
            | None ->
                ExprTranslator.tryTranslateWithContext keyCtx keySelector.Body
                |> Option.map (fun h -> [| h |])

        match partitionKeysOpt with
        | None -> None
        | Some partitionKeys ->
            let innerFilters =
                innerStages
                |> List.choose (function
                    | LinqStage.Filter l ->
                        let ctx = createContextForType l.Parameters.[0].Type lf l.Parameters.[0].Name
                        ExprTranslator.tryTranslateWithContext ctx l.Body
                        |> Option.map QueryOp.Filter
                    | _ -> None)

            let sortInfoOpt =
                innerStages
                |> List.tryPick (function
                    | LinqStage.Sort(l, isDesc, _) ->
                        let ctx = createContextForType l.Parameters.[0].Type lf l.Parameters.[0].Name
                        ExprTranslator.tryTranslateWithContext ctx l.Body
                        |> Option.map (fun h -> h, isDesc)
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
                | Some(sortH, isDesc) ->
                    let rankH = PolarsWrapper.Rank(sortH, PlRankMethod.Ordinal, isDesc, System.Nullable())
                    Some(
                        PolarsWrapper.Over(
                            rankH,
                            partitionKeys,
                            [||],
                            descending = false,
                            nullsLast = false,
                            multithreaded = true,
                            maintainOrder = true,
                            mappingCode = PlWindowMapping.GroupsToRows
                        )
                    )
                | None when takeOpt.IsSome || skipOpt.IsSome ->
                    let colNames = PolarsSchemaExtractor.GetColumnNames lf
                    if colNames.Length > 0 then
                        let col0 = PolarsWrapper.Col colNames.[0]
                        let cumCountH = PolarsWrapper.CumCount(col0, false)
                        Some(
                            PolarsWrapper.Over(
                                cumCountH,
                                partitionKeys,
                                [||],
                                descending = false,
                                nullsLast = false,
                                multithreaded = true,
                                maintainOrder = true,
                                mappingCode = PlWindowMapping.GroupsToRows
                            )
                        )
                    else
                        None
                | _ -> None

            let sliceFilterOpOpt =
                rankExprOpt
                |> Option.map (fun rankH ->
                    let cond =
                        match skipOpt, takeOpt with
                        | Some skip, Some take ->
                            PolarsWrapper.And(
                                PolarsWrapper.Gt(PolarsWrapper.CloneExpr rankH, PolarsWrapper.Lit skip),
                                PolarsWrapper.LtEq(rankH, PolarsWrapper.Lit(skip + take))
                            )
                        | None, Some take ->
                            PolarsWrapper.LtEq(rankH, PolarsWrapper.Lit take)
                        | Some skip, None ->
                            PolarsWrapper.Gt(rankH, PolarsWrapper.Lit skip)
                        | None, None -> PolarsWrapper.Lit true

                    QueryOp.Filter cond)

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
                        match ExprTranslator.unwrap expr with
                        | ExtractColumnName col -> Some(propName, col)
                        | MemberAccess(_, m) -> Some(propName, m.Name)
                        | _ -> None)
                | None -> []

            let keyColMap = Map.ofList keyColBindings

            let selectOpOpt =
                match resSelOpt with
                | Some resSel when resSel.Parameters.Count >= 2 ->
                    let gParam = resSel.Parameters.[0]
                    let topParam = resSel.Parameters.[1]
                    let baseCtx = createContextForType topParam.Type lf topParam.Name

                    let translateArg (arg: Expression) : ExprHandle option =
                        let cleanArg = ExprTranslator.unwrap arg
                        match cleanArg with
                        | MemberAccess(p, m) when p.NodeType = ExpressionType.Parameter && p.Type.IsGenericType && p.Type.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>> && m.Name = "Key" ->
                            singleKeyColOpt |> Option.map PolarsWrapper.Col
                        | MemberAccess(MemberAccess(p, k), m) when p.NodeType = ExpressionType.Parameter && k.Name = "Key" ->
                            let colName =
                                if (m.Name = "Item1" || m.Name = "Fst") && keyColBindings.Length > 0 then
                                    snd keyColBindings.[0]
                                elif (m.Name = "Item2" || m.Name = "Snd") && keyColBindings.Length > 1 then
                                    snd keyColBindings.[1]
                                else
                                    keyColMap.TryFind m.Name |> Option.defaultValue m.Name
                            Some(PolarsWrapper.Col colName)
                        | MethodCall(m, _, [ MemberAccess(p, k) ]) when (m.Name = "Fst" || m.Name = "fst") && k.Name = "Key" && keyColBindings.Length > 0 ->
                            Some(PolarsWrapper.Col(snd keyColBindings.[0]))
                        | MethodCall(m, _, [ MemberAccess(p, k) ]) when (m.Name = "Snd" || m.Name = "snd") && k.Name = "Key" && keyColBindings.Length > 1 ->
                            Some(PolarsWrapper.Col(snd keyColBindings.[1]))
                        | MemberAccess(p, m) when p.NodeType = ExpressionType.Parameter && (match p with :? ParameterExpression as pe -> pe.Name = topParam.Name | _ -> false) ->
                            Some(PolarsWrapper.Col m.Name)
                        | _ ->
                            ExprTranslator.tryTranslateWithContext baseCtx cleanArg

                    match tryExtractMemberBindings resSel.Body with
                    | Some bindings ->
                        let exprOpts =
                            bindings
                            |> List.choose (fun (name, argExpr) ->
                                translateArg argExpr
                                |> Option.map (fun h -> PolarsWrapper.Alias(h, name)))
                        if exprOpts.Length = bindings.Length then
                            Some(QueryOp.Select(List.toArray exprOpts))
                        else
                            None
                    | None -> None
                | _ -> None

            let sliceOps =
                match sliceFilterOpOpt with
                | Some f -> [ f ]
                | None -> []

            match selectOpOpt with
            | Some selOp -> Some(selOp :: sliceOps @ innerFilters)
            | None when not sliceOps.IsEmpty -> Some(sliceOps @ innerFilters)
            | _ -> None

    /// Compiles Explode (SelectMany over collection property) into QueryOp.Explode and optional renames

    let tryTranslateGroupByAgg
        (groupParamName: string)
        (targetColExprOpt: ExprHandle option)
        (scalarColOpt: string option)
        (wholeRowStructOpt: ExprHandle option)
        (scopeMap: Map<string * string, string>)
        (lf: LazyFrameHandle)
        (colName: string)
        (rawArgExpr: Expression)
        : ExprHandle option =

        let argExpr = ExprTranslator.unwrap rawArgExpr

        let isGroupParam (e: Expression) =
            match ExprTranslator.unwrap e with
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
                    let len = PolarsWrapper.Len()

                    let int64Dtype =
                        PolarsWrapper.DataTypeExprFromDataType(
                            PolarsWrapper.NewPrimitiveType(int PlDataType.Int64)
                        )

                    PolarsWrapper.ExprCast(len, int64Dtype, strict = false, wrapNumerical = false)
                else
                    PolarsWrapper.Len()

            Some(PolarsWrapper.Alias(core, colName))

        | MethodCall(m, target, callArgs) when
            isGroupReductionCall target callArgs && targetColExprOpt.IsSome
            ->
            let clonedColExpr = PolarsWrapper.CloneExpr targetColExprOpt.Value

            ExprTranslator.translateReductionOp m.Name clonedColExpr
            |> Option.map (fun core -> PolarsWrapper.Alias(core, colName))

        | _ ->
            match tryMatchNestedListAgg argExpr with
            | Some selLambda ->
                let selParam = selLambda.Parameters.[0]
                let selCtx = createContextWithScope scopeMap selParam.Type lf selParam.Name

                let colExprOpt =
                    match ExprTranslator.tryResolveColumnNameWithContext selCtx selLambda.Body with
                    | Some cName -> Some(PolarsWrapper.Col cName)
                    | None -> ExprTranslator.tryTranslateWithContext selCtx selLambda.Body

                colExprOpt |> Option.map (fun cExpr -> PolarsWrapper.Alias(cExpr, colName))
            | None ->
                AggTranslator.tryTranslateAggWithScalarColAndScope
                    groupParamName
                    scalarColOpt
                    wholeRowStructOpt
                    scopeMap
                    argExpr
                    colName

    /// Unified GroupBy compiler powering both single-key and multi-key GroupBy pipelines (C# LINQ & F# QueryBuilder)
    let compileUnifiedGroupBy
        (lf: LazyFrameHandle)
        (ctx: GroupFoldContext)
        (projBody: Expression)
        (groupParamName: string)
        (isKeyArg: Expression -> bool)
        (scopeMap: Map<string * string, string>)
        : QueryOp list option =

        let keyParam = ctx.KeyLambda.Parameters.[0]
        let keyCtx = createContextWithScope scopeMap keyParam.Type lf keyParam.Name

        let elemExprOpt =
            ctx.ElemLambdaOpt
            |> Option.bind (fun el ->
                let elParam = el.Parameters.[0]
                let elCtx = createContextWithScope scopeMap elParam.Type lf elParam.Name
                ExprTranslator.tryTranslateWithContext elCtx el.Body)

        let elemScoreMapOpt =
            ctx.ElemLambdaOpt
            |> Option.bind (fun el ->
                let elParam = el.Parameters.[0]
                match tryExtractMemberBindings el.Body with
                | Some bindings ->
                    let elCtx = createContextWithScope scopeMap elParam.Type lf elParam.Name
                    let resolvedBindings =
                        bindings
                        |> List.choose (fun (propName, expr) ->
                            match ExprTranslator.tryResolveColumnNameWithContext elCtx expr with
                            | Some cName -> Some((propName, cName))
                            | None -> None)
                    let updatedFromEntities =
                        bindings
                        |> List.fold (fun m (propName, expr) ->
                            let subScopeOpt =
                                match ExprTranslator.unwrap expr with
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
                        match ExprTranslator.unwrap el.Body with
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

        let wholeRowStructOpt: ExprHandle option =
            try
                let props = PolarsTypeHelper.GetModelProperties elemType

                if props.Length > 0 && not (isTupleOrAnonType elemType) then
                    let colExprs = props |> Array.map (fun p -> PolarsWrapper.Col p.Name)
                    Some(PolarsWrapper.AsStruct colExprs)
                else
                    None
            with _ ->
                None

        let translateKeyPart (e: Expression) : ExprHandle option =
            match ExprTranslator.tryTranslateWithContext keyCtx e with
            | Some h -> Some h
            | None ->
                match e with
                | MemberAccess(:? ParameterExpression as p, mem) when p.Name = keyParam.Name ->
                    Some(PolarsWrapper.Col mem.Name)
                | _ -> None

        let keyPairsOpt: (string * Expression) list option =
            match ExprTranslator.unwrap ctx.KeyLambda.Body with
            | :? NewExpression as ne when
                ne.Type.IsGenericType && (ne.Type.Name.StartsWith "Tuple`" || ne.Type.Name.StartsWith "ValueTuple`") ->
                let names =
                    ne.Arguments
                    |> Seq.mapi (fun i arg ->
                        match ExprTranslator.unwrap arg with
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

        let keyDefinitionsOpt: (string * ExprHandle) list option =
            keyPairsOpt
            |> Option.bind (fun pairs ->
                let translated =
                    pairs
                    |> List.choose (fun (name, expr) -> translateKeyPart expr |> Option.map (fun h -> (name, h)))

                if translated.Length = pairs.Length then
                    Some translated
                else
                    None)

        match keyDefinitionsOpt with
        | None -> None
        | Some keyDefs ->
            let isKeyExpression (e: Expression) : bool =
                let cleanE = ExprTranslator.unwrap e

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
                    let cleanProj = ExprTranslator.unwrap projBody
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
                                let arg = ExprTranslator.unwrap rawArg

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
                        PolarsWrapper.Alias(rawKeyExpr, outputColName))
                    |> List.toArray

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
                        tryGetScalarColumnName keyParam.Type lf

                let targetColExprOpt =
                    elemExprOpt |> Option.orElse (scalarColOpt |> Option.map PolarsWrapper.Col)

                let tryResolveKeyPropertyCol (e: Expression) : string option =
                    let cleanE = ExprTranslator.unwrap e
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

                let tryTranslateDerivedKeyExpr (rawArgExpr: Expression) : ExprHandle option =
                    let rewritten =
                        { new ExpressionVisitor() with
                            override _.VisitMember(node: MemberExpression) =
                                match tryResolveKeyPropertyCol node with
                                | Some colName ->
                                    Expression.Parameter(node.Type, sprintf "__key_col_%s" colName) :> Expression
                                | None -> base.VisitMember node
                            override _.VisitParameter(node: ParameterExpression) =
                                match tryResolveKeyPropertyCol node with
                                | Some colName ->
                                    Expression.Parameter(node.Type, sprintf "__key_col_%s" colName) :> Expression
                                | None -> base.VisitParameter node
                        }.Visit(rawArgExpr)
                    ExprTranslator.tryTranslateWithContext (TranslationContext.Entity groupParamName) rewritten

                let tryTranslateSingleAgg (colName: string) (rawArgExpr: Expression) : ExprHandle option =
                    tryTranslateGroupByAgg
                        groupParamName
                        targetColExprOpt
                        scalarColOpt
                        wholeRowStructOpt
                        effectiveScopeMap
                        lf
                        colName
                        rawArgExpr

                let windowAggs = ResizeArray<Expression * string * ExprHandle>()

                let findOrAddWindowAgg (node: Expression) : string option =
                    let unwrappedNode = ExprTranslator.unwrap node
                    let existingProjectedColOpt =
                        List.zip memberNames args
                        |> List.tryPick (fun (colName, argExpr) ->
                            if ExpressionEqualityComparer.Instance.Equals(ExprTranslator.unwrap argExpr, unwrappedNode) then
                                Some colName
                            else
                                None)
                    match existingProjectedColOpt with
                    | Some colName when (tryTranslateSingleAgg colName node).IsSome ->
                        Some colName
                    | _ ->
                        match windowAggs |> Seq.tryPick (fun (expr, c, _) -> if ExpressionEqualityComparer.Instance.Equals(expr, unwrappedNode) then Some c else None) with
                        | Some c -> Some c
                        | None ->
                            let colName = sprintf "__win_agg_%d" windowAggs.Count
                            match tryTranslateSingleAgg colName node with
                            | Some aggH ->
                                windowAggs.Add((unwrappedNode, colName, aggH))
                                Some colName
                            | None -> None

                let isGroupAggCall (node: MethodCallExpression) =
                    let targetOpt =
                        if not (isNull node.Object) then Some node.Object
                        elif node.Arguments.Count > 0 then Some node.Arguments.[0]
                        else None
                    match targetOpt with
                    | Some target ->
                        let unwrapped = ExprTranslator.unwrap target
                        let t = unwrapped.Type
                        (not (isNull t) && t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>)
                        || (match unwrapped with
                            | :? ParameterExpression as p -> p.Name = groupParamName
                            | _ -> false)
                    | None -> false

                let rec rewriteWindowExpr (derivedColsOpt: Map<string, ExprHandle> option) (expr: Expression) : Expression =
                    { new ExpressionVisitor() with
                        override self.Visit(node: Expression) =
                            if isNull node then null
                            else
                                match derivedColsOpt with
                                | Some derivedMap ->
                                    let unwrappedNode = ExprTranslator.unwrap node
                                    let matchingColOpt =
                                        List.zip memberNames args
                                        |> List.tryPick (fun (colName, argExpr) ->
                                            if derivedMap.ContainsKey colName && ExpressionEqualityComparer.Instance.Equals(ExprTranslator.unwrap argExpr, unwrappedNode) then
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
                            match tryResolveKeyPropertyCol node with
                            | Some colName ->
                                Expression.Parameter(node.Type, sprintf "__key_col_%s" colName) :> Expression
                            | None -> base.VisitParameter node
                    }.Visit(expr)

                let tryTranslatePostGroupExpr (derivedColsOpt: Map<string, ExprHandle> option) (rawArgExpr: Expression) : ExprHandle option =
                    let beforeCount = windowAggs.Count
                    let rewritten = rewriteWindowExpr derivedColsOpt rawArgExpr
                    match ExprTranslator.tryTranslateWithContext (TranslationContext.Entity groupParamName) rewritten with
                    | Some h -> Some h
                    | None ->
                        while windowAggs.Count > beforeCount do
                            windowAggs.RemoveAt(windowAggs.Count - 1)
                        None

                let classifiedNonKeyArgs =
                    nonKeyArgs
                    |> List.map (fun (colName, argExpr) ->
                        match tryTranslateSingleAgg colName argExpr with
                        | Some aggH -> Choice1Of3(colName, aggH)
                        | None ->
                            match tryTranslatePostGroupExpr None argExpr with
                            | Some derivedH -> Choice2Of3(colName, PolarsWrapper.Alias(derivedH, colName))
                            | None -> Choice3Of3 colName)

                if classifiedNonKeyArgs |> List.exists (function Choice3Of3 _ -> true | _ -> false) then
                    None
                else
                    let aggs =
                        (classifiedNonKeyArgs |> List.choose (function Choice1Of3(_, h) -> Some h | _ -> None))
                        @ (windowAggs |> Seq.map (fun (_, _, h) -> h) |> Seq.toList)
                    let derivedKeyColMap =
                        classifiedNonKeyArgs
                        |> List.choose (function Choice2Of3(c, h) -> Some(c, h) | _ -> None)
                        |> Map.ofList

                    let resolveResultColName (e: Expression) : string option =
                        let cleanE = ExprTranslator.unwrap e

                        match tryResolveKeyPropertyCol cleanE with
                        | Some colName -> Some colName
                        | None ->
                            match cleanE with
                            | MemberAccess(p, m) when not (isNull p) && p.NodeType = ExpressionType.Parameter -> Some m.Name
                            | MethodCall _ as aggCall ->
                                let unwrappedAgg = ExprTranslator.unwrap aggCall
                                List.zip memberNames args
                                |> List.tryPick (fun (colName, argExpr) ->
                                    if ExpressionEqualityComparer.Instance.Equals(ExprTranslator.unwrap argExpr, unwrappedAgg) then
                                        Some colName
                                    else
                                        None)
                            | _ -> None

                    let extraHavingAggs = ResizeArray<Expression * ExprHandle>()

                    let resolveHavingAggColName (aggCall: MethodCallExpression) : string option =
                        match resolveResultColName aggCall with
                        | Some colName -> Some colName
                        | None ->
                            let unwrappedAgg = ExprTranslator.unwrap aggCall
                            match extraHavingAggs |> Seq.tryFindIndex (fun (expr, _) -> ExpressionEqualityComparer.Instance.Equals(expr, unwrappedAgg)) with
                            | Some idx -> Some (sprintf "__having_agg_%d" idx)
                            | None ->
                                let colName = sprintf "__having_agg_%d" extraHavingAggs.Count
                                match tryTranslateSingleAgg colName aggCall with
                                | Some h ->
                                    extraHavingAggs.Add((unwrappedAgg, h))
                                    Some colName
                                | None -> None

                    let rec translateHaving (expr: Expression) : ExprHandle option =
                        let foldedExpr, _ =
                            if SubqueryDecorrelator.containsSubquery expr then
                                SubqueryDecorrelator.foldUncorrelatedSubqueries (Set.ofList [ groupParamName ]) expr
                            else
                                expr, false
                        match foldedExpr with
                        | Binary(op, left, right) ->
                            match translateHaving left, translateHaving right with
                            | Some l, Some r ->
                                try
                                    Some(ExprTranslator.translateBinary op l r)
                                with _ ->
                                    None
                            | _ ->
                                tryTranslatePostGroupExpr (Some derivedKeyColMap) foldedExpr
                        | Unary(op, inner) when op = ExpressionType.Convert || op = ExpressionType.ConvertChecked ->
                            match translateHaving inner with
                            | Some innerH ->
                                match ExprTranslator.tryTranslateCast (match foldedExpr with :? UnaryExpression as u -> u.Type | _ -> typeof<obj>) innerH with
                                | Some castH -> Some castH
                                | None -> Some innerH
                            | None -> tryTranslatePostGroupExpr (Some derivedKeyColMap) foldedExpr
                        | Unary(op, inner) ->
                            match translateHaving inner with
                            | Some innerH -> ExprTranslator.translateUnary op innerH
                            | None -> tryTranslatePostGroupExpr (Some derivedKeyColMap) foldedExpr
                        | Constant _ -> ExprTranslator.tryTranslateWithContext TranslationContext.Empty foldedExpr
                        | other ->
                            match resolveResultColName other with
                            | Some colName -> Some(PolarsWrapper.Col colName)
                            | None ->
                                match ExprTranslator.unwrap other with
                                | :? MethodCallExpression as aggCall ->
                                    match resolveHavingAggColName aggCall with
                                    | Some c -> Some(PolarsWrapper.Col c)
                                    | None -> tryTranslatePostGroupExpr (Some derivedKeyColMap) foldedExpr
                                | _ ->
                                    tryTranslatePostGroupExpr (Some derivedKeyColMap) foldedExpr

                    let havingFilterOpt =
                        ctx.HavingPreds
                        |> List.map (fun havingPred -> translateHaving havingPred.Body)

                    if not (havingFilterOpt |> List.forall Option.isSome) then
                        None
                    else
                        let havingFilterOps = havingFilterOpt |> List.choose id |> List.map QueryOp.Filter
                        let allAggs =
                            (classifiedNonKeyArgs |> List.choose (function Choice1Of3(_, h) -> Some h | _ -> None))
                            @ (windowAggs |> Seq.map (fun (_, _, h) -> h) |> Seq.toList)
                            @ (extraHavingAggs |> Seq.map snd |> Seq.toList)

                        let groupBySpec =
                            {
                                Keys = keyAliasedExprs
                                Aggs = allAggs |> List.toArray
                                Having = None
                                MaintainOrder = false
                            }

                        let groupByOp = QueryOp.GroupBy groupBySpec

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

                        let allSortSpecs =
                            sortStages
                            |> List.choose (fun (sortLambda, isDesc, nullsLast) ->
                                match resolveResultColName sortLambda.Body with
                                | Some colName ->
                                    Some
                                        {
                                            Expr = PolarsWrapper.Col colName
                                            Descending = isDesc
                                            NullsLast = nullsLast
                                        }
                                | None ->
                                    tryTranslateDerivedKeyExpr sortLambda.Body
                                    |> Option.orElseWith (fun () -> tryTranslatePostGroupExpr (Some derivedKeyColMap) sortLambda.Body)
                                    |> Option.map (fun exprH ->
                                        {
                                            Expr = exprH
                                            Descending = isDesc
                                            NullsLast = nullsLast
                                        }))

                        let earlySortSpecs =
                            if hasLateSort then
                                sortStages
                                |> List.choose (fun (sortLambda, isDesc, nullsLast) ->
                                    match resolveResultColName sortLambda.Body with
                                    | Some colName when not (derivedKeyColMap.ContainsKey colName) ->
                                        Some
                                            {
                                                Expr = PolarsWrapper.Col colName
                                                Descending = isDesc
                                                NullsLast = nullsLast
                                            }
                                    | None ->
                                        tryTranslateDerivedKeyExpr sortLambda.Body
                                        |> Option.map (fun exprH ->
                                            {
                                                Expr = exprH
                                                Descending = isDesc
                                                NullsLast = nullsLast
                                            })
                                    | _ -> None)
                            else
                                allSortSpecs

                        let lateSortSpecs = if hasLateSort then allSortSpecs else []

                        let earlySortOps = if earlySortSpecs.IsEmpty then [] else [ QueryOp.Sort earlySortSpecs ]
                        let lateSortOps = if lateSortSpecs.IsEmpty then [] else [ QueryOp.Sort lateSortSpecs ]

                        let preSelectWithColsOps =
                            if not derivedKeyColMap.IsEmpty then
                                let derivedCols = derivedKeyColMap |> Map.toList |> List.map (fun (_, h) -> PolarsWrapper.CloneExpr h) |> List.toArray
                                [ QueryOp.WithColumns derivedCols ]
                            else
                                []

                        let finalSelectOp =
                            let orderedColExprs =
                                List.zip memberNames args
                                |> List.collect (fun (memName, rawArg) ->
                                    let arg = ExprTranslator.unwrap rawArg
                                    if isKeyArg arg then
                                        if keyDefs.Length = 1 then
                                            [ PolarsWrapper.Col keyNameToOutputAliasMap.[fst keyDefs.[0]] ]
                                        else
                                            keyDefs |> List.map (fun (kName, _) -> PolarsWrapper.Col keyNameToOutputAliasMap.[kName])
                                    else
                                        [ PolarsWrapper.Col memName ])
                                |> List.toArray

                            QueryOp.Select orderedColExprs

                        Some(finalSelectOp :: lateSortOps @ havingFilterOps @ preSelectWithColsOps @ earlySortOps @ [ groupByOp ])

