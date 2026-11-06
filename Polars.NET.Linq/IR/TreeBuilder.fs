namespace Polars.NET.Linq.IR

open System
open System.Reflection
open System.Linq
open System.Linq.Expressions
open Polars.NET.Core.Helpers
open Polars.NET.Linq

/// Constructs pure symbolic LogicalPlan relational trees from LINQ query stages.
module internal TreeBuilder =

    /// Checks whether a two-parameter Join result selector merely packages parameters into a transparent identifier/tuple.
    let private isTransparentJoinResSelector (resSel: LambdaExpression) : bool =
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

    /// Active pattern categorizing an optional Join/Unpivot result selector into transparent (or absent) vs concrete.
    let private (|TransparentOrNoResSelector|ConcreteResSelector|) (resSelOpt: LambdaExpression option) =
        match resSelOpt with
        | Some resSel when not (isTransparentJoinResSelector resSel) -> ConcreteResSelector resSel
        | _ -> TransparentOrNoResSelector

    /// Registers the names a two-parameter result selector gives the joined sides as scopes: its parameters,
    /// which may differ from the key selectors', and the members a transparent selector binds them to
    /// (`new { E = x, D = y }`), so that `t.D.Name` resolves to the right side's column. `leftScope` is None
    /// when the left side is itself a transparent identifier rather than one entity.
    let private aliasResultSelectorScopes
        (leftScope: string option)
        (rightScope: string)
        (resSelOpt: LambdaExpression option)
        (scopeMap: Map<string * string, string>)
        : Map<string * string, string> =
        let copyScope (fromScope: string) (toScope: string) (m: Map<string * string, string>) =
            if fromScope = toScope then
                m
            else
                m |> Map.fold (fun acc (s, c) phys -> if s = fromScope then Map.add (toScope, c) phys acc else acc) m

        match resSelOpt with
        | Some resSel when resSel.Parameters.Count >= 2 ->
            let pLeft, pRight = resSel.Parameters.[0].Name, resSel.Parameters.[1].Name
            let withParams =
                match leftScope with
                | Some l -> scopeMap |> copyScope l pLeft |> copyScope rightScope pRight
                | None -> scopeMap |> copyScope rightScope pRight

            match tryExtractMemberBindings resSel.Body with
            | Some bindings ->
                bindings
                |> List.fold (fun acc (memberName, expr) ->
                    match ExpressionPatterns.unwrap expr with
                    | :? ParameterExpression as p when p.Name = pRight -> copyScope pRight memberName acc
                    | :? ParameterExpression as p when p.Name = pLeft && leftScope.IsSome -> copyScope pLeft memberName acc
                    | _ -> acc) withParams
            | None -> withParams
        | _ -> scopeMap

    /// Scopes of a transparent `let` projection (`x => new { x, f = ... }`), which keeps the row's columns and
    /// appends the computed ones: a member bound to the row parameter becomes a scope over the row's columns,
    /// and a member bound to an existing scope under another name aliases it.
    let private registerLetScopes
        (scopeMap: Map<string * string, string>)
        (plan: LogicalPlan)
        (lam: LambdaExpression)
        : Map<string * string, string> =
        match tryExtractMemberBindings lam.Body with
        | Some bindings when EntityPacking.isAnonymousType lam.ReturnType ->
            let rowParam = lam.Parameters.[0]
            let rowCols = (SchemaInference.inferPlanSchema plan).ColumnNames

            bindings
            |> List.fold (fun acc (memberName, expr) ->
                match ExpressionPatterns.unwrap expr with
                | :? ParameterExpression as p when p.Name = rowParam.Name && scopeMap.IsEmpty && not (isTupleOrAnonType p.Type) ->
                    rowCols |> List.fold (fun m c -> Map.add (memberName, c) c m) acc
                | e when not scopeMap.IsEmpty ->
                    match ColumnResolver.tryExtractScopeName e with
                    | Some scope when scope <> memberName ->
                        scopeMap |> Map.fold (fun m (s, c) phys -> if s = scope then Map.add (memberName, c) phys m else m) acc
                    | _ -> acc
                | _ -> acc) scopeMap
        | _ -> scopeMap

    /// The scope map of a join's result row. A result selector binding the sides to members (a transparent
    /// identifier) places each side's entity, or the left side's previous row, under its member's path;
    /// without a selector the row is the left side's and keeps its scopes. `updated` is the map after the
    /// join registered both sides (`leftScope` only when the left side was one entity, i.e. `preJoin` held
    /// no row path scopes).
    let private joinResultScopes
        (preJoin: Map<string * string, string>)
        (updated: Map<string * string, string>)
        (leftScope: string)
        (rightScope: string)
        (resSelOpt: LambdaExpression option)
        : Map<string * string, string> =
        match resSelOpt with
        | Some resSel when resSel.Parameters.Count >= 2 ->
            let entries scope =
                updated |> Map.toList |> List.choose (fun ((s, m), phys) -> if s = scope then Some(m, phys) else None)
            let leftSide = if EntityPacking.hasRowPathScopes preJoin then RowSide.Row else RowSide.Entity(entries leftScope)
            let sides = [ resSel.Parameters.[0].Name, leftSide; resSel.Parameters.[1].Name, RowSide.Entity(entries rightScope) ]

            tryExtractMemberBindings resSel.Body
            |> Option.bind (EntityPacking.tryReshapeRow updated sides)
            |> Option.defaultWith (fun () -> updated |> Map.filter (fun (s, _) _ -> not (isRowPathScope s)))
        | _ -> updated

    /// Rows a client predicate cannot receive from the native plan: System tuples, which the materializer
    /// reads by position and suffix conventions. Anonymous types and F# AnonymousObject rows, transparent
    /// identifiers included, materialize once their scope entities are packed (EntityPacking.packRow).
    let private isUnmaterializableClientRow (t: Type) =
        isTupleOrAnonType t && not (EntityPacking.isTransparentRowType t)

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

    /// Extracts a sequence of key ScalarExprs from a key selector lambda (supporting scalar columns or anonymous key objects).
    let private tryExtractKeyScalars (ctx: TranslationContext) (keyLambda: LambdaExpression) : ScalarExpr list option =
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
            ExprTranslator.tryTranslateToScalar ctx keyLambda.Body
            |> Option.map (fun s -> [ s ])

    /// Extracts projection bindings (name * ScalarExpr) from a projection LambdaExpression.
    /// Whole entities among the members (a join scope, or the row parameter itself) become struct columns.
    let private extractProjections (ctx: TranslationContext) (plan: LogicalPlan) (body: Expression) : (string * ScalarExpr) list option =
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

            SplitOptContext.Current <- if splitMaxMap.IsEmpty then None else Some splitMaxMap
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
                    FallbackTracker.Diagnostic <- Some(sprintf "Failed projection bindings: %s" (String.concat "; " reasons))
                    None
            finally
                SplitOptContext.Current <- None
        | None ->
            let unwrappedBody = ExpressionPatterns.unwrap body
            match unwrappedBody with
            | MemberAccess _ as ma when not (PolarsTypeHelper.IsScalarType ma.Type) && (ColumnResolver.tryResolveColumnNameWithContext ctx ma).IsNone ->
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
            | other ->
                ExprTranslator.tryTranslateToScalar ctx other
                |> Option.map (fun s ->
                    let colName =
                        match s with
                        | ScalarExpr.Column col -> col
                        | _ ->
                            match ctx with
                            | TranslationContext.Scalar(_, physCol) -> physCol
                            | _ -> "value"
                    [ (colName, s) ])

    let private tryExtractJoinProjections
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

    /// Helper to translate a single group-by reduction/aggregation into a pure ScalarExpr.
    let private tryTranslateGroupByAggScalar
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
    let private buildUnifiedGroupByPlan
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
                            | MethodCall _ as aggCall ->
                                let unwrappedAgg = ExpressionPatterns.unwrap aggCall
                                List.zip memberNames args
                                |> List.tryPick (fun (colName, argExpr) ->
                                    if ExpressionEqualityComparer.Instance.Equals(ExpressionPatterns.unwrap argExpr, unwrappedAgg) then
                                        Some colName
                                    else
                                        None)
                            | _ -> None

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

                    let rec translateHaving (expr: Expression) : ScalarExpr option =
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
                                    Some(OperatorAndUdfTranslator.translateBinary op l r)
                                with _ ->
                                    None
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

                    if not (havingFilterOpt |> List.forall Option.isSome) then
                        None
                    else
                        let havingFilterExprs = havingFilterOpt |> List.choose id
                        let allAggs =
                            (classifiedNonKeyArgs |> List.choose (function Choice1Of3(c, s) -> Some(c, s) | _ -> None))
                            @ (windowAggs |> Seq.map (fun (_, c, s) -> c, s) |> Seq.toList)
                            @ (extraHavingAggs |> Seq.map (fun (_, c, s) -> c, s) |> Seq.toList)

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

                        let groupPlan = LogicalPlan.GroupBy(basePlan, keyAliasedExprs, allAggs, None, true)

                        let afterEarlySortPlan =
                            if earlySortOrders.IsEmpty then groupPlan
                            else LogicalPlan.Sort(groupPlan, earlySortOrders)

                        let afterDerivedColsPlan =
                            if derivedKeyColMap.IsEmpty then afterEarlySortPlan
                            else LogicalPlan.WithColumns(afterEarlySortPlan, derivedKeyColMap |> Map.toList)

                        let afterHavingPlan =
                            havingFilterExprs
                            |> List.fold (fun p f -> LogicalPlan.Filter(p, f)) afterDerivedColsPlan

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

    /// Pure symbolic compiler for multi-dimensional grouping sets (ROLLUP, CUBE, GROUPING SETS).
    let private buildUnifiedGroupingSetsPlan
        (basePlan: LogicalPlan)
        (kind: GroupingKind)
        (keyLambda: LambdaExpression)
        (resLambda: LambdaExpression)
        (scopeMap: Map<string * string, string>)
        : LogicalPlan option =

        let cols = (SchemaInference.inferPlanSchema basePlan).ColumnNames
        let keyParam = keyLambda.Parameters.[0]
        let keyCtx = PipelineStageParser.createContextWithScopeAndCols scopeMap keyParam.Type cols keyParam.Name
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
            match ExpressionPatterns.unwrap keyLambda.Body with
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
                match tryExtractMemberBindings keyLambda.Body with
                | Some pairs -> Some pairs
                | None ->
                    let name =
                        match keyLambda.Body with
                        | ExtractColumnName col -> col
                        | _ -> "Key"
                    Some [ (name, keyLambda.Body) ]

        let keyDefinitionsOpt: (string * ScalarExpr) list option =
            keyPairsOpt
            |> Option.bind (fun pairs ->
                let translated =
                    pairs
                    |> List.choose (fun (name, expr) -> translateKeyPart expr |> Option.map (fun s -> name, s))
                if translated.Length = pairs.Length then
                    Some translated
                else
                    None)

        match keyDefinitionsOpt with
        | None -> None
        | Some keyDefs ->
            let numKeys = keyDefs.Length
            let isTwoParam = resLambda.Parameters.Count >= 2
            let groupParamName =
                if isTwoParam then resLambda.Parameters.[1].Name else resLambda.Parameters.[0].Name

            let isKeyArg (e: Expression) =
                let cleanE = ExpressionPatterns.unwrap e
                if isTwoParam then
                    let keyParamName = resLambda.Parameters.[0].Name
                    match cleanE with
                    | :? ParameterExpression as p when p.Name = keyParamName -> true
                    | MemberAccess(:? ParameterExpression as p, _) when p.Name = keyParamName -> true
                    | _ -> false
                else
                    let gParamName = resLambda.Parameters.[0].Name
                    match cleanE with
                    | MemberAccess(:? ParameterExpression as p, m) when p.Name = gParamName && m.Name = "Key" -> true
                    | MemberAccess(MemberAccess(:? ParameterExpression as p, k), _) when p.Name = gParamName && k.Name = "Key" -> true
                    | _ -> false

            let isKeyExpression (e: Expression) : bool =
                let cleanE = ExpressionPatterns.unwrap e
                if isKeyArg cleanE then
                    true
                else
                    match cleanE with
                    | MemberAccess(target, _) when isKeyArg target -> true
                    | MemberAccess(MemberAccess(p, keyProp), _) when
                        not (isNull p) && p.NodeType = ExpressionType.Parameter && keyProp.Name = "Key" ->
                        true
                    | _ -> false

            let memberBindingsOpt =
                match tryExtractMemberBindings resLambda.Body with
                | Some bindings -> Some bindings
                | None ->
                    let cleanProj = ExpressionPatterns.unwrap resLambda.Body
                    if isKeyExpression cleanProj then
                        let name =
                            match cleanProj with
                            | MemberAccess(_, m) when m.Name <> "Key" -> m.Name
                            | _ -> if keyDefs.Length = 1 then fst keyDefs.[0] else "Key"
                        Some [ (name, resLambda.Body) ]
                    else
                        None

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
                                    elif m.Name = sprintf "Item%d" (keyIdx + 1) || m.Name = "Fst" && keyIdx = 0 || m.Name = "Snd" && keyIdx = 1 then Some memName
                                    else None
                                | MemberAccess(p, m) when isKeyArg p ->
                                    if m.Name = origKeyName then Some memName
                                    elif m.Name = "Key" && keyDefs.Length = 1 then Some memName
                                    elif m.Name = sprintf "Item%d" (keyIdx + 1) || m.Name = "Fst" && keyIdx = 0 || m.Name = "Snd" && keyIdx = 1 then Some memName
                                    else None
                                | _ when isKeyArg arg && keyDefs.Length = 1 -> Some memName
                                | _ -> None)
                            |> Option.defaultValue origKeyName
                        origKeyName, outputColName)
                    |> Map.ofList

                let keyAliases =
                    keyDefs
                    |> List.map (fun (origKeyName, rawKeyExpr) ->
                        let outputColName = keyNameToOutputAliasMap.[origKeyName]
                        outputColName, rawKeyExpr)

                let (|GroupingCall|_|) (expr: Expression) : (string * Type) option =
                    match expr with
                    | MethodCall(m, _, [ propExpr ]) when m.Name = "Grouping" ->
                        let targetKeyNameOpt =
                            match ExpressionPatterns.unwrap propExpr with
                            | MemberAccess(_, mem) ->
                                if mem.Name = "Key" && keyDefs.Length = 1 then Some (fst keyDefs.[0])
                                else Some mem.Name
                            | ExtractColumnName col -> Some col
                            | p when isKeyArg p && keyDefs.Length = 1 -> Some (fst keyDefs.[0])
                            | _ -> None
                        targetKeyNameOpt |> Option.map (fun name -> name, m.ReturnType)
                    | MethodCall(m, _, [ _; CleanLambda lam ]) when m.Name = "Grouping" ->
                        let targetKeyNameOpt =
                            match ExpressionPatterns.unwrap lam.Body with
                            | MemberAccess(_, mem) -> Some mem.Name
                            | ExtractColumnName col -> Some col
                            | p when (ExpressionPatterns.unwrap p).NodeType = ExpressionType.Parameter && keyDefs.Length = 1 ->
                                Some (fst keyDefs.[0])
                            | _ -> None
                        targetKeyNameOpt |> Option.map (fun name -> name, m.ReturnType)
                    | _ -> None

                let containsGroupingCall (e: Expression) : bool =
                    let mutable found = false
                    let visitor =
                        { new ExpressionVisitor() with
                            override _.Visit(node) =
                                if found || isNull node then node
                                else
                                    match node with
                                    | GroupingCall _ ->
                                        found <- true
                                        node
                                    | _ -> base.Visit node }
                    visitor.Visit e |> ignore
                    found

                let rewriteGrouping (subsetSet: Set<int>) (e: Expression) : Expression =
                    let visitor =
                        { new ExpressionVisitor() with
                            override _.Visit(node) =
                                match node with
                                | GroupingCall(targetKeyName, retType) ->
                                    let keyIdxOpt =
                                        keyDefs
                                        |> List.tryFindIndex (fun (k, _) -> k = targetKeyName || keyNameToOutputAliasMap.[k] = targetKeyName)
                                    match keyIdxOpt with
                                    | Some keyIdx ->
                                        let isRolledUp = not (subsetSet.Contains keyIdx)
                                        let valInt = if isRolledUp then 1 else 0
                                        if retType = typeof<byte> then
                                            Expression.Constant(byte valInt, typeof<byte>) :> Expression
                                        elif retType = typeof<int64> then
                                            Expression.Constant(int64 valInt, typeof<int64>) :> Expression
                                        else
                                            Expression.Constant(valInt, typeof<int>) :> Expression
                                    | None -> base.Visit node
                                | _ -> base.Visit node }
                    visitor.Visit e

                let groupingExprMap: Map<string, Expression> =
                    List.zip memberNames args
                    |> List.choose (fun (memName, rawArg) ->
                        if containsGroupingCall rawArg then
                            Some (memName, rawArg)
                        else
                            None)
                    |> Map.ofList

                let nonKeyArgs =
                    List.zip memberNames args
                    |> List.filter (fun (memName, argExpr) ->
                        not (isKeyExpression argExpr) && not (groupingExprMap.ContainsKey memName))

                let scalarColOpt = PipelineStageParser.tryGetScalarColumnNameFromCols keyParam.Type cols
                let targetColExprOpt = scalarColOpt |> Option.map ScalarExpr.Column

                let translatedAggOpts =
                    nonKeyArgs
                    |> List.map (fun (colName, argExpr) ->
                        tryTranslateGroupByAggScalar
                            groupParamName
                            targetColExprOpt
                            scalarColOpt
                            wholeRowStructOpt
                            scopeMap
                            cols
                            argExpr
                        |> Option.map (fun s -> colName, s))

                if not (translatedAggOpts |> List.forall Option.isSome) then
                    None
                else
                    let aggs = translatedAggOpts |> List.choose id

                    let subsets: int list list =
                        match kind with
                        | GroupingKind.Rollup ->
                            [ for len in numKeys .. -1 .. 0 -> [ 0 .. len - 1 ] ]
                        | GroupingKind.Cube ->
                            [ for mask in (1 <<< numKeys) - 1 .. -1 .. 0 ->
                                [ for i in 0 .. numKeys - 1 do
                                    if mask &&& (1 <<< numKeys - 1 - i) <> 0 then yield i ] ]
                        | GroupingKind.GroupingSets setsExpr ->
                            let extractColsFromLambda (lam: LambdaExpression) : string list option =
                                match ExpressionPatterns.unwrap lam.Body with
                                | :? NewExpression as ne ->
                                    if not (isNull ne.Members) && ne.Members.Count > 0 then
                                        Some (ne.Members |> Seq.map (fun m -> m.Name) |> Seq.toList)
                                    else
                                        let cols =
                                            ne.Arguments
                                            |> Seq.choose (fun arg ->
                                                match ExpressionPatterns.unwrap arg with
                                                | ExtractColumnName col -> Some col
                                                | MemberAccess(_, m) -> Some m.Name
                                                | _ -> None)
                                            |> Seq.toList
                                        Some cols
                                | MemberAccess(_, m) ->
                                    Some [ m.Name ]
                                | _ ->
                                    None

                            let mapColNamesToIndices (cNames: string list) : int list =
                                cNames
                                |> List.choose (fun name ->
                                    keyDefs |> List.tryFindIndex (fun (k, _) -> k = name || keyNameToOutputAliasMap.[k] = name))

                            let extractedSetsOpt =
                                match ExpressionPatterns.unwrap setsExpr with
                                | :? ConstantExpression as c when not (isNull c.Value) && (c.Value :? System.Array) && (let elemType = c.Value.GetType().GetElementType() in not (isNull elemType) && typeof<LambdaExpression>.IsAssignableFrom elemType) ->
                                    let arr = c.Value :?> System.Array
                                    let sets =
                                        [ for i in 0 .. arr.Length - 1 do
                                            match arr.GetValue i with
                                            | :? LambdaExpression as lam ->
                                                match extractColsFromLambda lam with
                                                | Some cNames -> yield mapColNamesToIndices cNames
                                                | None -> ()
                                            | _ -> () ]
                                    Some sets
                                | :? NewArrayExpression as nae when nae.Expressions |> Seq.exists (fun elem -> match ExpressionPatterns.unwrap elem with CleanLambda _ -> true | _ -> false) ->
                                    let sets =
                                        nae.Expressions
                                        |> Seq.choose (fun elem ->
                                            match ExpressionPatterns.unwrap elem with
                                            | CleanLambda lam ->
                                                extractColsFromLambda lam |> Option.map mapColNamesToIndices
                                            | _ -> None)
                                        |> Seq.toList
                                    Some sets
                                | :? ConstantExpression as c when not (isNull c.Value) && c.Type = typeof<string[][]> ->
                                    let arr = c.Value :?> string[][]
                                    Some (arr |> Array.map (fun set ->
                                        set |> Array.choose (fun name ->
                                            keyDefs |> List.tryFindIndex (fun (k, _) -> k = name || keyNameToOutputAliasMap.[k] = name)
                                        ) |> Array.toList
                                    ) |> Array.toList)
                                | :? NewArrayExpression as nae ->
                                    let sets =
                                        nae.Expressions
                                        |> Seq.choose (fun elem ->
                                            match ExpressionPatterns.unwrap elem with
                                            | :? ConstantExpression as ce when not (isNull ce.Value) && ce.Type = typeof<string[]> ->
                                                Some(ce.Value :?> string[])
                                            | :? NewArrayExpression as innerNae ->
                                                let strs =
                                                    innerNae.Expressions
                                                    |> Seq.choose (fun sExpr ->
                                                        match ExpressionPatterns.unwrap sExpr with
                                                        | :? ConstantExpression as sc when sc.Type = typeof<string> ->
                                                            Some(sc.Value :?> string)
                                                        | _ -> None)
                                                    |> Seq.toArray
                                                Some strs
                                            | _ -> None)
                                        |> Seq.toArray
                                    Some (sets |> Array.map (fun set ->
                                        set |> Array.choose (fun name ->
                                            keyDefs |> List.tryFindIndex (fun (k, _) -> k = name || keyNameToOutputAliasMap.[k] = name)
                                        ) |> Array.toList
                                    ) |> Array.toList)
                                | _ -> None
                            extractedSetsOpt |> Option.defaultValue [ [ 0 .. numKeys - 1 ] ]

                    let evalGroupingLiterals (subsetIndices: int list) : Map<string, ScalarExpr> =
                        let subsetSet = Set.ofList subsetIndices
                        groupingExprMap
                        |> Map.map (fun _ expr ->
                            let levelExpr = rewriteGrouping subsetSet expr
                            let evaluated = PartialEvaluator.evaluateSubtree levelExpr
                            ScalarExprOps.toLiteral evaluated expr.Type)

                    let level0Literals = evalGroupingLiterals [ 0 .. numKeys - 1 ]
                    let levels = subsets |> List.map (fun idxs -> idxs, evalGroupingLiterals idxs)

                    let spec: LogicalGroupingSetsSpec =
                        {
                            KeyAliases = keyAliases
                            Aggs = aggs
                            Level0Literals = level0Literals
                            Levels = levels
                            OutputColumns = memberNames
                        }
                    Some(LogicalPlan.GroupingSets(basePlan, spec))

    /// Pure symbolic GroupPartitionSlice builder (window ranking over partitions + slice filter + projection).
    let private buildGroupPartitionSlicePlan
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

    /// Pure symbolic GroupJoin with explicit result selector builder (supporting pre-aggregated group counts and null-filling).
    let private buildGroupJoinWithResultPlan
        (resolveSequence: Expression -> LogicalPlan option)
        (basePlan: LogicalPlan)
        (innerExpr: Expression)
        (outerKey: LambdaExpression)
        (innerKey: LambdaExpression)
        (resSel: LambdaExpression)
        : LogicalPlan option =

        let tryExtractGroupAgg (groupParamName: string) (resLambda: LambdaExpression) : (string * ScalarExpr) option =
            match resLambda.Body with
            | :? NewExpression as newExpr ->
                let args = newExpr.Arguments |> Seq.toList
                let memberNames = extractPascalCaseMemberNames newExpr
                List.zip memberNames args
                |> List.tryPick (fun (memName, arg) ->
                    match arg with
                    | MethodCall(m, _, (:? ParameterExpression as p) :: _) when
                        p.Name = groupParamName && (m.Name = "Count" || m.Name = "LongCount")
                        ->
                        Some(memName, ScalarExpr.Aggregation(AggKind.Len, None, false))
                    | _ -> None)
            | _ -> None

        resolveSequence innerExpr
        |> Option.bind (fun rightPlan ->
            let outerParam = outerKey.Parameters.[0]
            let innerParam = innerKey.Parameters.[0]
            let leftCols = (SchemaInference.inferPlanSchema basePlan).ColumnNames
            let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames
            let leftCtx = PipelineStageParser.createContextWithScopeAndCols Map.empty outerParam.Type leftCols outerParam.Name
            let rightCtx = PipelineStageParser.createContextWithScopeAndCols Map.empty innerParam.Type rightCols innerParam.Name

            match
                ExprTranslator.tryTranslateToScalar leftCtx outerKey.Body,
                ExprTranslator.tryTranslateToScalar rightCtx innerKey.Body
            with
            | Some leftKey, Some rightKey ->
                let groupParam = resSel.Parameters.[1]

                let preAggRightPlan, aggColOpt =
                    match tryExtractGroupAgg groupParam.Name resSel with
                    | Some(aggColName, aggScalar) ->
                        LogicalPlan.GroupBy(rightPlan, [ rightKey ], [ (aggColName, aggScalar) ], None, false), Some aggColName
                    | None -> rightPlan, None

                let joinPlan =
                    LogicalPlan.Join(
                        basePlan,
                        preAggRightPlan,
                        [ leftKey ],
                        [ rightKey ],
                        JoinKind.Left,
                        Some LinqConstants.DefaultRightSuffix,
                        JoinKeyCoalescing.Default,
                        false
                    )

                let filledPlan =
                    match aggColOpt with
                    | Some aggCol ->
                        let filledExpr =
                            ScalarExpr.Function(
                                ScalarFunction.FillNull,
                                [ ScalarExpr.Column aggCol; ScalarExpr.Literal(LiteralValue.UInt32 0u) ],
                                LogicalTypes.UInt32
                            )
                        LogicalPlan.WithColumns(joinPlan, [ (aggCol, filledExpr) ])
                    | None -> joinPlan

                let renames = extractBinaryResultRenames resSel None
                let finalPlan =
                    if renames.IsEmpty then filledPlan
                    else LogicalPlan.Rename(filledPlan, renames |> List.map fst, renames |> List.map snd)

                Some finalPlan
            | _ -> None)

    /// Pure symbolic 2-way Zip builder into HorizontalConcat and optional column renames.
    let private buildZipPlan
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
    let private buildZip3Plan
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

    /// Extracts group aggregations and projections from a GroupBy result selector LambdaExpression.
    let private tryExtractGroupByResult
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

    /// Compiles a sequence of LinqStages into a CompiledLogicalQuery, determining pure symbolic
    /// pushdown into LogicalPlan versus client-side fallback boundaries (client predicates, projections, full fallback).
    /// The root source is registered as slot 0; every other source the pipeline reads gets the next slot.
    let buildCompiledQuery
        (backend: IQueryBackend)
        (root: SourceBinding)
        (rootSchema: LogicalSchema)
        (isGrouping: bool)
        (stages: LinqStage list)
        : CompiledLogicalQuery =
        use _scope = BackendScope.Enter backend
        let sources = SourceRegistry()
        let initialPlan = sources.Scan(root, rootSchema)
        let resolveSequence = SourceResolver.resolveSequencePlan backend sources

        let initState: CompiledLogicalQuery =
            {
                Plan = initialPlan
                Sources = [||]
                Residual = ClientResidual.None
                RowScopes = Map.empty
            }

        let fallback (state: CompiledLogicalQuery) : CompiledLogicalQuery =
            { state with Residual = ClientResidual.LinqToObjects }

        /// Appends a client row operation. The first one ends the native plan, whose rows are then
        /// materialized as the operation's input type, so the entities of the join scopes still in effect
        /// are packed into struct columns first.
        let appendClientOp (scopeMap: Map<string * string, string>) (op: ClientRowOp) (state: CompiledLogicalQuery) : CompiledLogicalQuery =
            match state.Residual with
            | ClientResidual.None ->
                match EntityPacking.packRow scopeMap state.Plan op.Lambda.Parameters.[0].Type with
                | RowPacking.NotNeeded -> { state with Residual = ClientResidual.RowOps [ op ] }
                | RowPacking.Packed plan -> { state with Plan = plan; Residual = ClientResidual.RowOps [ op ] }
                | RowPacking.Unresolvable -> fallback state
            | ClientResidual.RowOps ops -> { state with Residual = ClientResidual.RowOps(ops @ [ op ]) }
            | ClientResidual.LinqToObjects -> state

        let clientFilter (scopeMap: Map<string * string, string>) (lam: LambdaExpression) (state: CompiledLogicalQuery) : CompiledLogicalQuery =
            appendClientOp scopeMap (ClientRowOp.Filter(PipelineStageParser.compileClientPredicate lam, lam)) state

        let clientMap (scopeMap: Map<string * string, string>) (lam: LambdaExpression) (state: CompiledLogicalQuery) : CompiledLogicalQuery =
            appendClientOp scopeMap (ClientRowOp.Map(PipelineStageParser.compileToBoxedFunc lam, lam)) state

        let rec processStages (state: CompiledLogicalQuery) (scopeMap: Map<string * string, string>) (remainingStages: LinqStage list) : CompiledLogicalQuery =
            let makeCtx (t: Type) (pName: string) (plan: LogicalPlan) =
                let cols = (SchemaInference.inferPlanSchema plan).ColumnNames
                PipelineStageParser.createContextWithScopeAndCols scopeMap t cols pName

            match remainingStages with
            | [] ->
                match state.Residual with
                | ClientResidual.None -> { state with RowScopes = scopeMap }
                | _ -> state
            | stage :: tail ->
                let canPushdown = ClientResidualOps.isNative state.Residual

                match state.Residual with
                | ClientResidual.LinqToObjects ->
                    // The remaining stages are evaluated by LINQ-to-Objects; nothing more to record.
                    processStages state scopeMap tail
                | ClientResidual.RowOps _ ->
                    match stage with
                    | LinqStage.Filter lam -> processStages (clientFilter scopeMap lam state) scopeMap tail
                    | LinqStage.Project lam -> processStages (clientMap scopeMap lam state) scopeMap tail
                    // Order-insensitive or handled at materialization; the end-of-build check below
                    // escalates combinations the row-op executor cannot express.
                    | LinqStage.Cast _ | LinqStage.OfType _ | LinqStage.DefaultIfEmpty _ | LinqStage.Chunk _ | LinqStage.Shuffle | LinqStage.AggregateBy _ ->
                        processStages state scopeMap tail
                    | _ -> processStages (fallback state) scopeMap tail
                | ClientResidual.None ->
                    match stage with
                    | LinqStage.Filter lam ->
                        match SubqueryDecorrelator.tryDecorrelateFilter resolveSequence state.Plan lam scopeMap with
                        | Some (decorPlan, residualOpt) ->
                            match residualOpt with
                            | Some resLam ->
                                let p = resLam.Parameters.[0]
                                let ctx = makeCtx p.Type p.Name decorPlan
                                match ExprTranslator.tryTranslateToScalar ctx resLam.Body with
                                | Some pred -> processStages { state with Plan = LogicalPlan.Filter(decorPlan, pred) } scopeMap tail
                                | None ->
                                    if isUnmaterializableClientRow p.Type then
                                        processStages (fallback { state with Plan = decorPlan }) scopeMap tail
                                    else
                                        processStages (clientFilter scopeMap resLam { state with Plan = decorPlan }) scopeMap tail
                            | None ->
                                processStages { state with Plan = decorPlan } scopeMap tail
                        | None ->
                            let p = lam.Parameters.[0]
                            let ctx = makeCtx p.Type p.Name state.Plan
                            match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                            | Some pred ->
                                processStages { state with Plan = LogicalPlan.Filter(state.Plan, pred) } scopeMap tail
                            | None ->
                                if isUnmaterializableClientRow p.Type then
                                    processStages (fallback state) scopeMap tail
                                else
                                    processStages (clientFilter scopeMap lam state) scopeMap tail

                    | LinqStage.TakeWhile lam | LinqStage.SkipWhile lam as sStage ->
                        let isSkip = match sStage with LinqStage.SkipWhile _ -> true | _ -> false
                        let p = lam.Parameters.[0]
                        let ctx = makeCtx p.Type p.Name state.Plan
                        match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                        | Some boolExpr ->
                            let mask =
                                if isSkip then
                                    ScalarExpr.Function(ScalarFunction.CumMax, [ ScalarExpr.Unary(UnaryOp.Not, boolExpr) ], LogicalTypes.Boolean)
                                else
                                    ScalarExpr.Function(ScalarFunction.CumMin, [ boolExpr ], LogicalTypes.Boolean)
                            processStages { state with Plan = LogicalPlan.Filter(state.Plan, mask) } scopeMap tail
                        | None ->
                            processStages (clientFilter scopeMap lam state) scopeMap tail

                    | LinqStage.Project lam ->
                        match ExpressionPatterns.unwrap lam.Body with
                        | :? ParameterExpression as p when p.Name = lam.Parameters.[0].Name ->
                            processStages state scopeMap tail
                        | _ ->
                            match SubqueryDecorrelator.tryDecorrelateProjection resolveSequence state.Plan lam scopeMap with
                            | Some (decorPlan, rewrittenLam) ->
                                let p = rewrittenLam.Parameters.[0]
                                let ctx = makeCtx p.Type p.Name decorPlan
                                match extractProjections ctx decorPlan rewrittenLam.Body with
                                | Some projs ->
                                    processStages { state with Plan = LogicalPlan.Project(decorPlan, projs) } Map.empty tail
                                | None ->
                                    processStages (clientMap scopeMap rewrittenLam { state with Plan = decorPlan }) Map.empty tail
                            | None ->
                                let p = lam.Parameters.[0]
                                let ctx = makeCtx p.Type p.Name state.Plan
                                // A projection that keeps the row's entities (a `let`, or F#'s re-wrapping of its
                                // tuple rows) leaves their columns in place: it appends the computed members and
                                // moves the entities to their new paths in the row.
                                let transparentRowOpt =
                                    match tryExtractMemberBindings lam.Body with
                                    | Some bindings when EntityPacking.isTransparentRowType lam.ReturnType ->
                                        let rowCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                                        let named = registerLetScopes scopeMap state.Plan lam
                                        let sides = [ p.Name, EntityPacking.rowSideOf scopeMap rowCols ]
                                        let computedBindings = bindings |> List.filter (fun (_, e) -> not (EntityPacking.holdsEntities e))
                                        // A computed member replacing an existing column would change the entity
                                        // that column belongs to.
                                        let shadowsColumn = computedBindings |> List.exists (fun (name, _) -> List.contains name rowCols)

                                        if computedBindings.Length = bindings.Length || shadowsColumn then
                                            None
                                        else
                                            match EntityPacking.tryReshapeRow named sides bindings with
                                            | Some reshaped ->
                                                let translated =
                                                    computedBindings
                                                    |> List.choose (fun (name, expr) ->
                                                        ExprTranslator.tryTranslateToScalar ctx expr |> Option.map (fun s -> name, s))
                                                if translated.Length = computedBindings.Length then Some(translated, reshaped) else None
                                            | None -> None
                                    | _ -> None

                                match transparentRowOpt with
                                | Some(withCols, reshaped) ->
                                    let plan = if withCols.IsEmpty then state.Plan else LogicalPlan.WithColumns(state.Plan, withCols)
                                    processStages { state with Plan = plan } reshaped tail
                                | None ->
                                    match extractProjections ctx state.Plan lam.Body with
                                    | Some projs ->
                                        processStages { state with Plan = LogicalPlan.Project(state.Plan, projs) } Map.empty tail
                                    | None ->
                                        processStages (clientMap scopeMap lam state) Map.empty tail

                    | LinqStage.Sort(lam, isDesc, nullsLast) ->
                        if isGrouping then
                            processStages state scopeMap tail
                        else
                            let p = lam.Parameters.[0]
                            let ctx = makeCtx p.Type p.Name state.Plan
                            match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                            | Some s ->
                                let newOrder = { SortOrder.Expr = s; Descending = isDesc; NullsLast = nullsLast }
                                let newPlan =
                                    match state.Plan with
                                    | LogicalPlan.Sort(input, prevOrders) ->
                                        LogicalPlan.Sort(input, prevOrders @ [ newOrder ])
                                    | _ ->
                                        LogicalPlan.Sort(state.Plan, [ newOrder ])
                                processStages { state with Plan = newPlan } scopeMap tail
                            | None ->
                                processStages (fallback state) scopeMap tail

                    | LinqStage.SortDefault(isDesc, nullsLast) ->
                        let cols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                        let targetCol = if cols.Length > 0 then cols.[0] else ""
                        let newOrder = { SortOrder.Expr = ScalarExpr.Column targetCol; Descending = isDesc; NullsLast = nullsLast }
                        let newPlan =
                            match state.Plan with
                            | LogicalPlan.Sort(input, prevOrders) ->
                                LogicalPlan.Sort(input, prevOrders @ [ newOrder ])
                            | _ ->
                                LogicalPlan.Sort(state.Plan, [ newOrder ])
                        processStages { state with Plan = newPlan } scopeMap tail

                    | LinqStage.Take count ->
                        processStages { state with Plan = LogicalPlan.Limit(state.Plan, 0L, count) } scopeMap tail

                    | LinqStage.Skip count ->
                        processStages { state with Plan = LogicalPlan.Limit(state.Plan, int64 count, UInt32.MaxValue) } scopeMap tail

                    | LinqStage.TakeLast count ->
                        processStages { state with Plan = LogicalPlan.Limit(state.Plan, -int64 count, count) } scopeMap tail

                    | LinqStage.SkipLast count ->
                        processStages { state with Plan = LogicalPlan.SkipLast(state.Plan, count) } scopeMap tail

                    | LinqStage.Distinct ->
                        processStages { state with Plan = LogicalPlan.Distinct(state.Plan, None, DistinctKeep.First, true) } scopeMap tail

                    | LinqStage.DistinctBy lam ->
                        match PipelineStageParser.extractDistinctColumns lam with
                        | Some cols ->
                            processStages { state with Plan = LogicalPlan.Distinct(state.Plan, Some (Array.toList cols), DistinctKeep.First, true) } scopeMap tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Reverse ->
                        processStages { state with Plan = LogicalPlan.Reverse state.Plan } scopeMap tail

                    | LinqStage.Index ->
                        processStages { state with Plan = LogicalPlan.WithRowIndex(state.Plan, "index", None) } scopeMap tail

                    | LinqStage.CountBy lam ->
                        let p = lam.Parameters.[0]
                        let ctx = makeCtx p.Type p.Name state.Plan
                        match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                        | Some keyExpr ->
                            let groupPlan = LogicalPlan.GroupBy(state.Plan, [ keyExpr ], [ ("Value", ScalarExpr.Aggregation(AggKind.Len, None, false)) ], None, true)
                            let keyCol = match keyExpr with ScalarExpr.Column c -> c | _ -> "Key"
                            let projs = [ ("Key", ScalarExpr.Column keyCol); ("Value", ScalarExpr.Column "Value") ]
                            processStages { state with Plan = LogicalPlan.Project(groupPlan, projs) } Map.empty tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.GroupByKey keyLambda ->
                        if isGrouping then
                            processStages state scopeMap tail
                        else
                            slurpPipelineGroupBy state tail { KeyLambda = keyLambda; ElemLambdaOpt = None; HavingPreds = []; SortStages = [] } Map.empty scopeMap

                    | LinqStage.GroupByWithResult(keyLam, resLam) ->
                        if resLam.Parameters.Count < 2 then
                            if isGrouping then
                                processStages state scopeMap tail
                            else
                                slurpPipelineGroupBy state tail { KeyLambda = keyLam; ElemLambdaOpt = Some resLam; HavingPreds = []; SortStages = [] } Map.empty scopeMap
                        else
                            let keyP = keyLam.Parameters.[0]
                            let keyCtx = makeCtx keyP.Type keyP.Name state.Plan
                            let keyExprs = tryExtractKeyScalars keyCtx keyLam |> Option.defaultValue [ ScalarExpr.Column "Key" ]
                            match tryExtractGroupByResult state.Plan keyExprs resLam with
                            | Some plan -> processStages { state with Plan = plan } Map.empty tail
                            | None ->
                                let resKeyParamName = if resLam.Parameters.Count >= 1 then resLam.Parameters.[0].Name else keyP.Name
                                let isKeyArg (e: Expression) =
                                    match ExpressionPatterns.unwrap e with
                                    | :? ParameterExpression as p when p.Name = resKeyParamName || p.Name = keyP.Name -> true
                                    | MemberAccess(:? ParameterExpression as p, _) when p.Name = resKeyParamName || p.Name = keyP.Name -> true
                                    | _ -> false
                                let ctx: GroupFoldContext = { KeyLambda = keyLam; ElemLambdaOpt = None; HavingPreds = []; SortStages = [] }
                                let groupParamName = if resLam.Parameters.Count >= 2 then resLam.Parameters.[1].Name else resLam.Parameters.[0].Name
                                match buildUnifiedGroupByPlan state.Plan ctx resLam.Body groupParamName isKeyArg scopeMap with
                                | Some nextPlan ->
                                    processStages { state with Plan = nextPlan } Map.empty tail
                                | None ->
                                    processStages (fallback state) scopeMap tail

                    | LinqStage.GroupByWithElementAndResult(keyLam, elemLam, resLam) ->
                        let keyP = keyLam.Parameters.[0]
                        let keyCtx = makeCtx keyP.Type keyP.Name state.Plan
                        let keyExprs = tryExtractKeyScalars keyCtx keyLam |> Option.defaultValue [ ScalarExpr.Column "Key" ]
                        match tryExtractGroupByResult state.Plan keyExprs resLam with
                        | Some plan -> processStages { state with Plan = plan } Map.empty tail
                        | None ->
                            let resKeyParamName = if resLam.Parameters.Count >= 1 then resLam.Parameters.[0].Name else keyP.Name
                            let isKeyArg (e: Expression) =
                                match ExpressionPatterns.unwrap e with
                                | :? ParameterExpression as p when p.Name = resKeyParamName || p.Name = keyP.Name -> true
                                | MemberAccess(:? ParameterExpression as p, _) when p.Name = resKeyParamName || p.Name = keyP.Name -> true
                                | _ -> false
                            let ctx: GroupFoldContext = { KeyLambda = keyLam; ElemLambdaOpt = Some elemLam; HavingPreds = []; SortStages = [] }
                            let groupParamName = if resLam.Parameters.Count >= 2 then resLam.Parameters.[1].Name else resLam.Parameters.[0].Name
                            match buildUnifiedGroupByPlan state.Plan ctx resLam.Body groupParamName isKeyArg scopeMap with
                            | Some nextPlan ->
                                processStages { state with Plan = nextPlan } Map.empty tail
                            | None ->
                                processStages (fallback state) scopeMap tail

                    | LinqStage.GroupByRollup(keyLambda, resLambda) ->
                        match buildUnifiedGroupingSetsPlan state.Plan GroupingKind.Rollup keyLambda resLambda scopeMap with
                        | Some nextPlan ->
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.GroupByCube(keyLambda, resLambda) ->
                        match buildUnifiedGroupingSetsPlan state.Plan GroupingKind.Cube keyLambda resLambda scopeMap with
                        | Some nextPlan ->
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.GroupByGroupingSets(keyLambda, setsExpr, resLambda) ->
                        match buildUnifiedGroupingSetsPlan state.Plan (GroupingKind.GroupingSets setsExpr) keyLambda resLambda scopeMap with
                        | Some nextPlan ->
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.GroupPartitionSlice(keySelector, innerStages, resSelOpt) ->
                        match buildGroupPartitionSlicePlan state.Plan keySelector innerStages resSelOpt with
                        | Some nextPlan ->
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Join(methodInfo, innerExpr, outerKey, innerKey, resSelOpt) ->
                        match resolveSequence innerExpr with
                        | Some rightPlan ->
                            let isRightJoin = methodInfo.Name = "RightJoin"
                            let how =
                                match methodInfo.Name with
                                | "RightJoin"
                                | "LeftJoin"
                                | "GroupJoin" -> JoinKind.Left
                                | "FullJoin" -> JoinKind.Full
                                | "SemiJoin" -> JoinKind.Semi
                                | "AntiJoin" -> JoinKind.Anti
                                | _ -> JoinKind.Inner

                            let leftCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                            let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames
                            let leftColSet = Set.ofList leftCols

                            let baseSuffix = if isRightJoin then LinqConstants.DefaultLeftSuffix else LinqConstants.DefaultRightSuffix
                            let rec findUniqueSuffix (cnt: int) =
                                let candidate = if cnt = 1 then baseSuffix else sprintf "%s%d" baseSuffix cnt
                                let hasConflict = rightCols |> List.exists (fun rc -> leftColSet.Contains (rc + candidate))
                                if not hasConflict then candidate
                                else findUniqueSuffix (cnt + 1)
                            let uniqueSuffix = findUniqueSuffix 1

                            let outerParam = outerKey.Parameters.[0]
                            let innerParam = innerKey.Parameters.[0]
                            let leftCtx = PipelineStageParser.createContextWithScopeAndCols scopeMap outerParam.Type leftCols outerParam.Name
                            let rightCtx = PipelineStageParser.createContextWithScopeAndCols Map.empty innerParam.Type rightCols innerParam.Name

                            match tryExtractKeyScalars leftCtx outerKey, tryExtractKeyScalars rightCtx innerKey with
                            | Some leftKeys, Some rightKeys when leftKeys.Length = rightKeys.Length ->
                                let joinPlan =
                                    if isRightJoin then
                                        LogicalPlan.Join(rightPlan, state.Plan, rightKeys, leftKeys, how, Some uniqueSuffix, JoinKeyCoalescing.KeepBoth, false)
                                    else
                                        LogicalPlan.Join(state.Plan, rightPlan, leftKeys, rightKeys, how, Some uniqueSuffix, JoinKeyCoalescing.KeepBoth, false)

                                let leftScope = outerKey.Parameters.[0].Name
                                let rightScope =
                                    match resSelOpt with
                                    | Some resSel when resSel.Parameters.Count >= 2 -> resSel.Parameters.[1].Name
                                    | _ -> innerKey.Parameters.[0].Name

                                let updatedScopeMap =
                                    PipelineStageParser.updateJoinScopeMap scopeMap leftScope (List.toArray leftCols) rightScope (List.toArray rightCols) uniqueSuffix isRightJoin
                                    |> aliasResultSelectorScopes (if scopeMap.IsEmpty then Some leftScope else None) rightScope resSelOpt
                                // Downstream stages see the result row; the concrete selector itself reads the joined sides.
                                let rowScopeMap = joinResultScopes scopeMap updatedScopeMap leftScope rightScope resSelOpt

                                match resSelOpt with
                                | TransparentOrNoResSelector ->
                                    processStages { state with Plan = joinPlan } rowScopeMap tail
                                | ConcreteResSelector resSel ->
                                    let postJoinCols = (SchemaInference.inferPlanSchema joinPlan).ColumnNames
                                    let pLeft = resSel.Parameters.[0]
                                    let pRight = resSel.Parameters.[1]
                                    let resCtx =
                                        if not updatedScopeMap.IsEmpty then
                                            TranslationContext.ScopedEntity([ pLeft.Name; pRight.Name ], updatedScopeMap, postJoinCols)
                                        else
                                            TranslationContext.BinaryJoin(pLeft.Name, pRight.Name, leftColSet, uniqueSuffix)

                                    match SubqueryDecorrelator.tryDecorrelateProjection resolveSequence joinPlan resSel updatedScopeMap with
                                    | Some (decorPlan, rewrittenResSel) ->
                                        let postDecorCols = (SchemaInference.inferPlanSchema decorPlan).ColumnNames
                                        let pLeft2 = rewrittenResSel.Parameters.[0]
                                        let pRight2 = rewrittenResSel.Parameters.[1]
                                        let resCtx2 =
                                            if not updatedScopeMap.IsEmpty then
                                                TranslationContext.ScopedEntity([ pLeft2.Name; pRight2.Name ], updatedScopeMap, postDecorCols)
                                            else
                                                TranslationContext.BinaryJoin(pLeft2.Name, pRight2.Name, leftColSet, uniqueSuffix)

                                        match tryExtractJoinProjections resCtx2 decorPlan pLeft2 pRight2 leftCols rightCols uniqueSuffix isRightJoin rewrittenResSel.Body with
                                        | Some projs -> processStages { state with Plan = LogicalPlan.Project(decorPlan, projs) } Map.empty tail
                                        | None -> processStages { state with Plan = decorPlan } rowScopeMap tail
                                    | None ->
                                        match tryExtractJoinProjections resCtx joinPlan pLeft pRight leftCols rightCols uniqueSuffix isRightJoin resSel.Body with
                                        | Some projs -> processStages { state with Plan = LogicalPlan.Project(joinPlan, projs) } Map.empty tail
                                        | None -> processStages { state with Plan = joinPlan } rowScopeMap tail
                            | _ ->
                                processStages (fallback state) scopeMap tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.GroupJoin(m, innerExpr, outerKey, innerKey, resSelOpt) ->
                        match tail with
                        | LinqStage.Explode(collSelector, explodeResSelOpt) :: nextTail when (tryExtractDefaultIfEmpty collSelector.Body).IsSome ->
                            let effResSelOpt = explodeResSelOpt |> Option.orElse resSelOpt
                            // The flattening selector reads the group join's row (outer, group), not the outer
                            // entity: that row's members holding the outer side become its path scopes.
                            let groupRowScopeMap =
                                match explodeResSelOpt, resSelOpt with
                                | Some _, Some groupSel when groupSel.Parameters.Count >= 2 ->
                                    let cols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                                    let sides = [ groupSel.Parameters.[0].Name, EntityPacking.rowSideOf scopeMap cols ]
                                    tryExtractMemberBindings groupSel.Body
                                    |> Option.bind (EntityPacking.tryReshapeRow scopeMap sides)
                                    |> Option.defaultValue scopeMap
                                | _ -> scopeMap
                            processStages state groupRowScopeMap (LinqStage.Join(m, innerExpr, outerKey, innerKey, effResSelOpt) :: nextTail)
                        | LinqStage.Project projLambda :: nextTail when resSelOpt.IsNone ->
                            let outerType, innerType = outerKey.Parameters.[0].Type, innerKey.Parameters.[0].Type
                            let desugaredResSel = PipelineStageParser.desugarGroupJoinProject projLambda outerType innerType
                            processStages state scopeMap (LinqStage.GroupJoin(m, innerExpr, outerKey, innerKey, Some desugaredResSel) :: nextTail)
                        | _ ->
                            match resSelOpt with
                            | Some resSel ->
                                match buildGroupJoinWithResultPlan resolveSequence state.Plan innerExpr outerKey innerKey resSel with
                                | Some nextPlan ->
                                    processStages { state with Plan = nextPlan } Map.empty tail
                                | None ->
                                    processStages (fallback state) scopeMap tail
                            | None ->
                                processStages state scopeMap (LinqStage.Join(m, innerExpr, outerKey, innerKey, None) :: tail)

                    | LinqStage.CrossJoin(innerExpr, resSelOpt) ->
                        match resolveSequence innerExpr with
                        | Some rightPlan ->
                            let leftCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                            let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames
                            let leftColSet = Set.ofList leftCols
                            let uniqueSuffix = LinqConstants.DefaultRightSuffix
                            let joinPlan = LogicalPlan.Join(state.Plan, rightPlan, [], [], JoinKind.Cross, Some uniqueSuffix, JoinKeyCoalescing.Default, false)

                            let leftScope = "outer"
                            let rightScope = "inner"
                            let updatedScopeMap =
                                PipelineStageParser.updateJoinScopeMap scopeMap leftScope (List.toArray leftCols) rightScope (List.toArray rightCols) uniqueSuffix false
                                |> aliasResultSelectorScopes (if scopeMap.IsEmpty then Some leftScope else None) rightScope resSelOpt
                            // Downstream stages see the result row; the concrete selector itself reads the joined sides.
                            let rowScopeMap = joinResultScopes scopeMap updatedScopeMap leftScope rightScope resSelOpt

                            match resSelOpt with
                            | TransparentOrNoResSelector ->
                                processStages { state with Plan = joinPlan } rowScopeMap tail
                            | ConcreteResSelector resSel ->
                                let postJoinCols = (SchemaInference.inferPlanSchema joinPlan).ColumnNames
                                let pLeft = resSel.Parameters.[0]
                                let pRight = resSel.Parameters.[1]
                                let resCtx =
                                    if not updatedScopeMap.IsEmpty then
                                        TranslationContext.ScopedEntity([ pLeft.Name; pRight.Name ], updatedScopeMap, postJoinCols)
                                    else
                                        TranslationContext.BinaryJoin(pLeft.Name, pRight.Name, leftColSet, uniqueSuffix)
                                match tryExtractJoinProjections resCtx joinPlan pLeft pRight leftCols rightCols uniqueSuffix false resSel.Body with
                                | Some projs -> processStages { state with Plan = LogicalPlan.Project(joinPlan, projs) } Map.empty tail
                                | None -> processStages { state with Plan = joinPlan } rowScopeMap tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.JoinAsOf(_, inner, outerKey, innerKey, outerByOpt, innerByOpt, resSelOpt, toleranceStr, toleranceInt, toleranceFloat, strategy) ->
                        match resolveSequence inner with
                        | Some rightPlan ->
                            let leftCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                            let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames
                            let leftColSet = Set.ofList leftCols
                            let leftCtx = PipelineStageParser.createContextWithScopeAndCols scopeMap outerKey.Parameters.[0].Type leftCols outerKey.Parameters.[0].Name
                            let rightCtx = PipelineStageParser.createContextWithScopeAndCols Map.empty innerKey.Parameters.[0].Type rightCols innerKey.Parameters.[0].Name
                            match ExprTranslator.tryTranslateToScalar leftCtx outerKey.Body, ExprTranslator.tryTranslateToScalar rightCtx innerKey.Body with
                            | Some leftOn, Some rightOn ->
                                let leftBy =
                                    match outerByOpt with
                                    | Some ob -> tryExtractKeyScalars leftCtx ob |> Option.defaultValue []
                                    | None -> []
                                let rightBy =
                                    match innerByOpt with
                                    | Some ib -> tryExtractKeyScalars rightCtx ib |> Option.defaultValue []
                                    | None -> []
                                let uniqueSuffix = LinqConstants.DefaultRightSuffix
                                let spec: LogicalAsOfJoinSpec =
                                    {
                                        LeftOn = leftOn
                                        RightOn = rightOn
                                        LeftBy = leftBy
                                        RightBy = rightBy
                                        Strategy = strategy
                                        ToleranceStr = toleranceStr
                                        ToleranceInt = toleranceInt
                                        ToleranceFloat = toleranceFloat
                                        AllowEq = true
                                        CheckSorted = true
                                        Suffix = Some uniqueSuffix
                                        Coalesce = JoinKeyCoalescing.KeepBoth
                                        NullsEqual = false
                                    }
                                let asOfPlan = LogicalPlan.JoinAsOf(state.Plan, rightPlan, spec)
                                let leftScope = outerKey.Parameters.[0].Name
                                let rightScope =
                                    match resSelOpt with
                                    | Some resSel when resSel.Parameters.Count >= 2 -> resSel.Parameters.[1].Name
                                    | _ -> innerKey.Parameters.[0].Name

                                let updatedScopeMap =
                                    PipelineStageParser.updateJoinScopeMap scopeMap leftScope (List.toArray leftCols) rightScope (List.toArray rightCols) uniqueSuffix false
                                    |> aliasResultSelectorScopes (if scopeMap.IsEmpty then Some leftScope else None) rightScope resSelOpt
                                // Downstream stages see the result row; the concrete selector itself reads the joined sides.
                                let rowScopeMap = joinResultScopes scopeMap updatedScopeMap leftScope rightScope resSelOpt

                                match resSelOpt with
                                | TransparentOrNoResSelector ->
                                    processStages { state with Plan = asOfPlan } rowScopeMap tail
                                | ConcreteResSelector resSel ->
                                    let postJoinCols = (SchemaInference.inferPlanSchema asOfPlan).ColumnNames
                                    let pLeft = resSel.Parameters.[0]
                                    let pRight = resSel.Parameters.[1]
                                    let resCtx =
                                        if not updatedScopeMap.IsEmpty then
                                            TranslationContext.ScopedEntity([ pLeft.Name; pRight.Name ], updatedScopeMap, postJoinCols)
                                        else
                                            TranslationContext.BinaryJoin(pLeft.Name, pRight.Name, leftColSet, uniqueSuffix)
                                    match tryExtractJoinProjections resCtx asOfPlan pLeft pRight leftCols rightCols uniqueSuffix false resSel.Body with
                                    | Some projs -> processStages { state with Plan = LogicalPlan.Project(asOfPlan, projs) } Map.empty tail
                                    | None -> processStages { state with Plan = asOfPlan } rowScopeMap tail
                            | _ ->
                                processStages (fallback state) scopeMap tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.SetOp(methodInfo, secondExpr, key1Opt, key2Opt) ->
                        match resolveSequence secondExpr with
                        | Some rightPlan ->
                            let how =
                                if methodInfo.Name.StartsWith "Intersect" then
                                    JoinKind.Semi
                                else
                                    JoinKind.Anti

                            let leftCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                            let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames

                            let keysOpt =
                                match key1Opt, key2Opt with
                                | Some k1, Some k2 ->
                                    let p1 = k1.Parameters.[0]
                                    let p2 = k2.Parameters.[0]
                                    let ctx1 = PipelineStageParser.createContextWithScopeAndCols scopeMap p1.Type leftCols p1.Name
                                    let ctx2 = PipelineStageParser.createContextWithScopeAndCols Map.empty p2.Type rightCols p2.Name
                                    match ExprTranslator.tryTranslateToScalar ctx1 k1.Body, ExprTranslator.tryTranslateToScalar ctx2 k2.Body with
                                    | Some lk, Some rk -> Some([ lk ], [ rk ])
                                    | _ -> None
                                | Some k1, None ->
                                    let p1 = k1.Parameters.[0]
                                    let ctx1 = PipelineStageParser.createContextWithScopeAndCols scopeMap p1.Type leftCols p1.Name
                                    match ExprTranslator.tryTranslateToScalar ctx1 k1.Body with
                                    | Some lk when rightCols.Length > 0 ->
                                        Some([ lk ], [ ScalarExpr.Column rightCols.[0] ])
                                    | _ -> None
                                | None, None ->
                                    if leftCols.Length = rightCols.Length && leftCols.Length > 0 then
                                        Some(leftCols |> List.map ScalarExpr.Column, rightCols |> List.map ScalarExpr.Column)
                                    else
                                        Some(leftCols |> List.map ScalarExpr.Column, leftCols |> List.map ScalarExpr.Column)
                                | _ -> None

                            match keysOpt with
                            | Some(leftOn, rightOn) ->
                                processStages { state with Plan = LogicalPlan.Join(state.Plan, rightPlan, leftOn, rightOn, how, None, JoinKeyCoalescing.Default, true) } scopeMap tail
                            | None ->
                                processStages (fallback state) scopeMap tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Union(secondExpr, keyLambdaOpt) ->
                        match resolveSequence secondExpr with
                        | Some otherPlan ->
                            let subsetOpt = keyLambdaOpt |> Option.bind PipelineStageParser.extractDistinctColumns |> Option.map Array.toList
                            let unionPlan = LogicalPlan.Union(state.Plan, otherPlan, isAll = true)
                            processStages { state with Plan = LogicalPlan.Distinct(unionPlan, subsetOpt, DistinctKeep.First, true) } scopeMap tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Concat secondExpr ->
                        match resolveSequence secondExpr with
                        | Some otherPlan ->
                            processStages { state with Plan = LogicalPlan.Union(state.Plan, otherPlan, isAll = true) } scopeMap tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Append elemExpr ->
                        match PartialEvaluator.tryEvaluate elemExpr with
                        | Some elem when not (isNull elem) ->
                            match SourceResolver.singleRowPlan sources elem (elem.GetType()) with
                            | Some otherPlan ->
                                processStages { state with Plan = LogicalPlan.Union(state.Plan, otherPlan, isAll = true) } scopeMap tail
                            | None ->
                                processStages (fallback state) scopeMap tail
                        | _ ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Prepend elemExpr ->
                        match PartialEvaluator.tryEvaluate elemExpr with
                        | Some elem when not (isNull elem) ->
                            match SourceResolver.singleRowPlan sources elem (elem.GetType()) with
                            | Some otherPlan ->
                                processStages { state with Plan = LogicalPlan.Union(otherPlan, state.Plan, isAll = true) } scopeMap tail
                            | None ->
                                processStages (fallback state) scopeMap tail
                        | _ ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Zip(secondExpr, resSelOpt) ->
                        match buildZipPlan resolveSequence state.Plan secondExpr resSelOpt with
                        | Some nextPlan ->
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Zip3(secondExpr, thirdExpr) ->
                        match buildZip3Plan resolveSequence state.Plan secondExpr thirdExpr with
                        | Some nextPlan ->
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Unpivot(colLam, resLambdaOpt) ->
                        let availableCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                        match tryExtractUnpivotPattern colLam resLambdaOpt with
                        | Some pattern ->
                            let explicitIndexCols = pattern.ExplicitIndexCols
                            let onCols = pattern.OnCols
                            let indexCols =
                                if not explicitIndexCols.IsEmpty then explicitIndexCols
                                else availableCols |> List.filter (fun c -> not (Array.contains c onCols))

                            if indexCols.IsEmpty then
                                processStages (fallback state) scopeMap tail
                            else
                                let unpivotPlan = LogicalPlan.Unpivot(state.Plan, indexCols, Some (Array.toList onCols), pattern.VariableName, pattern.ValueName)
                                let postUnpivotCols = indexCols @ [ pattern.VariableName; pattern.ValueName ]
                                let leftScope = colLam.Parameters.[0].Name
                                let innerScope =
                                    match resLambdaOpt with
                                    | Some resSel when resSel.Parameters.Count >= 2 -> resSel.Parameters.[1].Name
                                    | _ -> "q"
                                let updatedScopeMap =
                                    let outerMappings = indexCols |> List.map (fun c -> (leftScope, c), c)
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
                                    |> Map.fold (fun m k v -> Map.add k v m) (Map.ofList (outerMappings @ innerMappings))

                                match resLambdaOpt with
                                | TransparentOrNoResSelector ->
                                    processStages { state with Plan = unpivotPlan } updatedScopeMap tail
                                | ConcreteResSelector resSel ->
                                    let resCtx = TranslationContext.ScopedEntity([ leftScope; innerScope ], updatedScopeMap, postUnpivotCols)
                                    match extractProjections resCtx unpivotPlan resSel.Body with
                                    | Some projs ->
                                        processStages { state with Plan = LogicalPlan.Project(unpivotPlan, projs) } Map.empty tail
                                    | None ->
                                        processStages (clientMap Map.empty resSel { state with Plan = unpivotPlan }) Map.empty tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Explode(colLam, resSelOpt) ->
                        let p = colLam.Parameters.[0]
                        let ctx = makeCtx p.Type p.Name state.Plan
                        match ColumnResolver.tryResolveColumnNameWithContext ctx colLam.Body with
                        | Some col ->
                            let explodePlan = LogicalPlan.Explode(state.Plan, [ col ], true, true)
                            let finalPlan =
                                match resSelOpt with
                                | Some resSel when resSel.Parameters.Count >= 2 ->
                                    let itemParam = resSel.Parameters.[1]
                                    let renames = extractBinaryResultRenames resSel (Some(itemParam.Name, col))
                                    if not renames.IsEmpty then
                                        let oldCols = renames |> List.map fst
                                        let newCols = renames |> List.map snd
                                        LogicalPlan.Rename(explodePlan, oldCols, newCols)
                                    else
                                        explodePlan
                                | _ -> explodePlan
                            processStages { state with Plan = finalPlan } scopeMap tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.OfType(sourceType, targetType) ->
                        if not (targetType.IsAssignableFrom sourceType || sourceType.IsAssignableFrom targetType) then
                            processStages { state with Plan = LogicalPlan.Filter(state.Plan, ScalarExpr.Literal(LiteralValue.Bool false)) } scopeMap tail
                        else
                            processStages state scopeMap tail

                    | LinqStage.Cast _
                    | LinqStage.DefaultIfEmpty _
                    | LinqStage.Chunk _
                    | LinqStage.Shuffle
                    | LinqStage.AggregateBy _ ->
                        processStages state scopeMap tail

        and slurpPipelineGroupBy
            (state: CompiledLogicalQuery)
            (s: LinqStage list)
            (ctx: GroupFoldContext)
            (letBindings: Map<string, Expression>)
            (scopeMap: Map<string * string, string>)
            =
            let substituteLet (expr: Expression) =
                if letBindings.IsEmpty then expr
                else
                    { new ExpressionVisitor() with
                        override self.VisitMember(node: MemberExpression) =
                            let visitedInner = self.Visit(node.Expression)
                            let rec isTransparentTarget (e: Expression) =
                                match ExpressionPatterns.unwrap e with
                                | :? ParameterExpression as p ->
                                    isTupleOrAnonType p.Type || (not (isNull p.Name) && p.Name.Contains "TransparentIdentifier")
                                | MemberAccess(_, m) when isNavigationMember m || isTupleOrAnonType m.DeclaringType ->
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
                slurpPipelineGroupBy state next { ctx with HavingPreds = ctx.HavingPreds @ [ newPred ] } letBindings scopeMap
            | LinqStage.Sort(sortLambda, isDesc, nullsLast) :: next ->
                let newBody = substituteLet sortLambda.Body
                let newSort = Expression.Lambda(newBody, sortLambda.Parameters)
                slurpPipelineGroupBy state next { ctx with SortStages = ctx.SortStages @ [ (newSort, isDesc, nullsLast) ] } letBindings scopeMap
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
                                        match ExpressionPatterns.unwrap expr with
                                        | :? ParameterExpression as p when
                                            (isGroupingType p.Type)
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
                            match ExpressionPatterns.unwrap expr with
                            | :? ParameterExpression as p when p.Name = param.Name && isGroupingType p.Type ->
                                Some(name, p :> Expression)
                            | _ -> None)
                    let updatedBindings =
                        bindings
                        |> List.fold (fun m (name, expr) ->
                            let subExpr = substituteLet expr
                            let normalizedExpr =
                                match gParamOpt with
                                | Some(_, gParamExpr) ->
                                    { new ExpressionVisitor() with
                                        override _.VisitParameter(p: ParameterExpression) =
                                            if p.Name = param.Name then gParamExpr
                                            else base.VisitParameter p
                                    }.Visit(subExpr)
                                | None -> subExpr
                            match ExpressionPatterns.unwrap normalizedExpr with
                            | :? ParameterExpression as p when p.Name = param.Name && isTupleOrAnonType p.Type ->
                                m
                            | _ ->
                                Map.add name normalizedExpr m) letBindings
                    slurpPipelineGroupBy state next ctx updatedBindings scopeMap
                | None ->
                    let finalProjBody = substituteLet projLambda.Body
                    let isKeyArg (e: Expression) =
                        match ExpressionPatterns.unwrap e with
                        | MemberAccess(p, m) when
                            not (isNull p) && (p.NodeType = ExpressionType.Parameter || isNavigationMember (match p with :? MemberExpression as me -> me.Member | _ -> null)) && m.Name = "Key"
                            ->
                            true
                        // A bare projection parameter bound as a member value stands for the key: F# `let` after
                        // groupBy carries the grouping as Item1 and only reads Item1.Key afterwards.
                        | :? ParameterExpression as p when p.Name = projLambda.Parameters.[0].Name ->
                            true
                        | _ -> false

                    let gParamName =
                        match letBindings |> Map.tryPick (fun _ v -> match ExpressionPatterns.unwrap v with :? ParameterExpression as p when isGroupingType p.Type -> Some p.Name | _ -> None) with
                        | Some name -> name
                        | None -> projLambda.Parameters.[0].Name

                    match buildUnifiedGroupByPlan state.Plan ctx finalProjBody gParamName isKeyArg scopeMap with
                    | Some nextPlan ->
                        processStages { state with Plan = nextPlan } Map.empty next
                    | None ->
                        processStages (fallback state) scopeMap next
            | [] ->
                if isGrouping then
                    // The rows are the grouped elements, materialized as the key selector's input type; the
                    // residual having predicates run over the groups.
                    let grouped = { state with RowScopes = scopeMap }
                    ctx.HavingPreds |> List.fold (fun acc havingPred -> clientFilter Map.empty havingPred acc) grouped
                else
                    (fallback state)
            | otherHead :: otherTail ->
                processStages (fallback state) scopeMap (otherHead :: otherTail)

        let finalState = processStages initState Map.empty stages

        let needsLinqToObjects = stages |> List.exists LinqStageTraits.breaksRowStream

        let completed =
            match finalState.Residual with
            | ClientResidual.RowOps _ when needsLinqToObjects -> fallback finalState
            | _ -> finalState

        { completed with Sources = sources.ToArray() }
