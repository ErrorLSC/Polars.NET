namespace Polars.NET.Linq.IR

open System
open System.Linq.Expressions
open Polars.NET.Linq
open Polars.NET.Linq.IR.GroupByPlanning

/// Multi-dimensional grouping plans (ROLLUP, CUBE, GROUPING SETS).
module internal GroupingSetsPlanning =

    /// Pure symbolic compiler for multi-dimensional grouping sets (ROLLUP, CUBE, GROUPING SETS).
    let buildUnifiedGroupingSetsPlan
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

                    let levels = subsets |> List.map (fun idxs -> idxs, evalGroupingLiterals idxs)

                    let spec: LogicalGroupingSetsSpec =
                        {
                            KeyAliases = keyAliases
                            Aggs = aggs
                            Levels = levels
                            OutputColumns = memberNames
                        }
                    Some(LogicalPlan.GroupingSets(basePlan, spec))
