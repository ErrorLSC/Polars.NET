namespace Polars.NET.Linq.IR

open System
open System.Linq.Expressions
open Polars.NET.Linq
open Polars.NET.Linq.IR.GroupByPlanning

/// Multi-dimensional grouping plans (ROLLUP, CUBE, GROUPING SETS).
///
/// The result selector becomes `Project(GroupingSets(..), members)`. The GroupingSets node outputs every key, the
/// aggregates the members need, and one hidden per-level literal column per distinct `Grouping` / `GroupingId`
/// call. Each member is then a key column, an aggregate column, or a post-grouping expression over those columns,
/// so a `Grouping` call mixes freely with keys and aggregates (`k.B.Grouping() == 0 ? k.A : null`).
module internal GroupingSetsPlanning =

    /// The 0-based position a tuple-style member name (Item1, Fst, Snd) stands for.
    let private itemIndex (name: string) : int option =
        if name = "Fst" then Some 0
        elif name = "Snd" then Some 1
        elif name.StartsWith "Item" then
            match Int32.TryParse(name.Substring 4) with
            | true, i when i >= 1 -> Some(i - 1)
            | _ -> None
        else None

    /// A `Grouping` / `GroupingId` marker call of the public API (Polars.CSharp.Linq.LinqExtensions).
    let private isGroupingMarker (m: Reflection.MethodInfo) =
        (m.Name = "Grouping" || m.Name = "GroupingId")
        && not (isNull m.DeclaringType)
        && m.DeclaringType.Name = "LinqExtensions"

    /// `k => new { }`: the partial evaluator folds the parameter-free body into a constant of a member-less type.
    let private (|EmptyObjectConstant|_|) (e: Expression) =
        match e with
        | :? ConstantExpression as c when
            not (isNull c.Value) && isTupleOrAnonType c.Type && c.Type.GetProperties().Length = 0
            ->
            Some()
        | _ -> None

    /// The value of GROUPING_ID(keys) in a level grouping by `subset`: one bit per listed key, the first key
    /// the most significant, set when the key is rolled up (outside the subset).
    let groupingIdValue (subset: Set<int>) (keyIdxs: int list) : int =
        let m = keyIdxs.Length
        keyIdxs
        |> List.mapi (fun j i -> if subset.Contains i then 0 else 1 <<< (m - 1 - j))
        |> List.sum

    /// The key subsets (by key index) of each level, in level order.
    let private extractSubsets
        (kind: GroupingKind)
        (numKeys: int)
        (keyIndexOfSetName: string -> int option)
        : int list list =
        match kind with
        | GroupingKind.Rollup -> [ for len in numKeys .. -1 .. 0 -> [ 0 .. len - 1 ] ]
        | GroupingKind.Cube ->
            [ for mask in (1 <<< numKeys) - 1 .. -1 .. 0 ->
                [ for i in 0 .. numKeys - 1 do
                    if mask &&& (1 <<< numKeys - 1 - i) <> 0 then yield i ] ]
        | GroupingKind.GroupingSets setsExpr ->
            let extractColsFromLambda (lam: LambdaExpression) : string list option =
                match ExpressionPatterns.unwrap lam.Body with
                | :? NewExpression as ne ->
                    if not (isNull ne.Members) && ne.Members.Count > 0 then
                        Some(ne.Members |> Seq.map (fun m -> m.Name) |> Seq.toList)
                    else
                        ne.Arguments
                        |> Seq.choose (fun arg ->
                            match ExpressionPatterns.unwrap arg with
                            | ExtractColumnName col -> Some col
                            | MemberAccess(_, m) -> Some m.Name
                            | _ -> None)
                        |> Seq.toList
                        |> Some
                | MemberAccess(_, m) -> Some [ m.Name ]
                | EmptyObjectConstant -> Some []
                | _ -> None

            let toIndices (names: string seq) = names |> Seq.choose keyIndexOfSetName |> Seq.toList

            let extractedSetsOpt =
                match ExpressionPatterns.unwrap setsExpr with
                | :? ConstantExpression as c when
                    not (isNull c.Value)
                    && (c.Value :? Array)
                    && (let elemType = c.Value.GetType().GetElementType()
                        not (isNull elemType) && typeof<LambdaExpression>.IsAssignableFrom elemType)
                    ->
                    let arr = c.Value :?> Array
                    Some
                        [ for i in 0 .. arr.Length - 1 do
                            match arr.GetValue i with
                            | :? LambdaExpression as lam ->
                                match extractColsFromLambda lam with
                                | Some cNames -> yield toIndices cNames
                                | None -> ()
                            | _ -> () ]
                | :? NewArrayExpression as nae when
                    nae.Expressions |> Seq.exists (fun elem -> match ExpressionPatterns.unwrap elem with CleanLambda _ -> true | _ -> false)
                    ->
                    nae.Expressions
                    |> Seq.choose (fun elem ->
                        match ExpressionPatterns.unwrap elem with
                        | CleanLambda lam -> extractColsFromLambda lam |> Option.map toIndices
                        | _ -> None)
                    |> Seq.toList
                    |> Some
                | :? ConstantExpression as c when not (isNull c.Value) && c.Type = typeof<string[][]> ->
                    (c.Value :?> string[][]) |> Array.map toIndices |> Array.toList |> Some
                | :? NewArrayExpression as nae ->
                    nae.Expressions
                    |> Seq.choose (fun elem ->
                        match ExpressionPatterns.unwrap elem with
                        | :? ConstantExpression as ce when not (isNull ce.Value) && ce.Type = typeof<string[]> ->
                            Some(toIndices (ce.Value :?> string[]))
                        | :? NewArrayExpression as innerNae ->
                            innerNae.Expressions
                            |> Seq.choose (fun sExpr ->
                                match ExpressionPatterns.unwrap sExpr with
                                | :? ConstantExpression as sc when sc.Type = typeof<string> -> Some(sc.Value :?> string)
                                | _ -> None)
                            |> toIndices
                            |> Some
                        | _ -> None)
                    |> Seq.toList
                    |> Some
                | _ -> None
            extractedSetsOpt |> Option.defaultValue [ [ 0 .. numKeys - 1 ] ]

    /// Pure symbolic compiler for multi-dimensional grouping sets (ROLLUP, CUBE, GROUPING SETS).
    let buildUnifiedGroupingSetsPlan
        (basePlan: LogicalPlan)
        (kind: GroupingKind)
        (keyLambda: LambdaExpression)
        (resLambda: LambdaExpression)
        (rowShape: RowShape)
        : LogicalPlan option =

        let cols = (SchemaInference.inferPlanSchema basePlan).ColumnNames
        let keyParam = keyLambda.Parameters.[0]
        let keyCtx = PipelineStageParser.createContextWithScopeAndCols rowShape keyParam.Type cols keyParam.Name

        let wholeRowStructOpt = EntityPacking.tryWholeRowStruct keyParam.Type cols

        let translateKeyPart (e: Expression) : ScalarExpr option =
            match ExprTranslator.tryTranslateToScalar keyCtx e with
            | Some s -> Some s
            | None ->
                match e with
                | MemberAccess(:? ParameterExpression as p, mem) when p.Name = keyParam.Name ->
                    Some(ScalarExpr.Column mem.Name)
                | _ -> None

        // A composite key (anonymous object, tuple) is reached through its members; a scalar key is a value.
        let keyIsComposite =
            match ExpressionPatterns.unwrap keyLambda.Body with
            | :? NewExpression
            | :? MemberInitExpression -> true
            | _ -> false

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
                Some(List.zip names (ne.Arguments |> Seq.toList))
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
                    pairs |> List.choose (fun (name, expr) -> translateKeyPart expr |> Option.map (fun s -> name, s))
                if translated.Length = pairs.Length then Some translated else None)

        let memberBindingsOpt (isKeyRef: Expression -> bool) =
            match tryExtractMemberBindings resLambda.Body with
            | Some bindings -> Some bindings
            | None ->
                if isKeyRef resLambda.Body then
                    let name =
                        match ExpressionPatterns.unwrap resLambda.Body with
                        | MemberAccess(_, m) when m.Name <> "Key" -> m.Name
                        | _ ->
                            match keyDefinitionsOpt with
                            | Some [ (k, _) ] -> k
                            | _ -> "Key"
                    Some [ (name, resLambda.Body) ]
                else
                    None

        match keyDefinitionsOpt with
        | None -> None
        | Some keyDefs ->
            let numKeys = keyDefs.Length
            let isTwoParam = resLambda.Parameters.Count >= 2
            let groupParamName =
                if isTwoParam then resLambda.Parameters.[1].Name else resLambda.Parameters.[0].Name

            let isParamNamed (name: string) (e: Expression) =
                match e with
                | :? ParameterExpression as p -> p.Name = name
                | _ -> false

            /// The whole key: `key` in a two-parameter selector, `g.Key` in a one-parameter one.
            let isWholeKey (e: Expression) =
                if isNull e then
                    false
                else
                    let e = ExpressionPatterns.unwrap e
                    if isTwoParam then
                        isParamNamed resLambda.Parameters.[0].Name e
                    else
                        match e with
                        | MemberAccess(p, m) when m.Name = "Key" && not (isNull p) ->
                            isParamNamed groupParamName (ExpressionPatterns.unwrap p)
                        | _ -> false

            /// The key a member of a composite key stands for: by key name, else by tuple position.
            let keyIdxOfMember (name: string) =
                if not keyIsComposite then
                    None
                else
                    keyDefs
                    |> List.tryFindIndex (fun (k, _) -> k = name)
                    |> Option.orElse (itemIndex name |> Option.filter (fun i -> i < numKeys))

            /// The key an expression reads: a scalar key itself, or one member of a composite key.
            let tryResolveKeyIdx (e: Expression) : int option =
                if isNull e then
                    None
                else
                    let e = ExpressionPatterns.unwrap e
                    if isWholeKey e then
                        if keyIsComposite then None else Some 0
                    else
                        match e with
                        | MemberAccess(target, m) when isWholeKey target -> keyIdxOfMember m.Name
                        | _ -> None

            /// The keys a `GroupingId` / `Grouping` selector lambda lists, in order.
            let lambdaKeyIdxs (lam: LambdaExpression) : int list option =
                let lamParam = lam.Parameters.[0]
                let one (e: Expression) =
                    match ExpressionPatterns.unwrap e with
                    | :? ParameterExpression as p when p.Name = lamParam.Name -> if keyIsComposite then None else Some 0
                    | MemberAccess(p, m) when isParamNamed lamParam.Name (ExpressionPatterns.unwrap p) -> keyIdxOfMember m.Name
                    | _ -> None
                match ExpressionPatterns.unwrap lam.Body with
                | :? NewExpression as ne when keyIsComposite || ne.Arguments.Count = 0 ->
                    let idxs = ne.Arguments |> Seq.map one |> Seq.toList
                    if idxs |> List.forall Option.isSome then Some(idxs |> List.choose id) else None
                | EmptyObjectConstant -> Some []
                | body -> one body |> Option.map List.singleton

            /// A `Grouping` / `GroupingId` call: Some(Some keys) when its keys resolve, Some None when they do not.
            let tryGroupingCall (e: Expression) : int list option option =
                match e with
                | :? MethodCallExpression as mc when isGroupingMarker mc.Method ->
                    let args = mc.Arguments |> Seq.toList
                    let keys =
                        match mc.Method.Name, args with
                        | "Grouping", [ prop ] -> tryResolveKeyIdx prop |> Option.map List.singleton
                        | "Grouping", [ key; CleanLambda lam ] when isWholeKey key ->
                            lambdaKeyIdxs lam |> Option.filter (fun idxs -> idxs.Length = 1)
                        | "GroupingId", [ key ] when isWholeKey key -> Some [ 0 .. numKeys - 1 ]
                        | "GroupingId", [ key; CleanLambda lam ] when isWholeKey key -> lambdaKeyIdxs lam
                        | _ -> None
                    // An int holds the bits of at most 31 keys.
                    Some(keys |> Option.filter (fun idxs -> idxs.Length <= 31))
                | _ -> None

            let memberNamesAndArgs = memberBindingsOpt (fun e -> (tryResolveKeyIdx e).IsSome)

            match memberNamesAndArgs with
            | None -> None
            | Some memberBindings ->
                let memberNames = memberBindings |> List.map fst

                // Each key's output column: the first member holding exactly that key, else the key's own name
                // (prefixed when a member of that name holds something else).
                let keyAliasNames =
                    keyDefs
                    |> List.mapi (fun keyIdx (origKeyName, _) ->
                        memberBindings
                        |> List.tryPick (fun (memName, arg) -> if tryResolveKeyIdx arg = Some keyIdx then Some memName else None)
                        |> Option.defaultWith (fun () ->
                            if List.contains origKeyName memberNames then "__group_key_" + origKeyName else origKeyName))

                let keyAliases = List.zip keyAliasNames (keyDefs |> List.map snd)

                let keyIndexOfSetName (name: string) =
                    keyDefs
                    |> List.indexed
                    |> List.tryPick (fun (i, (k, _)) -> if k = name || keyAliasNames.[i] = name then Some i else None)

                let scalarColOpt = PipelineStageParser.tryGetScalarColumnNameFromCols keyParam.Type cols
                let targetColExprOpt = scalarColOpt |> Option.map ScalarExpr.Column

                let tryTranslateSingleAgg (argExpr: Expression) =
                    tryTranslateGroupByAggScalar groupParamName targetColExprOpt scalarColOpt wholeRowStructOpt rowShape cols argExpr

                let isGroupAggCall (node: MethodCallExpression) =
                    let targetOpt =
                        if not (isNull node.Object) then Some node.Object
                        elif node.Arguments.Count > 0 then Some node.Arguments.[0]
                        else None
                    match targetOpt with
                    | Some target ->
                        let unwrapped = ExpressionPatterns.unwrap target
                        isGroupingType unwrapped.Type || isParamNamed groupParamName unwrapped
                    | None -> false

                let mentionsKeyOrGrouping (e: Expression) =
                    let mutable found = false
                    { new ExpressionVisitor() with
                        override _.Visit(node) =
                            if found || isNull node then node
                            elif (tryGroupingCall node).IsSome || isWholeKey node then
                                found <- true
                                node
                            else base.Visit node }.Visit e
                    |> ignore
                    found

                // Members that are a single aggregate become aggregate columns of their own name.
                let pureAggs =
                    memberBindings
                    |> List.choose (fun (memName, arg) ->
                        if (tryResolveKeyIdx arg).IsSome || mentionsKeyOrGrouping arg then None
                        else tryTranslateSingleAgg arg |> Option.map (fun s -> memName, (ExpressionPatterns.unwrap arg, s)))
                    |> Map.ofList

                let hiddenAggs = ResizeArray<Expression * string * ScalarExpr>()
                let groupingCols = ResizeArray<int list * string>()

                let aggColFor (node: Expression) : string option =
                    let unwrapped = ExpressionPatterns.unwrap node
                    let sameExpr (e: Expression) = ExpressionEqualityComparer.Instance.Equals(e, unwrapped)
                    match pureAggs |> Map.tryPick (fun name (e, _) -> if sameExpr e then Some name else None) with
                    | Some name -> Some name
                    | None ->
                        match hiddenAggs |> Seq.tryPick (fun (e, c, _) -> if sameExpr e then Some c else None) with
                        | Some c -> Some c
                        | None ->
                            tryTranslateSingleAgg node
                            |> Option.map (fun s ->
                                let c = sprintf "__gs_agg_%d" hiddenAggs.Count
                                hiddenAggs.Add((unwrapped, c, s))
                                c)

                let groupingColFor (idxs: int list) : string =
                    match groupingCols |> Seq.tryPick (fun (k, c) -> if k = idxs then Some c else None) with
                    | Some c -> c
                    | None ->
                        let c = sprintf "__grouping_%d" groupingCols.Count
                        groupingCols.Add((idxs, c))
                        c

                // A member mixing keys, aggregates and Grouping calls, as an expression over the GroupingSets
                // output: each of those becomes a column placeholder.
                let tryTranslateDerived (arg: Expression) : ScalarExpr option =
                    let mutable ok = true
                    let placeholder (t: Type) (col: string) = Expression.Parameter(t, "__key_col_" + col) :> Expression
                    let rewritten =
                        { new ExpressionVisitor() with
                            override _.Visit(node) =
                                if isNull node || not ok then
                                    node
                                else
                                    match tryGroupingCall node with
                                    | Some(Some idxs) -> placeholder node.Type (groupingColFor idxs)
                                    | Some None ->
                                        ok <- false
                                        node
                                    | None ->
                                        // Only the key access itself: a Convert around it stays, as a cast.
                                        let keyIdxOpt =
                                            match node with
                                            | :? MemberExpression
                                            | :? ParameterExpression -> tryResolveKeyIdx node
                                            | _ -> None
                                        match keyIdxOpt with
                                        | Some i -> placeholder node.Type keyAliasNames.[i]
                                        | None when (node :? MemberExpression || node :? ParameterExpression) && isWholeKey node ->
                                            ok <- false
                                            node
                                        | None ->
                                            match node with
                                            | :? MethodCallExpression as mc when isGroupAggCall mc ->
                                                match aggColFor node with
                                                | Some c -> placeholder node.Type c
                                                | None -> base.Visit node
                                            | _ -> base.Visit node }.Visit arg
                    if ok then
                        ExprTranslator.tryTranslateToScalar (TranslationContext.Entity groupParamName) rewritten
                    else
                        None

                let projectionOpts =
                    memberBindings
                    |> List.map (fun (memName, arg) ->
                        match tryResolveKeyIdx arg with
                        | Some i -> Some(memName, ScalarExpr.Column keyAliasNames.[i])
                        | None when pureAggs.ContainsKey memName -> Some(memName, ScalarExpr.Column memName)
                        | None -> tryTranslateDerived arg |> Option.map (fun s -> memName, s))

                if not (projectionOpts |> List.forall Option.isSome) then
                    None
                else
                    let projections = projectionOpts |> List.choose id

                    let aggs =
                        (memberBindings |> List.choose (fun (memName, _) -> pureAggs.TryFind memName |> Option.map (fun (_, s) -> memName, s)))
                        @ (hiddenAggs |> Seq.map (fun (_, c, s) -> c, s) |> Seq.toList)

                    let groupingColList = groupingCols |> Seq.toList

                    let levels =
                        extractSubsets kind numKeys keyIndexOfSetName
                        |> List.map (fun subset ->
                            let subsetSet = Set.ofList subset
                            let literals =
                                groupingColList
                                |> List.map (fun (idxs, c) -> c, ScalarExpr.Literal(LiteralValue.Int32(groupingIdValue subsetSet idxs)))
                                |> Map.ofList
                            subset, literals)

                    let spec: LogicalGroupingSetsSpec =
                        {
                            KeyAliases = keyAliases
                            Aggs = aggs
                            Levels = levels
                            LevelMerges = None
                            OutputColumns = keyAliasNames @ (aggs |> List.map fst) @ (groupingColList |> List.map snd)
                        }
                    Some(LogicalPlan.Project(LogicalPlan.GroupingSets(basePlan, spec), projections))
