namespace Polars.NET.Linq

open System
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core.Exceptions
open Polars.NET.Core.Config
open Polars.NET.Core.Helpers
open Polars.NET.Core.Enums
open Polars.NET.Linq.IR

[<AutoOpen>]
module internal PipelineStageParser =

    // =========================================================================
    // 1. Type, Schema & Policy Helpers
    // =========================================================================

    /// Creates a TranslationContext with scope and column names.
    let createContextWithScopeAndCols (scopeMap: Map<string * string, string>) (elemType: Type) (cols: string list) (paramName: string) : TranslationContext =
        if PolarsTypeHelper.IsScalarType elemType then
            SubqueryDecorrelator.createContextForTypeWithCols elemType cols paramName
        elif scopeMap.IsEmpty then
            SubqueryDecorrelator.createContextForTypeWithCols elemType cols paramName
        else
            TranslationContext.ScopedEntity([ paramName ], scopeMap, cols)

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

    /// Extracts the underlying sequence element type from generic collections/arrays
    let getSequenceElementType (t: Type) : Type =
        if t.IsGenericType then t.GetGenericArguments().[0]
        elif t.IsArray then t.GetElementType()
        else t

    /// Resolves the single physical column name from column names if the sequence is scalar.
    let tryGetScalarColumnNameFromCols (elemType: Type) (colNames: string seq) : string option =
        if PolarsTypeHelper.IsScalarType elemType then
            let cols = Seq.toList colNames
            if cols.Length = 1 then Some cols.[0] else None
        else
            None

    /// Enforces client evaluation policy on unpushed scalar/terminal predicates (e.g. First(pred), Any(pred))
    let enforceClientPredicatePolicy (stage: string, predLambdaOpt: LambdaExpression option) : unit =
        let policy = CoreConfig.ClientEvaluationPolicy
        match policy with
        | ClientEvaluationPolicy.Allow -> ()
        | ClientEvaluationPolicy.Warn ->
            let text = match predLambdaOpt with Some p -> p.ToString() | None -> "Predicate"
            System.Diagnostics.Trace.TraceWarning
                $"[Polars.NET Warning] Predicate in '%s{stage}' (%s{text}) could not be pushed down and is evaluated on client."
        | ClientEvaluationPolicy.Strict
        | _ ->
            let text = match predLambdaOpt with Some p -> p.ToString() | None -> "Predicate"
            raise (PolarsPushdownException(stage, text, $"Predicate in '%s{stage}' could not be pushed down to native Polars engine."))

    /// Enforces the ClientEvaluationPolicy against client predicates and full fallbacks
    let enforcePipelinePolicy
        (stage: string)
        (requiresClientFallback: bool)
        (clientPredsWithAst: (Func<obj, bool> * LambdaExpression) list)
        (expressionText: string)
        : unit =
        let policy = CoreConfig.ClientEvaluationPolicy
        match policy with
        | ClientEvaluationPolicy.Allow -> ()
        | ClientEvaluationPolicy.Warn ->
            if requiresClientFallback then
                System.Diagnostics.Trace.TraceWarning
                    $"[Polars.NET Warning] Query pipeline at stage '%s{stage}' fell back to full client-side in-memory LINQ: %s{expressionText}"
            elif not clientPredsWithAst.IsEmpty then
                let predTexts =
                    clientPredsWithAst
                    |> List.map (fun (_, l) -> l.ToString())
                    |> String.concat "; "
                System.Diagnostics.Trace.TraceWarning
                    $"[Polars.NET Warning] Query predicate at stage '%s{stage}' fell back to client-side evaluation: %s{predTexts}"
        | ClientEvaluationPolicy.Strict
        | _ ->
            if requiresClientFallback then
                raise (
                    PolarsPushdownException(
                        stage,
                        expressionText,
                        "Full in-memory LINQ fallback was triggered due to unsupported query operations or transparent identifiers."
                    )
                )
            elif not clientPredsWithAst.IsEmpty then
                let firstPred = clientPredsWithAst |> List.head |> snd
                raise (
                    PolarsPushdownException(
                        stage,
                        firstPred.ToString(),
                        sprintf "Method, member, or operator in predicate '%s' cannot be pushed down to native Polars engine." (firstPred.ToString())
                    )
                )

    /// Post-processes raw scalar aggregate results (handling NaN, empty collections, and F# Option/ValueOption types)
    let boxScalarResult<'TResult> (rawScalar: obj, methodName: string) : 'TResult =
        let targetType = typeof<'TResult>

        let isEmpty =
            isNull rawScalar || rawScalar :? double && Double.IsNaN(rawScalar :?> double)

        if isEmpty then
            match methodName with
            | "Sum" ->
                if PolarsTypeHelper.AcceptsNull targetType then
                    LinqReflectionCache.GetEmptyOptionalValue targetType :?> 'TResult
                else
                    Activator.CreateInstance targetType :?> 'TResult
            | "Average"
            | "Min"
            | "Max" ->
                if PolarsTypeHelper.AcceptsNull targetType then
                    LinqReflectionCache.GetEmptyOptionalValue targetType :?> 'TResult
                else
                    invalidOp "Sequence contains no elements."
            | _ -> invalidOp "Sequence contains no elements."
        else
            let struct (_, kind) = PolarsTypeHelper.GetOptionalInfo targetType

            match kind with
            | OptionalKind.FSharpOption ->
                let someMethod = targetType.GetMethod "Some"

                let converted =
                    Convert.ChangeType(rawScalar, PolarsTypeHelper.GetUnderlyingOrSelf targetType)

                someMethod.Invoke(null, [| converted |]) :?> 'TResult
            | OptionalKind.FSharpValueOption ->
                let vSomeMethod = targetType.GetMethod "NewValueSome"

                let converted =
                    Convert.ChangeType(rawScalar, PolarsTypeHelper.GetUnderlyingOrSelf targetType)

                vSomeMethod.Invoke(null, [| converted |]) :?> 'TResult
            | OptionalKind.Nullable ->
                Convert.ChangeType(rawScalar, PolarsTypeHelper.GetUnderlyingOrSelf targetType) :?> 'TResult
            | _ -> Convert.ChangeType(rawScalar, targetType) :?> 'TResult

    let private invokeWithBoxedParam (lambda: LambdaExpression) : InvocationExpression * ParameterExpression =
        let paramType = lambda.Parameters.[0].Type
        let rawParam = Expression.Parameter(typeof<obj>, "rawItem")

        let castParam =
            if paramType = typeof<obj> then
                rawParam :> Expression
            else
                Expression.Convert(rawParam, paramType) :> Expression

        Expression.Invoke(lambda, castParam), rawParam

    /// Compiles any lambda predicate into a type-safe boxed delegate Func<obj, bool>
    /// to safely evaluate against raw materialized source entities without generic coupling.
    let compileClientPredicate (lambda: LambdaExpression) : Func<obj, bool> =
        let invoked, rawParam = invokeWithBoxedParam lambda
        Expression.Lambda<Func<obj, bool>>(invoked, rawParam).Compile()

    /// Compiles a lambda into a typed Func<obj, obj> without DynamicInvoke reflection overhead.
    let compileToBoxedFunc (lambda: LambdaExpression) : Func<obj, obj> =
        let invoked, rawParam = invokeWithBoxedParam lambda
        let boxedResult = Expression.Convert(invoked, typeof<obj>)
        Expression.Lambda<Func<obj, obj>>(boxedResult, rawParam).Compile()

    // =========================================================================
    // 2. AST & Member Binding Extractors
    // =========================================================================

    let rec private hasStaticMethodCall (methodName: string) (e: Expression) : bool =
        match e with
        | null -> false
        | MethodCall(m, null, _) when m.Name = methodName -> true
        | MethodCall(_, null, args) -> args |> List.exists (hasStaticMethodCall methodName)
        | _ -> false

    /// Recursively inspects the expression tree to locate Chunk(size) invocations.
    let rec tryExtractChunkSize (e: Expression) : int option =
        match e with
        | null -> None
        | MethodCall(m, null, [ _; sizeExpr ]) when m.Name = "Chunk" ->
            tryEvaluate sizeExpr |> Option.map Convert.ToInt32
        | MethodCall(_, null, args) -> args |> List.tryPick tryExtractChunkSize
        | _ -> None

    /// Recursively inspects the expression tree to detect if Shuffle() was called.
    let hasShuffle (e: Expression) : bool = hasStaticMethodCall "Shuffle" e

    /// Recursively inspects the expression tree to locate AggregateBy invocations.
    let rec tryExtractAggregateBy (e: Expression) =
        match e with
        | null -> None
        | MethodCall(m, null, source :: CleanLambda k :: seedExpr :: CleanLambda f :: rest) when
            m.Name = "AggregateBy"
            ->
            let seedLambdaOpt =
                match seedExpr with
                | CleanLambda s -> Some s
                | _ -> None

            Some(source, k, seedExpr, seedLambdaOpt, f, List.tryHead rest)
        | MethodCall(_, null, args) -> args |> List.tryPick tryExtractAggregateBy
        | _ -> None

    /// Recursively inspects the expression tree to locate DefaultIfEmpty invocations.
    let rec tryExtractDefaultIfEmpty (e: Expression) : (bool * obj option) option =
        match e with
        | null -> None
        | MethodCall(m, null, _ :: rest) when m.Name = "DefaultIfEmpty" && rest.Length <= 1 ->
            Some(true, rest |> List.tryHead |> Option.bind tryEvaluate)
        | MethodCall(_, null, args) -> args |> List.tryPick tryExtractDefaultIfEmpty
        | _ -> None

    /// Recursively inspects the expression tree to locate GroupBy key and optional element selectors.
    let rec tryExtractGroupByInfo (e: Expression) : (LambdaExpression * LambdaExpression option) option =
        match e with
        | null -> None
        | MethodCall(m, null, [ _; CleanLambda k; CleanLambda elem ]) when m.Name = "GroupBy" ->
            Some(k, Some elem)
        | MethodCall(m, null, [ _; CleanLambda k ]) when m.Name = "GroupBy" -> Some(k, None)
        | MethodCall(_, null, args) -> args |> List.tryPick tryExtractGroupByInfo
        | _ -> None

    /// Recursively inspects the expression tree to detect if GroupJoin() was called.
    let hasGroupJoin (e: Expression) : bool = hasStaticMethodCall "GroupJoin" e

    /// Recursively inspects the expression tree to locate OrderBy/OrderByDescending sort selectors.
    let rec tryExtractSortSelector (e: Expression) : (LambdaExpression * bool) option =
        match e with
        | null -> None
        | MethodCall(m, null, [ _; CleanLambda l ]) when m.Name = "OrderBy" || m.Name = "OrderByDescending" ->
            Some(l, m.Name = "OrderByDescending")
        | MethodCall(_, null, args) -> args |> List.tryPick tryExtractSortSelector
        | _ -> None

    /// Recursively locates the root IQueryable / IPolarsInternalExplainable ConstantExpression in a LINQ pipeline.
    let rec tryFindRootQueryable (e: Expression) : (Expression * Type) option =
        match e with
        | null -> None
        | :? ConstantExpression as ce when
            typeof<IQueryable>.IsAssignableFrom(ce.Type)
            || typeof<IPolarsInternalExplainable>.IsAssignableFrom(ce.Type)
            ->
            Some(ce :> Expression, ce.Type)
        | :? MethodCallExpression as mc when mc.Arguments.Count > 0 -> tryFindRootQueryable mc.Arguments.[0]
        | _ -> None

    /// Extracts member names matching properties or capitalizes them to PascalCase
    let extractPascalCaseMemberNames (newExpr: NewExpression) : string list =
        if not (isNull newExpr.Members) then
            newExpr.Members |> Seq.map (fun m -> m.Name) |> Seq.toList
        else
            let t = newExpr.Type
            newExpr.Constructor.GetParameters()
            |> Seq.map (fun p ->
                if String.IsNullOrEmpty p.Name then
                    "Col"
                else
                    match t.GetProperty(p.Name, BindingFlags.Public ||| BindingFlags.Instance ||| BindingFlags.IgnoreCase) with
                    | prop when not (isNull prop) -> prop.Name
                    | _ ->
                        Char.ToUpperInvariant(p.Name.[0]).ToString() + p.Name.Substring 1)
            |> Seq.toList

    /// Extracts column names from a DistinctBy keySelector lambda
    let extractDistinctColumns (keyLambda: LambdaExpression) : string array option =
        match keyLambda.Body with
        | ExtractColumnName colName -> Some [| colName |]
        | :? NewExpression as newExpr -> Some(extractPascalCaseMemberNames newExpr |> List.toArray)
        | :? MemberInitExpression as initExpr ->
            Some(initExpr.Bindings |> Seq.map (fun b -> b.Member.Name) |> Seq.toArray)
        | _ -> None

    /// Extracts target column name for SelectMany/Explode
    let extractExplodeColumn (colLambda: LambdaExpression) : string option =
        match colLambda.Body with
        | ExtractColumnName colName -> Some colName
        | _ -> None

    /// Uniformly extracts member name and expression pairs from NewExpression or MemberInitExpression
    let tryExtractMemberBindings (expr: Expression) : (string * Expression) list option =
        match expr with
        | :? NewExpression as newExpr ->
            let names = extractPascalCaseMemberNames newExpr
            Some(List.zip names (newExpr.Arguments |> Seq.toList))
        | :? MemberInitExpression as initExpr ->
            let bindings =
                initExpr.Bindings
                |> Seq.choose (function
                    | :? MemberAssignment as a -> Some(a.Member.Name, a.Expression)
                    | _ -> None)
                |> Seq.toList

            if bindings.Length = initExpr.Bindings.Count then
                Some bindings
            else
                None
        | MethodCall(m, null, args) when m.Name = "Create" && not (isNull m.DeclaringType) && m.DeclaringType.Name = "Tuple" ->
            let names = [ for i in 1 .. args.Length -> sprintf "Item%d" i ]
            Some(List.zip names args)
        | MethodCall(m, null, [ k; v ]) when m.Name = "Create" && not (isNull m.DeclaringType) && m.DeclaringType.Name = "KeyValuePair" ->
            Some [ "Key", k; "Value", v ]
        | :? NewExpression as ne when ne.Type.Name.StartsWith "KeyValuePair" && ne.Arguments.Count = 2 ->
            Some [ "Key", ne.Arguments.[0]; "Value", ne.Arguments.[1] ]
        | :? ConstantExpression as ce when not (isNull ce.Value) && not (PolarsTypeHelper.IsScalarType ce.Type) ->
            let props = ce.Type.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
            if props.Length > 0 then
                let bindings =
                    props
                    |> Array.map (fun p -> p.Name, Expression.Constant(p.GetValue(ce.Value), p.PropertyType) :> Expression)
                    |> Array.toList
                Some bindings
            else
                None
        | _ -> None

    /// Pattern structure representing an Unpivot (Melt) operation extracted from SelectMany
    type internal UnpivotPattern =
        {
            ExplicitIndexCols: string list
            VarPropName: string
            ValPropName: string
            VariableName: string
            ValueName: string
            OnCols: string array
            LabelMappings: (string * string) list
        }

    /// Tries to recognize whether a SelectMany collection selector is unpivoting multiple columns
    let tryExtractUnpivotPattern
        (colLambda: LambdaExpression)
        (resLambdaOpt: LambdaExpression option)
        : UnpivotPattern option =
        if colLambda.Parameters.Count <> 1 then
            None
        else
            let outerParam = colLambda.Parameters.[0]
            let cleanBody = ExpressionPatterns.unwrap colLambda.Body

            match cleanBody with
            | :? NewArrayExpression as nae when
                (nae.NodeType = ExpressionType.NewArrayInit) && nae.Expressions.Count >= 1
                ->
                let elems = nae.Expressions |> Seq.toList
                let elemBindingsOpt =
                    elems
                    |> List.choose tryExtractMemberBindings

                if elemBindingsOpt.Length <> elems.Length || elems.IsEmpty then
                    None
                else
                    let firstNames = elemBindingsOpt.Head |> List.map fst
                    let allSameStructure =
                        elemBindingsOpt
                        |> List.forall (fun b -> List.map fst b = firstNames)

                    if not allSameStructure then
                        None
                    else
                        let memberColumns =
                            firstNames
                            |> List.map (fun name ->
                                let exprs =
                                    elemBindingsOpt
                                    |> List.map (fun b ->
                                        b
                                        |> List.find (fun (n, _) -> n = name)
                                        |> snd
                                        |> ExpressionPatterns.unwrap)
                                name, exprs)

                        let isInvariantOuterMember (exprs: Expression list) =
                            let names =
                                exprs
                                |> List.choose (function
                                    | MemberAccess(target, m) when
                                        match ExpressionPatterns.unwrap target with
                                        | :? ParameterExpression as p -> p.Name = outerParam.Name
                                        | _ -> false
                                        -> Some m.Name
                                    | _ -> None)
                            if names.Length = exprs.Length && (names |> Set.ofList |> Set.count = 1) then
                                Some names.Head
                            else
                                None

                        let isConstantMember (exprs: Expression list) =
                            let consts =
                                exprs
                                |> List.choose (function
                                    | :? ConstantExpression as ce when not (isNull ce.Value) ->
                                        Some(ce.Value.ToString())
                                    | other ->
                                        match tryEvaluate other with
                                        | Some v when not (isNull v) -> Some(v.ToString())
                                        | _ -> None)
                            if consts.Length = exprs.Length then
                                Some consts
                            else
                                None

                        let isVaryingOuterMember (exprs: Expression list) =
                            let names =
                                exprs
                                |> List.choose (function
                                    | MemberAccess(target, m) when
                                        match ExpressionPatterns.unwrap target with
                                        | :? ParameterExpression as p -> p.Name = outerParam.Name
                                        | _ -> false
                                        -> Some m.Name
                                    | _ -> None)
                            if names.Length = exprs.Length then
                                Some names
                            else
                                None

                        let mutable explicitIndexCols = []
                        let mutable varMemberOpt = None
                        let mutable valMemberOpt = None

                        for (name, exprs) in memberColumns do
                            match isInvariantOuterMember exprs with
                            | Some colName ->
                                explicitIndexCols <- explicitIndexCols @ [ colName ]
                            | None ->
                                match isConstantMember exprs with
                                | Some consts ->
                                    varMemberOpt <- Some (name, consts)
                                | None ->
                                    match isVaryingOuterMember exprs with
                                    | Some colNames ->
                                        valMemberOpt <- Some (name, colNames)
                                    | None -> ()

                        match varMemberOpt, valMemberOpt with
                        | Some(varPropName, consts), Some(valPropName, colNames) ->
                            let inferredVarName, inferredValName =
                                match resLambdaOpt with
                                | Some resSel when resSel.Parameters.Count >= 2 ->
                                    let pInner = resSel.Parameters.[1]
                                    match tryExtractMemberBindings resSel.Body with
                                    | Some bindings ->
                                        let findAssignedName prop =
                                            bindings
                                            |> List.tryPick (fun (targetName, e) ->
                                                match ExpressionPatterns.unwrap e with
                                                | MemberAccess(target, m) when
                                                    (match ExpressionPatterns.unwrap target with :? ParameterExpression as p -> p.Name = pInner.Name | _ -> false)
                                                    && m.Name = prop -> Some targetName
                                                | _ -> None)
                                        let vName = findAssignedName varPropName |> Option.defaultValue varPropName
                                        let valName = findAssignedName valPropName |> Option.defaultValue valPropName
                                        vName, valName
                                    | None -> varPropName, valPropName
                                | _ -> varPropName, valPropName

                            let finalVarName =
                                if inferredVarName.StartsWith "Item" || inferredVarName = "Key" then "variable"
                                else inferredVarName

                            let finalValName =
                                if inferredValName.StartsWith "Item" || inferredValName = "Value" then "value"
                                else inferredValName

                            let onCols = colNames |> List.toArray
                            let labelMappings = List.zip colNames consts

                            Some {
                                ExplicitIndexCols = explicitIndexCols
                                VarPropName = varPropName
                                ValPropName = valPropName
                                VariableName = finalVarName
                                ValueName = finalValName
                                OnCols = onCols
                                LabelMappings = labelMappings
                            }
                        | _ -> None
            | _ -> None

    let isUnpivotPattern (colLambda: LambdaExpression) : bool =
        tryExtractUnpivotPattern colLambda None |> Option.isSome

    /// Desugars a .NET 11 GroupJoin downstream projection (g => body)
    /// into a classic 2-parameter result selector ((outer, innerGroup) => body)
    /// where g.Key maps to 'outer' and 'g' (as IEnumerable) maps to 'innerGroup'.
    let desugarGroupJoinProject
        (projLambda: LambdaExpression)
        (outerType: Type)
        (innerType: Type)
        : LambdaExpression =
        let gParam = projLambda.Parameters.[0]
        let pOuter = Expression.Parameter(outerType, "outer")
        let innerSeqType = typedefof<IEnumerable<_>>.MakeGenericType(innerType)
        let pInnerGroup = Expression.Parameter(innerSeqType, "innerGroup")

        let replacer =
            { new ExpressionVisitor() with
                override _.VisitMember(node: MemberExpression) =
                    match ExpressionPatterns.unwrap node.Expression with
                    | :? ParameterExpression as p when p.Name = gParam.Name && node.Member.Name = "Key" ->
                        pOuter :> Expression
                    | _ -> base.VisitMember(node)

                override _.VisitParameter(node: ParameterExpression) =
                    if node.Name = gParam.Name then
                        pInnerGroup :> Expression
                    else
                        base.VisitParameter node
            }

        let newBody = replacer.Visit projLambda.Body

        let delegateType =
            typedefof<Func<_, _, _>>.MakeGenericType(outerType, innerSeqType, projLambda.ReturnType)

        Expression.Lambda(delegateType, newBody, [| pOuter; pInnerGroup |])

    /// Universally extracts column renaming mappings from a two-parameter result selector
    let extractBinaryResultRenames
        (resLambda: LambdaExpression)
        (explodedInfoOpt: (string * string) option)
        : (string * string) list =
        match resLambda.Body with
        | :? NewExpression as newExpr ->
            let args = newExpr.Arguments |> Seq.toList
            let memberNames = extractPascalCaseMemberNames newExpr

            List.zip memberNames args
            |> List.choose (fun (memName, arg) ->
                match explodedInfoOpt with
                | Some(itemParamName, explodedCol) when
                    (match arg with
                     | :? ParameterExpression as p -> p.Name = itemParamName
                     | _ -> false)
                    && memName <> explodedCol
                    ->
                    Some(explodedCol, memName)
                | _ ->
                    match arg with
                    | ExtractColumnName srcCol when srcCol <> memName -> Some(srcCol, memName)
                    | _ -> None)
        | _ -> []

    // =========================================================================
    // 3. LINQ Expression Stage Flattener
    // =========================================================================

    let internal extractTolerance (objVal: obj) : string option * int64 option * double option =
        if isNull objVal then
            None, None, None
        else
            let t = objVal.GetType()
            if t = typeof<string> then
                Some(objVal :?> string), None, None
            elif t = typeof<TimeSpan> then
                let ts = objVal :?> TimeSpan
                Some(DurationFormatter.ToPolarsString ts), None, None
            elif t = typeof<int64> then
                None, Some(objVal :?> int64), None
            elif t = typeof<int> then
                None, Some(int64 (objVal :?> int)), None
            elif t = typeof<double> then
                None, None, Some(objVal :?> double)
            elif t.Name = "AsOfTolerance" || (t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<Nullable<_>> && t.GetGenericArguments().[0].Name = "AsOfTolerance") then
                let actualObj =
                    if t.IsGenericType then
                        let hasValueProp = t.GetProperty("HasValue")
                        let valueProp = t.GetProperty("Value")
                        if not (hasValueProp.GetValue(objVal) :?> bool) then null
                        else valueProp.GetValue(objVal)
                    else objVal
                if isNull actualObj then
                    None, None, None
                else
                    let actualType = actualObj.GetType()
                    let sField = actualType.GetField("StringVal", BindingFlags.Instance ||| BindingFlags.NonPublic ||| BindingFlags.Public)
                    let iField = actualType.GetField("IntVal", BindingFlags.Instance ||| BindingFlags.NonPublic ||| BindingFlags.Public)
                    let dField = actualType.GetField("FloatVal", BindingFlags.Instance ||| BindingFlags.NonPublic ||| BindingFlags.Public)
                    let sVal = if isNull sField then null else sField.GetValue(actualObj) :?> string
                    let iVal = if isNull iField then Nullable<int64>() else iField.GetValue(actualObj) :?> Nullable<int64>
                    let dVal = if isNull dField then Nullable<double>() else dField.GetValue(actualObj) :?> Nullable<double>
                    Option.ofObj sVal, (if iVal.HasValue then Some iVal.Value else None), (if dVal.HasValue then Some dVal.Value else None)
            else
                None, None, None

    let internal extractStrategy (objVal: obj) : PlAsofStrategy =
        if isNull objVal then
            PlAsofStrategy.Backward
        else
            try
                let byteVal = Convert.ToByte(objVal)
                match byteVal with
                | 0uy -> PlAsofStrategy.Backward
                | 1uy -> PlAsofStrategy.Forward
                | 2uy -> PlAsofStrategy.Nearest
                | _ -> PlAsofStrategy.Backward
            with _ -> PlAsofStrategy.Backward

    /// Canonical classification of null-checking predicate expressions for sort fusion
    [<RequireQualifiedAccess>]
    type internal NullCheckKind =
        | IsNull of target: Expression      // true when target is null
        | IsNotNull of target: Expression   // true when target is not null

    /// Extracts whether an expression is a null check (x == null, x != null, !x.HasValue, etc.)
    let internal tryExtractNullCheck (expr: Expression) : NullCheckKind option =
        let isNullConst (e: Expression) =
            match unwrap e with
            | :? ConstantExpression as c when isNull c.Value -> true
            | _ -> false

        let isOneOrTrue (e: Expression) =
            match unwrap e with
            | :? ConstantExpression as c when not (isNull c.Value) ->
                match c.Value with
                | :? bool as b -> b
                | :? int as i -> i = 1
                | :? int64 as i -> i = 1L
                | _ -> false
            | _ -> false

        let isZeroOrFalse (e: Expression) =
            match unwrap e with
            | :? ConstantExpression as c when not (isNull c.Value) ->
                match c.Value with
                | :? bool as b -> not b
                | :? int as i -> i = 0
                | :? int64 as i -> i = 0L
                | _ -> false
            | _ -> false

        let rec analyze (e: Expression) : NullCheckKind option =
            let u = unwrap e
            match u with
            | :? BinaryExpression as b when b.NodeType = ExpressionType.Equal ->
                if isNullConst b.Right then Some(NullCheckKind.IsNull(unwrap b.Left))
                elif isNullConst b.Left then Some(NullCheckKind.IsNull(unwrap b.Right))
                else None
            | :? BinaryExpression as b when b.NodeType = ExpressionType.NotEqual ->
                if isNullConst b.Right then Some(NullCheckKind.IsNotNull(unwrap b.Left))
                elif isNullConst b.Left then Some(NullCheckKind.IsNotNull(unwrap b.Right))
                else None
            | :? UnaryExpression as u when u.NodeType = ExpressionType.Not ->
                match analyze u.Operand with
                | Some(NullCheckKind.IsNull t) -> Some(NullCheckKind.IsNotNull t)
                | Some(NullCheckKind.IsNotNull t) -> Some(NullCheckKind.IsNull t)
                | None -> None
            | MemberAccess(target, m) when isNullableHasValue m ->
                Some(NullCheckKind.IsNotNull(unwrap target))
            | :? ConditionalExpression as cond ->
                if isOneOrTrue cond.IfTrue && isZeroOrFalse cond.IfFalse then
                    analyze cond.Test
                elif isZeroOrFalse cond.IfTrue && isOneOrTrue cond.IfFalse then
                    match analyze cond.Test with
                    | Some(NullCheckKind.IsNull t) -> Some(NullCheckKind.IsNotNull t)
                    | Some(NullCheckKind.IsNotNull t) -> Some(NullCheckKind.IsNull t)
                    | None -> None
                else
                    None
            | _ -> None

        analyze expr

    /// Attempts to fuse an adjacent pair of sort stages (e.g. OrderBy(x => x.Score == null).ThenBy(x => x.Score))
    /// into a single Sort stage with the appropriate native Polars NullsLast setting.
    let internal tryFuseNullSortPair
        (l1: LambdaExpression)
        (isDesc1: bool)
        (l2: LambdaExpression)
        (isDesc2: bool)
        : (LambdaExpression * bool * bool) option =
        match tryExtractNullCheck l1.Body with
        | Some checkKind ->
            let target1 =
                match checkKind with
                | NullCheckKind.IsNull t -> t
                | NullCheckKind.IsNotNull t -> t

            let target2 =
                let u2 = unwrap l2.Body
                match u2 with
                | MemberAccess(inner, m) when isNullableValue m -> unwrap inner
                | _ -> u2

            let lambda1 = Expression.Lambda(target1, l1.Parameters)
            let lambda2 = Expression.Lambda(target2, l2.Parameters)

            if ExpressionEqualityComparer.Instance.Equals(lambda1, lambda2) then
                let nullsLast =
                    match checkKind with
                    | NullCheckKind.IsNull _ ->
                        if not isDesc1 then true else false
                    | NullCheckKind.IsNotNull _ ->
                        if not isDesc1 then false else true

                Some(l2, isDesc2, nullsLast)
            else
                None
        | None -> None

    /// Fuses adjacent sort stages that represent null-handling idioms (e.g. OrderBy(x => x.Score == null).ThenBy(x => x.Score))
    let rec fuseSortStages (stages: LinqStage list) : LinqStage list =
        match stages with
        | LinqStage.Sort(l1, isDesc1, _) :: LinqStage.Sort(l2, isDesc2, _) :: tail ->
            match tryFuseNullSortPair l1 isDesc1 l2 isDesc2 with
            | Some (fusedLambda, fusedDesc, fusedNullsLast) ->
                fuseSortStages (LinqStage.Sort(fusedLambda, fusedDesc, fusedNullsLast) :: tail)
            | None ->
                LinqStage.Sort(l1, isDesc1, (if isDesc1 then true else false)) :: fuseSortStages (LinqStage.Sort(l2, isDesc2, (if isDesc2 then true else false)) :: tail)
        | head :: tail -> head :: fuseSortStages tail
        | [] -> []

    /// Fuses adjacent sort pairs within GroupFoldContext
    let rec fuseSortPairs (pairs: (LambdaExpression * bool * bool) list) : (LambdaExpression * bool * bool) list =
        match pairs with
        | (l1, isDesc1, _) :: (l2, isDesc2, _) :: tail ->
            match tryFuseNullSortPair l1 isDesc1 l2 isDesc2 with
            | Some (fusedLambda, fusedDesc, fusedNullsLast) ->
                fuseSortPairs ((fusedLambda, fusedDesc, fusedNullsLast) :: tail)
            | None ->
                (l1, isDesc1, (if isDesc1 then true else false)) :: fuseSortPairs ((l2, isDesc2, (if isDesc2 then true else false)) :: tail)
        | head :: tail -> head :: fuseSortPairs tail
        | [] -> []

    let rec (|GroupByCall|_|) (expr: Expression) =
        match ExpressionPatterns.unwrap expr with
        | MethodCall(m, _, [ source; CleanLambda k ]) when m.Name = "GroupBy" ->
            Some(source, k)
        | MethodCall(m, _, [ source; CleanLambda k; CleanLambda _ ]) when m.Name = "GroupBy" ->
            Some(source, k)
        | _ -> None

    let rec extractProject (stages: LinqStage list) =
        match stages with
        | LinqStage.Project r :: rest -> Some(r, rest)
        | other :: rest ->
            match extractProject rest with
            | Some(r, remaining) -> Some(r, other :: remaining)
            | None -> None
        | [] -> None

    /// Recursively flattens a LINQ expression tree into a linear sequence of LinqStage values.
    let rec flattenStages (e: Expression) (acc: LinqStage list) : LinqStage list =
        let evalUInt32 opName (expr: Expression) =
            match tryEvaluate expr with
            | Some c -> Convert.ToUInt32 c
            | None -> failwithf "Could not evaluate %s count argument: %A" opName expr

        match e with
        | null -> fuseSortStages acc
        | MethodCall(m, null, [ source; StripQuotes(:? LambdaExpression as l) ]) when m.Name = "Select" ->
            let isPassthrough (body: Expression) =
                let p = l.Parameters.[0]

                let rec check (expr: Expression) =
                    match expr with
                    | null -> false
                    | :? ParameterExpression as pe when pe.Name = p.Name -> true
                    | Unary(ExpressionType.Convert, inner)
                    | Unary(ExpressionType.ConvertChecked, inner)
                    | Unary(ExpressionType.Quote, inner) -> check inner
                    | MemberAccess(:? ParameterExpression as pe, memInfo) when
                        pe.Name = p.Name && isFSharpAnonymousMember memInfo
                        ->
                        true
                    | :? NewExpression as ne when isTupleOrAnonType ne.Type ->
                        ne.Arguments
                        |> Seq.mapi (fun idx arg ->
                            match ExpressionPatterns.unwrap arg with
                            | MemberAccess(:? ParameterExpression as pe, mem) when
                                pe.Name = p.Name
                                && (mem.Name = sprintf "Item%d" (idx + 1)
                                    || mem.Name = "Outer" && idx = 0
                                    || mem.Name = "Inner" && idx = 1)
                                ->
                                true
                            | _ -> false)
                        |> Seq.forall id
                    | _ -> false

                check body

            if isPassthrough l.Body then
                flattenStages source acc
            else
                flattenStages source (LinqStage.Project l :: acc)

        // Single-lambda stages: Where, OrderBy/ThenBy, DistinctBy, GroupBy(key), TakeWhile, SkipWhile
        | MethodCall(m,
                     null,
                     [ source; StripQuotes(:? LambdaExpression as l) ]) when
            m.Name = "Where"
            || m.Name = "OrderBy"
            || m.Name = "ThenBy"
            || m.Name = "OrderByDescending"
            || m.Name = "ThenByDescending"
            || m.Name = "DistinctBy"
            || m.Name = "GroupBy"
            || m.Name = "TakeWhile"
            || m.Name = "SkipWhile"
            ->
            let stage =
                match m.Name with
                | "Where" -> LinqStage.Filter l
                | "OrderBy" | "ThenBy" -> LinqStage.Sort(l, isDescending = false, nullsLast = false)
                | "OrderByDescending" | "ThenByDescending" -> LinqStage.Sort(l, isDescending = true, nullsLast = true)
                | "DistinctBy" -> LinqStage.DistinctBy l
                | "GroupBy" -> LinqStage.GroupByKey l
                | "TakeWhile" -> LinqStage.TakeWhile l
                | _ -> LinqStage.SkipWhile l

            flattenStages source (stage :: acc)

        // Multidimensional Groupings (Scheme A & Scheme B)
        // Scheme A: RollUp / Cube / GroupingSets with result selector
        | MethodCall(m, _, [ GroupByCall(source, k); CleanLambda r ]) when m.Name = "RollUp" || m.Name = "Rollup" ->
            flattenStages source (LinqStage.GroupByRollup(k, r) :: acc)

        | MethodCall(m, _, [ GroupByCall(source, k); CleanLambda r ]) when m.Name = "Cube" ->
            flattenStages source (LinqStage.GroupByCube(k, r) :: acc)

        | MethodCall(m, _, [ GroupByCall(source, k); setsExpr; CleanLambda r ]) when m.Name = "GroupingSets" ->
            flattenStages source (LinqStage.GroupByGroupingSets(k, setsExpr, r) :: acc)

        // Scheme B: RollUp / Cube / GroupingSets without result selector, followed by Select (Project)
        | MethodCall(m, _, [ GroupByCall(source, k) ]) when m.Name = "RollUp" || m.Name = "Rollup" ->
            match extractProject acc with
            | Some(r, remaining) ->
                flattenStages source (LinqStage.GroupByRollup(k, r) :: remaining)
            | None ->
                flattenStages source acc

        | MethodCall(m, _, [ GroupByCall(source, k) ]) when m.Name = "Cube" ->
            match extractProject acc with
            | Some(r, remaining) ->
                flattenStages source (LinqStage.GroupByCube(k, r) :: remaining)
            | None ->
                flattenStages source acc

        | MethodCall(m, _, [ GroupByCall(source, k); setsExpr ]) when m.Name = "GroupingSets" ->
            match extractProject acc with
            | Some(r, remaining) ->
                flattenStages source (LinqStage.GroupByGroupingSets(k, setsExpr, r) :: remaining)
            | None ->
                flattenStages source acc

        // Single-argument (source-only) stages
        | MethodCall(m, null, [ source ]) ->
            let stageOpt =
                match m.Name with
                | "Distinct" -> Some LinqStage.Distinct
                | "Index" -> Some LinqStage.Index
                | "Order" -> Some(LinqStage.SortDefault(isDescending = false, nullsLast = false))
                | "OrderDescending" -> Some(LinqStage.SortDefault(isDescending = true, nullsLast = true))
                | "Reverse" -> Some LinqStage.Reverse
                | "Shuffle" -> Some LinqStage.Shuffle
                | "DefaultIfEmpty" -> Some(LinqStage.DefaultIfEmpty None)
                | "Cast" when m.IsGenericMethod -> Some(LinqStage.Cast(m.GetGenericArguments().[0]))
                | "OfType" when m.IsGenericMethod ->
                    Some(LinqStage.OfType(getSequenceElementType source.Type, m.GetGenericArguments().[0]))
                | _ -> None

            match stageOpt with
            | Some stage -> flattenStages source (stage :: acc)
            | None -> fuseSortStages acc

        // Count-based slicing stages: Take, Skip, TakeLast, SkipLast
        | MethodCall(m, null, [ source; countExpr ]) when
            m.Name = "Take" || m.Name = "Skip" || m.Name = "TakeLast" || m.Name = "SkipLast"
            ->
            let n = evalUInt32 m.Name countExpr

            let stage =
                match m.Name with
                | "Take" -> LinqStage.Take n
                | "Skip" -> LinqStage.Skip n
                | "TakeLast" -> LinqStage.TakeLast n
                | _ -> LinqStage.SkipLast n

            flattenStages source (stage :: acc)

        | MethodCall(m, null, [ source; CleanLambda k; CleanLambda e; CleanLambda r ]) when m.Name = "GroupBy" ->
            flattenStages source (LinqStage.GroupByWithElementAndResult(k, e, r) :: acc)

        | MethodCall(m,
                     null,
                     [ source; StripQuotes(:? LambdaExpression as k); StripQuotes(:? LambdaExpression as r) ]) when
            m.Name = "GroupBy"
            ->
            flattenStages source (LinqStage.GroupByWithResult(k, r) :: acc)

        // Join, LeftJoin, RightJoin, FullJoin, GroupJoin (with optional result selector)
        | MethodCall(m,
                     _,
                     outer :: inner :: StripQuotes(:? LambdaExpression as outerKey) :: StripQuotes(:? LambdaExpression as innerKey) :: rest) when
            m.Name = "Join"
            || m.Name = "LeftJoin"
            || m.Name = "RightJoin"
            || m.Name = "FullJoin"
            || m.Name = "GroupJoin"
            ->
            let resSelOpt =
                match rest with
                | StripQuotes(:? LambdaExpression as resSel) :: _ -> Some resSel
                | _ -> None

            let stage =
                if m.Name = "GroupJoin" then
                    LinqStage.GroupJoin(m, inner, outerKey, innerKey, resSelOpt)
                else
                    LinqStage.Join(m, inner, outerKey, innerKey, resSelOpt)

            flattenStages outer (stage :: acc)

        // JoinAsOf (time-series asof join with optional By partition, tolerance, and strategy)
        | MethodCall(m,
                     _,
                     outer :: inner :: StripQuotes(:? LambdaExpression as outerKey) :: StripQuotes(:? LambdaExpression as innerKey) :: rest) when
            m.Name = "JoinAsOf"
            ->
            let outerByOpt, innerByOpt, resSelOpt, tolExprOpt, stratExprOpt =
                match rest with
                | StripQuotes(:? LambdaExpression as ob) :: StripQuotes(:? LambdaExpression as ib) :: StripQuotes(:? LambdaExpression as rs) :: tail ->
                    let tol = tail |> List.tryItem 0
                    let strat = tail |> List.tryItem 1
                    Some ob, Some ib, Some rs, tol, strat
                | StripQuotes(:? LambdaExpression as ob) :: StripQuotes(:? LambdaExpression as ib) :: tail ->
                    let tol = tail |> List.tryItem 0
                    let strat = tail |> List.tryItem 1
                    Some ob, Some ib, None, tol, strat
                | StripQuotes(:? LambdaExpression as rs) :: tail ->
                    let tol = tail |> List.tryItem 0
                    let strat = tail |> List.tryItem 1
                    None, None, Some rs, tol, strat
                | tail ->
                    let tol = tail |> List.tryItem 0
                    let strat = tail |> List.tryItem 1
                    None, None, None, tol, strat

            let tolStr, tolInt, tolFloat =
                match tolExprOpt with
                | Some expr ->
                    let objVal =
                        match expr with
                        | :? ConstantExpression as ce -> ce.Value
                        | _ ->
                            try Expression.Lambda(expr).Compile().DynamicInvoke()
                            with _ -> null
                    extractTolerance objVal
                | None -> None, None, None

            let strat =
                match stratExprOpt with
                | Some expr ->
                    let objVal =
                        match expr with
                        | :? ConstantExpression as ce -> ce.Value
                        | _ ->
                            try Expression.Lambda(expr).Compile().DynamicInvoke()
                            with _ -> null
                    extractStrategy objVal
                | None -> PlAsofStrategy.Backward

            let stage =
                LinqStage.JoinAsOf(
                    m,
                    inner,
                    outerKey,
                    innerKey,
                    outerByOpt,
                    innerByOpt,
                    resSelOpt,
                    tolStr,
                    tolInt,
                    tolFloat,
                    strat
                )

            flattenStages outer (stage :: acc)

        | MethodCall(m, null, [ first; second ]) when m.Name = "Intersect" || m.Name = "Except" ->
            flattenStages first (LinqStage.SetOp(m, second, None, None) :: acc)

        | MethodCall(m, null, [ first; second; StripQuotes(:? LambdaExpression as keySel) ]) when
            m.Name = "IntersectBy" || m.Name = "ExceptBy"
            ->
            flattenStages first (LinqStage.SetOp(m, second, Some keySel, None) :: acc)

        | MethodCall(m,
                     null,
                     [ first; second; StripQuotes(:? LambdaExpression as k1); StripQuotes(:? LambdaExpression as k2) ]) when
            m.Name = "IntersectBy" || m.Name = "ExceptBy"
            ->
            flattenStages first (LinqStage.SetOp(m, second, Some k1, Some k2) :: acc)

        | MethodCall(m, null, [ first; second ]) when m.Name = "Concat" ->
            flattenStages first (LinqStage.Concat second :: acc)

        | MethodCall(m, null, [ first; second ]) when m.Name = "Union" ->
            flattenStages first (LinqStage.Union(second, None) :: acc)

        | MethodCall(m, null, [ first; second; StripQuotes(:? LambdaExpression as keySel) ]) when m.Name = "UnionBy" ->
            flattenStages first (LinqStage.Union(second, Some keySel) :: acc)

        | MethodCall(m, null, source :: StripQuotes(:? LambdaExpression as colSel) :: rest) when
            m.Name = "SelectMany"
            ->
            let resSelOpt =
                match rest with
                | [ StripQuotes(:? LambdaExpression as r) ] -> Some r
                | _ -> None

            let paramName = colSel.Parameters.[0].Name
            let containsParam = SubqueryDecorrelator.containsParameter paramName colSel.Body

            let isGroupSource, groupKeySelOpt, baseGroupSource =
                match source with
                | MethodCall(gm, null, gs :: StripQuotes(:? LambdaExpression as ks) :: _) when gm.Name = "GroupBy" ->
                    true, Some ks, gs
                | _ -> false, None, source

            if isGroupSource && containsParam then
                let innerStages =
                    match ExpressionPatterns.unwrap colSel.Body with
                    | :? MethodCallExpression as innerMc -> flattenStages innerMc []
                    | _ -> []
                let stage = LinqStage.GroupPartitionSlice(groupKeySelOpt.Value, innerStages, resSelOpt)
                flattenStages baseGroupSource (stage :: acc)
            else
                let nestedStagesOpt =
                    if containsParam then
                        match ExpressionPatterns.unwrap colSel.Body with
                        | :? MethodCallExpression as innerMc when innerMc.Method.Name <> "DefaultIfEmpty" ->
                            let innerStages = flattenStages innerMc []
                            if not innerStages.IsEmpty then Some innerStages else None
                        | _ -> None
                    else
                        None

                match nestedStagesOpt with
                | Some innerStages -> flattenStages source (innerStages @ acc)
                | None ->
                    if isUnpivotPattern colSel then
                        flattenStages source (LinqStage.Unpivot(colSel, resSelOpt) :: acc)
                    elif containsParam then
                        flattenStages source (LinqStage.Explode(colSel, resSelOpt) :: acc)
                    else
                        flattenStages source (LinqStage.CrossJoin(colSel.Body, resSelOpt) :: acc)

        | MethodCall(m, null, [ source; second ]) when m.Name = "Zip" ->
            flattenStages source (LinqStage.Zip(second, None) :: acc)

        | MethodCall(m, null, [ source; second; StripQuotes(:? LambdaExpression as resSel) ]) when m.Name = "Zip" ->
            flattenStages source (LinqStage.Zip(second, Some resSel) :: acc)

        | MethodCall(m, null, [ first; second; third ]) when m.Name = "Zip" ->
            flattenStages first (LinqStage.Zip3(second, third) :: acc)

        | MethodCall(m, null, [ source; sizeExpr ]) when m.Name = "Chunk" ->
            match tryEvaluate sizeExpr with
            | Some s ->
                let size = Convert.ToInt32 s

                if size <= 0 then
                    raise (ArgumentOutOfRangeException("size", "Chunk size must be positive."))

                flattenStages source (LinqStage.Chunk size :: acc)
            | None -> failwithf "Could not evaluate Chunk size argument: %A" sizeExpr

        | MethodCall(m, null, [ source; elemExpr ]) when m.Name = "Append" || m.Name = "Prepend" ->
            let stage = if m.Name = "Append" then LinqStage.Append elemExpr else LinqStage.Prepend elemExpr
            flattenStages source (stage :: acc)

        | MethodCall(m, null, [ source; defaultExpr ]) when m.Name = "DefaultIfEmpty" ->
            flattenStages source (LinqStage.DefaultIfEmpty(Some defaultExpr) :: acc)

        | MethodCall(m, null, source :: CleanLambda keyLambda :: _) when m.Name = "CountBy" ->
            flattenStages source (LinqStage.CountBy keyLambda :: acc)

        | MethodCall(m, null, source :: CleanLambda k :: CleanLambda seedFactory :: CleanLambda f :: _) when m.Name = "AggregateBy" ->
            flattenStages source (LinqStage.AggregateBy(k, null, Some seedFactory, f) :: acc)

        | MethodCall(m, null, source :: CleanLambda k :: seedExpr :: CleanLambda f :: _) when m.Name = "AggregateBy" ->
            flattenStages source (LinqStage.AggregateBy(k, seedExpr, None, f) :: acc)

        | _ -> fuseSortStages acc
