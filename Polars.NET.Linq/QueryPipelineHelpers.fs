namespace Polars.NET.Linq

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core
open Polars.NET.Core.Helpers

/// Internal classification of resolved predicate pushdown state
type internal ResolvedPredicate =
    | Pushdown of ExprHandle
    | Client of effectiveType: Type * pred: Delegate

/// Row selection strategy for scalar single-row operations
type internal RowFetchKind =
    | First
    | Last
    | Single

type internal ExecutionPlan<'T> =
    {
        NativePlan: LazyFrameHandle
        ClientPredicates: Func<obj, bool> list
        ClientProjection: (Type * Func<obj, obj>) option
    }

/// Internal interface allowing direct zero-reflection plan querying between Provider and Query instances
type internal IInternalPlanSource =
    abstract member GetPlanInfo: unit -> LazyFrameHandle * bool * (Type * Func<obj, obj>) option

/// Internal normalized context capturing all GroupBy variants (C# 4-args, F# pipeline, Having, Sorts)
type internal GroupFoldContext =
    {
        KeyLambda: LambdaExpression
        ElemLambdaOpt: LambdaExpression option
        HavingPreds: LambdaExpression list
        SortStages: (LambdaExpression * bool) list
    }

/// Strongly-typed IGrouping implementation for in-memory GroupJoin materialization.
type internal MaterializedGrouping<'TKey, 'TElement>(key: 'TKey, elements: 'TElement array) =
    // 1. Explicit base interface implementations resolving FS0363 interface diamond collision
    interface IEnumerable<'TElement> with
        member _.GetEnumerator() : IEnumerator<'TElement> =
            (elements :> IEnumerable<'TElement>).GetEnumerator()

    interface IEnumerable with
        member _.GetEnumerator() : IEnumerator = elements.GetEnumerator()

    // 2. IGrouping specific implementation
    interface IGrouping<'TKey, 'TElement> with
        member _.Key = key

    // 3. Collection interfaces for O(1) counting and fast BCL compatibility
    interface IReadOnlyCollection<'TElement> with
        member _.Count = elements.Length

    interface ICollection with
        member _.Count = elements.Length
        member _.IsSynchronized = false
        member _.SyncRoot = elements.SyncRoot
        member _.CopyTo(array, index) = elements.CopyTo(array, index)

/// Represents the diagnostic inspection result of a Polars LINQ query pipeline.
type PlanInspection =
    {
        NativePlan: string
        HasClientPredicates: bool
        ClientPredicateExpressions: LambdaExpression list
        HasClientProjection: bool
        IsFullFallback: bool
        FallbackReason: string option
    }

    /// Formats the inspection result into a human-readable diagnostic report.
    member this.FormatReport() : string =
        let sb = System.Text.StringBuilder()

        if this.IsFullFallback then
            sb.AppendLine "[Polars.NET Query Plan - Full Fallback to In-Memory LINQ]"
            |> ignore

            sb.AppendLine "---------------------------------------------------------"
            |> ignore

            sb.AppendLine "Warning: The query contains unsupported expressions or transparent identifiers."
            |> ignore

            match this.FallbackReason with
            | Some reason ->
                sb.AppendLine(sprintf "   Diagnostic reason: %s" reason) |> ignore
            | None -> ()

            sb.AppendLine "   The entire pipeline will be executed in managed memory via LINQ-to-Objects."
            |> ignore
        elif not this.HasClientPredicates && not this.HasClientProjection then
            sb.AppendLine "[Polars.NET Query Plan - 100% Native Pushdown]" |> ignore
            sb.AppendLine "---------------------------------------------" |> ignore
            sb.AppendLine this.NativePlan |> ignore
        else
            sb.AppendLine "[Polars.NET Query Plan - Hybrid Execution (Partial Pushdown)]"
            |> ignore

            sb.AppendLine "------------------------------------------------------------"
            |> ignore

            sb.AppendLine "Phase 1: Native Polars Pushdown Plan:" |> ignore
            sb.AppendLine this.NativePlan |> ignore
            sb.AppendLine "Phase 2: Managed Client Fallback:" |> ignore

            if this.HasClientPredicates then
                sb.AppendLine " Client-side Predicates (Evaluated in .NET memory):" |> ignore

                for pred in this.ClientPredicateExpressions do
                    sb.AppendFormat("     • {0}\n", pred.ToString()) |> ignore

            if this.HasClientProjection then
                sb.AppendLine " Client-side Projection: Expression could not be translated natively."
                |> ignore
                match this.FallbackReason with
                | Some r -> sb.AppendLine(sprintf "   Reason: %s" r) |> ignore
                | None -> ()

        sb.ToString().TrimEnd()

/// Public interface implemented by PolarsQueryable to expose query plan inspection.
type IPolarsExplainable =
    abstract member InspectPlan: optimized: bool -> PlanInspection
    abstract member Explain: optimized: bool -> string

type internal FallbackTracker =
    [<ThreadStatic; DefaultValue>]
    static val mutable private diagnostic: string option
    static member Diagnostic
        with get () = FallbackTracker.diagnostic
        and set (v: string option) = FallbackTracker.diagnostic <- v

/// Shared pure helpers for LINQ expression inspection, stage flattening, operator compilation, and policy enforcement.
[<AutoOpen>]
module internal QueryPipelineHelpers =

    // =========================================================================
    // 1. Type, Schema & Policy Helpers
    // =========================================================================

    /// Creates an appropriate TranslationContext based on whether the sequence element type
    /// is a scalar/primitive (bound to the underlying single physical column) or a composite entity.
    let createContextForType (elemType: Type) (lf: LazyFrameHandle) (paramName: string) : TranslationContext =
        SubqueryDecorrelator.createContextForType elemType lf paramName

    /// Creates a TranslationContext that prioritizes registered scope mappings (for deep multi-table joins)
    /// before falling back to default schema extraction.
    let createContextWithScope (scopeMap: Map<string * string, string>) (elemType: Type) (lf: LazyFrameHandle) (paramName: string) : TranslationContext =
        if scopeMap.IsEmpty then
            createContextForType elemType lf paramName
        else
            let cols = PolarsSchemaExtractor.GetColumnNames lf |> Array.toList
            TranslationContext.ScopedEntity([ paramName ], scopeMap, cols)

    /// Extracts the underlying sequence element type from generic collections/arrays
    let getSequenceElementType (t: Type) : Type =
        if t.IsGenericType then t.GetGenericArguments().[0]
        elif t.IsArray then t.GetElementType()
        else t

    /// Resolves the single physical column name if the sequence is scalar (e.g. string, int, double).
    let tryGetScalarColumnName (elemType: Type) (lf: LazyFrameHandle) : string option =
        if PolarsTypeHelper.IsScalarType elemType || elemType = typeof<string> then
            let colNames = PolarsSchemaExtractor.GetColumnNames lf
            if colNames.Length = 1 then Some colNames.[0] else None
        else
            None

    /// Dispatches and resolves predicate into either native ExprHandle pushdown or composed client Func<obj, bool>
    let tryResolvePredicate
        (hasClientPreds: bool)
        (clientProjOpt: (Type * Func<obj, obj>) option)
        (elemType: Type)
        (nativeLf: LazyFrameHandle)
        (predLambdaOpt: LambdaExpression option)
        : ResolvedPredicate voption =
        match predLambdaOpt with
        | None -> ValueNone
        | Some predLambda ->
            let nativeFilterOpt =
                if not hasClientPreds && clientProjOpt.IsNone then
                    let ctx = createContextForType elemType nativeLf predLambda.Parameters.[0].Name
                    ExprTranslator.tryTranslateWithContext ctx predLambda.Body
                else
                    None

            match nativeFilterOpt with
            | Some filterExpr -> ValueSome(Pushdown filterExpr)
            | None ->
                let compiledPred = predLambda.Compile()
                let predArgType = predLambda.Parameters.[0].Type

                match clientProjOpt with
                | Some(sourceType, projFunc) ->
                    let combinedPred =
                        LinqReflectionCache.CreateComposedPredicate(sourceType, projFunc, compiledPred, predArgType)

                    ValueSome(Client(sourceType, combinedPred))
                | None -> ValueSome(Client(elemType, compiledPred))

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

    /// Maps standard reduction method names to Polars aggregate expressions
    let resolveScalarAggExpr (methodName: string) (colExpr: ExprHandle) : ExprHandle =
        match methodName with
        | "Max" -> PolarsWrapper.Max colExpr
        | "Min" -> PolarsWrapper.Min colExpr
        | "Sum" -> PolarsWrapper.Sum colExpr
        | "Average" -> PolarsWrapper.Mean colExpr
        | other -> failwithf "Unsupported scalar aggregate operator: %s" other

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

    /// Helper to extract all column names and their corresponding DataTypeHandle from a LazyFrame schema
    let getLazySchemaFields (lf: LazyFrameHandle) : (string * DataTypeHandle) array =
        let schema = PolarsWrapper.GetLazySchema lf
        let count = PolarsWrapper.GetSchemaLen schema

        Array.init (int count) (fun i ->
            let mutable name = null
            let mutable dtHandle = new DataTypeHandle()
            PolarsWrapper.GetSchemaFieldAt(schema, uint64 i, &name, &dtHandle)
            name, dtHandle)

    /// Disambiguates duplicate column names against an existing set of columns by appending a given suffix,
    /// returning the safely renamed LazyFrame, updated column set, and a renaming map.
    let disambiguateColumns
        (existingCols: Set<string>)
        (lf: LazyFrameHandle)
        (suffix: string)
        : LazyFrameHandle * Set<string> * Map<string, string> =
        let cols = PolarsSchemaExtractor.GetColumnNames lf
        let duplicates = cols |> Array.filter existingCols.Contains

        if duplicates.Length > 0 then
            let newNames = duplicates |> Array.map (fun c -> c + suffix)
            let renamedLf = PolarsWrapper.LazyRename(lf, duplicates, newNames, strict = false)
            let suffixMap = Array.zip duplicates newNames |> Map.ofArray
            renamedLf, existingCols |> Set.union (Set.ofArray cols), suffixMap
        else
            lf, existingCols |> Set.union (Set.ofArray cols), Map.empty

    /// Applies eager row shuffling (sample_frac = 1.0, without replacement) to a DataFrame.
    let applyShuffle (df: DataFrameHandle) : DataFrameHandle =
        let fracHandle = PolarsWrapper.SeriesNew("", [| 1.0 |])
        PolarsWrapper.SampleFrac(df, fracHandle, false, Nullable true, Nullable())

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

    /// Recursively locates the root IQueryable / IPolarsPlanSource ConstantExpression in a LINQ pipeline.
    let rec tryFindRootQueryable (e: Expression) : (Expression * Type) option =
        match e with
        | null -> None
        | :? ConstantExpression as ce when
            typeof<IQueryable>.IsAssignableFrom(ce.Type)
            || typeof<IPolarsPlanSource>.IsAssignableFrom(ce.Type)
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
            Some [ ("Key", k); ("Value", v) ]
        | :? NewExpression as ne when ne.Type.Name.StartsWith "KeyValuePair" && ne.Arguments.Count = 2 ->
            Some [ ("Key", ne.Arguments.[0]); ("Value", ne.Arguments.[1]) ]
        | :? ConstantExpression as ce when not (isNull ce.Value) && not (PolarsTypeHelper.IsScalarType ce.Type || ce.Type = typeof<string>) ->
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
            let cleanBody = ExprTranslator.unwrap colLambda.Body

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
                                        |> ExprTranslator.unwrap)
                                name, exprs)

                        let isInvariantOuterMember (exprs: Expression list) =
                            let names =
                                exprs
                                |> List.choose (function
                                    | MemberAccess(target, m) when
                                        match ExprTranslator.unwrap target with
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
                                        match ExprTranslator.unwrap target with
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
                                                match ExprTranslator.unwrap e with
                                                | MemberAccess(target, m) when
                                                    (match ExprTranslator.unwrap target with :? ParameterExpression as p -> p.Name = pInner.Name | _ -> false)
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

    /// Inspects GroupJoin result selector to detect group-level aggregations (e.g., g.Count())
    let tryExtractGroupAgg
        (groupParamName: string)
        (resLambda: LambdaExpression)
        : (string * ExprHandle) option =
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
                    Some(memName, PolarsWrapper.Alias(PolarsWrapper.Len(), memName))
                | _ -> None)
        | _ -> None

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
                    match ExprTranslator.unwrap node.Expression with
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

    /// Builds a Polars equality filter expression matching an item (scalar primitive or composite entity/record)
    /// against the columns of a target LazyFrame.
    let buildItemEqualityFilter (itemVal: obj) (targetLf: LazyFrameHandle) : ExprHandle option =
        if isNull itemVal then
            None
        else
            let itemType = itemVal.GetType()
            if
                PolarsTypeHelper.IsScalarType itemType
                || itemType = typeof<string>
                || itemType.IsEnum
            then
                let colNames = PolarsSchemaExtractor.GetColumnNames targetLf
                let targetCol = if colNames.Length > 0 then colNames.[0] else ""
                let colExpr = PolarsWrapper.Col targetCol
                let litExpr = ExprTranslator.toLiteralHandle itemVal itemType
                Some(PolarsWrapper.Eq(colExpr, litExpr))
            else
                let props = itemType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)

                let conditions =
                    props
                    |> Array.choose (fun p ->
                        let v = p.GetValue itemVal

                        if isNull v then
                            None
                        else
                            try
                                let colExpr = PolarsWrapper.Col p.Name
                                let lit = ExprTranslator.toLiteralHandle v p.PropertyType
                                Some(PolarsWrapper.Eq(colExpr, lit))
                            with _ ->
                                None)

                if conditions.Length > 0 then
                    Some(conditions |> Array.reduce (fun acc c -> PolarsWrapper.And(acc, c)))
                else
                    None

    // =========================================================================
    // 3. LINQ Expression Stage Flattener
    // =========================================================================

    let internal extractTolerance (objVal: obj) : string option * int64 option * double option =
        if isNull objVal then
            (None, None, None)
        else
            let t = objVal.GetType()
            if t = typeof<string> then
                (Some(objVal :?> string), None, None)
            elif t = typeof<TimeSpan> then
                let ts = objVal :?> TimeSpan
                (Some(DurationFormatter.ToPolarsString ts), None, None)
            elif t = typeof<int64> then
                (None, Some(objVal :?> int64), None)
            elif t = typeof<int> then
                (None, Some(int64 (objVal :?> int)), None)
            elif t = typeof<double> then
                (None, None, Some(objVal :?> double))
            elif t.Name = "AsOfTolerance" || (t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<Nullable<_>> && t.GetGenericArguments().[0].Name = "AsOfTolerance") then
                let actualObj =
                    if t.IsGenericType then
                        let hasValueProp = t.GetProperty("HasValue")
                        let valueProp = t.GetProperty("Value")
                        if not (hasValueProp.GetValue(objVal) :?> bool) then null
                        else valueProp.GetValue(objVal)
                    else objVal
                if isNull actualObj then
                    (None, None, None)
                else
                    let actualType = actualObj.GetType()
                    let sField = actualType.GetField("StringVal", BindingFlags.Instance ||| BindingFlags.NonPublic ||| BindingFlags.Public)
                    let iField = actualType.GetField("IntVal", BindingFlags.Instance ||| BindingFlags.NonPublic ||| BindingFlags.Public)
                    let dField = actualType.GetField("FloatVal", BindingFlags.Instance ||| BindingFlags.NonPublic ||| BindingFlags.Public)
                    let sVal = if isNull sField then null else sField.GetValue(actualObj) :?> string
                    let iVal = if isNull iField then Nullable<int64>() else iField.GetValue(actualObj) :?> Nullable<int64>
                    let dVal = if isNull dField then Nullable<double>() else dField.GetValue(actualObj) :?> Nullable<double>
                    (Option.ofObj sVal, (if iVal.HasValue then Some iVal.Value else None), (if dVal.HasValue then Some dVal.Value else None))
            else
                (None, None, None)

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

    /// Recursively flattens a LINQ expression tree into a linear sequence of LinqStage values.
    let rec flattenStages (e: Expression) (acc: LinqStage list) : LinqStage list =
        let evalUInt32 opName (expr: Expression) =
            match tryEvaluate expr with
            | Some c -> Convert.ToUInt32 c
            | None -> failwithf "Could not evaluate %s count argument: %A" opName expr

        match e with
        | null -> acc
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
                            match ExprTranslator.unwrap arg with
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
                | "OrderBy" | "ThenBy" -> LinqStage.Sort(l, isDescending = false)
                | "OrderByDescending" | "ThenByDescending" -> LinqStage.Sort(l, isDescending = true)
                | "DistinctBy" -> LinqStage.DistinctBy l
                | "GroupBy" -> LinqStage.GroupByKey l
                | "TakeWhile" -> LinqStage.TakeWhile l
                | _ -> LinqStage.SkipWhile l

            flattenStages source (stage :: acc)

        // Single-argument (source-only) stages
        | MethodCall(m, null, [ source ]) ->
            let stageOpt =
                match m.Name with
                | "Distinct" -> Some LinqStage.Distinct
                | "Index" -> Some LinqStage.Index
                | "Order" -> Some(LinqStage.SortDefault(isDescending = false))
                | "OrderDescending" -> Some(LinqStage.SortDefault(isDescending = true))
                | "Reverse" -> Some LinqStage.Reverse
                | "Shuffle" -> Some LinqStage.Shuffle
                | "DefaultIfEmpty" -> Some(LinqStage.DefaultIfEmpty None)
                | "Cast" when m.IsGenericMethod -> Some(LinqStage.Cast(m.GetGenericArguments().[0]))
                | "OfType" when m.IsGenericMethod ->
                    Some(LinqStage.OfType(getSequenceElementType source.Type, m.GetGenericArguments().[0]))
                | _ -> None

            match stageOpt with
            | Some stage -> flattenStages source (stage :: acc)
            | None -> acc

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

        | MethodCall(m, null, [ source; CleanLambda k; CleanLambda r ]) when m.Name = "GroupByRollup" ->
            flattenStages source (LinqStage.GroupByRollup(k, r) :: acc)

        | MethodCall(m, null, [ source; CleanLambda k; CleanLambda r ]) when m.Name = "GroupByCube" ->
            flattenStages source (LinqStage.GroupByCube(k, r) :: acc)

        | MethodCall(m, null, [ source; CleanLambda k; setsExpr; CleanLambda r ]) when m.Name = "GroupByGroupingSets" ->
            flattenStages source (LinqStage.GroupByGroupingSets(k, setsExpr, r) :: acc)

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
                    match ExprTranslator.unwrap colSel.Body with
                    | :? MethodCallExpression as innerMc -> flattenStages innerMc []
                    | _ -> []
                let stage = LinqStage.GroupPartitionSlice(groupKeySelOpt.Value, innerStages, resSelOpt)
                flattenStages baseGroupSource (stage :: acc)
            else
                let nestedStagesOpt =
                    if containsParam then
                        match ExprTranslator.unwrap colSel.Body with
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

        | _ -> acc

    // =========================================================================
    // 4. Stage-to-QueryOp Compilers & Native Plan Builder
    // =========================================================================

    /// Attempts to decorrelate subquery expressions (EXISTS/NOT EXISTS via Any, and IN/NOT IN via Contains)
    /// into native Semi/Anti Joins.
    let tryDecorrelateSubquery
        (currentLf: LazyFrameHandle)
        (filterLambda: LambdaExpression)
        : JoinSpec option =
        match SubqueryDecorrelator.tryDecorrelateFilter currentLf filterLambda Map.empty with
        | Some {
                   GeneratedOps = [ QueryOp.Join spec ]
                   ResidualLambda = None
               } -> Some spec
        | _ -> None

    /// Inspects whether a projection is an intermediate Roslyn Transparent Identifier (e.g. C# 'let' clause),
    /// which packages existing context and introduces new calculated columns via WithColumns.
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
                match ExprTranslator.unwrap expr with
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
                        match ExprTranslator.unwrap expr with
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
                        match ExprTranslator.tryResolveColumnNameWithContext ctx colExpr with
                        | Some c -> Some(PolarsWrapper.Col c)
                        | None -> ExprTranslator.tryTranslateWithContext ctx colExpr

                    let litHandleOpt =
                        let cleanFallback = ExprTranslator.unwrap fallbackExpr

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
                ExprTranslator.tryResolveColumnNameWithContext ctx ma
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
            match projLambda.Body with
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
                let baseSuffix = if isRightJoin then "_left" else "_right"
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
                            Suffix = Some "_right"
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
                    pRight, pLeft, rightCols, leftCols, suffixOpt |> Option.defaultValue "_left"
                else
                    pLeft, pRight, leftCols, rightCols, suffixOpt |> Option.defaultValue "_right"

            let firstColSet = Set.ofArray firstCols
            let ctx =
                if not scopeMap.IsEmpty then
                    TranslationContext.ScopedEntity([ firstParam.Name; secondParam.Name ], scopeMap, Array.toList firstCols @ Array.toList secondCols)
                else
                    TranslationContext.BinaryJoin(firstParam.Name, secondParam.Name, firstColSet, secondSuffix)

            let unwrappedBody = ExprTranslator.unwrap resLambda.Body
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

            | MemberAccess _ as ma when not (PolarsTypeHelper.IsScalarType ma.Type || ma.Type = typeof<string>) ->
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
                typeof<DataFrameBuilder>
                    .GetMethod("FromRows", BindingFlags.Public ||| BindingFlags.Static)
                    .MakeGenericMethod(elemType)

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
            match ExprTranslator.unwrap expr with
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
            ExprTranslator.tryResolveColumnNameWithContext ctx ma
            |> Option.map PolarsWrapper.Col
        | Constant _ -> ExprTranslator.tryTranslateWithContext TranslationContext.Empty e
        | _ -> None

    /// Checks whether a two-parameter Join result selector merely packages parameters into a transparent identifier/tuple
    let isTransparentJoinResSelector (resSel: LambdaExpression) : bool =
        match tryExtractMemberBindings resSel.Body with
        | Some bindings when resSel.Parameters.Count >= 2 ->
            let p1, p2 = resSel.Parameters.[0].Name, resSel.Parameters.[1].Name

            let rec isTransparentArg (e: Expression) =
                match ExprTranslator.unwrap e with
                | :? ParameterExpression as p -> p.Name = p1 || p.Name = p2
                | MemberAccess(target, m) when isNavigationMember m -> isTransparentArg target
                | :? NewExpression as ne when isTupleOrAnonType ne.Type ->
                    ne.Arguments |> Seq.forall isTransparentArg
                | _ -> false

            bindings |> List.forall (fun (_, expr) -> isTransparentArg expr)
        | _ -> false

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
                let baseSuffix = "_right"
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
                            Suffix = Some "_right"
                            InvertSides = false
                            Coalesce = PlJoinCoalesce.JoinSpecific
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
                    | LinqStage.Sort(l, isDesc) ->
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
                        Suffix = Some "_right"
                        InvertSides = false
                        Coalesce = PlJoinCoalesce.KeepColumns
                    }

            let renameOps =
                resLambdaOpt
                |> Option.map (fun r -> extractBinaryResultRenames r None |> buildRenameOps)
                |> Option.defaultValue []

            renameOps @ [ joinOp ])

    /// Compiles CountBy into a GroupBy(Key, Len() as Value) operation
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
    let tryTranslateGroupByAgg
        (groupParamName: string)
        (targetColExprOpt: ExprHandle option)
        (scalarColOpt: string option)
        (wholeRowStructOpt: ExprHandle option)
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
                let selCtx = createContextForType selParam.Type lf selParam.Name

                let colExprOpt =
                    match ExprTranslator.tryResolveColumnNameWithContext selCtx selLambda.Body with
                    | Some cName -> Some(PolarsWrapper.Col cName)
                    | None -> ExprTranslator.tryTranslateWithContext selCtx selLambda.Body

                colExprOpt |> Option.map (fun cExpr -> PolarsWrapper.Alias(cExpr, colName))
            | None ->
                AggTranslator.tryTranslateAggWithScalarCol
                    groupParamName
                    scalarColOpt
                    wholeRowStructOpt
                    argExpr
                    colName

    /// Unified GroupBy compiler powering both single-key and multi-key GroupBy pipelines (C# LINQ & F# QueryBuilder)
    let compileUnifiedGroupBy
        (lf: LazyFrameHandle)
        (ctx: GroupFoldContext)
        (projBody: Expression)
        (groupParamName: string)
        (isKeyArg: Expression -> bool)
        : QueryOp list option =

        let keyParam = ctx.KeyLambda.Parameters.[0]
        let keyCtx = createContextForType keyParam.Type lf keyParam.Name

        let elemExprOpt =
            ctx.ElemLambdaOpt
            |> Option.bind (fun el ->
                let elParam = el.Parameters.[0]
                let elCtx = createContextForType elParam.Type lf elParam.Name
                ExprTranslator.tryTranslateWithContext elCtx el.Body)

        let elemType = keyParam.Type

        let wholeRowStructOpt: ExprHandle option =
            try
                let props = elemType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)

                if props.Length > 0 then
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

                let tryTranslateSingleAgg (colName: string) (rawArgExpr: Expression) : ExprHandle option =
                    tryTranslateGroupByAgg
                        groupParamName
                        targetColExprOpt
                        scalarColOpt
                        wholeRowStructOpt
                        lf
                        colName
                        rawArgExpr

                let translatedAggOpts =
                    nonKeyArgs
                    |> List.map (fun (colName, argExpr) -> tryTranslateSingleAgg colName argExpr)

                if not (translatedAggOpts |> List.forall Option.isSome) then
                    None
                else
                    let aggs = translatedAggOpts |> List.choose id

                    let resolveResultColName (e: Expression) : string option =
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
                        | MemberAccess(p, m) when not (isNull p) && p.NodeType = ExpressionType.Parameter -> Some m.Name
                        | MethodCall _ as aggCall ->
                            List.zip memberNames args
                            |> List.tryPick (fun (colName, argExpr) ->
                                if (ExprTranslator.unwrap argExpr).ToString() = aggCall.ToString() then
                                    Some colName
                                else
                                    None)
                        | _ -> None

                    let extraHavingAggs = ResizeArray<string * ExprHandle>()

                    let resolveHavingAggColName (aggCall: MethodCallExpression) : string option =
                        match resolveResultColName aggCall with
                        | Some colName -> Some colName
                        | None ->
                            let aggStr = aggCall.ToString()
                            match extraHavingAggs |> Seq.tryFindIndex (fun (str, _) -> str = aggStr) with
                            | Some idx -> Some (sprintf "__having_agg_%d" idx)
                            | None ->
                                let colName = sprintf "__having_agg_%d" extraHavingAggs.Count
                                match tryTranslateSingleAgg colName aggCall with
                                | Some h ->
                                    extraHavingAggs.Add((aggStr, h))
                                    Some colName
                                | None -> None

                    let rec translateHaving (expr: Expression) : ExprHandle option =
                        match expr with
                        | Binary(op, left, right) ->
                            match translateHaving left, translateHaving right with
                            | Some l, Some r ->
                                try
                                    Some(ExprTranslator.translateBinary op l r)
                                with _ ->
                                    None
                            | _ -> None
                        | Constant _ -> ExprTranslator.tryTranslateWithContext TranslationContext.Empty expr
                        | other ->
                            match resolveResultColName other with
                            | Some colName -> Some(PolarsWrapper.Col colName)
                            | None ->
                                match ExprTranslator.unwrap other with
                                | :? MethodCallExpression as aggCall ->
                                    resolveHavingAggColName aggCall |> Option.map PolarsWrapper.Col
                                | _ -> None

                    let havingFilterOpt =
                        ctx.HavingPreds
                        |> List.map (fun havingPred -> translateHaving havingPred.Body)

                    if not (havingFilterOpt |> List.forall Option.isSome) then
                        None
                    else
                        let havingFilterOps = havingFilterOpt |> List.choose id |> List.map QueryOp.Filter
                        let allAggs = aggs @ (extraHavingAggs |> Seq.map snd |> Seq.toList)

                        let groupBySpec =
                            {
                                Keys = keyAliasedExprs
                                Aggs = allAggs |> List.toArray
                                Having = None
                                MaintainOrder = false
                            }

                        let groupByOp = QueryOp.GroupBy groupBySpec

                        let sortSpecs =
                            ctx.SortStages
                            |> List.choose (fun (sortLambda, isDesc) ->
                                resolveResultColName sortLambda.Body
                                |> Option.map (fun colName ->
                                    {
                                        Expr = PolarsWrapper.Col colName
                                        Descending = isDesc
                                        NullsLast = false
                                    }))

                        let sortOps = if sortSpecs.IsEmpty then [] else [ QueryOp.Sort sortSpecs ]

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

                        Some(finalSelectOp :: sortOps @ havingFilterOps @ [ groupByOp ])

    /// Applies a list of QueryOp to a target LazyFrameHandle in forward order.
    let applyNativeOps (sourceLf: LazyFrameHandle) (ops: QueryOp list) : LazyFrameHandle =
        ops
        |> List.fold
            (fun currentLf op ->
                match op with
                | QueryOp.WithColumns exprs -> PolarsWrapper.LazyWithColumns(currentLf, exprs)

                | QueryOp.WithRowIndex(name, offsetOpt) ->
                    let offsetNullable =
                        match offsetOpt with
                        | Some o -> Nullable(int o)
                        | None -> Nullable 0

                    PolarsWrapper.LazyFrameWithRowIndex(currentLf, name, offsetNullable)

                | QueryOp.Filter exprHandle -> PolarsWrapper.LazyFilter(currentLf, exprHandle)

                | QueryOp.Select exprs -> PolarsWrapper.LazySelect(currentLf, exprs)

                | QueryOp.Slice(offset, len) -> PolarsWrapper.LazySlice(currentLf, offset, len)

                | QueryOp.Sort specs ->
                    let exprs = specs |> List.map (fun s -> s.Expr) |> List.toArray
                    let descending = specs |> List.map (fun s -> s.Descending) |> List.toArray
                    let nullsLast = specs |> List.map (fun s -> s.NullsLast) |> List.toArray
                    PolarsWrapper.LazyFrameSort(currentLf, exprs, descending, nullsLast, maintainOrder = false)

                | QueryOp.Join spec ->
                    let leftLf, rightLf, leftOn, rightOn, suffix =
                        if spec.InvertSides then
                            spec.RightLf,
                            currentLf,
                            spec.RightOn,
                            spec.LeftOn,
                            Option.toObj (spec.Suffix |> Option.orElse (Some "_left"))
                        else
                            currentLf, spec.RightLf, spec.LeftOn, spec.RightOn, Option.toObj spec.Suffix

                    PolarsWrapper.Join(
                        leftLf,
                        rightLf,
                        leftOn,
                        rightOn,
                        spec.How,
                        suffix,
                        PlJoinValidation.ManyToMany,
                        spec.Coalesce,
                        PlJoinMaintainOrder.None,
                        PlJoinSide.None,
                        false,
                        Nullable(),
                        0UL
                    )

                | QueryOp.JoinAsOf spec ->
                    PolarsWrapper.JoinAsOf(
                        currentLf,
                        spec.RightLf,
                        spec.LeftOn,
                        spec.RightOn,
                        Option.toObj spec.LeftBy,
                        Option.toObj spec.RightBy,
                        spec.Strategy,
                        Option.toObj spec.ToleranceStr,
                        Option.toNullable spec.ToleranceInt,
                        Option.toNullable spec.ToleranceFloat,
                        spec.AllowEq,
                        spec.CheckSorted,
                        Option.toObj spec.Suffix,
                        spec.Validation,
                        spec.Coalesce,
                        spec.MaintainOrder,
                        spec.JoinSide,
                        spec.NullsEqual,
                        Option.toNullable spec.SliceOffset,
                        spec.SliceLen
                    )

                | QueryOp.GroupBy spec ->
                    PolarsWrapper.LazyGroupByAgg(
                        currentLf,
                        spec.Keys,
                        spec.Aggs,
                        Option.toObj spec.Having,
                        spec.MaintainOrder
                    )

                | QueryOp.Unique spec ->
                    let selector =
                        match spec.SubsetCols with
                        | Some cols when cols.Length > 0 -> PolarsWrapper.SelectorCols cols
                        | _ -> null

                    PolarsWrapper.LazyUnique(currentLf, selector, spec.Keep, spec.MaintainOrder)

                | QueryOp.Concat spec ->
                    let alignSchema (baseLf: LazyFrameHandle) (targetLf: LazyFrameHandle) : LazyFrameHandle =
                        let baseFields = getLazySchemaFields baseLf
                        let targetFields = getLazySchemaFields targetLf

                        let baseNames = baseFields |> Array.map fst
                        let targetNames = targetFields |> Array.map fst

                        if Set.ofArray baseNames = Set.ofArray targetNames then
                            let targetFieldMap = targetFields |> Map.ofArray

                            let reorderedAndCastedExprs =
                                baseFields
                                |> Array.map (fun (colName, baseDt) ->
                                    let targetDt = targetFieldMap.[colName]
                                    let colExpr = PolarsWrapper.Col colName

                                    if not (PolarsWrapper.DataTypeEq(baseDt, targetDt)) then
                                        let dtExpr = PolarsWrapper.DataTypeExprFromDataType baseDt
                                        PolarsWrapper.ExprCast(colExpr, dtExpr, strict = false, wrapNumerical = false)
                                    else
                                        colExpr)

                            PolarsWrapper.LazySelect(targetLf, reorderedAndCastedExprs)
                        elif baseFields.Length = targetFields.Length then
                            let projectedExprs =
                                Array.zip baseFields targetFields
                                |> Array.map (fun ((baseName, baseDt), (targetName, targetDt)) ->
                                    let colExpr = PolarsWrapper.Col targetName
                                    let castedExpr =
                                        if not (PolarsWrapper.DataTypeEq(baseDt, targetDt)) then
                                            let dtExpr = PolarsWrapper.DataTypeExprFromDataType baseDt
                                            PolarsWrapper.ExprCast(colExpr, dtExpr, strict = false, wrapNumerical = false)
                                        else
                                            colExpr
                                    PolarsWrapper.Alias(castedExpr, baseName))
                            PolarsWrapper.LazySelect(targetLf, projectedExprs)
                        else
                            targetLf

                    let alignedOtherLf = alignSchema currentLf spec.OtherLf

                    let handles =
                        if spec.Prepend then
                            [| alignedOtherLf; currentLf |]
                        else
                            [| currentLf; alignedOtherLf |]

                    PolarsWrapper.LazyConcat(handles, PlConcatType.Vertical, false, true)

                | QueryOp.HorizontalConcat otherLfs ->
                    let allHandles = Array.append [| currentLf |] otherLfs
                    PolarsWrapper.LazyConcat(allHandles, PlConcatType.Horizontal, false, true)

                | QueryOp.Explode spec ->
                    let selector = PolarsWrapper.SelectorCols spec.ColumnNames
                    PolarsWrapper.LazyExplode(currentLf, selector, spec.EmptyAsNull, spec.KeepNulls)

                | QueryOp.Unpivot spec ->
                    let indexSel = PolarsWrapper.SelectorCols spec.Index
                    let onSel =
                        match spec.On with
                        | Some onCols when onCols.Length > 0 -> PolarsWrapper.SelectorCols onCols
                        | _ -> null
                    PolarsWrapper.LazyUnpivot(currentLf, indexSel, onSel, spec.VariableName, spec.ValueName)

                | QueryOp.Rename spec ->
                    PolarsWrapper.LazyRename(currentLf, spec.ExistingNames, spec.NewNames, strict = false)

                | QueryOp.SkipLast count ->
                    let withIndexLf =
                        PolarsWrapper.LazyFrameWithRowIndex(currentLf, "__row_idx", Nullable 0)

                    let rowIdxCol = PolarsWrapper.Col "__row_idx"
                    let countLit = PolarsWrapper.Lit(uint32 count)
                    let lhsExpr = PolarsWrapper.Add(rowIdxCol, countLit)
                    let totalLen = PolarsWrapper.Len()
                    let filterExpr = PolarsWrapper.Lt(lhsExpr, totalLen)

                    let filteredLf = PolarsWrapper.LazyFilter(withIndexLf, filterExpr)
                    let dropSelector = PolarsWrapper.SelectorCols [| "__row_idx" |]
                    PolarsWrapper.LazyFrameDrop(filteredLf, dropSelector)

                | QueryOp.Reverse -> PolarsWrapper.LazyFrameReverse currentLf

                | QueryOp.SelectPassthrough -> currentLf)
            (PolarsWrapper.LazyClone sourceLf)

    [<RequireQualifiedAccess>]
    type internal GroupingKind =
        | Rollup
        | Cube
        | GroupingSets of Expression

    /// Unified compiler for multi-dimensional grouping sets (ROLLUP, CUBE, GROUPING SETS)
    let compileUnifiedGroupingSets
        (activeLf: LazyFrameHandle)
        (kind: GroupingKind)
        (keyLambda: LambdaExpression)
        (resLambda: LambdaExpression)
        : LazyFrameHandle option =

        let keyParam = keyLambda.Parameters.[0]
        let keyCtx = createContextForType keyParam.Type activeLf keyParam.Name

        let elemType = keyParam.Type

        let wholeRowStructOpt: ExprHandle option =
            try
                let props = elemType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)

                if props.Length > 0 then
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
            match ExprTranslator.unwrap keyLambda.Body with
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
                match tryExtractMemberBindings keyLambda.Body with
                | Some pairs -> Some pairs
                | None ->
                    let name =
                        match keyLambda.Body with
                        | ExtractColumnName col -> col
                        | _ -> "Key"

                    Some [ (name, keyLambda.Body) ]

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
            let numKeys = keyDefs.Length
            let isTwoParam = resLambda.Parameters.Count >= 2
            let groupParamName =
                if isTwoParam then resLambda.Parameters.[1].Name else resLambda.Parameters.[0].Name

            let isKeyArg (e: Expression) =
                let cleanE = ExprTranslator.unwrap e
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
                let cleanE = ExprTranslator.unwrap e
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
                    let cleanProj = ExprTranslator.unwrap resLambda.Body
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

                let outputAliasToKeyIdx =
                    keyDefs
                    |> List.mapi (fun idx (origKeyName, _) -> keyNameToOutputAliasMap.[origKeyName], idx)
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

                let scalarColOpt = tryGetScalarColumnName keyParam.Type activeLf
                let targetColExprOpt = scalarColOpt |> Option.map PolarsWrapper.Col

                let translatedAggOpts =
                    nonKeyArgs
                    |> List.map (fun (colName, argExpr) ->
                        tryTranslateGroupByAgg
                            groupParamName
                            targetColExprOpt
                            scalarColOpt
                            wholeRowStructOpt
                            activeLf
                            colName
                            argExpr)

                if not (translatedAggOpts |> List.forall Option.isSome) then
                    None
                else
                    let aggs = translatedAggOpts |> List.choose id

                    // Generate subsets of key indices
                    let subsets: int list list =
                        match kind with
                        | GroupingKind.Rollup ->
                            [ for len in numKeys .. -1 .. 0 -> [ 0 .. len - 1 ] ]
                        | GroupingKind.Cube ->
                            [ for mask in (1 <<< numKeys) - 1 .. -1 .. 0 ->
                                [ for i in 0 .. numKeys - 1 do
                                    if (mask &&& (1 <<< (numKeys - 1 - i))) <> 0 then yield i ] ]
                        | GroupingKind.GroupingSets setsExpr ->
                            let extractedSetsOpt =
                                match ExprTranslator.unwrap setsExpr with
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
                                            match ExprTranslator.unwrap elem with
                                            | :? ConstantExpression as ce when not (isNull ce.Value) && ce.Type = typeof<string[]> ->
                                                Some(ce.Value :?> string[])
                                            | :? NewArrayExpression as innerNae ->
                                                let strs =
                                                    innerNae.Expressions
                                                    |> Seq.choose (fun sExpr ->
                                                        match ExprTranslator.unwrap sExpr with
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

                    // Compile Level 0 (all keys) to capture schema and types
                    let level0Keys = keyAliasedExprs |> Array.map PolarsWrapper.CloneExpr
                    let level0Aggs = aggs |> List.map PolarsWrapper.CloneExpr |> List.toArray
                    let level0GroupOp = QueryOp.GroupBy { Keys = level0Keys; Aggs = level0Aggs; Having = None; MaintainOrder = false }
                    let level0GroupedLf = applyNativeOps (PolarsWrapper.LazyClone activeLf) [ level0GroupOp ]
                    let level0SelectExprs =
                        memberNames
                        |> List.map (fun name -> PolarsWrapper.Col name)
                        |> List.toArray
                    let level0Lf = PolarsWrapper.LazySelect(level0GroupedLf, level0SelectExprs)

                    let level0Fields = getLazySchemaFields level0Lf
                    let colTypeMap = level0Fields |> Map.ofArray

                    let compileLevel (subsetIndices: int list) : LazyFrameHandle =
                        if subsetIndices.Length = numKeys then
                            PolarsWrapper.LazyClone level0Lf
                        else
                            let groupedLf =
                                if subsetIndices.IsEmpty then
                                    let dummyKey = PolarsWrapper.Alias(PolarsWrapper.Lit 1, "__dummy_rollup_key__")
                                    let levelAggs = aggs |> List.map PolarsWrapper.CloneExpr |> List.toArray
                                    let groupByOp = QueryOp.GroupBy { Keys = [| dummyKey |]; Aggs = levelAggs; Having = None; MaintainOrder = false }
                                    applyNativeOps (PolarsWrapper.LazyClone activeLf) [ groupByOp ]
                                else
                                    let levelKeys = [| for i in subsetIndices -> PolarsWrapper.CloneExpr keyAliasedExprs.[i] |]
                                    let levelAggs = aggs |> List.map PolarsWrapper.CloneExpr |> List.toArray
                                    let groupByOp = QueryOp.GroupBy { Keys = levelKeys; Aggs = levelAggs; Having = None; MaintainOrder = false }
                                    applyNativeOps (PolarsWrapper.LazyClone activeLf) [ groupByOp ]

                            let subsetSet = Set.ofList subsetIndices
                            let projectedExprs =
                                memberNames
                                |> List.map (fun name ->
                                    match outputAliasToKeyIdx.TryFind name with
                                    | Some keyIdx ->
                                        if subsetSet.Contains keyIdx then
                                            PolarsWrapper.Col name
                                        else
                                            let dt = colTypeMap.[name]
                                            let dtExpr = PolarsWrapper.DataTypeExprFromDataType dt
                                            let nullExpr = PolarsWrapper.ExprCast(PolarsWrapper.LitNull(), dtExpr, strict = false, wrapNumerical = false)
                                            PolarsWrapper.Alias(nullExpr, name)
                                    | None ->
                                        // Aggregation column
                                        PolarsWrapper.Col name)
                                |> List.toArray

                            PolarsWrapper.LazySelect(groupedLf, projectedExprs)

                    let levelLfs = subsets |> List.map compileLevel |> List.toArray
                    let concatLf = PolarsWrapper.LazyConcat(levelLfs, PlConcatType.Vertical, false, true)
                    Some concatLf
