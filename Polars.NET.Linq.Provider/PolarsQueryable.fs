namespace Polars.NET.Linq.Provider

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core
open Polars.NET.Core.Helpers

/// Specification for multi-column sorting pushdown
type internal SortSpec = {
    Expr: ExprHandle
    Descending: bool
    NullsLast: bool
}

/// Specification for native join pushdown
type internal JoinSpec = {
    RightLf: LazyFrameHandle
    LeftOn: ExprHandle array
    RightOn: ExprHandle array
    How: PlJoinType
    Suffix: string option
}

/// Specification for native GroupBy aggregation pushdown
type internal GroupBySpec = {
    Keys: ExprHandle array
    Aggs: ExprHandle array
    Having: ExprHandle option
    MaintainOrder: bool
}

/// Uniform contract to expose internal LazyFrameHandle without leaking generic parameters
type internal IPolarsPlanSource =
    abstract member GetLazyFrameHandle: unit -> LazyFrameHandle

/// Abstract representation of pushdown operations in the LINQ execution pipeline
[<RequireQualifiedAccess>]
type internal QueryOp =
    | Filter of ExprHandle
    | Limit of uint32
    | Sort of SortSpec list
    | Join of JoinSpec
    | GroupBy of GroupBySpec
    | SelectPassthrough

/// Linear representation of query expressions before optimization/fusion
[<RequireQualifiedAccess>]
type internal LinqStage =
    | Filter of LambdaExpression
    | Sort of LambdaExpression * isDescending: bool
    | Limit of uint32
    | Join of MethodInfo * Expression * LambdaExpression * LambdaExpression * LambdaExpression
    | GroupByKey of LambdaExpression
    | GroupByWithResult of LambdaExpression * LambdaExpression
    | Project of LambdaExpression

/// Strongly-typed IQueryProvider backed by Polars.NET.Core
type PolarsQueryProvider(initialLazyFrame: LazyFrameHandle, materializer: IDataFrameMaterializer) =
    let clonedLf = PolarsWrapper.LazyClone initialLazyFrame

    member _.LazyFrame = clonedLf
    member _.Materializer = materializer

    interface IQueryProvider with
        member _.CreateQuery(expression: Expression) : IQueryable =
            let elemType =
                match PolarsTypeHelper.TryGetEnumerableElementType(expression.Type) with
                | null -> typedefof<obj>
                | t -> t

            let queryType = typedefof<PolarsQuery<_>>.MakeGenericType(elemType)
            Activator.CreateInstance(queryType, [| box clonedLf; box materializer; box expression |]) :?> IQueryable

        member this.CreateQuery<'TElement>(expression: Expression) : IQueryable<'TElement> =
            PolarsQuery<'TElement>(clonedLf, materializer, expression) :> IQueryable<'TElement>

        member _.Execute(expression: Expression) : obj =
            failwith "Scalar execution is not supported yet"

        member _.Execute<'TResult>(expression: Expression) : 'TResult =
            failwith "Scalar execution is not supported yet"

and PolarsQuery<'T> private (lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer, exprOpt: Expression option) as this =
    let lfCloned = PolarsWrapper.LazyClone lazyFrameHandle
    let expression =
        match exprOpt with
        | Some e -> e
        | None -> Expression.Constant(this, typeof<IQueryable<'T>>) :> Expression

    let provider = PolarsQueryProvider(lfCloned, materializer)

    new(lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer, expression: Expression) =
        PolarsQuery<'T>(lazyFrameHandle, materializer, Some expression)

    new(lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer) =
        PolarsQuery<'T>(lazyFrameHandle, materializer, None)

    static member private ResolveMaterializer(mat: IDataFrameMaterializer) =
        if not (isNull mat) then mat
        elif not (isNull DataFrameMaterializerRegistry.Default) then DataFrameMaterializerRegistry.Default
        else failwith "No DataFrameMaterializer registered. Ensure upper API layers configured a materializer."

    /// Uniformly extracts a LazyFrameHandle from an Expression (PolarsQuery, C# DataFrame/LazyFrame, or in-memory collection)
    static member internal ResolveToLazyFrameHandle (expr: Expression) : LazyFrameHandle option =
        let rec unwrap (e: Expression) =
            match e with
            | Unary(ExpressionType.Convert, inner)
            | Unary(ExpressionType.ConvertChecked, inner)
            | Unary(ExpressionType.Quote, inner) -> unwrap inner
            | other -> other

        let tryEvalRuntimeObj (e: Expression) : obj option =
            match tryEvaluate e with
            | Some v -> Some v
            | None ->
                try
                    let lambda = Expression.Lambda<Func<obj>>(Expression.Convert(e, typeof<obj>)).Compile()
                    Some (lambda.Invoke())
                with _ ->
                    None

        match unwrap expr |> tryEvalRuntimeObj with
        // Case 1: Right source is a PolarsQuery instance
        | Some (:? IPolarsPlanSource as plan) ->
            Some (plan.GetLazyFrameHandle())

        // Case 2: C# DataFrame or LazyFrame instance (extract 'Handle' property via reflection)
        | Some value when not (isNull value) ->
            let valueType = value.GetType()
            let handleProp = valueType.GetProperty("Handle", BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
            
            if not (isNull handleProp) then
                match handleProp.GetValue(value) with
                | :? DataFrameHandle as dfHandle -> Some (PolarsWrapper.DataFrameToLazy(dfHandle))
                | :? LazyFrameHandle as lfHandle -> Some (PolarsWrapper.LazyClone lfHandle)
                | _ -> None
            else
                // Case 3: In-memory sequence (List<T>, Array, etc.) -> Transpose via DataFrameBuilder.FromRows<T>
                match PolarsTypeHelper.TryGetEnumerableElementType(valueType) with
                | null -> None
                | elemType ->
                    let fromRows = typeof<DataFrameBuilder>.GetMethod("FromRows", BindingFlags.Public ||| BindingFlags.Static).MakeGenericMethod(elemType)
                    let dfHandle = fromRows.Invoke(null, [| value |]) :?> DataFrameHandle
                    Some (PolarsWrapper.DataFrameToLazy(dfHandle))

        | _ -> None

    /// Applies a list of QueryOp to a target LazyFrameHandle in forward order.
    static member private ApplyNativeOps (sourceLf: LazyFrameHandle) (ops: QueryOp list) : LazyFrameHandle =
        ops
        |> List.fold (fun currentLf op ->
            match op with
            | QueryOp.Filter exprHandle -> 
                PolarsWrapper.LazyFilter(currentLf, exprHandle)

            | QueryOp.Limit count -> 
                PolarsWrapper.LazySlice(currentLf, 0L, count)

            | QueryOp.Sort specs ->
                let exprs = specs |> List.map (fun s -> s.Expr) |> List.toArray
                let descending = specs |> List.map (fun s -> s.Descending) |> List.toArray
                let nullsLast = specs |> List.map (fun s -> s.NullsLast) |> List.toArray
                PolarsWrapper.LazyFrameSort(currentLf, exprs, descending, nullsLast, maintainOrder = false)

            | QueryOp.Join spec ->
                PolarsWrapper.Join(
                    currentLf,
                    spec.RightLf,
                    spec.LeftOn,
                    spec.RightOn,
                    spec.How,
                    Option.toObj spec.Suffix,
                    PlJoinValidation.ManyToMany,
                    PlJoinCoalesce.JoinSpecific,
                    PlJoinMaintainOrder.None,
                    PlJoinSide.None,
                    false,
                    Nullable(),
                    0UL
                )

            | QueryOp.GroupBy spec ->
                PolarsWrapper.LazyGroupByAgg(
                    currentLf,
                    spec.Keys,
                    spec.Aggs,
                    Option.toObj spec.Having,
                    spec.MaintainOrder
                )

            | QueryOp.SelectPassthrough -> 
                currentLf
        ) (PolarsWrapper.LazyClone sourceLf)

    /// Extracts member names and capitalizes them to PascalCase
    static member private ExtractPascalCaseMemberNames (newExpr: NewExpression) : string list =
        if not (isNull newExpr.Members) then 
            newExpr.Members |> Seq.map (fun m -> m.Name) |> Seq.toList
        else 
            newExpr.Constructor.GetParameters() 
            |> Seq.map (fun p -> 
                if String.IsNullOrEmpty(p.Name) then "Col"
                else Char.ToUpperInvariant(p.Name.[0]).ToString() + p.Name.Substring(1)) 
            |> Seq.toList

    /// Compiles a GroupByWithResult lambda pair into a native GroupBySpec
    static member private CompileGroupByWithResult (keyLambda: LambdaExpression) (resLambda: LambdaExpression) : GroupBySpec option =
        let keyParam = keyLambda.Parameters.[0]
        let resKeyParam = resLambda.Parameters.[0]
        let resGroupParam = resLambda.Parameters.[1]

        match ExprTranslator.tryTranslate keyParam.Name keyLambda.Body with
        | None -> None
        | Some keyExpr ->
            match resLambda.Body with
            | :? NewExpression as newExpr ->
                let args = newExpr.Arguments |> Seq.toList
                let memberNames = PolarsQuery<'T>.ExtractPascalCaseMemberNames newExpr
                let aggs =
                    List.zip memberNames args
                    |> List.choose (fun (colName, argExpr) ->
                        match argExpr with
                        | :? ParameterExpression as p when p.Name = resKeyParam.Name -> None
                        | MemberAccess(p, _) when p <> null && p.NodeType = ExpressionType.Parameter -> None
                        | _ -> AggTranslator.tryTranslateAgg resGroupParam.Name argExpr colName
                    )
                Some {
                    Keys = [| keyExpr |]
                    Aggs = aggs |> List.toArray
                    Having = None
                    MaintainOrder = false
                }
            | _ -> None

    /// Compiles a GroupByKey and subsequent Project (Select) lambda pair into a native GroupBySpec
    static member private CompileGroupByAndProjection (keyLambda: LambdaExpression) (projLambda: LambdaExpression) : GroupBySpec option =
        let keyParam = keyLambda.Parameters.[0]
        let groupParam = projLambda.Parameters.[0]

        match ExprTranslator.tryTranslate keyParam.Name keyLambda.Body with
        | None -> None
        | Some keyExpr ->
            match projLambda.Body with
            | :? NewExpression as newExpr ->
                let args = newExpr.Arguments |> Seq.toList
                let memberNames = PolarsQuery<'T>.ExtractPascalCaseMemberNames newExpr
                let aggs =
                    List.zip memberNames args
                    |> List.choose (fun (colName, argExpr) ->
                        match argExpr with
                        | MemberAccess(p, m) when p <> null && p.NodeType = ExpressionType.Parameter && m.Name = "Key" -> None
                        | _ -> AggTranslator.tryTranslateAgg groupParam.Name argExpr colName
                    )
                Some {
                    Keys = [| keyExpr |]
                    Aggs = aggs |> List.toArray
                    Having = None
                    MaintainOrder = false
                }
            | _ -> None

    /// Compiles a Join stage into a native JoinSpec
    static member private CompileJoin (methodInfo: MethodInfo) (innerExpr: Expression) (outerKey: LambdaExpression) (innerKey: LambdaExpression) : JoinSpec option =
        let how = if methodInfo.Name = "LeftJoin" then PlJoinType.Left else PlJoinType.Inner
        let rightLfOpt = PolarsQuery<'T>.ResolveToLazyFrameHandle innerExpr
        let leftKeyOpt = ExprTranslator.tryTranslate outerKey.Parameters.[0].Name outerKey.Body
        let rightKeyOpt = ExprTranslator.tryTranslate innerKey.Parameters.[0].Name innerKey.Body

        match rightLfOpt, leftKeyOpt, rightKeyOpt with
        | Some rightLf, Some leftKey, Some rightKey ->
            Some {
                RightLf = rightLf
                LeftOn = [| leftKey |]
                RightOn = [| rightKey |]
                How = how
                Suffix = Some "_right"
            }
        | _ -> None

    /// Parses the LINQ expression tree into native ops and any trailing client predicates
    member private this.CompilePipeline() : QueryOp list * (Func<'T, bool> list) =
        
        // -------------------------------------------------------------
        // Phase 1: Flatten Expression Tree into Linear Stages
        // -------------------------------------------------------------
        let rec flatten (e: Expression) (acc: LinqStage list) : LinqStage list =
            match e with
            | null -> acc

            // 1. Where
            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as l) ]) when m.Name = "Where" ->
                flatten source (LinqStage.Filter l :: acc)

            // 2. Sorting
            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as l) ]) when m.Name = "OrderBy" || m.Name = "ThenBy" ->
                flatten source (LinqStage.Sort(l, isDescending = false) :: acc)

            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as l) ]) when m.Name = "OrderByDescending" || m.Name = "ThenByDescending" ->
                flatten source (LinqStage.Sort(l, isDescending = true) :: acc)

            // 3. Take
            | MethodCall(m, null, [ source; countExpr ]) when m.Name = "Take" ->
                match tryEvaluate countExpr with
                | Some c -> flatten source (LinqStage.Limit(Convert.ToUInt32(c)) :: acc)
                | None -> failwithf "Could not evaluate Take count argument: %A" countExpr

            // 4. Two-argument GroupBy (with ResultSelector): source.GroupBy(k, (k, g) => ...)
            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as k); StripQuotes (:? LambdaExpression as r) ]) 
                when m.Name = "GroupBy" ->
                flatten source (LinqStage.GroupByWithResult(k, r) :: acc)

            // 5. Single-argument GroupBy: source.GroupBy(k)
            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as k) ]) when m.Name = "GroupBy" ->
                flatten source (LinqStage.GroupByKey k :: acc)

            // 6. Select
            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as l) ]) when m.Name = "Select" ->
                flatten source (LinqStage.Project l :: acc)

            // 7. Join / LeftJoin
            | MethodCall(m, null, [ outer; inner; 
                                   StripQuotes (:? LambdaExpression as outerKey); 
                                   StripQuotes (:? LambdaExpression as innerKey); 
                                   StripQuotes (:? LambdaExpression as resSel) ]) 
                when m.Name = "Join" || m.Name = "LeftJoin" ->
                flatten outer (LinqStage.Join(m, inner, outerKey, innerKey, resSel) :: acc)

            // Stop at upstream root or unhandled expression
            | _ ->
                acc

        let stages = flatten expression []

        // -------------------------------------------------------------
        // Phase 2: Rule-Based Fusion and Native Op Assembly
        // -------------------------------------------------------------
        let addSort (spec: SortSpec) (ops: QueryOp list) =
            match ops with
            | QueryOp.Sort specs :: tail -> 
                // Maintain primary -> secondary sort precedence: specs @ [spec]
                QueryOp.Sort (specs @ [spec]) :: tail
            | _ -> 
                QueryOp.Sort [spec] :: ops

        let rec fuse (remaining: LinqStage list) (opsAcc: QueryOp list) (clientPreds: Func<'T, bool> list) =
            match remaining with
            // Rule A: GroupByKey followed immediately by Project (Select) -> Pushdown native GroupBy
            | LinqStage.GroupByKey keyLambda :: LinqStage.Project projLambda :: tail when clientPreds.IsEmpty ->
                match PolarsQuery<'T>.CompileGroupByAndProjection keyLambda projLambda with
                | Some spec -> fuse tail (QueryOp.GroupBy spec :: opsAcc) clientPreds
                | None -> failwith "Failed to compile GroupBy -> Select projection pushdown."

            // Rule B: GroupByWithResult -> Pushdown native GroupBy
            | LinqStage.GroupByWithResult(keyLambda, resLambda) :: tail when clientPreds.IsEmpty ->
                match PolarsQuery<'T>.CompileGroupByWithResult keyLambda resLambda with
                | Some spec -> fuse tail (QueryOp.GroupBy spec :: opsAcc) clientPreds
                | None -> failwith "Failed to compile GroupBy with result selector pushdown."

            // Rule C: Filter (Where)
            | LinqStage.Filter l :: tail ->
                match ExprTranslator.tryTranslate l.Parameters.[0].Name l.Body with
                | Some exprHandle when clientPreds.IsEmpty ->
                    fuse tail (QueryOp.Filter exprHandle :: opsAcc) clientPreds
                | _ ->
                    let compiled = l.Compile() :?> Func<'T, bool>
                    fuse tail opsAcc (compiled :: clientPreds)

            // Rule D: Sorting (collates contiguous sorts, guards against IGrouping)
            | LinqStage.Sort(l, isDesc) :: tail ->
                let param = l.Parameters.[0]
                let isGrouping = param.Type.IsGenericType && param.Type.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>
                if not isGrouping && clientPreds.IsEmpty then
                    match ExprTranslator.tryTranslate param.Name l.Body with
                    | Some exprHandle ->
                        let spec = { Expr = exprHandle; Descending = isDesc; NullsLast = false }
                        fuse tail (addSort spec opsAcc) clientPreds
                    | None ->
                        fuse tail opsAcc clientPreds
                else
                    // OrderBy operating on IGrouping.Key is handled downstream in-memory
                    fuse tail opsAcc clientPreds

            // Rule E: Join
            | LinqStage.Join(m, inner, outerKey, innerKey, _) :: tail when clientPreds.IsEmpty ->
                match PolarsQuery<'T>.CompileJoin m inner outerKey innerKey with
                | Some spec -> fuse tail (QueryOp.Join spec :: opsAcc) clientPreds
                | None -> failwithf "Failed to push down %s." m.Name

            // Rule F: Limit
            | LinqStage.Limit count :: tail when clientPreds.IsEmpty ->
                fuse tail (QueryOp.Limit count :: opsAcc) clientPreds

            // Rule G: Bare GroupBy without projection -> halts schema pushdown
            | LinqStage.GroupByKey _ :: tail ->
                fuse tail opsAcc clientPreds

            // Rule H: Projection passthrough
            | LinqStage.Project _ :: tail ->
                fuse tail (QueryOp.SelectPassthrough :: opsAcc) clientPreds

            | [] ->
                List.rev opsAcc, List.rev clientPreds

            | _ :: tail ->
                fuse tail opsAcc clientPreds

        fuse stages [] []

    /// Evaluates the complete pipeline, executing native plan first, then materializing
    member private this.ExecuteQuery() : IEnumerable<'T> =
        let targetType = typeof<'T>
        let isGrouping = targetType.IsGenericType && targetType.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

        if isGrouping then
            // Branch A: Bare GroupBy pipeline -> Materialize upstream rows and group in memory
            let genericArgs = targetType.GetGenericArguments()
            let keyType = genericArgs.[0]
            let elemType = genericArgs.[1]

            // 1. Run upstream native pipeline (SIMD Filter, Sorts before group, etc.)
            let ops, _ = this.CompilePipeline()
            let nativeLf = PolarsQuery<'T>.ApplyNativeOps lfCloned ops
            let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto, true)

            // 2. Materialize raw rows of type elemType (e.g. Person)
            let mat = PolarsQuery<'T>.ResolveMaterializer(materializer)
            let materializeMethod = mat.GetType().GetMethod("Materialize").MakeGenericMethod(elemType)
            let rawRows = materializeMethod.Invoke(mat, [| box dfHandle |]) :?> IEnumerable

            // 3. Extract the keySelector lambda from the expression tree safely
            let rec findKeySelector (e: Expression) : LambdaExpression option =
                match e with
                | null -> None
                | MethodCall(m, null, [ _; StripQuotes (:? LambdaExpression as l) ]) when m.Name = "GroupBy" ->
                    Some l
                | MethodCall(_, null, args) ->
                    args |> List.tryPick findKeySelector
                | _ -> None

            // 4. Check for downstream OrderBy(g => g.Key)
            let rec findSortDirection (e: Expression) : bool option =
                match e with
                | null -> None
                | MethodCall(m, null, _) when m.Name = "OrderBy" -> Some false
                | MethodCall(m, null, _) when m.Name = "OrderByDescending" -> Some true
                | MethodCall(_, null, args) ->
                    args |> List.tryPick findSortDirection
                | _ -> None

            match findKeySelector expression with
            | Some keyLambda ->
                let compiledKeySelector = keyLambda.Compile()

                // Call Enumerable.GroupBy<TSource, TKey>(rawRows, keySelector)
                let groupByMethod = 
                    typeof<Enumerable>.GetMethods()
                    |> Array.find (fun m -> 
                        m.Name = "GroupBy" 
                        && m.GetParameters().Length = 2 
                        && m.GetGenericArguments().Length = 2)
                    |> fun m -> m.MakeGenericMethod(elemType, keyType)

                let grouped = groupByMethod.Invoke(null, [| box rawRows; box compiledKeySelector |]) :?> IEnumerable<'T>

                // Apply downstream OrderBy(g => g.Key) if specified
                match findSortDirection expression with
                | Some false ->
                    let keyProp = targetType.GetProperty("Key")
                    grouped |> Seq.sortBy (fun g -> keyProp.GetValue(g) :?> IComparable)
                | Some true ->
                    let keyProp = targetType.GetProperty("Key")
                    grouped |> Seq.sortByDescending (fun g -> keyProp.GetValue(g) :?> IComparable)
                | None ->
                    grouped
            | None ->
                failwith "Could not locate keySelector for GroupBy pipeline."

        else
            // Branch B: Standard pushdown and direct row materialization
            let ops, clientPredicates = this.CompilePipeline()
            let mat = PolarsQuery<'T>.ResolveMaterializer(materializer)

            let nativeLf = PolarsQuery<'T>.ApplyNativeOps lfCloned ops
            let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto, true)
            let rows = mat.Materialize<'T>(dfHandle)

            if clientPredicates.IsEmpty then
                rows
            else
                clientPredicates
                |> List.fold (fun (acc: IEnumerable<'T>) pred -> acc.Where(pred.Invoke)) rows

    /// Compiles pipeline to LazyFrameHandle. If client predicates exist, materializes and transposes back.
    member this.CompileToLazyFrameHandle() : LazyFrameHandle =
        let ops, clientPredicates = this.CompilePipeline()

        if clientPredicates.IsEmpty then
            PolarsQuery<'T>.ApplyNativeOps lfCloned ops
        else
            let filteredRows = this.ExecuteQuery()
            let newDfHandle = DataFrameBuilder.FromRows<'T>(filteredRows)
            PolarsWrapper.DataFrameToLazy(newDfHandle)

    /// Compiles the query and materializes directly into a native DataFrameHandle
    member this.CompileToDataFrameHandle() : DataFrameHandle =
        let ops, clientPredicates = this.CompilePipeline()

        if clientPredicates.IsEmpty then
            let nativeLf = PolarsQuery<'T>.ApplyNativeOps lfCloned ops
            PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto, true)
        else
            let filteredRows = this.ExecuteQuery()
            DataFrameBuilder.FromRows<'T>(filteredRows)

    interface IQueryable<'T> with
        member this.GetEnumerator() = this.ExecuteQuery().GetEnumerator()
        member this.GetEnumerator() = (this :> IEnumerable<'T>).GetEnumerator() :> IEnumerator
        member _.ElementType = typeof<'T>
        member _.Expression = expression
        member _.Provider = provider :> IQueryProvider

    interface IOrderedQueryable<'T>

    interface IPolarsPlanSource with
        member this.GetLazyFrameHandle() = this.CompileToLazyFrameHandle()