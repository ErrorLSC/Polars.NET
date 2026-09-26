namespace Polars.NET.Linq.Provider

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core
open Polars.NET.Core.Helpers

type internal ExecutionPlan = {
    NativePlan: LazyFrameHandle
    ClientPredicates: Func<obj, bool> list
    ClientProjection: (Type * Delegate) option
}

/// Internal interface allowing direct zero-reflection plan querying between Provider and Query instances
type internal IInternalPlanSource =
    abstract member GetPlanInfo: unit -> LazyFrameHandle * bool

/// Internal normalized context capturing all GroupBy variants (C# 4-args, F# pipeline, Having, Sorts)
type private GroupFoldContext = {
    KeyLambda: LambdaExpression
    ElemLambdaOpt: LambdaExpression option
    HavingPreds: LambdaExpression list
    SortStages: (LambdaExpression * bool) list
}

type internal QueryMaterializerResolver =
    /// Resolves the effective IDataFrameMaterializer using explicit instance or registered fallback
    static member Resolve(matOpt: IDataFrameMaterializer option) : IDataFrameMaterializer =
        match matOpt with
        | Some mat when not (isNull (box mat)) -> mat
        | _ ->
            let defaultMat = DataFrameMaterializerRegistry.Default
            if not (isNull (box defaultMat)) then
                defaultMat
            else
                failwith "No DataFrameMaterializer registered. Ensure upper API layers configured a materializer."

/// Strongly-typed IQueryProvider backed by Polars.NET.Core
type PolarsQueryProvider(initialLazyFrame: LazyFrameHandle, materializer: IDataFrameMaterializer) =
    let clonedLf = PolarsWrapper.LazyClone initialLazyFrame

    member _.LazyFrame = clonedLf
    member _.Materializer = materializer

    /// Core scalar execution pipeline: compiles native plan, executes native reductions, 
    /// and extracts scalar results via IDataFrameMaterializer.
    member private this.ExecuteScalarInternal<'TResult>(expression: Expression) : 'TResult =
        Console.Error.WriteLine(sprintf "\x1b[1;31m[EXECUTE SCALAR ENTER] TargetType='%s', Expr=%A\x1b[0m" typeof<'TResult>.Name expression)
        let targetType = typeof<'TResult>
        let mat = QueryMaterializerResolver.Resolve (Some materializer)

        // Local helper to extract the sequence element type from IQueryable<T> or IEnumerable<T>
        let getSequenceElementType (t: Type) =
            if t.IsGenericType then t.GetGenericArguments().[0]
            elif t.IsArray then t.GetElementType()
            else t

        // Helper to extract native LazyFrame and check if client execution exists without reflection crashes
        let resolveQueryPlan (src: Expression) : LazyFrameHandle * bool =
            let query = (this :> IQueryProvider).CreateQuery(src)
            match query with
            | :? IInternalPlanSource as ps ->
                ps.GetPlanInfo()
            | _ ->
                match query with
                | :? IPolarsPlanSource as ps ->
                    ps.GetCompiledLazyFrameHandle(), false
                | _ ->
                    let getPlanMethod = 
                        query.GetType().GetMethod("GetCompiledPlan", BindingFlags.Instance ||| BindingFlags.NonPublic ||| BindingFlags.Public)
                    if not (isNull getPlanMethod) then
                        let planResult = getPlanMethod.Invoke(query, null)
                        let fields = Microsoft.FSharp.Reflection.FSharpValue.GetTupleFields(planResult)
                        let nativeLf = fields.[0] :?> LazyFrameHandle
                        let predsObj = fields.[1] :?> IEnumerable
                        let hasClientPreds = predsObj.GetEnumerator().MoveNext()
                        let hasClientProj = fields.Length > 2 && not (isNull fields.[2])
                        let hasFallback = fields.Length > 3 && (fields.[3] :?> bool)
                        nativeLf, (hasClientPreds || hasClientProj || hasFallback)
                    else
                        failwithf "Unable to extract LazyFrame from query source: %A" src

        // Helper to rewrite scalar operations with a predicate to Where(pred).TargetScalarMethod()
        let rewritePredicateScalar (m: MethodInfo) (src: Expression) (pred: LambdaExpression) =
            let elemType = getSequenceElementType src.Type
            let whereMethod = LinqReflectionCache.GetQueryableWhere elemType
            let filteredSource = Expression.Call(null, whereMethod, src, Expression.Quote pred)
            let scalarMethod = LinqReflectionCache.GetQueryableScalar(m.Name, elemType)
            Expression.Call(null, scalarMethod, filteredSource)

        match expression with
        // 1. Unified Rewrite: Scalar operations taking a predicate (Count, Any, Single, First, Last, etc.)
        | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as pred) ])
            when m.Name = "Count" || m.Name = "LongCount" || m.Name = "Any" || 
                 m.Name = "Single" || m.Name = "SingleOrDefault" ||
                 m.Name = "First" || m.Name = "FirstOrDefault" ||
                 m.Name = "Last" || m.Name = "LastOrDefault" ->

            let rewrittenCall = rewritePredicateScalar m source pred
            this.ExecuteScalarInternal<'TResult>(rewrittenCall)

        // 2. Count() / LongCount() without predicate
        | MethodCall(m, null, [ source ]) when m.Name = "Count" || m.Name = "LongCount" ->
            let nativeLf, hasClientPreds = resolveQueryPlan source
            if not hasClientPreds then
                let lenExpr = PolarsWrapper.Len()
                let aggLf = PolarsWrapper.LazySelect(nativeLf, [| lenExpr |])
                let dfHandle = PolarsWrapper.LazyCollect(aggLf, PlEngine.Auto, true)
                mat.MaterializeScalar<'TResult>(dfHandle)
            else
                let rawRows = (this :> IQueryProvider).CreateQuery(source)
                let count = Enumerable.Count(rawRows.Cast<obj>())
                Convert.ChangeType(count, targetType) :?> 'TResult

        // 3. Any() without predicate
        | MethodCall(m, null, [ source ]) when m.Name = "Any" ->
            let nativeLf, hasClientPreds = resolveQueryPlan source
            if not hasClientPreds then
                let slicedLf = PolarsWrapper.LazySlice(nativeLf, 0L, 1u)
                let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto, true)
                let height = PolarsWrapper.DataFrameHeight dfHandle
                box (height > 0L) :?> 'TResult
            else
                let rawRows = (this :> IQueryProvider).CreateQuery(source)
                box (Enumerable.Any(rawRows.Cast<obj>())) :?> 'TResult

        // 4. All(predicate) -> Short-circuit pushdown: filter(not(pred)).slice(0, 1)
        | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as pred) ]) when m.Name = "All" ->
            let nativeLf, hasClientPreds = resolveQueryPlan source
            let translatedExprOpt = ExprTranslator.tryTranslate pred.Parameters.[0].Name pred.Body

            match translatedExprOpt with
            | Some colExpr when not hasClientPreds ->
                let notExpr = PolarsWrapper.Not colExpr
                let filteredLf = PolarsWrapper.LazyFilter(nativeLf, notExpr)
                let slicedLf = PolarsWrapper.LazySlice(filteredLf, 0L, 1u)
                let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto, true)
                let height = PolarsWrapper.DataFrameHeight dfHandle
                box (height = 0L) :?> 'TResult
            | _ ->
                let rawRows = (this :> IQueryProvider).CreateQuery(source)
                let elemType = getSequenceElementType source.Type
                let compiledPred = pred.Compile()
                let closedMethod = LinqReflectionCache.GetEnumerableMethod("All", 2, [| elemType |])
                closedMethod.Invoke(null, [| box rawRows; box compiledPred |]) :?> 'TResult

        // 5. First() / FirstOrDefault() without predicate
        | MethodCall(m, null, [ source ]) when m.Name = "First" || m.Name = "FirstOrDefault" ->
            let isOrDefault = m.Name = "FirstOrDefault"
            let nativeLf, hasClientPreds = resolveQueryPlan source
            if not hasClientPreds then
                let slicedLf = PolarsWrapper.LazySlice(nativeLf, 0L, 1u)
                let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto, true)
                let height = PolarsWrapper.DataFrameHeight dfHandle
                if height = 0L then
                    if isOrDefault then Unchecked.defaultof<'TResult>
                    else invalidOp "Sequence contains no elements."
                else
                    let rows = mat.Materialize<'TResult>(dfHandle)
                    Enumerable.First rows
            else
                let rawRows = (this :> IQueryProvider).CreateQuery(source)
                if isOrDefault then Enumerable.FirstOrDefault(rawRows.Cast<'TResult>())
                else Enumerable.First(rawRows.Cast<'TResult>())

        // 6. Last() / LastOrDefault() without predicate
        | MethodCall(m, null, [ source ]) when m.Name = "Last" || m.Name = "LastOrDefault" ->
            let isOrDefault = m.Name = "LastOrDefault"
            let nativeLf, hasClientPreds = resolveQueryPlan source
            if not hasClientPreds then
                let slicedLf = PolarsWrapper.LazySlice(nativeLf, -1L, 1u)
                let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto, true)
                let height = PolarsWrapper.DataFrameHeight dfHandle
                if height = 0L then
                    if isOrDefault then Unchecked.defaultof<'TResult>
                    else invalidOp "Sequence contains no elements."
                else
                    let rows = mat.Materialize<'TResult>(dfHandle)
                    Enumerable.First rows
            else
                let rawRows = (this :> IQueryProvider).CreateQuery(source)
                if isOrDefault then Enumerable.LastOrDefault(rawRows.Cast<'TResult>())
                else Enumerable.Last(rawRows.Cast<'TResult>())

        // 7. Max, Min, Sum, Average with selector
        | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as selector) ])
            when m.Name = "Max" || m.Name = "Min" || m.Name = "Sum" || m.Name = "Average" ->

            let nativeLf, hasClientPreds = resolveQueryPlan source
            let translatedExprOpt = ExprTranslator.tryTranslate selector.Parameters.[0].Name selector.Body

            match translatedExprOpt with
            | Some colExpr when not hasClientPreds ->
                let aggExpr =
                    match m.Name with
                    | "Max" -> PolarsWrapper.Max colExpr
                    | "Min" -> PolarsWrapper.Min colExpr
                    | "Sum" -> PolarsWrapper.Sum colExpr
                    | "Average" -> PolarsWrapper.Mean colExpr
                    | _ -> failwith "Unsupported scalar aggregate"

                let aggLf = PolarsWrapper.LazySelect(nativeLf, [| aggExpr |])
                let dfHandle = PolarsWrapper.LazyCollect(aggLf, PlEngine.Auto, true)
                mat.MaterializeScalar<'TResult>(dfHandle)
            | _ ->
                let rawRows = (this :> IQueryProvider).CreateQuery(source)
                let compiledSel = selector.Compile()
                let genericArg = source.Type.GetGenericArguments().[0]
                let closedMethod = LinqReflectionCache.GetEnumerableMethod(m.Name, 2, [| genericArg |])
                closedMethod.Invoke(null, [| box rawRows; box compiledSel |]) :?> 'TResult

        // 8. Max, Min, Sum, Average without selector
        | MethodCall(m, null, [ source ])
            when m.Name = "Max" || m.Name = "Min" || m.Name = "Sum" || m.Name = "Average" ->

            let nativeLf, hasClientPreds = resolveQueryPlan source
            if not hasClientPreds then
                let colNames = PolarsQuery<obj>.GetLazyColumnNames nativeLf
                let targetCol = if colNames.Length > 0 then colNames.[0] else ""
                let colExpr = PolarsWrapper.Col targetCol
                let aggExpr =
                    match m.Name with
                    | "Max" -> PolarsWrapper.Max colExpr
                    | "Min" -> PolarsWrapper.Min colExpr
                    | "Sum" -> PolarsWrapper.Sum colExpr
                    | "Average" -> PolarsWrapper.Mean colExpr
                    | _ -> failwith "Unsupported scalar aggregate"

                let aggLf = PolarsWrapper.LazySelect(nativeLf, [| aggExpr |])
                let dfHandle = PolarsWrapper.LazyCollect(aggLf, PlEngine.Auto, true)
                mat.MaterializeScalar<'TResult>(dfHandle)
            else
                let rawRows = (this :> IQueryProvider).CreateQuery(source)
                let elemType = getSequenceElementType source.Type
                let genericArg = if source.Type.IsGenericType then source.Type.GetGenericArguments().[0] else elemType
                let closedMethod = LinqReflectionCache.GetEnumerableMethod(m.Name, 1, [| genericArg |])
                closedMethod.Invoke(null, [| box rawRows |]) :?> 'TResult

        // 9. Single() / SingleOrDefault() without predicate
        | MethodCall(m, null, [ source ]) when m.Name = "Single" || m.Name = "SingleOrDefault" ->
            let isOrDefault = m.Name = "SingleOrDefault"
            let nativeLf, hasClientPreds = resolveQueryPlan source

            if not hasClientPreds then
                let slicedLf = PolarsWrapper.LazySlice(nativeLf, 0L, 2u)
                let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto, true)
                let height = PolarsWrapper.DataFrameHeight dfHandle

                match height with
                | 0L ->
                    if isOrDefault then Unchecked.defaultof<'TResult>
                    else invalidOp "Sequence contains no elements."
                | 1L ->
                    let rows = mat.Materialize<'TResult>(dfHandle)
                    Enumerable.First rows
                | _ ->
                    invalidOp "Sequence contains more than one element."
            else
                let rawRows = (this :> IQueryProvider).CreateQuery(source)
                if isOrDefault then Enumerable.SingleOrDefault(rawRows.Cast<'TResult>())
                else Enumerable.Single(rawRows.Cast<'TResult>())

        // 10. ElementAt(index) / ElementAtOrDefault(index)
        | MethodCall(m, null, [ source; indexExpr ]) when m.Name = "ElementAt" || m.Name = "ElementAtOrDefault" ->
            let isOrDefault = m.Name = "ElementAtOrDefault"
            let index = 
                match tryEvaluate indexExpr with
                | Some idx -> Convert.ToInt64 idx
                | None -> failwith "Failed to evaluate index for ElementAt."

            if index < 0L then
                if isOrDefault then Unchecked.defaultof<'TResult>
                else raise (ArgumentOutOfRangeException("index", "Index was out of range."))
            else
                let nativeLf, hasClientPreds = resolveQueryPlan source
                if not hasClientPreds then
                    let slicedLf = PolarsWrapper.LazySlice(nativeLf, index, 1u)
                    let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto, true)
                    let height = PolarsWrapper.DataFrameHeight dfHandle
                    
                    if height = 0L then
                        if isOrDefault then Unchecked.defaultof<'TResult>
                        else raise (ArgumentOutOfRangeException("index", "Index was out of range."))
                    else
                        let rows = mat.Materialize<'TResult>(dfHandle)
                        Enumerable.First rows
                else
                    let rawRows = (this :> IQueryProvider).CreateQuery(source)
                    let typedSeq : seq<'TResult> = rawRows.Cast<'TResult>()
                    let intIdx = int index
                    match Seq.tryItem intIdx typedSeq with
                    | Some item -> item
                    | None ->
                        if isOrDefault then Unchecked.defaultof<'TResult>
                        else raise (ArgumentOutOfRangeException("index", "Index was out of range."))

        // 11. Contains(item)
        | MethodCall(m, null, [ source; itemExpr ]) when m.Name = "Contains" ->
            let itemVal = 
                match tryEvaluate itemExpr with
                | Some v -> v
                | None -> failwith "Failed to evaluate item parameter for Contains."

            let nativeLf, hasClientPreds = resolveQueryPlan source

            if not hasClientPreds && not (isNull itemVal) then
                let itemType = itemVal.GetType()
                if itemType.IsPrimitive || itemType = typeof<string> || itemType = typeof<decimal> then
                    let colExpr = PolarsWrapper.Col ""
                    let litExpr = 
                        match itemVal with
                        | :? string as s -> PolarsWrapper.Lit s
                        | :? int as i -> PolarsWrapper.Lit i
                        | :? int64 as l -> PolarsWrapper.Lit l
                        | :? double as d -> PolarsWrapper.Lit d
                        | :? bool as b -> PolarsWrapper.Lit b
                        | _ -> failwithf "Unsupported primitive type for Contains: %s" itemType.Name

                    let filterExpr = PolarsWrapper.Eq(colExpr, litExpr)
                    let filteredLf = PolarsWrapper.LazyFilter(nativeLf, filterExpr)
                    let slicedLf = PolarsWrapper.LazySlice(filteredLf, 0L, 1u)
                    let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto, true)
                    let height = PolarsWrapper.DataFrameHeight dfHandle
                    box (height > 0L) :?> 'TResult
                else
                    let props = itemType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                    let conditions = 
                        props
                        |> Array.choose (fun p ->
                            let v = p.GetValue(itemVal)
                            if isNull v then None
                            else
                                let colExpr = PolarsWrapper.Col p.Name
                                let litExprOpt =
                                    match v with
                                    | :? string as s -> Some (PolarsWrapper.Lit s)
                                    | :? int as i -> Some (PolarsWrapper.Lit i)
                                    | :? int64 as l -> Some (PolarsWrapper.Lit l)
                                    | :? double as d -> Some (PolarsWrapper.Lit d)
                                    | :? bool as b -> Some (PolarsWrapper.Lit b)
                                    | _ -> None
                                litExprOpt |> Option.map (fun lit -> PolarsWrapper.Eq(colExpr, lit)))

                    if conditions.Length > 0 then
                        let combinedFilter = conditions |> Array.reduce (fun acc c -> PolarsWrapper.And(acc, c))
                        let filteredLf = PolarsWrapper.LazyFilter(nativeLf, combinedFilter)
                        let slicedLf = PolarsWrapper.LazySlice(filteredLf, 0L, 1u)
                        let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto, true)
                        let height = PolarsWrapper.DataFrameHeight dfHandle
                        box (height > 0L) :?> 'TResult
                    else
                        let rawRows = (this :> IQueryProvider).CreateQuery(source)
                        let seq = rawRows.Cast<obj>()
                        box (Enumerable.Contains(seq, itemVal)) :?> 'TResult
            else
                let rawRows = (this :> IQueryProvider).CreateQuery(source)
                let seq = rawRows.Cast<obj>()
                box (Enumerable.Contains(seq, itemVal)) :?> 'TResult

        // 12. SequenceEqual(second)
        | MethodCall(m, null, [ source; secondExpr ]) when m.Name = "SequenceEqual" ->
            let elemType = getSequenceElementType source.Type
            let nativeLf, hasClientPreds = resolveQueryPlan source

            let rec tryGetSecondQueryPlan (e: Expression) : (LazyFrameHandle * bool) option =
                match e with
                | :? MethodCallExpression as mc when typeof<IQueryable>.IsAssignableFrom(mc.Type) ->
                    let q = (this :> IQueryProvider).CreateQuery(mc)
                    match q with
                    | :? IInternalPlanSource as ps ->
                        Some (ps.GetPlanInfo())
                    | :? IPolarsPlanSource as planSource ->
                        Some (planSource.GetCompiledLazyFrameHandle(), false)
                    | _ -> None
                | _ ->
                    match tryEvaluate e with
                    | Some (:? IInternalPlanSource as ps) ->
                        Some (ps.GetPlanInfo())
                    | Some (:? IPolarsPlanSource as planSource) ->
                        Some (planSource.GetCompiledLazyFrameHandle(), false)
                    | _ -> None

            match tryGetSecondQueryPlan secondExpr with
            | Some (secondLf, secondHasClientPreds) when not hasClientPreds && not secondHasClientPreds ->
                let colNames1 = nativeLf |> PolarsQuery<obj>.GetLazyColumnNames
                let colNames2 = secondLf |> PolarsQuery<obj>.GetLazyColumnNames

                if colNames1.Length <> colNames2.Length then
                    box false :?> 'TResult
                else
                    let fullDf1 = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto, true)
                    let fullDf2 = PolarsWrapper.LazyCollect(secondLf, PlEngine.Auto, true)

                    let h1 = PolarsWrapper.DataFrameHeight fullDf1
                    let h2 = PolarsWrapper.DataFrameHeight fullDf2

                    if h1 <> h2 then
                        box false :?> 'TResult
                    elif h1 = 0L then
                        box true :?> 'TResult
                    else
                        let areEqual = PolarsWrapper.DataFrameEquals(fullDf1, fullDf2, nullEqual = true)
                        box areEqual :?> 'TResult

            | _ ->
                let rawRows1 = (this :> IQueryProvider).CreateQuery(source) : IEnumerable
                let rawRows2 =
                    match tryEvaluate secondExpr with
                    | Some (:? IEnumerable as e) -> e
                    | _ ->
                        let q = (this :> IQueryProvider).CreateQuery(secondExpr)
                        let castMethod = LinqReflectionCache.GetEnumerableMethod("Cast", 1, [| elemType |])
                        let toListMethod = LinqReflectionCache.GetEnumerableMethod("ToList", 1, [| elemType |])
                        let casted = castMethod.Invoke(null, [| box q |])
                        toListMethod.Invoke(null, [| casted |]) :?> IEnumerable

                let seqEqMethod = LinqReflectionCache.GetEnumerableMethod("SequenceEqual", 2, [| elemType |])
                let areEqual = seqEqMethod.Invoke(null, [| box rawRows1; box rawRows2 |]) :?> bool
                box areEqual :?> 'TResult

        // 13. MinBy(keySelector) / MaxBy(keySelector)
        | MethodCall(m, null, source :: keyExpr :: _) when m.Name = "MinBy" || m.Name = "MaxBy" ->
            let isDescending = m.Name = "MaxBy"
            let nativeLf, hasClientPreds = resolveQueryPlan source

            let rec extractLambda (e: Expression) =
                match e with
                | :? LambdaExpression as l -> Some l
                | Unary(ExpressionType.Quote, inner) -> extractLambda inner
                | _ -> None

            let keyLambdaOpt = extractLambda keyExpr

            match keyLambdaOpt with
            | Some keyLambda when not hasClientPreds ->
                let keyParam = keyLambda.Parameters.[0]
                match ExprTranslator.tryTranslate keyParam.Name keyLambda.Body with
                | Some keyColExpr ->
                    let sortSpec = {
                        Expr = keyColExpr
                        Descending = isDescending
                        NullsLast = true
                    }
                    let sortedLf = 
                        PolarsWrapper.LazyFrameSort(
                            nativeLf, 
                            [| sortSpec.Expr |], 
                            [| sortSpec.Descending |], 
                            [| sortSpec.NullsLast |], 
                            maintainOrder = false
                        )

                    let slicedLf = PolarsWrapper.LazySlice(sortedLf, 0L, 1u)
                    let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto, true)
                    let height = PolarsWrapper.DataFrameHeight dfHandle

                    if height = 0L then
                        Unchecked.defaultof<'TResult>
                    else
                        let rows = mat.Materialize<'TResult>(dfHandle)
                        Enumerable.First rows
                | None ->
                    let rawRows = (this :> IQueryProvider).CreateQuery(source)
                    let elemType = getSequenceElementType source.Type
                    let keyType = keyLambda.ReturnType
                    let compiledKey = keyLambda.Compile()
                    let linqMethod = LinqReflectionCache.GetEnumerableMethod(m.Name, 2, [| elemType; keyType |])
                    linqMethod.Invoke(null, [| box rawRows; box compiledKey |]) :?> 'TResult

            | _ ->
                let rawRows = (this :> IQueryProvider).CreateQuery(source)
                let elemType = getSequenceElementType source.Type
                let compiledKey = 
                    match keyLambdaOpt with
                    | Some kl -> kl.Compile()
                    | None -> failwithf "Failed to compile keySelector for %s" m.Name

                let keyType = 
                    match keyLambdaOpt with
                    | Some kl -> kl.ReturnType
                    | None -> typeof<obj>

                let linqMethod = LinqReflectionCache.GetEnumerableMethod(m.Name, 2, [| elemType; keyType |])
                linqMethod.Invoke(null, [| box rawRows; box compiledKey |]) :?> 'TResult

        // 14. Aggregate
        | MethodCall(m, null, source :: rest) when m.Name = "Aggregate" ->
            let rawRows = (this :> IQueryProvider).CreateQuery(source)
            let elemType = getSequenceElementType source.Type

            match rest with
            | [ StripQuotes (:? LambdaExpression as func) ] ->
                let compiledFunc = func.Compile()
                let aggMethod = LinqReflectionCache.GetEnumerableMethod("Aggregate", 2, [| elemType |])
                aggMethod.Invoke(null, [| box rawRows; box compiledFunc |]) :?> 'TResult

            | [ seedExpr; StripQuotes (:? LambdaExpression as func) ] ->
                let seedVal = 
                    match tryEvaluate seedExpr with
                    | Some s -> s
                    | None -> failwith "Failed to evaluate seed parameter for Aggregate."

                let compiledFunc = func.Compile()
                let accumType = typeof<'TResult>
                let aggMethod = LinqReflectionCache.GetEnumerableMethod("Aggregate", 3, [| elemType; accumType |])
                aggMethod.Invoke(null, [| box rawRows; box seedVal; box compiledFunc |]) :?> 'TResult

            | [ seedExpr; StripQuotes (:? LambdaExpression as func); StripQuotes (:? LambdaExpression as resSel) ] ->
                let seedVal = 
                    match tryEvaluate seedExpr with
                    | Some s -> s
                    | None -> failwith "Failed to evaluate seed parameter for Aggregate."

                let compiledFunc = func.Compile()
                let compiledResSel = resSel.Compile()
                let accumType = func.ReturnType
                let aggMethod = LinqReflectionCache.GetEnumerableMethod("Aggregate", 4, [| elemType; accumType; typeof<'TResult> |])
                aggMethod.Invoke(null, [| box rawRows; box seedVal; box compiledFunc; box compiledResSel |]) :?> 'TResult

            | _ ->
                failwithf "Unsupported Aggregate overload: %A" expression
        | _ ->
            failwithf "Scalar execution for expression is not supported: %A" expression

    interface IQueryProvider with
        member this.CreateQuery(expression: Expression) : IQueryable =
            let elemType =
                match PolarsTypeHelper.TryGetEnumerableElementType expression.Type with
                | null -> typedefof<obj>
                | t -> t

            Console.Error.WriteLine(sprintf "\x1b[1;36m[PROVIDER CREATE_QUERY (Non-Generic)] ElemType='%s', Expr=%A\x1b[0m" elemType.Name expression)

            let queryType = typedefof<PolarsQuery<_>>.MakeGenericType elemType
            Activator.CreateInstance(queryType, [| box clonedLf; box materializer; box expression |]) :?> IQueryable

        member this.CreateQuery<'TElement>(expression: Expression) : IQueryable<'TElement> =
            Console.Error.WriteLine(sprintf "\x1b[1;36m[PROVIDER CREATE_QUERY<'TElement>] TElement='%s', Expr=%A\x1b[0m" typeof<'TElement>.Name expression)
            PolarsQuery<'TElement>(clonedLf, materializer, expression) :> IQueryable<'TElement>

        // Non-generic Execute: dynamically invokes typed Execute<'TResult>
        member this.Execute(expression: Expression) : obj =
            Console.Error.WriteLine(sprintf "\x1b[1;35m[PROVIDER EXECUTE (Non-Generic)] ReturnType='%s', Expr=%A\x1b[0m" expression.Type.Name expression)
            let executeMethod = 
                typeof<PolarsQueryProvider>
                    .GetInterface("IQueryProvider")
                    .GetMethods()
                    |> Array.find (fun m -> m.Name = "Execute" && m.IsGenericMethod)
                    |> fun m -> m.MakeGenericMethod(expression.Type)
            executeMethod.Invoke(this, [| expression |])

        // Generic Execute<'TResult>: checks if expression is scalar vs sequence
        member this.Execute<'TResult>(expression: Expression) : 'TResult =
            let targetType = typeof<'TResult>
            
            // Standard scalar reductions
            let isScalarReductionMethod (m: MethodInfo) =
                match m.Name with
                | "Count" | "LongCount" | "Sum" | "Average" | "Min" | "Max" 
                | "First" | "FirstOrDefault" | "Single" | "SingleOrDefault" 
                | "Last" | "LastOrDefault" | "Any" | "All" | "Contains" 
                | "ElementAt" | "ElementAtOrDefault" | "SequenceEqual"
                | "MinBy" | "MaxBy" | "Aggregate" | "AggregateBy" -> true
                | _ -> false

            match expression with
            // Branch A: Standard Scalar Reductions
            | MethodCall(m, _, _) when isScalarReductionMethod m ->
                this.ExecuteScalarInternal<'TResult>(expression)

            // Branch B: Target is primitive scalar or string without reduction operator
            | _ when PolarsTypeHelper.IsScalarType targetType || targetType = typeof<string> ->
                this.ExecuteScalarInternal<'TResult>(expression)

            // Branch C: Sequence or Composite object execution
            | _ ->
                let query = (this :> IQueryProvider).CreateQuery(expression)
                match box query with
                | :? 'TResult as matched -> matched
                | _ ->
                    let elemType = 
                        match PolarsTypeHelper.TryGetEnumerableElementType targetType with
                        | null -> typeof<obj>
                        | t -> t
                    
                    let toListMethod = LinqReflectionCache.GetEnumerableMethod("ToList", 1, [| elemType |])
                    let listObj = toListMethod.Invoke(null, [| box query |])

                    if targetType.IsArray then
                        let toArrayMethod = LinqReflectionCache.GetEnumerableMethod("ToArray", 1, [| elemType |])
                        toArrayMethod.Invoke(null, [| box query |]) :?> 'TResult
                    elif targetType.IsAssignableFrom(listObj.GetType()) then
                        listObj :?> 'TResult
                    else
                        failwithf "Cannot convert query sequence to requested terminal type '%s'" targetType.FullName

and PolarsQuery<'T> internal (lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer, exprOpt: Expression option) as this =
    do 
        Console.Error.WriteLine(sprintf "\x1b[1;31m[CTOR CALLED] Type='PolarsQuery<%s>', Expr=%A\x1b[0m" typeof<'T>.Name exprOpt)
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

    /// Compiles any sub-expression into an isolated LazyFrameHandle
    static member internal CompileSubtreeToLazyFrame (expr: Expression) (currentMat: IDataFrameMaterializer) : LazyFrameHandle option =
        match expr with
        | :? ConstantExpression as c when not (isNull c.Value) ->
            match c.Value with
            | :? IPolarsPlanSource as planSource ->
                Some (planSource.GetCompiledLazyFrameHandle())
            | _ -> None
        | _ ->
            let rec findRootQueryable (e: Expression) : IQueryable option =
                match e with
                | :? ConstantExpression as c when not (isNull c.Value) ->
                    match c.Value with
                    | :? IQueryable as q -> Some q
                    | _ -> None
                | :? MethodCallExpression as mc ->
                    mc.Arguments
                    |> Seq.tryPick findRootQueryable
                | _ -> None

            match findRootQueryable expr with
            | Some rootQuery ->
                try
                    let subQuery = rootQuery.Provider.CreateQuery(expr)
                    match subQuery with
                    | :? IPolarsPlanSource as ps ->
                        Some (ps.GetCompiledLazyFrameHandle())
                    | _ -> None
                with _ -> None
            | None ->
                match tryEvaluate expr with
                | Some (:? IPolarsPlanSource as ps) ->
                    Some (ps.GetCompiledLazyFrameHandle())
                | _ -> None

    /// Uniformly extracts a LazyFrameHandle from an Expression
    static member internal ResolveToLazyFrameHandle(expr: Expression) : LazyFrameHandle option =
        let rec unwrap (e: Expression) =
            match e with
            | Unary(ExpressionType.Convert, inner)
            | Unary(ExpressionType.ConvertChecked, inner)
            | Unary(ExpressionType.Quote, inner) -> unwrap inner
            | other -> other

        let cleanExpr = unwrap expr

        let rawObjOpt =
            match cleanExpr with
            | :? ConstantExpression as ce -> Some ce.Value
            | other ->
                match tryEvaluate other with
                | Some v -> Some v
                | None ->
                    try
                        let lambda = Expression.Lambda<Func<obj>>(Expression.Convert(other, typeof<obj>)).Compile()
                        Some (lambda.Invoke())
                    with _ -> None

        match rawObjOpt with
        | None -> None
        | Some null -> None
        | Some (:? IPolarsPlanSource as plan) ->
            Some (plan.GetCompiledLazyFrameHandle())
        | Some (:? LazyFrameHandle as lf) ->
            Some (PolarsWrapper.LazyClone lf)
        | Some (:? DataFrameHandle as df) ->
            Some (PolarsWrapper.DataFrameToLazy df)
        | Some value ->
            let valueType = value.GetType()
            let handleProp = valueType.GetProperty("Handle", BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
            if not (isNull handleProp) then
                match handleProp.GetValue value with
                | :? LazyFrameHandle as lf -> Some (PolarsWrapper.LazyClone lf)
                | :? DataFrameHandle as df -> Some (PolarsWrapper.DataFrameToLazy df)
                | _ -> None
            else
                match PolarsTypeHelper.TryGetEnumerableElementType valueType with
                | null -> None
                | elemType ->
                    let fromRows = typeof<DataFrameBuilder>.GetMethod("FromRows", BindingFlags.Public ||| BindingFlags.Static).MakeGenericMethod(elemType)
                    let dfHandle = fromRows.Invoke(null, [| value |]) :?> DataFrameHandle
                    Some (PolarsWrapper.DataFrameToLazy dfHandle)

    /// Helper to iterate and extract all column names from a LazyFrame schema
    static member internal GetLazyColumnNames(lf: LazyFrameHandle) : string array =
        PolarsSchemaExtractor.GetColumnNames lf

    /// Helper to extract all column names and their corresponding DataTypeHandle from a LazyFrame schema
    static member internal GetLazySchemaFields(lf: LazyFrameHandle) : (string * DataTypeHandle) array =
        let clonedLf = PolarsWrapper.LazyClone lf
        let schema = PolarsWrapper.GetLazySchema clonedLf
        let count = PolarsWrapper.GetSchemaLen schema
        Array.init (int count) (fun i ->
            let mutable name = null
            let mutable dtHandle = new DataTypeHandle()
            PolarsWrapper.GetSchemaFieldAt(schema, uint64 i, &name, &dtHandle)
            name, dtHandle
        )

    /// Materializes and batches a sequence into fixed-size arrays without intermediate collection copying
    static member private ChunkSequence<'Elem>(source: IEnumerable<'Elem>, size: int) : IEnumerable<'Elem array> =
        seq {
            use e = source.GetEnumerator()
            let mutable hasMore = e.MoveNext()
            while hasMore do
                let chunk = ResizeArray<'Elem>(size)
                let mutable count = 0
                while count < size && hasMore do
                    chunk.Add(e.Current)
                    count <- count + 1
                    hasMore <- e.MoveNext()
                yield chunk.ToArray()
        }

    /// Extracts member names and capitalizes them to PascalCase
    static member private ExtractPascalCaseMemberNames(newExpr: NewExpression) : string list =
        if not (isNull newExpr.Members) then 
            newExpr.Members |> Seq.map (fun m -> m.Name) |> Seq.toList
        else 
            newExpr.Constructor.GetParameters() 
            |> Seq.map (fun p -> 
                if String.IsNullOrEmpty p.Name then "Col"
                else Char.ToUpperInvariant(p.Name.[0]).ToString() + p.Name.Substring 1) 
            |> Seq.toList

    /// Extracts column names from a DistinctBy keySelector lambda
    static member private ExtractDistinctColumns(keyLambda: LambdaExpression) : string array option =
        match keyLambda.Body with
        | ExtractColumnName colName -> Some [| colName |]
        | :? NewExpression as newExpr ->
            Some (PolarsQuery<'T>.ExtractPascalCaseMemberNames newExpr |> List.toArray)
        | :? MemberInitExpression as initExpr ->
            Some (initExpr.Bindings |> Seq.map (fun b -> b.Member.Name) |> Seq.toArray)
        | _ -> None

    /// Extracts target column name for SelectMany/Explode
    static member private ExtractExplodeColumn(colLambda: LambdaExpression) : string option =
        match colLambda.Body with
        | ExtractColumnName colName -> Some colName
        | _ -> None

    /// Checks recursively whether an expression contains references to a specific parameter
    static member private ContainsParameter (paramName: string) (expr: Expression) : bool =
        let rec check (e: Expression) =
            match e with
            | null -> false
            | :? ParameterExpression as p -> p.Name = paramName
            | MethodCall(_, target, args) -> (target <> null && check target) || (args |> List.exists check)
            | MemberAccess(target, _) -> target <> null && check target
            | Unary(_, inner) -> check inner
            | Binary(_, left, right) -> check left || check right
            | Lambda(_, body) -> check body
            | _ -> false
        check expr

    /// Inspects GroupJoin result selector to detect group-level aggregations (e.g., g.Count())
    static member private TryExtractGroupAgg (groupParamName: string) (resLambda: LambdaExpression) : (string * ExprHandle) option =
        match resLambda.Body with
        | :? NewExpression as newExpr ->
            let args = newExpr.Arguments |> Seq.toList
            let memberNames = PolarsQuery<'T>.ExtractPascalCaseMemberNames newExpr
            List.zip memberNames args
            |> List.tryPick (fun (memName, arg) ->
                match arg with
                | MethodCall(m, _, [ :? ParameterExpression as p ]) 
                    when p.Name = groupParamName && (m.Name = "Count" || m.Name = "LongCount") ->
                    let countExpr = PolarsWrapper.Len()
                    let aliased = PolarsWrapper.Alias(countExpr, memName)
                    Some (memName, aliased)
                | MethodCall(m, _, target :: _) 
                    when not (isNull target) && (match target with :? ParameterExpression as p -> p.Name = groupParamName | _ -> false) 
                         && (m.Name = "Count" || m.Name = "LongCount") ->
                    let countExpr = PolarsWrapper.Len()
                    let aliased = PolarsWrapper.Alias(countExpr, memName)
                    Some (memName, aliased)
                | _ -> None
            )
        | _ -> None

    /// Universally extracts column renaming mappings from a two-parameter result selector
    static member private ExtractBinaryResultRenames (resLambda: LambdaExpression) (explodedInfoOpt: (string * string) option) : (string * string) list =
        match resLambda.Body with
        | :? NewExpression as newExpr ->
            let args = newExpr.Arguments |> Seq.toList
            let memberNames = PolarsQuery<'T>.ExtractPascalCaseMemberNames newExpr
            List.zip memberNames args
            |> List.choose (fun (memName, arg) ->
                match explodedInfoOpt with
                | Some(itemParamName, explodedCol) when 
                    (match arg with :? ParameterExpression as p -> p.Name = itemParamName | _ -> false) 
                    && memName <> explodedCol ->
                    Some(explodedCol, memName)
                | _ ->
                    match arg with
                    | ExtractColumnName srcCol when srcCol <> memName -> Some(srcCol, memName)
                    | _ -> None
            )
        | _ -> []

    /// Unified GroupBy compiler powering both single-key and multi-key GroupBy pipelines (C# LINQ & F# QueryBuilder)
    static member private CompileUnifiedGroupBy
        (ctx: GroupFoldContext)
        (projBody: Expression)
        (groupParamName: string)
        (isKeyArg: Expression -> bool) : QueryOp list option =

        let keyParam = ctx.KeyLambda.Parameters.[0]
        let elemExprOpt =
            ctx.ElemLambdaOpt
            |> Option.bind (fun el -> ExprTranslator.tryTranslate el.Parameters.[0].Name el.Body)

        let translateKeyPart (e: Expression) : ExprHandle option =
            match ExprTranslator.tryTranslate keyParam.Name e with
            | Some h -> Some h
            | None ->
                match e with
                | MemberAccess(:? ParameterExpression as p, mem) when p.Name = keyParam.Name ->
                    Some (PolarsWrapper.Col mem.Name)
                | _ -> None

        let keyDefinitionsOpt : (string * ExprHandle) list option =
            match ctx.KeyLambda.Body with
            | :? NewExpression as newExpr ->
                let args = newExpr.Arguments |> Seq.toList
                let memberNames = PolarsQuery<'T>.ExtractPascalCaseMemberNames newExpr
                let translated =
                    List.zip memberNames args
                    |> List.map (fun (name, arg) ->
                        translateKeyPart arg
                        |> Option.map (fun h -> (name, h))
                    )
                if translated |> List.forall Option.isSome then
                    Some (translated |> List.choose id)
                else None

            | :? MemberInitExpression as initExpr ->
                let bindings = initExpr.Bindings |> Seq.toList
                let translated =
                    bindings
                    |> List.choose (fun b ->
                        match b with
                        | :? MemberAssignment as assign ->
                            translateKeyPart assign.Expression
                            |> Option.map (fun h -> (assign.Member.Name, h))
                        | _ -> None
                    )
                if translated.Length = bindings.Length then
                    Some translated
                else None

            | singleExpr ->
                match translateKeyPart singleExpr with
                | Some h ->
                    let name =
                        match singleExpr with
                        | ExtractColumnName col -> col
                        | _ -> "Key"
                    Some [ (name, h) ]
                | None -> None

        match keyDefinitionsOpt with
        | None -> None
        | Some keyDefs ->
            match projBody with
            | :? NewExpression as newExpr ->
                let args = newExpr.Arguments |> Seq.toList
                let memberNames = PolarsQuery<'T>.ExtractPascalCaseMemberNames newExpr

                let isKeyExpression (e: Expression) : bool =
                    if isKeyArg e then true
                    else
                        match e with
                        | MemberAccess(target, _) when isKeyArg target -> true
                        | MemberAccess(MemberAccess(p, keyProp), _) 
                            when not (isNull p) && p.NodeType = ExpressionType.Parameter && keyProp.Name = "Key" -> true
                        | _ -> false

                // Mapping table from key component names to their effective output column names in the resulting DataFrame
                let keyNameToOutputAliasMap =
                    keyDefs
                    |> List.map (fun (origKeyName, _) ->
                        let outputColName =
                            List.zip memberNames args
                            |> List.tryPick (fun (memName, arg) ->
                                match arg with
                                | MemberAccess(MemberAccess(_, k), m) when k.Name = "Key" && m.Name = origKeyName -> Some memName
                                | MemberAccess(p, m) when isKeyArg p && m.Name = origKeyName -> Some memName
                                | _ when isKeyArg arg && keyDefs.Length = 1 -> Some memName
                                | _ -> None
                            )
                            |> Option.defaultValue origKeyName
                        origKeyName, outputColName
                    )
                    |> Map.ofList

                let keyAliasedExprs =
                    keyDefs
                    |> List.map (fun (origKeyName, rawKeyExpr) ->
                        let outputColName = keyNameToOutputAliasMap.[origKeyName]
                        PolarsWrapper.Alias(rawKeyExpr, outputColName)
                    )
                    |> List.toArray

                let aggs =
                    List.zip memberNames args
                    |> List.choose (fun (colName, argExpr) ->
                        if isKeyExpression argExpr then None
                        else
                            match elemExprOpt with
                            | Some elemExpr ->
                                let buildAgg (methodName: string) =
                                    let clonedInner = PolarsWrapper.CloneExpr elemExpr
                                    let aggCore =
                                        match methodName with
                                        | "Sum" -> PolarsWrapper.Sum clonedInner
                                        | "Min" -> PolarsWrapper.Min clonedInner
                                        | "Max" -> PolarsWrapper.Max clonedInner
                                        | "Average" -> PolarsWrapper.Mean clonedInner
                                        | "Count" -> PolarsWrapper.Len()
                                        | "LongCount" ->
                                            let len = PolarsWrapper.Len()
                                            let int64Dtype = PolarsWrapper.DataTypeExprFromDataType(PolarsWrapper.NewPrimitiveType(int PlDataType.Int64))
                                            PolarsWrapper.ExprCast(len, int64Dtype, strict = false, wrapNumerical = false)
                                        | _ -> failwithf "Unsupported aggregation operator: %s" methodName
                                    PolarsWrapper.Alias(aggCore, colName)

                                match argExpr with
                                | MethodCall(m, null, [ firstArg ]) when (match firstArg with :? ParameterExpression as p -> p.Name = groupParamName | _ -> false) ->
                                    Some (buildAgg m.Name)
                                | MethodCall(m, target, []) when not (isNull target) && (match target with :? ParameterExpression as p -> p.Name = groupParamName | _ -> false) ->
                                    Some (buildAgg m.Name)
                                | _ ->
                                    AggTranslator.tryTranslateAgg groupParamName argExpr colName
                            | None ->
                                AggTranslator.tryTranslateAgg groupParamName argExpr colName
                    )

                let groupBySpec = {
                    Keys = keyAliasedExprs
                    Aggs = aggs |> List.toArray
                    Having = None
                    MaintainOrder = false
                }
                let groupByOp = QueryOp.GroupBy groupBySpec

                // 4. Compile Having filters mapped to the aggregated column names
                let havingFilterOps =
                    ctx.HavingPreds
                    |> List.choose (fun havingPred ->
                        let rec translateHaving (expr: Expression) : ExprHandle option =
                            match expr with
                            | Binary(op, left, right) ->
                                match translateHaving left, translateHaving right with
                                | Some l, Some r ->
                                    let nodeType =
                                        match expr with
                                        | :? BinaryExpression as b -> b.NodeType
                                        | _ -> ExpressionType.Equal
                                    let translated =
                                        match nodeType with
                                        | ExpressionType.GreaterThan -> PolarsWrapper.Gt(l, r)
                                        | ExpressionType.GreaterThanOrEqual -> PolarsWrapper.GtEq(l, r)
                                        | ExpressionType.LessThan -> PolarsWrapper.Lt(l, r)
                                        | ExpressionType.LessThanOrEqual -> PolarsWrapper.LtEq(l, r)
                                        | ExpressionType.Equal -> PolarsWrapper.Eq(l, r)
                                        | ExpressionType.NotEqual -> PolarsWrapper.Neq(l, r)
                                        | ExpressionType.AndAlso -> PolarsWrapper.And(l, r)
                                        | ExpressionType.OrElse -> PolarsWrapper.Or(l, r)
                                        | _ -> PolarsWrapper.Gt(l, r)
                                    Some translated
                                | _ -> None

                            | Constant _ ->
                                ExprTranslator.tryTranslate "" expr

                            // Composite key member access in Having (e.g. g.Key.Year -> alias)
                            | MemberAccess(MemberAccess(paramExpr, k), m) 
                                when not (isNull paramExpr) && paramExpr.NodeType = ExpressionType.Parameter && k.Name = "Key" ->
                                let actualColName = keyNameToOutputAliasMap.TryFind m.Name |> Option.defaultValue m.Name
                                Some (PolarsWrapper.Col actualColName)

                            // Direct property access in Having (e.g. s.TotalRevenue)
                            | MemberAccess(paramExpr, m) when not (isNull paramExpr) && paramExpr.NodeType = ExpressionType.Parameter ->
                                Some (PolarsWrapper.Col m.Name)

                            | MethodCall _ as aggCall ->
                                let matchingColOpt =
                                    List.zip memberNames args
                                    |> List.tryPick (fun (colName, argExpr) ->
                                        if argExpr.ToString() = aggCall.ToString() then Some colName
                                        else None
                                    )
                                match matchingColOpt with
                                | Some colName -> Some (PolarsWrapper.Col colName)
                                | None ->
                                    AggTranslator.tryTranslateAgg havingPred.Parameters.[0].Name aggCall ""

                            | _ -> None

                        translateHaving havingPred.Body
                        |> Option.map QueryOp.Filter
                    )

                // 5. Compile Sort operations targeting GroupBy Keys or Aggregated columns
                let sortSpecs =
                    ctx.SortStages
                    |> List.choose (fun (sortLambda, isDesc) ->
                        let rec resolveSortCol (e: Expression) : string option =
                            match e with
                            // Single key: g.Key -> mapped alias
                            | MemberAccess(p, m) when m.Name = "Key" && keyDefs.Length = 1 ->
                                Some (keyNameToOutputAliasMap.[fst keyDefs.[0]])
                            // Composite key member: g.Key.Year, g.Key.Region -> mapped alias
                            | MemberAccess(MemberAccess(p, k), m) when k.Name = "Key" ->
                                Some (keyNameToOutputAliasMap.TryFind m.Name |> Option.defaultValue m.Name)
                            // Key parameter member (C# (k, g) => k.Year)
                            | MemberAccess(:? ParameterExpression as p, m) when isKeyArg (p :> Expression) ->
                                Some (keyNameToOutputAliasMap.TryFind m.Name |> Option.defaultValue m.Name)
                            // Direct property on result object: s.TotalRevenue
                            | MemberAccess(p, m) when not (isNull p) && p.NodeType = ExpressionType.Parameter ->
                                Some m.Name
                            // Aggregated column call: g.Sum(...)
                            | MethodCall _ as aggCall ->
                                List.zip memberNames args
                                |> List.tryPick (fun (colName, argExpr) ->
                                    if argExpr.ToString() = aggCall.ToString() then Some colName
                                    else None
                                )
                            | _ -> None

                        resolveSortCol sortLambda.Body
                        |> Option.map (fun colName ->
                            { Expr = PolarsWrapper.Col colName; Descending = isDesc; NullsLast = false }
                        )
                    )

                let sortOps =
                    if sortSpecs.IsEmpty then []
                    else [ QueryOp.Sort sortSpecs ]

                Some (sortOps @ havingFilterOps @ [ groupByOp ])

            | _ -> None

    /// Helper to translate individual projection arguments, supporting nested tuples and null-coalescing/IIF
    static member private TryTranslateSelectArg (paramName: string) (argExpr: Expression) : ExprHandle option =
        let rec extractLeafColumn (e: Expression) : string option =
            match e with
            | null -> None
            | MemberAccess(:? ParameterExpression as p, mem) when p.Name = paramName ->
                Some mem.Name
            | MemberAccess(inner, mem) ->
                match extractLeafColumn inner with
                | Some _ -> Some mem.Name
                | None -> Some mem.Name
            | _ -> None

        match ExprTranslator.tryTranslate paramName argExpr with
        | Some h -> Some h
        | None ->
            match argExpr with
            // Pattern: IIF((Box(tupledArg.Item3) == null), "NO_EMPLOYEE", tupledArg.Item3.Name) -> FillNull(Col "Name", Lit "NO_EMPLOYEE")
            | :? ConditionalExpression as cond ->
                let colNameOpt = extractLeafColumn cond.IfFalse
                let fallbackLitOpt =
                    match cond.IfTrue with
                    | :? ConstantExpression as ce when not (isNull ce.Value) ->
                        match ce.Value with
                        | :? string as s -> Some (PolarsWrapper.Lit s)
                        | :? int as i -> Some (PolarsWrapper.Lit i)
                        | :? int64 as l -> Some (PolarsWrapper.Lit l)
                        | :? double as d -> Some (PolarsWrapper.Lit d)
                        | _ -> None
                    | _ -> None

                match colNameOpt, fallbackLitOpt with
                | Some colName, Some fallbackLit ->
                    let colExpr = PolarsWrapper.Col colName
                    Some (PolarsWrapper.FillNull(colExpr, fallbackLit))
                | _ -> None

            // Multi-level tuple member access (e.g. tupledArg.Item1.Name -> Col "Name")
            | MemberAccess _ as ma ->
                extractLeafColumn ma
                |> Option.map PolarsWrapper.Col

            | _ -> None

    /// Compiles a Select projection lambda into native Polars Select expressions
    static member private CompileSelectProjection (projLambda: LambdaExpression) : ExprHandle array option =
        let param = projLambda.Parameters.[0]
        match projLambda.Body with
        | :? NewExpression as newExpr ->
            let args = newExpr.Arguments |> Seq.toList
            let memberNames = PolarsQuery<'T>.ExtractPascalCaseMemberNames newExpr
            
            let exprOpts =
                List.zip memberNames args
                |> List.map (fun (name, argExpr) ->
                    match PolarsQuery<'T>.TryTranslateSelectArg param.Name argExpr with
                    | Some h -> Some (PolarsWrapper.Alias(h, name))
                    | None -> None
                )
            
            if exprOpts |> List.forall Option.isSome then
                Some (exprOpts |> List.choose id |> List.toArray)
            else
                None

        | :? MemberInitExpression as initExpr ->
            let bindings = initExpr.Bindings |> Seq.toList
            let exprOpts =
                bindings
                |> List.choose (fun b ->
                    match b with
                    | :? MemberAssignment as assign ->
                        match PolarsQuery<'T>.TryTranslateSelectArg param.Name assign.Expression with
                        | Some h -> Some (PolarsWrapper.Alias(h, assign.Member.Name))
                        | None -> None
                    | _ -> None
                )
            if exprOpts.Length = bindings.Length then
                Some (exprOpts |> List.toArray)
            else
                None

        | ExtractColumnName colName ->
            Some [| PolarsWrapper.Col colName |]

        | _ ->
            None

    /// Compiles a Join stage into a native JoinSpec
    static member private CompileJoin (methodInfo: MethodInfo) (innerExpr: Expression) (outerKey: LambdaExpression) (innerKey: LambdaExpression) : JoinSpec option =
        let isRightJoin = methodInfo.Name = "RightJoin"
        let how = if isRightJoin || methodInfo.Name = "LeftJoin" then PlJoinType.Left else PlJoinType.Inner

        match PolarsQuery<'T>.ResolveToLazyFrameHandle innerExpr,
              ExprTranslator.tryTranslate outerKey.Parameters.[0].Name outerKey.Body,
              ExprTranslator.tryTranslate innerKey.Parameters.[0].Name innerKey.Body with
        | Some rightLf, Some leftKey, Some rightKey ->
            Some {
                RightLf = rightLf
                LeftOn = [| leftKey |]
                RightOn = [| rightKey |]
                How = how
                Suffix = if isRightJoin then Some "_left" else Some "_right"
                InvertSides = isRightJoin
            }
        | _ -> None

    /// Compiles Intersect/Except/IntersectBy/ExceptBy stages into a native JoinSpec (Semi/Anti Join)
    static member private CompileSetOp (methodInfo: MethodInfo) (secondExpr: Expression) (firstKeyOpt: LambdaExpression option) (secondKeyOpt: LambdaExpression option) (baseLf: LazyFrameHandle) : JoinSpec option =
        let how =
            match methodInfo.Name with
            | "Intersect" | "IntersectBy" -> PlJoinType.Semi
            | "Except" | "ExceptBy" -> PlJoinType.Anti
            | _ -> failwithf "Unsupported set operation: %s" methodInfo.Name

        match PolarsQuery<'T>.ResolveToLazyFrameHandle secondExpr with
        | None -> None
        | Some rightLf ->
            match firstKeyOpt, secondKeyOpt with
            | Some k1, Some k2 ->
                match ExprTranslator.tryTranslate k1.Parameters.[0].Name k1.Body,
                      ExprTranslator.tryTranslate k2.Parameters.[0].Name k2.Body with
                | Some lk, Some rk ->
                    Some { RightLf = rightLf; LeftOn = [| lk |]; RightOn = [| rk |]; How = how; Suffix = None; InvertSides = false }
                | _ -> None

            | Some k1, None ->
                match ExprTranslator.tryTranslate k1.Parameters.[0].Name k1.Body with
                | Some lk ->
                    let rightColNames = PolarsQuery<'T>.GetLazyColumnNames rightLf
                    if rightColNames.Length > 0 then
                        Some { RightLf = rightLf; LeftOn = [| lk |]; RightOn = [| PolarsWrapper.Col rightColNames.[0] |]; How = how; Suffix = None; InvertSides = false }
                    else None
                | None -> None

            | None, None ->
                let colNames = PolarsQuery<'T>.GetLazyColumnNames baseLf
                Some {
                    RightLf = rightLf
                    LeftOn = colNames |> Array.map PolarsWrapper.Col
                    RightOn = colNames |> Array.map PolarsWrapper.Col
                    How = how
                    Suffix = None
                    InvertSides = false
                }

            | _ -> None

    /// Applies a list of QueryOp to a target LazyFrameHandle in forward order.
    static member private ApplyNativeOps (sourceLf: LazyFrameHandle) (ops: QueryOp list) : LazyFrameHandle =
        ops
        |> List.fold (fun currentLf op ->
            match op with
            | QueryOp.WithColumns exprs ->
                PolarsWrapper.LazyWithColumns(currentLf, exprs)

            | QueryOp.WithRowIndex (name, offsetOpt) ->
                let offsetNullable = 
                    match offsetOpt with
                    | Some o -> Nullable(int o)
                    | None -> Nullable 0
                PolarsWrapper.LazyFrameWithRowIndex(currentLf, name, offsetNullable)

            | QueryOp.Filter exprHandle -> 
                PolarsWrapper.LazyFilter(currentLf, exprHandle)

            | QueryOp.Select exprs ->
                PolarsWrapper.LazySelect(currentLf, exprs)

            | QueryOp.Slice(offset, len) -> 
                PolarsWrapper.LazySlice(currentLf, offset, len)

            | QueryOp.Sort specs ->
                let exprs = specs |> List.map (fun s -> s.Expr) |> List.toArray
                let descending = specs |> List.map (fun s -> s.Descending) |> List.toArray
                let nullsLast = specs |> List.map (fun s -> s.NullsLast) |> List.toArray
                PolarsWrapper.LazyFrameSort(currentLf, exprs, descending, nullsLast, maintainOrder = false)

            | QueryOp.Join spec ->
                let leftLf, rightLf, leftOn, rightOn, suffix =
                    if spec.InvertSides then
                        spec.RightLf, currentLf, spec.RightOn, spec.LeftOn, Option.toObj (spec.Suffix |> Option.orElse (Some "_left"))
                    else
                        currentLf, spec.RightLf, spec.LeftOn, spec.RightOn, Option.toObj spec.Suffix

                PolarsWrapper.Join(
                    leftLf, rightLf, leftOn, rightOn, spec.How, suffix,
                    PlJoinValidation.ManyToMany, PlJoinCoalesce.JoinSpecific,
                    PlJoinMaintainOrder.None, PlJoinSide.None, false, Nullable(), 0UL
                )

            | QueryOp.GroupBy spec ->
                PolarsWrapper.LazyGroupByAgg(currentLf, spec.Keys, spec.Aggs, Option.toObj spec.Having, spec.MaintainOrder)

            | QueryOp.Unique spec ->
                let selector =
                    match spec.SubsetCols with
                    | Some cols when cols.Length > 0 -> PolarsWrapper.SelectorCols cols
                    | _ -> null
                PolarsWrapper.LazyUnique(currentLf, selector, spec.Keep, spec.MaintainOrder)

            | QueryOp.Concat spec ->
                let alignSchema (baseLf: LazyFrameHandle) (targetLf: LazyFrameHandle) : LazyFrameHandle =
                    let baseFields = PolarsQuery<'T>.GetLazySchemaFields baseLf
                    let targetFields = PolarsQuery<'T>.GetLazySchemaFields targetLf

                    let baseNames = baseFields |> Array.map fst
                    let targetNames = targetFields |> Array.map fst

                    if Set.ofArray baseNames = Set.ofArray targetNames then
                        let targetFieldMap = targetFields |> Map.ofArray

                        let reorderedAndCastedExprs =
                            baseFields
                            |> Array.map (fun (colName, baseDt) ->
                                let targetDt = targetFieldMap.[colName]
                                let colExpr = PolarsWrapper.Col colName
                                
                                let isSameType = PolarsWrapper.DataTypeEq(baseDt, targetDt)
                                if not isSameType then
                                    let dtExpr = PolarsWrapper.DataTypeExprFromDataType baseDt
                                    PolarsWrapper.ExprCast(colExpr, dtExpr, strict = false, wrapNumerical = false)
                                else
                                    colExpr
                            )
                        PolarsWrapper.LazySelect(targetLf, reorderedAndCastedExprs)
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

            | QueryOp.Rename spec ->
                PolarsWrapper.LazyRename(currentLf, spec.ExistingNames, spec.NewNames, strict = false)

            | QueryOp.SkipLast count ->
                let withIndexLf = PolarsWrapper.LazyFrameWithRowIndex(currentLf, "__row_idx", Nullable 0)
                let rowIdxCol = PolarsWrapper.Col "__row_idx"
                let countLit = PolarsWrapper.Lit (uint32 count)
                let lhsExpr = PolarsWrapper.Add(rowIdxCol, countLit)
                let totalLen = PolarsWrapper.Len()
                let filterExpr = PolarsWrapper.Lt(lhsExpr, totalLen)

                let filteredLf = PolarsWrapper.LazyFilter(withIndexLf, filterExpr)
                let dropSelector = PolarsWrapper.SelectorCols [| "__row_idx" |]
                PolarsWrapper.LazyFrameDrop(filteredLf, dropSelector)

            | QueryOp.Reverse ->
                PolarsWrapper.LazyFrameReverse currentLf

            | QueryOp.SelectPassthrough -> 
                currentLf
        ) (PolarsWrapper.LazyClone sourceLf)

    /// Parses the LINQ expression tree into native ops, client predicates, optional projection, and fallback signal
    member private this.CompilePipeline() : QueryOp list * (Func<'T, bool> list) * ((Type * Delegate) option) * bool =    
        Console.Error.WriteLine(sprintf "\x1b[1;31m[COMPILE PIPELINE ENTER] Type='PolarsQuery<%s>'\x1b[0m" typeof<'T>.Name)
        let rec flatten (e: Expression) (acc: LinqStage list) : LinqStage list =
            match e with
            | null -> acc
            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as l) ]) when m.Name = "Select" ->
                let rec isPassthroughTuple (body: Expression) =
                    match body with
                    | MemberAccess(:? ParameterExpression, memInfo) when isFSharpAnonymousMember memInfo -> true
                    | :? NewExpression as ne when ne.Type.Name.StartsWith("AnonymousObject") ->
                        // Detect F# redundant identity wrapper: _arg1 => new AnonymousObject`2(Item1 = _arg1.Item1, Item2 = _arg1.Item2)
                        let p = l.Parameters.[0]
                        let isIdentity =
                            ne.Arguments
                            |> Seq.mapi (fun idx arg ->
                                match arg with
                                | MemberAccess(:? ParameterExpression as pe, mem) when pe.Name = p.Name && mem.Name = sprintf "Item%d" (idx + 1) -> true
                                | _ -> false
                            )
                            |> Seq.forall id
                        isIdentity
                    | _ -> false

                if isPassthroughTuple l.Body then
                    flatten source acc
                else
                    flatten source (LinqStage.Project l :: acc)

            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as l) ]) when m.Name = "Where" ->
                flatten source (LinqStage.Filter l :: acc)

            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as l) ]) when m.Name = "OrderBy" || m.Name = "ThenBy" ->
                flatten source (LinqStage.Sort(l, isDescending = false) :: acc)

            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as l) ]) when m.Name = "OrderByDescending" || m.Name = "ThenByDescending" ->
                flatten source (LinqStage.Sort(l, isDescending = true) :: acc)

            | MethodCall(m, null, [ source; countExpr ]) when m.Name = "Take" ->
                match tryEvaluate countExpr with
                | Some c -> flatten source (LinqStage.Take(Convert.ToUInt32 c) :: acc)
                | None -> failwithf "Could not evaluate Take count argument: %A" countExpr

            | MethodCall(m, null, [ source; countExpr ]) when m.Name = "Skip" ->
                match tryEvaluate countExpr with
                | Some c -> flatten source (LinqStage.Skip(Convert.ToUInt32 c) :: acc)
                | None -> failwithf "Could not evaluate Skip count argument: %A" countExpr
            
            | MethodCall(m, null, [ source ]) when m.Name = "Distinct" ->
                flatten source (LinqStage.Distinct :: acc)

            | MethodCall(m, null, [ source ]) when m.Name = "Index" ->
                flatten source (LinqStage.Index :: acc)

            | MethodCall(m, null, [ source ]) when m.Name = "Order" ->
                flatten source (LinqStage.SortDefault(isDescending = false) :: acc)

            | MethodCall(m, null, [ source ]) when m.Name = "OrderDescending" ->
                flatten source (LinqStage.SortDefault(isDescending = true) :: acc)

            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as keySel) ]) when m.Name = "DistinctBy" ->
                flatten source (LinqStage.DistinctBy keySel :: acc)

            | MethodCall(m, null, [ source; kExpr; eExpr; rExpr ]) when m.Name = "GroupBy" ->
                let rec extractLambda (e: Expression) : LambdaExpression option =
                    match e with
                    | null -> None
                    | :? LambdaExpression as l -> Some l
                    | Unary(ExpressionType.Quote, inner) -> extractLambda inner
                    | _ -> None

                match extractLambda kExpr, extractLambda eExpr, extractLambda rExpr with
                | Some k, Some e, Some r ->
                    flatten source (LinqStage.GroupByWithElementAndResult(k, e, r) :: acc)
                | _ ->
                    printfn "[LINQ DEBUG] GroupBy 4-arg lambda extraction failed! kExpr=%A, eExpr=%A, rExpr=%A" kExpr eExpr rExpr
                    acc

            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as k); StripQuotes (:? LambdaExpression as r) ]) when m.Name = "GroupBy" ->
                flatten source (LinqStage.GroupByWithResult(k, r) :: acc)

            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as k) ]) when m.Name = "GroupBy" ->
                flatten source (LinqStage.GroupByKey k :: acc)

            | MethodCall(m, _, outer :: inner :: StripQuotes (:? LambdaExpression as outerKey) :: StripQuotes (:? LambdaExpression as innerKey) :: StripQuotes (:? LambdaExpression as resSel) :: _) 
                    when m.Name = "Join" || m.Name = "LeftJoin" || m.Name = "RightJoin" ->
                    flatten outer (LinqStage.Join(m, inner, outerKey, innerKey, resSel) :: acc)

            | MethodCall(m, null, [ first; second ]) when m.Name = "Intersect" || m.Name = "Except" ->
                flatten first (LinqStage.SetOp(m, second, None, None) :: acc)

            | MethodCall(m, null, [ first; second; StripQuotes (:? LambdaExpression as keySel) ]) 
                when m.Name = "IntersectBy" || m.Name = "ExceptBy" ->
                flatten first (LinqStage.SetOp(m, second, Some keySel, None) :: acc)

            | MethodCall(m, null, [ first; second; StripQuotes (:? LambdaExpression as k1); StripQuotes (:? LambdaExpression as k2) ]) 
                when m.Name = "IntersectBy" || m.Name = "ExceptBy" ->
                flatten first (LinqStage.SetOp(m, second, Some k1, Some k2) :: acc)
            
            | MethodCall(m, null, [ first; second ]) when m.Name = "Concat" ->
                flatten first (LinqStage.Concat second :: acc)

            | MethodCall(m, null, [ first; second ]) when m.Name = "Union" ->
                flatten first (LinqStage.Union(second, None) :: acc)

            | MethodCall(m, null, [ first; second; StripQuotes (:? LambdaExpression as keySel) ]) when m.Name = "UnionBy" ->
                flatten first (LinqStage.Union(second, Some keySel) :: acc)

            // | MethodCall(m, null, source :: StripQuotes (:? LambdaExpression as colSel) :: rest) when m.Name = "SelectMany" ->
            //     let resSelOpt =
            //         match rest with
            //         | [ StripQuotes (:? LambdaExpression as r) ] -> Some r
            //         | _ -> None

            //     let paramName = colSel.Parameters.[0].Name
            //     if PolarsQuery<'T>.ContainsParameter paramName colSel.Body then
            //         flatten source (LinqStage.Explode(colSel, resSelOpt) :: acc)
            //     else
            //         flatten source (LinqStage.CrossJoin(colSel.Body, resSelOpt) :: acc)
            | MethodCall(m, null, source :: StripQuotes (:? LambdaExpression as colSel) :: rest) when m.Name = "SelectMany" ->
                let resSelOpt =
                    match rest with
                    | [ StripQuotes (:? LambdaExpression as r) ] -> Some r
                    | _ -> None

                let rec unwrapQuote (e: Expression) =
                    match e with
                    | :? UnaryExpression as u when u.NodeType = ExpressionType.Quote || u.NodeType = ExpressionType.Convert -> unwrapQuote u.Operand
                    | _ -> e

                match unwrapQuote colSel.Body with
                | :? MethodCallExpression as innerMc ->
                    // 关键解包：若 SelectMany 内部嵌套了 GroupJoin/SelectMany 管道，递归扁平化拆包！
                    let innerStages = flatten innerMc []
                    flatten source (innerStages @ acc)
                | _ ->
                    let paramName = colSel.Parameters.[0].Name
                    if PolarsQuery<'T>.ContainsParameter paramName colSel.Body then
                        flatten source (LinqStage.Explode(colSel, resSelOpt) :: acc)
                    else
                        flatten source (LinqStage.CrossJoin(colSel.Body, resSelOpt) :: acc)

            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as l) ]) when m.Name = "TakeWhile" ->
                flatten source (LinqStage.TakeWhile l :: acc)

            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as l) ]) when m.Name = "SkipWhile" ->
                flatten source (LinqStage.SkipWhile l :: acc)

            | MethodCall(m, null, [ outer; inner; 
                                StripQuotes (:? LambdaExpression as outerKey); 
                                StripQuotes (:? LambdaExpression as innerKey); 
                                StripQuotes (:? LambdaExpression as resSel) ]) 
                when m.Name = "GroupJoin" ->
                flatten outer (LinqStage.GroupJoin(m, inner, outerKey, innerKey, resSel) :: acc)

            | MethodCall(m, null, [ source; second ]) when m.Name = "Zip" ->
                flatten source (LinqStage.Zip(second, None) :: acc)

            | MethodCall(m, null, [ source; second; StripQuotes (:? LambdaExpression as resSel) ]) when m.Name = "Zip" ->
                flatten source (LinqStage.Zip(second, Some resSel) :: acc)

            | MethodCall(m, null, [ first; second; third ]) when m.Name = "Zip" ->
                flatten first (LinqStage.Zip3(second, third) :: acc)

            | MethodCall(m, null, [ source; sizeExpr ]) when m.Name = "Chunk" ->
                match tryEvaluate sizeExpr with
                | Some s ->
                    let size = Convert.ToInt32 s
                    if size <= 0 then
                        raise (ArgumentOutOfRangeException("size", "Chunk size must be positive."))
                    flatten source (LinqStage.Chunk size :: acc)
                | None ->
                    failwithf "Could not evaluate Chunk size argument: %A" sizeExpr

            | MethodCall(m, null, [ source; countExpr ]) when m.Name = "TakeLast" ->
                match tryEvaluate countExpr with
                | Some c -> flatten source (LinqStage.TakeLast(Convert.ToUInt32 c) :: acc)
                | None -> failwithf "Could not evaluate TakeLast count: %A" countExpr

            | MethodCall(m, null, [ source; countExpr ]) when m.Name = "SkipLast" ->
                match tryEvaluate countExpr with
                | Some c -> flatten source (LinqStage.SkipLast(Convert.ToUInt32 c) :: acc)
                | None -> failwithf "Could not evaluate SkipLast count: %A" countExpr

            | MethodCall(m, null, [ source; elemExpr ]) when m.Name = "Append" ->
                flatten source (LinqStage.Append elemExpr :: acc)

            | MethodCall(m, null, [ source; elemExpr ]) when m.Name = "Prepend" ->
                flatten source (LinqStage.Prepend elemExpr :: acc)

            | MethodCall(m, null, [ source ]) when m.Name = "Reverse" ->
                flatten source (LinqStage.Reverse :: acc)

            | MethodCall(m, null, [ source ]) when m.Name = "Shuffle" ->
                flatten source (LinqStage.Shuffle :: acc)

            | MethodCall(m, null, [ source ]) when m.Name = "Cast" && m.IsGenericMethod ->
                let targetType = m.GetGenericArguments().[0]
                flatten source (LinqStage.Cast targetType :: acc)

            | MethodCall(m, null, [ source ]) when m.Name = "OfType" && m.IsGenericMethod ->
                let sourceType = 
                    if source.Type.IsGenericType then source.Type.GetGenericArguments().[0]
                    elif source.Type.IsArray then source.Type.GetElementType()
                    else typeof<obj>
                let targetType = m.GetGenericArguments().[0]
                flatten source (LinqStage.OfType(sourceType, targetType) :: acc)

            | MethodCall(m, null, [ source ]) when m.Name = "DefaultIfEmpty" ->
                flatten source (LinqStage.DefaultIfEmpty None :: acc)

            | MethodCall(m, null, [ source; defaultExpr ]) when m.Name = "DefaultIfEmpty" ->
                flatten source (LinqStage.DefaultIfEmpty (Some defaultExpr) :: acc)

            | MethodCall(m, null, source :: keyExpr :: _) when m.Name = "CountBy" ->
                let rec extractLambda (e: Expression) =
                    match e with
                    | :? LambdaExpression as l -> Some l
                    | Unary(ExpressionType.Quote, inner) -> extractLambda inner
                    | _ -> None

                match extractLambda keyExpr with
                | Some keyLambda ->
                    flatten source (LinqStage.CountBy keyLambda :: acc)
                | None ->
                    failwithf "Could not extract LambdaExpression from CountBy key argument: %A" keyExpr

            | _ -> acc

        printfn "\n\x1b[1;32m================ [ORIGINAL EXPRESSION TREE] ================\x1b[0m"
        printfn "%s" (expression.ToString())

        let effectiveExpr = FSharpAst.rewrite expression

        printfn "\n\x1b[1;32m================ [REWRITTEN EXPRESSION TREE] ================\x1b[0m"
        printfn "%s" (effectiveExpr.ToString())
        printfn "\x1b[1;32m============================================================\x1b[0m\n"

        let stages = flatten effectiveExpr []
        printfn "\x1b[33m[FLATTENED STAGES COUNT]\x1b[0m %d" stages.Length
        stages |> List.iteri (fun idx s -> printfn "  Stage[%d] = %A" idx s)

        let addSort (spec: SortSpec) (ops: QueryOp list) =
            match ops with
            | QueryOp.Sort specs :: tail -> QueryOp.Sort (specs @ [spec]) :: tail
            | _ -> QueryOp.Sort [spec] :: ops

        let rec fuse (remaining: LinqStage list) (opsAcc: QueryOp list) (clientPreds: Func<'T, bool> list) (clientProjOpt: (Type * Delegate) option) : QueryOp list * (Func<'T, bool> list) * ((Type * Delegate) option) * bool =
            match remaining with
            // 1. Universal Pipeline GroupBy
            | LinqStage.GroupByKey keyLambda :: tail when clientPreds.IsEmpty ->
                let rec slurp (stages: LinqStage list) (ctx: GroupFoldContext) =
                    match stages with
                    | LinqStage.Filter pred :: next ->
                        slurp next { ctx with HavingPreds = ctx.HavingPreds @ [ pred ] }
                    | LinqStage.Sort(sortLambda, isDesc) :: next ->
                        slurp next { ctx with SortStages = ctx.SortStages @ [ (sortLambda, isDesc) ] }
                    | LinqStage.Project projLambda :: next ->
                        let isKeyArg (e: Expression) =
                            match e with
                            | MemberAccess(p, m) when not (isNull p) && p.NodeType = ExpressionType.Parameter && m.Name = "Key" -> true
                            | _ -> false

                        match PolarsQuery<'T>.CompileUnifiedGroupBy ctx projLambda.Body projLambda.Parameters.[0].Name isKeyArg with
                        | Some compiledOps ->
                            fuse next (compiledOps @ opsAcc) clientPreds clientProjOpt
                        | None ->
                            Console.Error.WriteLine("\x1b[1;33m[GROUPBY PUSHDOWN] Native translation not possible. Switching to client in-memory execution.\x1b[0m")
                            [], [], None, true
                    | _ ->
                        Console.Error.WriteLine("\x1b[1;33m[GROUPBY PUSHDOWN] No terminal projection. Switching to client in-memory execution.\x1b[0m")
                        [], [], None, true

                let initCtx = { KeyLambda = keyLambda; ElemLambdaOpt = None; HavingPreds = []; SortStages = [] }
                slurp tail initCtx

            // 2. C# 3-arg GroupBy
            | LinqStage.GroupByWithResult(keyLambda, resLambda) :: tail when clientPreds.IsEmpty ->
                let havingPreds, nextTail =
                    match tail with
                    | LinqStage.Filter havingPred :: rest -> [ havingPred ], rest
                    | _ -> [], tail

                let ctx = { KeyLambda = keyLambda; ElemLambdaOpt = None; HavingPreds = havingPreds; SortStages = [] }
                let resKeyParam = resLambda.Parameters.[0]
                let isKeyArg (e: Expression) =
                    match e with
                    | :? ParameterExpression as p when p.Name = resKeyParam.Name -> true
                    | MemberAccess(p, _) when not (isNull p) && p.NodeType = ExpressionType.Parameter -> true
                    | _ -> false

                match PolarsQuery<'T>.CompileUnifiedGroupBy ctx resLambda.Body resLambda.Parameters.[1].Name isKeyArg with
                | Some compiledOps ->
                    fuse nextTail (compiledOps @ opsAcc) clientPreds clientProjOpt
                | None ->
                    Console.Error.WriteLine("\x1b[1;33m[GROUPBY PUSHDOWN] Native translation not possible. Switching to client in-memory execution.\x1b[0m")
                    [], [], None, true

            // 3. C# 4-arg GroupBy
            | LinqStage.GroupByWithElementAndResult(keyLambda, elemLambda, resLambda) :: tail when clientPreds.IsEmpty ->
                let havingPreds, nextTail =
                    match tail with
                    | LinqStage.Filter havingPred :: rest -> [ havingPred ], rest
                    | _ -> [], tail

                let ctx = { KeyLambda = keyLambda; ElemLambdaOpt = Some elemLambda; HavingPreds = havingPreds; SortStages = [] }
                let resKeyParam = resLambda.Parameters.[0]
                let isKeyArg (e: Expression) =
                    match e with
                    | :? ParameterExpression as p when p.Name = resKeyParam.Name -> true
                    | MemberAccess(p, _) when not (isNull p) && p.NodeType = ExpressionType.Parameter && (match p with :? ParameterExpression as pe -> pe.Name = resKeyParam.Name | _ -> false) -> true
                    | _ -> false

                match PolarsQuery<'T>.CompileUnifiedGroupBy ctx resLambda.Body resLambda.Parameters.[1].Name isKeyArg with
                | Some compiledOps ->
                    fuse nextTail (compiledOps @ opsAcc) clientPreds clientProjOpt
                | None ->
                    Console.Error.WriteLine("\x1b[1;33m[GROUPBY PUSHDOWN] Native translation not possible. Switching to client in-memory execution.\x1b[0m")
                    [], [], None, true

            | LinqStage.Filter l :: tail ->
                match ExprTranslator.tryTranslate l.Parameters.[0].Name l.Body with
                | Some exprHandle when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                    fuse tail (QueryOp.Filter exprHandle :: opsAcc) clientPreds clientProjOpt
                | _ ->
                    // Try translating member access on tuples: tupledArg.Item1.Name != "Bob"
                    let rec tryTranslateTupleFilter (e: Expression) =
                        match e with
                        | Binary(op, left, right) ->
                            match tryTranslateTupleFilter left, tryTranslateTupleFilter right with
                            | Some l, Some r ->
                                match op with
                                | ExpressionType.NotEqual -> Some (PolarsWrapper.Neq(l, r))
                                | ExpressionType.Equal -> Some (PolarsWrapper.Eq(l, r))
                                | ExpressionType.GreaterThan -> Some (PolarsWrapper.Gt(l, r))
                                | ExpressionType.GreaterThanOrEqual -> Some (PolarsWrapper.GtEq(l, r))
                                | ExpressionType.LessThan -> Some (PolarsWrapper.Lt(l, r))
                                | ExpressionType.LessThanOrEqual -> Some (PolarsWrapper.LtEq(l, r))
                                | ExpressionType.AndAlso -> Some (PolarsWrapper.And(l, r))
                                | ExpressionType.OrElse -> Some (PolarsWrapper.Or(l, r))
                                | _ -> None
                            | _ -> None
                        | MemberAccess _ as ma ->
                            match PolarsQuery<'T>.TryTranslateSelectArg l.Parameters.[0].Name ma with
                            | Some colExpr -> Some colExpr
                            | None -> None
                        | Constant _ ->
                            ExprTranslator.tryTranslate "" e
                        | _ -> None

                    match tryTranslateTupleFilter l.Body with
                    | Some exprHandle when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                        fuse tail (QueryOp.Filter exprHandle :: opsAcc) clientPreds clientProjOpt
                    | _ ->
                        let compiled = l.Compile()
                        let safePred = 
                            Func<'T, bool>(fun (item: 'T) ->
                                try compiled.DynamicInvoke([| box item |]) :?> bool
                                with _ -> true
                            )
                        fuse tail opsAcc (safePred :: clientPreds) clientProjOpt

            | LinqStage.Index :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                let indexOp = QueryOp.WithRowIndex("Index", Some 0u)
                let int32Dtype = PolarsWrapper.DataTypeExprFromDataType(PolarsWrapper.NewPrimitiveType(int PlDataType.Int32))
                let castIndexExpr = 
                    PolarsWrapper.Col "Index"
                    |> fun c -> PolarsWrapper.ExprCast(c, int32Dtype, strict = false, wrapNumerical=false)
                let withCastOp = QueryOp.WithColumns [| castIndexExpr |]
                fuse tail (withCastOp :: indexOp :: opsAcc) clientPreds clientProjOpt

            | LinqStage.SortDefault isDesc :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                let colNames = PolarsQuery<'T>.GetLazyColumnNames lfCloned
                let targetCol = if colNames.Length > 0 then colNames.[0] else ""
                let colExpr = PolarsWrapper.Col targetCol
                let spec = { Expr = colExpr; Descending = isDesc; NullsLast = false }
                fuse tail (addSort spec opsAcc) clientPreds clientProjOpt
            
            | LinqStage.Sort(l, isDesc) :: tail ->
                let param = l.Parameters.[0]
                let isGrouping = param.Type.IsGenericType && param.Type.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>
                if not isGrouping && clientPreds.IsEmpty && clientProjOpt.IsNone then
                    let sortExprOpt =
                        match ExprTranslator.tryTranslate param.Name l.Body with
                        | Some h -> Some h
                        | None -> PolarsQuery<'T>.TryTranslateSelectArg param.Name l.Body

                    match sortExprOpt with
                    | Some exprHandle ->
                        let spec = { Expr = exprHandle; Descending = isDesc; NullsLast = false }
                        fuse tail (addSort spec opsAcc) clientPreds clientProjOpt
                    | None -> fuse tail opsAcc clientPreds clientProjOpt
                else
                    fuse tail opsAcc clientPreds clientProjOpt

            | LinqStage.Join(m, inner, outerKey, innerKey, _) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match PolarsQuery<'T>.CompileJoin m inner outerKey innerKey with
                | Some spec -> fuse tail (QueryOp.Join spec :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.GroupJoin(m, inner, outerKey, innerKey, _) :: LinqStage.Explode(collSelector, _) :: tail 
                when collSelector.ToString().Contains("DefaultIfEmpty") && clientPreds.IsEmpty && clientProjOpt.IsNone ->
                
                match PolarsQuery<'T>.ResolveToLazyFrameHandle inner,
                      ExprTranslator.tryTranslate outerKey.Parameters.[0].Name outerKey.Body,
                      ExprTranslator.tryTranslate innerKey.Parameters.[0].Name innerKey.Body with
                | Some rightLf, Some leftKey, Some rightKey ->
                    let joinSpec = {
                        RightLf = rightLf
                        LeftOn = [| leftKey |]
                        RightOn = [| rightKey |]
                        How = PlJoinType.Left
                        Suffix = None
                        InvertSides = false
                    }
                    fuse tail (QueryOp.Join joinSpec :: opsAcc) clientPreds clientProjOpt
                | _ -> [], [], None, true

            | LinqStage.SetOp(m, second, k1Opt, k2Opt) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match PolarsQuery<'T>.CompileSetOp m second k1Opt k2Opt lfCloned with
                | Some spec -> fuse tail (QueryOp.Join spec :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.Skip s :: LinqStage.Take t :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail (QueryOp.Slice(int64 s, t) :: opsAcc) clientPreds clientProjOpt

            | LinqStage.Take count :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail (QueryOp.Slice(0L, count) :: opsAcc) clientPreds clientProjOpt

            | LinqStage.Skip count :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail (QueryOp.Slice(int64 count, UInt32.MaxValue) :: opsAcc) clientPreds clientProjOpt

            | LinqStage.Distinct :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                let spec = { SubsetCols = None; Keep = PlUniqueKeepStrategy.First; MaintainOrder = false }
                fuse tail (QueryOp.Unique spec :: opsAcc) clientPreds clientProjOpt

            | LinqStage.DistinctBy keyLambda :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match PolarsQuery<'T>.ExtractDistinctColumns keyLambda with
                | Some cols ->
                    let spec = { SubsetCols = Some cols; Keep = PlUniqueKeepStrategy.First; MaintainOrder = false }
                    fuse tail (QueryOp.Unique spec :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.Concat otherExpr :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match PolarsQuery<'T>.CompileSubtreeToLazyFrame otherExpr materializer with
                | Some otherCompiledLf ->
                    let spec = { OtherLf = otherCompiledLf; Prepend = false }
                    fuse tail (QueryOp.Concat spec :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.Zip(secondExpr, resLambdaOpt) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match PolarsQuery<'T>.ResolveToLazyFrameHandle secondExpr with
                | Some otherLf ->
                    let baseCols = PolarsQuery<'T>.GetLazyColumnNames lfCloned |> Set.ofArray
                    let secondCols = PolarsQuery<'T>.GetLazyColumnNames otherLf
                    let duplicateCols = secondCols |> Array.filter baseCols.Contains

                    let safeSecondLf, suffixMap =
                        if duplicateCols.Length > 0 then
                            let newNames = duplicateCols |> Array.map (fun c -> c + "_second")
                            let renamedLf = PolarsWrapper.LazyRename(otherLf, duplicateCols, newNames, strict = false)
                            let mapping = Array.zip duplicateCols newNames |> Map.ofArray
                            renamedLf, mapping
                        else
                            otherLf, Map.empty

                    let clonedOther = PolarsWrapper.LazyClone safeSecondLf
                    let zipOp = QueryOp.HorizontalConcat [| clonedOther |]

                    let renameOps =
                        match resLambdaOpt with
                        | Some resLambda ->
                            let renames = PolarsQuery<'T>.ExtractBinaryResultRenames resLambda None
                            if not renames.IsEmpty then
                                let mappedRenames =
                                    renames
                                    |> List.map (fun (srcCol, destCol) ->
                                        match suffixMap.TryFind srcCol with
                                        | Some actualColName -> (actualColName, destCol)
                                        | None -> (srcCol, destCol)
                                    )
                                [ QueryOp.Rename { ExistingNames = mappedRenames |> List.map fst |> List.toArray
                                                   NewNames = mappedRenames |> List.map snd |> List.toArray } ]
                            else []
                        | None -> []

                    let nextOps = renameOps @ (zipOp :: opsAcc)
                    fuse tail nextOps clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.Zip3(secondExpr, thirdExpr) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match PolarsQuery<'T>.ResolveToLazyFrameHandle secondExpr,
                      PolarsQuery<'T>.ResolveToLazyFrameHandle thirdExpr with
                | Some secondLf, Some thirdLf ->
                    let suffixDuplicateColumns (existingCols: Set<string>) (lf: LazyFrameHandle) (suffix: string) =
                        let cols = PolarsQuery<'T>.GetLazyColumnNames lf
                        let duplicates = cols |> Array.filter existingCols.Contains
                        if duplicates.Length > 0 then
                            let newNames = duplicates |> Array.map (fun c -> c + suffix)
                            let renamedLf = PolarsWrapper.LazyRename(lf, duplicates, newNames, strict = false)
                            renamedLf, existingCols |> Set.union (Set.ofArray cols)
                        else
                            lf, existingCols |> Set.union (Set.ofArray cols)

                    let baseCols = PolarsQuery<'T>.GetLazyColumnNames lfCloned |> Set.ofArray
                    let safeSecondLf, colsAfter2 = suffixDuplicateColumns baseCols secondLf "_second"
                    let safeThirdLf, _ = suffixDuplicateColumns colsAfter2 thirdLf "_third"

                    let zip3Op = QueryOp.HorizontalConcat [| PolarsWrapper.LazyClone safeSecondLf; PolarsWrapper.LazyClone safeThirdLf |]
                    fuse tail (zip3Op :: opsAcc) clientPreds clientProjOpt
                | _ -> [], [], None, true

            | LinqStage.Union(secondExpr, keyLambdaOpt) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match PolarsQuery<'T>.ResolveToLazyFrameHandle secondExpr with
                | Some otherLf ->
                    let concatOp = QueryOp.Concat { OtherLf = otherLf; Prepend = false }
                    let uniqueSpec =
                        match keyLambdaOpt with
                        | Some keyLambda ->
                            match PolarsQuery<'T>.ExtractDistinctColumns keyLambda with
                            | Some cols -> { SubsetCols = Some cols; Keep = PlUniqueKeepStrategy.First; MaintainOrder = false }
                            | None -> { SubsetCols = None; Keep = PlUniqueKeepStrategy.First; MaintainOrder = false }
                        | None -> { SubsetCols = None; Keep = PlUniqueKeepStrategy.First; MaintainOrder = false }

                    fuse tail (QueryOp.Unique uniqueSpec :: concatOp :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.Explode(colLambda, resLambdaOpt) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match PolarsQuery<'T>.ExtractExplodeColumn colLambda with
                | Some colName ->
                    let explodeOp = QueryOp.Explode { ColumnNames = [| colName |]; EmptyAsNull = true; KeepNulls = true }

                    let renameOps =
                        match resLambdaOpt with
                        | Some resLambda when resLambda.Parameters.Count >= 2 ->
                            let itemParam = resLambda.Parameters.[1]
                            let renames = PolarsQuery<'T>.ExtractBinaryResultRenames resLambda (Some(itemParam.Name, colName))
                            if not renames.IsEmpty then
                                [ QueryOp.Rename { ExistingNames = renames |> List.map fst |> List.toArray; NewNames = renames |> List.map snd |> List.toArray } ]
                            else []
                        | _ -> []

                    fuse tail (renameOps @ (explodeOp :: opsAcc)) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.CrossJoin(innerExpr, resLambdaOpt) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match PolarsQuery<'T>.ResolveToLazyFrameHandle innerExpr with
                | Some rightLf ->
                    let joinSpec = {
                        RightLf = rightLf
                        LeftOn = [||]
                        RightOn = [||]
                        How = PlJoinType.Cross
                        Suffix = Some "_right"
                        InvertSides = false
                    }
                    let joinOp = QueryOp.Join joinSpec
                    let renameOps =
                        match resLambdaOpt with
                        | Some resLambda ->
                            let renames = PolarsQuery<'T>.ExtractBinaryResultRenames resLambda None
                            if not renames.IsEmpty then
                                [ QueryOp.Rename { ExistingNames = renames |> List.map fst |> List.toArray; NewNames = renames |> List.map snd |> List.toArray } ]
                            else []
                        | None -> []

                    fuse tail (renameOps @ (joinOp :: opsAcc)) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.TakeWhile l :: tail ->
                let param = l.Parameters.[0]
                match ExprTranslator.tryTranslate param.Name l.Body with
                | Some boolExpr when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                    let cumMinExpr = PolarsWrapper.CumMin(boolExpr, false)
                    fuse tail (QueryOp.Filter cumMinExpr :: opsAcc) clientPreds clientProjOpt
                | _ ->
                    let compiled = l.Compile() :?> Func<'T, bool>
                    fuse tail opsAcc (compiled :: clientPreds) clientProjOpt

            | LinqStage.SkipWhile l :: tail ->
                let param = l.Parameters.[0]
                match ExprTranslator.tryTranslate param.Name l.Body with
                | Some boolExpr when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                    let notExpr = PolarsWrapper.Not boolExpr
                    let cumMaxExpr = PolarsWrapper.CumMax(notExpr, false)
                    fuse tail (QueryOp.Filter cumMaxExpr :: opsAcc) clientPreds clientProjOpt
                | _ -> 
                    let compiled = l.Compile() :?> Func<'T, bool>
                    fuse tail opsAcc (compiled :: clientPreds) clientProjOpt

            | LinqStage.GroupByKey _ :: tail ->
                fuse tail opsAcc clientPreds clientProjOpt

            | LinqStage.GroupJoin(_, innerExpr, outerKey, innerKey, resSel) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                let resolvedRightLfOpt = PolarsQuery<'T>.ResolveToLazyFrameHandle innerExpr
                let leftKeyOpt = ExprTranslator.tryTranslate outerKey.Parameters.[0].Name outerKey.Body
                let rightKeyOpt = ExprTranslator.tryTranslate innerKey.Parameters.[0].Name innerKey.Body

                match resolvedRightLfOpt, leftKeyOpt, rightKeyOpt with
                | Some rightLf, Some leftKey, Some rightKey ->
                    let groupParam = resSel.Parameters.[1]

                    let preAggRightLf, aggColOpt =
                        match PolarsQuery<'T>.TryExtractGroupAgg groupParam.Name resSel with
                        | Some (aggColName, aggExpr) ->
                            let clonedRight = PolarsWrapper.LazyClone rightLf
                            let aggKeyClone = PolarsWrapper.CloneExpr rightKey
                            let aggLf = PolarsWrapper.LazyGroupByAgg(clonedRight, [| aggKeyClone |], [| aggExpr |], null, false)
                            aggLf, Some aggColName
                        | None -> PolarsWrapper.LazyClone rightLf, None

                    let leftKeyForJoin = PolarsWrapper.CloneExpr leftKey
                    let rightKeyForJoin = PolarsWrapper.CloneExpr rightKey
                    let joinSpec = {
                        RightLf = preAggRightLf
                        LeftOn = [| leftKeyForJoin |]
                        RightOn = [| rightKeyForJoin |]
                        How = PlJoinType.Left
                        Suffix = Some "_right"
                        InvertSides = false
                    }
                    let joinOp = QueryOp.Join joinSpec

                    let renames = PolarsQuery<'T>.ExtractBinaryResultRenames resSel None
                    let renameOps =
                        if not renames.IsEmpty then
                            [ QueryOp.Rename { ExistingNames = renames |> List.map fst |> List.toArray; NewNames = renames |> List.map snd |> List.toArray } ]
                        else []

                    let fillOps =
                        match aggColOpt with
                        | Some aggCol ->
                            let colExpr = PolarsWrapper.Col aggCol
                            let zeroLit = PolarsWrapper.Lit 0u
                            let filledExpr = PolarsWrapper.FillNull(colExpr, zeroLit)
                            let aliased = PolarsWrapper.Alias(filledExpr, aggCol)
                            [ QueryOp.WithColumns [| aliased |] ]
                        | None -> []

                    let nextOps = renameOps @ fillOps @ (joinOp :: opsAcc)
                    fuse tail nextOps clientPreds clientProjOpt
                | _ -> [], [], None, true
            
            | LinqStage.TakeLast count :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                let sliceOp = QueryOp.Slice(-int64 count, count)
                fuse tail (sliceOp :: opsAcc) clientPreds clientProjOpt

            | LinqStage.SkipLast count :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail (QueryOp.SkipLast count :: opsAcc) clientPreds clientProjOpt

            | LinqStage.Append elemExpr :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match tryEvaluate elemExpr with
                | Some elem when not (isNull elem) ->
                    let elemType = elem.GetType()
                    let fromRows = typeof<DataFrameBuilder>.GetMethod("FromRows", BindingFlags.Public ||| BindingFlags.Static).MakeGenericMethod(elemType)
                    let typedArray = Array.CreateInstance(elemType, 1)
                    typedArray.SetValue(elem, 0)

                    let singleDf = fromRows.Invoke(null, [| box typedArray |]) :?> DataFrameHandle
                    let otherLf = PolarsWrapper.DataFrameToLazy singleDf
                    let concatOp = QueryOp.Concat { OtherLf = otherLf; Prepend = false }
                    fuse tail (concatOp :: opsAcc) clientPreds clientProjOpt
                | _ -> [], [], None, true

            | LinqStage.Prepend elemExpr :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match tryEvaluate elemExpr with
                | Some elem when not (isNull elem) ->
                    let elemType = elem.GetType()
                    let fromRows = typeof<DataFrameBuilder>.GetMethod("FromRows", BindingFlags.Public ||| BindingFlags.Static).MakeGenericMethod(elemType)
                    let typedArray = Array.CreateInstance(elemType, 1)
                    typedArray.SetValue(elem, 0)

                    let singleDf = fromRows.Invoke(null, [| box typedArray |]) :?> DataFrameHandle
                    let otherLf = PolarsWrapper.DataFrameToLazy singleDf
                    let concatOp = QueryOp.Concat { OtherLf = otherLf; Prepend = true }
                    fuse tail (concatOp :: opsAcc) clientPreds clientProjOpt
                | _ -> [], [], None, true

            | LinqStage.Reverse :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail (QueryOp.Reverse :: opsAcc) clientPreds clientProjOpt

            | LinqStage.DefaultIfEmpty _ :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail opsAcc clientPreds clientProjOpt

            // Universal Select Projection Pushdown vs. UDF Client Fallback
            | LinqStage.Project projLambda :: tail ->
                match PolarsQuery<'T>.CompileSelectProjection projLambda with
                | Some exprs when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                    fuse tail (QueryOp.Select exprs :: opsAcc) clientPreds None
                | _ ->
                    let sourceType = projLambda.Parameters.[0].Type
                    if sourceType.Name.StartsWith("AnonymousObject") || sourceType.Name.Contains("TransparentIdentifier") then
                        Console.Error.WriteLine("\x1b[1;33m[PROJECT FALLBACK] Transparent identifier cannot be materialized from native DF. Switching to in-memory LINQ.\x1b[0m")
                        [], [], None, true
                    else
                        let compiledDel = projLambda.Compile()
                        let nextProj =
                            match clientProjOpt with
                            | None -> Some (sourceType, compiledDel)
                            | Some (prevSrcType, prevDel) ->
                                let composed =
                                    Func<obj, obj>(fun (x: obj) ->
                                        let mid = prevDel.DynamicInvoke([| x |])
                                        compiledDel.DynamicInvoke([| mid |])
                                    )
                                Some (prevSrcType, composed :> Delegate)

                        fuse tail (QueryOp.SelectPassthrough :: opsAcc) clientPreds nextProj

            | LinqStage.Cast targetType :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
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
                match ExprTranslator.tryTranslate keyParam.Name keyLambda.Body with
                | Some keyExpr ->
                    let keyAliased = PolarsWrapper.Alias(keyExpr, "Key")
                    let lenExpr = PolarsWrapper.Len()
                    let valueAliased = PolarsWrapper.Alias(lenExpr, "Value")

                    let spec = {
                        Keys = [| keyAliased |]
                        Aggs = [| valueAliased |]
                        Having = None
                        MaintainOrder = false
                    }
                    fuse tail (QueryOp.GroupBy spec :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.AggregateBy _ :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail opsAcc clientPreds clientProjOpt

            | [] ->
                List.rev opsAcc, List.rev clientPreds, clientProjOpt, false

            | _ :: tail ->
                fuse tail opsAcc clientPreds clientProjOpt

        fuse stages [] [] None

    /// Compiles pipeline and builds the target native LazyFrameHandle alongside client predicates, projection, and fallback signal
    member private this.GetCompiledPlan() : LazyFrameHandle * (Func<'T,bool> list) * ((Type * Delegate) option) * bool =
        let ops, clientPredicates, clientProjOpt, requiresClientFallback = this.CompilePipeline()
        let nativeLf = PolarsQuery<'T>.ApplyNativeOps lfCloned ops
        nativeLf, clientPredicates, clientProjOpt, requiresClientFallback

    /// Executes the full LINQ expression pipeline in-memory via .NET EnumerableQuery LINQ-to-Objects engine
    member private this.ExecuteInMemory() : IEnumerable<'T> =
        Console.Error.WriteLine(sprintf "\x1b[1;35m[CLIENT FALLBACK EXECUTE IN-MEMORY] TargetType='%s'\x1b[0m" typeof<'T>.Name)
        
        // 1. Rewrite expression tree to eliminate F# transparent identifiers
        let effectiveExpr = FSharpAst.rewrite expression

        // 2. Extract root queryable source
        let rec findRootQueryable (e: Expression) : (Expression * Type) option =
            match e with
            | null -> None
            | :? ConstantExpression as ce when typeof<IQueryable>.IsAssignableFrom(ce.Type) || typeof<IPolarsPlanSource>.IsAssignableFrom(ce.Type) ->
                Some (ce :> Expression, ce.Type)
            | :? MethodCallExpression as mc when mc.Arguments.Count > 0 ->
                findRootQueryable mc.Arguments.[0]
            | _ -> None

        match findRootQueryable effectiveExpr with
        | Some (rootExpr, rootType) ->
            let rootConst = rootExpr :?> ConstantExpression
            let rootQueryable = rootConst.Value :?> IQueryable
            let rootElemType =
                match PolarsTypeHelper.TryGetEnumerableElementType rootType with
                | null -> rootQueryable.ElementType
                | t -> t

            // 3. Materialize the base table natively from Polars into an in-memory array once
            let toArrayMethod = LinqReflectionCache.GetEnumerableMethod("ToArray", 1, [| rootElemType |])
            let castMethod = LinqReflectionCache.GetEnumerableMethod("Cast", 1, [| rootElemType |])
            let casted = castMethod.Invoke(null, [| box rootQueryable |])
            let inMemoryArray = toArrayMethod.Invoke(null, [| casted |])

            // 4. Wrap into standard BCL EnumerableQuery (disconnects from Polars QueryProvider to prevent infinite recursion)
            let asQueryableMethod =
                typeof<Queryable>.GetMethods()
                |> Array.find (fun m -> m.Name = "AsQueryable" && m.IsGenericMethodDefinition)
                |> fun m -> m.MakeGenericMethod(rootElemType)
            let inMemoryQueryable = asQueryableMethod.Invoke(null, [| inMemoryArray |]) :?> IQueryable

            // 5. Replace root expression constant with the pure in-memory EnumerableQuery instance
            let targetQueryType = typedefof<IQueryable<_>>.MakeGenericType(rootElemType)
            let replacement = Expression.Constant(inMemoryQueryable, targetQueryType)

            let replacer =
                { new ExpressionVisitor() with
                    override this.VisitConstant(node: ConstantExpression) =
                        if not (isNull node.Value) && (obj.ReferenceEquals(node.Value, rootConst.Value) || typeof<IPolarsPlanSource>.IsAssignableFrom(node.Type)) then
                            replacement
                        else
                            base.VisitConstant(node)
                }

            let inMemoryExpr = replacer.Visit(effectiveExpr)

            // 6. Execute using standard BCL LINQ-to-Objects provider (completely bypassing Polars compilation pipeline)
            let resultQueryable = inMemoryQueryable.Provider.CreateQuery<'T>(inMemoryExpr)
            resultQueryable :> IEnumerable<'T>

        | None ->
            failwithf "Failed to locate root queryable for in-memory execution: %A" expression

    /// Evaluates the complete pipeline, executing native plan first, then materializing
    member private this.ExecuteQuery() : IEnumerable<'T> =
        printfn "\x1b[1;33m[EXECUTE QUERY ENTER] Generic Type 'T' = %s\x1b[0m" typeof<'T>.FullName
        let nativeLf, clientPredicates, clientProjOpt, requiresClientFallback = this.GetCompiledPlan()
        printfn "\x1b[1;33m[EXECUTE QUERY PLAN] Fallback=%b, Predicates=%d, HasProj=%b\x1b[0m" requiresClientFallback clientPredicates.Length clientProjOpt.IsSome

        // Branch -1: Universal In-Memory LINQ Fallback
        if requiresClientFallback then
            this.ExecuteInMemory()
        else
            let collectedDf = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto, true)
            let dfHeight = PolarsWrapper.DataFrameHeight collectedDf
            printfn "\x1b[1;32m[EXECUTE QUERY COLLECTED] Height=%d\x1b[0m" dfHeight
            
            let targetType = typeof<'T>
            let isGrouping = targetType.IsGenericType && targetType.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

            let rec hasShuffle (e: Expression) : bool =
                match e with
                | null -> false
                | MethodCall(m, null, _) when m.Name = "Shuffle" -> true
                | MethodCall(_, null, args) -> args |> List.exists hasShuffle
                | _ -> false

            let shouldShuffle = hasShuffle expression

            let applyShuffleIfNeeded (df: DataFrameHandle) : DataFrameHandle =
                if shouldShuffle then
                    let fracHandle = PolarsWrapper.SeriesNew("", [| 1.0 |])
                    PolarsWrapper.SampleFrac(df, fracHandle, false, Nullable true, Nullable())
                else
                    df

            let dfHandle = applyShuffleIfNeeded collectedDf

            let getMaterializeMethod (elemType: Type) =
                typeof<IDataFrameMaterializer>.GetMethods()
                |> Array.find (fun m -> m.Name = "Materialize" && m.IsGenericMethodDefinition)
                |> fun m -> m.MakeGenericMethod(elemType)

            // Branch 0: Client-side UDF Projection Fallback
            match clientProjOpt with
            | Some (sourceType, del) ->
                printfn "\x1b[1;32m[CLIENT PROJECTION HIT] Materializing source rows as '%s' and applying projection to '%s'\x1b[0m" sourceType.Name targetType.Name
                let mat = QueryMaterializerResolver.Resolve (Some materializer)
                let materializeMethod = getMaterializeMethod sourceType
                let rawSourceRows = materializeMethod.Invoke(mat, [| box dfHandle |]) :?> IEnumerable

                let transformedRows =
                    seq {
                        for item in rawSourceRows do
                            yield (del.DynamicInvoke([| item |]) :?> 'T)
                    }

                if clientPredicates.IsEmpty then transformedRows
                else clientPredicates |> List.fold (fun acc pred -> acc.Where pred.Invoke) transformedRows

            | None ->
                let rec findChunkSize (e: Expression) : int option =
                    match e with
                    | null -> None
                    | MethodCall(m, null, [ _; sizeExpr ]) when m.Name = "Chunk" ->
                        match tryEvaluate sizeExpr with
                        | Some s -> Some (Convert.ToInt32 s)
                        | None -> None
                    | MethodCall(_, null, args) -> args |> List.tryPick findChunkSize
                    | _ -> None

                let chunkSizeOpt = if targetType.IsArray then findChunkSize expression else None

                let rec extractLambda (expr: Expression) : LambdaExpression option =
                    match expr with
                    | null -> None
                    | :? LambdaExpression as l -> Some l
                    | Unary(ExpressionType.Quote, inner) -> extractLambda inner
                    | _ -> None

                let rec findAggregateBy (e: Expression) =
                    match e with
                    | null -> None
                    | MethodCall(m, null, source :: keyExpr :: seedExpr :: funcExpr :: rest) when m.Name = "AggregateBy" ->
                        match extractLambda keyExpr, extractLambda funcExpr with
                        | Some k, Some f ->
                            let seedLambdaOpt = extractLambda seedExpr
                            let comparerExprOpt = rest |> List.tryHead
                            Some (source, k, seedExpr, seedLambdaOpt, f, comparerExprOpt)
                        | _ -> None
                    | MethodCall(_, null, args) -> args |> List.tryPick findAggregateBy
                    | _ -> None

                let aggByInfoOpt = findAggregateBy expression

                // Case 1: GroupBy materialization (produces IEnumerable<IGrouping<TKey, TElement>>)
                if isGrouping then
                    let genericArgs = targetType.GetGenericArguments()
                    let keyType, elemType = genericArgs.[0], genericArgs.[1]

                    let mat = QueryMaterializerResolver.Resolve (Some materializer)
                    let materializeMethod = getMaterializeMethod elemType
                    let rawRows = materializeMethod.Invoke(mat, [| box dfHandle |]) :?> IEnumerable

                    let rec findKeySelector (e: Expression) : LambdaExpression option =
                        match e with
                        | null -> None
                        | MethodCall(m, null, [ _; StripQuotes (:? LambdaExpression as l) ]) when m.Name = "GroupBy" -> Some l
                        | MethodCall(_, null, args) -> args |> List.tryPick findKeySelector
                        | _ -> None

                    let rec findSortDirection (e: Expression) : bool option =
                        match e with
                        | null -> None
                        | MethodCall(m, null, _) when m.Name = "OrderBy" -> Some false
                        | MethodCall(m, null, _) when m.Name = "OrderByDescending" -> Some true
                        | MethodCall(_, null, args) -> args |> List.tryPick findSortDirection
                        | _ -> None

                    match findKeySelector expression with
                    | Some keyLambda ->
                        let compiledKeySelector = keyLambda.Compile()
                        let groupByMethod = 
                            typeof<Enumerable>.GetMethods()
                            |> Array.find (fun m -> m.Name = "GroupBy" && m.GetParameters().Length = 2 && m.GetGenericArguments().Length = 2)
                            |> fun m -> m.MakeGenericMethod(elemType, keyType)

                        let grouped = groupByMethod.Invoke(null, [| box rawRows; box compiledKeySelector |]) :?> IEnumerable<'T>

                        match findSortDirection expression with
                        | Some false ->
                            let keyProp = targetType.GetProperty "Key"
                            grouped |> Seq.sortBy (fun g -> keyProp.GetValue g :?> IComparable)
                        | Some true ->
                            let keyProp = targetType.GetProperty "Key"
                            grouped |> Seq.sortByDescending (fun g -> keyProp.GetValue g :?> IComparable)
                        | None -> grouped
                    | None ->
                        failwith "Could not locate keySelector for GroupBy pipeline."

                // Case 2: Chunk materialization (produces IEnumerable<TElem[]>)
                elif chunkSizeOpt.IsSome then
                    let chunkSize = chunkSizeOpt.Value
                    let elemType = targetType.GetElementType()

                    let mat = QueryMaterializerResolver.Resolve (Some materializer)
                    let materializeMethod = getMaterializeMethod elemType
                    let rawRows = materializeMethod.Invoke(mat, [| box dfHandle |])

                    let chunkMethod = 
                        typeof<PolarsQuery<'T>>
                            .GetMethod("ChunkSequence", BindingFlags.NonPublic ||| BindingFlags.Static)
                            .MakeGenericMethod(elemType)
                    chunkMethod.Invoke(null, [| rawRows; box chunkSize |]) :?> IEnumerable<'T>

                // Case 3: AggregateBy (produces IEnumerable<KeyValuePair<TKey, TAccum>>)
                elif aggByInfoOpt.IsSome then
                    let sourceExpr, keyLambda, seedExpr, seedLambdaOpt, funcLambda, comparerExprOpt = aggByInfoOpt.Value
                    let genericArgs = targetType.GetGenericArguments()
                    let keyType, accumType = genericArgs.[0], genericArgs.[1]

                    let rawRows = (provider :> IQueryProvider).CreateQuery(sourceExpr)
                    let sourceElemType = keyLambda.Parameters.[0].Type

                    let compiledKey = keyLambda.Compile()
                    let compiledFunc = funcLambda.Compile()

                    let comparerObj = 
                        match comparerExprOpt with
                        | Some ce -> tryEvaluate ce |> Option.toObj
                        | None -> null

                    match seedLambdaOpt with
                    | Some seedLambda ->
                        let compiledSeed = seedLambda.Compile()
                        let aggByMethod = 
                            typeof<Enumerable>.GetMethods()
                            |> Array.find (fun m -> 
                                m.Name = "AggregateBy" && 
                                m.GetParameters().Length = 5 && 
                                m.GetParameters().[2].ParameterType.IsGenericType && 
                                m.GetParameters().[2].ParameterType.GetGenericTypeDefinition() = typedefof<Func<_, _>>)
                            |> fun m -> m.MakeGenericMethod(sourceElemType, keyType, accumType)

                        aggByMethod.Invoke(null, [| box rawRows; box compiledKey; box compiledSeed; box compiledFunc; comparerObj |]) :?> IEnumerable<'T>

                    | None ->
                        let seedVal = 
                            match tryEvaluate seedExpr with
                            | Some s -> s
                            | None -> failwith "Failed to evaluate static seed for AggregateBy."

                        let aggByMethod = 
                            typeof<Enumerable>.GetMethods()
                            |> Array.find (fun m -> 
                                m.Name = "AggregateBy" && 
                                m.GetParameters().Length = 5 && 
                                m.GetParameters().[2].ParameterType.IsGenericParameter)
                            |> fun m -> m.MakeGenericMethod(sourceElemType, keyType, accumType)

                        aggByMethod.Invoke(null, [| box rawRows; box compiledKey; box seedVal; box compiledFunc; comparerObj |]) :?> IEnumerable<'T>

                // Case 4: Flat row materialization (standard records, anonymous types, DTOs)
                else
                    let height = PolarsWrapper.DataFrameHeight dfHandle
                    
                    let rec findDefaultIfEmpty (e: Expression) : (bool * obj option) option =
                        match e with
                        | null -> None
                        | MethodCall(m, null, [ _ ]) when m.Name = "DefaultIfEmpty" ->
                            Some (true, None)
                        | MethodCall(m, null, [ _; defExpr ]) when m.Name = "DefaultIfEmpty" ->
                            Some (true, tryEvaluate defExpr)
                        | MethodCall(_, null, args) -> args |> List.tryPick findDefaultIfEmpty
                        | _ -> None

                    match findDefaultIfEmpty expression with
                    | Some (true, customDefaultOpt) when height = 0L ->
                        let defaultItem =
                            match customDefaultOpt with
                            | Some v when not (isNull v) -> v :?> 'T
                            | _ -> Unchecked.defaultof<'T>
                        Seq.singleton defaultItem
                    | _ ->
                        let mat = QueryMaterializerResolver.Resolve (Some materializer)
                        let rows = mat.Materialize<'T>(dfHandle)
                        if clientPredicates.IsEmpty then rows
                        else clientPredicates |> List.fold (fun acc pred -> acc.Where pred.Invoke) rows

    /// Compiles pipeline to LazyFrameHandle. If client fallback, predicates or projections exist, materializes and transposes back.
    member this.CompileToLazyFrameHandle() : LazyFrameHandle =
        let nativeLf, clientPredicates, clientProjOpt, requiresClientFallback = this.GetCompiledPlan()
        
        let schema = PolarsWrapper.GetLazySchema nativeLf
        let width = PolarsWrapper.GetSchemaLen schema
        let targetType = typeof<'T>
        let isScalar = PolarsTypeHelper.IsScalarType targetType || targetType = typeof<string>

        let requiresClientEvaluation = 
            requiresClientFallback || not clientPredicates.IsEmpty || clientProjOpt.IsSome || (isScalar && width > 1UL)

        if not requiresClientEvaluation then
            nativeLf
        else
            let filteredRows = this.ExecuteQuery()
            let newDfHandle = DataFrameBuilder.FromRows<'T>(filteredRows)
            PolarsWrapper.DataFrameToLazy newDfHandle

    /// Compiles the query and materializes directly into a native DataFrameHandle
    member this.CompileToDataFrameHandle() : DataFrameHandle =
        let nativeLf, clientPredicates, clientProjOpt, requiresClientFallback = this.GetCompiledPlan()
        
        let schema = PolarsWrapper.GetLazySchema nativeLf
        let width = PolarsWrapper.GetSchemaLen schema
        let isScalar = PolarsTypeHelper.IsScalarType typeof<'T> || typeof<'T> = typeof<string>

        let canDirectlyCollectNative = 
            not requiresClientFallback && clientPredicates.IsEmpty && clientProjOpt.IsNone && not (isScalar && width > 1UL)

        if canDirectlyCollectNative then
            PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto, true)
        else
            let rows = this.ExecuteQuery()
            DataFrameBuilder.FromRows<'T>(rows)

    interface IInternalPlanSource with
        member this.GetPlanInfo() =
            let nativeLf, clientPreds, clientProjOpt, requiresClientFallback = this.GetCompiledPlan()
            nativeLf, (requiresClientFallback || not clientPreds.IsEmpty || clientProjOpt.IsSome)

    interface IEnumerable<'T> with
        member this.GetEnumerator() : IEnumerator<'T> =
            (this.ExecuteQuery()).GetEnumerator()

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
        member this.GetRawLazyFrameHandle() = 
            this.CompileToLazyFrameHandle()

        member this.GetCompiledLazyFrameHandle() =
            this.CompileToLazyFrameHandle()