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
    InvertSides: bool
}

/// Specification for native GroupBy aggregation pushdown
type internal GroupBySpec = {
    Keys: ExprHandle array
    Aggs: ExprHandle array
    Having: ExprHandle option
    MaintainOrder: bool
}

type internal UniqueSpec = {
    SubsetCols: string array option
    Keep: PlUniqueKeepStrategy
    MaintainOrder: bool
}

/// Specification for vertical concatenation pushdown
type internal ConcatSpec = {
    OtherLf: LazyFrameHandle
}

/// Uniform contract to expose internal LazyFrameHandle without leaking generic parameters
type internal IPolarsPlanSource =
    abstract member GetRawLazyFrameHandle: unit -> LazyFrameHandle

/// Abstract representation of pushdown operations in the LINQ execution pipeline
[<RequireQualifiedAccess>]
type internal QueryOp =
    | Filter of ExprHandle
    | Slice of offset: int64 * length: uint32
    | Sort of SortSpec list
    | Join of JoinSpec
    | GroupBy of GroupBySpec
    | Unique of UniqueSpec
    | Concat of ConcatSpec
    | SelectPassthrough

/// Linear representation of query expressions before optimization/fusion
[<RequireQualifiedAccess>]
type internal LinqStage =
    | Filter of LambdaExpression
    | Sort of LambdaExpression * isDescending: bool
    | Take of uint32
    | Skip of uint32
    | Distinct
    | DistinctBy of LambdaExpression
    | Concat of Expression
    | Union of Expression * keyLambdaOpt: LambdaExpression option
    | Join of MethodInfo * Expression * LambdaExpression * LambdaExpression * LambdaExpression
    | SetOp of MethodInfo * Expression * LambdaExpression option * LambdaExpression option
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

    /// Uniformly extracts a LazyFrameHandle from an Expression without triggering full recursive pipeline execution
    static member internal ResolveToLazyFrameHandle(expr: Expression) : LazyFrameHandle option =
        let rec unwrap (e: Expression) =
            match e with
            | Unary(ExpressionType.Convert, inner)
            | Unary(ExpressionType.ConvertChecked, inner)
            | Unary(ExpressionType.Quote, inner) -> unwrap inner
            | other -> other

        let cleanExpr = unwrap expr

        let evalOpt =
            match tryEvaluate cleanExpr with
            | Some v -> Some v
            | None ->
                match cleanExpr with
                | :? ConstantExpression as ce -> Some ce.Value
                | other ->
                    try
                        let lambda = Expression.Lambda<Func<obj>>(Expression.Convert(other, typeof<obj>)).Compile()
                        Some (lambda.Invoke())
                    with _ -> None

        match evalOpt with
        // Case 1: PolarsQuery instance (raw plan)
        | Some (:? IPolarsPlanSource as plan) ->
            Some (plan.GetRawLazyFrameHandle())

        // Case 2: Queryable backed by Polars
        | Some value when not (isNull value) && typeof<IQueryable>.IsAssignableFrom(value.GetType()) ->
            let qType = value.GetType()
            let compileMethod = qType.GetMethod("CompileToLazyFrameHandle", BindingFlags.Public ||| BindingFlags.Instance)
            if not (isNull compileMethod) then
                Some (compileMethod.Invoke(value, null) :?> LazyFrameHandle)
            else
                let handleProp = qType.GetProperty("Handle", BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
                if not (isNull handleProp) then
                    match handleProp.GetValue(value) with
                    | :? LazyFrameHandle as lf -> Some (PolarsWrapper.LazyClone lf)
                    | :? DataFrameHandle as df -> Some (PolarsWrapper.DataFrameToLazy df)
                    | _ -> None
                else None

        // Case 3: C# DataFrame or LazyFrame instance
        | Some value when not (isNull value) ->
            let valueType = value.GetType()
            let handleProp = valueType.GetProperty("Handle", BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
            if not (isNull handleProp) then
                match handleProp.GetValue(value) with
                | :? DataFrameHandle as dfHandle -> Some (PolarsWrapper.DataFrameToLazy dfHandle)
                | :? LazyFrameHandle as lfHandle -> Some (PolarsWrapper.LazyClone lfHandle)
                | _ -> None
            else
                // Case 4: In-memory sequence (List<T>, Array, etc.) -> Transpose via DataFrameBuilder.FromRows<T>
                match PolarsTypeHelper.TryGetEnumerableElementType valueType with
                | null -> None
                | elemType ->
                    let fromRows = typeof<DataFrameBuilder>.GetMethod("FromRows", BindingFlags.Public ||| BindingFlags.Static).MakeGenericMethod(elemType)
                    let dfHandle = fromRows.Invoke(null, [| value |]) :?> DataFrameHandle
                    Some (PolarsWrapper.DataFrameToLazy dfHandle)

        | _ -> None

    /// Helper to iterate and extract all column names from a LazyFrame schema
    static member private GetLazyColumnNames(lf: LazyFrameHandle) : string array =
        let schema = PolarsWrapper.GetLazySchema lf
        let count = PolarsWrapper.GetSchemaLen schema
        Array.init (int count) (fun i ->
            let mutable name = null
            let mutable dtHandle = new DataTypeHandle()
            PolarsWrapper.GetSchemaFieldAt(schema, uint64 i, &name, &dtHandle)
            name
        )

    /// Extracts column names from a DistinctBy keySelector lambda
    static member private ExtractDistinctColumns (keyLambda: LambdaExpression) : string array option =
        let paramName = keyLambda.Parameters.[0].Name
        match keyLambda.Body with
        | MemberAccess(p, m) when not (isNull p) && p.NodeType = ExpressionType.Parameter ->
            Some [| m.Name |]
        | :? NewExpression as newExpr ->
            let names = PolarsQuery<'T>.ExtractPascalCaseMemberNames newExpr
            Some (List.toArray names)
        | _ ->
            // Try extracting column name via translator if possible
            match ExprTranslator.tryTranslate paramName keyLambda.Body with
            | Some exprHandle ->
                // If it evaluates to a named column expression, extract its root
                None
            | None -> None

    /// Applies a list of QueryOp to a target LazyFrameHandle in forward order.
    static member private ApplyNativeOps (sourceLf: LazyFrameHandle) (ops: QueryOp list) : LazyFrameHandle =
        ops
        |> List.fold (fun currentLf op ->
            match op with
            | QueryOp.Filter exprHandle -> 
                PolarsWrapper.LazyFilter(currentLf, exprHandle)

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
                let handles = [| currentLf; spec.OtherLf |]
                PolarsWrapper.LazyConcat(handles, PlConcatType.Vertical, false, true)

            | QueryOp.SelectPassthrough -> 
                currentLf
        ) (PolarsWrapper.LazyClone sourceLf)

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

    /// Common helper to compile GroupBy projections
    static member private BuildGroupBySpec (keyLambda: LambdaExpression) (bodyExpr: Expression) (isKeyAccessIgnored: Expression -> bool) (groupParamName: string) : GroupBySpec option =
        let keyParam = keyLambda.Parameters.[0]
        match ExprTranslator.tryTranslate keyParam.Name keyLambda.Body with
        | None -> None
        | Some keyExpr ->
            match bodyExpr with
            | :? NewExpression as newExpr ->
                let args = newExpr.Arguments |> Seq.toList
                let memberNames = PolarsQuery<'T>.ExtractPascalCaseMemberNames newExpr
                let aggs =
                    List.zip memberNames args
                    |> List.choose (fun (colName, argExpr) ->
                        if isKeyAccessIgnored argExpr then None
                        else AggTranslator.tryTranslateAgg groupParamName argExpr colName
                    )
                Some {
                    Keys = [| keyExpr |]
                    Aggs = aggs |> List.toArray
                    Having = None
                    MaintainOrder = false
                }
            | _ -> None

    static member private CompileGroupByWithResult (keyLambda: LambdaExpression) (resLambda: LambdaExpression) : GroupBySpec option =
        let resKeyParam = resLambda.Parameters.[0]
        let isKeyAccess (e: Expression) =
            match e with
            | :? ParameterExpression as p when p.Name = resKeyParam.Name -> true
            | MemberAccess(p, _) when not (isNull p) && p.NodeType = ExpressionType.Parameter -> true
            | _ -> false
        PolarsQuery<'T>.BuildGroupBySpec keyLambda resLambda.Body isKeyAccess resLambda.Parameters.[1].Name

    static member private CompileGroupByAndProjection (keyLambda: LambdaExpression) (projLambda: LambdaExpression) : GroupBySpec option =
        let isKeyAccess (e: Expression) =
            match e with
            | MemberAccess(p, m) when not (isNull p) && p.NodeType = ExpressionType.Parameter && m.Name = "Key" -> true
            | _ -> false
        PolarsQuery<'T>.BuildGroupBySpec keyLambda projLambda.Body isKeyAccess projLambda.Parameters.[0].Name

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

    /// Parses the LINQ expression tree into native ops and any trailing client predicates
    member private this.CompilePipeline() : QueryOp list * (Func<'T, bool> list) =
        let rec flatten (e: Expression) (acc: LinqStage list) : LinqStage list =
            match e with
            | null -> acc

            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as l) ]) when m.Name = "Where" ->
                flatten source (LinqStage.Filter l :: acc)

            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as l) ]) when m.Name = "OrderBy" || m.Name = "ThenBy" ->
                flatten source (LinqStage.Sort(l, isDescending = false) :: acc)

            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as l) ]) when m.Name = "OrderByDescending" || m.Name = "ThenByDescending" ->
                flatten source (LinqStage.Sort(l, isDescending = true) :: acc)

            // 3. Take / Skip
            | MethodCall(m, null, [ source; countExpr ]) when m.Name = "Take" ->
                match tryEvaluate countExpr with
                | Some c -> flatten source (LinqStage.Take(Convert.ToUInt32 c) :: acc)
                | None -> failwithf "Could not evaluate Take count argument: %A" countExpr

            | MethodCall(m, null, [ source; countExpr ]) when m.Name = "Skip" ->
                match tryEvaluate countExpr with
                | Some c -> flatten source (LinqStage.Skip(Convert.ToUInt32 c) :: acc)
                | None -> failwithf "Could not evaluate Skip count argument: %A" countExpr
            
            // Distinct
            | MethodCall(m, null, [ source ]) when m.Name = "Distinct" ->
                flatten source (LinqStage.Distinct :: acc)

            // DistinctBy
            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as keySel) ]) when m.Name = "DistinctBy" ->
                flatten source (LinqStage.DistinctBy keySel :: acc)

            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as k); StripQuotes (:? LambdaExpression as r) ]) when m.Name = "GroupBy" ->
                flatten source (LinqStage.GroupByWithResult(k, r) :: acc)

            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as k) ]) when m.Name = "GroupBy" ->
                flatten source (LinqStage.GroupByKey k :: acc)

            | MethodCall(m, null, [ source; StripQuotes (:? LambdaExpression as l) ]) when m.Name = "Select" ->
                flatten source (LinqStage.Project l :: acc)

            | MethodCall(m, null, [ outer; inner; 
                                   StripQuotes (:? LambdaExpression as outerKey); 
                                   StripQuotes (:? LambdaExpression as innerKey); 
                                   StripQuotes (:? LambdaExpression as resSel) ]) 
                when m.Name = "Join" || m.Name = "LeftJoin" || m.Name = "RightJoin" ->
                flatten outer (LinqStage.Join(m, inner, outerKey, innerKey, resSel) :: acc)

            | MethodCall(m, null, [ first; second ]) 
                when m.Name = "Intersect" || m.Name = "Except" ->
                flatten first (LinqStage.SetOp(m, second, None, None) :: acc)

            | MethodCall(m, null, [ first; second; StripQuotes (:? LambdaExpression as keySel) ]) 
                when m.Name = "IntersectBy" || m.Name = "ExceptBy" ->
                flatten first (LinqStage.SetOp(m, second, Some keySel, None) :: acc)

            | MethodCall(m, null, [ first; second; StripQuotes (:? LambdaExpression as k1); StripQuotes (:? LambdaExpression as k2) ]) 
                when m.Name = "IntersectBy" || m.Name = "ExceptBy" ->
                flatten first (LinqStage.SetOp(m, second, Some k1, Some k2) :: acc)
            
            // Concat
            | MethodCall(m, null, [ first; second ]) when m.Name = "Concat" ->
                flatten first (LinqStage.Concat second :: acc)

            // Union (Full row distinct)
            | MethodCall(m, null, [ first; second ]) when m.Name = "Union" ->
                flatten first (LinqStage.Union(second, None) :: acc)

            // UnionBy (Distinct by key)
            | MethodCall(m, null, [ first; second; StripQuotes (:? LambdaExpression as keySel) ]) when m.Name = "UnionBy" ->
                flatten first (LinqStage.Union(second, Some keySel) :: acc)

            | _ -> acc

        let stages = flatten expression []

        let addSort (spec: SortSpec) (ops: QueryOp list) =
            match ops with
            | QueryOp.Sort specs :: tail -> QueryOp.Sort (specs @ [spec]) :: tail
            | _ -> QueryOp.Sort [spec] :: ops

        let rec fuse (remaining: LinqStage list) (opsAcc: QueryOp list) (clientPreds: Func<'T, bool> list) =
            match remaining with
            | LinqStage.GroupByKey keyLambda :: LinqStage.Project projLambda :: tail when clientPreds.IsEmpty ->
                match PolarsQuery<'T>.CompileGroupByAndProjection keyLambda projLambda with
                | Some spec -> fuse tail (QueryOp.GroupBy spec :: opsAcc) clientPreds
                | None -> failwith "Failed to compile GroupBy -> Select projection pushdown."

            | LinqStage.GroupByWithResult(keyLambda, resLambda) :: tail when clientPreds.IsEmpty ->
                match PolarsQuery<'T>.CompileGroupByWithResult keyLambda resLambda with
                | Some spec -> fuse tail (QueryOp.GroupBy spec :: opsAcc) clientPreds
                | None -> failwith "Failed to compile GroupBy with result selector pushdown."

            | LinqStage.Filter l :: tail ->
                match ExprTranslator.tryTranslate l.Parameters.[0].Name l.Body with
                | Some exprHandle when clientPreds.IsEmpty ->
                    fuse tail (QueryOp.Filter exprHandle :: opsAcc) clientPreds
                | _ ->
                    let compiled = l.Compile() :?> Func<'T, bool>
                    fuse tail opsAcc (compiled :: clientPreds)

            | LinqStage.Sort(l, isDesc) :: tail ->
                let param = l.Parameters.[0]
                let isGrouping = param.Type.IsGenericType && param.Type.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>
                if not isGrouping && clientPreds.IsEmpty then
                    match ExprTranslator.tryTranslate param.Name l.Body with
                    | Some exprHandle ->
                        let spec = { Expr = exprHandle; Descending = isDesc; NullsLast = false }
                        fuse tail (addSort spec opsAcc) clientPreds
                    | None -> fuse tail opsAcc clientPreds
                else
                    fuse tail opsAcc clientPreds

            | LinqStage.Join(m, inner, outerKey, innerKey, _) :: tail when clientPreds.IsEmpty ->
                match PolarsQuery<'T>.CompileJoin m inner outerKey innerKey with
                | Some spec -> fuse tail (QueryOp.Join spec :: opsAcc) clientPreds
                | None -> failwithf "Failed to push down %s." m.Name

            | LinqStage.SetOp(m, second, k1Opt, k2Opt) :: tail when clientPreds.IsEmpty ->
                match PolarsQuery<'T>.CompileSetOp m second k1Opt k2Opt lfCloned with
                | Some spec -> fuse tail (QueryOp.Join spec :: opsAcc) clientPreds
                | None -> failwithf "Failed to push down SetOp %s." m.Name

            // Continuous Skip followed by Take fusion: Skip(s).Take(t) -> Slice(s, t)
            | LinqStage.Skip s :: LinqStage.Take t :: tail when clientPreds.IsEmpty ->
                fuse tail (QueryOp.Slice(int64 s, t) :: opsAcc) clientPreds

            | LinqStage.Take count :: tail when clientPreds.IsEmpty ->
                fuse tail (QueryOp.Slice(0L, count) :: opsAcc) clientPreds

            | LinqStage.Skip count :: tail when clientPreds.IsEmpty ->
                fuse tail (QueryOp.Slice(int64 count, UInt32.MaxValue) :: opsAcc) clientPreds

            | LinqStage.Distinct :: tail when clientPreds.IsEmpty ->
                let spec = { SubsetCols = None; Keep = PlUniqueKeepStrategy.First; MaintainOrder = false }
                fuse tail (QueryOp.Unique spec :: opsAcc) clientPreds

            | LinqStage.DistinctBy keyLambda :: tail when clientPreds.IsEmpty ->
                match PolarsQuery<'T>.ExtractDistinctColumns keyLambda with
                | Some cols ->
                    let spec = { SubsetCols = Some cols; Keep = PlUniqueKeepStrategy.First; MaintainOrder = false }
                    fuse tail (QueryOp.Unique spec :: opsAcc) clientPreds
                | None ->
                    failwithf "Could not translate DistinctBy key selector: %A" keyLambda

            // Rule: Concat
            | LinqStage.Concat secondExpr :: tail when clientPreds.IsEmpty ->
                match PolarsQuery<'T>.ResolveToLazyFrameHandle secondExpr with
                | Some otherLf ->
                    let spec = { OtherLf = otherLf }
                    fuse tail (QueryOp.Concat spec :: opsAcc) clientPreds
                | None -> failwith "Failed to resolve second expression for Concat."

            // Rule: Union / UnionBy (Concat -> Unique fusion)
            | LinqStage.Union(secondExpr, keyLambdaOpt) :: tail when clientPreds.IsEmpty ->
                match PolarsQuery<'T>.ResolveToLazyFrameHandle secondExpr with
                | Some otherLf ->
                    let concatOp = QueryOp.Concat { OtherLf = otherLf }
                    let uniqueSpec =
                        match keyLambdaOpt with
                        | Some keyLambda ->
                            match PolarsQuery<'T>.ExtractDistinctColumns keyLambda with
                            | Some cols -> { SubsetCols = Some cols; Keep = PlUniqueKeepStrategy.First; MaintainOrder = false }
                            | None -> failwithf "Could not translate UnionBy key selector: %A" keyLambda
                        | None ->
                            { SubsetCols = None; Keep = PlUniqueKeepStrategy.First; MaintainOrder = false }

                    let uniqueOp = QueryOp.Unique uniqueSpec
                    fuse tail (uniqueOp :: concatOp :: opsAcc) clientPreds
                | None -> failwith "Failed to resolve second expression for Union."

            | LinqStage.GroupByKey _ :: tail ->
                fuse tail opsAcc clientPreds

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
            let genericArgs = targetType.GetGenericArguments()
            let keyType = genericArgs.[0]
            let elemType = genericArgs.[1]

            let ops, _ = this.CompilePipeline()
            let nativeLf = PolarsQuery<'T>.ApplyNativeOps lfCloned ops
            let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto, true)

            let mat = PolarsQuery<'T>.ResolveMaterializer materializer
            let materializeMethod = mat.GetType().GetMethod("Materialize").MakeGenericMethod(elemType)
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
                    let keyProp = targetType.GetProperty("Key")
                    grouped |> Seq.sortBy (fun g -> keyProp.GetValue(g) :?> IComparable)
                | Some true ->
                    let keyProp = targetType.GetProperty("Key")
                    grouped |> Seq.sortByDescending (fun g -> keyProp.GetValue(g) :?> IComparable)
                | None -> grouped
            | None ->
                failwith "Could not locate keySelector for GroupBy pipeline."

        else
            let ops, clientPredicates = this.CompilePipeline()
            let mat = PolarsQuery<'T>.ResolveMaterializer materializer
            let nativeLf = PolarsQuery<'T>.ApplyNativeOps lfCloned ops
            let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto, true)
            let rows = mat.Materialize<'T>(dfHandle)

            if clientPredicates.IsEmpty then rows
            else clientPredicates |> List.fold (fun acc pred -> acc.Where pred.Invoke) rows

    /// Compiles pipeline to LazyFrameHandle. If client predicates exist, materializes and transposes back.
    member this.CompileToLazyFrameHandle() : LazyFrameHandle =
        let ops, clientPredicates = this.CompilePipeline()
        if clientPredicates.IsEmpty then
            PolarsQuery<'T>.ApplyNativeOps lfCloned ops
        else
            let filteredRows = this.ExecuteQuery()
            let newDfHandle = DataFrameBuilder.FromRows<'T>(filteredRows)
            PolarsWrapper.DataFrameToLazy newDfHandle

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
        member this.GetRawLazyFrameHandle() = PolarsWrapper.LazyClone lfCloned