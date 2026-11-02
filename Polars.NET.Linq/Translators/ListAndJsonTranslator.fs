namespace Polars.NET.Linq

open Polars.NET.Core
open System
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core.Helpers
open System.Text.Json
open System.Text.Json.Nodes

[<AutoOpen>]
module internal ListAndJsonTranslator =

    // =========================================================================

    let internal tryCreateSeriesHandleFromObject (objVal: obj) : SeriesHandle option =
        match objVal with
        | null -> None
        | :? Array as arr ->
            try
                let h = SeriesFactory.Create("", arr)
                if not (isNull h || h.IsInvalid) then Some h else None
            with _ ->
                None
        | :? System.Collections.IEnumerable as seq when seq.GetType() <> typeof<string> ->
            try
                let elemType =
                    match PolarsTypeHelper.TryGetEnumerableElementType (seq.GetType()) with
                    | null -> typeof<obj>
                    | t -> t

                let createGeneric =
                    typeof<SeriesFactory>
                        .GetMethod("CreateGenericType", BindingFlags.Public ||| BindingFlags.Static)
                        .MakeGenericMethod(elemType)

                let h = createGeneric.Invoke(null, [| box ""; seq |]) :?> SeriesHandle
                if not (isNull h || h.IsInvalid) then Some h else None
            with _ ->
                None
        | _ -> None

    let internal translateListInvocation (eval: ExprEvaluator) (ctx: TranslationContext) (invocation: ListInvocation) : ExprHandle option =
        let t1 expr = eval ctx expr

        let t2 e1 e2 =
            match eval ctx e1, eval ctx e2 with
            | Some l, Some r -> Some(l, r)
            | _ -> None

        match invocation with
        | ListLen target -> t1 target |> Option.map PolarsWrapper.ListLen
        | ListContains(target, itemExpr) ->
            match t2 target itemExpr with
            | Some(t, item) -> Some(PolarsWrapper.ListContains(t, item, nullsEqual = false))
            | None ->
                // Check if target is an in-memory collection (IN predicate: item IN target)
                option {
                    let! itemH = t1 itemExpr
                    let unwrapped = unwrapSpanOrArray target
                    let! evaluated = tryEvaluate unwrapped

                    match evaluated with
                    | :? System.Collections.IEnumerable as coll when coll.GetType() <> typeof<string> ->
                        let! seriesHandle = tryCreateSeriesHandleFromObject coll
                        let litH = PolarsWrapper.Lit seriesHandle
                        let implodedH = PolarsWrapper.Implode(litH, maintainOrder = false)
                        return PolarsWrapper.IsIn(itemH, implodedH, nullsEqual = false)
                    | _ -> return! None
                }
        | ListGet(target, indexExpr) ->
            t2 target indexExpr
            |> Option.map (fun (t, idx) -> PolarsWrapper.ListGet(t, idx, nullOnOob = true))
        | ListFirst target ->
            t1 target
            |> Option.map (fun t -> PolarsWrapper.ListGet(t, PolarsWrapper.Lit 0, nullOnOob = true))
        | ListLast target ->
            t1 target
            |> Option.map (fun t -> PolarsWrapper.ListGet(t, PolarsWrapper.Lit -1, nullOnOob = true))
        | ListSum target -> t1 target |> Option.map PolarsWrapper.ListSum
        | ListMean target -> t1 target |> Option.map PolarsWrapper.ListMean
        | ListMin target -> t1 target |> Option.map PolarsWrapper.ListMin
        | ListMax target -> t1 target |> Option.map PolarsWrapper.ListMax

        | ListJoin(target, separatorExpr) ->
            let sepOpt =
                match tryEvaluate separatorExpr with
                | Some(:? string as sep) -> Some sep
                | Some(:? char as sep) -> Some(string sep)
                | _ -> None

            match unwrap target with
            | :? NewArrayExpression as nae when nae.NodeType = ExpressionType.NewArrayInit ->
                match sepOpt with
                | Some sep ->
                    let items = nae.Expressions |> Seq.toList
                    let itemHandles = items |> List.choose (eval ctx)
                    if itemHandles.Length = items.Length && not itemHandles.IsEmpty then
                        Some(PolarsWrapper.ConcatString(List.toArray itemHandles, sep, ignoreNulls = false))
                    else
                        None
                | None -> None
            | _ ->
                match t1 target, sepOpt with
                | Some t, Some sep -> Some(PolarsWrapper.ListJoin(t, sep, ignoreNulls = true))
                | _ -> None

        | ListHead(target, countExpr) -> t2 target countExpr |> Option.map PolarsWrapper.ListHead
        | ListTail(target, countExpr) -> t2 target countExpr |> Option.map PolarsWrapper.ListTail

        | ListSlice(target, offsetExpr, lengthOpt) ->
            match t2 target offsetExpr with
            | Some(t, offset) ->
                match lengthOpt with
                | Some lenExpr -> t1 lenExpr |> Option.map (fun len -> PolarsWrapper.ListSlice(t, offset, len))
                | None -> Some(PolarsWrapper.ListSlice(t, offset, PolarsWrapper.Lit Int64.MaxValue))
            | None -> None

        | ListUnique target ->
            t1 target
            |> Option.map (fun t -> PolarsWrapper.ListEval(t, PolarsWrapper.ExprUnique(PolarsWrapper.Col(""))))
        | ListReverse target ->
            t1 target
            |> Option.map (fun t -> PolarsWrapper.ListEval(t, PolarsWrapper.Reverse(PolarsWrapper.Col(""))))
        | ListSort(target, descending) ->
            t1 target
            |> Option.map (fun t ->
                PolarsWrapper.ListSort(t, descending = descending, nullsLast = false, maintainOrder = false))

        | ListSetIntersection(target, otherExpr) -> t2 target otherExpr |> Option.map PolarsWrapper.ListSetIntersection
        | ListSetUnion(target, otherExpr) -> t2 target otherExpr |> Option.map PolarsWrapper.ListSetUnion
        | ListSetDifference(target, otherExpr) -> t2 target otherExpr |> Option.map PolarsWrapper.ListSetDifference

        | ListAny(target, None) ->
            t1 target
            |> Option.map (fun t -> PolarsWrapper.Gt(PolarsWrapper.ListLen t, PolarsWrapper.Lit 0))

        | ListAny(target, Some lam) when lam.Parameters.Count = 1 ->
            match t1 target with
            | Some t ->
                let innerCtx = TranslationContext.ListElement lam.Parameters.[0].Name

                eval innerCtx lam.Body
                |> Option.map (fun innerCond ->
                    let anyExpr = PolarsWrapper.Any(innerCond, ignoreNulls = false)
                    let boolList = PolarsWrapper.ListEval(t, anyExpr)
                    PolarsWrapper.ListGet(boolList, PolarsWrapper.Lit 0, nullOnOob = true))
            | None -> None

        | ListAny _ -> None

        | ListAll(target, lam) when lam.Parameters.Count = 1 ->
            match t1 target with
            | Some t ->
                let innerCtx = TranslationContext.ListElement lam.Parameters.[0].Name

                eval innerCtx lam.Body
                |> Option.map (fun innerCond ->
                    let allExpr = PolarsWrapper.All(innerCond, ignoreNulls = false)
                    let boolList = PolarsWrapper.ListEval(t, allExpr)
                    PolarsWrapper.ListGet(boolList, PolarsWrapper.Lit 0, nullOnOob = true))
            | None -> None
        | ListAll _ -> None

        | ListSelect(target, lam) when lam.Parameters.Count = 1 ->
            match t1 target with
            | Some t ->
                let innerCtx = TranslationContext.ListElement lam.Parameters.[0].Name

                eval innerCtx lam.Body
                |> Option.map (fun mapperExpr -> PolarsWrapper.ListEval(t, mapperExpr))
            | None -> None
        | ListSelect _ -> None

    // =========================================================================
    // 8b. JSON Operations (System.Text.Json)
    // =========================================================================

    let internal translateJsonInvocation (eval: ExprEvaluator) (ctx: TranslationContext) (invocation: JsonInvocation) : ExprHandle option =
        match invocation with
        | JsonPathMatch(target, path, targetType) ->
            eval ctx target
            |> Option.bind (fun t ->
                let pathH = PolarsWrapper.Lit path
                let matchH = PolarsWrapper.StrJsonPathMatch(t, pathH)
                if targetType = typeof<string> || targetType = typeof<obj> then
                    Some matchH
                elif targetType = typeof<bool> || targetType = typeof<Nullable<bool>> then
                    let lowerH = PolarsWrapper.StrToLower matchH
                    Some(PolarsWrapper.Eq(lowerH, PolarsWrapper.Lit "true"))
                else
                    tryTranslateCast targetType matchH)

        | JsonSerialize target ->
            let unwrappedTarget = unwrap target
            match unwrappedTarget with
            | :? NewExpression as ne ->
                let memberNames =
                    if not (isNull ne.Members) && ne.Members.Count = ne.Arguments.Count then
                        ne.Members |> Seq.map (fun m -> m.Name) |> Seq.toArray
                    elif not (isNull ne.Constructor) then
                        let ctorParams = ne.Constructor.GetParameters()
                        ctorParams |> Array.map (fun p -> p.Name)
                    else
                        [||]

                let argHandles =
                    ne.Arguments
                    |> Seq.mapi (fun i argExpr ->
                        match eval ctx argExpr with
                        | Some h ->
                            let fieldName = if i < memberNames.Length then memberNames.[i] else sprintf "field_%d" i
                            Some(PolarsWrapper.Alias(h, fieldName))
                        | None -> None)
                    |> Seq.toList

                if argHandles |> List.forall Option.isSome then
                    let handlesArray = argHandles |> List.map Option.get |> List.toArray
                    let structH = PolarsWrapper.AsStruct handlesArray
                    Some(PolarsWrapper.StructJsonEncode structH)
                else
                    None

            | :? MemberInitExpression as mie ->
                let bindings =
                    mie.Bindings
                    |> Seq.choose (fun b ->
                        match b with
                        | :? MemberAssignment as ma ->
                            match eval ctx ma.Expression with
                            | Some h -> Some(PolarsWrapper.Alias(h, ma.Member.Name))
                            | None -> None
                        | _ -> None)
                    |> Seq.toArray
                if bindings.Length = mie.Bindings.Count then
                    let structH = PolarsWrapper.AsStruct bindings
                    Some(PolarsWrapper.StructJsonEncode structH)
                else
                    None

            | other ->
                eval ctx other
                |> Option.map (fun h -> PolarsWrapper.StructJsonEncode h)

        | JsonDeserialize(target, dtoType, steps) ->
            match JsonTypeInference.inferPolarsJsonDataType dtoType with
            | None -> None
            | Some dtypeHandle ->
                use _dtypeScope = dtypeHandle
                let dtypeExpr = PolarsWrapper.DataTypeExprFromDataType dtypeHandle

                match eval ctx target with
                | None ->
                    dtypeExpr.Dispose()
                    None
                | Some targetH ->
                    let decodedH = PolarsWrapper.StrJsonDecode(targetH, dtypeExpr)

                    let rec traverseSteps (currentH: ExprHandle) (currType: Type) (remainingSteps: JsonAccessStep list) : ExprHandle option =
                        match remainingSteps with
                        | [] -> Some currentH

                        | PropertyStep m :: rest when
                            (m.Name = "Length" || m.Name = "Count")
                            && not (isNull (PolarsTypeHelper.TryGetEnumerableElementType currType)) ->
                            let lenH = PolarsWrapper.ListLen currentH
                            traverseSteps lenH typeof<int> rest

                        | PropertyStep m :: rest ->
                            let memberType =
                                match m with
                                | :? PropertyInfo as p -> p.PropertyType
                                | :? FieldInfo as f -> f.FieldType
                                | _ -> typeof<obj>

                            match JsonTypeInference.tryResolveJsonFieldName currType m.Name with
                            | Some jsonFieldName ->
                                let fieldH = PolarsWrapper.StructFieldByName(currentH, [| jsonFieldName |])
                                traverseSteps fieldH memberType rest
                            | None ->
                                let fieldH = PolarsWrapper.StructFieldByName(currentH, [| m.Name |])
                                traverseSteps fieldH memberType rest

                        | IndexStep indexExpr :: rest ->
                            let elemType =
                                let elemOpt = PolarsTypeHelper.TryGetEnumerableElementType currType
                                if not (isNull elemOpt) then elemOpt
                                elif currType.IsArray then currType.GetElementType()
                                elif currType.IsGenericType then currType.GetGenericArguments().[0]
                                else typeof<obj>

                            match eval ctx indexExpr with
                            | Some idxH ->
                                let itemH = PolarsWrapper.ListGet(currentH, idxH, nullOnOob = true)
                                traverseSteps itemH elemType rest
                            | None -> None

                    traverseSteps decodedH dtoType steps

    // =========================================================================
