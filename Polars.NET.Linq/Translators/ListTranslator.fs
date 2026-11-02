namespace Polars.NET.Linq

open System
open System.Reflection
open System.Linq.Expressions
open Polars.NET.Core
open Polars.NET.Core.Helpers

[<AutoOpen>]
module internal ListTranslator =

    /// Represents extracted components of list/array member access or method invocations
    type internal ListInvocation =
        | ListLen of target: Expression
        | ListContains of target: Expression * itemExpr: Expression
        | ListGet of target: Expression * indexExpr: Expression
        | ListFirst of target: Expression
        | ListLast of target: Expression
        | ListSum of target: Expression
        | ListMean of target: Expression
        | ListMin of target: Expression
        | ListMax of target: Expression
        | ListJoin of target: Expression * separatorExpr: Expression
        | ListHead of target: Expression * countExpr: Expression
        | ListTail of target: Expression * countExpr: Expression
        | ListSlice of target: Expression * offsetExpr: Expression * lengthExpr: Expression option
        | ListUnique of target: Expression
        | ListReverse of target: Expression
        | ListSort of target: Expression * descending: bool
        | ListSetIntersection of target: Expression * otherExpr: Expression
        | ListSetUnion of target: Expression * otherExpr: Expression
        | ListSetDifference of target: Expression * otherExpr: Expression
        | ListAny of target: Expression * predicateLambda: LambdaExpression option
        | ListAll of target: Expression * predicateLambda: LambdaExpression
        | ListSelect of target: Expression * mapperLambda: LambdaExpression

    [<TailCall>]
    let rec internal unwrapSpanOrArray (expr: Expression) : Expression =
        match expr with
        | null -> null
        | :? UnaryExpression as u when
            u.NodeType = ExpressionType.Convert
            || u.NodeType = ExpressionType.ConvertChecked
            ->
            unwrapSpanOrArray u.Operand
        | :? MethodCallExpression as mc when
            mc.Method.Name = "op_Implicit"
            && mc.Arguments.Count = 1
            && (mc.Type.Name.StartsWith "ReadOnlySpan`1"
                || mc.Type.Name.StartsWith "Span`1"
                || mc.Type.Name.StartsWith "ReadOnlyMemory`1"
                || mc.Type.Name.StartsWith "Memory`1")
            ->
            unwrapSpanOrArray mc.Arguments.[0]
        | :? MethodCallExpression as mc when
            (mc.Method.Name = "AsSpan" || mc.Method.Name = "AsMemory")
            && not (isNull mc.Method.DeclaringType)
            && mc.Method.DeclaringType.Name = "MemoryExtensions"
            && (not (isNull mc.Object) || mc.Arguments.Count >= 1)
            ->
            let inner = if isNull mc.Object then mc.Arguments.[0] else mc.Object
            unwrapSpanOrArray inner
        | other -> other

    let private isListContainer (t: Type) =
        t <> typeof<string>
        && not (typeof<System.Text.RegularExpressions.MatchCollection>.IsAssignableFrom t)
        && (t.IsArray || typeof<System.Collections.IEnumerable>.IsAssignableFrom t)

    /// Pattern matching indexed string split: target.Split(sep)[idx]
    let internal (|SplitIndexAccess|_|) (expr: Expression) : (Expression * string * int) option =
        match expr with
        | ArrayIndexOp(target, indexExpr) ->
            match unwrapSpanOrArray target with
            | MethodCall(m, strTarget, sepExpr :: _) when
                m.DeclaringType = typeof<string>
                && m.Name = "Split"
                && not (isNull strTarget)
                && strTarget.Type = typeof<string>
                ->
                let sepOpt =
                    match tryEvaluate sepExpr with
                    | Some(:? char as c) -> Some(string c)
                    | Some(:? string as s) -> Some s
                    | Some(:? (char array) as ca) when ca.Length = 1 -> Some(string ca.[0])
                    | Some(:? (string array) as sa) when sa.Length = 1 -> Some sa.[0]
                    | _ -> None

                let idxOpt =
                    match tryEvaluate indexExpr with
                    | Some(:? int as i) when i >= 0 -> Some i
                    | _ -> None

                match sepOpt, idxOpt with
                | Some sep, Some idx -> Some(strTarget, sep, idx)
                | _ -> None
            | _ -> None
        | _ -> None

    let internal (|ListInvocation|_|) (expr: Expression) : ListInvocation option =
        match expr with
        // 1. Array / Indexer / Property Access
        | :? UnaryExpression as u when u.NodeType = ExpressionType.ArrayLength -> Some(ListLen u.Operand)
        | ArrayIndexOp(target, indexExpr) -> Some(ListGet(target, indexExpr))

        | MemberAccess(target, m) when
            not (isNull target)
            && (target.Type.IsArray && m.Name = "Length"
                || isListContainer target.Type && m.Name = "Count")
            ->
            Some(ListLen target)

        // 2. Instance Methods (Contains, Indexer get_Item)
        | MethodCall(m, target, [ arg ]) when not (isNull target) && isListContainer target.Type ->
            match m.Name with
            | "Contains" -> Some(ListContains(target, arg))
            | "get_Item" -> Some(ListGet(target, arg))
            | _ -> None

        // 3. Special Static Methods (MemoryExtensions.Contains, string.Join)
        | MethodCall(m, null, [ spanExpr; itemExpr ]) when
            not (isNull m.DeclaringType)
            && m.DeclaringType.Name = "MemoryExtensions"
            && m.Name = "Contains"
            ->
            let target = unwrapSpanOrArray spanExpr
            if isListContainer target.Type then Some(ListContains(target, itemExpr)) else None

        | MethodCall(m, null, [ sepExpr; target ]) when
            m.DeclaringType = typeof<string>
            && m.Name = "Join"
            && isListContainer target.Type
            ->
            Some(ListJoin(target, sepExpr))

        // 4. Enumerable / Queryable Static Extension Methods (1 & 2 args)
        | MethodCall(m, null, target :: args) when
            (m.DeclaringType = typeof<System.Linq.Enumerable> || (m.DeclaringType = typeof<System.Linq.Queryable> && m.Name = "Contains"))
            && isListContainer target.Type
            ->
            match m.Name, args with
            // 1-arg operations
            | "Count", [] -> Some(ListLen target)
            | "First", [] -> Some(ListFirst target)
            | "Last", [] -> Some(ListLast target)
            | "Sum", [] -> Some(ListSum target)
            | "Average", [] -> Some(ListMean target)
            | "Min", [] -> Some(ListMin target)
            | "Max", [] -> Some(ListMax target)
            | "Distinct", [] -> Some(ListUnique target)
            | "Reverse", [] -> Some(ListReverse target)
            | "Order", [] -> Some(ListSort(target, false))
            | "OrderDescending", [] -> Some(ListSort(target, true))
            | "Any", [] -> Some(ListAny(target, None))

            // 2-arg value/collection operations
            | "Contains", [ itemExpr ] -> Some(ListContains(target, itemExpr))
            | "ElementAt", [ indexExpr ] -> Some(ListGet(target, indexExpr))
            | "Take", [ countExpr ] -> Some(ListHead(target, countExpr))
            | "TakeLast", [ countExpr ] -> Some(ListTail(target, countExpr))
            | "Skip", [ countExpr ] -> Some(ListSlice(target, countExpr, None))
            | "Intersect", [ otherExpr ] -> Some(ListSetIntersection(target, otherExpr))
            | "Union", [ otherExpr ] -> Some(ListSetUnion(target, otherExpr))
            | "Except", [ otherExpr ] -> Some(ListSetDifference(target, otherExpr))

            // 2-arg lambda operations
            | ("OrderBy" | "OrderByDescending"), [ Lambda([ p ], body) ] when body = (p :> Expression) ->
                Some(ListSort(target, m.Name = "OrderByDescending"))
            | "Any", [ :? LambdaExpression as lam ] -> Some(ListAny(target, Some lam))
            | "All", [ :? LambdaExpression as lam ] -> Some(ListAll(target, lam))
            | "Select", [ :? LambdaExpression as lam ] -> Some(ListSelect(target, lam))
            | _ -> None

        | _ -> None

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

