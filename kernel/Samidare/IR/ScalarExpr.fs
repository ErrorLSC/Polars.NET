namespace Kaminari.Samidare.IR

open System
open System.ComponentModel
open Kaminari.Sazanami.Arrow
open Kaminari.Sazanami.Helpers


/// SRTP carrier mechanism for statically resolved scalar literal construction
[<EditorBrowsable(EditorBrowsableState.Never)>]
type ScalarLitMechanism = ScalarLitMechanism
    with
        static member ($)(ScalarLitMechanism, v: bool) = ScalarExpr.Literal(LiteralValue.Bool v)
        static member ($)(ScalarLitMechanism, v: int) = ScalarExpr.Literal(LiteralValue.Int32 v)
        static member ($)(ScalarLitMechanism, v: int64) = ScalarExpr.Literal(LiteralValue.Int64 v)
        static member ($)(ScalarLitMechanism, v: sbyte) = ScalarExpr.Literal(LiteralValue.Int8 v)
        static member ($)(ScalarLitMechanism, v: int16) = ScalarExpr.Literal(LiteralValue.Int16 v)
        static member ($)(ScalarLitMechanism, v: byte) = ScalarExpr.Literal(LiteralValue.UInt8 v)
        static member ($)(ScalarLitMechanism, v: uint16) = ScalarExpr.Literal(LiteralValue.UInt16 v)
        static member ($)(ScalarLitMechanism, v: uint32) = ScalarExpr.Literal(LiteralValue.UInt32 v)
        static member ($)(ScalarLitMechanism, v: uint64) = ScalarExpr.Literal(LiteralValue.UInt64 v)
        static member ($)(ScalarLitMechanism, v: float32) = ScalarExpr.Literal(LiteralValue.Float32 v)
        static member ($)(ScalarLitMechanism, v: double) = ScalarExpr.Literal(LiteralValue.Float64 v)
        static member ($)(ScalarLitMechanism, v: decimal) = ScalarExpr.Literal(LiteralValue.Decimal v)
        static member ($)(ScalarLitMechanism, v: string) = ScalarExpr.Literal(LiteralValue.String v)
        static member ($)(ScalarLitMechanism, v: DateOnly) = ScalarExpr.Literal(LiteralValue.Date v)
        static member ($)(ScalarLitMechanism, v: DateTime) = ScalarExpr.Literal(LiteralValue.DateTime v)
        static member ($)(ScalarLitMechanism, v: DateTimeOffset) = ScalarExpr.Literal(LiteralValue.DateTimeOffset v)
        static member ($)(ScalarLitMechanism, v: TimeSpan) = ScalarExpr.Literal(LiteralValue.TimeSpan v)
        static member ($)(ScalarLitMechanism, v: TimeOnly) = ScalarExpr.Literal(LiteralValue.TimeOnly v)
        static member ($)(ScalarLitMechanism, v: LiteralValue) = ScalarExpr.Literal v

/// Construction helpers and visitors for ScalarExpr
[<AutoOpen>]
[<EditorBrowsable(EditorBrowsableState.Never)>]
module ScalarExprOps =

    // =========================================================================
    // 1. Unified Constructors & Fluent Helpers
    // =========================================================================

    /// Column reference helper
    let col (name: string) : ScalarExpr = ScalarExpr.Column name

    /// Strongly-typed, zero-boxing overloaded literal constructor via SRTP
    let inline lit (v: ^T) : ScalarExpr =
        ((^T or ScalarLitMechanism): (static member ($): ScalarLitMechanism * ^T -> ScalarExpr) (ScalarLitMechanism, v))

    /// The mapping of a .NET enum (or Nullable / F# option of one) stored as an engine Enum, or None for a non-enum
    /// type or a [Flags] enum, which is stored as its underlying integer.
    let tryEnumMapping (t: Type) : EnumMapping option =
        if isNull t then
            None
        else
            let coreType = ClrTypeHelper.UnwrapCoreType t
            if coreType.IsEnum && EnumCategories.UsesEnumDataType coreType then
                let underlyingType = Enum.GetUnderlyingType coreType
                LogicalTypes.tryOfClrType underlyingType
                |> Option.map (fun underlying ->
                    {
                        Name = coreType.FullName
                        Categories = List.ofArray (EnumCategories.GetCategories coreType)
                        Values = EnumCategories.GetUnderlyingValues coreType |> Array.map LiteralValue.OfObject |> List.ofArray
                        Underlying = underlying
                    })
            else
                None

    /// The underlying integer type of an enum type (or Nullable / F# option of one), or None for a non-enum type.
    let tryEnumUnderlyingType (t: Type) : Type option =
        if isNull t then
            None
        else
            let coreType = ClrTypeHelper.UnwrapCoreType t
            if coreType.IsEnum then Some(Enum.GetUnderlyingType coreType) else None

    /// Pure symbolic conversion from a constant object and CLR type to a ScalarExpr literal, or None for a
    /// value of a type without a literal form. An enum constant is its underlying value, wrapped in an
    /// EnumConversion.FromValue unless the enum is stored as an integer ([Flags]).
    let rec tryToLiteral (value: obj) (t: Type) : ScalarExpr option =
        let plain (v: obj) =
            match v with
            | :? char as c -> Some(ScalarExpr.Literal(LiteralValue.String(string c)))
            | _ -> LiteralValue.TryOfObject v |> Option.map ScalarExpr.Literal

        let struct (coreType, optionalKind) = ClrTypeHelper.GetOptionalInfo t
        match value with
        | null ->
            let dt =
                (LogicalTypes.tryOfClrType t)
                |> Option.defaultValue LogicalTypes.Null
            Some(ScalarExpr.Literal(LiteralValue.Null dt))
        // An F# option constant is its payload (Some 3 is 3, None a null), as option columns store it.
        | _ when optionalKind = OptionalKind.FSharpOption || optionalKind = OptionalKind.FSharpValueOption ->
            match ClrTypeHelper.UnwrapOptionalValue(value, optionalKind) with
            | null -> Some(ScalarExpr.Literal(LiteralValue.Null(LogicalTypes.tryOfClrType coreType |> Option.defaultValue LogicalTypes.Null)))
            | payload -> tryToLiteral payload coreType
        | _ ->
            let coreType = ClrTypeHelper.UnwrapCoreType t
            if coreType.IsEnum then
                let underlyingVal = Convert.ChangeType(value, Enum.GetUnderlyingType coreType)
                match plain underlyingVal, tryEnumMapping coreType with
                | Some literal, Some mapping -> Some(ScalarExpr.EnumConvert(EnumConversion.FromValue, literal, mapping))
                | literal, _ -> literal
            else
                plain value

    /// Pure symbolic conversion from a constant object and CLR type to a ScalarExpr literal.
    let toLiteral (value: obj) (t: Type) : ScalarExpr =
        match tryToLiteral value t with
        | Some literal -> literal
        | None -> failwithf "Unsupported literal value type: %s" (value.GetType().FullName)

    /// Pure symbolic translation of type casting to ScalarExpr.Cast nodes.
    let tryTranslateCast (targetType: Type) (operand: ScalarExpr) : ScalarExpr option =
        if targetType = typeof<obj> then
            Some operand
        else
            (LogicalTypes.tryOfClrType targetType)
            |> Option.map (fun dt -> ScalarExpr.Cast(operand, dt, isStrict = false))

    /// Pure check whether a CLR type has an Arrow representation usable as a UDF return type.
    let isUdfReturnTypeSupported (t: Type) : bool =
        not (isNull (ArrowTypeResolver.TryGetArrowTypeFromNetType t))

    /// Pure symbolic translation of standard LINQ reduction method names to ScalarExpr.Aggregation nodes.
    let translateReductionOp (methodName: string) (targetColExpr: ScalarExpr) : ScalarExpr option =
        match methodName with
        | "Sum" -> Some(ScalarExpr.Aggregation(AggKind.Sum, Some targetColExpr, false))
        | "Mean"
        | "Average" -> Some(ScalarExpr.Aggregation(AggKind.Mean, Some targetColExpr, false))
        | "Min" -> Some(ScalarExpr.Aggregation(AggKind.Min, Some targetColExpr, false))
        | "Max" -> Some(ScalarExpr.Aggregation(AggKind.Max, Some targetColExpr, false))
        | "Median" -> Some(ScalarExpr.Aggregation(AggKind.Median, Some targetColExpr, false))
        | "First"
        | "FirstOrDefault" -> Some(ScalarExpr.Aggregation(AggKind.First, Some targetColExpr, false))
        | "Last"
        | "LastOrDefault" -> Some(ScalarExpr.Aggregation(AggKind.Last, Some targetColExpr, false))
        | "Std"
        | "StdDev" -> Some(ScalarExpr.Aggregation(AggKind.Std 1uy, Some targetColExpr, false))
        | "Var"
        | "Variance" -> Some(ScalarExpr.Aggregation(AggKind.Var 1uy, Some targetColExpr, false))
        | _ -> None

    /// Pure symbolic translation of statistical dispersion reduction operations.
    let translateDispersionOp
        (methodName: string)
        (ddof: byte)
        (targetColExpr: ScalarExpr)
        : ScalarExpr option =
        match methodName with
        | "Std"
        | "StdDev" -> Some(ScalarExpr.Aggregation(AggKind.Std ddof, Some targetColExpr, false))
        | "Var"
        | "Variance" -> Some(ScalarExpr.Aggregation(AggKind.Var ddof, Some targetColExpr, false))
        | _ -> None

    // =========================================================================
    // 2. Traversal, Visitors & Transformations
    // =========================================================================

    /// Recursively collects all physical column names referenced within a scalar expression.
    let rec collectColumns (expr: ScalarExpr) : Set<string> =
        match expr with
        | ScalarExpr.Column name -> Set.singleton name
        | ScalarExpr.Literal _ -> Set.empty
        | ScalarExpr.Binary(_, left, right) -> Set.union (collectColumns left) (collectColumns right)
        | ScalarExpr.Unary(_, inner) -> collectColumns inner
        | ScalarExpr.Cast(inner, _, _) -> collectColumns inner
        | ScalarExpr.Function(_, args, _) -> args |> List.map collectColumns |> Set.unionMany
        | ScalarExpr.Aggregation(_, innerOpt, _) ->
            innerOpt |> Option.map collectColumns |> Option.defaultValue Set.empty
        | ScalarExpr.Window(inner, partitionBy, orderBy) ->
            let innerCols = collectColumns inner
            let partCols = partitionBy |> List.map collectColumns |> Set.unionMany
            let orderCols = orderBy |> List.map (fun o -> collectColumns o.Expr) |> Set.unionMany
            Set.unionMany [ innerCols; partCols; orderCols ]
        | ScalarExpr.SortBy(inner, orderBy) ->
            let innerCols = collectColumns inner
            let orderCols = orderBy |> List.map (fun o -> collectColumns o.Expr) |> Set.unionMany
            Set.union innerCols orderCols
        | ScalarExpr.Conditional(test, ifTrue, ifFalse) ->
            Set.unionMany [ collectColumns test; collectColumns ifTrue; collectColumns ifFalse ]
        | ScalarExpr.Coalesce exprs -> exprs |> List.map collectColumns |> Set.unionMany
        | ScalarExpr.ItemAccess(target, idx) -> Set.union (collectColumns target) (collectColumns idx)
        | ScalarExpr.Slice(target, _, _) -> collectColumns target
        | ScalarExpr.Subquery(_, kind) ->
            match kind with
            | SubqueryKind.InSubquery(target, _) -> collectColumns target
            | _ -> Set.empty
        | ScalarExpr.Alias(inner, _) -> collectColumns inner
        | ScalarExpr.Udf(_, args, _, _) -> args |> List.map collectColumns |> Set.unionMany
        | ScalarExpr.InList(target, _, _, _) -> collectColumns target
        | ScalarExpr.EnumConvert(_, inner, _) -> collectColumns inner

    /// Checks whether an expression contains an aggregate function.
    let rec containsAggregation (expr: ScalarExpr) : bool =
        match expr with
        | ScalarExpr.Aggregation _ -> true
        | ScalarExpr.Binary(_, left, right) -> containsAggregation left || containsAggregation right
        | ScalarExpr.Unary(_, inner)
        | ScalarExpr.Cast(inner, _, _)
        | ScalarExpr.EnumConvert(_, inner, _) -> containsAggregation inner
        | ScalarExpr.Function(_, args, _) -> args |> List.exists containsAggregation
        | ScalarExpr.Window(inner, _, _)
        | ScalarExpr.SortBy(inner, _) -> containsAggregation inner
        | ScalarExpr.Conditional(test, ifTrue, ifFalse) ->
            containsAggregation test || containsAggregation ifTrue || containsAggregation ifFalse
        | ScalarExpr.Coalesce exprs -> exprs |> List.exists containsAggregation
        | ScalarExpr.ItemAccess(target, idx) -> containsAggregation target || containsAggregation idx
        | ScalarExpr.Slice(target, _, _) -> containsAggregation target
        | ScalarExpr.Alias(inner, _) -> containsAggregation inner
        | ScalarExpr.Udf(_, args, _, invoker) ->
            match invoker with
            | UdfInvoker.GroupSingleBatch _
            | UdfInvoker.GroupMultiBatch _ -> true
            | _ -> args |> List.exists containsAggregation
        | ScalarExpr.InList(target, _, _, _) -> containsAggregation target
        | _ -> false

    /// Checks whether an expression embeds a subquery.
    let rec containsSubquery (expr: ScalarExpr) : bool =
        match expr with
        | ScalarExpr.Subquery _ -> true
        | ScalarExpr.Binary(_, left, right) -> containsSubquery left || containsSubquery right
        | ScalarExpr.Unary(_, inner)
        | ScalarExpr.Cast(inner, _, _)
        | ScalarExpr.EnumConvert(_, inner, _) -> containsSubquery inner
        | ScalarExpr.Function(_, args, _) -> args |> List.exists containsSubquery
        | ScalarExpr.Aggregation(_, innerOpt, _) ->
            innerOpt |> Option.map containsSubquery |> Option.defaultValue false
        | ScalarExpr.Window(inner, partitionBy, orderBy) ->
            containsSubquery inner
            || partitionBy |> List.exists containsSubquery
            || orderBy |> List.exists (fun o -> containsSubquery o.Expr)
        | ScalarExpr.SortBy(inner, orderBy) ->
            containsSubquery inner
            || orderBy |> List.exists (fun o -> containsSubquery o.Expr)
        | ScalarExpr.Conditional(test, ifTrue, ifFalse) ->
            containsSubquery test || containsSubquery ifTrue || containsSubquery ifFalse
        | ScalarExpr.Coalesce exprs -> exprs |> List.exists containsSubquery
        | ScalarExpr.ItemAccess(target, idx) -> containsSubquery target || containsSubquery idx
        | ScalarExpr.Slice(target, _, _) -> containsSubquery target
        | ScalarExpr.Alias(inner, _) -> containsSubquery inner
        | ScalarExpr.Udf(_, args, _, _) -> args |> List.exists containsSubquery
        | ScalarExpr.InList(target, _, _, _) -> containsSubquery target
        | _ -> false

    /// Checks whether an expression embeds a window expression.
    let rec containsWindow (expr: ScalarExpr) : bool =
        match expr with
        | ScalarExpr.Window _ -> true
        | ScalarExpr.Binary(_, left, right) -> containsWindow left || containsWindow right
        | ScalarExpr.Unary(_, inner)
        | ScalarExpr.Cast(inner, _, _)
        | ScalarExpr.EnumConvert(_, inner, _) -> containsWindow inner
        | ScalarExpr.Function(_, args, _) -> args |> List.exists containsWindow
        | ScalarExpr.Aggregation(_, innerOpt, _) ->
            innerOpt |> Option.map containsWindow |> Option.defaultValue false
        | ScalarExpr.SortBy(inner, orderBy) ->
            containsWindow inner || orderBy |> List.exists (fun o -> containsWindow o.Expr)
        | ScalarExpr.Conditional(test, ifTrue, ifFalse) ->
            containsWindow test || containsWindow ifTrue || containsWindow ifFalse
        | ScalarExpr.Coalesce exprs -> exprs |> List.exists containsWindow
        | ScalarExpr.ItemAccess(target, idx) -> containsWindow target || containsWindow idx
        | ScalarExpr.Slice(target, _, _) -> containsWindow target
        | ScalarExpr.Alias(inner, _) -> containsWindow inner
        | ScalarExpr.Udf(_, args, _, _) -> args |> List.exists containsWindow
        | ScalarExpr.InList(target, _, _, _) -> containsWindow target
        | _ -> false

    /// Functions whose value for a row depends on the other rows of the column (series and group modifiers).
    /// An unknown Custom function is assumed to be one.
    let private isFrameFunction (fn: ScalarFunction) : bool =
        match fn with
        | ScalarFunction.Shift
        | ScalarFunction.Diff _
        | ScalarFunction.Rank _
        | ScalarFunction.PctChange
        | ScalarFunction.Interpolate _
        | ScalarFunction.InterpolateBy
        | ScalarFunction.CumSum
        | ScalarFunction.CumMin
        | ScalarFunction.CumMax
        | ScalarFunction.CumProd
        | ScalarFunction.CumCount
        | ScalarFunction.Implode
        | ScalarFunction.NUnique
        | ScalarFunction.Unique
        | ScalarFunction.MinBy
        | ScalarFunction.MaxBy
        | ScalarFunction.Filter
        | ScalarFunction.Quantile _
        | ScalarFunction.SeriesGet
        | ScalarFunction.Custom _ -> true
        | _ -> false

    /// Checks whether an expression's value for a row depends on other rows of its input: aggregations,
    /// windows, series functions (rank, shift, cumulative, ...), group UDFs and subqueries. Filtering the
    /// input before evaluating such an expression changes its result, so a predicate must not be moved
    /// below a projection that computes one.
    let rec dependsOnOtherRows (expr: ScalarExpr) : bool =
        containsAggregation expr
        || containsWindow expr
        || containsSubquery expr
        || dependsViaFunction expr

    and private dependsViaFunction (expr: ScalarExpr) : bool =
        match expr with
        | ScalarExpr.Function(fn, args, _) -> isFrameFunction fn || args |> List.exists dependsViaFunction
        | ScalarExpr.Binary(_, left, right) -> dependsViaFunction left || dependsViaFunction right
        | ScalarExpr.Unary(_, inner)
        | ScalarExpr.Cast(inner, _, _)
        | ScalarExpr.EnumConvert(_, inner, _)
        | ScalarExpr.Alias(inner, _)
        | ScalarExpr.Slice(inner, _, _)
        | ScalarExpr.InList(inner, _, _, _) -> dependsViaFunction inner
        | ScalarExpr.SortBy(inner, orderBy) ->
            dependsViaFunction inner || orderBy |> List.exists (fun o -> dependsViaFunction o.Expr)
        | ScalarExpr.Conditional(test, ifTrue, ifFalse) ->
            dependsViaFunction test || dependsViaFunction ifTrue || dependsViaFunction ifFalse
        | ScalarExpr.Coalesce exprs -> exprs |> List.exists dependsViaFunction
        | ScalarExpr.ItemAccess(target, idx) -> dependsViaFunction target || dependsViaFunction idx
        | ScalarExpr.Udf(_, args, _, _) -> args |> List.exists dependsViaFunction
        | _ -> false

    /// Substitutes column references in an expression using a mapping function.
    let rec substitute (subst: string -> ScalarExpr option) (expr: ScalarExpr) : ScalarExpr =
        match expr with
        | ScalarExpr.Column name ->
            match subst name with
            | Some repl -> repl
            | None -> expr
        | ScalarExpr.Literal _ -> expr
        | ScalarExpr.Binary(op, left, right) ->
            ScalarExpr.Binary(op, substitute subst left, substitute subst right)
        | ScalarExpr.Unary(op, inner) -> ScalarExpr.Unary(op, substitute subst inner)
        | ScalarExpr.Cast(inner, dtype, strict) -> ScalarExpr.Cast(substitute subst inner, dtype, strict)
        | ScalarExpr.Function(fn, args, ret) ->
            ScalarExpr.Function(fn, args |> List.map (substitute subst), ret)
        | ScalarExpr.Aggregation(kind, innerOpt, dist) ->
            ScalarExpr.Aggregation(kind, innerOpt |> Option.map (substitute subst), dist)
        | ScalarExpr.Window(inner, partBy, orderBy) ->
            ScalarExpr.Window(
                substitute subst inner,
                partBy |> List.map (substitute subst),
                orderBy
                |> List.map (fun o ->
                    { o with
                        Expr = substitute subst o.Expr
                    })
            )
        | ScalarExpr.SortBy(inner, orderBy) ->
            ScalarExpr.SortBy(
                substitute subst inner,
                orderBy
                |> List.map (fun o ->
                    { o with
                        Expr = substitute subst o.Expr
                    })
            )
        | ScalarExpr.Conditional(test, ifTrue, ifFalse) ->
            ScalarExpr.Conditional(substitute subst test, substitute subst ifTrue, substitute subst ifFalse)
        | ScalarExpr.Coalesce exprs -> ScalarExpr.Coalesce(exprs |> List.map (substitute subst))
        | ScalarExpr.ItemAccess(target, idx) ->
            ScalarExpr.ItemAccess(substitute subst target, substitute subst idx)
        | ScalarExpr.Slice(target, off, len) -> ScalarExpr.Slice(substitute subst target, off, len)
        | ScalarExpr.Subquery(plan, kind) ->
            let newKind =
                match kind with
                | SubqueryKind.InSubquery(target, neg) -> SubqueryKind.InSubquery(substitute subst target, neg)
                | other -> other
            ScalarExpr.Subquery(plan, newKind)
        | ScalarExpr.Alias(inner, name) -> ScalarExpr.Alias(substitute subst inner, name)
        | ScalarExpr.Udf(name, args, ret, inv) ->
            ScalarExpr.Udf(name, args |> List.map (substitute subst), ret, inv)
        | ScalarExpr.InList(target, src, neg, nullsEqual) ->
            ScalarExpr.InList(substitute subst target, src, neg, nullsEqual)
        | ScalarExpr.EnumConvert(conversion, inner, mapping) ->
            ScalarExpr.EnumConvert(conversion, substitute subst inner, mapping)

    /// Renames column references in an expression according to a rename dictionary.
    let renameColumns (renameMap: Map<string, string>) (expr: ScalarExpr) : ScalarExpr =
        substitute (fun name -> renameMap |> Map.tryFind name |> Option.map ScalarExpr.Column) expr
