namespace Polars.NET.Linq.IR

open System
open System.ComponentModel
open Polars.NET.Core

type PlDataType = Polars.NET.Core.PlDataType


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

/// Construction helpers, active patterns, and visitors for ScalarExpr
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
        | ScalarExpr.Opaque(_, cols, _) -> Set.ofList cols

    /// Checks whether an expression contains an aggregate function.
    let rec containsAggregation (expr: ScalarExpr) : bool =
        match expr with
        | ScalarExpr.Aggregation _ -> true
        | ScalarExpr.Binary(_, left, right) -> containsAggregation left || containsAggregation right
        | ScalarExpr.Unary(_, inner)
        | ScalarExpr.Cast(inner, _, _) -> containsAggregation inner
        | ScalarExpr.Function(_, args, _) -> args |> List.exists containsAggregation
        | ScalarExpr.Window(inner, _, _) -> containsAggregation inner
        | ScalarExpr.Conditional(test, ifTrue, ifFalse) ->
            containsAggregation test || containsAggregation ifTrue || containsAggregation ifFalse
        | ScalarExpr.Coalesce exprs -> exprs |> List.exists containsAggregation
        | ScalarExpr.ItemAccess(target, idx) -> containsAggregation target || containsAggregation idx
        | ScalarExpr.Slice(target, _, _) -> containsAggregation target
        | ScalarExpr.Alias(inner, _) -> containsAggregation inner
        | _ -> false

    /// Checks whether an expression embeds a subquery.
    let rec containsSubquery (expr: ScalarExpr) : bool =
        match expr with
        | ScalarExpr.Subquery _ -> true
        | ScalarExpr.Binary(_, left, right) -> containsSubquery left || containsSubquery right
        | ScalarExpr.Unary(_, inner)
        | ScalarExpr.Cast(inner, _, _) -> containsSubquery inner
        | ScalarExpr.Function(_, args, _) -> args |> List.exists containsSubquery
        | ScalarExpr.Aggregation(_, innerOpt, _) ->
            innerOpt |> Option.map containsSubquery |> Option.defaultValue false
        | ScalarExpr.Window(inner, partitionBy, orderBy) ->
            containsSubquery inner
            || partitionBy |> List.exists containsSubquery
            || orderBy |> List.exists (fun o -> containsSubquery o.Expr)
        | ScalarExpr.Conditional(test, ifTrue, ifFalse) ->
            containsSubquery test || containsSubquery ifTrue || containsSubquery ifFalse
        | ScalarExpr.Coalesce exprs -> exprs |> List.exists containsSubquery
        | ScalarExpr.ItemAccess(target, idx) -> containsSubquery target || containsSubquery idx
        | ScalarExpr.Slice(target, _, _) -> containsSubquery target
        | ScalarExpr.Alias(inner, _) -> containsSubquery inner
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
        | ScalarExpr.Function(name, args, ret) ->
            ScalarExpr.Function(name, args |> List.map (substitute subst), ret)
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
        | ScalarExpr.Opaque _ -> expr

    /// Renames column references in an expression according to a rename dictionary.
    let renameColumns (renameMap: Map<string, string>) (expr: ScalarExpr) : ScalarExpr =
        substitute (fun name -> renameMap |> Map.tryFind name |> Option.map ScalarExpr.Column) expr

    // =========================================================================
    // 3. Active Patterns
    // =========================================================================

    let (|LiteralBool|_|) (expr: ScalarExpr) =
        match expr with
        | ScalarExpr.Literal(LiteralValue.Bool b) -> Some b
        | _ -> None

    let (|LiteralInt|_|) (expr: ScalarExpr) =
        match expr with
        | ScalarExpr.Literal(LiteralValue.Int32 i) -> Some i
        | ScalarExpr.Literal(LiteralValue.Int64 i) -> Some(int i)
        | ScalarExpr.Literal(LiteralValue.Int16 s) -> Some(int s)
        | ScalarExpr.Literal(LiteralValue.Int8 b) -> Some(int b)
        | _ -> None

    let (|LiteralInt64|_|) (expr: ScalarExpr) =
        match expr with
        | ScalarExpr.Literal(LiteralValue.Int64 i) -> Some i
        | ScalarExpr.Literal(LiteralValue.Int32 i) -> Some(int64 i)
        | _ -> None

    let (|LiteralFloat|_|) (expr: ScalarExpr) =
        match expr with
        | ScalarExpr.Literal(LiteralValue.Float64 d) -> Some d
        | ScalarExpr.Literal(LiteralValue.Float32 f) -> Some(double f)
        | _ -> None

    let (|LiteralString|_|) (expr: ScalarExpr) =
        match expr with
        | ScalarExpr.Literal(LiteralValue.String s) -> Some s
        | _ -> None

    let (|LiteralNull|_|) (expr: ScalarExpr) =
        match expr with
        | ScalarExpr.Literal(LiteralValue.Null dt) -> Some dt
        | _ -> None

    let (|ColumnRef|_|) (expr: ScalarExpr) =
        match expr with
        | ScalarExpr.Column name -> Some name
        | _ -> None

    let (|BinaryComparison|_|) (expr: ScalarExpr) =
        match expr with
        | ScalarExpr.Binary(BinaryOp.Eq, l, r) -> Some(BinaryOp.Eq, l, r)
        | ScalarExpr.Binary(BinaryOp.NotEq, l, r) -> Some(BinaryOp.NotEq, l, r)
        | ScalarExpr.Binary(BinaryOp.Lt, l, r) -> Some(BinaryOp.Lt, l, r)
        | ScalarExpr.Binary(BinaryOp.LtEq, l, r) -> Some(BinaryOp.LtEq, l, r)
        | ScalarExpr.Binary(BinaryOp.Gt, l, r) -> Some(BinaryOp.Gt, l, r)
        | ScalarExpr.Binary(BinaryOp.GtEq, l, r) -> Some(BinaryOp.GtEq, l, r)
        | _ -> None
