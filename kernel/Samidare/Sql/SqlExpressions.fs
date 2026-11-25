namespace Kaminari.Samidare.Sql

open System
open System.Globalization
open System.Text
open Kaminari.Samidare.IR

/// A bound expression: the IR expression and its SQL type.
type internal SqlValue = { Expr: ScalarExpr; Type: LogicalType }

/// A subquery bound for an expression: the IR scope number its OuterColumns carry, its plan and its columns.
type internal SqlSubquery =
    { Scope: int
      Plan: LogicalPlan
      Columns: (string * LogicalType) list }

/// The groups an aggregating query's expressions are bound over.
type internal SqlGrouping =
    { /// Each grouping key bound over the input, and the value that holds it once grouped.
      Keys: (ScalarExpr * SqlValue) list
      /// Adds an aggregate over the input and gives the value that holds it once grouped.
      Aggregate: SqlValue -> SqlValue
      /// `grouping(key)` for the key at this index: the value the grouping sets give it, None without grouping sets.
      GroupingOf: int -> SqlValue option }

[<RequireQualifiedAccess>]
type internal SqlAggregates =
    /// Aggregates are refused here; the text names where (`in WHERE`).
    | Refused of where: string
    | Grouped of SqlGrouping

/// What an expression is bound in.
type internal SqlBinder =
    { Text: string
      Scope: SqlScope
      Aggregates: SqlAggregates
      /// Where window functions are refused, the text naming where; None where they are allowed (SELECT, ORDER BY).
      WindowsRefused: string option
      /// Binds a subquery inside this binder's scope.
      Subquery: SqlBinder -> SqlQuery -> SqlSubquery }

/// Binds expressions (sql-frontend design, S2): names to columns, SQL's types and functions to the IR's.
module internal SqlExpressions =

    let fail (binder: SqlBinder) (span: SqlSpan) (reason: string) (suggestion: string option) : 'T =
        raise (SqlException(reason, binder.Text, span, suggestion))

    let private value expr t = { Expr = expr; Type = t }
    let private lit (v: LiteralValue) = ScalarExpr.Literal v
    let private nullOf (t: LogicalType) = value (lit (LiteralValue.Null t)) t
    let private boolLit b = value (lit (LiteralValue.Bool b)) LogicalTypes.Boolean

    let private aggregateNames =
        set [ "count"; "sum"; "avg"; "min"; "max"; "stddev"; "stddev_samp"; "stddev_pop"; "variance"; "var_samp"; "var_pop" ]
    let private rankNames = set [ "rank"; "dense_rank"; "row_number" ]
    let private scalarNames =
        set [ "abs"; "round"; "upper"; "lower"; "substr"; "substring"; "concat"; "coalesce"; "nullif"; "length"; "char_length"; "grouping" ]

    let private functionName (call: SqlFunctionCall) =
        match call.Name with
        | [ name ] -> Some(name.Text.ToLowerInvariant())
        | _ -> None

    /// Whether a function call is an aggregate over a group (not a window).
    let isAggregateCall (call: SqlFunctionCall) =
        call.Over.IsNone && (functionName call |> Option.exists aggregateNames.Contains)

    /// Whether an expression holds an aggregate over a group outside subqueries (a window's arguments count: in
    /// `avg(sum(x)) OVER ()` the query aggregates).
    let rec containsAggregate (e: SqlExpr) : bool =
        match e.Kind with
        | SqlExprKind.Function call when isAggregateCall call -> true
        | SqlExprKind.Function call ->
            call.Arguments |> List.exists containsAggregate
            || (call.Over
                |> Option.exists (fun w -> w.PartitionBy |> List.exists containsAggregate || w.OrderBy |> List.exists (fun o -> containsAggregate o.Expr)))
        | _ -> children e |> List.exists containsAggregate

    /// The expressions directly under one (not inside subqueries).
    and children (e: SqlExpr) : SqlExpr list =
        match e.Kind with
        | SqlExprKind.Literal _
        | SqlExprKind.Column _
        | SqlExprKind.Exists _
        | SqlExprKind.ScalarSubquery _ -> []
        | SqlExprKind.Unary(_, a)
        | SqlExprKind.IsNull(a, _)
        | SqlExprKind.Cast(a, _, _)
        | SqlExprKind.Extract(_, a)
        | SqlExprKind.InSubquery(a, _, _) -> [ a ]
        | SqlExprKind.Binary(_, a, b)
        | SqlExprKind.IsDistinctFrom(a, b, _) -> [ a; b ]
        | SqlExprKind.Between(a, b, c, _) -> [ a; b; c ]
        | SqlExprKind.InList(a, items, _) -> a :: items
        | SqlExprKind.Like(a, b, escape, _) -> a :: b :: Option.toList escape
        | SqlExprKind.Case(operand, whens, otherwise) ->
            Option.toList operand @ (whens |> List.collect (fun (t, v) -> [ t; v ])) @ Option.toList otherwise
        | SqlExprKind.Function call ->
            call.Arguments
            @ (call.Over |> Option.map (fun w -> w.PartitionBy @ (w.OrderBy |> List.map (fun o -> o.Expr))) |> Option.defaultValue [])

    /// Whether an expression holds a window function, a subquery or grouping() (none is a grouping key).
    let rec private holdsWindowOrSubquery (e: SqlExpr) =
        match e.Kind with
        | SqlExprKind.Function { Over = Some _ }
        | SqlExprKind.Exists _
        | SqlExprKind.ScalarSubquery _
        | SqlExprKind.InSubquery _ -> true
        | SqlExprKind.Function call when functionName call = Some "grouping" -> true
        | _ -> children e |> List.exists holdsWindowOrSubquery

    // ----------------------------------------------------------------------------------------------------------
    // Casts and literals
    // ----------------------------------------------------------------------------------------------------------

    /// A string literal read as a date or a timestamp, for an implicit cast (`d_date > '2000-01-01'`).
    let private parseTemporal (binder: SqlBinder) (span: SqlSpan) (text: string) (target: LogicalType) : LiteralValue =
        match SqlTypes.classify target with
        | SqlTypeClass.Date ->
            match DateOnly.TryParseExact(text.Trim(), "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None) with
            | true, d -> LiteralValue.Date d
            | _ -> fail binder span $"'{text}' is not a date" (Some "Write dates as 'YYYY-MM-DD'.")
        | _ ->
            let formats = [| "yyyy-MM-dd HH:mm:ss"; "yyyy-MM-dd HH:mm:ss.FFFFFFF"; "yyyy-MM-ddTHH:mm:ss.FFFFFFF"; "yyyy-MM-dd" |]
            match DateTime.TryParseExact(text.Trim(), formats, CultureInfo.InvariantCulture, DateTimeStyles.None) with
            | true, d -> LiteralValue.DateTime d
            | _ -> fail binder span $"'{text}' is not a timestamp" (Some "Write timestamps as 'YYYY-MM-DD HH:MM:SS'.")

    /// A numeric literal as a literal of a wider type (an integer literal compared with a BIGINT column is a BIGINT, so
    /// an IN list stays a list of literals), or None.
    let private widenLiteral (expr: ScalarExpr) (target: LogicalType) : ScalarExpr option =
        let integer =
            match expr with
            | ScalarExpr.Literal(LiteralValue.Int16 v) -> Some(int64 v)
            | ScalarExpr.Literal(LiteralValue.Int32 v) -> Some(int64 v)
            | ScalarExpr.Literal(LiteralValue.Int64 v) -> Some v
            | _ -> None
        let exact =
            match expr, integer with
            | _, Some v -> Some(decimal v)
            | ScalarExpr.Cast(ScalarExpr.Literal(LiteralValue.Decimal m), _, _), _ -> Some m
            | _ -> None
        match SqlTypes.classify target, integer, exact with
        | SqlTypeClass.Integer(16, true), Some v, _ when v >= int64 Int16.MinValue && v <= int64 Int16.MaxValue -> Some(lit (LiteralValue.Int16(int16 v)))
        | SqlTypeClass.Integer(32, true), Some v, _ when v >= int64 Int32.MinValue && v <= int64 Int32.MaxValue -> Some(lit (LiteralValue.Int32(int v)))
        | SqlTypeClass.Integer(64, true), Some v, _ -> Some(lit (LiteralValue.Int64 v))
        | SqlTypeClass.Decimal _, _, Some m -> Some(ScalarExpr.Cast(lit (LiteralValue.Decimal m), target, false))
        | SqlTypeClass.Floating 64, _, Some m -> Some(lit (LiteralValue.Float64(float m)))
        | SqlTypeClass.Floating 32, _, Some m -> Some(lit (LiteralValue.Float32(float32 m)))
        | _ -> None

    /// A value as type `target`, for an implicit conversion: a NULL literal typed, a numeric literal widened, a string
    /// literal read as a date, otherwise a cast (numeric widening, which cannot fail).
    let convert (binder: SqlBinder) (span: SqlSpan) (v: SqlValue) (target: LogicalType) : SqlValue =
        if v.Type.Display = target.Display then v
        else
            match v.Expr, SqlTypes.classify target with
            | ScalarExpr.Literal(LiteralValue.Null _), _ -> nullOf target
            | literal, _ when (widenLiteral literal target).IsSome -> value (widenLiteral literal target).Value target
            | ScalarExpr.Literal(LiteralValue.String text), (SqlTypeClass.Date | SqlTypeClass.Timestamp) ->
                value (lit (parseTemporal binder span text target)) target
            | _, (SqlTypeClass.Date | SqlTypeClass.Timestamp) when SqlTypes.classify v.Type = SqlTypeClass.String ->
                value (ScalarExpr.Cast(v.Expr, target, true)) target
            | _ -> value (ScalarExpr.Cast(v.Expr, target, false)) target

    /// Two values in the type they meet in, or a failure naming both types.
    let private unify (binder: SqlBinder) (span: SqlSpan) (what: string) (a: SqlValue) (b: SqlValue) =
        match SqlTypes.common a.Type b.Type with
        | Some t -> convert binder span a t, convert binder span b t, t
        | None -> fail binder span $"Cannot {what} {SqlTypes.display a.Type} and {SqlTypes.display b.Type}" None

    /// Values in the one type they all meet in.
    let private unifyAll (binder: SqlBinder) (spans: SqlSpan list) (what: string) (values: SqlValue list) =
        let t =
            values
            |> List.map (fun v -> v.Type)
            |> List.reduce (fun a b ->
                match SqlTypes.common a b with
                | Some t -> t
                | None -> fail binder spans.Head $"Cannot {what} {SqlTypes.display a} and {SqlTypes.display b}" None)
        List.map2 (fun v span -> convert binder span v t) values spans, t

    /// The type and IR literal of a numeric literal as written.
    let private numberLiteral (binder: SqlBinder) (span: SqlSpan) (literal: SqlLiteral) : SqlValue =
        match literal with
        | SqlLiteral.Integer text ->
            match Int32.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture) with
            | true, v -> value (lit (LiteralValue.Int32 v)) LogicalTypes.Int32
            | _ ->
                match Int64.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture) with
                | true, v -> value (lit (LiteralValue.Int64 v)) LogicalTypes.Int64
                | _ ->
                    match Decimal.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture) with
                    | true, v ->
                        let t = SqlTypes.decimal (max 1 (text.TrimStart('0').Length), 0)
                        value (ScalarExpr.Cast(lit (LiteralValue.Decimal v), t, false)) t
                    | _ -> fail binder span $"The number {text} is too large" (Some "Write it as a floating value, with an exponent (1e30).")
        | SqlLiteral.Decimal text ->
            let point = text.IndexOf '.'
            let integral = text.Substring(0, point).TrimStart('0').Length
            let scale = text.Length - point - 1
            let precision = max 1 (integral + scale)
            match Decimal.TryParse(text, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture) with
            | true, v when precision <= SqlTypes.maxPrecision ->
                let t = SqlTypes.decimal (precision, scale)
                value (ScalarExpr.Cast(lit (LiteralValue.Decimal v), t, false)) t
            | _ -> fail binder span $"The number {text} has more digits than a decimal holds" (Some "Write it as a floating value, with an exponent.")
        | SqlLiteral.Float text -> value (lit (LiteralValue.Float64(Double.Parse(text, CultureInfo.InvariantCulture)))) LogicalTypes.Float64
        | _ -> invalidOp "not a number"

    // ----------------------------------------------------------------------------------------------------------
    // Expressions
    // ----------------------------------------------------------------------------------------------------------

    let private expectBoolean (binder: SqlBinder) (span: SqlSpan) (where: string) (v: SqlValue) =
        match SqlTypes.classify v.Type with
        | SqlTypeClass.Boolean -> v
        | SqlTypeClass.Null -> nullOf LogicalTypes.Boolean
        | _ -> fail binder span $"{where} must be a condition, not a value of type {SqlTypes.display v.Type}" None

    let private expectNumeric (binder: SqlBinder) (span: SqlSpan) (what: string) (v: SqlValue) =
        if not (SqlTypes.isNumeric v.Type || SqlTypes.isNull v.Type) then
            fail binder span $"{what} needs a number, not a value of type {SqlTypes.display v.Type}" None

    let rec bind (binder: SqlBinder) (e: SqlExpr) : SqlValue =
        match binder.Aggregates with
        | SqlAggregates.Grouped grouping when not (containsAggregate e) && not (holdsWindowOrSubquery e) ->
            // Over groups: an expression equal to a grouping key is that key's column.
            let ungrouped = { binder with Aggregates = SqlAggregates.Refused "" }
            let asInput = bindNode ungrouped e
            match grouping.Keys |> List.tryFind (fun (key, _) -> key = asInput.Expr) with
            | Some(_, grouped) -> grouped
            | None ->
                match e.Kind with
                | SqlExprKind.Column name when not (isOuter binder name) ->
                    let shown = name |> List.map (fun p -> p.Text) |> String.concat "."
                    fail binder e.Span $"The column {shown} must appear in GROUP BY or be used in an aggregate function" None
                | _ -> bindNode binder e
        | _ -> bindNode binder e

    /// Whether a column name reaches a column of a query around this one (constant over this query's groups).
    and private isOuter (binder: SqlBinder) (name: SqlName) =
        match SqlNames.resolve binder.Scope name with
        | SqlResolution.Found(_, _ :: _) -> true
        | _ -> false

    and private bindNode (binder: SqlBinder) (e: SqlExpr) : SqlValue =
        let span = e.Span
        match e.Kind with
        | SqlExprKind.Literal literal ->
            match literal with
            | SqlLiteral.Integer _
            | SqlLiteral.Decimal _
            | SqlLiteral.Float _ -> numberLiteral binder span literal
            | SqlLiteral.String s -> value (lit (LiteralValue.String s)) LogicalTypes.String
            | SqlLiteral.Boolean b -> boolLit b
            | SqlLiteral.Null -> nullOf LogicalTypes.Null
            | SqlLiteral.Typed("DATE", text) -> value (lit (parseTemporal binder span text LogicalTypes.Date)) LogicalTypes.Date
            | SqlLiteral.Typed("TIMESTAMP", text) -> value (lit (parseTemporal binder span text LogicalTypes.Datetime)) LogicalTypes.Datetime
            | SqlLiteral.Typed(typeName, _) -> fail binder span $"{typeName} literals are not supported" None
            | SqlLiteral.Interval(text, unit) ->
                match Int32.TryParse(text.Trim(), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture), unit with
                | (true, n), ("DAY" | "DAYS") -> value (lit (LiteralValue.TimeSpan(TimeSpan.FromDays(float n)))) LogicalTypes.Duration
                | (true, _), _ -> fail binder span $"INTERVAL ... {unit} is not supported" (Some "Intervals of days are: INTERVAL '5' DAY.")
                | _ -> fail binder span $"'{text}' is not a whole number of {unit.ToLowerInvariant()}s" None

        | SqlExprKind.Column name -> bindColumn binder span name

        | SqlExprKind.Unary(SqlUnaryOp.Not, operand) ->
            let v = expectBoolean binder operand.Span "The operand of NOT" (bind binder operand)
            value (ScalarExpr.Unary(UnaryOp.Not, v.Expr)) LogicalTypes.Boolean
        | SqlExprKind.Unary(SqlUnaryOp.Negate, operand) ->
            let v = bind binder operand
            expectNumeric binder operand.Span "Negation" v
            match v.Expr with
            | ScalarExpr.Literal(LiteralValue.Int32 n) when n <> Int32.MinValue -> value (lit (LiteralValue.Int32 -n)) v.Type
            | ScalarExpr.Literal(LiteralValue.Int64 n) when n <> Int64.MinValue -> value (lit (LiteralValue.Int64 -n)) v.Type
            | ScalarExpr.Literal(LiteralValue.Float64 x) -> value (lit (LiteralValue.Float64 -x)) v.Type
            | ScalarExpr.Cast(ScalarExpr.Literal(LiteralValue.Decimal m), t, strict) -> value (ScalarExpr.Cast(lit (LiteralValue.Decimal -m), t, strict)) v.Type
            | _ -> value (ScalarExpr.Unary(UnaryOp.Negate, v.Expr)) v.Type
        | SqlExprKind.Unary(SqlUnaryOp.Plus, operand) ->
            let v = bind binder operand
            expectNumeric binder operand.Span "Unary +" v
            v

        | SqlExprKind.Binary((SqlBinaryOp.And | SqlBinaryOp.Or) as op, left, right) ->
            let side (x: SqlExpr) = expectBoolean binder x.Span $"An operand of {op.ToString().ToUpperInvariant()}" (bind binder x)
            let l, r = side left, side right
            value (ScalarExpr.Binary((if op = SqlBinaryOp.And then BinaryOp.And else BinaryOp.Or), l.Expr, r.Expr)) LogicalTypes.Boolean

        | SqlExprKind.Binary((SqlBinaryOp.Eq | SqlBinaryOp.NotEq | SqlBinaryOp.Lt | SqlBinaryOp.LtEq | SqlBinaryOp.Gt | SqlBinaryOp.GtEq) as op, left, right) ->
            let l, r, _ = unify binder span "compare" (bind binder left) (bind binder right)
            let irOp =
                match op with
                | SqlBinaryOp.Eq -> BinaryOp.Eq
                | SqlBinaryOp.NotEq -> BinaryOp.NotEq
                | SqlBinaryOp.Lt -> BinaryOp.Lt
                | SqlBinaryOp.LtEq -> BinaryOp.LtEq
                | SqlBinaryOp.Gt -> BinaryOp.Gt
                | _ -> BinaryOp.GtEq
            value (ScalarExpr.Binary(irOp, l.Expr, r.Expr)) LogicalTypes.Boolean

        | SqlExprKind.Binary(SqlBinaryOp.Concat, left, right) ->
            let l, r = asText binder left (bind binder left), asText binder right (bind binder right)
            // SQL's || is null when an operand is (the IR's StringConcat reads null as the empty string).
            let anyNull = ScalarExpr.Binary(BinaryOp.Or, ScalarExpr.Unary(UnaryOp.IsNull, l.Expr), ScalarExpr.Unary(UnaryOp.IsNull, r.Expr))
            value (ScalarExpr.Conditional(anyNull, lit (LiteralValue.Null LogicalTypes.String), ScalarExpr.Binary(BinaryOp.StringConcat, l.Expr, r.Expr))) LogicalTypes.String

        | SqlExprKind.Binary(op, left, right) -> arithmetic binder span op (bind binder left) (bind binder right) left.Span right.Span

        | SqlExprKind.IsNull(operand, negated) ->
            let v = bind binder operand
            value (ScalarExpr.Unary((if negated then UnaryOp.IsNotNull else UnaryOp.IsNull), v.Expr)) LogicalTypes.Boolean

        | SqlExprKind.IsDistinctFrom(left, right, negated) ->
            let l, r, _ = unify binder span "compare" (bind binder left) (bind binder right)
            value (ScalarExpr.Binary((if negated then BinaryOp.EqMissing else BinaryOp.NotEqMissing), l.Expr, r.Expr)) LogicalTypes.Boolean

        | SqlExprKind.Between(operand, low, high, negated) ->
            let x = bind binder operand
            let values, _ = unifyAll binder [ operand.Span; low.Span; high.Span ] "compare" [ x; bind binder low; bind binder high ]
            let x, lo, hi = values.[0], values.[1], values.[2]
            let between = ScalarExpr.Binary(BinaryOp.And, ScalarExpr.Binary(BinaryOp.GtEq, x.Expr, lo.Expr), ScalarExpr.Binary(BinaryOp.LtEq, x.Expr, hi.Expr))
            value (if negated then ScalarExpr.Unary(UnaryOp.Not, between) else between) LogicalTypes.Boolean

        | SqlExprKind.InList(operand, items, negated) ->
            let values, _ = unifyAll binder (operand.Span :: (items |> List.map (fun i -> i.Span))) "compare" (bind binder operand :: (items |> List.map (bind binder)))
            let x, items = values.Head, values.Tail
            let literals = items |> List.choose (fun v -> match v.Expr with ScalarExpr.Literal l -> Some l | _ -> None)
            if literals.Length = items.Length then
                value (ScalarExpr.InList(x.Expr, InListSource.Literals literals, negated, false)) LogicalTypes.Boolean
            else
                let any = items |> List.map (fun v -> ScalarExpr.Binary(BinaryOp.Eq, x.Expr, v.Expr)) |> List.reduce (fun a b -> ScalarExpr.Binary(BinaryOp.Or, a, b))
                value (if negated then ScalarExpr.Unary(UnaryOp.Not, any) else any) LogicalTypes.Boolean

        | SqlExprKind.InSubquery(operand, query, negated) ->
            let x = bind binder operand
            let sub = binder.Subquery binder query
            match sub.Columns with
            | [ (column, t) ] ->
                let common =
                    match SqlTypes.common x.Type t with
                    | Some c -> c
                    | None -> fail binder span $"Cannot compare {SqlTypes.display x.Type} with the subquery's {SqlTypes.display t}" None
                let x = convert binder operand.Span x common
                let plan =
                    if t.Display = common.Display then sub.Plan
                    else LogicalPlan.Project(sub.Plan, [ column, ScalarExpr.Cast(ScalarExpr.Column column, common, false) ])
                let test = ScalarExpr.Subquery(sub.Scope, plan, SubqueryKind.In(x.Expr, false))
                value (if negated then ScalarExpr.Unary(UnaryOp.Not, test) else test) LogicalTypes.Boolean
            | columns -> fail binder query.Span $"The subquery of IN must give one column, not {columns.Length}" None

        | SqlExprKind.Exists query ->
            let sub = binder.Subquery binder query
            value (ScalarExpr.Subquery(sub.Scope, sub.Plan, SubqueryKind.Exists)) LogicalTypes.Boolean

        | SqlExprKind.ScalarSubquery query ->
            let sub = binder.Subquery binder query
            match sub.Columns with
            | [ (_, t) ] -> value (ScalarExpr.Subquery(sub.Scope, sub.Plan, SubqueryKind.Scalar)) t
            | columns -> fail binder query.Span $"A subquery used as a value must give one column, not {columns.Length}" None

        | SqlExprKind.Like(operand, pattern, escape, negated) -> like binder span operand pattern escape negated

        | SqlExprKind.Case(operand, whens, otherwise) ->
            let tests =
                match operand with
                | Some o ->
                    let subject = bind binder o
                    whens
                    |> List.map (fun (w, _) ->
                        let l, r, _ = unify binder w.Span "compare" subject (bind binder w)
                        value (ScalarExpr.Binary(BinaryOp.Eq, l.Expr, r.Expr)) LogicalTypes.Boolean)
                | None -> whens |> List.map (fun (w, _) -> expectBoolean binder w.Span "A WHEN condition" (bind binder w))
            let resultSpans = (whens |> List.map (fun (_, v) -> v.Span)) @ (otherwise |> Option.map (fun o -> [ o.Span ]) |> Option.defaultValue [ span ])
            let resultValues = (whens |> List.map (fun (_, v) -> bind binder v)) @ [ otherwise |> Option.map (bind binder) |> Option.defaultValue (nullOf LogicalTypes.Null) ]
            let results, t = unifyAll binder resultSpans "combine CASE results of" resultValues
            let rec chain (tests: SqlValue list) (results: SqlValue list) =
                match tests, results with
                | [], [ last ] -> last.Expr
                | test :: tests, result :: results -> ScalarExpr.Conditional(test.Expr, result.Expr, chain tests results)
                | _ -> invalidOp "CASE arms"
            value (chain tests results) t

        | SqlExprKind.Cast(operand, typeName, isTry) ->
            let target =
                match SqlTypes.ofTypeName typeName with
                | Some t -> t
                | None -> fail binder typeName.Span $"Unknown type {typeName.Name}" (Some "Types: BOOLEAN, SMALLINT, INTEGER, BIGINT, DECIMAL(p, s), REAL, DOUBLE, VARCHAR, DATE, TIMESTAMP.")
            let v = bind binder operand
            match v.Expr, SqlTypes.classify v.Type, SqlTypes.classify target with
            | ScalarExpr.Literal(LiteralValue.String text), _, (SqlTypeClass.Date | SqlTypeClass.Timestamp) ->
                value (lit (parseTemporal binder operand.Span text target)) target
            | _, (SqlTypeClass.Floating _ | SqlTypeClass.Decimal _), SqlTypeClass.Integer _ ->
                // SQL rounds to the nearest integer, halves away from zero (the IR's cast truncates).
                let rounded = ScalarExpr.Function(ScalarFunction.Round RoundMode.HalfAwayFromZero, [ v.Expr; lit (LiteralValue.Int32 0) ], v.Type)
                value (ScalarExpr.Cast(rounded, target, not isTry)) target
            | _ when v.Type.Display = target.Display -> v
            | _ -> value (ScalarExpr.Cast(v.Expr, target, not isTry)) target

        | SqlExprKind.Extract(field, operand) ->
            let v = bind binder operand
            match SqlTypes.classify v.Type with
            | SqlTypeClass.Date
            | SqlTypeClass.Timestamp
            | SqlTypeClass.Null ->
                let f =
                    match field with
                    | "YEAR" -> ScalarFunction.DtYear
                    | "MONTH" -> ScalarFunction.DtMonth
                    | "DAY" -> ScalarFunction.DtDay
                    | "HOUR" -> ScalarFunction.DtHour
                    | "MINUTE" -> ScalarFunction.DtMinute
                    | "SECOND" -> ScalarFunction.DtSecond
                    | "DOY" -> ScalarFunction.DtOrdinalDay
                    | _ -> fail binder span $"EXTRACT({field} ...) is not supported" (Some "Fields: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DOY.")
                value (ScalarExpr.Function(f, [ v.Expr ], LogicalTypes.Int32)) LogicalTypes.Int32
            | _ -> fail binder operand.Span $"EXTRACT needs a date or a timestamp, not {SqlTypes.display v.Type}" None

        | SqlExprKind.Function call -> bindCall binder span call

    and private bindColumn (binder: SqlBinder) (span: SqlSpan) (name: SqlName) : SqlValue =
        let shown = name |> List.map (fun p -> p.Text) |> String.concat "."
        match SqlNames.resolve binder.Scope name with
        | SqlResolution.Found(column, []) -> value (ScalarExpr.Column column.Internal) column.Type
        | SqlResolution.Found(column, [ subquery ]) -> value (ScalarExpr.OuterColumn(subquery, column.Internal, column.Type)) column.Type
        | SqlResolution.Found _ ->
            fail binder span $"The column {shown} belongs to a query two or more levels out, which is not supported" None
        | SqlResolution.Ambiguous candidates ->
            let choices = String.Join(" or ", candidates)
            fail binder span $"The column name {shown} is ambiguous: it could be {choices}" (Some "Qualify it with its table's name or alias.")
        | SqlResolution.NoRelation qualifier ->
            let relations = binder.Scope.Relations |> List.choose (fun r -> r.Name |> Option.map (fun n -> n.Text))
            let hint = SqlNames.suggest qualifier relations |> Option.map (fun n -> $"Did you mean {n}?")
            fail binder span $"No table or alias named {qualifier} in this query" hint
        | SqlResolution.NotFound inReach ->
            let last = (List.last name).Text
            let hint = SqlNames.suggest last inReach |> Option.map (fun n -> $"Did you mean {n}?")
            fail binder span $"Unknown column {shown}" hint

    /// A value as text, for || and concat (a number or a date as its SQL text).
    and private asText (binder: SqlBinder) (e: SqlExpr) (v: SqlValue) =
        match SqlTypes.classify v.Type with
        | SqlTypeClass.String -> v
        | SqlTypeClass.Null -> nullOf LogicalTypes.String
        | _ -> fail binder e.Span $"|| and concat take text; convert the {SqlTypes.display v.Type} with CAST(... AS VARCHAR)" None

    and private arithmetic (binder: SqlBinder) (span: SqlSpan) (op: SqlBinaryOp) (l: SqlValue) (r: SqlValue) (lSpan: SqlSpan) (rSpan: SqlSpan) : SqlValue =
        let opText =
            match op with
            | SqlBinaryOp.Add -> "+"
            | SqlBinaryOp.Subtract -> "-"
            | SqlBinaryOp.Multiply -> "*"
            | SqlBinaryOp.Divide -> "/"
            | _ -> "%"
        let days (v: SqlValue) (vSpan: SqlSpan) =
            match v.Expr with
            | ScalarExpr.Literal(LiteralValue.Int32 n) -> lit (LiteralValue.TimeSpan(TimeSpan.FromDays(float n)))
            | ScalarExpr.Literal(LiteralValue.TimeSpan _) -> v.Expr
            | _ -> fail binder vSpan "Only a whole number written as a literal (or INTERVAL 'n' DAY) is added to a date" None
        let negate (offset: ScalarExpr) =
            match offset with
            | ScalarExpr.Literal(LiteralValue.TimeSpan t) -> lit (LiteralValue.TimeSpan(-t))
            | other -> other
        match SqlTypes.classify l.Type, SqlTypes.classify r.Type, op with
        // date ± days (an extension: `d_date + 5`).
        | (SqlTypeClass.Date | SqlTypeClass.Timestamp), _, (SqlBinaryOp.Add | SqlBinaryOp.Subtract) when SqlTypes.isInteger r.Type || r.Type.Display = LogicalTypes.Duration.Display ->
            let offset = days r rSpan
            value (ScalarExpr.Function(ScalarFunction.DtOffsetBy, [ l.Expr; (if op = SqlBinaryOp.Subtract then negate offset else offset) ], l.Type)) l.Type
        | _, (SqlTypeClass.Date | SqlTypeClass.Timestamp), SqlBinaryOp.Add when SqlTypes.isInteger l.Type || l.Type.Display = LogicalTypes.Duration.Display ->
            value (ScalarExpr.Function(ScalarFunction.DtOffsetBy, [ r.Expr; days l lSpan ], r.Type)) r.Type
        | _ -> numeric binder op opText l r lSpan rSpan

    and private numeric (binder: SqlBinder) (op: SqlBinaryOp) (opText: string) (l: SqlValue) (r: SqlValue) (lSpan: SqlSpan) (rSpan: SqlSpan) : SqlValue =
        expectNumeric binder lSpan $"'{opText}'" l
        expectNumeric binder rSpan $"'{opText}'" r
        let irOp =
            match op with
            | SqlBinaryOp.Add -> BinaryOp.Add
            | SqlBinaryOp.Subtract -> BinaryOp.Subtract
            | SqlBinaryOp.Multiply -> BinaryOp.Multiply
            | SqlBinaryOp.Divide -> BinaryOp.Divide
            | _ -> BinaryOp.Modulo
        // Division and remainder by zero are null: a zero divisor is made null (the engine raises for decimals, gives
        // infinity for floating values, and evaluates both branches of a conditional, so the quotient cannot be masked).
        let safeDivisor (divisor: SqlValue) (zero: LiteralValue) =
            if op = SqlBinaryOp.Divide || op = SqlBinaryOp.Modulo then
                { divisor with Expr = ScalarExpr.Conditional(ScalarExpr.Binary(BinaryOp.Eq, divisor.Expr, lit zero), lit (LiteralValue.Null divisor.Type), divisor.Expr) }
            else divisor
        let common =
            match SqlTypes.commonNumeric l.Type r.Type with
            | Some t -> t
            | None -> if SqlTypes.isNull l.Type then r.Type else l.Type
        match SqlTypes.classify common with
        | SqlTypeClass.Null -> nullOf LogicalTypes.Null
        | SqlTypeClass.Integer _ ->
            let a, b = convert binder lSpan l common, convert binder rSpan r common
            // SQL integer division truncates toward zero; by zero it is null (the IR's integer division is too).
            let irOp = if op = SqlBinaryOp.Divide then BinaryOp.TruncatedDivide else irOp
            value (ScalarExpr.Binary(irOp, a.Expr, b.Expr)) common
        | SqlTypeClass.Floating _ ->
            let a, b = convert binder lSpan l common, convert binder rSpan r common
            let zero = if common.Display = LogicalTypes.Float32.Display then LiteralValue.Float32 0.0f else LiteralValue.Float64 0.0
            let b = safeDivisor b zero
            value (ScalarExpr.Binary(irOp, a.Expr, b.Expr)) common
        | _ ->
            // Decimals: the result type as Spark computes it (Q4); the operands meet in their common decimal, a dividend
            // widened to the quotient's scale, and the result is cast to its type.
            let d1, d2 = (SqlTypes.asDecimal l.Type).Value, (SqlTypes.asDecimal r.Type).Value
            let p, s = SqlTypes.decimalResult op d1 d2
            let result = SqlTypes.decimal (p, s)
            let operandType =
                match op with
                | SqlBinaryOp.Divide ->
                    let integral = max (fst d1 - snd d1) (fst d2 - snd d2)
                    SqlTypes.decimal (SqlTypes.fitDecimal (integral + s) s)
                | _ -> common
            let a, b = convert binder lSpan l operandType, convert binder rSpan r operandType
            let b = safeDivisor b (LiteralValue.Decimal 0m)
            let computed = ScalarExpr.Binary(irOp, a.Expr, b.Expr)
            // Spark's result type holds every result (a quotient's integral digits grow by the divisor's scale), so the
            // cast cannot fail: strict, it says so, and a non-null value stays non-null.
            value (if operandType.Display = result.Display then computed else ScalarExpr.Cast(computed, result, true)) result

    and private like (binder: SqlBinder) (span: SqlSpan) (operand: SqlExpr) (pattern: SqlExpr) (escape: SqlExpr option) (negated: bool) : SqlValue =
        let subject = asText binder operand (bind binder operand)
        let patternText =
            match pattern.Kind with
            | SqlExprKind.Literal(SqlLiteral.String p) -> p
            | _ -> fail binder pattern.Span "A LIKE pattern must be a string literal" None
        let escapeChar =
            match escape with
            | None -> None
            | Some { Kind = SqlExprKind.Literal(SqlLiteral.String e) } when e.Length = 1 -> Some e.[0]
            | Some e -> fail binder e.Span "ESCAPE takes one character, written as a string literal" None
        // The pattern as pieces: Some text for literal text, None for % (any text), and '_' (any one character).
        let pieces = ResizeArray<Choice<string, unit, unit>>()
        let text = StringBuilder()
        let flush () =
            if text.Length > 0 then
                pieces.Add(Choice1Of3(text.ToString()))
                text.Clear() |> ignore
        let mutable i = 0
        while i < patternText.Length do
            let c = patternText.[i]
            if escapeChar = Some c then
                if i + 1 >= patternText.Length then fail binder pattern.Span "A LIKE pattern cannot end with its escape character" None
                text.Append(patternText.[i + 1]) |> ignore
                i <- i + 2
            elif c = '%' then
                flush ()
                if pieces.Count = 0 || pieces.[pieces.Count - 1] <> Choice2Of3() then pieces.Add(Choice2Of3())
                i <- i + 1
            elif c = '_' then
                flush ()
                pieces.Add(Choice3Of3())
                i <- i + 1
            else
                text.Append c |> ignore
                i <- i + 1
        flush ()
        let str (s: string) = lit (LiteralValue.String s)
        let test =
            match List.ofSeq pieces with
            | [] -> ScalarExpr.Binary(BinaryOp.Eq, subject.Expr, str "")
            | [ Choice1Of3 t ] -> ScalarExpr.Binary(BinaryOp.Eq, subject.Expr, str t)
            | [ Choice1Of3 t; Choice2Of3() ] -> ScalarExpr.Binary(BinaryOp.StartsWith, subject.Expr, str t)
            | [ Choice2Of3(); Choice1Of3 t ] -> ScalarExpr.Binary(BinaryOp.EndsWith, subject.Expr, str t)
            // The IR's Like is a literal substring test.
            | [ Choice2Of3(); Choice1Of3 t; Choice2Of3() ] -> ScalarExpr.Binary(BinaryOp.Like, subject.Expr, str t)
            | [ Choice2Of3() ] -> ScalarExpr.Unary(UnaryOp.IsNotNull, subject.Expr)
            | pieces ->
                let regex = StringBuilder("^(?s:")
                for piece in pieces do
                    match piece with
                    | Choice1Of3 t -> regex.Append(Text.RegularExpressions.Regex.Escape t) |> ignore
                    | Choice2Of3() -> regex.Append(".*") |> ignore
                    | Choice3Of3() -> regex.Append('.') |> ignore
                ScalarExpr.Binary(BinaryOp.RegexMatch, subject.Expr, str (regex.Append(")$").ToString()))
        value (if negated then ScalarExpr.Unary(UnaryOp.Not, test) else test) LogicalTypes.Boolean

    // ----------------------------------------------------------------------------------------------------------
    // Function calls
    // ----------------------------------------------------------------------------------------------------------

    and private bindCall (binder: SqlBinder) (span: SqlSpan) (call: SqlFunctionCall) : SqlValue =
        let name =
            match functionName call with
            | Some n -> n
            | None -> fail binder span "Qualified function names are not supported" None
        match call.Over with
        | Some window ->
            match binder.WindowsRefused with
            | Some where -> fail binder span $"Window functions are not allowed {where}" None
            | None -> bindWindow binder span name call window
        | None when aggregateNames.Contains name ->
            match binder.Aggregates with
            | SqlAggregates.Refused where -> fail binder span $"Aggregate functions are not allowed {where}" None
            | SqlAggregates.Grouped grouping ->
                let inner = { binder with Aggregates = SqlAggregates.Refused "inside another aggregate"; WindowsRefused = Some "inside an aggregate" }
                grouping.Aggregate(aggregate inner span name call)
        | None when rankNames.Contains name -> fail binder span $"{name}() needs OVER (...)" (Some $"Write {name}() OVER (ORDER BY ...).")
        | None -> scalarFunction binder span name call

    /// An aggregate over its (input) arguments: its IR expression and SQL type.
    and private aggregate (binder: SqlBinder) (span: SqlSpan) (name: string) (call: SqlFunctionCall) : SqlValue =
        let distinct = call.Distinct
        let single () =
            match call.Arguments with
            | [ a ] -> bind binder a
            | args -> fail binder span $"{name}() takes one argument, not {args.Length}" None
        let agg kind (arg: ScalarExpr) = ScalarExpr.Aggregation(kind, Some arg, distinct)
        let asBigint (e: ScalarExpr) = ScalarExpr.Cast(e, LogicalTypes.Int64, false)
        match name with
        | "count" when call.Star -> value (asBigint (ScalarExpr.Aggregation(AggKind.Len, None, false))) LogicalTypes.Int64
        | "count" ->
            let v = single ()
            if distinct then
                // The IR's distinct count counts null as a value; SQL's skips it.
                let distinctCount = asBigint (ScalarExpr.Aggregation(AggKind.Count, Some v.Expr, true))
                let anyNull =
                    ScalarExpr.Binary(BinaryOp.Lt, asBigint (ScalarExpr.Aggregation(AggKind.Count, Some v.Expr, false)), asBigint (ScalarExpr.Aggregation(AggKind.Len, None, false)))
                let nullCounted = ScalarExpr.Conditional(anyNull, lit (LiteralValue.Int64 1L), lit (LiteralValue.Int64 0L))
                value (ScalarExpr.Binary(BinaryOp.Subtract, distinctCount, nullCounted)) LogicalTypes.Int64
            else value (asBigint (ScalarExpr.Aggregation(AggKind.Count, Some v.Expr, false))) LogicalTypes.Int64
        | _ when call.Star -> fail binder span $"{name}(*) is not an aggregate; only count(*) is" None
        | "sum" ->
            let v = single ()
            match SqlTypes.classify v.Type with
            | SqlTypeClass.Integer _ -> value (agg AggKind.SumOrNull (ScalarExpr.Cast(v.Expr, LogicalTypes.Int64, false))) LogicalTypes.Int64
            | SqlTypeClass.Decimal(p, s) ->
                let t = SqlTypes.decimal (min SqlTypes.maxPrecision (p + 10), s)
                value (agg AggKind.SumOrNull (ScalarExpr.Cast(v.Expr, t, false))) t
            | SqlTypeClass.Floating _ -> value (agg AggKind.SumOrNull (ScalarExpr.Cast(v.Expr, LogicalTypes.Float64, false))) LogicalTypes.Float64
            | SqlTypeClass.Null -> nullOf LogicalTypes.Int64
            | _ -> fail binder span $"sum() needs numbers, not {SqlTypes.display v.Type}" None
        | "avg" ->
            let v = single ()
            match SqlTypes.classify v.Type with
            | SqlTypeClass.Decimal(p, s) ->
                // AVG of a decimal is a decimal (Spark: p + 4, s + 4), the sum over the count; the IR's Mean of decimals
                // is LINQ's.
                let p', s' = SqlTypes.fitDecimal (p + 4) (s + 4)
                let result = SqlTypes.decimal (p', s')
                let wide = SqlTypes.decimal (SqlTypes.maxPrecision, s')
                let total = ScalarExpr.Cast(agg AggKind.SumOrNull (ScalarExpr.Cast(v.Expr, SqlTypes.decimal (min SqlTypes.maxPrecision (p + 10), s), false)), wide, false)
                let count = ScalarExpr.Aggregation(AggKind.Count, Some v.Expr, distinct)
                // No values: the count is made null rather than 0 (the engine raises on a decimal division by zero).
                let none = ScalarExpr.Binary(BinaryOp.Eq, asBigint count, lit (LiteralValue.Int64 0L))
                let divisor = ScalarExpr.Conditional(none, lit (LiteralValue.Null wide), ScalarExpr.Cast(count, wide, false))
                value (ScalarExpr.Cast(ScalarExpr.Binary(BinaryOp.Divide, total, divisor), result, false)) result
            | SqlTypeClass.Integer _
            | SqlTypeClass.Floating _ -> value (agg AggKind.Mean (ScalarExpr.Cast(v.Expr, LogicalTypes.Float64, false))) LogicalTypes.Float64
            | SqlTypeClass.Null -> nullOf LogicalTypes.Float64
            | _ -> fail binder span $"avg() needs numbers, not {SqlTypes.display v.Type}" None
        | "min"
        | "max" ->
            let v = single ()
            match SqlTypes.classify v.Type with
            | SqlTypeClass.Other -> fail binder span $"{name}() cannot order values of type {SqlTypes.display v.Type}" None
            | _ -> value (agg (if name = "min" then AggKind.MinNanLargest else AggKind.MaxNanLargest) v.Expr) v.Type
        | _ ->
            // Standard deviation and variance, over the values as doubles.
            let v = single ()
            expectNumeric binder span $"{name}()" v
            let kind =
                match name with
                | "stddev"
                | "stddev_samp" -> AggKind.Std 1uy
                | "stddev_pop" -> AggKind.Std 0uy
                | "variance"
                | "var_samp" -> AggKind.Var 1uy
                | _ -> AggKind.Var 0uy
            value (agg kind (ScalarExpr.Cast(v.Expr, LogicalTypes.Float64, false))) LogicalTypes.Float64

    and private orderOf (binder: SqlBinder) (item: SqlOrderItem) : SortOrder * SqlValue =
        let v = bind binder item.Expr
        let descending = item.Direction = SqlSortDirection.Descending
        // NULL is the smallest value (Q3, as LINQ): first ascending, last descending, unless written.
        let nullsLast =
            match item.Nulls with
            | SqlNullOrder.First -> false
            | SqlNullOrder.Last -> true
            | SqlNullOrder.Default -> descending
        { Expr = v.Expr; Descending = descending; NullsLast = nullsLast }, v

    and private bindWindow (binder: SqlBinder) (span: SqlSpan) (name: string) (call: SqlFunctionCall) (window: SqlWindow) : SqlValue =
        let inWindow = { binder with WindowsRefused = Some "inside a window function" }
        let partition = window.PartitionBy |> List.map (fun p -> (bind inWindow p).Expr)
        let orders = window.OrderBy |> List.map (orderOf inWindow)
        let wholePartition, cumulative =
            match window.Frame, orders with
            | None, [] -> true, false
            | Some { Unit = SqlFrameUnit.Rows; Start = SqlFrameBound.UnboundedPreceding; End = (None | Some SqlFrameBound.CurrentRow) }, _ :: _ -> false, true
            | Some { Start = SqlFrameBound.UnboundedPreceding; End = Some SqlFrameBound.UnboundedFollowing }, _ -> true, false
            | None, _ :: _ -> false, false
            | Some frame, _ -> fail binder frame.Span "This window frame is not supported" (Some "Supported: no frame, ROWS UNBOUNDED PRECEDING (to CURRENT ROW), or UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING.")
        if rankNames.Contains name then
            if not call.Arguments.IsEmpty then fail binder span $"{name}() takes no arguments" None
            // Each key as its dense rank over the whole input in its own direction, nulls at its end of the order (the
            // engine ranks a null as null): integers ranked ascending, several of them as one struct, field by field.
            let normalized =
                orders
                |> List.map (fun (order, key) ->
                    let dense = ScalarExpr.Function(ScalarFunction.Rank(RankMethod.Dense, None), [ key.Expr; lit (LiteralValue.Bool order.Descending) ], LogicalTypes.UInt32)
                    let nullRank = if order.NullsLast then Int64.MaxValue else 0L
                    ScalarExpr.Function(ScalarFunction.FillNull, [ ScalarExpr.Cast(dense, LogicalTypes.Int64, false); lit (LiteralValue.Int64 nullRank) ], LogicalTypes.Int64))
            let key =
                match normalized with
                | [] -> fail binder span $"{name}() needs ORDER BY in its window" None
                | [ single ] -> single
                | several -> ScalarExpr.Function(ScalarFunction.AsStruct, several |> List.mapi (fun i k -> ScalarExpr.Alias(k, $"k{i}")), LogicalType.Unknown)
            let method =
                match name with
                | "rank" -> RankMethod.Min
                | "dense_rank" -> RankMethod.Dense
                | _ -> RankMethod.Ordinal
            let ranked = ScalarExpr.Function(ScalarFunction.Rank(method, None), [ key; lit (LiteralValue.Bool false) ], LogicalTypes.Int64)
            // Without PARTITION BY the rank is over the whole input: no window.
            let windowed = if partition.IsEmpty then ranked else ScalarExpr.Window(ranked, partition, [])
            value (ScalarExpr.Cast(windowed, LogicalTypes.Int64, false)) LogicalTypes.Int64
        elif aggregateNames.Contains name then
            // An aggregate with OVER computes over its window; its arguments bind where the window is, so over groups
            // in an aggregating query (`avg(sum(x)) OVER ()` averages the groups' sums).
            let over = aggregate inWindow span name call
            // OVER () is one partition of every row: partitioned by a constant (an engine needs a partition or an order).
            let partition = if partition.IsEmpty then [ lit (LiteralValue.Bool true) ] else partition
            if wholePartition then value (ScalarExpr.Window(over.Expr, partition, [])) over.Type
            elif cumulative then
                let running =
                    match name, call.Arguments with
                    | "sum", [ _ ] ->
                        match over.Expr with
                        | ScalarExpr.Aggregation(AggKind.SumOrNull, Some arg, false) -> Some(ScalarFunction.CumSum, arg)
                        | _ -> None
                    | ("min" | "max"), [ _ ] ->
                        match over.Expr with
                        | ScalarExpr.Aggregation(_, Some arg, false) -> Some((if name = "min" then ScalarFunction.CumMin else ScalarFunction.CumMax), arg)
                        | _ -> None
                    | _ -> None
                match running with
                | Some(f, arg) ->
                    // SQL's running aggregate skips nulls: the engine's cumulative value is null where the input is, so
                    // the last value before stands for it.
                    let cum = ScalarExpr.Function(f, [ arg; lit (LiteralValue.Bool false) ], over.Type)
                    let running = ScalarExpr.Function(ScalarFunction.ForwardFill, [ cum ], over.Type)
                    value (ScalarExpr.Window(running, partition, orders |> List.map fst)) over.Type
                | None -> fail binder span $"A running {name}() is not supported" None
            else
                fail binder window.Span $"{name}() OVER (ORDER BY ...) takes SQL's default frame (RANGE, peers included), which is not supported yet"
                    (Some "Write ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW for a running value.")
        else fail binder span $"{name}() is not a window function" None

    and private scalarFunction (binder: SqlBinder) (span: SqlSpan) (name: string) (call: SqlFunctionCall) : SqlValue =
        if call.Distinct then fail binder span $"DISTINCT is for aggregates, not {name}()" None
        if call.Star then fail binder span $"{name}(*) is not a function call; only count(*) is" None
        let args = call.Arguments |> List.map (bind binder)
        let arity (counts: int list) =
            if not (counts |> List.contains args.Length) then
                let expected = String.Join(" or ", counts)
                fail binder span $"{name}() takes {expected} argument(s), not {args.Length}" None
        let integerLiteral (i: int) =
            match args.[i].Expr with
            | ScalarExpr.Literal(LiteralValue.Int32 n) -> n
            | _ -> fail binder call.Arguments.[i].Span $"Argument {i + 1} of {name}() must be a whole number written as a literal" None
        match name with
        | "abs" ->
            arity [ 1 ]
            expectNumeric binder span "abs()" args.[0]
            value (ScalarExpr.Unary(UnaryOp.Abs, args.[0].Expr)) args.[0].Type
        | "round" ->
            arity [ 1; 2 ]
            expectNumeric binder span "round()" args.[0]
            let digits = if args.Length = 2 then integerLiteral 1 else 0
            value (ScalarExpr.Function(ScalarFunction.Round RoundMode.HalfAwayFromZero, [ args.[0].Expr; lit (LiteralValue.Int32 digits) ], args.[0].Type)) args.[0].Type
        | "upper"
        | "lower" ->
            arity [ 1 ]
            let s = asText binder call.Arguments.[0] args.[0]
            value (ScalarExpr.Function((if name = "upper" then ScalarFunction.ToUpper else ScalarFunction.ToLower), [ s.Expr ], LogicalTypes.String)) LogicalTypes.String
        | "length"
        | "char_length" ->
            arity [ 1 ]
            let s = asText binder call.Arguments.[0] args.[0]
            value (ScalarExpr.Cast(ScalarExpr.Function(ScalarFunction.LenChars, [ s.Expr ], LogicalTypes.UInt32), LogicalTypes.Int64, false)) LogicalTypes.Int64
        | "substr"
        | "substring" ->
            arity [ 2; 3 ]
            let s = asText binder call.Arguments.[0] args.[0]
            let start = integerLiteral 1
            if start < 1 then fail binder call.Arguments.[1].Span $"{name}() counts from 1; a start of {start} is not supported" None
            let length = if args.Length = 3 then integerLiteral 2 else Int32.MaxValue
            if length < 0 then fail binder call.Arguments.[2].Span $"The length of {name}() cannot be negative" None
            let lengthArg = if args.Length = 3 then lit (LiteralValue.Int64(int64 length)) else lit (LiteralValue.Null LogicalTypes.Int64)
            value (ScalarExpr.Function(ScalarFunction.Slice, [ s.Expr; lit (LiteralValue.Int64(int64 (start - 1))); lengthArg ], LogicalTypes.String)) LogicalTypes.String
        | "concat" ->
            if args.IsEmpty then fail binder span "concat() needs arguments" None
            // concat skips NULL arguments (DuckDB, PostgreSQL), as the IR's ConcatString does.
            let items = List.map2 (asText binder) call.Arguments args |> List.map (fun v -> v.Expr)
            value (ScalarExpr.Function(ScalarFunction.ConcatString, lit (LiteralValue.String "") :: items, LogicalTypes.String)) LogicalTypes.String
        | "coalesce" ->
            if args.IsEmpty then fail binder span "coalesce() needs arguments" None
            let values, t = unifyAll binder (call.Arguments |> List.map (fun a -> a.Span)) "combine coalesce() arguments of" args
            value (ScalarExpr.Coalesce(values |> List.map (fun v -> v.Expr))) t
        | "nullif" ->
            arity [ 2 ]
            let a, b, t = unify binder span "compare" args.[0] args.[1]
            value (ScalarExpr.Conditional(ScalarExpr.Binary(BinaryOp.Eq, a.Expr, b.Expr), lit (LiteralValue.Null t), a.Expr)) t
        | "grouping" ->
            match binder.Aggregates with
            | SqlAggregates.Grouped grouping ->
                arity [ 1 ]
                let ungrouped = { binder with Aggregates = SqlAggregates.Refused "" }
                let key = bindNode ungrouped call.Arguments.[0]
                match grouping.Keys |> List.tryFindIndex (fun (k, _) -> k = key.Expr) with
                | Some index ->
                    match grouping.GroupingOf index with
                    | Some v -> v
                    | None -> value (lit (LiteralValue.Int32 0)) LogicalTypes.Int32
                | None -> fail binder call.Arguments.[0].Span "The argument of grouping() must be a GROUP BY key" None
            | SqlAggregates.Refused where -> fail binder span $"grouping() is not allowed {where}" None
        | _ ->
            let known = Set.unionMany [ aggregateNames; rankNames; scalarNames ] |> Set.toList
            let hint = SqlNames.suggest name known |> Option.map (fun n -> $"Did you mean {n}?")
            fail binder span $"Unknown function {name}()" hint
