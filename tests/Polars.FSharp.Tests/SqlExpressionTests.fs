namespace Polars.FSharp.Tests

open System
open Xunit
open Kaminari.Samidare.IR
open Kaminari.Samidare.Sql

/// Binding SQL expressions to the IR (sql-frontend S2): names to columns, SQL's types and functions, aggregates over
/// groups and windows, and the errors.
module SqlExpressionTests =

    let private column name (t: LogicalType) = { Name = name; Internal = $"{name}#1"; Type = t; Unqualified = true }

    let private sales: SqlRelation =
        { Name = Some { Text = "sales"; Quoted = false; Span = { Start = 0; Length = 0 } }
          Columns =
            [ column "item" LogicalTypes.Int32
              column "qty" LogicalTypes.Int64
              column "price" (LogicalTypes.decimalOf 7 2)
              column "ratio" LogicalTypes.Float64
              column "name" LogicalTypes.String
              column "sold" LogicalTypes.Date ] }

    let private items: SqlRelation =
        { Name = Some { Text = "items"; Quoted = false; Span = { Start = 0; Length = 0 } }
          Columns = [ { column "item" LogicalTypes.Int32 with Internal = "item#2" }; column "label" LogicalTypes.String ] }

    let private noSubqueries: SqlBinder -> SqlQuery -> SqlSubquery = fun _ _ -> failwith "no subqueries here"

    let private binderOf (scope: SqlScope) text aggregates =
        { Text = text; Scope = scope; Aggregates = aggregates; WindowsRefused = None; Subquery = noSubqueries; NextScope = (fun () -> failwith "no subqueries here"); RangeWindow = (fun _ -> failwith "no RANGE windows here"); Staged = (fun _ -> failwith "no staged columns here") }

    let private plainScope = { Relations = [ sales ]; Outer = None }

    /// Binds an expression over `sales` (no aggregates).
    let private bindIn (scope: SqlScope) (text: string) =
        SqlExpressions.bind (binderOf scope text (SqlAggregates.Refused "in WHERE")) (SqlParser.parseExpression SqlDialect.Standard text)

    let private bound text = bindIn plainScope text

    let private fails (scope: SqlScope) (text: string) =
        Assert.Throws<SqlException>(fun () -> bindIn scope text |> ignore)

    let private c name = ScalarExpr.Column name
    let private lit v = ScalarExpr.Literal v
    let private dec p s = LogicalTypes.decimalOf p s

    let private typeOf text = SqlTypes.display (bound text).Type

    // ----------------------------------------------------------------------------------------------------------
    // Names
    // ----------------------------------------------------------------------------------------------------------

    [<Fact>]
    let ``Names reach columns, ignoring case unless quoted`` () =
        Assert.Equal(c "qty#1", (bound "QTY").Expr)
        Assert.Equal(c "qty#1", (bound "Sales.qty").Expr)
        Assert.Equal(c "qty#1", (bound "\"qty\"").Expr)
        let e = fails plainScope "\"QTY\""
        Assert.StartsWith("Unknown column QTY (line 1, column 1)", e.Message)
        Assert.Contains("Did you mean qty?", e.Message)

    [<Fact>]
    let ``Ambiguous and unknown names are explained`` () =
        let both = { Relations = [ sales; items ]; Outer = None }
        let e = fails both "item"
        Assert.StartsWith("The column name item is ambiguous: it could be sales.item or items.item", e.Message)
        Assert.Contains("Qualify it", e.Message)
        Assert.Equal(c "item#2", (bindIn both "items.item").Expr)
        Assert.Contains("Did you mean items?", (fails both "itms.label").Message)
        Assert.StartsWith("No table or alias named itms in this query", (fails both "itms.label").Message)
        Assert.Contains("Did you mean price?", (fails both "prise").Message)

    [<Fact>]
    let ``A name of the query around a subquery is an outer column`` () =
        let inner = { Relations = [ items ]; Outer = Some(plainScope, 7) }
        Assert.Equal(ScalarExpr.OuterColumn(7, "qty#1", LogicalTypes.Int64), (bindIn inner "qty").Expr)
        Assert.Equal(c "item#2", (bindIn inner "item").Expr)
        let deeper = { Relations = []; Outer = Some(inner, 8) }
        Assert.StartsWith("The column qty belongs to a query two or more levels out", (fails deeper "qty").Message)

    // ----------------------------------------------------------------------------------------------------------
    // Literals and types
    // ----------------------------------------------------------------------------------------------------------

    [<Fact>]
    let ``Literals take the type their text gives`` () =
        Assert.Equal("INTEGER", typeOf "1")
        Assert.Equal("BIGINT", typeOf "3000000000")
        Assert.Equal("DECIMAL(25, 0)", typeOf "1000000000000000000000000")
        Assert.Equal("DECIMAL(4, 3)", typeOf "1.000")
        Assert.Equal("DECIMAL(1, 1)", typeOf ".5")
        Assert.Equal("DOUBLE", typeOf "1e3")
        Assert.Equal(lit (LiteralValue.Date(DateOnly(2000, 1, 31))), (bound "DATE '2000-01-31'").Expr)
        Assert.Equal(lit (LiteralValue.Int32 -5), (bound "-5").Expr)

    [<Fact>]
    let ``Arithmetic follows the numeric promotion and Spark's decimal rules`` () =
        Assert.Equal("BIGINT", typeOf "item + qty")
        Assert.Equal("DECIMAL(13, 2)", typeOf "item + price")
        Assert.Equal("DECIMAL(15, 4)", typeOf "price * price")
        Assert.Equal("DECIMAL(15, 7)", typeOf "price / 1.000")
        Assert.Equal("DECIMAL(17, 10)", typeOf "price / price")
        Assert.Equal("DOUBLE", typeOf "ratio * item")
        // SQL integer division truncates; by zero it is null.
        Assert.Equal(ScalarExpr.Binary(BinaryOp.TruncatedDivide, c "item#1", lit (LiteralValue.Int32 2)), (bound "item / 2").Expr)
        // A zero divisor is made null (the engine evaluates both branches of a conditional).
        match (bound "ratio / qty").Expr with
        | ScalarExpr.Binary(BinaryOp.Divide, _, ScalarExpr.Conditional(ScalarExpr.Binary(BinaryOp.Eq, _, ScalarExpr.Literal(LiteralValue.Float64 0.0)), ScalarExpr.Literal(LiteralValue.Null _), _)) -> ()
        | other -> failwithf "unguarded %A" other
        Assert.StartsWith("'+' needs a number, not a value of type VARCHAR", (fails plainScope "name + 1").Message)

    [<Fact>]
    let ``Comparisons meet in one type, a string literal read as a date`` () =
        Assert.Equal(
            ScalarExpr.Binary(BinaryOp.Gt, c "sold#1", lit (LiteralValue.Date(DateOnly(2000, 2, 1)))),
            (bound "sold > '2000-02-01'").Expr)
        Assert.StartsWith("'2000-13-01' is not a date", (fails plainScope "sold > '2000-13-01'").Message)
        Assert.StartsWith("Cannot compare VARCHAR and INTEGER", (fails plainScope "name = 1").Message)
        match (bound "item = price").Expr with
        | ScalarExpr.Binary(BinaryOp.Eq, ScalarExpr.Cast(_, t, false), _) -> Assert.Equal("DECIMAL(12, 2)", SqlTypes.display t)
        | other -> failwithf "unexpected %A" other

    [<Fact>]
    let ``A numeric literal meeting a wider type is a literal of it, so an IN list stays literals`` () =
        Assert.Equal(
            ScalarExpr.InList(c "qty#1", InListSource.Literals [ LiteralValue.Int64 1L; LiteralValue.Int64 2L ], false, false),
            (bound "qty IN (1, 2)").Expr)
        Assert.Equal(ScalarExpr.Binary(BinaryOp.Gt, c "ratio#1", lit (LiteralValue.Float64 1.0)), (bound "ratio > 1").Expr)

    [<Fact>]
    let ``Date arithmetic adds whole days`` () =
        match (bound "sold + 5").Expr with
        | ScalarExpr.Function(ScalarFunction.DtOffsetBy, [ ScalarExpr.Column "sold#1"; ScalarExpr.Literal(LiteralValue.TimeSpan t) ], _) -> Assert.Equal(TimeSpan.FromDays 5.0, t)
        | other -> failwithf "unexpected %A" other
        match (bound "sold - INTERVAL '30' DAY").Expr with
        | ScalarExpr.Function(ScalarFunction.DtOffsetBy, [ _; ScalarExpr.Literal(LiteralValue.TimeSpan t) ], _) -> Assert.Equal(TimeSpan.FromDays -30.0, t)
        | other -> failwithf "unexpected %A" other

    [<Fact>]
    let ``LIKE becomes a prefix, suffix, substring or pattern test`` () =
        let test text = (bound text).Expr
        let s v = lit (LiteralValue.String v)
        Assert.Equal(ScalarExpr.Binary(BinaryOp.StartsWith, c "name#1", s "ab"), test "name LIKE 'ab%'")
        Assert.Equal(ScalarExpr.Binary(BinaryOp.EndsWith, c "name#1", s "ab"), test "name LIKE '%ab'")
        Assert.Equal(ScalarExpr.Binary(BinaryOp.Like, c "name#1", s "a.b"), test "name LIKE '%a.b%'")
        Assert.Equal(ScalarExpr.Binary(BinaryOp.Eq, c "name#1", s "a%b"), test "name LIKE 'a!%b' ESCAPE '!'")
        Assert.Equal(ScalarExpr.Binary(BinaryOp.RegexMatch, c "name#1", s "^(?s:a\\..*b.)$"), test "name LIKE 'a.%b_'")
        Assert.Equal(ScalarExpr.Unary(UnaryOp.Not, ScalarExpr.Binary(BinaryOp.StartsWith, c "name#1", s "x")), test "name NOT LIKE 'x%'")

    [<Fact>]
    let ``CASE, COALESCE and NULLIF meet in one type`` () =
        Assert.Equal("DECIMAL(12, 2)", typeOf "CASE WHEN item > 1 THEN item ELSE price END")
        match (bound "CASE item WHEN 1 THEN 'a' END").Expr with
        | ScalarExpr.Conditional(ScalarExpr.Binary(BinaryOp.Eq, _, _), _, ScalarExpr.Literal(LiteralValue.Null _)) -> ()
        | other -> failwithf "unexpected %A" other
        Assert.Equal("BIGINT", typeOf "coalesce(item, qty, 0)")
        Assert.StartsWith("A WHEN condition must be a condition", (fails plainScope "CASE WHEN item THEN 1 END").Message)

    [<Fact>]
    let ``Casts round to integers and read string literals as dates`` () =
        match (bound "CAST(ratio AS INTEGER)").Expr with
        | ScalarExpr.Cast(ScalarExpr.Function(ScalarFunction.Round RoundMode.HalfAwayFromZero, _, _), _, true) -> ()
        | other -> failwithf "unexpected %A" other
        match (bound "TRY_CAST(name AS BIGINT)").Expr with
        | ScalarExpr.Cast(_, _, false) -> ()
        | other -> failwithf "unexpected %A" other
        Assert.Equal(lit (LiteralValue.Date(DateOnly(2000, 2, 1))), (bound "CAST('2000-02-01' AS DATE)").Expr)
        Assert.StartsWith("Unknown type MONEY", (fails plainScope "CAST(item AS MONEY)").Message)

    [<Fact>]
    let ``Scalar functions map to the IR's`` () =
        let slice offset length = ScalarExpr.Function(ScalarFunction.Slice, [ c "name#1"; lit (LiteralValue.Int64 offset); lit length ], LogicalTypes.String)
        Assert.Equal(slice 0L (LiteralValue.Int64 5L), (bound "substr(name, 1, 5)").Expr)
        Assert.Equal(slice 2L (LiteralValue.Null LogicalTypes.Int64), (bound "SUBSTRING(name, 3)").Expr)
        match (bound "concat(name, '-', name)").Expr with
        | ScalarExpr.Function(ScalarFunction.ConcatString, [ ScalarExpr.Literal(LiteralValue.String ""); _; _; _ ], _) -> ()
        | other -> failwithf "unexpected %A" other
        match (bound "name || 'x'").Expr with
        | ScalarExpr.Conditional(_, ScalarExpr.Literal(LiteralValue.Null _), ScalarExpr.Binary(BinaryOp.StringConcat, _, _)) -> ()
        | other -> failwithf "unexpected %A" other
        let e = fails plainScope "uper(name)"
        Assert.StartsWith("Unknown function uper()", e.Message)
        Assert.Contains("Did you mean upper?", e.Message)
        Assert.StartsWith("round() takes 1 or 2 argument(s), not 3", (fails plainScope "round(ratio, 1, 2)").Message)

    // ----------------------------------------------------------------------------------------------------------
    // Aggregates and windows
    // ----------------------------------------------------------------------------------------------------------

    /// Binds over groups of `sales` by `item`: the aggregates added, in order.
    let private grouped (text: string) =
        let added = ResizeArray<SqlValue>()
        let grouping =
            { Keys = [ c "item#1", { Expr = c "key0"; Type = LogicalTypes.Int32 } ]
              Aggregate = fun v -> added.Add v; { Expr = c $"agg{added.Count - 1}"; Type = v.Type }
              GroupingOf = fun _ -> None }
        let v = SqlExpressions.bind (binderOf plainScope text (SqlAggregates.Grouped grouping)) (SqlParser.parseExpression SqlDialect.Standard text)
        v, List.ofSeq added

    [<Fact>]
    let ``Over groups, keys and aggregates become their columns`` () =
        let v, added = grouped "item + sum(qty)"
        Assert.Equal(ScalarExpr.Binary(BinaryOp.Add, ScalarExpr.Cast(c "key0", LogicalTypes.Int64, false), c "agg0"), v.Expr)
        Assert.Equal<ScalarExpr list>([ ScalarExpr.Aggregation(AggKind.SumOrNull, Some(ScalarExpr.Cast(c "qty#1", LogicalTypes.Int64, false)), false) ], added |> List.map (fun a -> a.Expr))
        let e = Assert.Throws<SqlException>(fun () -> grouped "qty + 1" |> ignore)
        Assert.StartsWith("The column qty must appear in GROUP BY or be used in an aggregate function (line 1, column 1)", e.Message)
        let e = Assert.Throws<SqlException>(fun () -> grouped "sum(sum(qty))" |> ignore)
        Assert.StartsWith("Aggregate functions are not allowed inside another aggregate", e.Message)
        Assert.StartsWith("Aggregate functions are not allowed in WHERE", (fails plainScope "sum(qty) > 1").Message)

    [<Fact>]
    let ``Aggregates take SQL's types`` () =
        let typeOfAgg text = SqlTypes.display (fst (grouped text)).Type
        Assert.Equal("BIGINT", typeOfAgg "sum(item)")
        Assert.Equal("DECIMAL(17, 2)", typeOfAgg "sum(price)")
        Assert.Equal("DECIMAL(11, 6)", typeOfAgg "avg(price)")
        Assert.Equal("DOUBLE", typeOfAgg "avg(qty)")
        Assert.Equal("BIGINT", typeOfAgg "count(*)")
        Assert.Equal("DECIMAL(7, 2)", typeOfAgg "min(price)")
        let _, added = grouped "max(ratio)"
        Assert.Equal<ScalarExpr list>([ ScalarExpr.Aggregation(AggKind.MaxNanLargest, Some(c "ratio#1"), false) ], added |> List.map (fun a -> a.Expr))
        // count(DISTINCT x) does not count null (the IR's distinct count does).
        match (snd (grouped "count(DISTINCT qty)")).Head.Expr with
        | ScalarExpr.Binary(BinaryOp.Subtract, _, ScalarExpr.Conditional _) -> ()
        | other -> failwithf "unexpected %A" other

    [<Fact>]
    let ``Window functions rank, aggregate over partitions and run`` () =
        // A key ranks as its dense rank in its own direction, a null at its end of the order (0 first, Int64.MaxValue last).
        let denseKey column descending nullRank =
            ScalarExpr.Function(
                ScalarFunction.FillNull,
                [ ScalarExpr.Cast(ScalarExpr.Function(ScalarFunction.Rank(RankMethod.Dense, None), [ c column; lit (LiteralValue.Bool descending) ], LogicalTypes.UInt32), LogicalTypes.Int64, false)
                  lit (LiteralValue.Int64 nullRank) ],
                LogicalTypes.Int64)
        match (bound "rank() OVER (PARTITION BY item ORDER BY qty DESC)").Expr with
        | ScalarExpr.Cast(ScalarExpr.Window(ScalarExpr.Function(ScalarFunction.Rank(RankMethod.Min, None), [ key; ScalarExpr.Literal(LiteralValue.Bool false) ], _), [ ScalarExpr.Column "item#1" ], []), _, _) ->
            Assert.Equal(denseKey "qty#1" true Int64.MaxValue, key)
        | other -> failwithf "unexpected %A" other
        // Several keys, in their own directions: one struct of their dense ranks.
        match (bound "dense_rank() OVER (ORDER BY qty DESC, name NULLS LAST)").Expr with
        | ScalarExpr.Cast(ScalarExpr.Function(ScalarFunction.Rank(RankMethod.Dense, None), [ ScalarExpr.Function(ScalarFunction.AsStruct, fields, _); _ ], _), _, _) ->
            Assert.Equal<ScalarExpr list>([ ScalarExpr.Alias(denseKey "qty#1" true Int64.MaxValue, "k0"); ScalarExpr.Alias(denseKey "name#1" false Int64.MaxValue, "k1") ], fields)
        | other -> failwithf "unexpected %A" other
        let partitioned, _ = grouped "avg(sum(qty)) OVER (PARTITION BY item)"
        match partitioned.Expr with
        | ScalarExpr.Window(ScalarExpr.Aggregation(AggKind.Mean, Some(ScalarExpr.Cast(ScalarExpr.Column "agg0", _, _)), false), [ ScalarExpr.Column "key0" ], []) -> ()
        | other -> failwithf "unexpected %A" other
        match (bound "sum(qty) OVER (PARTITION BY item ORDER BY sold ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)").Expr with
        | ScalarExpr.Window(ScalarExpr.Function(ScalarFunction.ForwardFill, [ ScalarExpr.Function(ScalarFunction.CumSum, _, _) ], _), [ _ ], [ { Descending = false; NullsLast = false } ]) -> ()
        | other -> failwithf "unexpected %A" other
        // The default frame with ORDER BY (RANGE) goes to the query binder, which computes it by peers.
        let mutable range = None
        let ranged = { binderOf plainScope "sum(qty) OVER (ORDER BY sold)" (SqlAggregates.Refused "here") with RangeWindow = (fun r -> range <- Some r; ScalarExpr.Column "range") }
        Assert.Equal(ScalarExpr.Column "range", (SqlExpressions.bind ranged (SqlParser.parseExpression SqlDialect.Standard "sum(qty) OVER (ORDER BY sold)")).Expr)
        Assert.Equal(1, range.Value.Orders.Length)
        Assert.StartsWith("A running min() of text", (fails plainScope "min(name) OVER (ORDER BY sold)").Message)
        let inWhere = { binderOf plainScope "rank() OVER (ORDER BY qty)" (SqlAggregates.Refused "in WHERE") with WindowsRefused = Some "in WHERE" }
        let e = Assert.Throws<SqlException>(fun () -> SqlExpressions.bind inWhere (SqlParser.parseExpression SqlDialect.Standard "rank() OVER (ORDER BY qty)") |> ignore)
        Assert.StartsWith("Window functions are not allowed in WHERE", e.Message)
