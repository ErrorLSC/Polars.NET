namespace Polars.FSharp.Tests

open Xunit
open Kaminari.Samidare.IR
open Kaminari.Samidare.IR.ScalarExprOps
open Kaminari.Samidare.IR.Passes

/// JoinKeyPredicateTransfer over hand-built plans: which tests on a join key move to the other side's key.
module JoinKeyPredicateTransferTests =

    let private leftSchema = LogicalSchema.OfFields [ ("k", LogicalTypes.Int32); ("v", LogicalTypes.Int64) ]
    let private rightSchema = LogicalSchema.OfFields [ ("rk", LogicalTypes.Int32); ("w", LogicalTypes.Int64) ]
    let private otherSchema = LogicalSchema.OfFields [ ("ok", LogicalTypes.Int64) ]
    let private left = LogicalPlan.Scan(LogicalPlanSource.Slot 0, leftSchema)
    let private right = LogicalPlan.Scan(LogicalPlanSource.Slot 1, rightSchema)
    let private other = LogicalPlan.Scan(LogicalPlanSource.Slot 2, otherSchema)

    let private lit (value: int) = ScalarExpr.Literal(LiteralValue.Int32 value)
    let private eq name (value: int) = ScalarExpr.Binary(BinaryOp.Eq, col name, lit value)
    let private join how l r leftKey rightKey =
        LogicalPlan.Join(l, r, [ col leftKey ], [ col rightKey ], how, None, JoinKeyCoalescing.Default, false, true)
    let private transfer = JoinKeyPredicateTransfer.apply

    [<Fact>]
    let ``A test on the left key filters the right side of a left join`` () =
        let plan = join JoinKind.Left (LogicalPlan.Filter(left, eq "k" 2000)) right "k" "rk"
        Assert.Equal(join JoinKind.Left (LogicalPlan.Filter(left, eq "k" 2000)) (LogicalPlan.Filter(right, eq "rk" 2000)) "k" "rk", transfer plan)

    [<Fact>]
    let ``A test is known through a grouping key and a rename`` () =
        let sum = ScalarExpr.Aggregation(AggKind.Sum, Some(col "v"), false)
        let grouped = LogicalPlan.GroupBy(LogicalPlan.Filter(left, eq "k" 7), [ ScalarExpr.Alias(col "k", "key") ], [ "s", sum ], None, true)
        let years = LogicalPlan.Project(grouped, [ "year", col "key"; "s", col "s" ])
        let plan = join JoinKind.Left years right "year" "rk"
        Assert.Equal(join JoinKind.Left years (LogicalPlan.Filter(right, eq "rk" 7)) "year" "rk", transfer plan)

    [<Fact>]
    let ``An inner join moves tests both ways, a range and a disjunction among them`` () =
        let range = ScalarExpr.Binary(BinaryOp.GtEq, col "rk", lit 5)
        let either = ScalarExpr.Binary(BinaryOp.Or, eq "k" 1, eq "k" 2)
        let plan = join JoinKind.Inner (LogicalPlan.Filter(left, either)) (LogicalPlan.Filter(right, range)) "k" "rk"
        let expected =
            join JoinKind.Inner
                (LogicalPlan.Filter(LogicalPlan.Filter(left, either), ScalarExpr.Binary(BinaryOp.GtEq, col "k", lit 5)))
                (LogicalPlan.Filter(LogicalPlan.Filter(right, range), ScalarExpr.Binary(BinaryOp.Or, eq "rk" 1, eq "rk" 2)))
                "k" "rk"
        let once = transfer plan
        Assert.Equal(expected, once)
        // Moved once: the other side holds it now.
        Assert.Equal(once, transfer once)

    [<Fact>]
    let ``A left join's right side tells nothing about its output`` () =
        let plan = join JoinKind.Left left (LogicalPlan.Filter(right, eq "rk" 3)) "k" "rk"
        Assert.Equal(plan, transfer plan)
        // Nor does the right side of a left join below another join.
        let onW = ScalarExpr.Binary(BinaryOp.Eq, col "w", ScalarExpr.Literal(LiteralValue.Int64 3L))
        let above = join JoinKind.Inner (join JoinKind.Left left (LogicalPlan.Filter(right, onW)) "k" "rk") other "w" "ok"
        Assert.Equal(above, transfer above)

    [<Fact>]
    let ``Tests between columns, of a non-key column or across types stay`` () =
        let between = join JoinKind.Left (LogicalPlan.Filter(left, ScalarExpr.Binary(BinaryOp.Gt, col "k", col "v"))) right "k" "rk"
        Assert.Equal(between, transfer between)
        let nonKey = join JoinKind.Left (LogicalPlan.Filter(left, ScalarExpr.Binary(BinaryOp.Eq, col "v", ScalarExpr.Literal(LiteralValue.Int64 1L)))) right "k" "rk"
        Assert.Equal(nonKey, transfer nonKey)
        let acrossTypes = join JoinKind.Left (LogicalPlan.Filter(left, eq "k" 1)) other "k" "ok"
        Assert.Equal(acrossTypes, transfer acrossTypes)

    [<Fact>]
    let ``A test against null does not move`` () =
        let isNull = ScalarExpr.Binary(BinaryOp.Eq, col "k", ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Int32))
        let plan = join JoinKind.Left (LogicalPlan.Filter(left, isNull)) right "k" "rk"
        Assert.Equal(plan, transfer plan)

    [<Fact>]
    let ``A full join keeps every row of both sides and moves nothing`` () =
        let plan = join JoinKind.Full (LogicalPlan.Filter(left, eq "k" 1)) right "k" "rk"
        Assert.Equal(plan, transfer plan)

    [<Fact>]
    let ``A disjunction over two columns is no test of the key`` () =
        let plan = join JoinKind.Left (LogicalPlan.Filter(left, ScalarExpr.Binary(BinaryOp.Or, eq "k" 1, ScalarExpr.Binary(BinaryOp.Eq, col "v", ScalarExpr.Literal(LiteralValue.Int64 2L))))) right "k" "rk"
        Assert.Equal(plan, transfer plan)
