namespace Polars.FSharp.Tests

open Xunit
open Kaminari.Samidare.IR
open Kaminari.Samidare.IR.ScalarExprOps
open Kaminari.Samidare.IR.Passes

/// PredicatePushdown over hand-built plans: how far a predicate moves, and what stops it.
module PredicatePushdownTests =

    let private leftSchema = LogicalSchema.OfFields [ ("k", LogicalTypes.Int32); ("v", LogicalTypes.Int32) ]
    let private rightSchema = LogicalSchema.OfFields [ ("rk", LogicalTypes.Int32); ("w", LogicalTypes.Int32) ]
    let private left = LogicalPlan.Scan(LogicalPlanSource.Slot 0, leftSchema)
    let private right = LogicalPlan.Scan(LogicalPlanSource.Slot 1, rightSchema)

    let private gt name (value: int) = ScalarExpr.Binary(BinaryOp.Gt, col name, ScalarExpr.Literal(LiteralValue.Int32 value))
    let private join how l r =
        LogicalPlan.Join(l, r, [ col "k" ], [ col "rk" ], how, None, JoinKeyCoalescing.Default, false)
    let private sum = ScalarExpr.Aggregation(AggKind.Sum, Some(col "v"), false)
    let private windowed = ScalarExpr.Window(sum, [ col "k" ], [])

    [<Fact>]
    let ``An inner join takes a predicate down the side that has its columns`` () =
        Assert.Equal(
            join JoinKind.Inner (LogicalPlan.Filter(left, gt "v" 1)) right,
            PredicatePushdown.apply (LogicalPlan.Filter(join JoinKind.Inner left right, gt "v" 1)))
        Assert.Equal(
            join JoinKind.Inner left (LogicalPlan.Filter(right, gt "w" 1)),
            PredicatePushdown.apply (LogicalPlan.Filter(join JoinKind.Inner left right, gt "w" 1)))

    [<Fact>]
    let ``A predicate over both sides of a join stays above it`` () =
        let both = ScalarExpr.Binary(BinaryOp.Gt, col "v", col "w")
        let plan = LogicalPlan.Filter(join JoinKind.Inner left right, both)
        Assert.Equal(plan, PredicatePushdown.apply plan)

    [<Fact>]
    let ``A left join takes only predicates on its left side`` () =
        Assert.Equal(
            join JoinKind.Left (LogicalPlan.Filter(left, gt "v" 1)) right,
            PredicatePushdown.apply (LogicalPlan.Filter(join JoinKind.Left left right, gt "v" 1)))
        let onRight = LogicalPlan.Filter(join JoinKind.Left left right, gt "w" 1)
        Assert.Equal(onRight, PredicatePushdown.apply onRight)

    [<Fact>]
    let ``Semi and anti joins take predicates on their left side`` () =
        for how in [ JoinKind.Semi; JoinKind.Anti ] do
            Assert.Equal(
                join how (LogicalPlan.Filter(left, gt "v" 1)) right,
                PredicatePushdown.apply (LogicalPlan.Filter(join how left right, gt "v" 1)))

    [<Fact>]
    let ``A predicate reading other rows is not pushed`` () =
        let plan = LogicalPlan.Filter(join JoinKind.Inner left right, ScalarExpr.Binary(BinaryOp.Gt, windowed, ScalarExpr.Literal(LiteralValue.Int32 1)))
        Assert.Equal(plan, PredicatePushdown.apply plan)

    [<Fact>]
    let ``A predicate does not pass a filter or added column reading other rows`` () =
        let throughFilter = LogicalPlan.Filter(LogicalPlan.Filter(left, ScalarExpr.Binary(BinaryOp.Gt, windowed, ScalarExpr.Literal(LiteralValue.Int32 1))), gt "v" 1)
        Assert.Equal(throughFilter, PredicatePushdown.apply throughFilter)

        let throughColumn = LogicalPlan.Filter(LogicalPlan.WithColumns(left, [ "t", windowed ]), gt "v" 1)
        Assert.Equal(throughColumn, PredicatePushdown.apply throughColumn)

    [<Fact>]
    let ``A grouping takes predicates on its keys, under the key's own expression, and no others`` () =
        let grouped = LogicalPlan.GroupBy(left, [ ScalarExpr.Alias(col "k", "key") ], [ "s", sum ], None, true)
        Assert.Equal(
            LogicalPlan.GroupBy(LogicalPlan.Filter(left, gt "k" 1), [ ScalarExpr.Alias(col "k", "key") ], [ "s", sum ], None, true),
            PredicatePushdown.apply (LogicalPlan.Filter(grouped, gt "key" 1)))

        let onAggregate = LogicalPlan.Filter(grouped, gt "s" 1)
        Assert.Equal(onAggregate, PredicatePushdown.apply onAggregate)

    [<Fact>]
    let ``Sort and distinct let a predicate through, a distinct only on its subset`` () =
        let order = [ { SortOrder.Expr = col "k"; Descending = false; NullsLast = false } ]
        Assert.Equal(
            LogicalPlan.Sort(LogicalPlan.Filter(left, gt "v" 1), order),
            PredicatePushdown.apply (LogicalPlan.Filter(LogicalPlan.Sort(left, order), gt "v" 1)))

        let distinct subset = LogicalPlan.Distinct(left, subset, DistinctKeep.First, true)
        Assert.Equal(
            LogicalPlan.Distinct(LogicalPlan.Filter(left, gt "k" 1), Some [ "k" ], DistinctKeep.First, true),
            PredicatePushdown.apply (LogicalPlan.Filter(distinct (Some [ "k" ]), gt "k" 1)))
        let offSubset = LogicalPlan.Filter(distinct (Some [ "k" ]), gt "v" 1)
        Assert.Equal(offSubset, PredicatePushdown.apply offSubset)

    [<Fact>]
    let ``Optimizing again changes nothing: filters at one depth keep their order`` () =
        // Subplans optimized a different number of times must stay equal, or they are not recognized as common.
        let stacked = LogicalPlan.Filter(LogicalPlan.Filter(left, gt "v" 1), gt "v" 2)
        Assert.Equal(stacked, PredicatePushdown.apply stacked)

        let overJoin = LogicalPlan.Filter(LogicalPlan.Filter(join JoinKind.Inner left right, gt "v" 1), gt "v" 2)
        let once = PredicatePushdown.apply overJoin
        Assert.Equal(once, PredicatePushdown.apply once)
