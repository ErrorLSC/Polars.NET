namespace Polars.FSharp.Tests

open Xunit
open Kaminari.Samidare.IR
open Kaminari.Samidare.IR.ScalarExprOps
open Kaminari.Samidare.IR.Passes

/// SharedConsumerFilters over hand-built plans: a shared plan filtered by the disjunction of its consumers' filters.
module SharedConsumerFiltersTests =

    let private schema = LogicalSchema.OfFields [ ("k", LogicalTypes.Int32); ("v", LogicalTypes.Int64) ]
    let private scan = LogicalPlan.Scan(LogicalPlanSource.Slot 0, schema)
    let private body = LogicalPlan.GroupBy(scan, [ col "k" ], [ "s", ScalarExpr.Aggregation(AggKind.Sum, Some(col "v"), false) ], None, true)
    let private shared input = LogicalPlan.Shared(1, input)

    let private eq name (value: int) = ScalarExpr.Binary(BinaryOp.Eq, col name, ScalarExpr.Literal(LiteralValue.Int32 value))
    let private gt name (value: int64) = ScalarExpr.Binary(BinaryOp.Gt, col name, ScalarExpr.Literal(LiteralValue.Int64 value))
    let private orOf a b = ScalarExpr.Binary(BinaryOp.Or, a, b)
    let private andOf a b = ScalarExpr.Binary(BinaryOp.And, a, b)
    let private union a b = LogicalPlan.Union(a, b, true)

    [<Fact>]
    let ``Consumers that all filter a shared plan filter it by the disjunction of their filters`` () =
        let plan = union (LogicalPlan.Filter(LogicalPlan.Filter(shared body, eq "k" 1), gt "s" 0L)) (LogicalPlan.Filter(shared body, eq "k" 2))

        let filtered = shared (LogicalPlan.Filter(body, orOf (andOf (eq "k" 1) (gt "s" 0L)) (eq "k" 2)))
        let expected = union (LogicalPlan.Filter(LogicalPlan.Filter(filtered, eq "k" 1), gt "s" 0L)) (LogicalPlan.Filter(filtered, eq "k" 2))
        Assert.Equal(expected, SharedConsumerFilters.apply plan)

    [<Fact>]
    let ``Equal filters appear once in the disjunction`` () =
        let plan = union (LogicalPlan.Filter(shared body, eq "k" 1)) (LogicalPlan.Filter(shared body, eq "k" 1))
        let filtered = shared (LogicalPlan.Filter(body, eq "k" 1))
        Assert.Equal(union (LogicalPlan.Filter(filtered, eq "k" 1)) (LogicalPlan.Filter(filtered, eq "k" 1)), SharedConsumerFilters.apply plan)

    [<Fact>]
    let ``A consumer without a filter keeps the shared plan whole`` () =
        let plan = union (LogicalPlan.Filter(shared body, eq "k" 1)) (LogicalPlan.Project(shared body, [ "k", col "k" ; "s", col "s" ]))
        Assert.Equal(plan, SharedConsumerFilters.apply plan)

    [<Fact>]
    let ``A filter above another node is not the consumer's`` () =
        let plan = union (LogicalPlan.Filter(shared body, eq "k" 1)) (LogicalPlan.Filter(LogicalPlan.Project(shared body, [ "k", col "k"; "s", col "s" ]), eq "k" 2))
        Assert.Equal(plan, SharedConsumerFilters.apply plan)

    [<Fact>]
    let ``A filter reading other rows is left out`` () =
        let windowed = ScalarExpr.Binary(BinaryOp.Gt, ScalarExpr.Window(ScalarExpr.Aggregation(AggKind.Sum, Some(col "s"), false), [ col "k" ], []), ScalarExpr.Literal(LiteralValue.Int64 0L))
        let plan = union (LogicalPlan.Filter(shared body, windowed)) (LogicalPlan.Filter(shared body, eq "k" 2))
        // The first consumer's only conjunct reads other rows: it reads every row as far as the shared plan knows.
        Assert.Equal(plan, SharedConsumerFilters.apply plan)

    [<Fact>]
    let ``A shared plan with one consumer is no longer shared`` () =
        let plan = LogicalPlan.Filter(shared body, eq "k" 1)
        Assert.Equal(LogicalPlan.Filter(body, eq "k" 1), SharedConsumerFilters.apply plan)
