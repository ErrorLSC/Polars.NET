namespace Polars.FSharp.Tests

open Xunit
open Polars.NET.Linq.IR
open Polars.NET.Linq.IR.ScalarExprOps
open Polars.NET.Linq.IR.Passes

/// GroupOrderRelaxation over hand-built plans: which order guarantees it drops, and which it must keep.
module GroupOrderRelaxationTests =

    let private schema = LogicalSchema.OfFields [ ("k", LogicalTypes.Int32); ("v", LogicalTypes.Int32) ]
    let private scan = LogicalPlan.Scan(LogicalPlanSource.Empty, schema)
    let private sum = ScalarExpr.Aggregation(AggKind.Sum, Some(col "v"), false)
    let private first = ScalarExpr.Aggregation(AggKind.First, Some(col "v"), false)
    let private len = ScalarExpr.Aggregation(AggKind.Len, None, false)
    let private asc name = { SortOrder.Expr = col name; Descending = false; NullsLast = false }

    let private groupBy input aggs = LogicalPlan.GroupBy(input, [ col "k" ], aggs, None, true)

    /// The maintainOrder flags of the plan's GroupBys, outermost first.
    let rec private groupFlags (plan: LogicalPlan) : bool list =
        let own =
            match plan with
            | LogicalPlan.GroupBy(_, _, _, _, maintain) -> [ maintain ]
            | _ -> []
        own @ (LogicalPlanOps.getInputs plan |> List.collect groupFlags)

    let rec private hasSort (plan: LogicalPlan) =
        match plan with
        | LogicalPlan.Sort _ -> true
        | other -> LogicalPlanOps.getInputs other |> List.exists hasSort

    [<Fact>]
    let ``Groups whose order is observed keep it`` () =
        let plan = groupBy scan [ "s", sum ]
        Assert.Equal<bool list>([ true ], groupFlags (GroupOrderRelaxation.apply plan))

    [<Fact>]
    let ``A sort over all keys frees the group order, a sort that can tie does not`` () =
        let byKey = LogicalPlan.Sort(groupBy scan [ "s", sum ], [ asc "k" ])
        Assert.Equal<bool list>([ false ], groupFlags (GroupOrderRelaxation.apply byKey))

        let byAggregate = LogicalPlan.Sort(groupBy scan [ "s", sum ], [ asc "s" ])
        Assert.Equal<bool list>([ true ], groupFlags (GroupOrderRelaxation.apply byAggregate))

    [<Fact>]
    let ``Counting groups frees their order`` () =
        let count = LogicalPlan.Project(groupBy scan [ "s", sum ], [ "len", len ])
        Assert.Equal<bool list>([ false ], groupFlags (GroupOrderRelaxation.apply count))

    [<Fact>]
    let ``An order-free aggregate removes a sort beneath it`` () =
        let total = LogicalPlan.Project(LogicalPlan.Sort(scan, [ asc "v" ]), [ "total", sum ])
        Assert.False(hasSort (GroupOrderRelaxation.apply total))

    [<Fact>]
    let ``Order-dependent aggregates and slices keep the order beneath them`` () =
        let firstOfSorted = LogicalPlan.Project(LogicalPlan.Sort(scan, [ asc "v" ]), [ "first", first ])
        Assert.True(hasSort (GroupOrderRelaxation.apply firstOfSorted))

        let firstRow = LogicalPlan.Project(LogicalPlan.Limit(groupBy scan [ "s", sum ], 0L, 1u), [ "len", len ])
        Assert.Equal<bool list>([ true ], groupFlags (GroupOrderRelaxation.apply firstRow))

    [<Fact>]
    let ``A relaxed group with order-free aggregates frees its input, one with First does not`` () =
        let inner = groupBy (LogicalPlan.Sort(scan, [ asc "v" ])) [ "s", sum ]
        let relaxed = GroupOrderRelaxation.apply (LogicalPlan.Project(inner, [ "len", len ]))
        Assert.Equal<bool list>([ false ], groupFlags relaxed)
        Assert.False(hasSort relaxed)

        let withFirst = groupBy (LogicalPlan.Sort(scan, [ asc "v" ])) [ "f", first ]
        let kept = GroupOrderRelaxation.apply (LogicalPlan.Project(withFirst, [ "len", len ]))
        Assert.Equal<bool list>([ false ], groupFlags kept)
        Assert.True(hasSort kept)

    [<Fact>]
    let ``A grouping on a constant frees its input only with column statistics`` () =
        let schema = LogicalSchema.OfFields [ ("v", LogicalTypes.Int64) ]
        let table = LogicalPlan.Scan(LogicalPlanSource.Slot 0, schema)
        let sum = ScalarExpr.Aggregation(AggKind.Sum, Some(ScalarExpr.Column "v"), false)
        let plan = LogicalPlan.GroupBy(table, [ ScalarExpr.Alias(ScalarExpr.Literal(LiteralValue.Int32 1), "k") ], [ "total", sum ], None, true)

        Assert.Equal<bool list>([ true ], groupFlags (GroupOrderRelaxation.applyCollectingUnorderedJoins false false plan |> fst))
        Assert.Equal<bool list>([ false ], groupFlags (GroupOrderRelaxation.applyCollectingUnorderedJoins false true plan |> fst))
