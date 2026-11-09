namespace Polars.FSharp.Tests

open Xunit
open Polars.NET.Linq.IR
open Polars.NET.Linq.IR.ScalarExprOps
open Polars.NET.Linq.IR.Passes

/// UnionAggregateFlattening over hand-built plans: which groupings of unions of groupings merge into one grouping.
module UnionAggregateFlatteningTests =

    let private leftSchema =
        LogicalSchema.OfFields [ ("l_k", LogicalTypes.Int32); ("l_v", LogicalTypes.Int64); ("l_x", LogicalTypes.Float64) ]
    let private rightSchema =
        LogicalSchema.OfFields [ ("r_k", LogicalTypes.Int32); ("r_v", LogicalTypes.Int64); ("r_x", LogicalTypes.Float64) ]

    let private leftScan = LogicalPlan.Scan(LogicalPlanSource.Slot 0, leftSchema)
    let private rightScan = LogicalPlan.Scan(LogicalPlanSource.Slot 1, rightSchema)

    let private agg kind column = ScalarExpr.Aggregation(kind, Some(col column), false)
    let private zero = ScalarExpr.Literal(LiteralValue.Int64 0L)
    let private one = ScalarExpr.Literal(LiteralValue.Int64 1L)
    let private nullInt = ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Int64)
    let private guarded column = ScalarExpr.Coalesce [ agg AggKind.Sum column; zero ]
    let private keyAs source name = ScalarExpr.Alias(col source, name)

    let private left aggs = LogicalPlan.GroupBy(leftScan, [ keyAs "l_k" "k" ], aggs, None, true)
    let private right aggs = LogicalPlan.GroupBy(rightScan, [ keyAs "r_k" "k" ], aggs, None, true)
    let private union a b = LogicalPlan.Union(a, b, true)
    let private outer input aggs = LogicalPlan.GroupBy(input, [ col "k" ], aggs, None, true)

    let private projectLeft arguments = LogicalPlan.Project(leftScan, ("k", col "l_k") :: arguments)
    let private projectRight arguments = LogicalPlan.Project(rightScan, ("k", col "r_k") :: arguments)

    let private assertUnchanged plan = Assert.Equal(plan, UnionAggregateFlattening.apply plan)

    [<Fact>]
    let ``Guarded sums and a branch's zero become one guarded sum`` () =
        let plan =
            outer
                (union (left [ "a", guarded "l_v"; "b", zero ]) (right [ "a", zero; "b", guarded "r_v" ]))
                [ "a", agg AggKind.Sum "a"; "b", agg AggKind.Sum "b" ]

        let expected =
            outer
                (union
                    (projectLeft [ "__fu0", col "l_v"; "__fu1", nullInt ])
                    (projectRight [ "__fu0", nullInt; "__fu1", col "r_v" ]))
                [ "a", ScalarExpr.Coalesce [ agg AggKind.Sum "__fu0"; zero ]
                  "b", ScalarExpr.Coalesce [ agg AggKind.Sum "__fu1"; zero ] ]
        Assert.Equal(expected, UnionAggregateFlattening.apply plan)

    [<Fact>]
    let ``Plain sums stay a plain sum`` () =
        let plan = outer (union (left [ "a", agg AggKind.Sum "l_v" ]) (right [ "a", agg AggKind.Sum "r_v" ])) [ "a", agg AggKind.Sum "a" ]

        let expected =
            outer (union (projectLeft [ "__fu0", col "l_v" ]) (projectRight [ "__fu0", col "r_v" ])) [ "a", agg AggKind.Sum "__fu0" ]
        Assert.Equal(expected, UnionAggregateFlattening.apply plan)

    [<Fact>]
    let ``A max of a max and of a literal reads the literal on every row`` () =
        let plan = outer (union (left [ "m", agg AggKind.Max "l_v" ]) (right [ "m", one ])) [ "m", agg AggKind.Max "m" ]

        let expected =
            outer (union (projectLeft [ "__fu0", col "l_v" ]) (projectRight [ "__fu0", one ])) [ "m", agg AggKind.Max "__fu0" ]
        Assert.Equal(expected, UnionAggregateFlattening.apply plan)

    [<Fact>]
    let ``Min and Max accept floating arguments`` () =
        let plan = outer (union (left [ "m", agg AggKind.Min "l_x" ]) (right [ "m", agg AggKind.Min "r_x" ])) [ "m", agg AggKind.Min "m" ]

        match UnionAggregateFlattening.apply plan with
        | LogicalPlan.GroupBy(LogicalPlan.Union(LogicalPlan.Project _, LogicalPlan.Project _, true), _, _, None, _) -> ()
        | other -> failwithf "unexpected plan %s" (LogicalPlanOps.formatPlan other)

    [<Fact>]
    let ``Three branches of nested unions merge`` () =
        let third = LogicalPlan.GroupBy(rightScan, [ keyAs "r_k" "k" ], [ "a", agg AggKind.Sum "r_v" ], None, true)
        let plan =
            outer
                (union (union (left [ "a", agg AggKind.Sum "l_v" ]) (right [ "a", agg AggKind.Sum "r_v" ])) third)
                [ "a", agg AggKind.Sum "a" ]

        let expected =
            outer
                (union
                    (union (projectLeft [ "__fu0", col "l_v" ]) (projectRight [ "__fu0", col "r_v" ]))
                    (projectRight [ "__fu0", col "r_v" ]))
                [ "a", agg AggKind.Sum "__fu0" ]
        Assert.Equal(expected, UnionAggregateFlattening.apply plan)

    [<Fact>]
    let ``The column keeps the type the union's first branch gave it`` () =
        let priceSchema = LogicalSchema.OfFields [ ("p_k", LogicalTypes.Int32); ("p_v", LogicalTypes.decimalOf 7 2) ]
        let priceScan slot = LogicalPlan.Scan(LogicalPlanSource.Slot slot, priceSchema)
        let decimalZero = ScalarExpr.Literal(LiteralValue.Decimal 0m)
        let group slot aggs = LogicalPlan.GroupBy(priceScan slot, [ keyAs "p_k" "k" ], aggs, None, true)
        let guardedPrice = ScalarExpr.Coalesce [ agg AggKind.Sum "p_v"; decimalZero ]
        let plan = outer (union (group 0 [ "a", decimalZero ]) (group 1 [ "a", guardedPrice ])) [ "a", agg AggKind.Sum "a" ]

        match UnionAggregateFlattening.apply plan with
        | LogicalPlan.GroupBy(_, _, [ "a", ScalarExpr.Cast(ScalarExpr.Coalesce _, target, true) ], None, _) ->
            Assert.Equal(LogicalTypes.Decimal, target)
        | other -> failwithf "unexpected plan %s" (LogicalPlanOps.formatPlan other)

    [<Fact>]
    let ``A floating sum is left alone`` () =
        assertUnchanged (outer (union (left [ "a", agg AggKind.Sum "l_x" ]) (right [ "a", agg AggKind.Sum "r_x" ])) [ "a", agg AggKind.Sum "a" ])

    [<Fact>]
    let ``A plain sum beside a branch's zero is left alone`` () =
        assertUnchanged (outer (union (left [ "a", agg AggKind.Sum "l_v" ]) (right [ "a", zero ])) [ "a", agg AggKind.Sum "a" ])

    [<Fact>]
    let ``A nonzero literal under a sum is left alone`` () =
        assertUnchanged (outer (union (left [ "a", agg AggKind.Sum "l_v" ]) (right [ "a", one ])) [ "a", agg AggKind.Sum "a" ])

    [<Fact>]
    let ``An outer count is left alone`` () =
        assertUnchanged (outer (union (left [ "a", agg AggKind.Sum "l_v" ]) (right [ "a", agg AggKind.Sum "r_v" ])) [ "n", agg AggKind.Count "a" ])

    [<Fact>]
    let ``A max of a sum is left alone`` () =
        assertUnchanged (outer (union (left [ "a", agg AggKind.Sum "l_v" ]) (right [ "a", agg AggKind.Sum "r_v" ])) [ "m", agg AggKind.Max "a" ])

    [<Fact>]
    let ``A union that removes duplicates is left alone`` () =
        let plan = outer (LogicalPlan.Union(left [ "a", agg AggKind.Sum "l_v" ], right [ "a", agg AggKind.Sum "r_v" ], false)) [ "a", agg AggKind.Sum "a" ]
        assertUnchanged plan

    [<Fact>]
    let ``An inner having is left alone`` () =
        let filtered = LogicalPlan.GroupBy(rightScan, [ keyAs "r_k" "k" ], [ "a", agg AggKind.Sum "r_v" ], Some(ScalarExpr.Literal(LiteralValue.Bool true)), true)
        assertUnchanged (outer (union (left [ "a", agg AggKind.Sum "l_v" ]) filtered) [ "a", agg AggKind.Sum "a" ])

    [<Fact>]
    let ``Outer keys that differ from the inner keys are left alone`` () =
        let twoKeys = LogicalPlan.GroupBy(rightScan, [ keyAs "r_k" "k"; keyAs "r_v" "extra" ], [ "a", agg AggKind.Sum "r_v" ], None, true)
        let leftTwoKeys = LogicalPlan.GroupBy(leftScan, [ keyAs "l_k" "k"; keyAs "l_v" "extra" ], [ "a", agg AggKind.Sum "l_v" ], None, true)
        assertUnchanged (outer (union leftTwoKeys twoKeys) [ "a", agg AggKind.Sum "a" ])

    [<Fact>]
    let ``Branches with different output columns are left alone`` () =
        assertUnchanged (outer (union (left [ "a", agg AggKind.Sum "l_v" ]) (right [ "b", agg AggKind.Sum "r_v" ])) [ "a", agg AggKind.Sum "a" ])

    [<Fact>]
    let ``Arguments of different types are left alone`` () =
        let narrowSchema = LogicalSchema.OfFields [ ("r_k", LogicalTypes.Int32); ("r_v", LogicalTypes.Int32) ]
        let narrow = LogicalPlan.GroupBy(LogicalPlan.Scan(LogicalPlanSource.Slot 1, narrowSchema), [ keyAs "r_k" "k" ], [ "a", agg AggKind.Sum "r_v" ], None, true)
        assertUnchanged (outer (union (left [ "a", agg AggKind.Sum "l_v" ]) narrow) [ "a", agg AggKind.Sum "a" ])
