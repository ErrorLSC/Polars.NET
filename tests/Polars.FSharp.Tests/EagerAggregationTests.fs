namespace Polars.FSharp.Tests

open Xunit
open Kaminari.Samidare.IR
open Kaminari.Samidare.IR.ScalarExprOps
open Kaminari.Samidare.IR.Passes

/// EagerAggregation over hand-built plans: which groupings aggregate beneath the join of their dimension.
module EagerAggregationTests =

    type private Table(name: string, schema: LogicalSchema, rows: int64) =
        interface ILogicalTableSource with
            member _.Name = name
            member _.Schema = schema
        interface ISourceStatistics with
            member _.EstimatedRows = Some rows

    let private factSchema =
        LogicalSchema.OfFields [ ("f_k", LogicalTypes.Int32); ("f_d", LogicalTypes.Int32); ("f_v", LogicalTypes.Int64); ("f_x", LogicalTypes.Float64) ]
    let private dimSchema = LogicalSchema.OfFields [ ("d_k", LogicalTypes.Int32); ("d_a", LogicalTypes.String) ]
    let private dateSchema = LogicalSchema.OfFields [ ("t_k", LogicalTypes.Int32); ("t_y", LogicalTypes.Int32) ]

    let private sources =
        [|
            SourceBinding.Table(Table("facts", factSchema, 1_000_000L))
            SourceBinding.Table(Table("dims", dimSchema, 1_000L))
            SourceBinding.Table(Table("dates", dateSchema, 500L))
        |]

    let private facts = LogicalPlan.Scan(LogicalPlanSource.Slot 0, factSchema)
    let private dims = LogicalPlan.Scan(LogicalPlanSource.Slot 1, dimSchema)
    let private dates = LogicalPlan.Scan(LogicalPlanSource.Slot 2, dateSchema)

    let private inner left right leftKey rightKey =
        LogicalPlan.Join(left, right, [ col leftKey ], [ col rightKey ], JoinKind.Inner, None, JoinKeyCoalescing.Default, false)

    let private agg kind column = ScalarExpr.Aggregation(kind, Some(col column), false)
    let private len = ScalarExpr.Aggregation(AggKind.Len, None, false)
    let private partial index = sprintf "__ea%d" index

    let private apply = EagerAggregation.apply sources

    [<Fact>]
    let ``A sum over a dimension's column is aggregated on the join key first`` () =
        let plan = LogicalPlan.GroupBy(inner facts dims "f_k" "d_k", [ col "d_a" ], [ "total", agg AggKind.Sum "f_v" ], None, true)

        let pre = LogicalPlan.GroupBy(facts, [ col "f_k" ], [ partial 0, agg AggKind.Sum "f_v" ], None, true)
        let expected = LogicalPlan.GroupBy(inner pre dims "f_k" "d_k", [ col "d_a" ], [ "total", agg AggKind.Sum (partial 0) ], None, true)
        Assert.Equal(expected, apply plan)

    [<Fact>]
    let ``Len and Count are summed, and Min and Max keep their kind`` () =
        let aggs = [ "n", len; "c", agg AggKind.Count "f_v"; "lo", agg AggKind.Min "f_x"; "hi", agg AggKind.Max "f_x" ]
        let plan = LogicalPlan.GroupBy(inner facts dims "f_k" "d_k", [ col "d_a" ], aggs, None, true)

        let pre =
            LogicalPlan.GroupBy(
                facts,
                [ col "f_k" ],
                [ partial 0, len; partial 1, agg AggKind.Count "f_v"; partial 2, agg AggKind.Min "f_x"; partial 3, agg AggKind.Max "f_x" ],
                None,
                true
            )
        let finals =
            [ "n", agg AggKind.Sum (partial 0); "c", agg AggKind.Sum (partial 1); "lo", agg AggKind.Min (partial 2); "hi", agg AggKind.Max (partial 3) ]
        Assert.Equal(LogicalPlan.GroupBy(inner pre dims "f_k" "d_k", [ col "d_a" ], finals, None, true), apply plan)

    [<Fact>]
    let ``An expression over aggregates reuses one partial for one aggregate`` () =
        let ratio = ScalarExpr.Binary(BinaryOp.Divide, agg AggKind.Sum "f_v", agg AggKind.Sum "f_v")
        let plan = LogicalPlan.GroupBy(inner facts dims "f_k" "d_k", [ col "d_a" ], [ "total", agg AggKind.Sum "f_v"; "ratio", ratio ], None, true)

        match apply plan with
        | LogicalPlan.GroupBy(LogicalPlan.Join(LogicalPlan.GroupBy(_, _, partials, _, _), _, _, _, _, _, _, _), _, _, _, _) ->
            Assert.Single(partials) |> ignore
        | other -> failwithf "unexpected plan %s" (LogicalPlanOps.formatPlan other)

    [<Fact>]
    let ``The dimension moves above the other joins of the star`` () =
        let chain = inner (inner facts dims "f_k" "d_k") dates "f_d" "t_k"
        let plan = LogicalPlan.GroupBy(chain, [ col "d_a" ], [ "total", agg AggKind.Sum "f_v" ], None, true)

        let pre = LogicalPlan.GroupBy(inner facts dates "f_d" "t_k", [ col "f_k" ], [ partial 0, agg AggKind.Sum "f_v" ], None, true)
        let expected = LogicalPlan.GroupBy(inner pre dims "f_k" "d_k", [ col "d_a" ], [ "total", agg AggKind.Sum (partial 0) ], None, true)
        Assert.Equal(expected, apply plan)

    [<Fact>]
    let ``Keys and aggregates read through a projection, with output names kept`` () =
        let projected =
            LogicalPlan.Project(inner facts dims "f_k" "d_k", [ "name", col "d_a"; "value", col "f_v" ])
        let plan = LogicalPlan.GroupBy(projected, [ col "name" ], [ "total", agg AggKind.Sum "value" ], None, false)

        let pre = LogicalPlan.GroupBy(facts, [ col "f_k" ], [ partial 0, agg AggKind.Sum "f_v" ], None, false)
        let expected =
            LogicalPlan.GroupBy(inner pre dims "f_k" "d_k", [ ScalarExpr.Alias(col "d_a", "name") ], [ "total", agg AggKind.Sum (partial 0) ], None, false)
        Assert.Equal(expected, apply plan)

    [<Fact>]
    let ``A distinct over a dimension's columns deduplicates the join key first`` () =
        let projected = LogicalPlan.Project(inner facts dims "f_k" "d_k", [ "name", col "d_a" ])
        let plan = LogicalPlan.Distinct(projected, None, DistinctKeep.Any, true)

        let pre = LogicalPlan.Distinct(LogicalPlan.Project(facts, [ "f_k", col "f_k" ]), None, DistinctKeep.Any, true)
        let expected = LogicalPlan.Distinct(LogicalPlan.Project(inner pre dims "f_k" "d_k", [ "name", col "d_a" ]), None, DistinctKeep.Any, true)
        Assert.Equal(expected, apply plan)

    let private assertUnchanged plan = Assert.Equal(plan, apply plan)

    [<Fact>]
    let ``A floating sum is left alone`` () =
        assertUnchanged (LogicalPlan.GroupBy(inner facts dims "f_k" "d_k", [ col "d_a" ], [ "total", agg AggKind.Sum "f_x" ], None, true))

    [<Fact>]
    let ``A checked sum, a mean and a distinct count are left alone`` () =
        for aggregate in
            [ agg AggKind.CheckedSum "f_v"
              agg AggKind.Mean "f_v"
              ScalarExpr.Aggregation(AggKind.Count, Some(col "f_v"), true) ] do
            assertUnchanged (LogicalPlan.GroupBy(inner facts dims "f_k" "d_k", [ col "d_a" ], [ "total", aggregate ], None, true))

    [<Fact>]
    let ``An aggregate reading the dimension is left alone`` () =
        assertUnchanged (LogicalPlan.GroupBy(inner facts dims "f_k" "d_k", [ col "d_a" ], [ "n", agg AggKind.Max "d_a" ], None, true))

    [<Fact>]
    let ``Keys from two tables are left alone`` () =
        assertUnchanged (LogicalPlan.GroupBy(inner facts dims "f_k" "d_k", [ col "d_a"; col "f_d" ], [ "total", agg AggKind.Sum "f_v" ], None, true))

    [<Fact>]
    let ``A dimension nearly as large as the fact table is left alone`` () =
        let big = SourceBinding.Table(Table("big", dimSchema, 800_000L))
        let plan = LogicalPlan.GroupBy(inner facts dims "f_k" "d_k", [ col "d_a" ], [ "total", agg AggKind.Sum "f_v" ], None, true)
        Assert.Equal(plan, EagerAggregation.apply [| sources.[0]; big; sources.[2] |] plan)

    let private filteredDates = LogicalPlan.Filter(dates, ScalarExpr.Binary(BinaryOp.Gt, col "t_y", lit 2000))
    let private keysFromTwo = [ col "d_a"; col "t_y" ]
    let private totals = [ "total", agg AggKind.Sum "f_v" ]
    let private grouped input = LogicalPlan.GroupBy(input, keysFromTwo, totals, None, true)
    let private preAggregated =
        LogicalPlan.GroupBy(inner facts filteredDates "f_d" "t_k", [ col "f_k"; col "t_y" ], [ partial 0, agg AggKind.Sum "f_v" ], None, true)
    let private finalTotal = [ "total", agg AggKind.Sum (partial 0) ]

    [<Fact>]
    let ``Keys from two dimensions aggregate before the unfiltered one`` () =
        let plan = grouped (inner (inner facts filteredDates "f_d" "t_k") dims "f_k" "d_k")

        let expected = LogicalPlan.GroupBy(inner preAggregated dims "f_k" "d_k", keysFromTwo, finalTotal, None, true)
        Assert.Equal(expected, apply plan)

    [<Fact>]
    let ``A star written with the dimension first keeps it on the left`` () =
        let plan = grouped (inner (inner dims facts "d_k" "f_k") filteredDates "f_d" "t_k")

        let expected = LogicalPlan.GroupBy(inner dims preAggregated "d_k" "f_k", keysFromTwo, finalTotal, None, true)
        Assert.Equal(expected, apply plan)

    [<Fact>]
    let ``Two unfiltered dimensions are left alone`` () =
        assertUnchanged (grouped (inner (inner facts dates "f_d" "t_k") dims "f_k" "d_k"))

    [<Fact>]
    let ``A key dimension holding only fixed-width keys is left alone`` () =
        let numbers = LogicalSchema.OfFields [ ("d_k", LogicalTypes.Int32); ("d_a", LogicalTypes.Int32) ]
        assertUnchanged (grouped (inner (inner facts filteredDates "f_d" "t_k") (LogicalPlan.Scan(LogicalPlanSource.Slot 1, numbers)) "f_k" "d_k"))

    [<Fact>]
    let ``A string carried through the pre-aggregation is left alone`` () =
        let labels = LogicalSchema.OfFields [ ("t_k", LogicalTypes.Int32); ("t_y", LogicalTypes.String) ]
        let filtered = LogicalPlan.Filter(LogicalPlan.Scan(LogicalPlanSource.Slot 2, labels), ScalarExpr.Binary(BinaryOp.Eq, col "t_y", lit "x"))
        assertUnchanged (grouped (inner (inner facts filtered "f_d" "t_k") dims "f_k" "d_k"))

    [<Fact>]
    let ``A Distinct over two dimensions is left alone`` () =
        let joined = inner (inner facts filteredDates "f_d" "t_k") dims "f_k" "d_k"
        assertUnchanged (LogicalPlan.Distinct(LogicalPlan.Project(joined, [ "a", col "d_a"; "y", col "t_y" ]), None, DistinctKeep.First, true))

    let private overYears = ScalarExpr.Binary(BinaryOp.Gt, col "t_y", lit 2000)
    let private noValue = ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Int64)
    let private guardedSum value = ScalarExpr.Aggregation(AggKind.Sum, Some(ScalarExpr.Conditional(overYears, value, noValue)), false)

    [<Fact>]
    let ``A sum guarded by the dimension is tested after the join`` () =
        let plan = LogicalPlan.GroupBy(inner facts dates "f_d" "t_k", [ col "t_y" ], [ "total", guardedSum (col "f_v") ], None, true)

        let pre = LogicalPlan.GroupBy(facts, [ col "f_d" ], [ partial 0, agg AggKind.Sum "f_v" ], None, true)
        let expected = LogicalPlan.GroupBy(inner pre dates "f_d" "t_k", [ col "t_y" ], [ "total", guardedSum (col (partial 0)) ], None, true)
        Assert.Equal(expected, apply plan)

    [<Fact>]
    let ``A dimension behind a leading one carries the tested column`` () =
        let plan = LogicalPlan.GroupBy(inner (inner filteredDates facts "t_k" "f_d") dims "f_k" "d_k", [ col "d_a" ], [ "total", guardedSum (col "f_v") ], None, true)

        let pre = LogicalPlan.GroupBy(inner filteredDates facts "t_k" "f_d", [ col "f_k"; col "t_y" ], [ partial 0, agg AggKind.Sum "f_v" ], None, true)
        let expected = LogicalPlan.GroupBy(inner pre dims "f_k" "d_k", [ col "d_a" ], [ "total", guardedSum (col (partial 0)) ], None, true)
        Assert.Equal(expected, apply plan)

    [<Fact>]
    let ``A plain sum behind a leading dimension is left alone`` () =
        assertUnchanged (LogicalPlan.GroupBy(inner (inner filteredDates facts "t_k" "f_d") dims "f_k" "d_k", [ col "d_a" ], totals, None, true))

    let private meanOfInteger = ScalarExpr.Cast(col "f_d", LogicalTypes.Float64, false)

    let private finalMean =
        let sumOf name = ScalarExpr.Aggregation(AggKind.Sum, Some(col name), false)
        ScalarExpr.Conditional(
            ScalarExpr.Binary(BinaryOp.Eq, ScalarExpr.Cast(sumOf (partial 1), LogicalTypes.Int64, false), ScalarExpr.Literal(LiteralValue.Int64 0L)),
            ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Float64),
            ScalarExpr.Binary(BinaryOp.Divide, sumOf (partial 0), ScalarExpr.Cast(sumOf (partial 1), LogicalTypes.Float64, false)))

    let private meanPartials = [ partial 0, ScalarExpr.Aggregation(AggKind.Sum, Some meanOfInteger, false); partial 1, agg AggKind.Count "f_d" ]

    [<Fact>]
    let ``A mean of integers is computed from a partial sum and count`` () =
        let plan = LogicalPlan.GroupBy(inner facts dims "f_k" "d_k", [ col "d_a" ], [ "m", ScalarExpr.Aggregation(AggKind.Mean, Some meanOfInteger, false) ], None, true)

        let pre = LogicalPlan.GroupBy(facts, [ col "f_k" ], meanPartials, None, true)
        Assert.Equal(LogicalPlan.GroupBy(inner pre dims "f_k" "d_k", [ col "d_a" ], [ "m", finalMean ], None, true), apply plan)

    [<Fact>]
    let ``A mean of floats is left alone`` () =
        assertUnchanged (LogicalPlan.GroupBy(inner facts dims "f_k" "d_k", [ col "d_a" ], [ "m", agg AggKind.Mean "f_x" ], None, true))

    [<Fact>]
    let ``Grouping sets read the pre-aggregated rows`` () =
        let spec aggs : LogicalGroupingSetsSpec =
            { KeyAliases = [ "a", col "d_a" ]; Aggs = aggs; Levels = [ [ 0 ], Map.empty; [], Map.empty ]; OutputColumns = [ "a"; "m" ]; LevelMerges = None }
        let plan = LogicalPlan.GroupingSets(inner facts dims "f_k" "d_k", spec [ "m", ScalarExpr.Aggregation(AggKind.Mean, Some meanOfInteger, false) ])

        let pre = LogicalPlan.GroupBy(facts, [ col "f_k" ], meanPartials, None, true)
        Assert.Equal(LogicalPlan.GroupingSets(inner pre dims "f_k" "d_k", spec [ "m", finalMean ]), apply plan)

    [<Fact>]
    let ``A left join is left alone`` () =
        let joined = LogicalPlan.Join(facts, dims, [ col "f_k" ], [ col "d_k" ], JoinKind.Left, None, JoinKeyCoalescing.Default, false)
        assertUnchanged (LogicalPlan.GroupBy(joined, [ col "d_a" ], [ "total", agg AggKind.Sum "f_v" ], None, true))

    [<Fact>]
    let ``A join keyed on another dimension is left alone`` () =
        let snowflake = inner (inner facts dates "f_d" "t_k") dims "t_y" "d_k"
        assertUnchanged (LogicalPlan.GroupBy(snowflake, [ col "d_a" ], [ "total", agg AggKind.Sum "f_v" ], None, true))
