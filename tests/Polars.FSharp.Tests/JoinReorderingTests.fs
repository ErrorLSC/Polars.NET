namespace Polars.FSharp.Tests

open Xunit
open Polars.NET.Linq.IR
open Polars.NET.Linq.IR.ScalarExprOps

/// Join reordering over hand-built plans, through the complete optimizer (it reorders only what the order analysis frees).
module JoinReorderingTests =

    type private Table(rows: int64 option, schema: LogicalSchema) =
        interface ILogicalTableSource with
            member _.Name = "t"
            member _.Schema = schema
        interface ISourceStatistics with
            member _.EstimatedRows = rows

    let private ints (names: string list) = LogicalSchema.OfFields(names |> List.map (fun n -> n, LogicalTypes.Int32))
    let private factSchema = ints [ "a_key"; "b_key"; "c_key"; "v" ]
    let private aSchema = ints [ "a"; "a_v" ]
    let private bSchema = ints [ "b"; "b_v" ]
    let private cSchema = ints [ "c"; "c_v" ]
    let private scan slot schema = LogicalPlan.Scan(LogicalPlanSource.Slot slot, schema)
    let private fact = scan 0 factSchema
    let private a = scan 1 aSchema
    let private b = scan 2 bSchema
    let private c = scan 3 cSchema

    let private sources (rows: int64 option list) (schemas: LogicalSchema list) =
        List.zip rows schemas
        |> List.map (fun (r, s) -> SourceBinding.Table(Table(r, s)))
        |> Array.ofList

    let private star rows = sources rows [ factSchema; aSchema; bSchema; cSchema ]
    let private withStats = star [ Some 5_000_000L; Some 10_000L; Some 10_000L; Some 10_000L ]

    let private eq name (value: int) = ScalarExpr.Binary(BinaryOp.Eq, col name, ScalarExpr.Literal(LiteralValue.Int32 value))
    let private join l r lk rk = LogicalPlan.Join(l, r, [ col lk ], [ col rk ], JoinKind.Inner, None, JoinKeyCoalescing.Default, false)
    let private sum name = ScalarExpr.Aggregation(AggKind.Sum, Some(col name), false)
    /// One row of order-free aggregates: nothing observes its input's order.
    let private total name input = LogicalPlan.Project(input, [ "total", sum name ])

    /// Slots in left-to-right leaf order: the order of a left-deep join tree.
    let rec private slots (plan: LogicalPlan) : int list =
        match plan with
        | LogicalPlan.Scan(LogicalPlanSource.Slot i, _) -> [ i ]
        | other -> LogicalPlanOps.getInputs other |> List.collect slots

    /// fact ⋈ a ⋈ b ⋈ c, as written, with b filtered.
    let private written =
        let filteredB = LogicalPlan.Filter(b, eq "b_v" 1)
        join (join (join fact a "a_key" "a") filteredB "b_key" "b") c "c_key" "c"

    [<Fact>]
    let ``The filtered dimension joins the fact table first`` () =
        let result = LogicalOptimizer.optimizeComplete false false withStats (total "v" written)
        Assert.Equal<int list>([ 0; 2; 1; 3 ], slots result)

    type private MeasuredTable(rows: int64, schema: LogicalSchema, columns: Map<string, ColumnStatistics>, ranges: Map<string, float * float>) =
        interface ILogicalTableSource with
            member _.Name = "t"
            member _.Schema = schema
        interface ISourceStatistics with
            member _.EstimatedRows = Some rows
        interface IColumnStatisticsSource with
            member _.TryGetColumnStatistics column = columns.TryFind column
            member _.TryGetColumnRange column =
                ranges.TryFind column |> Option.map (fun (low, high) -> LiteralValue.Float64 low, LiteralValue.Float64 high)

    let private distinct count: ColumnStatistics =
        { NullFraction = Some 0.0; DistinctCount = Some count; Min = None; Max = None; FrequentValues = []; Histogram = None; Origin = StatisticsOrigin.Exact }

    /// fact ⋈ σ(a) ⋈ σ(b) ⋈ c: a's filter keeps 0.1% of a, b's keeps 5% of b.
    let private measuredSources (factKeyHigh: float option) =
        let wide = Map.ofList [ "a", (0.0, 1000.0) ]
        let factRanges =
            match factKeyHigh with
            | Some high -> Map.ofList [ "a_key", (0.0, high) ]
            | None -> Map.empty
        [|
            SourceBinding.Table(MeasuredTable(5_000_000L, factSchema, Map.empty, factRanges))
            SourceBinding.Table(MeasuredTable(10_000L, aSchema, Map.ofList [ "a_v", distinct 1000.0 ], wide))
            SourceBinding.Table(MeasuredTable(10_000L, bSchema, Map.ofList [ "b_v", distinct 20.0 ], Map.empty))
            SourceBinding.Table(Table(Some 10_000L, cSchema))
        |]

    let private measuredStar =
        join (join (join fact (LogicalPlan.Filter(a, eq "a_v" 1)) "a_key" "a") (LogicalPlan.Filter(b, eq "b_v" 1)) "b_key" "b") c "c_key" "c"

    [<Fact>]
    let ``Column statistics decide which filtered dimension joins first`` () =
        let result = LogicalOptimizer.optimizeComplete false false (measuredSources None) (total "v" measuredStar)
        Assert.Equal<int list>([ 0; 1; 2; 3 ], slots result)

    [<Fact>]
    let ``A dimension the fact table barely uses keeps more of the fact table than its filter suggests`` () =
        let result = LogicalOptimizer.optimizeComplete false false (measuredSources (Some 10.0)) (total "v" measuredStar)
        Assert.Equal<int list>([ 0; 2; 1; 3 ], slots result)

    [<Fact>]
    let ``An unfiltered dimension wider than the fact table's keys does not count as fanning out`` () =
        let plan = join (join (join fact a "a_key" "a") (LogicalPlan.Filter(b, eq "b_v" 1)) "b_key" "b") c "c_key" "c"
        let result = LogicalOptimizer.optimizeComplete false false (measuredSources (Some 10.0)) (total "v" plan)
        Assert.Equal<int list>([ 0; 2; 1; 3 ], slots result)

    [<Fact>]
    let ``Without statistics the filters still decide`` () =
        let result = LogicalOptimizer.optimizeComplete false false (star [ None; None; None; None ]) (total "v" written)
        Assert.Equal(2, (slots result).[1])

    [<Fact>]
    let ``A join whose order is observed keeps its written order`` () =
        Assert.Equal(written, LogicalOptimizer.optimizeComplete false false withStats written)

    [<Fact>]
    let ``Beneath a sort that can tie, joins are reordered only when ties are unspecified`` () =
        let sorted = LogicalPlan.Sort(written, [ { SortOrder.Expr = col "v"; Descending = false; NullsLast = false } ])
        Assert.Equal<int list>([ 0; 1; 2; 3 ], slots (LogicalOptimizer.optimizeComplete false false withStats sorted))
        Assert.Equal<int list>([ 0; 2; 1; 3 ], slots (LogicalOptimizer.optimizeComplete true false withStats sorted))

    [<Fact>]
    let ``Unspecified ties do not free a limit without a sort`` () =
        let limited = LogicalPlan.Limit(written, 0L, 5u)
        Assert.Equal<int list>([ 0; 1; 2; 3 ], slots (LogicalOptimizer.optimizeComplete true false withStats limited))

    [<Fact>]
    let ``A shared subplan is freed only when every consumer leaves its order unobserved`` () =
        let body = LogicalPlan.Shared(0, written)
        let sorted = LogicalPlan.Sort(body, [ { SortOrder.Expr = col "v"; Descending = false; NullsLast = false } ])
        let bothFree = LogicalPlan.Union(sorted, sorted, true)
        Assert.Equal<int list>([ 0; 2; 1; 3; 0; 2; 1; 3 ], slots (LogicalOptimizer.optimizeComplete true false withStats bothFree))

        let oneLimited = LogicalPlan.Union(sorted, LogicalPlan.Limit(body, 0L, 5u), true)
        Assert.Equal<int list>([ 0; 1; 2; 3; 0; 1; 2; 3 ], slots (LogicalOptimizer.optimizeComplete true false withStats oneLimited))

    [<Fact>]
    let ``A filter above an unordered join keeps every column it reads`` () =
        let both = ScalarExpr.Binary(BinaryOp.Gt, col "b_v", col "c_v")
        let result = LogicalOptimizer.optimizeComplete false false withStats (total "v" (LogicalPlan.Filter(written, both)))
        let rec filterAboveBothSides (plan: LogicalPlan) =
            match plan with
            | LogicalPlan.Filter(input, p) when p = both ->
                let leaves = slots input
                List.contains 2 leaves && List.contains 3 leaves
            | other -> LogicalPlanOps.getInputs other |> List.exists filterAboveBothSides
        Assert.True(filterAboveBothSides result, LogicalPlanOps.formatPlan result)

    [<Fact>]
    let ``Columns of the written joins are restored, including a suffixed self-join column`` () =
        let dim2 = scan 2 aSchema
        let selfJoin =
            let first = join fact (LogicalPlan.Filter(a, eq "a_v" 1)) "a_key" "a"
            LogicalPlan.Join(first, dim2, [ col "b_key" ], [ col "a" ], JoinKind.Inner, Some "_right", JoinKeyCoalescing.Default, false)
        let withC = join selfJoin c "c_key" "c"
        let plan = total "a_v_right" withC
        let schemas = sources [ Some 5_000_000L; Some 10_000L; Some 10_000L; Some 100L ] [ factSchema; aSchema; aSchema; cSchema ]

        let result = LogicalOptimizer.optimizeComplete false false schemas plan

        let rec reordered (node: LogicalPlan) =
            match node with
            | LogicalPlan.Project(LogicalPlan.Join _, projections) -> Some projections
            | other -> LogicalPlanOps.getInputs other |> List.tryPick reordered
        match reordered result with
        | Some projections ->
            let right = projections |> List.find (fun (name, _) -> name = "a_v_right") |> snd
            Assert.Equal(col "__jr2_a_v", right)
        | None -> failwithf "not reordered: %s" (LogicalPlanOps.formatPlan result)

    [<Fact>]
    let ``Outer joins are opaque, and two relations are left alone`` () =
        let leftJoin = LogicalPlan.Join(join fact a "a_key" "a", b, [ col "b_key" ], [ col "b" ], JoinKind.Left, None, JoinKeyCoalescing.Default, false)
        let plan = total "v" leftJoin
        Assert.Equal<int list>([ 0; 1; 2 ], slots (LogicalOptimizer.optimizeComplete false false withStats plan))

    [<Fact>]
    let ``A disconnected join graph keeps its written order`` () =
        // The last join's key belongs to no relation joined before it through an edge between the first two groups.
        let cross = LogicalPlan.Join(join fact a "a_key" "a", b, [], [], JoinKind.Cross, None, JoinKeyCoalescing.Default, false)
        let plan = total "v" (join cross c "c_key" "c")
        Assert.Equal<int list>([ 0; 1; 2; 3 ], slots (LogicalOptimizer.optimizeComplete false false withStats plan))

    // --- the order analysis that frees joins beneath windows ---

    let private partitioned = ints [ "p"; "y"; "v" ]
    let private groups = LogicalPlan.GroupBy(scan 0 partitioned, [ col "p"; col "y" ], [ "s", sum "v" ], None, true)
    let private asc name = { SortOrder.Expr = col name; Descending = false; NullsLast = false }

    let rec private groupFlags (plan: LogicalPlan) : bool list =
        let own =
            match plan with
            | LogicalPlan.GroupBy(_, _, _, _, maintain) -> [ maintain ]
            | _ -> []
        own @ (LogicalPlanOps.getInputs plan |> List.collect groupFlags)

    [<Fact>]
    let ``A window reading order inside partitions frees groups covered by its partition and the sort`` () =
        let shift = ScalarExpr.Window(col "s", [ col "p" ], [])
        let plan = LogicalPlan.WithColumns(LogicalPlan.Sort(groups, [ asc "y" ]), [ "prev", shift ])
        let outer = LogicalPlan.Sort(plan, [ asc "p"; asc "y" ])
        Assert.Equal<bool list>([ false ], groupFlags (Polars.NET.Linq.IR.Passes.GroupOrderRelaxation.apply outer))

    [<Fact>]
    let ``A partition that does not cover the keys keeps the group order`` () =
        let shift = ScalarExpr.Window(col "s", [ col "y" ], [])
        let plan = LogicalPlan.WithColumns(LogicalPlan.Sort(groups, [ asc "y" ]), [ "prev", shift ])
        let outer = LogicalPlan.Sort(plan, [ asc "y" ])
        Assert.Equal<bool list>([ true ], groupFlags (Polars.NET.Linq.IR.Passes.GroupOrderRelaxation.apply outer))
