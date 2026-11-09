namespace Polars.FSharp.Tests

open Xunit
open Polars.NET.Linq.IR
open Polars.NET.Linq.IR.ScalarExprOps
open Polars.NET.Linq.IR.Passes

/// SemiJoinReduction over hand-built plans: which tables get a semi join with the selective table.
module SemiJoinReductionTests =

    type private Table(name: string, schema: LogicalSchema, rows: int64) =
        interface ILogicalTableSource with
            member _.Name = name
            member _.Schema = schema
        interface ISourceStatistics with
            member _.EstimatedRows = Some rows

    let private factSchema = LogicalSchema.OfFields [ ("f_k", LogicalTypes.Int32); ("f_t", LogicalTypes.Int32); ("f_v", LogicalTypes.Int32) ]
    let private returnSchema = LogicalSchema.OfFields [ ("r_k", LogicalTypes.Int32); ("r_t", LogicalTypes.Int32) ]
    let private dimSchema = LogicalSchema.OfFields [ ("d_k", LogicalTypes.Int32); ("d_a", LogicalTypes.Int32); ("d_b", LogicalTypes.Int32) ]

    let private sources =
        [|
            SourceBinding.Table(Table("facts", factSchema, 5_000_000L))
            SourceBinding.Table(Table("returns", returnSchema, 500_000L))
            SourceBinding.Table(Table("dims", dimSchema, 26_000L))
        |]

    let private facts = LogicalPlan.Scan(LogicalPlanSource.Slot 0, factSchema)
    let private returns = LogicalPlan.Scan(LogicalPlanSource.Slot 1, returnSchema)
    let private dims = LogicalPlan.Scan(LogicalPlanSource.Slot 2, dimSchema)

    let private eq name (value: int) = ScalarExpr.Binary(BinaryOp.Eq, col name, ScalarExpr.Literal(LiteralValue.Int32 value))

    /// Keeps about 1% of the table by the fixed selectivities.
    let private selective = LogicalPlan.Filter(LogicalPlan.Filter(dims, eq "d_a" 1), eq "d_b" 2)

    let private inner left right leftKey rightKey =
        LogicalPlan.Join(left, right, [ col leftKey ], [ col rightKey ], JoinKind.Inner, None, JoinKeyCoalescing.Default, false)

    let private semi input key =
        LogicalPlan.Join(input, selective, [ col key ], [ col "d_k" ], JoinKind.Semi, None, JoinKeyCoalescing.Default, false)

    [<Fact>]
    let ``An expensive join is reduced on both sides through its key equality`` () =
        let plan = inner (inner facts returns "f_k" "r_k") selective "f_k" "d_k"

        let expected = inner (inner (semi facts "f_k") (semi returns "r_k") "f_k" "r_k") selective "f_k" "d_k"
        Assert.Equal(expected, SemiJoinReduction.apply sources plan)

    [<Fact>]
    let ``A key that is not the join key reduces only its own side`` () =
        let plan = inner (inner facts returns "f_t" "r_t") selective "f_k" "d_k"

        let expected = inner (inner (semi facts "f_k") returns "f_t" "r_t") selective "f_k" "d_k"
        Assert.Equal(expected, SemiJoinReduction.apply sources plan)

    [<Fact>]
    let ``A grouping on the key is reduced beneath it`` () =
        let grouped = LogicalPlan.GroupBy(facts, [ ScalarExpr.Alias(col "f_k", "key") ], [ "total", ScalarExpr.Aggregation(AggKind.Sum, Some(col "f_v"), false) ], None, true)
        let plan = inner grouped selective "key" "d_k"

        let reduced = LogicalPlan.GroupBy(semi facts "f_k", [ ScalarExpr.Alias(col "f_k", "key") ], [ "total", ScalarExpr.Aggregation(AggKind.Sum, Some(col "f_v"), false) ], None, true)
        Assert.Equal(inner reduced selective "key" "d_k", SemiJoinReduction.apply sources plan)

    [<Fact>]
    let ``A grouping on another column is left alone`` () =
        let grouped = LogicalPlan.GroupBy(facts, [ col "f_k" ], [ "f_t", ScalarExpr.Aggregation(AggKind.Sum, Some(col "f_t"), false) ], None, true)
        let plan = inner grouped selective "f_t" "d_k"

        Assert.Equal(plan, SemiJoinReduction.apply sources plan)

    [<Fact>]
    let ``A table directly joined with the selective table is left alone`` () =
        let plan = inner facts selective "f_k" "d_k"

        Assert.Equal(plan, SemiJoinReduction.apply sources plan)

    [<Fact>]
    let ``A table with a weak filter does not reduce`` () =
        let weak = LogicalPlan.Filter(dims, ScalarExpr.Unary(UnaryOp.IsNotNull, col "d_a"))
        let plan = inner (inner facts returns "f_k" "r_k") weak "f_k" "d_k"

        Assert.Equal(plan, SemiJoinReduction.apply sources plan)

    [<Fact>]
    let ``A single equality is too weak to reduce`` () =
        let plan = inner (inner facts returns "f_k" "r_k") (LogicalPlan.Filter(dims, eq "d_a" 1)) "f_k" "d_k"

        Assert.Equal(plan, SemiJoinReduction.apply sources plan)

    let private compare op name (value: int) = ScalarExpr.Binary(op, col name, ScalarExpr.Literal(LiteralValue.Int32 value))

    let private reducedBy (filter: LogicalPlan) =
        let plan = inner (inner facts returns "f_t" "r_t") filter "f_k" "d_k"
        let semiFacts = LogicalPlan.Join(facts, filter, [ col "f_k" ], [ col "d_k" ], JoinKind.Semi, None, JoinKeyCoalescing.Default, false)
        Assert.Equal(inner (inner semiFacts returns "f_t" "r_t") filter "f_k" "d_k", SemiJoinReduction.apply sources plan)

    let private notReducedBy (filter: LogicalPlan) =
        let plan = inner (inner facts returns "f_t" "r_t") filter "f_k" "d_k"
        Assert.Equal(plan, SemiJoinReduction.apply sources plan)

    [<Fact>]
    let ``A range bounded on both sides reduces`` () =
        reducedBy (LogicalPlan.Filter(LogicalPlan.Filter(dims, compare BinaryOp.GtEq "d_a" 1), compare BinaryOp.LtEq "d_a" 9))

    [<Fact>]
    let ``A range in one conjunction reduces`` () =
        reducedBy (LogicalPlan.Filter(dims, ScalarExpr.Binary(BinaryOp.And, compare BinaryOp.Gt "d_a" 1, compare BinaryOp.Lt "d_a" 9)))

    [<Fact>]
    let ``A range written with the literal first reduces`` () =
        let literalFirst op = ScalarExpr.Binary(op, ScalarExpr.Literal(LiteralValue.Int32 1), col "d_a")
        reducedBy (LogicalPlan.Filter(LogicalPlan.Filter(dims, literalFirst BinaryOp.LtEq), compare BinaryOp.Lt "d_a" 9))

    [<Fact>]
    let ``A bound on one side only does not reduce`` () =
        notReducedBy (LogicalPlan.Filter(dims, compare BinaryOp.GtEq "d_a" 1))

    [<Fact>]
    let ``Bounds on two columns do not reduce`` () =
        notReducedBy (LogicalPlan.Filter(LogicalPlan.Filter(dims, compare BinaryOp.GtEq "d_a" 1), compare BinaryOp.LtEq "d_b" 9))

    [<Fact>]
    let ``Two lower bounds on one column do not reduce`` () =
        notReducedBy (LogicalPlan.Filter(LogicalPlan.Filter(dims, compare BinaryOp.GtEq "d_a" 1), compare BinaryOp.Gt "d_a" 5))

    [<Fact>]
    let ``A filter reading other rows stops the reduction`` () =
        let ranked = ScalarExpr.Binary(BinaryOp.Eq, ScalarExpr.Window(col "f_v", [ col "f_t" ], []), ScalarExpr.Literal(LiteralValue.Int32 1))
        let plan = inner (LogicalPlan.Filter(inner facts returns "f_k" "r_k", ranked)) selective "f_k" "d_k"

        Assert.Equal(plan, SemiJoinReduction.apply sources plan)

    [<Fact>]
    let ``A shared subplan is not reduced for one consumer`` () =
        let plan = inner (LogicalPlan.Shared(0, inner facts returns "f_k" "r_k")) selective "f_k" "d_k"

        Assert.Equal(plan, SemiJoinReduction.apply sources plan)

    type private MeasuredTable(name: string, schema: LogicalSchema, rows: int64, column: string, statistics: ColumnStatistics, ranges: (string * (LiteralValue * LiteralValue)) list) =
        interface ILogicalTableSource with
            member _.Name = name
            member _.Schema = schema
        interface ISourceStatistics with
            member _.EstimatedRows = Some rows
        interface IColumnStatisticsSource with
            member _.TryGetColumnStatistics requested = if requested = column then Some statistics else None
            member _.TryGetColumnRange requested = ranges |> List.tryFind (fst >> (=) requested) |> Option.map snd

    let private measuredDims =
        let statistics: ColumnStatistics =
            { NullFraction = Some 0.0; DistinctCount = Some 100.0; Min = None; Max = None; FrequentValues = []; Origin = StatisticsOrigin.Exact }
        SourceBinding.Table(MeasuredTable("dims", dimSchema, 26_000L, "d_a", statistics, []))

    /// One equality, which only its statistics show to be selective.
    let private measured = LogicalPlan.Filter(dims, eq "d_a" 1)

    let private semiByMeasured input key =
        LogicalPlan.Join(input, measured, [ col key ], [ col "d_k" ], JoinKind.Semi, None, JoinKeyCoalescing.Default, false)

    [<Fact>]
    let ``A table selective by its statistics reduces past a large join`` () =
        let plan = inner (inner facts returns "f_t" "r_t") measured "f_k" "d_k"

        let expected = inner (inner (semiByMeasured facts "f_k") returns "f_t" "r_t") measured "f_k" "d_k"
        Assert.Equal(expected, SemiJoinReduction.apply [| sources.[0]; sources.[1]; measuredDims |] plan)

    let private dimsWithKeyRange =
        let statistics: ColumnStatistics =
            { NullFraction = Some 0.0; DistinctCount = Some 100.0; Min = None; Max = None; FrequentValues = []; Origin = StatisticsOrigin.Exact }
        let keys = [ "d_k", (LiteralValue.Float64 0.0, LiteralValue.Float64 1000.0) ]
        SourceBinding.Table(MeasuredTable("dims", dimSchema, 26_000L, "d_a", statistics, keys))

    let private factsWithKeyRange (high: float) =
        let unused: ColumnStatistics =
            { NullFraction = None; DistinctCount = None; Min = None; Max = None; FrequentValues = []; Origin = StatisticsOrigin.Exact }
        SourceBinding.Table(MeasuredTable("facts", factSchema, 5_000_000L, "", unused, [ "f_k", (LiteralValue.Float64 0.0, LiteralValue.Float64 high) ]))

    [<Fact>]
    let ``A fact table using most of the dimension's keys is reduced`` () =
        let plan = inner (inner facts returns "f_t" "r_t") measured "f_k" "d_k"

        let expected = inner (inner (semiByMeasured facts "f_k") returns "f_t" "r_t") measured "f_k" "d_k"
        Assert.Equal(expected, SemiJoinReduction.apply [| factsWithKeyRange 1000.0; sources.[1]; dimsWithKeyRange |] plan)

    [<Fact>]
    let ``A fact table using few of the dimension's keys is not reduced`` () =
        let plan = inner (inner facts returns "f_t" "r_t") measured "f_k" "d_k"
        Assert.Equal(plan, SemiJoinReduction.apply [| factsWithKeyRange 10.0; sources.[1]; dimsWithKeyRange |] plan)

    [<Fact>]
    let ``A table selective by its statistics does not reduce past a small join`` () =
        let small = SourceBinding.Table(Table("returns", returnSchema, 5_000L))
        let plan = inner (inner facts returns "f_t" "r_t") measured "f_k" "d_k"
        Assert.Equal(plan, SemiJoinReduction.apply [| sources.[0]; small; measuredDims |] plan)

    [<Fact>]
    let ``A left join alone is worth reducing`` () =
        let leftJoin input = LogicalPlan.Join(input, returns, [ col "f_k" ], [ col "r_k" ], JoinKind.Left, Some "_right", JoinKeyCoalescing.Default, false)

        Assert.Equal(inner (leftJoin (semi facts "f_k")) selective "f_k" "d_k", SemiJoinReduction.apply sources (inner (leftJoin facts) selective "f_k" "d_k"))

    let private distinctKeys input = LogicalPlan.Distinct(LogicalPlan.Project(input, [ "key", col "f_k" ]), None, DistinctKeep.First, false)
    let private joinOnKey left right = inner left right "d_k" "key"

    let private semiByKeys (keys: LogicalPlan) input key =
        LogicalPlan.Join(input, keys, [ col key ], [ col "d_k" ], JoinKind.Semi, None, JoinKeyCoalescing.Default, false)

    [<Fact>]
    let ``A small left input filters an expensive right input`` () =
        let plan = joinOnKey selective (distinctKeys (inner facts returns "f_k" "r_k"))

        let shared = LogicalPlan.Shared(0, selective)
        let keys = LogicalPlan.Project(shared, [ "d_k", col "d_k" ])
        let expected = joinOnKey shared (distinctKeys (inner (semiByKeys keys facts "f_k") (semiByKeys keys returns "r_k") "f_k" "r_k"))
        Assert.Equal(expected, SemiJoinReduction.apply sources plan)

    [<Fact>]
    let ``A shared table on the right is filtered above the shared node`` () =
        let sharedFacts = LogicalPlan.Shared(4, facts)
        let plan = joinOnKey selective (distinctKeys (inner sharedFacts returns "f_k" "r_k"))

        let shared = LogicalPlan.Shared(5, selective)
        let keys = LogicalPlan.Project(shared, [ "d_k", col "d_k" ])
        let expected = joinOnKey shared (distinctKeys (inner (semiByKeys keys sharedFacts "f_k") (semiByKeys keys returns "r_k") "f_k" "r_k"))
        Assert.Equal(expected, SemiJoinReduction.apply sources plan)

    [<Fact>]
    let ``A right input with nothing expensive is left alone`` () =
        let plan = joinOnKey selective (distinctKeys facts)
        Assert.Equal(plan, SemiJoinReduction.apply sources plan)

    [<Fact>]
    let ``A left input not much smaller than the table is left alone`` () =
        let plan = inner returns (distinctKeys (inner facts returns "f_k" "r_k")) "r_k" "key"
        Assert.Equal(plan, SemiJoinReduction.apply sources plan)

    [<Fact>]
    let ``A right input with the key on one side of its join only is left alone`` () =
        let plan = joinOnKey selective (distinctKeys (inner facts returns "f_t" "r_t"))
        Assert.Equal(plan, SemiJoinReduction.apply sources plan)

    [<Fact>]
    let ``A left join reduces its left side only`` () =
        let leftJoin = LogicalPlan.Join(inner facts returns "f_t" "r_t", returns, [ col "f_k" ], [ col "r_k" ], JoinKind.Left, Some "_right", JoinKeyCoalescing.Default, false)
        let plan = inner leftJoin selective "f_k" "d_k"

        let reducedLeft = inner (semi facts "f_k") returns "f_t" "r_t"
        let expected =
            inner (LogicalPlan.Join(reducedLeft, returns, [ col "f_k" ], [ col "r_k" ], JoinKind.Left, Some "_right", JoinKeyCoalescing.Default, false)) selective "f_k" "d_k"
        Assert.Equal(expected, SemiJoinReduction.apply sources plan)

    // A join on two columns, against a large table.

    let private salesSchema = LogicalSchema.OfFields [ ("s_i", LogicalTypes.Int32); ("s_d", LogicalTypes.Int32) ]
    let private stockSchema = LogicalSchema.OfFields [ ("t_i", LogicalTypes.Int32); ("t_d", LogicalTypes.Int32) ]
    let private yearsSchema = LogicalSchema.OfFields [ ("y_k", LogicalTypes.Int32); ("y_w", LogicalTypes.Int32); ("y_y", LogicalTypes.Int32) ]
    let private weeksSchema = LogicalSchema.OfFields [ ("w_k", LogicalTypes.Int32); ("w_w", LogicalTypes.Int32) ]

    let private compositeSources =
        [|
            SourceBinding.Table(Table("sales", salesSchema, 2_000_000L))
            SourceBinding.Table(Table("stock", stockSchema, 17_000_000L))
            SourceBinding.Table(Table("years", yearsSchema, 70_000L))
            SourceBinding.Table(Table("weeks", weeksSchema, 70_000L))
        |]

    let private sales = LogicalPlan.Scan(LogicalPlanSource.Slot 0, salesSchema)
    let private stock = LogicalPlan.Scan(LogicalPlanSource.Slot 1, stockSchema)
    let private years = LogicalPlan.Scan(LogicalPlanSource.Slot 2, yearsSchema)
    let private weeks = LogicalPlan.Scan(LogicalPlanSource.Slot 3, weeksSchema)
    let private oneYear = LogicalPlan.Filter(years, eq "y_y" 1999)

    let private composite left right leftKeys rightKeys =
        LogicalPlan.Join(left, right, leftKeys |> List.map col, rightKeys |> List.map col, JoinKind.Inner, None, JoinKeyCoalescing.Default, false)

    let private semiOn table key relation column =
        LogicalPlan.Join(table, relation, [ col key ], [ col column ], JoinKind.Semi, None, JoinKeyCoalescing.Default, false)

    let private applyComposite = SemiJoinReduction.apply compositeSources

    [<Fact>]
    let ``A composite join filters its table by a key that a joined dimension narrows`` () =
        let chain = inner (inner sales oneYear "s_d" "y_k") weeks "y_w" "w_w"
        let plan = composite chain stock [ "s_i"; "w_k" ] [ "t_i"; "t_d" ]

        let keySet = inner oneYear weeks "y_w" "w_w"
        let expected = composite chain (semiOn stock "t_d" keySet "w_k") [ "s_i"; "w_k" ] [ "t_i"; "t_d" ]
        Assert.Equal(expected, applyComposite plan)

    [<Fact>]
    let ``A composite join filters by a key of a filtered dimension directly`` () =
        let chain = inner sales oneYear "s_d" "y_k"
        let plan = composite chain stock [ "s_i"; "y_k" ] [ "t_i"; "t_d" ]

        let expected = composite chain (semiOn stock "t_d" oneYear "y_k") [ "s_i"; "y_k" ] [ "t_i"; "t_d" ]
        Assert.Equal(expected, applyComposite plan)

    [<Fact>]
    let ``A composite join follows its key through a projection`` () =
        let chain = LogicalPlan.Project(inner sales oneYear "s_d" "y_k", [ "item", col "s_i"; "day", col "y_k" ])
        let plan = composite chain stock [ "item"; "day" ] [ "t_i"; "t_d" ]

        let expected = composite chain (semiOn stock "t_d" oneYear "y_k") [ "item"; "day" ] [ "t_i"; "t_d" ]
        Assert.Equal(expected, applyComposite plan)

    [<Fact>]
    let ``Keys that only come from the fact table do not reduce`` () =
        let plan = composite sales stock [ "s_i"; "s_d" ] [ "t_i"; "t_d" ]

        Assert.Equal(plan, applyComposite plan)

    [<Fact>]
    let ``A dimension without any filter does not reduce`` () =
        let chain = inner (inner sales years "s_d" "y_k") weeks "y_w" "w_w"
        let plan = composite chain stock [ "s_i"; "w_k" ] [ "t_i"; "t_d" ]

        Assert.Equal(plan, applyComposite plan)

    [<Fact>]
    let ``A table not much larger than the key set does not reduce`` () =
        let small =
            [|
                compositeSources.[0]
                SourceBinding.Table(Table("stock", stockSchema, 100_000L))
                compositeSources.[2]
                compositeSources.[3]
            |]
        let plan = composite (inner sales oneYear "s_d" "y_k") stock [ "s_i"; "y_k" ] [ "t_i"; "t_d" ]

        Assert.Equal(plan, SemiJoinReduction.apply small plan)

    [<Fact>]
    let ``A key on the outer side of a left join does not reduce`` () =
        let outer = LogicalPlan.Join(sales, oneYear, [ col "s_d" ], [ col "y_k" ], JoinKind.Left, None, JoinKeyCoalescing.Default, false)
        let plan = composite outer stock [ "s_i"; "y_y" ] [ "t_i"; "t_d" ]

        Assert.Equal(plan, applyComposite plan)

    [<Fact>]
    let ``A joined side that is not a table does not reduce`` () =
        let grouped = LogicalPlan.GroupBy(stock, [ col "t_i"; col "t_d" ], [], None, true)
        let plan = composite (inner sales oneYear "s_d" "y_k") grouped [ "s_i"; "y_k" ] [ "t_i"; "t_d" ]

        Assert.Equal(plan, applyComposite plan)
