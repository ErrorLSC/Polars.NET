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

    [<Fact>]
    let ``A filter reading other rows stops the reduction`` () =
        let ranked = ScalarExpr.Binary(BinaryOp.Eq, ScalarExpr.Window(col "f_v", [ col "f_t" ], []), ScalarExpr.Literal(LiteralValue.Int32 1))
        let plan = inner (LogicalPlan.Filter(inner facts returns "f_k" "r_k", ranked)) selective "f_k" "d_k"

        Assert.Equal(plan, SemiJoinReduction.apply sources plan)

    [<Fact>]
    let ``A shared subplan is not reduced for one consumer`` () =
        let plan = inner (LogicalPlan.Shared(0, inner facts returns "f_k" "r_k")) selective "f_k" "d_k"

        Assert.Equal(plan, SemiJoinReduction.apply sources plan)

    [<Fact>]
    let ``A left join reduces its left side only`` () =
        let leftJoin = LogicalPlan.Join(inner facts returns "f_t" "r_t", returns, [ col "f_k" ], [ col "r_k" ], JoinKind.Left, Some "_right", JoinKeyCoalescing.Default, false)
        let plan = inner leftJoin selective "f_k" "d_k"

        let reducedLeft = inner (semi facts "f_k") returns "f_t" "r_t"
        let expected =
            inner (LogicalPlan.Join(reducedLeft, returns, [ col "f_k" ], [ col "r_k" ], JoinKind.Left, Some "_right", JoinKeyCoalescing.Default, false)) selective "f_k" "d_k"
        Assert.Equal(expected, SemiJoinReduction.apply sources plan)
