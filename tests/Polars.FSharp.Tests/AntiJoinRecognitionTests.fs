namespace Polars.FSharp.Tests

open Xunit
open Polars.NET.Linq.IR
open Polars.NET.Linq.IR.ScalarExprOps
open Polars.NET.Linq.IR.Passes

/// AntiJoinRecognition over hand-built plans: which left joins become anti joins, and which joins move beneath them.
module AntiJoinRecognitionTests =

    let private factSchema = LogicalSchema.OfFields [ ("f_k", LogicalTypes.Int32); ("f_t", LogicalTypes.Int32); ("f_v", LogicalTypes.Int64) ]
    let private returnSchema = LogicalSchema.OfFields [ ("r_k", LogicalTypes.Int32); ("r_x", LogicalTypes.Int32) ]
    let private dimSchema = LogicalSchema.OfFields [ ("d_k", LogicalTypes.Int32); ("d_a", LogicalTypes.Int32) ]

    let private facts = LogicalPlan.Scan(LogicalPlanSource.Slot 0, factSchema)
    let private returns = LogicalPlan.Scan(LogicalPlanSource.Slot 1, returnSchema)
    let private dims = LogicalPlan.Scan(LogicalPlanSource.Slot 2, dimSchema)
    let private filtered = LogicalPlan.Filter(dims, ScalarExpr.Binary(BinaryOp.Eq, col "d_a", lit 1))

    let private join how nullsEqual left right leftKey rightKey =
        LogicalPlan.Join(left, right, [ col leftKey ], [ col rightKey ], how, None, JoinKeyCoalescing.Default, nullsEqual)

    let private left = join JoinKind.Left false facts returns "f_k" "r_k"
    let private anti = join JoinKind.Anti false facts returns "f_k" "r_k"
    let private isNull name = ScalarExpr.Unary(UnaryOp.IsNull, col name)
    let private unmatched = LogicalPlan.Filter(left, isNull "r_k")
    let private grouped input key = LogicalPlan.GroupBy(input, [ col key ], [ "total", ScalarExpr.Aggregation(AggKind.Sum, Some(col "f_v"), false) ], None, true)
    let private assertUnchanged plan = Assert.Equal(plan, AntiJoinRecognition.apply plan)

    [<Fact>]
    let ``A left join kept for its unmatched rows becomes an anti join`` () =
        Assert.Equal(grouped anti "f_t", AntiJoinRecognition.apply (grouped unmatched "f_t"))

    [<Fact>]
    let ``A projection above counts as not reading the right side`` () =
        let plan = LogicalPlan.Project(unmatched, [ "t", col "f_t" ])
        Assert.Equal(LogicalPlan.Project(anti, [ "t", col "f_t" ]), AntiJoinRecognition.apply plan)

    [<Fact>]
    let ``A filtered dimension moves beneath the anti join`` () =
        let plan = grouped (join JoinKind.Inner false unmatched filtered "f_t" "d_k") "d_a"

        let expected = grouped (join JoinKind.Anti false (join JoinKind.Inner false facts filtered "f_t" "d_k") returns "f_k" "r_k") "d_a"
        Assert.Equal(expected, AntiJoinRecognition.apply plan)

    [<Fact>]
    let ``An unfiltered dimension stays above the anti join`` () =
        let plan = grouped (join JoinKind.Inner false unmatched dims "f_t" "d_k") "d_a"

        Assert.Equal(grouped (join JoinKind.Inner false anti dims "f_t" "d_k") "d_a", AntiJoinRecognition.apply plan)

    [<Fact>]
    let ``Reading a right column above keeps the left join`` () = assertUnchanged (grouped unmatched "r_x")

    [<Fact>]
    let ``A right column read by a filter above keeps the left join`` () =
        assertUnchanged (grouped (LogicalPlan.Filter(unmatched, ScalarExpr.Binary(BinaryOp.Eq, col "r_x", lit 1))) "f_t")

    [<Fact>]
    let ``Testing a right column that is not the key keeps the left join`` () =
        assertUnchanged (grouped (LogicalPlan.Filter(left, isNull "r_x")) "f_t")

    [<Fact>]
    let ``Null-safe keys keep the left join`` () =
        assertUnchanged (grouped (LogicalPlan.Filter(join JoinKind.Left true facts returns "f_k" "r_k", isNull "r_k")) "f_t")

    [<Fact>]
    let ``The plan's own columns keep the left join`` () = assertUnchanged unmatched
