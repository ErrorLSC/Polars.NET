namespace Polars.FSharp.Tests

open Xunit
open Polars.NET.Linq.IR
open Polars.NET.Linq.IR.ScalarExprOps
open Polars.NET.Linq.IR.Passes

/// OuterJoinSimplification over hand-built plans: which left joins become inner joins.
module OuterJoinSimplificationTests =

    let private factSchema = LogicalSchema.OfFields [ ("f_k", LogicalTypes.Int32); ("f_t", LogicalTypes.Int32) ]
    let private returnSchema = LogicalSchema.OfFields [ ("r_k", LogicalTypes.Int32); ("r_x", LogicalTypes.Int32) ]
    let private dimSchema = LogicalSchema.OfFields [ ("d_k", LogicalTypes.Int32) ]

    let private facts = LogicalPlan.Scan(LogicalPlanSource.Slot 0, factSchema)
    let private returns = LogicalPlan.Scan(LogicalPlanSource.Slot 1, returnSchema)
    let private dims = LogicalPlan.Scan(LogicalPlanSource.Slot 2, dimSchema)

    let private join how nullsEqual left right leftKey rightKey =
        LogicalPlan.Join(left, right, [ col leftKey ], [ col rightKey ], how, None, JoinKeyCoalescing.KeepBoth, nullsEqual)

    let private left = join JoinKind.Left false facts returns "f_k" "r_k"
    let private inner = join JoinKind.Inner false facts returns "f_k" "r_k"

    [<Fact>]
    let ``An inner join on a right column makes the left join inner`` () =
        let plan = join JoinKind.Inner false left dims "r_x" "d_k"

        Assert.Equal(join JoinKind.Inner false inner dims "r_x" "d_k", OuterJoinSimplification.apply plan)

    [<Fact>]
    let ``An inner join on a left column keeps the left join`` () =
        let plan = join JoinKind.Inner false left dims "f_t" "d_k"
        Assert.Equal(plan, OuterJoinSimplification.apply plan)

    [<Fact>]
    let ``A null-safe inner join keeps the left join`` () =
        let plan = join JoinKind.Inner true left dims "r_x" "d_k"
        Assert.Equal(plan, OuterJoinSimplification.apply plan)

    [<Fact>]
    let ``A left join above keeps the left join`` () =
        let plan = join JoinKind.Left false left dims "r_x" "d_k"
        Assert.Equal(plan, OuterJoinSimplification.apply plan)
