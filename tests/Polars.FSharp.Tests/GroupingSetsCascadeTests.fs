namespace Polars.FSharp.Tests

open Xunit
open Polars.NET.Linq.IR
open Polars.NET.Linq.IR.ScalarExprOps
open Polars.NET.Linq.IR.Passes

/// GroupingSetsCascade over hand-built plans: when a level may be computed from a finer one.
module GroupingSetsCascadeTests =

    let private schema =
        LogicalSchema.OfFields [ ("a", LogicalTypes.Int32); ("b", LogicalTypes.Int32); ("v", LogicalTypes.Int64); ("x", LogicalTypes.Float64) ]

    let private scan = LogicalPlan.Scan(LogicalPlanSource.Slot 0, schema)
    let private agg kind column = ScalarExpr.Aggregation(kind, Some(col column), false)

    let private rollup aggs levels =
        LogicalPlan.GroupingSets(
            scan,
            {
                KeyAliases = [ "a", col "a"; "b", col "b" ]
                Aggs = aggs
                Levels = levels
                OutputColumns = [ "a"; "b" ] @ (aggs |> List.map fst)
                LevelMerges = None
            }
        )

    let private levels = [ [ 0; 1 ], Map.empty; [ 0 ], Map.empty; [], Map.empty ]

    let private merges plan =
        match GroupingSetsCascade.apply plan with
        | LogicalPlan.GroupingSets(_, spec) -> spec.LevelMerges
        | other -> failwithf "unexpected plan %s" (LogicalPlanOps.formatPlan other)

    [<Fact>]
    let ``Sum, Min and Max merge over their own aliases`` () =
        let plan = rollup [ "s", agg AggKind.Sum "v"; "lo", agg AggKind.Min "x"; "hi", agg AggKind.Max "x" ] levels

        let expected = [ "s", agg AggKind.Sum "s"; "lo", agg AggKind.Min "lo"; "hi", agg AggKind.Max "hi" ]
        Assert.Equal(Some expected, merges plan)

    [<Fact>]
    let ``One aggregate that cannot merge leaves every level on the input`` () =
        for aggregate in
            [ agg AggKind.Sum "x"
              agg AggKind.CheckedSum "v"
              agg AggKind.Mean "v"
              ScalarExpr.Aggregation(AggKind.Sum, Some(col "v"), true)
              ScalarExpr.Binary(BinaryOp.Add, agg AggKind.Sum "v", agg AggKind.Sum "v") ] do
            Assert.Equal(None, merges (rollup [ "s", agg AggKind.Sum "v"; "other", aggregate ] levels))

    [<Fact>]
    let ``A single level has nothing to merge`` () =
        Assert.Equal(None, merges (rollup [ "s", agg AggKind.Sum "v" ] [ [ 0; 1 ], Map.empty ]))
