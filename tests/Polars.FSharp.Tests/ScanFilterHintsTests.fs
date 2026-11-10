namespace Polars.FSharp.Tests

open Xunit
open Kaminari.Samidare.IR
open Kaminari.Samidare.IR.ScalarExprOps

/// ScanFilterHints over hand-built plans: which filter each filter-aware source is told about.
module ScanFilterHintsTests =

    let private schema = LogicalSchema.OfFields [ ("k", LogicalTypes.Int32); ("v", LogicalTypes.Int32) ]

    /// A filter-aware source that remembers the hint it was narrowed with.
    type private Aware(hint: ScalarExpr option) =
        member _.Hint = hint
        interface ILogicalTableSource with
            member _.Name = "aware"
            member _.Schema = schema
        interface IFilterAwareSource with
            member _.WithFilterHint predicate = Aware(Some predicate)

    type private Plain() =
        interface ILogicalTableSource with
            member _.Name = "plain"
            member _.Schema = schema

    let private scan slot = LogicalPlan.Scan(LogicalPlanSource.Slot slot, schema)
    let private eq name (value: int) = ScalarExpr.Binary(BinaryOp.Eq, col name, ScalarExpr.Literal(LiteralValue.Int32 value))
    let private bin op l r = ScalarExpr.Binary(op, l, r)

    let private hintOf (sources: SourceBinding array) slot =
        match sources.[slot] with
        | SourceBinding.Table(:? Aware as a) -> a.Hint
        | _ -> failwith "not an aware source"

    let private apply plan =
        ScanFilterHints.apply [| SourceBinding.Table(Aware None); SourceBinding.Table(Plain()) |] plan

    [<Fact>]
    let ``Stacked filters on a scan are combined`` () =
        let plan = LogicalPlan.Project(LogicalPlan.Filter(LogicalPlan.Filter(scan 0, eq "k" 1), eq "v" 2), [ "k", col "k" ])
        Assert.Equal(Some(bin BinaryOp.And (eq "k" 1) (eq "v" 2)), hintOf (apply plan) 0)

    [<Fact>]
    let ``Two filtered scans of a slot give their disjunction`` () =
        let plan = LogicalPlan.Union(LogicalPlan.Filter(scan 0, eq "k" 1), LogicalPlan.Filter(scan 0, eq "k" 2), true)
        Assert.Equal(Some(bin BinaryOp.Or (eq "k" 1) (eq "k" 2)), hintOf (apply plan) 0)

    [<Fact>]
    let ``One unfiltered scan of a slot means no hint`` () =
        let plan = LogicalPlan.Union(LogicalPlan.Filter(scan 0, eq "k" 1), scan 0, true)
        Assert.Equal(None, hintOf (apply plan) 0)
        // A filter that is not right on the scan says nothing about the scan's rows.
        let above = LogicalPlan.Filter(LogicalPlan.Limit(scan 0, 0L, 5u), eq "k" 1)
        Assert.Equal(None, hintOf (apply above) 0)

    [<Fact>]
    let ``A subquery anywhere makes the plan opaque`` () =
        let sub = ScalarExpr.Subquery(scan 0, SubqueryKind.ScalarScalar)
        let plan = LogicalPlan.Filter(LogicalPlan.Filter(scan 0, eq "k" 1), bin BinaryOp.Eq (col "v") sub)
        Assert.Equal(None, hintOf (apply plan) 0)

    [<Fact>]
    let ``Sources that are not filter aware are left alone`` () =
        let sources = [| SourceBinding.Table(Plain()) |]
        Assert.Same(sources, ScanFilterHints.apply sources (LogicalPlan.Filter(scan 0, eq "k" 1)))
