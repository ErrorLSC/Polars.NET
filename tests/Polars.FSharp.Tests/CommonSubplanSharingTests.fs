namespace Polars.FSharp.Tests

open Xunit
open Polars.NET.Linq.IR
open Polars.NET.Linq.IR.ScalarExprOps
open Polars.NET.Linq.IR.Passes

/// CommonSubplanSharing over hand-built plans: which repeated subplans become Shared, with what body.
module CommonSubplanSharingTests =

    type private Table(name: string, schema: LogicalSchema) =
        interface ILogicalTableSource with
            member _.Name = name
            member _.Schema = schema

    let private factSchema = LogicalSchema.OfFields [ ("k", LogicalTypes.Int32); ("v", LogicalTypes.Int32) ]
    let private dimSchema = LogicalSchema.OfFields [ ("dk", LogicalTypes.Int32); ("name", LogicalTypes.String) ]
    let private facts = Table("facts", factSchema) :> ILogicalTableSource
    let private dims = Table("dims", dimSchema) :> ILogicalTableSource

    /// A backend sharing the subplans that `sharesItself` selects by itself.
    let private backendSharing (sharesItself: LogicalPlan -> bool) =
        { new IBackendCapabilities with
            member _.IsRegexPatternSupported _ = true
            member _.SupportsAssertions = true
            member _.SharesRepeatedSubplanItself subplan = sharesItself subplan }

    let private backend = backendSharing (fun _ -> false)

    let private scan slot schema = LogicalPlan.Scan(LogicalPlanSource.Slot slot, schema)
    let private sum = ScalarExpr.Aggregation(AggKind.Sum, Some(col "v"), false)

    /// facts ⋈ dims grouped by name, reading the given slots.
    let private aggregate factSlot dimSlot =
        let joined =
            LogicalPlan.Join(scan factSlot factSchema, scan dimSlot dimSchema, [ col "k" ], [ col "dk" ], JoinKind.Inner, None, JoinKeyCoalescing.Default, false)
        LogicalPlan.GroupBy(joined, [ col "name" ], [ "total", sum ], None, true)

    let private gt name (value: int) = ScalarExpr.Binary(BinaryOp.Gt, col name, ScalarExpr.Literal(LiteralValue.Int32 value))

    /// (id, input) of every Shared node, in pre-order.
    let rec private shared (plan: LogicalPlan) : (int * LogicalPlan) list =
        let own =
            match plan with
            | LogicalPlan.Shared(id, input) -> [ id, input ]
            | _ -> []
        own @ (LogicalPlanOps.getInputs plan |> List.collect shared)

    [<Fact>]
    let ``A subplan repeated over the same tables is shared, with one body`` () =
        // Each reference to the query read its tables through slots of its own.
        let sources = [| SourceBinding.Table facts; SourceBinding.Table dims; SourceBinding.Table facts; SourceBinding.Table dims |]
        let plan = LogicalPlan.Union(aggregate 0 1, aggregate 2 3, true)

        let result = CommonSubplanSharing.apply backend sources plan

        match shared result with
        | [ (a, bodyA); (b, bodyB) ] ->
            Assert.Equal(a, b)
            Assert.Equal(bodyA, bodyB)
            Assert.Equal(aggregate 0 1, bodyA)
        | other -> failwithf "expected two occurrences of one Shared subplan, got %A" other

    [<Fact>]
    let ``Equal subplans over different tables are not shared`` () =
        let otherFacts = Table("facts", factSchema) :> ILogicalTableSource
        let sources = [| SourceBinding.Table facts; SourceBinding.Table dims; SourceBinding.Table otherFacts; SourceBinding.Table dims |]
        let plan = LogicalPlan.Union(aggregate 0 1, aggregate 2 3, true)

        Assert.Empty(shared (CommonSubplanSharing.apply backend sources plan))

    [<Fact>]
    let ``A repeated scan or filter of a table is left to each consumer`` () =
        let sources = [| SourceBinding.Table facts; SourceBinding.Table facts |]
        let filtered slot = LogicalPlan.Filter(scan slot factSchema, gt "v" 1)
        let plan = LogicalPlan.Union(filtered 0, filtered 1, true)

        Assert.Empty(shared (CommonSubplanSharing.apply backend sources plan))

    [<Fact>]
    let ``When every consumer filters the subplan, it is filtered by their disjunction`` () =
        let sources = [| SourceBinding.Table facts; SourceBinding.Table dims |]
        let plan =
            LogicalPlan.Union(
                LogicalPlan.Filter(aggregate 0 1, gt "total" 10),
                LogicalPlan.Filter(aggregate 0 1, gt "total" 20),
                true)

        let result = CommonSubplanSharing.apply backend sources plan

        let either = ScalarExpr.Binary(BinaryOp.Or, gt "total" 10, gt "total" 20)
        match result with
        | LogicalPlan.Union(LogicalPlan.Filter(LogicalPlan.Shared(_, left), p1), LogicalPlan.Filter(LogicalPlan.Shared(_, right), p2), true) ->
            // Each consumer keeps its own filter over the shared rows.
            Assert.Equal(gt "total" 10, p1)
            Assert.Equal(gt "total" 20, p2)
            Assert.Equal(LogicalPlan.Filter(aggregate 0 1, either), left)
            Assert.Equal(left, right)
        | other -> failwithf "unexpected plan %s" (LogicalPlanOps.formatPlan other)

    [<Fact>]
    let ``A consumer that reads every row keeps the subplan unfiltered`` () =
        let sources = [| SourceBinding.Table facts; SourceBinding.Table dims |]
        let plan = LogicalPlan.Union(LogicalPlan.Filter(aggregate 0 1, gt "total" 10), aggregate 0 1, true)

        match shared (CommonSubplanSharing.apply backend sources plan) with
        | [ (_, body); _ ] -> Assert.Equal(aggregate 0 1, body)
        | other -> failwithf "expected two occurrences, got %A" other

    [<Fact>]
    let ``A subplan repeated inside a shared one and elsewhere is shared in every copy`` () =
        let sources = [| SourceBinding.Table facts; SourceBinding.Table dims |]
        let inner = aggregate 0 1
        let outer = LogicalPlan.Distinct(LogicalPlan.Union(inner, scan 1 dimSchema, true), None, DistinctKeep.Any, false)
        // outer twice, and inner once more on its own.
        let plan = LogicalPlan.Union(LogicalPlan.Union(outer, outer, true), inner, true)

        let occurrences = shared (CommonSubplanSharing.apply backend sources plan)
        let byId = occurrences |> List.countBy fst |> Map.ofList
        // outer: 2 occurrences; inner: once inside each copy of outer and once on its own.
        Assert.Equal(2, byId.Count)
        Assert.Equal<int list>([ 2; 3 ], byId |> Map.toList |> List.map snd |> List.sort)
        for id in byId.Keys do
            let bodies = occurrences |> List.filter (fst >> (=) id) |> List.map snd |> List.distinct
            Assert.Single(bodies) |> ignore

    [<Fact>]
    let ``Predicate pushdown stops at a shared subplan`` () =
        let sources = [| SourceBinding.Table facts; SourceBinding.Table dims |]
        let consumer = LogicalPlan.Filter(LogicalPlan.Project(aggregate 0 1, [ "name", col "name"; "total", col "total" ]), gt "total" 10)
        let plan = LogicalPlan.Union(consumer, LogicalPlan.Project(aggregate 0 1, [ "name", col "name"; "total", col "total" ]), true)

        let optimized = CommonSubplanSharing.apply backend sources plan |> PredicatePushdown.apply

        // Both copies keep an equal body: the filter of one consumer did not move into its copy.
        match shared optimized with
        | [ (_, a); (_, b) ] -> Assert.Equal(a, b)
        | other -> failwithf "expected two occurrences, got %A" other

    [<Fact>]
    let ``A subplan the backend shares itself is left to it, with everything inside it`` () =
        let sources = [| SourceBinding.Table facts; SourceBinding.Table dims |]
        let indexed = LogicalPlan.WithRowIndex(aggregate 0 1, "rid", None)
        let plan = LogicalPlan.Union(indexed, indexed, true)
        let sharingIndexed =
            backendSharing (function
                | LogicalPlan.WithRowIndex _ -> true
                | _ -> false)

        Assert.Empty(shared (CommonSubplanSharing.apply sharingIndexed sources plan))
        Assert.NotEmpty(shared (CommonSubplanSharing.apply backend sources plan))

    [<Fact>]
    let ``Polars shares a row-indexed query itself, and nothing else`` () =
        let polars = Polars.NET.Linq.Backend.Polars.PolarsQueryBackend.Instance

        Assert.True(polars.SharesRepeatedSubplanItself(LogicalPlan.WithRowIndex(aggregate 0 1, "rid", None)))
        Assert.False(polars.SharesRepeatedSubplanItself(aggregate 0 1))

    [<Fact>]
    let ``Grafting a plan unwraps its shared subplans`` () =
        let sources = [| SourceBinding.Table facts; SourceBinding.Table dims |]
        let plan = CommonSubplanSharing.apply backend sources (LogicalPlan.Union(aggregate 0 1, aggregate 0 1, true))
        Assert.NotEmpty(shared plan)

        let registry = SourceRegistry()
        registry.Scan(SourceBinding.Table dims, dimSchema) |> ignore
        let grafted = registry.Graft(plan, sources)

        Assert.Empty(shared grafted)
        Assert.Equal(LogicalPlan.Union(aggregate 1 2, aggregate 1 2, true), grafted)
