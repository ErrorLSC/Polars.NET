namespace Polars.FSharp.Tests

open System
open System.Linq
open Xunit
open Polars.FSharp
open Polars.FSharp.Query
open Polars.NET.Core
open Polars.NET.Linq
open Polars.NET.Linq.IR
open Polars.NET.Linq.IR.ScalarExprOps
open Polars.NET.Linq.IR.Passes

type TestRow = { Id: int; Val: int }

type LogicalPlanOptimizerTests() =

    let dummySchema =
        LogicalSchema.OfFields [ ("x", PlDataType.Int32); ("y", PlDataType.Int32); ("z", PlDataType.String) ]

    let dummyScan =
        LogicalPlan.Scan(LogicalPlanSource.Empty, dummySchema)

    [<Fact>]
    member _.``SortSliceFusion correctly fuses consecutive Limit slices``() =
        // Inner: rows 10..29 (count 20), Outer: skip 5 take 8 -> rows 15..22 (offset 15, count 8)
        let innerLimit = LogicalPlan.Limit(dummyScan, 10L, 20u)
        let outerLimit = LogicalPlan.Limit(innerLimit, 5L, 8u)

        let fused = SortSliceFusion.fuseLimit outerLimit

        match fused with
        | LogicalPlan.Limit(LogicalPlan.Scan _, offset, count) ->
            Assert.Equal(15L, offset)
            Assert.Equal(8u, count)
        | other ->
            Assert.Fail(sprintf "Expected single fused Limit, but got: %A" other)

    [<Fact>]
    member _.``SortSliceFusion eliminates identity slice``() =
        let identityLimit = LogicalPlan.Limit(dummyScan, 0L, UInt32.MaxValue)
        let fused = SortSliceFusion.fuseLimit identityLimit

        match fused with
        | LogicalPlan.Scan _ -> ()
        | other -> Assert.Fail(sprintf "Expected Scan, but got: %A" other)

    [<Fact>]
    member _.``SortSliceFusion fuses consecutive Sort nodes``() =
        let sort1 = LogicalPlan.Sort(dummyScan, [ { SortOrder.Expr = col "x"; Descending = false; NullsLast = false } ])
        let sort2 = LogicalPlan.Sort(sort1, [ { SortOrder.Expr = col "y"; Descending = true; NullsLast = true } ])

        let fused = SortSliceFusion.apply sort2

        match fused with
        | LogicalPlan.Sort(LogicalPlan.Scan _, orders) ->
            Assert.Equal(2, orders.Length)
            Assert.Equal("y", match orders.[0].Expr with ScalarExpr.Column c -> c | _ -> "")
            Assert.True(orders.[0].Descending)
            Assert.Equal("x", match orders.[1].Expr with ScalarExpr.Column c -> c | _ -> "")
            Assert.False(orders.[1].Descending)
        | other ->
            Assert.Fail(sprintf "Expected fused Sort, but got: %A" other)

    [<Fact>]
    member _.``PredicatePushdown pushes filter through Union to both branches``() =
        let leftScan = dummyScan
        let rightScan = dummyScan
        let unionNode = LogicalPlan.Union(leftScan, rightScan, isAll = true)
        let filterNode = LogicalPlan.Filter(unionNode, ScalarExpr.Binary(BinaryOp.Gt, col "x", lit 10))

        let pushed = PredicatePushdown.apply filterNode

        match pushed with
        | LogicalPlan.Union(LogicalPlan.Filter(LogicalPlan.Scan _, leftPred), LogicalPlan.Filter(LogicalPlan.Scan _, rightPred), true) ->
            Assert.Equal(leftPred, rightPred)
        | other ->
            Assert.Fail(sprintf "Expected Union of Filters, but got: %A" other)

    [<Fact>]
    member _.``PredicatePushdown pushes filter through Rename with inverted column names``() =
        let renameNode = LogicalPlan.Rename(dummyScan, [ "x" ], [ "x_renamed" ])
        let filterNode = LogicalPlan.Filter(renameNode, ScalarExpr.Binary(BinaryOp.Eq, col "x_renamed", lit 42))

        let pushed = PredicatePushdown.apply filterNode

        match pushed with
        | LogicalPlan.Rename(LogicalPlan.Filter(LogicalPlan.Scan _, pred), [ "x" ], [ "x_renamed" ]) ->
            match pred with
            | ScalarExpr.Binary(BinaryOp.Eq, ScalarExpr.Column c, ScalarExpr.Literal(LiteralValue.Int32 42)) ->
                Assert.Equal("x", c)
            | other ->
                Assert.Fail(sprintf "Expected reverted predicate on col x, but got: %A" other)
        | other ->
            Assert.Fail(sprintf "Expected Rename of Filter, but got: %A" other)

    [<Fact>]
    member _.``PredicatePushdown eliminates trivial true filter``() =
        let trivialFilter = LogicalPlan.Filter(dummyScan, lit true)
        let pushed = PredicatePushdown.apply trivialFilter

        match pushed with
        | LogicalPlan.Scan _ -> ()
        | other -> Assert.Fail(sprintf "Expected Scan without filter, but got: %A" other)

    [<Fact>]
    member _.``ProjectionPruning collapses double Project into single Project``() =
        let innerProjs = [ ("a", col "x"); ("b", col "y") ]
        let innerProject = LogicalPlan.Project(dummyScan, innerProjs)
        let outerProjs = [ ("sum", ScalarExpr.Binary(BinaryOp.Add, col "a", col "b")) ]
        let outerProject = LogicalPlan.Project(innerProject, outerProjs)

        let pruned = ProjectionPruning.apply outerProject

        match pruned with
        | LogicalPlan.Project(LogicalPlan.Scan _, projs) ->
            Assert.Single(projs) |> ignore
            let (name, expr) = projs.[0]
            Assert.Equal("sum", name)
            match expr with
            | ScalarExpr.Binary(BinaryOp.Add, ScalarExpr.Column "x", ScalarExpr.Column "y") -> ()
            | other -> Assert.Fail(sprintf "Expected substituted expression (x + y), but got: %A" other)
        | other ->
            Assert.Fail(sprintf "Expected single Project, but got: %A" other)

    [<Fact>]
    member _.``ProjectionPruning eliminates identity projection``() =
        let identityProjs = [ ("x", col "x"); ("y", col "y"); ("z", col "z") ]
        let proj = LogicalPlan.Project(dummyScan, identityProjs)

        let pruned = ProjectionPruning.apply proj

        match pruned with
        | LogicalPlan.Scan _ -> ()
        | other -> Assert.Fail(sprintf "Expected identity Project to be eliminated, but got: %A" other)

    [<Fact>]
    member _.``LogicalPlanOps formatPlan produces non-empty ASCII representation``() =
        let plan =
            LogicalPlan.Filter(
                LogicalPlan.Project(dummyScan, [ ("x", col "x") ]),
                ScalarExpr.Binary(BinaryOp.Gt, col "x", lit 0)
            )
        let ascii = LogicalPlanOps.formatPlan plan
        Assert.False(String.IsNullOrWhiteSpace ascii)
        Assert.Contains("Filter", ascii)
        Assert.Contains("Project", ascii)
        Assert.Contains("Scan", ascii)

    [<Fact>]
    member _.``FSharp Query pipeline produces and explains LogicalPlan IR correctly``() =
        let rows = [ { Id = 1; Val = 10 }; { Id = 2; Val = 20 }; { Id = 3; Val = 30 } ]
        use df = DataFrame.ofRecords rows

        let q =
            query {
                for r in df.AsQueryable<TestRow>() do
                where (r.Id > 1)
                sortBy r.Id
                select r.Val
            }

        let logicalPlanAscii = q.ExplainLogicalPlan()
        Assert.False(String.IsNullOrWhiteSpace logicalPlanAscii)
        Assert.Contains("Filter", logicalPlanAscii)
        Assert.Contains("Sort", logicalPlanAscii)
        Assert.Contains("Project", logicalPlanAscii)

        let inspection = (q :?> IPolarsExplainable).InspectPlan(optimized = true)
        Assert.True(inspection.LogicalPlan.IsSome)
        let lpStr = inspection.LogicalPlan.Value
        Assert.Contains("Filter", lpStr)
        Assert.Contains("Sort", lpStr)
        Assert.Contains("Project", lpStr)
