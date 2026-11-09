namespace Polars.FSharp.Tests

open Xunit
open Kaminari.Samidare.IR
open Kaminari.Samidare.IR.ScalarExprOps
open Kaminari.Samidare.IR.Passes

/// Subplan keys, the reference feedback store, and the estimator's use of observations.
module CardinalityFeedbackTests =

    type private File(identity: string option, schema: LogicalSchema) =
        interface ILogicalTableSource with
            member _.Name = "file"
            member _.Schema = schema
        interface ISourceIdentity with
            member _.SourceIdentity = identity

    let private schema = LogicalSchema.OfFields [ ("k", LogicalTypes.Int32) ]
    let private other = LogicalSchema.OfFields [ ("j", LogicalTypes.Int32) ]
    let private scan slot layout = LogicalPlan.Scan(LogicalPlanSource.Slot slot, layout)
    let private file name layout = SourceBinding.Table(File(Some name, layout))
    let private join left right =
        LogicalPlan.Join(left, right, [ col "k" ], [ col "j" ], JoinKind.Inner, None, JoinKeyCoalescing.Default, false)

    [<Fact>]
    let ``A subplan has the same key whichever slots its scans use`` () =
        let first = CardinalityFeedback.tryKey [| file "a" schema; file "b" other |] (join (scan 0 schema) (scan 1 other))
        let second = CardinalityFeedback.tryKey [| file "x" schema; file "b" other; file "a" schema |] (join (scan 2 schema) (scan 1 other))
        Assert.True(first.IsSome)
        Assert.Equal(first, second)

    [<Fact>]
    let ``Different files give different keys`` () =
        let plan = scan 0 schema
        Assert.NotEqual(CardinalityFeedback.tryKey [| file "a" schema |] plan, CardinalityFeedback.tryKey [| file "b" schema |] plan)

    [<Fact>]
    let ``A different literal is a different subplan`` () =
        let filter (value: int) = LogicalPlan.Filter(scan 0 schema, ScalarExpr.Binary(BinaryOp.Eq, col "k", lit value))
        let sources = [| file "a" schema |]
        Assert.NotEqual(CardinalityFeedback.tryKey sources (filter 1), CardinalityFeedback.tryKey sources (filter 2))

    [<Fact>]
    let ``A source without an identity has no key`` () =
        Assert.Equal(None, CardinalityFeedback.tryKey [| SourceBinding.Table(File(None, schema)) |] (scan 0 schema))

    let private key = (CardinalityFeedback.tryKey [| file "a" schema |] (scan 0 schema)).Value

    [<Fact>]
    let ``The store keeps the latest count and how many runs reported`` () =
        let store = InMemoryCardinalityFeedback() :> ICardinalityFeedback
        Assert.Equal(None, store.TryGetObserved key)
        store.Record(key, 100L)
        store.Record(key, 120L)
        Assert.Equal(Some { Rows = 120.0; Runs = 2 }, store.TryGetObserved key)

    [<Fact>]
    let ``Only a large change moves the generation`` () =
        let store = InMemoryCardinalityFeedback() :> ICardinalityFeedback
        store.Record(key, 100L)
        let afterFirst = store.Generation
        store.Record(key, 150L)
        Assert.Equal(afterFirst, store.Generation)
        store.Record(key, 400L)
        Assert.True(store.Generation > afterFirst)

    [<Fact>]
    let ``An observation replaces the estimate of its subplan`` () =
        let filtered = LogicalPlan.Filter(scan 0 schema, ScalarExpr.Binary(BinaryOp.Eq, col "k", lit 1))
        let estimates: JoinReordering.SourceEstimates =
            {
                Rows = (fun _ -> 1000.0)
                Column = (fun _ _ -> None)
                KeyRange = (fun _ _ -> None)
                Observed = (fun plan -> if plan = filtered then Some 7.0 else None)
            }
        let rows, tableRows = JoinReordering.estimate estimates filtered
        Assert.Equal(7.0, rows)
        Assert.Equal(1000.0, tableRows)

    [<Fact>]
    let ``Without an observation the estimate is unchanged`` () =
        let estimates: JoinReordering.SourceEstimates =
            { Rows = (fun _ -> 1000.0); Column = (fun _ _ -> None); KeyRange = (fun _ _ -> None); Observed = (fun _ -> None) }
        Assert.Equal(100.0, fst (JoinReordering.estimate estimates (LogicalPlan.Filter(scan 0 schema, ScalarExpr.Binary(BinaryOp.Eq, col "k", lit 1)))))
