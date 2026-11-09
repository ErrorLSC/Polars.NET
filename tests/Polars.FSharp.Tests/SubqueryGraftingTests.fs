namespace Polars.FSharp.Tests

open System
open System.Linq
open Xunit
open Polars.FSharp.Query
open Polars.FSharp
open Polars.NET.Linq
open Kaminari.Samidare

type SqRow = { Id: int; A: int; B: int; V: float }

/// Subqueries in F# queries (nested `query { ... contains / exists }`, captured query variables, `Seq.contains`)
/// are grafted into the outer plan as semi or anti joins. Every result is compared, in order, with the same query
/// over in-memory arrays.
module SubqueryGraftingTests =

    let private rows = [| for i in 0 .. 59 -> { Id = i; A = i % 7; B = i % 3; V = float (i * 13 % 50) } |]

    /// Runs the query over two Polars frames and over the arrays, asserts the results agree in order, and returns
    /// the native optimized logical plan.
    let private assertAgrees (query: IQueryable<SqRow> -> IQueryable<SqRow> -> IQueryable<int>) : string =
        use d1 = DataFrame.ofRecords rows
        use d2 = DataFrame.ofRecords rows
        let expected = query (rows.AsQueryable()) (rows.AsQueryable()) |> List.ofSeq
        let native = query (d1.AsQueryable<SqRow>()) (d2.AsQueryable<SqRow>())
        Assert.Equal<int list>(expected, native |> List.ofSeq)
        (native :> obj :?> IPolarsExplainable).InspectPlan(true).LogicalPlan.Value

    let private assertContainsAll (plan: string) (fragments: string list) =
        for fragment in fragments do
            Assert.Contains(fragment, plan)

    [<Fact>]
    let ``A nested contains query is grafted as a semi join`` () =
        let plan =
            assertAgrees (fun q q2 ->
                query {
                    for r in q do
                    where (query {
                        for x in q2 do
                        where (x.V > 40.0)
                        select x.A
                        contains r.B
                    })
                    select r.Id
                })
        assertContainsAll plan [ "how=Semi"; "Filter: (col(\"V\") > 40)" ]

    [<Fact>]
    let ``A negated nested contains over groups is grafted as an anti join`` () =
        let plan =
            assertAgrees (fun q q2 ->
                query {
                    for r in q do
                    where (not (query {
                        for x in q2 do
                        groupBy x.A into g
                        where (g.Count() > 8)
                        select g.Key
                        contains r.A
                    }))
                    select r.Id
                })
        assertContainsAll plan [ "how=Anti"; "GroupBy:"; "maintainOrder=false" ]

    [<Fact>]
    let ``A nested correlated exists is grafted as a semi join`` () =
        let plan =
            assertAgrees (fun q q2 ->
                query {
                    for r in q do
                    where (query {
                        for x in q2 do
                        exists (x.A = r.B && x.V > 45.0)
                    })
                    select r.Id
                })
        assertContainsAll plan [ "how=Semi"; "Filter: (col(\"V\") > 45)" ]

    [<Fact>]
    let ``A top-n subquery keeps its sort and limit`` () =
        let plan =
            assertAgrees (fun q q2 ->
                query {
                    for r in q do
                    where (query {
                        for x in q2 do
                        sortByDescending x.V
                        thenBy x.Id
                        take 5
                        select x.Id
                        contains r.Id
                    })
                    select r.Id
                })
        assertContainsAll plan [ "how=Semi"; "Limit: offset=0 count=5"; "Sort:" ]

    [<Fact>]
    let ``A subquery taking the first groups keeps the group order`` () =
        let plan =
            assertAgrees (fun q q2 ->
                query {
                    for r in q do
                    where (query {
                        for x in q2 do
                        groupBy x.A into g
                        select g.Key
                        take 3
                        contains r.A
                    })
                    select r.Id
                })
        assertContainsAll plan [ "how=Semi"; "Limit:"; "maintainOrder=true" ]

    [<Fact>]
    let ``A captured query variable is grafted through Contains and Seq.contains`` () =
        let byMethod =
            assertAgrees (fun q q2 ->
                let ids = query { for x in q2 do where (x.V > 40.0); select x.A }
                query { for r in q do where (ids.Contains r.A); select r.Id })
        let byFunction =
            assertAgrees (fun q q2 ->
                let ids = query { for x in q2 do where (x.V > 40.0); select x.A }
                query { for r in q do where (not (Seq.contains r.A ids)); select r.Id })
        assertContainsAll byMethod [ "how=Semi"; "Filter: (col(\"V\") > 40)" ]
        assertContainsAll byFunction [ "how=Anti"; "Filter: (col(\"V\") > 40)" ]

    [<Fact>]
    let ``Seq, List, Array and Set contains over local collections run natively`` () =
        let localList = [ 1; 3; 5 ]
        let localArray = [| 2; 4 |]
        let localSet = set [ 0; 6 ]
        let plans =
            [
                assertAgrees (fun q _ -> query { for r in q do where (List.contains r.A localList); select r.Id })
                assertAgrees (fun q _ -> query { for r in q do where (Array.contains r.A localArray); select r.Id })
                assertAgrees (fun q _ -> query { for r in q do where (Set.contains r.A localSet); select r.Id })
                assertAgrees (fun q _ -> query { for r in q do where (not (Seq.contains r.B [ 0; 2 ])); select r.Id })
            ]
        // A local collection of scalars is a membership test (IN list), not a join with a snapshot of it.
        for plan in plans do
            Assert.DoesNotContain("UDF", plan)
            Assert.DoesNotContain("Join: how=", plan)
            Assert.Contains(" IN [...])", plan)

    // ---------------------------------------------------------------------------------------------
    // Differential: IN / NOT IN subqueries under outer orderings and paging
    // ---------------------------------------------------------------------------------------------

    let private manyRows =
        let random = Random 20261108
        [| for i in 0 .. 1999 -> { Id = i; A = (i * 7 + random.Next 3) % 5; B = random.Next 3; V = Math.Round(random.NextDouble() * 100.0, 1) } |]

    /// A subquery case: the native query (filtering the outer query by a subquery over the inner one) and the same
    /// filter as a predicate over the rows, with the subquery evaluated once into a set.
    type private SubqueryCase =
        {
            Name: string
            Native: IQueryable<SqRow> -> IQueryable<SqRow> -> IQueryable<SqRow>
            Oracle: SqRow[] -> (SqRow -> bool)
        }

    let private cases : SubqueryCase list =
        [
            {
                Name = "in where-select"
                Native = fun q q2 ->
                    query { for r in q do where (query { for x in q2 do where (x.V > 95.0); select x.Id; contains r.Id }) }
                Oracle = fun inner ->
                    let ids = inner |> Seq.filter (fun x -> x.V > 95.0) |> Seq.map (fun x -> x.Id) |> Set.ofSeq
                    fun r -> ids.Contains r.Id
            }
            {
                Name = "not in having"
                Native = fun q q2 ->
                    query {
                        for r in q do
                        where (not (query {
                            for x in q2 do
                            groupBy (x.Id % 37) into g
                            where (g.Sum(fun x -> x.V) > 2750.0)
                            select g.Key
                            contains (r.Id % 37)
                        }))
                    }
                Oracle = fun inner ->
                    let keys =
                        inner |> Seq.groupBy (fun x -> x.Id % 37)
                        |> Seq.filter (fun (_, g) -> g |> Seq.sumBy (fun x -> x.V) > 2750.0)
                        |> Seq.map fst |> Set.ofSeq
                    fun r -> not (keys.Contains(r.Id % 37))
            }
            {
                Name = "in distinct"
                Native = fun q q2 ->
                    query { for r in q do where (query { for x in q2 do where (x.B = 1 && x.V < 20.0); select (x.Id % 50); distinct; contains (r.Id % 50) }) }
                Oracle = fun inner ->
                    let keys = inner |> Seq.filter (fun x -> x.B = 1 && x.V < 20.0) |> Seq.map (fun x -> x.Id % 50) |> Set.ofSeq
                    fun r -> keys.Contains(r.Id % 50)
            }
            {
                Name = "not in top-n"
                Native = fun q q2 ->
                    query { for r in q do where (not (query { for x in q2 do sortByDescending x.V; thenBy x.Id; take 300; select x.Id; contains r.Id })) }
                Oracle = fun inner ->
                    let ids = inner |> Seq.sortBy (fun x -> -x.V, x.Id) |> Seq.truncate 300 |> Seq.map (fun x -> x.Id) |> Set.ofSeq
                    fun r -> not (ids.Contains r.Id)
            }
            {
                Name = "in first-n"
                Native = fun q q2 ->
                    query { for r in q do where (query { for x in q2 do where (x.B = 2); take 150; select x.Id; contains r.Id }) }
                Oracle = fun inner ->
                    let ids = inner |> Seq.filter (fun x -> x.B = 2) |> Seq.truncate 150 |> Seq.map (fun x -> x.Id) |> Set.ofSeq
                    fun r -> ids.Contains r.Id
            }
            {
                Name = "exists correlated"
                Native = fun q q2 ->
                    query { for r in q do where (query { for x in q2 do exists (x.Id = r.Id + 1 && x.V > 50.0) }) }
                Oracle = fun inner ->
                    let ids = inner |> Seq.filter (fun x -> x.V > 50.0) |> Seq.map (fun x -> x.Id) |> Set.ofSeq
                    fun r -> ids.Contains(r.Id + 1)
            }
            {
                Name = "not Seq.contains"
                Native = fun q q2 ->
                    let keys = query { for x in q2 do where (x.V > 90.0); select x.A }
                    query { for r in q do where (not (Seq.contains r.A keys)) }
                Oracle = fun inner ->
                    let keys = inner |> Seq.filter (fun x -> x.V > 90.0) |> Seq.map (fun x -> x.A) |> Set.ofSeq
                    fun r -> not (keys.Contains r.A)
            }
        ]

    let private orderings : (string * (IQueryable<SqRow> -> IQueryable<SqRow>)) list =
        [
            "unordered", id
            "A", fun q -> q.OrderBy(fun r -> r.A) :> IQueryable<SqRow>
            "V desc, Id", fun q -> q.OrderByDescending(fun r -> r.V).ThenBy(fun r -> r.Id) :> IQueryable<SqRow>
            "B, A desc", fun q -> q.OrderBy(fun r -> r.B).ThenByDescending(fun r -> r.A) :> IQueryable<SqRow>
        ]

    let private pagings : (string * (IQueryable<SqRow> -> IQueryable<SqRow>)) list =
        [
            "whole", id
            "take 100", fun q -> q.Take 100
            "skip 50 take 30", fun q -> q.Skip(50).Take 30
        ]

    [<Fact>]
    let ``Subquery filters agree with in-memory evaluation under orderings and paging`` () =
        use d1 = DataFrame.ofRecords manyRows
        use d2 = DataFrame.ofRecords manyRows
        let failures = Collections.Generic.List<string>()
        let mutable checkedCount = 0

        for case in cases do
            let predicate = case.Oracle manyRows
            for orderingName, order in orderings do
                for pagingName, page in pagings do
                    checkedCount <- checkedCount + 1
                    let name = sprintf "%s | %s | %s" case.Name orderingName pagingName
                    try
                        let expected =
                            (manyRows.AsQueryable().Where(predicate) |> order |> page).Select(fun r -> r.Id) |> List.ofSeq
                        let actual =
                            (case.Native (d1.AsQueryable<SqRow>()) (d2.AsQueryable<SqRow>()) |> order |> page).Select(fun r -> r.Id)
                            |> List.ofSeq
                        if expected <> actual then
                            failures.Add(sprintf "%s: counts %d/%d" name expected.Length actual.Length)
                    with ex ->
                        failures.Add(sprintf "%s: %s: %s" name (ex.GetType().Name) (ex.Message.Split('\n').[0]))

        Assert.True(failures.Count = 0, sprintf "%d of %d combinations differ:\n%s" failures.Count checkedCount (String.Join("\n", failures)))
