namespace Polars.FSharp.Tests

open System.Linq
open Xunit
open Polars.FSharp.Query
open Polars.FSharp

// Both records have Name and DeptId, so every pair has colliding columns.
type NeEmp = { Name: string; DeptId: int; Salary: int }
type NeDept = { DeptId: int; Name: string; Floor: string }
type NeFloor = { Floor: string; City: string }

/// Whole records selected in F# queries (tuples and anonymous records of joined records, or a record next to
/// its own fields) are materialized natively. Every result is compared, in order, with the same query over
/// in-memory arrays.
module QueryNestedEntityTests =

    let private emps =
        [|
            { NeEmp.Name = "a"; DeptId = 1; Salary = 10 }
            { NeEmp.Name = "b"; DeptId = 2; Salary = 20 }
            { NeEmp.Name = "c"; DeptId = 3; Salary = 30 }
            { NeEmp.Name = "d"; DeptId = 1; Salary = 40 }
        |]

    let private depts =
        [|
            { NeDept.DeptId = 2; Name = "d2"; Floor = "F2" }
            { NeDept.DeptId = 1; Name = "d1"; Floor = "F1" }
        |]

    let private floors = [| { Floor = "F1"; City = "P" }; { Floor = "F2"; City = "Q" } |]

    /// Runs `query` over Polars frames and over the arrays; the Polars run must be native and agree row by row.
    let private assertMatches (query: IQueryable<NeEmp> -> IQueryable<NeDept> -> IQueryable<NeFloor> -> IQueryable<'T>) =
        use de = DataFrame.ofRecords emps
        use dd = DataFrame.ofRecords depts
        use df = DataFrame.ofRecords floors

        let expected = query (emps.AsQueryable()) (depts.AsQueryable()) (floors.AsQueryable()) |> Seq.map (sprintf "%A") |> List.ofSeq
        let actual = query (de.AsQueryable<NeEmp>()) (dd.AsQueryable<NeDept>()) (df.AsQueryable<NeFloor>())

        Assert.Equal<string list>(expected, actual |> Seq.map (sprintf "%A") |> List.ofSeq)

    /// The provider query behind an F# query whose result is post-processed in memory (tuples, anonymous records).
    let private assertNativeSource (query: IQueryable<NeEmp> -> IQueryable<NeDept> -> IQueryable<'T>) =
        use de = DataFrame.ofRecords emps
        use dd = DataFrame.ofRecords depts
        let q = query (de.AsQueryable<NeEmp>()) (dd.AsQueryable<NeDept>())
        // F# hands the provider a query over its AnonymousObject rows and converts them afterwards: the result
        // is an EnumerableQuery over a Select iterator whose source is the provider's query.
        let rec fields (t: System.Type) =
            if isNull t then [||]
            else
                Array.append
                    (t.GetFields(System.Reflection.BindingFlags.Instance ||| System.Reflection.BindingFlags.NonPublic ||| System.Reflection.BindingFlags.DeclaredOnly))
                    (fields t.BaseType)
        let providerQuery (o: obj) =
            let iterator = (fields (o.GetType()) |> Array.find (fun f -> f.Name = "_enumerable")).GetValue o
            fields (iterator.GetType()) |> Array.tryPick (fun f -> match f.GetValue iterator with :? IQueryable as q -> Some q | _ -> None)
        let inner = providerQuery ((q.Expression :?> System.Linq.Expressions.ConstantExpression).Value)
        Assert.True(inner.IsSome)
        Assert.False((inner.Value :?> Kaminari.Samidare.IPolarsExplainable).InspectPlan(true).IsFullFallback)

    [<Fact>]
    let ``Join select tuple of records`` () =
        assertMatches (fun eq dq _ -> query { for e in eq do join d in dq on (e.DeptId = d.DeptId); select (e, d) })
        assertNativeSource (fun eq dq -> query { for e in eq do join d in dq on (e.DeptId = d.DeptId); select (e, d) })

    [<Fact>]
    let ``Join select anonymous record of records`` () =
        assertMatches (fun eq dq _ -> query { for e in eq do join d in dq on (e.DeptId = d.DeptId); select {| e = e; d = d |} })
        assertNativeSource (fun eq dq -> query { for e in eq do join d in dq on (e.DeptId = d.DeptId); select {| e = e; d = d |} })

    [<Fact>]
    let ``Join select record next to a field of the other side`` () =
        assertMatches (fun eq dq _ -> query { for e in eq do join d in dq on (e.DeptId = d.DeptId); select {| e = e; dn = d.Name |} })

    [<Fact>]
    let ``Join filtered on the right side's colliding column`` () =
        assertMatches (fun eq dq _ ->
            query {
                for e in eq do
                    join d in dq on (e.DeptId = d.DeptId)
                    where (d.Name = "d1")
                    select (e, d)
            })

    [<Fact>]
    let ``Join sorted then select tuple`` () =
        assertMatches (fun eq dq _ ->
            query {
                for e in eq do
                    join d in dq on (e.DeptId = d.DeptId)
                    sortByDescending e.Salary
                    select (d, e)
            })

    [<Fact>]
    let ``Three way join select triple`` () =
        assertMatches (fun eq dq fq ->
            query {
                for e in eq do
                    join d in dq on (e.DeptId = d.DeptId)
                    join f in fq on (d.Floor = f.Floor)
                    select (e, d, f)
            })

    [<Fact>]
    let ``Three way join select anonymous record`` () =
        assertMatches (fun eq dq fq ->
            query {
                for e in eq do
                    join d in dq on (e.DeptId = d.DeptId)
                    join f in fq on (d.Floor = f.Floor)
                    select {| f = f; e = e; d = d |}
            })

    [<Fact>]
    let ``Join with let then select record and value`` () =
        assertMatches (fun eq dq _ ->
            query {
                for e in eq do
                    join d in dq on (e.DeptId = d.DeptId)
                    let label = d.Name + "/" + e.Name
                    select (e, label)
            })

    [<Fact>]
    let ``Single table select record next to its field`` () =
        assertMatches (fun eq _ _ -> query { for e in eq do select {| e = e; n = e.Name |} })
        assertNativeSource (fun eq _ -> query { for e in eq do select {| e = e; n = e.Name |} })

    [<Fact>]
    let ``Single table select tuple of record and computed value`` () =
        assertMatches (fun eq _ _ -> query { for e in eq do select (e, e.Salary * 2) })

    [<Fact>]
    let ``Join select colliding fields of both sides`` () =
        // d.Name used to resolve to e.Name: the right side's member is found by its path in the row.
        assertMatches (fun eq dq _ -> query { for e in eq do join d in dq on (e.DeptId = d.DeptId); select {| dn = d.Name; en = e.Name |} })

    // Employee "c" (DeptId 3) has no department: the right record is null on that row.

    [<Fact>]
    let ``Left outer join select tuple of records`` () =
        assertMatches (fun eq dq _ ->
            query {
                for e in eq do
                    leftOuterJoin d in dq on (e.DeptId = d.DeptId) into ds
                    for d in ds.DefaultIfEmpty() do
                        select (e, d)
            })

    [<Fact>]
    let ``Left outer join select anonymous record of records`` () =
        assertMatches (fun eq dq _ ->
            query {
                for e in eq do
                    leftOuterJoin d in dq on (e.DeptId = d.DeptId) into ds
                    for d in ds.DefaultIfEmpty() do
                        select {| e = e; d = d |}
            })

    [<Fact>]
    let ``Left outer join select record next to its own field`` () =
        assertMatches (fun eq dq _ ->
            query {
                for e in eq do
                    leftOuterJoin d in dq on (e.DeptId = d.DeptId) into ds
                    for d in ds.DefaultIfEmpty() do
                        select {| d = d; en = e.Name |}
            })

    [<Fact>]
    let ``Left outer join keeps only unmatched rows`` () =
        assertMatches (fun eq dq _ ->
            query {
                for e in eq do
                    leftOuterJoin d in dq on (e.DeptId = d.DeptId) into ds
                    for d in ds.DefaultIfEmpty() do
                        where (isNull (box d))
                        select (e, d)
            })

    [<Fact>]
    let ``Two left outer joins select triple`` () =
        // F# nests the second join inside the first one's SelectMany; the rewriter flattens it.
        // The second key reads the outer record, so a missing department does not throw.
        assertMatches (fun eq dq _ ->
            query {
                for e in eq do
                    leftOuterJoin d in dq on (e.DeptId = d.DeptId) into ds
                    for d in ds.DefaultIfEmpty() do
                        leftOuterJoin d2 in dq on (e.DeptId + 1 = d2.DeptId) into ds2
                        for d2 in ds2.DefaultIfEmpty() do
                            select (e, d, d2)
            })

    [<Fact>]
    let ``Left outer join then inner join on the outer record`` () =
        assertMatches (fun eq dq fq ->
            query {
                for e in eq do
                    leftOuterJoin d in dq on (e.DeptId = d.DeptId) into ds
                    for d in ds.DefaultIfEmpty() do
                        join d2 in dq on (e.DeptId = d2.DeptId)
                        select {| e = e; d = d; d2 = d2 |}
            })
