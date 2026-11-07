namespace Polars.FSharp.Tests

open System.Linq
open Xunit
open Polars.FSharp
open Polars.FSharp.Query

type OptRow = { Id: int64; O: int option; Name: string option }

/// F# option constants compare with option columns by their payload: `Some 3` is 3, `None` a null.
type QueryOptionSemanticsTests() =

    let rows: OptRow[] =
        [| { Id = 1L; O = None; Name = Some "a" }
           { Id = 2L; O = Some 3; Name = None }
           { Id = 3L; O = Some -1; Name = Some "b" }
           { Id = 4L; O = Some 3; Name = Some "a" } |]

    let compare (shape: IQueryable<OptRow> -> int64 list) =
        use df = DataFrame.ofRecords rows
        Assert.Equal<int64 list>(shape (Queryable.AsQueryable rows), shape (df.AsQueryable<OptRow>()))

    [<Fact>]
    member _.``Option equality with Some and None``() =
        compare (fun src -> query { for x in src do where (x.O = Some 3); select x.Id } |> Seq.toList)
        compare (fun src -> query { for x in src do where (x.O <> Some 3); select x.Id } |> Seq.toList)
        compare (fun src -> query { for x in src do where (x.O = None); select x.Id } |> Seq.toList)
        compare (fun src -> query { for x in src do where (x.Name = Some "a"); select x.Id } |> Seq.toList)
        let wanted = Some -1
        compare (fun src -> query { for x in src do where (x.O = wanted); select x.Id } |> Seq.toList)
