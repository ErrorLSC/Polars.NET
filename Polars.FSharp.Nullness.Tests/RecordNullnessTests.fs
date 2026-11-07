namespace Polars.FSharp.Nullness.Tests

open System
open System.Linq
open Xunit
open Polars.FSharp
open Polars.FSharp.Query

type NullableName = { Id: int64; Name: string | null }

type PlainName = { Id: int64; Name: string }

type PlainScore = { Id: int64; Score: float }

/// A record field receives a null only when its type says so: an option, or (with nullness checking enabled)
/// a reference type declared `| null`. A plain `string` field rejects the null with a message naming both.
type RecordNullnessTests() =

    let frame () =
        DataFrame.create [ pl.series "Id" [ 1L; 2L ]; pl.series "Name" [ Some "a"; None ] ]

    [<Fact>]
    member _.``A field declared | null receives the null``() =
        use df = frame ()
        let rows = df.ToRecords<NullableName>() |> Seq.toArray
        Assert.Equal<string | null>("a", rows.[0].Name)
        Assert.Null(rows.[1].Name)

    [<Fact>]
    member _.``A field declared | null receives the null from a query``() =
        use df = frame ()
        let row = query { for x in df.AsQueryable<NullableName>() do where (x.Id = 2L); exactlyOne }
        Assert.Null(row.Name)

    [<Fact>]
    member _.``A plain string field rejects the null and names the declarations that accept it``() =
        use df = frame ()
        let ex = Assert.Throws<InvalidOperationException>(fun () -> df.ToRecords<PlainName>() |> Seq.toArray |> ignore)
        Assert.Equal(
            "Record field 'Name' of type 'string' cannot hold the null in column 'Name' at row 1. "
            + "Declare it as 'string option', or as 'string | null' with nullness checking enabled, to receive nulls, "
            + "or fill the nulls before reading the rows.",
            ex.Message)

    [<Fact>]
    member _.``A value field rejects the null and names the option that accepts it``() =
        use df = DataFrame.create [ pl.series "Id" [ 1L; 2L ]; pl.series "Score" [ Some 1.5; None ] ]
        let ex = Assert.Throws<InvalidOperationException>(fun () -> df.ToRecords<PlainScore>() |> Seq.toArray |> ignore)
        Assert.Equal(
            "Record field 'Score' of type 'float' cannot hold the null in column 'Score' at row 1. "
            + "Declare it as 'float option' to receive nulls, or fill the nulls before reading the rows.",
            ex.Message)
