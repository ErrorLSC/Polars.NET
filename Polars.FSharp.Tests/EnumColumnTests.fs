namespace Polars.FSharp.Tests

open System.Linq
open Xunit
open Polars.FSharp
open Polars.FSharp.Query

type EcShade =
    | Red = 5
    | Green = 1
    | Black = -2

type EcOptionRow = { Id: int; Shade: EcShade option }
type EcRow = { Id: int; Shade: EcShade; Code: int }
type EcValueOptionRow = { Id: int; Shade: EcShade voption }

/// F# option and value option enum fields are stored as Polars Enum columns (categories in underlying-value
/// order) with None as null.
module EnumColumnTests =

    [<Fact>]
    let ``Option enum field is an Enum column with None as null`` () =
        let rows = [ { EcOptionRow.Id = 1; Shade = Some EcShade.Red }; { Id = 2; Shade = None }; { Id = 3; Shade = Some EcShade.Black } ]
        use df = DataFrame.ofRecords rows

        Assert.True((df.["Shade"].DataType.TryGetEnumCategories()).IsSome)
        Assert.Equal(1L, df.["Shade"].NullCount)
        Assert.Equal<EcOptionRow list>(rows, df |> DataFrame.toRows<EcOptionRow> |> Rows.mapToArray id |> List.ofArray)

    [<Fact>]
    let ``Value option enum field is an Enum column with ValueNone as null`` () =
        let rows = [ { EcValueOptionRow.Id = 1; Shade = ValueSome EcShade.Green }; { Id = 2; Shade = ValueNone } ]
        use df = DataFrame.ofRecords rows

        Assert.True((df.["Shade"].DataType.TryGetEnumCategories()).IsSome)
        Assert.Equal(1L, df.["Shade"].NullCount)
        Assert.Equal<EcValueOptionRow list>(rows, df |> DataFrame.toRows<EcValueOptionRow> |> Rows.mapToArray id |> List.ofArray)

    let private rows =
        [|
            { EcRow.Id = 1; Shade = EcShade.Red; Code = 1 }
            { Id = 2; Shade = EcShade.Green; Code = 5 }
            { Id = 3; Shade = EcShade.Black; Code = -2 }
            { Id = 4; Shade = EcShade.Red; Code = 1 }
        |]

    /// Runs `query` over a Polars frame (Enum column) and over the array; the results must agree in order.
    let private assertMatches (query: IQueryable<EcRow> -> seq<'T>) =
        use df = DataFrame.ofRecords rows
        let expected = query (rows.AsQueryable()) |> Seq.map (sprintf "%A") |> List.ofSeq
        let actual = query (df.AsQueryable<EcRow>()) |> Seq.map (sprintf "%A") |> List.ofSeq
        Assert.Equal<string list>(expected, actual)

    [<Fact>]
    let ``Query comparisons on an Enum column follow underlying values`` () =
        assertMatches (fun q -> query { for r in q do where (r.Shade = EcShade.Red); select r.Id })
        assertMatches (fun q -> query { for r in q do where (r.Shade <> EcShade.Red); select r.Id })
        assertMatches (fun q -> query { for r in q do where (r.Shade < EcShade.Red); select r.Id })
        assertMatches (fun q -> query { for r in q do where ([| EcShade.Black; EcShade.Green |].Contains r.Shade); select r.Id })

    [<Fact>]
    let ``Query conversions, names, sorting and grouping on an Enum column`` () =
        assertMatches (fun q -> query { for r in q do select (int r.Shade) })
        assertMatches (fun q -> query { for r in q do select (enum<EcShade> r.Code) })
        assertMatches (fun q -> query { for r in q do select (r.Shade.ToString()) })
        assertMatches (fun q -> query { for r in q do select (string r.Shade) })
        assertMatches (fun q -> query { for r in q do sortBy r.Shade; thenBy r.Id; select r.Id })
        assertMatches (fun q -> query { for r in q do groupBy r.Shade into g; sortBy g.Key; select (g.Key, g.Count()) })
