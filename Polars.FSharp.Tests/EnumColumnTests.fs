namespace Polars.FSharp.Tests

open Xunit
open Polars.FSharp

type EcShade =
    | Red = 5
    | Green = 1
    | Black = -2

type EcOptionRow = { Id: int; Shade: EcShade option }
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
