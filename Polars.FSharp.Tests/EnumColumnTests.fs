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
type EcInner = { X: int; Shade: EcShade option }
type EcNestedRow = { Id: int; Inner: EcInner; Shades: EcShade list; Maybe: EcShade option list }
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

    let tryShade (code: int) = if code > 0 then Some EcShade.Green else None
    let tryShadeV (code: int) = if code > 0 then ValueSome EcShade.Black else ValueNone

    /// Runs a query whose rows the provider produces itself; it must agree with the arrays and run as a UDF.
    let private assertUdfMatches (query: IQueryable<EcRow> -> IQueryable<'T>) =
        use df = DataFrame.ofRecords rows
        let native = query (df.AsQueryable<EcRow>())
        let inspection = (native :?> Polars.NET.Linq.IPolarsExplainable).InspectPlan(true)
        Assert.True(inspection.HasUdf, inspection.FormatReport())
        Assert.False(inspection.HasClientProjection, inspection.FormatReport())
        let expected = query (rows.AsQueryable()) |> Seq.map (sprintf "%A") |> List.ofSeq
        Assert.Equal<string list>(expected, native |> Seq.map (sprintf "%A") |> List.ofSeq)

    [<Fact>]
    let ``Udf returning an option enum is pushed down`` () =
        assertUdfMatches (fun q -> q.Select(fun r -> tryShade r.Code))
        assertUdfMatches (fun q -> q.Select(fun r -> tryShadeV r.Code))

    [<Fact>]
    let ``Nested option enums and enum lists are Enum fields`` () =
        let rows =
            [ { EcNestedRow.Id = 1; Inner = { X = 1; Shade = Some EcShade.Red }; Shades = [ EcShade.Green; EcShade.Black ]; Maybe = [ None; Some EcShade.Red ] }
              { Id = 2; Inner = { X = 2; Shade = None }; Shades = []; Maybe = [] } ]
        use df = DataFrame.ofRecords rows

        // Nested DataTypes keep their own handles, so their categories stay readable after the parent's are gone.
        let categoriesOf (dt: DataType) =
            match dt.TryGetEnumCategories() with
            | Some frozen -> frozen.GetCategories() |> List.ofArray
            | None -> failwithf "%A is not an Enum" dt.Kind
        let expected = [ "Black"; "Green"; "Red" ]

        let innerFields =
            match df.["Inner"].DataType.Kind with
            | DataTypeKind.Struct fields -> List.ofSeq fields
            | other -> failwithf "Inner is %A" other
        Assert.Equal<string list>(expected, categoriesOf (innerFields |> List.find (fun f -> f.Name = "Shade")).DataType)
        Assert.Equal<string list>(expected, categoriesOf df.["Shades"].DataType.InnerType)
        Assert.Equal<string list>(expected, categoriesOf df.["Maybe"].DataType.InnerType)

        // Schema lookups dispose the field handle they read; the DataType must not depend on it.
        let schemaShades = df.Schema.["Shades"]
        Assert.Equal<string list>(expected, categoriesOf schemaShades.InnerType)
        Assert.Equal<EcNestedRow list>(rows, df |> DataFrame.toRows<EcNestedRow> |> Rows.mapToArray id |> List.ofArray)

    [<Fact>]
    let ``Boxed query elements keep their values`` () =
        use df = DataFrame.ofRecords rows
        let expected = query { for r in rows.AsQueryable() do select (box r.Shade) } |> Seq.map string |> List.ofSeq
        let actual = query { for r in df.AsQueryable<EcRow>() do select (box r.Shade) } |> Seq.map string |> List.ofSeq
        Assert.Equal<string list>(expected, actual)

module EnumCategoriesInterfaceTests =
    open Polars.NET.Core

    [<Fact>]
    let ``IPolarsDataType GetEnumCategories returns categories for Enum and null otherwise`` () =
        use frozen = new FrozenCategories([| "lo"; "mid"; "hi" |])
        use enumType = DataType.Enum frozen
        Assert.Equal<string[]>([| "lo"; "mid"; "hi" |], (enumType :> IPolarsDataType).GetEnumCategories())
        use catType = DataType.Categorical()
        Assert.Null((catType :> IPolarsDataType).GetEnumCategories())

