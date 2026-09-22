namespace Polars.FSharp.Tests

open System
open Xunit
open Polars.FSharp
open Polars.NET.Core

type SimplePerson = {
    Id: int
    Name: string
    Score: float
}

type NullablePerson = {
    Id: int
    Nickname: string option
    Bonus: float voption
}

type StrictStudent = {
    Id: int
    Grade: int
}

type TemperatureReading = {
    City: string
    Celsius: float
    RecordedAt: DateTime
}

type TemperatureSummary = {
    City: string
    Fahrenheit: float
    IsFreezing: bool
    RecordedAt: DateTime
}

type SimpleInput = {
    Id: int
    Value: double
}

type SimpleOutput = {
    Id: int
    Doubled: double
}

type NonRecordClass(id: int) =
    member val Id = id with get, set

// C#-style mutable DTO with parameterless constructor
type MutablePersonDto() =
    member val Id = 0 with get, set
    member val Name = "" with get, set

type LogEntry = {
    Timestamp: int64
    Service: string
    LatencyMs: float
}

type SourceRecord = {
    Id: int
    Label: string
}

type MetricRecord = {
    Score: float
}

type ProductItem = {
    Sku: string
    Category: string
    Price: float
}

type FinalProductItem = {
    Sku: string
    FinalPrice: float
}

type DataFrameEnumeratorTests() =
    [<Fact>]
    [<Trait("DataFrame", "Iter")>]
    member _.``RowEnumerator iter traverses all rows sequentially and triggers side-effects`` () =
        let rows =
            [
                { Sku = "SKU-001"; Category = "Book"; Price = 19.99 }
                { Sku = "SKU-002"; Category = "Electronics"; Price = 299.99 }
                { Sku = "SKU-003"; Category = "Book"; Price = 9.99 }
            ]
            |> DataFrame.ofRecords
            |> DataFrame.toRows<ProductItem>

        let observedSkus = rows |> Rows.mapToArray (fun x -> x.Sku)
        let totalPrice   = rows |> Rows.sumBy (fun x -> x.Price)

        // Assert: Verify all rows were observed in exact order and values aggregated correctly
        Assert.Equal(3, observedSkus.Length)
        Assert.Equal<string>([ "SKU-001"; "SKU-002"; "SKU-003" ], observedSkus)
        Assert.Equal(329.97, totalPrice, 2)

    [<Fact>]
    [<Trait("DataFrame", "Iter")>]
    member _.``RowEnumerator iter handles empty DataFrame safely without invoking action`` () =
        // Arrange: Empty DataFrame
        use emptyDf = DataFrame.ofRecords<ProductItem> []
        let mutable invoked = false

        // Act: Attempt to iterate empty enumerator
        emptyDf.Rows<ProductItem>()
        |> Rows.iter (fun _ -> invoked <- true)

        // Assert: Action should never be executed
        Assert.False(invoked)

    [<Fact>]
    [<Trait("DataFrame", "Iteri")>]
    member _.``RowEnumerator iteri pairs each row with its zero-based 64-bit row index`` () =
        let observed = ResizeArray<int64 * string * float>()
        // Arrange: Create sample DataFrame
        [
            { Sku = "P-1"; Category = "Grocery"; Price = 3.50 }
            { Sku = "P-2"; Category = "Hardware"; Price = 12.00 }
        ]
        |> DataFrame.ofRecords
        |> DataFrame.toRows<ProductItem>
        |> Rows.iteri (fun idx item ->
            observed.Add((idx, item.Sku, item.Price))
        )

        // Assert: Verify strict contiguous row indices and aligned row contents
        Assert.Equal(2, observed.Count)
        Assert.Equal((0L, "P-1", 3.50), observed.[0])
        Assert.Equal((1L, "P-2", 12.00), observed.[1])

    [<Fact>]
    [<Trait("DataFrame", "Iteri")>]
    member _.``RowEnumerator iteri handles empty DataFrame safely without invoking action`` () =
        // Arrange: Empty DataFrame
        use emptyDf = DataFrame.ofRecords<ProductItem> []
        let mutable count = 0

        // Act: Run iteri over empty enumerator
        emptyDf.Rows<ProductItem>()
        |> Rows.iteri (fun _ _ -> count <- count + 1)

        // Assert: Counter must remain 0
        Assert.Equal(0, count)

    [<Fact>]
    [<Trait("DataFrame", "MapToArray")>]
    member _.``RowEnumerator mapToArray projects rows into a strongly-typed array`` () =
        let dtos =
            [
                { Sku = "SKU-A"; Category = "Clothing"; Price = 100.0 }
                { Sku = "SKU-B"; Category = "Clothing"; Price = 50.0 }
            ]
            |> DataFrame.ofRecords<ProductItem>
            |> DataFrame.toRows<ProductItem>
            |> Rows.mapToArray (fun item ->
                { Sku = item.Sku; FinalPrice = item.Price * 1.10 }
            )

        // Assert: Check length, types, and values
        Assert.Equal(2, dtos.Length)
        Assert.Equal("SKU-A", dtos.[0].Sku)
        Assert.Equal(110.0, dtos.[0].FinalPrice, 2)
        Assert.Equal("SKU-B", dtos.[1].Sku)
        Assert.Equal(55.0, dtos.[1].FinalPrice, 2)

    [<Fact>]
    [<Trait("DataFrame", "MapToArray")>]
    member _.``RowEnumerator mapToArray returns empty array when DataFrame is empty`` () =
        // Arrange: Empty DataFrame
        use emptyDf = DataFrame.ofRecords<ProductItem> []

        // Act: Project empty enumerator
        let results: string array =
            emptyDf.Rows<ProductItem>()
            |> Rows.mapToArray (fun item -> item.Sku)

        // Assert: Result is an empty array with zero allocation
        Assert.Empty(results)
        Assert.IsType<string array>(results)

    [<Fact>]
    [<Trait("DataFrame", "Enumerator")>]
    member _.``RowEnumerator foldi and mapi preserve strict row indices`` () =
        // Arrange
        let items = [
            { Sku = "SKU-1"; Category = "Book"; Price = 15.0 }
            { Sku = "SKU-2"; Category = "Toy"; Price = 25.0 }
            { Sku = "SKU-3"; Category = "Book"; Price = 30.0 }
        ]
        use df = DataFrame.ofRecords items

        // Act 1: foldi (weighted sum: idx * Price)

        let weightedSum =
            df.Rows<ProductItem>()
            |> Rows.foldi (fun idx acc r -> acc + (float idx * r.Price)) 0.0

        // Act 2: mapi
        let labeled =
            df.Rows<ProductItem>()
            |> Rows.mapi (fun idx r -> $"{idx}:{r.Sku}")
            |> Seq.toArray

        // Assert
        // 0*15.0 + 1*25.0 + 2*30.0 = 85.0
        Assert.Equal(85.0, weightedSum, 1)
        Assert.Equal(3, labeled.Length)
        Assert.Equal("0:SKU-1", labeled.[0])
        Assert.Equal("1:SKU-2", labeled.[1])
        Assert.Equal("2:SKU-3", labeled.[2])

    [<Fact>]
    [<Trait("DataFrame", "ShortCircuit")>]
    member _.``RowEnumerator tryPick, contains and reduce work as expected`` () =
        // Arrange
        let items = [
            { Sku = "A"; Category = "Food"; Price = 10.0 }
            { Sku = "B"; Category = "Food"; Price = 20.0 }
            { Sku = "C"; Category = "Tech"; Price = 50.0 }
        ]
        use df = DataFrame.ofRecords items

        // Act 1: tryPick
        let foundPrice =
            df
            |> DataFrame.toRows<ProductItem>
            |> Rows.tryPick (fun r -> if r.Sku = "B" then Some r.Price else None)

        // Act 2: contains
        let hasItem =
            df
            |> DataFrame.toRows<ProductItem>
            |> Rows.contains { Sku = "A"; Category = "Food"; Price = 10.0 }

        // Act 3: reduce
        let maxPriced =
            df
            |> DataFrame.toRows<ProductItem>
            |> Rows.reduce (fun acc r -> if r.Price > acc.Price then r else acc)

        // Assert
        Assert.Equal(Some 20.0, foundPrice)
        Assert.True(hasItem)
        Assert.Equal("C", maxPriced.Sku)

    [<Fact>]
    [<Trait("DataFrame", "CountByAndScan")>]
    member _.``RowEnumerator countBy and scan produce correct frequency and state prefix`` () =
        // Arrange
        let items = [
            { Sku = "A"; Category = "Book"; Price = 10.0 }
            { Sku = "B"; Category = "Food"; Price = 20.0 }
            { Sku = "C"; Category = "Book"; Price = 30.0 }
        ]
        use df = DataFrame.ofRecords items

        // Act 1: countBy Category
        let counts =
            df
            |> DataFrame.toRows<ProductItem>
            |> Rows.countBy (fun r -> r.Category)

        // Act 2: scan running total
        let runningTotals =
            df
            |> DataFrame.toRows<ProductItem>
            |> Rows.scan (fun acc r -> acc + r.Price) 0.0
            |> Seq.toArray

        // Assert: Category frequency
        Assert.Equal(2L, counts.["Book"])
        Assert.Equal(1L, counts.["Food"])

        // Assert: scan outputs [initial; +10; +20; +30]
        Assert.Equal<float>([| 0.0; 10.0; 30.0; 60.0 |], runningTotals)

    [<Fact>]
    [<Trait("DataFrame", "Enumerator")>]
    member _.``DataFrame.Rows<'T>: iterates records with duck-typing for loop`` () =
        // Arrange
        let names = pl.series "Name" [ "Alice"; "Bob"; "Charlie" ]
        let ids = pl.series "Id" [ 1; 2; 3 ]
        let scores = pl.series "Score" [ 90.5; 85.0; 95.5 ]

        use df = DataFrame.create [ ids; names; scores ]

        // Act - Duck-typing for loop directly on RowEnumerator<'T>
        let mutable count = 0
        let mutable sumScore = 0.0

        for person in df.Rows<SimplePerson>() do
            count <- count + 1
            sumScore <- sumScore + person.Score
            if person.Id = 1 then
                Assert.Equal("Alice", person.Name)

        // Assert
        Assert.Equal(3, count)
        Assert.Equal(271.0, sumScore)

    [<Fact>]
    [<Trait("DataFrame", "Enumerator")>]
    member _.``DataFrame.Rows<'T>: ToArray and ToList pre-allocates correctly`` () =
        // Arrange
        let ids = pl.series "Id" [ 10; 20 ]
        let names = pl.series "Name" [ "Dev1"; "Dev2" ]
        let scores = pl.series "Score" [ 100.0; 99.0 ]

        use df = DataFrame.create [ ids; names; scores ]

        // Act
        let arr = df.Rows<SimplePerson>().ToArray()
        let list = df.Rows<SimplePerson>().ToList()

        // Assert
        Assert.Equal(2, arr.Length)
        Assert.Equal(2, list.Length)
        Assert.Equal("Dev1", arr.[0].Name)
        Assert.Equal(100.0, list.[0].Score)
        Assert.Equal("Dev2", arr.[1].Name)

    [<Fact>]
    [<Trait("DataFrame", "Enumerator")>]
    member _.``DataFrame.Rows<'T>: First and TryFirst variants`` () =
        // Arrange
        let ids = pl.series "Id" [ 42 ]
        let names = pl.series "Name" [ "Zaphod" ]
        let scores = pl.series "Score" [ 42.0 ]

        use df = DataFrame.create [ ids; names; scores ]

        // Act
        let firstRow = df.Rows<SimplePerson>().First()
        let tryFirstRow = df.Rows<SimplePerson>().TryFirst()
        let tryFirstValueRow = df.Rows<SimplePerson>().TryFirstValue()

        // Assert
        Assert.Equal("Zaphod", firstRow.Name)
        Assert.True(tryFirstRow.IsSome)
        Assert.Equal(42, tryFirstRow.Value.Id)
        Assert.True(tryFirstValueRow.IsValueSome)
        Assert.Equal(42.0, tryFirstValueRow.Value.Score)

    [<Fact>]
    [<Trait("DataFrame", "Enumerator")>]
    member _.``DataFrame.Rows<'T>: correctly hydrates Option and ValueOption fields`` () =
        // Arrange
        let ids = pl.series "Id" [ 1; 2 ]
        let nicknames = pl.series "Nickname" [ Some "Ali"; None ]
        let bonuses = pl.series "Bonus" [ ValueSome 150.0; ValueNone ]

        use df = DataFrame.create [ ids; nicknames; bonuses ]

        // Act
        let rows = df.Rows<NullablePerson>().ToArray()

        // Assert
        Assert.Equal(2, rows.Length)

        // Row 0
        Assert.Equal(Some "Ali", rows.[0].Nickname)
        Assert.Equal(ValueSome 150.0, rows.[0].Bonus)

        // Row 1
        Assert.True(rows.[1].Nickname.IsNone)
        Assert.True(rows.[1].Bonus.IsValueNone)

    [<Fact>]
    [<Trait("DataFrame", "Enumerator")>]
    member _.``DataFrame.Rows<'T>: throws on null if Record field is not an Option`` () =
        // Arrange
        let ids = pl.series "Id" [ 1; 2 ]
        let grades = pl.series "Grade" [ Some 100; None ] // Row 1 is null

        use df = DataFrame.create [ ids; grades ]

        // Act & Assert
        let ex = Assert.Throws<InvalidOperationException>(fun () ->
            for _ in df.Rows<StrictStudent>() do ()
        )

        Assert.Contains("Record field 'Grade' does not accept Option", ex.Message)

    [<Fact>]
    [<Trait("DataFrame", "Enumerator")>]
    member _.``DataFrame.Rows<'T>: empty DataFrame behavior`` () =
        // Arrange
        let ids = pl.series "Id" []
        let names = pl.series "Name" []
        let scores = pl.series "Score" []

        use df = DataFrame.create [ ids; names; scores ]

        // Act & Assert
        let enumerator = df.Rows<SimplePerson>()
        Assert.Empty(enumerator.ToArray())
        Assert.Empty(enumerator.ToList())
        Assert.True(enumerator.TryFirst().IsNone)
        Assert.True(enumerator.TryFirstValue().IsValueNone)

        Assert.Throws<InvalidOperationException>(fun () ->
            df.Rows<SimplePerson>().First() |> ignore
        ) |> ignore

    [<Fact>]
    [<Trait("DataFrame", "Enumerator")>]
    member _.``DataFrame.Rows<'T>: supports mutable classes with parameterless ctor`` () =
        // Arrange
        let ids = pl.series "Id" [ 101; 102 ]
        let names = pl.series "Name" [ "Mutable1"; "Mutable2" ]

        // Act
        let dtoArray =
            pl.dataframe [ ids; names ]
            |> DataFrame.toRows<MutablePersonDto>
            |> Rows.toArray

        // Assert
        Assert.Equal(2, dtoArray.Length)
        Assert.Equal(101, dtoArray.[0].Id)
        Assert.Equal("Mutable1", dtoArray.[0].Name)
        Assert.Equal(102, dtoArray.[1].Id)
        Assert.Equal("Mutable2", dtoArray.[1].Name)
    [<Fact>]
    [<Trait("DataFrame", "Enumerator")>]
    member _.``DataFrame.Rows<'T>: CopyTo fills Span with zero heap allocation`` () =
        // Arrange
        let ids = pl.series "Id" [ 1; 2; 3 ]
        let names = pl.series "Name" [ "A"; "B"; "C" ]
        let scores = pl.series "Score" [ 10.0; 20.0; 30.0 ]

        use df = DataFrame.create [ ids; names; scores ]

        // Act - stackalloc buffer or rent
        let buffer = Array.zeroCreate<SimplePerson> 3
        let written = df.Rows<SimplePerson>().CopyTo(buffer.AsSpan())

        // Assert
        Assert.Equal(3, written)
        Assert.Equal("B", buffer.[1].Name)
        Assert.Equal(20.0, buffer.[1].Score)

    [<Fact>]
    [<Trait("DataFrame", "Enumerator")>]
    member _.``DataFrame.Rows<'T>: direct indexer Item access`` () =
        // Arrange
        let ids = pl.series "Id" [ 100; 200 ]
        let names = pl.series "Name" [ "First"; "Second" ]
        let scores = pl.series "Score" [ 1.0; 2.0 ]

        use df = DataFrame.create [ ids; names; scores ]

        // Act
        let enumerator = df.Rows<SimplePerson>()
        let row1 = enumerator.[1L]

        // Assert
        Assert.Equal("Second", row1.Name)
        Assert.Equal(200, row1.Id)
    [<Fact>]
    [<Trait("DataFrame","HStack")>]
    member _.``Test HStack and VStack``() =

        // a: [1, 2, 3]
        use df1 =
            DataFrame.create [
                Series.create("a", [1; 2; 3])
            ]

        // b: [10, 20, 30]
        use s_new = Series.create("b", [10; 20; 30])

        use h_stacked =
            df1
            |> DataFrame.hstack [s_new]

        Assert.Equal(3L, h_stacked.Height)
        Assert.Equal(2L, h_stacked.Width)

        let cols = h_stacked.ColumnNames
        Assert.Equal("a", cols.[0])
        Assert.Equal("b", cols.[1])

        Assert.Equal(10,h_stacked.Cell<int>("b",0)) // Row 0, Col "b"

        // DF2: [a, b]
        // a: [4, 5]
        // b: [40, 50]
        use df2 =
            DataFrame.create [
                Series.create("a", [4; 5])
                Series.create("b", [40; 50])
            ]

        use v_stacked =
            h_stacked
            |> DataFrame.vstack df2

        Assert.Equal(5L, v_stacked.Height)
        Assert.Equal(2L, v_stacked.Width)

        Assert.Equal(1,v_stacked.Cell("a", 0))

        Assert.Equal(4,v_stacked.["a"].GetValue<int> 3)

        Assert.Equal(50,v_stacked.["b"].GetValue<int> 4)
    [<Fact>]
    [<Trait("DataFrame","AsTensor")>]
    member _.``DataFrame: AsTensor extracts all columns to Row-Major 2D Tensor`` () =

        let s1 = Series.ofSeqNamed "feature1" [| 1.1f; 2.1f; 3.1f |]
        let s2 = Series.ofSeqNamed "feature2" [| 1.2f; 2.2f; 3.2f |]
        use df = DataFrame.create [| s1; s2 |]

        // Act
        let tensor = df.AsTensor<float32>()

        // Assert
        Assert.Equal(2, tensor.Rank)
        Assert.Equal(3, int tensor.Lengths.[0])
        Assert.Equal(2, int tensor.Lengths.[1])

        let valAt r c = tensor.[ReadOnlySpan<nativeint>([| nativeint r; nativeint c |])]

        Assert.Equal(1.1f, valAt 0 0)
        Assert.Equal(1.2f, valAt 0 1)

        Assert.Equal(2.1f, valAt 1 0)
        Assert.Equal(2.2f, valAt 1 1)

        Assert.Equal(3.1f, valAt 2 0)
        Assert.Equal(3.2f, valAt 2 1)

    [<Fact>]
    [<Trait("DataFrame","AsTensor")>]
    member _.``DataFrame: AsTensor extracts specifically selected columns`` () =
        let s1 = Series.create("id", [| 1; 2 |])
        let s2 = Series.create("feature1", [| 0.1f; 0.2f |])
        let s3 = Series.create("feature2", [| 0.9f; 0.8f |])
        use df = DataFrame.create [| s1; s2; s3 |]

        let tensor = df.AsTensor<float32>("feature1", "feature2")

        Assert.Equal(2, tensor.Rank)
        Assert.Equal(2, int tensor.Lengths.[0]) // 2 Height
        Assert.Equal(2, int tensor.Lengths.[1]) // 2 Columns

        let valAt r c = tensor.[ReadOnlySpan<nativeint>([| nativeint r; nativeint c |])]

        Assert.Equal(0.1f, valAt 0 0)
        Assert.Equal(0.9f, valAt 0 1)

        Assert.Equal(0.2f, valAt 1 0)
        Assert.Equal(0.8f, valAt 1 1)

    [<Fact>]
    [<Trait("DataFrame","AsTensorEmpty")>]
    member _.``DataFrame: AsTensor thHeight InvalidOperationException on empty DataFrame`` () =
        use df = DataFrame.create [||]

        let ex = Assert.Throws<InvalidOperationException>(fun () ->
            df.AsTensor<float32>() |> ignore
        )

        Assert.Contains("Cannot create a Tensor from an empty DataFrame", ex.Message)

    [<Fact>]
    [<Trait("DataFrame","AsTensorException")>]
    member _.``DataFrame: AsTensor throw Exception on type mismatch`` () =
        // Arrange
        let s1 = Series.create("age", [| 25; 30 |])
        let s2 = Series.create("salary", [| 5000.5f; 6000.5f |])
        use df = DataFrame.create [| s1; s2 |]

        let ex = Assert.Throws<InvalidOperationException>(fun () ->
            df.AsTensor<float32>() |> ignore
        )

        Assert.NotNull ex
    [<Fact>]
    [<Trait("DataFrame", "Creation")>]
    member _.``DataFrame: ofMaps infers schema, promotes types and handles missing values`` () =

        let data = [
            Map [ "Id", box 1; "Name", box "Alice"; "Value", box 10.5 ]

            Map [ "Id", box 2; "Value", box 20; "Age", box 30 ]

            Map [ "Id", box 3; "Name", box "Bob"; "Age", box 25 ]
        ]

        // Act
        use df = DataFrame.ofMaps data

        // Assert - Shape
        let columns = df.GetColumns()
        Assert.Equal(4, columns.Length)
        Assert.Equal(3, int df.Height)

        let getCol name = columns |> Array.find (fun c -> c.Name = name)

        let idCol = getCol "Id"
        Assert.Equal(Some 1, idCol.GetValueOption<int>(0L))
        Assert.Equal(Some 2, idCol.GetValueOption<int>(1L))
        Assert.Equal(Some 3, idCol.GetValueOption<int>(2L))

        let nameCol = getCol "Name"
        Assert.Equal(Some "Alice", nameCol.GetValueOption<string>(0L))

        Assert.Equal(None, nameCol.GetValueOption<string>(1L))
        Assert.Equal(Some "Bob", nameCol.GetValueOption<string>(2L))

        let valCol = getCol "Value"
        Assert.Equal(Some 10.5, valCol.GetValueOption<double>(0L))

        Assert.Equal(Some 20.0, valCol.GetValueOption<double>(1L))
        Assert.Equal(None, valCol.GetValueOption<double>(2L))

        let ageCol = getCol "Age"
        Assert.Equal(None, ageCol.GetValueOption<int>(0L))
        Assert.Equal(Some 30, ageCol.GetValueOption<int>(1L))
        Assert.Equal(Some 25, ageCol.GetValueOption<int>(2L))
    [<Fact>]
    [<Trait("DataFrame", "ToRecords")>]
    member _.``DataFrame: ToRecords<'T> instantiates F# Records and handles Options correctly`` () =

        let data = [
            Map [ "Id", box 1; "Name", box "Alice"; "Score", box 95.5 ]
            Map [ "Id", box 2; "Name", box "Bob" ]
            Map [ "Id", box 3; "Name", box "Charlie"; "Score", box 88.0 ]
        ]
        use df = DataFrame.ofMaps data

        let employees = df.ToRecords<Seitou>() |> Seq.toArray

        // Assert
        Assert.Equal(3, employees.Length)

        Assert.Equal(1, employees.[0].Id)
        Assert.Equal("Alice", employees.[0].Name)
        Assert.Equal(Some 95.5, employees.[0].Score)

        Assert.Equal(2, employees.[1].Id)
        Assert.Equal("Bob", employees.[1].Name)
        Assert.Equal(None, employees.[1].Score)

        Assert.Equal(3, employees.[2].Id)
        Assert.Equal("Charlie", employees.[2].Name)
        Assert.Equal(Some 88.0, employees.[2].Score)

    [<Fact>]
    [<Trait("DataFrame", "ToRecords")>]
    member _.``DataFrame: ToRecords<'T> throw on null if Record field is not an Option`` () =

        // Arrange
        let data = [
            Map [ "Id", box 1; "Name", box "Alice"; "Score", box 95.5 ]
            Map [ "Id", box 2; "Name", box "Bob" ]
        ]
        use df = DataFrame.ofMaps data

        // Act & Assert
        let ex = Assert.Throws<InvalidOperationException>(fun () ->
            df.ToRecords<StrictSeitou>() |> Seq.toArray |> ignore
        )

        Assert.Contains("Record field 'Score' does not accept Option", ex.Message)

    [<Fact>]
    [<Trait("DataFrame", "ToRecords")>]
    member _.``DataFrame: ToRecords<'T> rejects non-Record types`` () =
        use df = DataFrame.ofMaps [ Map ["Id", box 1] ]

        let ex = Assert.Throws<ArgumentException>(fun () ->
            df.ToRecords<Tuple<int, string>>() |> Seq.toArray |> ignore
        )

        Assert.Contains("not an F# Record type", ex.Message)
    [<Fact>]
    [<Trait("DataFrame", "Slice")>]
    member _. ``DataFrame slicing using F# native syntax works correctly`` () =
        // Arrange: Create a simple DataFrame with 5 rows (0, 1, 2, 3, 4)
        let s = Series.create("values", [| 0; 1; 2; 3; 4 |])
        let df = DataFrame.create [| s |]

        // Act & Assert: Test various F# slicing patterns

        // 1. Standard slice [start..finish] (Inclusive in F#)
        // Should include index 1, 2, 3 -> Height = 3
        let slice1 = df.[1..3]
        Assert.Equal(3L, slice1.Height)

        // 2. Open-ended right [start..]
        // Should include index 2 to the end (2, 3, 4) -> Height = 3
        let slice2 = df.[2..]
        Assert.Equal(3L, slice2.Height)

        // 3. Open-ended left [..finish]
        // Should include from start to index 2 (0, 1, 2) -> Height = 3
        let slice3 = df.[..2]
        Assert.Equal(3L, slice3.Height)

        // 4. Negative start index [-start..] (Counting from the end)
        // Should include the last two elements (3, 4) -> Height = 2
        let slice4 = df.[-2..]
        Assert.Equal(2L, slice4.Height)

        // 6. Out of bounds slice
        // Should gracefully return an empty DataFrame instead of throwing
        let slice6 = df.[10..20]
        Assert.Equal(0L, slice6.Height)
    [<Fact>]
    [<Trait("DataFrame", "InsertColumn")>]
    member _. ``InsertColumn with Series inserts at correct index and supports negative indexing`` () =
        // Arrange: Create a DataFrame with columns "A" and "C"
        let sA = Series.create("A", [| 1; 2; 3 |])
        let sC = Series.create("C", [| 7; 8; 9 |])
        let df = DataFrame.create([| sA; sC |])

        // New column to insert
        let sB = Series.create("B", [| 4; 5; 6 |])

        // Act 1: Insert "B" at index 1 (between "A" and "C")
        let result1 = df.InsertColumn(1, sB)

        // Assert 1
        Assert.Equal(3, int result1.Width)
        Assert.Equal("A", result1.Columns.[0])
        Assert.Equal("B", result1.Columns.[1])
        Assert.Equal("C", result1.Columns.[2])

        // Act 2: Test negative indexing (-1 meaning before the last column)
        let sNew = Series.create("New", [| 10; 11; 12 |])
        let result2 = df.InsertColumn(-1, sNew)

        // Assert 2: Original df had 2 columns, -1 index resolves to index 1 (width 2 + (-1) = 1)
        Assert.Equal(3, int result2.Width)
        Assert.Equal("A", result2.Columns.[0])
        Assert.Equal("New", result2.Columns.[1])
        Assert.Equal("C", result2.Columns.[2])

        // Act 3: Test out-of-bounds error handling
        let action = fun () -> df.InsertColumn(5, sNew) |> ignore
        Assert.Throws<ArgumentOutOfRangeException>(action) |> ignore

    [<Fact>]
    [<Trait("DataFrame", "ReplaceColumn")>]
    member _.``ReplaceColumn by index replaces in-place with correct keepName behavior`` () =
        // Arrange: Create a DataFrame with columns "A" and "B"
        let sA = Series.create("A", [| 10; 20 |])
        let sB = Series.create("B", [| 30; 40 |])
        let df = DataFrame.create([| sA; sB |])

        let sNew = Series.create("NewName", [| 100; 200 |])

        // Act 1: Replace column at index 1 ("B"), keepName = false (defaultArg fallback)
        let result1 = df.ReplaceColumn(1, sNew)

        // Assert 1: The instance is updated and the name is replaced by the new Series's name
        Assert.Equal("A", result1.Columns.[0])
        Assert.Equal("NewName", result1.Columns.[1])

        // Act 2: Replace column at index 0 ("A"), keepName = true
        let sAnother = Series.create("AnotherName", [| 500; 600 |])
        let result2 = df.ReplaceColumn(0, sAnother, keepName = true)

        // Assert 2: The column at index 0 is updated but retains its original name "A"
        Assert.Equal("A", result2.Columns.[0])

        // Act 3: Test negative indexing for replacement (-1 replaces the last column)
        let sLast = Series.create("FinalName", [| 99; 99 |])
        let result3 = df.ReplaceColumn(-1, sLast)
        Assert.Equal("FinalName", result3.Columns.[1])

    [<Fact>]
    [<Trait("DataFrame", "ReplaceColumn")>]
    member _.``ReplaceColumn by name replaces in-place with correct keepName behavior`` () =
        // Arrange: Create a DataFrame with columns "X" and "Y"
        let sX = pl.series "X" [| 1.0; 2.0 |]
        let sY = pl.series "Y" [| 3.0; 4.0 |]
        let df = pl.dataframe [| sX; sY |]

        let sNew = pl.series "Replacement" [| 5.0; 6.0 |]

        // Act 1: Replace column "Y", keepName = true (defaultArg fallback)
        let result1 = df.ReplaceColumn("Y", sNew)

        // Assert 1: Name "Y" should be kept
        Assert.Equal("X", result1.Columns.[0])
        Assert.Equal("Y", result1.Columns.[1])

        // Act 2: Replace column "X", keepName = false
        let sAnother = pl.series "NoKeep" [| 7.0; 8.0 |]
        let result2 = df.ReplaceColumn("X", sAnother, keepName = false)

        // Assert 2: Name should change to the series name "NoKeep"
        Assert.Equal("NoKeep", result2.Columns.[0])

        // Act 3: Attempting to replace a non-existent column name should throw
        let action = fun () -> df.ReplaceColumn("NonExistent", sNew) |> ignore
        Assert.Throws<PolarsException>(action) |> ignore
    [<Fact>]
    [<Trait("DataFrame", "ToDummies")>]
    member _.``ToDummies converts categorical columns and respects optional parameters`` () =
        // Arrange: Setup a dataframe with a string column and an integer column
        let sColor = Series.create("color", [| "red"; "blue" |])
        let sId = Series.create("id", [| 1; 2 |])
        let df = DataFrame.create([| sColor; sId |])
        // Act 1: Call ToDummies with default parameters (auto-detect string/categorical columns)
        let result1 = df.ToDummies()
        // Assert 1: "color" should be converted into dummy columns "color_blue" and "color_red"
        Assert.Equal(3, int result1.Width)
        Assert.Equal("id", result1.Columns.[2])
        // Act 2: Test specifying columns explicitly using native F# List (implied as seq)
        let result2 = df.ToDummies(columns = [ "color" ], separator = "-")
        // Assert 2: Separator should be modified
        Assert.Equal(3, int result2.Width)

        // Act 3: Test empty columns sequence exception branch
        let action = fun () -> df.ToDummies(columns = []) |> ignore
        Assert.Throws<ArgumentException>(action) |> ignore
    [<Fact>]
    [<Trait("DataFrame", "Map")>]
    member _.``DataFrame map transforms each column and preserves names by default`` () =
        // Arrange: Create a DataFrame with two columns
        let sA = pl.series "A" [| 1.0; 2.0; 3.0 |]
        let sB = pl.series "B" [| 10.0; 20.0; 30.0 |]
        use df = pl.dataframe [| sA; sB |]

        // Act: Double each numeric column without renaming
        use result =
            df
            |> DataFrame.map (fun s -> s * 2.0)

        // Assert: Column names and dimensions should be preserved, values doubled
        Assert.Equal(2L, result.Width)
        Assert.Equal(3L, result.Height)
        Assert.Equal("A", result.Columns.[0])
        Assert.Equal("B", result.Columns.[1])
        Assert.Equal(2.0, result.["A"].GetValue<double>(0L))
        Assert.Equal(20.0, result.["B"].GetValue<double>(0L))

    [<Fact>]
    [<Trait("DataFrame", "Map")>]
    member _.``DataFrame map respects explicit renamed series from mapping function`` () =
        // Arrange: Create a DataFrame
        let sX = pl.series "X" [| 100L; 200L |]
        use df = pl.dataframe [| sX |]

        // Act: Map column and explicitly rename it
        use result =
            df
            |> DataFrame.map (fun s -> (s + 1L).Rename "RenamedX")

        // Assert: Name should reflect the explicitly updated alias
        Assert.Equal("RenamedX", result.Columns.[0])
        Assert.Equal(101L, result.["RenamedX"].GetValue<int64>(0L))

    [<Fact>]
    [<Trait("DataFrame", "Map")>]
    member _.``DataFrame map on empty dataframe returns empty dataframe`` () =
        // Arrange: Empty DataFrame
        use emptyDf = pl.dataframe []

        // Act: Apply mapping
        use result =
            emptyDf
            |> DataFrame.map (fun s -> s)

        // Assert: Remains empty
        Assert.Equal(0L, result.Width)
        Assert.Equal(0L, result.Height)

    [<Fact>]
    [<Trait("DataFrame", "Map")>]
    member _.``DataFrame map throws ArgumentNullException when mapping is null`` () =
        // Arrange: Create a DataFrame
        let s = pl.series "Col" [| 1 |]
        use df = pl.dataframe [| s |]

        // Act & Assert
        let action = fun () -> df.Map (Unchecked.defaultof<Series -> Series>) |> ignore
        Assert.Throws<ArgumentNullException>(action) |> ignore
    [<Fact>]
    [<Trait("DataFrame", "MapRows")>]
    member _.``DataFrame mapRows transforms typed records with mixed types correctly`` () =
        // Arrange: Create source DataFrame from typed records
        let now = DateTime(2026, 9, 20, 12, 0, 0, DateTimeKind.Utc)
        let data = [
            { City = "Tokyo";    Celsius = 25.0;  RecordedAt = now }
            { City = "Harbin";   Celsius = -5.0;  RecordedAt = now.AddHours(1.0) }
            { City = "Helsinki"; Celsius = 0.0;   RecordedAt = now.AddHours(2.0) }
        ]
        use df = DataFrame.ofRecords data

        // Act: Apply mapRows transforming Celsius to Fahrenheit and computing freezing flag
        use result =
            df
            |> DataFrame.mapRows (fun (r: TemperatureReading) ->
                {
                    City = r.City.ToUpperInvariant()
                    Fahrenheit = r.Celsius * 1.8 + 32.0
                    IsFreezing = r.Celsius <= 0.0
                    RecordedAt = r.RecordedAt
                }
            )

        // Assert: Verify dimensions and schema
        Assert.Equal(3L, result.Height)
        Assert.Equal(4L, result.Width)
        Assert.Equal<string>([| "City"; "Fahrenheit"; "IsFreezing"; "RecordedAt" |], result.Columns)

        // Assert: Row 0 (Tokyo)
        Assert.Equal("TOKYO", result.["City"].GetValue<string>(0L))
        Assert.Equal(77.0, result.["Fahrenheit"].GetValue<double>(0L))
        Assert.False(result.["IsFreezing"].GetValue<bool>(0L))
        Assert.Equal(now, result.["RecordedAt"].GetValue<DateTime>(0L))

        // Assert: Row 1 (Harbin)
        Assert.Equal("HARBIN", result.["City"].GetValue<string>(1L))
        Assert.Equal(23.0, result.["Fahrenheit"].GetValue<double>(1L))
        Assert.True(result.["IsFreezing"].GetValue<bool>(1L))

        // Assert: Row 2 (Helsinki)
        Assert.Equal("HELSINKI", result.["City"].GetValue<string>(2L))
        Assert.Equal(32.0, result.["Fahrenheit"].GetValue<double>(2L))
        Assert.True(result.["IsFreezing"].GetValue<bool>(2L))

    [<Fact>]
    [<Trait("DataFrame", "MapRows")>]
    member _.``DataFrame mapRows handles empty dataframe while preserving output schema`` () =
        // Arrange: Empty source DataFrame
        use emptyDf = DataFrame.ofRecords<SimpleInput> []

        // Act: Apply mapping on empty DataFrame
        use result =
            emptyDf
            |> DataFrame.mapRows (fun (r: SimpleInput) ->
                { Id = r.Id; Doubled = r.Value * 2.0 }
            )

        // Assert: Should produce an empty DataFrame with output Record column schema
        Assert.Equal(0L, result.Height)
        Assert.Equal(2L, result.Width)
        Assert.Equal("Id", result.Columns.[0])
        Assert.Equal("Doubled", result.Columns.[1])

    [<Fact>]
    [<Trait("DataFrame", "MapRows")>]
    member _.``DataFrame mapRows rejects non-Record input type with ArgumentException`` () =
        // Arrange: DataFrame created from maps/columns
        use df = pl.dataframe [| pl.series "Id" [| 1; 2 |] |]

        // Act & Assert: Attempting to map rows to/from non-Record types should be rejected
        let action = fun () ->
            df.MapRows (fun (t: Tuple<int>) -> { Id = t.Item1; Doubled = 0.0 }) |> ignore

        let ex = Assert.Throws<ArgumentException>(action)
        Assert.Contains("is not an F# Record type", ex.Message)

    [<Fact>]
    [<Trait("DataFrame", "MapRows")>]
    member _.``DataFrame mapRows rejects non-Record output type with ArgumentException`` () =
        // Arrange: Valid Record DataFrame
        use df = DataFrame.ofRecords [ { Id = 1; Value = 10.0 } ]

        // Act & Assert: Returning a non-Record class or tuple
        let action = fun () ->
            df.MapRows (fun (r: SimpleInput) -> NonRecordClass(r.Id)) |> ignore

        let ex = Assert.Throws<ArgumentException>(action)
        Assert.Contains("is not an F# Record type", ex.Message)

    [<Fact>]
    [<Trait("DataFrame", "MapRows")>]
    member _.``DataFrame mapRows throws ArgumentNullException when mapping is null`` () =
        // Arrange: Valid Record DataFrame
        use df = DataFrame.ofRecords [ { Id = 1; Value = 10.0 } ]

        // Act & Assert: Passing null delegate
        let action = fun () ->
            df.MapRows (Unchecked.defaultof<SimpleInput -> SimpleOutput>) |> ignore

        Assert.Throws<ArgumentNullException>(action) |> ignore
    [<Fact>]
    [<Trait("DataFrame", "IterRows")>]
    member _.``DataFrame iterRows traverses all rows and triggers side effects`` () =
        // Arrange: Prepare test logs in a DataFrame
        let logs = [
            { Timestamp = 1001L; Service = "Auth"; LatencyMs = 12.5 }
            { Timestamp = 1002L; Service = "Gateway"; LatencyMs = 45.0 }
            { Timestamp = 1003L; Service = "Payment"; LatencyMs = 120.8 }
        ]
        use df = DataFrame.ofRecords logs

        let consumedServices = ResizeArray<string>()
        let mutable totalLatency = 0.0

        // Act: Consume rows using iterRows
        df
        |> DataFrame.iterRows (fun (entry: LogEntry) ->
            consumedServices.Add entry.Service
            totalLatency <- totalLatency + entry.LatencyMs
        )

        // Assert: Verify all rows were observed in sequential order
        Assert.Equal(3, consumedServices.Count)
        Assert.Equal<string>([ "Auth"; "Gateway"; "Payment" ], consumedServices)
        Assert.Equal(178.3, totalLatency, 2)

    [<Fact>]
    [<Trait("DataFrame", "IterRows")>]
    member _.``DataFrame iteriRows provides correct row index during iteration`` () =
        // Arrange: Sample DataFrame
        let logs = [
            { Timestamp = 1L; Service = "A"; LatencyMs = 1.0 }
            { Timestamp = 2L; Service = "B"; LatencyMs = 2.0 }
        ]
        use df = DataFrame.ofRecords logs
        let observed = ResizeArray<int64 * string * float>()

        // Act: Track iteration indices along with row record fields
        df
        |> DataFrame.iteriRows (fun idx (entry: LogEntry) ->
            observed.Add((idx, entry.Service, entry.LatencyMs))
        )

        // Assert: Verify both row indices and hydrated row contents are strictly aligned
        Assert.Equal(2, observed.Count)
        Assert.Equal((0L, "A", 1.0), observed.[0])
        Assert.Equal((1L, "B", 2.0), observed.[1])

    [<Fact>]
    [<Trait("DataFrame", "IterRows")>]
    member _.``DataFrame iterRows handles empty DataFrame without invoking action`` () =
        // Arrange: Empty DataFrame
        use emptyDf = DataFrame.ofRecords<LogEntry> []
        let mutable invoked = false

        // Act: Iterate over empty DataFrame
        emptyDf
        |> DataFrame.iterRows (fun _ -> invoked <- true)

        // Assert: Action should not be invoked
        Assert.False(invoked)

    [<Fact>]
    [<Trait("DataFrame", "IterRows")>]
    member _.``DataFrame iterRows throws ArgumentNullException when action is null`` () =
        // Arrange: Sample DataFrame
        use df = DataFrame.ofRecords [ { Timestamp = 1L; Service = "A"; LatencyMs = 1.0 } ]

        // Act & Assert
        let action = fun () -> df.IterRows<LogEntry>(Unchecked.defaultof<LogEntry -> unit>)
        Assert.Throws<ArgumentNullException>(action) |> ignore
    [<Fact>]
    [<Trait("DataFrame", "Iter2")>]
    member _.``DataFrame iter2 synchronizes rows from two DataFrames with zero allocation`` () =
        // Arrange: Source DF and Score DF with identical heights
        use dfSource = DataFrame.ofRecords [
            { Id = 101; Label = "Alpha" }
            { Id = 102; Label = "Beta" }
        ]
        use dfMetrics = DataFrame.ofRecords [
            { Score = 0.95 }
            { Score = 0.88 }
        ]

        let zipped = ResizeArray<int * string * float>()

        // Act: Synchronously consume both DataFrames
        DataFrame.iter2 (fun (src: SourceRecord) (met: MetricRecord) ->
            zipped.Add((src.Id, src.Label, met.Score))
        ) dfSource dfMetrics

        // Assert
        Assert.Equal(2, zipped.Count)
        Assert.Equal((101, "Alpha", 0.95), zipped.[0])
        Assert.Equal((102, "Beta", 0.88), zipped.[1])

    [<Fact>]
    [<Trait("DataFrame", "Iter2")>]
    member _.``DataFrame iter2 throws ArgumentException when heights mismatch`` () =
        use df1 = DataFrame.ofRecords [ { Id = 1; Label = "A" } ]
        use df2 = DataFrame.ofRecords [ { Score = 1.0 }; { Score = 2.0 } ]

        let action = fun () -> df1.Iter2<SourceRecord, MetricRecord>(df2, fun _ _ -> ())
        Assert.Throws<ArgumentException>(action) |> ignore
