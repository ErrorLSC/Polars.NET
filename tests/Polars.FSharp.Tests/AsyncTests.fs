namespace Polars.FSharp.Tests

open System
open System.Threading
open System.Threading.Tasks
open Xunit
open Polars.FSharp

type AsyncSensorRecord =
    { SensorId: int
      Temperature: float
      Location: string }

type AsyncTests() =

    [<Fact>]
    [<Trait("Async", "DataFrame")>]
    member _.``DataFrame RowsAsync streams strongly-typed records into FSharp list`` () =
        task {
            // Arrange: 4 sensor readings
            let sId = pl.series "SensorId" [| 101; 102; 103; 104 |]
            let sTemp = pl.series "Temperature" [| 21.5; 26.0; 19.8; 28.5 |]
            let sLoc = pl.series "Location" [| "Room_A"; "Room_B"; "Room_A"; "Room_C" |]
            use df = pl.dataframe [| sId; sTemp; sLoc |]

            // Act: Stream rows using small chunkSize (2) to exercise micro-batch boundary
            let! results =
                df.RowsAsync<AsyncSensorRecord>(chunkSize = 2)
                |> RowsAsync.toList

            // Assert: Exactly 4 rows hydrated into idiomatic F# record list
            Assert.Equal(4, results.Length)
            Assert.Equal({ SensorId = 101; Temperature = 21.5; Location = "Room_A" }, results.[0])
            Assert.Equal({ SensorId = 104; Temperature = 28.5; Location = "Room_C" }, results.[3])
        }
    [<Fact>]
    [<Trait("Async", "LazyFrame")>]
    member _.``LazyFrame RowsAsync applies filter pushdown and streams collected rows`` () =
        task {
            // Arrange: Sensor data with temperatures spanning thresholds
            let sId = pl.series "SensorId" [| 101; 102; 103; 104; 105 |]
            let sTemp = pl.series "Temperature" [| 21.5; 26.0; 19.8; 28.5; 23.0 |]
            let sLoc = pl.series "Location" [| "Room_A"; "Room_B"; "Room_A"; "Room_C"; "Room_B" |]

            // Construct lazy query plan: Temperature >= 23.0
            use lf =
                pl.dataframe [| sId; sTemp; sLoc |] 
                |> DataFrame.asLazy
                |> LazyFrame.filter (pl.col "Temperature" .>= pl.lit 23.0)

            // Act: Collect plan asynchronously and stream resulting records
            let! hotSensors =
                lf.RowsAsync<AsyncSensorRecord>(chunkSize = 2)
                |> RowsAsync.toList

            // Assert: Exactly 3 rows match the pushdown filter condition
            Assert.Equal(3, hotSensors.Length)
            Assert.Equal(102, hotSensors.[0].SensorId)
            Assert.Equal(104, hotSensors.[1].SensorId)
            Assert.Equal(105, hotSensors.[2].SensorId)
        }

    [<Fact>]
    [<Trait("Async", "Combinators")>]
    member _.``RowsAsync fold computes aggregate across async stream correctly`` () =
        task {
            // Arrange
            let sId = pl.series "SensorId" [| 1; 2; 3; 4 |]
            let sTemp = pl.series "Temperature" [| 10.0; 20.0; 30.0; 40.0 |]
            let sLoc = pl.series "Location" [| "A"; "B"; "C"; "D" |]
            use df = pl.dataframe [| sId; sTemp; sLoc |]

            // Act: Fold sum of temperatures across async stream
            let! totalTemp =
                df.RowsAsync<AsyncSensorRecord>(chunkSize = 2)
                |> RowsAsync.fold (fun acc s -> acc + s.Temperature) 0.0

            // Assert: Accurate accumulated aggregate
            Assert.Equal(100.0, totalTemp)
        }

    [<Fact>]
    [<Trait("Async", "Cancellation")>]
    member _.``RowsAsync cancellation throws OperationCanceledException`` () =
        task {
            // Arrange
            let sId = pl.series "SensorId" [| 1; 2; 3 |]
            let sTemp = pl.series "Temperature" [| 15.0; 16.0; 17.0 |]
            let sLoc = pl.series "Location" [| "A"; "B"; "C" |]
            use df = pl.dataframe [| sId; sTemp; sLoc |]

            use cts = new CancellationTokenSource()
            cts.Cancel() // Pre-cancelled token

            // Act & Assert
            let action = fun () ->
                task {
                    let! _ =
                        df.RowsAsync<AsyncSensorRecord>(chunkSize = 1, cancellationToken = cts.Token)
                        |> RowsAsync.toList
                    ()
                } :> Task

            let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(Func<Task>(action))
            ()
        }
    // Helper to generate a standardized test DataFrame

    [<Fact>]
    [<Trait("Async", "Combinators")>]
    member _.``RowsAsync toList materializes full async stream into FSharp list`` () = task {
        // Arrange
        use df =         
            let sId = pl.series "SensorId" [| 1; 2; 3; 4 |]
            let sTemp = pl.series "Temperature" [| 10.0; 20.0; 30.0; 40.0 |]
            let sLoc = pl.series "Location" [| "A"; "B"; "C"; "D" |]
            pl.dataframe [| sId; sTemp; sLoc |]

        // Act: Drain stream across micro-batches (chunkSize = 2)
        let! items =
            df.RowsAsync<AsyncSensorRecord>(chunkSize = 2)
            |> RowsAsync.toList

        // Assert
        Assert.Equal(4, items.Length)
        Assert.Equal(1, items.[0].SensorId)
        Assert.Equal(4, items.[3].SensorId)
    }

    [<Fact>]
    [<Trait("Async", "Combinators")>]
    member _.``RowsAsync toArray materializes full async stream into array`` () = task {
        // Arrange
        use df = 
            let sId = pl.series "SensorId" [| 1; 2; 3; 4 |]
            let sTemp = pl.series "Temperature" [| 10.0; 20.0; 30.0; 40.0 |]
            let sLoc = pl.series "Location" [| "A"; "B"; "C"; "D" |]
            pl.dataframe [| sId; sTemp; sLoc |]


        // Act
        let! arr =
            df.RowsAsync<AsyncSensorRecord>(chunkSize = 1)
            |> RowsAsync.toArray

        // Assert
        Assert.Equal(4, arr.Length)
        Assert.Equal("B", arr.[1].Location)
    }

    [<Fact>]
    [<Trait("Async", "Combinators")>]
    member _.``RowsAsync filter and map compose functional pipeline correctly`` () = task {
        // Arrange
        use df = 
            let sId = pl.series "SensorId" [| 1; 2; 3; 4 |]
            let sTemp = pl.series "Temperature" [| 10.0; 20.0; 30.0; 40.0 |]
            let sLoc = pl.series "Location" [| "A"; "B"; "C"; "D" |]
            pl.dataframe [| sId; sTemp; sLoc |]

        // Act: Filter rows with Temperature > 15.0 and project their Location
        let! filteredLocations =
            df.RowsAsync<AsyncSensorRecord>(chunkSize = 2)
            |> RowsAsync.filter (fun s -> s.Temperature > 15.0)
            |> RowsAsync.map (fun s -> s.Location)
            |> RowsAsync.toList

        // Assert: Items 2, 3, 4 matched
        Assert.Equal<string list>(["B"; "C"; "D"], filteredLocations)
    }

    [<Fact>]
    [<Trait("Async", "Combinators")>]
    member _.``RowsAsync head fetches first row and terminates stream immediately`` () = task {
        // Arrange
        use df = 
            let sId = pl.series "SensorId" [| 1; 2; 3; 4 |]
            let sTemp = pl.series "Temperature" [| 10.0; 20.0; 30.0; 40.0 |]
            let sLoc = pl.series "Location" [| "A"; "B"; "C"; "D" |]
            pl.dataframe [| sId; sTemp; sLoc |]


        // Act
        let! firstItem =
            df.RowsAsync<AsyncSensorRecord>(chunkSize = 2)
            |> RowsAsync.head

        // Assert
        Assert.Equal(1, firstItem.SensorId)
        Assert.Equal("A", firstItem.Location)
    }

    [<Fact>]
    [<Trait("Async", "Combinators")>]
    member _.``RowsAsync tryHead returns None on empty DataFrame`` () = task {
        // Arrange: Empty DataFrame
        use emptyDf = PolarsSchema.FromRecord<AsyncSensorRecord>().ToDataFrame()

        // Act
        let! headOpt =
            emptyDf.RowsAsync<AsyncSensorRecord>()
            |> RowsAsync.tryHead

        // Assert
        Assert.True(headOpt.IsNone)
    }

    [<Fact>]
    [<Trait("Async", "Combinators")>]
    member _.``RowsAsync tryHeadValue returns ValueSome on populated stream`` () = task {
        // Arrange
        use df =
            let sId = pl.series "SensorId" [| 1; 2; 3; 4 |]
            let sTemp = pl.series "Temperature" [| 10.0; 20.0; 30.0; 40.0 |]
            let sLoc = pl.series "Location" [| "A"; "B"; "C"; "D" |]
            pl.dataframe [| sId; sTemp; sLoc |]


        // Act
        let! headVOpt =
            df
            |> DataFrame.toRowsAsync<AsyncSensorRecord> 2
            |> RowsAsync.tryHeadValue

        // Assert
        match headVOpt with
        | ValueSome r -> Assert.Equal(10.0, r.Temperature)
        | ValueNone -> Assert.Fail("Expected ValueSome but received ValueNone")
    }

    [<Fact>]
    [<Trait("Async", "Combinators")>]
    member _.``RowsAsync length accurately counts elements across batch boundaries`` () = task {
        // Arrange
        use df = 
            let sId = pl.series "SensorId" [| 1; 2; 3; 4 |]
            let sTemp = pl.series "Temperature" [| 10.0; 20.0; 30.0; 40.0 |]
            let sLoc = pl.series "Location" [| "A"; "B"; "C"; "D" |]
            pl.dataframe [| sId; sTemp; sLoc |]

        // Act
        let! totalCount =
            df.RowsAsync<AsyncSensorRecord>(chunkSize = 1)
            |> RowsAsync.length

        // Assert
        Assert.Equal(4L, totalCount)
    }

    [<Fact>]
    [<Trait("Async", "Combinators")>]
    member _.``RowsAsync iteri tracks zero-based row index accurately`` () = task {
        // Arrange
        use df = 
            let sId = pl.series "SensorId" [| 1; 2; 3; 4 |]
            let sTemp = pl.series "Temperature" [| 10.0; 20.0; 30.0; 40.0 |]
            let sLoc = pl.series "Location" [| "A"; "B"; "C"; "D" |]
            pl.dataframe [| sId; sTemp; sLoc |]

        let indices = ResizeArray<int64>()
        let sensorIds = ResizeArray<int>()

        // Act
        do!
            df.RowsAsync<AsyncSensorRecord>(chunkSize = 2)
            |> RowsAsync.iteri (fun idx s ->
                indices.Add(idx)
                sensorIds.Add(s.SensorId)
            )

        // Assert
        Assert.Equal<int64 list>([ 0L; 1L; 2L; 3L ], indices |> Seq.toList)
        Assert.Equal<int list>([ 1; 2; 3; 4 ], sensorIds |> Seq.toList)
    }

    [<Fact>]
    [<Trait("Async", "LazyFrame")>]
    member _.``LazyFrame RowsAsync computes fold correctly over native streaming batches`` () = task {
        // Arrange
        use df = 
            let sId = pl.series "SensorId" [| 1; 2; 3; 4 |]
            let sTemp = pl.series "Temperature" [| 10.0; 20.0; 30.0; 40.0 |]
            let sLoc = pl.series "Location" [| "A"; "B"; "C"; "D" |]
            pl.dataframe [| sId; sTemp; sLoc |]

        use lazyDf = df.Lazy()

        // Act: Stream through LazyFrame via Rust batch receiver
        let! totalTemp =
            lazyDf.RowsAsync<AsyncSensorRecord>(chunkSize = 2L, engine = Engine.Auto)
            |> RowsAsync.fold (fun acc s -> acc + s.Temperature) 0.0

        // Assert
        Assert.Equal(100.0, totalTemp)
    }

    [<Fact>]
    [<Trait("Async", "LazyFrame")>]
    member _.``LazyFrame CollectBatchesAsync streams separate DataFrame chunks`` () = task {
        // Arrange
        let sId = pl.series "Id" [| 1..50 |]
        use df = pl.dataframe [| sId |]
        use lazyDf = df.Lazy()

        let mutable batchCount = 0
        let mutable totalRows = 0L

        // Act: Pull batches of size 10 from streaming engine
        let enumerator = lazyDf.CollectBatchesAsync(chunkSize = 10L).GetAsyncEnumerator()
        try
            let mutable hasNext = true
            while hasNext do
                let! moved = enumerator.MoveNextAsync()
                if moved then
                    use batch = enumerator.Current
                    batchCount <- batchCount + 1
                    totalRows <- totalRows + batch.Height
                    Assert.True(batch.Height > 0L)
                else
                    hasNext <- false
        finally
            let vt = enumerator.DisposeAsync()
            if not vt.IsCompletedSuccessfully then
                do vt.AsTask() |> ignore

        // Assert: 50 rows delivered in multiple chunk DataFrames
        Assert.Equal(50L, totalRows)
        Assert.True(batchCount >= 5)
    }

    [<Fact>]
    [<Trait("Async", "Cancellation")>]
    member _.``RowsAsync throws OperationCanceledException when pre-cancelled token passed`` () = task {
        // Arrange
        use df = 
            let sId = pl.series "SensorId" [| 1; 2; 3; 4 |]
            let sTemp = pl.series "Temperature" [| 10.0; 20.0; 30.0; 40.0 |]
            let sLoc = pl.series "Location" [| "A"; "B"; "C"; "D" |]
            pl.dataframe [| sId; sTemp; sLoc |]

        use cts = new CancellationTokenSource()
        cts.Cancel() // Pre-cancelled token

        // Act & Assert
        let! ex = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> task {
            let stream = df.RowsAsync<AsyncSensorRecord>(cancellationToken = cts.Token)
            let! _ = stream |> RowsAsync.toList
            ()
        })
        Assert.NotNull(ex)
    }