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

    // =========================================================================
    // Scenario 1: DataFrame RowsAsync streaming with micro-batch boundary
    // Verifies synchronous batch hydration and conversion to F# immutable list
    // =========================================================================
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

    // =========================================================================
    // Scenario 2: LazyFrame RowsAsync with filter pushdown and async collection
    // Verifies background engine execution followed by stream consumption
    // =========================================================================
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

    // =========================================================================
    // Scenario 3: Functional pipeline fold aggregation
    // Verifies RowsAsync.fold without allocating intermediate collections
    // =========================================================================
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

    // =========================================================================
    // Scenario 4: Cooperative cancellation test
    // Verifies that cancelled tokens immediately abort iteration
    // =========================================================================
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