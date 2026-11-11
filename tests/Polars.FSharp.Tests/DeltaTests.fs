module Polars.FSharp.Tests.DeltaTests

open System
open System.IO
open Xunit
open Polars.FSharp

[<Fact>]
let ``Scenario 1: Delta Lake Base Read Write and Append`` () =
    let testPath = Path.Combine(Path.GetTempPath(), $"delta_test_lake_{Guid.NewGuid()}")
    
    try
        let sId1 = Series.create("id", [1; 2; 3])
        let sName1 = Series.create("name", ["Alice"; "Bob"; "Charlie"])
        let sPrice1 = Series.create("price", [10.5; 20.0; 15.75])
        let df1 = DataFrame.create [| sId1; sName1; sPrice1 |]

        // ==========================================
        // Overwrite 
        // ==========================================
        df1.WriteDelta(testPath, mode = DeltaSaveMode.Overwrite, mkdir = true, syncOnClose=SyncOnClose.All)

        // ==========================================
        // ScanDelta 
        // ==========================================
        let readDf1 = DataFrame.ReadDelta testPath
        
        Assert.Equal(3L, readDf1.Height)
        Assert.Equal(3L, readDf1.Width)
        
        // ==========================================
        // Append
        // ==========================================
        let sId2 = Series.create("id", [4; 5])
        let sName2 = Series.create("name", ["Dave"; "Eve"])
        let sPrice2 = Series.create("price", [9.99; 99.9])
        let df2 = DataFrame.create [| sId2; sName2; sPrice2 |]

        // ==========================================
        // Append
        // ==========================================
        df2.WriteDelta(testPath, mode = DeltaSaveMode.Append)

        let readDf2 = LazyFrame.ScanDelta(testPath).Collect()
        
        Assert.Equal(5L, readDf2.Height)
        
        let sortedDf = readDf2.Sort "id"

        let lastRowNameOpt = sortedDf.Select(pl.col "name").Row(4).[0]
        
        Assert.Equal("Eve", lastRowNameOpt.ToString())

    finally

        if Directory.Exists testPath then
            Directory.Delete(testPath, true)
[<Fact>]
let ``Scenario 2: Delta Lake Time Travel (History & Restore)`` () =

    let testPath = Path.Combine(Path.GetTempPath(), $"delta_time_travel_{Guid.NewGuid()}")
    
    try
        // ==========================================
        // Version 0: Overwrite (creating the table is one commit, as in Spark)
        // ==========================================
        let sId1 = Series.create("id", [1; 2; 3])
        let sName1 = Series.create("name", ["Alice"; "Bob"; "Charlie"])
        let df1 = DataFrame.create [| sId1; sName1 |] 
        
        df1.WriteDelta(testPath, mode = DeltaSaveMode.Overwrite, mkdir = true)
        
        // ==========================================
        // Version 1: Append
        // ==========================================
        let sId2 = Series.create("id", [4; 5])
        let sName2 = Series.create("name", ["Dave"; "Eve"])
        let df2 = DataFrame.create [| sId2; sName2 |]
        
        df2.WriteDelta(testPath, mode = DeltaSaveMode.Append)
        
        // ==========================================
        // History
        // ==========================================
        let historyDf = Delta.History testPath
        
        Assert.True(historyDf.Height >= 2L, "History should contain at least 2 versions.")
        
        // ==========================================
        // Time Travel(Scan Version 0)
        // ==========================================
        let currentDf = LazyFrame.ScanDelta(testPath).Collect()
        Assert.Equal(5L, currentDf.Height)

        let v0Df = LazyFrame.ScanDelta(testPath, version = 0UL).Collect()
        Assert.Equal(3L, v0Df.Height) 
        
        // ==========================================
        // Restore to Version 0
        // ==========================================
        let restoredVersion = Delta.Restore(testPath, version = 0UL)
        Assert.Equal(2UL, restoredVersion)

        let restoredDf = LazyFrame.ScanDelta(testPath).Collect()
        
        Assert.Equal(3L, restoredDf.Height)
        
        let newHistoryDf = Delta.History testPath
        Assert.True(newHistoryDf.Height >= 3L, "History should now contain the Restore operation.")

    finally
        if Directory.Exists testPath then
            Directory.Delete(testPath, true)

[<Fact>]
let ``Scenario 3: Delta Lake Advanced MERGE Semantics`` () =
    let testPath = Path.Combine(Path.GetTempPath(), $"delta_merge_{Guid.NewGuid()}")

    try
        // ==========================================
        // Target
        // ==========================================
        let sId = Series.create("id", [1; 2; 3])
        let sName = Series.create("name", ["Alice"; "Bob"; "Charlie"])
        let sPrice = Series.create("price", [10.0; 20.0; 30.0])
        let targetDf = DataFrame.create [| sId; sName; sPrice |]
        
        targetDf.WriteDelta(testPath, mode = DeltaSaveMode.Overwrite, mkdir = true)
        
        // ==========================================
        // Source
        // ==========================================
        let sIdSrc = Series.create("id", [2; 3; 4])
        let sNameSrc = Series.create("name", ["Bob_Updated"; "Charlie_Downgraded"; "Dave"])
        let sPriceSrc = Series.create("price", [25.0; 15.0; 40.0])
        let sourceDf = DataFrame.create [| sIdSrc; sNameSrc; sPriceSrc |]

        // ==========================================
        // MERGE (Upsert)
        // ==========================================
        let updateCond = Delta.Source "price" .> Delta.Target "price"
        
        sourceDf.MergeDeltaOrdered(
            testPath,
            mergeKeys = ["id"]
        ).WhenMatchedUpdate(updateCond)
         .WhenNotMatchedInsert(pl.lit true)
         .Execute()

        let finalDf = LazyFrame.ScanDelta(testPath).Collect().Sort("id")
        
        Assert.Equal(4L, finalDf.Height)

        let nameCol = finalDf.Select(pl.col "name")

        let bobName = nameCol.Row(1).[0].ToString()
        Assert.Equal("Bob_Updated", bobName)

        let charlieName = nameCol.Row(2).[0].ToString()
        Assert.Equal("Charlie", charlieName)

        let daveName = nameCol.Row(3).[0].ToString()
        Assert.Equal("Dave", daveName)

    finally

        if Directory.Exists testPath then
            Directory.Delete(testPath, true)

[<Fact>]
let ``Scenario 4: Delta Lake Maintenance (Delete, Optimize, Vacuum)`` () =
    let testPath = Path.Combine(Path.GetTempPath(), $"delta_maint_{Guid.NewGuid()}")

    try

        let sId = Series.create("id", [1..10])
        let sVal = Series.create("val", ["A"; "B"; "C"; "D"; "E"; "F"; "G"; "H"; "I"; "J"])
        let df = DataFrame.create [| sId; sVal |]
        
        df.WriteDelta(testPath, mode = DeltaSaveMode.Overwrite, mkdir = true)

        // ==========================================
        // Deletion Vectors
        // ==========================================

        Delta.AddFeature(testPath, DeltaTableFeatures.DeletionVectors)

        // ==========================================
        // Delete
        // ==========================================
        let deleteCond = pl.col "id" .< pl.lit 5
        Delta.Delete(testPath, deleteCond)

        let afterDeleteDf = LazyFrame.ScanDelta(testPath).Collect()
        Assert.Equal(6L, afterDeleteDf.Height)

        // ==========================================
        // Optimize
        // ==========================================

        let optimizedFilesCount = 
            Delta.Optimize(
                testPath, 
                targetSizeMb = 128L, 
                zOrderColumns = ["id"]
            )

        let deletedFilesCount = 
            Delta.Vacuum(
                testPath, 
                retentionHours = 0, 
                enforceRetention = false
            )
        
        let finalDf = LazyFrame.ScanDelta(testPath).Collect()
        Assert.Equal(6L, finalDf.Height)
        
        let historyDf = Delta.History testPath
        historyDf.Show()
        Assert.True(historyDf.Height >= 4L)

    finally
        if Directory.Exists testPath then
            Directory.Delete(testPath, true)

[<Fact>]
let ``Turning the change data feed on lets appends feed it`` () =
    let testPath = Path.Combine(Path.GetTempPath(), $"delta_test_cdf_{Guid.NewGuid()}")

    try
        let df = DataFrame.create [ pl.series "Id" [ 1; 2 ] ]
        df.WriteDelta(testPath, mode = DeltaSaveMode.Overwrite, mkdir = true)
        Delta.SetTableProperties(testPath, [ DeltaTableProperties.EnableChangeDataFeed, "true" ])

        // The managed kernel writes tables with the change data feed (the old engine refused them).
        df.WriteDelta(testPath, mode = DeltaSaveMode.Append)
        let changes = Delta.ReadChangeDataFeed(path = testPath, startVersion = 2L, endVersion = 2L).Collect()
        Assert.Equal(2L, changes.Height)
        Assert.All(changes.["_change_type"].ToArray<string>(), fun t -> Assert.Equal("insert", t))
    finally
        if Directory.Exists testPath then Directory.Delete(testPath, true)

[<Fact>]
let ``Reading the change data feed of a table without one raises an exception`` () =
    let testPath = Path.Combine(Path.GetTempPath(), $"delta_test_nocdf_{Guid.NewGuid()}")

    try
        let df = DataFrame.create [ pl.series "Id" [ 1; 2 ] ]
        df.WriteDelta(testPath, mode = DeltaSaveMode.Overwrite, mkdir = true)

        Assert.ThrowsAny<Exception>(fun () ->
            Delta.ReadChangeDataFeed(path = testPath, startVersion = 0L, endVersion = 0L) |> ignore)
        |> ignore
    finally
        if Directory.Exists testPath then Directory.Delete(testPath, true)

[<Fact>]
let ``Delta CreateTable declares identity columns and ScanDelta returns row ids`` () =
    let path = Path.Combine(Path.GetTempPath(), $"delta_identity_{Guid.NewGuid()}")
    try
        let schema = PolarsSchema([ "id", DataType.Int64; "v", DataType.String; "p", DataType.String ])
        Delta.CreateTable(
            path, schema, partitionBy = [ "p" ],
            properties = [ DeltaTableProperties.EnableRowTracking, "true" ],
            identityColumns = [ DeltaIdentityColumn("id", generatedAlways = false) ])
        let rows = DataFrame.create [| Series.create("v", [ "a"; "b" ]); Series.create("p", [ "x"; "x" ]) |]
        rows.WriteDelta(path, mode = DeltaSaveMode.Append)
        let given = DataFrame.create [| Series.create("id", [ 50L ]); Series.create("v", [ "c" ]); Series.create("p", [ "y" ]) |]
        given.WriteDelta(path, mode = DeltaSaveMode.Append)

        let read = DataFrame.ReadDelta(path, rowIdColumn = "row_id", rowCommitVersionColumn = "row_version").Sort("v")
        Assert.Equal<int64 list>([ 1L; 2L; 50L ], read.["id"].ToArray<int64>() |> List.ofArray)
        Assert.Equal<int64 list>([ 0L; 1L; 2L ], read.["row_id"].ToArray<int64>() |> List.ofArray)
        Assert.Equal<int64 list>([ 1L; 1L; 2L ], read.["row_version"].ToArray<int64>() |> List.ofArray)
    finally
        if Directory.Exists path then Directory.Delete(path, true)
