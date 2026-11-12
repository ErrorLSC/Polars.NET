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
        Delta.SetTableProperties(testPath, [(DeltaTableProperties.EnableChangeDataFeed, "true" )])

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
        let schema = new PolarsSchema([ "id", DataType.Int64; "v", DataType.String; "p", DataType.String ])
        Delta.CreateTable(
            path, schema, partitionBy = [ "p" ],
            properties = [ (DeltaTableProperties.EnableRowTracking, "true") ],
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

[<Fact>]
let ``Column changes: RenameColumn, DropColumns and ChangeColumnType keep the data`` () =
    let path = Path.Combine(Path.GetTempPath(), $"delta_columns_{Guid.NewGuid()}")
    try
        let props = [ "delta.columnMapping.mode", "name"; "delta.enableTypeWidening", "true" ]
        Delta.CreateTable(path, new PolarsSchema([ "id", DataType.Int32; "name", DataType.String; "note", DataType.String ]), properties = props)
        DataFrame.create [| Series.create("id", [ 1; 2 ]); Series.create("name", [ "a"; "b" ]); Series.create("note", [ "x"; "y" ]) |]
        |> fun df -> df.WriteDelta(path, mode = DeltaSaveMode.Append)

        Delta.RenameColumn(path, "name", "label")
        Delta.DropColumns(path, [ "note"; "missing" ], ifExists = true)
        Delta.ChangeColumnType(path, "id", DataType.Int64)
        Delta.ChangeColumnType(path, "id", DataType.Int64) // already: no commit
        DataFrame.create [| Series.create("id", [ 10000000000L ]); Series.create("label", [ "c" ]) |]
        |> fun df -> df.WriteDelta(path, mode = DeltaSaveMode.Append)

        let read = DataFrame.ReadDelta(path).Sort("id")
        Assert.Equal<string list>([ "id"; "label" ], read.Columns |> List.ofArray)
        Assert.Equal<int64 list>([ 1L; 2L; 10000000000L ], read.["id"].ToArray<int64>() |> List.ofArray)
        Assert.Equal<string list>([ "a"; "b"; "c" ], read.["label"].ToArray<string>() |> List.ofArray)
        let operations = Delta.History(path).["operation"].ToArray<string>() |> List.ofArray
        Assert.Equal<string list>([ "WRITE"; "CHANGE COLUMN"; "DROP COLUMNS"; "RENAME COLUMN"; "WRITE"; "CREATE TABLE" ], operations)
        // Narrowing is not a widening; dropping without mapping or the last column is refused.
        Assert.ThrowsAny<exn>(fun () -> Delta.ChangeColumnType(path, "id", DataType.Int32)) |> ignore
        Assert.ThrowsAny<exn>(fun () -> Delta.DropColumns(path, [ "id"; "label" ])) |> ignore
    finally
        if Directory.Exists path then Directory.Delete(path, true)

[<Fact>]
let ``Column changes: DropColumnNotNull, SetColumnComment and MoveColumn change the schema only`` () =
    let path = Path.Combine(Path.GetTempPath(), $"delta_columns2_{Guid.NewGuid()}")
    try
        Delta.CreateTable(path, new PolarsSchema([ "id", DataType.Int32; "name", DataType.String; "note", DataType.String ]))
        DataFrame.create [| Series.create("id", [ 1; 2 ]); Series.create("name", [ "a"; "b" ]); Series.create("note", [ "x"; "y" ]) |]
        |> fun df -> df.WriteDelta(path, mode = DeltaSaveMode.Append)

        Delta.DropColumnNotNull(path, "id")
        Delta.SetColumnComment(path, "name", "who")
        Delta.MoveColumn(path, "note")
        Delta.MoveColumn(path, "id", after = "name")

        let read = DataFrame.ReadDelta(path).Sort("name")
        Assert.Equal<string list>([ "note"; "name"; "id" ], read.Columns |> List.ofArray)
        Assert.Equal<int list>([ 1; 2 ], read.["id"].ToArray<int>() |> List.ofArray)
        let operations = Delta.History(path).["operation"].ToArray<string>() |> List.ofArray
        Assert.Equal<string list>([ "CHANGE COLUMN"; "CHANGE COLUMN"; "CHANGE COLUMN"; "CHANGE COLUMN"; "WRITE"; "CREATE TABLE" ], operations)
        Assert.ThrowsAny<exn>(fun () -> Delta.MoveColumn(path, "id", after = "missing")) |> ignore
    finally
        if Directory.Exists path then Directory.Delete(path, true)

[<Fact>]
let ``Purge rewrites the files that hold a dropped column`` () =
    let path = Path.Combine(Path.GetTempPath(), $"delta_purge_{Guid.NewGuid()}")
    try
        Delta.CreateTable(path, new PolarsSchema([ "id", DataType.Int32; "note", DataType.String ]), properties = [ ("delta.columnMapping.mode", "name") ])
        DataFrame.create [| Series.create("id", [ 1; 2 ]); Series.create("note", [ "x"; "y" ]) |]
        |> fun df -> df.WriteDelta(path, mode = DeltaSaveMode.Append)
        Delta.DropColumns(path, [ "note" ])
        DataFrame.create [| Series.create("id", [ 3 ]) |]
        |> fun df -> df.WriteDelta(path, mode = DeltaSaveMode.Append)

        Assert.Equal(1L, Delta.Purge(path)) // the first file only
        Assert.Equal(0L, Delta.Purge(path)) // nothing left: no commit

        let read = DataFrame.ReadDelta(path).Sort("id")
        Assert.Equal<int list>([ 1; 2; 3 ], read.["id"].ToArray<int>() |> List.ofArray)
        let operations = Delta.History(path).["operation"].ToArray<string>() |> List.ofArray
        Assert.Equal<string list>([ "REORG"; "WRITE"; "DROP COLUMNS"; "WRITE"; "CREATE TABLE" ], operations)
    finally
        if Directory.Exists path then Directory.Delete(path, true)

[<Fact>]
let ``Liquid clustering: CreateTable clusterBy, Optimize clusters, ClusterBy changes the columns`` () =
    let path = Path.Combine(Path.GetTempPath(), $"delta_clustered_{Guid.NewGuid()}")
    try
        Delta.CreateTable(path, new PolarsSchema([ "k", DataType.Int64; "v", DataType.String ]), clusterBy = [ "k" ])
        for i in 0 .. 2 do
            DataFrame.create [| Series.create("k", [ int64 i; int64 (i + 10) ]); Series.create("v", [ "a"; "b" ]) |]
            |> fun df -> df.WriteDelta(path, mode = DeltaSaveMode.Append)
        Assert.True(Delta.Optimize(path) > 0L)
        // Z-ordering does not apply to a clustered table; FULL only to clustered ones.
        Assert.ThrowsAny<exn>(fun () -> Delta.Optimize(path, zOrderColumns = [ "v" ]) |> ignore) |> ignore
        Delta.ClusterBy(path, [ "v" ])
        Assert.Equal(0L, Delta.Optimize(path)) // nothing new: the old cube stays
        Assert.True(Delta.Optimize(path, full = true) > 0L)
        Delta.ClusterBy(path, [])

        let history = Delta.History(path)
        let operations = history.["operation"].ToArray<string>() |> List.ofArray
        Assert.Equal(2, operations |> List.filter ((=) "CLUSTER BY") |> List.length)
        Assert.Equal(2, operations |> List.filter ((=) "OPTIMIZE") |> List.length)
        let read = DataFrame.ReadDelta(path).Sort("k")
        Assert.Equal<int64 list>([ 0L; 1L; 2L; 10L; 11L; 12L ], read.["k"].ToArray<int64>() |> List.ofArray)
    finally
        if Directory.Exists path then Directory.Delete(path, true)

[<Fact>]
let ``Clustering refuses partitioning`` () =
    let path = Path.Combine(Path.GetTempPath(), $"delta_clustered_bad_{Guid.NewGuid()}")
    try
        let e = Assert.ThrowsAny<exn>(fun () ->
            Delta.CreateTable(path, new PolarsSchema([ "k", DataType.Int64; "p", DataType.String ]), partitionBy = [ "p" ], clusterBy = [ "k" ]))
        Assert.Contains("cannot both be specified", e.Message)
    finally
        if Directory.Exists path then Directory.Delete(path, true)

[<Fact>]
let ``Suspended row tracking gives appended rows ids only once it is turned on again`` () =
    let path = Path.Combine(Path.GetTempPath(), $"delta_rt_suspended_{Guid.NewGuid()}")
    try
        Delta.CreateTable(path, new PolarsSchema([ ("v", DataType.String) ]), properties = [ (DeltaTableProperties.EnableRowTracking, "true") ])
        DataFrame.create [| Series.create("v", [ "a"; "b" ]) |] |> fun df -> df.WriteDelta(path, mode = DeltaSaveMode.Append) // v1: ids 0, 1

        // Suspending needs row tracking off in the same change (as Spark).
        Assert.ThrowsAny<exn>(fun () -> Delta.SetTableProperties(path, [ (DeltaTableProperties.RowTrackingSuspended, "true") ], raiseIfNotExists = true))
        |> ignore
        Delta.SetTableProperties(
            path, [ DeltaTableProperties.EnableRowTracking, "false"; DeltaTableProperties.RowTrackingSuspended, "true" ], raiseIfNotExists = true)
        DataFrame.create [| Series.create("v", [ "c" ]) |] |> fun df -> df.WriteDelta(path, mode = DeltaSaveMode.Append) // v3: no ids
        Assert.ThrowsAny<exn>(fun () -> DataFrame.ReadDelta(path, rowIdColumn = "row_id") |> ignore) |> ignore

        // Resumed and turned on in one change (v4): "c" gets the next id, with v4 as its version.
        Delta.SetTableProperties(
            path, [ DeltaTableProperties.EnableRowTracking, "true"; DeltaTableProperties.RowTrackingSuspended, "false" ], raiseIfNotExists = true)
        let read = DataFrame.ReadDelta(path, rowIdColumn = "row_id", rowCommitVersionColumn = "row_version").Sort("v")
        Assert.Equal<int64 list>([ 0L; 1L; 2L ], read.["row_id"].ToArray<int64>() |> List.ofArray)
        Assert.Equal<int64 list>([ 1L; 1L; 4L ], read.["row_version"].ToArray<int64>() |> List.ofArray)
    finally
        if Directory.Exists path then Directory.Delete(path, true)

[<Fact>]
let ``Delta Update changes only the rows matching the predicate and commits an UPDATE`` () =
    let path = Path.Combine(Path.GetTempPath(), $"delta_update_pred_{Guid.NewGuid()}")
    try
        let df = DataFrame.create [| Series.create("Id", [ 1L; 2L; 3L ]); Series.create("Price", [ 10.0; 20.0; 30.0 ]) |]
        df.WriteDelta(path, mode = DeltaSaveMode.Overwrite, mkdir = true)

        Delta.Update(path, [ ("Price", pl.col "Price" * pl.lit 2.0) ], predicate = (pl.col "Id" .== pl.lit 2L))

        let read = DataFrame.ReadDelta(path).Sort("Id")
        Assert.Equal<float list>([ 10.0; 40.0; 30.0 ], read.["Price"].ToArray<float>() |> List.ofArray)

        let history = Delta.History(path)
        let versions = history.["version"].ToArray<int64>()
        let operations = history.["operation"].ToArray<string>()
        Assert.Equal("UPDATE", operations.[Array.IndexOf(versions, Array.max versions)])
    finally
        if Directory.Exists path then Directory.Delete(path, true)

[<Fact>]
let ``DataFrame UpdateDelta updates matched rows by key and inserts nothing`` () =
    let path = Path.Combine(Path.GetTempPath(), $"delta_update_merge_{Guid.NewGuid()}")
    try
        let df = DataFrame.create [| Series.create("Id", [ 1L; 2L; 3L ]); Series.create("Price", [ 10.0; 20.0; 30.0 ]) |]
        df.WriteDelta(path, mode = DeltaSaveMode.Overwrite, mkdir = true)

        let source = DataFrame.create [| Series.create("Id", [ 2L; 9L ]); Series.create("Price", [ 200.0; 900.0 ]) |]
        source.UpdateDelta(path, [ "Id" ])

        let read = DataFrame.ReadDelta(path).Sort("Id")
        Assert.Equal(3L, read.Height)
        Assert.Equal<float list>([ 10.0; 200.0; 30.0 ], read.["Price"].ToArray<float>() |> List.ofArray)
    finally
        if Directory.Exists path then Directory.Delete(path, true)

[<Fact>]
let ``Delta Update of an identity column is refused`` () =
    let path = Path.Combine(Path.GetTempPath(), $"delta_update_identity_{Guid.NewGuid()}")
    try
        let schema = new PolarsSchema([ "id", DataType.Int64; "v", DataType.String ])
        Delta.CreateTable(path, schema, identityColumns = [ DeltaIdentityColumn("id") ])
        let rows = DataFrame.create [| Series.create("v", [ "a"; "b" ]) |]
        rows.WriteDelta(path, mode = DeltaSaveMode.Append)

        let ex =
            Assert.ThrowsAny<Exception>(fun () ->
                Delta.Update(path, [ ("id", pl.lit 5L) ], predicate = (pl.col "v" .== pl.lit "a")) |> ignore)
        Assert.Contains("UPDATE on IDENTITY column", ex.Message)
    finally
        if Directory.Exists path then Directory.Delete(path, true)
