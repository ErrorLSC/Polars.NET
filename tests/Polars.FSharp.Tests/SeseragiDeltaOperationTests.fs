/// Operations on Delta tables through the managed kernel (Phase 3): delete, update, and the write-side checks they share.
module Polars.FSharp.Tests.SeseragiDeltaOperationTests

open System
open System.Collections.Generic
open System.IO
open System.Text.Json
open Xunit
open Polars.FSharp
open Polars.NET.Core.Delta
open Kaminari.Seseragi.Lakehouse.Delta
open Kaminari.Seseragi.Lakehouse.Delta.Actions
open Kaminari.Seseragi.Lakehouse.Delta.Schema
open Kaminari.Seseragi.Lakehouse.Engine

let private withTable (name: string) (body: string -> unit) =
    let path = Path.Combine(Path.GetTempPath(), $"seseragi_ops_{name}_{Guid.NewGuid():N}")
    try
        body path
    finally
        if Directory.Exists path then Directory.Delete(path, true)

let private frame (ids: int list) (region: string) =
    DataFrame.create [|
        Series.create("id", ids)
        Series.create("region", ids |> List.map (fun _ -> region))
        Series.create("price", ids |> List.map (fun i -> float i * 1.5))
    |]

let private config (pairs: (string * string) list) =
    let d = Dictionary<string, string>()
    for k, v in pairs do d[k] <- v
    d :> IReadOnlyDictionary<string, string>

let private create (path: string) (df: DataFrame) (partitionBy: string[]) (properties: (string * string) list) =
    DeltaKernelWrite.Write(path, df.Lazy().CloneHandle(), KernelWriteMode.ErrorIfExists, partitionBy, configuration = config properties)
    |> ignore

let private append (path: string) (df: DataFrame) =
    DeltaKernelWrite.Write(path, df.Lazy().CloneHandle(), KernelWriteMode.Append) |> ignore

let private delete (path: string) (predicate: Expr) =
    DeltaKernelDelete.Delete(path, predicate.CloneHandle())

let private ids (path: string) =
    use df = (new LazyFrame(DeltaKernelScan.Scan path)).Collect().Sort(pl.col "id")
    df.["id"].ToArray<int>() |> List.ofArray

let private commitInfo (path: string) (version: int64) =
    let line =
        File.ReadLines(Path.Combine(path, "_delta_log", $"{version:D20}.json"))
        |> Seq.find (fun l -> l.StartsWith("{\"commitInfo\"", StringComparison.Ordinal))
    JsonDocument.Parse(line).RootElement.GetProperty("commitInfo").Clone()

let private metric (info: JsonElement) (name: string) =
    Int64.Parse(info.GetProperty("operationMetrics").GetProperty(name).GetString())

let private dvOn = [ (TableFeatures.EnableDeletionVectorsKey, "true" ) ]
let private cdfOn = [ (ChangeFeed.EnableKey, "true" ) ]

[<Fact>]
let ``Delete rewrites files on a table without deletion vectors`` () =
    withTable "delete_rewrite" (fun path ->
        use first = frame [ 1 .. 5 ] "east"
        use second = frame [ 6 .. 10 ] "west"
        create path first [||] []
        append path second

        let result = delete path (pl.col "id" .< pl.lit 3)

        Assert.Equal(Nullable 2L, result.Version)
        Assert.Equal(2L, result.DeletedRows)
        Assert.Equal(1, result.RewrittenFiles)
        Assert.Equal(0, result.DeletionVectorsAdded)
        Assert.Equal<int list>([ 3 .. 10 ], ids path)
        let info = commitInfo path 2L
        Assert.Equal("DELETE", info.GetProperty("operation").GetString())
        Assert.Equal(2L, metric info "numDeletedRows")
        Assert.Equal(3L, metric info "numCopiedRows")
        Assert.False(info.GetProperty("isBlindAppend").GetBoolean())
        let snapshot = Snapshot.Load(LocalFileSystem path)
        Assert.Equal(2, snapshot.Files.Count)
        Assert.All(snapshot.Files, fun f -> Assert.Null f.DeletionVector)

        // The current engine reads the result too.
        use viaEngine = LazyFrame.ScanDelta(path).Collect()
        Assert.Equal(8L, viaEngine.Height))

[<Fact>]
let ``Delete removes whole files without rewriting, and a miss commits nothing`` () =
    withTable "delete_whole" (fun path ->
        use first = frame [ 1 .. 5 ] "east"
        use second = frame [ 6 .. 10 ] "west"
        create path first [||] []
        append path second

        let miss = delete path (pl.col "id" .> pl.lit 100)
        Assert.False(miss.Version.HasValue)
        Assert.False(File.Exists(Path.Combine(path, "_delta_log", $"{2:D20}.json")))

        let result = delete path (pl.col "region" .== pl.lit "east")
        Assert.Equal(Nullable 2L, result.Version)
        Assert.Equal((5L, 1, 0, 0), (result.DeletedRows, result.RemovedFiles, result.AddedFiles, result.RewrittenFiles))
        Assert.Equal<int list>([ 6 .. 10 ], ids path)

        // No predicate: every row, by removing every file.
        let all = DeltaKernelDelete.Delete(path, null)
        Assert.Equal(5L, all.DeletedRows)
        Assert.Empty(Snapshot.Load(LocalFileSystem path).Files)
        Assert.Empty(ids path))

[<Fact>]
let ``Delete marks rows in deletion vectors, merging with earlier ones`` () =
    withTable "delete_dv" (fun path ->
        use df = frame [ 1 .. 10 ] "east"
        create path df [||] dvOn
        let fs = LocalFileSystem path
        let created = Snapshot.Load fs
        Assert.Contains(TableFeatures.DeletionVectors, created.Protocol.ReaderFeatures)
        Assert.Contains(TableFeatures.DeletionVectors, created.Protocol.WriterFeatures)
        let original = Assert.Single created.Files

        let r1 = delete path (pl.col "id" .< pl.lit 4)
        Assert.Equal((3L, 1, 0), (r1.DeletedRows, r1.DeletionVectorsAdded, r1.RewrittenFiles))
        let r2 = delete path ((pl.col "id" .> pl.lit 8) .|| (pl.col "id" .== pl.lit 2)) // 2 is already gone
        Assert.Equal(2L, r2.DeletedRows)
        Assert.Equal<int list>([ 4 .. 8 ], ids path)

        let file = Assert.Single(Snapshot.Load(fs).Files)
        Assert.Equal(original.Path, file.Path)
        Assert.Equal(5L, file.DeletionVector.Cardinality)
        Assert.Equal<int64[]>([| 0L; 1L; 2L; 8L; 9L |], DeletionVector.Read(fs, file.DeletionVector).DeletedRows)
        Assert.False(file.ParsedStats.TightBounds)
        Assert.Equal(Nullable 10L, file.ParsedStats.NumRecords)
        // The second delete removed the file with the first vector and added it back with the merged one.
        let info = commitInfo path 2L
        Assert.Equal(1L, metric info "numDeletionVectorsRemoved")
        Assert.Single(Directory.GetFiles(path, "deletion_vector_*.bin") |> Array.filter (fun f -> f.Contains(file.DeletionVector.FilePath()))) |> ignore

        // Deleting the remaining rows removes the file.
        let r3 = delete path (pl.col "id" .< pl.lit 100)
        Assert.Equal((5L, 1, 0), (r3.DeletedRows, r3.RemovedFiles, r3.DeletionVectorsAdded))
        Assert.Empty(Snapshot.Load(fs).Files))

[<Fact>]
let ``Delete keeps rows where the predicate is null`` () =
    withTable "delete_null" (fun path ->
        use df =
            DataFrame.create [|
                Series.create("id", [ 1; 2; 3 ])
                Series.create("v", [ Nullable 1; Nullable(); Nullable 3 ])
            |]
        create path df [||] []
        let result = delete path (pl.col "v" .> pl.lit 1)
        Assert.Equal(1L, result.DeletedRows)
        Assert.Equal<int list>([ 1; 2 ], ids path))

[<Theory>]
[<InlineData(false)>]
[<InlineData(true)>]
let ``Delete writes change data on a partitioned change feed table`` (withVectors: bool) =
    withTable "delete_cdf" (fun path ->
        use east = frame [ 1 .. 4 ] "east"
        use west = frame [ 5 .. 8 ] "west"
        use both = east.VStack west
        create path both [| "region" |] (cdfOn @ (if withVectors then dvOn else []))

        let result = delete path ((pl.col "id" .== pl.lit 2) .|| (pl.col "id" .>= pl.lit 7))
        Assert.Equal(3L, result.DeletedRows)
        Assert.Equal(2, result.ChangeFiles)
        Assert.Equal((if withVectors then 2 else 0), result.DeletionVectorsAdded)
        Assert.Equal<int list>([ 1; 3; 4; 5; 6 ], ids path)

        let snapshot = Snapshot.Load(LocalFileSystem path)
        Assert.All(Directory.GetFiles(Path.Combine(path, "_change_data"), "*.parquet", SearchOption.AllDirectories), fun f ->
            Assert.Contains("region=", f))

        use changes = (new LazyFrame(DeltaKernelScan.ReadChangeFeed(path, 1L))).Collect().Sort(pl.col "id")
        Assert.Equal(3L, changes.Height)
        Assert.Equal<int list>([ 2; 7; 8 ], changes.["id"].ToArray<int>() |> List.ofArray)
        Assert.Equal<string list>([ "east"; "west"; "west" ], changes.["region"].ToArray<string>() |> List.ofArray)
        Assert.All(changes.["_change_type"].ToArray<string>(), fun t -> Assert.Equal("delete", t))
        Assert.All(changes.["_commit_version"].ToArray<int64>(), fun v -> Assert.Equal(1L, v))
        ignore snapshot)

[<Fact>]
let ``Delete on an append-only table is refused`` () =
    withTable "delete_append_only" (fun path ->
        use df = frame [ 1; 2 ] "east"
        create path df [||] [ (TableFeatures.AppendOnlyKey, "true") ]
        let e = Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () -> delete path (pl.col "id" .== pl.lit 1) |> ignore)
        Assert.Contains("append-only", e.Message)
        Assert.Equal<int list>([ 1; 2 ], ids path))

[<Fact>]
let ``Writes reject nulls in NOT NULL columns, nested fields included`` () =
    withTable "not_null" (fun path ->
        // A table whose schema declares id NOT NULL and s.k NOT NULL inside a nullable struct.
        let schema =
            """{"type":"struct","fields":[{"name":"id","type":"integer","nullable":false,"metadata":{}},"""
            + """{"name":"s","type":{"type":"struct","fields":[{"name":"k","type":"string","nullable":false,"metadata":{}}]},"nullable":true,"metadata":{}}]}"""
        let fs = LocalFileSystem path
        let metadata = MetadataAction(Id = Guid.NewGuid().ToString(), SchemaString = schema)
        Transaction.Create(fs, ProtocolAction(MinReaderVersion = 1, MinWriterVersion = 2), metadata).Commit() |> ignore

        let rows (ids: Nullable<int> list) (keys: string list) (structNull: bool list) =
            use df =
                DataFrame.create [|
                    Series.create("id", ids)
                    Series.create("k", keys)
                    Series.create("isNull", structNull)
                |]
            df.Select(
                [ pl.col "id"
                  pl.ifElse (pl.col "isNull") (pl.litNull ()) (pl.asStruct [ pl.col "k" ]) |> pl.alias "s" ])

        // A null struct may hold anything; a present struct needs k.
        use ok = rows [ Nullable 1; Nullable 2 ] [ "a"; null ] [ false; true ]
        append path ok
        use nullId = rows [ Nullable(); Nullable 3 ] [ "a"; "b" ] [ false; false ]
        let e1 = Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () -> append path nullId)
        Assert.Contains("NOT NULL: id", e1.Message)
        use nullKey = rows [ Nullable 4 ] [ null ] [ false ]
        let e2 = Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () -> append path nullKey)
        Assert.Contains("s.k", e2.Message)
        Assert.Equal(1L, Snapshot.Load(fs).Version))

// ---- update ----

let private update (path: string) (predicate: Expr option) (assignments: (string * Expr) list) =
    let pairs = assignments |> List.map (fun (c, e) -> System.ValueTuple.Create(c, e.CloneHandle()))
    DeltaKernelUpdate.Update(
        path,
        (match predicate with Some p -> p.CloneHandle() | None -> null),
        ResizeArray(pairs))

let private rowsOf (path: string) =
    use df = (new LazyFrame(DeltaKernelScan.Scan path)).Collect().Sort(pl.col "id")
    List.zip3 (df.["id"].ToArray<int>() |> List.ofArray) (df.["region"].ToArray<string>() |> List.ofArray) (df.["price"].ToArray<float>() |> List.ofArray)

[<Theory>]
[<InlineData(false)>]
[<InlineData(true)>]
let ``Update sets columns of matching rows, in place or through deletion vectors`` (withVectors: bool) =
    withTable "update" (fun path ->
        use df = frame [ 1 .. 6 ] "east"
        create path df [||] (if withVectors then dvOn else [])

        // price from the old row; a null-predicate row (none here) would be kept.
        let r = update path (Some(pl.col "id" .>= pl.lit 5)) [ "price", pl.col "price" * pl.lit 10.0; "region", pl.lit "west" ]

        Assert.Equal(Nullable 1L, r.Version)
        Assert.Equal(2L, r.UpdatedRows)
        if withVectors then
            Assert.Equal((1, 0, 0L), (r.DeletionVectorsAdded, r.RewrittenFiles, r.CopiedRows))
        else
            Assert.Equal((0, 1, 4L), (r.DeletionVectorsAdded, r.RewrittenFiles, r.CopiedRows))
        let expected =
            [ for i in 1 .. 6 -> if i >= 5 then (i, "west", float i * 15.0) else (i, "east", float i * 1.5) ]
        Assert.Equal<(int * string * float) list>(expected, rowsOf path)
        let info = commitInfo path 1L
        Assert.Equal("UPDATE", info.GetProperty("operation").GetString())
        Assert.Equal(2L, metric info "numUpdatedRows")

        // No match: nothing committed.
        let miss = update path (Some(pl.col "id" .> pl.lit 100)) [ ("price", pl.lit 0.0) ]
        Assert.False(miss.Version.HasValue))

[<Fact>]
let ``Update moves rows between partitions and writes pre and post images`` () =
    withTable "update_cdf" (fun path ->
        use east = frame [ 1 .. 3 ] "east"
        use west = frame [ 4 .. 5 ] "west"
        use both = east.VStack west
        create path both [| "region" |] (cdfOn @ dvOn)

        let r = update path (Some((pl.col "id" .== pl.lit 2) .|| (pl.col "id" .== pl.lit 4))) [ ("region", pl.lit "north") ]
        Assert.Equal(2L, r.UpdatedRows)
        Assert.Equal<(int * string * float) list>(
            [ (1, "east", 1.5); (2, "north", 3.0); (3, "east", 4.5); (4, "north", 6.0); (5, "west", 7.5) ], rowsOf path)
        let snapshot = Snapshot.Load(LocalFileSystem path)
        Assert.Contains(snapshot.Files, fun f -> f.PartitionValues["region"] = "north")

        use changes = (new LazyFrame(DeltaKernelScan.ReadChangeFeed(path, 1L))).Collect().Sort([ pl.col "id"; pl.col "_change_type" ])
        let seen =
            List.zip3
                (changes.["id"].ToArray<int>() |> List.ofArray)
                (changes.["region"].ToArray<string>() |> List.ofArray)
                (changes.["_change_type"].ToArray<string>() |> List.ofArray)
        Assert.Equal<(int * string * string) list>(
            // sorted by id, then change type: postimage before preimage
            [ (2, "north", "update_postimage"); (2, "east", "update_preimage"); (4, "north", "update_postimage"); (4, "west", "update_preimage") ],
            seen))

[<Fact>]
let ``Update without a predicate sets every row; unknown columns and values that do not fit are refused`` () =
    withTable "update_all" (fun path ->
        use df = frame [ 1; 2; 3 ] "east"
        create path df [||] []
        let r = update path None [ ("price", pl.lit 1.0) ]
        Assert.Equal(3L, r.UpdatedRows)
        Assert.All(rowsOf path, fun (_, _, p) -> Assert.Equal(1.0, p))

        let unknown = Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () -> update path None [ ("nope", pl.lit 1) ] |> ignore)
        Assert.Contains("nope", unknown.Message)
        // Values that do not fit the column type fail instead of being changed.
        Assert.ThrowsAny<exn>(fun () -> update path None [ ("id", pl.lit "x") ] |> ignore) |> ignore
        Assert.Equal(1L, Snapshot.Load(LocalFileSystem path).Version))

// ---- merge ----

let private src (name: string) = pl.col (name + DeltaKernelMerge.SourceSuffix)

let private merge (path: string) (source: DataFrame) (keys: string list) (clauses: (MergeClauseKind * Expr) list) (canEvolve: bool) =
    let list = ResizeArray(clauses |> List.map (fun (k, c) -> MergeClause(k, c.CloneHandle())))
    DeltaKernelMerge.Merge(path, source.Lazy().CloneHandle(), Array.ofList keys, list, canEvolve)

let private products (ids: int list) (prices: float list) (categories: string list) =
    DataFrame.create [|
        Series.create("id", ids)
        Series.create("price", prices)
        Series.create("category", categories)
    |]

let private productRows (path: string) =
    use df = (new LazyFrame(DeltaKernelScan.Scan path)).Collect().Sort(pl.col "id")
    List.zip3 (df.["id"].ToArray<int>() |> List.ofArray) (df.["price"].ToArray<float>() |> List.ofArray) (df.["category"].ToArray<string>() |> List.ofArray)

[<Theory>]
[<InlineData(false)>]
[<InlineData(true)>]
let ``Merge without clauses upserts`` (withVectors: bool) =
    withTable "merge_upsert" (fun path ->
        use target = products [ 1; 2; 3 ] [ 10.0; 20.0; 30.0 ] [ "a"; "a"; "b" ]
        create path target [||] (if withVectors then dvOn else [])
        use source = products [ 2; 4 ] [ 25.0; 40.0 ] [ "a"; "c" ]

        let r = merge path source [ "id" ] [] false

        Assert.Equal((1L, 1L, 0L), (r.InsertedRows, r.UpdatedRows, r.DeletedRows))
        Assert.Equal((if withVectors then 0L else 2L), r.CopiedRows)
        Assert.Equal<(int * float * string) list>(
            [ (1, 10.0, "a"); (2, 25.0, "a"); (3, 30.0, "b"); (4, 40.0, "c") ], productRows path)
        let info = commitInfo path r.Version.Value
        Assert.Equal("MERGE", info.GetProperty("operation").GetString())
        Assert.Equal(1L, metric info "numTargetRowsInserted")
        Assert.Equal(1L, metric info "numTargetRowsUpdated")
        Assert.Equal(2L, metric info "numSourceRows")
        if not withVectors then
            use viaEngine = LazyFrame.ScanDelta(path).Collect()
            Assert.Equal(4L, viaEngine.Height))

[<Theory>]
[<InlineData(false)>]
[<InlineData(true)>]
let ``Merge takes the first clause that holds, and not-matched-by-source sees the whole table`` (withVectors: bool) =
    withTable "merge_ordered" (fun path ->
        // Two files: ids 1..4 and, far from the source's keys, ids 100..101.
        use first = products [ 1; 2; 3; 4 ] [ 10.0; 20.0; 30.0; 100.0 ] [ "regular"; "seasonal"; "regular"; "regular" ]
        create path first [||] (if withVectors then dvOn else [])
        use far = products [ 100; 101 ] [ 1.0; 2.0 ] [ "seasonal"; "regular" ]
        append path far

        use source =
            DataFrame.create [|
                Series.create("id", [ 1; 3; 4; 5; 6 ])
                Series.create("price", [ 5.0; 35.0; 120.0; -1.0; 50.0 ])
                Series.create("category", [ "regular"; "regular"; "regular"; "new"; "new" ])
                Series.create("discontinued", [ true; false; false; false; false ])
            |]
        let clauses =
            [ MergeClauseKind.MatchedDelete, src "discontinued"
              MergeClauseKind.MatchedUpdate, src "price" .> pl.col "price"
              MergeClauseKind.NotMatchedInsert, src "price" .> pl.lit 0.0
              MergeClauseKind.NotMatchedBySourceDelete, pl.col "category" .== pl.lit "seasonal" ]
        // discontinued is not a table column: refused without evolution.
        Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () -> merge path source [ "id" ] clauses false |> ignore) |> ignore
        use narrow = source.Drop("discontinued")
        let clausesNarrow =
            [ MergeClauseKind.MatchedDelete, src "price" .< pl.lit 6.0
              MergeClauseKind.MatchedUpdate, src "price" .> pl.col "price"
              MergeClauseKind.NotMatchedInsert, src "price" .> pl.lit 0.0
              MergeClauseKind.NotMatchedBySourceDelete, pl.col "category" .== pl.lit "seasonal" ]

        let r = merge path narrow [ "id" ] clausesNarrow false

        // 1: deleted (first clause). 2: seasonal, not in source: deleted. 3: 35 > 30, updated. 4: 120 > 100, updated.
        // 5: price -1, not inserted. 6: inserted. 100: seasonal in a file outside the source's keys: deleted. 101 stays.
        Assert.Equal<(int * float * string) list>(
            [ (3, 35.0, "regular"); (4, 120.0, "regular"); (6, 50.0, "new"); (101, 2.0, "regular") ], productRows path)
        Assert.Equal((1L, 2L, 3L), (r.InsertedRows, r.UpdatedRows, r.DeletedRows)))

[<Fact>]
let ``Merge keys: null keys match nothing, and a target row matched twice is an error`` () =
    withTable "merge_keys" (fun path ->
        use target = products [ 1; 2 ] [ 10.0; 20.0 ] [ "a"; "a" ]
        create path target [||] []

        use withNull =
            DataFrame.create [|
                Series.create("id", [ Nullable 1; Nullable() ])
                Series.create("price", [ 11.0; 99.0 ])
                Series.create("category", [ "a"; "z" ])
            |]
        let r = merge path withNull [ "id" ] [] false
        Assert.Equal((1L, 1L), (r.InsertedRows, r.UpdatedRows))
        use all = (new LazyFrame(DeltaKernelScan.Scan path)).Collect()
        Assert.Equal(3L, all.Height)

        use twice = products [ 2; 2 ] [ 21.0; 22.0 ] [ "a"; "a" ]
        let e = Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () -> merge path twice [ "id" ] [] false |> ignore)
        Assert.Contains("more than one source row", e.Message)

        // Duplicate keys among rows that only insert are fine.
        use newTwice = products [ 7; 7 ] [ 1.0; 2.0 ] [ "n"; "n" ]
        let r2 = merge path newTwice [ "id" ] [] false
        Assert.Equal(2L, r2.InsertedRows))

[<Fact>]
let ``Merge evolves the schema, and keeps target values of columns the source lacks`` () =
    withTable "merge_evolve" (fun path ->
        use target = products [ 1; 2 ] [ 10.0; 20.0 ] [ "a"; "b" ]
        create path target [||] []

        // No category: an update keeps it, an insert has none. A new column: added with evolution.
        use source =
            DataFrame.create [|
                Series.create("id", [ 2; 3 ])
                Series.create("price", [ 25.0; 30.0 ])
                Series.create("stock", [ 5L; 6L ])
            |]
        Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () -> merge path source [ "id" ] [] false |> ignore) |> ignore
        let r = merge path source [ "id" ] [] true

        Assert.True(r.SchemaEvolved)
        let snapshot = Snapshot.Load(LocalFileSystem path)
        Assert.Equal<string list>([ "id"; "price"; "category"; "stock" ], snapshot.Schema.Fields |> Seq.map (fun f -> f.Name) |> List.ofSeq)
        use df = (new LazyFrame(DeltaKernelScan.Scan path)).Collect().Sort(pl.col "id")
        Assert.Equal<string list>([ "a"; "b"; null ], df.["category"].ToArray<string>() |> List.ofArray)
        Assert.Equal<Nullable<int64> list>([ Nullable(); Nullable 5L; Nullable 6L ], df.["stock"].ToArray<Nullable<int64>>() |> List.ofArray))

[<Fact>]
let ``Merge into a partitioned change feed table writes every change type`` () =
    withTable "merge_cdf" (fun path ->
        use target = products [ 1; 2; 3 ] [ 10.0; 20.0; 30.0 ] [ "a"; "a"; "b" ]
        create path target [| "category" |] (cdfOn @ dvOn)

        use source = products [ 1; 2; 9 ] [ 0.0; 21.0; 90.0 ] [ "a"; "b"; "c" ]
        let clauses =
            [ MergeClauseKind.MatchedDelete, src "price" .== pl.lit 0.0
              MergeClauseKind.MatchedUpdate, pl.lit true
              MergeClauseKind.NotMatchedInsert, pl.lit true ]
        let r = merge path source [ "id" ] clauses false
        Assert.Equal((1L, 1L, 1L), (r.InsertedRows, r.UpdatedRows, r.DeletedRows))
        Assert.Equal<(int * float * string) list>([ (2, 21.0, "b"); (3, 30.0, "b"); (9, 90.0, "c") ], productRows path)

        use changes = (new LazyFrame(DeltaKernelScan.ReadChangeFeed(path, 1L))).Collect().Sort([ pl.col "id"; pl.col "_change_type" ])
        let seen =
            List.zip3
                (changes.["id"].ToArray<int>() |> List.ofArray)
                (changes.["category"].ToArray<string>() |> List.ofArray)
                (changes.["_change_type"].ToArray<string>() |> List.ofArray)
        Assert.Equal<(int * string * string) list>(
            [ (1, "a", "delete"); (2, "b", "update_postimage"); (2, "a", "update_preimage"); (9, "c", "insert") ], seen))

[<Fact>]
let ``Merge into an empty table inserts`` () =
    withTable "merge_empty" (fun path ->
        use target = products [ 1 ] [ 1.0 ] [ "a" ]
        create path target [||] []
        DeltaKernelDelete.Delete(path, null) |> ignore
        use source = products [ 5; 6 ] [ 5.0; 6.0 ] [ "x"; "y" ]
        let r = merge path source [ "id" ] [] false
        Assert.Equal(2L, r.InsertedRows)
        Assert.Equal<(int * float * string) list>([ (5, 5.0, "x"); (6, 6.0, "y") ], productRows path))

// ---- optimize ----

let private optimize (path: string) (target: int64) (filters: (string * string) list) (zOrder: string list) =
    let f =
        if filters.IsEmpty then null
        else
            let d = Dictionary<string, string>()
            for k, v in filters do d[k] <- v
            d :> IReadOnlyDictionary<string, string>
    DeltaKernelOptimize.Optimize(path, target, f, (if zOrder.IsEmpty then null else Array.ofList zOrder))

let private logActions (path: string) (version: int64) (kind: string) =
    File.ReadLines(Path.Combine(path, "_delta_log", $"{version:D20}.json"))
    |> Seq.filter (fun l -> l.StartsWith($"{{\"{kind}\"", StringComparison.Ordinal))
    |> Seq.map (fun l -> JsonDocument.Parse(l).RootElement.GetProperty(kind).Clone())
    |> List.ofSeq

[<Fact>]
let ``Optimize compacts small files into one, as rows rearranged, and then has nothing to do`` () =
    withTable "optimize_compact" (fun path ->
        use first = frame [ 1; 2 ] "east"
        create path first [||] []
        for i in 3 .. 6 do
            use more = frame [ i ] "east"
            append path more
        Assert.Equal(5, Snapshot.Load(LocalFileSystem path).Files.Count)

        let r = optimize path (128L * 1024L * 1024L) [] []

        Assert.Equal((5, 1), (r.RemovedFiles, r.AddedFiles))
        Assert.Equal<int list>([ 1 .. 6 ], ids path)
        Assert.Single(Snapshot.Load(LocalFileSystem path).Files) |> ignore
        let v = r.Version.Value
        Assert.All(logActions path v "add", fun a -> Assert.False(a.GetProperty("dataChange").GetBoolean()))
        Assert.All(logActions path v "remove", fun a -> Assert.False(a.GetProperty("dataChange").GetBoolean()))
        let info = commitInfo path v
        Assert.Equal("OPTIMIZE", info.GetProperty("operation").GetString())
        Assert.Equal(5L, metric info "numRemovedFiles")

        Assert.False((optimize path (128L * 1024L * 1024L) [] []).Version.HasValue)
        use viaEngine = LazyFrame.ScanDelta(path).Collect()
        Assert.Equal(6L, viaEngine.Height))

[<Fact>]
let ``Optimize drops rows deleted through deletion vectors, only in the chosen partitions`` () =
    withTable "optimize_dv" (fun path ->
        use east = frame [ 1 .. 4 ] "east"
        use west = frame [ 5 .. 8 ] "west"
        use both = east.VStack west
        create path both [| "region" |] (dvOn @ cdfOn)
        delete path ((pl.col "id" .== pl.lit 2) .|| (pl.col "id" .== pl.lit 6)) |> ignore
        let before = Snapshot.Load(LocalFileSystem path)
        Assert.Equal(2, before.Files |> Seq.filter (fun f -> not (isNull f.DeletionVector)) |> Seq.length)

        let r = optimize path (128L * 1024L * 1024L) [ ("region", "east") ] []

        Assert.Equal((1, 1, 1), (r.RemovedFiles, r.AddedFiles, r.DeletionVectorsRemoved))
        let after = Snapshot.Load(LocalFileSystem path)
        let byRegion region = after.Files |> Seq.find (fun f -> f.PartitionValues["region"] = region)
        Assert.Null((byRegion "east").DeletionVector)
        Assert.NotNull((byRegion "west").DeletionVector)
        Assert.Equal(Nullable 3L, (byRegion "east").ParsedStats.NumRecords)
        Assert.Equal<int list>([ 1; 3; 4; 5; 7; 8 ], ids path)

        // Nothing changed for the change data feed.
        use changes = (new LazyFrame(DeltaKernelScan.ReadChangeFeed(path, r.Version.Value))).Collect()
        Assert.Equal(0L, changes.Height))

[<Fact>]
let ``Optimize Z-orders rows so files cluster on every column`` () =
    withTable "optimize_zorder" (fun path ->
        // A 16 x 16 grid in four files written in x order: each file spans every y.
        let points = [ for x in 0 .. 15 do for y in 0 .. 15 -> (x, y) ]
        use grid =
            DataFrame.create [|
                Series.create("id", points |> List.mapi (fun i _ -> i))
                Series.create("x", points |> List.map fst)
                Series.create("y", points |> List.map snd)
            |]
        create path grid [||] []
        let total = Snapshot.Load(LocalFileSystem path).Files |> Seq.sumBy (fun f -> f.Size)

        let r = optimize path ((total + 3L) / 4L) [] [ "x"; "y" ]

        Assert.Equal(4, r.AddedFiles)
        use df = (new LazyFrame(DeltaKernelScan.Scan path)).Collect()
        Assert.Equal(256L, df.Height)
        let span (f: AddAction) (column: string) =
            let ok1, lo = f.ParsedStats.TryGetMin(ReadOnlySpan<string>([| column |]))
            let ok2, hi = f.ParsedStats.TryGetMax(ReadOnlySpan<string>([| column |]))
            Assert.True(ok1 && ok2)
            hi.GetInt32() - lo.GetInt32()
        let files = Snapshot.Load(LocalFileSystem path).Files
        // Each output file is a block of the grid, narrow in both x and y.
        Assert.All(files, fun f ->
            let sx, sy = span f "x", span f "y"
            Assert.True(sx < 15 && sy < 15, $"x span {sx}, y span {sy}"))
        let info = commitInfo path r.Version.Value
        Assert.Equal("[\"x\",\"y\"]", info.GetProperty("operationParameters").GetProperty("zOrderBy").GetString())

        Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () -> optimize path 1000L [] [ "nope" ] |> ignore) |> ignore)

// ---- vacuum ----

[<Fact>]
let ``Vacuum deletes replaced and orphan files on disk, and the table reads the same`` () =
    withTable "vacuum" (fun path ->
        use east = frame [ 1 .. 4 ] "east"
        use west = frame [ 5 .. 8 ] "west"
        use both = east.VStack west
        create path both [| "region" |] dvOn
        delete path (pl.col "id" .== pl.lit 2) |> ignore          // a deletion vector
        optimize path (128L * 1024L * 1024L) [] [] |> ignore       // replaces the east file and drops its vector
        File.WriteAllText(Path.Combine(path, "region=west", "orphan.parquet"), "never committed")
        File.SetLastWriteTimeUtc(Path.Combine(path, "region=west", "orphan.parquet"), DateTime.UtcNow.AddDays(-30.0))
        delete path (pl.col "region" .== pl.lit "west") |> ignore   // the whole west file goes: its directory empties
        Directory.CreateDirectory(Path.Combine(path, "region=north", "nested")) |> ignore
        Directory.CreateDirectory(Path.Combine(path, ".staging")) |> ignore
        let before = Directory.GetFiles(path, "*", SearchOption.AllDirectories) |> Array.filter (fun f -> not (f.Contains "_delta_log"))

        // Removed moments ago: within the default 7 days only the month-old orphan would go, and a shorter
        // retention needs the check off.
        Assert.Equal<string seq>([ "region=west/orphan.parquet" ], (DeltaKernelVacuum.Vacuum(path, dryRun = true, full = true)).Files)
        Assert.Empty((DeltaKernelVacuum.Vacuum(path)).Files)
        Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () -> DeltaKernelVacuum.Vacuum(path, 0.0) |> ignore) |> ignore

        let dry = DeltaKernelVacuum.Vacuum(path, 0.0, enforceRetention = false, dryRun = true, full = true)
        let r = DeltaKernelVacuum.Vacuum(path, 0.0, enforceRetention = false, full = true)

        Assert.Equal<string seq>(dry.Files, r.Files)
        // The old east file, its deletion vector file, the west file and the orphan.
        Assert.Equal(4, r.Files.Count)
        Assert.Contains("region=west/orphan.parquet", r.Files)
        Assert.Contains(r.Files, fun f -> f.StartsWith("deletion_vector_", StringComparison.Ordinal))
        let after = Directory.GetFiles(path, "*", SearchOption.AllDirectories) |> Array.filter (fun f -> not (f.Contains "_delta_log"))
        Assert.Equal(before.Length - 4, after.Length)
        Assert.Equal<int list>([ 1; 3; 4 ], ids path)
        // As Spark: directories left without files go, hidden ones stay.
        Assert.Equal(3, r.Directories)
        Assert.False(Directory.Exists(Path.Combine(path, "region=west")))
        Assert.False(Directory.Exists(Path.Combine(path, "region=north")))
        Assert.True(Directory.Exists(Path.Combine(path, ".staging")))
        Assert.True(Directory.Exists(Path.Combine(path, "region=east")))
        Assert.Equal(3L, metric (commitInfo path (Snapshot.Load(LocalFileSystem path)).Version) "numVacuumedDirectories")
        let history = Snapshot.Load(LocalFileSystem path)
        Assert.Equal("VACUUM END", (commitInfo path history.Version).GetProperty("operation").GetString()))

// ---- restore and history ----

[<Fact>]
let ``Restore brings back an earlier version, by version or by time, and history lists every operation`` () =
    withTable "restore" (fun path ->
        use df = frame [ 1 .. 6 ] "east"
        create path df [||] dvOn                                                                   // v0
        delete path (pl.col "id" .< pl.lit 3) |> ignore                                            // v1
        update path (Some(pl.col "id" .== pl.lit 6)) [ ("price", pl.lit 0.0) ] |> ignore             // v2
        use source = frame [ 6; 7 ] "west"
        merge path source [ "id" ] [] false |> ignore                                              // v3
        let v1Time = (DeltaKernelHistory.Read(path) |> Seq.find (fun e -> e.Version = 1L)).Timestamp

        let r = DeltaKernelHistory.Restore(path, version = Nullable 1L)

        Assert.Equal(Nullable 4L, r.Version)
        Assert.Equal<int list>([ 3 .. 6 ], ids path)
        use atOne = (new LazyFrame(DeltaKernelScan.Scan(path, Nullable 1L))).Collect().Sort(pl.col "id")
        use now = (new LazyFrame(DeltaKernelScan.Scan path)).Collect().Sort(pl.col "id")
        Assert.True(now.Equals(atOne))

        // By time: back to v0, all six rows; restoring to where the table already is commits nothing.
        let r0 = DeltaKernelHistory.Restore(path, timestamp = Nullable(v1Time.AddMilliseconds(-1.0)))
        Assert.Equal<int list>([ 1 .. 6 ], ids path)
        Assert.False((DeltaKernelHistory.Restore(path, version = Nullable 0L)).Version.HasValue)

        let history = DeltaKernelHistory.Read(path)
        Assert.Equal<int64 list>([ 5L; 4L; 3L; 2L; 1L; 0L ], history |> Seq.map (fun e -> e.Version) |> List.ofSeq)
        Assert.Equal<string list>(
            [ "RESTORE"; "RESTORE"; "MERGE"; "UPDATE"; "DELETE"; "WRITE" ],
            history |> Seq.map (fun e -> e.CommitInfo.Operation) |> List.ofSeq)
        Assert.Equal(r0.Version, Nullable 5L)

        // The JSON the public history reads: one object per commit, version and time set by the log.
        use doc = JsonDocument.Parse(DeltaKernelHistory.ToJson(DeltaKernelHistory.Read(path, 2)))
        let rows = doc.RootElement.EnumerateArray() |> List.ofSeq
        Assert.Equal(2, rows.Length)
        Assert.Equal(5L, rows[0].GetProperty("version").GetInt64())
        Assert.Equal("0", rows[0].GetProperty("operationParameters").GetProperty("version").GetString())
        Assert.True(rows[0].GetProperty("operationMetrics").TryGetProperty("numRestoredFiles") |> fst))

// ---- failed operations ----

[<Fact>]
let ``A failed operation deletes the files it wrote`` () =
    withTable "failed_write" (fun path ->
        // id NOT NULL, deletion vectors on.
        let schema = """{"type":"struct","fields":[{"name":"id","type":"integer","nullable":false,"metadata":{}},{"name":"v","type":"integer","nullable":true,"metadata":{}}]}"""
        let fs = LocalFileSystem path
        let protocol = ProtocolAction(MinReaderVersion = 3, MinWriterVersion = 7, ReaderFeatures = [| TableFeatures.DeletionVectors |], WriterFeatures = [| TableFeatures.DeletionVectors |])
        let metadata = MetadataAction(Id = Guid.NewGuid().ToString(), SchemaString = schema, Configuration = config dvOn)
        Transaction.Create(fs, protocol, metadata).Commit() |> ignore
        use df = DataFrame.create [| Series.create("id", [ 1; 2; 3 ]); Series.create("v", [ 1; 2; 3 ]) |]
        append path df
        let files () = Directory.GetFiles(path, "*", SearchOption.AllDirectories) |> Array.filter (fun f -> not (f.Contains "_delta_log")) |> Array.sort

        let before = files ()
        // The deletion vector of the old row is written first; then the new row fails the NOT NULL check.
        Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () ->
            update path (Some(pl.col "v" .== pl.lit 1)) [ ("id", pl.litNull () ) ] |> ignore) |> ignore

        Assert.Equal<string[]>(before, files ())
        Assert.Equal(1L, Snapshot.Load(fs).Version)
        Assert.Equal<int list>([ 1; 2; 3 ], ids path))

// ---- schema evolution on write ----

let private writeWith (path: string) (df: DataFrame) (mode: KernelWriteMode) (partitionBy: string[]) (evolve: bool) =
    DeltaKernelWrite.Write(path, df.Lazy().CloneHandle(), mode, partitionBy, canEvolve = evolve) |> ignore

[<Fact>]
let ``Appends cast up and fill missing columns; evolution adds columns and widens types`` () =
    withTable "evolve_append" (fun path ->
        use first = DataFrame.create [| Series.create("id", [ 1L; 2L ]); Series.create("name", [ "a"; "b" ]) |]
        create path first [||] [ (SchemaEvolution.EnableTypeWideningKey, "true") ]
        // The property brings the type widening feature with it.
        Assert.Contains(TableFeatures.TypeWidening, Snapshot.Load(LocalFileSystem path).Protocol.WriterFeatures)

        // An Int32 id into the long column, no name: cast and filled, no schema change.
        use narrow = DataFrame.create [| Series.create("id", [ 3 ]) |]
        writeWith path narrow KernelWriteMode.Append null false
        // A new column without evolution is refused; with it, added.
        use wider = DataFrame.create [| Series.create("id", [ 4L ]); Series.create("name", [ "d" ]); Series.create("score", [ 1.5 ]) |]
        Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () -> writeWith path wider KernelWriteMode.Append null false) |> ignore
        writeWith path wider KernelWriteMode.Append null true

        use df = (new LazyFrame(DeltaKernelScan.Scan path)).Collect().Sort(pl.col "id")
        Assert.Equal<string list>([ "id"; "name"; "score" ], df.ColumnNames |> List.ofSeq)
        Assert.Equal<string list>([ "a"; "b"; null; "d" ], df.["name"].ToArray<string>() |> List.ofArray)
        Assert.Equal<Nullable<float> list>([ Nullable(); Nullable(); Nullable(); Nullable 1.5 ], df.["score"].ToArray<Nullable<float>>() |> List.ofArray)

        // Type widening: a table column of int32 widened to int64 by an evolving append; old files read as int64.
        withTable "evolve_widen" (fun widePath ->
            use small = DataFrame.create [| Series.create("v", [ 1; 2 ]) |]
            create widePath small [||] [ (SchemaEvolution.EnableTypeWideningKey, "true") ]
            let wfs = LocalFileSystem widePath
            use big = DataFrame.create [| Series.create("v", [ 5_000_000_000L ]) |]
            Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () -> writeWith widePath big KernelWriteMode.Append null false) |> ignore
            writeWith widePath big KernelWriteMode.Append null true
            let field = Snapshot.Load(wfs).Schema.Field("v")
            Assert.Equal("long", field.Type.ToString())
            Assert.Contains("\"fromType\":\"integer\"", field.Metadata[SchemaEvolution.TypeChangesKey])
            use all = (new LazyFrame(DeltaKernelScan.Scan widePath)).Collect().Sort(pl.col "v")
            Assert.Equal<int64 list>([ 1L; 2L; 5_000_000_000L ], all.["v"].ToArray<int64>() |> List.ofArray)))

[<Fact>]
let ``Overwrite with evolution replaces the schema and the partitioning`` () =
    withTable "evolve_overwrite" (fun path ->
        use first = frame [ 1; 2 ] "east"
        create path first [| "region" |] []
        use other = DataFrame.create [| Series.create("key", [ "x"; "y" ]); Series.create("day", [ 1; 2 ]) |]

        Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () -> writeWith path other KernelWriteMode.Overwrite [| "day" |] false) |> ignore
        writeWith path other KernelWriteMode.Overwrite [| "day" |] true
        // Overwriting again with the same shape and evolution on changes no metadata.
        use same = DataFrame.create [| Series.create("key", [ "z" ]); Series.create("day", [ 3 ]) |]
        writeWith path same KernelWriteMode.Overwrite [| "day" |] true
        let last = Snapshot.Load(LocalFileSystem path).Version
        Assert.DoesNotContain("\"metaData\"", File.ReadAllText(Path.Combine(path, "_delta_log", $"{last:D20}.json")))
        writeWith path other KernelWriteMode.Overwrite [| "day" |] false

        let snapshot = Snapshot.Load(LocalFileSystem path)
        Assert.Equal<string list>([ "key"; "day" ], snapshot.Schema.Fields |> Seq.map (fun f -> f.Name) |> List.ofSeq)
        Assert.Equal<string list>([ "day" ], snapshot.PartitionColumns |> List.ofSeq)
        use df = (new LazyFrame(DeltaKernelScan.Scan path)).Collect().Sort(pl.col "key")
        Assert.Equal<string list>([ "x"; "y" ], df.["key"].ToArray<string>() |> List.ofArray)
        Assert.Equal(1L, (new LazyFrame(DeltaKernelScan.Scan(path, Nullable 0L))).Collect().Select(pl.col "region").Unique().Height))

[<Fact>]
let ``Merge adds columns to a table with column mapping`` () =
    withTable "evolve_merge_mapping" (fun path ->
        use first = products [ 1; 2 ] [ 10.0; 20.0 ] [ "a"; "b" ]
        create path first [||] [ (TableFeatures.ColumnMappingModeKey, "name") ]
        let fs = LocalFileSystem path
        let created = Snapshot.Load fs
        Assert.Equal((2, 5), (created.Protocol.MinReaderVersion, created.Protocol.MinWriterVersion))
        Assert.All(created.Schema.Fields, fun f -> Assert.StartsWith("col-", f.PhysicalName))

        use source =
            DataFrame.create [| Series.create("id", [ 2; 3 ]); Series.create("price", [ 25.0; 30.0 ]); Series.create("stock", [ 5L; 6L ]) |]
        let r = merge path source [ "id" ] [] true

        Assert.True(r.SchemaEvolved)
        let stock = Snapshot.Load(fs).Schema.Field("stock")
        Assert.NotNull(stock.FieldId) |> ignore
        Assert.StartsWith("col-", stock.PhysicalName)
        use df = (new LazyFrame(DeltaKernelScan.Scan path)).Collect().Sort(pl.col "id")
        Assert.Equal<Nullable<int64> list>([ Nullable(); Nullable 5L; Nullable 6L ], df.["stock"].ToArray<Nullable<int64>>() |> List.ofArray)
        Assert.Equal<string list>([ "a"; "b"; null ], df.["category"].ToArray<string>() |> List.ofArray))

[<Fact>]
let ``Evolution adds nested fields, in structs and in arrays of structs, as Spark's mergeSchema`` () =
    withTable "evolve_nested" (fun path ->
        let rows (ids: int list) (withNew: bool) =
            use df =
                DataFrame.create [|
                    Series.create("id", ids)
                    Series.create("a", ids)
                    Series.create("b", ids |> List.map (fun i -> $"new{i}"))
                |]
            let sFields = if withNew then [ pl.col "a"; pl.col "b" ] else [ pl.col "a" ]
            let elementFields = if withNew then [ pl.col "a" |> pl.alias "x"; pl.col "b" |> pl.alias "y" ] else [ pl.col "a" |> pl.alias "x" ]
            df.Select([ pl.col "id"; pl.asStruct sFields |> pl.alias "s"; (pl.asStruct elementFields).Implode() |> pl.alias "arr" ])
        use first = rows [ 1 ] false
        create path first [||] []

        use wider = rows [ 2 ] true
        Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(fun () -> writeWith path wider KernelWriteMode.Append null false) |> ignore
        writeWith path wider KernelWriteMode.Append null true
        // Data of the old shape still goes in: the new nested fields are null.
        use older = rows [ 3 ] false
        writeWith path older KernelWriteMode.Append null false

        let schema = Snapshot.Load(LocalFileSystem path).Schema
        Assert.Equal("struct<a:integer,b:string>", schema.Field("s").Type.ToString().Replace(" ", ""))
        use df = (new LazyFrame(DeltaKernelScan.Scan path)).Collect().Sort(pl.col "id")
        use flat =
            df.Select([ pl.col "id"
                        pl.col("s").Struct.Field("b") |> pl.alias "sb"
                        pl.col("arr").List.First().Struct.Field("y") |> pl.alias "ay" ])
        Assert.Equal<string list>([ null; "new2"; null ], flat.["sb"].ToArray<string>() |> List.ofArray)
        Assert.Equal<string list>([ null; "new2"; null ], flat.["ay"].ToArray<string>() |> List.ofArray))

[<Fact>]
let ``Overwrite with evolution and no partitionBy leaves the table unpartitioned, as Spark`` () =
    withTable "evolve_unpartition" (fun path ->
        use first = frame [ 1; 2 ] "east"
        create path first [| "region" |] []
        use again = frame [ 3 ] "west"
        // Without evolution the partitioning stays; with it and no partitionBy it goes (delta-spark 4.4).
        writeWith path again KernelWriteMode.Overwrite null false
        Assert.Equal<string list>([ "region" ], Snapshot.Load(LocalFileSystem path).PartitionColumns |> List.ofSeq)
        writeWith path again KernelWriteMode.Overwrite null true
        Assert.Empty(Snapshot.Load(LocalFileSystem path).PartitionColumns)
        Assert.Equal<int list>([ 3 ], ids path))

[<Fact>]
let ``Creating a table with in-commit timestamps or v2 checkpoints brings their features`` () =
    withTable "create_features" (fun path ->
        let ictPath = Path.Combine(path, "ict")
        create ictPath (frame [ 1; 2 ] "east") [||] [ ("delta.enableInCommitTimestamps", "true") ]
        let ict = Snapshot.Load(LocalFileSystem ictPath)
        Assert.Equal((1, 7), (ict.Protocol.MinReaderVersion, ict.Protocol.MinWriterVersion))
        Assert.Contains(TableFeatures.InCommitTimestamp, ict.Protocol.WriterFeatures)
        Assert.Contains("\"inCommitTimestamp\"", File.ReadAllText(Path.Combine(ictPath, "_delta_log", "00000000000000000000.json")))
        append ictPath (frame [ 3 ] "west")
        Assert.Equal<int list>([ 1; 2; 3 ], ids ictPath)

        let v2Path = Path.Combine(path, "v2")
        create v2Path (frame [ 1; 2 ] "east") [||] [ ("delta.checkpointPolicy", "v2") ]
        let v2 = Snapshot.Load(LocalFileSystem v2Path)
        Assert.Equal((3, 7), (v2.Protocol.MinReaderVersion, v2.Protocol.MinWriterVersion))
        Assert.Contains(TableFeatures.V2Checkpoint, v2.Protocol.ReaderFeatures)
        Assert.Contains(TableFeatures.V2Checkpoint, v2.Protocol.WriterFeatures))
