/// Row tracking (stable row ids through deletes, updates, merges and optimize) and identity columns (generated
/// values, Spark's refusals) through the kernel's writers. Expectations follow delta-spark 4.1.
module Polars.FSharp.Tests.SeseragiRowTrackingIdentityTests

open System
open System.Collections.Generic
open System.IO
open Xunit
open Polars.FSharp
open Polars.NET.Core.Delta
open Kaminari.Seseragi.Lakehouse.Delta
open Kaminari.Seseragi.Lakehouse.Delta.Actions
open Kaminari.Seseragi.Lakehouse.Engine

let private withTable (name: string) (body: string -> unit) =
    let path = Path.Combine(Path.GetTempPath(), $"seseragi_rt_{name}_{Guid.NewGuid():N}")
    try
        body path
    finally
        if Directory.Exists path then Directory.Delete(path, true)

let private rows (ids: int list) (values: string list) =
    DataFrame.create [| Series.create("id", ids); Series.create("v", values) |]

let private write (path: string) (df: DataFrame) (mode: KernelWriteMode) (properties: (string * string) list) =
    let config = Dictionary<string, string>()
    for k, v in properties do config[k] <- v
    DeltaKernelWrite.Write(path, df.Lazy().CloneHandle(), mode, null, configuration = config)

let private snapshot (path: string) = Snapshot.Load(LocalFileSystem path)

/// (id, row id, row commit version) of every live row, by id, as Spark's _metadata.row_id / row_commit_version.
let private rowIds (path: string) =
    let table = snapshot path
    let columns = RowTracking.MaterializedColumns(table.Metadata)
    [ for file in table.Files do
        use lf = new LazyFrame(DeltaKernelScan.ScanFile(LocalFileSystem path, path, table, file, CloudArgs.Local, rowTracking = columns))
        use df = lf.Collect()
        yield!
            Array.zip3
                (df.["id"].ToArray<int>())
                (df.[DeltaKernelScan.RowIdColumn].ToArray<int64>())
                (df.[DeltaKernelScan.RowCommitVersionColumn].ToArray<int64>()) ]
    |> List.sortBy (fun (id, _, _) -> id)

let private src (name: string) = pl.col (name + DeltaKernelMerge.SourceSuffix)

let private merge (path: string) (source: DataFrame) (clauses: (MergeClauseKind * Expr) list) =
    let list = ResizeArray(clauses |> List.map (fun (k, c) -> MergeClause(k, c.CloneHandle())))
    DeltaKernelMerge.Merge(path, source.Lazy().CloneHandle(), [| "id" |], list, false)

let private update (path: string) (predicate: Expr) (column: string) (value: Expr) =
    DeltaKernelUpdate.Update(path, predicate.CloneHandle(), ResizeArray [ struct (column, value.CloneHandle()) ])

let private assertUnique (ids: (int * int64 * int64) list) =
    let rowIds = ids |> List.map (fun (_, r, _) -> r)
    Assert.Equal(rowIds.Length, (List.distinct rowIds).Length)

// ---------------------------------------------------------------- row tracking

[<Theory>]
[<InlineData(false)>]
[<InlineData(true)>]
let ``Row ids stay with their rows through delete, update, merge and optimize`` (deletionVectors: bool) =
    withTable "lifecycle" (fun path ->
        let props = [ ("delta.enableRowTracking", "true") ] @ (if deletionVectors then [ ("delta.enableDeletionVectors", "true") ] else [])
        use df = rows [ 1; 2; 3; 4 ] [ "a"; "b"; "c"; "d" ]
        write path df KernelWriteMode.ErrorIfExists props |> ignore
        let created = snapshot path
        Assert.True(RowTracking.IsEnabled(created.Protocol, created.Metadata))
        Assert.Contains(TableFeatures.DomainMetadata, created.Protocol.WriterFeatures)
        Assert.Equal<(int * int64 * int64) list>([ (1, 0L, 0L); (2, 1L, 0L); (3, 2L, 0L); (4, 3L, 0L) ], rowIds path)

        // Deleting keeps the other rows' ids and versions (rewritten or marked in a deletion vector).
        DeltaKernelDelete.Delete(path, (pl.col "id" .== pl.lit 2).CloneHandle()) |> ignore
        Assert.Equal<(int * int64 * int64) list>([ (1, 0L, 0L); (3, 2L, 0L); (4, 3L, 0L) ], rowIds path)

        // An updated row keeps its id and takes the update's version.
        update path (pl.col "id" .== pl.lit 3) "v" (pl.lit "C") |> ignore
        Assert.Equal<(int * int64 * int64) list>([ (1, 0L, 0L); (3, 2L, 2L); (4, 3L, 0L) ], rowIds path)

        // A merge: the updated row keeps its id, the inserted one gets a fresh id; both take the merge's version.
        use source = rows [ 1; 9 ] [ "A"; "I" ]
        merge path source [ (MergeClauseKind.MatchedUpdate, pl.lit true); (MergeClauseKind.NotMatchedInsert, pl.lit true) ] |> ignore
        let afterMerge = rowIds path
        Assert.Equal<(int * int64 * int64) list>([ (1, 0L, 3L); (3, 2L, 2L); (4, 3L, 0L) ], afterMerge |> List.filter (fun (id, _, _) -> id <> 9))
        let (_, newId, newVersion) = afterMerge |> List.find (fun (id, _, _) -> id = 9)
        Assert.True(newId > 3L, $"fresh id {newId}")
        Assert.Equal(3L, newVersion)
        assertUnique afterMerge
        Assert.True(newId <= RowTracking.HighWaterMark((snapshot path).DomainMetadata).Value)

        // Optimize rewrites every file and changes nothing about the rows.
        DeltaKernelOptimize.Optimize(path, zOrderColumns = [| "id" |]) |> ignore
        Assert.Equal<(int * int64 * int64) list>(afterMerge, rowIds path)
        Assert.Equal<int list>([ 1; 3; 4; 9 ], rowIds path |> List.map (fun (id, _, _) -> id)))

[<Fact>]
let ``Change data files of a row-tracking table carry the materialized columns`` () =
    withTable "cdc" (fun path ->
        use df = rows [ 1; 2 ] [ "a"; "b" ]
        write path df KernelWriteMode.ErrorIfExists [ ("delta.enableRowTracking", "true"); ("delta.enableChangeDataFeed", "true") ] |> ignore
        update path (pl.col "id" .== pl.lit 2) "v" (pl.lit "B") |> ignore
        let rowId, version = RowTracking.MaterializedColumns((snapshot path).Metadata).Value.ToTuple()
        let cdc = Directory.GetFiles(Path.Combine(path, "_change_data"), "*.parquet") |> Array.exactlyOne
        use changes = LazyFrame.ScanParquet(cdc).Collect().Sort(pl.col "_change_type")
        // As Spark writes them (sorted: update_postimage, update_preimage): the postimage with its id and no
        // version, the preimage with its id and version.
        Assert.Equal<int64 list>([ 1L; 1L ], changes.[rowId].ToArray<int64>() |> List.ofArray)
        Assert.Equal<Nullable<int64> list>([ Nullable(); Nullable 0L ], changes.[version].ToArray<Nullable<int64>>() |> List.ofArray)
        // Plain appends write no materialized columns.
        let appended = Snapshot.Load(LocalFileSystem path, 0L).Files |> Seq.exactlyOne
        use plain = LazyFrame.ScanParquet(Path.Combine(path, appended.DecodedPath)).Collect()
        Assert.DoesNotContain(rowId, plain.Columns))

[<Fact>]
let ``Turning row tracking on for an existing table backfills ids that later operations keep`` () =
    withTable "enable" (fun path ->
        use df = rows [ 1; 2; 3 ] [ "a"; "b"; "c" ]
        write path df KernelWriteMode.ErrorIfExists [] |> ignore
        use more = rows [ 4 ] [ "d" ]
        write path more KernelWriteMode.Append [] |> ignore
        let props = Dictionary<string, string>()
        props["delta.enableRowTracking"] <- "true"
        Assert.Equal(Nullable 2L, DeltaKernelAlter.SetTableProperties(path, props))
        let before = rowIds path
        assertUnique before
        Assert.All(before, fun (_, _, v) -> Assert.Equal(2L, v))

        DeltaKernelDelete.Delete(path, (pl.col "id" .== pl.lit 1).CloneHandle()) |> ignore
        Assert.Equal<(int * int64 * int64) list>(before |> List.filter (fun (id, _, _) -> id <> 1), rowIds path))

// ---------------------------------------------------------------- identity columns

let private identityTable (path: string) (always: bool) =
    let schema =
        """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{"delta.identity.step":5,"delta.identity.allowExplicitInsert":ALLOW,"delta.identity.start":10}},{"name":"v","type":"string","nullable":true,"metadata":{}}]}"""
            .Replace("ALLOW", (if always then "false" else "true"))
    Directory.CreateDirectory path |> ignore
    let protocol = ProtocolAction(MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = [| TableFeatures.IdentityColumns; "appendOnly"; "invariants" |])
    Transaction.Create(LocalFileSystem path, protocol, MetadataAction(Id = "t", SchemaString = schema)).Commit() |> ignore

let private values (path: string) =
    use df = (new LazyFrame(DeltaKernelScan.Scan path)).Collect().Sort(pl.col "v")
    List.zip (df.["v"].ToArray<string>() |> List.ofArray) (df.["id"].ToArray<Nullable<int64>>() |> List.ofArray)

let private mark (path: string) =
    (IdentityColumns.Of((snapshot path).Schema) |> Seq.exactlyOne).HighWaterMark

let private vs (values: string list) = DataFrame.create [| Series.create("v", values) |]

[<Fact>]
let ``GENERATED ALWAYS identity values are generated on append, overwrite and merge, and never given`` () =
    withTable "always" (fun path ->
        identityTable path true
        use first = vs [ "a"; "b"; "c" ]
        write path first KernelWriteMode.Append [] |> ignore
        Assert.Equal<(string * Nullable<int64>) list>([ ("a", Nullable 10L); ("b", Nullable 15L); ("c", Nullable 20L) ], values path)
        Assert.Equal(Nullable 20L, mark path)

        // Values are unique and the mark advances by the rows written; overwrite continues after it (as Spark).
        use second = vs [ "d" ]
        write path second KernelWriteMode.Overwrite [] |> ignore
        Assert.Equal<(string * Nullable<int64>) list>([ ("d", Nullable 25L) ], values path)
        Assert.Equal(Nullable 25L, mark path)

        // Merge inserts get values; updating other columns is fine.
        use source = vs [ "d"; "e" ]
        let ins = DeltaKernelMerge.Merge(
                    path, source.Lazy().CloneHandle(), [| "v" |],
                    ResizeArray [ MergeClause(MergeClauseKind.NotMatchedInsert, (pl.lit true).CloneHandle()) ], false)
        Assert.Equal(1L, ins.InsertedRows)
        Assert.Equal<(string * Nullable<int64>) list>([ ("d", Nullable 25L); ("e", Nullable 30L) ], values path)
        Assert.Equal(Nullable 30L, mark path)

        // Spark's refusals, nothing committed.
        let version = (snapshot path).Version
        use explicitRows = DataFrame.create [| Series.create("id", [ 1L ]); Series.create("v", [ "x" ]) |]
        let e1 = Assert.ThrowsAny<exn>(fun () -> write path explicitRows KernelWriteMode.Append [] |> ignore)
        Assert.Contains("GENERATED ALWAYS AS IDENTITY column id", e1.Message)
        let e2 = Assert.ThrowsAny<exn>(fun () -> update path (pl.col "v" .== pl.lit "d") "id" (pl.lit 1L) |> ignore)
        Assert.Contains("UPDATE on IDENTITY column id", e2.Message)
        let e3 = Assert.ThrowsAny<exn>(fun () ->
            DeltaKernelMerge.Merge(path, explicitRows.Lazy().CloneHandle(), [| "v" |],
                ResizeArray [ MergeClause(MergeClauseKind.MatchedUpdate, (pl.lit true).CloneHandle()) ], false) |> ignore)
        Assert.Contains("UPDATE on IDENTITY column id", e3.Message)
        Assert.Equal(version, (snapshot path).Version))

[<Fact>]
let ``GENERATED BY DEFAULT keeps given values, nulls included, without moving the mark`` () =
    withTable "bydefault" (fun path ->
        identityTable path false
        use given = DataFrame.create [| Series.create("id", [ Some 100L; None ]); Series.create("v", [ "x"; "y" ]) |]
        write path given KernelWriteMode.Append [] |> ignore
        Assert.Equal(Nullable(), mark path)
        use generated = vs [ "z" ]
        write path generated KernelWriteMode.Append [] |> ignore
        Assert.Equal<(string * Nullable<int64>) list>([ ("x", Nullable 100L); ("y", Nullable()); ("z", Nullable 10L) ], values path)
        Assert.Equal(Nullable 10L, mark path)

        // A merge inserting the source's ids keeps them.
        use source = DataFrame.create [| Series.create("id", [ 77L ]); Series.create("v", [ "m" ]) |]
        DeltaKernelMerge.Merge(path, source.Lazy().CloneHandle(), [| "v" |],
            ResizeArray [ MergeClause(MergeClauseKind.NotMatchedInsert, (pl.lit true).CloneHandle()) ], false) |> ignore
        Assert.Contains(("m", Nullable 77L), values path)
        Assert.Equal(Nullable 10L, mark path))
