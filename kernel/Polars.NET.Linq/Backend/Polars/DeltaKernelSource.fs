namespace Polars.NET.Linq.Backend.Polars

open System
open Polars.NET.Core
open Polars.NET.Core.Delta
open Polars.NET.Core.Helpers
open Kaminari.Samidare.IR
open Kaminari.Seseragi.Lakehouse.Samidare
open Kaminari.Seseragi.Lakehouse.Skipping

/// A Delta table read through the managed kernel, as a query source. The filter a query puts right on top of it
/// arrives as a hint (IFilterAwareSource), is translated to a kernel predicate, and lets the kernel leave out files
/// whose statistics rule every row out; the query still filters the rows.
/// Not reachable from the public API: the public Delta functions switch to the kernel in one step, later.
type internal DeltaKernelSource
    private (path: string, version: Nullable<int64>, schema: Lazy<LogicalSchema>, filter: Predicate, scannedFiles: Action<int>) =

    /// A source over the table at `version` (null: the latest when the query runs). `scannedFiles`, when not
    /// null, is told how many data files each lowering of a query scans.
    new(path: string, version: Nullable<int64>, scannedFiles: Action<int>) =
        let schema =
            lazy
                (use lf = DeltaKernelScan.Scan(path, version)
                 PolarsSchemaExtractor.GetFields lf
                 |> Array.map (fun struct (name, dataType) ->
                     use dataType = dataType
                     name, PolarsVocabulary.ofDataTypeHandle dataType)
                 |> LogicalSchema.OfFields)
        DeltaKernelSource(path, version, schema, null, scannedFiles)

    /// The kernel predicate the source scans with; null when it reads every file.
    member _.Filter = filter

    /// The table as a lazy frame, without the files the filter rules out.
    member _.Open() : LazyFrameHandle =
        DeltaKernelScan.ScanPath(path, version, Nullable(), filter, null, scannedFiles)

    interface ILogicalTableSource with
        member _.Name = "DeltaKernel"
        member _.Schema = schema.Value

    interface IFilterAwareSource with
        member this.WithFilterHint(predicate: ScalarExpr) : ILogicalTableSource =
            match PredicateTranslator.Translate(predicate, schema.Value) with
            | null -> this
            | translated ->
                let combined = if isNull filter then translated else Predicate.And(filter, translated)
                DeltaKernelSource(path, version, schema, combined, scannedFiles)
