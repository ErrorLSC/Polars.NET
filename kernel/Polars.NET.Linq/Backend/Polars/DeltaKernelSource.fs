namespace Polars.NET.Linq.Backend.Polars

open System
open Polars.NET.Core
open Polars.NET.Core.Delta
open Polars.NET.Core.Wrappers
open Kaminari.Samidare.IR

/// A Delta table scanned by the managed kernel, as a query source: a frame `LazyFrame.ScanDelta` (or a catalog table
/// scan) returned, whose origin says how to scan it again. The filter a query puts right on top of it arrives as a
/// hint (IFilterAwareSource) and leaves out the files whose partition values and statistics rule every row out (the
/// kernel's file skipping, from the IR as it is); the query still filters the rows. Statistics and identity are the
/// frame's.
type internal DeltaKernelSource
    private (origin: DeltaScanOrigin, frame: PolarsTableSource, filter: ScalarExpr, scannedFiles: Action<int>) =

    /// A source over a frame a Delta scan made, with the origin it carries. `scannedFiles`, when not null, is told
    /// how many data files each lowering of a query scans.
    new(origin: DeltaScanOrigin, frame: LazyFrameHandle, scannedFiles: Action<int>) =
        DeltaKernelSource(origin, PolarsTableSource(frame, PolarsTableSources.schemaOf frame), Unchecked.defaultof<_>, scannedFiles)

    /// A source over the table at `path` at `version` (null: the latest when the source is made).
    new(path: string, version: Nullable<int64>, scannedFiles: Action<int>) =
        let mutable read = 0L
        let mutable casts = false
        let frame = DeltaKernelScan.ScanPath(path, version, Nullable(), null, null, null, null, Unchecked.defaultof<ScalarExpr>, &read, &casts)
        let origin =
            DeltaScanOrigin(read, Func<ScalarExpr, Action<int>, LazyFrameHandle>(fun f scanned ->
                DeltaKernelScan.ScanPath(path, Nullable read, Nullable(), null, null, scanned, null, f)))
        DeltaKernelSource(origin, frame, scannedFiles)

    /// The row filter the source scans with; null when it reads every file.
    member _.Filter = filter

    /// The table as a lazy frame, without the files the filter rules out: scanned again with the filter, or the frame
    /// itself when there is none. When the new scan fails (the catalog client released, the version's log cleaned up)
    /// the frame is read whole: the same rows, without skipping.
    member _.Open() : LazyFrameHandle =
        if isNull (box filter) then
            PolarsWrapper.LazyClone frame.Handle
        else
            try
                origin.Scan(filter, scannedFiles)
            with e ->
                Diagnostics.Trace.TraceWarning($"[Polars.NET] Delta file skipping not applied, the table is read whole: {e.Message}")
                PolarsWrapper.LazyClone frame.Handle

    interface ILogicalTableSource with
        member _.Name = "DeltaKernel"
        member _.Schema = (frame :> ILogicalTableSource).Schema

    interface ISourceStatistics with
        member _.EstimatedRows = (frame :> ISourceStatistics).EstimatedRows

    interface ISourceIdentity with
        member _.SourceIdentity = (frame :> ISourceIdentity).SourceIdentity

    interface IColumnStatisticsSource with
        member _.TryGetColumnStatistics column = (frame :> IColumnStatisticsSource).TryGetColumnStatistics column
        member _.TryGetColumnRange column = (frame :> IColumnStatisticsSource).TryGetColumnRange column

    interface IFilterAwareSource with
        member this.WithFilterHint(predicate: ScalarExpr) : ILogicalTableSource =
            let combined = if isNull (box filter) then predicate else ScalarExpr.Binary(BinaryOp.And, filter, predicate)
            DeltaKernelSource(origin, frame, combined, scannedFiles)

/// Binding frames to query sources: a frame a Delta scan returned as it is becomes a DeltaKernelSource.
module internal DeltaKernelSources =
    /// The query source of `frame` (`original`, or a clone of it): a Delta scan's, with file skipping, when `original`
    /// carries the scan's origin; else the frame itself.
    let ofLazyFrame (original: LazyFrameHandle) (frame: LazyFrameHandle) : ILogicalTableSource =
        match original.DeltaOrigin with
        | null -> PolarsTableSources.ofLazyFrame frame
        | origin -> DeltaKernelSource(origin, frame, null)
