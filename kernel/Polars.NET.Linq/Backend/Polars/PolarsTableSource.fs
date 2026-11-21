namespace Polars.NET.Linq.Backend.Polars

open Polars.NET.Core
open Polars.NET.Core.Wrappers
open Polars.NET.Core.Helpers
open Kaminari.Samidare.IR
open Kaminari.Samidare.Config

/// Physical Polars LazyFrame implementation of ILogicalTableSource.
type internal PolarsTableSource(handle: LazyFrameHandle, schema: LogicalSchema) =
    let estimatedRows =
        lazy (
            let rows = PolarsWrapper.LazyEstimatedRows handle
            if rows.HasValue then Some rows.Value else None
        )
    let identity =
        lazy (
            let plan = PolarsWrapper.Explain(handle, false)
            if plan.Contains "SCAN [" then Some plan else None
        )
    member _.Handle = handle
    interface ILogicalTableSource with
        member _.Name = "PolarsLazyFrame"
        member _.Schema = schema
    interface ISourceStatistics with
        member _.EstimatedRows = estimatedRows.Value
    interface ISourceIdentity with
        member _.SourceIdentity = identity.Value
    interface IColumnStatisticsSource with
        member _.TryGetColumnStatistics column =
            if SamidareConfig.CollectColumnStatistics then
                PolarsColumnStatistics.tryGet handle schema estimatedRows.Value column
            else
                None
        member _.TryGetColumnRange column =
            if SamidareConfig.CollectColumnStatistics then
                PolarsColumnStatistics.tryGetRange handle schema column
            else
                None

/// Frames as table sources.
module internal PolarsTableSources =
    /// The schema, with column types, of a physical LazyFrameHandle.
    let schemaOf (lf: LazyFrameHandle) : LogicalSchema =
        PolarsSchemaExtractor.GetFields lf
        |> Array.map (fun struct (name, dataType) ->
            use dataType = dataType
            name, PolarsVocabulary.ofDataTypeHandle dataType)
        |> LogicalSchema.OfFields

    /// Wraps a physical LazyFrameHandle as an opaque ILogicalTableSource (schema, with column types, read from the frame).
    let ofLazyFrame (lf: LazyFrameHandle) : ILogicalTableSource = PolarsTableSource(lf, schemaOf lf)
