namespace Polars.FSharp

open Polars.NET.Core.Arrow
open Apache.Arrow
open System
open Polars.NET.Core.Helpers
open Polars.NET.Core
[<AutoOpen>]
module DataFrameFactory =
    open System.Collections.Generic

    type DataFrame with
        /// <summary> Create a DataFrame from a list of Series. </summary>
        static member create(series: seq<Series>) : DataFrame =
            let handles =
                series
                |> Seq.map (fun s -> s.Handle)
                |> Seq.toArray

            let h = PolarsWrapper.DataFrameNew handles
            new DataFrame(h)
        /// <summary> Create a DataFrame from an array of Series. </summary>
        static member create([<ParamArray>] series: Series[]) : DataFrame =
            let handles = series |> Array.map (fun s -> s.Handle)
            let h = PolarsWrapper.DataFrameNew handles
            new DataFrame(h)
        /// <summary> Create a DataFrame from a sequence of objects using Arrow streaming. </summary>
        static member ofSeq<'T>(data: seq<'T>, ?batchSize: int) : DataFrame =
            let size = defaultArg batchSize 100_000

            let schema = ArrowConverter.GetSchemaFromType<'T>()
            let batchStream =
                data
                |> Seq.chunkBySize size
                |> Seq.map ArrowFfiBridge.BuildRecordBatch

            let handle = ArrowStreamInterop.ImportEager(batchStream,schema)

            if handle.IsInvalid then
                let emptyBatch = new RecordBatch(schema, System.Array.Empty<Apache.Arrow.IArrowArray>(), 0)
                let safeHandle = ArrowFfiBridge.ImportDataFrame emptyBatch
                new DataFrame(safeHandle)
            else
                new DataFrame(handle)
        // ==========================================
        // High-Performance Record Converter
        // ==========================================

        /// <summary>
        /// Creates a DataFrame from an array or sequence of F# records.
        /// Uses Zero-Copy fast-path if all columns are supported primitives, dates, or options.
        /// Falls back to Arrow serialization for complex nested structs and lists.
        /// </summary>
        static member ofRecords<'T>(data: seq<'T>) : DataFrame =
            let props = ObjectSchemaTransposer<'T>.ModelProperties
            if props.Length = 0 then
                DataFrame.create []
            else
                if isNull data then
                    let emptyHandles = ObjectSchemaTransposer<'T>.CreateEmptySeriesHandles()
                    emptyHandles |> Array.map (fun h -> new Series(h)) |> DataFrame.create
                else
                    let records =
                        match data with
                        | :? ('T[]) as arr -> arr
                        | _ -> Seq.toArray data

                    if records.Length = 0 then
                        let emptyHandles = ObjectSchemaTransposer<'T>.CreateEmptySeriesHandles()
                        emptyHandles |> Array.map (fun h -> new Series(h)) |> DataFrame.create
                    else
                        // Dual-track compiled ingestion in Core
                        let handles = ObjectSchemaTransposer<'T>.Transpose records
                        let seriesList = handles |> Array.map (fun h -> new Series(h))
                        DataFrame.create seriesList
        /// <summary>
        /// Build DataFrame from maps
        /// </summary>
        static member ofMaps
            (
                data: seq<#IDictionary<string, obj>>,
                ?strict: bool,
                ?inferSchemaLength: uint
            ) : DataFrame =

            if isNull data then
                DataFrame.create []
            else
                let strictMode = defaultArg strict true
                let inferLen = defaultArg inferSchemaLength 100u |> int

                // Materialize to IReadOnlyList<IDictionary<string, obj>>
                let records: IReadOnlyList<IDictionary<string, obj>> =
                    match box data with
                    | :? IReadOnlyList<IDictionary<string, obj>> as list -> list
                    | _ ->
                        data
                        |> Seq.map (fun m -> m :> IDictionary<string, obj>)
                        |> Seq.toArray :> IReadOnlyList<IDictionary<string, obj>>

                if records.Count = 0 then
                    DataFrame.create []
                else
                    // Delegate schema inference and ingestion to Core DictSchemaTransposer
                    let colResults = DictSchemaTransposer.Transpose(
                        records,
                        null, // infer target keys automatically
                        strictMode,
                        Nullable inferLen
                    )

                    if colResults.Length = 0 then
                        DataFrame.create []
                    else
                        let seriesList =
                            colResults
                            |> Array.map (fun struct (_, handle) -> new Series(handle))

                        DataFrame.create seriesList
    type Series with
        /// <summary>
        /// Create Series From single column expression.
        /// </summary>
        static member ofExpr(expr:Expr) =
            let df = DataFrame.create()
            df.Select(expr).[0]
