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
        /// <summary>
        /// Creates a DataFrame from an array or sequence of F# records.
        /// Uses Zero-Copy fast-path if all columns are supported primitives, dates, or options.
        /// Falls back to Arrow serialization for complex nested structs and lists.
        /// </summary>
        static member ofRecords<'T>(data: seq<'T>) : DataFrame =
            if isNull data then DataFrame.create []
                else
                    let handle = DataFrameBuilder.FromRows(data)
                    new DataFrame(handle)
        /// <summary>
        /// Creates a DataFrame from an F# anonymous record (or any SoA container) where fields are collections.
        /// Example: DataFrame.ofColumns {| Time = [| dt1; dt2 |]; Val = [| 1.0; 2.0 |] |}
        /// </summary>
        static member ofColumns<'T when 'T : not struct>(columns: 'T) : DataFrame =
            if obj.ReferenceEquals(columns, null) then DataFrame.create []
            else
                let handle = DataFrameBuilder.FromColumns<'T> columns
                new DataFrame(handle)

        /// <summary>
        /// Creates a DataFrame from a sequence of column pairs (Name * Array).
        /// Example: DataFrame.ofColumns [ "A", box [| 1; 2 |]; "B", box [| "x"; "y" |] ]
        /// </summary>
        static member ofColumns(columns: seq<string * Array>) : DataFrame =
            if isNull columns then DataFrame.create []
            else
                let mapped =
                    columns
                    |> Seq.map (fun (name, arr) -> ValueTuple<string, Array>(name, arr))

                let handle = DataFrameBuilder.FromColumns(mapped)
                new DataFrame(handle)
        /// <summary>
        /// Build a DataFrame from a sequence of Maps or dictionaries.
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

                // Upcast to standard sequence of IDictionary<string, obj?>
                let dictSeq = data |> Seq.map (fun m -> m :> IDictionary<string, obj>)
                
                let handle = DataFrameBuilder.FromDicts(
                    dictSeq,
                    null,
                    strictMode,
                    Nullable inferLen
                )
                new DataFrame(handle)
            
    type Series with
        /// <summary>
        /// Create Series From single column expression.
        /// </summary>
        static member ofExpr(expr:Expr) =
            let df = DataFrame.create()
            df.Select(expr).[0]
