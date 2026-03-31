namespace Polars.FSharp

open Polars.NET.Core.Arrow
open Apache.Arrow
open Polars.NET.Core
open System
open System.Reflection

type internal RecordColumnTransposer =
    static member CreateSeriesFromColumn<'Rec, 'Field>(data: 'Rec[], name: string, prop: PropertyInfo) : Series =
        // Create Fast Getter (Delegate)
        let getterMethod = prop.GetGetMethod()
        let getter = Delegate.CreateDelegate(typeof<Func<'Rec, 'Field>>, getterMethod) :?> Func<'Rec, 'Field>
        
        // Transpose: Row-Oriented -> Column-Oriented
        let len = data.Length
        let colData = Array.zeroCreate<'Field> len
        
        for i = 0 to len - 1 do
            colData.[i] <- getter.Invoke(data.[i])
            
        // Delegate to C# SeriesFactory
        Series.create(name, colData)

[<AutoOpen>]
module DataFrameFactory =

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
        static member FromColumns([<ParamArray>] series: Series[]) : DataFrame =
            DataFrame.create(series)

        /// <summary>
        /// Stream F# sequences (or C# enumerables) into Polars.
        /// </summary>
        /// <param name="data">The input sequence of objects.</param>
        /// <param name="batchSize">Optional batch size for Arrow chunks. Defaults to 100,000.</param>
        /// <param name="providedSchema">Optional Arrow Schema. Inferred via reflection if not provided.</param>
        static member ReadSeq<'T>(data: seq<'T>, ?batchSize: int, ?providedSchema: Schema) : DataFrame =

            if isNull data then 
                invalidArg "data" "Data sequence cannot be null."

            let actualBatchSize = defaultArg batchSize 100_000

            let schema = 
                match providedSchema with
                | Some s -> s
                | None -> ArrowConverter.GetSchemaFromType<'T>()

            let stream = data.ToArrowBatches actualBatchSize

            let handle = ArrowStreamInterop.ImportEager(stream, schema)

            new DataFrame(handle)
        /// <summary> Create a DataFrame from a sequence of objects using Arrow streaming. </summary>
        static member ofSeqStream<'T>(data: seq<'T>, ?batchSize: int) : DataFrame =
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
        /// Check if a type is supported by the Fast Columnar Transposition path.
        /// Primitives, Strings, Dates, and their Option/VOption variants, or Arrays with non-null primitive data types are supported.
        /// Lists, Arrays with nullable or option type, and Nested Records must fallback to Arrow.
        /// </summary>
        static member private IsSupportedFastType (t: Type) =
            // 1. Unwrap Option/VOption/Nullable
            let coreType = 
                if t.IsGenericType && (t.GetGenericTypeDefinition() = typedefof<option<_>> || t.GetGenericTypeDefinition() = typedefof<voption<_>> || t.GetGenericTypeDefinition() = typedefof<Nullable<_>>) then
                    t.GetGenericArguments().[0]
                else
                    t

            if t.IsArray then false 
            else
                if coreType.IsPrimitive then true
                else if coreType = typeof<string> then true
                else if coreType = typeof<decimal> then true
                else if coreType = typeof<DateTime> then true
                else if coreType = typeof<DateOnly> then true
                else if coreType = typeof<TimeOnly> then true
                else if coreType = typeof<TimeSpan> then true
                else if coreType = typeof<DateTimeOffset> then true
                else if coreType = typeof<Int128> then true
                else if coreType = typeof<UInt128> then true
                else false
        /// <summary>
        /// Create a DataFrame from a sequence of records.
        /// <para>
        /// Strategy:
        /// 1. Inspects types. If all are simple primitives/strings/dates, uses Fast Columnar Transposition (Zero-Arrow).
        /// 2. If any complex types (Lists, Arrays, Nested Records) are found, falls back to ArrowFfiBridge.
        /// </para>
        /// </summary>
        static member ofRecords<'T>(data: seq<'T>) : DataFrame =
            let recordType = typeof<'T>
            let props = recordType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)

            // 1. Check Eligibility for Fast Path
            // We only use Fast Path if ALL columns are supported.
            let useFastPath = 
                props 
                |> Array.forall (fun p -> DataFrame.IsSupportedFastType p.PropertyType)

            if useFastPath then
                // ==================================================
                // PATH A: High-Performance Columnar Transposition
                // ==================================================
                let records = Seq.toArray data
                
                // Helper Cache
                let helperMethodDef = 
                    typeof<RecordColumnTransposer>.GetMethod("CreateSeriesFromColumn", BindingFlags.NonPublic ||| BindingFlags.Static)

                let seriesList = 
                    props
                    |> Array.map (fun prop ->
                        let fieldType = prop.PropertyType
                        let specificHelper = helperMethodDef.MakeGenericMethod(recordType, fieldType)
                        try 
                            specificHelper.Invoke(null, [| records; prop.Name; prop |]) :?> Series
                        with ex ->
                            failwithf "Failed to create series for column '%s': %s" prop.Name ex.InnerException.Message
                    )
                DataFrame.create seriesList

            else
                // ==================================================
                // PATH B: Arrow Fallback (The Old Way)
                // Supports Lists, Structs, and complex nesting
                // ==================================================
                let batch = ArrowFfiBridge.BuildRecordBatch data
                let handle = ArrowFfiBridge.ImportDataFrame batch
                new DataFrame(handle)