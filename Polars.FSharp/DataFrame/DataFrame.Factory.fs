namespace Polars.FSharp

open Polars.NET.Core.Arrow
open Apache.Arrow
open Polars.NET.Core
open System
open System.Reflection

type IColumnBuffer =
    abstract member Add: obj -> unit
    abstract member ToSeries: string -> Series 

type private ColumnBuffer<'TCol>(capacity: int) =
    let _data = ResizeArray<'TCol>(capacity)
    
    let _colType = typeof<'TCol>
    let _underlyingType = 
        let t = Nullable.GetUnderlyingType(_colType)
        if isNull t then _colType else t

    interface IColumnBuffer with
        member _.Add(valObj: obj) =
            if isNull valObj then
                _data.Add(Unchecked.defaultof<'TCol>)
            else
                match valObj with
                | :? 'TCol as exactVal -> 
                    _data.Add(exactVal)
                | _ ->
                    let converted = Convert.ChangeType(valObj, _underlyingType)
                    _data.Add(converted :?> 'TCol)

        member _.ToSeries(name: string) =
            Series.From(name, _data.ToArray())

module ColumnBufferFactory =

    let create (propType: Type) (capacity: int) : IColumnBuffer =
        let targetType = 
            if propType.IsValueType && isNull (Nullable.GetUnderlyingType propType) then
                typedefof<Nullable<_>>.MakeGenericType [| propType |]
            else
                propType

        let bufferType = typedefof<ColumnBuffer<_>>.MakeGenericType [| targetType |]
        
        Activator.CreateInstance(bufferType, [| box capacity |]) :?> IColumnBuffer

module TypeInference =
    
    let private isNumeric (t: Type) =
        t = typeof<int> || t = typeof<float> || t = typeof<double> || 
        t = typeof<decimal> || t = typeof<int64> || t = typeof<int16> || 
        t = typeof<byte>

    let rec promoteType (typeA: Type) (typeB: Type) : Type =
        if typeA = typeB then typeA
        else
            let baseA = defaultArg (Option.ofObj (Nullable.GetUnderlyingType(typeA))) typeA
            let baseB = defaultArg (Option.ofObj (Nullable.GetUnderlyingType(typeB))) typeB

            if baseA = baseB then baseA
            elif isNumeric baseA && isNumeric baseB then
                if baseA = typeof<double> || baseB = typeof<double> then typeof<double>
                elif baseA = typeof<float> || baseB = typeof<float> then typeof<double>
                elif baseA = typeof<decimal> || baseB = typeof<decimal> then typeof<decimal>
                elif baseA = typeof<int64> || baseB = typeof<int64> then typeof<int64>
                else typeof<int64> 
            else
                typeof<string>

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
        /// <summary>
        /// Build DataFrame from maps
        /// </summary>
        static member ofMaps
            (
                data: seq<Map<string, obj>>, 
                ?strict: bool, 
                ?inferSchemaLength: uint
            ) : DataFrame =

            let strictMode = defaultArg strict true
            let inferLen = defaultArg inferSchemaLength 100u |> int
            
            let records = Seq.toArray data
            
            if records.Length = 0 then 
                DataFrame.create() 
            else
                let columnTypes = Dictionary<string, Type>()

                // ==========================================
                // Phase 1: Schema Inference 
                // ==========================================
                let rowsToInfer = min inferLen records.Length
                
                for i = 0 to rowsToInfer - 1 do
                    let row = records.[i]
                    for kvp in row do
                        let colName = kvp.Key
                        let valObj = kvp.Value
                        
                        if not (isNull valObj) then
                            let newType = valObj.GetType()
                            
                            match columnTypes.TryGetValue(colName) with
                            | true, existingType when existingType <> newType ->
                                columnTypes.[colName] <- TypeInference.promoteType existingType newType
                            | false, _ ->
                                columnTypes.[colName] <- newType
                            | _ -> () 

                let finalSchema =
                    columnTypes
                    |> Seq.map (fun kvp -> 
                        let t = if isNull kvp.Value then typeof<string> else kvp.Value
                        kvp.Key, t)
                    |> dict

                // ==========================================
                // Phase 2: Buffer Loading 
                // ==========================================
                let buffers = Dictionary<string, IColumnBuffer>()
                for kvp in finalSchema do
                    buffers.[kvp.Key] <- ColumnBufferFactory.create kvp.Value records.Length 

                for row in records do
                    for colName in finalSchema.Keys do
                        match Map.tryFind colName row with
                        | Some valObj when not (isNull valObj) ->
                            try
                                buffers.[colName].Add(valObj)
                            with
                            | :? InvalidCastException as ex ->
                                if strictMode then 
                                    failwithf "Strict mode error on column '%s': %s" colName ex.Message
                                else 
                                    buffers.[colName].Add(null)
                        | _ -> 
                            buffers.[colName].Add(null)

                // ==========================================
                // Phase 3: Assembly
                // ==========================================
                let seriesList = 
                    finalSchema.Keys
                    |> Seq.map (fun key -> buffers.[key].ToSeries(key))
                    |> Seq.toArray

                DataFrame.create seriesList
    type Series with
        /// <summary>
        /// Create Series From single column expression.
        /// </summary>
        static member ofExpr(expr:Expr) =
            let df = DataFrame.create()
            df.Select(expr).[0]