namespace Polars.FSharp

open Polars.NET.Core.Arrow
open Apache.Arrow
open Polars.NET.Core
open System
open System.Reflection
open Polars.NET.Core.Helpers

/// <summary>
/// Internal abstraction for transposing row-oriented map values into columnar Series.
/// </summary>
type IColumnBuffer =
    abstract member Add: obj -> unit
    abstract member ToSeries: string -> Series

/// <summary>
/// Strongly-typed columnar buffer for F# row ingestion.
/// </summary>
type private ColumnBuffer<'TCol>(capacity: int) =
    let _data = ResizeArray<'TCol>(capacity)
    let _colType = typeof<'TCol>
    let _underlyingType = PolarsTypeHelper.UnwrapCoreType(_colType)

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
            // Directly calls the existing generic Series.create<'TCol>(string, seq<'TCol>) without reflection!
            Series.create(name, _data.ToArray())

module internal ColumnBufferFactory =

    let create (propType: Type) (capacity: int) : IColumnBuffer =
        let targetType =
            if propType.IsValueType && isNull (Nullable.GetUnderlyingType propType) then
                typedefof<Nullable<_>>.MakeGenericType [| propType |]
            else
                propType

        let bufferType = typedefof<ColumnBuffer<_>>.MakeGenericType [| targetType |]
        Activator.CreateInstance(bufferType, [| box capacity |]) :?> IColumnBuffer

/// <summary>
/// High-performance typed transposer for converting arrays of F# records into Polars Series.
/// Pre-computes compiled getter delegates to eliminate reflection in hot loops.
/// </summary>
type internal RecordColumnTransposer =
    static member CreateSeriesFromColumn<'Rec, 'Field>(data: 'Rec[], name: string, prop: PropertyInfo) : Series =
        let getterMethod = prop.GetGetMethod(true)
        let getter = Delegate.CreateDelegate(typeof<Func<'Rec, 'Field>>, getterMethod) :?> Func<'Rec, 'Field>

        let len = data.Length
        let colData = Array.zeroCreate<'Field> len

        for i = 0 to len - 1 do
            colData.[i] <- getter.Invoke(data.[i])

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

            if props.Length = 0 then
                DataFrame.create()
            else
                let records =
                    if isNull data then Array.empty<'T>
                    else
                        match data with
                        | :? ('T[]) as arr -> arr
                        | _ -> Seq.toArray data

                // Handle Empty Record Sequence: Create empty typed Series for each property
                if records.Length = 0 then
                    let seriesFromMethod =
                        typeof<Series>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                        |> Array.find (fun m ->
                            m.Name = "From" &&
                            m.IsGenericMethodDefinition &&
                            m.GetParameters().Length = 2 &&
                            m.GetParameters().[0].ParameterType = typeof<string> &&
                            m.GetParameters().[1].ParameterType.IsGenericType &&
                            m.GetParameters().[1].ParameterType.GetGenericTypeDefinition() = typedefof<seq<_>>
                        )

                    let emptySeriesList =
                        props
                        |> Array.map (fun prop ->
                            let emptyArr = Array.CreateInstance(prop.PropertyType, 0)
                            let specializedFrom = seriesFromMethod.MakeGenericMethod([| prop.PropertyType |])
                            specializedFrom.Invoke(null, [| box prop.Name; box emptyArr |]) :?> Series
                        )

                    DataFrame.create emptySeriesList
                else
                    // Check eligibility for fast zero-copy path
                    let useFastPath =
                        props |> Array.forall (fun p -> PolarsTypeHelper.IsSupportedSimpleType(p.PropertyType))

                    if useFastPath then
                        // PATH A: Zero-Copy compiled getter columnar transposition
                        let helperMethodDef =
                            typeof<RecordColumnTransposer>.GetMethod("CreateSeriesFromColumn", BindingFlags.NonPublic ||| BindingFlags.Static)

                        let seriesList =
                            props
                            |> Array.map (fun prop ->
                                let fieldType = prop.PropertyType
                                let specificHelper = helperMethodDef.MakeGenericMethod(recordType, fieldType)
                                specificHelper.Invoke(null, [| box records; box prop.Name; box prop |]) :?> Series
                            )
                        DataFrame.create seriesList
                    else
                        // PATH B: Arrow Fallback for nested structs / lists
                        let batch = ArrowFfiBridge.BuildRecordBatch records
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

            if isNull data then DataFrame.create()
            else
                let records = Seq.toArray data
                if records.Length = 0 then DataFrame.create()
                else
                    let strictMode = defaultArg strict true
                    let inferLen = defaultArg inferSchemaLength 100u |> int
                    let rowsToInfer = min inferLen records.Length

                    // ==========================================
                    // Phase 1: Robust Schema Inference
                    // ==========================================
                    let columnTypes = Dictionary<string, Type>()

                    // 1. Collect all unique column keys safely
                    for r = 0 to records.Length - 1 do
                        match records.[r] with
                        | m when not (Map.isEmpty m) ->
                            for KeyValue(k, _) in m do
                                if not (columnTypes.ContainsKey k) then
                                    columnTypes.[k] <- typeof<obj> // Safe initial fallback
                        | _ -> ()

                    // 2. Infer concrete types up to rowsToInfer
                    for i = 0 to rowsToInfer - 1 do
                        let row = records.[i]
                        for KeyValue(colName, valObj) in row do
                            if not (isNull valObj) then
                                let newType = valObj.GetType()
                                let existingType = columnTypes.[colName]
                                if existingType = typeof<obj> || isNull existingType then
                                    columnTypes.[colName] <- newType
                                elif existingType <> newType then
                                    columnTypes.[colName] <- PolarsTypeHelper.PromoteType(existingType, newType)

                    // 3. Ensure no null types remain
                    for KeyValue(colName, t) in columnTypes do
                        if isNull t then
                            columnTypes.[colName] <- typeof<string>

                    let colNames = columnTypes.Keys |> Seq.toArray
                    let rowCount = records.Length

                    // ==========================================
                    // Phase 2: Transposition & Native Series Construction
                    // ==========================================
                    let seriesFromMethod =
                        typeof<Series>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                        |> Array.find (fun m ->
                            m.Name = "From" &&
                            m.IsGenericMethodDefinition &&
                            m.GetParameters().Length = 2 &&
                            m.GetParameters().[0].ParameterType = typeof<string> &&
                            m.GetParameters().[1].ParameterType.IsGenericType &&
                            m.GetParameters().[1].ParameterType.GetGenericTypeDefinition() = typedefof<seq<_>>
                        )

                    let seriesList =
                        colNames
                        |> Array.map (fun colName ->
                            let rawType = columnTypes.[colName]

                            // Ensure value types are wrapped into Nullable<T> so nulls generate a proper Validity Bitmap
                            let targetType =
                                if rawType.IsValueType && isNull (Nullable.GetUnderlyingType rawType) then
                                    typedefof<Nullable<_>>.MakeGenericType [| rawType |]
                                else
                                    rawType

                            let values = Array.CreateInstance(targetType, rowCount)
                            let coreType = PolarsTypeHelper.UnwrapCoreType(targetType)

                            for r = 0 to rowCount - 1 do
                                match Map.tryFind colName records.[r] with
                                | Some v when not (isNull v) ->
                                    try
                                        let converted = Convert.ChangeType(v, coreType)
                                        // Activator handles boxing into Nullable<T> with HasValue = true
                                        let boxedVal =
                                            if targetType <> coreType then
                                                Activator.CreateInstance(targetType, [| converted |])
                                            else
                                                converted
                                        values.SetValue(boxedVal, r)
                                    with ex ->
                                        if strictMode then
                                            failwithf "Strict mode error on column '%s' at row %d: %s" colName r ex.Message
                                        else
                                            values.SetValue(null, r)
                                | _ ->
                                    values.SetValue(null, r)

                            // Make closed generic method: Series.From<Nullable<T>>(colName, values)
                            let specializedFrom = seriesFromMethod.MakeGenericMethod([| targetType |])
                            specializedFrom.Invoke(null, [| box colName; box values |]) :?> Series
                        )

                    DataFrame.create seriesList
    type Series with
        /// <summary>
        /// Create Series From single column expression.
        /// </summary>
        static member ofExpr(expr:Expr) =
            let df = DataFrame.create()
            df.Select(expr).[0]
