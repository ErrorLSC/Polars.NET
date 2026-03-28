namespace Polars.FSharp
open System.Runtime.CompilerServices

[<AutoOpen>]
module Describe =
    open System.Text
    
    type DataFrame with
        /// <summary>
        /// Generate a summary statistics DataFrame (count, mean, std, min, 25%, 50%, 75%, max).
        /// Similar to pandas/polars describe().
        /// </summary>
        member this.Describe() : DataFrame =
            use schema = this.Schema
            
            let numericCols = 
                schema.ToMap()
                |> Map.filter (fun _ dtype -> dtype.IsNumeric)
                |> Map.keys
                |> Seq.toList

            if numericCols.IsEmpty then
                failwith "No numeric columns to describe."

            let metrics = [
                "count",      fun (c: string) -> Expr.Col(c).Count().Cast Float64
                "null_count", fun c -> Expr.Col(c).IsNull().Sum().Cast Float64
                "mean",       fun c -> Expr.Col(c).Mean()
                "std",        fun c -> Expr.Col(c).Std()
                "min",        fun c -> Expr.Col(c).Min().Cast Float64
                "25%",        fun c -> Expr.Col(c).Quantile 0.25
                "50%",        fun c -> Expr.Col(c).Median().Cast Float64 
                "75%",        fun c -> Expr.Col(c).Quantile 0.75
                "max",        fun c -> Expr.Col(c).Max().Cast Float64
            ]

            let rowFrames = 
                metrics 
                |> List.map (fun (statName, op) ->
                    let exprs = 
                        [ pl.lit(statName).Alias "statistic" ] @
                        (numericCols |> List.map (fun c -> op c))
                    
                    this |> pl.select exprs
                )

            pl.concat rowFrames

        /// <summary>
        /// Return a dense preview of the DataFrame as a formatted string.
        /// </summary>
        member this.GlimpseString(?maxItemsPerColumn:int,?maxColnameLength: int ) =
            let nRows = this.Height
            let nCols = this.Width
            let itemLimit = defaultArg maxItemsPerColumn 10
            let nameLimit = defaultArg maxColnameLength 50
            let limit = int (min (int64 itemLimit) nRows)

            let schema = this.Schema
            let cols = schema.ToList()

            use headDf = this.Head limit
            use strDf = headDf.Select [| Expr.All().Cast DataType.String |]

            let mutable maxNameLen = 0
            let mutable maxDtypeLen = 0

            let rowInfos = 
                cols |> List.mapi (fun colIdx (colName, dtype) ->
                    
                    let displayColName = 
                        if colName.Length > nameLimit then
                            colName.Substring(0, nameLimit - 1) + "…"
                        else
                            colName
                    
                    maxNameLen <- max maxNameLen displayColName.Length

                    let dtypeStr = sprintf "<%s>" (dtype.ToString())
                    maxDtypeLen <- max maxDtypeLen dtypeStr.Length

                    use strSeries = strDf.Column colName 
                    
                    let valStrs =
                        [ for rowIdx in 0 .. (limit - 1) do
                            if strSeries.IsNullAt rowIdx then
                                yield "null"
                            else
                                let s = strSeries.GetValue<string> rowIdx
                                if dtype = DataType.String then
                                    yield sprintf "'%s'" s
                                else
                                    yield s
                        ]

                    displayColName, dtypeStr, System.String.Join(", ", valStrs)
                )

            let sb = StringBuilder()
            sb.AppendLine(sprintf "Rows: %d" nRows) |> ignore
            sb.AppendLine(sprintf "Columns: %d" nCols) |> ignore

            for name, dtypeStr, values in rowInfos do
                let paddedName = name.PadRight maxNameLen
                let paddedDtype = dtypeStr.PadLeft maxDtypeLen
                sb.AppendLine(sprintf "$ %s %s %s" paddedName paddedDtype values) |> ignore

            sb.ToString()

        /// <summary>
        /// Print a dense preview of the DataFrame to the standard output.
        /// </summary>
        member this.Glimpse(?maxItemsPerColumn:int,?maxColnameLength: int )  =
            let itemLimit = defaultArg maxItemsPerColumn 10
            let nameLimit = defaultArg maxColnameLength 50
            printf "%s" (this.GlimpseString(itemLimit, nameLimit))

        /// <summary>
        /// Return a dense preview of the DataFrame as a new DataFrame.
        /// Schema: "column" (String), "dtype" (String), "values" (List[String])
        /// </summary>
        member this.GlimpseFrame(?maxItemsPerColumn: int, ?maxColnameLength: int) =
            let itemLimit = defaultArg maxItemsPerColumn 10
            let nameLimit = defaultArg maxColnameLength 50
            let nRows = this.Height
            let limit = int (min (int64 itemLimit) nRows)

            let schema = this.Schema
            let cols = schema.ToList()

            use headDf = this.Head limit

            use strDf = headDf.Select [| Expr.All().Cast DataType.String |]

            let outColNames, outDtypes, outValues =
                cols 
                |> List.map (fun (colName, dtype) ->
                    
                    let displayColName = 
                        if colName.Length > nameLimit then
                            colName.Substring(0, nameLimit - 1) + "…"
                        else
                            colName
                    
                    let dtypeStr = dtype.ToString()

                    use strSeries = strDf.Column colName 
                    
                    let valArray =
                        [| for rowIdx in 0 .. (limit - 1) do
                            if strSeries.IsNullAt rowIdx then
                                yield "null"
                            else
                                let s = strSeries.GetValue<string> rowIdx
                                if dtype = DataType.String then
                                    yield sprintf "'%s'" s
                                else
                                    yield s
                        |]

                    displayColName, dtypeStr, valArray
                )
                |> List.unzip3 

            let s1 = Series.create("column", outColNames |> Array.ofList)
            let s2 = Series.create("dtype", outDtypes |> Array.ofList)
            let s3 = Series.create("values", outValues |> Array.ofList)  

            DataFrame.create(s1, s2, s3)
            
    type LazyFrame with
        /// <summary>
        /// Generate a summary statistics DataFrame (count, mean, std, min, 25%, 50%, 75%, max).
        /// Similar to pandas/polars describe().
        /// Notice: This will collect LazyFrame once, but LazyFrame won't be consumed.
        /// </summary>
        member this.Describe(): DataFrame =
            this.Clone().Collect().Describe()

    

[<Extension>]
type LazyFrameDeltaExtensions =
    /// <summary>
    /// Starts a fluent builder to merge a LazyFrame into a Delta Lake table with strict, order-preserving SQL MERGE semantics.
    /// <para>
    /// Unlike traditional merge methods, this builder guarantees that chained actions (Update, Delete, Insert) 
    /// are evaluated exactly in the order they are defined. If no actions are specified before execution, 
    /// it intelligently defaults to a standard Upsert (WhenMatchedUpdate + WhenNotMatchedInsert).
    /// </para>
    /// </summary>
    /// <param name="path">The URI to the target Delta Lake table (local or cloud).</param>
    /// <param name="mergeKeys">The column names to join on (must exist in both the Source DataFrame and Target Delta table).</param>
    /// <param name="canEvolve">If set to true, allows schema evolution (e.g., adding new columns from the Source to the Target). Default is false.</param>
    /// <param name="cloudOptions">Cloud storage credentials and configuration (e.g., AWS S3, Azure Blob).</param>
    /// <returns>A <see cref="DeltaMergeBuilder"/> instance used to chain match conditions, culminating in a call to <c>.Execute()</c>.</returns>
    [<Extension>]
    static member MergeDeltaOrdered(
        this: LazyFrame,
        path: string,
        mergeKeys: seq<string>,
        ?canEvolve: bool,
        ?cloudOptions: CloudOptions
    ) : DeltaMergeBuilder =
        let keysArr = mergeKeys |> Seq.toArray
        let evolve = defaultArg canEvolve false
        new DeltaMergeBuilder(this, path, keysArr, evolve, cloudOptions)

[<Extension>]
type DataFrameDeltaExtensions =
    /// <summary>
    /// Starts a fluent builder to merge a DataFrame into a Delta Lake table with strict, order-preserving SQL MERGE semantics.
    /// <para>
    /// Unlike traditional merge methods, this builder guarantees that chained actions (Update, Delete, Insert) 
    /// are evaluated exactly in the order they are defined. If no actions are specified before execution, 
    /// it intelligently defaults to a standard Upsert (WhenMatchedUpdate + WhenNotMatchedInsert).
    /// </para>
    /// </summary>
    /// <param name="path">The URI to the target Delta Lake table (local or cloud).</param>
    /// <param name="mergeKeys">The column names to join on (must exist in both the Source DataFrame and Target Delta table).</param>
    /// <param name="canEvolve">If set to true, allows schema evolution (e.g., adding new columns from the Source to the Target). Default is false.</param>
    /// <param name="cloudOptions">Cloud storage credentials and configuration (e.g., AWS S3, Azure Blob).</param>
    /// <returns>A <see cref="DeltaMergeBuilder"/> instance used to chain match conditions, culminating in a call to <c>.Execute()</c>.</returns>
    [<Extension>]
    static member MergeDeltaOrdered(
        this: DataFrame,
        path: string,
        mergeKeys: seq<string>,
        ?canEvolve: bool,
        ?cloudOptions: CloudOptions
    ) : DeltaMergeBuilder =
        let keysArr = mergeKeys |> Seq.toArray
        let evolve = defaultArg canEvolve false
        new DeltaMergeBuilder(this.Lazy(), path, keysArr, evolve, cloudOptions)

[<AutoOpen>]
module InterfaceUnwrapperExtensions =
    open Polars.NET.Core
    open System

    type IPolarsDataFrame with
        
        /// <summary>
        /// Unwrap IPolarsDataFrame as DataFrame 
        /// </summary>
        member this.AsDataFrame() : DataFrame =
            match this with
            | :? DataFrame as df -> df
            | _ -> raise (InvalidCastException "Not Standard Polars DataFrame")

    type IPolarsLazyFrame with

        /// <summary>
        /// Unwrap IPolarsLazyFrame as LazyFrame 
        /// </summary>
        member this.AsLazyFrame() : LazyFrame =
            match this with
            | :? LazyFrame as lf -> lf
            | _ -> raise (InvalidCastException "Not Standard Polars LazyFrame")

    type IPolarsSeries with
        member this.AsSeries() : Series = 
            match this with
            | :? Series as ips -> ips
            | _ -> raise (InvalidCastException "Not Standard Polars Series")
    let asDataFrame (idf: IPolarsDataFrame) : DataFrame = 
        idf.AsDataFrame()

    let asLazyFrame (ilf: IPolarsLazyFrame) : LazyFrame =
        ilf.AsLazyFrame()
    
    let asSeries (ips: IPolarsSeries) : Series =
        ips.AsSeries()

[<AutoOpen>]
module UnityCatalogExtensions =

    type LazyFrame with
        /// <summary>
        /// Starts building a Merge (Upsert) operation using this <see cref="LazyFrame"/> as the source.
        /// </summary>
        member this.MergeCatalogRecords(
            catalog: UnityCatalog,
            catalogName: string,
            schemaName: string,
            tableName: string,
            mergeKeys: string array,
            ?canEvolve: bool,
            ?cloudOptions: CloudOptions) =
            
            catalog.MergeCatalogRecords(
                catalogName, 
                schemaName, 
                tableName, 
                this, 
                mergeKeys, 
                ?canEvolve = canEvolve, 
                ?cloudOptions = cloudOptions
            )

    type DataFrame with
        /// <summary>
        /// Starts building a Merge (Upsert) operation using this <see cref="DataFrame"/> as the source.
        /// </summary>
        member this.MergeCatalogRecords(
            catalog: UnityCatalog,
            catalogName: string,
            schemaName: string,
            tableName: string,
            mergeKeys: string array,
            ?canEvolve: bool,
            ?cloudOptions: CloudOptions) =
            
            catalog.MergeCatalogRecords(
                catalogName, 
                schemaName, 
                tableName, 
                this, 
                mergeKeys, 
                ?canEvolve = canEvolve, 
                ?cloudOptions = cloudOptions
            )

    type LazyFrame with
        /// <summary>
        /// Sinks the <see cref="LazyFrame"/> to a Unity Catalog table.
        /// </summary>
        member this.SinkCatalogTable(
            catalog: UnityCatalog,
            catalogName: string,
            schemaName: string,
            tableName: string,
            ?partitionBy: Selector,
            ?mode: DeltaSaveMode,
            ?canEvolve: bool,
            ?includeKeys: bool,
            ?keysPreGrouped: bool,
            ?maxRowsPerFile: int,
            ?approxBytesPerFile: int64,
            ?compression: ParquetCompression,
            ?compressionLevel: int,
            ?statistics: bool,
            ?rowGroupSize: uint32,
            ?dataPageSize: uint32,
            ?compatLevel: int,
            ?maintainOrder: bool,
            ?syncOnClose: SyncOnClose,
            ?mkdir: bool,
            ?cloudOptions: CloudOptions) =
            
            catalog.SinkCatalogTable(
                catalogName, 
                schemaName, 
                tableName, 
                this,
                ?partitionBy = partitionBy,
                ?mode = mode,
                ?canEvolve = canEvolve,
                ?includeKeys = includeKeys,
                ?keysPreGrouped = keysPreGrouped,
                ?maxRowsPerFile = maxRowsPerFile,
                ?approxBytesPerFile = approxBytesPerFile,
                ?compression = compression,
                ?compressionLevel = compressionLevel,
                ?statistics = statistics,
                ?rowGroupSize = rowGroupSize,
                ?dataPageSize = dataPageSize,
                ?compatLevel = compatLevel,
                ?maintainOrder = maintainOrder,
                ?syncOnClose = syncOnClose,
                ?mkdir = mkdir,
                ?cloudOptions = cloudOptions
            )

    type DataFrame with
        /// <summary>
        /// Writes the <see cref="DataFrame"/> into a Unity Catalog table by converting it to a <see cref="LazyFrame"/>.
        /// </summary>
        member this.WriteCatalogTable(
            catalog: UnityCatalog,
            catalogName: string,
            schemaName: string,
            tableName: string,
            ?partitionBy: Selector,
            ?mode: DeltaSaveMode,
            ?canEvolve: bool,
            ?includeKeys: bool,
            ?keysPreGrouped: bool,
            ?maxRowsPerFile: int,
            ?approxBytesPerFile: int64,
            ?compression: ParquetCompression,
            ?compressionLevel: int,
            ?statistics: bool,
            ?rowGroupSize: uint32,
            ?dataPageSize: uint32,
            ?compatLevel: int,
            ?maintainOrder: bool,
            ?syncOnClose: SyncOnClose,
            ?mkdir: bool,
            ?cloudOptions: CloudOptions) =
            
            catalog.SinkCatalogTable(
                catalogName, 
                schemaName, 
                tableName, 
                this.Lazy(),
                ?partitionBy = partitionBy,
                ?mode = mode,
                ?canEvolve = canEvolve,
                ?includeKeys = includeKeys,
                ?keysPreGrouped = keysPreGrouped,
                ?maxRowsPerFile = maxRowsPerFile,
                ?approxBytesPerFile = approxBytesPerFile,
                ?compression = compression,
                ?compressionLevel = compressionLevel,
                ?statistics = statistics,
                ?rowGroupSize = rowGroupSize,
                ?dataPageSize = dataPageSize,
                ?compatLevel = compatLevel,
                ?maintainOrder = maintainOrder,
                ?syncOnClose = syncOnClose,
                ?mkdir = mkdir,
                ?cloudOptions = cloudOptions
            )