namespace Polars.FSharp

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
                "count",      fun (c: string) -> Expr.Col(c).Count().Cast<float>()
                "null_count", fun c -> Expr.Col(c).IsNull().Sum().Cast<float>()
                "mean",       fun c -> Expr.Col(c).Mean()
                "std",        fun c -> Expr.Col(c).Std()
                "min",        fun c -> Expr.Col(c).Min().Cast<float>()
                "25%",        fun c -> Expr.Col(c).Quantile 0.25
                "50%",        fun c -> Expr.Col(c).Median().Cast<float>()
                "75%",        fun c -> Expr.Col(c).Quantile 0.75
                "max",        fun c -> Expr.Col(c).Max().Cast<float>()
            ]
            let metricsSeq = metrics |> List.toSeq

            let rowFrames = 
                metricsSeq 
                |> Seq.map (fun (statName, op) ->
                    let exprs = 
                        [ pl.lit(statName).Alias "statistic" ] |> Seq.append (numericCols |> Seq.map (fun c -> op c))
                    
                    this |> pl.select exprs
                )

            pl.concat rowFrames

        /// <summary>
        /// Return a dense preview of the DataFrame as a formatted string.
        /// </summary>
        member this.GlimpseString(?maxItemsPerColumn: int, ?maxColnameLength: int) =
            let nRows = this.Height
            let nCols = this.Width
            let itemLimit = defaultArg maxItemsPerColumn 10
            let nameLimit = defaultArg maxColnameLength 50
            let limit = int (min (int64 itemLimit) nRows)

            use headDf = this.Head limit
            use strDf = headDf.Select [| Expr.All().Cast<string>() |]

            let rawRowInfos = 
                this.Schema.GetFields()
                |> Seq.map (fun (colName, dtype) ->
                    let displayColName = 
                        if colName.Length > nameLimit then colName.Substring(0, nameLimit - 1) + "…"
                        else colName
                    
                    let dtypeStr = $"<{dtype}>"
                    
                    use strSeries = strDf.Column colName
                    let valStrs = 
                        Array.init limit (fun rowIdx ->
                            if strSeries.IsNullAt rowIdx then "null"
                            else
                                let s = strSeries.GetValue<string> rowIdx
                                if dtype = DataType.String then $"'{s}'" else s
                        )
                    
                    displayColName, dtypeStr, String.concat ", " valStrs
                )
                |> Seq.toList 

            let maxNameLen = rawRowInfos |> List.map (fun (name, _, _) -> name.Length) |> function [] -> 0 | xs -> List.max xs
            let maxDtypeLen = rawRowInfos |> List.map (fun (_, dtypeStr, _) -> dtypeStr.Length) |> function [] -> 0 | xs -> List.max xs

            let outputLines = [
                yield $"Rows: {nRows}"
                yield $"Columns: {nCols}"
                
                for name, dtypeStr, values in rawRowInfos do
                    let paddedName = name.PadRight maxNameLen
                    let paddedDtype = dtypeStr.PadLeft maxDtypeLen
                    yield $"$ {paddedName} {paddedDtype} {values}"
            ]

            let sb = StringBuilder()
            outputLines |> List.iter (fun line -> sb.AppendLine line |> ignore)
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

            use strDf = headDf.Select [| Expr.All().Cast<string>(false) |]

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
    type Series with
        member this.Describe() = 
            this.ToFrame().Describe()

[<AutoOpen>]
module InterfaceUnwrapperExtensions =
    open Polars.NET.Core
    open System

    type IPolarsDataFrame with
        
        /// <summary>
        /// Unwrap IPolarsDataFrame as DataFrame 
        /// </summary>
        member internal this.AsDataFrame() : DataFrame =
            match this with
            | :? DataFrame as df -> df
            | _ -> raise (InvalidCastException "Not Standard Polars DataFrame")

    type IPolarsLazyFrame with

        /// <summary>
        /// Unwrap IPolarsLazyFrame as LazyFrame 
        /// </summary>
        member internal this.AsLazyFrame() : LazyFrame =
            match this with
            | :? LazyFrame as lf -> lf
            | _ -> raise (InvalidCastException "Not Standard Polars LazyFrame")

    type IPolarsSeries with
        member internal this.AsSeries() : Series = 
            match this with
            | :? Series as ips -> ips
            | _ -> raise (InvalidCastException "Not Standard Polars Series")
    let internal asDataFrame (idf: IPolarsDataFrame) : DataFrame = 
        idf.AsDataFrame()

    let internal asLazyFrame (ilf: IPolarsLazyFrame) : LazyFrame =
        ilf.AsLazyFrame()
    
    let internal asSeries (ips: IPolarsSeries) : Series =
        ips.AsSeries()

[<AutoOpen>]
module CategoriesExtensions =
    open Polars.NET.Core
    type FrozenCategories with
        member this.GetCategories() =
            let seriesHandle = PolarsWrapper.FrozenCategoriesGetCategories(this.Handle)
            let series = new Series(seriesHandle)
            series.ToArray<string>()