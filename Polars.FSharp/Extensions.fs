namespace Polars.FSharp
open System.Runtime.CompilerServices

[<AutoOpen>]
module Describe =
    
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
                "count",      fun (c: string) -> pl.col(c).Count().Cast Float64
                "null_count", fun c -> pl.col(c).IsNull().Sum().Cast Float64
                "mean",       fun c -> pl.col(c).Mean()
                "std",        fun c -> pl.col(c).Std()
                "min",        fun c -> pl.col(c).Min().Cast Float64
                "25%",        fun c -> pl.col(c).Quantile 0.25
                "50%",        fun c -> pl.col(c).Median().Cast Float64 
                "75%",        fun c -> pl.col(c).Quantile 0.75
                "max",        fun c -> pl.col(c).Max().Cast Float64
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
    open System.Linq

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

    let asDataFrame (idf: IPolarsDataFrame) : DataFrame = 
        idf.AsDataFrame()

    let asLazyFrame (ilf: IPolarsLazyFrame) : LazyFrame =
        ilf.AsLazyFrame()