namespace Polars.FSharp

[<AutoOpen>]
module DataFramePartitionBy =
    open Polars.NET.Core
    type DataFrame with
        /// <summary>
        /// Group by the given columns and return the groups as separate dataframes.
        /// </summary>
        /// <param name="by">Column name(s) to group by.</param>
        /// <param name="maintainOrder">Ensure that the order of the groups is consistent with the input data. This is slower than a default partition by operation.</param>
        /// <param name="includeKey">Include the columns used to partition the DataFrame in the output.</param>
        member this.PartitionBy(byCols:seq<string>, ?maintainOrder, ?includeKey) =
            let ma = defaultArg maintainOrder true
            let inc = defaultArg includeKey true
            let byArr = byCols |> Seq.toArray
            let handles = PolarsWrapper.PartitionBy(this.Handle, byArr, ma, inc)
            
            handles |> Array.map (fun h -> new DataFrame(h))