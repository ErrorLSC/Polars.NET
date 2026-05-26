namespace Polars.FSharp

[<AutoOpen>]
module DataFrameDescriptive =
    open Polars.NET.Core
    type DataFrame with
        /// <summary>
        /// Get a mask of all duplicated rows in this DataFrame.
        /// </summary>
        member this.IsDuplicated() = new Series(PolarsWrapper.DataFrameIsDuplicated this.Handle)
        /// <summary>
        /// Get a mask of all unique rows in this DataFrame.
        /// </summary>
        member this.IsUnique() = new Series(PolarsWrapper.DataFrameIsUnique this.Handle)
        /// <summary>
        /// True if the DataFrame contains no rows.
        /// </summary>
        member this.IsEmpty : bool = this.Height = 0L
        /// <summary>
        /// Return an estimation of the total (heap) allocated size of the DataFrame.
        /// Estimated size is given in the specified unit (bytes by default).
        /// </summary>
        /// <param name="unit">Scale the returned size to the given unit (uses 1024 base).</param>
        /// <returns>The estimated size as a double.</returns>
        member this.EstimatedSize(unit:SizeUnit) =

            let bytes = float (PolarsWrapper.DataFrameEstimatedSize this.Handle)

            match unit with
            | SizeUnit.Bytes     -> bytes
            | SizeUnit.Kilobytes -> bytes / 1024.0
            | SizeUnit.Megabytes -> bytes / 1024.0 ** 2.0
            | SizeUnit.Gigabytes -> bytes / 1024.0 ** 3.0
            | SizeUnit.Terabytes -> bytes / 1024.0 ** 4.0

        /// <summary>
        /// Returns the shape of the DataFrame as (Height, Width).
        /// </summary>
        member this.Shape = this.Height,this.Width