namespace Polars.FSharp

open Polars.NET.Core

[<AutoOpen>]
module SeriesDescriptiveOps = 
    type Series with
        /// <summary>
        /// Get the length of each individual chunk.
        /// </summary>
        /// <returns>An array of lengths as primitive int64 values.</returns>
        member this.ChunkLengths() =
            let nativeLengths = PolarsWrapper.SeriesChunkLengths(this.Handle)
            
            nativeLengths |> Array.map int64
        /// <summary>
        /// Return an estimation of the total (heap) allocated size of the Series.
        /// Estimated size is given in the specified unit (bytes by default).
        /// </summary>
        /// <param name="unit">Scale the returned size to the given unit (uses 1024 base).</param>
        /// <returns>The estimated size as a double.</returns>
        member this.EstimatedSize(unit:SizeUnit) =

            let bytes = float (PolarsWrapper.SeriesEstimatedSize this.Handle)

            match unit with
            | SizeUnit.Bytes     -> bytes
            | SizeUnit.Kilobytes -> bytes / 1024.0
            | SizeUnit.Megabytes -> bytes / 1024.0 ** 2.0
            | SizeUnit.Gigabytes -> bytes / 1024.0 ** 3.0
            | SizeUnit.Terabytes -> bytes / 1024.0 ** 4.0
        /// <summary>
        /// Check whether the Series contains one or more null values.
        /// </summary>
        member this.HasNulls() = PolarsWrapper.SeriesHasNulls(this.Handle)
        /// <summary> Check if floating point values are NaN. </summary>
        member this.IsNan() = new Series(PolarsWrapper.SeriesIsNan this.Handle)

        /// <summary> Check if floating point values are not NaN. </summary>
        member this.IsNotNan() = new Series(PolarsWrapper.SeriesIsNotNan this.Handle)

        /// <summary> Check if floating point values are finite (not NaN and not Inf). </summary>
        member this.IsFinite() = new Series(PolarsWrapper.SeriesIsFinite this.Handle)

        /// <summary> Check if floating point values are infinite. </summary>
        member this.IsInfinite() = new Series(PolarsWrapper.SeriesIsInfinite this.Handle)
        /// <summary>
        /// Get a boolean mask indicating which values are unique.
        /// </summary>
        member this.IsUnique() = new Series(PolarsWrapper.SeriesIsUnique this.Handle)
        /// <summary>
        /// Get a boolean mask indicating which values are duplicated.
        /// </summary>
        member this.IsDuplicated() = new Series(PolarsWrapper.SeriesIsDuplicated this.Handle)
        /// <summary>
        /// Return a boolean mask indicating the first occurrence of each distinct value.
        /// </summary>
        member this.IsFirstDistinct() = new Series(PolarsWrapper.SeriesIsFirstDistinct this.Handle)
        /// <summary>
        /// Return a boolean mask indicating the last occurrence of each distinct value.
        /// </summary>
        member this.IsLastDistinct() = new Series(PolarsWrapper.SeriesIsLastDistinct this.Handle)
        /// <summary>
        /// Check if elements of this Series are in the other Series.
        /// </summary>
        member this.IsIn(other: Series, ?nullsEqual: bool) : Series =
            let nEq = defaultArg nullsEqual false
            
            match other.DataType.ToPlDataType() with
            | PlDataType.List 
            | PlDataType.Array ->
                new Series(PolarsWrapper.SeriesIsIn(this.Handle, other.Handle, nEq))
            | _ ->
                use implodedOther = other.Implode()
                new Series(PolarsWrapper.SeriesIsIn(this.Handle, implodedOther.Handle, nEq))
        /// <summary>
        /// Check if elements of this Series are in the collections.
        /// </summary>
        member this.IsIn(collection: seq<'T>, ?nullsEqual: bool) : Series =
            use other = Series.create("__TEMP_FOR_ISIN", collection)
            this.IsIn(other, ?nullsEqual = nullsEqual)
        /// <summary>
        /// Returns a boolean Series indicating which values are null.
        /// </summary>
        member this.IsNull() : Series = 
            new Series(PolarsWrapper.SeriesIsNull this.Handle)
        /// <summary>
        /// Returns a boolean Series indicating which values are not null.
        /// </summary>
        member this.IsNotNull() : Series = 
            new Series(PolarsWrapper.SeriesIsNotNull this.Handle)
        /// <summary>
        /// Count the number of unique values.
        /// </summary>
        member this.NUnique() = PolarsWrapper.SeriesNUnique this.Handle
        /// <summary>
        /// Get an approximation of the number of unique values in this Series.
        /// Uses HyperLogLog algorithm for fast, memory-efficient counting.
        /// </summary>
        /// <returns>Approximate count of unique values.</returns>
        member this.ApproxNUnique() = PolarsWrapper.SeriesApproxNUnique this.Handle
        /// <summary>
        /// Return the lower bound of this Series’ dtype as a unit Series.
        /// </summary>        
        member this.LowerBound() = this.ApplyExpr(Expr.Col(this.Name).LowerBound())
        /// <summary>
        /// Return the upper bound of this Series’ dtype as a unit Series.
        /// </summary>
        member this.UpperBound() = this.ApplyExpr(Expr.Col(this.Name).UpperBound())
        /// <summary>
        /// Return a count of the unique values in the order of appearance.
        /// </summary>
        member this.UniqueCounts() = PolarsWrapper.SeriesUniqueCounts this.Handle
        /// <summary>
        /// Count the occurrences of unique values.
        /// Similar to SQL `GROUP BY val COUNT(*)`.
        /// </summary>
        /// <param name="sort">Sort the output by count in descending order. Default is true.</param>
        /// <param name="parallel">Execute in parallel. Default is true.</param>
        /// <param name="name">The name of the count column. Default is "count".</param>
        /// <param name="normalize">If true, the count column will contain probabilities instead of counts. Default is false.</param>
        member this.ValueCounts(?sort: bool, ?paralleling: bool, ?name: string, ?normalize: bool) =
            let sort = defaultArg sort true
            let paralleling = defaultArg paralleling true
            let name = defaultArg name "count"
            let normalize = defaultArg normalize false
            
            let dfHandle = PolarsWrapper.SeriesValueCounts(this.Handle, sort, paralleling, name, normalize)
            new DataFrame(dfHandle)
