namespace Polars.FSharp

open System
open System.Reflection
open Microsoft.FSharp.Reflection

[<AutoOpen>]
module DataFrameMappingExtensions =

    type DataFrame with
        /// <summary>
        /// Transforms each column in the DataFrame using a column-wise mapping function (Series -> Series).
        /// Preserves the original column name if the mapped Series does not provide a new one.
        /// </summary>
        /// <param name="mapping">The transformation function applied to each column Series.</param>
        /// <returns>A new DataFrame composed of the transformed Series.</returns>
        member this.Map(mapping: Series -> Series) : DataFrame =
            if isNull (box mapping) then
                nullArg (nameof mapping)

            this
            |> Seq.map (fun col ->
                let transformed = mapping col
                if String.IsNullOrEmpty transformed.Name then
                    transformed.Rename col.Name
                else
                    transformed
            )
            |> pl.dataframe
        /// <summary>
        /// Transforms DataFrame rows into a new DataFrame using a strongly-typed F# Record mapping function ('TIn -> 'TOut).
        /// Combines compiled zero-allocation row enumerator with zero-reflection columnar transposition.
        /// </summary>
        /// <typeparam name="'TIn">Input F# Record type representing the source row.</typeparam>
        /// <typeparam name="'TOut">Output F# Record type representing the transformed row.</typeparam>
        /// <param name="mapping">The pure transformation function applied to each row.</param>
        /// <returns>A new DataFrame materialized from the mapped output records.</returns>
        member this.MapRows<'TIn, 'TOut>(mapping: 'TIn -> 'TOut) : DataFrame =
            if isNull (box mapping) then
                nullArg (nameof mapping)

            // Validate that both 'TIn and 'TOut are F# Records
            if not (FSharpType.IsRecord(typeof<'TIn>, BindingFlags.Public ||| BindingFlags.NonPublic)) then
                invalidArg (nameof mapping) (sprintf "Input type '%s' is not an F# Record type." typeof<'TIn>.FullName)

            if not (FSharpType.IsRecord(typeof<'TOut>, BindingFlags.Public ||| BindingFlags.NonPublic)) then
                invalidArg (nameof mapping) (sprintf "Output type '%s' is not an F# Record type." typeof<'TOut>.FullName)

            let height = this.Height
            if height = 0L then
                // Materialize an empty DataFrame preserving output Record schema
                DataFrame.ofRecords<'TOut> Array.empty
            else
                if height > int64 Int32.MaxValue then
                    raise (OverflowException(sprintf "DataFrame height (%d) exceeds Int32.MaxValue." height))

                let count = int height
                let results = Array.zeroCreate<'TOut> count

                // 1. Stream rows using compiled zero-allocation F# RowMapper
                let enumerator = this.ToRecords<'TIn>().GetEnumerator()
                let mutable idx = 0

                while enumerator.MoveNext() do
                    results.[idx] <- mapping enumerator.Current
                    idx <- idx + 1

                // 2. Transpose mapped array back to DataFrame via native columnar Series
                DataFrame.ofRecords<'TOut> results
