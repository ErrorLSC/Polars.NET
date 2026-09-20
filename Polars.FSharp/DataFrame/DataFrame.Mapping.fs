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
        /// <summary>
        /// Applies the given action to each row in the DataFrame materialized as a strongly-typed F# Record or DTO.
        /// Utilizes pre-compiled expression tree hydration and hoisted Series handles for extreme throughput.
        /// </summary>
        /// <typeparam name="'T">The strongly-typed F# Record or DTO row type.</typeparam>
        /// <param name="action">The side-effecting action executed for each row.</param>
        member this.IterRows<'T>(action: 'T -> unit) : unit =
            ArgumentNullException.ThrowIfNull(action, nameof action)

            if this.Height > 0L then
                let mutable enumerator = this.Rows<'T>()
                while enumerator.MoveNext() do
                    action enumerator.Current

        /// <summary>
        /// Applies the given action to each row in the DataFrame along with its zero-based 64-bit row index.
        /// </summary>
        /// <typeparam name="'T">The strongly-typed F# Record or DTO row type.</typeparam>
        /// <param name="action">The action executed for each row (int64 index -> 'T row -> unit).</param>
        member this.IteriRows<'T>(action: int64 -> 'T -> unit) : unit =
            ArgumentNullException.ThrowIfNull(action, nameof action)

            if this.Height > 0L then
                let mutable enumerator = this.Rows<'T>()
                let mutable idx = 0L
                while enumerator.MoveNext() do
                    action idx enumerator.Current
                    idx <- idx + 1L
        /// <summary>
        /// Applies an action pairwise to strongly-typed rows from two equal-height DataFrames.
        /// Throws ArgumentException if DataFrame heights do not match.
        /// Uses dual compiled stack enumerators for zero heap allocation and hoisted Series lookups.
        /// </summary>
        /// <typeparam name="'T1">Row type for the first DataFrame.</typeparam>
        /// <typeparam name="'T2">Row type for the second DataFrame.</typeparam>
        /// <param name="other">The second DataFrame to pair with.</param>
        /// <param name="action">The function applied to each pair of hydrated rows ('T1 -> 'T2 -> unit).</param>
        member this.Iter2<'T1, 'T2>(other: DataFrame, action: 'T1 -> 'T2 -> unit) : unit =
            ArgumentNullException.ThrowIfNull(other, nameof other)
            ArgumentNullException.ThrowIfNull(action, nameof action)

            if this.Height <> other.Height then
                invalidArg (nameof other) $"DataFrame heights do not match: {this.Height} vs {other.Height}."

            if this.Height > 0L then
                let mutable enum1 = this.Rows<'T1>()
                let mutable enum2 = other.Rows<'T2>()

                while enum1.MoveNext() && enum2.MoveNext() do
                    action enum1.Current enum2.Current

        /// <summary>
        /// Applies an action pairwise to strongly-typed rows from two DataFrames along with their 64-bit row index.
        /// </summary>
        member this.Iteri2<'T1, 'T2>(other: DataFrame, action: int64 -> 'T1 -> 'T2 -> unit) : unit =
            ArgumentNullException.ThrowIfNull(other, nameof other)
            ArgumentNullException.ThrowIfNull(action, nameof action)

            if this.Height <> other.Height then
                invalidArg (nameof other) $"DataFrame heights do not match: {this.Height} vs {other.Height}."

            if this.Height > 0L then
                let mutable enum1 = this.Rows<'T1>()
                let mutable enum2 = other.Rows<'T2>()
                let mutable idx = 0L

                while enum1.MoveNext() && enum2.MoveNext() do
                    action idx enum1.Current enum2.Current
                    idx <- idx + 1L
