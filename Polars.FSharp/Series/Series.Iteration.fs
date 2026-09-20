namespace Polars.FSharp

open System
open Polars.NET.Core

[<AutoOpen>]
module SeriesIterationExtensions =

    type Series with

        /// <summary>
        /// Applies an action to every row in the Series, passing a ValueOption representing whether
        /// the element is present (ValueSome 'T) or null (ValueNone).
        /// Never skips rows silently, allowing explicit handling of nulls.
        /// </summary>
        /// <typeparam name="'T">The expected scalar element type.</typeparam>
        /// <param name="action">Action invoked for every row ('T voption -> unit).</param>
        member this.IterOpt<'T>(action: 'T voption -> unit) : unit =
            ArgumentNullException.ThrowIfNull(action, nameof action)

            let len = this.Length
            for i in 0L..len - 1L do
                action (this.TryGetValue<'T>(i))

        /// <summary>
        /// Applies an action to every row with its 64-bit index, passing a ValueOption for each row.
        /// </summary>
        member this.IteriOpt<'T>(action: int64 -> 'T voption -> unit) : unit =
            ArgumentNullException.ThrowIfNull(action, nameof action)

            let len = this.Length
            for i in 0L..len - 1L do
                action i (this.TryGetValue<'T>(i))

        /// <summary>
        /// Applies a dense action to non-null scalar elements in the Series.
        /// Null rows are automatically skipped.
        /// </summary>
        /// <typeparam name="'T">The scalar element type.</typeparam>
        /// <param name="action">Action invoked only for present scalar elements ('T -> unit).</param>
        member this.Iter<'T>(action: 'T -> unit) : unit =
            ArgumentNullException.ThrowIfNull(action, nameof action)

            let len = this.Length
            for i in 0L..len - 1L do
                match this.TryGetValue<'T>(i) with
                | ValueSome v -> action v
                | ValueNone -> ()

        /// <summary>
        /// Applies a dense action to non-null scalar elements along with their row index.
        /// Null rows are skipped.
        /// </summary>
        member this.Iteri<'T>(action: int64 -> 'T -> unit) : unit =
            ArgumentNullException.ThrowIfNull(action, nameof action)

            let len = this.Length
            for i in 0L..len - 1L do
                match this.TryGetValue<'T>(i) with
                | ValueSome v -> action i v
                | ValueNone -> ()

        /// <summary>
        /// Pairwise traversal over two equal-length Series, passing ValueOptions for both columns.
        /// Guarantees that null occurrences in either Series can be explicitly observed and handled.
        /// </summary>
        member this.Iter2Opt<'T1, 'T2>(other: Series, action: 'T1 voption -> 'T2 voption -> unit) : unit =
            ArgumentNullException.ThrowIfNull(other, nameof other)
            ArgumentNullException.ThrowIfNull(action, nameof action)

            if this.Length <> other.Length then
                invalidArg (nameof other) $"Series lengths do not match: {this.Length} vs {other.Length}."

            let len = this.Length
            for i in 0L..len - 1L do
                action (this.TryGetValue<'T1>(i)) (other.TryGetValue<'T2>(i))

        /// <summary>
        /// Pairwise dense traversal over two Series. Invokes action only when both elements are non-null.
        /// </summary>
        member this.Iter2<'T1, 'T2>(other: Series, action: 'T1 -> 'T2 -> unit) : unit =
            ArgumentNullException.ThrowIfNull(other, nameof other)
            ArgumentNullException.ThrowIfNull(action, nameof action)

            if this.Length <> other.Length then
                invalidArg (nameof other) $"Series lengths do not match: {this.Length} vs {other.Length}."

            let len = this.Length
            for i in 0L..len - 1L do
                match this.TryGetValue<'T1>(i), other.TryGetValue<'T2>(i) with
                | ValueSome v1, ValueSome v2 -> action v1 v2
                | _ -> ()
