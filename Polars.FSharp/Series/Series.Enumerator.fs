namespace Polars.FSharp

open System.Runtime.CompilerServices

/// <summary>
/// Stack-only, zero-allocation struct enumerator for F# Series iteration.
/// </summary>
[<Struct>]
type SeriesEnumerator<'T> =
    val private series: Series
    val private length: int64
    val private hasNulls: bool
    val mutable private index: int64
    val mutable private current: 'T

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    internal new(s: Series) =
        {
            series = s
            length = s.Length
            hasNulls = s.HasNulls()
            index = -1L
            current = Unchecked.defaultof<'T>
        }

    /// <summary>
    /// Moves the enumerator to the next element.
    /// </summary>
    member this.MoveNext() : bool =
        let nextIndex = this.index + 1L
        if nextIndex < this.length then
            this.index <- nextIndex
            // Delegates directly to Series.GetValue<'T>, which internally handles nulls & uncheck
            this.current <- this.series.GetValue<'T>(this.index, uncheck = true)
            true
        else
            this.current <- Unchecked.defaultof<'T>
            false

    /// <summary>
    /// Gets the element at the current position of the enumerator.
    /// </summary>
    member this.Current: 'T =
        this.current

    /// <summary>
    /// Returns the enumerator itself to satisfy F# duck-typing iteration patterns.
    /// </summary>
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.GetEnumerator() : SeriesEnumerator<'T> =
        this

[<AutoOpen>]
module SeriesEnumerator =
    type Series with
        /// <summary>
        /// Returns a zero-allocation struct enumerator for strongly-typed iteration.
        /// Usage: for v in series.As{int}() do ...
        /// </summary>
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        member this.As<'T>() : SeriesEnumerator<'T> =
            SeriesEnumerator<'T>(this)

        /// <summary>
        /// Default enumerator allowing direct 'for (x: obj) in series do ...' syntax.
        /// </summary>
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        member this.GetEnumerator() : SeriesEnumerator<obj> =
            SeriesEnumerator<obj>(this)
