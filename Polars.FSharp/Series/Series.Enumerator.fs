namespace Polars.FSharp

open System.Runtime.CompilerServices
open System

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

    /// <summary>
    /// Gets the total number of elements in the underlying Series.
    /// </summary>
    member this.Length: int64 = this.length

    /// <summary>
    /// Gets the total number of elements as an int32.
    /// </summary>
    member this.Count: int = int this.length

    /// <summary>
    /// Returns true if the underlying series is empty.
    /// </summary>
    member this.IsEmpty: bool = this.length = 0L

    /// <summary>
    /// Resets the enumerator to its initial position.
    /// </summary>
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.Reset() : unit =
        this.index <- -1L
        this.current <- Unchecked.defaultof<'T>
    /// <summary>
    /// Returns the first element of the series, or throws an InvalidOperationException if empty.
    /// </summary>
    member this.Head() : 'T =
        if this.length = 0L then
            invalidOp "Series contains no elements."
        this.series.GetValue<'T>(0L, uncheck = true)

    /// <summary>
    /// Returns the first element wrapped in ValueOption, or ValueNone if empty.
    /// </summary>
    member this.TryHead() : 'T voption =
        if this.length = 0L then
            ValueNone
        else
            ValueSome (this.series.GetValue<'T>(0L, uncheck = true))

    /// <summary>
    /// Materializes the remaining elements into an array.
    /// </summary>
    member this.ToArray() : 'T[] =
        if this.length > Int32.MaxValue then
            raise (OverflowException("Series length exceeds maximum supported array size."))

        // Fast batch route when untouched
        if this.index = -1L then
            this.series.ToArray<'T>()
        else
            let remaining = int (this.length - (this.index + 1L))
            if remaining <= 0 then
                Array.empty
            else
                let result = Array.zeroCreate<'T> remaining
                let mutable i = 0
                while this.MoveNext() do
                    result.[i] <- this.Current
                    i <- i + 1
                result

    /// <summary>
    /// Materializes the remaining elements into a ResizeArray (System.Collections.Generic.List).
    /// </summary>
    member this.ToResizeArray() : ResizeArray<'T> =
        ResizeArray<'T>(this.ToArray())

    /// <summary>
    /// Materializes the remaining elements into a standard F# immutable list.
    /// </summary>
    member this.ToList() : 'T list =
        this.ToArray() |> Array.toList
[<AutoOpen>]
module SeriesEnumerator =
    /// <summary>
    /// Materializes all or remaining elements into an array.
    /// </summary>
    let inline toArray (enumerator: SeriesEnumerator<'T>) : 'T[] =
        enumerator.ToArray()

    /// <summary>
    /// Materializes all or remaining elements into an F# immutable list.
    /// </summary>
    let inline toList (enumerator: SeriesEnumerator<'T>) : 'T list =
        enumerator.ToList()

    /// <summary>
    /// Materializes all or remaining elements into a ResizeArray.
    /// </summary>
    let inline toResizeArray (enumerator: SeriesEnumerator<'T>) : ResizeArray<'T> =
        enumerator.ToResizeArray()

    /// <summary>
    /// Returns the first element or throws if empty.
    /// </summary>
    let inline head (enumerator: SeriesEnumerator<'T>) : 'T =
        enumerator.Head()

    /// <summary>
    /// Returns the first element as ValueOption.
    /// </summary>
    let inline tryHead (enumerator: SeriesEnumerator<'T>) : 'T voption =
        enumerator.TryHead()

    /// <summary>
    /// Returns the total element count.
    /// </summary>
    let inline length (enumerator: SeriesEnumerator<'T>) : int64 =
        enumerator.Length
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
