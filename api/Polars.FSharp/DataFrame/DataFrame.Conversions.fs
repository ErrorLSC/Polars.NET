namespace Polars.FSharp

open System.Reflection
open Microsoft.FSharp.Reflection
open System.Collections.Generic
open Polars.NET.Core
open Polars.NET.Core.Data
open Polars.NET.Core.Wrappers
open System
open System.Runtime.CompilerServices
open Polars.NET.Core.Helpers
open Kaminari.Sazanami.Data
open Kaminari.Sazanami.Helpers

type internal FSharpRowMapper<'T>() =

    static member ColumnNames: string[] = ArrowRowMapper<'T>.ColumnNames

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    static member CreateCursor(df: DataFrame) : ArrowBatchCursor<'T> =
        DataFrameRowMapper<'T>.CreateCursor df.Handle

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    static member ReadRow(df: DataFrame, rowIdx: int64) : 'T =
        use cursor = DataFrameRowMapper<'T>.CreateCursor df.Handle
        cursor.ReadRow(int rowIdx)

/// <summary>
/// Stack-only, zero-allocation struct enumerator for F# DataFrame row iteration.
/// Eliminates state-machine allocations and maximizes hot-path throughput.
/// </summary>
[<Struct>]
type RowEnumerator<'T> =
    val private _df: DataFrame
    val private _cursor: ArrowBatchCursor<'T>
    val private _height: int64
    val mutable private _index: int64
    val mutable private _current: 'T

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    internal new(df: DataFrame) =
        let cursor = DataFrameRowMapper<'T>.CreateCursor df.Handle

        {
            _df = df
            _cursor = cursor
            _height = int64 cursor.Length
            _index = -1L
            _current = Unchecked.defaultof<'T>
        }

    /// <summary>
    /// Moves the enumerator to the next element.
    /// </summary>
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.MoveNext() : bool =
        let nextIndex = this._index + 1L

        if nextIndex < this._height then
            this._index <- nextIndex
            this._current <- this._cursor.ReadRow(int nextIndex)
            true
        else
            this._current <- Unchecked.defaultof<'T>
            false

    /// <summary>
    /// Gets the total number of rows.
    /// </summary>
    member this.Length: int64 = this._height

    /// <summary>
    /// Gets the total number of rows as an integer.
    /// Throws <see cref="OverflowException"/> if row count exceeds <see cref="Int32.MaxValue"/>.
    /// </summary>
    member this.Count: int = int this._height

    /// <summary>
    /// Returns true if the underlying DataFrame contains no rows.
    /// </summary>
    member this.IsEmpty: bool = this._df.IsEmpty

    /// <summary>
    /// Determines whether the DataFrame contains any rows.
    /// </summary>
    member this.Any() : bool = not this.IsEmpty

    /// <summary>
    /// Resets the enumerator to its initial position before the first row.
    /// </summary>
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.Reset() : unit =
        this._index <- -1L
        this._current <- Unchecked.defaultof<'T>

    /// <summary>
    /// Gets the current element.
    /// </summary>
    member this.Current: 'T = this._current

    /// <summary>
    /// Returns the enumerator itself.
    /// </summary>
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.GetEnumerator() : RowEnumerator<'T> = this

    /// <summary>
    /// Returns the first mapped row without mutating the enumerator cursor.
    /// Raises InvalidOperationException if the DataFrame contains no rows.
    /// </summary>
    member this.First() : 'T =
        if this._height = 0L then
            invalidOp "The DataFrame sequence contains no elements."

        this._cursor.ReadRow 0

    /// <summary>
    /// Returns the first mapped row as an option without mutating the enumerator cursor.
    /// </summary>
    member this.TryFirst() : 'T option =
        if this._height = 0L then
            None
        else
            Some(this._cursor.ReadRow 0)

    /// <summary>
    /// Returns the first mapped row as a ValueOption without mutating the enumerator cursor.
    /// </summary>
    member this.TryFirstValue() : 'T voption =
        if this._height = 0L then
            ValueNone
        else
            ValueSome(this._cursor.ReadRow 0)

    /// <summary>
    /// Returns the last mapped row without mutating the enumerator cursor.
    /// Raises InvalidOperationException if the DataFrame contains no rows.
    /// </summary>
    member this.Last() : 'T =
        if this._height = 0L then
            invalidOp "The DataFrame sequence contains no elements."

        this._cursor.ReadRow(int (this._height - 1L))

    /// <summary>
    /// Returns the last mapped row as an option without mutating the enumerator cursor.
    /// </summary>
    member this.TryLast() : 'T option =
        if this._height = 0L then
            None
        else
            Some(this._cursor.ReadRow(int (this._height - 1L)))

    /// <summary>
    /// Returns the last mapped row as a ValueOption without mutating the enumerator cursor.
    /// </summary>
    member this.TryLastValue() : 'T voption =
        if this._height = 0L then
            ValueNone
        else
            ValueSome(this._cursor.ReadRow(int (this._height - 1L)))

    /// <summary>
    /// Fetches a specific row directly by 64-bit index without sequential stepping.
    /// </summary>
    member this.Item(index: int64) : 'T =
        if index < 0L || index >= this._height then
            raise (InvalidOperationException $"Index {index} is out of bounds for DataFrame height {this._height}.")

        this._cursor.ReadRow(int index)

    /// <summary>
    /// Fetches a specific row directly by index, or None if out of bounds.
    /// </summary>
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.TryItem(index: int64) : 'T option =
        if index < 0L || index >= this._height then
            None
        else
            Some(this._cursor.ReadRow(int index))

    /// <summary>
    /// Fetches a specific row directly by index, or ValueNone if out of bounds.
    /// </summary>
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.TryGetValueItem(index: int64) : 'T voption =
        if index < 0L || index >= this._height then
            ValueNone
        else
            ValueSome(this._cursor.ReadRow(int index))

    /// <summary>
    /// Copies all mapped rows directly into the destination span without modifying enumerator state.
    /// Returns the number of rows written.
    /// </summary>
    member this.CopyTo(destination: Span<'T>) : int =
        if int64 destination.Length < this._height then
            invalidArg
                (nameof destination)
                $"Destination span length ({destination.Length}) is smaller than row count ({this._height})."

        let total = int this._height

        for i = 0 to total - 1 do
            destination.[i] <- this._cursor.ReadRow i

        total

    /// <summary>
    /// Attempts to copy all mapped rows into the destination span without heap allocation.
    /// </summary>
    member this.TryCopyTo(destination: Span<'T>, [<System.Runtime.InteropServices.Out>] written: byref<int>) : bool =
        if int64 destination.Length < this._height then
            written <- 0
            false
        else
            let total = int this._height

            for i = 0 to total - 1 do
                destination.[i] <- this._cursor.ReadRow i

            written <- total
            true

    /// <summary>
    /// Materializes all or remaining mapped rows into a strongly typed array.
    /// </summary>
    member this.ToArray() : 'T array =
        if this._height = 0L then
            [||]
        elif this._height > int64 Int32.MaxValue then
            raise (OverflowException($"DataFrame height ({this._height}) exceeds Int32.MaxValue."))
        elif this._index = -1L then
            let total = int this._height
            let arr = Array.zeroCreate<'T> total

            for i = 0 to total - 1 do
                arr.[i] <- this._cursor.ReadRow i

            arr
        else
            let remaining = int (this._height - (this._index + 1L))

            if remaining <= 0 then
                [||]
            else
                let arr = Array.zeroCreate<'T> remaining
                let mutable i = 0

                while this.MoveNext() do
                    arr.[i] <- this.Current
                    i <- i + 1

                arr

    /// <summary>
    /// Materializes all or remaining mapped rows into an F# immutable list.
    /// </summary>
    member this.ToList() : 'T list = this.ToArray() |> Array.toList

    /// <summary>
    /// Materializes all or remaining mapped rows into a ResizeArray (List{'T}).
    /// </summary>
    member this.ToResizeArray() : ResizeArray<'T> = ResizeArray<'T>(this.ToArray())

    /// <summary>
    /// Releases the underlying Arrow batch cursor and pinned buffers.
    /// </summary>
    member this.Dispose() : unit = this._cursor.Dispose()

[<RequireQualifiedAccess>]
module Rows =

    /// <summary>
    /// Materializes all or remaining mapped rows into an array.
    /// </summary>
    let inline toArray (enumerator: RowEnumerator<'T>) : 'T array = enumerator.ToArray()

    /// <summary>
    /// Materializes all or remaining mapped rows into an F# immutable list.
    /// </summary>
    let inline toList (enumerator: RowEnumerator<'T>) : 'T list = enumerator.ToList()

    /// <summary>
    /// Materializes all or remaining mapped rows into a ResizeArray.
    /// </summary>
    let inline toResizeArray (enumerator: RowEnumerator<'T>) : ResizeArray<'T> = enumerator.ToResizeArray()

    /// <summary>
    /// Returns the first mapped row or raises InvalidOperationException if empty.
    /// </summary>
    let inline head (enumerator: RowEnumerator<'T>) : 'T = enumerator.First()

    /// <summary>
    /// Returns the first mapped row as an option.
    /// </summary>
    let inline tryHead (enumerator: RowEnumerator<'T>) : 'T option = enumerator.TryFirst()

    /// <summary>
    /// Returns the first mapped row as a ValueOption.
    /// </summary>
    let inline tryHeadValue (enumerator: RowEnumerator<'T>) : 'T voption = enumerator.TryFirstValue()

    /// <summary>
    /// Returns the last mapped row or raises InvalidOperationException if empty.
    /// </summary>
    let inline last (enumerator: RowEnumerator<'T>) : 'T = enumerator.Last()

    /// <summary>
    /// Returns the last mapped row as an option.
    /// </summary>
    let inline tryLast (enumerator: RowEnumerator<'T>) : 'T option = enumerator.TryLast()

    /// <summary>
    /// Returns the last mapped row as a ValueOption.
    /// </summary>
    let inline tryLastValue (enumerator: RowEnumerator<'T>) : 'T voption = enumerator.TryLastValue()

    /// <summary>
    /// Returns the total row count as a 64-bit integer.
    /// </summary>
    let inline length (enumerator: RowEnumerator<'T>) : int64 = enumerator.Length

    /// <summary>
    /// Iterates over the rows of the enumerator, applying the action to each row.
    /// </summary>
    let inline iter ([<InlineIfLambda>] action: 'T -> unit) (enumerator: RowEnumerator<'T>) =
        let mutable it = enumerator

        while it.MoveNext() do
            action it.Current

    /// <summary>
    /// Iterates over the rows of the enumerator, applying the action with zero-based 64-bit row index.
    /// </summary>
    let inline iteri ([<InlineIfLambda>] action: int64 -> 'T -> unit) (enumerator: RowEnumerator<'T>) : unit =
        let mutable it = enumerator
        let mutable idx = 0L

        while it.MoveNext() do
            action idx it.Current
            idx <- idx + 1L

    /// <summary>
    /// Projects all rows of the enumerator into a strongly-typed array using an inlined mapping function.
    /// Eliminates intermediate IEnumerator heap boxing and lambda allocations.
    /// </summary>
    let inline mapToArray ([<InlineIfLambda>] mapping: 'TIn -> 'TOut) (enumerator: RowEnumerator<'TIn>) : 'TOut array =
        let mutable it = enumerator
        let len = it.Length

        if len = 0L then
            Array.empty<'TOut>
        elif len > int64 Int32.MaxValue then
            raise (OverflowException $"Row count ({len}) exceeds Int32.MaxValue.")
        else
            let count = int len
            let results = Array.zeroCreate<'TOut> count
            let mutable idx = 0

            while it.MoveNext() do
                results.[idx] <- mapping it.Current
                idx <- idx + 1

            results

    /// <summary>
    /// Iterates simultaneously over two equal-length RowEnumerators, applying the action pairwise.
    /// </summary>
    let inline iter2
        ([<InlineIfLambda>] action: 'T1 -> 'T2 -> unit)
        (enum1: RowEnumerator<'T1>)
        (enum2: RowEnumerator<'T2>)
        : unit =
        let mutable it1 = enum1
        let mutable it2 = enum2

        if it1.Length <> it2.Length then
            invalidArg (nameof enum2) $"Enumerator lengths do not match: {it1.Length} vs {it2.Length}."

        while it1.MoveNext() && it2.MoveNext() do
            action it1.Current it2.Current

    /// <summary>
    /// Iterates simultaneously over two equal-length RowEnumerators with zero-based 64-bit row index.
    /// </summary>
    let inline iteri2
        ([<InlineIfLambda>] action: int64 -> 'T1 -> 'T2 -> unit)
        (enum1: RowEnumerator<'T1>)
        (enum2: RowEnumerator<'T2>)
        : unit =
        let mutable it1 = enum1
        let mutable it2 = enum2

        if it1.Length <> it2.Length then
            invalidArg (nameof enum2) $"Enumerator lengths do not match: {enum1.Length} vs {enum2.Length}."

        let mutable idx = 0L

        while it1.MoveNext() && it2.MoveNext() do
            action idx it1.Current it2.Current
            idx <- idx + 1L

    /// <summary>
    /// Folds over the rows of the enumerator, applying the folder function to each row.
    /// </summary>
    let inline fold
        ([<InlineIfLambda>] folder: 'State -> 'T -> 'State)
        (initial: 'State)
        (enumerator: RowEnumerator<'T>)
        : 'State =
        let mutable it = enumerator
        let mutable state = initial

        while it.MoveNext() do
            state <- folder state it.Current

        state

    /// <summary>
    /// Folds over the rows of the enumerator with index.
    /// </summary>
    let inline foldi
        ([<InlineIfLambda>] folder: int64 -> 'State -> 'T -> 'State)
        (initial: 'State)
        (enumerator: RowEnumerator<'T>)
        : 'State =
        let mutable it = enumerator
        let mutable state = initial
        let mutable idx = 0L

        while it.MoveNext() do
            state <- folder idx state it.Current
            idx <- idx + 1L

        state

    /// <summary>
    /// Applies a function to each row, threading an accumulator argument through the computation.
    /// Raises InvalidOperationException if the enumerator has no elements.
    /// </summary>
    let inline reduce ([<InlineIfLambda>] reduction: 'T -> 'T -> 'T) (enumerator: RowEnumerator<'T>) : 'T =
        let mutable it = enumerator

        if not (it.MoveNext()) then
            invalidOp "The RowEnumerator sequence contains no elements to reduce."

        let mutable state = it.Current

        while it.MoveNext() do
            state <- reduction state it.Current

        state

    /// <summary>
    /// Like fold, but returns a lazy sequence of intermediate and final results.
    /// </summary>
    let inline scan
        ([<InlineIfLambda>] folder: 'State -> 'T -> 'State)
        (initial: 'State)
        (enumerator: RowEnumerator<'T>)
        : seq<'State> =
        seq {
            let mutable state = initial
            yield state
            let mutable it = enumerator

            while it.MoveNext() do
                state <- folder state it.Current
                yield state
        }

    /// <summary>
    /// Tries to find a row that satisfies the predicate.
    /// </summary>
    let inline tryFind ([<InlineIfLambda>] predicate: 'T -> bool) (enumerator: RowEnumerator<'T>) : 'T option =
        let mutable it = enumerator
        let mutable found = None

        while found.IsNone && it.MoveNext() do
            let cur = it.Current

            if predicate cur then
                found <- Some cur

        found

    /// <summary>
    /// Tries to find the first row that satisfies the predicate, returning a ValueOption to eliminate heap boxing.
    /// </summary>
    let inline tryFindValue ([<InlineIfLambda>] predicate: 'T -> bool) (enumerator: RowEnumerator<'T>) : 'T voption =
        let mutable it = enumerator
        let mutable found = ValueNone

        while found.IsNone && it.MoveNext() do
            let cur = it.Current

            if predicate cur then
                found <- ValueSome cur

        found

    /// <summary>
    /// Maps each row of the enumerator to a new value using the mapping function.
    /// </summary>
    let inline map (mapping: 'T -> 'U) (enumerator: RowEnumerator<'T>) : seq<'U> =
        seq {
            let mutable it = enumerator

            while it.MoveNext() do
                yield mapping it.Current
        }

    /// <summary>
    /// Maps each row with its zero-based 64-bit row index to a lazy F# sequence.
    /// </summary>
    let inline mapi ([<InlineIfLambda>] mapping: int64 -> 'T -> 'U) (enumerator: RowEnumerator<'T>) : seq<'U> =
        seq {
            let mutable it = enumerator
            let mutable idx = 0L

            while it.MoveNext() do
                yield mapping idx it.Current
                idx <- idx + 1L
        }

    /// <summary>
    /// Returns whether any row satisfies the predicate.
    /// </summary>
    let inline exists ([<InlineIfLambda>] predicate: 'T -> bool) (enumerator: RowEnumerator<'T>) : bool =
        let mutable it = enumerator
        let mutable found = false

        while not found && it.MoveNext() do
            if predicate it.Current then
                found <- true

        found

    /// <summary>
    /// Returns whether all rows satisfy the predicate.
    /// </summary>
    let inline forall ([<InlineIfLambda>] predicate: 'T -> bool) (enumerator: RowEnumerator<'T>) : bool =
        let mutable it = enumerator
        let mutable ok = true

        while ok && it.MoveNext() do
            if not (predicate it.Current) then
                ok <- false

        ok

    /// <summary>
    /// Applies the given function to successive elements, returning the first result where the function returns Some.
    /// Short-circuits immediately.
    /// </summary>
    let inline tryPick ([<InlineIfLambda>] chooser: 'T -> 'U option) (enumerator: RowEnumerator<'T>) : 'U option =
        let mutable it = enumerator
        let mutable res = None

        while res.IsNone && it.MoveNext() do
            match chooser it.Current with
            | Some _ as hit -> res <- hit
            | None -> ()

        res

    /// <summary>
    /// Applies the given function to successive elements, returning the first result where the function returns ValueSome.
    /// Short-circuits with zero heap allocation.
    /// </summary>
    let inline tryPickValue
        ([<InlineIfLambda>] chooser: 'T -> 'U voption)
        (enumerator: RowEnumerator<'T>)
        : 'U voption =
        let mutable it = enumerator
        let mutable res = ValueNone

        while res.IsNone && it.MoveNext() do
            match chooser it.Current with
            | ValueSome _ as hit -> res <- hit
            | ValueNone -> ()

        res

    /// <summary>
    /// Filters the rows of the enumerator that satisfy the predicate.
    /// </summary>
    let inline filter ([<InlineIfLambda>] predicate: 'T -> bool) (enumerator: RowEnumerator<'T>) : ResizeArray<'T> =
        let mutable it = enumerator
        let results = ResizeArray()

        while it.MoveNext() do
            let cur = it.Current

            if predicate cur then
                results.Add cur

        results

    /// <summary>
    /// Chooses values from the rows of the enumerator that satisfy the predicate.
    /// </summary>
    let inline choose ([<InlineIfLambda>] chooser: 'T -> 'U option) (enumerator: RowEnumerator<'T>) : ResizeArray<'U> =
        let mutable it = enumerator
        let results = ResizeArray<'U>()

        while it.MoveNext() do
            match chooser it.Current with
            | Some value -> results.Add(value)
            | None -> ()

        results

    /// <summary>
    /// Tests if the enumerator contains the specified element using default structural equality.
    /// Short-circuits upon discovery.
    /// </summary>
    let inline contains (element: 'T) (enumerator: RowEnumerator<'T>) : bool =
        let mutable it = enumerator
        let comparer = EqualityComparer<'T>.Default
        let mutable found = false

        while not found && it.MoveNext() do
            if comparer.Equals(it.Current, element) then
                found <- true

        found

    /// <summary>
    /// Sums the values of the rows of the enumerator using the projection function.
    /// </summary>
    let inline sumBy
        ([<InlineIfLambda>] projection: 'T -> ^Num)
        (enumerator: RowEnumerator<'T>)
        : ^Num when ^Num: (static member (+): ^Num * ^Num -> ^Num) and ^Num: (static member Zero: ^Num) =
        let mutable it = enumerator
        let mutable acc = LanguagePrimitives.GenericZero< ^Num>

        while it.MoveNext() do
            acc <- acc + projection it.Current

        acc

    /// <summary>
    /// Applies a key-generating function to each row and returns a dictionary with the frequency of each key.
    /// </summary>
    let inline countBy
        ([<InlineIfLambda>] projection: 'T -> 'Key)
        (enumerator: RowEnumerator<'T>)
        : Dictionary<'Key, int64> =
        let mutable it = enumerator
        let dict = Dictionary<'Key, int64>()

        while it.MoveNext() do
            let key = projection it.Current

            match dict.TryGetValue(key) with
            | true, count -> dict.[key] <- count + 1L
            | false, _ -> dict.[key] <- 1L

        dict

    /// <summary>
    /// Groups the rows of the enumerator using the key-generating function,
    /// returning an idiomatic F# immutable Map of key to row list.
    /// </summary>
    let inline groupBy
        ([<InlineIfLambda>] keySelector: 'T -> 'Key)
        (enumerator: RowEnumerator<'T>)
        : Map<'Key, 'T list> =
        let dict = Dictionary<'Key, ResizeArray<'T>>()
        let mutable it = enumerator

        while it.MoveNext() do
            let cur = it.Current
            let key = keySelector cur

            match dict.TryGetValue key with
            | true, list -> list.Add cur
            | false, _ ->
                let list = ResizeArray<'T>()
                list.Add cur
                dict.[key] <- list

        dict |> Seq.map (fun kv -> kv.Key, kv.Value |> Seq.toList) |> Map.ofSeq


[<AutoOpen>]
module DataFrameConversions =

    type DataFrame with
        /// <summary>
        /// Returns a zero-allocation stack enumerator for strongly-typed row hydration.
        /// Supports F# Records and parameterless DTOs.
        /// Usage: for row in df.Rows{MyRecord}() do ...
        /// </summary>
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        member this.Rows<'T>() : RowEnumerator<'T> = RowEnumerator<'T> this

        /// <summary>
        /// Convert the DataFrame to a dictionary of column name to Series.
        /// </summary>
        member this.ToMap() : Map<string, Series> =
            this |> Seq.map (fun series -> series.Name, series) |> Map.ofSeq

        /// <summary>
        /// Convert a DataFrame to a Series of type Struct.
        /// </summary>
        /// <param name="name">Name for the struct Series.</param>
        member this.ToStruct(?name: string) : Series =
            let n = defaultArg name ""
            use df: DataFrame = this.Select(Expr.AsStruct [| Expr.All() |])
            let series = df[0]
            series.Rename n

        /// <summary>
        /// Converts the DataFrame rows to a sequence of F# Record instances.
        /// Throws ArgumentException if 'T is not an F# Record type.
        /// </summary>
        /// <typeparam name="'T">Target F# Record type.</typeparam>
        member this.ToRecords<'T>() : seq<'T> =
            if not (FSharpType.IsRecord(typeof<'T>, true)) then
                raise (ArgumentException $"Type '{typeof<'T>.FullName}' is not an F# Record type.")

            seq {
                use cursor = DataFrameRowMapper<'T>.CreateCursor this.Handle
                let len = cursor.Length

                for r = 0 to len - 1 do
                    yield cursor.ReadRow r
            }

        /// <summary>
        /// Materializes all DataFrame rows into an F# list of records.
        /// </summary>
        member this.ToRecordList<'T>() : 'T list =
            let targetType = typeof<'T>

            if not (FSharpType.IsRecord(targetType, true)) then
                raise (ArgumentException($"Type '{targetType.FullName}' is not an F# Record type.", "T"))

            this.Rows<'T>().ToList()

type FSharpRowCursorMaterializer() =
    static member handleEmptySequence<'TSource>() : 'TSource =
        let t = typeof<'TSource>

        if ClrTypeHelper.AcceptsNull t then
            Unchecked.defaultof<'TSource>
        else
            invalidOp "Sequence contains no elements."

    interface IDataFrameMaterializer with
        member _.Materialize<'T>(handle: DataFrameHandle) : IEnumerable<'T> =
            seq {
                use cursor = DataFrameRowMapper<'T>.CreateCursor handle
                let len = cursor.Length

                for rowIdx = 0 to len - 1 do
                    yield cursor.ReadRow rowIdx
            }

        member _.MaterializeScalar<'T>(handle: DataFrameHandle) : 'T =
            use df = new DataFrame(handle)

            if df.Height = 0L || df.Width = 0 then
                Unchecked.defaultof<'T>
            else
                let firstCol = df.[0]
                let targetType = typeof<'T>

                if targetType.IsEnum then
                    let underlyingType = Enum.GetUnderlyingType targetType

                    let getValueMethod =
                        typeof<Series>
                            .GetMethod("GetValue", [| typeof<int64>; typeof<bool> |])
                            .MakeGenericMethod([| underlyingType |])

                    let rawVal = getValueMethod.Invoke(firstCol, [| box 0L; box true |])
                    Enum.ToObject(targetType, rawVal) :?> 'T
                else
                    firstCol.GetValue<'T>(0L)

        member _.Aggregate<'TSource, 'TAccum>
            (handle: DataFrameHandle, seed: 'TAccum, folder: Func<'TAccum, 'TSource, 'TAccum>)
            : 'TAccum =
            use df = new DataFrame(handle)
            let mutable enumerator = df.Rows<'TSource>()

            let mutable acc = seed

            while enumerator.MoveNext() do

                acc <- folder.Invoke(acc, enumerator.Current)

            acc

        /// Overload 1: Static seed value for all groups
        member _.AggregateBy<'TSource, 'TKey, 'TAccum>
            (
                handle: DataFrameHandle,
                keySelector: Func<'TSource, 'TKey>,
                seed: 'TAccum,
                folder: Func<'TAccum, 'TSource, 'TAccum>,
                comparer: IEqualityComparer<'TKey>
            ) : IEnumerable<KeyValuePair<'TKey, 'TAccum>> =

            ArgumentNullException.ThrowIfNull(keySelector, nameof keySelector)
            ArgumentNullException.ThrowIfNull(folder, nameof folder)

            use df = new DataFrame(handle)
            let dict = Dictionary<'TKey, 'TAccum>(comparer)

            if df.Height > 0L then
                let mutable enumerator = df.Rows<'TSource>()

                while enumerator.MoveNext() do
                    let current = enumerator.Current
                    let key = keySelector.Invoke current

                    match dict.TryGetValue key with
                    | true, existingVal -> dict.[key] <- folder.Invoke(existingVal, current)
                    | false, _ -> dict.[key] <- folder.Invoke(seed, current)

            dict :> IEnumerable<KeyValuePair<'TKey, 'TAccum>>

        /// Overload 2: Dynamic per-key seed factory
        member _.AggregateBy<'TSource, 'TKey, 'TAccum>
            (
                handle: DataFrameHandle,
                keySelector: Func<'TSource, 'TKey>,
                seedSelector: Func<'TKey, 'TAccum>,
                folder: Func<'TAccum, 'TSource, 'TAccum>,
                comparer: IEqualityComparer<'TKey>
            ) : IEnumerable<KeyValuePair<'TKey, 'TAccum>> =

            ArgumentNullException.ThrowIfNull(keySelector, nameof keySelector)
            ArgumentNullException.ThrowIfNull(seedSelector, nameof seedSelector)
            ArgumentNullException.ThrowIfNull(folder, nameof folder)

            use df = new DataFrame(handle)
            let dict = Dictionary<'TKey, 'TAccum>(comparer)

            if df.Height > 0L then
                let mutable enumerator = df.Rows<'TSource>()

                while enumerator.MoveNext() do
                    let current = enumerator.Current
                    let key = keySelector.Invoke current

                    match dict.TryGetValue key with
                    | true, existingVal -> dict.[key] <- folder.Invoke(existingVal, current)
                    | false, _ ->
                        let initialSeed = seedSelector.Invoke key
                        dict.[key] <- folder.Invoke(initialSeed, current)

            dict :> IEnumerable<KeyValuePair<'TKey, 'TAccum>>

        member _.Chunk<'T>(handle: DataFrameHandle, chunkSize: int) : IEnumerable<'T[]> =
            if chunkSize <= 0 then
                invalidArg (nameof chunkSize) "Chunk size must be greater than zero."

            use df = new DataFrame(handle)

            seq {
                if df.Height > 0L then
                    let mutable enumerator = df.Rows<'T>()
                    let buffer = ResizeArray<'T> chunkSize

                    while enumerator.MoveNext() do
                        buffer.Add enumerator.Current

                        if buffer.Count = chunkSize then
                            yield buffer.ToArray()
                            buffer.Clear()

                    if buffer.Count > 0 then
                        yield buffer.ToArray()
            }

        member _.Any<'TSource>(handle: DataFrameHandle, predicate: Func<'TSource, bool>) : bool =
            use df = new DataFrame(handle)

            if df.Height = 0L then
                false
            else
                let mutable enumerator = df.Rows<'TSource>()

                if isNull (box predicate) then
                    enumerator.MoveNext()
                else
                    let mutable matched = false

                    while not matched && enumerator.MoveNext() do
                        if predicate.Invoke enumerator.Current then
                            matched <- true

                    matched

        member _.All<'TSource>(handle: DataFrameHandle, predicate: Func<'TSource, bool>) : bool =
            ArgumentNullException.ThrowIfNull(predicate, nameof predicate)

            use df = new DataFrame(handle)

            if df.Height = 0L then
                true
            else
                let mutable enumerator = df.Rows<'TSource>()
                let mutable allMatched = true

                while allMatched && enumerator.MoveNext() do
                    if not (predicate.Invoke enumerator.Current) then
                        allMatched <- false

                allMatched

        member _.First<'TSource>(handle: DataFrameHandle, predicate: Func<'TSource, bool>) : 'TSource =
            use df = new DataFrame(handle)
            let enumerator = df.Rows<'TSource>()

            if isNull (box predicate) then
                enumerator.First()
            else
                let mutable enumerator = df.Rows<'TSource>()
                let mutable found = false
                let mutable result = Unchecked.defaultof<'TSource>

                while not found && enumerator.MoveNext() do
                    if predicate.Invoke enumerator.Current then
                        result <- enumerator.Current
                        found <- true

                if found then
                    result
                else
                    invalidOp "Sequence contains no matching element."

        member _.FirstOrDefault<'TSource>
            (handle: DataFrameHandle, predicate: Func<'TSource, bool>, defaultValue: 'TSource)
            : 'TSource =
            use df = new DataFrame(handle)
            let enumerator = df.Rows<'TSource>()

            if isNull (box predicate) then
                match enumerator.TryFirstValue() with
                | ValueSome row -> row
                | ValueNone -> defaultValue
            else
                let mutable enumerator = df.Rows<'TSource>()
                let mutable found = false
                let mutable result = defaultValue

                while not found && enumerator.MoveNext() do
                    if predicate.Invoke enumerator.Current then
                        result <- enumerator.Current
                        found <- true

                result

        member _.Last<'TSource>(handle: DataFrameHandle, predicate: Func<'TSource, bool>) : 'TSource =
            use df = new DataFrame(handle)
            let enumerator = df.Rows<'TSource>()

            if isNull (box predicate) then
                enumerator.Last()
            else
                let mutable enumerator = df.Rows<'TSource>()
                let mutable found = false
                let mutable lastMatch = Unchecked.defaultof<'TSource>

                while enumerator.MoveNext() do
                    if predicate.Invoke enumerator.Current then
                        lastMatch <- enumerator.Current
                        found <- true

                if found then
                    lastMatch
                else
                    invalidOp "Sequence contains no matching element."

        member _.LastOrDefault<'TSource>
            (handle: DataFrameHandle, predicate: Func<'TSource, bool>, defaultValue: 'TSource)
            : 'TSource =
            use df = new DataFrame(handle)

            let enumerator = df.Rows<'TSource>()

            if isNull (box predicate) then
                match enumerator.TryLastValue() with
                | ValueSome row -> row
                | ValueNone -> defaultValue
            else
                let mutable enumerator = df.Rows<'TSource>()
                let mutable found = false
                let mutable lastMatch = defaultValue

                while enumerator.MoveNext() do
                    if predicate.Invoke enumerator.Current then
                        lastMatch <- enumerator.Current
                        found <- true

                if found then lastMatch else defaultValue

        member _.Count<'TSource>(handle: DataFrameHandle, predicate: Func<'TSource, bool>) : int64 =
            use df = new DataFrame(handle)
            let height = df.Height

            if isNull (box predicate) then
                height
            elif height = 0L then
                0L
            else
                let mutable enumerator = df.Rows<'TSource>()
                let mutable count = 0L

                while enumerator.MoveNext() do
                    if predicate.Invoke enumerator.Current then
                        count <- count + 1L

                count

        member _.Single<'TSource>(handle: DataFrameHandle, predicate: Func<'TSource, bool>) : 'TSource =
            use df = new DataFrame(handle)
            let height = df.Height

            if isNull (box predicate) then
                if height = 0L then
                    invalidOp "Sequence contains no elements."
                elif height > 1L then
                    invalidOp "Sequence contains more than one element."
                else
                    df.Rows<'TSource>().First()

            elif height = 0L then
                invalidOp "Sequence contains no matching element."
            else
                let mutable enumerator = df.Rows<'TSource>()
                let mutable found = false
                let mutable matchVal = Unchecked.defaultof<'TSource>

                while enumerator.MoveNext() do
                    if predicate.Invoke enumerator.Current then
                        if found then
                            invalidOp "Sequence contains more than one matching element."

                        matchVal <- enumerator.Current
                        found <- true

                if found then
                    matchVal
                else
                    invalidOp "Sequence contains no matching element."

        member _.SingleOrDefault<'TSource>
            (handle: DataFrameHandle, predicate: Func<'TSource, bool>, defaultValue: 'TSource)
            : 'TSource =
            use df = new DataFrame(handle)
            let height = df.Height

            if isNull (box predicate) then
                if height = 0L then
                    defaultValue
                elif height > 1L then
                    invalidOp "Sequence contains more than one element."
                else
                    match df.Rows<'TSource>().TryFirstValue() with
                    | ValueSome row -> row
                    | ValueNone -> defaultValue
            elif height = 0L then
                defaultValue
            else
                let mutable enumerator = df.Rows<'TSource>()
                let mutable found = false
                let mutable matchVal = defaultValue

                while enumerator.MoveNext() do
                    if predicate.Invoke enumerator.Current then
                        if found then
                            invalidOp "Sequence contains more than one matching element."

                        matchVal <- enumerator.Current
                        found <- true

                if found then matchVal else defaultValue

        member _.ElementAt<'TSource>(handle: DataFrameHandle, index: int64) : 'TSource =
            if index < 0L then
                raise (ArgumentOutOfRangeException("index", "Index was out of range."))

            use df = new DataFrame(handle)

            df.Rows<'TSource>().Item index

        member _.ElementAtOrDefault<'TSource>(handle: DataFrameHandle, index: int64) : 'TSource =
            if index < 0L then
                Unchecked.defaultof<'TSource>
            else
                use df = new DataFrame(handle)

                match df.Rows<'TSource>().TryGetValueItem index with
                | ValueSome row -> row
                | ValueNone -> Unchecked.defaultof<'TSource>

        member _.Contains<'TSource>
            (handle: DataFrameHandle, item: 'TSource, comparer: IEqualityComparer<'TSource>)
            : bool =
            use df = new DataFrame(handle)

            if df.Height = 0L then
                false
            else
                let comp =
                    if isNull (box comparer) then
                        EqualityComparer<'TSource>.Default :> IEqualityComparer<'TSource>
                    else
                        comparer

                let mutable enumerator = df.Rows<'TSource>()
                let mutable found = false

                while not found && enumerator.MoveNext() do
                    if comp.Equals(enumerator.Current, item) then
                        found <- true

                found

        member _.SequenceEqual<'TSource>
            (handle1: DataFrameHandle, handle2: DataFrameHandle, comparer: IEqualityComparer<'TSource>)
            : bool =
            use df1 = new DataFrame(handle1)
            use df2 = new DataFrame(handle2)

            if df1.Height <> df2.Height then
                false
            elif df1.Height = 0L then
                true
            else
                let comp =
                    if isNull (box comparer) then
                        EqualityComparer<'TSource>.Default :> IEqualityComparer<'TSource>
                    else
                        comparer

                let mutable enum1 = df1.Rows<'TSource>()
                let mutable enum2 = df2.Rows<'TSource>()
                let mutable equal = true

                while equal && enum1.MoveNext() && enum2.MoveNext() do
                    if not (comp.Equals(enum1.Current, enum2.Current)) then
                        equal <- false

                equal

        member _.SequenceEqual<'TSource>
            (handle: DataFrameHandle, second: IEnumerable<'TSource>, comparer: IEqualityComparer<'TSource>)
            : bool =
            if isNull (box second) then
                nullArg "second"

            use df = new DataFrame(handle)

            let comp =
                if isNull (box comparer) then
                    EqualityComparer<'TSource>.Default :> IEqualityComparer<'TSource>
                else
                    comparer

            match second with
            | :? ICollection<'TSource> as c when df.Height <> int64 c.Count -> false
            | :? IReadOnlyCollection<'TSource> as rc when df.Height <> int64 rc.Count -> false
            | _ ->
                let mutable enum1 = df.Rows<'TSource>()
                use enum2 = second.GetEnumerator()
                let mutable equal = true

                while equal && enum1.MoveNext() do
                    if not (enum2.MoveNext() && comp.Equals(enum1.Current, enum2.Current)) then
                        equal <- false

                equal && not (enum2.MoveNext())

        member _.MinBy<'TSource, 'TKey>
            (handle: DataFrameHandle, keySelector: Func<'TSource, 'TKey>, comparer: IComparer<'TKey>)
            : 'TSource =
            if isNull (box keySelector) then
                nullArg "keySelector"

            use df = new DataFrame(handle)

            if df.Height = 0L then
                FSharpRowCursorMaterializer.handleEmptySequence<'TSource>()
            else
                let comp =
                    if isNull (box comparer) then
                        Comparer<'TKey>.Default :> IComparer<'TKey>
                    else
                        comparer

                let mutable enumerator = df.Rows<'TSource>()

                if not (enumerator.MoveNext()) then
                    FSharpRowCursorMaterializer.handleEmptySequence<'TSource>()
                else
                    let mutable minRow = enumerator.Current
                    let mutable minKey = keySelector.Invoke minRow

                    while enumerator.MoveNext() do
                        let curRow = enumerator.Current
                        let curKey = keySelector.Invoke curRow

                        if comp.Compare(curKey, minKey) < 0 then
                            minKey <- curKey
                            minRow <- curRow

                    minRow

        member _.MaxBy<'TSource, 'TKey>
            (handle: DataFrameHandle, keySelector: Func<'TSource, 'TKey>, comparer: IComparer<'TKey>)
            : 'TSource =
            if isNull (box keySelector) then
                nullArg "keySelector"

            use df = new DataFrame(handle)

            if df.Height = 0L then
                FSharpRowCursorMaterializer.handleEmptySequence<'TSource>()
            else
                let comp =
                    if isNull (box comparer) then
                        Comparer<'TKey>.Default :> IComparer<'TKey>
                    else
                        comparer

                let mutable enumerator = df.Rows<'TSource>()

                if not (enumerator.MoveNext()) then
                    FSharpRowCursorMaterializer.handleEmptySequence<'TSource>()
                else
                    let mutable maxRow = enumerator.Current
                    let mutable maxKey = keySelector.Invoke maxRow

                    while enumerator.MoveNext() do
                        let curRow = enumerator.Current
                        let curKey = keySelector.Invoke curRow

                        if comp.Compare(curKey, maxKey) > 0 then
                            maxKey <- curKey
                            maxRow <- curRow

                    maxRow
