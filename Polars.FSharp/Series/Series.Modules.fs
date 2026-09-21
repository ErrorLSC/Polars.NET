namespace Polars.FSharp

open Apache.Arrow
open System
open System.Collections.Generic

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Series =
    /// <summary>
    /// Get the head value of the Series.
    /// </summary>
    let inline head (series: Series) : 'T =
        series.Slice(0, 1UL).GetValue<'T> 0
    /// <summary>
    /// Try to get the head value of the Series.
    /// </summary>
    let inline tryHead (series: Series) : option<'T> =
        series.Slice(0, 1UL).GetValueOption<'T> 0
    /// <summary>
    /// Try to get the head value of the Series with a default value.
    /// </summary>
    let inline tryHeadOrDefault (defaultValue: 'T) (series: Series) : 'T =
        series.Slice(0, 1UL).GetValueOption<'T> 0 |> Option.defaultValue defaultValue
    /// <summary>
    /// Try to get the head value of the Series as a value option.
    /// </summary>
    let inline tryHeadV (series: Series) : voption<'T> =
        series.Slice(0, 1UL).TryGetValue<'T> 0
    /// <summary>
    /// Get the tail value of the Series.
    /// </summary>
    let inline tail (series: Series) : 'T =
        series.Slice(-1, 1UL).GetValue<'T> 0
    /// <summary>
    /// Try to get the tail value of the Series.
    /// </summary>
    let inline tryTail (series: Series) : option<'T> =
        series.Slice(-1, 1UL).GetValueOption<'T> 0
    /// <summary>
    /// Try to get the tail value of the Series with a default value.
    /// </summary>
    let inline tryTailOrDefault (defaultValue: 'T) (series: Series): 'T =
        series.Slice(-1, 1UL).GetValueOption<'T> 0 |> Option.defaultValue defaultValue
    /// <summary>
    /// Try to get the tail value of the Series as a value option.
    /// </summary>
    let inline tryTailV (series: Series) : voption<'T> =
        series.Slice(-1, 1UL).TryGetValue<'T> 0
    /// <summary>
    /// Rechunk the Series to ensure it is contiguous.
    /// </summary>
    /// <param name="series">The target Series.</param>
    let inline rechunk (series: Series) : Series =
        if series.IsContiguous then
            series
        else
            series.Rechunk()
    /// <summary>
    /// Map values of the Series using a standard F# function with automatic return DataType inference.
    /// Uses ArrowTypeResolver to map 'U to its corresponding Polars DataType.
    /// </summary>
    /// <param name="mapping">The mapping function applied to each element.</param>
    /// <param name="series">The target Series.</param>
    let inline map ([<InlineIfLambda>]mapping: 'T -> 'U) (series: Series) : Series =
        series.Map<'T, 'U> mapping

    /// <summary>
    /// Map values of the Series using an F# function that handles Option values with automatic return DataType inference.
    /// Uses ArrowTypeResolver to map 'U to its corresponding Polars DataType.
    /// </summary>
    /// <param name="mapping">The mapping function taking and returning F# Option.</param>
    /// <param name="series">The target Series.</param>
    let inline mapOption ([<InlineIfLambda>]mapping: 'T option -> 'U option) (series: Series) : Series =
        series.MapOption<'T, 'U> mapping

    /// <summary>
    /// Map values of the Series using an F# function that handles ValueOption with automatic return DataType inference.
    /// Uses ArrowTypeResolver to map 'U to its corresponding Polars DataType.
    /// </summary>
    /// <param name="mapping">The mapping function taking and returning F# ValueOption.</param>
    /// <param name="series">The target Series.</param>
    let inline mapValueOption ([<InlineIfLambda>]mapping: 'T voption -> 'U voption) (series: Series) : Series =
        series.MapValueOption<'T, 'U> mapping

    /// Builds a new Series whose elements are the results of applying the given function
    /// to each valid (non-null) element of the Series and its 0-based index.
    /// Null values in the input Series are preserved as Null in the output.
    /// The return DataType is automatically inferred from 'U using ArrowTypeResolver.
    /// </summary>
    /// <param name="mapping">A function that transforms the 0-based index and current element into a new value.</param>
    /// <param name="series">The target Series.</param>
    /// <returns>A new Series containing the transformed elements with nulls preserved.</returns>
    let mapi (mapping: int -> 'T -> 'U) (series: Series) : Series =
        let len = int series.Length
        let result = Array.zeroCreate<'U voption> len

        for i = 0 to len - 1 do
            match series.TryGetValue<'T>(int64 i) with
            | ValueSome item ->
                result.[i] <- ValueSome (mapping i item)
            | ValueNone ->
                result.[i] <- ValueNone

        pl.series (series.Name + "_mapi") result

    /// <summary>
    /// Builds a new Series whose elements are the results of applying the given function
    /// to each optional element and its 0-based index.
    /// </summary>
    /// <param name="mapping">A function that transforms the 0-based index and current option element.</param>
    /// <param name="series">The target Series.</param>
    /// <returns>A new Series containing the transformed elements.</returns>
    let mapiOption (mapping: int -> 'T option -> 'U option) (series: Series) : Series =
        let len = int series.Length
        let result = Array.zeroCreate<'U option> len

        for i = 0 to len - 1 do
            let opt =
                match series.TryGetValue<'T>(int64 i) with
                | ValueSome v -> Some v
                | ValueNone -> None
            result.[i] <- mapping i opt

        pl.series (series.Name + "_mapi") result

    /// <summary>
    /// Builds a new Series whose elements are the results of applying the given function
    /// to each value-optional element and its 0-based index.
    /// </summary>
    /// <param name="mapping">A function that transforms the 0-based index and current value-option element.</param>
    /// <param name="series">The target Series.</param>
    /// <returns>A new Series containing the transformed elements.</returns>
    let mapiValueOption (mapping: int -> 'T voption -> 'U voption) (series: Series) : Series =
        let len = int series.Length
        let result = Array.zeroCreate<'U voption> len

        for i = 0 to len - 1 do
            let vopt = series.TryGetValue<'T>(int64 i)
            result.[i] <- mapping i vopt

        pl.series (series.Name + "_mapi") result
    /// <summary>
    /// Builds a new Series whose elements are the results of applying the given function
    /// to the corresponding pairs of elements from the two Series.
    /// Throws ArgumentException if the two Series have different lengths.
    /// The return DataType is automatically inferred from 'V using ArrowTypeResolver.
    /// </summary>
    /// <param name="mapping">The mapping function applied to each pair of elements.</param>
    /// <param name="series1">The first input Series.</param>
    /// <param name="series2">The second input Series.</param>
    /// <returns>A new Series containing the transformed elements.</returns>
    let map2 (mapping: 'T -> 'U -> 'V) (series1: Series) (series2: Series) : Series =
        let len1 = int series1.Length
        let len2 = int series2.Length
        if len1 <> len2 then
            invalidArg (nameof series2) (sprintf "Series lengths differ: %d vs %d." len1 len2)

        let result = Array.zeroCreate<'V voption> len1
        for i = 0 to len1 - 1 do
            match series1.TryGetValue<'T> i, series2.TryGetValue<'U> i with
            | ValueSome v1, ValueSome v2 ->
                result.[i] <- ValueSome (mapping v1 v2)
            | _ ->
                result.[i] <- ValueNone

        pl.series (series1.Name + "_mapped2") result

    /// <summary>
    /// Builds a new Series whose elements are the results of applying the given function
    /// to the 0-based index and corresponding pairs of elements from the two Series.
    /// Throws ArgumentException if the two Series have different lengths.
    /// The return DataType is automatically inferred from 'V using ArrowTypeResolver.
    /// </summary>
    /// <param name="mapping">The mapping function applied to index and each pair of elements.</param>
    /// <param name="series1">The first input Series.</param>
    /// <param name="series2">The second input Series.</param>
    /// <returns>A new Series containing the transformed elements.</returns>
    let mapi2 (mapping: int -> 'T -> 'U -> 'V) (series1: Series) (series2: Series) : Series =
        let len1 = int series1.Length
        let len2 = int series2.Length
        if len1 <> len2 then
            invalidArg (nameof series2) (sprintf "Series lengths differ: %d vs %d." len1 len2)

        let result = Array.zeroCreate<'V voption> len1
        for i = 0 to len1 - 1 do
            let idx = int64 i
            match series1.TryGetValue<'T> idx, series2.TryGetValue<'U> idx with
            | ValueSome v1, ValueSome v2 ->
                result.[i] <- ValueSome (mapping i v1 v2)
            | _ ->
                result.[i] <- ValueNone

        pl.series (series1.Name + "_mapped2") result
    let inline private foldPrimitive< 'T, 'State when 'T : struct and 'T: (new: unit -> 'T) and 'T :> IEquatable<'T> and 'T :> ValueType >
        ([<InlineIfLambda>]f: 'State -> 'T -> 'State) (acc: 'State) (arrow: PrimitiveArray<'T>) =
            let span = arrow.Values
            let mutable current = acc
            if arrow.NullCount = 0 then
                for i = 0 to span.Length - 1 do
                    current <- f current span.[i]
            else
                for i = 0 to span.Length - 1 do
                    if arrow.IsValid(i) then
                        current <- f current span.[i]
            current
    /// <summary>
    /// Applies a function to each element of the Series
    /// threading an accumulator argument through the computation.
    /// </summary>
    /// <param name="folder">A function that updates the state given the current element.</param>
    /// <param name="state">The initial state.</param>
    /// <param name="series">The target Series.</param>
    /// <returns>The accumulated state.</returns>
    let inline fold ([<InlineIfLambda>]folder: 'State -> 'T -> 'State) (state: 'State) (series: Series) : 'State =
        let len = series.Length
        if len = 0L then
            state
        elif typeof<'T> = typeof<int> then
            let arrow = series.ToArrow() :?> PrimitiveArray<int>
            foldPrimitive (unbox<'State -> int -> 'State> folder) state arrow

        elif typeof<'T> = typeof<float> then
            let arrow = series.ToArrow() :?> PrimitiveArray<double>
            foldPrimitive (unbox<'State -> double -> 'State> folder) state arrow

        elif typeof<'T> = typeof<int64> then
            let arrow = series.ToArrow() :?> PrimitiveArray<int64>
            foldPrimitive (unbox<'State -> int64 -> 'State> folder) state arrow

        elif typeof<'T> = typeof<single> then
            let arrow = series.ToArrow() :?> PrimitiveArray<single>
            foldPrimitive (unbox<'State -> single -> 'State> folder) state arrow

        else
            let mutable acc = state
            for i in 0L .. len - 1L do
                match series.TryGetValue<'T>(i) with
                | ValueSome v -> acc <- folder acc v
                | ValueNone   -> ()
            acc
    /// <summary>
    /// Applies a function to each element of the Series using a span, without allocating a full managed array.
    /// </summary>
    /// <param name="folder">A function that updates the state given the current element.</param>
    /// <param name="state">The initial state.</param>
    /// <param name="series">The target Series.</param>
    /// <returns>The accumulated state.</returns>
    let inline foldSpan< 'T, 'State
        when 'T : unmanaged and 'T : struct and 'T :> ValueType and 'T : (new: unit -> 'T)
        and 'State : unmanaged and 'State : struct and 'State :> ValueType and 'State : (new: unit -> 'State)>
        ([<InlineIfLambda>]folder: 'State -> 'T -> 'State) (state: 'State) (series: Series) : 'State =

        if series.HasNulls() || not series.IsContiguous then
            invalidOp "foldSpan requires non-nullable and contiguous series, call fill or dropNulls or rechunk first."

        let span = series.AsReadOnlySpan<'T>()
        let mutable acc = state
        for i = 0 to span.Length - 1 do
            acc <- folder acc span.[i]
        acc
    /// <summary>
    /// Reduces the elements of the Series using the specified reduction function without array allocation.
    /// Throws an InvalidOperationException if the Series is empty.
    /// </summary>
    /// <param name="reduction">The reduction function.</param>
    /// <param name="series">The target Series.</param>
    /// <returns>The reduced value.</returns>
    let inline reduce ([<InlineIfLambda>]reduction: 'T -> 'T -> 'T) (series: Series) : 'T =
        let len = series.Length
        if len = 0L then
            invalidOp "Cannot reduce an empty Series."

        let mutable acc = ValueNone
        let mutable i = 0L

        while acc.IsNone && i < len do
            match series.TryGetValue<'T>(i) with
            | ValueSome v -> acc <- ValueSome v
            | ValueNone -> i <- i + 1L

        match acc with
        | ValueNone ->
            invalidOp "Cannot reduce a Series containing only null values."
        | ValueSome initial ->
            let mutable current = initial
            for idx in (i + 1L) .. (len - 1L) do
                match series.TryGetValue<'T> idx with
                | ValueSome v -> current <- reduction current v
                | ValueNone -> ()
            current

    /// <summary>
    /// Applies a function to each element and an accumulator, yielding a new Series of the accumulated values at each step.
    /// </summary>
    /// <param name="folder">The accumulation function.</param>
    /// <param name="state">The initial state value.</param>
    /// <param name="series">The target Series.</param>
    /// <returns>A new Series containing intermediate accumulated results.</returns>
    let inline scan ([<InlineIfLambda>]folder: 'State -> 'T -> 'State) (state: 'State) (series: Series) : Series =
        let len = int series.Length
        let result = Array.zeroCreate<'State voption> len
        let mutable acc = state

        for i in 0 .. (len - 1) do
            match series.TryGetValue<'T> i with
            | ValueSome v ->
                acc <- folder acc v
                result.[i] <- ValueSome acc
            | ValueNone ->
                result.[i] <- ValueNone

        pl.series (series.Name + "_scanned") result
    /// <summary>
    /// Applies a state-transition function to each element of the Series, accumulating a result.
    /// </summary>
    /// <param name="state">The initial state value.</param>
    /// <param name="series">The target Series.</param>
    /// <returns>A new Series containing intermediate accumulated results.</returns>
    let inline scanSpan< 'T, 'State
        when 'T : unmanaged and 'T : struct and 'T :> ValueType and 'T : (new: unit -> 'T)
        and 'State : unmanaged and 'State : struct and 'State :> ValueType and 'State : (new: unit -> 'State)>
        ([<InlineIfLambda>]folder: 'State -> 'T -> 'State) (state: 'State) (series: Series) : Series =

        if series.HasNulls() || not series.IsContiguous then
            invalidOp "scanSpan requires non-nullable and contiguous series, call fill or dropNulls or rechunk first."

        let span = series.AsReadOnlySpan<'T>()
        let result = Array.zeroCreate<'State> span.Length
        let mutable acc = state
        for i in 0 .. span.Length - 1 do
            acc <- folder acc span.[i]
            result.[i] <- acc

        Series.create(series.Name + "_scanned", result)
    /// <summary>
    /// Generates a new Series from a state-transition function, up to a maximum length.
    /// Emits None to terminate sequence generation early.
    /// </summary>
    let inline unfold ([<InlineIfLambda>]generator: 'State -> ('T * 'State) voption) (initialState: 'State) (maxLen: int) (name: string) : Series =
        let buffer = Array.zeroCreate<'T> maxLen
        let mutable state = initialState
        let mutable count = 0
        let mutable running = true

        while running && count < maxLen do
            match generator state with
            | ValueSome (value, nextState) ->
                buffer.[count] <- value
                state <- nextState
                count <- count + 1
            | ValueNone ->
                running <- false

        let effectiveSpan = ReadOnlySpan<'T>(buffer, 0, count)
        Series.create(name, effectiveSpan)

    /// <summary>
    /// Filter a series.
    /// </summary>
    /// <param name="predicate">Boolean expression used to filter the current expression.</param>
    /// <param name="series">The target Series.</param>
    let inline filter(predicate:Series)(series:Series) =
        series.Filter predicate
    /// <summary>
    /// Filters a Series using a strongly-typed F# predicate function ('T -> bool).
    /// </summary>
    let inline filterWith<'T> ([<InlineIfLambda>]predicate: 'T -> bool) (series: Series) : Series =
        series.FilterWith<'T> predicate
    /// <summary>
    /// Cast a series to a specific Polars DataType.
    /// </summary>
    /// <param name="dtype">The target Polars DataType.</param>
    /// <param name="s">The source Series.</param>
    let inline cast (dtype: DataType) (s: Series) = s.Cast dtype

    /// <summary>
    /// Cast a series to a specific .NET type.
    /// </summary>
    /// <param name="s">The source Series.</param>
    let inline castWithNetType<'T> (s: Series) = s.Cast<'T>()

    /// <summary>
    /// Compute the dot/inner product between two Series.
    /// </summary>
    let inline dot<'T> (other: Series) (s: Series) = s.Dot<'T>(other)

    /// <summary>
    /// Reshape this Series to a flat Series or an Array Series.
    /// </summary>
    let inline reshape (shape: int64 seq) (s: Series) = s.Reshape(shape)

    /// <summary>
    /// Reverse the order of the elements in the Series.
    /// </summary>
    let inline reverse (s: Series) = s.Reverse()

    /// <summary>
    /// Take values from self or other based on the given mask.
    /// Where mask evaluates true, take values from self. Where mask evaluates false, take values from other.
    /// </summary>
    let inline zipWith (mask: Series) (other: Series) (self:Series) = self.ZipWith(mask, other)

    /// <summary>
    /// Combines two Series into an array of paired tuples (traditional functional zip).
    /// Truncates to the length of the shorter Series if lengths differ.
    /// </summary>
    /// <param name="other">The second Series.</param>
    /// <param name="self">The first Series.</param>
    /// <returns>An array containing element-wise tuples ('T * 'U).</returns>
    let inline zip (other: Series) (self: Series) : ('T voption * 'U voption)[] =
        let len = min self.Length other.Length |> int
        let result = Array.zeroCreate<'T voption * 'U voption> len

        for i in 0 .. (len - 1) do
            let v1 =
                match self.TryGetValue<'T> i with
                | ValueSome v -> ValueSome v
                | ValueNone -> ValueNone
            let v2 =
                match other.TryGetValue<'U> i with
                | ValueSome v -> ValueSome v
                | ValueNone -> ValueNone
            result.[i] <- (v1, v2)

        result

    /// <summary>
    /// Converts the Series to a DataFrame with a single column.
    /// </summary>
    /// <param name="series">The target Series.</param>
    let inline toFrame (series: Series) : DataFrame =
        series.ToFrame()

    /// <summary>
    /// Converts the Series to an Arrow array.
    /// </summary>
    /// <param name="series">The target Series.</param>
    let inline toArrow (series: Series) : IArrowArray =
        series.ToArrow()

    /// <summary>
    /// Convert the Series to a strongly-typed managed array.
    /// Type 'T is inferred automatically from the call-site context.
    /// </summary>
    let inline toArray<'T> (series: Series) : 'T[] =
        series.ToArray<'T>()
    /// <summary>
    /// Convert the Series to Expr.
    /// </summary>
    let inline toExpr(series:Series) :Expr =
        pl.litSeries series
    /// <summary>
    /// Prints the Series to the console.
    /// </summary>
    let inline show (series: Series) : Series =
        series.Show()
        series

    /// <summary>
    /// Returns the length of the Series.
    /// </summary>
    let inline length (series: Series) : int =
        int series.Length

    /// <summary>
    /// Renames the Series.
    /// </summary>
    let inline rename (name: string) (series: Series) : Series =
        series.Rename name

    /// <summary>
    /// Returns the unique values of the Series.
    /// </summary>
    let inline unique (series: Series) : Series =
        series.Unique false

    /// <summary>
    /// Returns the unique values of the Series with the order maintained.
    /// </summary>
    let inline uniqueWithOrder (series: Series) : Series =
        series.Unique true

    /// <summary>
    /// Drops null values from the Series.
    /// </summary>
    let inline dropNulls (series: Series) : Series =
        series.DropNulls()

    /// <summary>
    /// Drops NaN values from the Series.
    /// </summary>
    let inline dropNans (series: Series) : Series =
        series.DropNans()

    /// <summary>
    /// Slices the Series.
    /// </summary>
    let inline slice (start: int) (length: int) (series: Series) : Series =
        series.Slice(start, uint64 length)

    /// <summary>
    /// Appends the Series to another Series.
    /// </summary>
    let inline append (other: Series) (self: Series) : Series =
        self.Append other

    /// <summary>
    /// Creates a new Series of the specified length filled with the value at the given index.
    /// </summary>
    /// <param name="index">The 0-based index of the value to replicate.</param>
    /// <param name="length">The length of the newly created Series.</param>
    /// <param name="series">The source Series.</param>
    /// <returns>A new Series filled with the replicated value.</returns>
    let inline replicateFromIndex (index: int) (length: int) (series: Series) : Series =
        series.NewFromIndex(index, length)

    /// <summary>
    /// Aggregate values into a list.
    /// Result is a Series with 1 row containing a List of all values.
    /// </summary>
    let inline implode (series: Series) : Series =
        series.Implode()
    /// <summary>
    /// Explode a list column into multiple rows.
    /// The resulting Series will be longer than the original.
    /// </summary>
    let inline explode (series: Series) : Series =
        series.Explode()

    /// <summary>
    /// Unnest a Struct series into a DataFrame.
    /// Shortcut for <see cref="SeriesStructOps.Unnest"/>.
    /// </summary>
    let inline unnest (series: Series) : DataFrame =
        series.Unnest()

    /// <summary>
    /// Shift the values in the Series by a given offset.
    /// </summary>
    let inline shift (offset: int) (series: Series) : Series =
        series.Shift(offset)

    /// <summary>
    /// Calculate the difference with a given period.
    /// </summary>
    let inline diff(offset:int) (series: Series) : Series =
        series.Diff(offset)

    /// <summary>
    /// Get the top K values from the Series.
    /// </summary>
    let inline topK (k: int) (series: Series) : Series =
        series.TopK(k)

    /// <summary>
    /// Get the bottom K values from the Series.
    /// </summary>
    let inline bottomK (k: int) (series: Series) : Series =
        series.BottomK(k)

    /// <summary>
    /// Sort the values in the Series in ascending order.
    /// </summary>
    let inline sortAscending (series: Series) : Series =
        series.Sort(descending=false)

    /// <summary>
    /// Sort the values in the Series in descending order.
    /// </summary>
    let inline sortDescending (series: Series) : Series =
        series.Sort(descending=true)
    /// <summary>
    /// Applies a function to each element of the Series, returning a new Series
    /// comprised of the results for each element where the function returns Some.
    /// The return DataType is inferred automatically from 'U.
    /// </summary>
    let inline choose ([<InlineIfLambda>]chooser: 'T -> 'U voption) (series: Series) : Series =
        let len = int series.Length
        let buffer = Array.zeroCreate<'U> len
        let mutable count = 0

        for i in 0 .. (len - 1) do
            match series.TryGetValue<'T> i with
            | ValueSome v ->
                match chooser v with
                | ValueSome u ->
                    buffer.[count] <- u
                    count <- count + 1
                | ValueNone -> ()
            | ValueNone -> ()

        let effectiveSpan = ReadOnlySpan<'U>(buffer, 0, count)
        Series.create(series.Name + "_chosen", effectiveSpan)
    /// <summary>
    /// Applies a function to each element of the Series, returning a new Series containing only the elements for which the function returns Some.
    /// </summary>
    /// <param name="chooser">The function to apply to each element.</param>
    /// <param name="series">The input Series.</param>
    /// <returns>A new Series containing only the elements for which the function returns Some.</returns>
    let inline chooseSpan<'T, 'U when 'T : unmanaged and 'T : struct and 'T :> ValueType and 'T : (new: unit -> 'T)
    and 'U : unmanaged and 'U : struct and 'U :> ValueType and 'U : (new: unit -> 'U)>
        ([<InlineIfLambda>] chooser: 'T -> 'U voption) (series: Series) : Series =

        if series.HasNulls() || not series.IsContiguous then
            invalidOp "chooseSpan requires non-nullable and contiguous series, call fill or dropNulls or rechunk first."

        let inSpan = series.AsReadOnlySpan<'T>()
        let len = inSpan.Length
        let buffer = Array.zeroCreate<'U> len
        let mutable count = 0

        for i = 0 to len - 1 do
            match chooser inSpan.[i] with
            | ValueSome u ->
                buffer.[count] <- u
                count <- count + 1
            | ValueNone -> ()

        let effectiveSpan = ReadOnlySpan<'U>(buffer, 0, count)
        Series.create(series.Name + "_chosen", effectiveSpan)

    /// <summary>
    /// Tests if any element of the Series satisfies the given predicate, short-circuiting on the first match.
    /// </summary>
    let exists (predicate: 'T -> bool) (series: Series) : bool =
        let len = series.Length
        let mutable found = false
        let mutable i = 0L
        while not found && i < len do
            match series.TryGetValue<'T> i with
            | ValueSome v when predicate v ->
                found <- true
            | _ ->
                i <- i + 1L
        found

    /// <summary>
    /// Tests if any element satisfies the boolean predicate using native Rust engine execution.
    /// </summary>
    let inline existsSeries (predicate: Series) (series: Series) : bool =
        series.Filter(predicate).Any() |> Option.defaultValue false
    /// <summary>
    /// Returns true if the Series contains the given value.
    /// </summary>
    let inline contains(value: 'T) (series: Series) : bool =
        let len = series.Length
        if len = 0L then
            false
        else
            let comparer = EqualityComparer<'T>.Default
            let mutable found = false
            let mutable i = 0L

            // Short-circuit: stop scanning the moment a match is found
            while not found && i < len do
                match series.TryGetValue<'T>(i) with
                | ValueSome v when comparer.Equals(v, value) -> found <- true
                | _ -> ()
                i <- i + 1L

            found

    /// <summary>
    /// Tests if all elements of the Series satisfy the given predicate, short-circuiting on the first failure.
    /// </summary>
    let forall (predicate: 'T -> bool) (series: Series) : bool =
        let len = series.Length
        let mutable satisfied = true
        let mutable i = 0L
        while satisfied && i < len do
            match series.TryGetValue<'T> i with
            | ValueSome v when not (predicate v) ->
                satisfied <- false
            | _ ->
                i <- i + 1L
        satisfied
    /// <summary>
    /// Tests if all elements satisfy the boolean Series using native Rust engine execution.
    /// </summary>
    let inline forallSeries (predicate: Series) (series: Series) : bool =
        // (predicate.Not().Any() == false) or length matching
        let filtered = series.Filter(predicate)
        filtered.Length = series.Length
    /// <summary>
    /// Applies a function to each pair of adjacent elements (x_i, x_{i+1}), yielding a Series of length (N - 1).
    /// </summary>
    let pairwiseMap (mapping: 'T -> 'T -> 'U) (series: Series) : Series =
        let len = int series.Length
        if len < 2 then
            pl.series (series.Name + "_pairwise") Array.empty<'U>
        else
            let count = len - 1
            let result = Array.zeroCreate<'U voption> count
            for i in 0 .. (len - 2) do
                match series.TryGetValue<'T> i, series.TryGetValue<'T> (i + 1 |> int64) with
                | ValueSome c, ValueSome n ->
                    result.[i] <- ValueSome (mapping c n)
                | _ ->
                    result.[i] <- ValueNone
            pl.series (series.Name + "_pairwise") result

    /// <summary>
    /// Returns the first non-null element for which the given function returns true,
    /// or None if no such element is found.
    /// </summary>
    let tryFind (predicate: 'T -> bool) (series: Series) : 'T option =
        let len = series.Length
        let mutable result = None
        let mutable i = 0L
        while result.IsNone && i < len do
            match series.TryGetValue<'T> i with
            | ValueSome v when predicate v ->
                result <- Some v
            | _ ->
                i <- i + 1L
        result

    /// <summary>
    /// Returns the 0-based index of the first non-null element satisfying the predicate,
    /// or None if no such element is found.
    /// </summary>
    let tryFindIndex (predicate: 'T -> bool) (series: Series) : int64 option =
        let len = series.Length
        let mutable result = None
        let mutable i = 0L
        while result.IsNone && i < len do
            match series.TryGetValue<'T>(i) with
            | ValueSome v when predicate v ->
                result <- Some i
            | _ ->
                i <- i + 1L
        result

    /// <summary>
    /// ValueOption variant of tryFind to avoid reference-type heap allocation.
    /// </summary>
    let tryFindV (predicate: 'T -> bool) (series: Series) : 'T voption =
        let len = series.Length
        let mutable result = ValueNone
        let mutable i = 0L
        while result.IsNone && i < len do
            match series.TryGetValue<'T>(i) with
            | ValueSome v when predicate v ->
                result <- ValueSome v
            | _ ->
                i <- i + 1L
        result
    /// <summary>
    /// Pipeline alias for Series.IterOpt: ('T voption -> unit) -> Series -> unit.
    /// </summary>
    let inline iterOpt<'T> ([<InlineIfLambda>]action: 'T voption -> unit) (series: Series) : unit =
        series.IterOpt<'T> action

    /// <summary>
    /// Pipeline alias for Series.IteriOpt: (int64 -> 'T voption -> unit) -> Series -> unit.
    /// </summary>
    let inline iteriOpt<'T> ([<InlineIfLambda>]action: int64 -> 'T voption -> unit) (series: Series) : unit =
        series.IteriOpt<'T> action

    /// <summary>
    /// Pipeline alias for Series.Iter: ('T -> unit) -> Series -> unit.
    /// </summary>
    let inline iter<'T> ([<InlineIfLambda>]action: 'T -> unit) (series: Series) : unit =
        series.Iter<'T> action

    /// <summary>
    /// Pipeline alias for Series.Iteri: (int64 -> 'T -> unit) -> Series -> unit.
    /// </summary>
    let inline iteri<'T> ([<InlineIfLambda>]action: int64 -> 'T -> unit) (series: Series) : unit =
        series.Iteri<'T> action

    /// <summary>
    /// Pipeline alias for Series.Iter2Opt: ('T1 voption -> 'T2 voption -> unit) -> Series -> Series -> unit.
    /// </summary>
    let inline iter2Opt<'T1, 'T2> ([<InlineIfLambda>]action: 'T1 voption -> 'T2 voption -> unit) (s1: Series) (s2: Series) : unit =
        s1.Iter2Opt<'T1, 'T2>(s2, action)

    /// <summary>
    /// Pipeline alias for Series.Iter2: ('T1 -> 'T2 -> unit) -> Series -> Series -> unit.
    /// </summary>
    let inline iter2<'T1, 'T2> ([<InlineIfLambda>]action: 'T1 -> 'T2 -> unit) (s1: Series) (s2: Series) : unit =
        s1.Iter2<'T1, 'T2>(s2, action)

    /// <summary>
    /// Creates a new Series of length n filled with the zero value of type 'T.
    /// </summary>
    /// <param name="n">The length of the resulting Series.</param>
    /// <returns>A new Series filled with typed zeros.</returns>
    let inline zeroCreate<'T> (n: int) : Series =
        pl.zerosAsSeries<'T> n

    /// <summary>
    /// Creates a new Series of length n filled with the one value of type 'T.
    /// </summary>
    /// <param name="n">The length of the resulting Series.</param>
    /// <returns>A new Series filled with typed ones.</returns>
    let inline oneCreate<'T> (n: int) : Series =
        pl.onesAsSeries<'T> n

    /// <summary>
    /// Creates a new Series of length n filled with a specified scalar constant.
    /// </summary>
    /// <param name="n">The length of the resulting Series.</param>
    /// <param name="value">The constant scalar value.</param>
    /// <returns>A new Series repeating the scalar value.</returns>
    let inline replicate<^T when (^T or LitMechanism) : (static member ($) : LitMechanism * ^T -> Expr)>
            (n: int) (value: ^T) : Series =
        let targetType = DataType.FromNetType typeof< ^T >
        pl.repeatAsSeries (pl.lit value) n (Some targetType)
    /// <summary>
    /// Creates a new Series of length n where each element is computed by calling the initializer on its row index.
    /// Idiomatic functional counterpart to Array.init / List.init.
    /// </summary>
    /// <param name="name">The name of the resulting Series.</param>
    /// <param name="n">The number of elements to create.</param>
    /// <param name="initializer">A function that computes the element at each index (0 to n - 1).</param>
    /// <returns>A new Series populated with the computed elements.</returns>
    let inline init (name: string) (n: int) ([<InlineIfLambda>]initializer: int -> 'T) : Series =
        let data = Array.init n initializer
        Series.create (name, data)
    /// <summary>
    /// Overload that creates an unnamed or default-named Series of length n.
    /// </summary>
    let inline initUnnamed (n: int) ([<InlineIfLambda>]initializer: int -> 'T) : Series =
        init "" n initializer
    /// <summary>
    /// Generate a range of integers as a series.
    /// </summary>
    /// <param name="start">Start of the range (inclusive).</param>
    /// <param name="end">End of the range (exclusive). If set to Null (default), the value of start is used and start is set to 0.</param>
    /// <param name="step">Step size of the range.</param>
    let intRange<'T>(name:string) (start:int64) (endRange:int64) (step:int64) =
        let exp = pl.intRange<'T> start endRange step
        Series.ofExpr(exp).Rename(name).SetSorted(descending = (step < 0))
    /// <summary>
    /// Generate a date range.
    /// </summary>
    /// <param name="start">Lower bound of the date range.</param>
    /// <param name="end">Upper bound of the date range.</param>
    /// <param name="interval">Interval of the range periods, “1w2d” # 1 week, 2 days.Default is 1 day.</param>
    /// <param name="closed">Define which sides of the range are closed</param>
    /// <returns>Series of data type Date</returns>
    let dateRange (name:string)(start:DateOnly)(endRange:DateOnly)(interval:Dur)(closed:ClosedInterval) =
        let expr = pl.dateRange start endRange interval closed
        Series.ofExpr(expr).Rename name

    /// <summary>
    /// Converts a datetime range expression to a Series with the specified name.
    /// </summary>
    /// <param name="name">Name of the resulting Series.</param>
    /// <param name="start">Start datetime of the range.</param>
    /// <param name="endRange">End datetime of the range.</param>
    /// <param name="interval">Interval between datetime values.</param>
    /// <param name="closed">Closed window of the range.</param>
    /// <param name="unit">Time unit of the resulting Datetime data type.</param>
    /// <param name="timeZone">Time zone of the resulting Datetime data type.</param>
    let datetimeRange(name:string)(start:DateTime)(endRange:DateTime)(interval:Dur)(closed:ClosedInterval)(unit:TimeUnit)(timeZone:string option) =
        let expr = pl.datetimeRange start endRange interval closed unit timeZone
        Series.ofExpr(expr).Rename name

    /// <summary>
    /// Converts a time range expression to a Series with the specified name.
    /// </summary>
    /// <param name="name">Name of the resulting Series.</param>
    /// <param name="start">Start time of the range.</param>
    /// <param name="endRange">End time of the range.</param>
    /// <param name="interval">Interval between time values.</param>
    /// <param name="closed">Closed window of the range.</param>
    let timeRange(name:string)(start:TimeOnly)(endRange:TimeOnly)(interval:Dur)(closed:ClosedInterval) =
        let expr = pl.timeRange start endRange interval closed
        Series.ofExpr(expr).Rename name

    /// <summary>
    /// Converts a linear space expression to a Series with the specified name.
    /// </summary>
    /// <param name="name">Name of the resulting Series.</param>
    /// <param name="start">Lower bound of the linear space.</param>
    /// <param name="endRange">Upper bound of the linear space.</param>
    /// <param name="numSamples">Number of samples to generate.</param>
    /// <param name="closed">Whether the intervals are closed or open.</param>
    let linearSpace(name:string)(start:Expr)(endRange:Expr)(numSamples:int)(closed:ClosedInterval) =
        let expr = pl.linearSpace start endRange numSamples closed
        Series.ofExpr(expr).Rename name
    /// <summary>
    /// Clamps series values between scalar bounds.
    /// </summary>
    let inline clamp (lowerBound: ^T) (upperBound: ^T) (series: Series) : Series =
        series.Clip(lowerBound, upperBound)
    /// <summary>
    /// Replace values in the series using replacement expressions.
    /// </summary>
    /// <param name="old">The expression or series identifying values to replace.</param>
    /// <param name="newExpr">The expression or series containing replacement values.</param>
    /// <param name="series">The target Series.</param>
    /// <returns>A new Series with replaced values.</returns>
    let replace (old: Expr) (newExpr: Expr) (series: Series) : Series =
        series.Replace(old, newExpr)

    /// <summary>
    /// Replace scalar values in the series using SRTP literals.
    /// </summary>
    let inline replaceScalar (oldVal: ^Old) (newVal: ^New) (series: Series) : Series =
        series.Replace(pl.lit oldVal, pl.lit newVal)

    /// <summary>
    /// Strictly replace values in the series. Throws an error if unmatched unless defaultExpr is provided.
    /// </summary>
    let replaceStrict (old: Expr) (newExpr: Expr) (defaultExpr: Expr option) (returnDataType: DataTypeExpr option) (series: Series) : Series =
        series.ReplaceStrict(old, newExpr, ?defaultExpr = defaultExpr, ?returnDataType = returnDataType)

    /// <summary>
    /// Fills floating-point NaN values with a float (double) literal.
    /// Default floating-point pipeline operator.
    /// </summary>
    let fillNan (fillValue: float) (series: Series) : Series =
        series.FillNan fillValue

    /// <summary>
    /// Fills floating-point NaN values with a float32 (single) literal.
    /// </summary>
    let fillNanSingle (fillValue: float32) (series: Series) : Series =
        series.FillNan fillValue

    /// <summary>
    /// Fills floating-point NaN values with values from another Series.
    /// </summary>
    let fillNanWith (fillSeries: Series) (series: Series) : Series =
        series.FillNan fillSeries

    /// <summary>
    /// Fills floating-point NaN values using an expression.
    /// </summary>
    let fillNanExpr (expr: Expr) (series: Series) : Series =
        series.FillNan expr

    /// <summary>
    /// Fills null values with a scalar literal using SRTP.
    /// </summary>
    let inline fillNull (value: ^T) (series: Series) : Series =
        series.FillNull(pl.lit value)

    /// <summary>
    /// Fills null values with another Series (coalesce).
    /// </summary>
    let fillNullWith (fillSeries: Series) (series: Series) : Series =
        series.FillNull fillSeries

    /// <summary>
    /// Fills null values using a dynamic expression.
    /// </summary>
    let fillNullExpr (expr: Expr) (series: Series) : Series =
        series.FillNull expr

    let fillNullStrategy (strategy: FillNullStrategy)(limit:int option) (series: Series) : Series =
        series.FillNull (strategy,?limit=limit)
    /// <summary>
    /// Forward-fills null values up to an consecutive limit.
    /// </summary>
    let forwardFill (limit: int) (series: Series) : Series =
        series.FillNull(FillNullStrategy.Forward, limit)

    /// <summary>
    /// Backward-fills null values up to an consecutive limit.
    /// </summary>
    let backwardFill (limit: int) (series: Series) : Series =
        series.FillNull(FillNullStrategy.Backward,limit)
    /// <summary>
    /// Interpolates intermediate null values linearly or using nearest neighbors.
    /// </summary>
    let interpolate (method: InterpolationMethod) (series: Series) : Series =
        series.Interpolate(method = method)

    /// <summary>
    /// Interpolates intermediate values guided by an independent variable series.
    /// </summary>
    let interpolateBy (by: Series) (series: Series) : Series =
        series.InterpolateBy by
