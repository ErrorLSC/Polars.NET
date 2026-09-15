namespace Polars.FSharp

open Apache.Arrow

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Series =
    /// <summary>
    /// Map values of the Series using a standard F# function with automatic return DataType inference.
    /// Uses ArrowTypeResolver to map 'U to its corresponding Polars DataType.
    /// </summary>
    /// <param name="mapping">The mapping function applied to each element.</param>
    /// <param name="series">The target Series.</param>
    let inline map (mapping: 'T -> 'U) (series: Series) : Series =
        let returnType = DataType.FromNetType<'U>()
        series.Map<'T, 'U>(mapping, returnType)

    /// <summary>
    /// Map values of the Series using an F# function that handles Option values with automatic return DataType inference.
    /// Uses ArrowTypeResolver to map 'U to its corresponding Polars DataType.
    /// </summary>
    /// <param name="mapping">The mapping function taking and returning F# Option.</param>
    /// <param name="series">The target Series.</param>
    let inline mapOption (mapping: 'T option -> 'U option) (series: Series) : Series =
        let returnType = DataType.FromNetType<'U>()
        series.MapOption<'T, 'U>(mapping, returnType)

    /// <summary>
    /// Map values of the Series using an F# function that handles ValueOption with automatic return DataType inference.
    /// Uses ArrowTypeResolver to map 'U to its corresponding Polars DataType.
    /// </summary>
    /// <param name="mapping">The mapping function taking and returning F# ValueOption.</param>
    /// <param name="series">The target Series.</param>
    let inline mapValueOption (mapping: 'T voption -> 'U voption) (series: Series) : Series =
        let returnType = DataType.FromNetType<'U>()
        series.MapValueOption<'T, 'U>(mapping, returnType)

    /// <summary>
    /// Builds a new Series whose elements are the results of applying the given function
    /// to each element of the Series and its 0-based index.
    /// The return DataType is automatically inferred from 'U using ArrowTypeResolver.
    /// </summary>
    /// <param name="mapping">A function that transforms the 0-based index and current element into a new value.</param>
    /// <param name="series">The target Series.</param>
    /// <returns>A new Series containing the transformed elements.</returns>
    let mapi (mapping: int -> 'T -> 'U) (series: Series) : Series =
        let len = int series.Length
        let result = Array.zeroCreate<'U> len

        for i = 0 to len - 1 do
            let item = series.GetValue<'T>(i)
            result.[i] <- mapping i item

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
                if series.IsNullAt(i) then None
                else Some (series.GetValue<'T>(i))
            result.[i] <- mapping i opt

        // Let SeriesFactory construct the series with null bitmaps from F# options
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
            let vopt =
                if series.IsNullAt(i) then ValueNone
                else ValueSome (series.GetValue<'T>(i))
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

        let result = Array.zeroCreate<'V> len1
        for i = 0 to len1 - 1 do
            let v1 = series1.GetValue<'T>(i)
            let v2 = series2.GetValue<'U>(i)
            result.[i] <- mapping v1 v2

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

        let result = Array.zeroCreate<'V> len1
        for i = 0 to len1 - 1 do
            let v1 = series1.GetValue<'T>(i)
            let v2 = series2.GetValue<'U>(i)
            result.[i] <- mapping i v1 v2

        pl.series (series1.Name + "_mapped2") result
    /// <summary>
    /// Applies a function to each element of the Series without allocating a full managed array,
    /// threading an accumulator argument through the computation.
    /// </summary>
    /// <param name="folder">A function that updates the state given the current element.</param>
    /// <param name="state">The initial state.</param>
    /// <param name="series">The target Series.</param>
    /// <returns>The accumulated state.</returns>
    let fold (folder: 'State -> 'T -> 'State) (state: 'State) (series: Series) : 'State =
        let len = int series.Length
        let mutable acc = state

        if typeof<'T> = typeof<int> then
            let arrow = series.ToArrow() :?> PrimitiveArray<int>
            let span = arrow.Values
            let f = unbox<'State -> int -> 'State> folder
            for i = 0 to span.Length - 1 do
                acc <- f acc span.[i]
        elif typeof<'T> = typeof<float> then
            let arrow = series.ToArrow() :?> PrimitiveArray<double>
            let span = arrow.Values
            let f = unbox<'State -> float -> 'State> folder
            for i = 0 to span.Length - 1 do
                acc <- f acc span.[i]
        elif typeof<'T> = typeof<int64> then
            let arrow = series.ToArrow() :?> PrimitiveArray<int64>
            let span = arrow.Values
            let f = unbox<'State -> int64 -> 'State> folder
            for i = 0 to span.Length - 1 do
                acc <- f acc span.[i]
        else
            for i = 0 to len - 1 do
                acc <- folder acc (series.GetValue<'T>(i))
        acc

    /// <summary>
    /// Reduces the elements of the Series using the specified reduction function without array allocation.
    /// Throws an InvalidOperationException if the Series is empty.
    /// </summary>
    /// <param name="reduction">The reduction function.</param>
    /// <param name="series">The target Series.</param>
    /// <returns>The reduced value.</returns>
    let inline reduce (reduction: 'T -> 'T -> 'T) (series: Series) : 'T =
        let len = int series.Length
        if len = 0 then
            invalidOp "Cannot reduce an empty Series."

        let mutable acc = series.GetValue<'T>(0)
        for i = 1 to len - 1 do
            acc <- reduction acc (series.GetValue<'T>(i))
        acc

    /// <summary>
    /// Applies a function to each element and an accumulator, yielding a new Series of the accumulated values at each step.
    /// </summary>
    /// <param name="folder">The accumulation function.</param>
    /// <param name="state">The initial state value.</param>
    /// <param name="series">The target Series.</param>
    /// <returns>A new Series containing intermediate accumulated results.</returns>
    let inline scan (folder: 'State -> 'T -> 'State) (state: 'State) (series: Series) : Series =
        let len = int series.Length
        let result = Array.zeroCreate<'State> len
        let mutable acc = state

        for i = 0 to len - 1 do
            acc <- folder acc (series.GetValue<'T>(i))
            result.[i] <- acc

        pl.series (series.Name + "_scanned") result

    /// <summary>
    /// Generates a new Series from a state-transition function, up to a maximum length.
    /// Emits None to terminate sequence generation early.
    /// </summary>
    let inline unfold (generator: 'State -> ('T * 'State) option) (initialState: 'State) (maxLen: int) (name: string) : Series =
        let builder = System.Collections.Generic.List<'T>(maxLen)
        let mutable state = initialState
        let mutable running = true
        while running && builder.Count < maxLen do
            match generator state with
            | Some (value, nextState) ->
                builder.Add(value)
                state <- nextState
            | None ->
                running <- false

        let arr = builder.ToArray()
        pl.series name arr

    /// <summary>
    /// Filter a series.
    /// </summary>
    /// <param name="predicate">Boolean expression used to filter the current expression.</param>
    /// <param name="series">The target Series.</param>
    let inline filter(predicate:Expr)(series:Series) =
        series.Filter predicate

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
    let inline zip (other: Series) (self: Series) : ('T * 'U)[] =
        let len = min (int self.Length) (int other.Length)
        let result = Array.zeroCreate<'T * 'U> len
        for i = 0 to len - 1 do
            result.[i] <- self.GetValue<'T>(i), other.GetValue<'U>(i)
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
    let choose (chooser: 'T -> 'U option) (series: Series) : Series =
        let len = int series.Length
        let list = System.Collections.Generic.List<'U>(len)
        for i = 0 to len - 1 do
            match chooser (series.GetValue<'T>(i)) with
            | Some v -> list.Add(v)
            | None -> ()
        pl.series (series.Name + "_chosen") list

    /// <summary>
    /// Tests if any element of the Series satisfies the given predicate, short-circuiting on the first match.
    /// </summary>
    let exists (predicate: 'T -> bool) (series: Series) : bool =
        let len = int series.Length
        let mutable found = false
        let mutable i = 0
        while not found && i < len do
            if predicate (series.GetValue<'T>(i)) then
                found <- true
            else
                i <- i + 1
        found

    /// <summary>
    /// Tests if any element satisfies the Polars boolean expression using native Rust engine execution.
    /// </summary>
    let inline existsExpr (predicate: Expr) (series: Series) : bool =
        series.Filter(predicate).Any() |> Option.defaultValue false

    /// <summary>
    /// Tests if all elements of the Series satisfy the given predicate, short-circuiting on the first failure.
    /// </summary>
    let forall (predicate: 'T -> bool) (series: Series) : bool =
        let len = int series.Length
        let mutable satisfied = true
        let mutable i = 0
        while satisfied && i < len do
            if not (predicate (series.GetValue<'T>(i))) then
                satisfied <- false
            else
                i <- i + 1
        satisfied
    /// <summary>
    /// Tests if all elements satisfy the Polars boolean expression using native Rust engine execution.
    /// </summary>
    let inline forallExpr (predicate: Expr) (series: Series) : bool =
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
            let result = Array.zeroCreate<'U> (len - 1)
            for i = 0 to len - 2 do
                let c = series.GetValue<'T>(int64 i)
                let n = series.GetValue<'T>(int64 i + 1L)
                result.[i] <- mapping c n
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
            match series.TryGetValue<'T>(i) with
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
