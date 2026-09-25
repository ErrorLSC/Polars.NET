namespace Polars.FSharp

open System.Reflection
open Microsoft.FSharp.Reflection
open System.Linq.Expressions
open System.Collections.Generic
open Polars.NET.Core
open System
open System.Runtime.CompilerServices

/// <summary>
/// Fast typed field extractor that enforces null checking on non-Option fields.
/// Marked public inside assembly to guarantee reflection accessibility.
/// </summary>
type FSharpRowExtractor =
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    static member ExtractField<'Field>(col: Series, rowIdx: int64, acceptsNull: bool, fieldName: string) : 'Field =
        if not acceptsNull && col.IsNullAt(rowIdx, uncheck = true) then
            invalidOp $"Record field '{fieldName}' does not accept Option, but column '{col.Name}' contains null at row {rowIdx}."
        col.GetValue<'Field>(rowIdx, true)

type internal FSharpRowMapper<'T>() =

    static let extractFieldMethodDef : MethodInfo =
        let methods = typeof<FSharpRowExtractor>.GetMethods(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Static)
        methods
        |> Array.find (fun m -> m.Name = "ExtractField" && m.IsGenericMethodDefinition)

    /// Collects all flattened field/property names required by target type 'T
    static let rec collectRequiredNames (t: Type) : string[] =
        if FSharpType.IsTuple(t) then
            FSharpType.GetTupleElements(t)
            |> Array.collect collectRequiredNames
        elif t.IsValueType && not (isNull t.FullName) && t.FullName.StartsWith("System.ValueTuple`") then
            t.GetGenericArguments()
            |> Array.collect collectRequiredNames
        elif FSharpType.IsRecord(t, true) then
            FSharpType.GetRecordFields(t, true)
            |> Array.map (fun f -> f.Name)
        else
            t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
            |> Array.filter (fun p -> p.CanWrite)
            |> Array.map (fun p -> p.Name)

    static let columnNames : string[] = collectRequiredNames typeof<'T>

    // Compiled factory signature: Func<Series[], int64, 'T>
    static let compiledHydrator : Func<Series[], int64, 'T> =
        let targetType = typeof<'T>
        let colsParam = Expression.Parameter(typeof<Series[]>, "cols")
        let rowIdxParam = Expression.Parameter(typeof<int64>, "rowIdx")

        // Helper to extract a single primitive/scalar column value
        let createExtractExpr (colIdx: int) (propType: Type) (fieldName: string) =
            let isOption =
                propType.IsGenericType &&
                (propType.GetGenericTypeDefinition() = typedefof<option<_>> ||
                 propType.GetGenericTypeDefinition() = typedefof<voption<_>>)
            let isList =
                propType.IsGenericType && propType.GetGenericTypeDefinition() = typedefof<list<_>>
            let isArray = propType.IsArray
            let isNullable = Nullable.GetUnderlyingType(propType) <> null
            let acceptsNull = isOption || isList || isArray || isNullable

            let closedExtractMethod = extractFieldMethodDef.MakeGenericMethod([| propType |])
            let colAccess = Expression.ArrayIndex(colsParam, Expression.Constant(colIdx))
            let acceptsNullConst = Expression.Constant(acceptsNull, typeof<bool>)
            let fieldNameConst = Expression.Constant(fieldName, typeof<string>)

            Expression.Call(closedExtractMethod, colAccess, rowIdxParam, acceptsNullConst, fieldNameConst) :> Expression

        // Helper to construct a single non-tuple instance (Record or Class/Struct)
        let rec buildSingleObjectExpr (t: Type) (nameLookup: string -> int option) : Expression =
            if FSharpType.IsRecord(t, true) then
                let fields = FSharpType.GetRecordFields(t, true)
                let ctors = t.GetConstructors(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
                let recordCtor =
                    ctors
                    |> Array.tryFind (fun c -> c.GetParameters().Length = fields.Length)
                    |> Option.defaultWith (fun () -> ctors.[0])

                let ctorArgs =
                    fields
                    |> Array.map (fun f ->
                        match nameLookup f.Name with
                        | Some colIdx -> createExtractExpr colIdx f.PropertyType f.Name
                        | None -> Expression.Default(f.PropertyType) :> Expression
                    )
                Expression.New(recordCtor, ctorArgs) :> Expression

            else
                // Positional constructor (C# Record / Immutable DTO fallback)
                let ctors = t.GetConstructors(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
                let primaryCtorOpt =
                    ctors
                    |> Array.filter (fun c -> c.GetParameters().Length > 0)
                    |> Array.sortByDescending (fun c -> c.GetParameters().Length)
                    |> Array.tryHead

                match primaryCtorOpt with
                | Some primaryCtor ->
                    let ctorParams = primaryCtor.GetParameters()
                    let ctorArgs =
                        ctorParams
                        |> Array.map (fun p ->
                            match nameLookup p.Name with
                            | Some colIdx -> createExtractExpr colIdx p.ParameterType p.Name
                            | None -> Expression.Default(p.ParameterType) :> Expression
                        )
                    Expression.New(primaryCtor, ctorArgs) :> Expression

                | None ->
                    // Parameterless constructor + mutable properties fallback
                    let defaultCtor = t.GetConstructor(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance, null, Type.EmptyTypes, null)
                    let instanceVar = Expression.Variable(t, "inst")
                    let newExpr =
                        if not (isNull defaultCtor) then Expression.New(defaultCtor)
                        else Expression.New(t)

                    let assignInst = Expression.Assign(instanceVar, newExpr)
                    let blockExprs = List<Expression>()
                    blockExprs.Add(assignInst)

                    let props =
                        t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                        |> Array.filter (fun p -> p.CanWrite)

                    for prop in props do
                        match nameLookup prop.Name with
                        | Some colIdx ->
                            let valExpr = createExtractExpr colIdx prop.PropertyType prop.Name
                            let assignProp = Expression.Assign(Expression.Property(instanceVar, prop), valExpr)
                            blockExprs.Add(assignProp)
                        | None -> ()

                    blockExprs.Add(instanceVar)
                    Expression.Block([| instanceVar |], blockExprs) :> Expression

        // Dynamic column lookup helper (resolved by matching series name at runtime or by schema position)
        let nameLookup (propName: string) =
            let idx = Array.tryFindIndex (fun (c: string) -> String.Equals(c, propName, StringComparison.OrdinalIgnoreCase)) columnNames
            idx

        // Top-level build branch
        let rootExpr : Expression =
            // 1. Reference Tuple (F# Standard Tuple)
            if FSharpType.IsTuple(targetType) then
                let elemTypes = FSharpType.GetTupleElements(targetType)
                let tupleCtor = targetType.GetConstructor(elemTypes)
                let subArgs = elemTypes |> Array.map (fun subT -> buildSingleObjectExpr subT nameLookup)
                Expression.New(tupleCtor, subArgs) :> Expression

            // 2. Struct Tuple (System.ValueTuple<...>)
            elif targetType.IsValueType && not (isNull targetType.FullName) && targetType.FullName.StartsWith("System.ValueTuple`") then
                let elemTypes = targetType.GetGenericArguments()
                let tupleCtor =
                    targetType.GetConstructor(elemTypes)
                    |> Option.ofObj
                    |> Option.defaultWith (fun () ->
                        targetType.GetConstructors(BindingFlags.Public ||| BindingFlags.Instance).[0]
                    )
                let subArgs = elemTypes |> Array.map (fun subT -> buildSingleObjectExpr subT nameLookup)
                Expression.New(tupleCtor, subArgs) :> Expression

            // 3. Single record or object
            else
                buildSingleObjectExpr targetType nameLookup

        let lambda = Expression.Lambda<Func<Series[], int64, 'T>>(rootExpr, colsParam, rowIdxParam)
        lambda.Compile()

    static member ColumnNames = columnNames

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    static member Hydrate(cols: Series[], rowIdx: int64) : 'T =
        compiledHydrator.Invoke(cols, rowIdx)

/// <summary>
/// Stack-only, zero-allocation struct enumerator for F# DataFrame row iteration.
/// Eliminates state-machine allocations and maximizes hot-path throughput.
/// </summary>
[<Struct>]
type RowEnumerator<'T> =
    val private _df: DataFrame
    val private _cols: Series[]
    val private _height: int64
    val mutable private _index: int64
    val mutable private _current: 'T

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    internal new(df: DataFrame) =
        let colNames = FSharpRowMapper<'T>.ColumnNames
        let cols = colNames |> Array.map (fun name -> df.[name])
        {
            _df = df
            _cols = cols
            _height = df.Height
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
            // Pure straight-line hydration: array index access + compiled expression tree
            this._current <- FSharpRowMapper<'T>.Hydrate(this._cols, this._index)
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
        if this._height = 0L then invalidOp "The DataFrame sequence contains no elements."
        FSharpRowMapper<'T>.Hydrate(this._cols, 0L)

    /// <summary>
    /// Returns the first mapped row as an option without mutating the enumerator cursor.
    /// </summary>
    member this.TryFirst() : 'T option =
        if this._height = 0L then None
        else Some (FSharpRowMapper<'T>.Hydrate(this._cols, 0L))

    /// <summary>
    /// Returns the first mapped row as a ValueOption without mutating the enumerator cursor.
    /// </summary>
    member this.TryFirstValue() : 'T voption =
        if this._height = 0L then ValueNone
        else ValueSome (FSharpRowMapper<'T>.Hydrate(this._cols, 0L))
    /// <summary>
    /// Returns the last mapped row without mutating the enumerator cursor.
    /// Raises InvalidOperationException if the DataFrame contains no rows.
    /// </summary>
    member this.Last() : 'T =
        if this._height = 0L then invalidOp "The DataFrame sequence contains no elements."
        FSharpRowMapper<'T>.Hydrate(this._cols, this._height - 1L)

    /// <summary>
    /// Returns the last mapped row as an option without mutating the enumerator cursor.
    /// </summary>
    member this.TryLast() : 'T option =
        if this._height = 0L then None
        else Some (FSharpRowMapper<'T>.Hydrate(this._cols, this._height - 1L))

    /// <summary>
    /// Returns the last mapped row as a ValueOption without mutating the enumerator cursor.
    /// </summary>
    member this.TryLastValue() : 'T voption =
        if this._height = 0L then ValueNone
        else ValueSome (FSharpRowMapper<'T>.Hydrate(this._cols, this._height - 1L))
    /// <summary>
    /// Fetches a specific row directly by 64-bit index without sequential stepping.
    /// </summary>
    member this.Item(index: int64) : 'T =
        if index < 0L || index >= this._height then
            raise (IndexOutOfRangeException($"Index {index} is out of bounds for DataFrame height {this._height}."))
        FSharpRowMapper<'T>.Hydrate(this._cols, index)
    /// <summary>
    /// Fetches a specific row directly by index, or None if out of bounds.
    /// </summary>
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.TryItem(index: int64) : 'T option =
        if index < 0L || index >= this._height then None
        else Some (FSharpRowMapper<'T>.Hydrate(this._cols, index))
    /// <summary>
    /// Fetches a specific row directly by index, or ValueNone if out of bounds.
    /// </summary>
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.TryGetValueItem(index: int64) : 'T voption =
        if index < 0L || index >= this._height then ValueNone
        else ValueSome (FSharpRowMapper<'T>.Hydrate(this._cols, index))
    /// <summary>
    /// Copies all mapped rows directly into the destination span without modifying enumerator state.
    /// Returns the number of rows written.
    /// </summary>
    member this.CopyTo(destination: Span<'T>) : int =
        if int64 destination.Length < this._height then
            invalidArg (nameof destination) $"Destination span length ({destination.Length}) is smaller than row count ({this._height})."

        let total = int this._height
        for i = 0 to total - 1 do
            destination.[i] <- FSharpRowMapper<'T>.Hydrate(this._cols, int64 i)
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
                destination.[i] <- FSharpRowMapper<'T>.Hydrate(this._cols, int64 i)
            written <- total
            true
    /// <summary>
    /// Materializes all or remaining mapped rows into a strongly typed array.
    /// </summary>
    member this.ToArray() : 'T array =
        if this._height = 0L then [||]
        elif this._height > int64 Int32.MaxValue then
            raise (OverflowException($"DataFrame height ({this._height}) exceeds Int32.MaxValue."))
        else
            // Materialize all rows cleanly if untracked
            if this._index = -1L then
                let total = int this._height
                let arr = Array.zeroCreate<'T> total
                for i = 0 to total - 1 do
                    arr.[i] <- FSharpRowMapper<'T>.Hydrate(this._cols, int64 i)
                arr
            else
                // Materialize only remaining elements if enumeration already began
                let remaining = int (this._height - (this._index + 1L))
                if remaining <= 0 then [||]
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
    member this.ToList() : 'T list =
        this.ToArray() |> Array.toList

    /// <summary>
    /// Materializes all or remaining mapped rows into a ResizeArray (List{'T}).
    /// </summary>
    member this.ToResizeArray() : ResizeArray<'T> =
        ResizeArray<'T>(this.ToArray())

[<RequireQualifiedAccess>]
module Rows =

    /// <summary>
    /// Materializes all or remaining mapped rows into an array.
    /// </summary>
    let inline toArray (enumerator: RowEnumerator<'T>) : 'T array =
        enumerator.ToArray()

    /// <summary>
    /// Materializes all or remaining mapped rows into an F# immutable list.
    /// </summary>
    let inline toList (enumerator: RowEnumerator<'T>) : 'T list =
        enumerator.ToList()

    /// <summary>
    /// Materializes all or remaining mapped rows into a ResizeArray.
    /// </summary>
    let inline toResizeArray (enumerator: RowEnumerator<'T>) : ResizeArray<'T> =
        enumerator.ToResizeArray()

    /// <summary>
    /// Returns the first mapped row or raises InvalidOperationException if empty.
    /// </summary>
    let inline head (enumerator: RowEnumerator<'T>) : 'T =
        enumerator.First()

    /// <summary>
    /// Returns the first mapped row as an option.
    /// </summary>
    let inline tryHead (enumerator: RowEnumerator<'T>) : 'T option =
        enumerator.TryFirst()

    /// <summary>
    /// Returns the first mapped row as a ValueOption.
    /// </summary>
    let inline tryHeadValue (enumerator: RowEnumerator<'T>) : 'T voption =
        enumerator.TryFirstValue()

    /// <summary>
    /// Returns the last mapped row or raises InvalidOperationException if empty.
    /// </summary>
    let inline last (enumerator: RowEnumerator<'T>) : 'T =
        enumerator.Last()

    /// <summary>
    /// Returns the last mapped row as an option.
    /// </summary>
    let inline tryLast (enumerator: RowEnumerator<'T>) : 'T option =
        enumerator.TryLast()

    /// <summary>
    /// Returns the last mapped row as a ValueOption.
    /// </summary>
    let inline tryLastValue (enumerator: RowEnumerator<'T>) : 'T voption =
        enumerator.TryLastValue()

    /// <summary>
    /// Returns the total row count as a 64-bit integer.
    /// </summary>
    let inline length (enumerator: RowEnumerator<'T>) : int64 =
        enumerator.Length
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
    let inline iteri ([<InlineIfLambda>] action: int64 -> 'T -> unit) (enumerator: RowEnumerator<'T> ) : unit =
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
    let inline iter2 ([<InlineIfLambda>] action: 'T1 -> 'T2 -> unit) (enum1: RowEnumerator<'T1>) (enum2: RowEnumerator<'T2>) : unit =
        let mutable it1 = enum1
        let mutable it2 = enum2
        if it1.Length <> it2.Length then
            invalidArg (nameof enum2) $"Enumerator lengths do not match: {it1.Length} vs {it2.Length}."

        while it1.MoveNext() && it2.MoveNext() do
            action it1.Current it2.Current

    /// <summary>
    /// Iterates simultaneously over two equal-length RowEnumerators with zero-based 64-bit row index.
    /// </summary>
    let inline iteri2 ([<InlineIfLambda>] action: int64 -> 'T1 -> 'T2 -> unit) (enum1: RowEnumerator<'T1> ) (enum2: RowEnumerator<'T2> ) : unit =
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
    let inline fold ([<InlineIfLambda>] folder: 'State -> 'T -> 'State) (initial: 'State) (enumerator: RowEnumerator<'T>) : 'State =
        let mutable it = enumerator
        let mutable state = initial
        while it.MoveNext() do
            state <- folder state it.Current
        state
    /// <summary>
    /// Folds over the rows of the enumerator with index.
    /// </summary>
    let inline foldi ([<InlineIfLambda>] folder: int64 -> 'State -> 'T -> 'State) (initial: 'State) (enumerator: RowEnumerator<'T>) : 'State =
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
    let inline scan ([<InlineIfLambda>] folder: 'State -> 'T -> 'State) (initial: 'State) (enumerator: RowEnumerator<'T>) : seq<'State> =
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
    let inline tryPickValue ([<InlineIfLambda>] chooser: 'T -> 'U voption) (enumerator: RowEnumerator<'T>) : 'U voption =
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
                results.Add(cur)
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
    let inline sumBy ([<InlineIfLambda>] projection: 'T -> ^Num) (enumerator: RowEnumerator<'T>) : ^Num
        when ^Num: (static member (+) : ^Num * ^Num -> ^Num) and ^Num: (static member Zero : ^Num) =
        let mutable it = enumerator
        let mutable acc = LanguagePrimitives.GenericZero<^Num>
        while it.MoveNext() do
            acc <- acc + projection it.Current
        acc
    /// <summary>
    /// Applies a key-generating function to each row and returns a dictionary with the frequency of each key.
    /// </summary>
    let inline countBy ([<InlineIfLambda>] projection: 'T -> 'Key) (enumerator: RowEnumerator<'T>) : Dictionary<'Key, int64> =
        let mutable it = enumerator
        let dict = Dictionary<'Key, int64>()
        while it.MoveNext() do
            let key = projection it.Current
            match dict.TryGetValue(key) with
            | true, count -> dict.[key] <- count + 1L
            | false, _    -> dict.[key] <- 1L
        dict

[<AutoOpen>]
module DataFrameConversions =

    type DataFrame with
        /// <summary>
        /// Returns a zero-allocation stack enumerator for strongly-typed row hydration.
        /// Supports F# Records and parameterless DTOs.
        /// Usage: for row in df.Rows{MyRecord}() do ...
        /// </summary>
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        member this.Rows<'T>() : RowEnumerator<'T> =
            RowEnumerator<'T>(this)
        /// <summary>
        /// Convert the DataFrame to a dictionary of column name to Series.
        /// </summary>
        member this.ToMap() : Map<string, Series> =
            this
            |> Seq.map (fun series -> series.Name, series)
            |> Map.ofSeq
        /// <summary>
        /// Convert a DataFrame to a Series of type Struct.
        /// </summary>
        /// <param name="name">Name for the struct Series.</param>
        member this.ToStruct(?name:string):Series =
            let n = defaultArg name ""
            use df: DataFrame = this.Select(Expr.AsStruct [|Expr.All()|])
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
            let colNames = FSharpRowMapper<'T>.ColumnNames
            // Hoist Series instances once: 0 hash lookup during row iteration
            let cols = colNames |> Array.map (fun name -> this.[name])
            let height = this.Height

            seq {
                for r = 0L to height - 1L do
                    yield FSharpRowMapper<'T>.Hydrate(cols, r)
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
    interface IDataFrameMaterializer with
        member _.Materialize<'T>(handle: DataFrameHandle) : IEnumerable<'T> =
            let df = new DataFrame(handle)
            let cursor = df.Rows<'T>()
            seq {
                for item in cursor do
                    yield item
            }
