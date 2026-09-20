namespace Polars.FSharp

open System.Reflection
open Microsoft.FSharp.Reflection
open System
open System.Runtime.CompilerServices

/// <summary>
/// Unified high-performance row mapper for hydrating F# Records and DTOs from DataFrame rows.
/// </summary>
type internal FSharpRowMapper<'T>() =
    static let mapper : (DataFrame * int64 -> 'T) =
        let targetType = typeof<'T>
        let getValueMethodDef =
            typeof<Series>.GetMethod("GetValue", [| typeof<int64>; typeof<bool> |])

        if FSharpType.IsRecord(targetType, true) then
            let fields = FSharpType.GetRecordFields(targetType, true)
            let ctor = FSharpValue.PreComputeRecordConstructor(targetType, true)
            let fieldCount = fields.Length

            let fieldExtractors =
                fields
                |> Array.map (fun f ->
                    let colName = f.Name
                    let fieldType = f.PropertyType

                    let isOption =
                        fieldType.IsGenericType &&
                        (fieldType.GetGenericTypeDefinition() = typedefof<option<_>> ||
                         fieldType.GetGenericTypeDefinition() = typedefof<voption<_>>)

                    let isList =
                        fieldType.IsGenericType && fieldType.GetGenericTypeDefinition() = typedefof<list<_>>

                    let isArray = fieldType.IsArray
                    let acceptsNull = isOption || isList || isArray

                    let getValueGeneric = getValueMethodDef.MakeGenericMethod([| fieldType |])

                    fun (df: DataFrame, rowIdx: int64) ->
                        let col = df.[colName]

                        if not acceptsNull && col.IsNullAt(rowIdx, uncheck = true) then
                            invalidOp $"Record field '{f.Name}' does not accept Option, but column '{colName}' contains null at row {rowIdx}."

                        getValueGeneric.Invoke(col, [| box rowIdx; box true |])
                )

            fun (df: DataFrame, rowIdx: int64) ->
                let args = Array.zeroCreate<obj> fieldCount
                for i = 0 to fieldCount - 1 do
                    args.[i] <- fieldExtractors.[i](df, rowIdx)
                ctor args :?> 'T

        else
            // Fallback for classes with parameterless constructors
            let defaultCtor = targetType.GetConstructor Type.EmptyTypes
            if isNull defaultCtor && not targetType.IsValueType then
                raise (ArgumentException $"Type '{targetType.FullName}' is neither an F# Record nor does it have a parameterless constructor.")

            let props = targetType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
            let propExtractors =
                props
                |> Array.choose (fun p ->
                    if not p.CanWrite then None
                    else
                        let propName = p.Name
                        let propType = p.PropertyType
                        let getValueGeneric = getValueMethodDef.MakeGenericMethod [| propType |]
                        Some (fun (instance: obj, df: DataFrame, rowIdx: int64) ->
                            if df.ColumnNames |> Array.contains propName then
                                let col = df.[propName]
                                if not (col.IsNullAt(rowIdx, uncheck = true)) then
                                    let v = getValueGeneric.Invoke(col, [| box rowIdx; box true |])
                                    p.SetValue(instance, v)
                        )
                )

            fun (df: DataFrame, rowIdx: int64) ->
                let instance = Activator.CreateInstance<'T>()
                for extract in propExtractors do
                    extract(instance, df, rowIdx)
                instance

    static member Hydrate(df: DataFrame, rowIdx: int64) : 'T =
        mapper (df, rowIdx)

/// <summary>
/// Stack-only, zero-allocation struct enumerator for F# DataFrame row iteration.
/// Eliminates state-machine allocations and maximizes hot-path throughput.
/// </summary>
[<Struct>]
type DataFrameRowEnumerator<'T> =
    val private df: DataFrame
    val private height: int64
    val mutable private index: int64
    val mutable private current: 'T

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    internal new(df: DataFrame) =
        {
            df = df
            height = df.Height
            index = -1L
            current = Unchecked.defaultof<'T>
        }

    /// <summary>
    /// Moves the enumerator to the next element.
    /// </summary>
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.MoveNext() : bool =
        let nextIndex = this.index + 1L
        if nextIndex < this.height then
            this.index <- nextIndex
            this.current <- FSharpRowMapper<'T>.Hydrate(this.df, this.index)
            true
        else
            this.current <- Unchecked.defaultof<'T>
            false
    /// <summary>
    /// Gets the total number of rows.
    /// </summary>
    member this.Length: int64 = this.height
    /// <summary>
    /// Gets the current element.
    /// </summary>
    member this.Current: 'T = this.current
    /// <summary>
    /// Returns the enumerator itself.
    /// </summary>
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.GetEnumerator() : DataFrameRowEnumerator<'T> = this
    /// <summary>
    /// Returns the first mapped row, or raises InvalidOperationException if empty.
    /// </summary>
    member this.First() : 'T =
        if this.MoveNext() then this.current
        else invalidOp "The DataFrame sequence contains no elements."
    /// <summary>
    /// Returns the first mapped row, or None if empty.
    /// </summary>
    member this.TryFirst() : 'T option =
        if this.MoveNext() then Some this.current
        else None
    /// <summary>
    /// Returns the first mapped row, or ValueNone if empty.
    /// </summary>
    member this.TryFirstValue() : 'T voption =
        if this.MoveNext() then ValueSome this.current
        else ValueNone
    /// <summary>
    /// Fetches a specific row directly by 64-bit index without sequential stepping.
    /// </summary>
    member this.Item(index: int64) : 'T =
        if index < 0L || index >= this.height then
            raise (IndexOutOfRangeException($"Index {index} is out of bounds for DataFrame height {this.height}."))
        FSharpRowMapper<'T>.Hydrate(this.df, index)

    /// <summary>
    /// Fills the destination Span{'T} directly.
    /// </summary>
    member this.CopyTo(destination: Span<'T>) : int =
        if int64 destination.Length < this.height then
            invalidArg (nameof destination) $"Destination span length ({destination.Length}) is smaller than row count ({this.height})."

        let mutable written = 0
        while this.MoveNext() do
            destination.[written] <- this.Current
            written <- written + 1
        written
    /// <summary>
    /// Materializes all mapped rows into an array with exact pre-allocation.
    /// </summary>
    member this.ToArray() : 'T array =
        if this.height = 0L then [||]
        elif this.height > int64 Int32.MaxValue then
            raise (OverflowException($"DataFrame height ({this.height}) exceeds Int32.MaxValue."))
        else
            let len = int this.height
            let arr = Array.zeroCreate<'T> len
            let mutable i = 0
            while this.MoveNext() do
                arr.[i] <- this.Current
                i <- i + 1
            arr
    /// <summary>
    /// Materializes all mapped rows into an F# list.
    /// </summary>
    member this.ToList() : 'T list =
        this.ToArray() |> Array.toList

[<AutoOpen>]
module DataFrameConversions =

    type DataFrame with
        /// <summary>
        /// Returns a zero-allocation stack enumerator for strongly-typed row hydration.
        /// Supports F# Records and parameterless DTOs.
        /// Usage: for row in df.Rows{MyRecord}() do ...
        /// </summary>
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        member this.Rows<'T>() : DataFrameRowEnumerator<'T> =
            DataFrameRowEnumerator<'T>(this)
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
            let targetType = typeof<'T>
            if not (FSharpType.IsRecord(targetType, true)) then
                raise (ArgumentException($"Type '{targetType.FullName}' is not an F# Record type.", "T"))

            let height = this.Height
            if height = 0L then Seq.empty
            else
                seq {
                    for r = 0L to height - 1L do
                        yield FSharpRowMapper<'T>.Hydrate(this, r)
                }

        /// <summary>
        /// Materializes all DataFrame rows into an F# list of records.
        /// </summary>
        member this.ToRecordList<'T>() : 'T list =
            let targetType = typeof<'T>
            if not (FSharpType.IsRecord(targetType, true)) then
                raise (ArgumentException($"Type '{targetType.FullName}' is not an F# Record type.", "T"))

            this.Rows<'T>().ToList()
