namespace Polars.FSharp

open System.Reflection
open Microsoft.FSharp.Reflection
open System.Linq.Expressions
open System.Collections.Generic
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

    // Safely resolve the open generic ExtractField method definition once
    static let extractFieldMethodDef : MethodInfo =
        let methods = typeof<FSharpRowExtractor>.GetMethods(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Static)
        methods
        |> Array.find (fun m -> m.Name = "ExtractField" && m.IsGenericMethodDefinition)

    static let columnNames : string[] =
        let targetType = typeof<'T>
        if FSharpType.IsRecord(targetType, true) then
            FSharpType.GetRecordFields(targetType, true)
            |> Array.map (fun f -> f.Name)
        else
            targetType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
            |> Array.filter (fun p -> p.CanWrite)
            |> Array.map (fun p -> p.Name)

    // Compiled factory signature: (Series[] columns, int64 rowIdx) -> 'T
    static let compiledHydrator : (Series[] * int64 -> 'T) =
        let targetType = typeof<'T>
        let colsParam = Expression.Parameter(typeof<Series[]>, "cols")
        let rowIdxParam = Expression.Parameter(typeof<int64>, "rowIdx")

        if FSharpType.IsRecord(targetType, true) then
            let fields = FSharpType.GetRecordFields(targetType, true)
            let fieldCount = fields.Length

            // F# records always have a single canonical constructor taking all fields in order
            let ctors = targetType.GetConstructors(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
            let recordCtor =
                ctors
                |> Array.tryFind (fun c -> c.GetParameters().Length = fieldCount)
                |> Option.defaultWith (fun () -> ctors.[0])

            let ctorArgs =
                fields
                |> Array.mapi (fun i f ->
                    let fieldType = f.PropertyType

                    let isOption =
                        fieldType.IsGenericType &&
                        (fieldType.GetGenericTypeDefinition() = typedefof<option<_>> ||
                         fieldType.GetGenericTypeDefinition() = typedefof<voption<_>>)
                    let isList =
                        fieldType.IsGenericType && fieldType.GetGenericTypeDefinition() = typedefof<list<_>>
                    let isArray = fieldType.IsArray
                    let isNullable = Nullable.GetUnderlyingType(fieldType) <> null
                    let acceptsNull = isOption || isList || isArray || isNullable

                    let closedExtractMethod = extractFieldMethodDef.MakeGenericMethod([| fieldType |])

                    // cols[i]
                    let colAccess = Expression.ArrayIndex(colsParam, Expression.Constant(i))
                    let acceptsNullConst = Expression.Constant(acceptsNull, typeof<bool>)
                    let fieldNameConst = Expression.Constant(f.Name, typeof<string>)

                    // FSharpRowExtractor.ExtractField<FieldType>(cols[i], rowIdx, acceptsNull, fieldName)
                    Expression.Call(closedExtractMethod, colAccess, rowIdxParam, acceptsNullConst, fieldNameConst) :> Expression
                )

            let newRecordExpr = Expression.New(recordCtor, ctorArgs)
            let lambda = Expression.Lambda<Func<Series[], int64, 'T>>(newRecordExpr, colsParam, rowIdxParam)
            let fn = lambda.Compile()

            fun (cols: Series[], rowIdx: int64) -> fn.Invoke(cols, rowIdx)

        else
            // Fallback for classes/structs with parameterless constructors
            let defaultCtor = targetType.GetConstructor(BindingFlags.Public ||| BindingFlags.Instance, null, Type.EmptyTypes, null)
            if isNull defaultCtor && not targetType.IsValueType then
                raise (ArgumentException $"Type '{targetType.FullName}' is neither an F# Record nor does it have a public parameterless constructor.")

            let props =
                targetType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                |> Array.filter (fun p -> p.CanWrite)

            let propCount = props.Length
            let instanceVar = Expression.Variable(targetType, "inst")
            let newExpr =
                if not (isNull defaultCtor) then
                    Expression.New(defaultCtor)
                else
                    Expression.New(targetType) // ValueType struct fallback

            let assignInst = Expression.Assign(instanceVar, newExpr)
            let blockExprs = List<Expression>()
            blockExprs.Add(assignInst)

            for i = 0 to propCount - 1 do
                let prop = props.[i]
                let propType = prop.PropertyType
                let closedExtractMethod = extractFieldMethodDef.MakeGenericMethod([| propType |])

                let colAccess = Expression.ArrayIndex(colsParam, Expression.Constant(i))
                let valExpr =
                    Expression.Call(
                        closedExtractMethod,
                        colAccess,
                        rowIdxParam,
                        Expression.Constant(true, typeof<bool>),
                        Expression.Constant(prop.Name, typeof<string>)
                    )

                let assignProp = Expression.Assign(Expression.Property(instanceVar, prop), valExpr)
                blockExprs.Add(assignProp)

            blockExprs.Add(instanceVar)

            let body = Expression.Block([| instanceVar |], blockExprs)
            let lambda = Expression.Lambda<Func<Series[], int64, 'T>>(body, colsParam, rowIdxParam)
            let fn = lambda.Compile()

            fun (cols: Series[], rowIdx: int64) -> fn.Invoke(cols, rowIdx)

    static member ColumnNames = columnNames

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    static member Hydrate(cols: Series[], rowIdx: int64) : 'T =
        compiledHydrator (cols, rowIdx)

/// <summary>
/// Stack-only, zero-allocation struct enumerator for F# DataFrame row iteration.
/// Eliminates state-machine allocations and maximizes hot-path throughput.
/// </summary>
[<Struct>]
type DataFrameRowEnumerator<'T> =
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
    /// Gets the current element.
    /// </summary>
    member this.Current: 'T = this._current
    /// <summary>
    /// Returns the enumerator itself.
    /// </summary>
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.GetEnumerator() : DataFrameRowEnumerator<'T> = this
    /// <summary>
    /// Returns the first mapped row, or raises InvalidOperationException if empty.
    /// </summary>
    member this.First() : 'T =
        if this.MoveNext() then this._current
        else invalidOp "The DataFrame sequence contains no elements."
    /// <summary>
    /// Returns the first mapped row, or None if empty.
    /// </summary>
    member this.TryFirst() : 'T option =
        if this.MoveNext() then Some this._current
        else None
    /// <summary>
    /// Returns the first mapped row, or ValueNone if empty.
    /// </summary>
    member this.TryFirstValue() : 'T voption =
        if this.MoveNext() then ValueSome this._current
        else ValueNone
    /// <summary>
    /// Fetches a specific row directly by 64-bit index without sequential stepping.
    /// </summary>
    member this.Item(index: int64) : 'T =
        if index < 0L || index >= this._height then
            raise (IndexOutOfRangeException($"Index {index} is out of bounds for DataFrame height {this._height}."))
        FSharpRowMapper<'T>.Hydrate(this._cols, index)

    /// <summary>
    /// Fills the destination Span{'T} directly.
    /// </summary>
    member this.CopyTo(destination: Span<'T>) : int =
        if int64 destination.Length < this._height then
            invalidArg (nameof destination) $"Destination span length ({destination.Length}) is smaller than row count ({this._height})."

        let mutable written = 0
        while this.MoveNext() do
            destination.[written] <- this.Current
            written <- written + 1
        written
    /// <summary>
    /// Materializes all mapped rows into an array with exact pre-allocation.
    /// </summary>
    member this.ToArray() : 'T array =
        if this._height = 0L then [||]
        elif this._height > int64 Int32.MaxValue then
            raise (OverflowException($"DataFrame height ({this._height}) exceeds Int32.MaxValue."))
        else
            let len = int this._height
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
