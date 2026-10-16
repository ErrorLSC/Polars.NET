namespace Polars.FSharp

open System.Reflection
open Microsoft.FSharp.Reflection
open System.Linq.Expressions
open System.Collections.Generic
open Polars.NET.Core
open System
open System.Runtime.CompilerServices
open Polars.NET.Core.Helpers;
open System.Collections.Concurrent

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

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    static member IsNull(col: Series, rowIdx: int64) : bool =
        col.IsNullAt(rowIdx, uncheck = true)

type internal FSharpRowMapper<'T>() =

    // Cache compiled hydrators per schema signature for optimal high-throughput execution
    static let cache = ConcurrentDictionary<string, Func<Series[], int64, 'T>>()

    static let extractFieldMethodDef : MethodInfo =
        let methods = typeof<FSharpRowExtractor>.GetMethods(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Static)
        methods
        |> Array.find (fun m -> m.Name = "ExtractField" && m.IsGenericMethodDefinition)

    // Resolved reliably from FSharpRowExtractor, eliminating extension method reflection failures
    static let isNullMethodDef : MethodInfo =
        typeof<FSharpRowExtractor>.GetMethod("IsNull", BindingFlags.Public ||| BindingFlags.Static)

    static let isFSharpAnonymousObject (t: Type) =
        not (isNull t || isNull t.FullName) &&
        (t.FullName.StartsWith "Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject" ||
         t.IsGenericType && t.Name.StartsWith("AnonymousObject`"))

    /// Helper to identify all scalar/primitive types including string, DateTime, Guid, etc.
    static let isScalarOrString (t: Type) =
        PolarsTypeHelper.IsScalarType t || t = typeof<string> || t.IsEnum

    static let isOptionType (t: Type) =
        t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<option<_>>

    static let isValueOptionType (t: Type) =
        t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<voption<_>>

    static let isNullableType (t: Type) =
        not (isNull (Nullable.GetUnderlyingType t))

    /// Identifies whether a type represents a single scalar value, optionally wrapped in Nullable/Option/ValueOption
    static let rec isScalarLike (t: Type) =
        if isScalarOrString t then true
        elif isNullableType t then isScalarLike (Nullable.GetUnderlyingType t)
        elif isOptionType t then isScalarLike (t.GetGenericArguments().[0])
        elif isValueOptionType t then isScalarLike (t.GetGenericArguments().[0])
        else false

    /// Computes schema signature for caching
    static let computeSchemaSignature (colNames: string[]) (dataTypes: string[]) =
        Array.map2 (fun n d -> $"{n}:{d}") colNames dataTypes
        |> String.concat "|"

    /// Builds a compiled Func<Series[], int64, 'T> based on the actual physical DataFrame columns
    static let buildHydrator (actualColNames: string[]) : Func<Series[], int64, 'T> =
        let targetType = typeof<'T>
        let colsParam = Expression.Parameter(typeof<Series[]>, "cols")
        let rowIdxParam = Expression.Parameter(typeof<int64>, "rowIdx")

        let colMap = Dictionary<string, int> StringComparer.OrdinalIgnoreCase
        actualColNames |> Array.iteri (fun i name -> colMap.[name] <- i)

        // Checks whether a physical column is null at the specified row via FSharpRowExtractor.IsNull
        let createIsNullExpr (colIdx: int) : Expression =
            let colAccess = Expression.ArrayIndex(colsParam, Expression.Constant(colIdx))
            Expression.Call(isNullMethodDef, colAccess, rowIdxParam) :> Expression

        // Helper to extract a single primitive/scalar column value by column index
        let createExtractExpr (colIdx: int) (propType: Type) (fieldName: string) : Expression =
            let isOption = isOptionType propType || isValueOptionType propType
            let isList = propType.IsGenericType && propType.GetGenericTypeDefinition() = typedefof<list<_>>
            let isArray = propType.IsArray
            let isNullable = isNullableType propType
            let acceptsNull = isOption || isList || isArray || isNullable

            let colAccess = Expression.ArrayIndex(colsParam, Expression.Constant(colIdx))
            let acceptsNullConst = Expression.Constant(acceptsNull, typeof<bool>)
            let fieldNameConst = Expression.Constant(fieldName, typeof<string>)

            if propType.IsEnum then
                let underlyingType = Enum.GetUnderlyingType propType
                let closedExtractMethod = extractFieldMethodDef.MakeGenericMethod([| underlyingType |])
                let call = Expression.Call(closedExtractMethod, colAccess, rowIdxParam, acceptsNullConst, fieldNameConst)
                Expression.Convert(call, propType) :> Expression
            elif isNullable && (Nullable.GetUnderlyingType propType).IsEnum then
                let underlyingEnum = Nullable.GetUnderlyingType propType
                let underlyingType = Enum.GetUnderlyingType underlyingEnum
                let nullableUnderlying = typedefof<Nullable<_>>.MakeGenericType underlyingType
                let closedExtractMethod = extractFieldMethodDef.MakeGenericMethod([| nullableUnderlying |])
                let call = Expression.Call(closedExtractMethod, colAccess, rowIdxParam, acceptsNullConst, fieldNameConst)
                Expression.Convert(call, propType) :> Expression
            else
                let closedExtractMethod = extractFieldMethodDef.MakeGenericMethod([| propType |])
                Expression.Call(closedExtractMethod, colAccess, rowIdxParam, acceptsNullConst, fieldNameConst) :> Expression

        // Resolves column index by considering scope suffixes (_left, _right, _second, _third) across joins
        let tryFindCol (propName: string) (tupleIdx: int) : int option =
            let candidates = [
                if tupleIdx = 0 then
                    propName + "_left"
                    propName + "_first"
                elif tupleIdx = 1 then
                    propName + "_second"
                    propName + "_right"
                elif tupleIdx = 2 then
                    propName + "_third"
                propName
            ]
            candidates
            |> List.tryPick (fun candidate ->
                match colMap.TryGetValue candidate with
                | true, idx -> Some idx
                | false, _  -> None
            )

        // Helper to construct a single non-tuple instance (Record or Class/Struct), with Outer Join Null Folding
        let rec buildSingleObjectExpr (t: Type) (tupleIdx: int) : Expression =
            let underlyingTypeOpt = Nullable.GetUnderlyingType(t) |> Option.ofObj
            let isOpt = isOptionType t
            let isVOpt = isValueOptionType t
            let isNullable = underlyingTypeOpt.IsSome || isOpt || isVOpt || not t.IsValueType

            let actualType =
                if isOpt || isVOpt then t.GetGenericArguments().[0]
                elif underlyingTypeOpt.IsSome then underlyingTypeOpt.Value
                else t

            let usedColIndices = List<int>()

            let instanceExpr =
                if FSharpType.IsRecord(actualType, true) then
                    let fields = FSharpType.GetRecordFields(actualType, true)
                    let ctors = actualType.GetConstructors(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
                    let recordCtor =
                        ctors
                        |> Array.tryFind (fun c -> c.GetParameters().Length = fields.Length)
                        |> Option.defaultWith (fun () -> ctors.[0])

                    let ctorArgs =
                        fields
                        |> Array.mapi (fun i f ->
                            match tryFindCol f.Name tupleIdx with
                            | Some colIdx ->
                                usedColIndices.Add(colIdx)
                                createExtractExpr colIdx f.PropertyType f.Name
                            | None ->
                                if tupleIdx = 0 && i < actualColNames.Length then
                                    usedColIndices.Add(i)
                                    createExtractExpr i f.PropertyType f.Name
                                else
                                    Expression.Default(f.PropertyType) :> Expression
                        )
                    Expression.New(recordCtor, ctorArgs) :> Expression

                else
                    // Positional constructor (C# Record / AnonymousObject / Immutable DTO fallback)
                    let ctors = actualType.GetConstructors(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
                    let primaryCtorOpt =
                        ctors
                        |> Array.filter (fun c ->
                            // Exclude copy constructor: e.g. T(T original)
                            not (c.GetParameters().Length = 1 && c.GetParameters().[0].ParameterType = actualType) &&
                            c.GetParameters().Length > 0
                        )
                        |> Array.sortByDescending (fun c -> c.GetParameters().Length)
                        |> Array.tryHead

                    match primaryCtorOpt with
                    | Some primaryCtor ->
                        let ctorParams = primaryCtor.GetParameters()
                        let ctorArgs =
                            ctorParams
                            |> Array.mapi (fun i p ->
                                let cleanName = p.Name.TrimStart('@')
                                let colIdxOpt =
                                    match tryFindCol cleanName tupleIdx with
                                    | Some idx -> Some idx
                                    | None ->
                                        actualType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                                        |> Array.tryFind (fun prop -> String.Equals(prop.Name, cleanName, StringComparison.OrdinalIgnoreCase))
                                        |> Option.bind (fun prop -> tryFindCol prop.Name tupleIdx)

                                match colIdxOpt with
                                | Some colIdx ->
                                    usedColIndices.Add(colIdx)
                                    createExtractExpr colIdx p.ParameterType p.Name
                                | None ->
                                    if tupleIdx = 0 && i < actualColNames.Length then
                                        usedColIndices.Add(i)
                                        createExtractExpr i p.ParameterType p.Name
                                    else
                                        Expression.Default(p.ParameterType) :> Expression
                            )
                        Expression.New(primaryCtor, ctorArgs) :> Expression

                    | None ->
                        // Parameterless constructor + mutable properties/fields fallback
                        let defaultCtor = actualType.GetConstructor(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance, null, Type.EmptyTypes, null)
                        let instanceVar = Expression.Variable(actualType, "subInstance")
                        let createInstanceExpr =
                            if not (isNull defaultCtor) then Expression.New(defaultCtor)
                            else Expression.New(actualType)

                        let blockExpressions = List<Expression>()
                        blockExpressions.Add(Expression.Assign(instanceVar, createInstanceExpr))

                        // 1. Bind Properties (Standard and init-only)
                        let props = actualType.GetProperties(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
                        for prop in props do
                            if prop.Name <> "EqualityContract" then
                                match tryFindCol prop.Name tupleIdx with
                                | Some colIdx ->
                                    usedColIndices.Add(colIdx)
                                    let readExpr = createExtractExpr colIdx prop.PropertyType prop.Name
                                    if not (isNull prop.SetMethod) then
                                        blockExpressions.Add(Expression.Call(instanceVar, prop.SetMethod, readExpr))
                                    else
                                        let backingField = actualType.GetField($"<{prop.Name}>k__BackingField", BindingFlags.Instance ||| BindingFlags.NonPublic)
                                        if not (isNull backingField) then
                                            blockExpressions.Add(Expression.Assign(Expression.Field(instanceVar, backingField), readExpr))
                                | None -> ()

                        // 2. Bind Public Instance Fields
                        let fields = actualType.GetFields(BindingFlags.Public ||| BindingFlags.Instance)
                        for field in fields do
                            match tryFindCol field.Name tupleIdx with
                            | Some colIdx ->
                                usedColIndices.Add colIdx
                                let readExpr = createExtractExpr colIdx field.FieldType field.Name
                                blockExpressions.Add(Expression.Assign(Expression.Field(instanceVar, field), readExpr))
                            | None -> ()

                        blockExpressions.Add instanceVar
                        Expression.Block([| instanceVar |], blockExpressions) :> Expression

            // Outer Join Null folding: If target accepts null and all mapped columns are null in current row, fold to null/None/ValueNone
            if isNullable && usedColIndices.Count > 0 then
                let distinctCols = usedColIndices |> Seq.distinct |> Seq.toArray
                let allNullExpr =
                    distinctCols
                    |> Array.map createIsNullExpr
                    |> Array.reduce (fun acc e -> Expression.AndAlso(acc, e) :> Expression)

                if isOpt then
                    let noneExpr = Expression.Default(t)
                    let someMethod = t.GetMethod("Some", [| actualType |])
                    let someExpr = Expression.Call(someMethod, instanceExpr)
                    Expression.Condition(allNullExpr, noneExpr, someExpr) :> Expression
                elif isVOpt then
                    let vnoneExpr = Expression.Default(t)
                    let vsomeMethod = t.GetMethod("NewValueSome", [| actualType |])
                    let vsomeExpr = Expression.Call(vsomeMethod, instanceExpr)
                    Expression.Condition(allNullExpr, vnoneExpr, vsomeExpr) :> Expression
                elif underlyingTypeOpt.IsSome then
                    let nullConst = Expression.Constant(null, t)
                    let valueExpr = Expression.Convert(instanceExpr, t)
                    Expression.Condition(allNullExpr, nullConst, valueExpr) :> Expression
                else
                    let nullConst = Expression.Constant(null, t)
                    Expression.Condition(allNullExpr, nullConst, instanceExpr) :> Expression
            else
                if isOpt then
                    let someMethod = t.GetMethod("Some", [| actualType |])
                    Expression.Call(someMethod, instanceExpr) :> Expression
                elif isVOpt then
                    let vsomeMethod = t.GetMethod("NewValueSome", [| actualType |])
                    Expression.Call(vsomeMethod, instanceExpr) :> Expression
                elif underlyingTypeOpt.IsSome then
                    Expression.Convert(instanceExpr, t) :> Expression
                else
                    instanceExpr

        // Builds an argument for tuple elements (supports scalar aliases like "Outer", "Inner", "Item1" and scope suffixes)
        let buildTupleElementExpr (subType: Type) (tupleIdx: int) : Expression =
            if isScalarLike subType then
                let candidateNames = [
                    $"Item{tupleIdx + 1}"
                    if tupleIdx = 0 then "Outer"; "Index"; "First"
                    elif tupleIdx = 1 then "Inner"; "Item"; "Second"
                    elif tupleIdx = 2 then "Third"
                ]

                let resolvedColIdx =
                    candidateNames
                    |> List.tryPick (fun name ->
                        match colMap.TryGetValue name with
                        | true, idx -> Some idx
                        | false, _  -> None
                    )
                    |> Option.orElseWith (fun () ->
                        if tupleIdx < actualColNames.Length then Some tupleIdx
                        else None
                    )

                match resolvedColIdx with
                | Some colIdx ->
                    createExtractExpr colIdx subType candidateNames.Head
                | None ->
                    Expression.Default(subType) :> Expression
            else
                // Forward tupleIdx so child records prioritize their join scope suffixes (_left, _second, _right, etc.)
                buildSingleObjectExpr subType tupleIdx

        // Top-level build branch
        let isTuple =
            FSharpType.IsTuple targetType ||
            not (isNull targetType.FullName) &&
            (targetType.FullName.StartsWith "System.ValueTuple`" || targetType.FullName.StartsWith "System.Tuple`")

        let rootExpr : Expression =
            // 0. Primitive / Scalar / Enum -> Read directly from column 0
            if isScalarOrString targetType then
                createExtractExpr 0 targetType (if actualColNames.Length > 0 then actualColNames.[0] else "")

            // 1. Reference Tuple (F# Standard Tuple)
            elif FSharpType.IsTuple(targetType) then
                let elemTypes = FSharpType.GetTupleElements(targetType)
                let tupleCtor = targetType.GetConstructor(elemTypes)
                let subArgs = elemTypes |> Array.mapi (fun i subT -> buildTupleElementExpr subT i)
                Expression.New(tupleCtor, subArgs) :> Expression

            // 2. Struct Tuple (System.ValueTuple<...>)
            elif targetType.IsValueType && not (isNull targetType.FullName) && targetType.FullName.StartsWith("System.ValueTuple`") then
                let elemTypes = targetType.GetGenericArguments()
                let tupleCtor =
                    targetType.GetConstructor elemTypes
                    |> Option.ofObj
                    |> Option.defaultWith (fun () ->
                        targetType.GetConstructors(BindingFlags.Public ||| BindingFlags.Instance).[0]
                    )
                let subArgs = elemTypes |> Array.mapi (fun i subT -> buildTupleElementExpr subT i)
                Expression.New(tupleCtor, subArgs) :> Expression

            // 3. System.Tuple<...> (Reference Tuple matching C# Tuple)
            elif not (isNull targetType.FullName) && targetType.FullName.StartsWith "System.Tuple`" then
                let elemTypes = targetType.GetGenericArguments()
                let tupleCtor = targetType.GetConstructor(elemTypes)
                let subArgs = elemTypes |> Array.mapi (fun i subT -> buildTupleElementExpr subT i)
                Expression.New(tupleCtor, subArgs) :> Expression

            // 4. F# AnonymousObject: treat positional arguments like a Tuple
            elif isFSharpAnonymousObject targetType then
                let ctors = targetType.GetConstructors(BindingFlags.Public ||| BindingFlags.Instance)
                let primaryCtor = ctors.[0]
                let ctorParams = primaryCtor.GetParameters()
                let subArgs =
                    ctorParams
                    |> Array.mapi (fun i p ->
                        let colIdx = if i < actualColNames.Length then i else 0
                        createExtractExpr colIdx p.ParameterType p.Name
                    )
                Expression.New(primaryCtor, subArgs) :> Expression

            // 5. Single record or object
            else
                buildSingleObjectExpr targetType 0

        let lambda = Expression.Lambda<Func<Series[], int64, 'T>>(rootExpr, colsParam, rowIdxParam)
        lambda.Compile()

    /// Collects static required field names (fallback compatibility)
    static let rec collectRequiredNames (t: Type) : string[] =
        if isScalarOrString t then [||]
        elif FSharpType.IsTuple(t) then
            FSharpType.GetTupleElements(t) |> Array.collect collectRequiredNames
        elif t.IsValueType && not (isNull t.FullName) && t.FullName.StartsWith("System.ValueTuple`") then
            t.GetGenericArguments() |> Array.collect collectRequiredNames
        elif FSharpType.IsRecord(t, true) then
            FSharpType.GetRecordFields(t, true) |> Array.map (fun f -> f.Name)
        elif isFSharpAnonymousObject t then
            t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance) |> Array.map (fun p -> p.Name)
        else
            let writeable =
                t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                |> Array.filter (fun p -> p.CanWrite)
                |> Array.map (fun p -> p.Name)
            if writeable.Length > 0 then writeable
            else t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance) |> Array.map (fun p -> p.Name)

    static let defaultColumnNames : string[] = collectRequiredNames typeof<'T>

    /// Gets or compiles a hydrator delegate tailored to the schema of the provided DataFrame
    static member GetOrCreate(df: DataFrame) : Func<Series[], int64, 'T> =
        let names = df.ColumnNames
        let types = names |> Array.map (fun n -> df.[n].DataType.ToString())
        let sigKey = computeSchemaSignature names types
        match cache.TryGetValue sigKey with
        | true, h -> h
        | false, _ ->
            cache.GetOrAdd(sigKey, fun _ -> buildHydrator names)

    /// Gets or compiles a hydrator delegate tailored to the column array schema
    static member GetOrCreate(cols: Series[]) : Func<Series[], int64, 'T> =
        let names = cols |> Array.map (fun c -> c.Name)
        let types = cols |> Array.map (fun c -> c.DataType.ToString())
        let sigKey = computeSchemaSignature names types
        match cache.TryGetValue sigKey with
        | true, h -> h
        | false, _ ->
            cache.GetOrAdd(sigKey, fun _ -> buildHydrator names)

    static member ColumnNames = defaultColumnNames

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    static member Hydrate(cols: Series[], rowIdx: int64) : 'T =
        let hydrator = FSharpRowMapper<'T>.GetOrCreate cols
        hydrator.Invoke(cols, rowIdx)

/// <summary>
/// Stack-only, zero-allocation struct enumerator for F# DataFrame row iteration.
/// Eliminates state-machine allocations and maximizes hot-path throughput.
/// </summary>
[<Struct>]
type RowEnumerator<'T> =
    val private _df: DataFrame
    val private _cols: Series[]
    val private _hydrator: Func<Series[], int64, 'T>
    val private _height: int64
    val mutable private _index: int64
    val mutable private _current: 'T

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    internal new(df: DataFrame) =
        let width = int df.Width
        let cols = Array.init width (fun i -> df.[i])
        let hydrator = FSharpRowMapper<'T>.GetOrCreate(df)
        {
            _df = df
            _cols = cols
            _hydrator = hydrator
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
            this._current <- this._hydrator.Invoke(this._cols, this._index)
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
        this._hydrator.Invoke(this._cols, 0L)

    /// <summary>
    /// Returns the first mapped row as an option without mutating the enumerator cursor.
    /// </summary>
    member this.TryFirst() : 'T option =
        if this._height = 0L then None
        else Some (this._hydrator.Invoke(this._cols, 0L))

    /// <summary>
    /// Returns the first mapped row as a ValueOption without mutating the enumerator cursor.
    /// </summary>
    member this.TryFirstValue() : 'T voption =
        if this._height = 0L then ValueNone
        else ValueSome (this._hydrator.Invoke(this._cols, 0L))

    /// <summary>
    /// Returns the last mapped row without mutating the enumerator cursor.
    /// Raises InvalidOperationException if the DataFrame contains no rows.
    /// </summary>
    member this.Last() : 'T =
        if this._height = 0L then invalidOp "The DataFrame sequence contains no elements."
        this._hydrator.Invoke(this._cols, this._height - 1L)

    /// <summary>
    /// Returns the last mapped row as an option without mutating the enumerator cursor.
    /// </summary>
    member this.TryLast() : 'T option =
        if this._height = 0L then None
        else Some (this._hydrator.Invoke(this._cols, this._height - 1L))

    /// <summary>
    /// Returns the last mapped row as a ValueOption without mutating the enumerator cursor.
    /// </summary>
    member this.TryLastValue() : 'T voption =
        if this._height = 0L then ValueNone
        else ValueSome (this._hydrator.Invoke(this._cols, this._height - 1L))

    /// <summary>
    /// Fetches a specific row directly by 64-bit index without sequential stepping.
    /// </summary>
    member this.Item(index: int64) : 'T =
        if index < 0L || index >= this._height then
            raise (InvalidOperationException $"Index {index} is out of bounds for DataFrame height {this._height}.")
        this._hydrator.Invoke(this._cols, index)

    /// <summary>
    /// Fetches a specific row directly by index, or None if out of bounds.
    /// </summary>
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.TryItem(index: int64) : 'T option =
        if index < 0L || index >= this._height then None
        else Some (this._hydrator.Invoke(this._cols, index))

    /// <summary>
    /// Fetches a specific row directly by index, or ValueNone if out of bounds.
    /// </summary>
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.TryGetValueItem(index: int64) : 'T voption =
        if index < 0L || index >= this._height then ValueNone
        else ValueSome (this._hydrator.Invoke(this._cols, index))

    /// <summary>
    /// Copies all mapped rows directly into the destination span without modifying enumerator state.
    /// Returns the number of rows written.
    /// </summary>
    member this.CopyTo(destination: Span<'T>) : int =
        if int64 destination.Length < this._height then
            invalidArg (nameof destination) $"Destination span length ({destination.Length}) is smaller than row count ({this._height})."

        let total = int this._height
        for i = 0 to total - 1 do
            destination.[i] <- this._hydrator.Invoke(this._cols, int64 i)
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
                destination.[i] <- this._hydrator.Invoke(this._cols, int64 i)
            written <- total
            true

    /// <summary>
    /// Materializes all or remaining mapped rows into a strongly typed array.
    /// </summary>
    member this.ToArray() : 'T array =
        if this._height = 0L then [||]
        elif this._height > int64 Int32.MaxValue then
            raise (OverflowException($"DataFrame height ({this._height}) exceeds Int32.MaxValue."))
        elif this._index = -1L then
            let total = int this._height
            let arr = Array.zeroCreate<'T> total
            for i = 0 to total - 1 do
                arr.[i] <- this._hydrator.Invoke(this._cols, int64 i)
            arr
        else
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
    
    /// <summary>
    /// Groups the rows of the enumerator using the key-generating function,
    /// returning an idiomatic F# immutable Map of key to row list.
    /// </summary>
    let inline groupBy ([<InlineIfLambda>] keySelector: 'T -> 'Key) (enumerator: RowEnumerator<'T>) : Map<'Key, 'T list> =
        let dict = Dictionary<'Key, ResizeArray<'T>>()
        let mutable it = enumerator
        while it.MoveNext() do
            let cur = it.Current
            let key = keySelector cur
            match dict.TryGetValue key with
            | true, list -> list.Add cur
            | false, _   -> 
                let list = ResizeArray<'T>()
                list.Add cur
                dict.[key] <- list
        
        dict
        |> Seq.map (fun kv -> kv.Key, kv.Value |> Seq.toList)
        |> Map.ofSeq


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
            RowEnumerator<'T> this
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
            let hydrator = FSharpRowMapper<'T>.GetOrCreate this
            let width = int this.Width
            let cols = Array.init width (fun i -> this.[i])
            let height = this.Height

            seq {
                for r = 0L to height - 1L do
                    yield hydrator.Invoke(cols, r)
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
        if PolarsTypeHelper.AcceptsNull t then
            Unchecked.defaultof<'TSource>
        else
            invalidOp "Sequence contains no elements."
    
    interface IDataFrameMaterializer with
        member _.Materialize<'T>(handle: DataFrameHandle) : IEnumerable<'T> =
            let height = PolarsWrapper.DataFrameHeight handle
            let width = int (PolarsWrapper.DataFrameWidth handle)

            seq {
                let cols = 
                    Array.init width (fun i -> 
                        let colHandle = PolarsWrapper.DataFrameGetColumnAt(handle, int64 i)
                        new Series(colHandle)
                    )
                let hydrator = FSharpRowMapper<'T>.GetOrCreate cols
                for rowIdx in 0L .. (height - 1L) do
                    let item = hydrator.Invoke(cols, rowIdx)
                    yield item
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
                    let getValueMethod = typeof<Series>.GetMethod("GetValue", [| typeof<int64>; typeof<bool> |]).MakeGenericMethod([| underlyingType |])
                    let rawVal = getValueMethod.Invoke(firstCol, [| box 0L; box true |])
                    Enum.ToObject(targetType, rawVal) :?> 'T
                else
                    firstCol.GetValue<'T>(0L)

        member _.Aggregate<'TSource, 'TAccum>(handle:DataFrameHandle,seed:'TAccum,folder: Func<'TAccum, 'TSource, 'TAccum> ):'TAccum =
            use df = new DataFrame(handle)
            let mutable enumerator = df.Rows<'TSource>() 
                
            let mutable acc = seed
            while enumerator.MoveNext() do
                
                acc <- folder.Invoke(acc, enumerator.Current)
                
            acc

        /// Overload 1: Static seed value for all groups
        member _.AggregateBy<'TSource, 'TKey, 'TAccum>(
            handle: DataFrameHandle,
            keySelector: Func<'TSource, 'TKey>,
            seed: 'TAccum,
            folder: Func<'TAccum, 'TSource, 'TAccum>,
            comparer: IEqualityComparer<'TKey>) : IEnumerable<KeyValuePair<'TKey, 'TAccum>> =

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
                    | true, existingVal ->
                        dict.[key] <- folder.Invoke(existingVal, current)
                    | false, _ ->
                        dict.[key] <- folder.Invoke(seed, current)

            dict :> IEnumerable<KeyValuePair<'TKey, 'TAccum>>

        /// Overload 2: Dynamic per-key seed factory
        member _.AggregateBy<'TSource, 'TKey, 'TAccum>(
            handle: DataFrameHandle,
            keySelector: Func<'TSource, 'TKey>,
            seedSelector: Func<'TKey, 'TAccum>,
            folder: Func<'TAccum, 'TSource, 'TAccum>,
            comparer: IEqualityComparer<'TKey>) : IEnumerable<KeyValuePair<'TKey, 'TAccum>> =

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
                    | true, existingVal ->
                        dict.[key] <- folder.Invoke(existingVal, current)
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

                if found then result
                else invalidOp "Sequence contains no matching element."

        member _.FirstOrDefault<'TSource>(handle: DataFrameHandle, predicate: Func<'TSource, bool>, defaultValue: 'TSource) : 'TSource =
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

                if found then lastMatch
                else invalidOp "Sequence contains no matching element."

        member _.LastOrDefault<'TSource>(handle: DataFrameHandle, predicate: Func<'TSource, bool>, defaultValue: 'TSource) : 'TSource =
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
                if height = 0L then invalidOp "Sequence contains no elements."
                elif height > 1L then invalidOp "Sequence contains more than one element."
                else
                    df.Rows<'TSource>().First()
                    
            elif height = 0L then invalidOp "Sequence contains no matching element."
            else
                let mutable enumerator = df.Rows<'TSource>()
                let mutable found = false
                let mutable matchVal = Unchecked.defaultof<'TSource>
                while enumerator.MoveNext() do
                    if predicate.Invoke enumerator.Current then
                        if found then invalidOp "Sequence contains more than one matching element."
                        matchVal <- enumerator.Current
                        found <- true
                if found then matchVal
                else invalidOp "Sequence contains no matching element."

        member _.SingleOrDefault<'TSource>(handle: DataFrameHandle, predicate: Func<'TSource, bool>, defaultValue: 'TSource) : 'TSource =
            use df = new DataFrame(handle)
            let height = df.Height

            if isNull (box predicate) then
                if height = 0L then defaultValue
                elif height > 1L then invalidOp "Sequence contains more than one element."
                else
                    match df.Rows<'TSource>().TryFirstValue() with
                    | ValueSome row -> row
                    | ValueNone -> defaultValue
            elif height = 0L then defaultValue
            else
                let mutable enumerator = df.Rows<'TSource>()
                let mutable found = false
                let mutable matchVal = defaultValue
                while enumerator.MoveNext() do
                    if predicate.Invoke enumerator.Current then
                        if found then invalidOp "Sequence contains more than one matching element."
                        matchVal <- enumerator.Current
                        found <- true
                if found then matchVal else defaultValue

        member _.ElementAt<'TSource>(handle: DataFrameHandle, index: int64) : 'TSource =
            if index < 0L then
                raise (ArgumentOutOfRangeException("index", "Index was out of range."))
            use df = new DataFrame(handle)
            
            df.Rows<'TSource>().Item index
        member _.ElementAtOrDefault<'TSource>(handle: DataFrameHandle, index: int64) : 'TSource =
            if index < 0L then Unchecked.defaultof<'TSource>
            else
                use df = new DataFrame(handle)
                match df.Rows<'TSource>().TryGetValueItem index with
                | ValueSome row -> row
                | ValueNone -> Unchecked.defaultof<'TSource>

        member _.Contains<'TSource>(handle: DataFrameHandle, item: 'TSource, comparer: IEqualityComparer<'TSource>) : bool =
            use df = new DataFrame(handle)
            if df.Height = 0L then false
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

        member _.SequenceEqual<'TSource>(handle1: DataFrameHandle, handle2: DataFrameHandle, comparer: IEqualityComparer<'TSource>) : bool =
            use df1 = new DataFrame(handle1)
            use df2 = new DataFrame(handle2)
            if df1.Height <> df2.Height then false
            elif df1.Height = 0L then true
            else
                let comp = if isNull (box comparer) then EqualityComparer<'TSource>.Default :> IEqualityComparer<'TSource> else comparer
                let mutable enum1 = df1.Rows<'TSource>()
                let mutable enum2 = df2.Rows<'TSource>()
                let mutable equal = true
                while equal && enum1.MoveNext() && enum2.MoveNext() do
                    if not (comp.Equals(enum1.Current, enum2.Current)) then
                        equal <- false
                equal

        member _.SequenceEqual<'TSource>(handle: DataFrameHandle, second: IEnumerable<'TSource>, comparer: IEqualityComparer<'TSource>) : bool =
            if isNull (box second) then nullArg "second"
            use df = new DataFrame(handle)
            let comp = if isNull (box comparer) then EqualityComparer<'TSource>.Default :> IEqualityComparer<'TSource> else comparer

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

        member _.MinBy<'TSource, 'TKey>(handle: DataFrameHandle, keySelector: Func<'TSource, 'TKey>, comparer: IComparer<'TKey>) : 'TSource =
            if isNull (box keySelector) then nullArg "keySelector"
            use df = new DataFrame(handle)
            if df.Height = 0L then FSharpRowCursorMaterializer.handleEmptySequence<'TSource>()
            else
                let comp = if isNull (box comparer) then Comparer<'TKey>.Default :> IComparer<'TKey> else comparer
                let mutable enumerator = df.Rows<'TSource>()
                if not (enumerator.MoveNext()) then FSharpRowCursorMaterializer.handleEmptySequence<'TSource>()
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

        member _.MaxBy<'TSource, 'TKey>(handle: DataFrameHandle, keySelector: Func<'TSource, 'TKey>, comparer: IComparer<'TKey>) : 'TSource =
            if isNull (box keySelector) then nullArg "keySelector"
            use df = new DataFrame(handle)
            if df.Height = 0L then FSharpRowCursorMaterializer.handleEmptySequence<'TSource>()
            else
                let comp = if isNull (box comparer) then Comparer<'TKey>.Default :> IComparer<'TKey> else comparer
                let mutable enumerator = df.Rows<'TSource>()
                if not (enumerator.MoveNext()) then FSharpRowCursorMaterializer.handleEmptySequence<'TSource>()
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


