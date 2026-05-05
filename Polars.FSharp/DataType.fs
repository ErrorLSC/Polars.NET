namespace Polars.FSharp

open Polars.NET.Core
open Polars.NET.Core.Arrow
open System

/// <summary>
/// Polars data types for casting and schema definitions.
/// </summary>
type DataType =
    | Boolean
    | Int8 | Int16 | Int32 | Int64
    | UInt8 | UInt16 | UInt32 | UInt64
    | Float32 | Float64
    | String
    | Date | Datetime of TimeUnit * string option | Time
    | Duration of TimeUnit
    | Binary
    | Categorical of Categories option 
    | Decimal of precision: int option * scale: int option
    | Unknown | SameAsInput | Null | List of DataType | Array of DataType * shape: uint[] 
    | Struct of Field list 
    | Int128 | UInt128 | Float16 | Enum of FrozenCategories
    | Extension of {| Name: string; Storage: DataType; Metadata: string option |}
    member this.Code : int =
        match this with
        | Unknown | SameAsInput -> 0
        | Boolean -> 1
        | Int8 -> 2
        | Int16 -> 3
        | Int32 -> 4
        | Int64 -> 5
        | UInt8 -> 6
        | UInt16 -> 7
        | UInt32 -> 8
        | UInt64 -> 9
        | Float32 -> 10
        | Float64 -> 11
        | String -> 12
        | Date -> 13
        | Datetime _ -> 14 
        | Time -> 15
        | Duration _ -> 16
        | Binary -> 17
        | Null -> 18
        | Struct _ -> 19
        | List _ -> 20
        | Categorical _ -> 21
        | Decimal _ -> 22
        | Array _ -> 23
        | Int128 -> 24
        | UInt128 -> 25
        | Float16 -> 26
        | Enum _ -> 27
        | Extension _ -> 28
    static member FromHandle (handle: DataTypeHandle) : DataType =
        let kind = PolarsWrapper.GetDataTypeKind handle

        match kind with
        | PlDataType.Boolean -> Boolean
        | PlDataType.Int8 -> Int8
        | PlDataType.Int16 -> Int16
        | PlDataType.Int32 -> Int32
        | PlDataType.Int64 -> Int64
        | PlDataType.UInt8 -> UInt8
        | PlDataType.UInt16 -> UInt16
        | PlDataType.UInt32 -> UInt32
        | PlDataType.UInt64 -> UInt64
        | PlDataType.Float32 -> Float32
        | PlDataType.Float64 -> Float64
        | PlDataType.String -> String 
        | PlDataType.Date -> Date
        | PlDataType.Int128 -> Int128
        | PlDataType.UInt128 -> UInt128
        | PlDataType.Float16 -> Float16
        
        // --- Complex Type ---
        
        // Datetime
        | PlDataType.Datetime -> 
            let unitCode = PolarsWrapper.GetTimeUnit handle
            let unit = 
                match unitCode with 
                | PlTimeUnit.Nanoseconds -> Nanoseconds 
                | PlTimeUnit.Microseconds -> Microseconds 
                | PlTimeUnit.Milliseconds -> Milliseconds 
                | _ -> Microseconds
            
            let tz = Option.ofObj (PolarsWrapper.GetTimeZone handle)
            Datetime(unit, tz)

        | PlDataType.Time -> Time
        
        // Duration
        | PlDataType.Duration-> 
            let unitCode = PolarsWrapper.GetTimeUnit handle
            let unit = 
                match unitCode with 
                | PlTimeUnit.Nanoseconds -> Nanoseconds 
                | PlTimeUnit.Microseconds -> Microseconds 
                | PlTimeUnit.Milliseconds -> Milliseconds 
                | _ -> Microseconds
            Duration unit

        | PlDataType.Binary -> Binary
        | PlDataType.Null -> Null
        
        // Struct
        | PlDataType.Struct -> 
            let len = PolarsWrapper.GetStructLen handle
            let fields = 
                [ for i in 0UL .. len - 1UL do
                    let mutable name = Unchecked.defaultof<string>
                    let mutable fieldHandle = Unchecked.defaultof<DataTypeHandle>
                    
                    PolarsWrapper.GetStructField(handle, i, &name, &fieldHandle)

                    use h = fieldHandle 
                    yield { Name = name; DataType = DataType.FromHandle h }
                ]
            Struct fields

        // List
        | PlDataType.List -> 
            use innerHandle = PolarsWrapper.GetInnerType handle
            let innerType = DataType.FromHandle innerHandle
            List innerType

        | PlDataType.Categorical ->
            let maybeCatHandle = PolarsWrapper.GetCategories handle
            let maybeCat = 
                if maybeCatHandle = null || maybeCatHandle.IsInvalid then 
                    None
                else 
                    Some (new Categories(maybeCatHandle))
            Categorical maybeCat
        | PlDataType.Enum ->
            let frozenHandle = PolarsWrapper.GetEnumCategories handle
            if frozenHandle = null || frozenHandle.IsInvalid then
                raise (InvalidOperationException "Invalid FrozenCategoriesHandle in Enum type")
            else
                Enum (new FrozenCategories(frozenHandle))
        // Decimal
        | PlDataType.Decimal -> 
            let mutable prec = 0
            let mutable scale = 0
            PolarsWrapper.GetDecimalInfo(handle, &prec, &scale)
            Decimal(Some prec,Some scale)

        | PlDataType.Array -> 
            use innerHandle = PolarsWrapper.GetInnerType handle
            let shape = PolarsWrapper.GetArrayShape handle       // 返回 uint[]
            let innerType = DataType.FromHandle innerHandle
            Array(innerType, shape)

        | PlDataType.Extension ->
            let name = PolarsWrapper.DataTypeGetExtensionName(handle)       // 假设已添加
            let storage = DataType.FromHandle(PolarsWrapper.GetInnerType(handle))
            let metadata = Option.ofObj (PolarsWrapper.DataTypeGetExtensionMetadata(handle))
            
            // 通过注册表尝试解析
            match ExtensionRegistry.TryResolve(name) with
            | Some factory -> factory(storage, metadata)
            | None -> Extension {| Name = name; Storage = storage; Metadata = metadata |}

        | _ -> Unknown

    member this.IsNumeric =
        match this with
        | UInt8 | UInt16 | UInt32 | UInt64
        | Int8 | Int16 | Int32 | Int64
        | Float32 | Float64 | Int128 | Float16
        | Decimal _ -> true
        | _ -> false
    /// <summary>
    /// Get Apache Arrow Type back to Polars.NET DataType.
    /// </summary>
    static member FromArrowType (arrowType: Apache.Arrow.Types.IArrowType) : DataType =
        match arrowType with
        | :? Apache.Arrow.Types.Int8Type -> Int8
        | :? Apache.Arrow.Types.Int16Type -> Int16
        | :? Apache.Arrow.Types.Int32Type -> Int32
        | :? Apache.Arrow.Types.Int64Type -> Int64
        | :? Apache.Arrow.Types.UInt8Type -> UInt8
        | :? Apache.Arrow.Types.UInt16Type -> UInt16
        | :? Apache.Arrow.Types.UInt32Type -> UInt32
        | :? Apache.Arrow.Types.UInt64Type -> UInt64
        | :? Apache.Arrow.Types.HalfFloatType -> Float16
        | :? Apache.Arrow.Types.FloatType -> Float32
        | :? Apache.Arrow.Types.DoubleType -> Float64
        | :? Apache.Arrow.Types.BooleanType -> Boolean
        
        | :? Apache.Arrow.Types.Decimal128Type as d -> Decimal(Some d.Precision, Some d.Scale)
        | :? Apache.Arrow.Types.Decimal256Type as d -> Decimal(Some d.Precision, Some d.Scale)
        
        | :? Apache.Arrow.Types.StringType
        | :? Apache.Arrow.Types.StringViewType -> String
        | :? Apache.Arrow.Types.BinaryType
        | :? Apache.Arrow.Types.BinaryViewType -> Binary
        
        | :? Apache.Arrow.Types.Date32Type -> Date
        | :? Apache.Arrow.Types.Time64Type -> Time
        
        | :? Apache.Arrow.Types.TimestampType as t ->
            let unit = 
                match t.Unit with
                | Apache.Arrow.Types.TimeUnit.Microsecond -> Microseconds
                | Apache.Arrow.Types.TimeUnit.Millisecond -> Milliseconds
                | Apache.Arrow.Types.TimeUnit.Nanosecond -> Nanoseconds
                | _ -> Microseconds
            Datetime(unit, Option.ofObj t.Timezone)
            
        | :? Apache.Arrow.Types.DurationType as d ->
            let unit = 
                match d.Unit with
                | Apache.Arrow.Types.TimeUnit.Microsecond -> Microseconds
                | Apache.Arrow.Types.TimeUnit.Millisecond -> Milliseconds
                | Apache.Arrow.Types.TimeUnit.Nanosecond -> Nanoseconds
                | _ -> Microseconds
            Duration unit
            
        | :? Apache.Arrow.Types.ListType as l -> 
            List(DataType.FromArrowType l.ValueDataType)
        | :? Apache.Arrow.Types.LargeListType as l -> 
            List(DataType.FromArrowType l.ValueDataType)
        | :? Apache.Arrow.Types.FixedSizeListType as l -> 
            Array(DataType.FromArrowType l.ValueDataType, [| uint l.ListSize |])
            
        | :? Apache.Arrow.Types.StructType as s ->
            let fields = 
                s.Fields 
                |> Seq.map (fun f -> { Name = f.Name; DataType = DataType.FromArrowType f.DataType })
                |> Seq.toList
            Struct fields
            
        | _ -> 
            raise (NotSupportedException(sprintf "ArrowType %s is not supported yet." (arrowType.GetType().Name)))
    /// <summary>
    /// Creates a native Polars DataTypeHandle from this F# DataType.
    /// Recursive structures (List, Struct) are handled automatically.
    /// </summary>
    member internal this.CreateHandle() : DataTypeHandle =
        
        let toUnitCode tu = 
            match tu with 
            | Nanoseconds -> 0 
            | Microseconds -> 1 
            | Milliseconds -> 2

        match this with
        | SameAsInput -> PolarsWrapper.NewPrimitiveType 0
        | Null -> PolarsWrapper.NewPrimitiveType 18
        | Boolean -> PolarsWrapper.NewPrimitiveType 1
        | Int8 -> PolarsWrapper.NewPrimitiveType 2
        | Int16 -> PolarsWrapper.NewPrimitiveType 3
        | Int32 -> PolarsWrapper.NewPrimitiveType 4
        | Int64 -> PolarsWrapper.NewPrimitiveType 5
        | UInt8 -> PolarsWrapper.NewPrimitiveType 6
        | UInt16 -> PolarsWrapper.NewPrimitiveType 7
        | UInt32 -> PolarsWrapper.NewPrimitiveType 8
        | UInt64 -> PolarsWrapper.NewPrimitiveType 9
        | Float32 -> PolarsWrapper.NewPrimitiveType 10
        | Float64 -> PolarsWrapper.NewPrimitiveType 11
        | String -> PolarsWrapper.NewPrimitiveType 12
        | Binary -> PolarsWrapper.NewPrimitiveType 17
        | Date -> PolarsWrapper.NewPrimitiveType 13
        | Time -> PolarsWrapper.NewPrimitiveType 15
        | Int128 -> PolarsWrapper.NewPrimitiveType 24
        | UInt128 -> PolarsWrapper.NewPrimitiveType 25
        | Float16 -> PolarsWrapper.NewPrimitiveType 26
        
        // --- Complex Type ---

        // Datetime: Unit and Timezone
        | Datetime(unit, tz) ->
            let code = toUnitCode unit
            let tzStr = Option.toObj tz // None -> null
            PolarsWrapper.NewDateTimeType(byte code, tzStr)

        // Duration
        | Duration unit ->
            let code = toUnitCode unit
            PolarsWrapper.NewDurationType (byte code)

        // Categorical
        | Categorical maybeCat ->
            let cat = 
                match maybeCat with
                | Some c -> c
                | None -> Categories.Global()
            PolarsWrapper.NewCategoricalType cat.Handle
        | Enum frozenCat ->
            PolarsWrapper.NewEnumType frozenCat.Handle

        // Decimal: precision (p, s)
        | Decimal(p, s) ->
            let prec = defaultArg p 0
            let scale = defaultArg s 0 
            PolarsWrapper.NewDecimalType(prec, scale)

        // List:
        | List innerType ->
            use innerHandle = innerType.CreateHandle()
            
            PolarsWrapper.NewListType innerHandle

        // Struct
        | Struct fields ->
            let names = fields |> List.map (fun f -> f.Name) |> List.toArray

            let typeHandles = fields |> List.map (fun f -> f.DataType.CreateHandle()) |> List.toArray
            
            try
                PolarsWrapper.NewStructType(names, typeHandles)
            finally
                for h in typeHandles do h.Dispose()

        | Array(innerType, shape) ->
            if shape.Length = 0 then
                invalidArg "shape" "Shape must not be empty."
            use innerHandle = innerType.CreateHandle()
            let span = System.ReadOnlySpan(shape)
            PolarsWrapper.NewArrayType(innerHandle, span)

        | Extension ext ->
            PolarsWrapper.NewExtensionType(ext.Name, ext.Storage.CreateHandle() , Option.toObj ext.Metadata)

        | Unknown -> PolarsWrapper.NewPrimitiveType 0
       
    interface IDisposable with
        member this.Dispose() = 
            ()

    interface IPolarsDataType with
        member this.GetArrowType (): Apache.Arrow.Types.IArrowType = 
            use handle = this.CreateHandle()
            
            ArrowFfiBridge.ImportDataType handle

and Field = { Name: string; DataType: DataType }

/// <summary>
/// Represents a Polars categorical type with optional name, namespace, and physical representation.
/// </summary>
and Categories internal (handle: CategoriesHandle) =
    let mutable disposed = false

    // --- Public constructors ---
    
    /// <summary>Create a new Categories with optional name, namespace, and physical type.</summary>
    new (?name: string, ?nameSpace: string, ?physical: CategoricalPhysical) =
        let physical = defaultArg physical CategoricalPhysical.U32
        let name = Option.toObj name            // None -> null
        let nameSpace = defaultArg nameSpace ""  // None -> ""
        let h = PolarsWrapper.CategoriesNew(name, nameSpace, physical.ToNative())
        new Categories(h)    
    // --- Properties ---
    member internal this.Handle = handle
    member this.Name = PolarsWrapper.CategoriesGetName(handle)
    member this.NameSpace = PolarsWrapper.CategoriesGetNameSpace(handle)
    member this.IsGlobal = PolarsWrapper.CategoriesIsGlobal(handle)
    member this.Physical =
        let raw : PlCategoricalPhysical = PolarsWrapper.CategoriesPhysical(handle)
        match raw with
        | PlCategoricalPhysical.U32 -> CategoricalPhysical.U32
        | PlCategoricalPhysical.U16 -> CategoricalPhysical.U16
        | PlCategoricalPhysical.U8  -> CategoricalPhysical.U8
        | _ -> failwithf "Unexpected PlCategoricalPhysical value: %A" raw

    member this.Hash = PolarsWrapper.CategoriesHash(handle)

    // --- Factory methods ---

    /// <summary>Create a random Categories.</summary>
    static member Random(?nameSpace: string, ?physical: CategoricalPhysical) =
        let phys = defaultArg physical CategoricalPhysical.U32
        let nameSpace = defaultArg nameSpace "" 
        let handle = PolarsWrapper.CategoriesRandom(nameSpace, phys.ToNative())
        new Categories(handle)

    /// <summary>Get a global (unnamed) Categories.</summary>
    static member Global() =
        new Categories(PolarsWrapper.CategoriesGlobal())

    // --- Conversion ---

    /// <summary>Freeze into a FrozenCategories.</summary>
    member this.Freeze() =
        new FrozenCategories(PolarsWrapper.CategoriesFreeze handle)

    // --- Equality ---

    interface IEquatable<Categories> with
        member this.Equals(other: Categories) =
            if obj.ReferenceEquals(this, other) then true
            elif obj.ReferenceEquals(other, null) then false
            else this.Hash = other.Hash

    override this.Equals(obj: obj) =
        match obj with
        | :? Categories as other -> (this :> IEquatable<_>).Equals(other)
        | _ -> false

    override this.GetHashCode() = int this.Hash

    static member op_Equality (left: Categories, right: Categories) =
        if obj.ReferenceEquals(left, right) then true
        elif obj.ReferenceEquals(left, null) then false
        else left.Equals(right)

    static member op_Inequality (left: Categories, right: Categories) =
        not (left = right)

    // --- IDisposable ---

    interface IDisposable with
        member this.Dispose() =
            if not disposed then
                if handle |> box |> isNull |> not && not handle.IsInvalid then
                    handle.Dispose()
                disposed <- true
                GC.SuppressFinalize(this)

    // --- Display ---

    override this.ToString() =
        let name = this.Name
        let ns = this.NameSpace
        let global' = this.IsGlobal
        let phys = this.Physical
        $"Categories(Name={name}, Namespace={ns}, Global={global'}, Physical={phys})"

/// <summary>
/// Represents a frozen (immutable) categorical type with an explicit list of categories.
/// </summary>
and FrozenCategories internal (handle: FrozenCategoriesHandle) =
    let mutable disposed = false

    /// <summary>Create a FrozenCategories from a string array of categories.</summary>
    new (categories: string[]) =
        let handle = PolarsWrapper.FrozenCategoriesNew(categories)
        new FrozenCategories(handle)

    member this.Handle = handle

    /// <summary>The physical representation of the categories.</summary>
    member this.Physical =
        let raw : PlCategoricalPhysical = PolarsWrapper.FrozenCategoriesPhysical(handle)
        match raw with
        | PlCategoricalPhysical.U32 -> CategoricalPhysical.U32
        | PlCategoricalPhysical.U16 -> CategoricalPhysical.U16
        | PlCategoricalPhysical.U8  -> CategoricalPhysical.U8
        | _ -> failwithf "Unexpected PlCategoricalPhysical value: %A" raw

    member this.Hash = PolarsWrapper.FrozenCategoriesHash(handle)

    interface IEquatable<FrozenCategories> with
        member this.Equals(other: FrozenCategories) =
            if obj.ReferenceEquals(this, other) then true
            elif obj.ReferenceEquals(other, null) then false
            else this.Hash = other.Hash

    override this.Equals(obj: obj) =
        match obj with
        | :? FrozenCategories as other -> (this :> IEquatable<_>).Equals(other)
        | _ -> false

    override this.GetHashCode() = int this.Hash

    static member op_Equality (left: FrozenCategories, right: FrozenCategories) =
        if obj.ReferenceEquals(left, right) then true
        elif obj.ReferenceEquals(left, null) then false
        else left.Equals(right)

    static member op_Inequality (left: FrozenCategories, right: FrozenCategories) =
        not (left = right)

    // ---------- IDisposable ----------

    interface IDisposable with
        member this.Dispose() =
            if not disposed then
                if not (obj.ReferenceEquals(handle, null)) && not handle.IsInvalid then
                    handle.Dispose()
                disposed <- true
                GC.SuppressFinalize(this)

and ExtensionRegistry private () =
    static let registry = System.Collections.Concurrent.ConcurrentDictionary<string, (DataType * string option -> DataType) option>()

    static member Register(extName: string, factory: (DataType * string option -> DataType) option) =
        if not (registry.TryAdd(extName, factory)) then
            invalidOp $"Extension type '{extName}' is already registered."

    static member Unregister(extName: string) =
        registry.TryRemove(extName) |> ignore

    static member internal TryResolve(extName: string) =
        match registry.TryGetValue(extName) with
        | true, Some factory -> Some factory
        | true, None -> 
            Some (fun (storage, _) -> storage)
        | false, _ -> None