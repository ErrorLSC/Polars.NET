namespace Polars.FSharp

open Polars.NET.Core
open Polars.NET.Core.Arrow
open System

type DataTypeKind =
    | SameAsInput
    | Null
    | Boolean
    | Int8 | Int16 | Int32 | Int64
    | UInt8 | UInt16 | UInt32 | UInt64
    | Float32 | Float64
    | String
    | Binary
    | Date
    | Time
    | Int128 | UInt128
    | Float16
    | Datetime of TimeUnit * string option
    | Duration of TimeUnit
    | Categorical of Categories option
    | Decimal of int option * int option
    | List of DataType
    | Struct of Field list
    | Array of DataType * uint[]
    | Enum of FrozenCategories
    | Extension of {| Name: string; Storage: DataType; Metadata: string option |}
    | Unknown

/// <summary>
/// Polars data types for casting and schema definitions.
/// </summary>
and DataType (handle: DataTypeHandle, kind: DataTypeKind) =
    let mutable disposed = false
    [<DefaultValue>]
    val mutable private _displayString : string
    static let toUnitCode tu =
        match tu with
        | Nanoseconds -> 0uy
        | Microseconds -> 1uy
        | Milliseconds -> 2uy
    member internal this.Handle = handle
    member this.Kind = kind

    static member SameAsInput = new DataType(PolarsWrapper.NewPrimitiveType(0), DataTypeKind.SameAsInput)
    static member Null        = new DataType(PolarsWrapper.NewPrimitiveType(18), DataTypeKind.Null)
    static member Boolean     = new DataType(PolarsWrapper.NewPrimitiveType(1), DataTypeKind.Boolean)
    static member Int8        = new DataType(PolarsWrapper.NewPrimitiveType(2), DataTypeKind.Int8)
    static member Int16       = new DataType(PolarsWrapper.NewPrimitiveType(3), DataTypeKind.Int16)
    static member Int32       = new DataType(PolarsWrapper.NewPrimitiveType(4), DataTypeKind.Int32)
    static member Int64       = new DataType(PolarsWrapper.NewPrimitiveType(5), DataTypeKind.Int64)
    static member UInt8       = new DataType(PolarsWrapper.NewPrimitiveType(6), DataTypeKind.UInt8)
    static member UInt16      = new DataType(PolarsWrapper.NewPrimitiveType(7), DataTypeKind.UInt16)
    static member UInt32      = new DataType(PolarsWrapper.NewPrimitiveType(8), DataTypeKind.UInt32)
    static member UInt64      = new DataType(PolarsWrapper.NewPrimitiveType(9), DataTypeKind.UInt64)
    static member Float32     = new DataType(PolarsWrapper.NewPrimitiveType(10), DataTypeKind.Float32)
    static member Float64     = new DataType(PolarsWrapper.NewPrimitiveType(11), DataTypeKind.Float64)
    static member String      = new DataType(PolarsWrapper.NewPrimitiveType(12), DataTypeKind.String)
    static member Binary      = new DataType(PolarsWrapper.NewPrimitiveType(17), DataTypeKind.Binary)
    static member Date        = new DataType(PolarsWrapper.NewPrimitiveType(13), DataTypeKind.Date)
    static member Time        = new DataType(PolarsWrapper.NewPrimitiveType(15), DataTypeKind.Time)
    static member Int128      = new DataType(PolarsWrapper.NewPrimitiveType(24), DataTypeKind.Int128)
    static member UInt128     = new DataType(PolarsWrapper.NewPrimitiveType(25), DataTypeKind.UInt128)
    static member Float16     = new DataType(PolarsWrapper.NewPrimitiveType(26), DataTypeKind.Float16)

    // ---------- Equality & Hashing ----------

    interface IEquatable<DataType> with
        member this.Equals(other: DataType) =
            if obj.ReferenceEquals(other, null) then false
            elif obj.ReferenceEquals(this, other) then true
            else PolarsWrapper.DataTypeEq(this.Handle, other.Handle)

    override this.Equals(otherObj: obj) =
        match otherObj with
        | :? DataType as other -> (this :> IEquatable<DataType>).Equals(other)
        | _ -> false

    override this.GetHashCode() = 
        this.ToString().GetHashCode()

    static member op_Equality (left: DataType, right: DataType) =
        if obj.ReferenceEquals(left, right) then true
        elif obj.ReferenceEquals(left, null) then false
        else left.Equals(right)

    static member op_Inequality (left: DataType, right: DataType) =
        not (left = right)

    interface IDisposable with
        member this.Dispose() =
            if not disposed then
                if not (isNull (box handle)) && not handle.IsInvalid then
                    handle.Dispose()
                disposed <- true
                GC.SuppressFinalize(this)

    interface IPolarsDataType with
        member this.GetArrowType() : Apache.Arrow.Types.IArrowType =
            ArrowFfiBridge.ImportDataType this.Handle


    member internal this.CreateHandle() : DataTypeHandle =
        match this.Kind with
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

        | Datetime(unit, tz) ->
            let tzStr = Option.toObj tz
            PolarsWrapper.NewDateTimeType(toUnitCode unit, tzStr)

        | Duration unit ->
            PolarsWrapper.NewDurationType(toUnitCode unit)

        | Categorical maybeCat ->
            let cat = defaultArg maybeCat (Categories.Global())
            PolarsWrapper.NewCategoricalType(cat.Handle)

        | Enum frozenCat ->
            PolarsWrapper.NewEnumType(frozenCat.Handle)

        | Decimal(p, s) ->
            let prec = defaultArg p 0
            let scale = defaultArg s 0
            PolarsWrapper.NewDecimalType(prec, scale)

        | List innerType ->
            use innerHandle = innerType.CreateHandle()
            PolarsWrapper.NewListType(innerHandle)

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
            PolarsWrapper.NewExtensionType(ext.Name, ext.Storage.CreateHandle(), Option.toObj ext.Metadata)

        | Unknown -> PolarsWrapper.NewPrimitiveType 0

    static member Datetime(unit, ?tz) =
        let handle = PolarsWrapper.NewDateTimeType(toUnitCode unit, Option.toObj tz)
        new DataType(handle, DataTypeKind.Datetime(unit, tz))

    static member Duration unit =
        let handle = PolarsWrapper.NewDurationType(toUnitCode unit)
        new DataType(handle, DataTypeKind.Duration unit)

    static member Categorical ?categories =
        let cat = defaultArg categories (Categories.Global())
        let handle = PolarsWrapper.NewCategoricalType(cat.Handle)
        new DataType(handle, DataTypeKind.Categorical(Some cat))

    static member Enum(frozen: FrozenCategories) =
        let handle = PolarsWrapper.NewEnumType(frozen.Handle)
        new DataType(handle, DataTypeKind.Enum frozen)

    static member Decimal(?precision, ?scale) =
        let prec = defaultArg precision 0
        let sc = defaultArg scale 0
        let handle = PolarsWrapper.NewDecimalType(prec, sc)
        new DataType(handle, DataTypeKind.Decimal(Some prec, Some sc))

    static member List(inner: DataType) =
        use innerHandle = inner.CreateHandle()
        let handle = PolarsWrapper.NewListType(innerHandle)
        new DataType(handle, DataTypeKind.List inner)

    static member Struct(fields: Field list) =
        let names = fields |> List.map (fun f -> f.Name) |> List.toArray
        let typeHandles = fields |> List.map (fun f -> f.DataType.CreateHandle()) |> List.toArray
        try
            let handle = PolarsWrapper.NewStructType(names, typeHandles)
            new DataType(handle, DataTypeKind.Struct fields)
        finally
            for h in typeHandles do h.Dispose()

    static member Array(inner: DataType, shape: uint[]) =
        if shape.Length = 0 then invalidArg "shape" "Shape must not be empty."
        use innerHandle = inner.CreateHandle()
        let span = System.ReadOnlySpan(shape)
        let handle = PolarsWrapper.NewArrayType(innerHandle, span)
        new DataType(handle, DataTypeKind.Array(inner, shape))

    static member Extension(name: string, storage: DataType, ?metadata: string) =
        let handle = PolarsWrapper.NewExtensionType(name, storage.CreateHandle(), Option.toObj metadata)
        new DataType(handle, DataTypeKind.Extension {| Name = name; Storage = storage; Metadata = metadata |})
    static member FromHandle(handle: DataTypeHandle) : DataType =
        let kind = PolarsWrapper.GetDataTypeKind handle
        match kind with
        | PlDataType.Boolean -> DataType.Boolean
        | PlDataType.Int8 -> DataType.Int8
        | PlDataType.Int16 -> DataType.Int16
        | PlDataType.Int32 -> DataType.Int32
        | PlDataType.Int64 -> DataType.Int64
        | PlDataType.UInt8 -> DataType.UInt8
        | PlDataType.UInt16 -> DataType.UInt16
        | PlDataType.UInt32 -> DataType.UInt32
        | PlDataType.UInt64 -> DataType.UInt64
        | PlDataType.Float32 -> DataType.Float32
        | PlDataType.Float64 -> DataType.Float64
        | PlDataType.String -> DataType.String
        | PlDataType.Date -> DataType.Date
        | PlDataType.Int128 -> DataType.Int128
        | PlDataType.UInt128 -> DataType.UInt128
        | PlDataType.Float16 -> DataType.Float16

        | PlDataType.Datetime ->
            let unitCode = PolarsWrapper.GetTimeUnit handle
            let unit =
                match unitCode with
                | PlTimeUnit.Nanoseconds -> Nanoseconds
                | PlTimeUnit.Microseconds -> Microseconds
                | PlTimeUnit.Milliseconds -> Milliseconds
                | _ -> Microseconds
            let tz = Option.ofObj (PolarsWrapper.GetTimeZone handle)
            DataType.Datetime(unit, ?tz=tz)

        | PlDataType.Time -> DataType.Time
        | PlDataType.Duration ->
            let unitCode = PolarsWrapper.GetTimeUnit handle
            let unit =
                match unitCode with
                | PlTimeUnit.Nanoseconds -> Nanoseconds
                | PlTimeUnit.Microseconds -> Microseconds
                | PlTimeUnit.Milliseconds -> Milliseconds
                | _ -> Microseconds
            DataType.Duration unit

        | PlDataType.Binary -> DataType.Binary
        | PlDataType.Null -> DataType.Null

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
            DataType.Struct fields

        | PlDataType.List ->
            use innerHandle = PolarsWrapper.GetInnerType handle
            let innerType = DataType.FromHandle innerHandle
            DataType.List innerType

        | PlDataType.Categorical ->
            let maybeCatHandle = PolarsWrapper.GetCategories handle
            let maybeCat =
                if maybeCatHandle = null || maybeCatHandle.IsInvalid then None
                else Some (new Categories(maybeCatHandle))
            new DataType(handle, DataTypeKind.Categorical maybeCat)

        | PlDataType.Enum ->
            let frozenHandle = PolarsWrapper.GetEnumCategories handle
            if frozenHandle = null || frozenHandle.IsInvalid then
                raise (InvalidOperationException "Invalid FrozenCategoriesHandle in Enum type")
            else
                new DataType(handle, DataTypeKind.Enum (new FrozenCategories(frozenHandle)))

        | PlDataType.Decimal ->
            let mutable prec = 0
            let mutable scale = 0
            PolarsWrapper.GetDecimalInfo(handle, &prec, &scale)
            DataType.Decimal(prec, scale)

        | PlDataType.Array ->
            use innerHandle = PolarsWrapper.GetInnerType handle
            let shape = PolarsWrapper.GetArrayShape handle
            let innerType = DataType.FromHandle innerHandle
            DataType.Array(innerType, shape)

        | PlDataType.Extension ->
            let name = PolarsWrapper.DataTypeGetExtensionName handle
            let storage = DataType.FromHandle(PolarsWrapper.GetInnerType handle)
            let metadata = Option.ofObj (PolarsWrapper.DataTypeGetExtensionMetadata handle)
            match ExtensionRegistry.TryResolve(name) with
            | Some factory -> factory(storage, metadata)
            | None -> DataType.Extension(name, storage, ?metadata=metadata)

        | _ -> DataType.SameAsInput // fallback

    override this.ToString() =
        if isNull this._displayString then
            this._displayString <- PolarsWrapper.GetDataTypeString(this.Handle)
        this._displayString
    member this.IsNumeric =
        match this.Kind with
        | UInt8 | UInt16 | UInt32 | UInt64
        | Int8 | Int16 | Int32 | Int64
        | Float32 | Float64 | Int128 | Float16
        | Decimal _ -> true
        | _ -> false
    member this.IsInteger =
        match this.Kind with
        | Int8 | Int16 | Int32 | Int64
        | UInt8 | UInt16 | UInt32 | UInt64
        | Int128 | UInt128 -> true
        | _ -> false

    member this.IsFloat =
        match this.Kind with
        | Float16 | Float32 | Float64 -> true
        | _ -> false

    member this.IsDecimal =
        match this.Kind with
        | Decimal _ -> true
        | _ -> false

    member this.IsExtension =
        match this.Kind with
        | Extension _ -> true
        | _ -> false

    member this.IsNested =
        match this.Kind with
        | List _ | Array _ | Struct _ -> true
        | _ -> false

    member this.IsTemporal =
        match this.Kind with
        | Duration _ | Date | Datetime _ | Time -> true
        | _ -> false

    member this.IsSignedInteger =
        match this.Kind with
        | Int8 | Int16 | Int32 | Int64 | Int128 -> true
        | _ -> false

    member this.IsUnsignedInteger =
        match this.Kind with
        | UInt8 | UInt16 | UInt32 | UInt64 | UInt128 -> true
        | _ -> false
    member this.Code =
        match this.Kind with
        | SameAsInput | Unknown -> 0
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
    /// <summary>Get the corresponding PlDataType enum value.</summary>
    member this.ToPlDataType() = enum<PlDataType>(this.Code)
    member this.GetArrowType() : Apache.Arrow.Types.IArrowType =
        ArrowFfiBridge.ImportDataType(this.Handle)

    member this.GetNetType() : Type =
        ArrowTypeResolver.GetNetTypeFromArrowType(this.GetArrowType())

    static member FromNetType<'T>() =
        let arrowType = ArrowTypeResolver.GetArrowTypeFromNetType(typeof<'T>)
        DataType.FromArrowType(arrowType)

    static member FromNetType(t: Type) =
        let arrowType = ArrowTypeResolver.GetArrowTypeFromNetType(t)
        DataType.FromArrowType(arrowType)

    static member op_Implicit(t: Type) = DataType.FromNetType(t)

    // ---------- FromArrowType ----------
    static member FromArrowType(arrowType: Apache.Arrow.Types.IArrowType) =
        match arrowType with
        | :? Apache.Arrow.Types.Int8Type -> DataType.Int8
        | :? Apache.Arrow.Types.Int16Type -> DataType.Int16
        | :? Apache.Arrow.Types.Int32Type -> DataType.Int32
        | :? Apache.Arrow.Types.Int64Type -> DataType.Int64
        | :? Apache.Arrow.Types.UInt8Type -> DataType.UInt8
        | :? Apache.Arrow.Types.UInt16Type -> DataType.UInt16
        | :? Apache.Arrow.Types.UInt32Type -> DataType.UInt32
        | :? Apache.Arrow.Types.UInt64Type -> DataType.UInt64
        | :? Apache.Arrow.Types.HalfFloatType -> DataType.Float16
        | :? Apache.Arrow.Types.FloatType -> DataType.Float32
        | :? Apache.Arrow.Types.DoubleType -> DataType.Float64
        | :? Apache.Arrow.Types.BooleanType -> DataType.Boolean
        | :? Apache.Arrow.Types.Decimal128Type as d -> DataType.Decimal(d.Precision, d.Scale)
        | :? Apache.Arrow.Types.Decimal256Type as d -> DataType.Decimal(d.Precision, d.Scale)
        | :? Apache.Arrow.Types.StringType
        | :? Apache.Arrow.Types.StringViewType -> DataType.String
        | :? Apache.Arrow.Types.BinaryType
        | :? Apache.Arrow.Types.BinaryViewType -> DataType.Binary
        | :? Apache.Arrow.Types.Date32Type -> DataType.Date
        | :? Apache.Arrow.Types.Time64Type -> DataType.Time
        | :? Apache.Arrow.Types.TimestampType as t ->
            let unit =
                match t.Unit with
                | Apache.Arrow.Types.TimeUnit.Microsecond -> Microseconds
                | Apache.Arrow.Types.TimeUnit.Millisecond -> Milliseconds
                | Apache.Arrow.Types.TimeUnit.Nanosecond -> Nanoseconds
                | _ -> Microseconds
            DataType.Datetime(unit, ?tz=Option.ofObj t.Timezone)
        | :? Apache.Arrow.Types.DurationType as d ->
            let unit =
                match d.Unit with
                | Apache.Arrow.Types.TimeUnit.Microsecond -> Microseconds
                | Apache.Arrow.Types.TimeUnit.Millisecond -> Milliseconds
                | Apache.Arrow.Types.TimeUnit.Nanosecond -> Nanoseconds
                | _ -> Microseconds
            DataType.Duration unit
        | :? Apache.Arrow.Types.ListType as l -> DataType.List(DataType.FromArrowType l.ValueDataType)
        | :? Apache.Arrow.Types.LargeListType as l -> DataType.List(DataType.FromArrowType l.ValueDataType)
        | :? Apache.Arrow.Types.FixedSizeListType as l ->
            DataType.Array(DataType.FromArrowType l.ValueDataType, [| uint l.ListSize |])
        | :? Apache.Arrow.Types.StructType as s ->
            let fields =
                s.Fields |> Seq.map (fun f -> { Name = f.Name; DataType = DataType.FromArrowType f.DataType }) |> Seq.toList
            DataType.Struct fields
        | _ -> raise (NotSupportedException(sprintf "ArrowType %s is not supported yet." (arrowType.GetType().Name)))

and Field = { Name: string; DataType: DataType }

/// <summary>
/// Represents a Polars categorical type with optional name, namespace, and physical representation.
/// </summary>
and Categories (handle: CategoriesHandle) =
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
and FrozenCategories (handle: FrozenCategoriesHandle) =
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