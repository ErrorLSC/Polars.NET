namespace Polars.NET.Linq

open System
open System.Collections.Concurrent
open System.Reflection
open System.Text.Json.Serialization
open Polars.NET.Core
open Polars.NET.Core.Helpers
open Polars.NET.Core.Arrow

/// Metadata representation of an inferred JSON schema field
type internal JsonFieldMeta =
    {
        CSharpName: string
        JsonName: string
        PropertyType: Type
    }

/// Metadata representation of an inferred JSON object schema
type internal JsonStructMeta =
    {
        TargetType: Type
        Fields: JsonFieldMeta list
    }

module internal JsonTypeInference =

    let private metaCache = ConcurrentDictionary<Type, JsonStructMeta option>()

    /// Resolves the JSON property name for a PropertyInfo, respecting [JsonPropertyName].
    let resolveJsonPropertyName (prop: PropertyInfo) : string =
        let attr = prop.GetCustomAttribute<JsonPropertyNameAttribute>(true)
        if not (isNull attr) && not (String.IsNullOrEmpty attr.Name) then
            attr.Name
        else
            prop.Name

    /// Extracts metadata for a class, record, or struct type using PolarsTypeHelper.
    let tryGetStructMeta (t: Type) : JsonStructMeta option =
        metaCache.GetOrAdd(t, fun targetType ->
            let coreType = PolarsTypeHelper.UnwrapCoreType targetType

            if not (PolarsTypeHelper.IsStructType coreType) then
                None
            else
                let props =
                    PolarsTypeHelper.GetModelProperties coreType
                    |> Array.filter (fun p -> not (p.IsDefined(typeof<JsonIgnoreAttribute>, true)))
                    |> Array.map (fun p ->
                        {
                            CSharpName = p.Name
                            JsonName = resolveJsonPropertyName p
                            PropertyType = p.PropertyType
                        })
                    |> Array.toList

                if props.IsEmpty then
                    None
                else
                    Some { TargetType = coreType; Fields = props })

    /// Recursively synthesizes a fresh Polars DataTypeHandle for a .NET Type.
    let rec inferPolarsJsonDataType (t: Type) : DataTypeHandle option =
        let coreType = PolarsTypeHelper.UnwrapCoreType t

        // 1. Scalar / Primitive types (primitives, string, dates, times, decimals, guid)
        if PolarsTypeHelper.IsScalarType coreType then
            if coreType = typeof<Guid> then
                // In JSON, Guid is serialized as a UUID string
                Some(PolarsWrapper.NewPrimitiveType(int PlDataType.String))
            else
                try
                    let arrowType = ArrowTypeResolver.GetArrowTypeFromNetType coreType
                    Some(PolarsWrapper.MapArrowToDataTypeHandle arrowType)
                with _ ->
                    None

        // 2. Arrays and Collections -> List
        elif not (isNull (PolarsTypeHelper.TryGetEnumerableElementType coreType)) then
            let elemType = PolarsTypeHelper.TryGetEnumerableElementType coreType
            inferPolarsJsonDataType elemType
            |> Option.map PolarsWrapper.NewListType

        // 3. Composite Objects (Classes / Records / Structs) -> Struct
        elif PolarsTypeHelper.IsStructType coreType then
            match tryGetStructMeta coreType with
            | Some meta ->
                let fieldResults =
                    meta.Fields
                    |> List.map (fun f -> f.JsonName, inferPolarsJsonDataType f.PropertyType)

                if fieldResults |> List.forall (fun (_, opt) -> opt.IsSome) then
                    let names = fieldResults |> List.map fst |> List.toArray
                    let handles = fieldResults |> List.map (snd >> Option.get) |> List.toArray
                    Some(PolarsWrapper.NewStructType(names, handles))
                else
                    None
            | None -> None

        else
            None

    /// Resolves the actual JSON field name corresponding to a C# member/property name.
    let tryResolveJsonFieldName (t: Type) (csharpPropName: string) : string option =
        match tryGetStructMeta t with
        | Some meta ->
            // 1. Exact match on C# property name
            match meta.Fields |> List.tryFind (fun f -> f.CSharpName = csharpPropName) with
            | Some f -> Some f.JsonName
            | None ->
                // 2. Case-insensitive match on C# name or JSON name
                match meta.Fields |> List.tryFind (fun f ->
                    String.Equals(f.CSharpName, csharpPropName, StringComparison.OrdinalIgnoreCase)
                    || String.Equals(f.JsonName, csharpPropName, StringComparison.OrdinalIgnoreCase)) with
                | Some f -> Some f.JsonName
                | None -> None
        | None -> None
