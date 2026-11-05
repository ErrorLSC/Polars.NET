namespace Polars.NET.Linq.IR

open System
open Polars.NET.Core.Arrow
open Polars.NET.Core.Helpers

/// Pure symbolic description of the target schema for JSON decoding, inferred from a .NET DTO type.
/// Backends materialize it into their own physical data type representation.
[<RequireQualifiedAccess>]
type JsonDataType =
    | Primitive of primitiveType: LogicalType
    | List of element: JsonDataType
    | Struct of fields: (string * JsonDataType) list

module internal JsonDataTypeInference =

    /// Recursively infers a symbolic JsonDataType for a .NET type.
    /// Returns None when any nested member cannot be represented.
    let rec tryInfer (t: Type) : JsonDataType option =
        let coreType = PolarsTypeHelper.UnwrapCoreType t
        if PolarsTypeHelper.IsScalarType coreType then
            if coreType = typeof<Guid> then
                Some(JsonDataType.Primitive LogicalTypes.String)
            else
                try
                    Some(JsonDataType.Primitive(LogicalType.Arrow(ArrowTypeResolver.GetArrowTypeFromNetType coreType)))
                with _ ->
                    None
        else
            match PolarsTypeHelper.TryGetEnumerableElementType coreType with
            | null ->
                if PolarsTypeHelper.IsStructType coreType then
                    let fields =
                        ArrowTypeResolver.GetReadableMembers coreType
                        |> Array.map (fun m ->
                            PolarsTypeHelper.GetSerializedName m, tryInfer (ArrowTypeResolver.GetMemberType m))
                    if fields |> Array.forall (fun (_, opt) -> opt.IsSome) then
                        Some(JsonDataType.Struct(fields |> Array.map (fun (n, opt) -> n, opt.Value) |> Array.toList))
                    else
                        None
                else
                    None
            | elemType -> tryInfer elemType |> Option.map JsonDataType.List
