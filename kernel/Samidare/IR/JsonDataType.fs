namespace Kaminari.Samidare.IR

open System
open Kaminari.Sazanami.Arrow
open Kaminari.Sazanami.Helpers

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
        let coreType = ClrTypeHelper.UnwrapCoreType t
        if ClrTypeHelper.IsScalarType coreType then
            if coreType = typeof<Guid> then
                Some(JsonDataType.Primitive LogicalTypes.String)
            else
                match ArrowTypeResolver.TryGetArrowTypeFromNetType coreType with
                | null -> None
                | arrowType -> Some(JsonDataType.Primitive(LogicalType.Arrow arrowType))
        else
            match ClrTypeHelper.TryGetEnumerableElementType coreType with
            | null ->
                if ClrTypeHelper.IsStructType coreType then
                    let fields =
                        ArrowTypeResolver.GetReadableMembers coreType
                        |> Array.map (fun m ->
                            ClrTypeHelper.GetSerializedName m, tryInfer (ArrowTypeResolver.GetMemberType m))
                    if fields |> Array.forall (fun (_, opt) -> opt.IsSome) then
                        Some(JsonDataType.Struct(fields |> Array.map (fun (n, opt) -> n, opt.Value) |> Array.toList))
                    else
                        None
                else
                    None
            | elemType -> tryInfer elemType |> Option.map JsonDataType.List
