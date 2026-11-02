namespace Polars.NET.Linq

open System
open System.Collections.Concurrent
open System.Reflection
open System.Linq.Expressions
open System.Text.Json
open System.Text.Json.Nodes
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
                            JsonName = PolarsTypeHelper.GetSerializedName p
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

[<AutoOpen>]
module internal JsonTranslator =

    type internal JsonAccessStep =
        | PropertyStep of MemberInfo
        | IndexStep of Expression

    /// Represents extracted components of JSON operations (System.Text.Json)
    type internal JsonInvocation =
        | JsonPathMatch of target: Expression * path: string * targetType: Type
        | JsonSerialize of target: Expression
        | JsonDeserialize of target: Expression * targetType: Type * steps: JsonAccessStep list

    [<TailCall>]
    let rec private tryUnrollJsonSegmentsAcc (expr: Expression) (acc: string list) : (Expression * string list) option =
        match expr with
        | null -> None

        // 1. Root: JsonNode.Parse(target) / JsonDocument.Parse(target) / JsonObject.Parse(target) / JsonArray.Parse(target)
        | MethodCall(m, null, target :: _) when
            not (isNull m.DeclaringType)
            && (m.DeclaringType = typeof<JsonNode>
                || m.DeclaringType = typeof<JsonObject>
                || m.DeclaringType = typeof<JsonArray>
                || m.DeclaringType = typeof<JsonDocument>)
            && m.Name = "Parse"
            && (unwrapSpanOrArray target).Type = typeof<string>
            ->
            Some(unwrapSpanOrArray target, acc)

        // 2. MemberAccess: doc.RootElement
        | MemberAccess(inner, prop) when
            not (isNull inner)
            && typeof<JsonDocument>.IsAssignableFrom inner.Type
            && prop.Name = "RootElement"
            ->
            tryUnrollJsonSegmentsAcc inner acc

        // 3. MethodCall: element.GetProperty("foo")
        | MethodCall(m, inner, [ arg ]) when
            not (isNull inner)
            && typeof<JsonElement>.IsAssignableFrom inner.Type
            && m.Name = "GetProperty"
            ->
            match tryEvaluate arg with
            | Some(:? string as propName) ->
                tryUnrollJsonSegmentsAcc inner ("." + propName :: acc)
            | _ -> None

        // 4. Indexer on JsonNode / JsonObject / JsonArray / JsonElement: node["foo"] or node[0]
        | MethodCall(m, inner, [ arg ]) when
            not (isNull inner)
            && (typeof<JsonNode>.IsAssignableFrom inner.Type
                || typeof<JsonElement>.IsAssignableFrom inner.Type)
            && (m.Name = "get_Item" || m.Name = "GetItem")
            ->
            let segmentOpt =
                match tryEvaluate arg with
                | Some(:? string as propName) -> Some("." + propName)
                | Some(:? int as idx) -> Some("[" + string idx + "]")
                | _ -> None
            match segmentOpt with
            | Some seg -> tryUnrollJsonSegmentsAcc inner (seg :: acc)
            | None -> None

        // 5. IndexExpression (node["foo"] or node[0])
        | :? IndexExpression as idx when
            not (isNull idx.Object)
            && idx.Arguments.Count = 1
            && (typeof<JsonNode>.IsAssignableFrom idx.Object.Type
                || typeof<JsonElement>.IsAssignableFrom idx.Object.Type)
            ->
            let segmentOpt =
                match tryEvaluate idx.Arguments.[0] with
                | Some(:? string as propName) -> Some("." + propName)
                | Some(:? int as i) -> Some("[" + string i + "]")
                | _ -> None
            match segmentOpt with
            | Some seg -> tryUnrollJsonSegmentsAcc idx.Object (seg :: acc)
            | None -> None

        | _ -> None

    let private tryUnrollJsonSegments (expr: Expression) : (Expression * string list) option =
        tryUnrollJsonSegmentsAcc expr []

    let rec private tryUnrollDeserializeMembers (expr: Expression) (acc: JsonAccessStep list) : (Expression * Type * JsonAccessStep list) option =
        match expr with
        | MemberAccess(target, m) when not (isNull target) ->
            tryUnrollDeserializeMembers target (PropertyStep m :: acc)

        | ArrayIndexOp(target, indexExpr) ->
            tryUnrollDeserializeMembers target (IndexStep indexExpr :: acc)

        | MethodCall(m, target, [ indexExpr ]) when
            not (isNull target)
            && (m.Name = "get_Item" || m.Name = "ElementAt")
            ->
            tryUnrollDeserializeMembers target (IndexStep indexExpr :: acc)

        | MethodCall(m, null, [ target; indexExpr ]) when
            not (isNull m.DeclaringType)
            && m.DeclaringType = typeof<System.Linq.Enumerable>
            && m.Name = "ElementAt"
            ->
            tryUnrollDeserializeMembers target (IndexStep indexExpr :: acc)

        | MethodCall(m, null, [ target ]) when
            not (isNull m.DeclaringType)
            && m.DeclaringType = typeof<System.Linq.Enumerable>
            && m.Name = "First"
            ->
            tryUnrollDeserializeMembers target (IndexStep (Expression.Constant(0)) :: acc)

        | MethodCall(m, null, [ target ]) when
            not (isNull m.DeclaringType)
            && m.DeclaringType = typeof<System.Linq.Enumerable>
            && m.Name = "Last"
            ->
            tryUnrollDeserializeMembers target (IndexStep (Expression.Constant(-1)) :: acc)

        | Unary(ExpressionType.Convert, inner)
        | Unary(ExpressionType.ConvertChecked, inner) when
            not (isNull inner) &&
            match inner with
            | MethodCall(m, null, _) when not (isNull m.DeclaringType) && m.DeclaringType = typeof<JsonSerializer> && m.Name = "Deserialize" -> true
            | _ -> false
            ->
            tryUnrollDeserializeMembers inner acc

        | MethodCall(m, null, args) when
            not (isNull m.DeclaringType)
            && m.DeclaringType = typeof<JsonSerializer>
            && m.Name = "Deserialize"
            && not args.IsEmpty
            ->
            let target = args.Head
            if m.IsGenericMethod then
                let dtoType = m.GetGenericArguments().[0]
                Some(target, dtoType, acc)
            elif args.Length >= 2 && m.GetParameters().[1].ParameterType = typeof<Type> then
                match tryEvaluate args.[1] with
                | Some(:? Type as dtoType) -> Some(target, dtoType, acc)
                | _ -> None
            else
                None
        | _ -> None

    let internal (|JsonInvocation|_|) (expr: Expression) : JsonInvocation option =
        match expr with
        // 1. Explicit Cast / Convert on JsonNode / JsonElement: (int)node["age"], (string)node["name"], etc.
        | :? UnaryExpression as u when
            (u.NodeType = ExpressionType.Convert || u.NodeType = ExpressionType.ConvertChecked)
            && (not (isNull u.Method) && u.Method.Name = "op_Explicit"
                || typeof<JsonNode>.IsAssignableFrom u.Operand.Type
                || typeof<JsonElement>.IsAssignableFrom u.Operand.Type)
            ->
            tryUnrollJsonSegments u.Operand
            |> Option.map (fun (target, segs) ->
                let path = "$" + String.concat "" segs
                JsonPathMatch(target, path, u.Type))

        // 2. Extractor methods on JsonElement: element.GetString(), element.GetInt32(), etc.
        | MethodCall(m, inner, []) when
            not (isNull inner)
            && typeof<JsonElement>.IsAssignableFrom inner.Type
            && (m.Name.StartsWith "Get" || m.Name = "ToString")
            ->
            let targetTypeOpt =
                match m.Name with
                | "GetString" -> Some typeof<string>
                | "GetInt32" -> Some typeof<int>
                | "GetInt64" -> Some typeof<int64>
                | "GetDouble" -> Some typeof<double>
                | "GetSingle" -> Some typeof<float32>
                | "GetBoolean" -> Some typeof<bool>
                | "GetDecimal" -> Some typeof<decimal>
                | "GetInt16" -> Some typeof<int16>
                | "GetByte" -> Some typeof<byte>
                | "ToString" -> Some typeof<string>
                | _ -> None
            match targetTypeOpt with
            | Some targetType ->
                tryUnrollJsonSegments inner
                |> Option.map (fun (target, segs) ->
                    let path = "$" + String.concat "" segs
                    JsonPathMatch(target, path, targetType))
            | None -> None

        // 3. node.GetValue<T>() or node.ToString()
        | MethodCall(m, inner, []) when
            not (isNull inner)
            && typeof<JsonNode>.IsAssignableFrom inner.Type
            && (m.Name = "GetValue" || m.Name = "ToString")
            ->
            let targetType = if m.Name = "GetValue" && m.IsGenericMethod then m.GetGenericArguments().[0] else typeof<string>
            tryUnrollJsonSegments inner
            |> Option.map (fun (target, segs) ->
                let path = "$" + String.concat "" segs
                JsonPathMatch(target, path, targetType))

        // 4. Raw JsonNode/JsonElement indexing in an expression where no explicit conversion was specified
        | _ ->
            match tryUnrollJsonSegments expr with
            | Some(target, segs) when not segs.IsEmpty ->
                let path = "$" + String.concat "" segs
                Some(JsonPathMatch(target, path, typeof<string>))
            | _ ->
                // 5. JsonSerializer.Deserialize<T>(target)...
                match tryUnrollDeserializeMembers expr [] with
                | Some(target, dtoType, memberPath) ->
                    Some(JsonDeserialize(target, dtoType, memberPath))
                | None ->
                    // 6. JsonSerializer.Serialize(...)
                    match expr with
                    | MethodCall(m, null, target :: _) when
                        not (isNull m.DeclaringType)
                        && m.DeclaringType = typeof<JsonSerializer>
                        && m.Name = "Serialize"
                        ->
                        Some(JsonSerialize target)
                    | _ -> None

    let internal translateJsonInvocation (eval: ExprEvaluator) (ctx: TranslationContext) (invocation: JsonInvocation) : ExprHandle option =
        match invocation with
        | JsonPathMatch(target, path, targetType) ->
            eval ctx target
            |> Option.bind (fun t ->
                let pathH = PolarsWrapper.Lit path
                let matchH = PolarsWrapper.StrJsonPathMatch(t, pathH)
                if targetType = typeof<string> || targetType = typeof<obj> then
                    Some matchH
                elif targetType = typeof<bool> || targetType = typeof<Nullable<bool>> then
                    let lowerH = PolarsWrapper.StrToLower matchH
                    Some(PolarsWrapper.Eq(lowerH, PolarsWrapper.Lit "true"))
                else
                    tryTranslateCast targetType matchH)

        | JsonSerialize target ->
            let unwrappedTarget = unwrap target
            match unwrappedTarget with
            | :? NewExpression as ne ->
                let memberNames =
                    if not (isNull ne.Members) && ne.Members.Count = ne.Arguments.Count then
                        ne.Members |> Seq.map (fun m -> m.Name) |> Seq.toArray
                    elif not (isNull ne.Constructor) then
                        let ctorParams = ne.Constructor.GetParameters()
                        ctorParams |> Array.map (fun p -> p.Name)
                    else
                        [||]

                let argHandles =
                    ne.Arguments
                    |> Seq.mapi (fun i argExpr ->
                        match eval ctx argExpr with
                        | Some h ->
                            let fieldName = if i < memberNames.Length then memberNames.[i] else sprintf "field_%d" i
                            Some(PolarsWrapper.Alias(h, fieldName))
                        | None -> None)
                    |> Seq.toList

                if argHandles |> List.forall Option.isSome then
                    let handlesArray = argHandles |> List.map Option.get |> List.toArray
                    let structH = PolarsWrapper.AsStruct handlesArray
                    Some(PolarsWrapper.StructJsonEncode structH)
                else
                    None

            | :? MemberInitExpression as mie ->
                let bindings =
                    mie.Bindings
                    |> Seq.choose (fun b ->
                        match b with
                        | :? MemberAssignment as ma ->
                            match eval ctx ma.Expression with
                            | Some h -> Some(PolarsWrapper.Alias(h, ma.Member.Name))
                            | None -> None
                        | _ -> None)
                    |> Seq.toArray
                if bindings.Length = mie.Bindings.Count then
                    let structH = PolarsWrapper.AsStruct bindings
                    Some(PolarsWrapper.StructJsonEncode structH)
                else
                    None

            | other ->
                eval ctx other
                |> Option.map (fun h -> PolarsWrapper.StructJsonEncode h)

        | JsonDeserialize(target, dtoType, steps) ->
            match JsonTypeInference.inferPolarsJsonDataType dtoType with
            | None -> None
            | Some dtypeHandle ->
                use _dtypeScope = dtypeHandle
                let dtypeExpr = PolarsWrapper.DataTypeExprFromDataType dtypeHandle

                match eval ctx target with
                | None ->
                    dtypeExpr.Dispose()
                    None
                | Some targetH ->
                    let decodedH = PolarsWrapper.StrJsonDecode(targetH, dtypeExpr)

                    let rec traverseSteps (currentH: ExprHandle) (currType: Type) (remainingSteps: JsonAccessStep list) : ExprHandle option =
                        match remainingSteps with
                        | [] -> Some currentH

                        | PropertyStep m :: rest when
                            (m.Name = "Length" || m.Name = "Count")
                            && not (isNull (PolarsTypeHelper.TryGetEnumerableElementType currType)) ->
                            let lenH = PolarsWrapper.ListLen currentH
                            traverseSteps lenH typeof<int> rest

                        | PropertyStep m :: rest ->
                            let memberType =
                                match m with
                                | :? PropertyInfo as p -> p.PropertyType
                                | :? FieldInfo as f -> f.FieldType
                                | _ -> typeof<obj>

                            match JsonTypeInference.tryResolveJsonFieldName currType m.Name with
                            | Some jsonFieldName ->
                                let fieldH = PolarsWrapper.StructFieldByName(currentH, [| jsonFieldName |])
                                traverseSteps fieldH memberType rest
                            | None ->
                                let fieldH = PolarsWrapper.StructFieldByName(currentH, [| m.Name |])
                                traverseSteps fieldH memberType rest

                        | IndexStep indexExpr :: rest ->
                            let elemType =
                                let elemOpt = PolarsTypeHelper.TryGetEnumerableElementType currType
                                if not (isNull elemOpt) then elemOpt
                                elif currType.IsArray then currType.GetElementType()
                                elif currType.IsGenericType then currType.GetGenericArguments().[0]
                                else typeof<obj>

                            match eval ctx indexExpr with
                            | Some idxH ->
                                let itemH = PolarsWrapper.ListGet(currentH, idxH, nullOnOob = true)
                                traverseSteps itemH elemType rest
                            | None -> None

                    traverseSteps decodedH dtoType steps

