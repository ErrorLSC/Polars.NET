namespace Kaminari.Samidare

open System
open System.Collections.Generic
open System.Linq.Expressions
open Kaminari.Samidare.IR

/// Dictionary members on a map column (`d[k]`, `GetValueOrDefault`, `ContainsKey`, `Count`, `Keys`, `Values`; F# Map
/// `m.[k]`, `TryFind`). A missing key reads null where .NET throws (as the engines do, and as row conversions do:
/// guards run in the engine on every row); a null map gives null for every member.
[<AutoOpen>]
module internal MapTranslator =

    type internal MapInvocation =
        /// The value of a key: null when missing, or the fallback (`GetValueOrDefault`).
        | MapGet of target: Expression * key: Expression * fallback: Expression option
        | MapContainsKey of target: Expression * key: Expression
        | MapCount of target: Expression
        | MapKeys of target: Expression
        | MapValues of target: Expression

    /// The key and value types of a dictionary type (Dictionary, IDictionary, IReadOnlyDictionary, F# Map, ...).
    let internal dictionaryTypes (t: Type) : (Type * Type) option =
        if isNull t || t = typeof<string> then
            None
        else
            (if t.IsInterface then t :: List.ofArray (t.GetInterfaces()) else List.ofArray (t.GetInterfaces()))
            |> List.tryPick (fun i ->
                if
                    i.IsGenericType
                    && (i.GetGenericTypeDefinition() = typedefof<IDictionary<_, _>>
                        || i.GetGenericTypeDefinition() = typedefof<IReadOnlyDictionary<_, _>>)
                then
                    match i.GetGenericArguments() with
                    | [| k; v |] -> Some(k, v)
                    | _ -> None
                else
                    None)

    /// A dictionary laid out as a map column (not every dictionary type is one: Regex GroupCollection, JsonObject).
    let private isMap (t: Type) =
        (dictionaryTypes t).IsSome
        && match LogicalTypes.tryOfClrType t with
           | Some(LogicalType.Arrow(:? Apache.Arrow.Types.MapType)) -> true
           | _ -> false

    let private isFSharpMap (t: Type) =
        t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<Map<_, _>>

    let internal (|MapInvocation|_|) (expr: Expression) : MapInvocation option =
        match expr with
        | MethodCall(m, target, [ key ]) when not (isNull target) && isMap target.Type ->
            match m.Name with
            | "get_Item" -> Some(MapGet(target, key, None))
            | "ContainsKey" -> Some(MapContainsKey(target, key))
            // F# Map.TryFind: Some value / None, as a nullable value read back as an option.
            | "TryFind" when isFSharpMap target.Type -> Some(MapGet(target, key, None))
            | _ -> None
        | :? IndexExpression as index when index.Arguments.Count = 1 && not (isNull index.Object) && isMap index.Object.Type ->
            Some(MapGet(index.Object, index.Arguments.[0], None))
        | MemberAccess(target, m) when not (isNull target) && isMap target.Type ->
            match m.Name with
            | "Count" -> Some(MapCount target)
            | "Keys" -> Some(MapKeys target)
            | "Values" -> Some(MapValues target)
            | _ -> None
        // CollectionExtensions.GetValueOrDefault(dictionary, key[, defaultValue])
        | MethodCall(m, null, target :: key :: rest) when
            m.Name = "GetValueOrDefault"
            && m.DeclaringType = typeof<CollectionExtensions>
            && isMap target.Type
            ->
            match rest with
            | [] -> Some(MapGet(target, key, Some(Expression.Default(m.ReturnType) :> Expression)))
            | [ fallback ] -> Some(MapGet(target, key, Some fallback))
            | _ -> None
        // Enumerable.Count(dictionary) without a predicate: its entries.
        | MethodCall(m, null, [ target ]) when m.DeclaringType = typeof<System.Linq.Enumerable> && m.Name = "Count" && isMap target.Type ->
            Some(MapCount target)
        | _ -> None

    let internal translateMapInvocation (eval: ScalarExprEvaluator) (ctx: TranslationContext) (invocation: MapInvocation) : ScalarExpr option =
        let typeOf (t: Type) = LogicalTypes.tryOfClrType t |> Option.defaultValue LogicalType.Unknown
        let partsOf (target: Expression) = dictionaryTypes target.Type |> Option.get

        match invocation with
        | MapGet(target, key, fallback) ->
            let _, valueType = partsOf target
            option {
                let! map = eval ctx target
                let! keyS = eval ctx key
                let valueLogical = typeOf valueType
                let get = ScalarExpr.Function(ScalarFunction.MapGet, [ map; keyS ], valueLogical)
                // GetValueOrDefault: the key's value where the map holds it (a null value included), else the default;
                // a null map gives null, as every member of a null dictionary.
                let orElse (fallbackS: ScalarExpr) =
                    let held = ScalarExpr.Function(ScalarFunction.MapContainsKey, [ map; keyS ], LogicalTypes.Boolean)
                    ScalarExpr.Conditional(
                        ScalarExpr.Unary(UnaryOp.IsNull, map),
                        ScalarExpr.Literal(LiteralValue.Null valueLogical),
                        ScalarExpr.Conditional(held, get, fallbackS))
                match fallback with
                | None -> return get
                | Some fallbackExpr ->
                    match unwrap fallbackExpr with
                    // default(V) for a reference or nullable type is null: the value as it is.
                    | :? DefaultExpression as d when not d.Type.IsValueType || not (isNull (Nullable.GetUnderlyingType d.Type)) -> return get
                    | :? DefaultExpression as d ->
                        let! zero = ScalarExprOps.tryToLiteral (Activator.CreateInstance d.Type) d.Type
                        return orElse zero
                    | other ->
                        let! fallbackS = eval ctx other
                        return orElse fallbackS
            }
        | MapContainsKey(target, key) ->
            option {
                let! map = eval ctx target
                let! keyS = eval ctx key
                return ScalarExpr.Function(ScalarFunction.MapContainsKey, [ map; keyS ], LogicalTypes.Boolean)
            }
        // C# Count is an int (the engine counts in u32).
        | MapCount target ->
            eval ctx target
            |> Option.map (fun map ->
                ScalarExpr.Cast(ScalarExpr.Function(ScalarFunction.MapLen, [ map ], LogicalTypes.UInt32), LogicalTypes.Int32, false))
        | MapKeys target ->
            let keyType, _ = partsOf target
            eval ctx target |> Option.map (fun map -> ScalarExpr.Function(ScalarFunction.MapKeys, [ map ], LogicalType.ListOf(typeOf keyType)))
        | MapValues target ->
            let _, valueType = partsOf target
            eval ctx target |> Option.map (fun map -> ScalarExpr.Function(ScalarFunction.MapValues, [ map ], LogicalType.ListOf(typeOf valueType)))
