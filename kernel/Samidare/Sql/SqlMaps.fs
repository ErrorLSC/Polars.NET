namespace Kaminari.Samidare.Sql

open Kaminari.Samidare.IR

/// Map values in SQL (ISO extensions and the Spark profile): construction from entries, lookups by key.
module internal SqlMaps =

    /// A map of the entries (keys and values already of the map's types), in order; a key repeated in a row keeps its
    /// first position and its last value (literal repeats are refused before).
    let construct (entries: (ScalarExpr * ScalarExpr) list) (mapType: LogicalType) : ScalarExpr =
        // Each key and value as the map's types (a literal's own type may not be settled in the engine).
        let typed (part: Apache.Arrow.Field) (e: ScalarExpr) = ScalarExpr.Cast(e, LogicalType.Arrow part.DataType, true)
        let entryArgs =
            match mapType with
            | LogicalType.Arrow(:? Apache.Arrow.Types.MapType as m) -> entries |> List.collect (fun (k, v) -> [ typed m.KeyField k; typed m.ValueField v ])
            | _ -> entries |> List.collect (fun (k, v) -> [ k; v ])
        ScalarExpr.Function(ScalarFunction.MapOf, entryArgs, mapType)

    /// The first key written twice among literal keys, if any.
    let repeatedLiteralKey (keys: ScalarExpr list) : LiteralValue option =
        keys
        |> List.choose (function
            | ScalarExpr.Literal value -> Some value
            | _ -> None)
        |> List.countBy id
        |> List.tryFind (fun (_, n) -> n > 1)
        |> Option.map fst

    /// The text of a literal key, for messages.
    let keyText (key: LiteralValue) =
        match key with
        | LiteralValue.String s -> $"'{s}'"
        | other -> string other

    /// The value of a key in a map (null where the map lacks it).
    let get (map: ScalarExpr) (key: ScalarExpr) (valueType: LogicalType) : ScalarExpr =
        ScalarExpr.Function(ScalarFunction.MapGet, [ map; key ], valueType)
