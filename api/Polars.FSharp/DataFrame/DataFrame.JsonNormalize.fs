namespace Polars.FSharp

open Polars.NET.Core
open Polars.NET.Core.Helpers
open System.Collections.Generic
open System.Diagnostics.CodeAnalysis
open Kaminari.Sazanami.Helpers

[<AutoOpen>]
module DataFrameJsonNormalizeOps =

    /// Default encoder for data parsed from a JSON string.
    [<UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "Values come from JsonDocument (dictionaries, lists, strings, numbers, booleans, null), which JsonValueEncoder writes directly; its reflection-based fallback is never reached.")>]
    [<UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "See IL2026: parsed-JSON values never reach the reflection-based fallback.")>]
    let private encodeParsedJson (value: obj) : string = JsonValueEncoder.Encode value

    type DataFrame with
        /// <summary>
        /// Normalize a single JSON object (represented as a Map) into a DataFrame.
        /// </summary>
        /// <param name="data">A single JSON object.</param>
        /// <param name="separator">Separator for nested fields.</param>
        /// <param name="maxLevel">Max nesting level.</param>
        /// <param name="schema">Optional schema for the output.</param>
        /// <param name="strict">Strict mode for type inference.</param>
        /// <param name="inferSchemaLength">Rows used for schema inference.</param>
        /// <param name="encoder">Custom value-to-string encoder.</param>
        [<RequiresUnreferencedCode(AotMessages.JsonPocoFallback); RequiresDynamicCode(AotMessages.JsonPocoFallback)>]
        static member JsonNormalize
            (
                data: Map<string, obj>,
                ?separator: string,
                ?maxLevel: int,
                ?schema: PolarsSchema,
                ?strict: bool,
                ?inferSchemaLength: uint,
                ?encoder: obj -> string
            ) : DataFrame =
            DataFrame.JsonNormalize(
                [ data ],
                ?separator = separator,
                ?maxLevel = maxLevel,
                ?schema = schema,
                ?strict = strict,
                ?inferSchemaLength = inferSchemaLength,
                ?encoder = encoder
            )

        /// <summary>
        /// Normalize a sequence of JSON objects into a DataFrame.
        /// </summary>
        /// <param name="data">A sequence of JSON objects.</param>
        /// <param name="separator">Separator for nested fields.</param>
        /// <param name="maxLevel">Max nesting level.</param>
        /// <param name="schema">Optional schema for the output.</param>
        /// <param name="strict">Strict mode for type inference.</param>
        /// <param name="inferSchemaLength">Rows used for schema inference.</param>
        /// <param name="encoder">Custom value-to-string encoder.</param>
        /// <inheritdoc cref="JsonNormalize(Map{string, obj})"/>
        [<RequiresUnreferencedCode(AotMessages.JsonPocoFallback); RequiresDynamicCode(AotMessages.JsonPocoFallback)>]
        static member JsonNormalize
            (
                data: seq<Map<string, obj>>,
                ?separator: string,
                ?maxLevel: int,
                ?schema: PolarsSchema,
                ?strict: bool,
                ?inferSchemaLength: uint,
                ?encoder: obj -> string
            ) : DataFrame =
            let separator = defaultArg separator "."

            let actualMaxLevel =
                match maxLevel with
                | Some l -> l + 1
                | None -> System.Int32.MaxValue

            let realEncoder =
                match encoder with
                | Some e -> e
                | None -> JsonValueEncoder.Encode

            let strict = defaultArg strict true
            let inferSchemaLength = defaultArg inferSchemaLength 100u

            // Convert F# Maps to C# IDictionary for the normalization helper
            let dicts: seq<IDictionary<string, obj>> =
                data
                |> Seq.map (fun map ->
                    let d = Dictionary<string, obj>()

                    for kv in map do
                        d.Add(kv.Key, kv.Value)

                    d :> IDictionary<string, obj>)
                // Call Core helper

            let rawList =
                JsonNormalizeHelper.SimpleJsonNormalize(dicts, separator, actualMaxLevel, realEncoder)

            // Safe conversion: cast IEnumerable/obj to typed sequence of dictionaries
            let dictSequence =
                match box rawList with
                | :? System.Collections.IEnumerable as nonGenericEnum ->
                    nonGenericEnum |> Seq.cast<IDictionary<string, obj>>
                | _ -> Seq.empty

            // Convert back to F# Map
            let normalizedMaps =
                dictSequence |> Seq.map (Seq.map (fun kv -> kv.Key, kv.Value) >> Map.ofSeq)

            let df =
                DataFrame.ofMaps (normalizedMaps, strict = strict, inferSchemaLength = inferSchemaLength)

            // Apply schema if provided: cast each existing column to the desired type
            match schema with
            | Some s ->
                s.ToMap()
                |> Map.fold
                    (fun (df: DataFrame) (colName: string) (dtype: DataType) ->
                        if df.Columns |> Array.contains colName then
                            let castExpr = Expr.Col(colName).Cast(dtype).Alias(colName)
                            df.WithColumns castExpr
                        else
                            df)
                    df
            | None -> df

        /// <summary>
        /// Normalize a JSON string representing a single object or an array of objects.
        /// </summary>
        /// <param name="jsonString">A valid JSON string.</param>
        /// <param name="separator">Separator for nested fields.</param>
        /// <param name="maxLevel">Max nesting level.</param>
        /// <param name="schema">Optional schema for the output.</param>
        /// <param name="strict">Strict mode for type inference.</param>
        /// <param name="inferSchemaLength">Rows used for schema inference.</param>
        /// <param name="encoder">Custom value-to-string encoder.</param>
        /// <inheritdoc cref="JsonNormalize(Map{string, obj})"/>
        [<UnconditionalSuppressMessage("Trimming", "IL2026",
            Justification = "Always passes an encoder (encodeParsedJson unless the caller gives one); the annotated JsonNormalize overload only needs reflection for its default encoder.")>]
        [<UnconditionalSuppressMessage("AOT", "IL3050",
            Justification = "See IL2026.")>]
        static member JsonNormalizeFromString
            (
                jsonString: string,
                ?separator: string,
                ?maxLevel: int,
                ?schema: PolarsSchema,
                ?strict: bool,
                ?inferSchemaLength: uint,
                ?encoder: obj -> string
            ) : DataFrame =
            // JsonDocument.Parse needs no serializer metadata (Native AOT safe); Clone detaches the
            // element from the pooled document buffer so the document can be disposed here.
            let parsed =
                use doc = System.Text.Json.JsonDocument.Parse jsonString
                doc.RootElement.Clone()

            let objResult = JsonNormalizeHelper.ConvertJsonElement parsed

            let dataList: seq<Map<string, obj>> =
                match objResult with
                | :? Dictionary<string, obj> as singleDict ->
                    [ singleDict |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq ]
                | :? List<obj> as list ->
                    list
                    |> Seq.map (fun item ->
                        let dict = item :?> Dictionary<string, obj>
                        dict |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq)
                | _ -> invalidArg (nameof jsonString) "JSON must be an object or array of objects"

            DataFrame.JsonNormalize(
                dataList,
                ?separator = separator,
                ?maxLevel = maxLevel,
                ?schema = schema,
                ?strict = strict,
                ?inferSchemaLength = inferSchemaLength,
                encoder = defaultArg encoder encodeParsedJson
            )
