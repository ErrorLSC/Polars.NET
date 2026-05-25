namespace Polars.FSharp

open Polars.NET.Core.Helpers
open System.Collections.Generic
[<AutoOpen>]
module DataFrameJsonNormalizeOps =

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
            DataFrame.JsonNormalize([data], ?separator = separator, ?maxLevel = maxLevel, 
                                ?schema = schema, ?strict = strict, 
                                ?inferSchemaLength = inferSchemaLength, ?encoder = encoder)

        /// <summary>
        /// Normalize a sequence of JSON objects into a DataFrame.
        /// </summary>
        /// <param name="data">A sequence of JSON objects.</param>
        /// <inheritdoc cref="JsonNormalize(Map{string, obj})"/>
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
                | None -> System.Text.Json.JsonSerializer.Serialize
            let strict = defaultArg strict true
            let inferSchemaLength = defaultArg inferSchemaLength 100u

            // Convert F# Maps to C# IDictionary for the normalization helper
            let dicts : seq<IDictionary<string, obj>> =
                data |> Seq.map (fun map -> 
                    let d = System.Collections.Generic.Dictionary<string, obj>()
                    for kv in map do d.Add(kv.Key, kv.Value)
                    d :> IDictionary<string, obj>)

            // Normalize: SimpleJsonNormalize handles a list of dicts and returns List<IDictionary<string, object?>>
            let normalizedList = 
                JsonNormalizeHelper.SimpleJsonNormalize(dicts, separator, actualMaxLevel, realEncoder)
                :?> System.Collections.Generic.IEnumerable<IDictionary<string, obj>>

            // Convert back to F# Map and build DataFrame via ofMaps
            let normalizedMaps =
                normalizedList
                |> Seq.map (fun d -> 
                    d |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq)
            let df = DataFrame.ofMaps(normalizedMaps, strict = strict, inferSchemaLength = inferSchemaLength)

            // Apply schema if provided: cast each existing column to the desired type
            match schema with
            | Some s ->
                s.ToMap() |> Map.fold (fun (df: DataFrame) (colName: string) (dtype: DataType) ->
                    if df.Columns |> Array.contains colName then
                            let castExpr = 
                                Expr.Col(colName).Cast(dtype).Alias(colName)
                            df.WithColumns(castExpr)
                        else
                            df
                ) df
            | None -> df
        /// <summary>
        /// Normalize a JSON string representing a single object or an array of objects.
        /// </summary>
        /// <param name="jsonString">A valid JSON string.</param>
        /// <inheritdoc cref="JsonNormalize(seq{Map{string, obj}})"/>
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
            let parsed = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(jsonString)
            
            let objResult = JsonNormalizeHelper.ConvertJsonElement(parsed)
            let dataList : seq<Map<string, obj>> =
                match objResult with
                | :? Dictionary<string, obj> as singleDict ->
                    [ singleDict |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq ]
                | :? List<obj> as list ->
                    list |> Seq.map (fun item ->
                        let dict = item :?> Dictionary<string, obj>
                        dict |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq)
                | _ -> invalidArg (nameof jsonString) "JSON must be an object or array of objects"

            DataFrame.JsonNormalize(dataList, ?separator = separator, ?maxLevel = maxLevel,
                                    ?schema = schema, ?strict = strict,
                                    ?inferSchemaLength = inferSchemaLength, ?encoder = encoder)