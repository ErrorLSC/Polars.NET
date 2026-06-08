namespace Polars.FSharp

open System
open System.Collections.Generic
open Polars.NET.Core

[<RequireQualifiedAccess>] 
type Active = 
    | Set of bool
    | ResetToDefault

    member internal this.ToNative() =
        match this with
        | Set true -> Nullable true
        | Set false -> Nullable false
        | ResetToDefault -> Nullable ()

[<RequireQualifiedAccess>] 
type CharSet = 
    | Set of char
    | ResetToDefault
    member internal this.ToNative() =
        match this with
        | Set cr -> Nullable cr
        | ResetToDefault -> Nullable ()

[<RequireQualifiedAccess>] 
type NumSet =
    | Set of int
    | ResetToDefault
    member internal this.ToNative() =
        match this with
        | Set i -> Nullable i
        | ResetToDefault -> Nullable ()

[<RequireQualifiedAccess>] 
type ChunkSet =
    | Set of uint64
    | ResetToDefault
    member internal this.ToNative() =
        match this with
        | Set i -> Nullable i
        | ResetToDefault -> Nullable ()

[<RequireQualifiedAccess>] 
type ConfigScope =
    | SetOnly
    | All

/// <summary>
/// Config for Polars.FSharp
/// </summary>
[<RequireQualifiedAccess>] 
module Config =
    let internal setEnv (k, v) = 
        PolarsWrapper.SetEnvVar(k, v)
        PolarsWrapper.ReloadEnvVar k
    let internal setEnvKvp (kvp: KeyValuePair<string, string>) = 
        setEnv(kvp.Key, kvp.Value)
    /// Inject Environment var to Rust
    let set (key: string) (value: string) = (key,value) |> setEnv

    /// Inject Environment vars to Rust from KeyValuePair sequence
    let setMap (variables: Map<string, string>) =
        ArgumentNullException.ThrowIfNull(variables)
        variables |> Seq.iter setEnvKvp
    /// Inject Environment vars to Rust from a sequence of tuples
    let setMany (variables: seq<string * string>) =
        ArgumentNullException.ThrowIfNull(variables)
        variables |> Seq.iter setEnv
    /// <summary>
    /// Save the current set of Config options as a JSON string.
    /// </summary>
    let save(scope:ConfigScope) = 
        match scope with
        | ConfigScope.SetOnly -> CoreConfig.Save(ifSet=true)
        | ConfigScope.All -> CoreConfig.Save(ifSet=false)
    /// <summary>
    /// Save the current set of Config options as a JSON file.
    /// </summary>
    let saveToFile path = CoreConfig.SaveToFile(path=path,ifSet=false)
    /// <summary>
    /// Load (and set) previously saved Config options from a JSON string.Only already set parameter will be recorded.
    /// </summary>
    let load cfgJson = CoreConfig.Load cfgJson
    /// <summary>
    /// Load (and set) previously saved Config options from file.
    /// </summary>
    let loadFromFile path = CoreConfig.LoadFromFile path
    /// <summary>
    /// Reset all polars Config settings to their default state.
    /// </summary>
    let restoreDefaults() = CoreConfig.RestoreDefaults()
    /// <summary>
    /// Show the current state of all Config variables in the environment as a dict.
    /// </summary>
    let status (scope:ConfigScope) = 
        match scope with
        | ConfigScope.SetOnly -> CoreConfig.GetState(ifSet=true,envOnly=false)
        | ConfigScope.All -> CoreConfig.GetState(ifSet=false,envOnly=false)
    /// Safely retrieves the current configuration value for the specified key.
    /// Matches keys in a case-insensitive manner.
    /// </summary>
    let tryGet (key: string) : string option =
        if String.IsNullOrEmpty(key) then None
        else
            let currentStatus = status ConfigScope.All 
            match currentStatus.TryGetValue(key) with
            | true, value -> Some value
            | false, _ ->
                match currentStatus.TryGetValue(key.ToLowerInvariant()) with
                | true, value -> Some value
                | false, _ -> None

    /// <summary>
    /// Retrieves the current configuration value for the specified key, 
    /// or returns a default value if the key is not found.
    /// </summary>
    let getOr (defaultValue: string) (key: string) : string =
        tryGet key |> Option.defaultValue defaultValue
    // =========================================================================
    // Core Scoped Configuration Execution Engine
    // =========================================================================

    /// <summary>
    /// Executes a function within a temporary configuration scope.
    /// Captures the current configuration snapshot, applies the provided configuration adjustments,
    /// runs the operation, and guarantees restoration of the previous state when finished.
    /// </summary>
    /// <param name="actions">A sequence of configuration modification actions (unit -> unit).</param>
    /// <param name="f">The operation to execute within this configuration scope.</param>
    let withConfig (actions: list<unit -> unit>) (f: unit -> 'T) : 'T =
        ArgumentNullException.ThrowIfNull actions
        let backupPayload = CoreConfig.Save(ifSet = false)
        try
            actions |> List.iter (fun f -> f ())
            PolarsWrapper.ReloadEnvVarAll()
            f ()
        finally
            CoreConfig.Load backupPayload

    // =========================================================================
    // Individual Configuration Actions (Returning unit -> unit)
    // =========================================================================

    /// <summary>
    /// Use ASCII characters to display table outlines.
    /// Set False to revert to the standard UTF8_FULL_CONDENSED formatting style.
    /// </summary>
    let asciiTables (active: Active) =
        fun () -> CoreConfig.AsciiTables <- active.ToNative()

    /// <summary>
    /// Set the decimal separator character.
    /// </summary>
    let decimalSeparator (separator: CharSet) =
        fun () -> CoreConfig.DecimalSeparator <- separator.ToNative()

    /// <summary>
    /// Configures the thousands separator with a specific single character.
    /// </summary>
    let thousandsSeparator (separator: CharSet) =
        fun () -> CoreConfig.ThousandsSeparator <- separator.ToNative()

    /// <summary>
    /// Strip trailing zeros from Decimal data type values.
    /// </summary>
    let trimDecimalZeros (active: Active) =
        fun () -> CoreConfig.TrimDecimalZeros <- active.ToNative() 

    /// <summary>
    /// Set which engine to use by default.
    /// </summary>
    let engineAffinity (engine: Engine) =
        fun () -> CoreConfig.EngineAffinity <- Nullable (engine.ToNative())
                
    /// <summary>
    /// Control the number of decimal places displayed for floating point values.
    /// </summary>
    let floatPrecision (precision: NumSet) =
        fun () -> CoreConfig.FloatPrecision <- precision.ToNative()

    /// <summary>
    /// Control how floating point values are displayed.
    /// </summary>
    let formatFloat (format: FloatFormat) =
        fun () -> CoreConfig.FloatFormat <- format.ToNative()

    /// <summary>
    /// Set the number of characters used to display string values.
    /// </summary>
    let formatStringLength (n: NumSet) =
        fun () -> CoreConfig.StringLength <- n.ToNative()

    /// <summary>
    /// Set the number of elements to display for List values.
    /// </summary>
    let formatTableCellListLength (n: NumSet) =
        fun () -> CoreConfig.TableCellListLength <- n.ToNative()

    /// <summary>
    /// Overwrite chunk size used in streaming engine.
    /// </summary>
    let streamingChunkSize (size: ChunkSet) =
        fun () -> CoreConfig.StreamingChunkSize <- size.ToNative()

    /// <summary>
    /// Set table cell alignment.
    /// </summary>
    let tableCellAlignment (format: Alignment) =
        fun () -> CoreConfig.TableCellAlignment <- format.ToNative()

    /// <summary>
    /// Set table cell alignment for numeric columns.
    /// </summary>
    let tableCellNumericAlignment (format: Alignment) =
        fun () -> CoreConfig.TableCellNumericAlignment <- format.ToNative()

    /// <summary>
    /// Set the number of columns that are visible when displaying tables.
    /// </summary>
    let tableCols (n: NumSet) =
        fun () -> CoreConfig.TableMaxCols <- n.ToNative()

    /// <summary>
    /// Set the max number of rows used to draw the table (both Dataframe and Series).
    /// </summary>
    let tableRows (n: NumSet) =
        fun () -> CoreConfig.TableMaxRows <- n.ToNative()

    /// <summary>
    /// Display the data type next to the column name (to the right, in parentheses).
    /// </summary>
    let tableColumnDataTypeInline (active: Active) =
        fun () -> CoreConfig.TableColumnDataTypeInline <- active.ToNative()

    /// <summary>
    /// Configures whether Polars should run in verbose mode, printing query profiles and optimization decisions.
    /// </summary>
    let verbose (active: Active) =
        fun () -> CoreConfig.Verbose <- active.ToNative()

    /// <summary>
    /// Print the DataFrame shape information below the data when displaying tables.
    /// </summary>
    let tableDataFrameShapeBelow (active: Active) =
        fun () ->  CoreConfig.TableDataFrameShapeBelow <- active.ToNative()

    /// <summary>
    /// Sets the text/ASCII formatting style for outputting DataFrames.
    /// </summary>
    let tableFormatting (format: TableFormatting, roundedCorners: Active) =
        fun () -> CoreConfig.TableFormatting <- format.ToNative(), roundedCorners.ToNative()

    /// <summary>
    /// Hide table column data types (i64, f64, str etc.).
    /// </summary>
    let tableHideColumnDataTypes (active: Active) =
        fun () ->  CoreConfig.TableHideColumnDataTypes <- active.ToNative()

    /// <summary>
    /// Hide table column names.
    /// </summary>
    let tableHideColumnNames (active: Active) =
        fun () -> CoreConfig.TableHideColumnNames <- active.ToNative()

    /// <summary>
    /// Hide the DataFrame shape information when displaying tables.
    /// </summary>
    let tableHideDataFrameShape (active: Active) =
        fun () -> CoreConfig.TableHideDataFrameShape <- active.ToNative()

    /// <summary>
    /// Hide the ‘—’ separator displayed between the column names and column types.
    /// </summary>
    let tableHideDataTypeSeparator (active: Active) =
        fun () -> CoreConfig.TableHideDataTypeSeparator <- active.ToNative()

    /// <summary>
    /// Set the maximum width of a table in characters.
    /// </summary>
    let tableWidthChars (width: NumSet) =
        fun () -> CoreConfig.TableWidthChars <- width.ToNative()

    /// <summary>
    /// Set the maximum Retires for Delta Table operations.
    /// </summary>
    let deltaMaxRetries (retries: NumSet) =
        fun () -> CoreConfig.PolarsDeltaMaxRetry <- retries.ToNative()