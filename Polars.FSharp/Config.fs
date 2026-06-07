namespace Polars.FSharp

open System
open System.Collections.Generic
open Polars.NET.Core

/// <summary>
/// Config for Polars.FSharp
/// </summary>
[<RequireQualifiedAccess>] 
module Config =

    /// Inject Environment var to Rust
    let set (key: string) (value: string) =
        PolarsWrapper.SetEnvVar(key, value)
        PolarsWrapper.ReloadEnvVar key

    /// Inject Environment vars to Rust from KeyValuePair sequence
    let setFromKvp (variables: seq<KeyValuePair<string, string>>) =
        ArgumentNullException.ThrowIfNull(variables)
        for kvp in variables do
            PolarsWrapper.SetEnvVar(kvp.Key, kvp.Value)
        PolarsWrapper.ReloadEnvVarAll()

    /// Inject Environment vars to Rust from a sequence of tuples
    let setMany (variables: seq<string * string>) =
        ArgumentNullException.ThrowIfNull(variables)
        for key, value in variables do
            PolarsWrapper.SetEnvVar(key, value)
        PolarsWrapper.ReloadEnvVarAll()
    /// <summary>
    /// Save the current set of Config options as a JSON string.
    /// </summary>
    let save() = CoreConfig.Save(ifSet=false)
    /// <summary>
    /// Save the current set of Config options as a JSON file.
    /// </summary>
    let saveToFile path = CoreConfig.SaveToFile(path=path,ifSet=false)
    /// <summary>
    /// Load (and set) previously saved Config options from a JSON string.
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
    let status() = CoreConfig.GetState(false,false)
    /// <summary>
    /// Safely retrieves the current configuration value for the specified key.
    /// Matches keys in a case-insensitive manner.
    /// </summary>
    let tryGet (key: string) : string option =
        if String.IsNullOrEmpty(key) then None
        else
            let currentStatus = status() 
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
    let withConfig (actions: seq<unit -> unit>) (f: unit -> 'T) : 'T =
        ArgumentNullException.ThrowIfNull actions
        let backupPayload = CoreConfig.Save(ifSet = false)
        try
            for action in actions do
                action ()
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
    let asciiTables (active: bool option) =
        fun () -> 
            match active with
            | Some v -> CoreConfig.AsciiTables <- Nullable v
            | None   -> CoreConfig.AsciiTables <- Nullable ()

    /// <summary>
    /// Set the decimal separator character.
    /// </summary>
    let decimalSeparator (separator: char option) =
        fun () -> 
            match separator with
            | Some v -> CoreConfig.DecimalSeparator <- Nullable v
            | None   -> CoreConfig.DecimalSeparator <- Nullable ()

    /// <summary>
    /// Configures the thousands separator with a shortcut boolean.
    /// If true, applies standard English format: sets thousands separator to ',' and decimal separator to '.'.
    /// If false, clears the thousands separator.
    /// </summary>
    let thousandsSeparatorFormat (useDefaultFormat: bool) =
        fun () ->
            if useDefaultFormat then
                CoreConfig.DecimalSeparator <- Nullable '.'
                CoreConfig.ThousandsSeparator <- Nullable ','
            else
                CoreConfig.ThousandsSeparator <- Nullable ()

    /// <summary>
    /// Configures the thousands separator with a specific single character.
    /// Set to None to clear/reset the separator.
    /// </summary>
    let thousandsSeparator (separator: char option) =
        fun () -> 
            match separator with
            | Some v -> CoreConfig.ThousandsSeparator <- Nullable v
            | None   -> CoreConfig.ThousandsSeparator <- Nullable ()

    /// <summary>
    /// Strip trailing zeros from Decimal data type values.
    /// </summary>
    let trimDecimalZeros (active: bool option) =
        fun () -> 
            match active with
            | Some v -> CoreConfig.TrimDecimalZeros <- Nullable v
            | None   -> CoreConfig.TrimDecimalZeros <- Nullable ()

    /// <summary>
    /// Set which engine to use by default.
    /// </summary>
    let engineAffinity (engine: Engine option) =
        fun () -> 
            CoreConfig.EngineAffinity <- 
                match engine with 
                | Some e -> Nullable (e.ToNative()) 
                | None   -> Nullable ()

    /// <summary>
    /// Control the number of decimal places displayed for floating point values.
    /// </summary>
    let floatPrecision (precision: int64 option) =
        fun () -> 
            match precision with
            | Some v -> CoreConfig.FloatPrecision <- Nullable v
            | None   -> CoreConfig.FloatPrecision <- Nullable ()

    /// <summary>
    /// Control how floating point values are displayed.
    /// </summary>
    let formatFloat (format: FloatFormat option) =
        fun () -> 
            CoreConfig.FloatFormat <- 
                match format with 
                | Some e -> Nullable (e.ToNative()) 
                | None   -> Nullable ()

    /// <summary>
    /// Set the number of characters used to display string values.
    /// </summary>
    let formatStringLength (n: int option) =
        fun () -> 
            match n with
            | Some v -> CoreConfig.StringLength <- Nullable v
            | None   -> CoreConfig.StringLength <- Nullable ()

    /// <summary>
    /// Set the number of elements to display for List values.
    /// </summary>
    let formatTableCellListLength (n: int option) =
        fun () -> 
            match n with
            | Some v -> CoreConfig.TableCellListLength <- Nullable v
            | None   -> CoreConfig.TableCellListLength <- Nullable ()

    /// <summary>
    /// Overwrite chunk size used in streaming engine.
    /// </summary>
    let streamingChunkSize (size: uint64 option) =
        fun () -> 
            match size with
            | Some v -> CoreConfig.StreamingChunkSize <- Nullable v
            | None   -> CoreConfig.StreamingChunkSize <- Nullable ()

    /// <summary>
    /// Set table cell alignment.
    /// </summary>
    let tableCellAlignment (format: Alignment option) =
        fun () -> 
            CoreConfig.TableCellAlignment <- 
                match format with 
                | Some e -> Nullable (e.ToNative()) 
                | None   -> Nullable ()

    /// <summary>
    /// Set table cell alignment for numeric columns.
    /// </summary>
    let tableCellNumericAlignment (format: Alignment option) =
        fun () -> 
            CoreConfig.TableCellNumericAlignment <- 
                match format with 
                | Some e -> Nullable (e.ToNative()) 
                | None   -> Nullable ()

    /// <summary>
    /// Set the number of columns that are visible when displaying tables.
    /// </summary>
    let tableCols (n: int option) =
        fun () -> 
            match n with
            | Some v -> CoreConfig.TableMaxCols <- Nullable v
            | None   -> CoreConfig.TableMaxCols <- Nullable ()

    /// <summary>
    /// Set the max number of rows used to draw the table (both Dataframe and Series).
    /// </summary>
    let tableRows (n: int option) =
        fun () -> 
            match n with
            | Some v -> CoreConfig.TableMaxRows <- Nullable v
            | None   -> CoreConfig.TableMaxRows <- Nullable ()

    /// <summary>
    /// Display the data type next to the column name (to the right, in parentheses).
    /// </summary>
    let tableColumnDataTypeInline (active: bool option) =
        fun () -> 
            match active with
            | Some v -> CoreConfig.TableColumnDataTypeInline <- Nullable v
            | None   -> CoreConfig.TableColumnDataTypeInline <- Nullable ()

    /// <summary>
    /// Configures whether Polars should run in verbose mode, printing query profiles and optimization decisions.
    /// </summary>
    let verbose (active: bool option) =
        fun () -> 
            match active with
            | Some v -> CoreConfig.Verbose <- Nullable v
            | None   -> CoreConfig.Verbose <- Nullable ()

    /// <summary>
    /// Print the DataFrame shape information below the data when displaying tables.
    /// </summary>
    let tableDataFrameShapeBelow (active: bool option) =
        fun () -> 
            match active with
            | Some v -> CoreConfig.TableDataFrameShapeBelow <- Nullable v
            | None   -> CoreConfig.TableDataFrameShapeBelow <- Nullable ()

    /// <summary>
    /// Sets the text/ASCII formatting style for outputting DataFrames.
    /// </summary>
    let tableFormatting (format: TableFormatting option, roundedCorners: bool option) =
        fun () -> 
            let nativeFormat =             
                match format with 
                | Some e -> Nullable (e.ToNative()) 
                | None   -> Nullable ()
            let rc =             
                match roundedCorners with
                | Some v -> Nullable v
                | None   -> Nullable false
            CoreConfig.TableFormatting <- nativeFormat, rc

    /// <summary>
    /// Hide table column data types (i64, f64, str etc.).
    /// </summary>
    let tableHideColumnDataTypes (active: bool option) =
        fun () -> 
            match active with
            | Some v -> CoreConfig.TableHideColumnDataTypes <- Nullable v
            | None   -> CoreConfig.TableHideColumnDataTypes <- Nullable ()

    /// <summary>
    /// Hide table column names.
    /// </summary>
    let tableHideColumnNames (active: bool option) =
        fun () -> 
            match active with
            | Some v -> CoreConfig.TableHideColumnNames <- Nullable v
            | None   -> CoreConfig.TableHideColumnNames <- Nullable ()

    /// <summary>
    /// Hide the DataFrame shape information when displaying tables.
    /// </summary>
    let tableHideDataFrameShape (active: bool option) =
        fun () -> 
            match active with
            | Some v -> CoreConfig.TableHideDataFrameShape <- Nullable v
            | None   -> CoreConfig.TableHideDataFrameShape <- Nullable ()

    /// <summary>
    /// Hide the ‘—’ separator displayed between the column names and column types.
    /// </summary>
    let tableHideDataTypeSeparator (active: bool option) =
        fun () -> 
            match active with
            | Some v -> CoreConfig.TableHideDataTypeSeparator <- Nullable v
            | None   -> CoreConfig.TableHideDataTypeSeparator <- Nullable ()

    /// <summary>
    /// Set the maximum width of a table in characters.
    /// </summary>
    let tableWidthChars (width: int option) =
        fun () -> 
            match width with
            | Some v -> CoreConfig.TableWidthChars <- Nullable v
            | None   -> CoreConfig.TableWidthChars <- Nullable ()

    /// <summary>
    /// Set the maximum Retires for Delta Table operations.
    /// </summary>
    let deltaMaxRetries (retries: int option) =
        fun () -> 
            match retries with
            | Some v -> CoreConfig.PolarsDeltaMaxRetry <- Nullable v
            | None   -> CoreConfig.PolarsDeltaMaxRetry <- Nullable ()