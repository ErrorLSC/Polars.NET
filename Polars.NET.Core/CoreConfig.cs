using System.Runtime.CompilerServices;
using System.Text.Json;

namespace Polars.NET.Core;

/// <summary>
/// Config for Polars.NET
/// </summary>
internal static class CoreConfig
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string? ToFmtString(this bool? value) =>
        value == null ? null : (value.Value ? "1" : "0");
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string ToFmtString(this bool value) => value ? "1" : "0";
    
    private const string PrefetchBufferSizeKey = "POLARS_NET_PREFETCH_SIZE";
    private static int _defaultPrefetchBufferSize = GetDefaultPrefetchSizeFromEnv();
    /// <summary>
    /// Background Async Prefetch batch size 
    /// Default is 2. Can change this by SetEnvironmentVariable("POLARS_NET_PREFETCH_SIZE")
    /// </summary>
    public static int DefaultPrefetchBufferSize 
    { 
        get => _defaultPrefetchBufferSize;
        set
        {
            if (value <= 0) throw new ArgumentOutOfRangeException(nameof(value), "Must be greater than 0");
            _defaultPrefetchBufferSize = value;
            
            Environment.SetEnvironmentVariable(PrefetchBufferSizeKey, value.ToString());
        }
    }

    private static int GetDefaultPrefetchSizeFromEnv()
    {
        var envValue = Environment.GetEnvironmentVariable(PrefetchBufferSizeKey);
        
        if (int.TryParse(envValue, out int size) && size > 0)
        {
            return size;
        }
        
        return 2;
    }

    private static readonly string[] KnownEnvVars =
    [
        VerboseKey,
        WarnUnknownConfigKey,
        ThreadPoolSizeKey,
        WarnUnstableKey,
        StreamingChunkSizeKey,
        EngineAffinityKey,
        ParquetBinaryStatisticsTruncateLengthKey,
        PruneParquetMetadataKey,
        AllowNestedCspeKey,
        ResolveMetadataLevelKey,
        VerboseSensitiveKey,
        ForceAsyncKey,
        ImportIntervalAsStructKey,
        OOCDriftThresholdKey,
        OOCSpillPolicyKey,
        OOCSpillFormatKey,
        OOCMemoryBudgetFractionKey,
        OOCSpillMinBytesKey,
        OOCSpillDirKey,
        JoinSampleLimitKey,
        ProjectionPushdownPruneStrictHconcatInputsKey,
        // Formats
        StringLengthKey,
        TableFormattingKey,
        TableCellListLengthKey,
        TableHideColumnDataTypesKey,
        TableCellAlignmentKey,
        TableCellNumericAlignmentKey,
        TableMaxColsKey,
        TableMaxRowsKey,
        TableColumnDataTypeInlineKey,
        TableDataFrameShapeBelowKey,
        RoundedCornersKey,
        TableHideColumnNamesKey,
        TableHideDataFrameShapeKey,
        TableHideDataTypeSeparatorKey,
        TableWidthCharsKey,
        // Polars.NET specified
        PrefetchBufferSizeKey
        

    ];

    private static readonly (string DirectKey, Func<string?> ValueGetter)[] DirectVarRules =
    [
        (
            "decimal_separator", 
            () => DecimalSeparator?.ToString() 
        ),
        (
            "thousands_separator", 
            () => ThousandsSeparator?.ToString()
        ),
        (
            "float_precision", 
            () => FloatPrecision?.ToString()
        ),
        (
            "float_format", 
            () => FloatFormat?.ToString() 
        ),
        (
            "trim_decimal_zeros", 
            () => TrimDecimalZeros?.ToFmtString() 
        )
    ];

    public static Dictionary<string, string?> GetState(bool ifSet = false, bool envOnly = false)
    {
        var state = new Dictionary<string, string?>(StringComparer.Ordinal);

        foreach (var varName in KnownEnvVars)
        {
            string? envVal = Environment.GetEnvironmentVariable(varName);

            if (ifSet && envVal == null)
                continue;

            state[varName] = envVal;
        }

        if (!envOnly)
        {
            foreach (var (directKey, valueGetter) in DirectVarRules)
            {
                string? currentVal = valueGetter();

                if (ifSet && currentVal == null)
                    continue;

                state[directKey] = currentVal;
            }
        }

        return state;
    }
    private static readonly JsonSerializerOptions SaveOptionsCache = new()
    {
        WriteIndented = false,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.Never 
    };
    /// <summary>
    /// Saves the current Polars.NET configuration states into a compact JSON string.
    /// </summary>
    /// <param name="ifSet">If true, only exports configuration items that have been explicitly set.</param>
    /// <returns>A compact JSON string representation of the full configuration state.</returns>
    public static string Save(bool ifSet = false)
    {
        var environmentVars = GetState(ifSet, envOnly: true);

        var allState = GetState(ifSet, envOnly: false);
        var directVars = new Dictionary<string, string?>(StringComparer.Ordinal);
        
        foreach (var (directKey, _) in DirectVarRules)
        {
            if (allState.TryGetValue(directKey, out string? value))
            {
                directVars[directKey] = value;
            }
        }

        var optionsPayload = new
        {
            environment = environmentVars,
            direct = directVars
        };

        return JsonSerializer.Serialize(optionsPayload, SaveOptionsCache);
    }
    /// <summary>
    /// Saves the current configuration states into a JSON file.
    /// </summary>
    /// <param name="path">The file path where the configuration JSON will be written.</param>
    /// <param name="ifSet">If true, only exports configuration items that have been explicitly set.</param>
    public static void SaveToFile(string path, bool ifSet = false)
    {
        string json = Save(ifSet);
        File.WriteAllText(path, json);
    }

    /// <summary>
    /// Asynchronously saves the current configuration states into a JSON file.
    /// </summary>
    public static async Task SaveToFileAsync(string path, bool ifSet = false)
    {
        string json = Save(ifSet);
        await File.WriteAllTextAsync(path, json).ConfigureAwait(false);
    }
    /// <summary>
    /// Loads a Polars.NET configuration state from a JSON string, synchronizes BOTH .NET and Rust 
    /// environments, restores direct API states, and triggers a full backend reload.
    /// </summary>
    /// <param name="cfg">The JSON configuration string generated by Save().</param>
    /// <exception cref="ArgumentException">Thrown when the config string is invalid or corrupted.</exception>
    public static void Load(string cfg)
    {
        JsonDocument doc;
        try
        {
            doc = JsonDocument.Parse(cfg);
        }
        catch (JsonException ex)
        {
            throw new ArgumentException("Invalid Config string (did you mean to use `LoadFromFile`?)", ex);
        }

        using (doc)
        {
            var root = doc.RootElement;

            if (root.TryGetProperty("environment", out var envProp) && envProp.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in envProp.EnumerateObject())
                {
                    string key = prop.Name;
                    
                    if (prop.Value.ValueKind == JsonValueKind.Null)
                    {
                        PolarsWrapper.SetEnvVar(key, null);
                    }
                    else
                    {
                        string? val = prop.Value.GetString();
                        PolarsWrapper.SetEnvVar(key, val);
                    }
                }
            }

            if (root.TryGetProperty("direct", out var directProp) && directProp.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in directProp.EnumerateObject())
                {
                    string key = prop.Name.ToLowerInvariant();
                    bool isNull = prop.Value.ValueKind == JsonValueKind.Null;
                    string? strValue = isNull ? null : prop.Value.GetString();

                    switch (key)
                    {
                        case "prefetch_buffer_size":
                            DefaultPrefetchBufferSize = isNull ? 2 : (int.TryParse(strValue, out int pbSize) ? pbSize : 2);
                            break;

                        case "decimal_separator":

                            if (isNull) DecimalSeparator = null;
                            else if (!string.IsNullOrEmpty(strValue)) DecimalSeparator = strValue[0];
                            break;

                        case "trim_decimal_zeros":
                            if (isNull || string.IsNullOrEmpty(strValue))
                            {
                                TrimDecimalZeros = null;
                            }
                            else
                            {
                                string cleanVal = strValue.Trim().ToLowerInvariant();
                                
                                if (cleanVal is "1" or "true")
                                {
                                    TrimDecimalZeros = true;
                                }
                                else if (cleanVal is "0" or "false")
                                {
                                    TrimDecimalZeros = false;
                                }
                                else
                                {
                                    TrimDecimalZeros = null; 
                                }
                            }
                            break;

                        case "thousands_separator":
                            if (isNull) ThousandsSeparator = null;
                            else if (!string.IsNullOrEmpty(strValue)) ThousandsSeparator = strValue[0];
                            break;

                        case "float_precision":
                            if (isNull) FloatPrecision = null;
                            else if (long.TryParse(strValue, out long precision)) FloatPrecision = precision;
                            break;

                        case "float_format":
                            if (isNull) FloatFormat = null;
                            else if (Enum.TryParse<PlFloatFormat>(strValue, out var fFormat)) FloatFormat = fFormat;
                            break;
                    }
                }
            }
        }

        PolarsWrapper.ReloadEnvVarAll(); 
    }
    /// <summary>
    /// Loads a configuration state from a JSON file, updates states, and triggers a backend reload.
    /// </summary>
    /// <param name="path">The path to the configuration JSON file.</param>
    /// <exception cref="ArgumentException">Thrown when the file cannot be read or contains corrupted data.</exception>
    public static void LoadFromFile(string path)
    {
        string json;
        try
        {
            json = File.ReadAllText(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new ArgumentException($"Invalid Config file (did you mean to use `Load`?)\n{ex.Message}", ex);
        }

        Load(json);
    }

    /// <summary>
    /// Asynchronously loads a configuration state from a JSON file, updates states, and triggers a backend reload.
    /// </summary>
    public static async Task LoadFromFileAsync(string path)
    {
        string json;
        try
        {
            json = await File.ReadAllTextAsync(path).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new ArgumentException($"Invalid Config file (did you mean to use `Load`?)\n{ex.Message}", ex);
        }

        Load(json);
    }
    /// <summary>
    /// Resets all Polars.NET global configurations and environment variables back to their default factory states.
    /// </summary>
    public static void RestoreDefaults()
    {
        foreach (var varName in KnownEnvVars)
        {
            PolarsWrapper.SetEnvVar(varName, null);
        }

        DefaultPrefetchBufferSize = 2; 
        DecimalSeparator = null;         
        ThousandsSeparator = null;       
        FloatPrecision = null;           
        FloatFormat = null;
        TrimDecimalZeros = null;              

        PolarsWrapper.ReloadEnvVarAll();
    }
    private const string TableFormattingKey = "POLARS_FMT_TABLE_FORMATTING";

    public static bool? AsciiTables
    {
        get
        {
            string? current = Environment.GetEnvironmentVariable(TableFormattingKey);
            if (current == null) return null;
            return current == "ASCII_FULL_CONDENSED";
        }
        set
        {
            string? fmtStr = value switch
            {
                true => "ASCII_FULL_CONDENSED",
                false => "UTF8_FULL_CONDENSED",
                null => null
            };
            PolarsWrapper.SetEnvVar(TableFormattingKey, fmtStr);
        }
    }

    /// <summary>
    /// Gets or sets the decimal separator used by Polars global configuration.
    /// </summary>
    public static char? DecimalSeparator
    {
        get
        {
            string? res = PolarsWrapper.ConfigGetDecimalSeparator();
            return string.IsNullOrEmpty(res) ? null : res[0];
        }
        set
        {
            PolarsWrapper.ConfigSetDecimalSeparator(value?.ToString());
        }
    }
    public static char? ThousandsSeparator
    {
        get
        {
            string? res = PolarsWrapper.ConfigGetThousandsSeparator();
            return string.IsNullOrEmpty(res) ? null : res[0];
        }
        set
        {
            PolarsWrapper.ConfigSetThousandsSeparator(value?.ToString());
        }
    }
    private const string EngineAffinityKey = "POLARS_ENGINE_AFFINITY";
    /// <summary>
    /// Gets or sets the global engine affinity for query execution. 
    /// Returns null if not explicitly overridden via this property.
    /// </summary>
    public static PlEngine? EngineAffinity
    {
        get => PolarsWrapper.ConfigGetEngineAffinity();
        set
        {
            if (value == PlEngine.Gpu)
            {
                throw new NotImplementedException("GPU engine with non-defaults not yet supported in Polars.NET.");
            }


            string? envValue = value switch
            {
                PlEngine.Auto => "auto",
                PlEngine.InMemory => "in-memory",
                PlEngine.Streaming => "streaming",
                _ => null 
            };

            PolarsWrapper.SetEnvVar(EngineAffinityKey, envValue);
        }
    }

    public static long? FloatPrecision
    {
        get
        {
            long precision = PolarsWrapper.ConfigGetFloatPrecision();
            return precision < 0 ? null : precision;
        }
        set
        {
            PolarsWrapper.ConfigSetFloatPrecision(value ?? -1);
        }
    } 

    public static PlFloatFormat? FloatFormat
    {
        get => PolarsWrapper.ConfigGetFloatFormat();
        set => PolarsWrapper.ConfigSetFloatFormat(value ?? PlFloatFormat.Mixed);
    }

    private const string StringLengthKey = "POLARS_FMT_STR_LEN";

    public static int? StringLength
    {
        get
        {
            string? current = Environment.GetEnvironmentVariable(StringLengthKey);
            return int.TryParse(current, out int result) ? result : null;
        }
        set
        {
            PolarsWrapper.SetEnvVar(StringLengthKey, value?.ToString());
        }
    }

    private const string TableCellListLengthKey = "POLARS_FMT_TABLE_CELL_LIST_LEN";

    public static int? TableCellListLength
    {
        get
        {
            string? current = Environment.GetEnvironmentVariable(TableCellListLengthKey);
            return int.TryParse(current, out int result) ? result : null;
        }
        set
        {
            PolarsWrapper.SetEnvVar(TableCellListLengthKey, value?.ToString());
        }
    }

    private const string StreamingChunkSizeKey = "POLARS_IDEAL_MORSEL_SIZE";

    public static ulong? StreamingChunkSize
    {
        get => PolarsWrapper.ConfigGetIdealMorselSize();
        set
        {
            if (value < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "Number of rows per chunk must be >= 1.");
            }

            PolarsWrapper.SetEnvVar(StreamingChunkSizeKey, value?.ToString());
        }
    }

    private const string TableCellAlignmentKey = "POLARS_FMT_TABLE_CELL_ALIGNMENT";

    public static PlTableCellAlignment? TableCellAlignment
    {
        get
        {
            string? envValue = Environment.GetEnvironmentVariable(TableCellAlignmentKey);
            
            return envValue?.ToUpperInvariant() switch
            {
                "LEFT" => PlTableCellAlignment.Left,
                "CENTER" => PlTableCellAlignment.Center,
                "RIGHT" => PlTableCellAlignment.Right,
                _ => null
            };
        }
        set
        {
            string? envValue = value switch
            {
                PlTableCellAlignment.Left => "LEFT",
                PlTableCellAlignment.Center => "CENTER",
                PlTableCellAlignment.Right => "RIGHT",
                _ => null 
            };
            PolarsWrapper.SetEnvVar(TableCellAlignmentKey, envValue);
        }
    }

    private const string TableCellNumericAlignmentKey = "POLARS_FMT_TABLE_CELL_NUMERIC_ALIGNMENT";

    public static PlTableCellAlignment? TableCellNumericAlignment
    {
        get
        {
            string? envValue = Environment.GetEnvironmentVariable(TableCellNumericAlignmentKey);
            
            return envValue?.ToUpperInvariant() switch
            {
                "LEFT" => PlTableCellAlignment.Left,
                "CENTER" => PlTableCellAlignment.Center,
                "RIGHT" => PlTableCellAlignment.Right,
                _ => null
            };
        }
        set
        {
            string? envValue = value switch
            {
                PlTableCellAlignment.Left => "LEFT",
                PlTableCellAlignment.Center => "CENTER",
                PlTableCellAlignment.Right => "RIGHT",
                _ => null 
            };
            PolarsWrapper.SetEnvVar(TableCellNumericAlignmentKey, envValue);
        }
    }
    private const string TableMaxColsKey = "POLARS_FMT_MAX_COLS";

    public static int? TableMaxCols
    {
        get
        {
            string? current = Environment.GetEnvironmentVariable(TableMaxColsKey);
            return int.TryParse(current, out int result) ? result : null;
        }
        set => PolarsWrapper.SetEnvVar(TableMaxColsKey, value?.ToString());
    }
    private const string TableMaxRowsKey = "POLARS_FMT_MAX_ROWS";

    public static int? TableMaxRows
    {
        get
        {
            string? current = Environment.GetEnvironmentVariable(TableMaxRowsKey);
            return int.TryParse(current, out int result) ? result : null;
        }
        set => PolarsWrapper.SetEnvVar(TableMaxRowsKey, value?.ToString());
    }
    private const string TableWidthCharsKey = "POLARS_TABLE_WIDTH";

    public static int? TableWidthChars
    {
        get
        {
            string? current = Environment.GetEnvironmentVariable(TableWidthCharsKey);
            return int.TryParse(current, out int result) ? result : null;
        }
        set => PolarsWrapper.SetEnvVar(TableWidthCharsKey, value?.ToString());
    }
    private const string TableColumnDataTypeInlineKey = "POLARS_FMT_TABLE_INLINE_COLUMN_DATA_TYPE";

    public static bool? TableColumnDataTypeInline
    {
        get
        {
            string? current = Environment.GetEnvironmentVariable(TableColumnDataTypeInlineKey);
            return current == null ? null : current == "1";
        }
        set => PolarsWrapper.SetEnvVar(TableColumnDataTypeInlineKey, value.ToFmtString());
    }

    private const string TableDataFrameShapeBelowKey = "POLARS_FMT_TABLE_DATAFRAME_SHAPE_BELOW";
    public static bool? TableDataFrameShapeBelow
    {
        get
        {
            string? current = Environment.GetEnvironmentVariable(TableDataFrameShapeBelowKey);
            return current == null ? null : current == "1";
        }
        set => PolarsWrapper.SetEnvVar(TableDataFrameShapeBelowKey, value.ToFmtString());
        
    }
    private const string RoundedCornersKey = "POLARS_FMT_TABLE_ROUNDED_CORNERS";

    /// <summary>
    /// Gets or sets the table formatting style and corner roundness as a tuple.
    /// Set to null or pass null components to reset to defaults.
    /// </summary>
    public static (PlTableFormatting? Format, bool? RoundedCorners) TableFormatting
    {
        get
        {
            string? fmtEnv = Environment.GetEnvironmentVariable(TableFormattingKey);
            PlTableFormatting? format = fmtEnv?.ToUpperInvariant() switch
            {
                "ASCII_FULL" => PlTableFormatting.AsciiFull,
                "ASCII_FULL_CONDENSED" => PlTableFormatting.AsciiFullCondensed,
                "ASCII_NO_BORDERS" => PlTableFormatting.AsciiNoBorders,
                "ASCII_BORDERS_ONLY" => PlTableFormatting.AsciiBordersOnly,
                "ASCII_BORDERS_ONLY_CONDENSED" => PlTableFormatting.AsciiBordersOnlyCondensed,
                "ASCII_HORIZONTAL_ONLY" => PlTableFormatting.AsciiHorizontalOnly,
                "ASCII_MARKDOWN" => PlTableFormatting.AsciiMarkdown,
                "MARKDOWN" => PlTableFormatting.Markdown,
                "UTF8_FULL" => PlTableFormatting.Utf8Full,
                "UTF8_FULL_CONDENSED" => PlTableFormatting.Utf8FullCondensed,
                "UTF8_NO_BORDERS" => PlTableFormatting.Utf8NoBorders,
                "UTF8_BORDERS_ONLY" => PlTableFormatting.Utf8BordersOnly,
                "UTF8_HORIZONTAL_ONLY" => PlTableFormatting.Utf8HorizontalOnly,
                "NOTHING" => PlTableFormatting.Nothing,
                _ => null
            };

            string? cornersEnv = Environment.GetEnvironmentVariable(RoundedCornersKey);
            bool? roundedCorners = cornersEnv switch
            {
                "1" => true,
                "0" => false,
                _ => null
            };

            return (format, roundedCorners);
        }
        set
        {
            string? fmtStr = value.Format switch
            {
                PlTableFormatting.AsciiFull => "ASCII_FULL",
                PlTableFormatting.AsciiFullCondensed => "ASCII_FULL_CONDENSED",
                PlTableFormatting.AsciiNoBorders => "ASCII_NO_BORDERS",
                PlTableFormatting.AsciiBordersOnly => "ASCII_BORDERS_ONLY",
                PlTableFormatting.AsciiBordersOnlyCondensed => "ASCII_BORDERS_ONLY_CONDENSED",
                PlTableFormatting.AsciiHorizontalOnly => "ASCII_HORIZONTAL_ONLY",
                PlTableFormatting.AsciiMarkdown => "ASCII_MARKDOWN",
                PlTableFormatting.Markdown => "MARKDOWN",
                PlTableFormatting.Utf8Full => "UTF8_FULL",
                PlTableFormatting.Utf8FullCondensed => "UTF8_FULL_CONDENSED",
                PlTableFormatting.Utf8NoBorders => "UTF8_NO_BORDERS",
                PlTableFormatting.Utf8BordersOnly => "UTF8_BORDERS_ONLY",
                PlTableFormatting.Utf8HorizontalOnly => "UTF8_HORIZONTAL_ONLY",
                PlTableFormatting.Nothing => "NOTHING",
                _ => null
            };

            PolarsWrapper.SetEnvVar(TableFormattingKey, fmtStr);
            PolarsWrapper.SetEnvVar(RoundedCornersKey, value.RoundedCorners.ToFmtString());
        }
    }
    private const string TableHideColumnDataTypesKey = "POLARS_FMT_TABLE_HIDE_COLUMN_DATA_TYPES";

    public static bool? TableHideColumnDataTypes
    {
        get
        {
            string? current = Environment.GetEnvironmentVariable(TableHideColumnDataTypesKey);
            return current == null ? null : current == "1";
        }
        set => PolarsWrapper.SetEnvVar(TableHideColumnDataTypesKey, value.ToFmtString());
    }
    private const string TableHideColumnNamesKey = "POLARS_FMT_TABLE_HIDE_COLUMN_NAMES";

    public static bool? TableHideColumnNames
    {
        get
        {
            string? current = Environment.GetEnvironmentVariable(TableHideColumnNamesKey);
            return current == null ? null : current == "1";
        }
        set => PolarsWrapper.SetEnvVar(TableHideColumnNamesKey, value.ToFmtString());
    }
    private const string TableHideDataFrameShapeKey = "POLARS_FMT_TABLE_HIDE_DATAFRAME_SHAPE_INFORMATION";

    public static bool? TableHideDataFrameShape
    {
        get
        {
            string? current = Environment.GetEnvironmentVariable(TableHideDataFrameShapeKey);
            return current == null ? null : current == "1";
        }
        set => PolarsWrapper.SetEnvVar(TableHideDataFrameShapeKey, value.ToFmtString());
    }
    private const string TableHideDataTypeSeparatorKey = "POLARS_FMT_TABLE_HIDE_COLUMN_SEPARATOR";

    public static bool? TableHideDataTypeSeparator
    {
        get
        {
            string? current = Environment.GetEnvironmentVariable(TableHideDataTypeSeparatorKey);
            return current == null ? null : current == "1";
        }
        set => PolarsWrapper.SetEnvVar(TableHideDataTypeSeparatorKey, value.ToFmtString());
    }
    public static bool? TrimDecimalZeros
    {
        get => PolarsWrapper.ConfigGetTrimDecimalZeros();
        set
        {
            bool realValue = value ?? false;
            PolarsWrapper.ConfigSetTrimDecimalZeros(realValue,value.HasValue);
        }
    }

    private const string VerboseKey = "POLARS_VERBOSE";

    public static bool? Verbose
    {
        get => PolarsWrapper.ConfigGetVerbose();
        set => PolarsWrapper.SetEnvVar(VerboseKey, value.ToFmtString());
    }
    private const string WarnUnstableKey = "POLARS_WARN_UNSTABLE";

    public static bool? WarnUnstable
    {
        get => PolarsWrapper.ConfigGetWarnUnstable();
        set => PolarsWrapper.SetEnvVar(WarnUnstableKey, value.ToFmtString());
    }
    private const string ImportIntervalAsStructKey = "POLARS_IMPORT_INTERVAL_AS_STRUCT";
    public static bool? ImportIntervalAsStruct
    {
        get => PolarsWrapper.ConfigGetImportIntervalAsStruct();
        set => PolarsWrapper.SetEnvVar(ImportIntervalAsStructKey, value.ToFmtString());
    }
    private const string ThreadPoolSizeKey = "POLARS_MAX_THREADS";
    public static ulong? ThreadPoolSize
    {
        get => PolarsWrapper.ConfigGetMaxThreads();
        set => PolarsWrapper.SetEnvVar(ThreadPoolSizeKey,value?.ToString());
    }

    private const string WarnUnknownConfigKey = "POLARS_WARN_UNKNOWN_CONFIG";
    public static bool? WarnUnknownConfig
    {
        get
        {
            string? current = Environment.GetEnvironmentVariable(WarnUnknownConfigKey);
            return current == null ? null : current == "1";
        }
        set => PolarsWrapper.SetEnvVar(WarnUnknownConfigKey, value.ToFmtString());
    }

    private const string ParquetBinaryStatisticsTruncateLengthKey ="POLARS_PARQUET_BINARY_STATISTICS_TRUNCATE_LEN";

    public static ulong? ParquetBinaryStatisticsTruncateLength
    {
        get => PolarsWrapper.ConfigGetParquetBinaryStatisticsTruncateLength();
        set => PolarsWrapper.SetEnvVar(ParquetBinaryStatisticsTruncateLengthKey, value?.ToString());
    }
    private const string PruneParquetMetadataKey = "POLARS_PRUNE_PARQUET_METADATA";
    public static bool? PruneParquetMetadata
    {
        get => PolarsWrapper.ConfigGetPruneParquetMetadata();
        set => PolarsWrapper.SetEnvVar(PruneParquetMetadataKey, value.ToFmtString());
    }
    private const string AllowNestedCspeKey = "POLARS_ALLOW_NESTED_CSPE";
    public static bool? AllowNestedCspe
    {
        get => PolarsWrapper.ConfigGetAllowNestedCspe();
        set => PolarsWrapper.SetEnvVar(AllowNestedCspeKey, value.ToFmtString());
    }
    private const string ResolveMetadataLevelKey = "POLARS_RESOLVE_METADATA_LEVEL";
    public static PlResolveMode? ResolveMetadataLevel
    {
        get => PolarsWrapper.ConfigGetResolveMetadataLevel();
        set
        {
            string? envValue = value switch
            {
                PlResolveMode.None => "none",
                PlResolveMode.RowCounts => "row_counts",
                PlResolveMode.Full => "full",
                _ => null 
            };
            PolarsWrapper.SetEnvVar(ResolveMetadataLevelKey, envValue);
        }
    }
    private const string VerboseSensitiveKey = "POLARS_VERBOSE_SENSITIVE";
    public static bool? VerboseSensitive
    {
        get => PolarsWrapper.ConfigGetVerboseSensitive();
        set => PolarsWrapper.SetEnvVar(VerboseSensitiveKey, value.ToFmtString());
    }
    private const string ForceAsyncKey = "POLARS_FORCE_ASYNC";
    public static bool? ForceAsync
    {
        get => PolarsWrapper.ConfigGetForceAsync();
        set => PolarsWrapper.SetEnvVar(ForceAsyncKey,value.ToFmtString());
    }
    private const string OOCDriftThresholdKey = "POLARS_OOC_DRIFT_THRESHOLD";
    public static ulong? OOCDriftThreshold
    {
        get => PolarsWrapper.ConfigGetOOCDriftThreshold();
        set => PolarsWrapper.SetEnvVar(OOCDriftThresholdKey,value?.ToString());
    }
    private const string OOCSpillPolicyKey = "POLARS_OOC_SPILL_POLICY";
    public static PlOOCSpillPolicy? OOCSpillPolicy
    {
        get => PolarsWrapper.ConfigGetOOCSpillPolicy();
        set
        {
            string? envValue = value switch
            {
                PlOOCSpillPolicy.NoSpill => "no_spill",
                PlOOCSpillPolicy.Spill => "spill",
                _ => null 
            };
            PolarsWrapper.SetEnvVar(OOCSpillPolicyKey , envValue);
        }
    }
    private const string OOCSpillFormatKey = "POLARS_OOC_SPILL_FORMAT";

    public static PlOOCSpillFormat? OOCSpillFormat
    {
        get => PolarsWrapper.ConfigGetOOCSpillFormat();
        set
        {
            string? envValue = value switch
            {
                PlOOCSpillFormat.Ipc => "ipc",
                _ => null 
            };
            PolarsWrapper.SetEnvVar(OOCSpillFormatKey, envValue);
        }
    }
    private const string OOCMemoryBudgetFractionKey = "POLARS_OOC_MEMORY_BUDGET_FRACTION";
    public static double? OOCMemoryBudgetFraction
    {
        get => PolarsWrapper.ConfigGetOOCMemoryBudgetFraction();
        set => PolarsWrapper.SetEnvVar(OOCMemoryBudgetFractionKey,value?.ToString());
    }
    private const string OOCSpillMinBytesKey = "POLARS_OOC_SPILL_MIN_BYTES";
    public static ulong? OOCSpillMinBytes
    {
        get => PolarsWrapper.ConfigGetOOCSpillMinBytes();
        set => PolarsWrapper.SetEnvVar(OOCSpillMinBytesKey,value?.ToString());
    }
    private const string OOCSpillDirKey = "POLARS_OOC_SPILL_DIR";
    public static string? OOCSpillDir
    {
        get => PolarsWrapper.ConfigGetOOCSpillDir();
        set => PolarsWrapper.SetEnvVar(OOCSpillDirKey,value);
    }
    private const string JoinSampleLimitKey = "POLARS_JOIN_SAMPLE_LIMIT";
    public static ulong? JoinSampleLimit
    {
        get => PolarsWrapper.ConfigGetJoinSampleLimit();
        set => PolarsWrapper.SetEnvVar(JoinSampleLimitKey,value?.ToString());
    }
    private const string ProjectionPushdownPruneStrictHconcatInputsKey = "POLARS_PROJECTION_PUSHDOWN_PRUNE_STRICT_HCONCAT_INPUTS";
    public static bool? ProjectionPushdownPruneStrictHconcatInputs
    {
        get => PolarsWrapper.ConfigGetProjectionPushdownPruneStrictHconcatInputs();
        set => PolarsWrapper.SetEnvVar(ProjectionPushdownPruneStrictHconcatInputsKey,value.ToFmtString());
    }
}