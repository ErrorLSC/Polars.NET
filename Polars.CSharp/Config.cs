#pragma warning disable CS1591
using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// Config for Polars.NET
/// </summary>
public sealed class PolarsConfig:IDisposable
{
    private string? _previousStateBackup;
    private bool _isContextActivated;
    public PolarsConfig() { }
    private void RecordOldValueAndApply(Action applyAction)
    {
        if (!_isContextActivated && _previousStateBackup == null)
        {
            _previousStateBackup = CoreConfig.Save(ifSet: false);
        }

        applyAction();

        PolarsWrapper.ReloadEnvVarAll();
    }
    /// <summary>
    /// Activates this config profile by backing up the current global state and applying overrides.
    /// </summary>
    public PolarsConfig Enter()
    {
        _isContextActivated = true;
        return this;
    }

    /// <summary>
    /// Releases the configuration context, rolling back BOTH .NET and Rust configurations
    /// precisely to the state they were in before <see cref="Enter"/> was called.
    /// </summary>
    public void Dispose()
    {
        if (_previousStateBackup != null)
        {
            CoreConfig.Load(_previousStateBackup);
            _previousStateBackup = null;
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Save the current set of Config options as a JSON string.
    /// </summary>
    /// <param name="ifSet">By default this will save the state of all configuration options; set to False to save only those that have been set to a non-default value.</param>
    public string Save(bool ifSet = false) => CoreConfig.Save(ifSet);

    /// <summary>
    /// Save the current set of Config options as a JSON file.
    /// </summary>
    public void SaveToFile(string path, bool ifSet = false) => CoreConfig.SaveToFile(path, ifSet);

    /// <summary>
    /// Load (and set) previously saved Config options from a JSON string.
    /// </summary>
    public void Load(string cfgJson) => CoreConfig.Load(cfgJson);

    /// <summary>
    /// Load (and set) previously saved Config options from file.
    /// </summary>
    public void LoadFromFile(string path) => CoreConfig.LoadFromFile(path);

    /// <summary>
    /// Reset all polars Config settings to their default state.
    /// </summary>
    public void RestoreDefaults() => CoreConfig.RestoreDefaults();

    /// <summary>
    /// Reset all polars Config settings to their default state.
    /// </summary>
    /// <param name="ifSet">By default this will show the state of all Config environment variables. 
    /// change this to True to restrict the returned dictionary to include only those that have been set to a specific value.</param>
    /// <param name="envOnly">Include only Config environment variables in the output; some options (such as “set_fmt_float”) are set directly, not via an environment variable.</param>
    public Dictionary<string, string?> Status(bool ifSet = false, bool envOnly = false) 
        => CoreConfig.GetState(ifSet, envOnly);

    // ========================================================
    // Indexer
    // ========================================================
    
    /// <summary>
    /// string style = cfg["decimal_separator"];
    /// </summary>
    public string? this[string key]
    {
        get
        {
            var currentStatus = Status(ifSet: false, envOnly: false);
            if (currentStatus.TryGetValue(key, out var val)) return val;
            if (currentStatus.TryGetValue(key.ToLowerInvariant(), out var valLower)) return valLower;
            return null;
        }
        set
        {
            if (string.IsNullOrEmpty(key)) return;

            string normalizedKey = key.Trim();
            string lowerKey = normalizedKey.ToLowerInvariant();

            RecordOldValueAndApply(() =>
            {
                if (lowerKey is "decimal_separator" or "thousands_separator" or "float_precision" or "float_format" or "prefetch_buffer_size" or "trim_decimal_zeros")
                {
                    switch (lowerKey)
                    {
                        case "decimal_separator":
                            CoreConfig.DecimalSeparator = string.IsNullOrEmpty(value) ? null : value[0];
                            break;
                        case "thousands_separator":
                            CoreConfig.ThousandsSeparator = string.IsNullOrEmpty(value) ? null : value[0];
                            break;
                        case "float_precision":
                            CoreConfig.FloatPrecision = long.TryParse(value, out long prec) ? prec : null;
                            break;
                        case "float_format":
                            CoreConfig.FloatFormat = Enum.TryParse<PlFloatFormat>(value, out var fmt) ? fmt : null;
                            break;
                        case "prefetch_buffer_size":
                            if (int.TryParse(value, out int pbSize)) CoreConfig.DefaultPrefetchBufferSize = pbSize;
                            break;
                        case "trim_decimal_zeros":
                            if (value == null)
                            {
                                CoreConfig.TrimDecimalZeros = null;
                            }
                            else
                            {
                                string cleanVal = value.Trim().ToLowerInvariant();
                                
                                if (cleanVal is "1" or "true")
                                {
                                    CoreConfig.TrimDecimalZeros = true;
                                }
                                else if (cleanVal is "0" or "false")
                                {
                                    CoreConfig.TrimDecimalZeros = false;
                                }
                                else
                                {
                                    CoreConfig.TrimDecimalZeros = null; 
                                }
                            }
                            break;
                    }
                }
                else
                {
                    PolarsWrapper.SetEnvVar(normalizedKey, value);
                }
            });
        }
    }
    /// <summary>
    /// Inject Environment var to Rust
    /// </summary>
    public static void SetEnvVar(string key, string value)
    {
        PolarsWrapper.SetEnvVar(key, value);
        PolarsWrapper.ReloadEnvVar(key);
    }
    /// <summary>
    /// Inject Environment var to Rust by KeyValuePair
    /// </summary>
    /// <param name="variables">Collection or dictionary contains environment var keyvalue pair. </param>
    public static void SetEnvVars(IEnumerable<KeyValuePair<string, string>> variables)
    {
        ArgumentNullException.ThrowIfNull(variables);
        
        foreach (var kvp in variables)
        {
            PolarsWrapper.SetEnvVar(kvp.Key, kvp.Value);
        }

        PolarsWrapper.ReloadEnvVarAll();
    }
    /// <summary>
    /// Inject Environment var to Rust by KeyValuePair tuple
    /// </summary>
    public static void SetEnvVars(params (string Key, string Value)[] variables)
    {
        if (variables == null || variables.Length == 0) return;
        
        foreach (var (key, value) in variables)
        {
            PolarsWrapper.SetEnvVar(key, value);
        }

        PolarsWrapper.ReloadEnvVarAll();
    }
    /// <summary>
    /// Use ASCII characters to display table outlines.
    /// Set False to revert to the standard UTF8_FULL_CONDENSED formatting style.
    /// </summary>
    public PolarsConfig SetAsciiTables(bool? active=true)
    {
        CoreConfig.AsciiTables = active;
        return this;
    }

    /// <summary>
    /// Set the decimal separator character.
    /// </summary>
    /// <param name="separator">Character to use as the decimal separator. Set to None to revert to the default ('.').</param>
    public PolarsConfig SetDecimalSeparator(char? separator=null)
    {
        CoreConfig.DecimalSeparator = separator;
        return this;
    }
    /// <summary>
    /// Configures the thousands separator with a shortcut boolean.
    /// </summary>
    /// <param name="useDefaultFormat">
    /// If true, applies standard English format: sets thousands separator to ',' and decimal separator to '.'.
    /// If false, clears the thousands separator.
    /// </param>
    public PolarsConfig SetThousandsSeparator(bool useDefaultFormat)
    {
        if (useDefaultFormat)
        {
            CoreConfig.DecimalSeparator = '.';
            CoreConfig.ThousandsSeparator = ',';
        }
        else
        {
            CoreConfig.ThousandsSeparator = null;
        }

        return this;
    }

    /// <summary>
    /// Configures the thousands separator with a specific single character.
    /// Set to null to clear/reset the separator.
    /// </summary>
    public PolarsConfig SetThousandsSeparator(char? separator = null)
    {
        RecordOldValueAndApply(() => {CoreConfig.ThousandsSeparator = separator;});
        return this;
    }
    /// <summary>
    /// Strip trailing zeros from Decimal data type values.
    /// </summary>
    /// <param name="active">Enable stripping of trailing ‘0’ characters from Decimal values.</param>
    public PolarsConfig SetTrimDecimalZeros(bool? active=true)
    {
        RecordOldValueAndApply(() => {CoreConfig.TrimDecimalZeros = active;});
        return this;
    }
    /// <summary>
    /// Set which engine to use by default.
    /// </summary>
    /// <param name="engine">The default execution engine Polars will attempt to use when calling .Collect(). 
    /// However, the query is not guaranteed to execute with the specified engine.</param>
    public PolarsConfig SetEngineAffinity(Engine? engine=null)
    {
        RecordOldValueAndApply(() => {CoreConfig.EngineAffinity = engine?.ToNative();});
        return this;
    }
    /// <summary>
    /// Control the number of decimal places displayed for floating point values.
    /// </summary>
    /// <param name="precision">Number of decimal places to display; set to None to revert to the default/standard behaviour.</param>
    public PolarsConfig SetFloatPrecision(long? precision=null)
    {
        RecordOldValueAndApply(() => {CoreConfig.FloatPrecision = precision;});
        return this;
    }
    /// <summary>
    /// Control how floating point values are displayed.
    /// </summary>
    /// <param name="format">How to format floating point numbers</param>
    public PolarsConfig SetFormatFloat(FloatFormat? format=FloatFormat.Mixed)
    {
        RecordOldValueAndApply(() => {CoreConfig.FloatFormat = format?.ToNative();});
        return this;
    }
    /// <summary>
    /// Set the number of characters used to display string values.
    /// </summary>
    /// <param name="n">Number of characters to display.</param>
    public PolarsConfig SetFormatStringLength(int? n)
    {
        RecordOldValueAndApply(() => {CoreConfig.StringLength = n;});
        return this;
    }
    /// <summary>
    /// Set the number of elements to display for List values.
    /// Empty lists will always print “[]”. 
    /// <para>Negative values will result in all values being printed.</para> 
    /// <para>A value of 0 will always “[…]” for lists with contents. </para> 
    /// A value of 1 will print only the final item in the list.
    /// </summary>
    /// <param name="n">Number of values to display.</param>
    public PolarsConfig SetFormatTableCellListLength(int? n)
    {
        RecordOldValueAndApply(() => {CoreConfig.TableCellListLength = n;});
        return this;
    }
    /// <summary>
    /// Overwrite chunk size used in streaming engine.
    /// <para>By default, the chunk size is determined by the schema and size of the thread pool. </para>
    /// For some datasets (esp. when you have large string elements) this can be too optimistic and lead to Out of Memory errors.
    /// </summary>
    /// <param name="size">Number of rows per chunk. Every thread will process chunks of this size.</param>
    public PolarsConfig SetStreamingChunkSize(ulong? size)
    {
        RecordOldValueAndApply(() => {CoreConfig.StreamingChunkSize = size;});
        return this;
    }
    /// <summary>
    /// Set table cell alignment.
    /// </summary>
    public PolarsConfig SetTableCellAlignment(Alignment? format)
    {
        RecordOldValueAndApply(() => {CoreConfig.TableCellAlignment = format?.ToNative();});
        return this;
    }
    /// <summary>
    /// Set table cell alignment for numeric columns.
    /// </summary>
    public PolarsConfig SetTableCellNumericAlignment(Alignment? format)
    {
        RecordOldValueAndApply(() => {CoreConfig.TableCellNumericAlignment = format?.ToNative();});
        return this;
    }
    /// <summary>
    /// Set the number of columns that are visible when displaying tables.
    /// </summary>
    /// <param name="n">Number of columns to display; if n is less than 0 (eg: -1), display all columns.</param>
    public PolarsConfig SetTableCols(int? n)
    {
        RecordOldValueAndApply(() => {CoreConfig.TableMaxCols = n;});
        return this;
    }
    /// <summary>
    /// Set the max number of rows used to draw the table (both Dataframe and Series).
    /// </summary>
    /// <param name="n">Number of rows to display; if n is less than 0 (eg: -1), display all rows (DataFrame) and all elements (Series).</param>
    public PolarsConfig SetTableRows(int? n)
    {
        RecordOldValueAndApply(() => {CoreConfig.TableMaxRows = n;});
        return this;
    }
    /// <summary>
    /// Display the data type next to the column name (to the right, in parentheses).
    /// </summary>
    public PolarsConfig SetTableColumnDataTypeInline(bool? active=true)
    {
        RecordOldValueAndApply(() => {CoreConfig.TableColumnDataTypeInline = active;});
        return this;
    }
    /// <summary>
    /// Configures whether Polars should run in verbose mode, printing query profiles and optimization decisions.
    /// </summary>
    public PolarsConfig SetVerbose(bool? verbose=true)
    {
        RecordOldValueAndApply(() => {CoreConfig.Verbose = verbose;});
        return this; 
    }
    /// <summary>
    /// Print the DataFrame shape information below the data when displaying tables.
    /// </summary>
    public PolarsConfig SetTableDataFrameShapeBelow(bool? active = true)
    {
        RecordOldValueAndApply(() => {CoreConfig.TableDataFrameShapeBelow = active;});
        return this;
    }
    /// <summary>
    /// Sets the text/ASCII formatting style for outputting DataFrames.
    /// </summary>
    public PolarsConfig SetTableFormatting(TableFormatting? format=null, bool? roundedCorners = false)
    {
        RecordOldValueAndApply(() => {CoreConfig.TableFormatting = (format?.ToNative(), roundedCorners);});
        return this;
    }
    /// <summary>
    /// Hide table column data types (i64, f64, str etc.).
    /// </summary>
    public PolarsConfig SetTableHideColumnDataTypes(bool? active = true)
    {
        RecordOldValueAndApply(() => {CoreConfig.TableHideColumnDataTypes = active;});
        return this;
    }
    /// <summary>
    /// Hide table column names.
    /// </summary>
    public PolarsConfig SetTableHideColumnNames(bool? active = true)
    {
        RecordOldValueAndApply(() => {CoreConfig.TableHideColumnNames = active;});
        return this;
    }
    /// <summary>
    /// Hide the DataFrame shape information when displaying tables.
    /// </summary>
    public PolarsConfig SetTableHideDataFrameShape(bool? active = true)
    {
        RecordOldValueAndApply(() => {CoreConfig.TableHideDataFrameShape = active;});
        return this;
    }
    /// <summary>
    /// Hide the ‘—’ separator displayed between the column names and column types.
    /// </summary>
    public PolarsConfig SetTableHideDataTypeSeparator(bool? active = true)
    {
        RecordOldValueAndApply(() => {CoreConfig.TableHideDataTypeSeparator =active;});
        return this;
    }
    /// <summary>
    /// Set the maximum width of a table in characters.
    /// </summary>
    /// <param name="width">Maximum table width in characters; if n is less than 0 (eg: -1), display full width.</param>
    public PolarsConfig SetTableWidthChars(int? width)
    {
        RecordOldValueAndApply(() => {CoreConfig.TableWidthChars =width;});
        return this;
    }

}