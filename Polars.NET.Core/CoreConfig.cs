using System.Collections.Concurrent;
using System.Text.Json.Serialization;

namespace Polars.NET.Core;

/// <summary>
/// Config for Polars.NET
/// </summary>
internal static class CoreConfig
{
    /// <summary>
    /// Background Async Prefetch batch size 
    /// Default is 2. Can change this by GetEnvironmentVariable("POLARS_NET_PREFETCH_SIZE")
    /// </summary>
    public static int DefaultPrefetchBufferSize { get; set; } = GetDefaultPrefetchSizeFromEnv();

    private static int GetDefaultPrefetchSizeFromEnv()
    {
        var envValue = Environment.GetEnvironmentVariable("POLARS_NET_PREFETCH_SIZE");
        
        if (int.TryParse(envValue, out int size) && size > 0)
        {
            return size;
        }
        
        return 2;
    }
}

// public static class PolarsConfigManager
// {
//     // 使用线程安全的字典作为 C# 端的 State Cache
//     private static readonly ConcurrentDictionary<string, string> _stateCache = new();

//     /// <summary>
//     /// 静态构造函数：在类第一次被使用时触发。
//     /// 负责捕获程序启动时，操作系统自带的环境变量。
//     /// </summary>
//     static PolarsConfigManager()
//     {
//         foreach (var key in PolarsConfigEnv.EnvVars)
//         {
//             // 抓取进程启动时的真实环境变量
//             var val = Environment.GetEnvironmentVariable(key);
//             if (!string.IsNullOrEmpty(val))
//             {
//                 _stateCache[key] = val;
//             }
//         }
//     }

//     /// <summary>
//     /// 统一的设置入口
//     /// </summary>
//     public static void SetEnvVar(string key, string value)
//     {
//         // 1. 更新 C# 端的缓存
//         _stateCache[key] = value;
        
//         // 2. 穿透 FFI，通知 Rust 更新其内部环境
//         PolarsWrapper.SetEnvVar(key, value);
//     }

//     /// <summary>
//     /// 获取当前状态 (无需调用 FFI，直接读缓存)
//     /// </summary>
//     public static Dictionary<string, string?> GetState(bool ifSet = false, bool envOnly = false)
//     {
//         var configState = new Dictionary<string, string?>();

//         // 遍历所有已知的 Polars 配置 Key
//         foreach (var key in PolarsConfigEnv.EnvVars.OrderBy(v => v))
//         {
//             bool hasValue = _stateCache.TryGetValue(key, out var val);
            
//             if (hasValue)
//             {
//                 configState[key] = val;
//             }
//             else if (!ifSet) // 如果 ifSet=false，没设置的也要返回 null
//             {
//                 configState[key] = null;
//             }
//         }

//         if (!envOnly)
//         {
//             // 获取那些非环境变量的 Direct Vars (仍然需要 FFI)
//             // configState["set_fmt_float"] = NativeBindings.GetFloatFmt();
//             // ... 
//         }

//         return configState;
//     }
//         /// <summary>
//     /// Set the max number of rows used to print tables.
//     /// </summary>
//     /// <param name="n">Number of rows. -1 means display all.</param>
//     public static void SetTblRows(int n) => SetEnvVar("POLARS_FMT_MAX_ROWS", n.ToString());
    
//     /// <summary>
//     /// Set the max number of columns used to print tables.
//     /// </summary>
//     /// <param name="n">Number of columns. -1 means display all.</param>
//     public static void SetTblCols(int n) => SetEnvVar("POLARS_FMT_MAX_COLS", n.ToString());
    
//     /// <summary>
//     /// Enable or disable Polars verbose mode. 
//     /// Prints detailed info regarding optimizations.
//     /// </summary>
//     /// <param name="enable">True to enable, false to disable.</param>
//     public static void SetVerbose(bool enable) => SetEnvVar("POLARS_VERBOSE", enable ? "1" : "0");
//     /// <summary>
//     /// Set the max number of threads used by Polars.
//     /// </summary>
//     /// <param name="threads">Number of threads.</param>
//     public static void SetMaxThreads(int threads)
//     {
//         if (threads <= 0) throw new ArgumentOutOfRangeException(nameof(threads), "Must be greater than 0");
//         SetEnvVar("POLARS_MAX_THREADS", threads.ToString());
//     }

//     /// <summary>
//     /// Enable or disable the string limits when formatting DataFrame.
//     /// </summary>
//     /// <param name="n">Max length of characters to print. -1 means unlimited.</param>
//     public static void SetFmtStrLengths(int n) => SetEnvVar("POLARS_FMT_STR_LEN", n.ToString());
//     /// <summary>
//     /// 设置执行引擎的偏好
//     /// </summary>
//     public static void SetEngineAffinity(PlEngine engine)
//     {
//         // 将 C# 枚举映射为 Polars 底层认识的字符串
//         string engineStr = engine switch
//         {
//             PlEngine.Auto => "auto",
//             PlEngine.InMemory => "in_memory",
//             PlEngine.Streaming => "streaming",
//             PlEngine.Gpu => "gpu",
//             _ => "auto"
//         };

//         // 直接复用底层通道，穿透到 Rust 环境变量并更新 C# 缓存
//         SetEnvVar("POLARS_ENGINE_AFFINITY", engineStr);
//     }

//     /// <summary>
//     /// 获取当前的引擎偏好
//     /// </summary>
//     public static PlEngine GetEngineAffinity()
//     {
//         // 方案A：如果你用了我们之前的 Smart Cache 方案，直接读缓存或环境变量
//         string? val = _stateCache.TryGetValue("POLARS_ENGINE_AFFINITY", out var v) ? v : "auto";
        
//         // 方案B：或者你想用你刚贴出来的那个 Rust Getter FFI (CheckString 读取)
//         // string? val = GetEngineAffinityFromRust(); 
        
//         // 假设这里我们直接读系统环境变量（最简单）
//         // string val = Environment.GetEnvironmentVariable("POLARS_ENGINE_AFFINITY") ?? "auto";

//         // 将底层字符串反向映射回 C# 枚举
//         return val.ToLowerInvariant() switch
//         {
//             "in_memory" => PlEngine.InMemory,
//             "streaming" => PlEngine.Streaming,
//             "gpu" => PlEngine.Gpu,
//             "auto" => PlEngine.Auto,
//             _ => PlEngine.Auto // 默认回退
//         };
//     }
// }

// internal static class PolarsConfigEnv
// {
//     public static readonly HashSet<string> EnvVars = new(StringComparer.OrdinalIgnoreCase)
//     {
//         "POLARS_WARN_UNSTABLE",
//         "POLARS_FMT_MAX_COLS",
//         "POLARS_FMT_MAX_ROWS",
//         "POLARS_FMT_NUM_DECIMAL",
//         "POLARS_FMT_NUM_GROUP_SEPARATOR",
//         "POLARS_FMT_NUM_LEN",
//         "POLARS_FMT_STR_LEN",
//         "POLARS_FMT_TABLE_CELL_ALIGNMENT",
//         "POLARS_FMT_TABLE_CELL_LIST_LEN",
//         "POLARS_FMT_TABLE_CELL_NUMERIC_ALIGNMENT",
//         "POLARS_FMT_TABLE_DATAFRAME_SHAPE_BELOW",
//         "POLARS_FMT_TABLE_FORMATTING",
//         "POLARS_FMT_TABLE_HIDE_COLUMN_DATA_TYPES",
//         "POLARS_FMT_TABLE_HIDE_COLUMN_NAMES",
//         "POLARS_FMT_TABLE_HIDE_COLUMN_SEPARATOR",
//         "POLARS_FMT_TABLE_HIDE_DATAFRAME_SHAPE_INFORMATION",
//         "POLARS_FMT_TABLE_INLINE_COLUMN_DATA_TYPE",
//         "POLARS_FMT_TABLE_ROUNDED_CORNERS",
//         "POLARS_STREAMING_CHUNK_SIZE",
//         "POLARS_TABLE_WIDTH",
//         "POLARS_VERBOSE",
//         "POLARS_MAX_EXPR_DEPTH",
//         "POLARS_ENGINE_AFFINITY"
//     };
// }

// public class PolarsConfigParameters
// {
//     [JsonPropertyName("ascii_tables")] public bool? AsciiTables { get; set; }
//     // [JsonPropertyName("auto_structify")] public bool? AutoStructify { get; set; }
//     [JsonPropertyName("decimal_separator")] public string? DecimalSeparator { get; set; }
//     [JsonPropertyName("thousands_separator")] public string? ThousandsSeparator { get; set; }
//     [JsonPropertyName("float_precision")] public int? FloatPrecision { get; set; }
    
//     [JsonPropertyName("fmt_float")] public string? FmtFloat { get; set; } 
//     [JsonPropertyName("fmt_str_lengths")] public int? FmtStrLengths { get; set; }
//     [JsonPropertyName("fmt_table_cell_list_len")] public int? FmtTableCellListLen { get; set; }
//     [JsonPropertyName("streaming_chunk_size")] public int? StreamingChunkSize { get; set; }
//     [JsonPropertyName("tbl_cell_alignment")] public string? TblCellAlignment { get; set; }
//     [JsonPropertyName("tbl_cell_numeric_alignment")] public string? TblCellNumericAlignment { get; set; }
//     [JsonPropertyName("tbl_cols")] public int? TblCols { get; set; }
//     [JsonPropertyName("tbl_column_data_type_inline")] public bool? TblColumnDataTypeInline { get; set; }
//     [JsonPropertyName("tbl_dataframe_shape_below")] public bool? TblDataframeShapeBelow { get; set; }
//     [JsonPropertyName("tbl_formatting")] public string? TblFormatting { get; set; }
//     [JsonPropertyName("tbl_hide_column_data_types")] public bool? TblHideColumnDataTypes { get; set; }
//     [JsonPropertyName("tbl_hide_column_names")] public bool? TblHideColumnNames { get; set; }
//     [JsonPropertyName("tbl_hide_dtype_separator")] public bool? TblHideDtypeSeparator { get; set; }
//     [JsonPropertyName("tbl_hide_dataframe_shape")] public bool? TblHideDataframeShape { get; set; }
//     [JsonPropertyName("tbl_rows")] public int? TblRows { get; set; }
//     [JsonPropertyName("tbl_width_chars")] public int? TblWidthChars { get; set; }
//     [JsonPropertyName("trim_decimal_zeros")] public bool? TrimDecimalZeros { get; set; }
//     [JsonPropertyName("verbose")] public bool? Verbose { get; set; }
//     [JsonPropertyName("expr_depth_warning")] public int? ExprDepthWarning { get; set; }
//     [JsonPropertyName("engine_affinity")] public string? EngineAffinity { get; set; }
// }