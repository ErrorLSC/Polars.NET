using Polars.NET.Core.Native;

namespace Polars.NET.Core;

/// <summary>
/// Config for Polars.NET
/// </summary>
public static class PolarsNetConfig
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
    /// <summary>
    /// 强行向 Rust 底层注入环境变量配置
    /// </summary>
    public static void Set(string key, string value)
    {
        NativeBindings.pl_set_env_var(key, value);
    }

    /// <summary>
    /// 快捷开启：将 DuckDB 吐出的 MonthDayNano 间隔强行解析为 Struct
    /// (专治 .NET TimeSpan / Duration 在鸭子肚子里的消化不良)
    /// </summary>
    public static void EnableIntervalAsStruct()
    {
        Set("POLARS_IMPORT_INTERVAL_AS_STRUCT", "1");
    }
}