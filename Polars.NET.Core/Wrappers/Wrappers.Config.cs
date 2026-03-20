using Polars.NET.Core.Native;

namespace Polars.NET.Core;

/// <summary>
/// Config for Polars.NET
/// </summary>
public static partial class PolarsWrapper
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
    /// Inject Environment var to Rust
    /// </summary>
    public static void SetEnvVar(string key, string value)
    {
        NativeBindings.pl_set_env_var(key, value);
    }
}