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