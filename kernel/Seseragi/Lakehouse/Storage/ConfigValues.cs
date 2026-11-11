namespace Kaminari.Seseragi.Lakehouse.Storage;

/// <summary>Configuration values read as the Rust <c>object_store</c> library reads them.</summary>
internal static class ConfigValues
{
    /// <summary>The HTTP client keys every store accepts (each store also takes them behind its own prefix).</summary>
    public static readonly string[] ClientKeys =
    [
        "allow_http", "allow_invalid_certificates", "connect_timeout", "default_content_type", "http1_only",
        "http2_only", "http2_keep_alive_interval", "http2_keep_alive_timeout", "http2_keep_alive_while_idle",
        "http2_max_frame_size", "pool_idle_timeout", "pool_max_idle_per_host", "proxy_url", "proxy_ca_certificate",
        "proxy_excludes", "randomize_addresses", "timeout", "user_agent",
    ];

    /// <summary>Client keys that change nothing for the managed clients.</summary>
    public static readonly string[] HarmlessClientKeys =
    [
        "allow_http", "user_agent", "timeout", "connect_timeout", "default_content_type", "http1_only", "http2_only",
        "http2_keep_alive_interval", "http2_keep_alive_timeout", "http2_keep_alive_while_idle", "http2_max_frame_size",
        "pool_idle_timeout", "pool_max_idle_per_host", "randomize_addresses",
    ];

    /// <summary>A boolean (<c>true/false</c>, <c>1/0</c>, <c>on/off</c>, <c>yes/no</c>, <c>y/n</c>), or null when the value is none.</summary>
    public static bool? Bool(string value) => value.ToLowerInvariant() switch
    {
        "1" or "true" or "on" or "yes" or "y" => true,
        "0" or "false" or "off" or "no" or "n" => false,
        _ => null,
    };

    public static bool IsTrue(string? value) => value is not null && Bool(value) == true;

    public static bool IsFalse(string? value) => value is not null && Bool(value) == false;
}
