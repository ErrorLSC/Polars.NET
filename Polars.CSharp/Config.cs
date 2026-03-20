using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// Config for Polars.NET
/// </summary>
public static class PolarsConfig
{
    /// <summary>
    /// Inject Environment var to Rust
    /// </summary>
    public static void SetEnvVar(string key, string value)
        =>PolarsWrapper.SetEnvVar(key, value);
    /// <summary>
    /// Inject Environment var to Rust by KeyValuePair
    /// </summary>
    /// <param name="variables">Collection or dictionary contains environment var keyvalue pair. </param>
    public static void SetEnvVars(IEnumerable<KeyValuePair<string, string>> variables)
    {
        ArgumentNullException.ThrowIfNull(variables);
        
        foreach (var kvp in variables)
        {
            SetEnvVar(kvp.Key, kvp.Value);
        }
    }
    /// <summary>
    /// Inject Environment var to Rust by KeyValuePair tuple
    /// </summary>
    public static void SetEnvVars(params (string Key, string Value)[] variables)
    {
        if (variables == null) return;
        
        foreach (var (key, value) in variables)
        {
            SetEnvVar(key, value);
        }
    }
}