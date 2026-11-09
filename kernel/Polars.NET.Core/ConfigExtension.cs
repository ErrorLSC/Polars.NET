namespace Polars.NET.Core.Config;

/// <summary>
/// Settings that a project above this one contributes to the saved and restored configuration state.
/// </summary>
/// <param name="EnvKeys">The environment variables of these settings.</param>
/// <param name="DirectRules">The settings saved by value, with their getters.</param>
/// <param name="ApplyDirect">Applies a saved value (lower-case key, null value when saved as null).</param>
/// <param name="AfterEnvironmentRestored">Runs once a saved state's environment variables are restored.</param>
/// <param name="RestoreDefaults">Restores the default values, after the environment variables are cleared.</param>
internal sealed record ConfigExtension(
    IReadOnlyList<string> EnvKeys,
    IReadOnlyList<(string DirectKey, Func<string?> ValueGetter)> DirectRules,
    Action<string, string?> ApplyDirect,
    Action AfterEnvironmentRestored,
    Action RestoreDefaults);
