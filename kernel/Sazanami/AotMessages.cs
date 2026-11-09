namespace Kaminari.Sazanami;

/// <summary>
/// Shared messages for <c>RequiresDynamicCode</c> / <c>RequiresUnreferencedCode</c> annotations,
/// so every entry point of a non-AOT feature reports the same reason.
/// </summary>
internal static class AotMessages
{
    /// <summary>JSON encoding of arbitrary objects (JsonNormalize default encoder).</summary>
    public const string JsonPocoFallback =
        "JSON-shaped values (dictionaries, lists, primitives, dates, JsonElement) are encoded without reflection, " +
        "but other objects (POCOs, enums, typed dictionaries, ...) fall back to reflection-based JsonSerializer. " +
        "Safe under trimming/Native AOT when the data holds only JSON-shaped values, or when a custom encoder is passed.";
}
