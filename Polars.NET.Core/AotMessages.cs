namespace Polars.NET.Core;

/// <summary>
/// Shared messages for <c>RequiresDynamicCode</c> / <c>RequiresUnreferencedCode</c> annotations,
/// so every entry point of a non-AOT feature reports the same reason.
/// </summary>
internal static class AotMessages
{
    /// <summary>The LINQ provider (IQueryable over DataFrame/LazyFrame).</summary>
    public const string LinqProvider =
        "The Polars LINQ provider translates and materializes queries with expression-tree compilation and " +
        "runtime generic instantiation over the query's types. It is not supported under trimming or Native AOT; " +
        "use the DataFrame/LazyFrame/Expr API instead.";

    /// <summary>JSON encoding of arbitrary objects (JsonNormalize default encoder).</summary>
    public const string JsonPocoFallback =
        "JSON-shaped values (dictionaries, lists, primitives, dates, JsonElement) are encoded without reflection, " +
        "but other objects (POCOs, enums, typed dictionaries, ...) fall back to reflection-based JsonSerializer. " +
        "Safe under trimming/Native AOT when the data holds only JSON-shaped values, or when a custom encoder is passed.";
}
