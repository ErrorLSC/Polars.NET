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
    public const string JsonPocoFallback = Kaminari.Sazanami.AotMessages.JsonPocoFallback;
}
