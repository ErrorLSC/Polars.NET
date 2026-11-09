using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace Kaminari.Sazanami.Helpers;

/// <summary>
/// Late-bound bridge to LINQPad's <c>LINQPad.Util.RawHtml</c>, used by the <c>ToDump()</c> rendering hooks
/// of DataFrame, LazyFrame, Schema and Expr (C# and F#) without a compile-time dependency on LINQPad.
/// </summary>
internal static class LinqPadInterop
{
    /// <summary>
    /// Wraps <paramref name="html"/> in LINQPad's RawHtml object so Dump() renders it as HTML.
    /// Returns null when LINQPad is not loaded or the call fails; callers then fall back to their plain rendering.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "LINQPad hook only. LINQPad loads assemblies dynamically and never runs trimmed or Native AOT apps; " +
                        "in a trimmed app LINQPad is absent, the lookup returns null and the caller falls back to plain rendering.")]
    [UnconditionalSuppressMessage("Trimming", "IL2075",
        Justification = "LINQPad hook only; see IL2026 justification. RawHtml lives in LINQPad's own (never trimmed) assembly.")]
    public static object? TryWrapRawHtml(string html)
    {
        try
        {
            var utilType = Type.GetType("LINQPad.Util, LINQPad")
                        ?? Type.GetType("LINQPad.Util, LINQPad.Runtime")
                        ?? AppDomain.CurrentDomain.GetAssemblies()
                            .FirstOrDefault(a => a.GetName().Name?.StartsWith("LINQPad", StringComparison.Ordinal) is true)
                            ?.GetType("LINQPad.Util");

            var rawHtmlMethod = utilType?.GetMethod("RawHtml", BindingFlags.Public | BindingFlags.Static, [typeof(string)]);
            if (rawHtmlMethod is null) return null;

            return rawHtmlMethod.Invoke(null, [html]) ?? html;
        }
        catch
        {
            return null;
        }
    }
}
