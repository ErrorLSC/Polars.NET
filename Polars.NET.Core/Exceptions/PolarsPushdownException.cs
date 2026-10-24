namespace Polars.NET.Core;

/// <summary>
/// Exception thrown when a LINQ expression or query stage cannot be pushed down
/// to the native Polars engine and client-side evaluation is restricted by <see cref="ClientEvaluationPolicy.Strict"/>.
/// </summary>
[Serializable]
public class PolarsPushdownException : PolarsException
{
    /// <summary>
    /// The LINQ pipeline stage where pushdown failed (e.g., "Where", "GroupBy", "Join", "FullFallback").
    /// </summary>
    public string? Stage { get; }

    /// <summary>
    /// The raw or formatted expression that could not be translated.
    /// </summary>
    public string? ExpressionText { get; }

    /// <summary>
    /// The specific method, member, or operator within the expression that was unsupported.
    /// </summary>
    public string? UnsupportedFeature { get; }

    public PolarsPushdownException(string message)
        : base(message)
    {
    }

    public PolarsPushdownException(string message, Exception inner)
        : base(message, inner)
    {
    }

    public PolarsPushdownException(
        string stage,
        string? expressionText = null,
        string? unsupportedFeature = null,
        string? customRemediation = null)
        : base(FormatMessage(stage, expressionText, unsupportedFeature, customRemediation))
    {
        Stage = stage;
        ExpressionText = expressionText;
        UnsupportedFeature = unsupportedFeature;
    }

    private static string FormatMessage(
        string stage,
        string? expressionText,
        string? unsupportedFeature,
        string? customRemediation)
    {
        var sb = new System.Text.StringBuilder();
        sb.AppendLine("[PolarsPushdownException] The query expression could not be pushed down to the native Polars engine.");
        sb.Append("  • Stage: ").AppendLine(stage);

        if (!string.IsNullOrWhiteSpace(expressionText))
            sb.Append("  • Expression: ").AppendLine(expressionText);

        if (!string.IsNullOrWhiteSpace(unsupportedFeature))
            sb.Append("  • Unsupported Feature: ").AppendLine(unsupportedFeature);

        sb.AppendLine();
        sb.AppendLine("Remediation Options:");
        if (!string.IsNullOrWhiteSpace(customRemediation))
        {
            sb.AppendLine($"  • {customRemediation}");
        }
        else
        {
            sb.AppendLine("  1. Rewrite the expression using native Polars operations or built-in functions.");
            sb.AppendLine("  2. If this is an entity projection, move it into the trailing projection (C#: '.Select(...)', F#: 'select ...').");
            sb.AppendLine("  3. To evaluate in managed memory using Polars.NET's high-performance row cursor:");
            sb.AppendLine("     • In C#: insert '.AsEnumerable()' before this operation.");
            sb.AppendLine("     • In F#: insert '.AsSeq()' (or '|> Query.asSeq' / '|> Seq.filter') before this operation.");
            sb.AppendLine("     • Or allow client evaluation globally:");
            sb.AppendLine("       - C#: Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow)");
            sb.AppendLine("       - F#: Config.clientEvaluationPolicy <- ClientEvaluationPolicy.Allow");
            sb.AppendLine("       - Env: POLARS_CLIENT_EVAL=allow");
        }

        return sb.ToString().TrimEnd();
    }
}
