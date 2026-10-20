namespace Polars.NET.Core;

/// <summary>
/// Defines the policy for handling operations or expressions that cannot be pushed down
/// to the native Polars engine and would otherwise require client-side (.NET managed) evaluation.
/// </summary>
public enum ClientEvaluationPolicy
{
    /// <summary>
    /// Strict mode (Default):
    /// - Allows terminal projections (.Select) to evaluate on the client via high-performance row cursors.
    /// - Throws a <see cref="PolarsPushdownException"/> if intermediate operations (e.g., Where, GroupBy, Join)
    ///   or full pipeline fallbacks cannot be pushed down to the native Polars engine.
    /// </summary>
    Strict = 0,

    /// <summary>
    /// Warning mode:
    /// Emits a diagnostic warning when pushdown fails, but allows falling back to client-side evaluation.
    /// </summary>
    Warn = 1,

    /// <summary>
    /// Allow mode:
    /// Silently allows client-side evaluation fallback without warnings or exceptions.
    /// </summary>
    Allow = 2
}
