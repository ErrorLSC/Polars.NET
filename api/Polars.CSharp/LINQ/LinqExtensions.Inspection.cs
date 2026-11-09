using Polars.NET.Core.Helpers;
using Polars.NET.Linq;
using Kaminari.Samidare;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Helpers;

namespace Polars.CSharp.Linq;

public static partial class LinqExtensions
{
    /// <summary>
    /// Returns the Polars native query execution plan, including warnings if parts of the query
    /// fallback to managed client-side execution.
    /// </summary>
    /// <param name="source">The source queryable.</param>
    /// <param name="optimized">Whether to display the Polars optimized logical plan or the raw plan.</param>
    public static string Explain<T>(this IQueryable<T> source, bool optimized = true)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (source is IQueryExplainable explainable)
        {
            return explainable.Explain(optimized);
        }

        return "[Polars.NET] Query source is not a native PolarsQueryable. In-memory execution only.";
    }

    /// <summary>
    /// Explains the intermediate representation (IR) LogicalPlan of the query pipeline.
    /// </summary>
    /// <param name="source">The source queryable.</param>
    public static string ExplainLogicalPlan<T>(this IQueryable<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (source is IQueryExplainable explainable)
        {
            return explainable.ExplainLogicalPlan();
        }

        return "[Polars.NET] Query source is not a native PolarsQueryable. Logical plan inspection unavailable.";
    }

    /// <summary>
    /// Compiles the query pipeline into a pure symbolic CompiledLogicalQuery IR.
    /// </summary>
    /// <param name="source">The source queryable.</param>
    internal static CompiledLogicalQuery? CompileLogicalQuery<T>(this IQueryable<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (source is IInternalQueryExplainable explainable)
        {
            return explainable.CompileLogicalQuery();
        }

        return null;
    }
    /// <summary>
    /// Gets or sets the default plan inspector action used when none is explicitly specified.
    /// Defaults to <see cref="Console.WriteLine(string)"/>.
    /// </summary>
    public static Action<string> DefaultInspector { get; set; } = Console.WriteLine;
    /// <summary>
    /// Inspects the query plan using the default inspector (<see cref="DefaultInspector"/>)
    /// </summary>
    /// <param name="source">The source queryable.</param>
    /// <param name="optimized">Whether to display the optimized plan. Defaults to true.</param>
    public static IQueryable<T> InspectPlan<T>(
        this IQueryable<T> source,
        bool optimized = true)
        => source.InspectPlan(DefaultInspector, optimized);

    /// <summary>
    /// Inspects the query plan at the current pipeline stage and invokes a custom inspector action,
    /// </summary>
    /// <param name="source">The source queryable.</param>
    /// <param name="inspector">Action receiving the diagnostic plan string.</param>
    /// <param name="optimized">Whether to display the optimized plan. Defaults to true.</param>
    public static IQueryable<T> InspectPlan<T>(
        this IQueryable<T> source,
        Action<string> inspector,
        bool optimized = true)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(inspector);

        inspector(TerminalDisplayHelpers.SanitizeForDisplay(source.Explain(optimized)));
        return source;
    }
}
