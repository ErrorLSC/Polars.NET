using System.Reflection;
using Kaminari.Samidare;
using Kaminari.Samidare.Config;
using Kaminari.Samidare.IR;
using Microsoft.FSharp.Core;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Linq.Backend;

/// <summary>Polars implementation of the backend services consumed by the LINQ frontend during IR construction.</summary>
internal sealed class PolarsQueryBackend : IQueryBackend
{
    private PolarsQueryBackend()
    {
    }

    public static readonly IQueryBackend Instance = new PolarsQueryBackend();

    public Exception CreatePushdownFailure(string stage, string expressionText, string unsupportedFeature) =>
        new PolarsPushdownException(stage, expressionText, unsupportedFeature);

    public bool IsRegexPatternSupported(string pattern) => PolarsWrapper.RegexIsValid(pattern);

    public bool IsDateTextSupported(string format, string culture) => PolarsDateText.TryStrftime(format, culture) is not null;

    // The check runs as a managed callback, which DisallowUdfPushdown rules out.
    public bool SupportsAssertions => !SamidareConfig.DisallowUdfPushdown;

    // A row-indexed query read twice is how a correlated EXISTS is decorrelated: one selective outer query, whose
    // filters do the work. Polars finds this repeat itself and still pushes the filters into the scans, which it does
    // not do beneath an explicit cache.
    public bool SharesRepeatedSubplanItself(LogicalPlan subplan) => subplan is LogicalPlan.WithRowIndex;

    public bool IsEngineAssembly(Assembly assembly) =>
        assembly == typeof(PolarsPushdownException).Assembly
        || assembly.GetName().Name is "Polars.CSharp" or "Polars.FSharp";

    public FSharpOption<ILogicalTableSource>? TryBindExternalSource(object value) =>
        PolarsObjectConverter.TryBindExternalSource(value).ToOption();
}
