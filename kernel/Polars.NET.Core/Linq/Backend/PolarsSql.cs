using Kaminari.Samidare;
using Kaminari.Samidare.IR;
using Kaminari.Samidare.Sql;

namespace Polars.NET.Core.Linq.Backend;

/// <summary>SQL queries on Polars: the SQL frontend's plan (Samidare.Sql) optimized as a LINQ query's is, and lowered.</summary>
internal static class PolarsSql
{
    /// <summary>The optimized IR plan of a SQL query over the catalog's tables, and the sources its Scan slots read.</summary>
    public static (LogicalPlan Plan, SourceBinding[] Sources) Plan(ISqlCatalog catalog, string text)
    {
        var (plan, sources) = SqlFrontend.Plan(catalog, PolarsQueryBackend.Instance, text);
        return (plan, sources);
    }

    /// <summary>A SQL query over the catalog's tables as a LazyFrame.</summary>
    public static LazyFrameHandle LazyFrame(ISqlCatalog catalog, string text)
    {
        using var schemas = SqlFrontend.EnterSchemaCache();
        var (plan, sources) = Plan(catalog, text);
        return PolarsPhysicalLowerer.LowerPlan(sources, plan);
    }
}
