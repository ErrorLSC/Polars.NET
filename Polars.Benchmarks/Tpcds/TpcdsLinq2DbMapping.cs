using System.Text.RegularExpressions;
using LinqToDB.Mapping;
using Polars.Tpcds;

namespace Polars.Benchmarks.Tpcds;

/// <summary>
/// Maps the shared TPC-DS schema records (Polars.Tpcds) to the DuckDB views of
/// <see cref="TpcdsBenchEnvironment.OpenDuckDb"/>: table name = the record name in snake case, columns by property name.
/// </summary>
internal static partial class TpcdsLinq2DbMapping
{
    public static readonly MappingSchema Schema = Build();

    [GeneratedRegex("(?<=[a-z])(?=[A-Z])")]
    private static partial Regex WordBoundary();

    private static MappingSchema Build()
    {
        var schema = new MappingSchema();
        var builder = new FluentMappingBuilder(schema);
        Map<CallCenter>(builder);
        Map<CatalogPage>(builder);
        Map<CatalogReturns>(builder);
        Map<CatalogSales>(builder);
        Map<Customer>(builder);
        Map<CustomerAddress>(builder);
        Map<CustomerDemographics>(builder);
        Map<DateDim>(builder);
        Map<HouseholdDemographics>(builder);
        Map<IncomeBand>(builder);
        Map<Inventory>(builder);
        Map<Item>(builder);
        Map<Promotion>(builder);
        Map<Reason>(builder);
        Map<ShipMode>(builder);
        Map<Store>(builder);
        Map<StoreReturns>(builder);
        Map<StoreSales>(builder);
        Map<TimeDim>(builder);
        Map<Warehouse>(builder);
        Map<WebPage>(builder);
        Map<WebReturns>(builder);
        Map<WebSales>(builder);
        Map<WebSite>(builder);
        builder.Build();
        return schema;
    }

    private static void Map<T>(FluentMappingBuilder builder) =>
        builder.Entity<T>().HasTableName(WordBoundary().Replace(typeof(T).Name, "_").ToLowerInvariant());
}
