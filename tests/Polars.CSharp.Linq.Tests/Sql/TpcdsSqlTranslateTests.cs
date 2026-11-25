using Kaminari.Samidare.Sql;
using Polars.CSharp;
using Polars.NET.Core.Linq.Backend;
using Polars.Tpcds.Tests;

namespace Polars.CSharp.Linq.Tests.Sql;

/// <summary>
/// The SQL frontend translates DuckDB's 99 TPC-DS texts to IR plans over the TPC-DS tables (their Parquet schemas),
/// subqueries replaced by joins.
/// </summary>
[Trait("Category", "Sql")]
public class TpcdsSqlTranslateTests
{
    static readonly Lazy<ISqlCatalog> Catalog = new(() =>
    {
        var catalog = new SqlCatalog();
        foreach (var path in Directory.GetFiles(TpcdsData.DataDir, "*.parquet"))
            catalog.Register(Path.GetFileNameWithoutExtension(path), PolarsTableSources.OfLazyFrame(LazyFrame.ScanParquet(path).Handle));
        return catalog;
    });

    [Theory]
    [MemberData(nameof(TpcdsSqlParseTests.Queries), MemberType = typeof(TpcdsSqlParseTests))]
    public void Query_translates_to_an_IR_plan(int n)
    {
        var plan = SqlQueries.translate(Catalog.Value, true, TpcdsSqlParseTests.Text(n));
        Assert.NotEmpty(plan.Columns);
    }
}
