namespace Polars.Tpcds.Tests;

/// <summary>Every TPC-DS query runs natively on Polars and returns exactly what DuckDB returns for the reference SQL.</summary>
public class TpcdsQueryTests
{
    public static TheoryData<string> Queries => new(TpcdsCatalog.All.Select(c => c.Name));

    [Theory]
    [MemberData(nameof(Queries))]
    public void Matches_DuckDb(string name)
    {
        var query = TpcdsCatalog.Get(name);

        Assert.Contains("100% Native Pushdown", query.Explain());
        using var duckDb = TpcdsVerification.OpenDuckDb();
        Assert.Null(TpcdsVerification.FindDifference(TpcdsVerification.Rows(duckDb, query.Sql()), query.RunPolars()));
    }
}
