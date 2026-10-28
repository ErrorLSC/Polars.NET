namespace Polars.Tpcds.Tests;

public class TpcdsSanityTests
{
    [Fact, Trait("TPCDS", "Sanity")]
    public void Test_Tpcds_Parquet_Files_And_Scan_Work()
    {
        Assert.True(Directory.Exists(TpcdsData.DataDir), "Data directory should exist.");

        using var storeSalesLf = TpcdsData.StoreSales();
        using var storeSalesDf = storeSalesLf.Limit(5).Collect();
        Assert.Equal(5, storeSalesDf.Height);

        using var itemLf = TpcdsData.Item();
        using var itemDf = itemLf.Limit(5).Collect();
        Assert.Equal(5, itemDf.Height);

        using var dateDimLf = TpcdsData.DateDim();
        using var dateDimDf = dateDimLf.Limit(5).Collect();
        Assert.Equal(5, dateDimDf.Height);
    }
}
