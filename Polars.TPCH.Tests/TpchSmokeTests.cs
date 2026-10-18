using Polars.CSharp;
using Xunit;

namespace Polars.TPCH.Tests;

public class TpchSmokeTests
{
    [Fact]
    public void Test_AllTables_Exist_And_CanBeScanned()
    {
        Assert.True(File.Exists(TpchData.CustomerPath));
        Assert.True(File.Exists(TpchData.LineItemPath));
        Assert.True(File.Exists(TpchData.NationPath));
        Assert.True(File.Exists(TpchData.OrdersPath));
        Assert.True(File.Exists(TpchData.PartPath));
        Assert.True(File.Exists(TpchData.PartSuppPath));
        Assert.True(File.Exists(TpchData.RegionPath));
        Assert.True(File.Exists(TpchData.SupplierPath));

        using var dfCustomer = TpchData.Customer().Limit(5).Collect();
        Assert.Equal(5, dfCustomer.Height);

        using var dfLineItem = TpchData.LineItem().Limit(5).Collect();
        Assert.Equal(5, dfLineItem.Height);

        using var dfNation = TpchData.Nation().Limit(5).Collect();
        Assert.Equal(5, dfNation.Height);

        using var dfOrders = TpchData.Orders().Limit(5).Collect();
        Assert.Equal(5, dfOrders.Height);

        using var dfPart = TpchData.Part().Limit(5).Collect();
        Assert.Equal(5, dfPart.Height);

        using var dfPartSupp = TpchData.PartSupp().Limit(5).Collect();
        Assert.Equal(5, dfPartSupp.Height);

        using var dfRegion = TpchData.Region().Limit(5).Collect();
        Assert.Equal(5, dfRegion.Height);

        using var dfSupplier = TpchData.Supplier().Limit(5).Collect();
        Assert.Equal(5, dfSupplier.Height);
    }

    [Fact]
    public void Test_Simple_Aggregation_On_LineItem()
    {
        using var df = TpchData.LineItem()
            .Select(
                Pl.Col("l_quantity").Sum().Alias("sum_qty"),
                Pl.Col("l_extendedprice").Sum().Alias("sum_base_price")
            )
            .Collect();

        Assert.Equal(1, df.Height);
        Assert.Equal(2, df.Width);
    }
}
