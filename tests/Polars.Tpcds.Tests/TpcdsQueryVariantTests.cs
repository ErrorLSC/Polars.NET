namespace Polars.Tpcds.Tests;

/// <summary>Equivalent ways of writing a TPC-DS query that the provider must translate to the same answer.</summary>
public class TpcdsQueryVariantTests
{
    [Fact]
    public void Q41_correlated_Any()
    {
        var itemQuery = TpcdsData.ItemQuery();

        var q41_any = itemQuery
            .Where(i1 => i1.i_manufact_id >= 738 && i1.i_manufact_id <= 738 + 40 &&
                         itemQuery.Any(i2 => i2.i_manufact == i1.i_manufact &&
                            ((i2.i_category == "Women" && (i2.i_color == "powder" || i2.i_color == "khaki") && (i2.i_units == "Ounce" || i2.i_units == "Oz") && (i2.i_size == "medium" || i2.i_size == "extra large")) ||
                             (i2.i_category == "Women" && (i2.i_color == "brown" || i2.i_color == "honeydew") && (i2.i_units == "Bunch" || i2.i_units == "Ton") && (i2.i_size == "N/A" || i2.i_size == "small")) ||
                             (i2.i_category == "Men" && (i2.i_color == "floral" || i2.i_color == "deep") && (i2.i_units == "N/A" || i2.i_units == "Dozen") && (i2.i_size == "petite" || i2.i_size == "petite")) ||
                             (i2.i_category == "Men" && (i2.i_color == "light" || i2.i_color == "cornflower") && (i2.i_units == "Box" || i2.i_units == "Pound") && (i2.i_size == "medium" || i2.i_size == "extra large")) ||
                             (i2.i_category == "Women" && (i2.i_color == "midnight" || i2.i_color == "snow") && (i2.i_units == "Pallet" || i2.i_units == "Gross") && (i2.i_size == "medium" || i2.i_size == "extra large")) ||
                             (i2.i_category == "Women" && (i2.i_color == "cyan" || i2.i_color == "papaya") && (i2.i_units == "Cup" || i2.i_units == "Dram") && (i2.i_size == "N/A" || i2.i_size == "small")) ||
                             (i2.i_category == "Men" && (i2.i_color == "orange" || i2.i_color == "frosted") && (i2.i_units == "Each" || i2.i_units == "Tbl") && (i2.i_size == "petite" || i2.i_size == "petite")) ||
                             (i2.i_category == "Men" && (i2.i_color == "forest" || i2.i_color == "ghost") && (i2.i_units == "Lb" || i2.i_units == "Bundle") && (i2.i_size == "medium" || i2.i_size == "extra large"))
                            )))
            .Select(i1 => i1.i_product_name)
            .Distinct()
            .OrderBy(p => p)
            .Take(100);

        Assert.Contains("100% Native Pushdown", q41_any.Explain());
        Assert.Equal(TpcdsCatalog.Get("Q41").RunPolars(), q41_any.ToList().Cast<object?>());
    }

    [Fact]
    public void Q41_correlated_Count()
    {
        var itemQuery = TpcdsData.ItemQuery();

        var q41_count = itemQuery
            .Where(i1 => i1.i_manufact_id >= 738 && i1.i_manufact_id <= 738 + 40 &&
                         itemQuery.Count(i2 => i2.i_manufact == i1.i_manufact &&
                            ((i2.i_category == "Women" && (i2.i_color == "powder" || i2.i_color == "khaki") && (i2.i_units == "Ounce" || i2.i_units == "Oz") && (i2.i_size == "medium" || i2.i_size == "extra large")) ||
                             (i2.i_category == "Women" && (i2.i_color == "brown" || i2.i_color == "honeydew") && (i2.i_units == "Bunch" || i2.i_units == "Ton") && (i2.i_size == "N/A" || i2.i_size == "small")) ||
                             (i2.i_category == "Men" && (i2.i_color == "floral" || i2.i_color == "deep") && (i2.i_units == "N/A" || i2.i_units == "Dozen") && (i2.i_size == "petite" || i2.i_size == "petite")) ||
                             (i2.i_category == "Men" && (i2.i_color == "light" || i2.i_color == "cornflower") && (i2.i_units == "Box" || i2.i_units == "Pound") && (i2.i_size == "medium" || i2.i_size == "extra large")) ||
                             (i2.i_category == "Women" && (i2.i_color == "midnight" || i2.i_color == "snow") && (i2.i_units == "Pallet" || i2.i_units == "Gross") && (i2.i_size == "medium" || i2.i_size == "extra large")) ||
                             (i2.i_category == "Women" && (i2.i_color == "cyan" || i2.i_color == "papaya") && (i2.i_units == "Cup" || i2.i_units == "Dram") && (i2.i_size == "N/A" || i2.i_size == "small")) ||
                             (i2.i_category == "Men" && (i2.i_color == "orange" || i2.i_color == "frosted") && (i2.i_units == "Each" || i2.i_units == "Tbl") && (i2.i_size == "petite" || i2.i_size == "petite")) ||
                             (i2.i_category == "Men" && (i2.i_color == "forest" || i2.i_color == "ghost") && (i2.i_units == "Lb" || i2.i_units == "Bundle") && (i2.i_size == "medium" || i2.i_size == "extra large"))
                            )) > 0)
            .Select(i1 => i1.i_product_name)
            .Distinct()
            .OrderBy(p => p)
            .Take(100);

        Assert.Contains("100% Native Pushdown", q41_count.Explain());
        Assert.Equal(TpcdsCatalog.Get("Q41").RunPolars(), q41_count.ToList().Cast<object?>());
    }
}
