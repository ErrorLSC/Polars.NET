namespace Polars.Tpcds;

// Result row types of the shared TPC-DS queries. Property order, names and types mirror the projection of the
// query (and the DuckDB column order), so positional comparisons such as TpcdsAssert.RowsEqual keep working.

public class Q3Row
{
    public long? d_year { get; init; }
    public long? brand_id { get; init; }
    public string? brand { get; init; }
    public decimal? sum_agg { get; init; }
}

public class Q5Row
{
    public string? channel { get; init; }
    public string? id { get; init; }
    public decimal? sales { get; init; }
    public decimal? returns_ { get; init; }
    public decimal? profit { get; init; }
}

public class Q6Row
{
    public string? state { get; init; }
    public long cnt { get; init; }
}

public class Q14Row
{
    public string? channel { get; init; }
    public long? i_brand_id { get; init; }
    public long? i_class_id { get; init; }
    public long? i_category_id { get; init; }
    public decimal? sum_sales { get; init; }
    public long sum_number_sales { get; init; }
}

public class Q16Row
{
    public long order_count { get; init; }
    public decimal? total_shipping_cost { get; init; }
    public decimal? total_net_profit { get; init; }
}

public class Q36Row
{
    public double gross_margin { get; init; }
    public string? i_category { get; init; }
    public string? i_class { get; init; }
    public int lochierarchy { get; init; }
    public long rank_within_parent { get; init; }
}

public class Q38Row
{
    public string? c_last_name { get; init; }
    public string? c_first_name { get; init; }
    public DateOnly? d_date { get; init; }
}

public class Q47Row
{
    public string? i_category { get; init; }
    public string? i_brand { get; init; }
    public string? s_store_name { get; init; }
    public string? s_company_name { get; init; }
    public long? d_year { get; init; }
    public long? d_moy { get; init; }
    public double avg_monthly_sales { get; init; }
    public decimal? sum_sales { get; init; }
    public decimal? psum { get; init; }
    public decimal? nsum { get; init; }
}

public class Q51Row
{
    public long item_sk { get; init; }
    public DateOnly? d_date { get; init; }
    public decimal? web_sales { get; init; }
    public decimal? store_sales { get; init; }
    public decimal? web_cumulative { get; init; }
    public decimal? store_cumulative { get; init; }
}

public class Q55Row
{
    public long? brand_id { get; init; }
    public string? brand { get; init; }
    public decimal? ext_price { get; init; }
}

public class Q64Row
{
    public string? product_name { get; init; }
    public string? store_name { get; init; }
    public string? store_zip { get; init; }
    public string? b_street_number { get; init; }
    public string? b_street_name { get; init; }
    public string? b_city { get; init; }
    public string? b_zip { get; init; }
    public string? c_street_number { get; init; }
    public string? c_street_name { get; init; }
    public string? c_city { get; init; }
    public string? c_zip { get; init; }
    public long? cs1syear { get; init; }
    public long cs1cnt { get; init; }
    public decimal? s11 { get; init; }
    public decimal? s21 { get; init; }
    public decimal? s31 { get; init; }
    public decimal? s12 { get; init; }
    public decimal? s22 { get; init; }
    public decimal? s32 { get; init; }
    public long? syear { get; init; }
    public long cnt { get; init; }
}

public class Q67Row
{
    public string? i_category { get; init; }
    public string? i_class { get; init; }
    public string? i_brand { get; init; }
    public string? i_product_name { get; init; }
    public long? d_year { get; init; }
    public long? d_qoy { get; init; }
    public long? d_moy { get; init; }
    public string? s_store_id { get; init; }
    public decimal sumsales { get; init; }
    public long rk { get; init; }
}

public class Q72Row
{
    public string? i_item_desc { get; init; }
    public string? w_warehouse_name { get; init; }
    public long? d_week_seq { get; init; }
    public long no_promo { get; init; }
    public long promo { get; init; }
    public long total_cnt { get; init; }
}

public class Q86Row
{
    public decimal? total_sum { get; init; }
    public string? i_category { get; init; }
    public string? i_class { get; init; }
    public int lochierarchy { get; init; }
    public long rank_within_parent { get; init; }
}

public class Q87Row
{
    public string? c_last_name { get; init; }
    public string? c_first_name { get; init; }
    public DateOnly? d_date { get; init; }
}

public class Q94Row
{
    public long order_count { get; init; }
    public decimal? total_shipping_cost { get; init; }
    public decimal? total_net_profit { get; init; }
}

public class Q98Row
{
    public string? i_item_id { get; init; }
    public string? i_item_desc { get; init; }
    public string? i_category { get; init; }
    public string? i_class { get; init; }
    public decimal? i_current_price { get; init; }
    public decimal? itemrevenue { get; init; }
    public double? revenueratio { get; init; }
}
