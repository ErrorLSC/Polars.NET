namespace Polars.Tpcds.Tests;

#region TPC-DS Schema Records

public record CallCenter(
    long cc_call_center_sk,
    string? cc_call_center_id,
    DateOnly? cc_rec_start_date,
    DateOnly? cc_rec_end_date,
    long? cc_closed_date_sk,
    long? cc_open_date_sk,
    string? cc_name,
    string? cc_class,
    long? cc_employees,
    long? cc_sq_ft,
    string? cc_hours,
    string? cc_manager,
    long? cc_mkt_id,
    string? cc_mkt_class,
    string? cc_mkt_desc,
    string? cc_market_manager,
    long? cc_division,
    string? cc_division_name,
    long? cc_company,
    string? cc_company_name,
    string? cc_street_number,
    string? cc_street_name,
    string? cc_street_type,
    string? cc_suite_number,
    string? cc_city,
    string? cc_county,
    string? cc_state,
    string? cc_zip,
    string? cc_country,
    decimal? cc_gmt_offset,
    decimal? cc_tax_percentage
);

public record CatalogPage(
    long cp_catalog_page_sk,
    string? cp_catalog_page_id,
    long? cp_start_date_sk,
    long? cp_end_date_sk,
    string? cp_department,
    long? cp_catalog_number,
    long? cp_catalog_page_number,
    string? cp_description,
    string? cp_type
);

public record CatalogReturns(
    long? cr_returned_date_sk,
    long? cr_returned_time_sk,
    long cr_item_sk,
    long? cr_refunded_customer_sk,
    long? cr_refunded_cdemo_sk,
    long? cr_refunded_hdemo_sk,
    long? cr_refunded_addr_sk,
    long? cr_returning_customer_sk,
    long? cr_returning_cdemo_sk,
    long? cr_returning_hdemo_sk,
    long? cr_returning_addr_sk,
    long? cr_call_center_sk,
    long? cr_catalog_page_sk,
    long? cr_ship_mode_sk,
    long? cr_warehouse_sk,
    long? cr_reason_sk,
    long cr_order_number,
    long? cr_return_quantity,
    decimal? cr_return_amount,
    decimal? cr_return_tax,
    decimal? cr_return_amt_inc_tax,
    decimal? cr_fee,
    decimal? cr_return_ship_cost,
    decimal? cr_refunded_cash,
    decimal? cr_reversed_charge,
    decimal? cr_store_credit,
    decimal? cr_net_loss
);

public record CatalogSales(
    long? cs_sold_date_sk,
    long? cs_sold_time_sk,
    long? cs_ship_date_sk,
    long? cs_bill_customer_sk,
    long? cs_bill_cdemo_sk,
    long? cs_bill_hdemo_sk,
    long? cs_bill_addr_sk,
    long? cs_ship_customer_sk,
    long? cs_ship_cdemo_sk,
    long? cs_ship_hdemo_sk,
    long? cs_ship_addr_sk,
    long? cs_call_center_sk,
    long? cs_catalog_page_sk,
    long? cs_ship_mode_sk,
    long? cs_warehouse_sk,
    long cs_item_sk,
    long? cs_promo_sk,
    long cs_order_number,
    long? cs_quantity,
    decimal? cs_wholesale_cost,
    decimal? cs_list_price,
    decimal? cs_sales_price,
    decimal? cs_ext_discount_amt,
    decimal? cs_ext_sales_price,
    decimal? cs_ext_wholesale_cost,
    decimal? cs_ext_list_price,
    decimal? cs_ext_tax,
    decimal? cs_coupon_amt,
    decimal? cs_ext_ship_cost,
    decimal? cs_net_paid,
    decimal? cs_net_paid_inc_tax,
    decimal? cs_net_paid_inc_ship,
    decimal? cs_net_paid_inc_ship_tax,
    decimal? cs_net_profit
);

public record Customer(
    long c_customer_sk,
    string? c_customer_id,
    long? c_current_cdemo_sk,
    long? c_current_hdemo_sk,
    long? c_current_addr_sk,
    long? c_first_shipto_date_sk,
    long? c_first_sales_date_sk,
    string? c_salutation,
    string? c_first_name,
    string? c_last_name,
    string? c_preferred_cust_flag,
    long? c_birth_day,
    long? c_birth_month,
    long? c_birth_year,
    string? c_birth_country,
    string? c_login,
    string? c_email_address,
    int? c_last_review_date_sk
);

public record CustomerAddress(
    long ca_address_sk,
    string? ca_address_id,
    string? ca_street_number,
    string? ca_street_name,
    string? ca_street_type,
    string? ca_suite_number,
    string? ca_city,
    string? ca_county,
    string? ca_state,
    string? ca_zip,
    string? ca_country,
    decimal? ca_gmt_offset,
    string? ca_location_type
);

public record CustomerDemographics(
    long cd_demo_sk,
    string? cd_gender,
    string? cd_marital_status,
    string? cd_education_status,
    long? cd_purchase_estimate,
    string? cd_credit_rating,
    long? cd_dep_count,
    long? cd_dep_employed_count,
    int? cd_dep_college_count
);

public record DateDim(
    long d_date_sk,
    string? d_date_id,
    DateOnly? d_date,
    long? d_month_seq,
    long? d_week_seq,
    long? d_quarter_seq,
    long? d_year,
    long? d_dow,
    long? d_moy,
    long? d_dom,
    long? d_qoy,
    long? d_fy_year,
    long? d_fy_quarter_seq,
    long? d_fy_week_seq,
    string? d_day_name,
    string? d_quarter_name,
    string? d_holiday,
    string? d_weekend,
    string? d_following_holiday,
    long? d_first_dom,
    long? d_last_dom,
    long? d_same_day_ly,
    long? d_same_day_lq,
    string? d_current_day,
    string? d_current_week,
    string? d_current_month,
    string? d_current_quarter,
    string? d_current_year
);

public record HouseholdDemographics(
    long hd_demo_sk,
    long? hd_income_band_sk,
    string? hd_buy_potential,
    long? hd_dep_count,
    int? hd_vehicle_count
);

public record IncomeBand(
    long ib_income_band_sk,
    long? ib_lower_bound,
    int? ib_upper_bound
);

public record Inventory(
    long inv_date_sk,
    long inv_item_sk,
    long inv_warehouse_sk,
    int? inv_quantity_on_hand
);

public record Item(
    long i_item_sk,
    string? i_item_id,
    DateOnly? i_rec_start_date,
    DateOnly? i_rec_end_date,
    string? i_item_desc,
    decimal? i_current_price,
    decimal? i_wholesale_cost,
    long? i_brand_id,
    string? i_brand,
    long? i_class_id,
    string? i_class,
    long? i_category_id,
    string? i_category,
    long? i_manufact_id,
    string? i_manufact,
    string? i_size,
    string? i_formulation,
    string? i_color,
    string? i_units,
    string? i_container,
    long? i_manager_id,
    string? i_product_name
);

public record Promotion(
    long p_promo_sk,
    string? p_promo_id,
    long? p_start_date_sk,
    long? p_end_date_sk,
    long? p_item_sk,
    decimal? p_cost,
    long? p_response_target,
    string? p_promo_name,
    string? p_channel_dmail,
    string? p_channel_email,
    string? p_channel_catalog,
    string? p_channel_tv,
    string? p_channel_radio,
    string? p_channel_press,
    string? p_channel_event,
    string? p_channel_demo,
    string? p_channel_details,
    string? p_purpose,
    string? p_discount_active
);

public record Reason(
    long r_reason_sk,
    string? r_reason_id,
    string? r_reason_desc
);

public record ShipMode(
    long sm_ship_mode_sk,
    string? sm_ship_mode_id,
    string? sm_type,
    string? sm_code,
    string? sm_carrier,
    string? sm_contract
);

public record Store(
    long s_store_sk,
    string? s_store_id,
    DateOnly? s_rec_start_date,
    DateOnly? s_rec_end_date,
    long? s_closed_date_sk,
    string? s_store_name,
    long? s_number_employees,
    long? s_floor_space,
    string? s_hours,
    string? s_manager,
    long? s_market_id,
    string? s_geography_class,
    string? s_market_desc,
    string? s_market_manager,
    long? s_division_id,
    string? s_division_name,
    long? s_company_id,
    string? s_company_name,
    string? s_street_number,
    string? s_street_name,
    string? s_street_type,
    string? s_suite_number,
    string? s_city,
    string? s_county,
    string? s_state,
    string? s_zip,
    string? s_country,
    decimal? s_gmt_offset,
    decimal? s_tax_percentage
);

public record StoreReturns(
    long? sr_returned_date_sk,
    long? sr_return_time_sk,
    long sr_item_sk,
    long? sr_customer_sk,
    long? sr_cdemo_sk,
    long? sr_hdemo_sk,
    long? sr_addr_sk,
    long? sr_store_sk,
    long? sr_reason_sk,
    long sr_ticket_number,
    long? sr_return_quantity,
    decimal? sr_return_amt,
    decimal? sr_return_tax,
    decimal? sr_return_amt_inc_tax,
    decimal? sr_fee,
    decimal? sr_return_ship_cost,
    decimal? sr_refunded_cash,
    decimal? sr_reversed_charge,
    decimal? sr_store_credit,
    decimal? sr_net_loss
);

public record StoreSales(
    long? ss_sold_date_sk,
    long? ss_sold_time_sk,
    long ss_item_sk,
    long? ss_customer_sk,
    long? ss_cdemo_sk,
    long? ss_hdemo_sk,
    long? ss_addr_sk,
    long? ss_store_sk,
    long? ss_promo_sk,
    long ss_ticket_number,
    long? ss_quantity,
    decimal? ss_wholesale_cost,
    decimal? ss_list_price,
    decimal? ss_sales_price,
    decimal? ss_ext_discount_amt,
    decimal? ss_ext_sales_price,
    decimal? ss_ext_wholesale_cost,
    decimal? ss_ext_list_price,
    decimal? ss_ext_tax,
    decimal? ss_coupon_amt,
    decimal? ss_net_paid,
    decimal? ss_net_paid_inc_tax,
    decimal? ss_net_profit
);

public record TimeDim(
    long t_time_sk,
    string? t_time_id,
    long? t_time,
    long? t_hour,
    long? t_minute,
    long? t_second,
    string? t_am_pm,
    string? t_shift,
    string? t_sub_shift,
    string? t_meal_time
);

public record Warehouse(
    long w_warehouse_sk,
    string? w_warehouse_id,
    string? w_warehouse_name,
    long? w_warehouse_sq_ft,
    string? w_street_number,
    string? w_street_name,
    string? w_street_type,
    string? w_suite_number,
    string? w_city,
    string? w_county,
    string? w_state,
    string? w_zip,
    string? w_country,
    decimal? w_gmt_offset
);

public record WebPage(
    long wp_web_page_sk,
    string? wp_web_page_id,
    DateOnly? wp_rec_start_date,
    DateOnly? wp_rec_end_date,
    long? wp_creation_date_sk,
    long? wp_access_date_sk,
    string? wp_autogen_flag,
    long? wp_customer_sk,
    string? wp_url,
    string? wp_type,
    long? wp_char_count,
    long? wp_link_count,
    long? wp_image_count,
    int? wp_max_ad_count
);

public record WebReturns(
    long? wr_returned_date_sk,
    long? wr_returned_time_sk,
    long wr_item_sk,
    long? wr_refunded_customer_sk,
    long? wr_refunded_cdemo_sk,
    long? wr_refunded_hdemo_sk,
    long? wr_refunded_addr_sk,
    long? wr_returning_customer_sk,
    long? wr_returning_cdemo_sk,
    long? wr_returning_hdemo_sk,
    long? wr_returning_addr_sk,
    long? wr_web_page_sk,
    long? wr_reason_sk,
    long wr_order_number,
    long? wr_return_quantity,
    decimal? wr_return_amt,
    decimal? wr_return_tax,
    decimal? wr_return_amt_inc_tax,
    decimal? wr_fee,
    decimal? wr_return_ship_cost,
    decimal? wr_refunded_cash,
    decimal? wr_reversed_charge,
    decimal? wr_account_credit,
    decimal? wr_net_loss
);

public record WebSales(
    long? ws_sold_date_sk,
    long? ws_sold_time_sk,
    long? ws_ship_date_sk,
    long ws_item_sk,
    long? ws_bill_customer_sk,
    long? ws_bill_cdemo_sk,
    long? ws_bill_hdemo_sk,
    long? ws_bill_addr_sk,
    long? ws_ship_customer_sk,
    long? ws_ship_cdemo_sk,
    long? ws_ship_hdemo_sk,
    long? ws_ship_addr_sk,
    long? ws_web_page_sk,
    long? ws_web_site_sk,
    long? ws_ship_mode_sk,
    long? ws_warehouse_sk,
    long? ws_promo_sk,
    long ws_order_number,
    long? ws_quantity,
    decimal? ws_wholesale_cost,
    decimal? ws_list_price,
    decimal? ws_sales_price,
    decimal? ws_ext_discount_amt,
    decimal? ws_ext_sales_price,
    decimal? ws_ext_wholesale_cost,
    decimal? ws_ext_list_price,
    decimal? ws_ext_tax,
    decimal? ws_coupon_amt,
    decimal? ws_ext_ship_cost,
    decimal? ws_net_paid,
    decimal? ws_net_paid_inc_tax,
    decimal? ws_net_paid_inc_ship,
    decimal? ws_net_paid_inc_ship_tax,
    decimal? ws_net_profit
);

public record WebSite(
    long web_site_sk,
    string? web_site_id,
    DateOnly? web_rec_start_date,
    DateOnly? web_rec_end_date,
    string? web_name,
    long? web_open_date_sk,
    long? web_close_date_sk,
    string? web_class,
    string? web_manager,
    long? web_mkt_id,
    string? web_mkt_class,
    string? web_mkt_desc,
    string? web_market_manager,
    long? web_company_id,
    string? web_company_name,
    string? web_street_number,
    string? web_street_name,
    string? web_street_type,
    string? web_suite_number,
    string? web_city,
    string? web_county,
    string? web_state,
    string? web_zip,
    string? web_country,
    decimal? web_gmt_offset,
    decimal? web_tax_percentage
);

#endregion
