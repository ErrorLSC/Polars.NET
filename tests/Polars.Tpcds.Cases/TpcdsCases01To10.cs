namespace Polars.Tpcds;

public static partial class TpcdsCases
{
    private static readonly string[] Q8TargetZips = [
        "24128", "76232", "65084", "87816", "83926", "77556", "20548", "26231",
        "43848", "15126", "91137", "61265", "98294", "25782", "17920", "18426",
        "98235", "40081", "84093", "28577", "55565", "17183", "54601", "67897",
        "22752", "86284", "18376", "38607", "45200", "21756", "29741", "96765",
        "23932", "89360", "29839", "25989", "28898", "91068", "72550", "10390",
        "18845", "47770", "82636", "41367", "76638", "86198", "81312", "37126",
        "39192", "88424", "72175", "81426", "53672", "10445", "42666", "66864",
        "66708", "41248", "48583", "82276", "18842", "78890", "49448", "14089",
        "38122", "34425", "79077", "19849", "43285", "39861", "66162", "77610",
        "13695", "99543", "83444", "83041", "12305", "57665", "68341", "25003",
        "57834", "62878", "49130", "81096", "18840", "27700", "23470", "50412",
        "21195", "16021", "76107", "71954", "68309", "18119", "98359", "64544",
        "10336", "86379", "27068", "39736", "98569", "28915", "24206", "56529",
        "57647", "54917", "42961", "91110", "63981", "14922", "36420", "23006",
        "67467", "32754", "30903", "20260", "31671", "51798", "72325", "85816",
        "68621", "13955", "36446", "41766", "68806", "16725", "15146", "22744",
        "35850", "88086", "51649", "18270", "52867", "39972", "96976", "63792",
        "11376", "94898", "13595", "10516", "90225", "58943", "39371", "94945",
        "28587", "96576", "57855", "28488", "26105", "83933", "25858", "34322",
        "44438", "73171", "30122", "34102", "22685", "71256", "78451", "54364",
        "13354", "45375", "40558", "56458", "28286", "45266", "47305", "69399",
        "83921", "26233", "11101", "15371", "69913", "35942", "15882", "25631",
        "24610", "44165", "99076", "33786", "70738", "26653", "14328", "72305",
        "62496", "22152", "10144", "64147", "48425", "14663", "21076", "18799",
        "30450", "63089", "81019", "68893", "24996", "51200", "51211", "45692",
        "92712", "70466", "79994", "22437", "25280", "38935", "71791", "73134",
        "56571", "14060", "19505", "72425", "56575", "74351", "68786", "51650",
        "20004", "18383", "76614", "11634", "18906", "15765", "41368", "73241",
        "76698", "78567", "97189", "28545", "76231", "75691", "22246", "51061",
        "90578", "56691", "68014", "51103", "94167", "57047", "14867", "73520",
        "15734", "63435", "25733", "35474", "24676", "94627", "53535", "17879",
        "15559", "53268", "59166", "11928", "59402", "33282", "45721", "43933",
        "68101", "33515", "36634", "71286", "19736", "58058", "55253", "67473",
        "41918", "19515", "36495", "19430", "22351", "77191", "91393", "49156",
        "50298", "87501", "18652", "53179", "18767", "63193", "23968", "65164",
        "68880", "21286", "72823", "58470", "67301", "13394", "31016", "70372",
        "67030", "40604", "24317", "45748", "39127", "26065", "77721", "31029",
        "31880", "60576", "24671", "45549", "13376", "50016", "33123", "19769",
        "22927", "97789", "46081", "72151", "15723", "46136", "51949", "68100",
        "96888", "64528", "14171", "79777", "28709", "11489", "25103", "32213",
        "78668", "22245", "15798", "27156", "37930", "62971", "21337", "51622",
        "67853", "10567", "38415", "15455", "58263", "42029", "60279", "37125",
        "56240", "88190", "50308", "26859", "64457", "89091", "82136", "62377",
        "36233", "63837", "58078", "17043", "30010", "60099", "28810", "98025",
        "29178", "87343", "73273", "30469", "64034", "39516", "86057", "21309",
        "90257", "67875", "40162", "11356", "73650", "61810", "72013", "30431",
        "22461", "19512", "13375", "55307", "30625", "83849", "68908", "26689",
        "96451", "38193", "46820", "88885", "84935", "69035", "83144", "47537",
        "56616", "94983", "48033", "69952", "25486", "61547", "27385", "61860",
        "58048", "56910", "16807", "17871", "35258", "31387", "35458", "35576"
    ];

    private static readonly string[] Q10TargetCounties = [
        "Rush County", "Toole County", "Jefferson County", "Dona Ana County", "La Porte County"
    ];

    /*
     * TPC-DS Query 2
     *
     * WITH wscs AS
     *   (SELECT sold_date_sk,
     *           sales_price
     *    FROM
     *   (SELECT ws_sold_date_sk sold_date_sk,
     *                ws_ext_sales_price sales_price
     *         FROM web_sales
     *         UNION ALL SELECT cs_sold_date_sk sold_date_sk,
     *                          cs_ext_sales_price sales_price
     *         FROM catalog_sales) sq1),
     *        wswscs AS
     *     (SELECT d_week_seq,
     *             sum(CASE
     *                     WHEN (d_day_name='Sunday') THEN sales_price
     *                     ELSE NULL
     *                 END) sun_sales,
     *             sum(CASE
     *                     WHEN (d_day_name='Monday') THEN sales_price
     *                     ELSE NULL
     *                 END) mon_sales,
     *             sum(CASE
     *                     WHEN (d_day_name='Tuesday') THEN sales_price
     *                     ELSE NULL
     *                 END) tue_sales,
     *             sum(CASE
     *                     WHEN (d_day_name='Wednesday') THEN sales_price
     *                     ELSE NULL
     *                 END) wed_sales,
     *             sum(CASE
     *                     WHEN (d_day_name='Thursday') THEN sales_price
     *                     ELSE NULL
     *                 END) thu_sales,
     *             sum(CASE
     *                     WHEN (d_day_name='Friday') THEN sales_price
     *                     ELSE NULL
     *                 END) fri_sales,
     *             sum(CASE
     *                     WHEN (d_day_name='Saturday') THEN sales_price
     *                     ELSE NULL
     *                 END) sat_sales
     *      FROM wscs,
     *           date_dim
     *      WHERE d_date_sk = sold_date_sk
     *      GROUP BY d_week_seq)
     *   SELECT d_week_seq1,
     *          round(sun_sales1/sun_sales2, 2) r1,
     *          round(mon_sales1/mon_sales2, 2) r2,
     *          round(tue_sales1/tue_sales2, 2) r3,
     *          round(wed_sales1/wed_sales2, 2) r4,
     *          round(thu_sales1/thu_sales2, 2) r5,
     *          round(fri_sales1/fri_sales2, 2) r6,
     *          round(sat_sales1/sat_sales2, 2) r7
     *   FROM
     *     (SELECT wswscs.d_week_seq d_week_seq1,
     *             sun_sales sun_sales1,
     *             mon_sales mon_sales1,
     *             tue_sales tue_sales1,
     *             wed_sales wed_sales1,
     *             thu_sales thu_sales1,
     *             fri_sales fri_sales1,
     *             sat_sales sat_sales1
     *      FROM wswscs,
     *           date_dim
     *      WHERE date_dim.d_week_seq = wswscs.d_week_seq
     *        AND d_year = 2001) y,
     *     (SELECT wswscs.d_week_seq d_week_seq2,
     *             sun_sales sun_sales2,
     *             mon_sales mon_sales2,
     *             tue_sales tue_sales2,
     *             wed_sales wed_sales2,
     *             thu_sales thu_sales2,
     *             fri_sales fri_sales2,
     *             sat_sales sat_sales2
     *      FROM wswscs,
     *           date_dim
     *      WHERE date_dim.d_week_seq = wswscs.d_week_seq
     *        AND d_year = 2001+1) z
     *   WHERE d_week_seq1 = d_week_seq2-53
     *   ORDER BY d_week_seq1 NULLS FIRST;
     */
    public static IQueryable<object> Q2()
    {
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        // 1. wscs: UNION ALL
        var ws = webSalesQuery.Select(w => new { sold_date_sk = w.ws_sold_date_sk, sales_price = w.ws_ext_sales_price });
        var cs = catalogSalesQuery.Select(c => new { sold_date_sk = c.cs_sold_date_sk, sales_price = c.cs_ext_sales_price });
        var wscs = ws.Concat(cs);

        // 2. wswscs: group by d_week_seq
        var wswscs = from s in wscs
                     join d in dateDimQuery on s.sold_date_sk equals d.d_date_sk
                     group new { s, d } by d.d_week_seq into g
                     select new
                     {
                         d_week_seq = g.Key,
                         sun_sales = g.Sum(x => x.d.d_day_name == "Sunday" ? x.s.sales_price : null),
                         mon_sales = g.Sum(x => x.d.d_day_name == "Monday" ? x.s.sales_price : null),
                         tue_sales = g.Sum(x => x.d.d_day_name == "Tuesday" ? x.s.sales_price : null),
                         wed_sales = g.Sum(x => x.d.d_day_name == "Wednesday" ? x.s.sales_price : null),
                         thu_sales = g.Sum(x => x.d.d_day_name == "Thursday" ? x.s.sales_price : null),
                         fri_sales = g.Sum(x => x.d.d_day_name == "Friday" ? x.s.sales_price : null),
                         sat_sales = g.Sum(x => x.d.d_day_name == "Saturday" ? x.s.sales_price : null),
                     };

        // 3. y (d_year = 2001) and z (d_year = 2002)
        var y = from w in wswscs
                join d in dateDimQuery.Where(d => d.d_year == 2001) on w.d_week_seq equals d.d_week_seq
                select new
                {
                    d_week_seq1 = w.d_week_seq,
                    sun_sales1 = w.sun_sales,
                    mon_sales1 = w.mon_sales,
                    tue_sales1 = w.tue_sales,
                    wed_sales1 = w.wed_sales,
                    thu_sales1 = w.thu_sales,
                    fri_sales1 = w.fri_sales,
                    sat_sales1 = w.sat_sales,
                };

        var z = from w in wswscs
                join d in dateDimQuery.Where(d => d.d_year == 2002) on w.d_week_seq equals d.d_week_seq
                select new
                {
                    d_week_seq2 = w.d_week_seq,
                    sun_sales2 = w.sun_sales,
                    mon_sales2 = w.mon_sales,
                    tue_sales2 = w.tue_sales,
                    wed_sales2 = w.wed_sales,
                    thu_sales2 = w.thu_sales,
                    fri_sales2 = w.fri_sales,
                    sat_sales2 = w.sat_sales,
                };

        // 4. Join y and z on d_week_seq1 = d_week_seq2 - 53
        var q2 = from yItem in y
                 join zItem in z on yItem.d_week_seq1 equals zItem.d_week_seq2 - 53
                 orderby yItem.d_week_seq1
                 select new
                 {
                     yItem.d_week_seq1,
                     r1 = (yItem.sun_sales1 == null || zItem.sun_sales2 == null || zItem.sun_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.sun_sales1 / (double)zItem.sun_sales2, 2),
                     r2 = (yItem.mon_sales1 == null || zItem.mon_sales2 == null || zItem.mon_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.mon_sales1 / (double)zItem.mon_sales2, 2),
                     r3 = (yItem.tue_sales1 == null || zItem.tue_sales2 == null || zItem.tue_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.tue_sales1 / (double)zItem.tue_sales2, 2),
                     r4 = (yItem.wed_sales1 == null || zItem.wed_sales2 == null || zItem.wed_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.wed_sales1 / (double)zItem.wed_sales2, 2),
                     r5 = (yItem.thu_sales1 == null || zItem.thu_sales2 == null || zItem.thu_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.thu_sales1 / (double)zItem.thu_sales2, 2),
                     r6 = (yItem.fri_sales1 == null || zItem.fri_sales2 == null || zItem.fri_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.fri_sales1 / (double)zItem.fri_sales2, 2),
                     r7 = (yItem.sat_sales1 == null || zItem.sat_sales2 == null || zItem.sat_sales2 == 0m)
                          ? (double?)null
                          : Math.Round((double)yItem.sat_sales1 / (double)zItem.sat_sales2, 2),
                 };

        return q2;
    }

    public static string Q2Sql() =>
        $@"
            WITH wscs AS
              (SELECT sold_date_sk,
                      sales_price
               FROM
            (SELECT ws_sold_date_sk sold_date_sk,
                         ws_ext_sales_price sales_price
                  FROM read_parquet('{TpcdsData.WebSalesPath}')
                  UNION ALL SELECT cs_sold_date_sk sold_date_sk,
                                   cs_ext_sales_price sales_price
                  FROM read_parquet('{TpcdsData.CatalogSalesPath}')) sq1),
                 wswscs AS
              (SELECT d_week_seq,
                      sum(CASE
                              WHEN (d_day_name='Sunday') THEN sales_price
                              ELSE NULL
                          END) sun_sales,
                      sum(CASE
                              WHEN (d_day_name='Monday') THEN sales_price
                              ELSE NULL
                          END) mon_sales,
                      sum(CASE
                              WHEN (d_day_name='Tuesday') THEN sales_price
                              ELSE NULL
                          END) tue_sales,
                      sum(CASE
                              WHEN (d_day_name='Wednesday') THEN sales_price
                              ELSE NULL
                          END) wed_sales,
                      sum(CASE
                              WHEN (d_day_name='Thursday') THEN sales_price
                              ELSE NULL
                          END) thu_sales,
                      sum(CASE
                              WHEN (d_day_name='Friday') THEN sales_price
                              ELSE NULL
                          END) fri_sales,
                      sum(CASE
                              WHEN (d_day_name='Saturday') THEN sales_price
                              ELSE NULL
                          END) sat_sales
               FROM wscs,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE d_date_sk = sold_date_sk
               GROUP BY d_week_seq)
            SELECT d_week_seq1,
                   round(sun_sales1/sun_sales2, 2) r1,
                   round(mon_sales1/mon_sales2, 2) r2,
                   round(tue_sales1/tue_sales2, 2) r3,
                   round(wed_sales1/wed_sales2, 2) r4,
                   round(thu_sales1/thu_sales2, 2) r5,
                   round(fri_sales1/fri_sales2, 2) r6,
                   round(sat_sales1/sat_sales2, 2) r7
            FROM
              (SELECT wswscs.d_week_seq d_week_seq1,
                      sun_sales sun_sales1,
                      mon_sales mon_sales1,
                      tue_sales tue_sales1,
                      wed_sales wed_sales1,
                      thu_sales thu_sales1,
                      fri_sales fri_sales1,
                      sat_sales sat_sales1
               FROM wswscs,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE date_dim.d_week_seq = wswscs.d_week_seq
                 AND d_year = 2001) y,
              (SELECT wswscs.d_week_seq d_week_seq2,
                      sun_sales sun_sales2,
                      mon_sales mon_sales2,
                      tue_sales tue_sales2,
                      wed_sales wed_sales2,
                      thu_sales thu_sales2,
                      fri_sales fri_sales2,
                      sat_sales sat_sales2
               FROM wswscs,
                    read_parquet('{TpcdsData.DateDimPath}') date_dim
               WHERE date_dim.d_week_seq = wswscs.d_week_seq
                 AND d_year = 2001+1) z
            WHERE d_week_seq1 = d_week_seq2-53
            ORDER BY d_week_seq1 NULLS FIRST;
        ";

    /*
     * TPC-DS Query 4
     *
     * WITH year_total AS
     *   (SELECT c_customer_id customer_id,
     *           c_first_name customer_first_name,
     *           c_last_name customer_last_name,
     *           c_preferred_cust_flag customer_preferred_cust_flag,
     *           c_birth_country customer_birth_country,
     *           c_login customer_login,
     *           c_email_address customer_email_address,
     *           d_year dyear,
     *           sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,
     *           's' sale_type
     *    FROM customer,
     *         store_sales,
     *         date_dim
     *    WHERE c_customer_sk = ss_customer_sk
     *      AND ss_sold_date_sk = d_date_sk
     *    GROUP BY c_customer_id,
     *             c_first_name,
     *             c_last_name,
     *             c_preferred_cust_flag,
     *             c_birth_country,
     *             c_login,
     *             c_email_address,
     *             d_year
     *    UNION ALL SELECT c_customer_id customer_id,
     *                     c_first_name customer_first_name,
     *                     c_last_name customer_last_name,
     *                     c_preferred_cust_flag customer_preferred_cust_flag,
     *                     c_birth_country customer_birth_country,
     *                     c_login customer_login,
     *                     c_email_address customer_email_address,
     *                     d_year dyear,
     *                     sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2)) year_total,
     *                     'c' sale_type
     *    FROM customer,
     *         catalog_sales,
     *         date_dim
     *    WHERE c_customer_sk = cs_bill_customer_sk
     *      AND cs_sold_date_sk = d_date_sk
     *    GROUP BY c_customer_id,
     *             c_first_name,
     *             c_last_name,
     *             c_preferred_cust_flag,
     *             c_birth_country,
     *             c_login,
     *             c_email_address,
     *             d_year
     *    UNION ALL SELECT c_customer_id customer_id,
     *                     c_first_name customer_first_name,
     *                     c_last_name customer_last_name,
     *                     c_preferred_cust_flag customer_preferred_cust_flag,
     *                     c_birth_country customer_birth_country,
     *                     c_login customer_login,
     *                     c_email_address customer_email_address,
     *                     d_year dyear,
     *                     sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2)) year_total,
     *                     'w' sale_type
     *    FROM customer,
     *         web_sales,
     *         date_dim
     *    WHERE c_customer_sk = ws_bill_customer_sk
     *      AND ws_sold_date_sk = d_date_sk
     *    GROUP BY c_customer_id,
     *             c_first_name,
     *             c_last_name,
     *             c_preferred_cust_flag,
     *             c_birth_country,
     *             c_login,
     *             c_email_address,
     *             d_year)
     * SELECT t_s_secyear.customer_id,
     *        t_s_secyear.customer_first_name,
     *        t_s_secyear.customer_last_name,
     *        t_s_secyear.customer_preferred_cust_flag
     * FROM year_total t_s_firstyear,
     *      year_total t_s_secyear,
     *      year_total t_c_firstyear,
     *      year_total t_c_secyear,
     *      year_total t_w_firstyear,
     *      year_total t_w_secyear
     * WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
     *   AND t_s_firstyear.customer_id = t_c_secyear.customer_id
     *   AND t_s_firstyear.customer_id = t_c_firstyear.customer_id
     *   AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
     *   AND t_s_firstyear.customer_id = t_w_secyear.customer_id
     *   AND t_s_firstyear.sale_type = 's'
     *   AND t_c_firstyear.sale_type = 'c'
     *   AND t_w_firstyear.sale_type = 'w'
     *   AND t_s_secyear.sale_type = 's'
     *   AND t_c_secyear.sale_type = 'c'
     *   AND t_w_secyear.sale_type = 'w'
     *   AND t_s_firstyear.dyear = 2001
     *   AND t_s_secyear.dyear = 2001+1
     *   AND t_c_firstyear.dyear = 2001
     *   AND t_c_secyear.dyear = 2001+1
     *   AND t_w_firstyear.dyear = 2001
     *   AND t_w_secyear.dyear = 2001+1
     *   AND t_s_firstyear.year_total > 0
     *   AND t_c_firstyear.year_total > 0
     *   AND t_w_firstyear.year_total > 0
     *   AND CASE
     *           WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
     *           ELSE NULL
     *       END > CASE
     *                 WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
     *                 ELSE NULL
     *             END
     *   AND CASE
     *           WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
     *           ELSE NULL
     *       END > CASE
     *                 WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
     *                 ELSE NULL
     *             END
     * ORDER BY t_s_secyear.customer_id NULLS FIRST,
     *          t_s_secyear.customer_first_name NULLS FIRST,
     *          t_s_secyear.customer_last_name NULLS FIRST,
     *          t_s_secyear.customer_preferred_cust_flag NULLS FIRST
     * LIMIT 100;
     */
    public static IQueryable<object> Q4()
    {
        var customerQuery = TpcdsData.CustomerQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        // 1. Channel streams grouped by customer & year
        var storeYear = from c in customerQuery
                        join ss in storeSalesQuery on c.c_customer_sk equals ss.ss_customer_sk
                        join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                        where d.d_year == 2001 || d.d_year == 2002
                        group new { c, ss, d } by new
                        {
                            c.c_customer_id,
                            c.c_first_name,
                            c.c_last_name,
                            c.c_preferred_cust_flag,
                            d.d_year
                        } into g
                        select new
                        {
                            customer_id = g.Key.c_customer_id,
                            customer_first_name = g.Key.c_first_name,
                            customer_last_name = g.Key.c_last_name,
                            customer_preferred_cust_flag = g.Key.c_preferred_cust_flag,
                            dyear = g.Key.d_year,
                            year_total = g.Sum(x => (x.ss.ss_ext_list_price - x.ss.ss_ext_wholesale_cost - x.ss.ss_ext_discount_amt + x.ss.ss_ext_sales_price) / 2m),
                        };

        var catalogYear = from c in customerQuery
                          join cs in catalogSalesQuery on c.c_customer_sk equals cs.cs_bill_customer_sk
                          join d in dateDimQuery on cs.cs_sold_date_sk equals d.d_date_sk
                          where d.d_year == 2001 || d.d_year == 2002
                          group new { c, cs, d } by new { c.c_customer_id, d.d_year } into g
                          select new
                          {
                              customer_id = g.Key.c_customer_id,
                              dyear = g.Key.d_year,
                              year_total = g.Sum(x => (x.cs.cs_ext_list_price - x.cs.cs_ext_wholesale_cost - x.cs.cs_ext_discount_amt + x.cs.cs_ext_sales_price) / 2m),
                          };

        var webYear = from c in customerQuery
                      join ws in webSalesQuery on c.c_customer_sk equals ws.ws_bill_customer_sk
                      join d in dateDimQuery on ws.ws_sold_date_sk equals d.d_date_sk
                      where d.d_year == 2001 || d.d_year == 2002
                      group new { c, ws, d } by new { c.c_customer_id, d.d_year } into g
                      select new
                      {
                          customer_id = g.Key.c_customer_id,
                          dyear = g.Key.d_year,
                          year_total = g.Sum(x => (x.ws.ws_ext_list_price - x.ws.ws_ext_wholesale_cost - x.ws.ws_ext_discount_amt + x.ws.ws_ext_sales_price) / 2m),
                      };

        // 2. Filter year and positive sales
        var ts1 = storeYear.Where(x => x.dyear == 2001 && x.year_total > 0m);
        var ts2 = storeYear.Where(x => x.dyear == 2002);
        var tc1 = catalogYear.Where(x => x.dyear == 2001 && x.year_total > 0m);
        var tc2 = catalogYear.Where(x => x.dyear == 2002);
        var tw1 = webYear.Where(x => x.dyear == 2001 && x.year_total > 0m);
        var tw2 = webYear.Where(x => x.dyear == 2002);

        // 3. Multi-channel customer self-join and relative growth filter
        var q4 = from s1 in ts1
                 join s2 in ts2 on s1.customer_id equals s2.customer_id
                 join c1 in tc1 on s1.customer_id equals c1.customer_id
                 join c2 in tc2 on s1.customer_id equals c2.customer_id
                 join w1 in tw1 on s1.customer_id equals w1.customer_id
                 join w2 in tw2 on s1.customer_id equals w2.customer_id
                 where (double)c2.year_total! / (double)c1.year_total! > (double)s2.year_total! / (double)s1.year_total!
                    && (double)c2.year_total! / (double)c1.year_total! > (double)w2.year_total! / (double)w1.year_total!
                 orderby s2.customer_id, s2.customer_first_name, s2.customer_last_name, s2.customer_preferred_cust_flag
                 select new
                 {
                     s2.customer_id,
                     s2.customer_first_name,
                     s2.customer_last_name,
                     s2.customer_preferred_cust_flag
                 };

        var limitedQ4 = q4.Take(100);

        return limitedQ4;
    }

    public static string Q4Sql() =>
        $@"
            WITH year_total AS
              (SELECT c_customer_id customer_id,
                      c_first_name customer_first_name,
                      c_last_name customer_last_name,
                      c_preferred_cust_flag customer_preferred_cust_flag,
                      c_birth_country customer_birth_country,
                      c_login customer_login,
                      c_email_address customer_email_address,
                      d_year dyear,
                      sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,
                      's' sale_type
               FROM read_parquet('{TpcdsData.CustomerPath}'),
                    read_parquet('{TpcdsData.StoreSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}')
               WHERE c_customer_sk = ss_customer_sk
                 AND ss_sold_date_sk = d_date_sk
               GROUP BY c_customer_id,
                        c_first_name,
                        c_last_name,
                        c_preferred_cust_flag,
                        c_birth_country,
                        c_login,
                        c_email_address,
                        d_year
               UNION ALL SELECT c_customer_id customer_id,
                                c_first_name customer_first_name,
                                c_last_name customer_last_name,
                                c_preferred_cust_flag customer_preferred_cust_flag,
                                c_birth_country customer_birth_country,
                                c_login customer_login,
                                c_email_address customer_email_address,
                                d_year dyear,
                                sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2)) year_total,
                                'c' sale_type
               FROM read_parquet('{TpcdsData.CustomerPath}'),
                    read_parquet('{TpcdsData.CatalogSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}')
               WHERE c_customer_sk = cs_bill_customer_sk
                 AND cs_sold_date_sk = d_date_sk
               GROUP BY c_customer_id,
                        c_first_name,
                        c_last_name,
                        c_preferred_cust_flag,
                        c_birth_country,
                        c_login,
                        c_email_address,
                        d_year
               UNION ALL SELECT c_customer_id customer_id,
                                c_first_name customer_first_name,
                                c_last_name customer_last_name,
                                c_preferred_cust_flag customer_preferred_cust_flag,
                                c_birth_country customer_birth_country,
                                c_login customer_login,
                                c_email_address customer_email_address,
                                d_year dyear,
                                sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2)) year_total,
                                'w' sale_type
               FROM read_parquet('{TpcdsData.CustomerPath}'),
                    read_parquet('{TpcdsData.WebSalesPath}'),
                    read_parquet('{TpcdsData.DateDimPath}')
               WHERE c_customer_sk = ws_bill_customer_sk
                 AND ws_sold_date_sk = d_date_sk
               GROUP BY c_customer_id,
                        c_first_name,
                        c_last_name,
                        c_preferred_cust_flag,
                        c_birth_country,
                        c_login,
                        c_email_address,
                        d_year)
            SELECT t_s_secyear.customer_id,
                   t_s_secyear.customer_first_name,
                   t_s_secyear.customer_last_name,
                   t_s_secyear.customer_preferred_cust_flag
            FROM year_total t_s_firstyear,
                 year_total t_s_secyear,
                 year_total t_c_firstyear,
                 year_total t_c_secyear,
                 year_total t_w_firstyear,
                 year_total t_w_secyear
            WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
              AND t_s_firstyear.customer_id = t_c_secyear.customer_id
              AND t_s_firstyear.customer_id = t_c_firstyear.customer_id
              AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
              AND t_s_firstyear.customer_id = t_w_secyear.customer_id
              AND t_s_firstyear.sale_type = 's'
              AND t_c_firstyear.sale_type = 'c'
              AND t_w_firstyear.sale_type = 'w'
              AND t_s_secyear.sale_type = 's'
              AND t_c_secyear.sale_type = 'c'
              AND t_w_secyear.sale_type = 'w'
              AND t_s_firstyear.dyear = 2001
              AND t_s_secyear.dyear = 2001+1
              AND t_c_firstyear.dyear = 2001
              AND t_c_secyear.dyear = 2001+1
              AND t_w_firstyear.dyear = 2001
              AND t_w_secyear.dyear = 2001+1
              AND t_s_firstyear.year_total > 0
              AND t_c_firstyear.year_total > 0
              AND t_w_firstyear.year_total > 0
              AND CASE
                      WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
                      ELSE NULL
                  END > CASE
                            WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
                            ELSE NULL
                        END
              AND CASE
                      WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
                      ELSE NULL
                  END > CASE
                            WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
                            ELSE NULL
                        END
            ORDER BY t_s_secyear.customer_id NULLS FIRST,
                     t_s_secyear.customer_first_name NULLS FIRST,
                     t_s_secyear.customer_last_name NULLS FIRST,
                     t_s_secyear.customer_preferred_cust_flag NULLS FIRST
            LIMIT 100;
        ";

    /*
     * TPC-DS Query 7
     *
     * SELECT i_item_id,
     *        avg(ss_quantity) agg1,
     *        avg(ss_list_price) agg2,
     *        avg(ss_coupon_amt) agg3,
     *        avg(ss_sales_price) agg4
     * FROM store_sales,
     *      customer_demographics,
     *      date_dim,
     *      item,
     *      promotion
     * WHERE ss_sold_date_sk = d_date_sk
     *   AND ss_item_sk = i_item_sk
     *   AND ss_cdemo_sk = cd_demo_sk
     *   AND ss_promo_sk = p_promo_sk
     *   AND cd_gender = 'M'
     *   AND cd_marital_status = 'S'
     *   AND cd_education_status = 'College'
     *   AND (p_channel_email = 'N'
     *        OR p_channel_event = 'N')
     *   AND d_year = 2000
     * GROUP BY i_item_id
     * ORDER BY i_item_id
     * LIMIT 100;
     */
    public static IQueryable<object> Q7()
    {

        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var customerDemographicsQuery = TpcdsData.CustomerDemographicsQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();
        var itemQuery = TpcdsData.ItemQuery();
        var promotionQuery = TpcdsData.PromotionQuery();

        var filteredCd = customerDemographicsQuery.Where(cd => cd.cd_gender == "M"
                                                            && cd.cd_marital_status == "S"
                                                            && cd.cd_education_status == "College");

        var filteredPromo = promotionQuery.Where(p => p.p_channel_email == "N" || p.p_channel_event == "N");

        var filteredDates = dateDimQuery.Where(d => d.d_year == 2000);

        var q7 = (from ss in storeSalesQuery
                  join d in filteredDates on ss.ss_sold_date_sk equals d.d_date_sk
                  join i in itemQuery on ss.ss_item_sk equals i.i_item_sk
                  join cd in filteredCd on ss.ss_cdemo_sk equals cd.cd_demo_sk
                  join p in filteredPromo on ss.ss_promo_sk equals p.p_promo_sk
                  group ss by i.i_item_id into g
                  select new
                  {
                      i_item_id = g.Key,
                      agg1 = g.Average(x => (double?)x.ss_quantity),
                      agg2 = g.Average(x => (double?)x.ss_list_price),
                      agg3 = g.Average(x => (double?)x.ss_coupon_amt),
                      agg4 = g.Average(x => (double?)x.ss_sales_price),
                  } into r
                  orderby r.i_item_id
                  select r).Take(100);

        return q7;
    }

    public static string Q7Sql() =>
        $@"
            SELECT i_item_id,
                   avg(ss_quantity) agg1,
                   avg(ss_list_price) agg2,
                   avg(ss_coupon_amt) agg3,
                   avg(ss_sales_price) agg4
            FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                 read_parquet('{TpcdsData.CustomerDemographicsPath}'),
                 read_parquet('{TpcdsData.DateDimPath}'),
                 read_parquet('{TpcdsData.ItemPath}'),
                 read_parquet('{TpcdsData.PromotionPath}')
            WHERE ss_sold_date_sk = d_date_sk
              AND ss_item_sk = i_item_sk
              AND ss_cdemo_sk = cd_demo_sk
              AND ss_promo_sk = p_promo_sk
              AND cd_gender = 'M'
              AND cd_marital_status = 'S'
              AND cd_education_status = 'College'
              AND (p_channel_email = 'N'
                   OR p_channel_event = 'N')
              AND d_year = 2000
            GROUP BY i_item_id
            ORDER BY i_item_id
            LIMIT 100;
        ";

    /*
     * TPC-DS Query 8
     *
     * SELECT s_store_name,
     *        sum(ss_net_profit)
     * FROM store_sales,
     *      date_dim,
     *      store,
     *   (SELECT ca_zip
     *    FROM
     *      (SELECT SUBSTRING(ca_zip, 1, 5) ca_zip
     *       FROM customer_address
     *       WHERE SUBSTRING(ca_zip, 1, 5) IN (<400 zip codes>)
     *       INTERSECT
     *       SELECT ca_zip
     *       FROM
     *         (SELECT SUBSTRING(ca_zip, 1, 5) ca_zip,
     *                 count(*) cnt
     *          FROM customer_address,
     *               customer
     *          WHERE ca_address_sk = c_current_addr_sk
     *            AND c_preferred_cust_flag='Y'
     *          GROUP BY ca_zip
     *          HAVING count(*) > 10)A1)A2) V1
     * WHERE ss_store_sk = s_store_sk
     *   AND ss_sold_date_sk = d_date_sk
     *   AND d_qoy = 2
     *   AND d_year = 1998
     *   AND (SUBSTRING(s_zip, 1, 2) = SUBSTRING(V1.ca_zip, 1, 2))
     * GROUP BY s_store_name
     * ORDER BY s_store_name
     * LIMIT 100;
     */
    public static IQueryable<object> Q8()
    {
        IQueryable<dynamic> BuildQuery(string[] targetZips, long minCount)
        {
            var storeSalesQuery = TpcdsData.StoreSalesQuery();
            var dateDimQuery = TpcdsData.DateDimQuery();
            var storeQuery = TpcdsData.StoreQuery();
            var addressQuery1 = TpcdsData.CustomerAddressQuery();
            var addressQuery2 = TpcdsData.CustomerAddressQuery();
            var customerQuery = TpcdsData.CustomerQuery();

            // V1: ca_zip set intersection
            var z1 = (from ca in addressQuery1
                      where ca.ca_zip != null && targetZips.Contains(ca.ca_zip.Substring(0, 5))
                      select new { ca_zip = ca.ca_zip!.Substring(0, 5) }).Distinct();

            var z2 = from ca in addressQuery2
                     join c in customerQuery on ca.ca_address_sk equals c.c_current_addr_sk
                     where c.c_preferred_cust_flag == "Y" && ca.ca_zip != null
                     group ca by ca.ca_zip!.Substring(0, 5) into g
                     where g.Count() > minCount
                     select new { ca_zip = g.Key };

            var v1 = z1.Intersect(z2);

            var filteredDates = dateDimQuery.Where(d => d.d_qoy == 2 && d.d_year == 1998);

            var query = (from ss in storeSalesQuery
                         join d in filteredDates on ss.ss_sold_date_sk equals d.d_date_sk
                         join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                         join v in v1 on s.s_zip!.Substring(0, 2) equals v.ca_zip.Substring(0, 2)
                         group ss by s.s_store_name into g
                         select new
                         {
                             s_store_name = g.Key,
                             net_profit = g.Sum(x => x.ss_net_profit)
                         } into r
                         orderby r.s_store_name
                         select r).Take(100);

            return query;
        }

        var standardQuery = BuildQuery(Q8TargetZips, 10);

        return standardQuery;
    }

    public static string Q8Sql()
    {
        var zipList = string.Join(", ", Q8TargetZips.Select(z => $"'{z}'"));
        return $@"
            SELECT s_store_name,
                   sum(ss_net_profit)
            FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                 read_parquet('{TpcdsData.DateDimPath}'),
                 read_parquet('{TpcdsData.StorePath}'),
              (SELECT ca_zip
               FROM
                 (SELECT SUBSTRING(ca_zip, 1, 5) ca_zip
                  FROM read_parquet('{TpcdsData.CustomerAddressPath}')
                  WHERE SUBSTRING(ca_zip, 1, 5) IN ({zipList})
                  INTERSECT
                  SELECT ca_zip
                  FROM
                    (SELECT SUBSTRING(ca_zip, 1, 5) ca_zip,
                            count(*) cnt
                     FROM read_parquet('{TpcdsData.CustomerAddressPath}'),
                          read_parquet('{TpcdsData.CustomerPath}')
                     WHERE ca_address_sk = c_current_addr_sk
                       AND c_preferred_cust_flag='Y'
                     GROUP BY ca_zip
                     HAVING count(*) > 10)A1)A2) V1
            WHERE ss_store_sk = s_store_sk
              AND ss_sold_date_sk = d_date_sk
              AND d_qoy = 2
              AND d_year = 1998
              AND (SUBSTRING(s_zip, 1, 2) = SUBSTRING(V1.ca_zip, 1, 2))
            GROUP BY s_store_name
            ORDER BY s_store_name NULLS FIRST
            LIMIT 100;
        ";
    }

    /*
     * TPC-DS Query 9
     *
     * SELECT CASE
     *            WHEN
     *                   (SELECT count(*)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 1 AND 20) > 74129 THEN
     *                   (SELECT avg(ss_ext_discount_amt)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 1 AND 20)
     *            ELSE
     *                   (SELECT avg(ss_net_paid)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 1 AND 20)
     *        END bucket1,
     *        CASE
     *            WHEN
     *                   (SELECT count(*)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 21 AND 40) > 122840 THEN
     *                   (SELECT avg(ss_ext_discount_amt)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 21 AND 40)
     *            ELSE
     *                   (SELECT avg(ss_net_paid)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 21 AND 40)
     *        END bucket2,
     *        CASE
     *            WHEN
     *                   (SELECT count(*)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 41 AND 60) > 56580 THEN
     *                   (SELECT avg(ss_ext_discount_amt)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 41 AND 60)
     *            ELSE
     *                   (SELECT avg(ss_net_paid)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 41 AND 60)
     *        END bucket3,
     *        CASE
     *            WHEN
     *                   (SELECT count(*)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 61 AND 80) > 10097 THEN
     *                   (SELECT avg(ss_ext_discount_amt)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 61 AND 80)
     *            ELSE
     *                   (SELECT avg(ss_net_paid)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 61 AND 80)
     *        END bucket4,
     *        CASE
     *            WHEN
     *                   (SELECT count(*)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 81 AND 100) > 165306 THEN
     *                   (SELECT avg(ss_ext_discount_amt)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 81 AND 100)
     *            ELSE
     *                   (SELECT avg(ss_net_paid)
     *                    FROM store_sales
     *                    WHERE ss_quantity BETWEEN 81 AND 100)
     *        END bucket5
     * FROM reason
     * WHERE r_reason_sk = 1;
     */
    public static IQueryable<object> Q9()
    {
        var reasonQuery = TpcdsData.ReasonQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();

        var q9 = from r in reasonQuery.Where(r => r.r_reason_sk == 1)
                 select new
                 {
                     bucket1 = (storeSalesQuery.Where(ss => ss.ss_quantity >= 1 && ss.ss_quantity <= 20).LongCount() > 74129)
                         ? storeSalesQuery.Where(ss => ss.ss_quantity >= 1 && ss.ss_quantity <= 20).Average(ss => (double?)ss.ss_ext_discount_amt)
                         : storeSalesQuery.Where(ss => ss.ss_quantity >= 1 && ss.ss_quantity <= 20).Average(ss => (double?)ss.ss_net_paid),
                     bucket2 = (storeSalesQuery.Where(ss => ss.ss_quantity >= 21 && ss.ss_quantity <= 40).LongCount() > 122840)
                         ? storeSalesQuery.Where(ss => ss.ss_quantity >= 21 && ss.ss_quantity <= 40).Average(ss => (double?)ss.ss_ext_discount_amt)
                         : storeSalesQuery.Where(ss => ss.ss_quantity >= 21 && ss.ss_quantity <= 40).Average(ss => (double?)ss.ss_net_paid),
                     bucket3 = (storeSalesQuery.Where(ss => ss.ss_quantity >= 41 && ss.ss_quantity <= 60).LongCount() > 56580)
                         ? storeSalesQuery.Where(ss => ss.ss_quantity >= 41 && ss.ss_quantity <= 60).Average(ss => (double?)ss.ss_ext_discount_amt)
                         : storeSalesQuery.Where(ss => ss.ss_quantity >= 41 && ss.ss_quantity <= 60).Average(ss => (double?)ss.ss_net_paid),
                     bucket4 = (storeSalesQuery.Where(ss => ss.ss_quantity >= 61 && ss.ss_quantity <= 80).LongCount() > 10097)
                         ? storeSalesQuery.Where(ss => ss.ss_quantity >= 61 && ss.ss_quantity <= 80).Average(ss => (double?)ss.ss_ext_discount_amt)
                         : storeSalesQuery.Where(ss => ss.ss_quantity >= 61 && ss.ss_quantity <= 80).Average(ss => (double?)ss.ss_net_paid),
                     bucket5 = (storeSalesQuery.Where(ss => ss.ss_quantity >= 81 && ss.ss_quantity <= 100).LongCount() > 165306)
                         ? storeSalesQuery.Where(ss => ss.ss_quantity >= 81 && ss.ss_quantity <= 100).Average(ss => (double?)ss.ss_ext_discount_amt)
                         : storeSalesQuery.Where(ss => ss.ss_quantity >= 81 && ss.ss_quantity <= 100).Average(ss => (double?)ss.ss_net_paid),
                 };

        return q9;
    }

    public static string Q9Sql() =>
        $@"
            SELECT CASE
                       WHEN
                              (SELECT count(*)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 1 AND 20) > 74129 THEN
                              (SELECT avg(ss_ext_discount_amt)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 1 AND 20)
                       ELSE
                              (SELECT avg(ss_net_paid)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 1 AND 20)
                   END bucket1,
                   CASE
                       WHEN
                              (SELECT count(*)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 21 AND 40) > 122840 THEN
                              (SELECT avg(ss_ext_discount_amt)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 21 AND 40)
                       ELSE
                              (SELECT avg(ss_net_paid)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 21 AND 40)
                   END bucket2,
                   CASE
                       WHEN
                              (SELECT count(*)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 41 AND 60) > 56580 THEN
                              (SELECT avg(ss_ext_discount_amt)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 41 AND 60)
                       ELSE
                              (SELECT avg(ss_net_paid)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 41 AND 60)
                   END bucket3,
                   CASE
                       WHEN
                              (SELECT count(*)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 61 AND 80) > 10097 THEN
                              (SELECT avg(ss_ext_discount_amt)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 61 AND 80)
                       ELSE
                              (SELECT avg(ss_net_paid)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 61 AND 80)
                   END bucket4,
                   CASE
                       WHEN
                              (SELECT count(*)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 81 AND 100) > 165306 THEN
                              (SELECT avg(ss_ext_discount_amt)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 81 AND 100)
                       ELSE
                              (SELECT avg(ss_net_paid)
                               FROM read_parquet('{TpcdsData.StoreSalesPath}')
                               WHERE ss_quantity BETWEEN 81 AND 100)
                   END bucket5
            FROM read_parquet('{TpcdsData.ReasonPath}')
            WHERE r_reason_sk = 1;
        ";

    /*
     * TPC-DS Query 10
     *
     * SELECT cd_gender,
     *        cd_marital_status,
     *        cd_education_status,
     *        count(*) cnt1,
     *        cd_purchase_estimate,
     *        count(*) cnt2,
     *        cd_credit_rating,
     *        count(*) cnt3,
     *        cd_dep_count,
     *        count(*) cnt4,
     *        cd_dep_employed_count,
     *        count(*) cnt5,
     *        cd_dep_college_count,
     *        count(*) cnt6
     * FROM customer c,
     *      customer_address ca,
     *      customer_demographics
     * WHERE c.c_current_addr_sk = ca.ca_address_sk
     *   AND ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County')
     *   AND cd_demo_sk = c.c_current_cdemo_sk
     *   AND EXISTS
     *     (SELECT *
     *      FROM store_sales,
     *           date_dim
     *      WHERE c.c_customer_sk = ss_customer_sk
     *        AND ss_sold_date_sk = d_date_sk
     *        AND d_year = 2002
     *        AND d_moy BETWEEN 1 AND 1+3)
     *   AND (EXISTS
     *          (SELECT *
     *           FROM web_sales,
     *                date_dim
     *           WHERE c.c_customer_sk = ws_bill_customer_sk
     *             AND ws_sold_date_sk = d_date_sk
     *             AND d_year = 2002
     *             AND d_moy BETWEEN 1 AND 1+3)
     *        OR EXISTS
     *          (SELECT *
     *           FROM catalog_sales,
     *                date_dim
     *           WHERE c.c_customer_sk = cs_ship_customer_sk
     *             AND cs_sold_date_sk = d_date_sk
     *             AND d_year = 2002
     *             AND d_moy BETWEEN 1 AND 1+3))
     * GROUP BY cd_gender,
     *          cd_marital_status,
     *          cd_education_status,
     *          cd_purchase_estimate,
     *          cd_credit_rating,
     *          cd_dep_count,
     *          cd_dep_employed_count,
     *          cd_dep_college_count
     * ORDER BY cd_gender,
     *          cd_marital_status,
     *          cd_education_status,
     *          cd_purchase_estimate,
     *          cd_credit_rating,
     *          cd_dep_count,
     *          cd_dep_employed_count,
     *          cd_dep_college_count
     * LIMIT 100;
     */
    public static IQueryable<object> Q10()
    {
        var customerQuery = TpcdsData.CustomerQuery();
        var customerAddressQuery = TpcdsData.CustomerAddressQuery();
        var customerDemographicsQuery = TpcdsData.CustomerDemographicsQuery();
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var webSalesQuery = TpcdsData.WebSalesQuery();
        var catalogSalesQuery = TpcdsData.CatalogSalesQuery();
        var dateDimQuery = TpcdsData.DateDimQuery();

        var storeSalesInPeriod = from ss in storeSalesQuery
                                 join d in dateDimQuery on ss.ss_sold_date_sk equals d.d_date_sk
                                 where d.d_year == 2002 && d.d_moy >= 1 && d.d_moy <= 4
                                 select ss;

        var webSalesInPeriod = from ws in webSalesQuery
                               join d in dateDimQuery on ws.ws_sold_date_sk equals d.d_date_sk
                               where d.d_year == 2002 && d.d_moy >= 1 && d.d_moy <= 4
                               select ws;

        var catSalesInPeriod = from cs in catalogSalesQuery
                               join d in dateDimQuery on cs.cs_sold_date_sk equals d.d_date_sk
                               where d.d_year == 2002 && d.d_moy >= 1 && d.d_moy <= 4
                               select cs;

        var q10 = (from c in customerQuery
                   join ca in customerAddressQuery on c.c_current_addr_sk equals ca.ca_address_sk
                   join cd in customerDemographicsQuery on c.c_current_cdemo_sk equals cd.cd_demo_sk
                   where Q10TargetCounties.Contains(ca.ca_county) &&
                         storeSalesInPeriod.Any(ss => ss.ss_customer_sk == c.c_customer_sk) &&
                         (webSalesInPeriod.Any(ws => ws.ws_bill_customer_sk == c.c_customer_sk) ||
                          catSalesInPeriod.Any(cs => cs.cs_ship_customer_sk == c.c_customer_sk))
                   group cd by new
                   {
                       cd.cd_gender,
                       cd.cd_marital_status,
                       cd.cd_education_status,
                       cd.cd_purchase_estimate,
                       cd.cd_credit_rating,
                       cd.cd_dep_count,
                       cd.cd_dep_employed_count,
                       cd.cd_dep_college_count,
                   } into g
                   orderby g.Key.cd_gender,
                           g.Key.cd_marital_status,
                           g.Key.cd_education_status,
                           g.Key.cd_purchase_estimate,
                           g.Key.cd_credit_rating,
                           g.Key.cd_dep_count,
                           g.Key.cd_dep_employed_count,
                           g.Key.cd_dep_college_count
                   select new
                   {
                       g.Key.cd_gender,
                       g.Key.cd_marital_status,
                       g.Key.cd_education_status,
                       cnt1 = g.LongCount(),
                       g.Key.cd_purchase_estimate,
                       cnt2 = g.LongCount(),
                       g.Key.cd_credit_rating,
                       cnt3 = g.LongCount(),
                       g.Key.cd_dep_count,
                       cnt4 = g.LongCount(),
                       g.Key.cd_dep_employed_count,
                       cnt5 = g.LongCount(),
                       g.Key.cd_dep_college_count,
                       cnt6 = g.LongCount(),
                   }).Take(100);

        return q10;
    }

    public static string Q10Sql() =>
        $@"
            SELECT 
                cd_gender, 
                cd_marital_status, 
                cd_education_status, 
                COUNT(*) AS cnt1, 
                cd_purchase_estimate, 
                COUNT(*) AS cnt2, 
                cd_credit_rating, 
                COUNT(*) AS cnt3, 
                cd_dep_count, 
                COUNT(*) AS cnt4, 
                cd_dep_employed_count, 
                COUNT(*) AS cnt5, 
                cd_dep_college_count, 
                COUNT(*) AS cnt6 
            FROM 
                read_parquet('{TpcdsData.CustomerPath}') c, 
                read_parquet('{TpcdsData.CustomerAddressPath}') ca, 
                read_parquet('{TpcdsData.CustomerDemographicsPath}') cd
            WHERE 
                c.c_current_addr_sk = ca.ca_address_sk 
                AND ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County') 
                AND cd.cd_demo_sk = c.c_current_cdemo_sk 
                AND EXISTS (
                    SELECT * 
                    FROM read_parquet('{TpcdsData.StoreSalesPath}') ss, 
                         read_parquet('{TpcdsData.DateDimPath}') d 
                    WHERE c.c_customer_sk = ss.ss_customer_sk 
                      AND ss.ss_sold_date_sk = d.d_date_sk 
                      AND d.d_year = 2002 
                      AND d.d_moy BETWEEN 1 AND 1 + 3
                ) 
                AND (
                    EXISTS (
                        SELECT * 
                        FROM read_parquet('{TpcdsData.WebSalesPath}') ws, 
                             read_parquet('{TpcdsData.DateDimPath}') d 
                        WHERE c.c_customer_sk = ws.ws_bill_customer_sk 
                          AND ws.ws_sold_date_sk = d.d_date_sk 
                          AND d.d_year = 2002 
                          AND d.d_moy BETWEEN 1 AND 1 + 3
                    ) 
                    OR EXISTS (
                        SELECT * 
                        FROM read_parquet('{TpcdsData.CatalogSalesPath}') cs, 
                             read_parquet('{TpcdsData.DateDimPath}') d 
                        WHERE c.c_customer_sk = cs.cs_ship_customer_sk 
                          AND cs_sold_date_sk = d.d_date_sk 
                          AND d.d_year = 2002 
                          AND d.d_moy BETWEEN 1 AND 1 + 3
                    )
                ) 
            GROUP BY 
                cd_gender, 
                cd_marital_status, 
                cd_education_status, 
                cd_purchase_estimate, 
                cd_credit_rating, 
                cd_dep_count, 
                cd_dep_employed_count, 
                cd_dep_college_count 
            ORDER BY 
                cd_gender, 
                cd_marital_status, 
                cd_education_status, 
                cd_purchase_estimate, 
                cd_credit_rating, 
                cd_dep_count, 
                cd_dep_employed_count, 
                cd_dep_college_count 
            LIMIT 100;
        ";
}
