#nullable enable
using Polars.CSharp;
using Polars.CSharp.Linq;

namespace Polars.Tpcds.Tests;

/// <summary>
/// TPC-DS ROLLUP queries written in one result selector: Grouping() mixed with keys, aggregates and the rank window
/// (`CASE WHEN grouping(b) = 0 THEN a END` as a partition key), as the SQL writes them. The originals in
/// Polars.Tpcds.Tests carry helper columns into a second projection (Q70, Q86) or spell the levels out as a
/// UNION (Q36). Results are cross-validated with DuckDB running the SQL ROLLUP.
/// </summary>
public class TpcdsGroupingQueryTests
{
    private const string NativePushdown = "100% Native Pushdown";

    private static void AssertPlan<T>(IQueryable<T> query, string expectedPlan)
    {
        var actualPlan = query.Explain();
        Assert.Contains(expectedPlan, actualPlan);
    }

    /*
     * TPC-DS Query 36 (ROLLUP form)
     *
     * SELECT sum(ss_net_profit)/sum(ss_ext_sales_price) AS gross_margin,
     *        i_category,
     *        i_class,
     *        grouping(i_category)+grouping(i_class) AS lochierarchy,
     *        rank() OVER (PARTITION BY grouping(i_category)+grouping(i_class),
     *                                  CASE
     *                                      WHEN grouping(i_class) = 0 THEN i_category
     *                                  END
     *                     ORDER BY sum(ss_net_profit)/sum(ss_ext_sales_price) ASC) AS rank_within_parent
     * FROM store_sales,
     *      date_dim d1,
     *      item,
     *      store
     * WHERE d1.d_year = 2001
     *   AND d1.d_date_sk = ss_sold_date_sk
     *   AND i_item_sk = ss_item_sk
     *   AND s_store_sk = ss_store_sk
     *   AND s_state IN ('TN','TN','TN','TN','TN','TN','TN','TN')
     * GROUP BY rollup(i_category,i_class)
     * ORDER BY lochierarchy DESC,
     *          CASE
     *              WHEN lochierarchy = 0 THEN i_category
     *          END,
     *          rank_within_parent
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q36")]
    [Trait("LINQ", "Grouping")]
    public void Test_Q36_Rollup()
    {
        var filteredDates = TpcdsData.DateDimQuery().Where(d => d.d_year == 2001);
        var filteredStores = TpcdsData.StoreQuery().Where(s => s.s_state == "TN");

        var baseSales = from ss in TpcdsData.StoreSalesQuery()
                        join d in filteredDates on ss.ss_sold_date_sk equals d.d_date_sk
                        join i in TpcdsData.ItemQuery() on ss.ss_item_sk equals i.i_item_sk
                        join s in filteredStores on ss.ss_store_sk equals s.s_store_sk
                        select new { i.i_category, i.i_class, ss.ss_net_profit, ss.ss_ext_sales_price };

        var q36 = baseSales
            .GroupBy(x => new { x.i_category, x.i_class })
            .RollUp((key, g) => new
            {
                gross_margin = (double)g.Sum(x => x.ss_net_profit)! / (double)g.Sum(x => x.ss_ext_sales_price)!,
                key.i_category,
                key.i_class,
                lochierarchy = key.i_category.Grouping() + key.i_class.Grouping(),
                rank_within_parent = (long)((double)g.Sum(x => x.ss_net_profit)! / (double)g.Sum(x => x.ss_ext_sales_price)!)
                    .Rank(RankMethod.Min)
                    .Over(new
                    {
                        lochierarchy = key.i_category.Grouping() + key.i_class.Grouping(),
                        parent = key.i_class.Grouping() == 0 ? key.i_category : null
                    })
            })
            .OrderByDescending(r => r.lochierarchy)
            .ThenBy(r => r.lochierarchy == 0 ? r.i_category : null)
            .ThenBy(r => r.rank_within_parent)
            .Take(100);

        AssertPlan(q36, NativePushdown);

        var polarsResult = q36.ToList();

        var duckDbResults = DuckDbFixture.Query($@"
            SELECT sum(ss_net_profit)/sum(ss_ext_sales_price) AS gross_margin,
                   i_category,
                   i_class,
                   grouping(i_category)+grouping(i_class) AS lochierarchy,
                   rank() OVER (PARTITION BY grouping(i_category)+grouping(i_class),
                                             CASE
                                                 WHEN grouping(i_class) = 0 THEN i_category
                                             END
                                ORDER BY sum(ss_net_profit)/sum(ss_ext_sales_price) ASC) AS rank_within_parent
            FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                 read_parquet('{TpcdsData.DateDimPath}') d1,
                 read_parquet('{TpcdsData.ItemPath}'),
                 read_parquet('{TpcdsData.StorePath}')
            WHERE d1.d_year = 2001
              AND d1.d_date_sk = ss_sold_date_sk
              AND i_item_sk = ss_item_sk
              AND s_store_sk = ss_store_sk
              AND s_state IN ('TN','TN','TN','TN','TN','TN','TN','TN')
            GROUP BY rollup(i_category,i_class)
            ORDER BY lochierarchy DESC,
                     CASE
                         WHEN lochierarchy = 0 THEN i_category
                     END,
                     rank_within_parent
            LIMIT 100;
        ", r => new
        {
            gross_margin = r.IsDBNull(0) ? (double?)null : Convert.ToDouble(r.GetValue(0)),
            i_category = r.IsDBNull(1) ? null : r.GetString(1),
            i_class = r.IsDBNull(2) ? null : r.GetString(2),
            lochierarchy = Convert.ToInt32(r.GetValue(3)),
            rank_within_parent = Convert.ToInt64(r.GetValue(4)),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            TpcdsAssert.Close(d.gross_margin, p.gross_margin);
            Assert.Equal(d.i_category, p.i_category);
            Assert.Equal(d.i_class, p.i_class);
            Assert.Equal(d.lochierarchy, p.lochierarchy);
            Assert.Equal(d.rank_within_parent, p.rank_within_parent);
        }
    }

    /*
     * TPC-DS Query 70
     *
     * SELECT sum(ss_net_profit) AS total_sum,
     *        s_state,
     *        s_county,
     *        grouping(s_state)+grouping(s_county) AS lochierarchy,
     *        rank() OVER (PARTITION BY grouping(s_state)+grouping(s_county),
     *                                  CASE
     *                                      WHEN grouping(s_county) = 0 THEN s_state
     *                                  END
     *                     ORDER BY sum(ss_net_profit) DESC) AS rank_within_parent
     * FROM store_sales,
     *      date_dim d1,
     *      store
     * WHERE d1.d_month_seq BETWEEN 1200 AND 1200+11
     *   AND d1.d_date_sk = ss_sold_date_sk
     *   AND s_store_sk = ss_store_sk
     *   AND s_state IN
     *     (SELECT s_state
     *      FROM
     *        (SELECT s_state AS s_state,
     *                rank() OVER (PARTITION BY s_state
     *                             ORDER BY sum(ss_net_profit) DESC) AS ranking
     *         FROM store_sales,
     *              store,
     *              date_dim
     *         WHERE d_month_seq BETWEEN 1200 AND 1200+11
     *           AND d_date_sk = ss_sold_date_sk
     *           AND s_store_sk = ss_store_sk
     *         GROUP BY s_state) tmp1
     *      WHERE ranking <= 5 )
     * GROUP BY rollup(s_state,s_county)
     * ORDER BY lochierarchy DESC ,
     *          CASE
     *              WHEN grouping(s_state)+grouping(s_county) = 0 THEN s_state
     *          END ,
     *          rank_within_parent
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q70")]
    [Trait("LINQ", "Grouping")]
    public void Test_Q70_Inline()
    {
        var storeSalesQuery = TpcdsData.StoreSalesQuery();
        var storeQuery = TpcdsData.StoreQuery();
        var filteredDates = TpcdsData.DateDimQuery().Where(d => d.d_month_seq >= 1200 && d.d_month_seq <= 1211);

        var topStates = from ss in storeSalesQuery
                        join d in filteredDates on ss.ss_sold_date_sk equals d.d_date_sk
                        join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                        group ss by s.s_state into g
                        let ranking = (long)g.Sum(x => x.ss_net_profit).Rank(RankMethod.Min, true).Over(g.Key)
                        where ranking <= 5
                        select new { s_state = g.Key };

        var baseSales = from ss in storeSalesQuery
                        join d in filteredDates on ss.ss_sold_date_sk equals d.d_date_sk
                        join s in storeQuery on ss.ss_store_sk equals s.s_store_sk
                        join ts in topStates on s.s_state equals ts.s_state
                        select new { s.s_state, s.s_county, ss.ss_net_profit };

        var q70 = baseSales
            .GroupBy(x => new { x.s_state, x.s_county })
            .RollUp((key, g) => new
            {
                total_sum = g.Sum(x => x.ss_net_profit),
                key.s_state,
                key.s_county,
                lochierarchy = key.s_state.Grouping() + key.s_county.Grouping(),
                rank_within_parent = (long)g.Sum(x => x.ss_net_profit)
                    .Rank(RankMethod.Min, true)
                    .Over(new
                    {
                        lochierarchy = key.s_state.Grouping() + key.s_county.Grouping(),
                        parent = key.s_county.Grouping() == 0 ? key.s_state : null
                    })
            })
            .OrderByDescending(r => r.lochierarchy)
            .ThenBy(r => r.lochierarchy == 0 ? r.s_state : null)
            .ThenBy(r => r.rank_within_parent)
            .Take(100);

        AssertPlan(q70, NativePushdown);

        var polarsResult = q70.ToList();

        var duckDbResults = DuckDbFixture.Query($@"
            SELECT sum(ss_net_profit) AS total_sum,
                   s_state,
                   s_county,
                   grouping(s_state)+grouping(s_county) AS lochierarchy,
                   rank() OVER (PARTITION BY grouping(s_state)+grouping(s_county),
                                             CASE
                                                 WHEN grouping(s_county) = 0 THEN s_state
                                             END
                                ORDER BY sum(ss_net_profit) DESC) AS rank_within_parent
            FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                 read_parquet('{TpcdsData.DateDimPath}') d1,
                 read_parquet('{TpcdsData.StorePath}')
            WHERE d1.d_month_seq BETWEEN 1200 AND 1200+11
              AND d1.d_date_sk = ss_sold_date_sk
              AND s_store_sk = ss_store_sk
              AND s_state IN
                (SELECT s_state
                 FROM
                   (SELECT s_state AS s_state,
                           rank() OVER (PARTITION BY s_state
                                        ORDER BY sum(ss_net_profit) DESC) AS ranking
                    FROM read_parquet('{TpcdsData.StoreSalesPath}'),
                         read_parquet('{TpcdsData.StorePath}'),
                         read_parquet('{TpcdsData.DateDimPath}')
                    WHERE d_month_seq BETWEEN 1200 AND 1200+11
                      AND d_date_sk = ss_sold_date_sk
                      AND s_store_sk = ss_store_sk
                    GROUP BY s_state) tmp1
                 WHERE ranking <= 5 )
            GROUP BY rollup(s_state,s_county)
            ORDER BY lochierarchy DESC,
                     CASE
                         WHEN grouping(s_state)+grouping(s_county) = 0 THEN s_state
                     END,
                     rank_within_parent
            LIMIT 100;
        ", r => new
        {
            total_sum = r.IsDBNull(0) ? (decimal?)null : r.GetDecimal(0),
            s_state = r.IsDBNull(1) ? null : r.GetString(1),
            s_county = r.IsDBNull(2) ? null : r.GetString(2),
            lochierarchy = Convert.ToInt32(r.GetValue(3)),
            rank_within_parent = Convert.ToInt64(r.GetValue(4)),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.total_sum, p.total_sum);
            Assert.Equal(d.s_state, p.s_state);
            Assert.Equal(d.s_county, p.s_county);
            Assert.Equal(d.lochierarchy, p.lochierarchy);
            Assert.Equal(d.rank_within_parent, p.rank_within_parent);
        }
    }

    /*
     * TPC-DS Query 86
     *
     * SELECT sum(ws_net_paid) AS total_sum ,
     *        i_category ,
     *        i_class ,
     *        grouping(i_category)+grouping(i_class) AS lochierarchy ,
     *        rank() OVER ( PARTITION BY grouping(i_category)+grouping(i_class),
     *                                   CASE
     *                                       WHEN grouping(i_class) = 0 THEN i_category
     *                                   END
     *                     ORDER BY sum(ws_net_paid) DESC) AS rank_within_parent
     * FROM web_sales ,
     *      date_dim d1 ,
     *      item
     * WHERE d1.d_month_seq BETWEEN 1200 AND 1200+11
     *   AND d1.d_date_sk = ws_sold_date_sk
     *   AND i_item_sk = ws_item_sk
     * GROUP BY rollup(i_category,i_class)
     * ORDER BY lochierarchy DESC NULLS FIRST,
     *          CASE
     *              WHEN grouping(i_category)+grouping(i_class) = 0 THEN i_category
     *          END NULLS FIRST,
     *          rank_within_parent NULLS FIRST
     * LIMIT 100;
     */
    [Fact]
    [Trait("TPCDS", "Q86")]
    [Trait("LINQ", "Grouping")]
    public void Test_Q86_Inline()
    {
        var filteredDates = TpcdsData.DateDimQuery().Where(d => d.d_month_seq >= 1200 && d.d_month_seq <= 1211);

        var baseSales = from ws in TpcdsData.WebSalesQuery()
                        join d in filteredDates on ws.ws_sold_date_sk equals d.d_date_sk
                        join i in TpcdsData.ItemQuery() on ws.ws_item_sk equals i.i_item_sk
                        select new { i.i_category, i.i_class, ws.ws_net_paid };

        var q86 = baseSales
            .GroupBy(x => new { x.i_category, x.i_class })
            .RollUp((key, g) => new
            {
                total_sum = g.Sum(x => x.ws_net_paid),
                key.i_category,
                key.i_class,
                lochierarchy = key.i_category.Grouping() + key.i_class.Grouping(),
                rank_within_parent = (long)g.Sum(x => x.ws_net_paid)
                    .Rank(RankMethod.Min, true)
                    .Over(new
                    {
                        lochierarchy = key.i_category.Grouping() + key.i_class.Grouping(),
                        parent = key.i_class.Grouping() == 0 ? key.i_category : null
                    })
            })
            .OrderByDescending(r => r.lochierarchy)
            .ThenBy(r => r.lochierarchy == 0 ? r.i_category : null)
            .ThenBy(r => r.rank_within_parent)
            .ThenBy(r => r.i_class) // tie-breaker for equal ranks, as in the DuckDB SQL
            .Take(100);

        AssertPlan(q86, NativePushdown);

        var polarsResult = q86.ToList();

        var duckDbResults = DuckDbFixture.Query($@"
            SELECT sum(ws_net_paid) AS total_sum ,
                   i_category ,
                   i_class ,
                   grouping(i_category)+grouping(i_class) AS lochierarchy ,
                   rank() OVER ( PARTITION BY grouping(i_category)+grouping(i_class),
                                              CASE
                                                  WHEN grouping(i_class) = 0 THEN i_category
                                              END
                                ORDER BY sum(ws_net_paid) DESC) AS rank_within_parent
            FROM read_parquet('{TpcdsData.WebSalesPath}') web_sales ,
                 read_parquet('{TpcdsData.DateDimPath}') d1 ,
                 read_parquet('{TpcdsData.ItemPath}') item
            WHERE d1.d_month_seq BETWEEN 1200 AND 1200+11
              AND d1.d_date_sk = ws_sold_date_sk
              AND i_item_sk = ws_item_sk
            GROUP BY rollup(i_category,i_class)
            ORDER BY lochierarchy DESC NULLS FIRST,
                     CASE
                         WHEN grouping(i_category)+grouping(i_class) = 0 THEN i_category
                     END NULLS FIRST,
                     rank_within_parent NULLS FIRST,
                     i_class NULLS FIRST
            LIMIT 100;
        ", r => new
        {
            total_sum = r.IsDBNull(0) ? (decimal?)null : r.GetDecimal(0),
            i_category = r.IsDBNull(1) ? null : r.GetString(1),
            i_class = r.IsDBNull(2) ? null : r.GetString(2),
            lochierarchy = Convert.ToInt32(r.GetValue(3)),
            rank_within_parent = Convert.ToInt64(r.GetValue(4)),
        });

        Assert.Equal(duckDbResults.Count, polarsResult.Count);
        for (int i = 0; i < duckDbResults.Count; i++)
        {
            var d = duckDbResults[i];
            var p = polarsResult[i];

            Assert.Equal(d.total_sum, p.total_sum);
            Assert.Equal(d.i_category, p.i_category);
            Assert.Equal(d.i_class, p.i_class);
            Assert.Equal(d.lochierarchy, p.lochierarchy);
            Assert.Equal(d.rank_within_parent, p.rank_within_parent);
        }
    }
}
