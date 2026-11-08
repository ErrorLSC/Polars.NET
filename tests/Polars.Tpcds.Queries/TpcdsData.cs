using Polars.CSharp;
using Polars.CSharp.Linq;

namespace Polars.Tpcds;

public static class TpcdsData
{
    private static string? _dataDir;

    /// <summary>
    /// The directory of the TPC-DS Parquet files. Defaults to <c>POLARS_TPCDS_DIR</c> when set, otherwise the
    /// test project's <c>Data</c> directory; set it before the first query to point at other data.
    /// </summary>
    public static string DataDir
    {
        get => _dataDir ??= FindDataDir();
        set => _dataDir = Path.GetFullPath(value);
    }

    private static string FindDataDir()
    {
        var configured = Environment.GetEnvironmentVariable("POLARS_TPCDS_DIR");
        if (!string.IsNullOrEmpty(configured)) return Path.GetFullPath(configured);

        var appBase = AppContext.BaseDirectory;
        var candidate = Path.Combine(appBase, "Data");
        if (Directory.Exists(candidate) && File.Exists(Path.Combine(candidate, "store_sales.parquet")))
        {
            return Path.GetFullPath(candidate);
        }

        var dir = new DirectoryInfo(appBase);
        while (dir != null)
        {
            var projectData = Path.Combine(dir.FullName, "tests", "Polars.Tpcds.Tests", "Data");
            if (Directory.Exists(projectData) && File.Exists(Path.Combine(projectData, "store_sales.parquet")))
            {
                return Path.GetFullPath(projectData);
            }

            var directData = Path.Combine(dir.FullName, "Data");
            if (Directory.Exists(directData) && File.Exists(Path.Combine(directData, "store_sales.parquet")))
            {
                return Path.GetFullPath(directData);
            }

            dir = dir.Parent;
        }

        throw new DirectoryNotFoundException(
            "Could not find TPC-DS parquet data directory. Run 'tests/Polars.Tpcds.Tests/generate_tpcds_data.sh' to generate test data.");
    }

    public static string CallCenterPath => Path.Combine(DataDir, "call_center.parquet");
    public static string CatalogPagePath => Path.Combine(DataDir, "catalog_page.parquet");
    public static string CatalogReturnsPath => Path.Combine(DataDir, "catalog_returns.parquet");
    public static string CatalogSalesPath => Path.Combine(DataDir, "catalog_sales.parquet");
    public static string CustomerPath => Path.Combine(DataDir, "customer.parquet");
    public static string CustomerAddressPath => Path.Combine(DataDir, "customer_address.parquet");
    public static string CustomerDemographicsPath => Path.Combine(DataDir, "customer_demographics.parquet");
    public static string DateDimPath => Path.Combine(DataDir, "date_dim.parquet");
    public static string HouseholdDemographicsPath => Path.Combine(DataDir, "household_demographics.parquet");
    public static string IncomeBandPath => Path.Combine(DataDir, "income_band.parquet");
    public static string InventoryPath => Path.Combine(DataDir, "inventory.parquet");
    public static string ItemPath => Path.Combine(DataDir, "item.parquet");
    public static string PromotionPath => Path.Combine(DataDir, "promotion.parquet");
    public static string ReasonPath => Path.Combine(DataDir, "reason.parquet");
    public static string ShipModePath => Path.Combine(DataDir, "ship_mode.parquet");
    public static string StorePath => Path.Combine(DataDir, "store.parquet");
    public static string StoreReturnsPath => Path.Combine(DataDir, "store_returns.parquet");
    public static string StoreSalesPath => Path.Combine(DataDir, "store_sales.parquet");
    public static string TimeDimPath => Path.Combine(DataDir, "time_dim.parquet");
    public static string WarehousePath => Path.Combine(DataDir, "warehouse.parquet");
    public static string WebPagePath => Path.Combine(DataDir, "web_page.parquet");
    public static string WebReturnsPath => Path.Combine(DataDir, "web_returns.parquet");
    public static string WebSalesPath => Path.Combine(DataDir, "web_sales.parquet");
    public static string WebSitePath => Path.Combine(DataDir, "web_site.parquet");

    public static LazyFrame CallCenter() => LazyFrame.ScanParquet(CallCenterPath);
    public static LazyFrame CatalogPage() => LazyFrame.ScanParquet(CatalogPagePath);
    public static LazyFrame CatalogReturns() => LazyFrame.ScanParquet(CatalogReturnsPath);
    public static LazyFrame CatalogSales() => LazyFrame.ScanParquet(CatalogSalesPath);
    public static LazyFrame Customer() => LazyFrame.ScanParquet(CustomerPath);
    public static LazyFrame CustomerAddress() => LazyFrame.ScanParquet(CustomerAddressPath);
    public static LazyFrame CustomerDemographics() => LazyFrame.ScanParquet(CustomerDemographicsPath);
    public static LazyFrame DateDim() => LazyFrame.ScanParquet(DateDimPath);
    public static LazyFrame HouseholdDemographics() => LazyFrame.ScanParquet(HouseholdDemographicsPath);
    public static LazyFrame IncomeBand() => LazyFrame.ScanParquet(IncomeBandPath);
    public static LazyFrame Inventory() => LazyFrame.ScanParquet(InventoryPath);
    public static LazyFrame Item() => LazyFrame.ScanParquet(ItemPath);
    public static LazyFrame Promotion() => LazyFrame.ScanParquet(PromotionPath);
    public static LazyFrame Reason() => LazyFrame.ScanParquet(ReasonPath);
    public static LazyFrame ShipMode() => LazyFrame.ScanParquet(ShipModePath);
    public static LazyFrame Store() => LazyFrame.ScanParquet(StorePath);
    public static LazyFrame StoreReturns() => LazyFrame.ScanParquet(StoreReturnsPath);
    public static LazyFrame StoreSales() => LazyFrame.ScanParquet(StoreSalesPath);
    public static LazyFrame TimeDim() => LazyFrame.ScanParquet(TimeDimPath);
    public static LazyFrame Warehouse() => LazyFrame.ScanParquet(WarehousePath);
    public static LazyFrame WebPage() => LazyFrame.ScanParquet(WebPagePath);
    public static LazyFrame WebReturns() => LazyFrame.ScanParquet(WebReturnsPath);
    public static LazyFrame WebSales() => LazyFrame.ScanParquet(WebSalesPath);
    public static LazyFrame WebSite() => LazyFrame.ScanParquet(WebSitePath);

    /// <summary>
    /// When true, each table's queryable is created once and reused, so building a query does not scan the
    /// Parquet file's metadata again (benchmarks of translation alone set it). Default false: a fresh scan per call.
    /// </summary>
    public static bool ReuseSources { get; set; }

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, object> _sources = new();

    private static IQueryable<T> Source<T>(Func<IQueryable<T>> create) =>
        ReuseSources ? (IQueryable<T>)_sources.GetOrAdd(typeof(T), _ => create()) : create();

    // Strongly-typed Queryable entrypoints
    public static IQueryable<CallCenter> CallCenterQuery() => Source(static () => CallCenter().AsQueryable<CallCenter>());
    public static IQueryable<CatalogPage> CatalogPageQuery() => Source(static () => CatalogPage().AsQueryable<CatalogPage>());
    public static IQueryable<CatalogReturns> CatalogReturnsQuery() => Source(static () => CatalogReturns().AsQueryable<CatalogReturns>());
    public static IQueryable<CatalogSales> CatalogSalesQuery() => Source(static () => CatalogSales().AsQueryable<CatalogSales>());
    public static IQueryable<Customer> CustomerQuery() => Source(static () => Customer().AsQueryable<Customer>());
    public static IQueryable<CustomerAddress> CustomerAddressQuery() => Source(static () => CustomerAddress().AsQueryable<CustomerAddress>());
    public static IQueryable<CustomerDemographics> CustomerDemographicsQuery() => Source(static () => CustomerDemographics().AsQueryable<CustomerDemographics>());
    public static IQueryable<DateDim> DateDimQuery() => Source(static () => DateDim().AsQueryable<DateDim>());
    public static IQueryable<HouseholdDemographics> HouseholdDemographicsQuery() => Source(static () => HouseholdDemographics().AsQueryable<HouseholdDemographics>());
    public static IQueryable<IncomeBand> IncomeBandQuery() => Source(static () => IncomeBand().AsQueryable<IncomeBand>());
    public static IQueryable<Inventory> InventoryQuery() => Source(static () => Inventory().AsQueryable<Inventory>());
    public static IQueryable<Item> ItemQuery() => Source(static () => Item().AsQueryable<Item>());
    public static IQueryable<Promotion> PromotionQuery() => Source(static () => Promotion().AsQueryable<Promotion>());
    public static IQueryable<Reason> ReasonQuery() => Source(static () => Reason().AsQueryable<Reason>());
    public static IQueryable<ShipMode> ShipModeQuery() => Source(static () => ShipMode().AsQueryable<ShipMode>());
    public static IQueryable<Store> StoreQuery() => Source(static () => Store().AsQueryable<Store>());
    public static IQueryable<StoreReturns> StoreReturnsQuery() => Source(static () => StoreReturns().AsQueryable<StoreReturns>());
    public static IQueryable<StoreSales> StoreSalesQuery() => Source(static () => StoreSales().AsQueryable<StoreSales>());
    public static IQueryable<TimeDim> TimeDimQuery() => Source(static () => TimeDim().AsQueryable<TimeDim>());
    public static IQueryable<Warehouse> WarehouseQuery() => Source(static () => Warehouse().AsQueryable<Warehouse>());
    public static IQueryable<WebPage> WebPageQuery() => Source(static () => WebPage().AsQueryable<WebPage>());
    public static IQueryable<WebReturns> WebReturnsQuery() => Source(static () => WebReturns().AsQueryable<WebReturns>());
    public static IQueryable<WebSales> WebSalesQuery() => Source(static () => WebSales().AsQueryable<WebSales>());
    public static IQueryable<WebSite> WebSiteQuery() => Source(static () => WebSite().AsQueryable<WebSite>());
}
