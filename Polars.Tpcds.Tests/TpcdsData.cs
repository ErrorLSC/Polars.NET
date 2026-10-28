using Polars.CSharp;
using Polars.CSharp.Linq;

namespace Polars.Tpcds.Tests;

public static class TpcdsData
{
    private static readonly Lazy<string> _dataDir = new(FindDataDir);

    public static string DataDir => _dataDir.Value;

    private static string FindDataDir()
    {
        var appBase = AppContext.BaseDirectory;
        var candidate = Path.Combine(appBase, "Data");
        if (Directory.Exists(candidate) && File.Exists(Path.Combine(candidate, "store_sales.parquet")))
        {
            return Path.GetFullPath(candidate);
        }

        var dir = new DirectoryInfo(appBase);
        while (dir != null)
        {
            var projectData = Path.Combine(dir.FullName, "Polars.Tpcds.Tests", "Data");
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
            "Could not find TPC-DS parquet data directory. Run 'Polars.Tpcds.Tests/generate_tpcds_data.sh' to generate test data.");
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

    // Strongly-typed Queryable entrypoints
    public static IQueryable<CallCenter> CallCenterQuery() => CallCenter().AsQueryable<CallCenter>();
    public static IQueryable<CatalogPage> CatalogPageQuery() => CatalogPage().AsQueryable<CatalogPage>();
    public static IQueryable<CatalogReturns> CatalogReturnsQuery() => CatalogReturns().AsQueryable<CatalogReturns>();
    public static IQueryable<CatalogSales> CatalogSalesQuery() => CatalogSales().AsQueryable<CatalogSales>();
    public static IQueryable<Customer> CustomerQuery() => Customer().AsQueryable<Customer>();
    public static IQueryable<CustomerAddress> CustomerAddressQuery() => CustomerAddress().AsQueryable<CustomerAddress>();
    public static IQueryable<CustomerDemographics> CustomerDemographicsQuery() => CustomerDemographics().AsQueryable<CustomerDemographics>();
    public static IQueryable<DateDim> DateDimQuery() => DateDim().AsQueryable<DateDim>();
    public static IQueryable<HouseholdDemographics> HouseholdDemographicsQuery() => HouseholdDemographics().AsQueryable<HouseholdDemographics>();
    public static IQueryable<IncomeBand> IncomeBandQuery() => IncomeBand().AsQueryable<IncomeBand>();
    public static IQueryable<Inventory> InventoryQuery() => Inventory().AsQueryable<Inventory>();
    public static IQueryable<Item> ItemQuery() => Item().AsQueryable<Item>();
    public static IQueryable<Promotion> PromotionQuery() => Promotion().AsQueryable<Promotion>();
    public static IQueryable<Reason> ReasonQuery() => Reason().AsQueryable<Reason>();
    public static IQueryable<ShipMode> ShipModeQuery() => ShipMode().AsQueryable<ShipMode>();
    public static IQueryable<Store> StoreQuery() => Store().AsQueryable<Store>();
    public static IQueryable<StoreReturns> StoreReturnsQuery() => StoreReturns().AsQueryable<StoreReturns>();
    public static IQueryable<StoreSales> StoreSalesQuery() => StoreSales().AsQueryable<StoreSales>();
    public static IQueryable<TimeDim> TimeDimQuery() => TimeDim().AsQueryable<TimeDim>();
    public static IQueryable<Warehouse> WarehouseQuery() => Warehouse().AsQueryable<Warehouse>();
    public static IQueryable<WebPage> WebPageQuery() => WebPage().AsQueryable<WebPage>();
    public static IQueryable<WebReturns> WebReturnsQuery() => WebReturns().AsQueryable<WebReturns>();
    public static IQueryable<WebSales> WebSalesQuery() => WebSales().AsQueryable<WebSales>();
    public static IQueryable<WebSite> WebSiteQuery() => WebSite().AsQueryable<WebSite>();
}
