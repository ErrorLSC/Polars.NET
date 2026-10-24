using Polars.CSharp;

namespace Polars.TPCH.Tests;

public static class TpchData
{
    private static readonly Lazy<string> _dataDir = new(FindDataDir);

    public static string DataDir => _dataDir.Value;

    private static string FindDataDir()
    {
        var appBase = AppContext.BaseDirectory;
        var candidate = Path.Combine(appBase, "Data");
        if (Directory.Exists(candidate) && File.Exists(Path.Combine(candidate, "lineitem.parquet")))
        {
            return Path.GetFullPath(candidate);
        }

        var dir = new DirectoryInfo(appBase);
        while (dir != null)
        {
            var projectData = Path.Combine(dir.FullName, "Polars.TPCH.Tests", "Data");
            if (Directory.Exists(projectData) && File.Exists(Path.Combine(projectData, "lineitem.parquet")))
            {
                return Path.GetFullPath(projectData);
            }

            var directData = Path.Combine(dir.FullName, "Data");
            if (Directory.Exists(directData) && File.Exists(Path.Combine(directData, "lineitem.parquet")))
            {
                return Path.GetFullPath(directData);
            }

            dir = dir.Parent;
        }

        throw new DirectoryNotFoundException(
            "Could not find TPC-H parquet data directory. Run 'Polars.TPCH.Tests/generate_tpch_data.sh' to generate test data.");
    }

    public static string CustomerPath => Path.Combine(DataDir, "customer.parquet");
    public static string LineItemPath => Path.Combine(DataDir, "lineitem.parquet");
    public static string NationPath => Path.Combine(DataDir, "nation.parquet");
    public static string OrdersPath => Path.Combine(DataDir, "orders.parquet");
    public static string PartPath => Path.Combine(DataDir, "part.parquet");
    public static string PartSuppPath => Path.Combine(DataDir, "partsupp.parquet");
    public static string RegionPath => Path.Combine(DataDir, "region.parquet");
    public static string SupplierPath => Path.Combine(DataDir, "supplier.parquet");

    public static LazyFrame Customer() => LazyFrame.ScanParquet(CustomerPath);
    public static LazyFrame LineItem() => LazyFrame.ScanParquet(LineItemPath);
    public static LazyFrame Nation() => LazyFrame.ScanParquet(NationPath);
    public static LazyFrame Orders() => LazyFrame.ScanParquet(OrdersPath);
    public static LazyFrame Part() => LazyFrame.ScanParquet(PartPath);
    public static LazyFrame PartSupp() => LazyFrame.ScanParquet(PartSuppPath);
    public static LazyFrame Region() => LazyFrame.ScanParquet(RegionPath);
    public static LazyFrame Supplier() => LazyFrame.ScanParquet(SupplierPath);
}
