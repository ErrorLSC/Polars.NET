using Cs = Polars.CSharp.Polars.Selectors;
namespace Polars.CSharp.Tests;

public class ParquetPartitionTests : IDisposable
{
    private readonly string _testBaseDir;

    public ParquetPartitionTests()
    {
        _testBaseDir = Path.Combine(Path.GetTempPath(), "polars_net_partition_test_" + Guid.NewGuid());
        Directory.CreateDirectory(_testBaseDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testBaseDir))
        {
            try { Directory.Delete(_testBaseDir, true); } catch { /* Ignore cleanup errors */ }
        }
    }

    [Fact]
    [Trait("IO","Partitioned")]
    public void SinkParquetPartitioned_ScanParquet_EndToEnd()
    {
        var sGroup = new Series("Group", ["A", "A", "B", "B", "C"]);
        var sValue = new Series("Value", [1, 2, 3, 4, 5]);
        var df = new DataFrame(sGroup, sValue);

        var lf = df.Lazy();
        
        lf.SinkParquetPartitioned(
            _testBaseDir,
            partitionBy: Cs.ByName("Group"), 
            includeKeys: true
        );

        Assert.True(Directory.Exists(Path.Combine(_testBaseDir, "Group=A")), "Partition directory Group=A missing");
        Assert.True(Directory.Exists(Path.Combine(_testBaseDir, "Group=B")), "Partition directory Group=B missing");
        Assert.True(Directory.Exists(Path.Combine(_testBaseDir, "Group=C")), "Partition directory Group=C missing");

        var lfScan = LazyFrame.ScanParquet(
            _testBaseDir,
            glob: true,             
            tryParseHiveDates: true 
        );

        var dfResult = lfScan.Collect();

        var dfExpected = df.Sort("Value");
        
        var dfActual = dfResult.Sort("Value");

        Assert.Equal(dfExpected.Height, dfActual.Height);

        var expectedValues = dfExpected["Value"].ToArray<int>();
        var actualValues = dfActual["Value"].ToArray<int>();
        Assert.Equal(expectedValues, actualValues);

        var expectedGroups = dfExpected["Group"].ToArray<string>();
        var actualGroups = dfActual["Group"].ToArray<string>();
        Assert.Equal(expectedGroups, actualGroups);
    }
    [Fact]
    [Trait("IO","Partitioned")]
    public void SinkIpcPartitioned_ScanIpc_EndToEnd()
    {
        var df = new DataFrame(
            new Series("Group", ["A", "A", "B", "B", "C"]),
            new Series("Value", [1, 2, 3, 4, 5]));

        df.Lazy().SinkIpcPartitioned(_testBaseDir, partitionBy: Cs.ByName("Group"), includeKeys: true);

        string[] gs = ["A", "B", "C"];
        foreach (var g in gs)
            Assert.True(Directory.Exists(Path.Combine(_testBaseDir, $"Group={g}")), $"Partition directory Group={g} missing");

        var dfActual = LazyFrame.ScanIpc(Path.Combine(_testBaseDir, "**", "*.ipc"), hivePartitioning: true)
            .Collect()
            .Sort("Value");

        Assert.Equal([1, 2, 3, 4, 5], dfActual["Value"].ToArray<int>());
        Assert.Equal(["A", "A", "B", "B", "C"], dfActual["Group"].ToArray<string>());
    }

    [Fact]
    [Trait("IO","Partitioned")]
    public void WriteIpcPartitioned_DataFrame_Writes_Every_Partition()
    {
        var df = new DataFrame(
            new Series("Group", ["X", "Y", "X"]),
            new Series("Value", [10, 20, 30]));

        df.WriteIpcPartitioned(_testBaseDir, partitionBy: Cs.ByName("Group"));

        var files = Directory.GetFiles(_testBaseDir, "*", SearchOption.AllDirectories);
        Assert.Contains(files, f => f.Contains("Group=X"));
        Assert.Contains(files, f => f.Contains("Group=Y"));

        var total = files.Sum(f => DataFrame.ReadIpc(f).Height);
        Assert.Equal(3, total);
    }
}
