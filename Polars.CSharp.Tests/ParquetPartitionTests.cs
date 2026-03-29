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
    public void SinkParquetPartitioned_ScanParquet_EndToEnd()
    {
        var sGroup = new Series("Group", ["A", "A", "B", "B", "C"]);
        var sValue = new Series("Value", [1, 2, 3, 4, 5]);
        var df = new DataFrame(sGroup, sValue);

        var lf = df.Lazy();
        
        lf.SinkParquetPartitioned(
            _testBaseDir,
            partitionBy: Cs.Col("Group"), 
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
}
