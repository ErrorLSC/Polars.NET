using Polars.CSharp;

namespace Polars.Integration.Tests;

public class HuggingFaceTests
{
    [Fact]
    public void Test_Scan_Real_HuggingFace_Public()
    {

        var hfUrl = "https://huggingface.co/datasets/scikit-learn/iris/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet";

        var options = CloudOptions.Http(new Dictionary<string, string>
        {
            { "User-Agent", "Polars.NET-Test" }
        });

        using var lf = LazyFrame.ScanParquet(hfUrl, cloudOptions: options);
        using var df = lf.Collect(useStreaming:true);

        Assert.True(df.Height > 0);
        // shape: (150, 6)
        // ┌─────┬───────────────┬──────────────┬───────────────┬──────────────┬────────────────┐
        // │ Id  ┆ SepalLengthCm ┆ SepalWidthCm ┆ PetalLengthCm ┆ PetalWidthCm ┆ Species        │
        // │ --- ┆ ---           ┆ ---          ┆ ---           ┆ ---          ┆ ---            │
        // │ i64 ┆ f64           ┆ f64          ┆ f64           ┆ f64          ┆ str            │
        // ╞═════╪═══════════════╪══════════════╪═══════════════╪══════════════╪════════════════╡
        // │ 1   ┆ 5.1           ┆ 3.5          ┆ 1.4           ┆ 0.2          ┆ Iris-setosa    │
        // │ 2   ┆ 4.9           ┆ 3.0          ┆ 1.4           ┆ 0.2          ┆ Iris-setosa    │
        // │ 3   ┆ 4.7           ┆ 3.2          ┆ 1.3           ┆ 0.2          ┆ Iris-setosa    │
        // │ 4   ┆ 4.6           ┆ 3.1          ┆ 1.5           ┆ 0.2          ┆ Iris-setosa    │
        // │ 5   ┆ 5.0           ┆ 3.6          ┆ 1.4           ┆ 0.2          ┆ Iris-setosa    │
        // │ …   ┆ …             ┆ …            ┆ …             ┆ …            ┆ …              │
        // │ 146 ┆ 6.7           ┆ 3.0          ┆ 5.2           ┆ 2.3          ┆ Iris-virginica │
        // │ 147 ┆ 6.3           ┆ 2.5          ┆ 5.0           ┆ 1.9          ┆ Iris-virginica │
        // │ 148 ┆ 6.5           ┆ 3.0          ┆ 5.2           ┆ 2.0          ┆ Iris-virginica │
        // │ 149 ┆ 6.2           ┆ 3.4          ┆ 5.4           ┆ 2.3          ┆ Iris-virginica │
        // │ 150 ┆ 5.9           ┆ 3.0          ┆ 5.1           ┆ 1.8          ┆ Iris-virginica │
        // └─────┴───────────────┴──────────────┴───────────────┴──────────────┴────────────────┘
        
        var columns = df.Columns;
        Assert.Contains("Id", columns); 
        Assert.Contains("SepalLengthCm", columns);

    }
}