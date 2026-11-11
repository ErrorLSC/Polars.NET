namespace Polars.CSharp.Tests;

/// <summary>
/// inferSchemaLength follows Python Polars: 100 rows by default, null means the whole input (#36).
/// </summary>
public class InferSchemaLengthTests
{
    // x is an integer in the first 200 rows, then a float.
    private static DisposableFile Csv() => new(
        "id,x\n" + string.Concat(Enumerable.Range(0, 300).Select(i => i == 200 ? $"{i},8.1\n" : $"{i},{i}\n")), ".csv");

    private static DisposableFile NdJson() => new(
        string.Concat(Enumerable.Range(0, 300).Select(i => i == 200 ? $"{{\"id\":{i},\"x\":8.1}}\n" : $"{{\"id\":{i},\"x\":{i}}}\n")), ".ndjson");

    private static string TypeOfX(DataFrame df) => df.Schema["x"].ToString()!;

    [Fact]
    public void Csv_Default_Infers_From_100_Rows()
    {
        using var csv = Csv();
        Assert.Throws<global::Polars.NET.Core.Exceptions.PolarsException>(() => LazyFrame.ScanCsv(csv.Path).Collect());
        Assert.Throws<global::Polars.NET.Core.Exceptions.PolarsException>(() => DataFrame.ReadCsv(csv.Path));
    }

    [Fact]
    public void Csv_Null_Infers_From_The_Whole_File()
    {
        using var csv = Csv();
        using (var df = LazyFrame.ScanCsv(csv.Path, inferSchemaLength: null).Collect()) Assert.Equal("f64", TypeOfX(df));
        using (var df = DataFrame.ReadCsv(csv.Path, inferSchemaLength: null)) Assert.Equal("f64", TypeOfX(df));
    }

    [Fact]
    public void Csv_Huge_Lengths_Mean_The_Whole_File_Without_Panicking()
    {
        using var csv = Csv();
        foreach (var n in new ulong[] { ulong.MaxValue, int.MaxValue, 1UL << 30 })
            using (var df = LazyFrame.ScanCsv(csv.Path, inferSchemaLength: n).Collect()) Assert.Equal("f64", TypeOfX(df));
    }

    [Fact]
    public void Csv_Zero_Reads_Every_Column_As_String()
    {
        using var csv = Csv();
        using var df = LazyFrame.ScanCsv(csv.Path, inferSchemaLength: 0).Collect();
        Assert.Equal("str", TypeOfX(df));
    }

    [Fact]
    public void NdJson_Null_And_Zero_Infer_From_The_Whole_Input()
    {
        using var nd = NdJson();
        using (var df = LazyFrame.ScanNdjson(nd.Path, inferSchemaLength: null).Collect()) Assert.Equal("f64", TypeOfX(df));
        using (var df = LazyFrame.ScanNdjson(nd.Path, inferSchemaLength: 0).Collect()) Assert.Equal("f64", TypeOfX(df));
        using (var df = LazyFrame.ScanNdjson(nd.Path, inferSchemaLength: ulong.MaxValue).Collect()) Assert.Equal("f64", TypeOfX(df));
    }
}
