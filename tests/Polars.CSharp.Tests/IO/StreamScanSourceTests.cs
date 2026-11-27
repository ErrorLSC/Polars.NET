#nullable enable
using System.Data;
using Apache.Arrow;
using Kaminari.Sazanami.Data.Database;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// .NET sources scanned through the I/O plugin bridge: what each run reads again (a factory opens a new reader, an
/// enumerable is enumerated again), what is read once (a DataReader, a batch stream: a later run fails saying so; a
/// run that counts the rows first, as Tail does, still reads once), batches that are not of the declared types, and
/// 128-bit integers.
/// </summary>
public class StreamScanSourceTests
{
    private const int Rows = 1000;

    private static DataTable Table()
    {
        var t = new DataTable();
        t.Columns.Add("Id", typeof(int));
        t.Columns.Add("G", typeof(string));
        for (int i = 0; i < Rows; i++) t.Rows.Add(i, i % 2 == 0 ? "a" : "b");
        return t;
    }

    private static int[] Ids(DataFrame df) => [.. Enumerable.Range(0, (int)df.Height).Select(i => Convert.ToInt32(df[i, "Id"]))];

    [Fact]
    public void A_Data_Reader_Is_Read_Once()
    {
        using var lf = LazyFrame.ScanDatabase(Table().CreateDataReader(), batchSize: 100);
        using (var df = lf.Collect()) Assert.Equal(Rows, df.Height);
        var error = Assert.Throws<InvalidOperationException>(() => lf.Collect().Dispose());
        Assert.Contains("can be read only once", error.Message);
        Assert.Contains("ScanDatabase(Func<IDataReader>)", error.Message);
    }

    [Fact]
    public void A_Batch_Stream_Is_Read_Once()
    {
        var reader = Table().CreateDataReader();
        using var lf = LazyFrame.ScanRecordBatches(reader.ToArrowBatches(100), reader.GetArrowSchema());
        using (var df = lf.Collect()) Assert.Equal(Rows, df.Height);
        var error = Assert.Throws<InvalidOperationException>(() => lf.Collect().Dispose());
        Assert.Contains("can be read only once", error.Message);
    }

    [Theory]
    [InlineData(Engine.InMemory)]
    [InlineData(Engine.Streaming)]
    public void Tail_Of_A_Data_Reader_Reads_It_Once(Engine engine)
    {
        using var lf = LazyFrame.ScanDatabase(Table().CreateDataReader(), batchSize: 100);
        using var q = lf.Tail(3);
        using var df = q.Collect(engine);
        Assert.Equal([997, 998, 999], Ids(df));
    }

    [Theory]
    [InlineData(Engine.InMemory)]
    [InlineData(Engine.Streaming)]
    public void Counting_A_Data_Reader_Reads_It_Once(Engine engine)
    {
        using var lf = LazyFrame.ScanDatabase(Table().CreateDataReader(), batchSize: 100);
        using (var q = lf.Select(Pl.Len()))
        using (var df = q.Collect(engine)) Assert.Equal(Rows, Convert.ToInt64(df[0, 0]));
        var error = Assert.Throws<InvalidOperationException>(() => lf.Collect().Dispose());
        Assert.Contains("can be read only once", error.Message);
    }

    [Fact]
    public void A_Factory_Opens_A_Reader_For_Every_Run()
    {
        var table = Table();
        int opened = 0;
        using var lf = LazyFrame.ScanDatabase(() =>
        {
            Interlocked.Increment(ref opened);
            return table.CreateDataReader();
        }, batchSize: 100);
        int probe = opened; // the schema's reader
        using (var df = lf.Collect()) Assert.Equal(Rows, df.Height);
        using (var q = lf.Filter(Pl.Col("Id") < 5))
        using (var df = q.Collect(Engine.Streaming)) Assert.Equal([0, 1, 2, 3, 4], Ids(df));
        using (var q = lf.Tail(2))
        using (var df = q.Collect()) Assert.Equal([998, 999], Ids(df));
        // One reader per run.
        Assert.Equal(probe + 3, opened);
    }

    [Fact]
    public void An_Enumerable_Is_Enumerated_For_Every_Run()
    {
        int enumerated = 0;
        IEnumerable<Row> Rows()
        {
            Interlocked.Increment(ref enumerated);
            for (int i = 0; i < 10; i++) yield return new Row(i, i % 2 == 0 ? "a" : "b");
        }
        using var lf = LazyFrame.ScanEnumerable(Rows(), batchSize: 4);
        using (var df = lf.Collect()) Assert.Equal(10, df.Height);
        using (var df = lf.Collect(Engine.Streaming)) Assert.Equal(10, df.Height);
        Assert.Equal(2, enumerated);
    }

    [Fact]
    public void Batches_Of_Other_Types_Than_The_Schema_Are_Refused_By_Name()
    {
        var reader = Table().CreateDataReader();
        Schema actual = reader.GetArrowSchema();
        // The schema says G is a 64-bit integer: the batches hold strings.
        var declared = new Schema([actual.GetFieldByName("Id"), new Apache.Arrow.Field("G", Apache.Arrow.Types.Int64Type.Default, true)], null);
        using var lf = LazyFrame.ScanRecordBatches(reader.ToArrowBatches(100), declared);
        var error = Assert.ThrowsAny<Exception>(() => lf.Collect().Dispose());
        Assert.Contains("Column \"G\"", error.Message);
        Assert.Contains("give the schema the batches' types", error.Message);
    }

    [Fact]
    public void Wide_Integers_Round_Trip()
    {
        Int128 big = Int128.MaxValue - 5;
        UInt128 bigger = UInt128.MaxValue - 7;
        using var lf = LazyFrame.ScanEnumerable<Wide>([new(1, big, bigger), new(2, Int128.MinValue, UInt128.Zero)]);
        using var df = lf.Collect();
        Assert.Equal(big, (Int128)df[0, "Signed"]!);
        Assert.Equal(bigger, (UInt128)df[0, "Unsigned"]!);
        Assert.Equal(Int128.MinValue, (Int128)df[1, "Signed"]!);
    }

    private sealed record Row(int Id, string G);

    private sealed record Wide(int Id, Int128 Signed, UInt128 Unsigned);

    [Fact]
    public void A_Buffered_Scan_Applies_Filters_Exactly()
    {
        using var lf = LazyFrame.ScanDatabaseBuffered(Table().CreateDataReader(), batchSize: 100);
        using var q = lf.Filter((Pl.Col("Id") < 10) & (Pl.Col("G") == "b")).Select(Pl.Col("Id"));
        string plan = q.Explain();
        Assert.Contains("filters rows by", plan);
        Assert.DoesNotContain("SELECTION:", plan);
        Assert.DoesNotContain("FILTER", plan);
        using var df = q.Collect();
        Assert.Equal([1, 3, 5, 7, 9], Ids(df));
    }
}
