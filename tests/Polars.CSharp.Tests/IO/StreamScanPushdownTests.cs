#nullable enable
using System.Data;
using Apache.Arrow;
using Kaminari.Sazanami.Data.Database;
using Polars.NET.Core.Arrow;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// .NET stream sources receive the columns and leading rows the query needs (<see cref="StreamScanRequest"/>), on
/// every engine path: in memory (the engine calls the scan itself), streaming collects (the source is read into
/// memory first) and sinks (the source is spooled to a temporary file first).
/// </summary>
public class StreamScanPushdownTests
{
    private const int Rows = 1000;
    private const int BatchSize = 100;
    private static readonly string[] AllColumns = ["Id", "G", "V"];

    public enum Run { InMemory, Streaming, Sink }

    public static TheoryData<Run> Runs => [Run.InMemory, Run.Streaming, Run.Sink];

    /// <summary>A source over the same rows that records each request and what it read.</summary>
    private sealed class Source
    {
        public List<StreamScanRequest> Requests { get; } = [];
        public int BatchesRead { get; private set; }
        public int Disposed { get; private set; }

        /// <summary>When false the source ignores the request and gives every column and row.</summary>
        public bool Honours { get; init; } = true;

        /// <summary>When set, the source fails with this message after its first batch.</summary>
        public string? FailsWith { get; init; }

        public LazyFrame Scan()
        {
            using var reader = Table().CreateDataReader();
            Schema schema = reader.GetArrowSchema();
            return new LazyFrame(ArrowStreamInterop.ScanStream(Open, schema));
        }

        private IEnumerable<RecordBatch> Open(StreamScanRequest request)
        {
            Requests.Add(request);
            using var reader = Table().CreateDataReader();
            long left = Honours ? request.RowLimit ?? long.MaxValue : long.MaxValue;
            try
            {
                foreach (var batch in reader.ToArrowBatches(BatchSize))
                {
                    if (left <= 0) yield break;
                    if (FailsWith is not null && BatchesRead == 1) throw new InvalidOperationException(FailsWith);
                    BatchesRead++;
                    left -= batch.Length;
                    yield return Honours && request.Columns is { } columns ? Project(batch, columns) : batch;
                }
            }
            finally { Disposed++; }
        }

        private static RecordBatch Project(RecordBatch batch, string[] columns)
        {
            int[] indices = [.. columns.Select(c => batch.Schema.GetFieldIndex(c))];
            var schema = new Schema([.. indices.Select(i => batch.Schema.GetFieldByIndex(i))], null);
            return new RecordBatch(schema, [.. indices.Select(i => batch.Column(i))], batch.Length);
        }
    }

    private static DataTable Table()
    {
        var t = new DataTable();
        t.Columns.Add("Id", typeof(int));
        t.Columns.Add("G", typeof(string));
        t.Columns.Add("V", typeof(double));
        for (int i = 0; i < Rows; i++) t.Rows.Add(i, i % 2 == 0 ? "a" : "b", i * 0.5);
        return t;
    }

    /// <summary>Runs the query the way <paramref name="run"/> says and returns its result.</summary>
    private static DataFrame Execute(LazyFrame query, Run run)
    {
        switch (run)
        {
            case Run.InMemory: return query.Collect(Engine.InMemory);
            case Run.Streaming: return query.Collect(Engine.Streaming);
            default:
                var dir = Directory.CreateTempSubdirectory("scan-pushdown-").FullName;
                try
                {
                    var path = Path.Combine(dir, "out.parquet");
                    query.SinkParquet(path);
                    return DataFrame.ReadParquet(path);
                }
                finally { Directory.Delete(dir, true); }
        }
    }

    private static int[] Ids(DataFrame df) => [.. Enumerable.Range(0, (int)df.Height).Select(i => Convert.ToInt32(df[i, "Id"]))];

    [Theory]
    [MemberData(nameof(Runs))]
    public void One_Column_Is_All_The_Source_Reads(Run run)
    {
        var source = new Source();
        using var lf = source.Scan();
        using var df = Execute(lf.Select(Pl.Col("G")), run);

        Assert.Equal(["G"], df.Columns);
        Assert.Equal(Rows, df.Height);
        var request = Assert.Single(source.Requests);
        Assert.Equal(["G"], request.Columns!);
        Assert.Null(request.RowLimit);
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void Head_Stops_The_Source_Early_And_Disposes_It(Run run)
    {
        var source = new Source();
        using var lf = source.Scan();
        using var df = Execute(lf.Select(Pl.Col("Id")).Head(5), run);

        Assert.Equal([0, 1, 2, 3, 4], Ids(df));
        var request = Assert.Single(source.Requests);
        Assert.Equal(["Id"], request.Columns!);
        Assert.Equal(5, request.RowLimit);
        Assert.Equal(1, source.BatchesRead);
        Assert.Equal(1, source.Disposed);
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void Engine_Trims_A_Source_That_Ignores_The_Request(Run run)
    {
        // The engine stops pulling once it has the rows: the ignoring source is released after its first batch.
        var source = new Source { Honours = false };
        using var lf = source.Scan();
        using var df = Execute(lf.Select(Pl.Col("V"), Pl.Col("Id")).Head(5), run);

        Assert.Equal(["V", "Id"], df.Columns);
        Assert.Equal([0, 1, 2, 3, 4], Ids(df));
        // The engine reads a batch or two ahead before it stops the source (never the whole source).
        Assert.InRange(source.BatchesRead, 1, 3);
        Assert.Equal(1, source.Disposed);
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void A_Projection_Of_Every_Column_Keeps_Its_Order(Run run)
    {
        foreach (bool honours in (bool[])[true, false])
        {
            var source = new Source { Honours = honours };
            using var lf = source.Scan();
            using var df = Execute(lf.Select(Pl.Col("V"), Pl.Col("G"), Pl.Col("Id")), run);

            Assert.Equal(["V", "G", "Id"], df.Columns);
            Assert.Equal(Rows, df.Height);
            Assert.Equal(1.5, Convert.ToDouble(df[3, "V"]));
            Assert.Equal("b", df[3, "G"]);
            Assert.Equal(3, Convert.ToInt32(df[3, "Id"]));
        }
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void A_Failing_Source_Fails_The_Query(Run run)
    {
        // The error reaches the query: the source's stream does not just end (a truncated result).
        var source = new Source { FailsWith = "the source broke" };
        using var lf = source.Scan();
        var error = Assert.ThrowsAny<Exception>(() => Execute(lf.Select(Pl.Col("Id")), run).Dispose());
        Assert.Contains("the source broke", error.Message);
        Assert.Equal(1, source.Disposed);
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void Count_Reads_One_Column(Run run)
    {
        var source = new Source();
        using var lf = source.Scan();
        using var df = Execute(lf.Select(Pl.Len()), run);

        Assert.Equal(Rows, Convert.ToInt64(df[0, 0]));
        var request = Assert.Single(source.Requests);
        Assert.Single(request.Columns!);
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void An_Offset_Slice_Reads_To_Its_End_And_A_Head_After_A_Filter_Reads_Every_Row(Run run)
    {
        var source = new Source();
        using var lf = source.Scan();
        using (var df = Execute(lf.Slice(10, 5), run)) Assert.Equal([10, 11, 12, 13, 14], Ids(df));
        using (var df = Execute(lf.Filter(Pl.Col("Id") > 3).Head(5), run)) Assert.Equal([4, 5, 6, 7, 8], Ids(df));

        Assert.Equal(2, source.Requests.Count);
        // An offset slice reads up to its end (offset + length); a head above an engine filter reads every row.
        Assert.Equal([15, null], source.Requests.Select(r => r.RowLimit));
        Assert.Equal(1 + Rows / BatchSize, source.BatchesRead);
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void A_Plan_Naming_Unread_Columns_Still_Runs(Run run)
    {
        // The query above the scan names V, which the optimized plan never reads.
        var source = new Source();
        using var lf = source.Scan();
        using (var df = Execute(lf.WithColumns((Pl.Col("V") * 2).Alias("V2")).Select(Pl.Col("Id")), run))
        {
            Assert.Equal(["Id"], df.Columns);
            Assert.Equal(Rows, df.Height);
        }
        using (var df = Execute(lf.Drop("V").Head(3), run))
        {
            Assert.Equal(["Id", "G"], df.Columns);
            Assert.Equal([0, 1, 2], Ids(df));
            Assert.Equal("b", df[1, "G"]);
        }

        Assert.Equal(["Id"], source.Requests[0].Columns!);
        Assert.Equal(["Id", "G"], source.Requests[1].Columns!);
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void A_Shared_Source_Reads_The_Columns_Of_Every_Use_Once(Run run)
    {
        var source = new Source();
        using var lf = source.Scan();
        using var left = lf.Select(Pl.Col("Id")).Filter(Pl.Col("Id") < 10);
        using var right = lf.Select(Pl.Col("Id"), Pl.Col("G")).Head(20);
        using var df = Execute(left.Join(right, "Id"), run);

        Assert.Equal(10, df.Height);
        Assert.Equal(["Id", "G"], df.Columns);
        // The in-memory engine runs the scan once per use, each with its own request.
        if (run == Run.InMemory) return;
        // Read once with both uses' columns; the left use needs every row.
        var request = Assert.Single(source.Requests);
        Assert.Equal(["Id", "G"], request.Columns!);
        Assert.Null(request.RowLimit);
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void A_Subquery_Over_The_Same_Source_Reads_Its_Own_Columns(Run run)
    {
        // The IN subquery is a plan inside an expression and reads V, which the outer query does not.
        var source = new Source();
        using var lf = source.Scan();
        using var ctx = new SqlContext();
        ctx.Register("t", lf);
        // ORDER BY: a LIMIT alone may take any rows (the streaming engine's semi join keeps no order).
        using var q = ctx.Execute("SELECT Id, G FROM t WHERE G IN (SELECT G FROM t WHERE V > 499) ORDER BY Id LIMIT 5");
        using var df = Execute(q, run);

        Assert.Equal(["Id", "G"], df.Columns);
        Assert.Equal([1, 3, 5, 7, 9], Ids(df));
    }

    [Fact]
    public void CollectAll_Reads_A_Shared_Source_Once()
    {
        var source = new Source();
        using var lf = source.Scan();
        using var a = lf.Select(Pl.Col("G")).Head(3);
        using var b = lf.Select(Pl.Col("Id")).Head(7);
        var dfs = Pl.CollectAll([a, b], Engine.Streaming);
        try
        {
            Assert.Equal(["G"], dfs[0].Columns);
            Assert.Equal(3, dfs[0].Height);
            Assert.Equal([0, 1, 2, 3, 4, 5, 6], Ids(dfs[1]));
        }
        finally { foreach (var df in dfs) df.Dispose(); }

        // The engine merges the two scans into one before pushing projections and limits down: one read, of every
        // column and row (the anonymous scan's merge read the union of the columns and the larger limit).
        Assert.Single(source.Requests);
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void A_Plan_Whose_Schema_Was_Read_Gets_The_Request(Run run)
    {
        var source = new Source();
        using var lf = source.Scan();
        using var query = lf.Select(Pl.Col("V"));
        using (var schema = query.Schema) Assert.Single(schema);
        using var df = Execute(query, run);

        Assert.Equal(["V"], df.Columns);
        Assert.Equal(["V"], Assert.Single(source.Requests).Columns!);
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void Whole_Scan_Asks_For_Everything(Run run)
    {
        var source = new Source();
        using var lf = source.Scan();
        using var df = Execute(lf, run);

        Assert.Equal(AllColumns, df.Columns);
        Assert.Equal(Rows, df.Height);
        Assert.Equal(StreamScanRequest.All, Assert.Single(source.Requests));
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void Avro_Scan_Reads_The_Requested_Columns_And_Rows(Run run)
    {
        var dir = Directory.CreateTempSubdirectory("scan-pushdown-avro-").FullName;
        try
        {
            var path = Path.Combine(dir, "t.avro");
            using (var source = DataFrame.From<AvroRow>([.. Enumerable.Range(0, Rows).Select(i => new AvroRow(i, $"s{i}", i * 0.25))]))
                source.WriteAvro(path);

            using (var lf = LazyFrame.ScanAvro(path))
            using (var df = Execute(lf.Select(Pl.Col("Name"), Pl.Col("Id")).Head(4), run))
            {
                Assert.Equal(["Name", "Id"], df.Columns);
                Assert.Equal([0, 1, 2, 3], Ids(df));
                Assert.Equal("s3", df[3, "Name"]);
            }
            // The user's own projection and row limit combine with the query's.
            using (var lf = LazyFrame.ScanAvro(path, nRows: 2, projection: [0, 2]))
            using (var df = Execute(lf.Select(Pl.Col("Score")), run))
            {
                Assert.Equal(["Score"], df.Columns);
                Assert.Equal(2, df.Height);
                Assert.Equal(0.25, Convert.ToDouble(df[1, "Score"]));
            }
        }
        finally { Directory.Delete(dir, true); }
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void Excel_Scan_Stops_After_The_Requested_Rows(Run run)
    {
        var dir = Directory.CreateTempSubdirectory("scan-pushdown-excel-").FullName;
        try
        {
            var path = Path.Combine(dir, "t.xlsx");
            using (var source = DataFrame.From<ExcelRow>([.. Enumerable.Range(0, Rows).Select(i => new ExcelRow(i, $"s{i}"))]))
                source.WriteExcel(path);

            // Read as Int8, Id overflows from row 128 on: a read of the whole sheet fails there, the leading rows
            // are read without parsing that far.
            using (var lf = LazyFrame.ScanExcel(path, schema: PolarsSchema.From<ExcelNarrowRow>()))
            {
                using (var df = Execute(lf.Select(Pl.Col("Name"), Pl.Col("Id")).Head(4), run))
                {
                    Assert.Equal(["Name", "Id"], df.Columns);
                    Assert.Equal([0, 1, 2, 3], Ids(df));
                    Assert.Equal("s3", df[3, "Name"]);
                }
                Assert.ThrowsAny<Exception>(() => Execute(lf.Select(Pl.Col("Id")), run).Dispose());
            }
            // Leading rows after empty rows: the rows of the whole read, as many as asked.
            using (var gaps = DataFrame.From<ExcelRow>([new(0, "a"), new(1, "b"), new(2, "c"), new(3, "d")]))
                gaps.WriteExcel(path);
            using (var lf = LazyFrame.ScanExcel(path))
            using (var df = Execute(lf.Head(3), run))
                Assert.Equal([0, 1, 2], Ids(df));
        }
        finally { Directory.Delete(dir, true); }
    }

    public record AvroRow(int Id, string Name, double Score);

    public record ExcelRow(long Id, string Name);

    public record ExcelNarrowRow(sbyte Id);
}
