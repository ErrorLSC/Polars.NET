#nullable enable
using System.Collections.Concurrent;
using System.Data;
using Apache.Arrow;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Data.Database;
using Polars.NET.Core.IO.Plugins;
using Xunit.Abstractions;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// The managed scans as Polars I/O plugins (<see cref="IoPluginBridge"/>): the contract layouts, what the engine pushes
/// to a resolver (columns, filters, limit), every engine path (in memory, streaming, sinks), errors, early stops and
/// the release of everything the engine held.
/// </summary>
public class IoPluginBridgeTests(ITestOutputHelper output)
{
    private const int Rows = 1000;
    private const int BatchSize = 100;
    private const int Parts = 2;

    public enum Run { InMemory, Streaming, Sink }

    public static TheoryData<Run> Runs => [Run.InMemory, Run.Streaming, Run.Sink];

    /// <summary>A table read in <see cref="Parts"/> sources, recording what the engine asks of it.</summary>
    private sealed class Table
    {
        public ConcurrentQueue<ResolveRequest> Requests { get; } = [];
        public ConcurrentQueue<(string Source, string[] Columns)> Reads { get; } = [];
        public ConcurrentBag<int> ReadThreads { get; } = [];
        public int BatchesRead;
        public int ResolversReleased;
        public int BuildersReleased;
        public int BuildersMade;
        public int ScansOpened;
        public int ScansDisposed;
        public int ReadersOpened;
        public int ReadersDisposed;
        public int Counts;

        /// <summary>When set, a reader fails with this message after its first batch.</summary>
        public string? FailsWith { get; init; }

        /// <summary>When true, every source repeats its rows forever (a source that ignores the limit).</summary>
        public bool Endless { get; init; }

        /// <summary>When true, a reader skips the rows the pushed <c>Id &lt; k</c> filters reject (as a pruning reader does).</summary>
        public bool Prunes { get; init; }

        /// <summary>When true, every builder has one fingerprint, whatever the resolution baked in (a broken resolver).</summary>
        public bool ConstantFingerprint { get; init; }

        /// <summary>When true, a reader blocks after its first batch until the engine cancels it.</summary>
        public bool BlocksUntilCancelled { get; init; }

        public int CancelsSeen;

        /// <summary>The source strings handed to the engine, one per part.</summary>
        public string[] Sources { get; init; } = [.. Enumerable.Range(0, Parts).Select(p => p.ToString())];

        public Schema Schema { get; } = Batches(0, 1).First().Schema;

        public LazyFrame Scan(ulong fingerprint = 1) => new(IoPluginBridge.Scan(new Resolver(this, fingerprint)));

        public static IEnumerable<RecordBatch> Batches(int from, int to)
        {
            var t = new DataTable();
            t.Columns.Add("Id", typeof(int));
            t.Columns.Add("G", typeof(string));
            t.Columns.Add("V", typeof(double));
            for (int i = from; i < to; i++) t.Rows.Add(i, i % 2 == 0 ? "a" : "b", i * 0.5);
            using var reader = t.CreateDataReader();
            foreach (RecordBatch batch in reader.ToArrowBatches(BatchSize)) yield return batch;
        }
    }

    private sealed class Resolver(Table table, ulong fingerprint) : ScanResolver("table", fingerprint, table.Schema)
    {
        public override ScanPlan? Resolve(ResolveRequest request)
        {
            table.Requests.Enqueue(request);
            Schema schema = request.Projection is { } columns
                ? new Schema([.. columns.Select(c => table.Schema.GetFieldByName(c))], null)
                : table.Schema;
            // The builder bakes in the request: its fingerprint covers it.
            ulong builderPrint = table.ConstantFingerprint ? 42 : (ulong)HashCode.Combine(Fingerprint, string.Join(",", request.Projection ?? []),
                string.Join("|", request.Filters.Select(f => f.Document)), request.Limit);
            Interlocked.Increment(ref table.BuildersMade);
            return new ScanPlan(table.Sources, new Builder(table, builderPrint, schema, IdBound(request.Filters)), RowEstimate: Rows);
        }

        protected override void OnReleased() => Interlocked.Increment(ref table.ResolversReleased);

        /// <summary>The smallest k of the pushed <c>Id &lt; k</c> filters (what a pruning reader skips by).</summary>
        private static int IdBound(IReadOnlyList<PushedFilter> filters)
        {
            int bound = int.MaxValue;
            foreach (PushedFilter filter in filters)
            {
                if (filter.Exact is ScalarExpr.Binary { op.IsLt: true, left: ScalarExpr.Column { name: "Id" }, right: ScalarExpr.Literal { value: var value } })
                    bound = Math.Min(bound, Convert.ToInt32(value.BoxedValue));
            }
            return bound;
        }
    }

    private sealed class Builder(Table table, ulong fingerprint, Schema schema, int idBound) : ScanReaderBuilder("table reader", fingerprint, schema)
    {
        public override IReadOnlyList<KeyValuePair<string, string>> Explain => [new("parts", Parts.ToString())];

        public override ScanExecution BeginScan()
        {
            Interlocked.Increment(ref table.ScansOpened);
            return new Execution(table, idBound);
        }

        protected override void OnReleased() => Interlocked.Increment(ref table.BuildersReleased);
    }

    private sealed class Execution(Table table, int idBound) : ScanExecution
    {
        public override SourceReader OpenSource(string source)
        {
            Interlocked.Increment(ref table.ReadersOpened);
            int part = System.Array.IndexOf(table.Sources, source);
            if (part < 0) throw new InvalidOperationException($"Unknown source \"{source}\".");
            return new Reader(table, part, table.Prunes ? idBound : int.MaxValue);
        }

        public override void Dispose()
        {
            Interlocked.Increment(ref table.ScansDisposed);
            base.Dispose();
        }
    }

    private sealed class Reader(Table table, int part, int idBound) : SourceReader
    {
        private const int PartRows = Rows / Parts;

        public override Schema Schema => table.Schema;

        public override long CountRows()
        {
            Interlocked.Increment(ref table.Counts);
            return PartRows;
        }

        public override IEnumerable<RecordBatch> Read(IReadOnlyList<string> columns, CancellationToken cancellation)
        {
            table.Reads.Enqueue((table.Sources[part], [.. columns]));
            table.ReadThreads.Add(Environment.CurrentManagedThreadId);
            return Batches();

            IEnumerable<RecordBatch> Batches()
            {
                int read = 0;
                do
                {
                    int from = part * PartRows, to = Math.Min((part + 1) * PartRows, Math.Max(from, idBound));
                    foreach (RecordBatch batch in Table.Batches(from, to))
                    {
                        if (table.BlocksUntilCancelled && read == 1)
                        {
                            batch.Dispose();
                            // Blocks as a reader waiting for a slow store does; only the engine's cancel ends it.
                            if (!cancellation.WaitHandle.WaitOne(TimeSpan.FromSeconds(30))) throw new TimeoutException("never cancelled");
                            Interlocked.Increment(ref table.CancelsSeen);
                            cancellation.ThrowIfCancellationRequested();
                        }
                        cancellation.ThrowIfCancellationRequested();
                        if (table.FailsWith is not null && read == 1)
                        {
                            batch.Dispose();
                            throw new InvalidOperationException(table.FailsWith);
                        }
                        read++;
                        Interlocked.Increment(ref table.BatchesRead);
                        yield return batch;
                    }
                }
                while (table.Endless);
            }
        }

        public override void Dispose()
        {
            Interlocked.Increment(ref table.ReadersDisposed);
            base.Dispose();
        }
    }

    private static DataFrame Execute(LazyFrame query, Run run)
    {
        switch (run)
        {
            case Run.InMemory: return query.Collect(Engine.InMemory);
            case Run.Streaming: return query.Collect(Engine.Streaming);
            default:
                var dir = Directory.CreateTempSubdirectory("io-plugin-").FullName;
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

    [Fact]
    public void Contract_Layouts_Match_The_Engine()
    {
        Assert.Empty(IoPluginLayout.Mismatches());
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void Every_Row_And_Column_Arrives(Run run)
    {
        var table = new Table();
        using (var lf = table.Scan())
        using (var df = Execute(lf, run))
        {
            Assert.Equal(["Id", "G", "V"], df.Columns);
            Assert.Equal([.. Enumerable.Range(0, Rows)], Ids(df));
            Assert.Equal("b", df[3, "G"]);
            Assert.Equal(1.5, Convert.ToDouble(df[3, "V"]));
        }
        output.WriteLine($"requests {table.Requests.Count}, reads {table.Reads.Count}, threads {string.Join(",", table.ReadThreads.Distinct())}");
        var request = Assert.Single(table.Requests);
        Assert.Null(request.Limit);
        Assert.Empty(request.Filters);
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void The_Projection_Reaches_Resolver_And_Readers(Run run)
    {
        var table = new Table();
        using var lf = table.Scan();
        using var df = Execute(lf.Select(Pl.Col("V"), Pl.Col("Id")), run);

        Assert.Equal(["V", "Id"], df.Columns);
        Assert.Equal(Rows, df.Height);
        var request = Assert.Single(table.Requests);
        Assert.Equal(["Id", "V"], request.Projection!.Order());
        Assert.All(table.Reads, r => Assert.Equal(["Id", "V"], r.Columns.Order()));
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void Filters_Reach_The_Resolver_As_Documents_And_Ir(Run run)
    {
        var table = new Table();
        using var lf = table.Scan();
        using var df = Execute(lf.Filter((Pl.Col("Id") > 500) & (Pl.Col("G") == "a") & Pl.Col("V").IsBetween(Pl.Lit(0.0), Pl.Lit(400.0))), run);

        Assert.Equal([.. Enumerable.Range(501, 300).Where(i => i % 2 == 0)], Ids(df));
        var request = Assert.Single(table.Requests);
        foreach (PushedFilter filter in request.Filters) output.WriteLine($"{filter.Index}: {filter.Document}\n   exact {filter.Exact}");
        Assert.Equal(3, request.Filters.Count);
        Assert.All(request.Filters, f => Assert.NotNull(f.Exact));
        Assert.Null(request.Limit);
        Assert.Contains("Id", request.FilterColumns);
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void Head_Pushes_A_Limit_And_Stops_Early(Run run)
    {
        var table = new Table();
        using (var lf = table.Scan())
        using (var df = Execute(lf.Select(Pl.Col("Id")).Head(5), run))
            Assert.Equal([0, 1, 2, 3, 4], Ids(df));
        var request = Assert.Single(table.Requests);
        Assert.Equal(5, request.Limit);
        output.WriteLine($"batches read {table.BatchesRead}");
        // Each reader may prefetch a batch or two before the engine stops it; never the whole source.
        Assert.True(table.BatchesRead < Rows / BatchSize, $"read {table.BatchesRead} batches");
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void An_Endless_Source_Is_Stopped(Run run)
    {
        var table = new Table { Endless = true };
        using (var lf = table.Scan())
        using (var df = Execute(lf.Head(3), run))
            Assert.Equal([0, 1, 2], Ids(df));
        Assert.Equal(table.ReadersOpened, table.ReadersDisposed);
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void A_Failing_Source_Fails_The_Query_With_Its_Exception(Run run)
    {
        var table = new Table { FailsWith = "the source broke" };
        using var lf = table.Scan();
        var error = Assert.ThrowsAny<Exception>(() => Execute(lf.Select(Pl.Col("Id")), run).Dispose());
        output.WriteLine($"{error.GetType()}: {error.Message}");
        Assert.IsType<InvalidOperationException>(error);
        Assert.Equal("the source broke", error.Message);
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void Count_Asks_Row_Counts_Only(Run run)
    {
        var table = new Table();
        using var lf = table.Scan();
        using var df = Execute(lf.Select(Pl.Len()), run);

        Assert.Equal(Rows, Convert.ToInt64(df[0, 0]));
        Assert.Equal(Parts, table.Counts);
        Assert.Empty(table.Reads);
    }

    [Fact]
    public void Everything_The_Engine_Held_Is_Released()
    {
        var table = new Table();
        using (var lf = table.Scan())
        {
            using (var q = lf.Filter(Pl.Col("Id") < 10))
            using (var a = q.Collect()) Assert.Equal(10, a.Height);
            using (var q = lf.Select(Pl.Col("G")))
            using (var b = q.Collect(Engine.Streaming)) Assert.Equal(Rows, b.Height);
        }
        output.WriteLine($"resolver released {table.ResolversReleased}, builders {table.BuildersMade}/{table.BuildersReleased}, scans {table.ScansOpened}/{table.ScansDisposed}, readers {table.ReadersOpened}/{table.ReadersDisposed}");
        Assert.Equal(1, table.ResolversReleased);
        Assert.Equal(table.BuildersMade, table.BuildersReleased);
        Assert.Equal(table.ScansOpened, table.ScansDisposed);
        Assert.Equal(table.ReadersOpened, table.ReadersDisposed);
    }

    [Fact]
    public void Explain_Names_The_Plugin()
    {
        var table = new Table();
        using var lf = table.Scan();
        string plan = lf.Filter(Pl.Col("Id") > 3).Explain();
        output.WriteLine(plan);
        Assert.Contains("seseragi", plan);
    }

    [Fact]
    public void One_Scan_Serves_Both_Sides_Of_A_Self_Join_With_One_Filter()
    {
        var table = new Table();
        using var lf = table.Scan();
        using var left = lf.Filter(Pl.Col("Id") < 100);
        using var right = lf.Filter(Pl.Col("Id") < 100).Select(Pl.Col("Id"), Pl.Col("V").Alias("V2"));
        using var joined = left.Join(right, Pl.Col("Id"));
        using var df = joined.Collect();
        Assert.Equal(100, df.Height);
        Assert.Single(table.Requests);
        Assert.Equal(1, table.ScansOpened);
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void Scans_With_Different_Filters_Are_Not_Merged(Run run)
    {
        // Same projection on both sides, readers that skip what their baked-in filter rejects: a merge of the two
        // scans would feed one side the other's rows. (Probed 2026-11-27: the engine does not merge these even with
        // equal builder fingerprints; the fingerprints stay exact as the contract asks.)
        var table = new Table { Prunes = true };
        using var lf = table.Scan();
        using var left = lf.Filter(Pl.Col("Id") < 100);
        using var right = lf.Filter(Pl.Col("Id") < 50);
        using var joined = left.Join(right, Pl.Col("Id"), suffix: "_r");
        using var df = Execute(joined, run);
        Assert.Equal([.. Enumerable.Range(0, 50)], Ids(df).Order());
        Assert.Equal(2, table.Requests.Count);
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void A_Read_Blocked_In_The_Source_Is_Cancelled(Run run)
    {
        var table = new Table { BlocksUntilCancelled = true };
        var clock = System.Diagnostics.Stopwatch.StartNew();
        using (var lf = table.Scan())
        using (var q = lf.Select(Pl.Col("Id")).Head(3))
        using (var df = Execute(q, run))
            Assert.Equal([0, 1, 2], Ids(df));
        // The engine waits for its cancels before the query returns: every blocked reader was woken by one.
        Assert.True(table.CancelsSeen > 0, "no reader saw a cancel");
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(15), $"took {clock.Elapsed}");
        Assert.Equal(table.ReadersOpened, table.ReadersDisposed);
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void Slices_From_The_End_And_Offsets_Are_Placed_By_Row_Counts(Run run)
    {
        var table = new Table();
        using var lf = table.Scan();
        using (var q = lf.Tail(5))
        using (var df = Execute(q, run)) Assert.Equal([995, 996, 997, 998, 999], Ids(df));
        using (var q = lf.Slice(498, 4))
        using (var df = Execute(q, run)) Assert.Equal([498, 499, 500, 501], Ids(df));
    }

    [Fact]
    public void Sources_Come_Back_Verbatim()
    {
        string[] sources = ["s3://bucket/a b#c%20d.csv", @"C:\data\x y.csv"];
        var table = new Table { Sources = sources };
        using (var lf = table.Scan())
        using (var df = lf.Collect()) Assert.Equal(Rows, df.Height);
        Assert.Equal(sources.Order(), table.Reads.Select(r => r.Source).Order());
    }

    /// <summary>One source handing out a frame's batch: the engine's types round-trip through reader schema and batches.</summary>
    private sealed class FrameResolver(DataFrame frame) : ScanResolver("frame", 7, FrameReader.FrameSchema(frame))
    {
        public override ScanPlan? Resolve(ResolveRequest request)
        {
            Schema schema = request.Projection is { } columns ? new Schema([.. columns.Select(c => Schema.GetFieldByName(c))], null) : Schema;
            return new ScanPlan(["frame"], new FrameBuilder(frame, schema, (ulong)HashCode.Combine(string.Join(",", request.Projection ?? []))));
        }
    }

    private sealed class FrameBuilder(DataFrame frame, Schema schema, ulong fingerprint) : ScanReaderBuilder("frame reader", fingerprint, schema)
    {
        public override ScanExecution BeginScan() => new FrameExecution(frame);
    }

    private sealed class FrameExecution(DataFrame frame) : ScanExecution
    {
        public override SourceReader OpenSource(string source) => new FrameReader(frame);
    }

    private sealed class FrameReader(DataFrame frame) : SourceReader
    {
        private readonly Schema _schema = FrameSchema(frame);

        public static Schema FrameSchema(DataFrame frame)
        {
            using RecordBatch batch = frame.ToArrow();
            return batch.Schema;
        }

        public override Schema Schema => _schema;

        public override long CountRows() => frame.Height;

        public override IEnumerable<RecordBatch> Read(IReadOnlyList<string> columns, CancellationToken cancellation)
        {
            yield return frame.ToArrow();
        }
    }

    [Theory]
    [MemberData(nameof(Runs))]
    public void Every_Type_Round_Trips(Run run)
    {
        using var source = DataFrame.From<TypedRow>([
            new(1, "a", 1.25m, new DateTime(2024, 1, 2, 3, 4, 5), new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero), TimeSpan.FromSeconds(90),
                new DateOnly(2024, 1, 2), true, 0.5, [1, 2], null),
            new(2, null, -3.5m, DateTime.UnixEpoch, DateTimeOffset.UnixEpoch, TimeSpan.Zero, DateOnly.MinValue, false, null, [], "x"),
        ]);
        using var lf = new LazyFrame(IoPluginBridge.Scan(new FrameResolver(source)));
        using var df = Execute(lf, run);
        output.WriteLine(df.ToString());
        Assert.True(source.Equals(df), $"source\n{source}\nread\n{df}");
        using var picked = Execute(lf.Select(Pl.Col("Tags"), Pl.Col("Price")), run);
        Assert.Equal(["Tags", "Price"], picked.Columns);
    }

    private sealed record TypedRow(int Id, string? Name, decimal Price, DateTime When, DateTimeOffset At, TimeSpan Took, DateOnly Day,
        bool Flag, double? Score, int[] Tags, string? Note);
}
