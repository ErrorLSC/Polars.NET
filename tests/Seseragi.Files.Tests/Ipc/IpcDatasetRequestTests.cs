using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;
using RemoteStore = Seseragi.Files.Tests.Parquet.ParquetDatasetRequestTests.RemoteStore;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// The requests an IPC dataset read sends to a store whose requests wait on the network: bounded
/// (<see cref="DatasetOptions.MaxRequestsInFlight"/>, or by the files in flight) apart from the files open, so that a
/// slow request does not stop the files after it; opens let in in file order; none counted from local files and
/// memory; a read stopped early while requests wait returns.
/// </summary>
public sealed class IpcDatasetRequestTests
{
    /// <summary>A file of <c>id</c> and <c>x</c>, <paramref name="rows"/> rows in batches of <paramref name="batchRows"/>.</summary>
    private static byte[] File(int first, int rows, int batchRows)
    {
        var schema = new Schema([new Field("id", Int64Type.Default, true), new Field("x", Int64Type.Default, true)], null);
        using var stream = new MemoryStream();
        using (IpcWriter writer = IpcWriter.Create(stream, schema))
        {
            for (int at = 0; at < rows; at += batchRows)
            {
                int n = Math.Min(batchRows, rows - at);
                using var batch = new RecordBatch(schema,
                [
                    new Int64Array.Builder().AppendRange(Enumerable.Range(first + at, n).Select(i => (long)i)).Build(),
                    new Int64Array.Builder().AppendRange(Enumerable.Range(first + at, n).Select(i => (long)(i * 7 % 1000))).Build(),
                ], n);
                writer.Write(batch);
            }
            writer.Complete();
        }
        return stream.ToArray();
    }

    private static List<long> Ids(IEnumerable<RecordBatch> batches)
    {
        List<long> ids = [];
        foreach (RecordBatch batch in batches)
        {
            using (batch) ids.AddRange(((Int64Array)batch.Column("id")).Values.ToArray());
        }
        return ids;
    }

    private static MemoryFileStore Files(int files, int rows, int batchRows)
    {
        var store = new MemoryFileStore();
        for (int f = 0; f < files; f++) store.Add($"d/f{f:D3}.arrow", File(f * rows, rows, batchRows));
        return store;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task ASlowFile_DoesNotStopTheFilesAfterIt(int threads)
    {
        // The second file's open waits (the first is opened by the dataset for its schema, and read first).
        var store = new RemoteStore(Files(60, 10, 10)) { Holding = "d/f001.arrow" };
        using var dataset = await IpcDataset.OpenAsync("d/", new IpcDatasetOptions { Store = store, MaxRequestsInFlight = 4 });
        using IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = threads });
        int before = store.Opened;
        Task<List<long>> read = Task.Run(() => Ids(reader.ReadBatches()));
        // The other files open meanwhile, four requests at a time.
        var watch = Stopwatch.StartNew();
        while (store.Opened - before < 59 && watch.Elapsed < TimeSpan.FromSeconds(20)) await Task.Delay(10);
        Assert.Equal(59, store.Opened - before);
        Assert.False(read.IsCompleted);
        store.Release();
        Assert.Equal([.. Enumerable.Range(0, 600).Select(i => (long)i)], await read);
        Assert.InRange(store.PeakRequests, 1, 4);
    }

    [Theory]
    [InlineData(1, 8)]
    [InlineData(3, 8)]
    [InlineData(3, 1)]
    public void Requests_StayWithinTheBound(int requests, int threads)
    {
        // Files of several batches (over the tail read): opens, then a fetch per batch.
        var store = new RemoteStore(Files(24, 8000, 1000)) { Delay = 2 };
        using var dataset = IpcDataset.Open("d/", new IpcDatasetOptions { Store = store, MaxRequestsInFlight = requests });
        using IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = threads });
        Assert.Equal([.. Enumerable.Range(0, 24 * 8000).Select(i => (long)i)], Ids(reader.ReadBatches()));
        Assert.InRange(store.PeakRequests, 1, requests);
        Assert.InRange(reader.PeakRequests, 1, requests);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public void Opens_AreLetIn_InFileOrder(int threads)
    {
        // One place: the files open one after another, in the order they are read.
        var store = new RemoteStore(Files(80, 10, 10));
        using var dataset = IpcDataset.Open("d/", new IpcDatasetOptions { Store = store, MaxRequestsInFlight = 1 });
        store.Opens.Clear();
        using IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = threads });
        Assert.Equal(800, Ids(reader.ReadBatches()).Count);
        Assert.Equal([.. Enumerable.Range(0, 80).Select(f => $"d/f{f:D3}.arrow")], store.Opens);
    }

    [Fact]
    public void FilesInFlight_BoundTheRequestsToo()
    {
        var store = new RemoteStore(Files(24, 8000, 1000)) { Delay = 2 };
        using var dataset = IpcDataset.Open("d/", new IpcDatasetOptions { Store = store, MaxFilesInFlight = 2 });
        using IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = 8 });
        Assert.Equal(24 * 8000, Ids(reader.ReadBatches()).Count);
        Assert.InRange(store.PeakRequests, 1, 2);
        // Files open count those whose batches are not all taken yet, not only those opening.
        Assert.InRange(reader.PeakInFlight.Files, 1, 2);
    }

    [Fact]
    public void ByDefault_RemoteRequestsAreBounded_LocalOnesNot()
    {
        MemoryFileStore memory = Files(200, 10, 10);
        var remote = new RemoteStore(memory) { Delay = 1 };
        using (var dataset = IpcDataset.Open("d/", new IpcDatasetOptions { Store = remote }))
        using (IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = 16 }))
        {
            Assert.Equal(2000, Ids(reader.ReadBatches()).Count);
            Assert.InRange(reader.PeakRequests, 1, DatasetLimits.RemoteRequests);
            Assert.InRange(remote.PeakRequests, 1, DatasetLimits.RemoteRequests);
        }
        foreach (int threads in (int[])[1, 16])
        {
            using var dataset = IpcDataset.Open("d/", new IpcDatasetOptions { Store = memory });
            using IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = threads });
            Assert.Equal(2000, Ids(reader.ReadBatches()).Count);
            Assert.Equal(0, reader.PeakRequests);
        }
        Assert.Throws<ArgumentOutOfRangeException>(() => IpcDataset.Open("d/", new IpcDatasetOptions { Store = memory, MaxRequestsInFlight = 0 }));
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(1, true)]
    [InlineData(8, false)]
    [InlineData(8, true)]
    public async Task StoppingEarly_WithRequestsWaiting_Returns(int threads, bool useAsync)
    {
        // Two places, one taken by an open that hangs until cancelled; the other serves the files before it, the opens
        // after them wait.
        var store = new RemoteStore(Files(40, 10, 10)) { Hanging = "d/f005.arrow" };
        using var dataset = await IpcDataset.OpenAsync("d/", new IpcDatasetOptions { Store = store, MaxRequestsInFlight = 2 });
        Task<int> read = Task.Run(async () =>
        {
            using IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = threads });
            int rows = 0;
            if (useAsync)
            {
                await foreach (RecordBatch batch in reader.ReadBatchesAsync())
                {
                    using (batch) rows += batch.Length;
                    if (rows >= 30) break;
                }
            }
            else
            {
                foreach (RecordBatch batch in reader.ReadBatches())
                {
                    using (batch) rows += batch.Length;
                    if (rows >= 30) break;
                }
            }
            return rows;
        });
        Assert.Same(read, await Task.WhenAny(read, Task.Delay(TimeSpan.FromSeconds(30))));
        Assert.Equal(30, await read);
    }
}
