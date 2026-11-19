using System.Net.Http.Headers;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Network;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests;

/// <summary>
/// <see cref="HttpFileStore"/>: one ranged request learns a file's size and footer, later ranges require the version
/// first seen, servers ignoring ranges or compressing are handled or refused, nothing is listed and no HEAD is sent;
/// datasets of http(s) URLs read over a local server as they read from disk, keep large files' footers with their
/// versions, and fail naming a file changed between reads.
/// </summary>
public sealed class HttpFileStoreTests
{
    private static readonly HttpFileStoreOptions Quick = new() { Retry = new RetrySettings(3, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(5)) };

    private static byte[] Parquet(long first, int rows, bool large = false)
    {
        var schema = new Apache.Arrow.Schema([new Field("id", Int64Type.Default, true), new Field("s", StringType.Default, true)], null);
        var ids = new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => first + i)).Build();
        var s = new StringArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => $"v{first + i}")).Build();
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, new ParquetWriteOptions { RowGroupRows = Math.Max(1, rows / 4), Compression = large ? NoCompression.Instance : SnappyCompression.Instance }))
        {
            writer.Write(new RecordBatch(schema, [ids, s], rows));
            writer.Complete();
        }
        byte[] bytes = stream.ToArray();
        if (large) Assert.True(bytes.Length > ParquetMetadata.TailGuess);
        return bytes;
    }

    /// <summary>An Arrow IPC file of the same columns (four batches; <paramref name="large"/>: uncompressed, beyond the first tail read).</summary>
    private static byte[] Ipc(long first, int rows, bool large = false)
    {
        var schema = new Apache.Arrow.Schema([new Field("id", Int64Type.Default, true), new Field("s", StringType.Default, true)], null);
        using var stream = new MemoryStream();
        using (var writer = Kaminari.Seseragi.Files.Ipc.IpcWriter.Create(stream, schema,
            new Kaminari.Seseragi.Files.Ipc.IpcWriteOptions { Compression = large ? NoCompression.Instance : new ZstandardCompression() }))
        {
            int batchRows = Math.Max(1, rows / 4);
            for (long at = first; at < first + rows; at += batchRows)
            {
                int n = (int)Math.Min(batchRows, first + rows - at);
                using var batch = new RecordBatch(schema,
                [
                    new Int64Array.Builder().AppendRange(Enumerable.Range(0, n).Select(i => at + i)).Build(),
                    new StringArray.Builder().AppendRange(Enumerable.Range(0, n).Select(i => $"v{at + i}")).Build(),
                ], n);
                writer.Write(batch);
            }
            writer.Complete();
        }
        byte[] bytes = stream.ToArray();
        Assert.True(large == bytes.Length > Kaminari.Seseragi.Files.Ipc.IpcFile.TailGuess, $"{bytes.Length} bytes");
        return bytes;
    }

    private static byte[] File(string format, long first, int rows, bool large = false) => format == "ipc" ? Ipc(first, rows, large) : Parquet(first, rows, large);

    /// <summary>A dataset of either format: its ids read at some threads, and its footers kept.</summary>
    private sealed class Opened(IDisposable dataset) : IDisposable
    {
        public static Opened Of(string format, string[] paths, IFileStore? store = null) => format == "ipc"
            ? new(Kaminari.Seseragi.Files.Ipc.IpcDataset.Open(paths, new Kaminari.Seseragi.Files.Ipc.IpcDatasetOptions { Store = store }))
            : new(ParquetDataset.Open(paths, new ParquetDatasetOptions { Store = store }));

        public int Footers => dataset switch { ParquetDataset p => p.Footers.Count, Kaminari.Seseragi.Files.Ipc.IpcDataset i => i.Footers.Count, _ => 0 };

        public List<long> Ids(int threads)
        {
            if (dataset is ParquetDataset parquet) return HttpFileStoreTests.Ids(parquet, threads);
            using var reader = ((Kaminari.Seseragi.Files.Ipc.IpcDataset)dataset).OpenReader(new Kaminari.Seseragi.Files.Ipc.IpcReadOptions { MaxDegreeOfParallelism = threads });
            List<long> ids = [];
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch) ids.AddRange(((Int64Array)batch.Column(0)).Values.ToArray());
            }
            return ids;
        }

        public void Dispose() => dataset.Dispose();
    }

    public static TheoryData<string> Formats => ["parquet", "ipc"];

    private static byte[] Bytes(int n) => [.. Enumerable.Range(0, n).Select(i => (byte)(i * 31 + 7))];

    private static (HttpTestServer Server, HttpFileStore Store) Serve(HttpFileStoreOptions? options = null)
    {
        var server = new HttpTestServer();
        return (server, new HttpFileStore(new HttpClient(server), options ?? Quick));
    }

    private static string Range(HttpRequestMessage request) => request.Headers.TryGetValues("Range", out var values) ? values.Single() : "";

    [Fact]
    public async Task OpeningAFile_AsksForItsTailOnce_AndServesItFromMemory()
    {
        var (server, store) = Serve();
        byte[] file = Bytes(200_000);
        server.Put("/f", file);
        IRandomAccess source = (await store.OpenAsync("http://h/f"))!;
        Assert.Equal(file.Length, source.Length);
        Assert.Equal(1, server.Requests);
        Assert.Equal($"bytes=-{HttpFileStore.TailRequest}", Range(server.Seen.Single()));
        byte[] buffer = new byte[100];
        Assert.Equal(100, await source.ReadAtAsync(file.Length - 100, buffer, default));
        Assert.Equal(file[^100..], buffer);
        Assert.Equal(1, server.Requests); // from the tail
        Assert.Equal(100, source.ReadAt(10, buffer));
        Assert.Equal(file[10..110], buffer);
        Assert.Equal(2, server.Requests);
        HttpRequestMessage ranged = server.Seen.Last();
        Assert.Equal("bytes=10-109", Range(ranged));
        Assert.True(ranged.Headers.TryGetValues("If-Match", out _)); // the version first seen
        Assert.Equal(0, await source.ReadAtAsync(file.Length, buffer, default)); // past the end: nothing asked
        Assert.Equal(2, server.Requests);
        Assert.All(server.Seen, r => Assert.Equal(HttpMethod.Get, r.Method)); // never HEAD
        Assert.All(server.Seen, r => Assert.Contains("identity", r.Headers.AcceptEncoding.ToString()));
    }

    /// <summary>
    /// A server without suffix ranges (Azure Blob Storage among them) is asked for explicit ones: the first bytes (with
    /// the size; a small file whole), then the last; remembered by host, so later files go there at once.
    /// </summary>
    [Theory]
    [InlineData("500")]
    [InlineData("ignore")]
    [InlineData("416")]
    public async Task AServerWithoutSuffixRanges_IsAskedForExplicitOnes(string refusal)
    {
        var (server, store) = Serve();
        server.NoSuffix = refusal;
        byte[] large = Bytes(300_000), small = Bytes(1000);
        server.Put("/large", large);
        server.Put("/small", small);
        server.Put("/large2", large);
        IRandomAccess source = (await store.OpenAsync("http://h/large"))!;
        Assert.Equal(large.Length, source.Length);
        Assert.Equal(3, server.Requests); // the suffix (not retried), the head, the tail
        byte[] buffer = new byte[100];
        Assert.Equal(100, await source.ReadAtAsync(large.Length - 100, buffer, default));
        Assert.Equal(large[^100..], buffer);
        Assert.Equal(3, server.Requests); // from the tail
        IRandomAccess whole = (await store.OpenAsync("http://h/small"))!;
        Assert.Equal(4, server.Requests); // the head held it whole; no suffix asked again on this host
        Assert.Equal(1000, whole.ReadAt(0, new byte[1000]));
        Assert.Equal(4, server.Requests);
        Assert.Equal(large.Length, (await store.OpenAsync("http://h/large2"))!.Length);
        Assert.Equal(6, server.Requests);
        Assert.Null(await store.OpenAsync("http://h/missing"));
    }

    [Fact]
    public async Task SmallAndEmptyAndMissingFiles()
    {
        var (server, store) = Serve();
        byte[] small = Bytes(500);
        server.Put("/small", small);
        server.Put("/empty", []);
        IRandomAccess source = (await store.OpenAsync("http://h/small"))!;
        Assert.Equal(500, source.Length);
        byte[] all = new byte[500];
        Assert.Equal(500, await source.ReadAtAsync(0, all, default));
        Assert.Equal(small, all);
        Assert.Equal(1, server.Requests); // the tail held it whole
        Assert.Equal(0, (await store.OpenAsync("http://h/empty"))!.Length);
        Assert.Null(await store.OpenAsync("http://h/missing"));
        Assert.Null(await store.StatAsync("http://h/missing"));
        StoredFile stat = (await store.StatAsync("http://h/small"))!.Value;
        Assert.Equal(500, stat.Size);
        Assert.NotNull(stat.ETag);
        Assert.False(store.CanList);
        Assert.Throws<NotSupportedException>(() => store.ListAsync("http://h/", recursive: true));
    }

    [Fact]
    public async Task AServerIgnoringRanges_GivesTheWholeFile_WithinTheLimit()
    {
        var (server, store) = Serve(Quick with { WholeFileLimit = 300_000 });
        server.IgnoreRanges = true;
        byte[] file = Bytes(250_000);
        server.Put("/f", file);
        IRandomAccess opened = (await store.OpenAsync("http://h/f"))!;
        byte[] part = new byte[1000];
        Assert.Equal(1000, opened.ReadAt(5000, part));
        Assert.Equal(file[5000..6000], part);
        // Opened with its size known, the first ranged read gets the whole file: kept for the next reads.
        IRandomAccess known = await store.OpenAsync(new StoredFile("http://h/f", file.Length));
        int before = server.Requests;
        Assert.Equal(1000, await known.ReadAtAsync(100_000, part, default));
        Assert.Equal(file[100_000..101_000], part);
        Assert.Equal(1000, await known.ReadAtAsync(7, part, default));
        Assert.Equal(before + 1, server.Requests);

        server.Put("/big", Bytes(400_000));
        var error = await Assert.ThrowsAnyAsync<IOException>(async () => await store.OpenAsync("http://h/big"));
        Assert.Contains("WholeFileLimit", error.Message);
    }

    [Theory]
    [InlineData("strong")]
    [InlineData("date")]
    public async Task AFileChangedWhileRead_Fails(string validators)
    {
        var (server, store) = Serve();
        server.Validators = validators;
        server.Put("/f", Bytes(200_000));
        IRandomAccess source = (await store.OpenAsync("http://h/f"))!;
        server.Put("/f", Bytes(200_000)); // a new version
        var error = await Assert.ThrowsAnyAsync<IOException>(async () => await source.ReadAtAsync(0, new byte[10], default));
        Assert.Contains("changed while it was read", error.Message);
    }

    /// <summary>A server that does not check the version asked for answers another one: the change is noticed all the same.</summary>
    [Fact]
    public async Task AServerNotCheckingVersions_IsCheckedByTheClient()
    {
        var (server, store) = Serve();
        server.CheckConditions = false;
        server.Put("/f", Bytes(200_000));
        IRandomAccess source = (await store.OpenAsync("http://h/f"))!;
        server.Put("/f", Bytes(200_000));
        var error = await Assert.ThrowsAnyAsync<IOException>(async () => await source.ReadAtAsync(0, new byte[10], default));
        Assert.Contains("changed while it was read", error.Message);
    }

    /// <summary>Opened with its size only (a listing's), a file takes its version from the first answer and requires it after.</summary>
    [Fact]
    public async Task AFileOpenedWithoutAVersion_TakesTheFirstAnswers()
    {
        var (server, store) = Serve();
        server.Put("/f", Bytes(200_000));
        IRandomAccess source = await store.OpenAsync(new StoredFile("http://h/f", 200_000));
        Assert.Equal(10, await source.ReadAtAsync(0, new byte[10], default));
        Assert.False(server.Seen.Last().Headers.Contains("If-Match"));
        server.Put("/f", Bytes(200_000));
        var error = await Assert.ThrowsAnyAsync<IOException>(async () => await source.ReadAtAsync(100, new byte[10], default));
        Assert.Contains("changed while it was read", error.Message);
    }

    [Fact]
    public async Task WeakOrNoValidators_AreNotRequired()
    {
        foreach (string validators in (string[])["weak", "none"])
        {
            var (server, store) = Serve();
            server.Validators = validators;
            server.Put("/f", Bytes(200_000));
            IRandomAccess source = (await store.OpenAsync("http://h/f"))!;
            server.Put("/f", Bytes(200_000));
            Assert.Equal(10, await source.ReadAtAsync(0, new byte[10], default)); // nothing to check against
            HttpRequestMessage last = server.Seen.Last();
            Assert.False(last.Headers.Contains("If-Match"));
            Assert.Null(last.Headers.IfUnmodifiedSince);
        }
    }

    [Fact]
    public async Task CompressedAnswers_AreRefused_FailuresRetried_HeadersSent()
    {
        var (server, store) = Serve(Quick with { Headers = new Dictionary<string, string> { ["X-Token"] = "secret" } });
        server.Put("/f", Bytes(1000));
        server.FailNext = 2;
        Assert.Equal(1000, (await store.OpenAsync("http://h/f"))!.Length);
        Assert.Equal(3, server.Requests);
        Assert.All(server.Seen, r => Assert.Equal("secret", r.Headers.GetValues("X-Token").Single()));
        server.Compress = true;
        var error = await Assert.ThrowsAnyAsync<IOException>(async () => await store.OpenAsync("http://h/f"));
        Assert.Contains("compressed", error.Message);
        // A file gone between reads.
        server.Compress = false;
        IRandomAccess source = await store.OpenAsync(new StoredFile("http://h/gone", 1000));
        await Assert.ThrowsAsync<FileNotFoundException>(async () => await source.ReadAtAsync(0, new byte[10], default));
    }

    // ---- datasets over a local server -----------------------------------------------------------------------------

    private static List<long> Ids(ParquetDataset dataset, int threads)
    {
        using var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = threads });
        List<long> ids = [];
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch) ids.AddRange(((Int64Array)batch.Column(0)).Values.ToArray());
        }
        return ids;
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void ADatasetOfUrls_ReadsOverHttp_AsFromDisk(string format)
    {
        var server = new HttpTestServer();
        for (int f = 0; f < 6; f++) server.Put($"/data/f{f}.{format}", File(format, f * 10_000, 10_000, large: f % 2 == 0));
        using HttpTestServer.Listening listening = server.Listen();
        string[] urls = [.. Enumerable.Range(0, 6).Select(f => $"{listening.BaseUrl}/data/f{f}.{format}")];
        using Opened dataset = Opened.Of(format, urls); // no store given: http(s) URLs are read by an HttpFileStore
        foreach (int threads in (int[])[1, 8])
            Assert.Equal(Enumerable.Range(0, 60_000).Select(i => (long)i), dataset.Ids(threads));
    }

    /// <summary>
    /// Large files' footers are kept with their sizes and versions: a later read opens them without a request; a
    /// small file comes whole in one request each time. A file replaced between reads fails, naming it.
    /// </summary>
    [Theory]
    [MemberData(nameof(Formats))]
    public void KeptFooters_SpareRequests_AndAFileReplacedFails(string format)
    {
        var server = new HttpTestServer();
        server.Put($"/d/a.{format}", File(format, 0, 20_000, large: true));
        server.Put($"/d/b.{format}", File(format, 20_000, 20_000, large: true));
        server.Put($"/d/c.{format}", File(format, 40_000, 100));
        var store = new HttpFileStore(new HttpClient(server), Quick);
        using Opened dataset = Opened.Of(format, [$"http://h/d/a.{format}", $"http://h/d/b.{format}", $"http://h/d/c.{format}"], store);
        Assert.Equal(40_100, dataset.Ids(4).Count);
        int Tails() => server.Seen.Count(r => Range(r).StartsWith("bytes=-", StringComparison.Ordinal));
        int tails = Tails();
        Assert.Equal(40_100, dataset.Ids(4).Count);
        Assert.Equal(tails + 1, Tails()); // only the small file's (whole) read
        Assert.Equal(2, dataset.Footers);

        server.Put($"/d/b.{format}", File(format, 20_000, 20_000, large: true)); // replaced: another version
        var error = Assert.ThrowsAny<IOException>(() => dataset.Ids(4));
        Assert.Contains($"http://h/d/b.{format}", error.Message);
        Assert.Contains("changed while it was read", error.Message);
    }

    [Fact]
    public void UrlsWithoutAStore_AreReadByTheirSchemesStores()
    {
        var error = Assert.Throws<ArgumentException>(() => ParquetDataset.Open("s3://bucket/data/*.parquet"));
        Assert.Contains("CloudFileStore.Open", error.Message);
        error = Assert.Throws<ArgumentException>(() => ParquetDataset.Open(["/tmp/a.parquet", "https://host/b.parquet"]));
        Assert.Contains("different stores", error.Message);
        // hf:// goes to the Hub's store, which checks the form of its paths.
        error = Assert.Throws<ArgumentException>(() => ParquetDataset.Open("hf://models/x/y/z.parquet"));
        Assert.Contains("hf://datasets/", error.Message);
        // A plain server cannot list: directories are refused (an http(s) URL is never a glob: '?' starts its query).
        Assert.Throws<NotSupportedException>(() => ParquetDataset.Open("https://host/data/"));
    }

    // ---- Hugging Face ---------------------------------------------------------------------------------------------

    [Fact]
    public void HuggingFace_ListsThroughTheTreeApi_PageByPage_AndReadsResolveUrls()
    {
        var server = new HttpTestServer();
        const string rev = "refs%2Fconvert%2Fparquet";
        server.Put("/datasets/me/repo/resolve/refs/convert/parquet/data/a.parquet", Parquet(0, 300));
        server.Put("/datasets/me/repo/resolve/refs/convert/parquet/data/sub/b.parquet", Parquet(300, 10_000, large: true));
        long sizeB = Parquet(300, 10_000, large: true).Length;
        server.Route($"/api/datasets/me/repo/tree/{rev}/data?recursive=true", _ => HttpTestServer.Json(
            """[{"type":"directory","path":"data/sub"},{"type":"file","path":"data/a.parquet","size":""" + Parquet(0, 300).Length + "}]",
            $"<http://hub/api/datasets/me/repo/tree/{rev}/data?recursive=true&cursor=2>; rel=\"next\""));
        server.Route($"/api/datasets/me/repo/tree/{rev}/data?recursive=true&cursor=2", _ => HttpTestServer.Json(
            """[{"type":"file","path":"data/sub/b.parquet","size":134,"lfs":{"oid":"x","size":""" + sizeB + ""","pointerSize":134}},{"type":"file","path":"data/readme.md","size":5}]"""));
        var hub = new HuggingFaceFileStore("tok", new HttpClient(server), Quick, endpoint: "http://hub");
        using var dataset = ParquetDataset.Open($"hf://datasets/me/repo@{rev}/data/**/*.parquet", new ParquetDatasetOptions { Store = hub });
        Assert.Equal([$"hf://datasets/me/repo@{rev}/data/a.parquet", $"hf://datasets/me/repo@{rev}/data/sub/b.parquet"], dataset.Files.Select(f => f.Path));
        Assert.Equal(sizeB, dataset.Files[1].Size);
        Assert.Equal(Enumerable.Range(0, 10_300).Select(i => (long)i), Ids(dataset, 4));
        Assert.All(server.Seen, r => Assert.Equal(new AuthenticationHeaderValue("Bearer", "tok").ToString(), r.Headers.GetValues("Authorization").Single()));
        Assert.Equal($"http://hub/datasets/me/repo/resolve/{rev}/data/a.parquet", hub.Resolve($"hf://datasets/me/repo@{rev}/data/a.parquet"));
        Assert.Equal("http://hub/datasets/me/repo/resolve/main/x%20y.parquet", hub.Resolve("hf://datasets/me/repo/x y.parquet"));
    }

    [Fact]
    public void HuggingFace_TellsDirectoriesByTheTreeApi()
    {
        var server = new HttpTestServer();
        server.Route("/api/datasets/me/repo/tree/main/data?recursive=false", _ => HttpTestServer.Json("[]"));
        var hub = new HuggingFaceFileStore("", new HttpClient(server), Quick, endpoint: "http://hub");
        Assert.True(hub.IsDirectory("hf://datasets/me/repo/data"));
        Assert.True(hub.IsDirectory("hf://datasets/me/repo"));
        Assert.False(hub.IsDirectory("hf://datasets/me/repo/data/a.parquet"));
    }

    /// <summary>
    /// The real Hub (only with <c>SESERAGI_NETWORK_TESTS=1</c>): a public dataset's Parquet conversion (one small file),
    /// and files beyond the first tail read, read twice at several threads (ranges through the Hub's redirect to its CDN,
    /// the kept footer's version required).
    /// </summary>
    [Fact]
    [Trait("Category", "Network")]
    public void HuggingFace_ThePublicHub()
    {
        if (Environment.GetEnvironmentVariable("SESERAGI_NETWORK_TESTS") != "1") return;
        using var dataset = ParquetDataset.Open("hf://datasets/scikit-learn/iris@refs%2Fconvert%2Fparquet/default/train/*.parquet");
        using var reader = dataset.OpenReader();
        long rows = 0;
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch) rows += batch.Length;
        }
        Assert.Equal(150, rows);

        using var glue = ParquetDataset.Open("hf://datasets/nyu-mll/glue/cola/*.parquet");
        Assert.Contains(glue.Files, f => f.Size > ParquetMetadata.TailGuess);
        for (int round = 0; round < 2; round++)
        {
            using var read = glue.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = 8 });
            long count = 0;
            foreach (RecordBatch batch in read.ReadBatches())
            {
                using (batch) count += batch.Length;
            }
            Assert.Equal(10_657, count);
        }
    }
}
