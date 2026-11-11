using System.Collections.Concurrent;
using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Polars.CSharp;

namespace Polars.Integration.Tests;

/// <summary>
/// Hosts a real Kestrel server on loopback and streams Polars output into ASP.NET Core
/// response bodies (<c>HttpResponse.BodyWriter</c> / <c>HttpResponse.Body</c>).
/// </summary>
public sealed class KestrelFixture : IAsyncLifetime
{
    private WebApplication? _app;

    public HttpClient Client { get; private set; } = null!;

    /// <summary>Outcome of each request handler, keyed by request id (null = completed successfully).</summary>
    public ConcurrentDictionary<string, TaskCompletionSource<Exception?>> Outcomes { get; } = new();

    public static DataFrame SmallFrame() => DataFrame.FromColumns(new
    {
        id = new[] { 1, 2, 3, 4, 5 },
        name = new[] { "Alice", "Bob", null, "David", "Eve" },
        score = new[] { 1.5, 2.5, 3.5, 4.5, 5.5 },
    });

    public static DataFrame LargeFrame(int rows = 300_000) => DataFrame.FromColumns(new
    {
        id = Enumerable.Range(0, rows).ToArray(),
        text = Enumerable.Range(0, rows).Select(i => $"row-{i:D8}").ToArray(),
    });

    public async Task InitializeAsync()
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseKestrel(o => o.Listen(IPAddress.Loopback, 0));
        _app = builder.Build();

        // Records how each handler ended, so tests can assert on server-side behavior.
        _app.Use(async (ctx, next) =>
        {
            var id = ctx.Request.Query["id"].ToString();
            var tcs = id.Length > 0 ? Outcomes.GetOrAdd(id, _ => new(TaskCreationOptions.RunContinuationsAsynchronously)) : null;
            try
            {
                await next(ctx);
                tcs?.TrySetResult(null);
            }
            catch (Exception ex)
            {
                tcs?.TrySetResult(ex);
                if (!ctx.Response.HasStarted) ctx.Response.StatusCode = 500;
            }
        });

        _app.MapGet("/ipc-stream/pipe", async ctx =>
        {
            using var df = SmallFrame();
            ctx.Response.ContentType = "application/vnd.apache.arrow.stream";
            await df.WriteIpcStreamAsync(ctx.Response.BodyWriter, cancellationToken: ctx.RequestAborted);
        });

        _app.MapGet("/parquet/pipe", async ctx =>
        {
            using var df = LargeFrame();
            await df.Lazy().SinkParquetAsync(ctx.Response.BodyWriter, cancellationToken: ctx.RequestAborted);
        });

        _app.MapGet("/parquet/stream-async", async ctx =>
        {
            using var df = LargeFrame();
            await df.WriteParquetAsync(ctx.Response.Body, cancellationToken: ctx.RequestAborted);
        });

        _app.MapGet("/parquet/stream-sync", (HttpContext ctx) =>
        {
            using var df = SmallFrame();
            df.WriteParquet(ctx.Response.Body); // Kestrel disallows synchronous I/O by default
        });

        _app.MapGet("/csv/pipe", async ctx =>
        {
            using var df = LargeFrame();
            ctx.Response.ContentType = "text/csv";
            await df.Lazy().SinkCsvAsync(ctx.Response.BodyWriter, cancellationToken: ctx.RequestAborted);
        });

        _app.MapGet("/ndjson/pipe", async ctx =>
        {
            using var df = LargeFrame();
            await df.Lazy().SinkJsonAsync(ctx.Response.BodyWriter, cancellationToken: ctx.RequestAborted);
        });

        _app.MapGet("/ipc/huge", async ctx =>
        {
            using var df = LargeFrame(3_000_000);
            await df.Lazy().SinkIpcAsync(ctx.Response.BodyWriter, cancellationToken: ctx.RequestAborted);
        });

        await _app.StartAsync();

        var address = _app.Services.GetRequiredService<Microsoft.AspNetCore.Hosting.Server.IServer>()
                          .Features.Get<IServerAddressesFeature>()!.Addresses.First();
        Client = new HttpClient { BaseAddress = new Uri(address), Timeout = TimeSpan.FromMinutes(2) };
    }

    public async Task DisposeAsync()
    {
        Client.Dispose();
        if (_app is not null)
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }
    }

    public Task<Exception?> OutcomeOf(string id) =>
        Outcomes.GetOrAdd(id, _ => new(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
}

public class AspNetCoreStreamingTests(KestrelFixture server) : IClassFixture<KestrelFixture>
{
    private static string NewId() => Guid.NewGuid().ToString("N");

    [Fact]
    public async Task BodyWriter_IpcStream_MatchesMemory()
    {
        var id = NewId();
        byte[] body = await server.Client.GetByteArrayAsync($"/ipc-stream/pipe?id={id}");

        using var df = KestrelFixture.SmallFrame();
        Assert.Equal(df.WriteIpcStreamMemory(), body);
        Assert.Null(await server.OutcomeOf(id));
    }

    [Fact]
    public async Task BodyWriter_LargeParquet_RoundTrip()
    {
        var id = NewId();
        byte[] body = await server.Client.GetByteArrayAsync($"/parquet/pipe?id={id}");

        using var expected = KestrelFixture.LargeFrame();
        using var back = DataFrame.ReadParquet(body);
        Assert.True(expected.Equals(back));
        Assert.Null(await server.OutcomeOf(id));
    }

    [Fact]
    public async Task ResponseBody_AsyncStream_WorksWithoutSynchronousIO()
    {
        var id = NewId();
        byte[] body = await server.Client.GetByteArrayAsync($"/parquet/stream-async?id={id}");

        using var expected = KestrelFixture.LargeFrame();
        using var back = DataFrame.ReadParquet(body);
        Assert.True(expected.Equals(back));
        Assert.Null(await server.OutcomeOf(id));
    }

    [Fact]
    public async Task ResponseBody_SyncStream_SurfacesKestrelSynchronousIOError()
    {
        var id = NewId();
        using var response = await server.Client.GetAsync($"/parquet/stream-sync?id={id}");

        var ex = await server.OutcomeOf(id);
        Assert.IsType<InvalidOperationException>(ex); // the original Kestrel exception, not a wrapped native error
        Assert.Contains("Synchronous operations are disallowed", ex!.Message);
    }

    [Fact]
    public async Task BodyWriter_Csv_IsStreamedIncrementally()
    {
        var id = NewId();
        using var response = await server.Client.GetAsync($"/csv/pipe?id={id}", HttpCompletionOption.ResponseHeadersRead);
        response.EnsureSuccessStatusCode();
        Assert.Null(response.Content.Headers.ContentLength); // chunked: not buffered up-front

        using var df = KestrelFixture.LargeFrame();
        string[] expected = System.Text.Encoding.UTF8.GetString(df.Lazy().SinkCsvMemory()).Split('\n');

        // Consume line by line as it arrives.
        using var reader = new StreamReader(await response.Content.ReadAsStreamAsync());
        int lines = 0;
        while (await reader.ReadLineAsync() is { } line)
        {
            Assert.Equal(expected[lines], line);
            lines++;
        }
        Assert.Equal(300_000 + 1, lines);
        Assert.Null(await server.OutcomeOf(id));
    }

    [Fact]
    public async Task BodyWriter_NdJson_RoundTrip()
    {
        var id = NewId();
        byte[] body = await server.Client.GetByteArrayAsync($"/ndjson/pipe?id={id}");

        using var expected = KestrelFixture.LargeFrame();
        Assert.Equal(expected.Lazy().SinkJsonMemory(), body);
        Assert.Null(await server.OutcomeOf(id));
    }

    [Fact]
    public async Task ClientDisconnect_AbortsServerWriteQuickly_AndServerKeepsServing()
    {
        var id = NewId();
        using (var response = await server.Client.GetAsync($"/ipc/huge?id={id}", HttpCompletionOption.ResponseHeadersRead))
        {
            await using var stream = await response.Content.ReadAsStreamAsync();
            var buffer = new byte[64 * 1024];
            int read = 0;
            while (read < 256 * 1024)
            {
                int n = await stream.ReadAsync(buffer);
                if (n == 0) break;
                read += n;
            }
        } // dispose mid-body: client disconnects

        var outcome = server.OutcomeOf(id);
        var finished = await Task.WhenAny(outcome, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.Same(outcome, finished); // the native writer did not keep running / hang

        var ex = await outcome;
        Assert.True(ex is OperationCanceledException or IOException,
            $"expected cancellation or IO error, got {(ex is null ? "success" : ex.GetType().Name + ": " + ex.Message)}");

        // Server still healthy.
        var id2 = NewId();
        byte[] body = await server.Client.GetByteArrayAsync($"/ipc-stream/pipe?id={id2}");
        Assert.NotEmpty(body);
    }
}
