using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace Polars.Integration.Tests.Fixtures;

/// <summary>
/// A Google Cloud Storage emulator (fake-gcs-server) with one bucket, preloaded with the Delta sample tables of
/// Seseragi.Lakehouse.Tests under <see cref="Prefix"/>.
/// </summary>
public class FakeGcsFixture : IAsyncLifetime
{
    private const int Port = 4443;
    private readonly IContainer _container;

    public const string BucketName = "seseragi";
    public const string Prefix = "tables";

    private readonly int _hostPort;

    public FakeGcsFixture()
    {
        // The server answers the XML API (which the object store client uses to list and read) only for requests
        // whose Host is its public host, so the host port is fixed before the start and named as that host.
        using (var probe = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0))
        {
            probe.Start();
            _hostPort = ((System.Net.IPEndPoint)probe.LocalEndpoint).Port;
        }
        var builder = new ContainerBuilder("fsouza/fake-gcs-server:latest")
            .WithPortBinding(_hostPort, Port)
            .WithCommand("-scheme", "http", "-port", Port.ToString(), "-backend", "memory", "-public-host", $"127.0.0.1:{_hostPort}")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(Port).ForPath("/storage/v1/b")));
        // Every folder under /data/<bucket> is loaded as objects of that bucket when the server starts.
        foreach (string table in SampleTables.Names)
            builder = builder.WithResourceMapping(new DirectoryInfo(Path.Combine(SampleTables.Root, table)), $"/data/{BucketName}/{Prefix}/{table}/");
        _container = builder.Build();
    }

    public string BaseUrl => $"http://127.0.0.1:{_hostPort}";

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

/// <summary>The Delta sample tables vendored in Seseragi.Lakehouse.Tests.</summary>
public static class SampleTables
{
    /// <summary>
    /// Tables that cover the object store paths: deletion vectors read by range, a checkpoint found through
    /// _last_checkpoint, partition directories, Parquet sidecars, and file names with a literal '%'.
    /// </summary>
    public static readonly string[] Names = ["table-with-dv-small", "with-short-dv", "app-txn-checkpoint", "v2-parquet-sidecars-struct-stats-only"];

    public static string Root
    {
        get
        {
            var dir = new DirectoryInfo(AppContext.BaseDirectory);
            while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Polars.NET.slnx"))) dir = dir.Parent;
            if (dir is null) throw new InvalidOperationException("Repository root not found.");
            return Path.Combine(dir.FullName, "tests", "Seseragi.Lakehouse.Tests", "TestData");
        }
    }
}
