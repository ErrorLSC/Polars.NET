using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace Polars.Integration.Tests.Fixtures;

/// <summary>
/// An open-source Unity Catalog server (0.7.0) in a container, with managed tables on local storage. The storage is a
/// host directory mounted at the same path inside the container, so the <c>file://</c> locations the catalog hands out
/// are valid for the host's writers (the kernel, Spark) too.
/// </summary>
public sealed class UnityCatalogFixture : IAsyncLifetime
{
    private const string Image = "unitycatalog/unitycatalog:v0.7.0";
    private const int Port = 8080;

    private readonly IContainer _container;

    public UnityCatalogFixture()
    {
        Storage = Path.Combine(Path.GetTempPath(), $"seseragi_uc_{Guid.NewGuid():N}");
        Directory.CreateDirectory(Storage);
        // The server writes there under the container's user.
        if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(Storage, (UnixFileMode)0b111_111_111);
        _container = new ContainerBuilder(Image)
            .WithPortBinding(Port, true)
            // :Z relabels the directory for SELinux (Fedora, podman).
            .WithCreateParameterModifier(p => (p.HostConfig ??= new()).Binds = [$"{Storage}:{Storage}:Z"])
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(Port).ForPath("/api/2.1/unity-catalog/catalogs")))
            .Build();
    }

    /// <summary>The host directory the catalog keeps managed tables under.</summary>
    public string Storage { get; }

    /// <summary>The server's URL (no token needed).</summary>
    public string Url => $"http://{_container.Hostname}:{_container.GetMappedPublicPort(Port)}";

    /// <summary>A new catalog with a schema <c>s</c>, its managed tables stored under <see cref="Storage"/>.</summary>
    public async Task<string> NewCatalogAsync()
    {
        string name = $"c{Guid.NewGuid():N}"[..12];
        using var http = new HttpClient { BaseAddress = new Uri(Url) };
        await Post("/api/2.1/unity-catalog/catalogs", $$"""{"name":"{{name}}","storage_root":"file://{{Storage}}/{{name}}"}""");
        await Post("/api/2.1/unity-catalog/schemas", $$"""{"name":"s","catalog_name":"{{name}}"}""");
        return name;

        async Task Post(string path, string json) =>
            (await http.PostAsync(path, new StringContent(json, Encoding.UTF8, "application/json"))).EnsureSuccessStatusCode();
    }

    public Task InitializeAsync() => _container.StartAsync();

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
        try
        {
            Directory.Delete(Storage, true);
        }
        catch (IOException)
        {
            // Files the container wrote may belong to another user: left for the temp cleanup.
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
