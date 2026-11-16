using System.Net;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Kaminari.Sazanami.Network;

namespace Kaminari.Seseragi.Files.IO;

/// <summary>
/// Files of Hugging Face Hub repositories, named <c>hf://datasets/{owner}/{repo}[@{revision}]/{path}</c> (or
/// <c>hf://spaces/…</c>): listed through the Hub's tree API (globs and directories work), read by ranged GETs of their
/// <c>resolve</c> URLs (see <see cref="HttpFileStore"/>). The revision defaults to <c>main</c> and is written as the Hub
/// spells it in URLs (<c>refs%2Fconvert%2Fparquet</c>).
/// </summary>
/// <remarks>
/// The token, when there is one, is the one given, else <c>HF_TOKEN</c>, else the file <c>token</c> in <c>HF_HOME</c>
/// (by default <c>~/.cache/huggingface</c>); public repositories need none. The endpoint is the one given, else
/// <c>HF_ENDPOINT</c>, else <c>https://huggingface.co</c> (a mirror may stand in).
/// </remarks>
public sealed class HuggingFaceFileStore : IFileStore, IDirectoryAware
{
    private const string Scheme = "hf://";

    private readonly string _endpoint;
    private readonly HttpFileStore _files;
    private readonly HttpSender _api;
    private readonly string? _token;

    /// <param name="token">The access token; null to look for one (see the remarks).</param>
    /// <param name="client">The HTTP client; null for a shared one.</param>
    /// <param name="options">Retries, extra headers, the whole-file limit.</param>
    /// <param name="endpoint">The Hub's address; null for <c>HF_ENDPOINT</c> or <c>https://huggingface.co</c>.</param>
    public HuggingFaceFileStore(string? token = null, HttpClient? client = null, HttpFileStoreOptions? options = null, string? endpoint = null)
    {
        options ??= new HttpFileStoreOptions();
        _token = token ?? FindToken();
        _endpoint = (endpoint ?? (Environment.GetEnvironmentVariable("HF_ENDPOINT") is { Length: > 0 } env ? env : "https://huggingface.co")).TrimEnd('/');
        Dictionary<string, string> headers = options.Headers is { } given ? new(given, StringComparer.OrdinalIgnoreCase) : new(StringComparer.OrdinalIgnoreCase);
        if (_token is not null) headers["Authorization"] = $"Bearer {_token}";
        HttpFileStoreOptions withToken = options with { Headers = headers };
        _files = new HttpFileStore(client, withToken);
        _api = new HttpSender(client ?? HttpFileStore.Shared, options.Retry);
        Headers = headers;
    }

    private IReadOnlyDictionary<string, string> Headers { get; }

    /// <inheritdoc/>
    public bool CanList => true;

    /// <summary>The files below a directory of a repository (<c>hf://datasets/{owner}/{repo}[@{revision}]/{directory}</c>).</summary>
    public async IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        HubPath at = Parse(prefix, allowRepositoryPrefix: true);
        string? url = Tree(at, at.Path.TrimEnd('/'), recursive);
        while (url is not null)
        {
            string page = url;
            var (entries, next) = await _api.SendAsync<(List<(string Path, long Size)> Entries, string? Next)>(() => Get(page), async (response, _, ct) =>
            {
                if (response.StatusCode == HttpStatusCode.NotFound) return ([], null);
                if (!response.IsSuccessStatusCode)
                    throw new HttpAnswerException($"{prefix}: listing failed: HTTP {(int)response.StatusCode} {response.ReasonPhrase}.");
                Stream body = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
                await using (body.ConfigureAwait(false))
                {
                    using JsonDocument json = await JsonDocument.ParseAsync(body, cancellationToken: ct).ConfigureAwait(false);
                    return (Files(json.RootElement), NextPage(response));
                }
            }, cancellationToken: cancellationToken).ConfigureAwait(false);
            foreach (var (path, size) in entries) yield return new StoredFile($"{at.Repository}/{path}", size);
            url = next;
        }
    }

    /// <summary>Whether <paramref name="path"/> is a directory of its repository (asked of the tree API).</summary>
    public bool IsDirectory(string path)
    {
        if (!path.StartsWith(Scheme, StringComparison.Ordinal)) return false;
        HubPath at = Parse(path, allowRepositoryPrefix: true);
        if (at.Path.Length == 0) return true; // the repository's root
        string url = Tree(at, at.Path.TrimEnd('/'), recursive: false);
        return _api.Send(() => Get(url), static (response, _) => response.IsSuccessStatusCode);
    }

    /// <inheritdoc/>
    public async ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) =>
        await _files.StatAsync(Resolve(path), cancellationToken).ConfigureAwait(false) is { } stored ? stored with { Path = path } : null;

    /// <inheritdoc/>
    public ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default) =>
        _files.OpenAsync(file with { Path = Resolve(file.Path) }, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default) =>
        _files.OpenAsync(Resolve(path), cancellationToken);

    /// <summary>The <c>resolve</c> URL of a file.</summary>
    internal string Resolve(string path)
    {
        HubPath at = Parse(path, allowRepositoryPrefix: false);
        return $"{_endpoint}/{at.Kind}/{at.Name}/resolve/{at.Revision}/{Escape(at.Path)}";
    }

    /// <summary>The first page of the tree API under a directory of the repository.</summary>
    private string Tree(HubPath at, string directory, bool recursive) =>
        $"{_endpoint}/api/{at.Kind}/{at.Name}/tree/{at.Revision}{(directory.Length == 0 ? "" : "/" + Escape(directory))}?recursive={(recursive ? "true" : "false")}";

    private HttpRequestMessage Get(string url)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, url);
        foreach (var (name, value) in Headers) request.Headers.TryAddWithoutValidation(name, value);
        return request;
    }

    /// <summary>The files of a page of the tree API (directories left out): paths in the repository, sizes.</summary>
    private static List<(string Path, long Size)> Files(JsonElement page)
    {
        List<(string, long)> files = [];
        if (page.ValueKind != JsonValueKind.Array) throw new HttpAnswerException("The Hub's tree API answered something else than a list of entries.");
        foreach (JsonElement entry in page.EnumerateArray())
        {
            if (!entry.TryGetProperty("type", out JsonElement type) || type.GetString() != "file") continue;
            string path = entry.GetProperty("path").GetString() ?? throw new HttpAnswerException("A tree entry has no path.");
            long size = entry.TryGetProperty("lfs", out JsonElement lfs) && lfs.ValueKind == JsonValueKind.Object && lfs.TryGetProperty("size", out JsonElement lfsSize)
                ? lfsSize.GetInt64()
                : entry.GetProperty("size").GetInt64();
            files.Add((path, size));
        }
        return files;
    }

    /// <summary>The next page's URL from a <c>Link</c> header (<c>&lt;url&gt;; rel="next"</c>), if any.</summary>
    internal static string? NextPage(HttpResponseMessage response)
    {
        if (!response.Headers.TryGetValues("Link", out IEnumerable<string>? links)) return null;
        foreach (string header in links)
        {
            foreach (string link in header.Split(','))
            {
                int open = link.IndexOf('<'), close = link.IndexOf('>');
                if (open >= 0 && close > open && link.AsSpan(close).Contains("rel=\"next\"", StringComparison.Ordinal)) return link[(open + 1)..close];
            }
        }
        return null;
    }

    private static string Escape(string path) => string.Join('/', path.Split('/').Select(Uri.EscapeDataString));

    /// <summary>A path of the Hub: the kind of repository, its name, the revision, the path in it; and how it was spelled up to the path.</summary>
    private readonly record struct HubPath(string Kind, string Name, string Revision, string Path, string Repository);

    /// <exception cref="ArgumentException">Not a path of a dataset or space repository.</exception>
    private static HubPath Parse(string path, bool allowRepositoryPrefix)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        if (!path.StartsWith(Scheme, StringComparison.Ordinal)) throw new ArgumentException($"'{path}' is not an hf:// path.", nameof(path));
        string[] parts = path[Scheme.Length..].Split('/', 4);
        if (parts.Length < 3 || parts[0] is not ("datasets" or "spaces") || parts[1].Length == 0 || parts[2].Length == 0)
            throw new ArgumentException($"'{path}' is not of the form hf://datasets/{{owner}}/{{repo}}[@{{revision}}]/{{path}} (or hf://spaces/…).", nameof(path));
        string repo = parts[2], revision = "main";
        int at = repo.IndexOf('@');
        if (at >= 0)
        {
            revision = repo[(at + 1)..];
            repo = repo[..at];
            if (revision.Length == 0 || repo.Length == 0) throw new ArgumentException($"'{path}' has an empty repository or revision.", nameof(path));
        }
        string inner = parts.Length == 4 ? parts[3] : "";
        if (inner.Length == 0 && !allowRepositoryPrefix) throw new ArgumentException($"'{path}' names a repository, not a file in it.", nameof(path));
        return new HubPath(parts[0], $"{parts[1]}/{repo}", revision, inner, $"{Scheme}{parts[0]}/{parts[1]}/{parts[2]}");
    }

    /// <summary><c>HF_TOKEN</c>, else the token file of <c>HF_HOME</c> (default <c>~/.cache/huggingface</c>), else none.</summary>
    private static string? FindToken()
    {
        if (Environment.GetEnvironmentVariable("HF_TOKEN") is { Length: > 0 } token) return token.Trim();
        string home = Environment.GetEnvironmentVariable("HF_HOME") is { Length: > 0 } hf
            ? hf
            : Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".cache", "huggingface");
        string file = Path.Combine(home, "token");
        try
        {
            return File.Exists(file) && File.ReadAllText(file).Trim() is { Length: > 0 } stored ? stored : null;
        }
        catch (IOException)
        {
            return null;
        }
        catch (UnauthorizedAccessException)
        {
            return null;
        }
    }
}
