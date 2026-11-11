using System.Net;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Kaminari.Seseragi.Lakehouse.Storage.Http;

/// <summary>
/// How a store's connections are made, as the object_store library's client options set them: a proxy for every
/// request (with hosts that bypass it and a CA certificate trusted for all connections), or no certificate checks.
/// </summary>
/// <param name="ProxyUrl">The proxy (<c>http://</c>, with optional <c>user:password@</c>), or null.</param>
/// <param name="ProxyCaPem">A CA certificate (PEM) trusted besides the system's, with a proxy only.</param>
/// <param name="ProxyExcludes">Hosts that bypass the proxy (comma-separated domains, IPs or networks).</param>
/// <param name="AllowInvalidCertificates">Whether servers' certificates go unchecked.</param>
internal sealed record HttpOptions(string? ProxyUrl, string? ProxyCaPem, string? ProxyExcludes, bool AllowInvalidCertificates)
{
    /// <summary>The system's proxy settings and certificate checks.</summary>
    public static readonly HttpOptions Default = new(null, null, null, false);

    /// <summary>The configuration keys these options come from (canonical client key names).</summary>
    public static readonly IReadOnlySet<string> Keys =
        new HashSet<string>(StringComparer.Ordinal) { "proxy_url", "proxy_ca_certificate", "proxy_excludes", "allow_invalid_certificates" };

    /// <summary>What tells option sets apart (for sharing clients and credentials).</summary>
    public string Key => string.Join('\0', ProxyUrl ?? "", ProxyCaPem ?? "", ProxyExcludes ?? "", AllowInvalidCertificates ? "insecure" : "");

    /// <summary>
    /// The options of a configuration (canonical keys), or null with the reason when the library would refuse them or
    /// this client cannot honour them (a proxy other than <c>http://</c>).
    /// </summary>
    public static HttpOptions? From(IReadOnlyDictionary<string, string> config, out string? reason)
    {
        reason = null;
        bool insecure = false;
        if (config.TryGetValue("allow_invalid_certificates", out string? flag))
        {
            if (ConfigValues.Bool(flag) is not { } value)
            {
                reason = "the setting 'allow_invalid_certificates' is not a boolean";
                return null;
            }
            insecure = value;
        }
        string? proxy = config.GetValueOrDefault("proxy_url");
        string? ca = null, excludes = null;
        if (proxy is not null)
        {
            if (!Uri.TryCreate(proxy, UriKind.Absolute, out Uri? uri) || uri.Scheme != "http")
            {
                reason = "the managed clients take http:// proxies only";
                return null;
            }
            ca = config.GetValueOrDefault("proxy_ca_certificate");
            if (ca is not null)
            {
                try
                {
                    using var certificate = X509Certificate2.CreateFromPem(ca);
                }
                catch (CryptographicException)
                {
                    reason = "the proxy CA certificate is not a PEM certificate";
                    return null;
                }
            }
            excludes = config.GetValueOrDefault("proxy_excludes");
        }
        return proxy is null && !insecure ? Default : new HttpOptions(proxy, ca, excludes, insecure);
    }

    /// <summary>The connection handler these options describe.</summary>
    internal SocketsHttpHandler Handler(TimeSpan connectTimeout)
    {
        var handler = new SocketsHttpHandler
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
            ConnectTimeout = connectTimeout,
            AutomaticDecompression = DecompressionMethods.None,
            AllowAutoRedirect = false,
        };
        if (ProxyUrl is not null)
        {
            handler.Proxy = new ExcludingProxy(ProxyUrl, ProxyExcludes);
            handler.UseProxy = true;
        }
        if (AllowInvalidCertificates)
        {
            handler.SslOptions.RemoteCertificateValidationCallback = static (_, _, _, _) => true;
        }
        else if (ProxyCaPem is not null)
        {
            X509Certificate2 authority = X509Certificate2.CreateFromPem(ProxyCaPem);
            handler.SslOptions.RemoteCertificateValidationCallback = (_, certificate, _, errors) =>
                errors == SslPolicyErrors.None
                || (errors == SslPolicyErrors.RemoteCertificateChainErrors && certificate is not null && IssuedBy(certificate, authority));
        }
        return handler;
    }

    // Trusts the extra CA besides the system's: only chain errors are forgiven, never a name mismatch.
    private static bool IssuedBy(X509Certificate certificate, X509Certificate2 authority)
    {
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(authority);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        using var leaf = new X509Certificate2(certificate);
        return chain.Build(leaf);
    }

    /// <summary>A proxy with the engine's bypass rules (reqwest's <c>NoProxy</c>).</summary>
    private sealed class ExcludingProxy : IWebProxy
    {
        private readonly Uri _proxy;
        private readonly List<(IPAddress Address, int Prefix)> _networks = [];
        private readonly List<string> _domains = [];

        public ExcludingProxy(string proxyUrl, string? excludes)
        {
            var uri = new Uri(proxyUrl);
            if (uri.UserInfo.Length > 0)
            {
                // The proxy's credentials travel as credentials, never in its address (which shows up in errors).
                string[] parts = uri.UserInfo.Split(':', 2);
                Credentials = new NetworkCredential(Uri.UnescapeDataString(parts[0]), parts.Length > 1 ? Uri.UnescapeDataString(parts[1]) : "");
                uri = new UriBuilder(uri) { UserName = "", Password = "" }.Uri;
            }
            _proxy = uri;
            foreach (string part in (excludes ?? "").Split(',').Select(p => p.Trim()))
            {
                if (part.Length == 0) continue;
                if (Network(part) is { } network) _networks.Add(network);
                else if (IPAddress.TryParse(part, out IPAddress? address)) _networks.Add((address, address.GetAddressBytes().Length * 8));
                else _domains.Add(part);
            }
        }

        public ICredentials? Credentials { get; set; }

        public Uri GetProxy(Uri destination) => _proxy;

        public bool IsBypassed(Uri host)
        {
            string name = host.Host.Trim('[', ']');
            if (IPAddress.TryParse(name, out IPAddress? address)) return _networks.Any(n => Contains(n, address));
            foreach (string d in _domains)
            {
                if (d.Equals(name, StringComparison.OrdinalIgnoreCase)
                    || (d.StartsWith('.') && d[1..].Equals(name, StringComparison.OrdinalIgnoreCase)))
                    return true;
                if (name.Length > d.Length && name.EndsWith(d, StringComparison.OrdinalIgnoreCase)
                    && (d.StartsWith('.') || name[name.Length - d.Length - 1] == '.'))
                    return true;
                if (d == "*") return true;
            }
            return false;
        }

        private static (IPAddress, int)? Network(string text)
        {
            int slash = text.IndexOf('/');
            if (slash < 0 || !IPAddress.TryParse(text[..slash], out IPAddress? address)
                || !int.TryParse(text[(slash + 1)..], out int prefix) || prefix < 0 || prefix > address.GetAddressBytes().Length * 8)
                return null;
            return (address, prefix);
        }

        private static bool Contains((IPAddress Address, int Prefix) network, IPAddress address)
        {
            if (network.Address.AddressFamily != address.AddressFamily)
            {
                if (address.IsIPv4MappedToIPv6) address = address.MapToIPv4();
                if (network.Address.AddressFamily != address.AddressFamily) return false;
            }
            byte[] a = network.Address.GetAddressBytes(), b = address.GetAddressBytes();
            int bits = network.Prefix;
            for (int i = 0; i < a.Length && bits > 0; i++, bits -= 8)
            {
                int mask = bits >= 8 ? 0xFF : (0xFF << (8 - bits)) & 0xFF;
                if ((a[i] & mask) != (b[i] & mask)) return false;
            }
            return true;
        }
    }
}
