using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Seseragi.Lakehouse.Tests;

/// <summary>A throwaway certificate authority and server certificates it issues, for https stand-ins.</summary>
public sealed class TestCertificates : IDisposable
{
    private readonly RSA _caKey = RSA.Create(2048);

    public TestCertificates()
    {
        var request = new CertificateRequest("CN=Seseragi Test CA", _caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        Authority = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-10), DateTimeOffset.UtcNow.AddDays(2));
    }

    /// <summary>The authority (with its key).</summary>
    public X509Certificate2 Authority { get; }

    /// <summary>The authority as PEM, as a configuration key takes it.</summary>
    public string AuthorityPem => Authority.ExportCertificatePem();

    /// <summary>A server certificate (with its key) for <c>127.0.0.1</c>, <c>localhost</c> and <paramref name="names"/>.</summary>
    public X509Certificate2 Server(params string[] names)
    {
        using RSA key = RSA.Create(2048);
        var request = new CertificateRequest($"CN={(names.Length > 0 ? names[0] : "localhost")}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        if (names.Length == 0)
        {
            san.AddIpAddress(IPAddress.Loopback);
            san.AddDnsName("localhost");
        }
        foreach (string name in names) san.AddDnsName(name);
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.1")], false));
        request.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromCertificate(Authority, true, false));
        byte[] serial = RandomNumberGenerator.GetBytes(16);
        serial[0] &= 0x7F;
        using X509Certificate2 issued = request.Create(Authority, DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(1), serial);
        X509Certificate2 withKey = issued.CopyWithPrivateKey(key);
        // SslStream on Linux wants a key it can export to its own store.
        return X509CertificateLoader.LoadPkcs12(withKey.Export(X509ContentType.Pkcs12), null);
    }

    public void Dispose()
    {
        Authority.Dispose();
        _caKey.Dispose();
    }
}
