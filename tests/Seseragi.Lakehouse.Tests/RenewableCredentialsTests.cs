using Kaminari.Seseragi.Lakehouse.Storage;

namespace Seseragi.Lakehouse.Tests;

/// <summary>Vended credentials renewed before they expire, and the managed clients using the renewed ones.</summary>
public class RenewableCredentialsTests
{
    private static readonly DateTimeOffset Start = new(2026, 11, 11, 12, 0, 0, TimeSpan.Zero);

    private sealed class Clock
    {
        public DateTimeOffset Now = Start;
        public DateTimeOffset Get() => Now;
    }

    private static VendedCredentials Aws(string key, DateTimeOffset? expires) =>
        new(new Dictionary<string, string> { ["aws_access_key_id"] = key, ["aws_secret_access_key"] = "secret-" + key }, expires);

    [Fact]
    public void Credentials_AreRenewed_HalfwayThroughAShortLife_AndFiveMinutesBeforeALongOne()
    {
        var clock = new Clock();
        int fetches = 0;
        var credentials = new RenewableCredentials(
            () => Aws($"k{++fetches}", clock.Now.AddHours(1)), initial: Aws("k0", Start.AddSeconds(4)), clock: clock.Get);

        // The initial ones (4 s of life) are used, and due again after half of it.
        Assert.Equal("k0", credentials.Current(out DateTimeOffset? renewAt).Options["aws_access_key_id"]);
        Assert.Equal(Start.AddSeconds(2), renewAt);
        clock.Now = Start.AddSeconds(1.9);
        Assert.Equal("k0", credentials.Current(out _).Options["aws_access_key_id"]);
        Assert.Equal(0, fetches);

        // Due: fetched again; an hour of life is renewed five minutes before it ends.
        clock.Now = Start.AddSeconds(2);
        Assert.Equal("k1", credentials.Current(out renewAt).Options["aws_access_key_id"]);
        Assert.Equal(clock.Now.AddMinutes(55), renewAt);
        Assert.Equal("k1", credentials.Current(out _).Options["aws_access_key_id"]);
        Assert.Equal(1, fetches);
    }

    [Fact]
    public void AFailedRenewal_KeepsTheValidCredentials_RetriesAfterASecond_AndFailsOnceTheyExpire()
    {
        var clock = new Clock();
        int attempts = 0;
        bool fail = true;
        var credentials = new RenewableCredentials(
            () => { attempts++; return fail ? throw new InvalidOperationException("catalog down") : Aws("new", clock.Now.AddHours(1)); },
            initial: Aws("old", Start.AddSeconds(10)), clock: clock.Get);

        clock.Now = Start.AddSeconds(6); // past the renewal point (5 s)
        Assert.Equal("old", credentials.Current(out DateTimeOffset? renewAt).Options["aws_access_key_id"]);
        Assert.Equal(clock.Now.AddSeconds(1), renewAt);
        Assert.Equal("old", credentials.Current(out _).Options["aws_access_key_id"]);
        Assert.Equal(1, attempts); // not asked again within the second

        clock.Now = Start.AddSeconds(10); // expired: the failure is reported
        var failure = Assert.Throws<InvalidOperationException>(() => credentials.Current(out _));
        Assert.Equal("catalog down", failure.Message);

        fail = false;
        Assert.Equal("new", credentials.Current(out _).Options["aws_access_key_id"]);
    }

    [Fact]
    public void Offered_Credentials_ReplaceOnlyShorterLivedOnes_AndOnesThatNeverExpireAreNeverRenewed()
    {
        var clock = new Clock();
        var credentials = new RenewableCredentials(() => throw new InvalidOperationException("not asked"), Aws("a", Start.AddMinutes(30)), clock.Get);
        credentials.Offer(Aws("shorter", Start.AddMinutes(10)));
        Assert.Equal("a", credentials.Current(out _).Options["aws_access_key_id"]);
        credentials.Offer(Aws("longer", Start.AddMinutes(60)));
        Assert.Equal("longer", credentials.Current(out _).Options["aws_access_key_id"]);

        var forever = new RenewableCredentials(() => throw new InvalidOperationException("not asked"), Aws("f", null), clock.Get);
        clock.Now = Start.AddYears(5);
        Assert.Equal("f", forever.Current(out DateTimeOffset? renewAt).Options["aws_access_key_id"]);
        Assert.Null(renewAt);
    }

    [Fact]
    public void Projections_MakeEachCloudsCredentials_AndRefuseAnotherClouds()
    {
        var clock = new Clock();
        var sas = new RenewableCredentials(() => throw new InvalidOperationException(),
            new VendedCredentials(new Dictionary<string, string> { ["azure_storage_sas_token"] = "?sv=1&sig=a%2Bb%3D" }, Start.AddHours(1)), clock.Get);
        Assert.Equal([new("sv", "1"), new("sig", "a+b=")], sas.ForAzureSas().Get());
        Assert.Contains("not AWS credentials", Assert.Throws<StorageException>(() => sas.ForS3().Get()).Message);

        var gcs = new RenewableCredentials(() => throw new InvalidOperationException(),
            new VendedCredentials(new Dictionary<string, string> { ["google_oauth_token"] = "ya29.v" }, Start.AddHours(1)), clock.Get);
        Assert.Equal("ya29.v", gcs.ForGcs().Get());
        Assert.Contains("Azure SAS", Assert.Throws<StorageException>(() => gcs.ForAzureSas().Get()).Message);
    }

    // ------------------------------------------------------------------ the clients use the renewed credentials

    private static (RenewableCredentials Credentials, Clock Clock) TwoGenerations(Func<int, Dictionary<string, string>> options)
    {
        var clock = new Clock();
        int generation = 0;
        var credentials = new RenewableCredentials(
            () => new VendedCredentials(options(++generation), clock.Now.AddHours(1)),
            initial: new VendedCredentials(options(0), Start.AddSeconds(4)), clock: clock.Get);
        return (credentials, clock);
    }

    [Fact]
    public void S3_SignsWithTheRenewedKeys_NotTheStaticOnes()
    {
        using var server = new FakeHttpServer(_ => (404, ""));
        var (credentials, clock) = TwoGenerations(g => new() { ["aws_access_key_id"] = $"KEY{g}", ["aws_secret_access_key"] = "s", ["aws_session_token"] = $"tok{g}" });
        var options = new Dictionary<string, string>
        {
            ["aws_access_key_id"] = "STATIC", ["aws_secret_access_key"] = "s", ["aws_endpoint"] = server.Url, ["aws_allow_http"] = "true",
            ["aws_region"] = "us-east-1",
        };
        CloudFileSystem fs = CloudStorage.TryOpen("s3://bucket/t", options, RetrySettings.Default, new Dictionary<string, string>(), null,
            out string? reason, renewable: credentials) ?? throw new InvalidOperationException(reason);

        Assert.Null(fs.ReadAll("a"));
        clock.Now = Start.AddSeconds(3);
        Assert.Null(fs.ReadAll("b"));
        var seen = server.Requests.Select(r => (r.Headers["Authorization"].Contains("Credential=KEY0/"), r.Headers["Authorization"].Contains("Credential=KEY1/"), r.Headers.GetValueOrDefault("x-amz-security-token"))).ToList();
        Assert.Equal([(true, false, "tok0"), (false, true, "tok1")], seen);
    }

    [Fact]
    public void Azure_SendsTheRenewedSas()
    {
        using var server = new FakeHttpServer(_ => (404, ""));
        var (credentials, clock) = TwoGenerations(g => new() { ["azure_storage_sas_token"] = $"sv=1&sig=gen{g}" });
        var options = new Dictionary<string, string>
        {
            ["account_name"] = "acct", ["sas_token"] = "sv=1&sig=static", ["endpoint"] = server.Url, ["allow_http"] = "true",
        };
        CloudFileSystem fs = CloudStorage.TryOpen("az://c/t", options, RetrySettings.Default, new Dictionary<string, string>(), null,
            out string? reason, renewable: credentials) ?? throw new InvalidOperationException(reason);

        Assert.Null(fs.ReadAll("a"));
        clock.Now = Start.AddSeconds(3);
        Assert.Null(fs.ReadAll("b"));
        Assert.Equal([true, false], server.Requests.Select(r => r.PathAndQuery.Contains("sig=gen0")));
        Assert.Equal([false, true], server.Requests.Select(r => r.PathAndQuery.Contains("sig=gen1")));
        Assert.DoesNotContain(server.Requests, r => r.PathAndQuery.Contains("static"));
    }

    [Fact]
    public void Gcs_SendsTheRenewedToken()
    {
        using var server = new FakeHttpServer(_ => (404, ""));
        var (credentials, clock) = TwoGenerations(g => new() { ["google_oauth_token"] = $"ya29.gen{g}" });
        var options = new Dictionary<string, string> { ["google_oauth_token"] = "ya29.static", ["google_base_url"] = server.Url };
        CloudFileSystem fs = CloudStorage.TryOpen("gs://bucket/t", options, RetrySettings.Default, new Dictionary<string, string>(), null,
            out string? reason, renewable: credentials) ?? throw new InvalidOperationException(reason);

        Assert.Null(fs.ReadAll("a"));
        clock.Now = Start.AddSeconds(3);
        Assert.Null(fs.ReadAll("b"));
        Assert.Equal(["Bearer ya29.gen0", "Bearer ya29.gen1"], server.Requests.Select(r => r.Headers["Authorization"]));
    }
}
