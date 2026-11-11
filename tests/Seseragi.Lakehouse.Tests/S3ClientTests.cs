using System.Globalization;
using System.Text;
using Kaminari.Seseragi.Lakehouse.Storage;
using Kaminari.Seseragi.Lakehouse.Storage.S3;

namespace Seseragi.Lakehouse.Tests;

public class S3ClientTests
{
    private static DateTimeOffset Time(string amzDate) =>
        DateTimeOffset.ParseExact(amzDate, "yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);

    private static string Signature(string authorization) => authorization[(authorization.IndexOf("Signature=", StringComparison.Ordinal) + 10)..];

    [Fact]
    public void SigV4_MatchesTheTestSuite_GetVanilla()
    {
        // aws-sig-v4-test-suite, get-vanilla.
        var credential = new AwsCredential("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", null);
        var headers = new Dictionary<string, string> { ["host"] = "example.amazonaws.com", ["x-amz-date"] = "20150830T123600Z" };
        string authorization = SigV4.Authorization("GET", "/", "", headers, SigV4.EmptyPayloadHash, credential, "us-east-1", "service", Time("20150830T123600Z"));
        Assert.Equal(
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, " +
            "Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31",
            authorization);
    }

    private static readonly AwsCredential S3Example = new("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", null);

    [Fact]
    public void SigV4_MatchesTheS3Examples()
    {
        // The examples of the S3 documentation ("Signature calculations for the Authorization header").
        var get = new Dictionary<string, string>
        {
            ["host"] = "examplebucket.s3.amazonaws.com",
            ["range"] = "bytes=0-9",
            ["x-amz-content-sha256"] = SigV4.EmptyPayloadHash,
            ["x-amz-date"] = "20130524T000000Z",
        };
        Assert.Equal("f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41", Signature(SigV4.Authorization(
            "GET", "/test.txt", "", get, SigV4.EmptyPayloadHash, S3Example, "us-east-1", "s3", Time("20130524T000000Z"))));

        byte[] body = Encoding.UTF8.GetBytes("Welcome to Amazon S3.");
        string bodyHash = SigV4.Hash(body);
        Assert.Equal("44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072", bodyHash);
        var put = new Dictionary<string, string>
        {
            ["date"] = "Fri, 24 May 2013 00:00:00 GMT",
            ["host"] = "examplebucket.s3.amazonaws.com",
            ["x-amz-content-sha256"] = bodyHash,
            ["x-amz-date"] = "20130524T000000Z",
            ["x-amz-storage-class"] = "REDUCED_REDUNDANCY",
        };
        Assert.Equal("/test%24file.text", "/" + SigV4.Encode("test$file.text", encodeSlash: false));
        Assert.Equal("98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd", Signature(SigV4.Authorization(
            "PUT", "/test%24file.text", "", put, bodyHash, S3Example, "us-east-1", "s3", Time("20130524T000000Z"))));

        var list = new Dictionary<string, string>
        {
            ["host"] = "examplebucket.s3.amazonaws.com",
            ["x-amz-content-sha256"] = SigV4.EmptyPayloadHash,
            ["x-amz-date"] = "20130524T000000Z",
        };
        string query = SigV4.CanonicalQuery([new("prefix", "J"), new("max-keys", "2")]);
        Assert.Equal("max-keys=2&prefix=J", query);
        Assert.Equal("34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7", Signature(SigV4.Authorization(
            "GET", "/", query, list, SigV4.EmptyPayloadHash, S3Example, "us-east-1", "s3", Time("20130524T000000Z"))));
    }

    [Fact]
    public void SigV4_SignsTheSessionToken()
    {
        var headers = new Dictionary<string, string> { ["host"] = "h", ["x-amz-date"] = "20130524T000000Z" };
        string plain = SigV4.Authorization("GET", "/", "", headers, SigV4.EmptyPayloadHash, S3Example, "us-east-1", "s3", Time("20130524T000000Z"));
        headers["x-amz-security-token"] = "token";
        string withToken = SigV4.Authorization("GET", "/", "", headers, SigV4.EmptyPayloadHash, S3Example with { Token = "token" }, "us-east-1", "s3", Time("20130524T000000Z"));
        Assert.Contains("SignedHeaders=host;x-amz-date;x-amz-security-token,", withToken);
        Assert.NotEqual(Signature(plain), Signature(withToken));
    }

    [Theory]
    [InlineData("p=a b/x.parquet", "p%3Da%20b/x.parquet")]
    [InlineData("p=x%2Fy/x.parquet", "p%3Dx%252Fy/x.parquet")]
    [InlineData("p=東京 50%/x", "p%3D%E6%9D%B1%E4%BA%AC%2050%25/x")]
    [InlineData("p=1+1=2/a&b@c;d~_.-", "p%3D1%2B1%3D2/a%26b%40c%3Bd~_.-")]
    public void Paths_AreEncodedOnce_AndSentVerbatim(string key, string encoded)
    {
        Assert.Equal(encoded, SigV4.Encode(key, encodeSlash: false));
        // What is signed is what goes on the wire: the URI must not re-escape or unescape anything.
        var uri = new Uri("http://localhost:9000/bucket/" + encoded, new UriCreationOptions { DangerousDisablePathAndQueryCanonicalization = true });
        Assert.Equal("/bucket/" + encoded, uri.AbsolutePath);
        Assert.Equal("/bucket/" + encoded, uri.PathAndQuery);
    }

    [Fact]
    public void Listing_IsParsed_WithPagesAndEscapes()
    {
        string xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
              <Name>b</Name><Prefix>t/</Prefix><KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated>
              <Contents><Key>t/a&amp;b.json</Key><LastModified>2026-11-11T10:00:00.000Z</LastModified><ETag>"x"</ETag><Size>12</Size><StorageClass>STANDARD</StorageClass></Contents>
              <Contents><Key>t/p=東京 50%/c.json</Key><LastModified>2026-11-11T10:00:01.500Z</LastModified><Size>0</Size></Contents>
              <CommonPrefixes><Prefix>t/dir/</Prefix></CommonPrefixes>
              <NextContinuationToken>abc==</NextContinuationToken>
            </ListBucketResult>
            """;
        var (objects, next) = S3Client.ParseListing(Encoding.UTF8.GetBytes(xml));
        Assert.Equal(["t/a&b.json", "t/p=東京 50%/c.json"], objects.Select(o => o.Key));
        Assert.Equal([12L, 0L], objects.Select(o => o.Size));
        Assert.Equal(new DateTimeOffset(2026, 11, 11, 10, 0, 1, 500, TimeSpan.Zero), objects[1].LastModified);
        Assert.Equal("abc==", next);

        var (_, last) = S3Client.ParseListing(Encoding.UTF8.GetBytes(xml.Replace("<IsTruncated>true", "<IsTruncated>false")));
        Assert.Null(last);
    }

    private static readonly Dictionary<string, string> NoEnvironment = [];

    private static S3Settings? Resolve(Dictionary<string, string> options, Dictionary<string, string>? environment = null, string? configFile = null) =>
        S3Settings.Resolve("bucket", options, environment ?? NoEnvironment, configFile, out _);

    [Fact]
    public void Settings_FollowTheKeyNamesAndPrecedence()
    {
        var keys = new Dictionary<string, string> { ["AWS_ACCESS_KEY_ID"] = "id", ["Secret_Access_Key"] = "secret" };

        // Explicit keys over the environment; a default region never replaces a region.
        S3Settings s = Resolve(new(keys) { ["aws_region"] = "eu-west-1" }, new() { ["AWS_REGION"] = "us-west-2" })!;
        Assert.Equal("eu-west-1", s.Region);
        Assert.Equal("https://s3.eu-west-1.amazonaws.com/bucket", s.BucketEndpoint);
        Assert.Equal("us-west-2", Resolve(new(keys) { ["default_region"] = "eu-west-1" }, new() { ["AWS_REGION"] = "us-west-2" })!.Region);
        Assert.Equal("us-west-2", Resolve(new(keys) { ["default_region"] = "eu-west-1" }, new() { ["AWS_DEFAULT_REGION"] = "us-west-2" })!.Region);
        Assert.Equal("ap-east-1", Resolve(new(keys) { ["default_region"] = "eu-west-1" }, configFile: "[default]\nregion = ap-east-1\n")!.Region);
        Assert.Equal("eu-west-1", Resolve(new(keys) { ["default_region"] = "eu-west-1" })!.Region);

        // Credentials from the environment, a session token, path style vs virtual-hosted, an endpoint.
        S3Settings fromEnv = Resolve(new() { ["region"] = "r" }, new() { ["AWS_ACCESS_KEY_ID"] = "e", ["AWS_SECRET_ACCESS_KEY"] = "s", ["AWS_SESSION_TOKEN"] = "t" })!;
        Assert.Equal(new AwsCredential("e", "s", "t"), fromEnv.Credential!.Get());
        Assert.Equal("https://bucket.s3.r.amazonaws.com", Resolve(new(keys) { ["region"] = "r", ["aws_virtual_hosted_style_request"] = "true" })!.BucketEndpoint);
        S3Settings minio = Resolve(new(keys) { ["aws_endpoint"] = "http://localhost:9000/", ["aws_allow_http"] = "true", ["aws_s3_force_path_style"] = "true", ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true" })!;
        Assert.Equal("http://localhost:9000/bucket", minio.BucketEndpoint);
        Assert.Equal("us-east-1", minio.Region); // an endpoint without a region
        Assert.Equal("http://other:1/bucket", Resolve(new(keys) { ["endpoint"] = "http://localhost:9000", ["aws_endpoint_url_s3"] = "http://other:1" })!.BucketEndpoint);
        Assert.Null(Resolve(new() { ["region"] = "r", ["aws_skip_signature"] = "true" })!.Credential);
    }

    [Fact]
    public void Settings_LeaveUnsupportedSetups_ToAnotherImplementation()
    {
        var keys = new Dictionary<string, string> { ["aws_access_key_id"] = "id", ["aws_secret_access_key"] = "secret", ["aws_region"] = "r" };
        Assert.NotNull(Resolve(keys));

        string? reason;
        Assert.Null(S3Settings.Resolve("bucket", new Dictionary<string, string> { ["aws_region"] = "r", ["aws_access_key_id"] = "id" }, NoEnvironment, null, out reason));
        Assert.Contains("without its secret", reason);
        Assert.Null(Resolve(new(keys) { ["aws_imdsv1_fallback"] = "sometimes" }));
        Assert.Null(S3Settings.Resolve("bucket", new Dictionary<string, string> { ["aws_access_key_id"] = "id", ["aws_secret_access_key"] = "s" }, NoEnvironment, null, out reason));
        Assert.Contains("region", reason);
        Assert.Null(Resolve(new(keys) { ["aws_server_side_encryption"] = "rot13" }));
        Assert.Null(Resolve(new(keys) { ["aws_server_side_encryption"] = "sse-c" })); // without its key
        Assert.Null(Resolve(new(keys) { ["aws_request_payer"] = "sometimes" }));
        Assert.Null(Resolve(new(keys) { ["aws_s3_express"] = "true" }));
        Assert.Null(Resolve(new(keys) { ["proxy_url"] = "http://proxy" }));
        Assert.Null(Resolve(new(keys) { ["aws_conditional_put"] = "dynamo:table" }));
        S3Headers kms = Resolve(new(keys) { ["aws_server_side_encryption"] = "aws:kms", ["aws_sse_bucket_key_enabled"] = "yes" },
            new() { ["AWS_SSE_KMS_KEY_ID"] = "k" })!.Headers!;
        Assert.Equal(new Dictionary<string, string>
        {
            ["x-amz-server-side-encryption"] = "aws:kms", ["x-amz-server-side-encryption-aws-kms-key-id"] = "k",
            ["x-amz-server-side-encryption-bucket-key-enabled"] = "true",
        }, kms.Put);
        Assert.Empty(kms.Read);
        Assert.True(Resolve(new(keys) { ["aws_request_payer"] = "true" })!.Headers!.RequesterPays);
        // A key id without an encryption type changes nothing (as in the engine).
        Assert.Same(S3Headers.None, Resolve(new(keys) { ["aws_sse_kms_key_id"] = "k" })!.Headers);

        // Harmless keys, and credential-chain keys once static credentials are given, change nothing.
        Assert.NotNull(Resolve(new(keys) { ["aws_request_payer"] = "false", ["aws_conditional_put"] = "etag", ["aws_role_arn"] = "arn" }));
        Assert.NotNull(Resolve(keys, new() { ["AWS_WEB_IDENTITY_TOKEN_FILE"] = "/t", ["AWS_PROFILE"] = "p", ["AWS_CONFIG_FILE"] = "/c" }));
    }

    [Fact]
    public void AMissingRegion_IsLookedUp_OnlyWithoutAnEndpoint()
    {
        // Vended credentials name no region: the bucket's region is asked for, as the engine's own store does.
        var keys = new Dictionary<string, string> { ["aws_access_key_id"] = "id", ["aws_secret_access_key"] = "secret" };
        var asked = new List<string>();
        S3Settings found = S3Settings.Resolve("bucket", keys, NoEnvironment, null, out _, b => { asked.Add(b); return "eu-central-1"; })!;
        Assert.Equal("eu-central-1", found.Region);
        Assert.Equal("https://s3.eu-central-1.amazonaws.com/bucket", found.BucketEndpoint);
        Assert.Equal(["bucket"], asked);

        Assert.Null(S3Settings.Resolve("bucket", keys, NoEnvironment, null, out string? reason, _ => null));
        Assert.Contains("region", reason);
        Assert.Equal("us-east-1", S3Settings.Resolve("bucket", new Dictionary<string, string>(keys) { ["aws_endpoint"] = "http://h:1" }, NoEnvironment, null, out _,
            _ => throw new InvalidOperationException("no lookup with an endpoint"))!.Region);
        // Without credentials the instance's are used, in the bucket's region.
        S3Settings instance = S3Settings.Resolve("bucket", new Dictionary<string, string>(), NoEnvironment, null, out _, _ => "eu-north-1")!;
        Assert.Equal("eu-north-1", instance.Region);
        Assert.StartsWith("instance metadata", Assert.IsType<CachedCredential<AwsCredential>>(instance.Credential).Origin);
    }

    [Fact]
    public void OnlyServedSchemes_AreOpened()
    {
        var keys = new Dictionary<string, string> { ["aws_access_key_id"] = "id", ["aws_secret_access_key"] = "secret", ["aws_region"] = "r" };
        Assert.NotNull(CloudStorage.TryOpen("s3://bucket/t", keys, RetrySettings.Default, NoEnvironment, null, out _));
        Assert.NotNull(CloudStorage.TryOpen("S3A://bucket", keys, RetrySettings.Default, NoEnvironment, null, out _));
        Assert.Null(CloudStorage.TryOpen("hdfs://bucket/t", keys, RetrySettings.Default, NoEnvironment, null, out string? reason));
        Assert.Contains("scheme", reason);
        Assert.Null(CloudStorage.TryOpen("/local/path", keys, RetrySettings.Default, NoEnvironment, null, out _));
    }
}
