using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Polars.NET.Core.Delta;

namespace Polars.Integration.Tests;

/// <summary>Compute Engine REST calls and V4 signed URLs, as the test's service account.</summary>
public static class RealGce
{
    private const string Compute = "https://compute.googleapis.com/compute/v1";
    private static readonly HttpClient s_http = new() { Timeout = TimeSpan.FromMinutes(2) };
    private static (string Token, DateTimeOffset Fetched)? s_token;

    public static string Project => RealGcs.Project!;

    public static string Zone => Environment.GetEnvironmentVariable("SESERAGI_GCE_ZONE") ?? "asia-northeast1-b";

    /// <summary>The service account the VM runs as: Storage Object Admin on the bucket, nothing else.</summary>
    public static string VmServiceAccount => $"seseragi-test-vm@{Project}.iam.gserviceaccount.com";

    private static string Token()
    {
        if (s_token is not { } t || DateTimeOffset.UtcNow - t.Fetched > TimeSpan.FromMinutes(30))
            s_token = (RealGcs.Token("https://www.googleapis.com/auth/cloud-platform"), DateTimeOffset.UtcNow);
        return s_token.Value.Token;
    }

    /// <summary>One Compute Engine request below the project's zone; null on 404. Fails with the API's error text.</summary>
    public static JsonNode? Send(HttpMethod method, string path, JsonNode? body = null)
    {
        using var request = new HttpRequestMessage(method, $"{Compute}/projects/{Project}/{path}");
        request.Headers.TryAddWithoutValidation("Authorization", "Bearer " + Token());
        if (body is not null) request.Content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json");
        using HttpResponseMessage response = s_http.Send(request);
        string text = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
        if (response.StatusCode == HttpStatusCode.NotFound) return null;
        if (!response.IsSuccessStatusCode) throw new InvalidOperationException($"{method} {path}: HTTP {(int)response.StatusCode}: {text}");
        return string.IsNullOrWhiteSpace(text) ? null : JsonNode.Parse(text);
    }

    /// <summary>Waits for a zonal operation to finish; fails with its errors.</summary>
    public static void Await(JsonNode operation, TimeSpan timeout)
    {
        string name = operation["name"]!.GetValue<string>();
        var clock = System.Diagnostics.Stopwatch.StartNew();
        while (true)
        {
            JsonNode current = Send(HttpMethod.Get, $"zones/{Zone}/operations/{name}")!;
            if (current["status"]?.GetValue<string>() == "DONE")
            {
                if (current["error"] is { } error) throw new InvalidOperationException($"Operation {name} failed: {error.ToJsonString()}");
                return;
            }
            if (clock.Elapsed > timeout) throw new TimeoutException($"Operation {name} not done after {timeout}.");
            Thread.Sleep(TimeSpan.FromSeconds(3));
        }
    }

    /// <summary>
    /// A V4 signed URL of an object of the test bucket (GOOG4-RSA-SHA256, signed with the service account's key; the
    /// host is the only signed header).
    /// </summary>
    public static string SignedUrl(string method, string objectName, TimeSpan expires)
    {
        var (email, rsa) = RealGcs.SigningKey();
        using (rsa)
        {
            DateTime now = DateTime.UtcNow;
            string date = now.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
            string stamp = now.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
            string scope = $"{date}/auto/storage/goog4_request";
            string path = "/" + RealGcs.Bucket + "/" + string.Join('/', objectName.Split('/').Select(Uri.EscapeDataString));
            var query = new SortedDictionary<string, string>(StringComparer.Ordinal)
            {
                ["X-Goog-Algorithm"] = "GOOG4-RSA-SHA256",
                ["X-Goog-Credential"] = $"{email}/{scope}",
                ["X-Goog-Date"] = stamp,
                ["X-Goog-Expires"] = ((int)expires.TotalSeconds).ToString(CultureInfo.InvariantCulture),
                ["X-Goog-SignedHeaders"] = "host",
            };
            string canonicalQuery = string.Join('&', query.Select(p => $"{Uri.EscapeDataString(p.Key)}={Uri.EscapeDataString(p.Value)}"));
            string canonical = $"{method}\n{path}\n{canonicalQuery}\nhost:storage.googleapis.com\n\nhost\nUNSIGNED-PAYLOAD";
            string toSign = $"GOOG4-RSA-SHA256\n{stamp}\n{scope}\n{Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical))).ToLowerInvariant()}";
            byte[] signature = rsa.SignData(Encoding.UTF8.GetBytes(toSign), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            return $"https://storage.googleapis.com{path}?{canonicalQuery}&X-Goog-Signature={Convert.ToHexString(signature).ToLowerInvariant()}";
        }
    }
}

/// <summary>A fact that runs only on request (SESERAGI_GCE_VM=1): it rents a small Compute Engine VM for a few minutes.</summary>
public sealed class GceVmFactAttribute : FactAttribute
{
    public GceVmFactAttribute()
    {
        if (!RealGcs.Available || RealGcs.Project is null) Skip = "No real bucket and project: .secrets/GCS (bucket, project id) and .secrets/gcs-key.json.";
        else if (Environment.GetEnvironmentVariable("SESERAGI_GCE_VM") != "1") Skip = "Rents a Compute Engine VM: set SESERAGI_GCE_VM=1 to run it.";
    }
}

/// <summary>
/// The credentials of a machine (the metadata server's token of the VM's service account), on a real Compute Engine
/// VM rented for the test and deleted afterwards: the probe (tests/RealCloudProbe, published self-contained) runs
/// there with no keys at all, on the managed and the native store, and reports through a signed URL.
/// </summary>
/// <remarks>
/// The VM cannot outlive the test: it is deleted in <c>finally</c>, its boot disk goes with it, and Compute Engine
/// itself deletes it 30 minutes after launch (<c>maxRunDuration</c> with termination action DELETE) should this
/// process die first. Fedora 44, since the native library needs glibc 2.43.
/// </remarks>
[Trait("RealCloud", "GceVm")]
public sealed class RealCloudGceMachineTests : IClassFixture<RealGcsCleanup>
{
    private const string Label = "seseragi-test";
    private const string MachineType = "e2-small";
    private const string FedoraImage = "projects/fedora-cloud/global/images/family/fedora-cloud-44-x86-64";

    [GceVmFact]
    public void TheMetadataServer_ServesBothStores_OnARealVm()
    {
        // A free check first: the zone answers to the test's account.
        Assert.NotNull(RealGce.Send(HttpMethod.Get, $"zones/{RealGce.Zone}"));

        // The probe, published for linux-x64 with the release native library, goes to the bucket.
        string package = RealCloudAwsMachineTests.PublishProbe();
        string prefix = $"{RealGcs.RunPrefix}/vm";
        using (var fs = new ObjectStoreFileSystem($"gs://{RealGcs.Bucket}", RealGcs.Args(), ObjectStoreBackend.Managed))
            fs.Overwrite($"{prefix}/probe.tgz", File.ReadAllBytes(package));
        string get = RealGce.SignedUrl("GET", $"{prefix}/probe.tgz", TimeSpan.FromMinutes(45));
        string put = RealGce.SignedUrl("PUT", $"{prefix}/result.txt", TimeSpan.FromMinutes(45));
        // The signature is checked here, before anything is rented: the first byte of the probe through the signed URL.
        using (var http = new HttpClient())
        using (var head = new HttpRequestMessage(HttpMethod.Get, get))
        {
            head.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(0, 0);
            using HttpResponseMessage answer = http.Send(head);
            Assert.True(answer.StatusCode == HttpStatusCode.PartialContent,
                $"The signed URL is refused: HTTP {(int)answer.StatusCode}: {answer.Content.ReadAsStringAsync().GetAwaiter().GetResult()}");
        }
        string tables = $"gs://{RealGcs.Bucket}/{prefix}/table";
        string script = $$"""
            #!/bin/bash
            export HOME=/root
            cd /root
            {
              if curl -sSf -o probe.tgz '{{get}}' && tar xzf probe.tgz; then
                for b in managed native; do
                  POLARS_NET_OBJECT_STORE=$b ./probe-publish/RealCloudProbe "{{tables}}_$b" 2>&1 | tail -n 1
                done
              else
                echo '{"ok":false,"step":"download the probe"}'
              fi
            } > result.txt 2>&1
            curl -sSf -X PUT --upload-file result.txt '{{put}}'
            """;

        string name = $"seseragi-{RealGcs.RunPrefix[^8..]}";
        string stamp = DateTime.UtcNow.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture);
        bool created = false;
        try
        {
            JsonNode insert = RealGce.Send(HttpMethod.Post, $"zones/{RealGce.Zone}/instances", new JsonObject
            {
                ["name"] = name,
                ["machineType"] = $"zones/{RealGce.Zone}/machineTypes/{MachineType}",
                ["labels"] = new JsonObject { [Label] = stamp },
                ["disks"] = new JsonArray(new JsonObject
                {
                    ["boot"] = true,
                    ["autoDelete"] = true,
                    ["initializeParams"] = new JsonObject { ["sourceImage"] = FedoraImage },
                }),
                // An ephemeral public address: the way out to the bucket (the default network has no NAT).
                ["networkInterfaces"] = new JsonArray(new JsonObject
                {
                    ["network"] = "global/networks/default",
                    ["accessConfigs"] = new JsonArray(new JsonObject { ["type"] = "ONE_TO_ONE_NAT", ["name"] = "External NAT" }),
                }),
                ["serviceAccounts"] = new JsonArray(new JsonObject
                {
                    ["email"] = RealGce.VmServiceAccount,
                    ["scopes"] = new JsonArray("https://www.googleapis.com/auth/devstorage.read_write"),
                }),
                // The Fedora image on Compute Engine has Google's guest agent, not cloud-init: it runs startup-script
                // (and ignores user-data) at boot.
                ["metadata"] = new JsonObject
                {
                    ["items"] = new JsonArray(new JsonObject { ["key"] = "startup-script", ["value"] = script.Replace("\r", "") }),
                },
                // The dead-man switch: Compute Engine deletes the VM 30 minutes after it starts.
                ["scheduling"] = new JsonObject
                {
                    ["provisioningModel"] = "STANDARD",
                    ["instanceTerminationAction"] = "DELETE",
                    ["maxRunDuration"] = new JsonObject { ["seconds"] = 1800 },
                },
            })!;
            created = true;
            RealGce.Await(insert, TimeSpan.FromMinutes(5));

            string result = AwaitResult($"{prefix}/result.txt", name, TimeSpan.FromMinutes(15));
            string[] lines = [.. result.Split('\n', StringSplitOptions.RemoveEmptyEntries)];
            Assert.True(lines.Length == 2, result);
            foreach (var (line, backend) in lines.Zip(["managed", "native"]))
            {
                using var doc = System.Text.Json.JsonDocument.Parse(line);
                var root = doc.RootElement;
                Assert.True(root.GetProperty("ok").GetBoolean(), line);
                Assert.Equal(backend, root.GetProperty("implementation").GetString());
                Assert.Equal([1L, 3L], root.GetProperty("ids").EnumerateArray().Select(e => e.GetInt64()));
            }
        }
        finally
        {
            if (created) Delete(name);
        }

        // Nothing of this run is left: no instance, no disk.
        var clock = System.Diagnostics.Stopwatch.StartNew();
        while (Leftovers(name) is { Count: > 0 } left)
        {
            Assert.True(clock.Elapsed < TimeSpan.FromMinutes(3), "Left behind: " + string.Join(", ", left));
            Thread.Sleep(TimeSpan.FromSeconds(10));
        }
    }

    /// <summary>Waits for the probe's report; fails early when the VM is gone or stopped without one.</summary>
    private static string AwaitResult(string objectName, string instance, TimeSpan timeout)
    {
        using var fs = new ObjectStoreFileSystem($"gs://{RealGcs.Bucket}", RealGcs.Args(), ObjectStoreBackend.Managed);
        var clock = System.Diagnostics.Stopwatch.StartNew();
        while (clock.Elapsed < timeout)
        {
            if (fs.ReadAll(objectName) is byte[] bytes) return Encoding.UTF8.GetString(bytes);
            JsonNode? vm = RealGce.Send(HttpMethod.Get, $"zones/{RealGce.Zone}/instances/{instance}");
            Assert.True(vm is not null, $"The VM is gone after {clock.Elapsed:mm\\:ss} and reported nothing.");
            string status = vm!["status"]!.GetValue<string>();
            Assert.True(status is "PROVISIONING" or "STAGING" or "RUNNING", $"The VM is {status} after {clock.Elapsed:mm\\:ss} and reported nothing.");
            Thread.Sleep(TimeSpan.FromSeconds(15));
        }
        throw new TimeoutException($"No report from the VM within {timeout}.");
    }

    private static void Delete(string name)
    {
        try
        {
            if (RealGce.Send(HttpMethod.Delete, $"zones/{RealGce.Zone}/instances/{name}") is { } operation)
                RealGce.Await(operation, TimeSpan.FromMinutes(5));
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"Deleting {name} failed: {e.Message}");
        }
    }

    /// <summary>Instances and disks of the zone named after this run.</summary>
    private static List<string> Leftovers(string name)
    {
        var left = new List<string>();
        string[] kinds = ["instances", "disks"];
        foreach (string kind in kinds)
        {
            JsonNode? list = RealGce.Send(HttpMethod.Get, $"zones/{RealGce.Zone}/{kind}?filter=" + Uri.EscapeDataString($"name = \"{name}*\""));
            if (list?["items"] is JsonArray items) left.AddRange(items.Select(i => $"{kind}/{i!["name"]!.GetValue<string>()}"));
        }
        return left;
    }
}
