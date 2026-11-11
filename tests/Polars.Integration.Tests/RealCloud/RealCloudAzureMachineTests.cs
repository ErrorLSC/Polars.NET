using System.Net;
using System.Text.Json.Nodes;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;

namespace Polars.Integration.Tests;

/// <summary>
/// Azure Resource Manager calls as the test's service principal (Contributor on the test resource group only).
/// </summary>
public static class RealAzureArm
{
    private const string Host = "https://management.azure.com";
    private static readonly HttpClient s_http = new() { Timeout = TimeSpan.FromMinutes(2) };
    private static (string Token, DateTimeOffset Fetched)? s_token;

    public static string Subscription => RealAzure.Deployment!.Value.Subscription;
    public static string Group => RealAzure.Deployment!.Value.Group;

    /// <summary>The id of a resource of the test group.</summary>
    public static string Id(string provider, string name) =>
        $"/subscriptions/{Subscription}/resourceGroups/{Group}/providers/{provider}/{name}";

    private static string Token()
    {
        // Tokens live an hour; a run takes minutes, but a fresh one every 30 minutes costs nothing.
        if (s_token is not { } t || DateTimeOffset.UtcNow - t.Fetched > TimeSpan.FromMinutes(30))
            s_token = (RealAzure.Token("https://management.azure.com/.default"), DateTimeOffset.UtcNow);
        return s_token.Value.Token;
    }

    /// <summary>One request; the status and the JSON answer (null when empty). Fails with ARM's error text unless the status is allowed.</summary>
    public static (HttpStatusCode Status, JsonNode? Body) Send(HttpMethod method, string path, string apiVersion, JsonNode? body = null, params HttpStatusCode[] allowed)
    {
        string url = $"{Host}{path}{(path.Contains('?') ? '&' : '?')}api-version={apiVersion}";
        using var request = new HttpRequestMessage(method, url);
        request.Headers.TryAddWithoutValidation("Authorization", "Bearer " + Token());
        if (body is not null) request.Content = new StringContent(body.ToJsonString(), System.Text.Encoding.UTF8, "application/json");
        using HttpResponseMessage response = s_http.Send(request);
        string text = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
        if (!response.IsSuccessStatusCode && !allowed.Contains(response.StatusCode))
            throw new InvalidOperationException($"{method} {path}: HTTP {(int)response.StatusCode}: {text}");
        return (response.StatusCode, string.IsNullOrWhiteSpace(text) ? null : JsonNode.Parse(text));
    }

    public static JsonNode? Get(string path, string apiVersion) =>
        Send(HttpMethod.Get, path, apiVersion, null, HttpStatusCode.NotFound) is (HttpStatusCode.OK, var body) ? body : null;

    /// <summary>Creates or updates a resource and waits until it is provisioned.</summary>
    public static JsonNode Put(string path, string apiVersion, JsonNode body, TimeSpan timeout)
    {
        Send(HttpMethod.Put, path, apiVersion, body);
        var clock = System.Diagnostics.Stopwatch.StartNew();
        while (true)
        {
            JsonNode resource = Send(HttpMethod.Get, path, apiVersion).Body!;
            string? state = resource["properties"]?["provisioningState"]?.GetValue<string>();
            if (state is null or "Succeeded") return resource;
            if (state is "Failed" or "Canceled") throw new InvalidOperationException($"{path}: provisioning {state}: {resource.ToJsonString()}");
            if (clock.Elapsed > timeout) throw new TimeoutException($"{path}: still {state} after {timeout}.");
            Thread.Sleep(TimeSpan.FromSeconds(5));
        }
    }

    /// <summary>Deletes a resource and waits until it is gone; false when it was not there.</summary>
    public static bool Delete(string path, string apiVersion, TimeSpan timeout)
    {
        var clock = System.Diagnostics.Stopwatch.StartNew();
        // 409: another operation is still writing the resource (deleting the VM touches its shutdown schedule).
        HttpStatusCode status;
        while ((status = Send(HttpMethod.Delete, path, apiVersion, null, HttpStatusCode.NotFound, HttpStatusCode.Conflict).Status) == HttpStatusCode.Conflict)
        {
            if (clock.Elapsed > timeout) throw new TimeoutException($"{path}: still busy after {timeout}.");
            Thread.Sleep(TimeSpan.FromSeconds(10));
        }
        if (status == HttpStatusCode.NotFound) return false;
        while (Send(HttpMethod.Get, path, apiVersion, null, HttpStatusCode.NotFound).Status != HttpStatusCode.NotFound)
        {
            if (clock.Elapsed > timeout) throw new TimeoutException($"{path}: not deleted after {timeout}.");
            Thread.Sleep(TimeSpan.FromSeconds(5));
        }
        return true;
    }

    /// <summary>The resources of the test group carrying a tag (id, type, tag value).</summary>
    public static List<(string Id, string Type, string Value)> Tagged(string tag)
    {
        JsonNode body = Send(HttpMethod.Get, $"/subscriptions/{Subscription}/resourceGroups/{Group}/resources?$filter=tagName eq '{tag}'", "2021-04-01").Body!;
        return [.. body["value"]!.AsArray().Select(r => (
            r!["id"]!.GetValue<string>(), r["type"]!.GetValue<string>(), r["tags"]?[tag]?.GetValue<string>() ?? ""))];
    }
}

/// <summary>A fact that runs only on request (SESERAGI_AZURE_VM=1): it rents a small Azure VM for a few minutes.</summary>
public sealed class AzureVmFactAttribute : FactAttribute
{
    public AzureVmFactAttribute()
    {
        if (!RealAzure.Available || RealAzure.Deployment is null) Skip = "No subscription and resource group: lines 9 and 10 of .secrets/AZURE.";
        else if (Environment.GetEnvironmentVariable("SESERAGI_AZURE_VM") != "1") Skip = "Rents an Azure VM: set SESERAGI_AZURE_VM=1 to run it.";
    }
}

/// <summary>
/// The credentials of a machine (a user-assigned managed identity through the instance metadata endpoint), on a real
/// Azure VM rented for the test and deleted afterwards: the probe (tests/RealCloudProbe, published self-contained) runs
/// there with no keys at all, on the managed and the native store, and once more naming the identity by client id.
/// </summary>
/// <remarks>
/// Azure has no "terminate on shutdown": a VM powered off from inside stays allocated and billed. So the VM, its disk,
/// network card and public address are deleted in <c>finally</c> (they go with the VM); an auto-shutdown schedule
/// deallocates it 30 minutes after launch should the test die first; and every run first deletes test resources older
/// than 30 minutes. Fedora 44, since the native library needs glibc 2.43.
/// </remarks>
[Trait("RealCloud", "AzureVm")]
public sealed class RealCloudAzureMachineTests : IClassFixture<RealAzureCleanup>
{
    private const string Tag = "seseragi-test";
    private const string IdentityName = "seseragi-test-vm";
    // New subscriptions may not rent the older sizes (B, Dv5, Fsv2: NotAvailableForSubscription in japaneast); the AMD
    // v7 sizes are available, and they take NVMe disks only (the Fedora image supports both).
    private static string VmSize => Environment.GetEnvironmentVariable("SESERAGI_AZURE_VM_SIZE") ?? "Standard_F2als_v7";
    private const string FedoraImage = "/CommunityGalleries/Fedora-5e266ba4-2250-406d-adad-5d73860d958f/Images/Fedora-Cloud-44-x64/Versions/latest";
    private const string ComputeApi = "2024-07-01";
    private const string NetworkApi = "2024-05-01";
    private const string ScheduleApi = "2018-09-15";
    private const AzureAccountKind Account = AzureAccountKind.Adls;

    [AzureVmFact]
    public void AManagedIdentity_ServesBothStores_OnARealVm()
    {
        // Free checks first: the role on the group, and the identity the VM gets.
        JsonNode group = RealAzureArm.Get($"/subscriptions/{RealAzureArm.Subscription}/resourceGroups/{RealAzureArm.Group}", "2021-04-01")
            ?? throw new InvalidOperationException("The resource group is not visible to the service principal.");
        string location = Environment.GetEnvironmentVariable("SESERAGI_AZURE_VM_LOCATION") ?? group["location"]!.GetValue<string>();
        string identityId = RealAzureArm.Id("Microsoft.ManagedIdentity/userAssignedIdentities", IdentityName);
        JsonNode identity = RealAzureArm.Get(identityId, "2023-01-31")
            ?? throw new InvalidOperationException($"No user-assigned identity '{IdentityName}' in the group.");
        string identityClientId = identity["properties"]!["clientId"]!.GetValue<string>();

        Sweep();

        // The probe, published for linux-x64 with the release native library, goes to the container.
        string package = RealCloudAwsMachineTests.PublishProbe();
        string prefix = $"{RealAzure.RunPrefix}/vm";
        BlobContainerClient container = RealAzure.ContainerClient(Account);
        container.GetBlobClient($"{prefix}/probe.tgz").Upload(package, overwrite: true);
        string get = container.GetBlobClient($"{prefix}/probe.tgz").GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.UtcNow.AddMinutes(45)).ToString();
        string put = container.GetBlobClient($"{prefix}/result.txt").GenerateSasUri(BlobSasPermissions.Create | BlobSasPermissions.Write, DateTimeOffset.UtcNow.AddMinutes(45)).ToString();
        string tables = RealAzure.Url(Account, $"{prefix}/table"); // one table per pass
        string script = $$"""
            #!/bin/bash
            export HOME=/root
            cd /root
            {
              if curl -sSf -o probe.tgz '{{get}}' && tar xzf probe.tgz; then
                for b in managed native; do
                  POLARS_NET_OBJECT_STORE=$b ./probe-publish/RealCloudProbe "{{tables}}_$b" 2>&1 | tail -n 1
                done
                POLARS_NET_OBJECT_STORE=managed ./probe-publish/RealCloudProbe "{{tables}}_byid" '{{identityClientId}}' 2>&1 | tail -n 1
              else
                echo '{"ok":false,"step":"download the probe"}'
              fi
            } > result.txt 2>&1
            curl -sSf -X PUT -H 'x-ms-blob-type: BlockBlob' --upload-file result.txt '{{put}}'
            """;

        string name = $"seseragi-{RealAzure.RunPrefix[^8..]}";
        string stamp = DateTime.UtcNow.ToString("yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture);
        string vnetId = RealAzureArm.Id("Microsoft.Network/virtualNetworks", name);
        string vmId = RealAzureArm.Id("Microsoft.Compute/virtualMachines", name);
        string scheduleId = RealAzureArm.Id("Microsoft.DevTestLab/schedules", $"shutdown-computevm-{name}");
        JsonObject Tags() => new() { [Tag] = stamp };
        try
        {
            RealAzureArm.Put(vnetId, NetworkApi, new JsonObject
            {
                ["location"] = location,
                ["tags"] = Tags(),
                ["properties"] = new JsonObject
                {
                    ["addressSpace"] = new JsonObject { ["addressPrefixes"] = new JsonArray("10.77.0.0/16") },
                    ["subnets"] = new JsonArray(new JsonObject { ["name"] = "default", ["properties"] = new JsonObject { ["addressPrefix"] = "10.77.0.0/24" } }),
                },
            }, TimeSpan.FromMinutes(3));

            // Disk, network card and public address are created with the VM and deleted with it.
            RealAzureArm.Put(vmId, ComputeApi, new JsonObject
            {
                ["location"] = location,
                ["tags"] = Tags(),
                ["identity"] = new JsonObject { ["type"] = "UserAssigned", ["userAssignedIdentities"] = new JsonObject { [identityId] = new JsonObject() } },
                ["properties"] = new JsonObject
                {
                    ["hardwareProfile"] = new JsonObject { ["vmSize"] = VmSize },
                    ["storageProfile"] = new JsonObject
                    {
                        ["diskControllerType"] = VmSize.EndsWith("_v7", StringComparison.Ordinal) ? "NVMe" : "SCSI",
                        ["imageReference"] = new JsonObject { ["communityGalleryImageId"] = FedoraImage },
                        ["osDisk"] = new JsonObject
                        {
                            ["createOption"] = "FromImage", ["deleteOption"] = "Delete",
                            ["managedDisk"] = new JsonObject { ["storageAccountType"] = "Standard_LRS" },
                        },
                    },
                    ["osProfile"] = new JsonObject
                    {
                        ["computerName"] = "seseragi",
                        ["adminUsername"] = "seseragi",
                        // Required, never used: no inbound port is open, and the password is not kept.
                        ["adminPassword"] = ThrowawayPassword(),
                        ["customData"] = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(script.Replace("\r", ""))),
                    },
                    ["networkProfile"] = new JsonObject
                    {
                        ["networkApiVersion"] = "2022-11-01",
                        ["networkInterfaceConfigurations"] = new JsonArray(new JsonObject
                        {
                            ["name"] = name + "-nic",
                            ["properties"] = new JsonObject
                            {
                                ["primary"] = true,
                                ["deleteOption"] = "Delete",
                                ["ipConfigurations"] = new JsonArray(new JsonObject
                                {
                                    ["name"] = "ip",
                                    ["properties"] = new JsonObject
                                    {
                                        ["subnet"] = new JsonObject { ["id"] = vnetId + "/subnets/default" },
                                        // New networks have no default way out; the address is the VM's (outbound only).
                                        ["publicIPAddressConfiguration"] = new JsonObject
                                        {
                                            ["name"] = name + "-ip",
                                            ["sku"] = new JsonObject { ["name"] = "Standard" },
                                            ["properties"] = new JsonObject { ["deleteOption"] = "Delete", ["publicIPAllocationMethod"] = "Static" },
                                        },
                                    },
                                }),
                            },
                        }),
                    },
                },
            }, TimeSpan.FromMinutes(10));

            // The dead-man switch: deallocated 30 minutes from now should this process die before finally.
            RealAzureArm.Put(scheduleId, ScheduleApi, new JsonObject
            {
                ["location"] = location,
                ["tags"] = Tags(),
                ["properties"] = new JsonObject
                {
                    ["status"] = "Enabled",
                    ["taskType"] = "ComputeVmShutdownTask",
                    ["dailyRecurrence"] = new JsonObject { ["time"] = DateTime.UtcNow.AddMinutes(30).ToString("HHmm", System.Globalization.CultureInfo.InvariantCulture) },
                    ["timeZoneId"] = "UTC",
                    ["targetResourceId"] = vmId,
                    ["notificationSettings"] = new JsonObject { ["status"] = "Disabled" },
                },
            }, TimeSpan.FromMinutes(2));

            string result = AwaitResult(container, $"{prefix}/result.txt", vmId, TimeSpan.FromMinutes(15));
            string[] lines = [.. result.Split('\n', StringSplitOptions.RemoveEmptyEntries)];
            Assert.True(lines.Length == 3, result);
            foreach (var (line, backend) in lines.Zip(["managed", "native", "managed"]))
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
            DeleteAll(vmId, scheduleId, vnetId);
        }

        // Nothing of this run is left: no VM, disk, network card, address, schedule or network.
        var left = RealAzureArm.Tagged(Tag).Where(r => r.Value == stamp).ToList();
        var clock = System.Diagnostics.Stopwatch.StartNew();
        while (left.Count > 0 && clock.Elapsed < TimeSpan.FromMinutes(3))
        {
            Thread.Sleep(TimeSpan.FromSeconds(10));
            left = [.. RealAzureArm.Tagged(Tag).Where(r => r.Value == stamp)];
        }
        Assert.True(left.Count == 0, "Left behind: " + string.Join(", ", left.Select(r => r.Id)));
        // The disk, card and address carry no tag of their own: check by name.
        var named = Untagged(name);
        Assert.True(named.Count == 0, "Left behind: " + string.Join(", ", named));
    }

    private static string ThrowawayPassword()
    {
        const string alphabet = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789";
        string random = System.Security.Cryptography.RandomNumberGenerator.GetString(alphabet, 28);
        return random + "aZ9!"; // every character class Azure asks for
    }

    /// <summary>Waits for the probe's report; fails early when the VM is gone or failed.</summary>
    private static string AwaitResult(BlobContainerClient container, string name, string vmId, TimeSpan timeout)
    {
        var blob = container.GetBlobClient(name);
        var clock = System.Diagnostics.Stopwatch.StartNew();
        while (clock.Elapsed < timeout)
        {
            if (blob.Exists().Value) return blob.DownloadContent().Value.Content.ToString();
            JsonNode? view = RealAzureArm.Get(vmId + "/instanceView", ComputeApi);
            Assert.True(view is not null, $"The VM is gone after {clock.Elapsed:mm\\:ss} and reported nothing.");
            var codes = view!["statuses"]!.AsArray().Select(s => s!["code"]!.GetValue<string>()).ToList();
            Assert.True(!codes.Any(c => c is "PowerState/stopped" or "PowerState/deallocated" or "ProvisioningState/failed"),
                $"The VM is {string.Join(", ", codes)} after {clock.Elapsed:mm\\:ss} and reported nothing.");
            Thread.Sleep(TimeSpan.FromSeconds(15));
        }
        throw new TimeoutException($"No report from the VM within {timeout}.");
    }

    private static void DeleteAll(string vmId, string scheduleId, string vnetId)
    {
        (string, string)[] items = [(vmId, ComputeApi), (scheduleId, ScheduleApi), (vnetId, NetworkApi)];
        foreach (var (id, api) in items)
        {
            try
            {
                RealAzureArm.Delete(id, api, TimeSpan.FromMinutes(10));
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"Deleting {id} failed: {e.Message}");
            }
        }
    }

    /// <summary>Resources of the group whose name starts with the run's name (disk, card, address).</summary>
    private static List<string> Untagged(string name)
    {
        JsonNode body = RealAzureArm.Send(HttpMethod.Get, $"/subscriptions/{RealAzureArm.Subscription}/resourceGroups/{RealAzureArm.Group}/resources", "2021-04-01").Body!;
        return [.. body["value"]!.AsArray().Select(r => r!["id"]!.GetValue<string>())
            .Where(id => id[(id.LastIndexOf('/') + 1)..].StartsWith(name, StringComparison.OrdinalIgnoreCase))];
    }

    /// <summary>Deletes test resources of runs that started more than 30 minutes ago (runs that crashed), VMs first.</summary>
    private static void Sweep()
    {
        string cutoff = DateTime.UtcNow.AddMinutes(-30).ToString("yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture);
        var old = RealAzureArm.Tagged(Tag).Where(r => string.CompareOrdinal(r.Value, cutoff) < 0).ToList();
        static int Order(string type) => type switch
        {
            "Microsoft.Compute/virtualMachines" => 0,
            "Microsoft.Network/virtualNetworks" => 2,
            _ => 1,
        };
        foreach (var (id, type, _) in old.OrderBy(r => Order(r.Type)))
        {
            string api = type.StartsWith("Microsoft.Compute", StringComparison.Ordinal) ? ComputeApi
                : type.StartsWith("Microsoft.DevTestLab", StringComparison.Ordinal) ? ScheduleApi : NetworkApi;
            try
            {
                RealAzureArm.Delete(id, api, TimeSpan.FromMinutes(10));
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"Sweeping {id} failed: {e.Message}");
            }
        }
        // Disks, cards and addresses of a crashed run's VM go with that VM; ones left over by a failed creation are
        // named after it: seseragi-xxxxxxxx-nic / -ip and the disk. They are reported, not guessed at.
    }
}
