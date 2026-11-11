using System.Text.Json;
using Polars.CSharp;
using Polars.NET.Core.Delta;

// Usage: RealCloudProbe s3://bucket/table <region>
//        RealCloudProbe abfss://container@account.dfs.core.windows.net/table [client id]
//        RealCloudProbe gs://bucket/table
// No keys: on S3 only the region, on Azure nothing or the client id of the identity to use, on GCS nothing, so the
// credentials come from the machine (IMDSv2 on EC2, the managed identity endpoint on an Azure VM, the metadata server
// on GCE).
// Prints one JSON line: {"backend":..., "implementation":..., "ok":true, ...} or {"ok":false, "step":..., "error":...}.
string url = args[0];
var keys = new Dictionary<string, string>();
if (url.StartsWith("s3://", StringComparison.Ordinal)) keys["aws_region"] = args[1];
else if (args.Length > 1) keys["azure_client_id"] = args[1];
string backend = Environment.GetEnvironmentVariable("POLARS_NET_OBJECT_STORE") ?? "auto";
string step = "start";
var report = new Dictionary<string, object?> { ["backend"] = backend };
try
{
    var options = new CloudOptions { Credentials = keys };

    step = "open the store";
    using (var fs = new ObjectStoreFileSystem(url, CloudArgs.ForLocation(url, [.. keys.Keys], [.. keys.Values])))
    {
        report["implementation"] = fs.Implementation;
        step = "put-if-absent, read, list, delete";
        if (!fs.TryCreate("probe/hello.txt", "hello"u8.ToArray())) throw new InvalidOperationException("The probe file existed already.");
        if (System.Text.Encoding.UTF8.GetString(fs.ReadAll("probe/hello.txt")!) != "hello") throw new InvalidOperationException("Read back something else.");
        if (fs.List("probe").Count != 1) throw new InvalidOperationException("The listing does not hold the probe file.");
        if (!fs.Delete("probe/hello.txt")) throw new InvalidOperationException("The probe file was not deleted.");
    }

    step = "create a table";
    Delta.CreateTable(url, new PolarsSchema().Add("Id", DataType.Int64).Add("Name", DataType.String),
        properties: new() { [DeltaTableProperties.EnableRowTracking] = "true" },
        identityColumns: [new DeltaIdentityColumn("Id")], cloudOptions: options);
    step = "write";
    using (var rows = DataFrame.FromColumns(new { Name = new[] { "a", "b", "c" } }))
        rows.WriteDelta(url, mode: DeltaSaveMode.Append, cloudOptions: options);
    step = "delete";
    Delta.Delete(url, Polars.CSharp.Polars.Col("Name") == "b", cloudOptions: options);
    step = "read";
    using var read = DataFrame.ReadDelta(url, cloudOptions: options, rowIdColumn: "row_id").Sort(["Id"]);
    report["ids"] = read["Id"].ToArray<long>();
    report["rowIds"] = read["row_id"].ToArray<long>();
    report["ok"] = true;
}
catch (Exception e)
{
    report["ok"] = false;
    report["step"] = step;
    report["error"] = $"{e.GetType().Name}: {e.Message}";
}

using var stdout = Console.OpenStandardOutput();
using (var json = new Utf8JsonWriter(stdout))
{
    json.WriteStartObject();
    foreach (var (key, value) in report)
    {
        switch (value)
        {
            case null: json.WriteNull(key); break;
            case bool b: json.WriteBoolean(key, b); break;
            case string s: json.WriteString(key, s); break;
            case long[] numbers:
                json.WriteStartArray(key);
                foreach (long n in numbers) json.WriteNumberValue(n);
                json.WriteEndArray();
                break;
        }
    }
    json.WriteEndObject();
}
Console.WriteLine();
return report["ok"] is true ? 0 : 1;
