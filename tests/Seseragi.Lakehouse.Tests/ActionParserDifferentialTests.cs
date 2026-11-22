using System.Text;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;

namespace Seseragi.Lakehouse.Tests;

/// <summary>
/// <see cref="ActionParser"/> (lines read token by token) against <see cref="JsonDocumentActionParser"/> (a JSON
/// document per line, the reader it replaced) as the oracle: the same actions, every field, or both refusing, on the
/// commits of the test tables, hand-made lines (repeated fields, fields of other kinds, escapes, nesting, line
/// framing), random action lines (valid and mutated), and commits large enough to be read in parts. One difference is
/// meant: a commit that is not UTF-8 is refused, where the oracle let some invalid bytes inside strings through.
/// </summary>
public class ActionParserDifferentialTests
{
    private static readonly string[] Cases =
    [
        """{"add":{"path":"a","size":1,"modificationTime":2,"dataChange":true}}""",
        """{"add":{"path":"a","path":"b","size":1,"size":2}}""",                          // the last of repeated fields counts
        """{"add":{"path":"a","size":1.5,"size":3}}""",                                 // a bad value overridden by a later one
        """{"add":{"path":"a","size":3,"size":1.5}}""",                                 // and the other way round
        """{"add":{"path":"a","size":"7","dataChange":"yes","stats":5}}""",             // other kinds read as absent
        """{"add":{"path":"a","size":99999999999999999999}}""",
        """{"add":{"path":"a","size":-0,"modificationTime":1e3}}""",
        """{"add":{"path":5}}""", """{"add":{}}""", """{"add":null}""", """{"add":[1]}""",
        """{"add":{"path":"pé\n\"q\"\\\/","partitionValues":{"kA":"v","n":null,"x":1,"o":{"a":[1, 2]},"b":true}}}""",
        """{"add":{"path":"a","partitionValues":{"k":"1","k":null}}}""",
        """{"add":{"path":"a","tags":{"t":"1"},"tags":null}}""",
        """{"add":{"path":"a","deletionVector":{"storageType":"u","pathOrInlineDv":"x","offset":1,"sizeInBytes":2,"cardinality":3}}}""",
        """{"add":{"path":"a","deletionVector":{"storageType":"u","pathOrInlineDv":"x","sizeInBytes":2}}}""",
        """{"add":{"path":"a","deletionVector":{"storageType":"u","pathOrInlineDv":"x","sizeInBytes":2,"cardinality":3,"offset":4294967297}}}""",
        """{"add":{"path":"a","stats":"{\"numRecords\":3,\"minValues\":{\"a\":\"x\\\"y\"}}"}}""",
        """{"remove":{"path":"r","deletionTimestamp":5,"dataChange":false,"extendedFileMetadata":true,"partitionValues":{"a":"b"},"size":9}}""",
        """{"metaData":{"id":"i","format":{"provider":"parquet","options":{"a":"b","n":null}},"schemaString":"{}","partitionColumns":["p",null],"configuration":{"x":"1","y":null},"createdTime":7}}""",
        """{"metaData":{"id":"i","schemaString":"{}","partitionColumns":["p",1]}}""",
        """{"metaData":{"id":"i","schemaString":"{}","partitionColumns":"p","format":"f"}}""",
        """{"metaData":{"id":"i","schemaString":"{}","format":{"provider":5,"options":[1]}}}""",
        """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":[]}}""",
        """{"protocol":{"minReaderVersion":3}}""", """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[["x"]]}}""",
        """{"txn":{"appId":"a","version":1,"lastUpdated":2}}""", """{"txn":{"appId":"a"}}""",
        """{"cdc":{"path":"c","partitionValues":{},"size":3,"dataChange":false}}""",
        """{"domainMetadata":{"domain":"d","configuration":"{\"a\":1}","removed":false}}""", """{"domainMetadata":{"domain":"d"}}""",
        """{"commitInfo":{"timestamp":1,"operation":"WRITE","operationParameters":{"mode":"Append","x":[1,{"y":null}]},"inCommitTimestamp":2}}""",
        """{"commitInfo":  { "timestamp" : 1 ,  "nested" : { "a" : [ 1 , 2 ] } }  }""",
        """{"sidecar":{"path":"s","sizeInBytes":1,"modificationTime":2}}""", """{"checkpointMetadata":{"version":4}}""", """{"checkpointMetadata":{}}""",
        """{"unknown":{"a":1}}""", """{"unknown":1,"add":{"path":"a"}}""", """{"add":5,"remove":{"path":"r"}}""",
        """{"add":{"path":"a"},"remove":{"path":"r"}}""", """{"add":{"path":"a"},"add":{"path":"b"}}""",
        """{"add":{"path":"a"},"x":[1,}""", """{"add":{"path":"a"}} x""", """{"add":{"path":"a"}}{}""", """[1]""", """5""", "\"s\"", "null",
        """{"add":{"path":"a",}}""", """{"add":{"path":"\uD800"}}""", """{"add":{"path":"a","tags":{"k":"\uDC00"}}}""",
        """{"add":{"path":"a"}}""" + "\r", " \t" + """{"add":{"path":"a"}}""", """{"add":{"path":"x"}}""",
        """{"add":{"path":"x","size":4}}""",
        Nested(62), Nested(63), Nested(64), Nested(65),
    ];

    // A line whose add carries a tag nested so that the line is depth levels deep.
    private static string Nested(int depth) =>
        """{"add":{"path":"a","tags":{"t":""" + new string('[', depth - 3) + new string(']', depth - 3) + "}}}";

    [Fact]
    public void HandMadeLines_ReadAsTheOracleReadsThem()
    {
        foreach (string line in Cases) Same(line);
    }

    [Fact]
    public void LineFraming_AsTheOracle()
    {
        string add = """{"add":{"path":"a"}}""", remove = """{"remove":{"path":"r"}}""";
        foreach (string text in (string[])[$"{add}\n{remove}\n", $"{add}\r\n{remove}\r\n", $"\n \n{add}\n\t\n{remove}", $"{add}\n{{\"add\":\n{{\"path\":\"b\"}}}}\n", "", "\n\n", $"{add}\n   "])
            Same(text);
    }

    [Fact]
    public void TheTestTablesCommits_ReadAsTheOracleReadsThem()
    {
        string root = Path.Combine(AppContext.BaseDirectory, "TestData");
        int files = 0;
        foreach (string file in Directory.EnumerateFiles(root, "*.json", SearchOption.AllDirectories).Where(f => f.Contains("_delta_log")))
        {
            Same(File.ReadAllText(file));
            files++;
        }
        Assert.True(files > 100, $"only {files} commit files found");
    }

    [Fact]
    public void RandomLines_ValidAndMutated_ReadAsTheOracleReadsThem()
    {
        var random = new Random(20261123);
        for (int n = 0; n < 30_000; n++)
        {
            string line = RandomAction(random);
            byte[] bytes = Encoding.UTF8.GetBytes(line);
            if (random.Next(2) == 0) bytes = Mutate(random, bytes);
            Same(bytes, $"case {n}");
        }
    }

    [Fact]
    public void LargeCommits_ReadInParts_AsTheOracle()
    {
        // Over 2 MiB: read in parts side by side, in order; a failing line anywhere fails the commit.
        var text = new StringBuilder();
        for (int i = 0; text.Length < 5 << 20; i++)
            text.Append($$"""{"add":{"path":"part-{{i}}.parquet","partitionValues":{"d":"{{i % 7}}"},"size":{{i}},"modificationTime":{{i}},"dataChange":true,"stats":"{\"numRecords\":""")
                .Append(i).Append("}\"}}\n");
        string commit = text.ToString();
        Same(commit);
        Assert.Equal(commit.Count(c => c == '\n'), ActionParser.ParseCommit(Encoding.UTF8.GetBytes(commit)).Count);
        int middle = commit.IndexOf('\n', commit.Length / 2) + 1;
        Same(commit[..middle] + "{\"add\":{\"path\":\n" + commit[middle..]);
        Same(commit[..middle] + "{\"add\":{\"size\":1}}\n" + commit[middle..]);
        // Failures in two parts: the one earlier in the commit is reported, as reading it in order would.
        int late = commit.IndexOf('\n', commit.Length * 4 / 5) + 1;
        string twice = commit[..middle] + "{\"add\":{\"size\":1}}\n" + commit[middle..late] + "{\"add\":\n" + commit[late..];
        var e = Assert.Throws<DeltaException>(() => ActionParser.ParseCommit(Encoding.UTF8.GetBytes(twice)));
        Assert.Equal("A 'add' action lacks the required field 'path'.", e.Message);
    }

    // ------------------------------------------------------------------

    private static void Same(string text) => Same(Encoding.UTF8.GetBytes(text), text);

    private static void Same(byte[] bytes, string label)
    {
        // A commit that is not UTF-8 is refused (a JSON document reader lets some such strings through).
        string oracle = System.Text.Unicode.Utf8.IsValid(bytes) ? Outcome(() => JsonDocumentActionParser.ParseCommit(bytes)) : "refused";
        string ours = Outcome(() => ActionParser.ParseCommit(bytes));
        if (oracle != ours) Assert.Fail($"{label}\n{Convert.ToHexString(bytes.AsSpan(0, Math.Min(bytes.Length, 400)))}\n oracle: {Cut(oracle)}\n ours:   {Cut(ours)}");

        static string Cut(string s) => s.Length > 2000 ? s[..2000] + "…" : s;
    }

    private static string Outcome(Func<List<DeltaAction>> parse)
    {
        try
        {
            return string.Join("\n", parse().Select(Describe));
        }
        catch (DeltaException)
        {
            return "refused";
        }
    }

    private static string Describe(DeltaAction action) => action switch
    {
        AddAction a => $"add {S(a.Path)} pv={M(a.PartitionValues)} size={a.Size} mt={a.ModificationTime} dc={a.DataChange} stats={S(a.Stats)} tags={M(a.Tags)} dv={D(a.DeletionVector)} brid={a.BaseRowId} drcv={a.DefaultRowCommitVersion} cp={S(a.ClusteringProvider)}",
        RemoveAction r => $"remove {S(r.Path)} dt={r.DeletionTimestamp} dc={r.DataChange} efm={r.ExtendedFileMetadata} pv={M(r.PartitionValues)} size={r.Size} dv={D(r.DeletionVector)} brid={r.BaseRowId} drcv={r.DefaultRowCommitVersion}",
        MetadataAction m => $"metaData {S(m.Id)} {S(m.Name)} {S(m.Description)} {S(m.FormatProvider)} fo={M(m.FormatOptions)} {S(m.SchemaString)} pc={L(m.PartitionColumns)} conf={M(m.Configuration)} ct={m.CreatedTime}",
        ProtocolAction p => $"protocol {p.MinReaderVersion} {p.MinWriterVersion} rf={L(p.ReaderFeatures)} wf={L(p.WriterFeatures)}",
        TxnAction t => $"txn {S(t.AppId)} {t.Version} {t.LastUpdated}",
        CdcAction c => $"cdc {S(c.Path)} pv={M(c.PartitionValues)} size={c.Size} tags={M(c.Tags)}",
        DomainMetadataAction d => $"domain {S(d.Domain)} {S(d.Configuration)} {d.Removed}",
        CommitInfoAction c => $"commitInfo {S(c.Json)} {c.Timestamp} {c.InCommitTimestamp} {S(c.Operation)}",
        SidecarAction s => $"sidecar {S(s.Path)} {s.SizeInBytes} {s.ModificationTime}",
        CheckpointMetadataAction c => $"checkpointMetadata {c.Version}",
        _ => action.GetType().Name,
    };

    private static string S(string? s) => s is null ? "∅" : $"«{s}»";

    private static string L(IReadOnlyList<string>? list) => list is null ? "∅" : $"[{string.Join(",", list.Select(S))}]";

    private static string M<TValue>(IReadOnlyDictionary<string, TValue>? map) =>
        map is null ? "∅" : $"{{{string.Join(",", map.OrderBy(p => p.Key, StringComparer.Ordinal).Select(p => $"{S(p.Key)}:{S(p.Value?.ToString())}"))}}}";

    private static string D(DeletionVectorDescriptor? d) =>
        d is null ? "∅" : $"{S(d.StorageType)}/{S(d.PathOrInlineDv)}/{d.Offset}/{d.SizeInBytes}/{d.Cardinality}";

    // ------------------------------------------------------------------ random action lines

    private static string RandomAction(Random random)
    {
        string[] kinds = ["add", "remove", "metaData", "protocol", "txn", "cdc", "domainMetadata", "commitInfo", "sidecar", "checkpointMetadata", "other"];
        string kind = kinds[random.Next(kinds.Length)];
        string[] fields = kind switch
        {
            "add" => ["path", "partitionValues", "size", "modificationTime", "dataChange", "stats", "tags", "deletionVector", "baseRowId", "defaultRowCommitVersion", "clusteringProvider"],
            "remove" => ["path", "deletionTimestamp", "dataChange", "extendedFileMetadata", "partitionValues", "size", "deletionVector", "baseRowId", "defaultRowCommitVersion"],
            "metaData" => ["id", "name", "description", "format", "schemaString", "partitionColumns", "configuration", "createdTime"],
            "protocol" => ["minReaderVersion", "minWriterVersion", "readerFeatures", "writerFeatures"],
            "txn" => ["appId", "version", "lastUpdated"],
            "cdc" => ["path", "partitionValues", "size", "tags"],
            "domainMetadata" => ["domain", "configuration", "removed"],
            "commitInfo" => ["timestamp", "inCommitTimestamp", "operation", "operationParameters"],
            "sidecar" => ["path", "sizeInBytes", "modificationTime"],
            "checkpointMetadata" => ["version"],
            _ => ["a", "b"],
        };
        var text = new StringBuilder("{");
        if (random.Next(5) == 0) text.Append("\"extra\":").Append(Value(random, "any", 0)).Append(',');
        text.Append('"').Append(kind).Append("\":{");
        bool first = true;
        foreach (string field in fields)
        {
            int times = random.Next(8) switch { 0 => 0, 1 => 2, _ => 1 }; // absent, repeated, or once
            for (int t = 0; t < times; t++)
            {
                if (!first) text.Append(',');
                first = false;
                text.Append(random.Next(20) == 0 ? Escaped(field) : $"\"{field}\"").Append(':').Append(Value(random, field, 0));
            }
        }
        text.Append('}');
        if (random.Next(5) == 0) text.Append(",\"add\":{\"path\":\"second\"}");
        return text.Append('}').ToString();

        static string Escaped(string name) => "\"" + string.Concat(name.Select(c => $"\\u{(int)c:x4}")) + "\"";
    }

    // A value for a field: mostly of its kind, sometimes of another.
    private static string Value(Random random, string field, int depth)
    {
        string kind = random.Next(6) == 0 ? "any" : field switch
        {
            "path" or "stats" or "id" or "name" or "description" or "schemaString" or "appId" or "domain" => "string",
            "partitionValues" or "tags" or "configuration" or "operationParameters" => "map",
            "deletionVector" => "dv",
            "format" => "format",
            "partitionColumns" or "readerFeatures" or "writerFeatures" => "strings",
            "dataChange" or "extendedFileMetadata" or "removed" => "bool",
            "operation" or "clusteringProvider" => "string",
            _ => "integer",
        };
        if (field == "configuration" && random.Next(2) == 0) kind = "string"; // domainMetadata's is text, metaData's a map
        return kind switch
        {
            "string" => RandomString(random),
            "integer" => random.Next(10) switch
            {
                0 => "1.5", 1 => "1e3", 2 => "-0", 3 => "99999999999999999999", 4 => "9223372036854775807", 5 => "-9223372036854775808",
                _ => random.NextInt64(-1000, 1L << 50).ToString(),
            },
            "bool" => random.Next(3) switch { 0 => "true", 1 => "false", _ => "null" },
            "map" => "{" + string.Join(",", Enumerable.Range(0, random.Next(4)).Select(i => $"\"k{random.Next(3)}\":{(random.Next(4) == 0 ? Value(random, "any", depth + 1) : RandomString(random))}")) + "}",
            "strings" => "[" + string.Join(",", Enumerable.Range(0, random.Next(4)).Select(_ => random.Next(8) switch { 0 => "null", 1 => "1", _ => RandomString(random) })) + "]",
            "dv" => "{" + string.Join(",", ((string[])["storageType", "pathOrInlineDv", "offset", "sizeInBytes", "cardinality"]).Where(_ => random.Next(6) != 0)
                .Select(f => $"\"{f}\":{(f is "storageType" or "pathOrInlineDv" ? RandomString(random) : Value(random, "integer", depth + 1))}")) + "}",
            "format" => $"{{\"provider\":{RandomString(random)},\"options\":{Value(random, "configuration", depth + 1)}}}",
            _ => depth > 3 ? "null" : random.Next(7) switch
            {
                0 => "null", 1 => "true", 2 => "3.25", 3 => RandomString(random), 4 => "[1,\"x\",{\"y\":[]}]",
                5 => "{\"n\":" + Value(random, "any", depth + 1) + "}", _ => "[]",
            },
        };
    }

    private static string RandomString(Random random)
    {
        var text = new StringBuilder("\"");
        for (int p = random.Next(5); p > 0; p--)
            text.Append(random.Next(8) switch
            {
                0 => "\\\"", 1 => "\\\\", 2 => "\\n", 3 => "\\u00e9", 4 => "中文", 5 => "\\uD83D\\uDE00", 6 => "{\\\"numRecords\\\":3}",
                _ => "part-0000" + random.Next(100),
            });
        return text.Append('"').ToString();
    }

    private static byte[] Mutate(Random random, byte[] bytes)
    {
        ReadOnlySpan<byte> choices = "{}[]:,\"\\\n 0-.etnx"u8;
        List<byte> list = [.. bytes];
        for (int m = random.Next(1, 3); m > 0 && list.Count > 0; m--)
        {
            byte b = random.Next(10) == 0 ? (byte)random.Next(0x80, 0x100) : choices[random.Next(choices.Length)];
            int at = random.Next(list.Count);
            switch (random.Next(3))
            {
                case 0: list[at] = b; break;
                case 1: list.Insert(at, b); break;
                default: list.RemoveAt(at); break;
            }
        }
        return [.. list];
    }
}
