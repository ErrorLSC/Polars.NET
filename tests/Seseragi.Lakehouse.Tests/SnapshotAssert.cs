using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Xunit;

namespace Seseragi.Lakehouse.Tests;

/// <summary>Compares two snapshots of one version: one read through a checkpoint, one replayed from the commits.</summary>
internal static class SnapshotAssert
{
    /// <summary>Asserts two snapshots of one version agree on everything a checkpoint carries.</summary>
    public static void Same(Snapshot expected, Snapshot actual)
    {
        Assert.Equal(expected.Version, actual.Version);

        Assert.Equal(expected.Protocol.MinReaderVersion, actual.Protocol.MinReaderVersion);
        Assert.Equal(expected.Protocol.MinWriterVersion, actual.Protocol.MinWriterVersion);
        Assert.Equal(expected.Protocol.ReaderFeatures ?? [], actual.Protocol.ReaderFeatures ?? []);
        Assert.Equal(expected.Protocol.WriterFeatures ?? [], actual.Protocol.WriterFeatures ?? []);

        Assert.Equal(expected.Metadata.Id, actual.Metadata.Id);
        Assert.Equal(expected.Metadata.Name, actual.Metadata.Name);
        Assert.Equal(expected.Metadata.Description, actual.Metadata.Description);
        Assert.Equal(expected.Metadata.SchemaString, actual.Metadata.SchemaString);
        Assert.Equal(expected.Metadata.PartitionColumns, actual.Metadata.PartitionColumns);
        Assert.Equal(expected.Metadata.FormatProvider, actual.Metadata.FormatProvider);
        Assert.Equal(Sorted(expected.Metadata.FormatOptions), Sorted(actual.Metadata.FormatOptions));
        Assert.Equal(Sorted(expected.Metadata.Configuration), Sorted(actual.Metadata.Configuration));
        Assert.Equal(expected.Metadata.CreatedTime, actual.Metadata.CreatedTime);

        Assert.Equal(expected.AppTransactions.Keys.Order(), actual.AppTransactions.Keys.Order());
        foreach (var (app, txn) in expected.AppTransactions)
        {
            Assert.Equal(txn.Version, actual.AppTransactions[app].Version);
            Assert.Equal(txn.LastUpdated, actual.AppTransactions[app].LastUpdated);
        }
        Assert.Equal(expected.DomainMetadata.Keys.Order(), actual.DomainMetadata.Keys.Order());
        foreach (var (name, domain) in expected.DomainMetadata)
            Assert.Equal(domain.Configuration, actual.DomainMetadata[name].Configuration);

        Assert.Equal(expected.Files.Select(Key).Order(), actual.Files.Select(Key).Order());
        Dictionary<string, AddAction> byKey = actual.Files.ToDictionary(Key);
        foreach (AddAction want in expected.Files)
        {
            AddAction got = byKey[Key(want)];
            Assert.Equal(want.Size, got.Size);
            Assert.Equal(want.ModificationTime, got.ModificationTime);
            Assert.Equal(Sorted(want.PartitionValues), Sorted(got.PartitionValues));
            Assert.Equal(Sorted(want.Tags), Sorted(got.Tags));
            Assert.Equal(want.BaseRowId, got.BaseRowId);
            Assert.Equal(want.DefaultRowCommitVersion, got.DefaultRowCommitVersion);
            Assert.Equal(want.ClusteringProvider, got.ClusteringProvider);
            Assert.Equal(want.DeletionVector?.Offset, got.DeletionVector?.Offset);
            Assert.Equal(want.DeletionVector?.SizeInBytes, got.DeletionVector?.SizeInBytes);
            Assert.Equal(want.DeletionVector?.Cardinality, got.DeletionVector?.Cardinality);
            AssertSameStats(want.Path, want.Stats, got.Stats);
        }
    }

    private static string Key(AddAction a) => $"{a.Path}|{a.DeletionVector?.UniqueId}";

    private static List<KeyValuePair<string, string?>> Sorted<T>(IReadOnlyDictionary<string, T>? map) where T : class? =>
        map is null ? [] : [.. map.Select(kv => new KeyValuePair<string, string?>(kv.Key, kv.Value as string)).OrderBy(kv => kv.Key, StringComparer.Ordinal)];

    /// <summary>
    /// The statistics agree: the same text, or (when the checkpoint kept them as a struct) every value the checkpoint
    /// has equals the commit's, and it has the row count. A struct holds only the table's current statistics columns,
    /// so it may lack values the commit text has.
    /// </summary>
    private static void AssertSameStats(string path, string? expected, string? actual)
    {
        if (expected == actual) return;
        Assert.True(expected is not null && actual is not null, $"{path}: statistics {expected ?? "null"} vs {actual ?? "null"}");
        using JsonDocument want = JsonDocument.Parse(expected!);
        using JsonDocument got = JsonDocument.Parse(actual!);
        Assert.True(got.RootElement.TryGetProperty("numRecords", out _), $"{path}: no row count in {actual}");
        int compared = Compare(path, want.RootElement, got.RootElement);
        Assert.True(compared > 1, $"{path}: nothing compared in {actual}");
    }

    private static int Compare(string path, JsonElement want, JsonElement got)
    {
        if (got.ValueKind == JsonValueKind.Object)
        {
            Assert.Equal(JsonValueKind.Object, want.ValueKind);
            int count = 0;
            foreach (JsonProperty p in got.EnumerateObject())
            {
                Assert.True(want.TryGetProperty(p.Name, out JsonElement w), $"{path}: {p.Name} is not in the commit's statistics");
                count += Compare($"{path}.{p.Name}", w, p.Value);
            }
            return count;
        }
        Assert.True(SameValue(want, got), $"{path}: {want.GetRawText()} vs {got.GetRawText()}");
        return 1;
    }

    private static bool SameValue(JsonElement want, JsonElement got) => (want.ValueKind, got.ValueKind) switch
    {
        (JsonValueKind.Number, JsonValueKind.Number) => want.TryGetDecimal(out decimal a) && got.TryGetDecimal(out decimal b)
            ? a == b
            : want.GetDouble().Equals(got.GetDouble()),
        // Text exactly: dates and timestamps are written back to the log as they are read.
        (JsonValueKind.String, JsonValueKind.String) => want.GetString() == got.GetString(),
        _ => want.GetRawText() == got.GetRawText(),
    };
}
