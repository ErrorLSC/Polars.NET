#nullable enable
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Lakehouse.Parquet;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>Delta.Purge (Spark's REORG … APPLY (PURGE)): deleted rows and dropped columns leave the current files.</summary>
public class DeltaPurgeTests
{
    private sealed record Row(int Id, short Size, string Name);
    private sealed record Narrow(int Id, short Size);

    private static void InTable(string name, Action<string> body)
    {
        string path = Path.Combine(Path.GetTempPath(), $"delta_purge_{name}_{Guid.NewGuid():N}");
        try
        {
            body(path);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, true);
        }
    }

    private static void Write<T>(string path, IEnumerable<T> rows)
    {
        using DataFrame df = DataFrame.From(rows);
        df.WriteDelta(path, mode: DeltaSaveMode.Append, mkdir: true);
    }

    private static Snapshot Latest(string path) => Snapshot.Load(new LocalFileSystem(path));

    private static string[] Operations(string path, long from) =>
        [.. History.Read(new LocalFileSystem(path)).Where(h => h.Version >= from).OrderBy(h => h.Version).Select(h => h.CommitInfo?.Operation ?? "(none)")];

    /// <summary>The footers of the table's current files, by path.</summary>
    private static Dictionary<string, ParquetMetadata> Footers(string path)
    {
        Snapshot snapshot = Latest(path);
        var footers = ParquetFooters.Read(new LocalFileSystem(path), [.. snapshot.Files.Select(f => f.DecodedPath)]);
        return snapshot.Files.Select((f, i) => (f.Path, footers[i].Footer)).ToDictionary(p => p.Path, p => p.Footer);
    }

    /// <summary>The current files whose footer still holds the column (Name was written before column mapping came in,
    /// so its physical name is its name).</summary>
    private static int FilesHolding(string path, string column) => Footers(path).Values.Count(f => f.Schema.Any(e => e.Name == column));

    [Fact]
    public void Purge_RewritesTheFilesHoldingDroppedColumnsOrDeletedRows_Only()
    {
        InTable("mixed", path =>
        {
            Write<Row>(path, [new(1, 10, "a"), new(2, 20, "b"), new(3, 30, "c")]);
            Delta.SetTableProperties(path, new() { ["delta.columnMapping.mode"] = "name", ["delta.enableDeletionVectors"] = "true" });
            Delta.DropColumns(path, ["Name"]);
            Write<Narrow>(path, [new(4, 40), new(5, 50)]);
            Write<Narrow>(path, [new(6, 60), new(7, 70)]);
            Delta.Delete(path, Col("Id") == 4);
            Snapshot before = Latest(path);
            AddAction untouched = before.Files.Single(f => f.DeletionVector is null && f.ParsedStats?.NumRecords == 2);
            Assert.Equal(1, FilesHolding(path, "Name")); // the first file still holds Name

            Assert.Equal(2, Delta.Purge(path));

            Snapshot purged = Latest(path);
            Assert.Equal(["REORG"], Operations(path, before.Version + 1));
            Assert.Contains(purged.Files, f => f.Path == untouched.Path);
            Assert.DoesNotContain(purged.Files, f => f.DeletionVector is not null);
            Assert.Equal(0, FilesHolding(path, "Name"));
            using (DataFrame read = DataFrame.ReadDelta(path).Sort("Id"))
            {
                Assert.Equal(["Id", "Size"], read.Columns);
                Assert.Equal([1, 2, 3, 5, 6, 7], read.Column("Id").ToArray<int>());
            }

            // Nothing left to purge: no commit.
            Assert.Equal(0, Delta.Purge(path));
            Assert.Equal(purged.Version, Latest(path).Version);
        });
    }

    [Fact]
    public void DroppingDeletionVectors_AlsoPurgesTheDroppedColumns_AsSpark()
    {
        InTable("dv", path =>
        {
            Write<Row>(path, [new(1, 10, "a"), new(2, 20, "b")]);
            Delta.SetTableProperties(path, new() { ["delta.columnMapping.mode"] = "name", ["delta.enableDeletionVectors"] = "true" });
            Delta.DropColumns(path, ["Name"]);
            Write<Narrow>(path, [new(3, 30), new(4, 40)]);
            Delta.Delete(path, Col("Id") == 3);

            Delta.DropFeature(path, DeltaTableFeatures.DeletionVectors);

            Assert.Equal(0, FilesHolding(path, "Name"));
            Assert.DoesNotContain(Latest(path).Files, f => f.DeletionVector is not null);
            using DataFrame read = DataFrame.ReadDelta(path).Sort("Id");
            Assert.Equal([1, 2, 4], read.Column("Id").ToArray<int>());
        });
    }

    [Fact]
    public void TheMaterializedRowTrackingColumns_AreNotPurged()
    {
        InTable("rows", path =>
        {
            Write<Narrow>(path, [new(1, 10), new(2, 20)]);
            Delta.SetTableProperties(path, new() { ["delta.enableRowTracking"] = "true" });
            Delta.Update(path, Col("Id") == 2, new Dictionary<string, Expr> { ["Size"] = Col("Size") + Lit((short)1) });
            Snapshot before = Latest(path);
            var (rowId, _) = RowTracking.MaterializedColumns(before.Metadata)!.Value;
            Assert.Contains(Footers(path).Values, f => f.Schema.Any(e => e.Name == rowId)); // the update wrote them

            Assert.Equal(0, Delta.Purge(path));
            Assert.Equal(before.Version, Latest(path).Version);
        });
    }
}
