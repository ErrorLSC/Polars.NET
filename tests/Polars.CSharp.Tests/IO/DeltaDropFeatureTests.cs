#nullable enable
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Engine;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>Delta.DropFeature on tables Polars.NET wrote: the features whose drop rewrites data files.</summary>
public class DeltaDropFeatureTests
{
    private sealed record Row(int Id, short Size, string Name);
    private sealed record Wide(int Id, int Size, string Name);
    private sealed record Labeled(int Id, short Size, string Label);

    private static void InTable(string name, Action<string> body)
    {
        string path = Path.Combine(Path.GetTempPath(), $"delta_drop_{name}_{Guid.NewGuid():N}");
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

    [Fact]
    public void DeletionVectors_ArePurged_TheRowsStay()
    {
        InTable("dv", path =>
        {
            Write<Row>(path, [new(1, 10, "a"), new(2, 20, "b"), new(3, 30, "c")]);
            Write<Row>(path, [new(4, 40, "d"), new(5, 50, "e")]);
            Delta.SetTableProperties(path, new() { ["delta.enableDeletionVectors"] = "true" });
            Delta.Delete(path, Col("Id") == 2);
            Delta.Delete(path, Col("Id") >= 4); // every row of the second file: the file is removed
            Assert.Contains(Latest(path).Files, f => f.DeletionVector is not null);
            long from = Latest(path).Version + 1;

            Delta.DropFeature(path, DeltaTableFeatures.DeletionVectors);

            Snapshot dropped = Latest(path);
            Assert.DoesNotContain(dropped.Files, f => f.DeletionVector is not null);
            Assert.DoesNotContain("deletionVectors", dropped.Protocol.ReaderFeatures ?? []);
            Assert.Equal(["UNSET TBLPROPERTIES", "REORG", "Deletion Vector Tombstones", "Empty Commit", "Empty Commit", "Empty Commit", "DROP FEATURE"],
                Operations(path, from));
            using DataFrame read = DataFrame.ReadDelta(path).Sort("Id");
            Assert.Equal([1, 3], read.Column("Id").ToArray<int>());
        });
    }

    [Fact]
    public void TypeWidening_RewritesTheFilesInOlderTypes_Only()
    {
        InTable("tw", path =>
        {
            Write<Row>(path, [new(1, 10, "a")]);
            Delta.SetTableProperties(path, new() { ["delta.enableTypeWidening"] = "true" });
            Delta.ChangeColumnType(path, "Size", DataType.Int32);
            Write<Wide>(path, [new(2, 70000, "b")]);
            string newer = Latest(path).Files.MaxBy(f => f.ModificationTime)!.Path;
            long from = Latest(path).Version + 1;

            Delta.DropFeature(path, "typeWidening");

            Snapshot dropped = Latest(path);
            Assert.Contains(dropped.Files, f => f.Path == newer); // written in the new type: not rewritten
            Assert.Equal(2, dropped.Files.Count);
            Assert.DoesNotContain("delta.typeChanges", dropped.Metadata.SchemaString);
            Assert.Equal(["UNSET TBLPROPERTIES", "REORG", "DROP FEATURE", "Empty Commit", "Empty Commit", "Empty Commit", "DROP FEATURE"],
                Operations(path, from));
            using DataFrame read = DataFrame.ReadDelta(path).Sort("Id");
            Assert.Equal([10, 70000], read.Column("Size").ToArray<int>());
        });
    }

    [Fact]
    public void ColumnMapping_RewritesEveryFileUnderTheLogicalNames()
    {
        InTable("cm", path =>
        {
            Write<Row>(path, [new(1, 10, "a"), new(2, 20, "b")]);
            Delta.SetTableProperties(path, new() { ["delta.columnMapping.mode"] = "name" });
            Delta.RenameColumn(path, "Name", "Label");
            Write<Labeled>(path, [new(3, 30, "c")]);
            long from = Latest(path).Version + 1;

            Delta.DropFeature(path, DeltaTableFeatures.ColumnMapping);

            Snapshot dropped = Latest(path);
            Assert.Equal(ColumnMappingMode.None, dropped.ColumnMappingMode);
            Assert.DoesNotContain("physicalName", dropped.Metadata.SchemaString);
            Assert.All(dropped.Files, f => Assert.DoesNotContain("col-", f.Stats));
            Assert.Equal(["REMOVE COLUMN MAPPING", "Empty Commit", "Empty Commit", "Empty Commit", "DROP FEATURE"], Operations(path, from));
            using DataFrame read = DataFrame.ReadDelta(path).Sort("Id");
            Assert.Equal(["Id", "Size", "Label"], read.Columns);
            Assert.Equal(["a", "b", "c"], read.Column("Label").ToArray<string>());
        });
    }
}
