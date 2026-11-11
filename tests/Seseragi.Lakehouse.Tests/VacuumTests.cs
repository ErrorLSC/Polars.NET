using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Seseragi.Lakehouse.Tests;

public class VacuumTests
{
    private static readonly DateTimeOffset Now = DateTimeOffset.UtcNow;
    private static long DaysAgo(double days) => Now.AddDays(-days).ToUnixTimeMilliseconds();

    /// <summary>
    /// a.parquet removed 10 days ago, b.parquet removed a day ago, c.parquet live; plus files no commit references.
    /// Every data file was written long ago, except recent-orphan.parquet.
    /// </summary>
    private static (MemoryFileSystem Fs, Snapshot Latest) Table()
    {
        var fs = new MemoryFileSystem();
        var metadata = new MetadataAction { Id = "t", SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}""" };
        Transaction.Create(fs, new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 }, metadata).Commit();
        Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(new AddAction { Path = "a.parquet", Size = 10 }).Add(new AddAction { Path = "b.parquet", Size = 20 }).Commit();
        Transaction.Start(fs, Snapshot.Load(fs), "DELETE")
            .Add(new RemoveAction { Path = "a.parquet", DeletionTimestamp = DaysAgo(10), ExtendedFileMetadata = true, Size = 10 })
            .Add(new AddAction { Path = "c.parquet", Size = 30 }).Commit();
        Transaction.Start(fs, Snapshot.Load(fs), "DELETE")
            .Add(new RemoveAction { Path = "b.parquet", DeletionTimestamp = DaysAgo(1), ExtendedFileMetadata = true, Size = 20 }).Commit();

        string[] paths = [
                     "a.parquet", "b.parquet", "c.parquet", "orphan.parquet", "region=east/old.parquet", "_change_data/cdc-1.parquet",
                     ".hidden/x.parquet", "_SUCCESS", "_staging/y.parquet", "region=east/.part.crc",
                 ];
        foreach (string path in paths)
            fs.PutBytes(path, [1, 2, 3]);
        fs.Put("recent-orphan.parquet", "x", Now);
        return (fs, Snapshot.Load(fs));
    }

    private static string[] Data(MemoryFileSystem fs) => [.. fs.ListAll("").Select(f => f.Path).Where(p => !p.StartsWith("_delta_log/", StringComparison.Ordinal))];

    [Fact]
    public void Lite_DeletesOnlyFilesRemovedLongerAgoThanTheRetention()
    {
        var (fs, latest) = Table();
        VacuumResult result = Vacuum.Run(fs, latest, now: Now);

        Assert.Equal(["a.parquet"], result.Files);
        Assert.Equal(10, result.Bytes);
        Assert.DoesNotContain("a.parquet", Data(fs));
        Assert.Contains("b.parquet", Data(fs));
        Assert.Contains("orphan.parquet", Data(fs));

        // The vacuum is in the history: a start and an end commit, without actions.
        Snapshot after = Snapshot.Load(fs);
        Assert.Equal(latest.Version + 2, after.Version);
        string end = System.Text.Encoding.UTF8.GetString(fs.ReadAll($"_delta_log/{after.Version:D20}.json")!);
        Assert.Contains("\"operation\":\"VACUUM END\"", end);
        Assert.Contains("\"numDeletedFiles\":\"1\"", end);
        Assert.Equal(latest.Files.Select(f => f.Path), after.Files.Select(f => f.Path));

        // Nothing left to do: no further commits.
        Assert.Empty(Vacuum.Run(fs, after, now: Now).Files);
        Assert.Equal(after.Version, Snapshot.Load(fs).Version);
    }

    [Fact]
    public void Full_AlsoDeletesFilesNoCommitReferences_ButNotHiddenOrRecentOnes()
    {
        var (fs, latest) = Table();
        VacuumResult result = Vacuum.Run(fs, latest, mode: VacuumMode.Full, now: Now);

        Assert.Equal(["_change_data/cdc-1.parquet", "a.parquet", "orphan.parquet", "region=east/old.parquet"], result.Files);
        Assert.Equal(
            [".hidden/x.parquet", "_SUCCESS", "_staging/y.parquet", "b.parquet", "c.parquet", "recent-orphan.parquet", "region=east/.part.crc"],
            Data(fs));
    }

    [Fact]
    public void DryRun_DeletesNothing_AndShortRetentionsNeedTheCheckOff()
    {
        var (fs, latest) = Table();
        VacuumResult dry = Vacuum.Run(fs, latest, dryRun: true, mode: VacuumMode.Full, now: Now);
        Assert.True(dry.DryRun);
        Assert.Equal(4, dry.Files.Count);
        Assert.Contains("a.parquet", Data(fs));
        Assert.Equal(latest.Version, Snapshot.Load(fs).Version);

        Assert.Contains("shorter than the table's", Assert.Throws<DeltaException>(() => Vacuum.Run(fs, latest, TimeSpan.FromHours(1), now: Now)).Message);
        VacuumResult forced = Vacuum.Run(fs, latest, TimeSpan.FromHours(1), enforceRetention: false, now: Now);
        Assert.Equal(["a.parquet", "b.parquet"], forced.Files);
    }

    [Fact]
    public void DeletionVectorFiles_GoWithTheirExpiredRemoves_AndStayWhileReferenced()
    {
        var fs = new MemoryFileSystem();
        var metadata = new MetadataAction { Id = "t", SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}""" };
        var protocol = new ProtocolAction { MinReaderVersion = 3, MinWriterVersion = 7, ReaderFeatures = [TableFeatures.DeletionVectors], WriterFeatures = [TableFeatures.DeletionVectors] };
        Transaction.Create(fs, protocol, metadata).Commit();
        fs.PutBytes("f.parquet", [1]);
        DeletionVectorDescriptor first = Assert.Single(DeletionVector.WriteFile(fs, [DeletionVector.FromRows([1])]));
        DeletionVectorDescriptor second = Assert.Single(DeletionVector.WriteFile(fs, [DeletionVector.FromRows([1, 2])]));
        Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(new AddAction { Path = "f.parquet", DeletionVector = first }).Commit();
        Transaction.Start(fs, Snapshot.Load(fs), "DELETE")
            .Add(new RemoveAction { Path = "f.parquet", DeletionVector = first, DeletionTimestamp = DaysAgo(30) })
            .Add(new AddAction { Path = "f.parquet", DeletionVector = second }).Commit();

        VacuumResult result = Vacuum.Run(fs, Snapshot.Load(fs), now: Now);

        Assert.Equal([first.FilePath()], result.Files);
        Assert.NotNull(fs.ReadAll(second.FilePath()));
        Assert.NotNull(fs.ReadAll("f.parquet"));
    }

    [Theory]
    [InlineData("_delta_log/00000000000000000000.json", true)]
    [InlineData("_change_data/cdc.parquet", false)]
    [InlineData("x/_change_data/cdc.parquet", true)]
    [InlineData("_col=1/f.parquet", false)]
    [InlineData("region=east/_tmp/f.parquet", true)]
    [InlineData("region=east/.f.crc", true)]
    [InlineData("deletion_vector_x.bin", false)]
    public void HiddenFiles(string path, bool hidden) => Assert.Equal(hidden, Vacuum.IsHidden(path));
}
