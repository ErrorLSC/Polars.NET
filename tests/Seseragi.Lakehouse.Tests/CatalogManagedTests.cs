using System.Text;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Seseragi.Lakehouse.Tests;

/// <summary>Catalog-managed tables (the catalogManaged feature): read and committed through the managing catalog.</summary>
public class CatalogManagedTests
{
    private const string ProtocolLine =
        """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["catalogManaged","deletionVectors"],"writerFeatures":["catalogManaged","inCommitTimestamp","deletionVectors"]}}""";

    private static string MetadataLine(bool inCommitTimestamps = true) =>
        Lines.Metadata(configuration: inCommitTimestamps ? "\"delta.enableInCommitTimestamps\":\"true\"" : "");

    private static string CommitInfoLine(long inCommitTimestamp) =>
        $$$"""{"commitInfo":{"inCommitTimestamp":{{{inCommitTimestamp}}},"timestamp":{{{inCommitTimestamp}}},"operation":"WRITE"}}""";

    /// <summary>A catalog-managed table at version 0, created (and published) as Spark creates one.</summary>
    private static (MemoryFileSystem Store, FakeCatalog Catalog) NewTable(bool inCommitTimestamps = true)
    {
        var store = new MemoryFileSystem().Commit(0, CommitInfoLine(1000), ProtocolLine, MetadataLine(inCommitTimestamps));
        return (store, new FakeCatalog(store));
    }

    private static AddAction NewFile(string path) => new() { Path = path, Size = 10, ModificationTime = 5, DataChange = true };

    private static string Text(byte[]? bytes) => Encoding.UTF8.GetString(bytes ?? throw new InvalidOperationException("missing file"));

    [Fact]
    public void ByPath_TheTableIsRefused()
    {
        var (store, _) = NewTable();
        Assert.Contains("catalog-managed", Assert.Throws<DeltaProtocolException>(() => Snapshot.Load(store)).Message);
    }

    [Fact]
    public void Reads_FollowTheCatalog_UnpublishedCommitsCount_LaterPublishedFilesDoNot()
    {
        var (store, catalog) = NewTable();
        // Version 1 ratified but not published; a published file for version 1 that the catalog overrules; a published
        // version 2 the catalog does not know (yet): ignored.
        catalog.RatifyStaged(1, string.Join("\n", CommitInfoLine(1001), Lines.Add("a.parquet")));
        store.Commit(1, CommitInfoLine(1001), Lines.Add("stale.parquet"));
        store.Commit(2, CommitInfoLine(1002), Lines.Add("b.parquet"));

        Snapshot snapshot = Snapshot.Load(new CatalogManagedFileSystem(store, catalog));
        Assert.Equal(1, snapshot.Version);
        Assert.Equal(["a.parquet"], snapshot.Files.Select(f => f.Path));
    }

    [Fact]
    public void ACommit_IsStaged_Ratified_ThenPublished()
    {
        var (store, catalog) = NewTable();
        var fs = new CatalogManagedFileSystem(store, catalog);

        long version = Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(NewFile("a.parquet")).Commit();

        Assert.Equal(1, version);
        CatalogCommit ratified = Assert.Single(catalog.Offered);
        Assert.Equal(0, ratified.LatestPublishedVersion);
        Assert.StartsWith("00000000000000000001.", ratified.FileName);
        byte[] staged = store.ReadAll(CatalogManaged.StagedPath(ratified.FileName))!;
        Assert.Equal(staged, store.ReadAll(LogFile.CommitPath(1)));
        Assert.Equal(staged.Length, ratified.Size);
        CommitInfoAction info = ActionParser.ParseCommit(staged).OfType<CommitInfoAction>().Single();
        Assert.True(info.InCommitTimestamp > 1000);
        Assert.Equal(ratified.Timestamp, info.InCommitTimestamp);
        Assert.Contains("\"txnId\":", info.Json);
        Assert.Equal(["a.parquet"], Snapshot.Load(fs).Files.Select(f => f.Path));
    }

    [Fact]
    public void CommitsOthersLeftUnpublished_ArePublishedFirst_InOrder()
    {
        var (store, catalog) = NewTable();
        var fs = new CatalogManagedFileSystem(store, catalog);
        Snapshot read = Snapshot.Load(fs);
        // Another writer ratified versions 1 and 2 and stopped before publishing them.
        string first = catalog.RatifyStaged(1, string.Join("\n", CommitInfoLine(1001), Lines.Add("x.parquet")));
        string second = catalog.RatifyStaged(2, string.Join("\n", CommitInfoLine(1002), Lines.Add("y.parquet")));

        long version = Transaction.Start(fs, read, "WRITE").Add(NewFile("a.parquet")).Commit();

        Assert.Equal(3, version);
        Assert.Equal(Text(store.ReadAll(CatalogManaged.StagedPath(first))), Text(store.ReadAll(LogFile.CommitPath(1))));
        Assert.Equal(Text(store.ReadAll(CatalogManaged.StagedPath(second))), Text(store.ReadAll(LogFile.CommitPath(2))));
        Assert.Equal(2, catalog.Offered[^1].LatestPublishedVersion);
        Assert.Equal(["a.parquet", "x.parquet", "y.parquet"], Snapshot.Load(fs).Files.Select(f => f.Path).Order(StringComparer.Ordinal));
    }

    [Fact]
    public void AfterAConflict_TheCommitMovesPastABlindAppend_AndStopsAtARealConflict()
    {
        var (store, catalog) = NewTable();
        var fs = new CatalogManagedFileSystem(store, catalog);
        Snapshot read = Snapshot.Load(fs);
        var other = Transaction.Start(fs, read, "WRITE").Add(NewFile("b.parquet"));

        // Another writer's blind append is ratified while this commit is on its way: this append goes after it.
        catalog.BeforeRatify = () => catalog.RatifyStaged(1, string.Join("\n",
            """{"commitInfo":{"inCommitTimestamp":1001,"timestamp":1001,"operation":"WRITE","isBlindAppend":true}}""",
            Lines.Add("a.parquet")));
        Assert.Equal(2, other.Commit());

        // A delete of a file this transaction read is a conflict.
        Snapshot before = Snapshot.Load(fs);
        var reader = Transaction.Start(fs, before, "DELETE").ReadFiles(before.Files).Add(NewFile("c.parquet"));
        catalog.RatifyStaged(3, string.Join("\n", CommitInfoLine(1003), Lines.Remove("a.parquet")));
        Assert.Throws<DeltaCommitConflictException>(() => reader.Commit());
    }

    [Fact]
    public void APublishThatFails_AfterTheCatalogRatified_StillCommits()
    {
        var (store, catalog) = NewTable();
        var fs = new CatalogManagedFileSystem(new FailingPublishes(store), catalog);

        long version = Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(NewFile("a.parquet")).Commit();

        Assert.Equal(1, version);
        Assert.Null(store.ReadAll(LogFile.CommitPath(1)));
        Assert.Equal(["a.parquet"], Snapshot.Load(fs).Files.Select(f => f.Path));
    }

    [Fact]
    public void AnUnknownOutcome_KeepsTheDataFiles_ARefusalRollsThemBack()
    {
        foreach (var (failure, kept) in new (Exception, bool)[]
                 {
                     (new CatalogCommitOutcomeUnknownException("connection reset"), true),
                     (new CatalogCommitRejectedException("bad request"), false),
                 })
        {
            var (store, catalog) = NewTable();
            catalog.Failure = failure;
            var tracking = new TrackingFileSystem(new CatalogManagedFileSystem(store, catalog));
            tracking.Track("a.parquet");
            store.Put("a.parquet", "data");

            Assert.Throws(failure.GetType(), () => Transaction.Start(tracking, Snapshot.Load(tracking), "WRITE").Add(NewFile("a.parquet")).Commit());

            Assert.Equal(kept ? [] : ["a.parquet"], tracking.Rollback());
        }
    }

    [Fact]
    public void ARatifiedCommit_CountsAsCommitted_ForRollback()
    {
        var (store, catalog) = NewTable();
        var tracking = new TrackingFileSystem(new CatalogManagedFileSystem(new FailingPublishes(store), catalog));
        tracking.Track("a.parquet");
        store.Put("a.parquet", "data");

        Transaction.Start(tracking, Snapshot.Load(tracking), "WRITE").Add(NewFile("a.parquet")).Commit();

        Assert.Empty(tracking.Rollback());
    }

    [Fact]
    public void Maintenance_LeavesTheLogAlone_AndCheckpointsWaitForPublishing()
    {
        var (store, catalog) = NewTable();
        var fs = new CatalogManagedFileSystem(store, catalog);
        catalog.RatifyStaged(1, string.Join("\n", CommitInfoLine(1001), Lines.Add("a.parquet")));
        Snapshot snapshot = Snapshot.Load(fs);

        Assert.Empty(LogCleanup.Run(fs, snapshot, DateTimeOffset.UtcNow.AddYears(10)));
        Assert.Contains("not published", Assert.Throws<DeltaException>(() => Checkpoints.Write(fs, snapshot)).Message);
        Assert.Throws<DeltaProtocolException>(() => fs.Delete(LogFile.CommitPath(0)));
    }

    [Fact]
    public void WithoutInCommitTimestamps_ACommitIsRefused()
    {
        var (store, catalog) = NewTable(inCommitTimestamps: false);
        var fs = new CatalogManagedFileSystem(store, catalog);
        Assert.Contains("in-commit timestamps",
            Assert.Throws<DeltaProtocolException>(() => Transaction.Start(fs, Snapshot.Load(fs), "WRITE").Add(NewFile("a.parquet")).Commit()).Message);
    }

    [Fact]
    public void MetadataChanges_ReachTheCatalog_WithTheStateTheyBuildOn()
    {
        var (store, catalog) = NewTable();
        var fs = new CatalogManagedFileSystem(store, catalog);
        Snapshot read = Snapshot.Load(fs);
        var changed = new MetadataAction
        {
            Id = read.Metadata.Id, SchemaString = read.Metadata.SchemaString, PartitionColumns = read.Metadata.PartitionColumns,
            Configuration = new Dictionary<string, string>(read.Metadata.Configuration) { ["foo"] = "bar" },
        };

        Transaction.Start(fs, read, "SET TBLPROPERTIES").SetMetadata(changed).Commit();

        CatalogCommit offered = Assert.Single(catalog.Offered);
        Assert.Same(changed, offered.Metadata);
        Assert.False(offered.ReadMetadata.Configuration.ContainsKey("foo"));
        Assert.Null(offered.Protocol);
    }

    [Fact]
    public void CatalogManaged_CannotBeAddedToATable()
    {
        Assert.Null(TableFeatures.SupportedFeatureNamed("catalogManaged"));
    }

    /// <summary>A catalog that ratifies the next version only, as Unity Catalog does.</summary>
    private sealed class FakeCatalog(MemoryFileSystem store) : ICatalogCommitter
    {
        private readonly List<RatifiedCommit> _ratified = [];
        private long _latest;

        public List<CatalogCommit> Offered { get; } = [];
        public Exception? Failure { get; set; }
        /// <summary>Runs once, when the next commit reaches the catalog (another writer winning the race).</summary>
        public Action? BeforeRatify { get; set; }

        public CatalogLog GetLog() => new(_latest, [.. _ratified]);

        public CatalogCommitOutcome Ratify(CatalogCommit commit)
        {
            if (Failure is not null) throw Failure;
            if (BeforeRatify is { } race)
            {
                BeforeRatify = null;
                race();
            }
            if (commit.Version <= _latest) return CatalogCommitOutcome.Conflict;
            Assert.Equal(_latest + 1, commit.Version);
            Offered.Add(commit);
            _ratified.Add(new RatifiedCommit(commit.Version, commit.FileName, commit.Size, commit.ModificationTime));
            _latest = commit.Version;
            return CatalogCommitOutcome.Ratified;
        }

        /// <summary>Another writer's commit: staged and ratified, not published. Returns the staged file name.</summary>
        public string RatifyStaged(long version, string content)
        {
            Assert.Equal(_latest + 1, version);
            string name = CatalogManaged.NewStagedName(version);
            store.Put(CatalogManaged.StagedPath(name), content);
            _ratified.Add(new RatifiedCommit(version, name, Encoding.UTF8.GetByteCount(content), DateTimeOffset.UnixEpoch));
            _latest = version;
            return name;
        }
    }

    /// <summary>Storage on which publishing a commit file always fails.</summary>
    private sealed class FailingPublishes(MemoryFileSystem inner) : IWritableFileSystem
    {
        public bool TryCreate(string path, ReadOnlyMemory<byte> content) =>
            path.StartsWith("_delta_log/0", StringComparison.Ordinal) ? throw new IOException("publish failed") : inner.TryCreate(path, content);
        public void Overwrite(string path, ReadOnlyMemory<byte> content) => inner.Overwrite(path, content);
        public bool Delete(string path) => inner.Delete(path);
        public IReadOnlyList<FileEntry> ListAll(string directory) => inner.ListAll(directory);
        public IReadOnlyList<FileEntry> List(string directory, string? startFrom = null) => inner.List(directory, startFrom);
        public byte[]? ReadAll(string path) => inner.ReadAll(path);
        public byte[]? ReadRange(string path, long offset, int length) => inner.ReadRange(path, offset, length);
    }
}
