using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Lakehouse.Parquet;

namespace Seseragi.Lakehouse.Tests;

public class ParquetFootersTests
{
    /// <summary>Counts the calls a reader makes; reads tails only when told it may.</summary>
    private sealed class Counting(IWritableFileSystem inner, bool tails) : IFileSystem, ITailReadable
    {
        public int Lists, Tails, Ranges;

        public bool CanReadTails => tails;

        public FileTail? ReadTail(string path, int length)
        {
            Interlocked.Increment(ref Tails);
            return ((ITailReadable)inner).ReadTail(path, length);
        }

        public IReadOnlyList<FileEntry> List(string directory, string? startFrom = null)
        {
            Interlocked.Increment(ref Lists);
            return inner.List(directory, startFrom);
        }

        public byte[]? ReadAll(string path) => inner.ReadAll(path);

        public byte[]? ReadRange(string path, long offset, int length)
        {
            Interlocked.Increment(ref Ranges);
            return inner.ReadRange(path, offset, length);
        }
    }

    private static readonly string Table = Path.Combine(AppContext.BaseDirectory, "TestData", "app-txn-checkpoint");

    private static string[] DataFiles() =>
        [.. Directory.EnumerateFiles(Table, "*.parquet", SearchOption.AllDirectories)
            .Where(f => !f.Contains("_delta_log", StringComparison.Ordinal))
            .Select(f => Path.GetRelativePath(Table, f).Replace('\\', '/'))
            .Order(StringComparer.Ordinal)];

    [Fact]
    public void AFileSystemThatReadsTails_IsNeverListed_AndReadOncePerFile()
    {
        string[] files = DataFiles();
        Assert.True(files.Length >= 2 && files.Select(f => f.LastIndexOf('/') >= 0 ? f[..f.LastIndexOf('/')] : "").Distinct().Count() >= 2);
        var local = new LocalFileSystem(Table);
        var withTails = new Counting(local, tails: true);
        var listed = new Counting(local, tails: false);

        var fromTails = ParquetFooters.Read(withTails, files);
        var fromListing = ParquetFooters.Read(listed, files);

        Assert.Equal((0, files.Length, 0), (withTails.Lists, withTails.Tails, withTails.Ranges));
        Assert.Equal(files.Select(f => f.LastIndexOf('/') >= 0 ? f[..f.LastIndexOf('/')] : "").Distinct().Count(), listed.Lists);
        Assert.Equal(0, listed.Tails);
        for (int i = 0; i < files.Length; i++)
        {
            Assert.Equal(new FileInfo(Path.Combine(Table, files[i])).Length, fromTails[i].Size);
            Assert.Equal(fromListing[i].Size, fromTails[i].Size);
            Assert.Equal(fromListing[i].Footer.RowCount, fromTails[i].Footer.RowCount);
        }
    }

    [Fact]
    public void AMissingFile_IsReported_EitherWay()
    {
        var local = new LocalFileSystem(Table);
        string[] files = [.. DataFiles().Take(1), "nowhere/part-missing.parquet"];
        Assert.Contains("was not found after it was written", Assert.Throws<FileNotFoundException>(() => ParquetFooters.Read(new Counting(local, true), files)).Message);
        Assert.Contains("was not found after it was written", Assert.Throws<FileNotFoundException>(() => ParquetFooters.Read(new Counting(local, false), files)).Message);
    }

    [Fact]
    public void AFooterLongerThanTheTail_TakesOneMoreRead()
    {
        var local = new LocalFileSystem(Table);
        string file = DataFiles()[0];
        long size = new FileInfo(Path.Combine(Table, file)).Length;
        FileTail full = local.ReadTail(file, ParquetMetadata.TailGuess)!.Value;
        var counting = new Counting(local, tails: true);
        ParquetMetadata footer = ParquetFooters.FooterFromTail(counting, file, new FileTail(full.Bytes[^16..], size));
        Assert.Equal(ParquetFooters.FooterFromTail(local, file, full).RowCount, footer.RowCount);
        Assert.Equal(1, counting.Ranges);
    }

    [Fact]
    public void LocalTails_AreTheEndAndTheSize()
    {
        string root = Path.Combine(Path.GetTempPath(), $"tails_{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            var fs = new LocalFileSystem(root);
            fs.Overwrite("a.bin", new byte[] { 1, 2, 3, 4, 5 });
            FileTail tail = fs.ReadTail("a.bin", 2)!.Value;
            Assert.Equal([4, 5], tail.Bytes);
            Assert.Equal(5, tail.Size);
            Assert.Equal([1, 2, 3, 4, 5], fs.ReadTail("a.bin", 99)!.Value.Bytes);
            Assert.Null(fs.ReadTail("none.bin", 2));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void SuffixAnswers_GiveTheSize_FromTheContentRange_OrTheWholeBody()
    {
        byte[] body = new byte[100];
        Assert.Equal(1000, Kaminari.Seseragi.Lakehouse.Storage.ObjectStoreClient.SuffixAnswer(System.Net.HttpStatusCode.PartialContent, "bytes 900-999/1000", body, 100)!.Value.Size);
        var whole = Kaminari.Seseragi.Lakehouse.Storage.ObjectStoreClient.SuffixAnswer(System.Net.HttpStatusCode.OK, null, new byte[250], 100)!.Value;
        Assert.Equal((250L, 100), (whole.Size, whole.Tail.Length));
        Assert.Equal(0, Kaminari.Seseragi.Lakehouse.Storage.ObjectStoreClient.SuffixAnswer(System.Net.HttpStatusCode.PartialContent, "bytes */0", [], 100)!.Value.Size);
        Assert.Null(Kaminari.Seseragi.Lakehouse.Storage.ObjectStoreClient.SuffixAnswer(System.Net.HttpStatusCode.PartialContent, "nonsense", body, 100));
        Assert.Null(Kaminari.Seseragi.Lakehouse.Storage.ObjectStoreClient.SuffixAnswer(System.Net.HttpStatusCode.PartialContent, null, body, 100));
    }
}
