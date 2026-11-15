using Kaminari.Seseragi.Files.IO;

namespace Seseragi.Files.Tests;

/// <summary><see cref="LocalFileStore"/> and <see cref="MemoryFileStore"/>: listing (direct or recursive, hidden files included), stats, opening.</summary>
public sealed class FileStoreTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-store-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    private void Put(string relative, int bytes)
    {
        string path = Path.Combine(_root, relative);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllBytes(path, [.. Enumerable.Range(0, bytes).Select(i => (byte)i)]);
    }

    private static async Task<List<StoredFile>> List(IFileStore store, string prefix, bool recursive)
    {
        List<StoredFile> files = [];
        await foreach (StoredFile file in store.ListAsync(prefix, recursive)) files.Add(file);
        return [.. files.OrderBy(f => f.Path, StringComparer.Ordinal)];
    }

    [Fact]
    public async Task ALocalStore_ListsWithTheSpellingGiven_HiddenFilesIncluded()
    {
        Put("d/a.bin", 3);
        Put("d/.hidden", 1);
        Put("d/_x", 2);
        Put("d/sub/b.bin", 5);
        Directory.CreateDirectory(Path.Combine(_root, "d/emptydir"));
        var store = new LocalFileStore(_root);
        string sep = Path.DirectorySeparatorChar.ToString();
        Assert.Equal([$"d{sep}.hidden", $"d{sep}_x", $"d{sep}a.bin"], (await List(store, "d", recursive: false)).Select(f => f.Path));
        List<StoredFile> all = await List(store, "d/", recursive: true);
        Assert.Equal(["d/.hidden", "d/_x", "d/a.bin", $"d/sub{sep}b.bin"], all.Select(f => f.Path));
        Assert.Equal([1L, 2, 3, 5], all.Select(f => f.Size));
        Assert.All(all, f => Assert.NotNull(f.LastModified));
        Assert.Empty(await List(store, "missing", recursive: true));
        Assert.True(store.IsDirectory("d"));
        Assert.False(store.IsDirectory("d/a.bin"));
    }

    [Fact]
    public async Task ALocalStore_StatsAndOpens()
    {
        Put("f.bin", 10);
        var store = new LocalFileStore(_root);
        Assert.Equal(10, (await store.StatAsync("f.bin"))!.Value.Size);
        Assert.Null(await store.StatAsync("nope.bin"));
        Assert.Null(await store.StatAsync("."));
        IRandomAccess source = (await store.OpenAsync("f.bin"))!;
        using (source as IDisposable)
        {
            Assert.Equal(10, source.Length);
            var bytes = new byte[4];
            Assert.Equal(4, source.ReadAt(6, bytes));
            Assert.Equal([6, 7, 8, 9], bytes);
        }
        Assert.Null(await store.OpenAsync("nope.bin"));
        Assert.Null(await store.OpenAsync("nodir/nope.bin"));
        await Assert.ThrowsAsync<FileNotFoundException>(async () => await store.OpenAsync(new StoredFile("nodir/nope.bin", 3)));
        await Assert.ThrowsAsync<FileNotFoundException>(async () => await store.OpenAsync(new StoredFile("nope.bin", 3)));
        // An absolute path ignores the root.
        Assert.NotNull(await new LocalFileStore("/elsewhere").StatAsync(Path.Combine(_root, "f.bin")));
    }

    [Fact]
    public async Task AMemoryStore_ListsByDirectory()
    {
        var store = new MemoryFileStore();
        store.Add("a/x.bin", new byte[3]);
        store.Add("a/b/y.bin", new byte[4]);
        store.Add("ab/z.bin", new byte[5]);
        store.Add("top.bin", new byte[1]);
        Assert.Equal(["a/x.bin"], (await List(store, "a", recursive: false)).Select(f => f.Path));
        Assert.Equal(["a/b/y.bin", "a/x.bin"], (await List(store, "a/", recursive: true)).Select(f => f.Path));
        Assert.Equal(["top.bin"], (await List(store, "", recursive: false)).Select(f => f.Path));
        Assert.Equal(4, (await List(store, "", recursive: true)).Count);
        Assert.True(store.IsDirectory("a"));
        Assert.False(store.IsDirectory("a/x.bin"));
        Assert.Throws<ArgumentException>(() => store.Add("a/x.bin", new byte[1]));
        Assert.Throws<ArgumentException>(() => store.Add("dir/", new byte[1]));
        Assert.True(store.Remove("a/x.bin"));
        Assert.Null(await store.StatAsync("a/x.bin"));
        Assert.Null(await store.OpenAsync("a/x.bin"));
        Assert.Equal(4, (await store.OpenAsync(new StoredFile("a/b/y.bin", 4))).Length);
    }
}
