using Kaminari.Seseragi.Files.IO;

namespace Seseragi.Files.Tests;

/// <summary>
/// <see cref="IWritableFileStore"/> over <see cref="LocalFileStore"/> and <see cref="MemoryFileStore"/>: a file appears
/// whole at commit or not at all, create-only commits lose to a file there, replacing commits win, deletes.
/// </summary>
public sealed class WritableFileStoreTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-wstore-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    public static TheoryData<string, bool> Stores => new()
    {
        { "local", false }, { "local", true }, { "memory", false }, { "memory", true },
    };

    private IWritableFileStore Store(string kind) => kind == "local" ? new LocalFileStore(_root) : new MemoryFileStore();

    private static byte[] Bytes(int count, int seed) => [.. Enumerable.Range(0, count).Select(i => (byte)(i * 7 + seed))];

    private static async Task<byte[]?> Read(IWritableFileStore store, string path)
    {
        if (await store.OpenAsync(path) is not { } source) return null;
        using (source as IDisposable)
        {
            var bytes = new byte[source.Length];
            Assert.Equal(bytes.Length, await source.ReadAtAsync(0, bytes, default));
            return bytes;
        }
    }

    private static async Task<List<string>> List(IWritableFileStore store)
    {
        List<string> paths = [];
        await foreach (StoredFile file in store.ListAsync("", recursive: true)) paths.Add(file.Path.Replace('\\', '/'));
        return [.. paths.Order(StringComparer.Ordinal)];
    }

    /// <summary>Writes in pieces (small ones gathered, large ones straight through), sync or async, and commits.</summary>
    private static async Task<bool> Write(IWritableFileStore store, string path, byte[] content, bool createOnly, bool async)
    {
        await using FileWriteStream stream = store.OpenWrite(path, createOnly);
        int at = 0;
        foreach (int piece in (int[])[1, 100, 40_000, 3])
        {
            int n = Math.Min(piece, content.Length - at);
            if (async) await stream.WriteAsync(content.AsMemory(at, n));
            else stream.Write(content.AsSpan(at, n));
            at += n;
        }
        if (async) await stream.WriteAsync(content.AsMemory(at));
        else stream.Write(content.AsSpan(at));
        Assert.Equal(content.Length, stream.Position);
        return async ? await stream.CommitAsync() : stream.Commit();
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task AFile_AppearsWholeAtCommit_InDirectoriesMadeForIt(string kind, bool async)
    {
        IWritableFileStore store = Store(kind);
        byte[] content = Bytes(100_000, 1);
        await using (FileWriteStream stream = store.OpenWrite("a/b/c.bin", createOnly: true))
        {
            stream.Write(content.AsSpan(0, 10));
            Assert.Null(await Read(store, "a/b/c.bin")); // nothing before the commit
            stream.Write(content.AsSpan(10));
            Assert.True(async ? await stream.CommitAsync() : stream.Commit());
            Assert.Throws<InvalidOperationException>(() => stream.Write([1]));
            Assert.False(stream.CanWrite);
        }
        Assert.Equal(content, await Read(store, "a/b/c.bin"));
        Assert.Equal(["a/b/c.bin"], await List(store)); // no temporary file left
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task AStreamDisposedUncommitted_LeavesNothing(string kind, bool async)
    {
        IWritableFileStore store = Store(kind);
        Assert.True(await Write(store, "keep.bin", Bytes(10, 2), createOnly: true, async));
        if (async)
        {
            await using FileWriteStream dropped = store.OpenWrite("d/dropped.bin", createOnly: false);
            await dropped.WriteAsync(Bytes(50_000, 3));
        }
        else
        {
            using FileWriteStream dropped = store.OpenWrite("d/dropped.bin", createOnly: false);
            dropped.Write(Bytes(50_000, 3));
        }
        Assert.Equal(["keep.bin"], await List(store));
        if (kind == "local") Assert.Empty(Directory.GetFiles(Path.Combine(_root, "d"), "*", SearchOption.AllDirectories)); // hidden ones too
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task CreateOnly_LosesToAFileThere_ReplacingWins(string kind, bool async)
    {
        IWritableFileStore store = Store(kind);
        byte[] first = Bytes(3000, 4), second = Bytes(70_000, 5), third = Bytes(5, 6);
        Assert.True(await Write(store, "f.bin", first, createOnly: true, async));
        Assert.False(await Write(store, "f.bin", second, createOnly: true, async));
        Assert.Equal(first, await Read(store, "f.bin"));
        Assert.True(await Write(store, "f.bin", third, createOnly: false, async));
        Assert.Equal(third, await Read(store, "f.bin"));
        Assert.Equal(["f.bin"], await List(store));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task OfTwoCreateOnlyStreams_ExactlyOneCreatesTheFile(string kind, bool async)
    {
        IWritableFileStore store = Store(kind);
        await using FileWriteStream one = store.OpenWrite("g.bin", createOnly: true), two = store.OpenWrite("g.bin", createOnly: true);
        one.Write(Bytes(10, 7));
        two.Write(Bytes(20, 8));
        bool[] won = async ? await Task.WhenAll(one.CommitAsync().AsTask(), two.CommitAsync().AsTask()) : [one.Commit(), two.Commit()];
        Assert.Equal(1, won.Count(w => w));
        Assert.Equal(won[0] ? Bytes(10, 7) : Bytes(20, 8), await Read(store, "g.bin"));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task Delete_RemovesAFile_FalseWhenNone(string kind, bool async)
    {
        IWritableFileStore store = Store(kind);
        Assert.True(await Write(store, "x/y.bin", Bytes(4, 9), createOnly: true, async));
        Assert.True(await store.DeleteAsync("x/y.bin"));
        Assert.False(await store.DeleteAsync("x/y.bin"));
        Assert.Null(await Read(store, "x/y.bin"));
        Assert.Empty(await List(store));
    }

    [Fact]
    public async Task AMemoryFile_ReadsOn_WhenReplacedOrRemoved()
    {
        var store = new MemoryFileStore();
        Assert.True(await Write(store, "f", Bytes(1000, 1), createOnly: true, async: false));
        IRandomAccess source = (await store.OpenAsync("f"))!;
        Assert.True(await Write(store, "f", Bytes(10, 2), createOnly: false, async: false));
        Assert.True(await store.DeleteAsync("f"));
        var bytes = new byte[1000];
        Assert.Equal(1000, await source.ReadAtAsync(0, bytes, default));
        Assert.Equal(Bytes(1000, 1), bytes);
    }
}
