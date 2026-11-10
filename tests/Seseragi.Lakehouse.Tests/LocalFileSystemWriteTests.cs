using Kaminari.Seseragi.Lakehouse.Engine;

namespace Seseragi.Lakehouse.Tests;

public class LocalFileSystemWriteTests
{
    [Fact]
    public void Listing_ToleratesFilesThatVanish_WhileOthersWrite()
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_fs_{Guid.NewGuid():N}");
        try
        {
            var fs = new LocalFileSystem(root);
            fs.TryCreate("log/seed.json", "x"u8.ToArray());
            // Writers create files through temporary files that appear and vanish in the listed directory. Each
            // writes a bounded number, so the directory stays small whatever happens to the listing loop.
            const int perWriter = 250;
            Task[] writers = [.. Enumerable.Range(0, 4).Select(w => Task.Run(() =>
            {
                for (int i = 0; i < perWriter; i++)
                    fs.TryCreate($"log/{w}-{i}.json", "y"u8.ToArray());
            }))];
            while (!writers.All(t => t.IsCompleted))
                Assert.Contains(fs.List("log"), f => f.Path == "log/seed.json");
            Task.WaitAll(writers);
            Assert.Equal(1 + 4 * perWriter, fs.List("log").Count);
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }

    [Fact]
    public void TryCreate_IsPutIfAbsent_AndOverwriteReplaces()
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_fs_{Guid.NewGuid():N}");
        try
        {
            var fs = new LocalFileSystem(root);
            Assert.True(fs.TryCreate("a/b.json", "first"u8.ToArray()));
            Assert.False(fs.TryCreate("a/b.json", "second"u8.ToArray()));
            Assert.Equal("first"u8.ToArray(), fs.ReadAll("a/b.json"));
            fs.Overwrite("a/b.json", "third"u8.ToArray());
            Assert.Equal("third"u8.ToArray(), fs.ReadAll("a/b.json"));
            Assert.Single(Directory.GetFiles(Path.Combine(root, "a")));
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }
}
