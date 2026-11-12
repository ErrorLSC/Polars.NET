using Apache.Arrow;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Avro;

namespace Seseragi.Files.Tests;

/// <summary>Parallel decoding must return exactly the sequential rows, in order.</summary>
public class AvroParallelTests
{
    private static async Task<(List<string> Rows, List<int> Lengths)> Read(string path, AvroReadOptions options)
    {
        await using var reader = await AvroReader.OpenAsync(path, options);
        List<string> rows = [];
        List<int> lengths = [];
        await foreach (var b in reader.ReadBatchesAsync())
        {
            using var _ = b;
            rows.AddRange(ArrowRender.Rows(b));
            lengths.Add(b.Length);
        }
        return (rows, lengths);
    }

    public static TheoryData<string, int, long?, string[]?> Cases => new()
    {
        { "many_null", 777, null, null },
        { "many_deflate", 100, null, null },
        { "many_snappy", 5000, 1234, null },
        { "many_zstandard", 333, null, ["s", "n"] },
        { "many_null", 1, 50, null },
        { "complex", 2, null, null },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Parallel_rows_match_sequential(string fixture, int batchSize, long? limit, string[]? columns)
    {
        string path = AvroReadTests.Fixture(fixture);
        var sequential = await Read(path, new() { BatchSize = batchSize, RowLimit = limit, Columns = columns });
        foreach (int dop in (int[])[2, 4, -1])
        {
            var parallel = await Read(path, new() { BatchSize = batchSize, RowLimit = limit, Columns = columns, MaxDegreeOfParallelism = dop });
            Assert.Equal(sequential.Rows, parallel.Rows);
            Assert.All(parallel.Lengths, n => Assert.InRange(n, 1, batchSize));
        }
    }

    [Fact]
    public void Sync_parallel_matches_sequential()
    {
        string path = AvroReadTests.Fixture("many_snappy");
        using var seq = AvroReader.Open(path, new() { BatchSize = 900 });
        using var par = AvroReader.Open(path, new() { BatchSize = 900, MaxDegreeOfParallelism = 3 });
        Assert.Equal(ArrowRender.Rows(seq.ReadBatches()), ArrowRender.Rows(par.ReadBatches()));
    }

    [Fact]
    public async Task Large_blocks_are_split_into_batches()
    {
        // One block of 5000 rows written by our writer, read in batches of 1000 with parallelism on.
        var input = AvroWriteTests.Wide(0, 5000);
        var ms = new MemoryStream();
        AvroWriter.Write(ms, input.Schema, [input], new() { BlockSize = int.MaxValue, Compression = SnappyCompression.Instance, LeaveOpen = true });
        ms.Position = 0;
        await using var reader = await AvroReader.OpenAsync(ms, new() { BatchSize = 1000, MaxDegreeOfParallelism = 4 });
        var batches = await reader.ReadBatchesAsync().ToListAsync();
        Assert.Equal([1000, 1000, 1000, 1000, 1000], batches.Select(b => b.Length));
        Assert.Equal(ArrowRender.Rows(input), ArrowRender.Rows(batches));
    }

    [Fact]
    public async Task Corrupt_block_error_reaches_the_consumer()
    {
        var input = AvroWriteTests.Wide(0, 3000);
        var ms = new MemoryStream();
        AvroWriter.Write(ms, input.Schema, [input], new() { BlockSize = 4096, LeaveOpen = true });
        var file = ms.ToArray();
        // Make the string "s1501✓" (in a middle block) invalid UTF-8: a worker thread hits the error.
        int at = file.AsSpan().IndexOf("s1501\u2713"u8);
        Assert.True(at > 0);
        file[at] = 0xFF;
        var e = await Assert.ThrowsAsync<AvroFormatException>(async () =>
        {
            await using var reader = await AvroReader.OpenAsync(new MemoryStream(file), new() { BatchSize = 100, MaxDegreeOfParallelism = 4 });
            await foreach (var b in reader.ReadBatchesAsync()) b.Dispose();
        });
        Assert.Contains("UTF-8", e.Message);
    }

    [Fact]
    public async Task Stopping_early_releases_in_flight_work()
    {
        for (int i = 0; i < 20; i++)
        {
            await using var reader = await AvroReader.OpenAsync(AvroReadTests.Fixture("many_zstandard"),
                new() { BatchSize = 50, MaxDegreeOfParallelism = 4 });
            await foreach (var b in reader.ReadBatchesAsync())
            {
                b.Dispose();
                break;
            }
        }
    }

    [Fact]
    public void Stopping_early_with_sync_dispose_releases_in_flight_work()
    {
        for (int i = 0; i < 20; i++)
        {
            using var reader = AvroReader.Open(AvroReadTests.Fixture("many_zstandard"), new() { BatchSize = 50, MaxDegreeOfParallelism = 4 });
            using var first = reader.ReadBatches().First();
            Assert.Equal(50, first.Length);
        }
    }

    [Fact]
    public async Task Async_dispose_waits_for_in_flight_chunks()
    {
        var reader = await AvroReader.OpenAsync(AvroReadTests.Fixture("many_deflate"), new() { BatchSize = 20, MaxDegreeOfParallelism = 8 });
        await using (var e = reader.ReadBatchesAsync().GetAsyncEnumerator())
        {
            Assert.True(await e.MoveNextAsync()); // several chunks are now decoding on the pool
            e.Current.Dispose();
        }
        await reader.DisposeAsync();
        await reader.DisposeAsync(); // idempotent
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-2)]
    public async Task Invalid_parallelism_is_refused(int dop) =>
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            AvroReader.OpenAsync(AvroReadTests.Fixture("primitives"), new() { MaxDegreeOfParallelism = dop }));
}
