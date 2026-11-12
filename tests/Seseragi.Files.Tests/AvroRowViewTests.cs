using Apache.Arrow;
using Kaminari.Sazanami.Arrow;
using Kaminari.Seseragi.Files.Avro;

namespace Seseragi.Files.Tests;

/// <summary>The Sazanami row view over batches from the Avro reader, whose buffers are native memory imported
/// through the Arrow C data interface (not managed arrays as in Sazanami's own tests).</summary>
public class AvroRowViewTests
{
    [Fact]
    public async Task RowView_reads_the_complex_fixture()
    {
        var (_, batches) = await AvroReadTests.ReadAll("complex");
        ArrowRows rows = ArrowRows.Over(Assert.Single(batches));
        Assert.Equal(3, rows.Count);

        Assert.Equal([1, 2, 3], rows[0].GetList<int>("arr").ToArray());
        Assert.Equal(0, rows[1].GetList<int>("arr").Count);
        Assert.Equal(["a", null], (IEnumerable<string?>)rows[0].GetList<string?>("arrn").ToArray());

        ArrowMap<string, long> m = rows[0].GetMap<string, long>("m");
        Assert.Equal(2L, m["j"]);
        Assert.Equal(-5L, rows[2].GetMap<string, long>("m")["z"]);

        Assert.Equal(4, rows[2].GetStruct("rec").Get<int>("x"));
        Assert.Null(rows[1].GetStruct("rec").Get<string>("y"));
        Assert.False(rows[0].TryGetStruct("recn", out _));
        Assert.Equal("q", rows[1].GetStruct("recn").Get<string>("y"));

        Assert.Equal(["BLUE", "RED", "GREEN"], [rows[0].Get<string>("e"), rows[1].Get<string>("e"), rows[2].Get<string>("e")]);
        Assert.Equal("xyz"u8.ToArray(), rows[1].GetBytes("fx").ToArray());
        Assert.Equal(1L << 40, rows[1].Get<long>("il"));
        Assert.Null(rows[1].Get<double?>("fd"));

        // General union: a struct of member0..n, a null union is a null struct.
        Assert.Equal("s", rows[0].GetStruct("u").Get<string>("member0"));
        Assert.False(rows[1].TryGetStruct("u", out _));
        Assert.Equal(9, rows[2].GetStruct("u").Get<int?>("member1"));
        Assert.Null(rows[0].GetValue("nul"));

        // No allocation per row over native-memory buffers either (after the first pass builds the layouts).
        int arr = rows.Ordinal("arr"), map = rows.Ordinal("m"), rec = rows.Ordinal("rec"), e = rows.Ordinal("e");
        long Pass()
        {
            long sum = 0;
            foreach (ArrowRow row in rows)
            {
                foreach (int v in row.GetList<int>(arr)) sum += v;
                if (row.GetMap<string, long>(map).TryGetValue("k", out long k)) sum += k;
                sum += row.GetStruct(rec).Get<int>("x") + row.GetBytes(e).Length;
            }
            return sum;
        }
        long expected = Pass(); // builds the nested layouts
        long before = GC.GetAllocatedBytesForCurrentThread();
        long again = Pass();
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(expected, again);
        Assert.Equal(0, allocated);
    }

    [Fact]
    public async Task RowView_reads_parallel_batches_like_sequential_ones()
    {
        static async Task<List<string>> Rows(int dop)
        {
            await using var reader = await AvroReader.OpenAsync(AvroReadTests.Fixture("many_deflate"),
                new() { BatchSize = 300, MaxDegreeOfParallelism = dop });
            List<string> result = [];
            await foreach (RecordBatch batch in reader.ReadBatchesAsync())
            {
                ArrowRows rows = ArrowRows.Over(batch);
                foreach (ArrowRow row in rows)
                {
                    result.Add(string.Join('|', Enumerable.Range(0, rows.FieldCount).Select(i => row.GetValue(i) switch
                    {
                        byte[] bytes => Convert.ToHexString(bytes),
                        var v => v?.ToString() ?? "null",
                    })));
                }
            }
            return result;
        }

        List<string> sequential = await Rows(1);
        Assert.NotEmpty(sequential);
        Assert.Equal(sequential, await Rows(4));
    }
}
