#nullable enable
using System.Buffers.Binary;
using Polars.NET.Core.IO;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// Row counts of the managed Parquet scan (<see cref="ManagedParquet.ScanParquet"/>), against the engine's own scan of
/// the same Hive-partitioned files: a count, after a head, a partition filter or a row filter, with a row index. A
/// count without a row filter reads the footers only: it still answers once every data page is zeroed.
/// </summary>
public class ManagedParquetCountTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("managed-pq-count-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    /// <summary>Three partitions: 30 000 rows (8 row groups), 700, 20 000.</summary>
    private string Write(string name)
    {
        string root = Path.Combine(_dir, name);
        foreach (var (k, first, rows) in ((int, int, int)[])[(0, 0, 30_000), (1, 30_000, 700), (2, 30_700, 20_000)])
        {
            string dir = Path.Combine(root, $"k={k}");
            Directory.CreateDirectory(dir);
            using DataFrame df = DataFrame.From(Enumerable.Range(first, rows).Select(i => new Row(i, $"name {i} {new string('x', i % 23)}")));
            df.WriteParquet(Path.Combine(dir, "part.parquet"), rowGroupSize: 4000);
        }
        return root;
    }

    /// <summary>The queries, by name, over a scan.</summary>
    private static readonly (string Name, Func<LazyFrame, LazyFrame> Query, bool ReadsData)[] Queries =
    [
        ("count", lf => lf.Select(Pl.Len()), false),
        ("head 1600, count", lf => lf.Head(1600).Select(Pl.Len()), false),
        ("head 0, count", lf => lf.Head(0).Select(Pl.Len()), false),
        ("head past the end, count", lf => lf.Head(1_000_000).Select(Pl.Len()), false),
        ("partition k = 1, count", lf => lf.Filter(Pl.Col("k") == 1).Select(Pl.Len()), false),
        ("partition k != 1, count", lf => lf.Filter(Pl.Col("k") != 1).Select(Pl.Len()), false),
        ("row index, count", lf => lf.WithRowIndex("n").Select(Pl.Len()), false),
        ("tail 3", lf => lf.Tail(3).Select("id"), true),
        ("head 1600, tail 3", lf => lf.Head(1600).Tail(3).Select("id"), true),
        ("partition k = 2, tail 2", lf => lf.Filter(Pl.Col("k") == 2).Tail(2).Select("id"), true),
        ("id >= 1600, count", lf => lf.Filter(Pl.Col("id") >= 1600).Select(Pl.Len()), true),
        ("k = 2 and id < 30 800, count", lf => lf.Filter((Pl.Col("k") == 2) & (Pl.Col("id") < 30_800)).Select(Pl.Len()), true),
    ];

    private static DataFrame Run(LazyFrame scan, Func<LazyFrame, LazyFrame> query)
    {
        using LazyFrame q = query(scan);
        return q.Collect();
    }

    [Fact]
    public void Counts_EqualTheEngineScans()
    {
        string root = Write("plain");
        using LazyFrame native = LazyFrame.ScanParquet(root, hivePartitioning: true);
        using LazyFrame managed = new(ManagedParquet.ScanParquet(root));
        foreach (var (name, query, _) in Queries)
        {
            using DataFrame expected = Run(native, query);
            using DataFrame actual = Run(managed, query);
            Assert.True(expected.Equals(actual), $"{name}: engine\n{expected}\nmanaged\n{actual}");
        }
        // The scan's own row index and file path column take no read either.
        using LazyFrame indexed = new(ManagedParquet.ScanParquet(root, rowIndexName: "n", includePathColumn: "path"));
        using DataFrame count = Run(indexed, static lf => lf.Select(Pl.Len()));
        Assert.Equal(50_700, Convert.ToInt64(count[0, 0]));
    }

    [Fact]
    public void ACountWithoutARowFilter_ReadsTheFootersOnly()
    {
        string root = Write("zeroed");
        long[] expected;
        using (LazyFrame native = LazyFrame.ScanParquet(root, hivePartitioning: true)) expected = [.. Queries.Select(q => Count(native, q.Query))];
        // Every byte between the leading magic and the footer zeroed: the data pages cannot be read any more.
        foreach (string file in Directory.EnumerateFiles(root, "*.parquet", SearchOption.AllDirectories))
        {
            byte[] bytes = File.ReadAllBytes(file);
            int footer = bytes.Length - 8 - BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8));
            bytes.AsSpan(4, footer - 4).Clear();
            File.WriteAllBytes(file, bytes);
        }
        using LazyFrame managed = new(ManagedParquet.ScanParquet(root));
        for (int q = 0; q < Queries.Length; q++)
        {
            var (name, query, readsData) = Queries[q];
            if (readsData) Assert.ThrowsAny<Exception>(() => Run(managed, query).Dispose());
            else Assert.True(expected[q] == Count(managed, query), name);
        }

        static long Count(LazyFrame scan, Func<LazyFrame, LazyFrame> query)
        {
            using DataFrame df = Run(scan, query);
            return df.Height == 1 && df.Width == 1 ? Convert.ToInt64(df[0, 0]) : -1;
        }
    }

    private sealed record Row(long id, string name);
}
