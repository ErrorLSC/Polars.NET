#nullable enable
using Polars.CSharp.Linq;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// LINQ queries over a frame ScanDelta returned leave out the data files whose statistics rule every row out. The
/// table has three files (ids 1–3 named a…, 11–13 named b…, 21–23 named c…); the middle one is then damaged, so a
/// query reads it only when it does not leave it out.
/// </summary>
public class DeltaFileSkippingTests
{
    public record Row(long Id, string Name);

    private static string NewTable()
    {
        string path = Path.Combine(Path.GetTempPath(), $"delta_skipping_{Guid.NewGuid():N}");
        string? middle = null;
        foreach (var (first, prefix) in new[] { (1L, "a"), (11L, "b"), (21L, "c") })
        {
            string[] before = DataFiles(path);
            using var df = DataFrame.From<Row>([new(first, prefix + "1"), new(first + 1, prefix + "2"), new(first + 2, prefix + "3")]);
            df.WriteDelta(path, mode: DeltaSaveMode.Append, mkdir: true);
            if (prefix == "b") middle = Assert.Single(DataFiles(path).Except(before));
        }
        File.WriteAllBytes(middle!, "not a parquet file"u8.ToArray());
        return path;
    }

    private static string[] DataFiles(string path) =>
        Directory.Exists(path) ? Directory.GetFiles(path, "*.parquet", SearchOption.AllDirectories) : [];

    private static void InTable(Action<string> body)
    {
        string path = NewTable();
        try { body(path); }
        finally { Directory.Delete(path, true); }
    }

    [Fact]
    public void Where_LeavesOutTheFilesItRulesOut()
    {
        InTable(path =>
        {
            using var lf = LazyFrame.ScanDelta(path);
            var rows = lf.AsQueryable<Row>();
            Assert.Equal([21L, 22L, 23L], rows.Where(r => r.Id > 20).Select(r => r.Id).ToList().Order());
            Assert.Equal([1L, 2L, 3L], rows.Where(r => r.Id < 5 || r.Id > 100).Select(r => r.Id).ToList().Order());
            Assert.Equal([22L, 23L], rows.Where(r => r.Id * 2 > 42).Select(r => r.Id).ToList().Order());
            Assert.Equal(["c1", "c2", "c3"], rows.Where(r => r.Name.StartsWith("c")).Select(r => r.Name).ToList().Order());
            long[] wanted = [2, 22];
            Assert.Equal([2L, 22L], rows.Where(r => wanted.Contains(r.Id)).Select(r => r.Id).ToList().Order());
            // Joined with another scan of the table, each side leaves out what its filter rules out.
            using var other = LazyFrame.ScanDelta(path);
            var joined = from a in rows.Where(r => r.Id > 20)
                         join b in other.AsQueryable<Row>().Where(r => r.Id < 5) on a.Id - 20 equals b.Id
                         select a.Id + b.Id;
            Assert.Equal([22L, 24L, 26L], joined.ToList().Order());
            // The damaged file is read when nothing rules it out.
            Assert.ThrowsAny<Exception>(() => rows.Where(r => r.Id == 12).ToList());
        });
    }

    [Fact]
    public void ConstantsAndCapturedValues_AreTheirOwn_ForEachQuery()
    {
        // The compiler caches a plan per query shape: each run must skip by its own values.
        InTable(path =>
        {
            using var lf = LazyFrame.ScanDelta(path);
            var rows = lf.AsQueryable<Row>();
            Assert.Equal([21L, 22L, 23L], rows.Where(r => r.Id > 20).Select(r => r.Id).ToList().Order());
            Assert.ThrowsAny<Exception>(() => rows.Where(r => r.Id > 0).Select(r => r.Id).ToList());
            long threshold = 20;
            Assert.Equal([21L, 22L, 23L], rows.Where(r => r.Id > threshold).Select(r => r.Id).ToList().Order());
            threshold = 0;
            Assert.ThrowsAny<Exception>(() => rows.Where(r => r.Id > threshold).Select(r => r.Id).ToList());
            threshold = 20;
            Assert.Equal([21L, 22L, 23L], rows.Where(r => r.Id > threshold).Select(r => r.Id).ToList().Order());
        });
    }

    public record RowWithFile(long Id, string Name, string File);

    [Fact]
    public void AFilterThatRulesOutEveryFile_ReadsNoRows()
    {
        InTable(path =>
        {
            using var lf = LazyFrame.ScanDelta(path);
            var rows = lf.AsQueryable<Row>();
            Assert.Empty(rows.Where(r => r.Id > 1000).ToList());
            Assert.Equal(0, rows.Count(r => r.Id > 1000));
            Assert.Equal(0L, rows.Where(r => r.Id > 1000).Sum(r => r.Id));
            using var withFiles = LazyFrame.ScanDelta(path, includePathColumn: "File");
            Assert.Empty(withFiles.AsQueryable<RowWithFile>().Where(r => r.Id > 1000).ToList());
            Assert.All(withFiles.AsQueryable<RowWithFile>().Where(r => r.Id > 20).ToList(), r => Assert.EndsWith(".parquet", r.File));
        });
    }

    [Fact]
    public void RowLimits_And_FrameOperations_KeepEveryFile()
    {
        InTable(path =>
        {
            // Leaving files out would change which rows a row limit keeps: such a scan reads every file.
            using var limited = LazyFrame.ScanDelta(path, nRows: 100);
            Assert.ThrowsAny<Exception>(() => limited.AsQueryable<Row>().Where(r => r.Id > 20).ToList());
            // A frame changed since the scan is no longer the table.
            using var scanned = LazyFrame.ScanDelta(path);
            using var changed = scanned.WithColumns(Col("Id").Alias("Id"));
            Assert.ThrowsAny<Exception>(() => changed.AsQueryable<Row>().Where(r => r.Id > 20).ToList());
        });
    }

    [Fact]
    public void WhenTheTableCannotBeScannedAgain_TheQueryReadsTheFrameWhole()
    {
        string path = Path.Combine(Path.GetTempPath(), $"delta_skipping_fallback_{Guid.NewGuid():N}");
        try
        {
            foreach (long first in (long[])[1, 11, 21])
            {
                using var df = DataFrame.From<Row>([new(first, "x"), new(first + 1, "y")]);
                df.WriteDelta(path, mode: DeltaSaveMode.Append, mkdir: true);
            }
            using var lf = LazyFrame.ScanDelta(path);
            // The log is gone: a new scan fails, the frame (its files already listed) still reads.
            Directory.Delete(Path.Combine(path, "_delta_log"), true);
            Assert.Equal([21L, 22L], lf.AsQueryable<Row>().Where(r => r.Id > 20).Select(r => r.Id).ToList().Order());
        }
        finally { Directory.Delete(path, true); }
    }

    [Fact]
    public void TheQuery_ReadsTheVersionTheScanRead()
    {
        InTable(path =>
        {
            using var lf = LazyFrame.ScanDelta(path);
            using (var more = DataFrame.From<Row>([new(31, "d1")]))
                more.WriteDelta(path, mode: DeltaSaveMode.Append);
            Assert.Equal([21L, 22L, 23L], lf.AsQueryable<Row>().Where(r => r.Id > 20).Select(r => r.Id).ToList().Order());
        });
    }

    public record Reading(double Value);

    public record WholeReading(long Value);

    [Fact]
    public void ASchemaThatChangesTypes_KeepsEveryFile()
    {
        // The filter reads the cast values (2.7 and 2.9 become 2), the statistics the stored ones (2.7..2.9).
        string path = Path.Combine(Path.GetTempPath(), $"delta_skipping_cast_{Guid.NewGuid():N}");
        try
        {
            using (var first = DataFrame.From<Reading>([new(2.7), new(2.9)])) first.WriteDelta(path, mode: DeltaSaveMode.Append, mkdir: true);
            using (var second = DataFrame.From<Reading>([new(10.5)])) second.WriteDelta(path, mode: DeltaSaveMode.Append);
            using var schema = PolarsSchema.From<WholeReading>();
            using var lf = LazyFrame.ScanDelta(path, schema: schema);
            Assert.Equal(2, lf.AsQueryable<WholeReading>().Where(r => r.Value == 2).ToList().Count);
        }
        finally { Directory.Delete(path, true); }
    }

    [Fact]
    public void AGivenSchema_IsKeptForTheQuery()
    {
        InTable(path =>
        {
            LazyFrame lf;
            using (var schema = PolarsSchema.From<Row>())
                lf = LazyFrame.ScanDelta(path, schema: schema);
            using (lf)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                Assert.Equal([21L, 22L, 23L], lf.AsQueryable<Row>().Where(r => r.Id > 20).Select(r => r.Id).ToList().Order());
            }
        });
    }
}
