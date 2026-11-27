using System.Data;
using Polars.CSharp;
using Polars.NET.Core.IO.Plugins;

namespace Polars.Config.Tests;

/// <summary>
/// The file a buffered scan spools its source to (<see cref="SpooledScans"/>): open while the frame lives, with no
/// name in the engine's temporary directory, closed (and so gone) with the frame (serial project: the open files are
/// counted process-wide).
/// </summary>
public class SpooledScanFileTests
{
    private const int Rows = 1000;

    /// <summary>The open spool files of this process (Linux: the descriptors of deleted files named as spools).</summary>
    private static int OpenSpools() =>
        new DirectoryInfo("/proc/self/fd").EnumerateFileSystemInfos()
            .Select(static fd => { try { return fd.LinkTarget ?? ""; } catch (IOException) { return ""; } })
            .Count(static target => target.Contains("polars-net-spool-", StringComparison.Ordinal));

    private static IDataReader Reader()
    {
        var t = new DataTable();
        t.Columns.Add("Id", typeof(int));
        for (int i = 0; i < Rows; i++) t.Rows.Add(i);
        return t.CreateDataReader();
    }

    [Fact]
    public void A_Buffered_Scan_Spools_To_A_File_Without_A_Name_Released_With_The_Frame()
    {
        if (!OperatingSystem.IsLinux()) return; // open files are counted through /proc
        GC.Collect();
        GC.WaitForPendingFinalizers();
        int before = OpenSpools();
        using (var lf = LazyFrame.ScanDatabaseBuffered(Reader(), batchSize: 100))
        {
            Assert.Equal(before + 1, OpenSpools());
            // No name in the directory: the file lives as long as it is open.
            Assert.Empty(Directory.EnumerateFiles(PolarsTempDirectory.Path, $"polars-net-spool-{Environment.ProcessId}-*"));
            using (var df = lf.Collect()) Assert.Equal(Rows, df.Height);
            using (var df = lf.Collect(Engine.Streaming)) Assert.Equal(Rows, df.Height);
        }
        Assert.Equal(before, OpenSpools());
    }
}
