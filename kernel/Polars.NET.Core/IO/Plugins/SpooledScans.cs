using Apache.Arrow;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files.Ipc;

namespace Polars.NET.Core.IO.Plugins;

/// <summary>
/// Sources read now, whole, into an anonymous temporary Arrow IPC file, and scanned from it each time a plan runs
/// (any number of runs): the file is in the engine's temporary directory (<see cref="PolarsTempDirectory"/>), has no
/// name there (removed from the directory once open; on Windows, deleted when closed) and goes away with the last
/// frame that scans it. Runs read the columns and leading rows they need, the filters the IPC reader applies exactly
/// left out of the engine's plan.
/// </summary>
internal static class SpooledScans
{
    /// <summary>A scan of <paramref name="batches"/> (of <paramref name="schema"/>), read now and released as read.</summary>
    public static LazyFrameHandle Open(IEnumerable<RecordBatch> batches, Schema schema)
    {
        FileStream file = AnonymousFile();
        IpcFile? ipc = null;
        try
        {
            using (IpcWriter writer = IpcWriter.Create(file, schema, new IpcWriteOptions { LeaveOpen = true }))
            {
                foreach (RecordBatch batch in batches)
                {
                    // The spool consumes its batches, as a stream's export does.
                    using (batch) writer.Write(batch);
                }
                writer.Complete();
            }
            file.Flush();
            // The file is read from its start (a stream is read from its position).
            file.Position = 0;
            ipc = IpcFile.Open(file, leaveOpen: true);
            Schema read;
            using (IpcReader probe = ipc.OpenReader()) read = probe.Schema;
            IpcFile spooled = ipc;
            var scan = new ManagedFrames.Scan(read, null, 0, null);
            return ManagedScans.Open("spooled", ManagedScans.Identity(), scan, null, Batches, Exactly, new Owner(spooled, file));

            IEnumerable<RecordBatch> Batches(ScanRun run)
            {
                using IpcReader reader = spooled.OpenReader(new IpcReadOptions
                {
                    Columns = run.Columns,
                    MaxRows = run.Limit,
                    Filter = run.Filter,
                    FilterRows = run.Filter is not null,
                });
                foreach (RecordBatch batch in reader.ReadBatches()) yield return batch;
            }

            bool Exactly(ScalarExpr filter)
            {
                using IpcReader reader = spooled.OpenReader(new IpcReadOptions { Filter = filter, FilterRows = true });
                return reader.FiltersExactly;
            }
        }
        catch
        {
            ipc?.Dispose();
            file.Dispose();
            throw;
        }
    }

    /// <summary>A new file in the engine's temporary directory, open for reading and writing, with no name there.</summary>
    private static FileStream AnonymousFile()
    {
        string path = Path.Combine(PolarsTempDirectory.Path, $"polars-net-spool-{Environment.ProcessId}-{Guid.NewGuid():N}.arrow");
        var file = new FileStream(path, new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.ReadWrite,
            Share = FileShare.Read | FileShare.Write | FileShare.Delete,
            Options = FileOptions.DeleteOnClose,
            BufferSize = 1 << 16,
        });
        // Unix: the open file outlives its name (Windows deletes it when it closes).
        if (!OperatingSystem.IsWindows())
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // Left to DeleteOnClose.
            }
        }
        return file;
    }

    /// <summary>The spooled file and the reader of it, released with the scan.</summary>
    private sealed class Owner(IpcFile ipc, FileStream file) : IDisposable
    {
        public void Dispose()
        {
            try
            {
                ipc.Dispose();
            }
            finally
            {
                file.Dispose();
            }
        }
    }
}

/// <summary>
/// The engine's temporary directory, as the engine finds it: <c>POLARS_TEMP_DIR</c>, else (Unix) <c>polars-$USER</c>
/// in the system's temporary directory, created owner-only (0700), or (Windows) <c>polars</c> there.
/// </summary>
internal static class PolarsTempDirectory
{
    private static readonly Lazy<string> Resolved = new(Resolve);

    public static string Path => Resolved.Value;

    private static string Resolve()
    {
        if (Environment.GetEnvironmentVariable("POLARS_TEMP_DIR") is { Length: > 0 } given)
        {
            Directory.CreateDirectory(given);
            return given;
        }
        string temp = System.IO.Path.GetTempPath();
        if (OperatingSystem.IsWindows()) return Directory.CreateDirectory(System.IO.Path.Combine(temp, "polars")).FullName;
        string user = Environment.GetEnvironmentVariable("USER") is { Length: > 0 } u ? u : Environment.UserName;
        string path = System.IO.Path.Combine(temp, $"polars-{user}");
        Directory.CreateDirectory(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        return path;
    }
}
