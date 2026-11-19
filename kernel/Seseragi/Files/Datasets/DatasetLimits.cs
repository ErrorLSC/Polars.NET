using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files;

/// <summary>
/// How far a dataset reader of any format reads ahead: units (a format's pieces of a file: Parquet row groups, Arrow
/// IPC record batches) decoded ahead of the consumer, files open at once, requests to a store at once. Measured on
/// Parquet datasets (P4/P5d); the same for every format, as the options document them.
/// </summary>
internal static class DatasetLimits
{
    /// <summary>How many units are decoded at once ahead of the consumer, per thread, from local files and memory.</summary>
    internal const int UnitsAheadPerThread = 1;

    /// <summary>
    /// How many units are fetched and decoded at once ahead of the consumer, per thread, from a remote store: their
    /// requests wait on the network. Measured (16 threads, 64 files of 2 MB, 4 of 33 MB, stores 6 and 23 ms away): 4
    /// read 5–30% faster than 1.
    /// </summary>
    internal const int RemoteUnitsAheadPerThread = 4;

    /// <summary>
    /// The fewest units ahead from a remote store, whatever the threads: with one thread, 16 units ahead read 60
    /// partitioned files of 425 KB 3.5–5 times as fast as 1 (stores 6, 23 and 170 ms away).
    /// </summary>
    internal const int RemoteMinUnitsAhead = 16;

    /// <summary>The fewest files open at once (read, or opened ahead) when <see cref="DatasetOptions.MaxFilesInFlight"/> is not set.</summary>
    internal const int MinFilesAhead = 16;

    /// <summary>
    /// The requests to a remote store at once when <see cref="DatasetOptions.MaxRequestsInFlight"/> is not set
    /// (provisional). Measured on 2000 files of 52 KB, 16 threads, from one home link: the best number differs by the
    /// store's distance (16–32 at 6 ms, 64 at 23 ms, 128 or more at 170 ms), and many more requests at once lose
    /// packets on the way, so that a few requests take hundreds of milliseconds.
    /// </summary>
    internal const int RemoteRequests = 64;

    /// <summary>
    /// The files open at once from a remote store when <see cref="DatasetOptions.MaxFilesInFlight"/> is not set:
    /// many, so that the files after one whose request is slow keep being read while the consumer waits for it (each
    /// holds its footer, and a small file its bytes).
    /// </summary>
    internal const int RemoteFilesHeld = 1024;

    /// <summary>Whether a store's requests wait on the network (not local files or memory).</summary>
    public static bool Remote(IFileStore store) => store is not (LocalFileStore or MemoryFileStore);

    /// <summary>How many units are decoded at once ahead of the consumer.</summary>
    public static int UnitsAhead(int parallelism, bool remote) => remote
        ? Math.Max(RemoteMinUnitsAhead, Math.Max(1, parallelism) * RemoteUnitsAheadPerThread)
        : Math.Max(1, parallelism) * UnitsAheadPerThread;

    /// <summary>
    /// How many files are open at once (read, planned, or opened ahead): as the options say; else, from local files
    /// and memory, enough to feed the units ahead (a small file is one unit), and from other stores
    /// <see cref="RemoteFilesHeld"/> (their requests bounded apart, see <see cref="RequestsInFlight"/>).
    /// </summary>
    public static int FilesInFlight(DatasetOptions options, bool remote, int unitsAhead) =>
        options.MaxFilesInFlight ?? (remote ? RemoteFilesHeld : Math.Max(MinFilesAhead, unitsAhead + 1));

    /// <summary>
    /// How many requests to the store run at once (files opened, units fetched), null when not bounded: as the options
    /// say, else <see cref="RemoteRequests"/> from a remote store; never more than the files open at once when the
    /// options bound those.
    /// </summary>
    public static int? RequestsInFlight(DatasetOptions options, bool remote) =>
        (options.MaxRequestsInFlight ?? (remote ? RemoteRequests : null)) is int requests
            ? Math.Min(requests, options.MaxFilesInFlight ?? int.MaxValue)
            : null;
}
