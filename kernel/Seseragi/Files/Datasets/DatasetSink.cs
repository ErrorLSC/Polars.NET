using System.Runtime.ExceptionServices;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Grouping;
using Kaminari.Sazanami.Ordering;
using Kaminari.Sazanami.Platform;
using Kaminari.Sazanami.Text;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files;

/// <summary>What a <see cref="DatasetSink{TPartition}"/> writes, and how.</summary>
/// <param name="Store">Where the files go.</param>
/// <param name="Format">The files' format.</param>
/// <param name="Keys">The input columns whose values split the rows into partitions (none: one partition).</param>
/// <param name="EmptyIsNull">Per key, whether an empty string is the same partition as a null (Hive paths, as Delta takes them, have no way to tell them apart; a dataset's Hive paths do).</param>
/// <param name="Data">The input columns written into the files, in their order.</param>
/// <param name="MaxRowsPerFile">The rows a file closes at (0: no limit).</param>
/// <param name="ApproxBytesPerFile">The size on disk a file closes at, its last rows cut to fit (0: no limit).</param>
/// <param name="MemoryBudget">The bytes the files may hold in memory together (see <see cref="DatasetSink{TPartition}.HoldMemory"/>).</param>
/// <param name="MaxDegreeOfParallelism">The threads the files are written on together.</param>
/// <param name="SyncOnCommit">Whether a local file is synced to disk before it is renamed into place.</param>
internal sealed record DatasetSinkSettings(
    IWritableFileStore Store, DatasetFileFormat Format, int[] Keys, bool[] EmptyIsNull, int[] Data,
    long MaxRowsPerFile, long ApproxBytesPerFile, long MemoryBudget, int MaxDegreeOfParallelism, bool SyncOnCommit);

/// <summary>The first row of a partition's rows, by which it is described: the row of a chunk of the batch written, and that chunk's key columns.</summary>
internal readonly record struct PartitionRow(int Chunk, int Row, IReadOnlyList<IArrowArray> Keys);

/// <summary>A file a sink wrote: its path in the store, partition, rows, size and footer (where the format reports one).</summary>
internal sealed record SunkFile<TPartition>(string Path, TPartition Partition, long Rows, long Bytes, object? Footer);

/// <summary>
/// Writes rows as files of a dataset: split by the values of their keys (each value a partition: the rows of one
/// partition only in a file), by size on disk and by rows; files of several partitions written side by side, their
/// publications (the streams' commits) going on while the next rows are written. The rows come as batches, each of
/// one or more chunks (an engine's batch, as it holds it). A partition is described once, from its first row
/// (<c>describe</c>, null without keys); its files are named by <c>pathFor</c> (the partition, the file's index in it:
/// 0, 1, …), when they open (calls are serialized). No rows, no file.
/// </summary>
/// <remarks>
/// Shared by the dataset writers and the Delta sink. On a failure the caller gives the sink up
/// (<see cref="GiveUp"/>): every open file is given up (nothing of it stays); the files published before stay, listed
/// by <see cref="Landed"/>.
/// </remarks>
internal sealed class DatasetSink<TPartition>(DatasetSinkSettings settings, Func<PartitionRow?, TPartition> describe, Func<TPartition, long, string> pathFor)
    : IDisposable
{
    /// <summary>
    /// The files a sink publishes at once (their streams committing: a cloud upload's last part and completion, or a
    /// small file's one request), while the next rows are written.
    /// </summary>
    private const int MaxPublishing = 64;

    /// <summary>
    /// The default of <see cref="DatasetSinkSettings.MemoryBudget"/> for a <paramref name="setting"/> (null or blank: an
    /// eighth of the memory the process may use, as the runtime sees it — a container's limit, not the machine's — at
    /// least 256 MiB; else a size such as <c>2GiB</c> or a share such as <c>25%</c>) with <paramref name="available"/> bytes.
    /// </summary>
    /// <exception cref="ArgumentException"><paramref name="setting"/> holds neither a size nor a share.</exception>
    internal static long Budget(string? setting, long available, string source)
    {
        if (string.IsNullOrWhiteSpace(setting)) return Math.Max(256L << 20, available / 8);
        if (ByteSizes.TryParseSizeOrShare(setting, available, out long bytes) && bytes > 0) return bytes;
        throw new ArgumentException(
            $"{source} is '{setting}': neither a size (such as 2GiB or 512MiB) nor a share of the memory the process may use (such as 25%).");
    }

    /// <summary>The default memory budget: an eighth of the memory the process may use, at least 256 MiB.</summary>
    internal static long DefaultBudget => Budget(null, GC.GetGCMemoryInfo().TotalAvailableMemoryBytes, "");

    /// <summary>A partition: its description, its open file and the files it closed.</summary>
    private sealed class Partition(TPartition description)
    {
        public TPartition Description { get; } = description;
        public File? Open { get; set; }
        public long NextIndex { get; set; }
        public List<File> Closed { get; } = [];

        // Of the batch being written: its number when the partition has rows in it, those rows' runs, the error met.
        public long Batch { get; set; }
        public List<Run> Runs { get; } = [];
        public Exception? Error { get; set; }
    }

    /// <summary>
    /// Rows of one partition in a chunk of a batch (or in the chunk gathered in group order): the chunk by its number
    /// in the batch, not by reference. Partitions live as long as the sink, in the oldest generation; a reference to
    /// each new chunk stored in a thousand of them is a thousand old-to-young pointers the garbage collector scans
    /// on every collection, and keeps the chunk's objects alive until the partition's next batch.
    /// </summary>
    private readonly record struct Run(int Chunk, int Start, int Count);

    private readonly long _target = settings.ApproxBytesPerFile;
    private readonly long _maxRows = settings.MaxRowsPerFile;
    private readonly int[] _keys = settings.Keys;
    // The partitions in the order they first came, numbered by their keys: a partition's index is its keys' global group.
    private readonly List<Partition> _partitions = [];
    private readonly KeyGroups? _groups = settings.Keys.Length > 0 ? new KeyGroups(settings.Keys.Length, settings.EmptyIsNull) : null;
    // Scratch kept from chunk to chunk (native): each row's group in its chunk, the chunk's groups (global ids, first
    // rows, starts in group order) and its rows in group order.
    private readonly NativeArray<int> _ids = new(), _globals = new(), _firstRows = new(), _starts = new(), _order = new();
    // Kept from batch to batch: the partitions a batch touches (in order), the batch's number.
    private readonly List<Partition> _touched = [];
    private readonly List<RecordBatch> _sources = []; // the batch's chunks as the runs number them (cleared after the batch)
    private long _batch;
    private Schema? _dataSchema;
    private bool _identity; // the data columns are the input's, in order: chunks written as they are
    private readonly object _naming = new();
    private int _closedForMemory;
    private bool _footersHandedOut; // by Finish: the files' footers are then the caller's to dispose
    // The files closed whose streams are still committing, oldest first (closes come from several threads).
    private readonly List<(File File, Task Commit)> _publishing = [];
    private readonly object _publishingLock = new();
    private readonly SemaphoreSlim _publishers = new(MaxPublishing);

    /// <summary>How many files were closed before their size to keep the bytes held under the memory budget.</summary>
    public int ClosedForMemory => _closedForMemory;

    /// <summary>
    /// Writes a batch (its chunks; the caller keeps them, and may let them go once this returns). Without keys every
    /// chunk is written as it is. With keys each chunk's rows are grouped by their keys (<see cref="KeyGroups"/>, then
    /// <see cref="CountingSort"/>: no sort) and gathered into group order once (unless grouped already, or of one
    /// partition: then nothing is copied); each partition's rows are then written by its file as ranges, the
    /// partitions side by side.
    /// </summary>
    /// <param name="chunks">The batch's chunks.</param>
    /// <param name="preGrouped">Whether the rows come grouped by their keys (each partition's rows together, across
    /// batches): then not gathered, and a partition's file is closed once the next partition's rows come.</param>
    /// <remarks>
    /// Measured (10M rows, 2026-11-18): one gather per chunk on this thread beats each partition gathering its own
    /// rows side by side: a gather has a fixed cost (some 33 KB managed for 8 columns), which 1000 partitions paid
    /// 81,000 times (3× slower, 6 GB allocated), and even 30 cost 200 MB and 5–8 % of the time.
    /// </remarks>
    public void Write(IReadOnlyList<RecordBatch> chunks, bool preGrouped = false)
    {
        if (_groups is null)
        {
            foreach (RecordBatch chunk in chunks)
            {
                if (chunk.Length == 0) continue;
                RecordBatch data = Data(chunk);
                if (_partitions.Count == 0) _partitions.Add(new Partition(describe(null)));
                WriteRows(_partitions[0], data, 0, data.Length);
            }
            return;
        }
        _batch++;
        _touched.Clear();
        List<RecordBatch> owned = [];
        try
        {
            for (int c = 0; c < chunks.Count; c++)
            {
                if (chunks[c].Length > 0) Group(chunks[c], c, owned);
            }
            ForEachPartition(_touched, partition =>
            {
                foreach (Run run in partition.Runs) WriteRows(partition, _sources[run.Chunk], run.Start, run.Count);
            });
        }
        finally
        {
            _sources.Clear();
            foreach (RecordBatch gathered in owned) gathered.Dispose();
        }

        // Grouped rows: a partition before the last one of this batch has no rows to come.
        if (preGrouped && _touched.Count > 0)
        {
            Partition last = _touched[^1];
            foreach (Partition partition in _partitions)
            {
                if (partition != last && partition.Open is not null) Close(partition);
            }
        }
    }

    /// <summary>
    /// The data columns of a chunk (sharing its arrays: never disposed apart); the schema of the files and the format
    /// prepared from the first.
    /// </summary>
    private RecordBatch Data(RecordBatch chunk)
    {
        if (_dataSchema is null)
        {
            _identity = settings.Data.Length == chunk.ColumnCount && settings.Data.Select((column, i) => column == i).All(static same => same);
            _dataSchema = _identity ? chunk.Schema : new Schema([.. settings.Data.Select(chunk.Schema.GetFieldByIndex)], chunk.Schema.Metadata);
            settings.Format.Prepare(_dataSchema, sideBySide: _groups is not null, settings.MaxDegreeOfParallelism, measured: _target > 0);
        }
        return _identity ? chunk : new RecordBatch(_dataSchema, [.. settings.Data.Select(chunk.Column)], chunk.Length);
    }

    /// <summary>
    /// The runs of chunk <paramref name="index"/>, each partition's added to it: ranges of the chunk when its rows come
    /// grouped, else of the chunk gathered in group order (kept in <paramref name="owned"/> until the batch is written).
    /// </summary>
    private void Group(RecordBatch chunk, int index, List<RecordBatch> owned)
    {
        int rows = chunk.Length;
        RecordBatch data = Data(chunk);
        IArrowArray[] keys = [.. _keys.Select(chunk.Column)];
        _ids.Reserve(rows);
        _globals.Reserve(rows);
        _firstRows.Reserve(rows);
        int count = _groups!.Assign(keys, _ids.Span, _globals.Span, _firstRows.Span);
        _starts.Reserve(count + 1);
        _order.Reserve(rows);
        if (!CountingSort.Permutation(_ids.AsSpan(0, rows), _starts.AsSpan(0, count + 1), _order.AsSpan(0, rows)))
        {
            // A permutation of the whole chunk: the long values of views are shared, not copied.
            // Columns side by side, one per physical core at most (a gather is bound by memory: a core's second
            // hardware thread adds nothing). Measured (10M rows, 8 columns, 8 cores): 50–100 ms less a write.
            data = ArrowTake.Take(data, _order.AsSpan(0, rows), maxDegreeOfParallelism: Math.Min(data.ColumnCount, CpuTopology.PhysicalCores));
            owned.Add(data);
        }
        int source = _sources.Count;
        _sources.Add(data);
        for (int local = 0; local < count; local++)
        {
            int global = _globals[local];
            if (global == _partitions.Count) _partitions.Add(new Partition(describe(new PartitionRow(index, _firstRows[local], keys))));
            Partition partition = _partitions[global];
            if (partition.Batch != _batch)
            {
                partition.Batch = _batch;
                partition.Runs.Clear();
                partition.Error = null;
                _touched.Add(partition);
            }
            partition.Runs.Add(new Run(source, _starts[local], _starts[local + 1] - _starts[local]));
        }
    }

    /// <summary>
    /// Runs <paramref name="work"/> for each partition, side by side (each partition on one thread at a time); once
    /// all are done, the first failure in the partitions' order is rethrown.
    /// </summary>
    private void ForEachPartition(List<Partition> partitions, Action<Partition> work)
    {
        foreach (Partition partition in partitions) partition.Error = null;
        void Run(int i)
        {
            try
            {
                work(partitions[i]);
            }
            catch (Exception e)
            {
                partitions[i].Error = e;
            }
        }
        if (partitions.Count == 1) Run(0);
        else if (partitions.Count > 1) Parallel.For(0, partitions.Count, new ParallelOptions { MaxDegreeOfParallelism = settings.MaxDegreeOfParallelism }, Run);
        foreach (Partition partition in partitions)
        {
            if (partition.Error is { } error) ExceptionDispatchInfo.Throw(error);
        }
    }

    /// <summary>
    /// Rows <paramref name="start"/> .. +<paramref name="count"/> of a batch into a partition's files: as many as the
    /// open file takes (up to its row limit, and up to its size, a row's bytes on disk as the file shows them so far;
    /// before its first rows, their logical Arrow bytes: mostly no fewer than on disk), a full file closed and the next
    /// one opened. Rows are passed as ranges, never sliced (a slice is an array object per column). One thread at a
    /// time per partition.
    /// </summary>
    private void WriteRows(Partition partition, RecordBatch rows, int start, int count)
    {
        for (int at = start, end = start + count; at < end;)
        {
            File open = partition.Open ??= OpenFile(partition);
            int take = end - at;
            if (_maxRows > 0) take = (int)Math.Min(take, _maxRows - open.Writer.Rows);
            if (_target > 0)
            {
                double perRow = open.Writer.PerRow ?? (double)ArrowSizes.LogicalBytes(rows, at, end - at) / (end - at);
                take = (int)Math.Min(take, Math.Max(1, (long)((_target - open.Writer.Bytes(_target)) / perRow)));
            }
            open.Writer.Write(rows, at, take); // encoded at once
            at += take;
            // Closed at its row limit, or when not one more row fits.
            if ((_maxRows > 0 && open.Writer.Rows >= _maxRows) || (_target > 0 && _target - open.Writer.Bytes(_target) < open.Writer.PerRow)) Close(partition);
        }
    }

    private File OpenFile(Partition partition)
    {
        string path;
        lock (_naming) path = pathFor(partition.Description, partition.NextIndex++);
        FileWriteStream stream = settings.Store.OpenWrite(path, createOnly: true);
        stream.SyncOnCommit = settings.SyncOnCommit;
        try
        {
            return new File(path, partition.Description, stream, settings.Format.Open(stream));
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Closes a partition's open file: its last pages and footer written here; its publication (the stream's commit)
    /// goes on without a thread while the next rows are written, waited for by <see cref="HoldMemory"/> (for memory)
    /// and <see cref="Finish"/>.
    /// </summary>
    private void Close(Partition partition)
    {
        File file = partition.Open!;
        partition.Open = null;
        try
        {
            file.Complete();
        }
        catch
        {
            // Neither open nor closed now: given up here, or nothing would remove what it left (a temporary file).
            try
            {
                file.GiveUp();
            }
            catch (Exception)
            {
                // The failure to complete is the one reported.
            }
            throw;
        }
        partition.Closed.Add(file);
        Task commit = file.CommitAsync(_publishers);
        lock (_publishingLock) _publishing.Add((file, commit));
    }

    /// <summary>
    /// Forgets the files published (the first failure among them thrown); returns the bytes those still publishing
    /// hold.
    /// </summary>
    private long ForgetPublished()
    {
        lock (_publishingLock)
        {
            Task? failed = _publishing.Find(static p => p.Commit.IsFaulted || p.Commit.IsCanceled).Commit;
            failed?.GetAwaiter().GetResult();
            _publishing.RemoveAll(static p => p.Commit.IsCompletedSuccessfully);
            long held = 0;
            foreach (var (file, _) in _publishing) held += file.BufferedBytes;
            return held;
        }
    }

    /// <summary>Waits for the oldest file still publishing (none: returns false).</summary>
    private bool WaitForOldest()
    {
        Task oldest;
        lock (_publishingLock)
        {
            if (_publishing.Count == 0) return false;
            oldest = _publishing[0].Commit;
        }
        ((IAsyncResult)oldest).AsyncWaitHandle.WaitOne();
        return true;
    }

    /// <summary>
    /// Keeps the bytes the files hold in memory under the budget: the files being published waited for first (their
    /// bytes leave memory without making files smaller), then the largest files' pending pages written out early
    /// (<see cref="DatasetFileWriter.EndChunk"/>), as many as it takes; then, still beyond (streams holding their
    /// bytes), the largest files closed early. Side by side.
    /// </summary>
    public void HoldMemory()
    {
        long budget = settings.MemoryBudget;
        List<Partition> open = [.. _partitions.Where(p => p.Open is not null)];
        long publishing = ForgetPublished();
        long held = open.Sum(p => p.Open!.HeldBytes);
        while (held + publishing > budget && WaitForOldest()) publishing = ForgetPublished();
        if (held <= budget) return;
        // In rounds, the largest first: a file whose pages went out keeps some scratch, which the estimate leaves out
        // (counted as its stream's bytes alone); the files not ended yet go next while some remain.
        HashSet<Partition> ended = [];
        while (held > budget && ended.Count < open.Count)
        {
            List<Partition> ending = [];
            long estimate = held;
            foreach (Partition partition in open.Where(p => !ended.Contains(p)).OrderByDescending(p => p.Open!.HeldBytes))
            {
                if (estimate <= budget) break;
                ending.Add(partition);
                ended.Add(partition);
                estimate -= partition.Open!.HeldBytes - partition.Open.BufferedBytes;
            }
            ForEachPartition(ending, p => p.Open!.Writer.EndChunk());
            held = open.Sum(p => p.Open!.HeldBytes);
        }
        if (held <= budget) return;
        List<Partition> closing = [];
        foreach (Partition partition in open.OrderByDescending(p => p.Open!.HeldBytes))
        {
            if (held <= budget) break;
            closing.Add(partition);
            held -= partition.Open!.HeldBytes;
        }
        ForEachPartition(closing, Close);
        _closedForMemory += closing.Count;
    }

    /// <summary>
    /// Closes the files still open, side by side, and waits for every publication; every file written, partition by
    /// partition. Their footers (disposable where the format's are) are the caller's from then on.
    /// </summary>
    public List<SunkFile<TPartition>> Finish()
    {
        ForEachPartition([.. _partitions.Where(p => p.Open is not null)], Close);
        while (WaitForOldest()) ForgetPublished();
        List<SunkFile<TPartition>> files = [.. _partitions.SelectMany(p => p.Closed).Select(f => f.Sunk!)];
        _footersHandedOut = true;
        return files;
    }

    /// <summary>Gives up every open file (nothing of them stays), once the files being published have landed or not.</summary>
    public void GiveUp()
    {
        Task[] publishing;
        lock (_publishingLock) publishing = [.. _publishing.Select(static p => p.Commit)];
        foreach (Task commit in publishing) ((IAsyncResult)commit).AsyncWaitHandle.WaitOne();
        foreach (Partition partition in _partitions)
        {
            try
            {
                partition.Open?.GiveUp();
            }
            catch (Exception)
            {
                // The first failure is the one reported; the other files are given up anyway.
            }
            partition.Open = null;
        }
    }

    /// <summary>The files published so far (after <see cref="GiveUp"/>: every file that landed).</summary>
    public List<SunkFile<TPartition>> Landed() => [.. _partitions.SelectMany(p => p.Closed).Where(f => f.Landed).Select(f => f.Sunk!)];

    /// <summary>Frees the keys' and the scratch's native memory, and the footers of the files written unless <see cref="Finish"/> handed them out.</summary>
    public void Dispose()
    {
        if (!_footersHandedOut)
        {
            _footersHandedOut = true;
            foreach (File file in _partitions.SelectMany(p => p.Closed)) (file.Sunk?.Footer as IDisposable)?.Dispose();
        }
        _groups?.Dispose();
        _publishers.Dispose();
        foreach (NativeArray<int> scratch in (NativeArray<int>[])[_ids, _globals, _firstRows, _starts, _order]) scratch.Dispose();
    }

    /// <summary>A file being written: its stream and writer; once complete, what it is.</summary>
    private sealed class File(string path, TPartition partition, FileWriteStream stream, DatasetFileWriter writer)
    {
        private int _landed;

        public DatasetFileWriter Writer => writer;

        /// <summary>The file complete (null before).</summary>
        public SunkFile<TPartition>? Sunk { get; private set; }

        /// <summary>Whether its commit succeeded.</summary>
        public bool Landed => Volatile.Read(ref _landed) != 0;

        /// <summary>What the file holds in memory (at least): its writer's and its stream's.</summary>
        public long HeldBytes => writer.HeldBytes + stream.BufferedBytes;

        /// <summary>What its stream buffers.</summary>
        public long BufferedBytes => stream.BufferedBytes;

        /// <summary>Completes the file (its last pages and footer go to the stream); <see cref="CommitAsync"/> publishes it.</summary>
        public void Complete()
        {
            object? footer = writer.Complete();
            Sunk = new SunkFile<TPartition>(path, partition, writer.Rows, writer.BytesWritten, footer);
        }

        /// <summary>
        /// Publishes the completed file without holding a thread, one of <paramref name="publishers"/>' slots taken
        /// meanwhile; the stream is disposed after (a failed commit leaves nothing).
        /// </summary>
        public async Task CommitAsync(SemaphoreSlim publishers)
        {
            await publishers.WaitAsync().ConfigureAwait(false);
            try
            {
                if (!await stream.CommitAsync().ConfigureAwait(false)) throw new IOException($"The file {path} exists already.");
                Volatile.Write(ref _landed, 1);
            }
            finally
            {
                publishers.Release();
                await stream.DisposeAsync().ConfigureAwait(false);
            }
        }

        /// <summary>Gives the file up: nothing buffered is written on, nothing of it stays.</summary>
        public void GiveUp()
        {
            try
            {
                writer.Abort();
            }
            finally
            {
                stream.Dispose();
            }
        }
    }
}
