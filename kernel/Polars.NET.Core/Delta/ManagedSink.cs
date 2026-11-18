using System.Runtime.ExceptionServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Grouping;
using Kaminari.Sazanami.Ordering;
using Kaminari.Sazanami.Platform;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Writing;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Wrappers;
using ArrowTypes = Apache.Arrow.Types;
using DeltaArray = Kaminari.Seseragi.Lakehouse.Delta.Schema.ArrayType;
using DeltaMap = Kaminari.Seseragi.Lakehouse.Delta.Schema.MapType;
using DeltaStruct = Kaminari.Seseragi.Lakehouse.Delta.Schema.StructType;
using DeltaType = Kaminari.Seseragi.Lakehouse.Delta.Schema.DeltaType;
using StructField = Kaminari.Seseragi.Lakehouse.Delta.Schema.StructField;

namespace Polars.NET.Core.Delta;

/// <summary>
/// Writes a table's data files with the managed Parquet writer instead of the engine's sink: the engine streams the
/// rows (its streaming engine, batch by batch, as Arrow), the writer encodes them and the store takes the bytes as
/// they come (in parts on a cloud store, a temporary file renamed on a local disk). Files are split by size on disk
/// (and by rows), maps are written as Parquet maps, field ids come from the table's column mapping, and each file's
/// footer comes from the writer (nothing read back).
/// </summary>
internal static class ManagedSink
{
    /// <summary>
    /// The environment variable choosing the sink of data files: <c>managed</c>, <c>native</c> (the engine's), or
    /// <c>auto</c> (default: the engine's for now).
    /// </summary>
    public const string Variable = "POLARS_NET_DELTA_SINK";

    /// <summary>The rows of a row group (and of a batch the engine hands over): the engine's own sink's row groups.</summary>
    internal const int DefaultRowGroupRows = 122_880;

    /// <summary>A file written: its path relative to the table, size and footer.</summary>
    public readonly record struct Written(string Relative, long Size, ParquetMetadata Footer);

    /// <summary>
    /// The files written, partition by partition in the order the partitions first came (each partition's in order),
    /// and how many of them were closed before their size to keep the buffered bytes under <see cref="MemoryBudget"/>.
    /// </summary>
    public sealed record SinkResult(List<Written> Files, int ClosedForMemory);

    /// <summary>
    /// The bytes the files may hold in memory together (the open files' pages not written yet, their writers' scratch,
    /// and what the streams buffer: a cloud upload's parts, the files still being published); beyond, the files being
    /// published are waited for, then the largest row groups ended early, then the largest files closed early. The
    /// fixed buffers of each open writer come on top. An eighth of the memory the process may use (as the runtime
    /// sees it: a container's limit, not the machine's), at least 256 MiB. Internal knob (tests set it).
    /// </summary>
    internal static long MemoryBudget { get; set; } = Math.Max(256L << 20, GC.GetGCMemoryInfo().TotalAvailableMemoryBytes / 8);

    /// <summary>
    /// The files a sink publishes at once (their streams committing: a cloud upload's last part and completion, or a
    /// small file's one request), while the next rows are written.
    /// </summary>
    private const int MaxPublishing = 64;

    /// <summary>
    /// Whether the managed writer writes these files: asked for, into a store that streams (a local disk, or a cloud
    /// store served by the managed clients: the native store would hold every file whole in memory).
    /// </summary>
    public static bool Applies(IWritableFileSystem fileSystem) =>
        Environment.GetEnvironmentVariable(Variable)?.Trim().ToLowerInvariant() == "managed" && Streams(fileSystem);

    private static bool Streams(IWritableFileSystem fileSystem) => fileSystem switch
    {
        TrackingFileSystem tracking => Streams(tracking.Inner),
        CatalogManagedFileSystem catalog => Streams(catalog.Inner),
        LocalFileSystem => true,
        ObjectStoreFileSystem store => store.StreamsWrites,
        _ => false,
    };

    /// <summary>
    /// Sinks <paramref name="rows"/> (consumed: the <paramref name="keys"/> columns, then the data columns under their
    /// physical names in <paramref name="dataFields"/>' order, then any extra columns) as files, split by the keys'
    /// values (each value a partition: rows of one partition only in a file, the key columns left out), by size on disk
    /// and by rows. <paramref name="describe"/> describes a partition from a one-row frame of its rows (null without
    /// keys), once, when its first rows come; <paramref name="pathFor"/> names its file of an index (0, 1, …), when the
    /// file opens (calls are serialized). A file is committed when it is full, or at the end. No rows, no file. On a
    /// failure every open file is given up (nothing of it stays) and the first error, in the order of the rows,
    /// rethrown; files committed before stay for the caller to remove.
    /// </summary>
    /// <param name="keysPreGrouped">Whether the rows come grouped by their keys (each partition's rows together): then
    /// not sorted, and a partition's file is closed once the next partition's rows come.</param>
    /// <param name="approxBytesPerFile">The size a file closes at on disk (the last rows cut to fit); 0 for no limit.</param>
    public static SinkResult Sink<TPartition>(
        IWritableFileSystem fileSystem, LazyFrameHandle rows, string[] keys, bool keysPreGrouped, StructField[] dataFields,
        Func<DataFrameHandle?, TPartition> describe, Func<TPartition, long, string> pathFor, KernelWriteOptions options, ulong approxBytesPerFile)
    {
        using var sink = new Sinking<TPartition>(fileSystem, keys, dataFields, describe, pathFor, options, approxBytesPerFile);
        try
        {
            using CollectBatchesHandle batches = PolarsWrapper.LazyCollectBatches(rows, PlEngine.Streaming, options.MaintainOrder, (nuint)sink.RowGroupRows);
            while (true)
            {
                using DataFrameHandle frame = PolarsWrapper.CollectBatchesNext(batches, out bool done);
                if (done) break;
                if (keys.Length == 0)
                {
                    foreach (RecordBatch chunk in Chunks(frame))
                    {
                        using (chunk) sink.WriteUnkeyed(chunk);
                    }
                }
                else sink.WriteKeyed(frame, keysPreGrouped);
                sink.HoldMemory();
            }
            return sink.Finish();
        }
        catch
        {
            sink.GiveUp();
            throw;
        }
    }

    /// <summary>A partition: its description, its open file and the files it closed.</summary>
    private sealed class Partition<TPartition>(TPartition description)
    {
        public TPartition Description { get; } = description;
        public File? Open { get; set; }
        public long NextIndex { get; set; }
        public List<Written> Closed { get; } = [];

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

    /// <summary>The state of one sink: the writer schema, the partitions, the shared threads, the memory held.</summary>
    private sealed class Sinking<TPartition>(
        IWritableFileSystem fileSystem, string[] keys, StructField[] dataFields, Func<DataFrameHandle?, TPartition> describe,
        Func<TPartition, long, string> pathFor, KernelWriteOptions options, ulong approxBytesPerFile) : IDisposable
    {
        private readonly long _target = (long)Math.Min(approxBytesPerFile, long.MaxValue);
        private readonly long _maxRows = (long)Math.Min(options.MaxRowsPerFile, long.MaxValue);
        private readonly bool _sync = options.SyncOnClose != PlSyncOnClose.None;
        // The partitions in the order they first came, numbered by their keys (an empty string is a null, as Hive takes
        // it): a partition's index is its keys' global group.
        private readonly List<Partition<TPartition>> _partitions = [];
        private readonly KeyGroups? _groups = keys.Length > 0 ? new KeyGroups(keys.Length, [.. keys.Select(_ => true)]) : null;
        // Scratch kept from chunk to chunk (native): each row's group in its chunk, the chunk's groups (global ids, first
        // rows, starts in group order) and its rows in group order.
        private readonly NativeArray<int> _ids = new(), _globals = new(), _firstRows = new(), _starts = new(), _order = new();
        // Kept from batch to batch: the partitions a batch touches (in order), the batch's number.
        private readonly List<Partition<TPartition>> _touched = [];
        private readonly List<RecordBatch> _sources = []; // the batch's chunks as the runs number them (cleared after the batch)
        private long _batch;
        private Schema? _dataSchema;
        private readonly object _naming = new();
        // Files written side by side (one per partition) keep the processors busy together, not each of them.
        private readonly WorkBudget? _threads = keys.Length > 0 ? new WorkBudget(Environment.ProcessorCount) : null;
        private ParquetWriteOptions? _writerOptions;
        private ParquetWriter.WritePlan? _plan; // shared by the files written side by side (one per partition)
        private Schema? _schema;
        private int _closedForMemory;
        // The files closed whose streams are still committing, oldest first (closes come from several threads).
        private readonly List<(File File, Task Commit)> _publishing = [];
        private readonly object _publishingLock = new();
        private readonly SemaphoreSlim _publishers = new(MaxPublishing);

        public int RowGroupRows { get; } = options.RowGroupSize > 0 ? options.RowGroupSize : DefaultRowGroupRows;

        /// <summary>Rows of the one partition there is without keys.</summary>
        public void WriteUnkeyed(RecordBatch chunk)
        {
            Prepare(chunk.Schema, 0, chunk.ColumnCount);
            if (_partitions.Count == 0) _partitions.Add(new Partition<TPartition>(describe(null)));
            WriteRows(_partitions[0], chunk, 0, chunk.Length);
        }

        /// <summary>
        /// Rows split by their keys: the batch exported chunk by chunk (nothing copied), each chunk's rows grouped by
        /// their keys (<see cref="KeyGroups"/>, then <see cref="CountingSort"/>: no sort) and gathered into group order
        /// once (unless grouped already, or of one partition: then nothing is copied); each partition's rows then
        /// written by its file as ranges, the partitions side by side.
        /// </summary>
        /// <remarks>
        /// Measured (10M rows, 2026-11-18): one gather per chunk on this thread beats each partition gathering its own
        /// rows side by side: a gather has a fixed cost (some 33 KB managed for 8 columns), which 1000 partitions paid
        /// 81,000 times (3× slower, 6 GB allocated), and even 30 cost 200 MB and 5–8 % of the time.
        /// </remarks>
        public void WriteKeyed(DataFrameHandle frame, bool preGrouped)
        {
            _batch++;
            _touched.Clear();
            List<RecordBatch> chunks = [];
            try
            {
                int rowsBefore = 0;
                foreach (RecordBatch chunk in Chunks(frame))
                {
                    chunks.Add(chunk);
                    if (chunk.Length > 0) Group(frame, chunk, rowsBefore, chunks);
                    rowsBefore += chunk.Length;
                }
                ForEachPartition(_touched, partition =>
                {
                    foreach (Run run in partition.Runs) WriteRows(partition, _sources[run.Chunk], run.Start, run.Count);
                });
            }
            finally
            {
                _sources.Clear();
                foreach (RecordBatch chunk in chunks) chunk.Dispose();
            }

            // Grouped rows: a partition before the last one of this batch has no rows to come.
            if (preGrouped && _touched.Count > 0)
            {
                Partition<TPartition> last = _touched[^1];
                foreach (Partition<TPartition> partition in _partitions)
                {
                    if (partition != last && partition.Open is not null) Close(partition);
                }
            }
        }

        /// <summary>
        /// The runs of a chunk (rows <paramref name="rowsBefore"/> on of the batch), each partition's added to it: ranges
        /// of the chunk when its rows come grouped, else of the chunk gathered in group order (kept in
        /// <paramref name="owned"/> until the batch is written).
        /// </summary>
        private void Group(DataFrameHandle frame, RecordBatch chunk, int rowsBefore, List<RecordBatch> owned)
        {
            int rows = chunk.Length;
            Prepare(chunk.Schema, keys.Length, chunk.ColumnCount);
            _dataSchema ??= new Schema([.. chunk.Schema.FieldsList.Skip(keys.Length)], null);
            // The data columns (sharing the chunk's arrays: never disposed apart).
            var data = new RecordBatch(_dataSchema, [.. Enumerable.Range(keys.Length, chunk.ColumnCount - keys.Length).Select(chunk.Column)], rows);
            _ids.Reserve(rows);
            _globals.Reserve(rows);
            _firstRows.Reserve(rows);
            int count = _groups!.Assign([.. Enumerable.Range(0, keys.Length).Select(chunk.Column)], _ids.Span, _globals.Span, _firstRows.Span);
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
                int index = _globals[local];
                if (index == _partitions.Count)
                {
                    using DataFrameHandle first = PolarsWrapper.Slice(frame, rowsBefore + _firstRows[local], 1);
                    _partitions.Add(new Partition<TPartition>(describe(first)));
                }
                Partition<TPartition> partition = _partitions[index];
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
        private static void ForEachPartition(List<Partition<TPartition>> partitions, Action<Partition<TPartition>> work)
        {
            foreach (Partition<TPartition> partition in partitions) partition.Error = null;
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
            else if (partitions.Count > 1) Parallel.For(0, partitions.Count, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, Run);
            foreach (Partition<TPartition> partition in partitions)
            {
                if (partition.Error is { } error) ExceptionDispatchInfo.Throw(error);
            }
        }

        /// <summary>The writer schema and options, from the first batch: its fields first .. end.</summary>
        private void Prepare(Schema exported, int first, int end)
        {
            if (_schema is not null) return;
            (_schema, IReadOnlySet<string>? maps) = WriterSchema(new Schema([.. exported.FieldsList.Take(end).Skip(first)], null), dataFields);
            _writerOptions = WriterOptions(options, RowGroupRows, maps) with
            {
                SharedBudget = _threads,
                MaxDegreeOfParallelism = _threads?.Max ?? Environment.ProcessorCount,
            };
            if (_threads is not null) _plan = ParquetWriter.WritePlan.Create(_schema, _writerOptions);
        }

        /// <summary>
        /// Rows <paramref name="start"/> .. +<paramref name="count"/> of a batch into a partition's files: as many as the
        /// open file takes (up to its row limit, and up to its size, a row's bytes on disk as the file shows them so
        /// far; before its first rows, their logical Arrow bytes: mostly no fewer than on disk), a full file closed and
        /// the next one opened. Rows are passed as ranges, never sliced (a slice is an array object per column). One
        /// thread at a time per partition.
        /// </summary>
        private void WriteRows(Partition<TPartition> partition, RecordBatch rows, int start, int count)
        {
            for (int at = start, end = start + count; at < end;)
            {
                File open = partition.Open ??= OpenFile(partition);
                int take = end - at;
                if (_maxRows > 0) take = (int)Math.Min(take, _maxRows - open.Rows);
                if (_target > 0)
                {
                    double perRow = open.PerRow ?? (double)ArrowSizes.LogicalBytes(rows, at, end - at) / (end - at);
                    take = (int)Math.Min(take, Math.Max(1, (long)((_target - open.Bytes(_target)) / perRow)));
                }
                open.Write(rows, at, take); // encoded at once
                at += take;
                // Closed at its row limit, or when not one more row fits.
                if ((_maxRows > 0 && open.Rows >= _maxRows) || (_target > 0 && _target - open.Bytes(_target) < open.PerRow)) Close(partition);
            }
        }

        private File OpenFile(Partition<TPartition> partition)
        {
            string relative;
            lock (_naming) relative = pathFor(partition.Description, partition.NextIndex++);
            return File.Open(fileSystem, relative, _plan ?? ParquetWriter.WritePlan.Create(_schema!, _writerOptions!), _sync);
        }

        /// <summary>
        /// Closes a partition's open file: its last row group and footer written here; its publication (the stream's
        /// commit) goes on without a thread while the next rows are written, waited for by <see cref="HoldMemory"/> (for
        /// memory) and <see cref="Finish"/>.
        /// </summary>
        private void Close(Partition<TPartition> partition)
        {
            File file = partition.Open!;
            partition.Open = null;
            partition.Closed.Add(file.Complete());
            Task commit = file.CommitAsync(_publishers);
            lock (_publishingLock) _publishing.Add((file, commit));
        }

        /// <summary>
        /// Forgets the files published (the first failure among them thrown); returns the bytes those still publishing
        /// hold.
        /// </summary>
        private long Published()
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
        /// Keeps the bytes the open files hold in memory under <see cref="MemoryBudget"/>: the row groups of the largest
        /// files ended early (their pages go to the streams), as many as it takes; then, still beyond (streams holding
        /// their bytes), the largest files closed early. Side by side.
        /// </summary>
        public void HoldMemory()
        {
            List<Partition<TPartition>> open = [.. _partitions.Where(p => p.Open is not null)];
            long publishing = Published();
            long held = open.Sum(p => p.Open!.HeldBytes);
            // Files being published first: their bytes leave memory without making files smaller.
            while (held + publishing > MemoryBudget && WaitForOldest()) publishing = Published();
            if (held <= MemoryBudget) return;
            List<Partition<TPartition>> ending = [];
            foreach (Partition<TPartition> partition in open.OrderByDescending(p => p.Open!.HeldBytes))
            {
                if (held <= MemoryBudget) break;
                ending.Add(partition);
                held -= partition.Open!.HeldBytes - partition.Open.BufferedBytes;
            }
            ForEachPartition(ending, p => p.Open!.EndRowGroup());
            held = open.Sum(p => p.Open!.HeldBytes);
            if (held <= MemoryBudget) return;
            List<Partition<TPartition>> closing = [];
            foreach (Partition<TPartition> partition in open.OrderByDescending(p => p.Open!.HeldBytes))
            {
                if (held <= MemoryBudget) break;
                closing.Add(partition);
                held -= partition.Open!.HeldBytes;
            }
            ForEachPartition(closing, Close);
            _closedForMemory += closing.Count;
        }

        /// <summary>Closes the files still open, side by side; every file written, partition by partition.</summary>
        public SinkResult Finish()
        {
            ForEachPartition([.. _partitions.Where(p => p.Open is not null)], Close);
            while (WaitForOldest()) Published();
            return new SinkResult([.. _partitions.SelectMany(p => p.Closed)], _closedForMemory);
        }

        /// <summary>Frees the keys' and the scratch's native memory.</summary>
        public void Dispose()
        {
            _groups?.Dispose();
            _publishers.Dispose();
            foreach (NativeArray<int> scratch in (NativeArray<int>[])[_ids, _globals, _firstRows, _starts, _order]) scratch.Dispose();
        }

        /// <summary>Gives up every open file (nothing of them stays).</summary>
        public void GiveUp()
        {
            // The files being published land or not before this returns (nothing changes afterwards).
            Task[] publishing;
            lock (_publishingLock) publishing = [.. _publishing.Select(static p => p.Commit)];
            foreach (Task commit in publishing) ((IAsyncResult)commit).AsyncWaitHandle.WaitOne();
            foreach (Partition<TPartition> partition in _partitions)
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
    }

    /// <summary>
    /// An engine batch as Arrow, chunk by chunk as the engine holds it: nothing copied (one Arrow batch for the whole
    /// would concatenate its chunks, and compact a join's gathered strings); each chunk owns its memory.
    /// </summary>
    private static IEnumerable<RecordBatch> Chunks(DataFrameHandle frame)
    {
        using IArrowArrayStream stream = ArrowStreamInterop.ExportToStream(frame);
        while (stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult() is { } chunk) yield return chunk;
    }

    private static ParquetWriteOptions WriterOptions(KernelWriteOptions options, int rowGroupRows, IReadOnlySet<string>? maps) => new()
    {
        Compression = Compression(options.Compression, options.CompressionLevel),
        RowGroupRows = rowGroupRows,
        DataPageBytes = options.DataPageSize > 0 ? options.DataPageSize : new ParquetWriteOptions().DataPageBytes,
        // Whole bounds: the table's statistics are taken from them (and cut to their own rules there).
        StatisticsTruncateLength = null,
        StoreArrowSchema = false,
        MapColumns = maps,
        LeaveOpen = true,
    };

    private static IParquetCompression Compression(PlParquetCompression codec, int level) => codec switch
    {
        PlParquetCompression.Uncompressed => NoCompression.Instance,
        PlParquetCompression.Gzip => level < 0 ? new GzipCompression() : new GzipCompression(level),
        PlParquetCompression.Brotli => level < 0 ? new BrotliCompression() : new BrotliCompression(level),
        PlParquetCompression.ZSTD => level < 0 ? new ZstandardCompression() : new ZstandardCompression(level),
        PlParquetCompression.Lz4Raw => Lz4RawCompression.Instance,
        _ => SnappyCompression.Instance,
    };

    /// <summary>
    /// The schema the files are written with: the engine's, the table's field ids on the data columns' struct fields
    /// at any depth (as Spark writes them), and the paths of the map columns (lists of key/value structs to the
    /// engine) to be written as Parquet maps.
    /// </summary>
    internal static (Schema Schema, IReadOnlySet<string>? Maps) WriterSchema(Schema exported, StructField[] dataFields)
    {
        HashSet<string> maps = [];
        List<Field> fields = [.. exported.FieldsList];
        for (int f = 0; f < dataFields.Length && f < fields.Count; f++)
            fields[f] = With(fields[f], dataFields[f].Type, dataFields[f].FieldId, fields[f].Name);
        return (new Schema(fields, null), maps.Count == 0 ? null : maps);

        // The field with its id, its nested fields with theirs, map paths noted (named as the writer names columns).
        Field With(Field field, DeltaType type, int? id, string where)
        {
            ArrowTypes.IArrowType arrow = (type, field.DataType) switch
            {
                (DeltaStruct s, ArrowTypes.StructType st) => new ArrowTypes.StructType(
                    [.. st.Fields.Select((child, i) => i < s.Fields.Count ? With(child, s.Fields[i].Type, s.Fields[i].FieldId, $"{where}.{child.Name}") : child)]),
                (DeltaArray a, ArrowTypes.ListType l) => new ArrowTypes.ListType(With(l.ValueField, a.ElementType, null, where + "[]")),
                (DeltaArray a, ArrowTypes.LargeListType l) => new ArrowTypes.LargeListType(With(l.ValueField, a.ElementType, null, where + "[]")),
                (DeltaMap m, ArrowTypes.ListType { ValueDataType: ArrowTypes.StructType { Fields.Count: 2 } entry } l) =>
                    new ArrowTypes.ListType(new Field(l.ValueField.Name, Entries(entry, m, where), l.ValueField.IsNullable)),
                (DeltaMap m, ArrowTypes.LargeListType { ValueDataType: ArrowTypes.StructType { Fields.Count: 2 } entry } l) =>
                    new ArrowTypes.LargeListType(new Field(l.ValueField.Name, Entries(entry, m, where), l.ValueField.IsNullable)),
                _ => field.DataType,
            };
            return ArrowFieldIds.With(new Field(field.Name, arrow, field.IsNullable, field.HasMetadata ? field.Metadata : null), id);
        }

        ArrowTypes.StructType Entries(ArrowTypes.StructType entry, DeltaMap map, string where)
        {
            maps.Add(where);
            return new ArrowTypes.StructType(
            [
                With(entry.Fields[0], map.KeyType, null, where + ".key"),
                With(entry.Fields[1], map.ValueType, null, where + ".value"),
            ]);
        }
    }

    /// <summary>A file being written: its stream, writer and rows so far.</summary>
    private sealed class File(string relative, FileWriteStream stream, ParquetWriter writer)
    {
        // The last exact measure of the file's size (it waits for the pages being compressed) and its rows then.
        private long _measuredBytes, _measuredRows;

        public long Rows { get; private set; }

        /// <summary>The bytes of one of its rows, as last measured; null before the first measure.</summary>
        public double? PerRow => _measuredRows > 0 ? (double)_measuredBytes / _measuredRows : null;

        /// <summary>
        /// What the file takes so far on disk (its pages, the open ones as plainly encoded): measured when an estimate
        /// from the last measure comes near <paramref name="target"/> (or there is none), else that estimate, so that
        /// the pages being compressed are waited for only a few times per file.
        /// </summary>
        public long Bytes(long target)
        {
            if (PerRow is double perRow)
            {
                long estimate = _measuredBytes + (long)((Rows - _measuredRows) * perRow);
                if (estimate < target * 9 / 10) return estimate;
            }
            _measuredBytes = writer.BytesWritten + writer.PendingBytes;
            _measuredRows = Rows;
            return _measuredBytes;
        }

        public static File Open(IWritableFileSystem fileSystem, string relative, ParquetWriter.WritePlan plan, bool sync)
        {
            FileWriteStream stream = fileSystem.OpenWrite(relative, createOnly: true);
            stream.SyncOnCommit = sync;
            try
            {
                return new File(relative, stream, ParquetWriter.Create(stream, plan));
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }

        /// <summary>
        /// What the file holds in memory (at least): its pages not written yet, its column writers' scratch and what its
        /// stream buffers.
        /// </summary>
        public long HeldBytes => writer.PendingBytesBound + writer.ScratchBytes + stream.BufferedBytes;

        /// <summary>What its stream buffers.</summary>
        public long BufferedBytes => stream.BufferedBytes;

        /// <summary>
        /// Ends the open row group (its pages, and the row groups before it, go to the stream) and returns the column
        /// writers' scratch to the pool.
        /// </summary>
        public void EndRowGroup()
        {
            writer.EndRowGroup();
            writer.TrimScratch();
        }

        /// <summary>Encodes rows of a batch (the caller keeps it, and may let it go once this returns).</summary>
        public void Write(RecordBatch batch, int start, int count)
        {
            writer.Write(batch, start, count);
            Rows += count;
        }

        /// <summary>Completes the file (its last row group and footer go to the stream); <see cref="CommitAsync"/> publishes it.</summary>
        public Written Complete()
        {
            writer.Complete();
            writer.Dispose();
            return new Written(relative, writer.BytesWritten, writer.Metadata);
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
                if (!await stream.CommitAsync().ConfigureAwait(false)) throw new IOException($"The data file {relative} exists already.");
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
            writer.Abort();
            try
            {
                writer.Dispose();
            }
            finally
            {
                stream.Dispose();
            }
        }
    }
}
