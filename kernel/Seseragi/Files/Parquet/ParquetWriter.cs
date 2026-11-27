using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet.Encryption;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;
using Kaminari.Seseragi.Files.Parquet.Writing;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Writes Arrow record batches as a Parquet file: flat and nested columns (structs, lists of every layout, maps,
/// run-end encoded and dictionary-encoded nested values; not unions), each row group
/// buffered in memory until it is full (<see cref="ParquetWriteOptions.RowGroupRows"/>, <see cref="ParquetWriteOptions.RowGroupBytes"/>)
/// and then written column chunk by column chunk; the footer on <see cref="Complete"/>. Values Parquet cannot hold
/// exactly are refused with <see cref="NotSupportedException"/>, never changed; types it cannot say (time zone
/// names, durations, seconds, dictionaries, large and view types) come back through the stored Arrow schema.
/// </summary>
public sealed class ParquetWriter : IAsyncDisposable, IDisposable
{
    private static ReadOnlySpan<byte> Magic => "PAR1"u8;
    private readonly Stream _stream;
    private readonly ParquetWriteOptions _options;
    private readonly SchemaElement[] _elements;
    private readonly ColumnPlan[] _plans;
    private ColumnWriter[] _columns;   // the open row group's column writers (with the caller owning the batches)
    private Shredder?[] _shredders;
    private readonly FooterBuilder _footer = new(); // the row groups laid out, as the footer and page indexes will hold them
    private readonly List<(int Chunk, NativeBufferWriter Bitset)> _bloomFilters = []; // native, freed once written
    private readonly KeyValue[]? _keyValues;
    private readonly FileEncryptor? _encryptor; // Parquet modular encryption: this file's id, keys and AAD
    private readonly string[] _paths;          // the leaves' dot-separated paths (an encrypted file names its columns by them)
    private int _groupsStarted;                // row groups flushed or opened: the next one's ordinal (an encrypted file seals its chunks with it)
    private readonly WorkBudget _budget;
    private readonly PageSettings[] _columnSettings;
    private Task _writing = Task.CompletedTask; // the last row group's bytes, written while the next one is encoded

    // With the writer owning the batches and more than one thread, row groups are encoded side by side, each by a set
    // of column writers of its own, from the batches themselves; laid out and written in order. With the caller owning
    // them and more than one thread, a full row group's set is sealed instead: finished (its last pages compressed, its
    // dictionary page, indexes) by a task while the next row group fills another set; laid out in order the same way.
    private readonly ConcurrentExclusiveSchedulerPair? _columnWork; // the column work of every row group, MaxDegreeOfParallelism at once
    private readonly ConcurrentStack<(ColumnWriter[] Columns, Shredder?[] Shredders)> _spares = new();
    private readonly Queue<(Task<FinishedChunk[]> Encoding, long Rows, long Bytes)> _encoding = new();
    private long _encodingBytes; // the logical Arrow bytes of the row groups being encoded
    private long _laidOutLogicalBytes, _laidOutChunkBytes; // of the row groups encoded from batches and laid out: their compression ratio
    private OpenGroup? _openGroup;             // the open row group, being encoded as its rows come (the writer owning the batches)
    private long _backlog;                     // the logical bytes of the rows handed to encoders and not encoded yet
    private long _lastGroupBytes;              // the logical bytes of the last row group closed (the next one's guess)
    private readonly SemaphoreSlim _progress = new(0); // released by encoders as rows are encoded (the backlog went down)
    private long _openBytes;                   // their batches' logical Arrow bytes
    private long _rowGroupRows, _totalRows, _position;
    private bool _started, _completed, _faulted, _aborted, _disposed;
    private ParquetFooter? _footerHeld; // once completed: the footer, its bytes in native memory (until taken or disposed)
    private byte[]? _footerBytes;       // once disposed with the footer neither taken nor parsed: its Thrift bytes
    private bool _footerTaken;
    private ParquetMetadata? _metadata;

    private ParquetWriter(Stream stream, WritePlan plan)
    {
        ParquetWriteOptions options = plan.Options;
        _stream = stream;
        _options = options;
        Schema = plan.Schema;
        _budget = plan.Budget;
        _elements = plan.Elements;
        _plans = plan.Columns;
        _columnSettings = plan.Settings;
        _keyValues = plan.KeyValues;
        _paths = [.. _plans.Select(p => string.Join('.', p.Path))];
        if (options.Encryption is { } encryption)
        {
            _encryptor = FileEncryptor.Create(encryption, _paths);
            _footer.EnableEncryption();
        }
        (_columns, _shredders) = NewColumns();
        if (options.BatchOwnership == BatchOwnership.Writer && options.MaxDegreeOfParallelism > 1)
        {
            _columnWork = new ConcurrentExclusiveSchedulerPair(TaskScheduler.Default, options.MaxDegreeOfParallelism);
            _spares.Push((_columns, _shredders));
        }
    }

    /// <summary>
    /// What a writer derives from its schema and options before the first row (validated as <see cref="ParquetWriter.Create(System.IO.Stream, Apache.Arrow.Schema, ParquetWriteOptions)"/>
    /// validates them): one plan serves every file a sink writes with that schema and options, which share their
    /// <see cref="ParquetWriteOptions.SharedBudget"/>.
    /// </summary>
    internal sealed class WritePlan
    {
        private WritePlan(Apache.Arrow.Schema schema, ParquetWriteOptions options)
        {
            options.Validate();
            if (options.TimestampUnit == TimeUnit.Second)
                throw new ArgumentException("Parquet stores no timestamps in seconds: TimestampUnit must be milliseconds, microseconds or nanoseconds.", nameof(options));
            if (schema.FieldsList.Count == 0) throw new ArgumentException("A Parquet file needs at least one column.", nameof(schema));
            Schema = schema;
            Options = options;
            var (elements, plans) = WriteSchema.Plan(schema, options);
            var settings = new PageSettings(options.Compression.ParquetCodec, options.Compression.ParquetLevel, options.DataPageVersion,
                options.DataPageBytes, options.DataPageRows, options.Dictionary, options.DictionaryPageBytes, options.Statistics,
                options.StatisticsTruncateLength, options.WriteChecksums, options.PageIndex, options.SizeStatistics,
                Budget: Budget = options.SharedBudget ?? new WorkBudget(options.MaxDegreeOfParallelism));
            Elements = [new SchemaElement { Name = "schema", NumChildren = schema.FieldsList.Count }, .. elements];
            Columns = plans;
            ParquetColumnOptions?[] columns = ColumnSettings.Resolve(plans, options.Columns);
            Settings = [.. columns.Select((column, c) => Chosen(column, string.Join('.', plans[c].Path)))];
            List<KeyValue> keyValues = [];
            if (options.StoreArrowSchema)
                keyValues.Add(new KeyValue(Parquet.Schema.ArrowMapping.ArrowSchemaKey, Convert.ToBase64String(ArrowSchemaMessage(StoredSchema(schema, options.MapColumns)))));
            if (options.Metadata is { } metadata) keyValues.AddRange(metadata.Select(kv => new KeyValue(kv.Key, kv.Value)));
            KeyValues = keyValues.Count > 0 ? [.. keyValues] : null;

            PageSettings Chosen(ParquetColumnOptions? column, string path)
            {
                PageSettings chosen = column is not null
                    ? settings with { Dictionary = column.Dictionary ?? options.Dictionary, Encoding = column.Encoding, Bloom = column.BloomFilter }
                    : settings;
                // The pages of an encrypted column are checksummed once sealed (the checksum covers the module), not before.
                return options.Encryption is { } encryption && FileEncryptor.Encrypts(encryption, path) ? chosen with { Checksums = false } : chosen;
            }
        }

        /// <summary>The plan of files of <paramref name="schema"/> written with <paramref name="options"/>.</summary>
        public static WritePlan Create(Apache.Arrow.Schema schema, ParquetWriteOptions options)
        {
            ArgumentNullException.ThrowIfNull(schema);
            ArgumentNullException.ThrowIfNull(options);
            return new(schema, options);
        }

        public Apache.Arrow.Schema Schema { get; }
        public ParquetWriteOptions Options { get; }
        /// <summary>The options' shared budget, or the plan's own (one file at a time then).</summary>
        public WorkBudget Budget { get; }
        /// <summary>The Parquet schema, its root first.</summary>
        public SchemaElement[] Elements { get; }
        public ColumnPlan[] Columns { get; }
        /// <summary>The page settings of each leaf column.</summary>
        public PageSettings[] Settings { get; }
        public KeyValue[]? KeyValues { get; }
    }

    /// <summary>A set of column writers (and the shredders of nested columns): one row group being encoded.</summary>
    private (ColumnWriter[] Columns, Shredder?[] Shredders) NewColumns()
    {
        ColumnWriter[] columns = [.. _plans.Select((p, c) => p.Leaf.CreateWriter(_columnSettings[c]))];
        Shredder?[] shredders = [.. _plans.Select(p => p.Steps is null ? null : new Shredder(p.Steps))];
        lock (_allColumns)
        {
            _allColumns.AddRange(columns);
            foreach (Shredder? shredder in shredders)
            {
                if (shredder is not null) _allColumns.Add(shredder);
            }
        }
        return (columns, shredders);
    }

    /// <summary>Every column writer and shredder made (their native buffers are freed when the writer is disposed).</summary>
    private readonly List<IDisposable> _allColumns = [];

    /// <summary>Frees the column writers' and shredders' native buffers, once nothing encodes or writes any more.</summary>
    private void FreeColumns()
    {
        lock (_allColumns)
        {
            foreach (IDisposable column in _allColumns) column.Dispose();
            _allColumns.Clear();
        }
        FreeBloomFilters();
        _footer.Dispose();
    }

    /// <summary>Frees the bitsets of the Bloom filters taken from the chunks laid out (written, or the file given up).</summary>
    private void FreeBloomFilters()
    {
        foreach (var (_, bitset) in _bloomFilters) bitset.Dispose();
        _bloomFilters.Clear();
    }

    /// <summary>The Arrow schema the batches must have.</summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>
    /// The bytes of the file so far (the row groups closed, and the footer once completed; with more than one thread,
    /// the last few row groups closed by <see cref="Write(RecordBatch)"/> may still be on their way to the stream until
    /// later ones close, <see cref="EndRowGroup"/> is called or the file completes).
    /// </summary>
    public long BytesWritten => _position;

    /// <summary>
    /// About how many bytes the row groups not in <see cref="BytesWritten"/> yet will take: the open one (its pages so
    /// far, the open ones as plainly encoded) and, with more than one thread, those closed and still being finished
    /// (exactly: waits for them). With <see cref="BatchOwnership.Writer"/> and more than one thread,
    /// also those still being encoded: exactly once encoded, else (and the open one) their batches' logical Arrow bytes scaled by
    /// the ratio the row groups laid out so far compressed to (unscaled before the first). So BytesWritten +
    /// PendingBytes is what the file would take now, which a caller splitting files by size can go by.
    /// </summary>
    public long PendingBytes
    {
        get
        {
            if (_columnWork is null)
            {
                long sealedBytes = 0;
                foreach (var (sealing, _, _) in _encoding) sealedBytes += sealing.GetAwaiter().GetResult().Sum(c => c.CompressedSize);
                return sealedBytes + _columns.Sum(c => c.BufferedBytes);
            }
            double ratio = _laidOutLogicalBytes > 0 ? (double)_laidOutChunkBytes / _laidOutLogicalBytes : 1;
            double pending = _openBytes * ratio;
            foreach (var (encoding, _, bytes) in _encoding)
            {
                pending += encoding.IsCompletedSuccessfully ? encoding.Result.Sum(c => c.CompressedSize) : bytes * ratio;
            }
            return (long)pending;
        }
    }

    /// <summary>
    /// At least what the row groups not in <see cref="BytesWritten"/> yet take, with the caller owning the batches,
    /// known without waiting for pages being compressed (those count at their largest compressed size): for holding
    /// many writers' buffered bytes under a budget.
    /// </summary>
    internal long PendingBytesBound
    {
        get
        {
            long bytes = 0;
            foreach (ColumnWriter column in _columns) bytes += column.BufferedBytesBound;
            foreach (var (sealing, _, bound) in _encoding)
            {
                if (!sealing.IsCompletedSuccessfully)
                {
                    bytes += bound;
                    continue;
                }
                foreach (FinishedChunk chunk in sealing.Result) bytes += chunk.CompressedSize;
            }
            return bytes;
        }
    }

    /// <summary>The native bytes the open row group's column writers keep as scratch (see <see cref="TrimScratch"/>).</summary>
    internal long ScratchBytes
    {
        get
        {
            long bytes = 0;
            foreach (ColumnWriter column in _columns) bytes += column.ScratchBytes;
            return bytes;
        }
    }

    /// <summary>
    /// Returns the column writers' scratch memory to the pool, between row groups (after <see cref="EndRowGroup"/>):
    /// for a writer that stays open long with little to write (one per partition), at the cost of growing it again.
    /// </summary>
    /// <exception cref="InvalidOperationException">A row group is open.</exception>
    internal void TrimScratch()
    {
        if (_rowGroupRows != 0) throw new InvalidOperationException("A row group is open.");
        foreach (ColumnWriter column in _columns) column.Trim();
    }

    /// <summary>
    /// The rows of a batch the columns take at once, in parallel; the row group's size is checked after each segment
    /// (segments depend on the rows alone, never on the threads, so neither do the file's bytes).
    /// </summary>
    private const int SegmentRows = 16_384;

    /// <summary>Whether the open row group reached RowGroupBytes: decided on the pages' exact sizes, waited for only near the limit.</summary>
    private bool RowGroupFull()
    {
        // Loops, not LINQ: called after every write (a sink's partitions write a few rows at a time).
        long bound = 0, buffered = 0;
        foreach (ColumnWriter column in _columns) bound += column.BufferedBytesBound;
        if (bound < _options.RowGroupBytes) return false;
        foreach (ColumnWriter column in _columns) buffered += column.BufferedBytes;
        return buffered >= _options.RowGroupBytes;
    }

    /// <summary>
    /// Runs <paramref name="work"/> for every column, in parallel when the budget allows and the work is worth it; the
    /// first column's exception (in column order, as one thread would meet it) is rethrown.
    /// </summary>
    private void ForEachColumn(Action<int> work, bool parallel)
    {
        if (!Serial(parallel))
        {
            ForEachColumnSideBySide(work);
            return;
        }
        _budget.Enter();
        try
        {
            for (int c = 0; c < _columns.Length; c++) work(c);
        }
        finally
        {
            _budget.Exit();
        }
    }

    /// <summary>Whether work on every column runs on this thread (not worth it in parallel, or no budget for it).</summary>
    private bool Serial(bool parallel) => !parallel || _budget.Max == 1 || _columns.Length == 1;

    /// <summary>
    /// <see cref="ForEachColumn"/>'s parallel half, a method of its own: its closure is allocated only when it runs (the
    /// compiler allocates a method's closure on entry).
    /// </summary>
    private void ForEachColumnSideBySide(Action<int> work)
    {
        var errors = new Exception?[_columns.Length];
        Parallel.For(0, _columns.Length, new ParallelOptions { MaxDegreeOfParallelism = _budget.Max }, c =>
        {
            _budget.Enter();
            try
            {
                work(c);
            }
            catch (Exception e)
            {
                errors[c] = e;
            }
            finally
            {
                _budget.Exit();
            }
        });
        if (errors.FirstOrDefault(e => e is not null) is { } first) ExceptionDispatchInfo.Throw(first);
    }

    /// <summary>The footer written, once the file is complete.</summary>
    /// <exception cref="InvalidOperationException">The file is not complete yet.</exception>
    public ParquetMetadata Metadata => _metadata ??= _footerHeld is { } held ? held.Metadata
        : _footerBytes is { } footer ? ParquetMetadata.FromThrift(FileMetaData.Parse(footer))
        : _footerTaken ? throw new InvalidOperationException("The footer of the Parquet file was handed over.")
        : throw new InvalidOperationException("The Parquet file is not complete yet: its footer is written by Complete.");

    /// <summary>
    /// Hands the footer over once the file is complete: its Thrift bytes in native memory, the taker's to dispose (a
    /// sink reads its files' statistics from them without parsing).
    /// </summary>
    internal ParquetFooter TakeFooter()
    {
        ParquetFooter footer = _footerHeld ?? throw new InvalidOperationException("The Parquet file is not complete, or its footer was handed over.");
        _footerHeld = null;
        _footerTaken = true;
        return footer;
    }

    /// <summary>Creates a writer over a stream. Nothing is written before the first row group closes (or the file completes).</summary>
    public static ParquetWriter Create(Stream stream, Apache.Arrow.Schema schema, ParquetWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(schema);
        if (!stream.CanWrite) throw new ArgumentException("The stream is not writable.", nameof(stream));
        return new(stream, WritePlan.Create(schema, options ?? new()));
    }

    /// <summary>
    /// Creates a writer into a buffer writer: an array or native buffer writer, a pipe (flushed once a mebibyte is written and on completion, so writing waits while its reader is behind; never completed here). Nothing is written before the first row group closes (or the
    /// file completes).
    /// </summary>
    public static ParquetWriter Create(IBufferWriter<byte> output, Apache.Arrow.Schema schema, ParquetWriteOptions? options = null) =>
        Create(BufferWriterStream.Over(output), schema, options);

    /// <summary>
    /// Creates a writer over a stream from a plan made beforehand (<see cref="WritePlan.Create"/>): the files a sink
    /// writes side by side share one.
    /// </summary>
    internal static ParquetWriter Create(Stream stream, WritePlan plan)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(plan);
        if (!stream.CanWrite) throw new ArgumentException("The stream is not writable.", nameof(stream));
        return new(stream, plan);
    }

    /// <summary>Creates (or overwrites) a file.</summary>
    public static ParquetWriter Create(string path, Apache.Arrow.Schema schema, ParquetWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(schema);
        var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 1, FileOptions.Asynchronous);
        try
        {
            return new(stream, WritePlan.Create(schema, (options ?? new()) with { LeaveOpen = false }));
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    /// <summary>Writes all batches to a stream and completes the file. If writing fails, the stream holds a file without its footer.</summary>
    public static void Write(Stream stream, Apache.Arrow.Schema schema, IEnumerable<RecordBatch> batches, ParquetWriteOptions? options = null)
    {
        using var writer = Create(stream, schema, options);
        writer.WriteAll(batches);
    }

    /// <summary>Writes all batches to a file and completes it. If writing fails, the partial file is deleted.</summary>
    public static void Write(string path, Apache.Arrow.Schema schema, IEnumerable<RecordBatch> batches, ParquetWriteOptions? options = null)
    {
        var writer = Create(path, schema, options);
        try
        {
            writer.WriteAll(batches);
        }
        catch
        {
            writer.Dispose();
            File.Delete(path);
            throw;
        }
        writer.Dispose();
    }

    /// <inheritdoc cref="Write(Stream, Apache.Arrow.Schema, IEnumerable{RecordBatch}, ParquetWriteOptions?)"/>
    public static async Task WriteAsync(Stream stream, Apache.Arrow.Schema schema, IAsyncEnumerable<RecordBatch> batches,
        ParquetWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        await using var writer = Create(stream, schema, options);
        await writer.WriteAllAsync(batches, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc cref="Write(string, Apache.Arrow.Schema, IEnumerable{RecordBatch}, ParquetWriteOptions?)"/>
    public static async Task WriteAsync(string path, Apache.Arrow.Schema schema, IAsyncEnumerable<RecordBatch> batches,
        ParquetWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        var writer = Create(path, schema, options);
        try
        {
            await writer.WriteAllAsync(batches, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await writer.DisposeAsync().ConfigureAwait(false);
            File.Delete(path);
            throw;
        }
        await writer.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Writes rows of <typeparamref name="T"/> to a file and completes it (<see cref="ParquetWriter{T}"/>). If writing
    /// fails, the partial file is deleted.
    /// </summary>
    public static void WriteRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string path, IEnumerable<T> rows,
        ParquetWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(rows);
        var writer = ParquetWriter<T>.Create(path, options);
        try
        {
            writer.WriteRows(rows);
            writer.Complete();
        }
        catch
        {
            writer.Abort();
            writer.Dispose();
            File.Delete(path);
            throw;
        }
        writer.Dispose();
    }

    /// <summary>
    /// Writes rows of <typeparamref name="T"/> to a stream and completes the file (<see cref="ParquetWriter{T}"/>). If
    /// writing fails, the stream holds a file without its footer.
    /// </summary>
    public static void WriteRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(Stream stream, IEnumerable<T> rows,
        ParquetWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(rows);
        using var writer = ParquetWriter<T>.Create(stream, options);
        try
        {
            writer.WriteRows(rows);
            writer.Complete();
        }
        catch
        {
            writer.Abort();
            throw;
        }
    }

    /// <inheritdoc cref="WriteRows{T}(string, IEnumerable{T}, ParquetWriteOptions?)"/>
    public static async Task WriteRowsAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string path, IAsyncEnumerable<T> rows,
        ParquetWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        var writer = ParquetWriter<T>.Create(path, options);
        try
        {
            await writer.WriteRowsAsync(rows, cancellationToken).ConfigureAwait(false);
            await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            writer.Abort();
            await writer.DisposeAsync().ConfigureAwait(false);
            File.Delete(path);
            throw;
        }
        await writer.DisposeAsync().ConfigureAwait(false);
    }

    /// <inheritdoc cref="WriteRows{T}(Stream, IEnumerable{T}, ParquetWriteOptions?)"/>
    public static async Task WriteRowsAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(Stream stream, IAsyncEnumerable<T> rows,
        ParquetWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        await using var writer = ParquetWriter<T>.Create(stream, options);
        try
        {
            await writer.WriteRowsAsync(rows, cancellationToken).ConfigureAwait(false);
            await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            writer.Abort();
            throw;
        }
    }

    /// <summary>Writes all batches to a buffer writer and completes the file. If writing fails, the output holds a file without its footer.</summary>
    public static void Write(IBufferWriter<byte> output, Apache.Arrow.Schema schema, IEnumerable<RecordBatch> batches, ParquetWriteOptions? options = null) =>
        Write(BufferWriterStream.Over(output), schema, batches, options);

    /// <inheritdoc cref="Write(IBufferWriter{byte}, Apache.Arrow.Schema, IEnumerable{RecordBatch}, ParquetWriteOptions?)"/>
    public static Task WriteAsync(IBufferWriter<byte> output, Apache.Arrow.Schema schema, IAsyncEnumerable<RecordBatch> batches,
        ParquetWriteOptions? options = null, CancellationToken cancellationToken = default) =>
        WriteAsync(BufferWriterStream.Over(output), schema, batches, options, cancellationToken);

    /// <summary>
    /// Writes rows of <typeparamref name="T"/> to a buffer writer and completes the file (<see cref="ParquetWriter{T}"/>).
    /// If writing fails, the output holds a file without its footer.
    /// </summary>
    public static void WriteRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(IBufferWriter<byte> output, IEnumerable<T> rows,
        ParquetWriteOptions? options = null) =>
        WriteRows(BufferWriterStream.Over(output), rows, options);

    /// <inheritdoc cref="WriteRows{T}(IBufferWriter{byte}, IEnumerable{T}, ParquetWriteOptions?)"/>
    public static Task WriteRowsAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(IBufferWriter<byte> output, IAsyncEnumerable<T> rows,
        ParquetWriteOptions? options = null, CancellationToken cancellationToken = default) =>
        WriteRowsAsync(BufferWriterStream.Over(output), rows, options, cancellationToken);

    private void WriteAll(IEnumerable<RecordBatch> batches)
    {
        try
        {
            foreach (RecordBatch batch in batches) Write(batch);
        }
        catch
        {
            Abort();
            throw;
        }
        Complete();
    }

    private async Task WriteAllAsync(IAsyncEnumerable<RecordBatch> batches, CancellationToken ct)
    {
        try
        {
            await foreach (RecordBatch batch in batches.WithCancellation(ct).ConfigureAwait(false))
                await WriteAsync(batch, ct).ConfigureAwait(false);
        }
        catch
        {
            Abort();
            throw;
        }
        await CompleteAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Adds a batch's rows. By default the batch is not kept: it may be disposed once this returns. With
    /// <see cref="BatchOwnership.Writer"/> the writer takes it (in every case, also when this fails) and disposes
    /// it once its rows are encoded, perhaps after this returns.
    /// </summary>
    public void Write(RecordBatch batch) => WriteCoreAsync(batch, false, default).AsTask().GetAwaiter().GetResult();

    /// <summary>
    /// Writes rows <paramref name="start"/> .. +<paramref name="count"/> of a batch, as <see cref="Write(RecordBatch)"/>
    /// writes a slice of it, without slicing (a slice is an array object per column: a sink cutting batches into runs
    /// of many partitions made them by the hundred thousand). The caller owns the batch.
    /// </summary>
    internal void Write(RecordBatch batch, int start, int count)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (_options.BatchOwnership != BatchOwnership.Caller)
            throw new InvalidOperationException("Rows of a batch are written only when the caller owns the batches.");
        ArgumentOutOfRangeException.ThrowIfNegative(start);
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, batch.Length - start);
        WriteNowAsync(batch, start, count, false, default).AsTask().GetAwaiter().GetResult();
    }

    /// <inheritdoc cref="Write(RecordBatch)"/>
    public ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellationToken = default) => WriteCoreAsync(batch, true, cancellationToken);

    /// <summary>
    /// Closes the open row group now (nothing happens when it holds no rows); its bytes, and those of the row groups
    /// before it, are written to the stream before this returns (a failure to write them surfaces here).
    /// </summary>
    public void EndRowGroup() => EndRowGroupCoreAsync(false, default).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc cref="EndRowGroup"/>
    public ValueTask EndRowGroupAsync(CancellationToken cancellationToken = default) => EndRowGroupCoreAsync(true, cancellationToken);

    /// <summary>Writes the last row group and the footer, and flushes the stream.</summary>
    public void Complete() => CompleteCoreAsync(false, default).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc cref="Complete"/>
    public ValueTask CompleteAsync(CancellationToken cancellationToken = default) => CompleteCoreAsync(true, cancellationToken);

    /// <summary>
    /// Gives the file up: <see cref="Dispose"/> then frees the writer without completing it (no last row group, no
    /// footer: the stream keeps what was written so far, and is closed unless <see cref="ParquetWriteOptions.LeaveOpen"/>).
    /// For a failure outside the writer, which it cannot see:
    /// <c>try { …; writer.Complete(); } catch { writer.Abort(); throw; }</c>. Writing or completing afterwards throws.
    /// Does no I/O and waits for nothing; repeating it, or calling it on a completed or disposed writer, does nothing.
    /// </summary>
    public void Abort()
    {
        if (!_completed && !_disposed) _aborted = true;
    }

    /// <summary>Whether rows can still be written (not completed, aborted, failed or disposed).</summary>
    internal bool IsWritable => !_completed && !_aborted && !_faulted && !_disposed;

    /// <summary>Whether the writer owns the batches given to it (<see cref="BatchOwnership.Writer"/>).</summary>
    internal bool Owns => _options.BatchOwnership == BatchOwnership.Writer;

    private void CheckWritable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed) throw new InvalidOperationException("The Parquet writer has been completed.");
        if (_aborted) throw new InvalidOperationException("The Parquet writer was aborted; the file cannot be completed.");
        if (_faulted) throw new InvalidOperationException("The Parquet writer failed earlier; the file cannot be completed.");
    }

    private async ValueTask WriteCoreAsync(RecordBatch batch, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (_options.BatchOwnership == BatchOwnership.Caller)
        {
            await WriteNowAsync(batch, 0, batch.Length, useAsync, ct).ConfigureAwait(false);
            return;
        }
        var taken = new Taken(batch);
        try
        {
            // On one thread encoded at once, row groups cut as on several (by the rows' logical bytes).
            if (_columnWork is null) await WriteNowAsync(batch, 0, batch.Length, useAsync, ct, logical: true).ConfigureAwait(false);
            else await WriteTakenAsync(taken, useAsync, ct).ConfigureAwait(false);
        }
        finally
        {
            taken.Done(); // this call's hold on it
        }
    }

    /// <summary>
    /// Encodes rows <paramref name="start"/> .. +<paramref name="count"/> of a batch before returning, segment by
    /// segment, each segment's columns side by side. A row group is full by its encoded pages' bytes, or with
    /// <paramref name="logical"/> (a batch the writer owns, on one thread) by its rows' logical Arrow bytes, counted
    /// as <see cref="WriteTakenAsync"/> counts them: the file is the same whatever the threads.
    /// </summary>
    private async ValueTask WriteNowAsync(RecordBatch batch, int start, int count, bool useAsync, CancellationToken ct, bool logical = false)
    {
        CheckWritable();
        CheckBatch(batch, start, count);
        try
        {
            long bytes = logical ? ArrowSizes.LogicalBytes(batch) : 0;
            int offset = start, end = start + count;
            while (offset < end)
            {
                int take = (int)Math.Min(end - offset, Math.Min(_options.RowGroupRows - _rowGroupRows, SegmentRows));
                WriteColumns(batch, offset, take);
                _rowGroupRows += take;
                offset += take;
                if (logical) _openBytes += bytes * take / batch.Length;
                if (_rowGroupRows >= _options.RowGroupRows || (logical ? _openBytes >= _options.RowGroupBytes : RowGroupFull()))
                    await FlushRowGroupAsync(useAsync, ct).ConfigureAwait(false);
            }
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    /// <summary>
    /// Adds rows of a batch to every column's writer, side by side when the rows are many; a few rows (a sink's
    /// partitions write some at a time) on this thread without a closure or delegate.
    /// </summary>
    private void WriteColumns(RecordBatch batch, int at, int take)
    {
        if (!Serial((long)take * _columns.Length >= 4096))
        {
            WriteColumnsSideBySide(batch, at, take);
            return;
        }
        _budget.Enter();
        try
        {
            for (int c = 0; c < _columns.Length; c++) WriteColumn(_columns[c], _shredders[c], batch.Column(_plans[c].Field), at, take);
        }
        finally
        {
            _budget.Exit();
        }
    }

    /// <summary><see cref="WriteColumns"/>'s parallel half, apart: the closure over its arguments is allocated only here.</summary>
    private void WriteColumnsSideBySide(RecordBatch batch, int at, int take) =>
        ForEachColumnSideBySide(c => WriteColumn(_columns[c], _shredders[c], batch.Column(_plans[c].Field), at, take));

    /// <summary>Adds rows of a column to its writer; nested columns are shredded in steps, keeping the level buffers small.</summary>
    private static void WriteColumn(ColumnWriter column, Shredder? shredder, IArrowArray array, int at, int take)
    {
        if (shredder is null)
        {
            column.Write(array, at, take);
            return;
        }
        for (int step = 0; step < take; step += 1024)
        {
            int n = Math.Min(1024, take - step);
            shredder.Shred(array, at + step, n);
            column.Write(shredder.Def, shredder.Rep, shredder.Leaf, shredder.Indices);
        }
    }

    /// <summary>A batch the writer owns: disposed once nothing reads it (the Write call, and each row group encoding some of its rows).</summary>
    private sealed class Taken(RecordBatch batch)
    {
        private int _readers = 1;

        public RecordBatch Batch => batch;

        public void Read() => Interlocked.Increment(ref _readers);

        public void Done()
        {
            if (Interlocked.Decrement(ref _readers) == 0) batch.Dispose();
        }
    }

    /// <summary>
    /// Rows of a batch in a row group (cut as <see cref="WriteNowAsync"/> cuts them: the file's bytes are the same), and
    /// their share of the batch's logical Arrow bytes.
    /// </summary>
    private readonly record struct Segment(Taken Batch, int Offset, int Rows, long Bytes);

    /// <summary>
    /// The open row group: its set of column writers, encoding its segments as they come (<see cref="EncodeAsync"/>),
    /// each segment's batch let go once encoded.
    /// </summary>
    private sealed class OpenGroup((ColumnWriter[] Columns, Shredder?[] Shredders) set, int ordinal)
    {
        public (ColumnWriter[] Columns, Shredder?[] Shredders) Set { get; } = set;

        /// <summary>The row group's ordinal in the file: row groups are laid out in the order they open.</summary>
        public int Ordinal { get; } = ordinal;

        public Channel<Segment> Segments { get; } = Channel.CreateUnbounded<Segment>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });

        public Task<FinishedChunk[]> Encoding { get; set; } = null!;

        /// <summary>Set when the file is given up: the segments still to come are let go, not encoded.</summary>
        public volatile bool Abandoned;
    }

    /// <summary>The row groups encoded at once at most, the open one included (each holding its column writers' pages).</summary>
    internal int MaxRowGroupsInFlight => Math.Max(2, _options.MaxDegreeOfParallelism / 2);

    /// <summary>
    /// The row groups sealed and not laid out yet, at most, with the caller owning the batches (each holding its
    /// compressed pages and a set of column writers; finishing one takes far less than filling the next).
    /// </summary>
    internal const int MaxSealedRowGroups = 1;

    /// <summary>The most row groups that were being encoded at once (for tests).</summary>
    internal int PeakRowGroupsInFlight { get; private set; }

    /// <summary>The most logical bytes handed to encoders and not encoded yet at once (for tests).</summary>
    internal long PeakBacklogBytes { get; private set; }

    /// <summary>
    /// Adds a batch the writer owns to the open row group, segment by segment: each goes to the group's encoder at once
    /// (once the rows waiting for encoders stay within <see cref="ParquetWriteOptions.InFlightBytes"/>); a row group full
    /// (by rows, or by its rows' logical Arrow bytes: known before encoding) is closed, the next one opens.
    /// </summary>
    private async ValueTask WriteTakenAsync(Taken taken, bool useAsync, CancellationToken ct)
    {
        RecordBatch batch = taken.Batch;
        CheckWritable();
        CheckBatch(batch, 0, batch.Length);
        try
        {
            long bytes = ArrowSizes.LogicalBytes(batch);
            int offset = 0;
            while (offset < batch.Length)
            {
                int take = (int)Math.Min(batch.Length - offset, Math.Min(_options.RowGroupRows - _rowGroupRows, SegmentRows));
                long share = bytes * take / batch.Length;
                OpenGroup group = _openGroup ?? await OpenRowGroupAsync(useAsync, ct).ConfigureAwait(false);
                // The rows waiting for encoders held within their bytes (one segment at least goes).
                while (Volatile.Read(ref _backlog) > 0 && Volatile.Read(ref _backlog) + share > _options.InFlightBytes)
                {
                    if (useAsync) await _progress.WaitAsync(ct).ConfigureAwait(false);
                    else _progress.Wait(ct);
                }
                taken.Read();
                PeakBacklogBytes = Math.Max(PeakBacklogBytes, Interlocked.Add(ref _backlog, share));
                group.Segments.Writer.TryWrite(new Segment(taken, offset, take, share));
                _rowGroupRows += take;
                _openBytes += share;
                offset += take;
                if (_rowGroupRows >= _options.RowGroupRows || _openBytes >= _options.RowGroupBytes) CloseRowGroup();
            }
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    /// <summary>
    /// Opens a row group, once fewer than <see cref="MaxRowGroupsInFlight"/> are encoded with it (the oldest are laid out
    /// and written meanwhile): a set of column writers and its encoder, started.
    /// </summary>
    private async ValueTask<OpenGroup> OpenRowGroupAsync(bool useAsync, CancellationToken ct)
    {
        // As many as their state allows: the row groups closed and still encoding, with the one opening (taken to be as
        // large as the last), within InFlightBytes of their rows' logical bytes, two at least. Measured: without this
        // bound eight row groups encoded at once (memory up, 10M rows of eight columns 505 → 655 ms).
        while (_encoding.Count + 1 > MaxRowGroupsInFlight || _encoding.Count > 1 && _encodingBytes + _lastGroupBytes > _options.InFlightBytes)
            await WriteEncodedAsync(useAsync, ct).ConfigureAwait(false);
        var group = new OpenGroup(_spares.TryPop(out var spare) ? spare : NewColumns(), _groupsStarted++);
        group.Encoding = Task.Run(() => EncodeAsync(group));
        _openGroup = group;
        PeakRowGroupsInFlight = Math.Max(PeakRowGroupsInFlight, _encoding.Count + 1);
        return group;
    }

    /// <summary>Closes the open row group: no more rows come to its encoder, which finishes it; laid out in order later.</summary>
    private void CloseRowGroup()
    {
        if (_openGroup is not { } group) return;
        group.Segments.Writer.TryComplete();
        _encoding.Enqueue((group.Encoding, _rowGroupRows, _openBytes));
        _encodingBytes += _openBytes;
        _lastGroupBytes = _openBytes;
        _openGroup = null;
        _rowGroupRows = 0;
        _openBytes = 0;
    }

    /// <summary>
    /// Encodes a row group's segments as they come, letting each batch go once its rows are encoded, then finishes its
    /// chunks; its column writers are kept for another. After a failure (or once the file is given up) the segments
    /// still to come are let go unencoded, and the failure surfaces when the row group is laid out.
    /// </summary>
    private async Task<FinishedChunk[]> EncodeAsync(OpenGroup group)
    {
        var (columns, shredders) = group.Set;
        var finished = new FinishedChunk[columns.Length];
        bool reusable = false;
        Exception? failure = null;
        ChannelReader<Segment> segments = group.Segments.Reader;
        while (await segments.WaitToReadAsync().ConfigureAwait(false))
        {
            while (segments.TryRead(out Segment segment))
            {
                try
                {
                    if (failure is null && !group.Abandoned)
                    {
                        RecordBatch batch = segment.Batch.Batch;
                        await ForEachColumnAsync(c => WriteColumn(columns[c], shredders[c], batch.Column(_plans[c].Field), segment.Offset, segment.Rows),
                            parallel: (long)segment.Rows * columns.Length >= 4096).ConfigureAwait(false);
                    }
                }
                catch (Exception e)
                {
                    failure = e;
                }
                finally
                {
                    segment.Batch.Done();
                    Interlocked.Add(ref _backlog, -segment.Bytes);
                    if (_progress.CurrentCount == 0) _progress.Release(); // a waiter rechecks the backlog (a spare release is harmless)
                }
            }
        }
        if (failure is not null) ExceptionDispatchInfo.Throw(failure);
        if (group.Abandoned) return [];
        try
        {
            await ForEachColumnAsync(c => finished[c] = Sealed(columns[c].Finish(), c, group.Ordinal), parallel: true).ConfigureAwait(false);
            foreach (ColumnWriter column in columns) column.Reset();
            reusable = true;
            return finished;
        }
        catch
        {
            foreach (FinishedChunk chunk in finished)
            {
                if (chunk.Pages is not null) chunk.Release(); // chunks finished before the failure
            }
            throw;
        }
        finally
        {
            if (reusable) _spares.Push(group.Set);
        }
    }

    /// <summary>
    /// Runs <paramref name="work"/> for every column on the writer's column scheduler (at most MaxDegreeOfParallelism
    /// at once, whatever the row groups); the first column's exception, in column order, is rethrown.
    /// </summary>
    private async Task ForEachColumnAsync(Action<int> work, bool parallel)
    {
        TaskScheduler scheduler = _columnWork!.ConcurrentScheduler;
        int count = _plans.Length;
        if (!parallel || count == 1)
        {
            await Task.Factory.StartNew(() => Counted(0, count), CancellationToken.None, TaskCreationOptions.DenyChildAttach, scheduler).ConfigureAwait(false);
            return;
        }
        var tasks = new Task[count];
        for (int c = 0; c < count; c++)
        {
            int column = c;
            tasks[c] = Task.Factory.StartNew(() => Counted(column, column + 1), CancellationToken.None, TaskCreationOptions.DenyChildAttach, scheduler);
        }
        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch
        {
            ExceptionDispatchInfo.Throw(System.Array.Find(tasks, t => t.IsFaulted)!.Exception!.InnerException!);
        }

        void Counted(int first, int end)
        {
            _budget.Enter();
            try
            {
                for (int c = first; c < end; c++) work(c);
            }
            finally
            {
                _budget.Exit();
            }
        }
    }

    /// <summary>Lays out and writes the oldest row group being encoded, once it is (its failure surfaces here).</summary>
    private async ValueTask WriteEncodedAsync(bool useAsync, CancellationToken ct)
    {
        var (encoding, rows, bytes) = _encoding.Dequeue();
        FinishedChunk[] finished;
        if (_columnWork is null) finished = await encoding.ConfigureAwait(false); // a sealed row group (its bytes a bound)
        else
        {
            _encodingBytes -= bytes;
            finished = await encoding.ConfigureAwait(false);
            _laidOutLogicalBytes += bytes;
            _laidOutChunkBytes += finished.Sum(c => c.CompressedSize);
        }
        await LayOutAsync(finished, rows, useAsync, ct).ConfigureAwait(false);
    }

    /// <summary>Closes the open row group and lays out every row group encoded or being encoded.</summary>
    private async ValueTask EndOpenRowGroupAsync(bool useAsync, CancellationToken ct)
    {
        if (_columnWork is null) await FlushRowGroupAsync(useAsync, ct).ConfigureAwait(false);
        else CloseRowGroup();
        while (_encoding.Count > 0) await WriteEncodedAsync(useAsync, ct).ConfigureAwait(false);
    }

    /// <summary>Lets go of the rows not written (a writer given up): the open row group's batches, the row groups being encoded.</summary>
    private void Abandon()
    {
        if (_openGroup is { } open)
        {
            // Its encoder lets the rows still to come go unencoded, and ends.
            open.Abandoned = true;
            open.Segments.Writer.TryComplete();
            _encoding.Enqueue((open.Encoding, 0, 0));
            _openGroup = null;
        }
        while (_encoding.TryDequeue(out var pending))
        {
            try
            {
                foreach (FinishedChunk chunk in pending.Encoding.GetAwaiter().GetResult()) chunk.Release();
            }
            catch (Exception)
            {
                // Its failure no longer matters: the file is given up.
            }
        }
    }

    /// <summary>
    /// The batch's columns have the writer's types and lengths, and no null where the schema says non-nullable, at
    /// any depth (checked before anything is written).
    /// </summary>
    /// <summary>The batch fits the writer schema, and rows <paramref name="start"/> .. +<paramref name="count"/> its nullability.</summary>
    private void CheckBatch(RecordBatch batch, int start, int count)
    {
        int fields = Schema.FieldsList.Count;
        if (batch.ColumnCount != fields)
            throw new ArgumentException($"The batch has {batch.ColumnCount} columns; the writer schema has {fields}.", nameof(batch));
        for (int f = 0; f < fields; f++)
        {
            Field field = Schema.FieldsList[f];
            IArrowArray column = batch.Column(f);
            if (column.Length != batch.Length)
                throw new ArgumentException($"Column {f} ('{field.Name}') has {column.Length} rows; the batch has {batch.Length}.", nameof(batch));
            if (!ReferenceEquals(batch.Schema, Schema) && !ArrowTypes.SameType(field.DataType, column.Data.DataType))
                throw new ArgumentException($"Column {f} ('{field.Name}') is {column.Data.DataType.Name} in the batch but {field.DataType.Name} in the writer schema.", nameof(batch));
            if (Mismatch(field.DataType, column.Data, field.Name) is { } where)
                throw new ArgumentException($"The arrays of column {f} ('{field.Name}') do not have the types its type declares: {where}.", nameof(batch));
        }
        for (int c = 0; c < _columns.Length; c++)
        {
            IArrowArray column = batch.Column(_plans[c].Field);
            if (_shredders[c] is { } shredder)
            {
                try
                {
                    shredder.Validate(column);
                }
                catch (ArgumentException e)
                {
                    throw new ArgumentException(e.Message, nameof(batch));
                }
            }
            else if (_plans[c].Leaf.Element.Repetition == FieldRepetition.Required && ArrowValues.NullCount(column) > 0)
            {
                for (int i = start; i < start + count; i++)
                {
                    if (ArrowValues.IsNull(column, i))
                        throw new ArgumentException($"Column '{_plans[c].Path[0]}' is not nullable but row {i} of the batch is null.", nameof(batch));
                }
            }
        }
    }

    /// <summary>
    /// Where the arrays under a nested array do not have the types its type declares (Apache.Arrow builds such arrays
    /// without a check), or null.
    /// </summary>
    private static string? Mismatch(IArrowType declared, ArrayData data, string where)
    {
        if (!ArrowTypes.SameType(declared, data.DataType)) return $"'{where}' is {data.DataType.Name}, not {declared.Name}";
        return declared switch
        {
            StructType st when data.Children.Length != st.Fields.Count => $"'{where}' has {data.Children.Length} child arrays for {st.Fields.Count} fields",
            StructType st => FieldsMismatch(st, data, where),
            ListType or LargeListType or FixedSizeListType or ListViewType or LargeListViewType when data.Children.Length != 1 => $"'{where}' has no item array",
            ListType l => Mismatch(l.ValueDataType, data.Children[0], where + "[]"),
            LargeListType l => Mismatch(l.ValueDataType, data.Children[0], where + "[]"),
            FixedSizeListType l => Mismatch(l.ValueDataType, data.Children[0], where + "[]"),
            ListViewType l => Mismatch(l.ValueDataType, data.Children[0], where + "[]"),
            LargeListViewType l => Mismatch(l.ValueDataType, data.Children[0], where + "[]"),
            MapType m when data.Children.Length != 1 => $"'{where}' has no entries array",
            MapType m => Mismatch(m.KeyValueType, data.Children[0], where),
            RunEndEncodedType r when data.Children.Length != 2 => $"'{where}' has no run ends and values arrays",
            RunEndEncodedType r => Mismatch(r.RunEndsDataType, data.Children[0], where + ".run_ends") ?? Mismatch(r.ValuesDataType, data.Children[1], where + ".values"),
            DictionaryType d when data.Dictionary is null => $"'{where}' has no dictionary",
            DictionaryType d => Mismatch(d.ValueType, data.Dictionary, where + ".dictionary"),
            _ => null,
        };
    }

    /// <summary>
    /// The first mismatch among a struct's fields (a loop of its own: a lambda in <see cref="Mismatch"/> would make it
    /// allocate a closure on every call, every type).
    /// </summary>
    private static string? FieldsMismatch(StructType type, ArrayData data, string where)
    {
        for (int k = 0; k < type.Fields.Count; k++)
        {
            Field field = type.Fields[k];
            if (Mismatch(field.DataType, data.Children[k], $"{where}.{field.Name}") is { } mismatch) return mismatch;
        }
        return null;
    }

    private async ValueTask EndRowGroupCoreAsync(bool useAsync, CancellationToken ct)
    {
        CheckWritable();
        try
        {
            await EndOpenRowGroupAsync(useAsync, ct).ConfigureAwait(false);
            await _writing.ConfigureAwait(false); // written behind only when Write closes the group
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    private async ValueTask FlushRowGroupAsync(bool useAsync, CancellationToken ct)
    {
        if (_rowGroupRows == 0) return;
        long rows = _rowGroupRows;
        int ordinal = _groupsStarted++; // laid out in the order flushed
        _openBytes = 0; // the rows' logical bytes, counted when the writer owns the batches
        // Writers sharing a budget (one per partition, many open) do not seal: a second set of column writers each would
        // double what they hold.
        if (_budget.Max > 1 && _options.SharedBudget is null)
        {
            // Sealed: its column writers finish it on a task (the oldest sealed laid out first when too many are),
            // the next row group fills a spare set.
            while (_encoding.Count >= MaxSealedRowGroups) await WriteEncodedAsync(useAsync, ct).ConfigureAwait(false);
            var set = (_columns, _shredders);
            (_columns, _shredders) = _spares.TryPop(out var spare) ? spare : NewColumns();
            _rowGroupRows = 0;
            // Its bytes as far as known without waiting (the pages in flight at their largest), for PendingBytesBound.
            _encoding.Enqueue((Task.Run(() => Finish(set, ordinal, spare: true)), rows, set.Item1.Sum(c => c.BufferedBytesBound)));
            PeakRowGroupsInFlight = Math.Max(PeakRowGroupsInFlight, _encoding.Count);
            return;
        }
        FinishedChunk[] finished = Finish((_columns, _shredders), ordinal, spare: false);
        _rowGroupRows = 0;
        await LayOutAsync(finished, rows, useAsync, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Finishes every chunk of a set of column writers (last pages, dictionary pages, statistics, indexes; an encrypted
    /// file's chunks sealed for row group <paramref name="ordinal"/>) in parallel and readies the set for another row
    /// group (a sealed set goes back to the spares); on a failure the chunks finished are let go and the set is not used again.
    /// </summary>
    private FinishedChunk[] Finish((ColumnWriter[] Columns, Shredder?[] Shredders) set, int ordinal, bool spare)
    {
        var finished = new FinishedChunk[set.Columns.Length];
        try
        {
            ForEachColumn(c => finished[c] = Sealed(set.Columns[c].Finish(), c, ordinal), parallel: true);
            foreach (ColumnWriter column in set.Columns) column.Reset();
        }
        catch
        {
            foreach (FinishedChunk chunk in finished)
            {
                if (chunk.Pages is not null) chunk.Release();
            }
            throw;
        }
        if (spare) _spares.Push(set);
        return finished;
    }

    /// <summary>
    /// Lays a row group's finished chunks out after the row groups before it (in column order) and writes them: after
    /// the previous row group's bytes, by a task when threads are allowed. The chunks are released once written.
    /// </summary>
    private async ValueTask LayOutAsync(FinishedChunk[] finished, long rows, bool useAsync, CancellationToken ct)
    {
        await StartAsync(useAsync, ct).ConfigureAwait(false);
        long groupStart = _position, uncompressed = 0, compressed = 0;
        List<ReadOnlyMemory<byte>> pieces = [];
        int ordinal = _footer.GroupCount;
        for (int c = 0; c < finished.Length; c++)
        {
            FinishedChunk chunk = finished[c];
            long start = _position;
            using ColumnEncryptor? encryptor = _encryptor?.ForChunk(_paths[c], ordinal, c);
            // Sealed in the order the row groups were flushed or opened, which is the order they are laid out in.
            if (encryptor is not null && chunk.SealedGroup != ordinal)
                throw new InvalidOperationException($"An encrypted chunk sealed for row group {chunk.SealedGroup} was laid out as row group {ordinal}.");
            long size = chunk.CompressedSize;
            OffsetIndex? offsetIndex = chunk.OffsetIndex;
            pieces.Add(chunk.DictionaryPage);
            pieces.AddRange(chunk.DataPages);
            _position += size;
            SchemaElement element = _plans[c].Leaf.Element;
            var meta = new ColumnMetaData
            {
                Type = element.Type!.Value,
                Encodings = chunk.Encodings,
                PathInSchema = _plans[c].Path,
                Codec = _options.Compression.ParquetCodec,
                NumValues = chunk.Values,
                TotalUncompressedSize = chunk.UncompressedSize,
                TotalCompressedSize = size,
                DataPageOffset = start + chunk.DictionaryPage.Length,
                DictionaryPageOffset = chunk.DictionaryPage.IsEmpty ? null : start,
                Statistics = chunk.Statistics,
                EncodingStats = chunk.EncodingStats,
                SizeStatistics = chunk.SizeStatistics,
            };
            int placed = _footer.AddChunk(start, meta, chunk.BloomFilter is not null, chunk.ColumnIndex, offsetIndex, meta.DataPageOffset,
                encryptor, _encryptor?.CryptoMetaData(_paths[c], _plans[c].Path), _encryptor?.SeparatesMetadata(_paths[c]) ?? false,
                _encryptor?.PlaintextFooter ?? false);
            if (chunk.BloomFilter is { } bitset) _bloomFilters.Add((placed, bitset));
            uncompressed += chunk.UncompressedSize;
            compressed += size;
        }
        await _writing.ConfigureAwait(false); // one row group on its way at most
        if (_budget.Max == 1)
        {
            try
            {
                await WritePiecesAsync(pieces, useAsync, ct).ConfigureAwait(false);
            }
            finally
            {
                foreach (FinishedChunk chunk in finished) chunk.ReleasePages(); // the Bloom filters are the writer's now
            }
        }
        else
        {
            _writing = Task.Run(async () =>
            {
                try
                {
                    await WritePiecesAsync(pieces, useAsync, ct).ConfigureAwait(false);
                }
                finally
                {
                    foreach (FinishedChunk chunk in finished) chunk.ReleasePages();
                }
            }, CancellationToken.None);
        }
        _footer.AddGroup(uncompressed, rows, groupStart, compressed);
        _totalRows += rows;
    }

    /// <summary>
    /// A finished chunk as an encrypted file stores it (Parquet modular encryption), sealed on its column's thread for
    /// row group <paramref name="rowGroup"/>: each page body becomes a module, then its header (with the module's size,
    /// and its checksum when the page had one) a module before it, all in one native buffer the chunk owns; the plain
    /// pages are let go at once, and the offset index takes the sealed sizes. A chunk written in plain comes back as it is.
    /// </summary>
    private FinishedChunk Sealed(FinishedChunk chunk, int column, int rowGroup)
    {
        if (_encryptor is null) return chunk;
        NativeByteBuffer? buffer = null;
        try
        {
            using ColumnEncryptor? encryptor = _encryptor.ForChunk(_paths[column], rowGroup, column);
            if (encryptor is null) return chunk;
            // Room for every piece's module, with some to spare for a header's checksum: the buffer never grows, so the
            // pieces taken from it stay where they are.
            long bound = chunk.DictionaryPage.Length + 2L * Overhead;
            foreach (ReadOnlyMemory<byte> piece in chunk.DataPages) bound += piece.Length + Overhead;
            if (bound > System.Array.MaxLength) throw new InvalidOperationException("An encrypted column chunk cannot exceed 2 GB.");
            buffer = new NativeByteBuffer((int)bound, pooled: true);
            int at = 0;
            if (!chunk.DictionaryPage.IsEmpty)
            {
                PageHeader header = PageHeader.Parse(chunk.DictionaryPage.Span, out int headerLength);
                var (headerAt, sealedHeader, bodyAt, body) = Seal(header, chunk.DictionaryPage[headerLength..], ParquetModule.DictionaryPageHeader, ParquetModule.DictionaryPage, -1);
                // The dictionary page is one piece, its header first: the body was sealed first (the header holds its
                // checksum), so the header module (a few dozen bytes) is held aside while the body moves behind it.
                Span<byte> span = buffer.Span, held = stackalloc byte[256];
                if (sealedHeader > held.Length) throw new InvalidOperationException($"A dictionary page header of {sealedHeader} bytes was sealed.");
                span.Slice(headerAt, sealedHeader).CopyTo(held);
                span.Slice(bodyAt, body).CopyTo(span[sealedHeader..]);
                held[..sealedHeader].CopyTo(span);
            }
            int dictionary = at;
            OffsetIndex? index = chunk.OffsetIndex;
            PageLocation[]? locations = index is null ? null : new PageLocation[index.PageLocations.Length];
            List<ReadOnlyMemory<byte>> pieces = new(chunk.DataPages.Count);
            for (int i = 0, page = 0; i + 1 < chunk.DataPages.Count; i += 2, page++)
            {
                PageHeader header = PageHeader.Parse(chunk.DataPages[i].Span, out _);
                int offset = at - dictionary;
                var (headerAt, headerLength, bodyAt, bodyLength) = Seal(header, chunk.DataPages[i + 1], ParquetModule.DataPageHeader, ParquetModule.DataPage, page);
                pieces.Add(buffer.Memory.Slice(headerAt, headerLength));
                pieces.Add(buffer.Memory.Slice(bodyAt, bodyLength));
                if (locations is not null) locations[page] = new PageLocation(offset, headerLength + bodyLength, index!.PageLocations[page].FirstRowIndex);
            }
            OffsetIndex? offsets = index is null ? null : new OffsetIndex { PageLocations = locations!, UnencodedByteArrayDataBytes = index.UnencodedByteArrayDataBytes };
            chunk.ReleasePages();
            return chunk with
            {
                DictionaryPage = buffer.Memory[..dictionary], DictionaryOwner = null, DataPages = pieces, DataPagesLength = at - dictionary,
                OffsetIndex = offsets, Pages = [], SealedOwner = buffer, SealedGroup = rowGroup,
            };

            // Seals a body then its header after it, at the end of what is sealed; where each went.
            (int HeaderAt, int HeaderLength, int BodyAt, int BodyLength) Seal(PageHeader header, ReadOnlyMemory<byte> body, ParquetModule headerType, ParquetModule bodyType, int ordinal)
            {
                Span<byte> span = buffer.Span;
                int bodyAt = at, bodyLength = encryptor.ModuleLength(bodyType, body.Length);
                encryptor.Encrypt(bodyType, ordinal, body.Span, span.Slice(bodyAt, bodyLength));
                at += bodyLength;
                var sealedHeader = new PageHeader
                {
                    Type = header.Type,
                    UncompressedPageSize = header.UncompressedPageSize,
                    CompressedPageSize = bodyLength,
                    // The checksum of an encrypted page covers its module, as parquet-mr and Arrow write it (the plain
                    // page was not checksummed).
                    Crc = _options.WriteChecksums ? (int)System.IO.Hashing.Crc32.HashToUInt32(span.Slice(bodyAt, bodyLength)) : null,
                    DataPageHeader = header.DataPageHeader,
                    DictionaryPageHeader = header.DictionaryPageHeader,
                    DataPageHeaderV2 = header.DataPageHeaderV2,
                };
                ThriftCompactWriter thrift = ThriftCompactWriter.ThreadScratch();
                sealedHeader.Write(thrift);
                int headerAt = at, headerLength = encryptor.ModuleLength(headerType, thrift.WrittenSpan.Length);
                encryptor.Encrypt(headerType, ordinal, thrift.WrittenSpan, span.Slice(headerAt, headerLength));
                at += headerLength;
                return (headerAt, headerLength, bodyAt, bodyLength);
            }
        }
        catch
        {
            buffer?.Release();
            chunk.Release();
            throw;
        }
    }

    /// <summary>The bytes a module adds to a piece at most: its length, nonce and tag, and a checksum a header gains.</summary>
    private const int Overhead = 64;

    /// <summary>
    /// Writes the Bloom filters after the row groups (each a header and its bitset, as pyarrow places them: together,
    /// so that a reader fetches them at once) and points the column chunks at them.
    /// </summary>
    private async ValueTask WriteBloomFiltersAsync(bool useAsync, CancellationToken ct)
    {
        var thrift = new ThriftCompactWriter();
        foreach (var (chunk, bitset) in _bloomFilters)
        {
            thrift.Clear();
            // Members by field id: algorithm BLOCK = 1, hash XXHASH = 1, compression UNCOMPRESSED = 1.
            new BloomFilterHeader(bitset.WrittenCount, 1, 1, 1).Write(thrift);
            int column = chunk % _columns.Length;
            ColumnEncryptor? encryptor = _encryptor?.ForChunk(_paths[column], chunk / _columns.Length, column);
            if (encryptor is not null)
            {
                // An encrypted chunk's filter: the header and the bitset, each a module (the bitset's in native memory).
                byte[] header;
                int bitsLength = encryptor.ModuleLength(ParquetModule.BloomFilterBitset, bitset.WrittenCount);
                var bits = new NativeByteBuffer(bitsLength, pooled: true);
                try
                {
                    using (encryptor)
                    {
                        header = encryptor.Encrypt(ParquetModule.BloomFilterHeader, -1, thrift.WrittenSpan);
                        encryptor.Encrypt(ParquetModule.BloomFilterBitset, -1, bitset.WrittenSpan, bits.Span[..bitsLength]);
                    }
                    _footer.PlaceBloomFilter(chunk, _position, header.Length + bitsLength);
                    await WriteBytesAsync(header, useAsync, ct).ConfigureAwait(false);
                    await WriteBytesAsync(bits.Memory[..bitsLength], useAsync, ct).ConfigureAwait(false);
                }
                finally
                {
                    bits.Release();
                }
                continue;
            }
            _footer.PlaceBloomFilter(chunk, _position, thrift.WrittenSpan.Length + bitset.WrittenCount);
            await WriteBytesAsync(thrift.WrittenMemory, useAsync, ct).ConfigureAwait(false); // written before the next Clear
            await WriteBytesAsync(bitset.WrittenMemory, useAsync, ct).ConfigureAwait(false);
        }
        FreeBloomFilters();
    }

    /// <summary>
    /// Writes the page indexes after the row groups (every column index, then every offset index, as parquet-mr and
    /// Arrow place them) and points the column chunks at them.
    /// </summary>
    private async ValueTask WritePageIndexesAsync(bool useAsync, CancellationToken ct)
    {
        await WriteBytesAsync(_footer.PlaceColumnIndexes(_position), useAsync, ct).ConfigureAwait(false);
        await WriteBytesAsync(_footer.PlaceOffsetIndexes(_position), useAsync, ct).ConfigureAwait(false);
    }

    private async ValueTask StartAsync(bool useAsync, CancellationToken ct)
    {
        if (_started) return;
        _started = true;
        // An encrypted footer: PARE at both ends.
        await WriteBytesAsync(_encryptor is { PlaintextFooter: false } ? "PARE"u8.ToArray() : Magic.ToArray(), useAsync, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes page headers and bodies (laid out beforehand: <see cref="_position"/> already counts them), small pieces
    /// gathered into one write, large ones as they are.
    /// </summary>
    private async Task WritePiecesAsync(IReadOnlyList<ReadOnlyMemory<byte>> pieces, bool useAsync, CancellationToken ct)
    {
        const int Gather = 1 << 16;
        var staging = new NativeByteBuffer(Gather, pooled: true);
        try
        {
            int staged = 0;
            foreach (ReadOnlyMemory<byte> piece in pieces)
            {
                if (staged + piece.Length > Gather && staged > 0)
                {
                    await RawWriteAsync(staging.Memory[..staged], useAsync, ct).ConfigureAwait(false);
                    staged = 0;
                }
                if (piece.Length >= Gather) await RawWriteAsync(piece, useAsync, ct).ConfigureAwait(false);
                else
                {
                    piece.Span.CopyTo(staging.Span[staged..]);
                    staged += piece.Length;
                }
            }
            if (staged > 0) await RawWriteAsync(staging.Memory[..staged], useAsync, ct).ConfigureAwait(false);
        }
        finally
        {
            staging.Release();
        }
    }

    private async ValueTask WriteBytesAsync(ReadOnlyMemory<byte> bytes, bool useAsync, CancellationToken ct)
    {
        await RawWriteAsync(bytes, useAsync, ct).ConfigureAwait(false);
        _position += bytes.Length;
    }

    private async ValueTask RawWriteAsync(ReadOnlyMemory<byte> bytes, bool useAsync, CancellationToken ct)
    {
        if (bytes.IsEmpty) return;
        if (useAsync) await _stream.WriteAsync(bytes, ct).ConfigureAwait(false);
        else _stream.Write(bytes.Span);
    }

    private async ValueTask CompleteCoreAsync(bool useAsync, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed) return;
        if (_aborted) throw new InvalidOperationException("The Parquet writer was aborted; the file cannot be completed.");
        if (_faulted) throw new InvalidOperationException("The Parquet writer failed earlier; the file cannot be completed.");
        try
        {
            await EndOpenRowGroupAsync(useAsync, ct).ConfigureAwait(false);
            await StartAsync(useAsync, ct).ConfigureAwait(false);
            await _writing.ConfigureAwait(false);
            await WriteBloomFiltersAsync(useAsync, ct).ConfigureAwait(false);
            await WritePageIndexesAsync(useAsync, ct).ConfigureAwait(false);
            // The footer's bytes go to native memory (the scratch is copied out before any await).
            ThriftCompactWriter thrift = ThriftCompactWriter.ThreadScratch();
            FileEncryptor? encryptor = _encryptor;
            _footer.WriteFooter(thrift, _elements, _totalRows, _columns.Length, _keyValues, CreatedBy,
                encryptor is { PlaintextFooter: true } ? encryptor.Algorithm : null,
                encryptor is null ? null : (g, c) => encryptor.ForChunk(_paths[c], g, c));
            // An encrypted file's plaintext footer is signed (PAR1), an encrypted one follows its crypto metadata (PARE).
            ParquetFooter footer = encryptor is null
                ? ParquetFooter.Of(thrift.WrittenSpan)
                : ParquetFooter.Encrypted(thrift.WrittenSpan, encryptor.StoredFooterLength(thrift.WrittenSpan.Length), encryptor.WriteStoredFooter,
                    encryptor.PlaintextFooter ? "PAR1"u8 : "PARE"u8);
            try
            {
                await WriteBytesAsync(footer.FileTail, useAsync, ct).ConfigureAwait(false);
                if (useAsync) await _stream.FlushAsync(ct).ConfigureAwait(false);
                else _stream.Flush();
            }
            catch
            {
                footer.Dispose();
                throw;
            }
            _completed = true;
            _footerHeld = footer; // parsed when Metadata is first asked for
            _footer.Dispose();
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    /// <summary>The writer's name and version, as readers parse it (<c>app version x.y.z</c>).</summary>
    private static readonly string CreatedBy = $"seseragi version {typeof(ParquetWriter).Assembly.GetName().Version?.ToString(3) ?? "0.0.0"}";

    /// <summary>
    /// The Arrow schema stored with the file: the columns written as MAP groups (<see cref="ParquetWriteOptions.MapColumns"/>,
    /// lists of key / value structs) as Arrow maps, so the stored schema says what the file holds.
    /// </summary>
    private static Apache.Arrow.Schema StoredSchema(Apache.Arrow.Schema schema, IReadOnlySet<string>? maps)
    {
        if (maps is null || maps.Count == 0) return schema;
        return new Apache.Arrow.Schema([.. schema.FieldsList.Select(f => Restored(f, f.Name))], schema.Metadata);

        Field Restored(Field field, string path) => new(field.Name, Type(field.DataType, path), field.IsNullable, field.Metadata);

        IArrowType Type(IArrowType type, string path) => type switch
        {
            ListType or LargeListType when maps.Contains(path) && Entries(type) is { } entries => new MapType(
                new Field("key", Type(entries.Fields[0].DataType, path + ".key"), false, entries.Fields[0].Metadata),
                new Field("value", Type(entries.Fields[1].DataType, path + ".value"), entries.Fields[1].IsNullable, entries.Fields[1].Metadata)),
            ListType l => new ListType(Restored(l.ValueField, path + "[]")),
            LargeListType l => new LargeListType(Restored(l.ValueField, path + "[]")),
            StructType st => new StructType([.. st.Fields.Select(c => Restored(c, $"{path}.{c.Name}"))]),
            MapType m => new MapType(Restored(m.KeyField, path + ".key"), Restored(m.ValueField, path + ".value"), m.KeySorted),
            _ => type,
        };

        static StructType? Entries(IArrowType type) => type switch
        {
            ListType { ValueDataType: StructType { Fields.Count: 2 } entries } => entries,
            LargeListType { ValueDataType: StructType { Fields.Count: 2 } entries } => entries,
            _ => null,
        };
    }

    /// <summary>The Arrow IPC schema message, as Arrow writers store it in <c>ARROW:schema</c>.</summary>
    private static byte[] ArrowSchemaMessage(Apache.Arrow.Schema schema) => Kaminari.Sazanami.Arrow.Ipc.IpcSchemaMessage.Write(schema);

    /// <summary>Lets a row group still on its way to the stream finish (its failure was or will be reported elsewhere).</summary>
    private void WaitForWrites()
    {
        try
        {
            _writing.Wait();
        }
        catch (AggregateException)
        {
            // A failed write faulted the writer where it was awaited (or the file is being given up anyway).
        }
    }

    /// <summary>
    /// Completes the file unless writing failed or the writer was aborted (<see cref="Abort"/>; then the stream keeps
    /// what was written, without a footer), and closes the stream unless asked not to.
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;
        try
        {
            if (!_completed && !_faulted && !_aborted) Complete();
        }
        finally
        {
            _disposed = true;
            ReleaseFooter();
            Abandon();
            WaitForWrites();
            FreeColumns();
            if (!_options.LeaveOpen) _stream.Dispose();
        }
    }

    /// <summary>
    /// Frees the footer's native bytes on dispose unless handed over; the metadata stays available (parsed already,
    /// or its bytes kept on the managed heap to parse when asked for).
    /// </summary>
    private void ReleaseFooter()
    {
        if (_footerHeld is not { } footer) return;
        if (_metadata is null) _footerBytes = footer.Thrift.ToArray();
        footer.Dispose();
        _footerHeld = null;
    }

    /// <inheritdoc cref="Dispose"/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        try
        {
            if (!_completed && !_faulted && !_aborted) await CompleteAsync().ConfigureAwait(false);
        }
        finally
        {
            _disposed = true;
            ReleaseFooter();
            Abandon();
            WaitForWrites();
            FreeColumns();
            if (!_options.LeaveOpen) await _stream.DisposeAsync().ConfigureAwait(false);
        }
    }
}
