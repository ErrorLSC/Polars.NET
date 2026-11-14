using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Helpers;
using Kaminari.Sazanami.Rows;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet.Codecs;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Reading;
using Kaminari.Seseragi.Files.Parquet.Schema;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Reads the row groups of a <see cref="ParquetFile"/> as Arrow record batches in native memory, row group by row
/// group (a batch never spans two). The unit of work is a column of a row group (a nested column's leaves
/// together), decoded page by page from start to end and cut into batches; the row group's batches are put together
/// once all its columns are done. Column chunks are read as the source suits: sliced when it lives in memory, page by
/// page when it is a local file, and otherwise (a store) fetched with few ranged reads before decoding.
/// </summary>
public sealed class ParquetReader : IDisposable, IAsyncDisposable
{
    /// <summary>Column chunks closer than this are fetched in one read.</summary>
    private const long CoalesceGap = 1 << 20;

    /// <summary>A coalesced read grows no larger than this (a single bigger chunk is read alone).</summary>
    private const long CoalesceLimit = 64L << 20;

    private readonly ParquetFile _file;
    private readonly ParquetReadOptions _options;
    private readonly ColumnMapping[] _columns;
    private readonly ReadNode[] _leaves;  // every leaf read, in column order: one column chunk each
    private readonly int[] _candidates;              // the row groups to read, in order, after pruning by their statistics
    private readonly Pruning.PruneNode? _pruning;    // the compiled filter, when it can rule something out
    private readonly Pruning.LeafDomain[] _predicates; // the leaves the filter tests
    private ReadUnit[]? _planned;                    // the units, once planned (on the first read)
    private readonly bool _oldParquetMr;
    private readonly NativeBufferPool _buffers;

    /// <summary>The pool of the chunk, window and scratch buffers (tests).</summary>
    internal NativeBufferPool Buffers => _buffers;
    private readonly (int First, int Count)[] _columnLeaves; // each column's leaves in _leaves
    private readonly ReadOnlyMemory<byte>? _memory;           // the source's bytes, when it lives in memory
    private readonly bool _fetch;                             // whether chunks are fetched before decoding (not a local source)
    private readonly ConcurrentExclusiveSchedulerPair _schedulers;

    internal ParquetReader(ParquetFile file, ParquetReadOptions options)
    {
        options.Validate();
        _file = file;
        _options = options;
        var (_, all) = ArrowMapping.Map(file.Tree, file.Thrift, options);
        _columns = Select(all, options.Columns, options.ColumnPaths);
        _leaves = [.. _columns.SelectMany(c => c.Read.Leaves())];
        _columnLeaves = new (int, int)[_columns.Length];
        for (int c = 0, first = 0; c < _columns.Length; c++)
        {
            int count = _columns[c].Read.Leaves().Count();
            _columnLeaves[c] = (first, count);
            first += count;
        }
        if (options.Reading is ChunkReading.Auto or ChunkReading.Slice && file.Source is ILocalSource local && local.TryGetMemory(out ReadOnlyMemory<byte> memory))
            _memory = memory;
        _fetch = options.Reading switch
        {
            ChunkReading.Fetch => true,
            ChunkReading.Stream => false,
            _ => _memory is null && file.Source is not ILocalSource { SmallReadsAreCheap: true },
        };
        _schedulers = new ConcurrentExclusiveSchedulerPair(TaskScheduler.Default, Math.Max(1, options.MaxDegreeOfParallelism));
        int[] rowGroups = options.RowGroups is { } groups ? [.. groups] : [.. Enumerable.Range(0, file.Thrift.RowGroups.Length)];
        foreach (int g in rowGroups)
        {
            if ((uint)g >= (uint)file.Thrift.RowGroups.Length)
                throw new ArgumentOutOfRangeException(nameof(options), g, $"The file has {file.Thrift.RowGroups.Length} row groups.");
        }
        _predicates = [];
        if (options.Filter is { } filter)
        {
            var compiler = new Pruning.FilterCompiler(file.Tree, file.Thrift, options);
            _pruning = compiler.Compile(filter);
            _predicates = [.. compiler.Leaves];
        }
        if (_pruning is { } pruning)
            rowGroups = [.. rowGroups.Where(g => (pruning.Evaluate(new Pruning.RowGroupStatistics(file.Thrift.RowGroups[g])) & Pruning.Outcomes.True) != 0)];
        _candidates = rowGroups;
        long total = file.Thrift.RowGroups.Sum(g => Math.Max(0, g.NumRows));
        if (options.Rows is { } rows && rows.End > total)
            throw new ArgumentOutOfRangeException(nameof(options), $"The rows selected reach row {rows.End - 1}; the file has {total} rows.");
        Field[] fields = [.. _columns.Select(c => c.Field)];
        if (options.RowIndexColumn is { } rowIndex)
        {
            if (fields.Any(f => f.Name == rowIndex))
                throw new ArgumentException($"The row index column '{rowIndex}' has the name of a column read.", nameof(options));
            fields = [.. fields, new Field(rowIndex, Int64Type.Default, nullable: false)];
        }
        Schema = new Apache.Arrow.Schema(fields, null);
        _oldParquetMr = IsOldParquetMr(file.Thrift.CreatedBy);
        // Per row group in flight: its fetched chunks (a few), and per leaf a decompression scratch and a read window.
        int inFlight = options.MaxDegreeOfParallelism > 1 ? Math.Min(options.MaxDegreeOfParallelism, _candidates.Length) + 1 : 1;
        _buffers = new NativeBufferPool((2 * _leaves.Length + 4) * inFlight);
    }

    /// <summary>The Arrow schema of the batches (the selected columns, in file order).</summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>How column chunks are read: fetched, sliced from memory, or page by page.</summary>
    internal ChunkReading EffectiveReading => _fetch ? ChunkReading.Fetch : _memory is not null ? ChunkReading.Slice : ChunkReading.Stream;

    private int _mappedChunks;

    /// <summary>How many column chunks were decoded from the file's mapping (for tests).</summary>
    internal int MappedChunks => Volatile.Read(ref _mappedChunks);

    /// <summary>The row groups read, in order (after <see cref="ParquetReadOptions.Filter"/> pruned some; for tests).</summary>
    internal IReadOnlyList<int> RowGroupsRead => [.. Units().Select(u => u.Group)];

    /// <summary>The rows read of each row group read (null: all of them), in its own numbering (for tests).</summary>
    internal IReadOnlyList<RowRanges?> RowsRead => [.. Units().Select(u => u.Rows)];

    private int _pagesSkipped, _pagesRead;

    /// <summary>Data pages read and decompressed (for tests and measurements).</summary>
    internal int PagesRead => Volatile.Read(ref _pagesRead);

    /// <summary>Data pages passed over unread (by their headers or the offset index; for tests and measurements).</summary>
    internal int PagesSkipped => Volatile.Read(ref _pagesSkipped);

    /// <summary>A row group to read: its number, its first row's number in the file, and the rows read (null: all).</summary>
    private readonly record struct ReadUnit(int Group, long FirstRow, RowRanges? Rows);

    /// <summary>The units, planned on first use (planning may read page indexes); every caller gets the same.</summary>
    private ReadUnit[] Units() => _planned ?? Publish(PlanAsync(useAsync: false, default).AsTask().GetAwaiter().GetResult());

    private async ValueTask<ReadUnit[]> UnitsAsync(CancellationToken cancellationToken) =>
        _planned ?? Publish(await PlanAsync(useAsync: true, cancellationToken).ConfigureAwait(false));

    private ReadUnit[] Publish(ReadUnit[] units)
    {
        Interlocked.CompareExchange(ref _planned, units, null);
        return _planned!;
    }

    /// <summary>
    /// The row groups to read in order, each with its rows: those <see cref="ParquetReadOptions.Rows"/> selects, less
    /// the pages whose column index shows that no row passes the filter, up to <see cref="ParquetReadOptions.MaxRows"/>;
    /// row groups left without rows are not read. Page indexes are read only when rows are selected or the filter tests
    /// a leaf: a full scan reads none.
    /// </summary>
    private async ValueTask<ReadUnit[]> PlanAsync(bool useAsync, CancellationToken cancellationToken)
    {
        RowGroup[] groups = _file.Thrift.RowGroups;
        var first = new long[groups.Length];
        long total = 0;
        for (int g = 0; g < groups.Length; g++)
        {
            first[g] = total;
            total += Math.Max(0, groups[g].NumRows);
        }
        List<(int Group, RowRanges? Rows)> selected = [];
        foreach (int g in _candidates)
        {
            RowRanges? rows = _options.Rows?.Slice(first[g], Math.Max(0, groups[g].NumRows));
            if (rows is not { IsEmpty: true }) selected.Add((g, rows));
        }

        // Pages: the column indexes of the leaves the filter tests.
        if (_pruning is { } pruning && _predicates.Length > 0)
        {
            await _file.PageIndexes.LoadAsync([.. selected.SelectMany(u => _predicates.Select(d => (u.Group, d.Leaf)))],
                [.. selected.SelectMany(u => _predicates.Select(d => (u.Group, d.Leaf)))], useAsync, cancellationToken).ConfigureAwait(false);
            for (int i = selected.Count - 1; i >= 0; i--)
            {
                var (g, rows) = selected[i];
                if (Pruning.PagePruning.Keep(pruning, _predicates, groups[g], g, _file.PageIndexes) is not { } kept) continue;
                rows = rows is null ? kept : rows.Intersect(kept);
                if (rows.IsEmpty) selected.RemoveAt(i);
                else selected[i] = (g, rows);
            }
        }

        long budget = _options.MaxRows ?? long.MaxValue;
        List<ReadUnit> units = [];
        foreach (var (g, rows) in selected)
        {
            if (budget == 0) break;
            long n = Math.Max(0, groups[g].NumRows);
            RowRanges? read = rows;
            long count = read?.RowCount ?? n;
            if (count > budget)
            {
                read = (read ?? RowRanges.All(n)).Take(budget);
                count = budget;
            }
            budget -= count;
            if (read is { Count: 1 } && read[0] == (0, n)) read = null; // the whole row group
            units.Add(new ReadUnit(g, first[g], read));
        }

        // The offset indexes of the chunks read where rows are skipped: pages wholly skipped are then not read at all.
        (int, int)[] chunks = [.. units.Where(u => u.Rows is not null).SelectMany(u => _leaves.Select(l => (u.Group, l.Leaf!.LeafIndex)))];
        if (chunks.Length > 0) await _file.PageIndexes.LoadAsync(chunks, [], useAsync, cancellationToken).ConfigureAwait(false);
        return [.. units];
    }

    /// <summary>The number of column chunks read per row group (the leaves of the selection).</summary>
    internal int LeafCount => _leaves.Length;

    /// <summary>
    /// The columns to read, in file order: whole ones named in <paramref name="names"/>, and ones pruned to the struct
    /// fields on <paramref name="paths"/> (a column named whole is read whole). All of them when both are null.
    /// </summary>
    private static ColumnMapping[] Select(ColumnMapping[] all, IReadOnlyList<string>? names, IReadOnlyList<string[]>? paths)
    {
        if (names is null && paths is null) return all;
        HashSet<string> whole = new(names ?? [], StringComparer.Ordinal);
        foreach (string name in whole)
        {
            if (!all.Any(c => c.Field.Name == name)) throw new ArgumentException($"The file has no top-level column '{name}'.", nameof(names));
        }
        Dictionary<string, List<string[]>> partial = new(StringComparer.Ordinal);
        foreach (string[] path in paths ?? [])
        {
            if (path is null || path.Length == 0) throw new ArgumentException("A column path is empty.", nameof(paths));
            if (!all.Any(c => c.Field.Name == path[0])) throw new ArgumentException($"The file has no top-level column '{path[0]}'.", nameof(paths));
            (partial.TryGetValue(path[0], out var list) ? list : partial[path[0]] = []).Add(path[1..]);
        }
        List<ColumnMapping> selected = [];
        foreach (ColumnMapping column in all)
        {
            string name = column.Field.Name;
            if (whole.Contains(name)) selected.Add(column);
            else if (partial.TryGetValue(name, out var below))
            {
                ReadNode pruned = ArrowMapping.Prune(column.Read, below, name);
                selected.Add(column with { Field = pruned.Field, Read = pruned });
            }
        }
        return [.. selected];
    }

    /// <summary>Reads the batches.</summary>
    /// <exception cref="ParquetFormatException">A page is damaged, or uses an encoding or codec this library does not read.</exception>
    public IEnumerable<RecordBatch> ReadBatches()
    {
        ReadUnit[] units = Units(); // planned when the enumeration starts
        foreach (RecordBatch batch in InParallel(units) ? ReadParallel(units) : ReadSequential(units)) yield return batch;
    }

    /// <summary>Whether there is more than one unit to run side by side.</summary>
    private bool InParallel(ReadUnit[] units) => _options.MaxDegreeOfParallelism > 1 && (units.Length > 1 || _columns.Length > 1);

    private IEnumerable<RecordBatch> ReadSequential(ReadUnit[] units)
    {
        foreach (ReadUnit unit in units)
        {
            foreach (RecordBatch batch in Hand(DecodeRowGroup(unit, default))) yield return batch;
        }
    }

    /// <summary>Hands the batches out one by one; those not taken when the consumer stops are disposed.</summary>
    private static IEnumerable<RecordBatch> Hand(RecordBatch[] batches)
    {
        int i = 0;
        try
        {
            for (; i < batches.Length; i++)
            {
                RecordBatch batch = batches[i];
                batches[i] = null!;
                yield return batch;
            }
        }
        finally
        {
            for (; i < batches.Length; i++) batches[i]?.Dispose();
        }
    }

    /// <summary>
    /// The batches as an Arrow stream that owns them, for handing to another library (a dataframe's Arrow
    /// stream import, a C stream export): each batch is disposed when the next one is read or the stream is disposed. A consumer
    /// that exports a batch keeps its memory alive through the export; one that keeps a batch beyond the next read
    /// must use <see cref="ReadBatches"/> instead.
    /// </summary>
    public Apache.Arrow.Ipc.IArrowArrayStream ReadArrowStream() => new OwningBatchStream(Schema, ReadBatches());

    private sealed class OwningBatchStream(Apache.Arrow.Schema schema, IEnumerable<RecordBatch> batches) : Apache.Arrow.Ipc.IArrowArrayStream
    {
        private readonly IEnumerator<RecordBatch> _batches = batches.GetEnumerator();
        private RecordBatch? _current;

        public Apache.Arrow.Schema Schema => schema;

        public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            _current?.Dispose();
            _current = _batches.MoveNext() ? _batches.Current : null;
            return ValueTask.FromResult(_current);
        }

        public void Dispose()
        {
            _current?.Dispose();
            _current = null;
            _batches.Dispose();
        }
    }

    /// <inheritdoc cref="ReadBatches"/>
    public async IAsyncEnumerable<RecordBatch> ReadBatchesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ReadUnit[] units = await UnitsAsync(cancellationToken).ConfigureAwait(false);
        if (InParallel(units))
        {
            await foreach (RecordBatch batch in ReadParallelAsync(units, cancellationToken).ConfigureAwait(false)) yield return batch;
            yield break;
        }
        foreach (ReadUnit unit in units)
        {
            RecordBatch[] batches = await DecodeRowGroupAsync(unit, useAsync: true, parallel: false, cancellationToken).ConfigureAwait(false);
            foreach (RecordBatch batch in Hand(batches))
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return batch;
            }
        }
    }

    /// <summary>
    /// Row groups decoded side by side, their columns as units on a scheduler running at most
    /// <see cref="ParquetReadOptions.MaxDegreeOfParallelism"/> of them; up to that many row groups ahead of the
    /// consumer (fewer when their sizes pass <see cref="ParquetReadOptions.InFlightBytes"/>), delivered in order. A consumer that stops
    /// early cancels the rest and frees what they decoded.
    /// </summary>
    private IEnumerable<RecordBatch> ReadParallel(ReadUnit[] units)
    {
        using var window = new RowGroupWindow(this, units, CancellationToken.None);
        for (int i = 0; i < units.Length; i++)
        {
            foreach (RecordBatch batch in window.Take(i).GetAwaiter().GetResult()) yield return batch;
        }
    }

    private async IAsyncEnumerable<RecordBatch> ReadParallelAsync(ReadUnit[] units, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var window = new RowGroupWindow(this, units, cancellationToken);
        for (int i = 0; i < units.Length; i++)
        {
            foreach (RecordBatch batch in await window.Take(i).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return batch;
            }
        }
    }

    /// <summary>The row groups being decoded ahead of the consumer, and the batches of the one it is taking.</summary>
    private sealed class RowGroupWindow(ParquetReader reader, ReadUnit[] units, CancellationToken cancellationToken) : IDisposable
    {
        private readonly CancellationTokenSource _cancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        private readonly Task<RecordBatch[]>?[] _tasks = new Task<RecordBatch[]>?[units.Length];
        private RecordBatch[]? _taking;
        private int _taken; // batches of _taking already handed out
        private int _started;
        private long _inFlight; // uncompressed bytes of the row groups started and not yet taken

        private long Size(int index) => Math.Max(0, reader._file.Thrift.RowGroups[units[index].Group].TotalByteSize);

        /// <summary>Starts the row groups up to the window's end and waits for the <paramref name="index"/>th.</summary>
        public async Task<IEnumerable<RecordBatch>> Take(int index)
        {
            for (; _started < _tasks.Length && _started < index + reader._options.MaxDegreeOfParallelism
                   && (_started == index || _inFlight + Size(_started) <= reader._options.InFlightBytes); _started++)
            {
                ReadUnit unit = units[_started];
                CancellationToken token = _cancel.Token;
                _inFlight += Size(_started);
                _tasks[_started] = Task.Run(() => reader.DecodeRowGroupAsync(unit, useAsync: true, parallel: true, token), token);
            }
            RecordBatch[] batches = await _tasks[index]!.ConfigureAwait(false);
            _tasks[index] = null;
            _inFlight -= Size(index);
            _taking = batches;
            _taken = 0;
            return Hand(batches);
        }

        private IEnumerable<RecordBatch> Hand(RecordBatch[] batches)
        {
            for (; _taken < batches.Length; _taken++)
            {
                RecordBatch batch = batches[_taken];
                batches[_taken] = null!;
                yield return batch;
            }
        }

        public void Dispose()
        {
            _cancel.Cancel();
            if (_taking is not null)
            {
                foreach (RecordBatch? batch in _taking) batch?.Dispose();
            }
            foreach (Task<RecordBatch[]>? task in _tasks)
            {
                if (task is null) continue;
                try
                {
                    foreach (RecordBatch batch in task.GetAwaiter().GetResult()) batch.Dispose();
                }
                catch
                {
                    // abandoned: its error no longer matters
                }
            }
            _cancel.Dispose();
        }
    }

    /// <summary>Reads and decodes a whole row group, its columns one after the other.</summary>
    private RecordBatch[] DecodeRowGroup(ReadUnit unit, CancellationToken cancellationToken) =>
        DecodeRowGroupAsync(unit, useAsync: false, parallel: false, cancellationToken).GetAwaiter().GetResult();

    /// <summary>Reads and decodes a whole row group, its columns one after the other (for tests).</summary>
    internal RecordBatch[] DecodeRowGroup(int g, CancellationToken cancellationToken) => DecodeRowGroup(new ReadUnit(g, 0, null), cancellationToken);

    /// <summary>Reads and decodes a whole row group (the unit parallel reading schedules), its columns side by side when asked.</summary>
    private async Task<RecordBatch[]> DecodeRowGroupAsync(ReadUnit unit, bool useAsync, bool parallel, CancellationToken cancellationToken)
    {
        using RowGroupChunks? chunks = _fetch ? await Fetch(unit.Group, unit.Rows, useAsync, cancellationToken).ConfigureAwait(false) : null;
        return await DecodeColumnsAsync(unit.Group, chunks, unit.Rows, unit.FirstRow, parallel, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc cref="DecodeRowGroupAsync(ReadUnit, bool, bool, CancellationToken)"/>
    internal Task<RecordBatch[]> DecodeRowGroupAsync(int g, bool useAsync, bool parallel, CancellationToken cancellationToken) =>
        DecodeRowGroupAsync(new ReadUnit(g, 0, null), useAsync, parallel, cancellationToken);

    /// <summary>
    /// Decodes a row group into batches, from fetched <paramref name="chunks"/> or (null) straight from the source;
    /// <paramref name="ranges"/> (internal) selects row ranges.
    /// </summary>
    internal IEnumerable<RecordBatch> Decode(int g, RowGroupChunks? chunks, IReadOnlyList<(long Start, long Count)>? ranges = null) =>
        Hand(DecodeColumnsAsync(g, chunks, ranges, 0, parallel: false, default).GetAwaiter().GetResult());

    /// <summary>
    /// Decodes the selected columns of a row group, each from its first page to its last (side by side when
    /// <paramref name="parallel"/>), and puts the batches together. Of several failing columns the first one's error
    /// is reported, whatever the order they ran in.
    /// </summary>
    private async Task<RecordBatch[]> DecodeColumnsAsync(int g, RowGroupChunks? chunks, IReadOnlyList<(long Start, long Count)>? ranges, long firstRow,
        bool parallel, CancellationToken cancellationToken)
    {
        RowGroup group = _file.Thrift.RowGroups[g];
        long rows = group.NumRows;
        if (rows < 0) throw new ParquetFormatException($"Row group {g} has {rows} rows.");
        // Every column chunk holds a value (or a null) per row at least: a row count beyond them all is damage, refused
        // before anything is planned by it.
        if (group.Columns.Length > 0)
        {
            long most = group.Columns.Max(c => c.MetaData?.NumValues ?? 0);
            if (rows > most) throw new ParquetFormatException($"Row group {g} has {rows} rows, more than any of its column chunks holds values ({most}).");
        }
        IEnumerable<(bool Read, int Count, bool Export)> plan = Plan(rows, ranges);
        (long Start, long Length)[] at = ChunkRanges(g);
        // A hold on the file's mapping for the decode of this row group (the reader may outlive a disposed file), taken
        // only when a chunk of it is read from the mapping: making and dropping a mapping is not free (measured on
        // compressed files read on many threads) and buys nothing for chunks read the ordinary way.
        long mapStart = 0;
        MappedFile? map = null;
        if (chunks is null && _memory is null && _options.MemoryMap != ParquetMemoryMap.Off && _file.Source is ILocalSource local
            && _leaves.Any(leaf => FromMapping(group.Columns[leaf.Leaf!.LeafIndex].MetaData!, leaf.Leaf)))
        {
            map = local.Map(out mapStart);
            if (map is not null && !map.TryAcquire()) map = null;
        }
        var readers = new LeafReader[_leaves.Length];
        var builders = new NativeColumnBuilder[_columns.Length];
        var arrays = new IArrowArray[]?[_columns.Length];
        Dictionary<ReadNode, LeafReader> byNode = [];
        try
        {
            for (int l = 0; l < _leaves.Length; l++)
            {
                ReadNode leaf = _leaves[l];
                ColumnMetaData meta = group.Columns[leaf.Leaf!.LeafIndex].MetaData!;
                // With rows skipped, a chunk with a usable offset index is read by it: pages wholly skipped are not read.
                ChunkPageIndex? index = ranges is null ? null : _file.PageIndexes.Get(g, leaf.Leaf.LeafIndex, withColumns: false);
                PageReader pages = chunks is not null
                    ? chunks.IsWhole(l)
                        ? new PageReader(chunks.Chunk(l), meta.Codec, _options.VerifyChecksums, _buffers)
                        : new PageReader(chunks.Pieces(l, at[l].Start), at[l].Length, meta.Codec, _options.VerifyChecksums, _buffers)
                    : _memory is { } memory
                        ? new PageReader(memory.Slice((int)at[l].Start, (int)at[l].Length), meta.Codec, _options.VerifyChecksums, _buffers)
                        : map is not null && FromMapping(meta, leaf.Leaf)
                            ? Mapped(map.Range(mapStart + at[l].Start, (int)at[l].Length), meta)
                            : new PageReader(_file.Source, at[l].Start, at[l].Length, meta.Codec, _options.VerifyChecksums, _buffers);
                if (index is not null) pages.UseIndex(index, at[l].Start, Needed(index, ranges!));
                readers[l] = LeafReaders.Create(pages, leaf.Leaf, leaf.LeafType!, meta.NumValues);
                readers[l].UncompressedBytes = meta.TotalUncompressedSize;
                readers[l].LevelsAsBits = _options.LevelsAsBits;
                // Measured: staged non-temporal writes of strings win when threads compete for memory bandwidth and
                // lose a few percent on one thread.
                if (readers[l].Builder is NativeBinaryViewBuilder views)
                    views.WritesPastCaches = _options.MaxDegreeOfParallelism > 1 && (_candidates.Length > 1 || _columns.Length > 1);
                byNode[leaf] = readers[l];
            }
            foreach (ColumnMapping column in _columns) ConfigureLeaves(column.Read, byNode, keepLevels: !column.IsFlat, fixedSizeList: null);
            for (int c = 0; c < _columns.Length; c++) builders[c] = NestedAssembler.CreateBuilder(_columns[c].Read, byNode);

            if (parallel && _columns.Length > 1)
            {
                var tasks = new Task<IArrowArray[]>[_columns.Length];
                for (int c = 0; c < tasks.Length; c++)
                {
                    int column = c;
                    tasks[c] = Task.Factory.StartNew(() => DecodeColumn(column, readers, builders[column], byNode, plan, rows, ranges is null, cancellationToken),
                        cancellationToken, TaskCreationOptions.DenyChildAttach, _schedulers.ConcurrentScheduler);
                }
                try
                {
                    await Task.WhenAll(tasks).ConfigureAwait(false);
                }
                catch
                {
                    foreach (Task<IArrowArray[]> task in tasks)
                    {
                        if (task.IsCompletedSuccessfully) Dispose(task.Result);
                    }
                    Task first = System.Array.Find(tasks, t => !t.IsCompletedSuccessfully)!;
                    if (first.Exception?.InnerException is { } error) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(error);
                    throw new OperationCanceledException(cancellationToken);
                }
                for (int c = 0; c < tasks.Length; c++) arrays[c] = tasks[c].Result;
            }
            else
            {
                for (int c = 0; c < _columns.Length; c++) arrays[c] = DecodeColumn(c, readers, builders[c], byNode, plan, rows, ranges is null, cancellationToken);
            }

            List<RecordBatch> batches = [];
            int b = 0, length = 0;
            long row = firstRow; // the file's number of the step's first row
            List<(long Start, int Count)> rowsOfBatch = [];
            foreach (var (read, n, export) in plan)
            {
                if (read)
                {
                    rowsOfBatch.Add((row, n));
                    length += n;
                }
                row += n;
                if (!export) continue;
                int batch = b++;
                IArrowArray[] columns = [.. arrays.Select(column => column![batch])];
                if (_options.RowIndexColumn is not null) columns = [.. columns, RowIndex(rowsOfBatch, length)];
                batches.Add(new RecordBatch(Schema, columns, length));
                rowsOfBatch.Clear();
                length = 0;
            }
            return [.. batches];
        }
        catch
        {
            foreach (IArrowArray[]? column in arrays) Dispose(column);
            throw;
        }
        finally
        {
            foreach (LeafReader? r in readers)
            {
                if (r is null) continue;
                Interlocked.Add(ref _pagesSkipped, r.SkippedPages);
                Interlocked.Add(ref _pagesRead, r.DataPagesRead);
                r.Dispose();
            }
            // A flat column's builder is its reader's: disposing twice is harmless.
            foreach (NativeColumnBuilder? builder in builders) builder?.Dispose();
            map?.Release();
        }

        static void Dispose(IArrowArray[]? arrays)
        {
            foreach (IArrowArray array in arrays ?? []) array.Dispose();
        }
    }

    /// <summary>The numbers of the rows of a batch (runs of consecutive rows) as an Int64 column.</summary>
    private static IArrowArray RowIndex(List<(long Start, int Count)> runs, int length)
    {
        using var builder = new NativeFixedWidthBuilder<long>(Int64Type.Default);
        Span<long> rows = builder.AppendSlots(length);
        int at = 0;
        foreach (var (start, count) in runs)
        {
            for (int i = 0; i < count; i++) rows[at++] = start + i;
        }
        return builder.ExportArray();
    }

    /// <summary>
    /// Whether a column chunk is decoded from the file's mapping: all of them when it is on; when automatic, those
    /// uncompressed and of a fixed-width physical type (measured: the mapping pays there, and costs on compressed and
    /// string chunks read on many threads).
    /// </summary>
    private PageReader Mapped(ReadOnlyMemory<byte> chunk, ColumnMetaData meta)
    {
        Interlocked.Increment(ref _mappedChunks);
        return new PageReader(chunk, meta.Codec, _options.VerifyChecksums, _buffers);
    }

    private bool FromMapping(ColumnMetaData meta, SchemaNode leaf) => _options.MemoryMap == ParquetMemoryMap.On
        || meta.Codec == CompressionCodec.Uncompressed && leaf.Element.Type is ParquetType.Boolean or ParquetType.Int32 or ParquetType.Int64
            or ParquetType.Int96 or ParquetType.Float or ParquetType.Double or ParquetType.FixedLenByteArray;

    /// <summary>
    /// The rows to read and to skip, in order (<paramref name="ranges"/>, all rows when null), and where a batch ends:
    /// a batch gathers the rows read across skips until it holds the batch size, so a scattered selection still gives
    /// full batches.
    /// </summary>
    /// <remarks>
    /// The ranges are checked at once; the steps are made as they are taken (enumerated once per column), so that a
    /// row count the data does not hold costs no memory before decoding finds that out.
    /// </remarks>
    private IEnumerable<(bool Read, int Count, bool Export)> Plan(long rows, IReadOnlyList<(long Start, long Count)>? ranges)
    {
        long at = 0;
        foreach (var (start, count) in ranges ?? [(0, rows)])
        {
            if (start < at || count < 0 || start + count > rows) throw new ArgumentOutOfRangeException(nameof(ranges), "Row ranges must be ordered and inside the row group.");
            at = start + count;
        }
        return Steps(rows, ranges, _options.BatchSize);

        static IEnumerable<(bool Read, int Count, bool Export)> Steps(long rows, IReadOnlyList<(long Start, long Count)>? ranges, int batchSize)
        {
            long at = 0;
            int pending = 0; // rows read into the batch being gathered
            foreach (var (start, count) in ranges ?? [(0, rows)])
            {
                for (long skip = start - at; skip > 0; skip -= int.MaxValue) yield return (false, (int)Math.Min(skip, int.MaxValue), false);
                for (long left = count; left > 0;)
                {
                    int n = (int)Math.Min(left, batchSize - pending);
                    left -= n;
                    pending += n;
                    bool full = pending == batchSize;
                    yield return (true, n, full);
                    if (full) pending = 0;
                }
                at = start + count;
            }
            if (pending > 0) yield return (false, 0, true);
        }
    }

    /// <summary>
    /// Decodes one column of a row group (its leaves side by side, record by record) through the whole
    /// <paramref name="plan"/>: an array per batch, each over memory of its own.
    /// </summary>
    private IArrowArray[] DecodeColumn(int c, LeafReader[] readers, NativeColumnBuilder builder, Dictionary<ReadNode, LeafReader> byNode,
        IEnumerable<(bool Read, int Count, bool Export)> plan, long rows, bool finish, CancellationToken cancellationToken)
    {
        ColumnMapping column = _columns[c];
        var (first, count) = _columnLeaves[c];
        ReadOnlySpan<LeafReader> leaves = readers.AsSpan(first, count);
        List<IArrowArray> arrays = [];
        try
        {
            int n = 0; // rows read into the batch
            foreach (var (read, steps, export) in plan)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (read)
                {
                    foreach (LeafReader r in leaves)
                    {
                        r.Reserve(steps, rows);
                        r.Read(steps);
                    }
                    n += steps;
                }
                else if (steps > 0)
                {
                    foreach (LeafReader r in leaves) r.Skip(steps);
                }
                if (!export) continue;
                try
                {
                    if (!column.IsFlat) NestedAssembler.Assemble(column.Read, builder, byNode, column.Field.Name);
                }
                catch (InvalidOperationException e)
                {
                    throw new ParquetFormatException($"Column '{column.Field.Name}': {e.Message}", e);
                }
                if (builder.Length != n)
                    throw new ParquetFormatException($"Column '{column.Field.Name}' holds {builder.Length} rows of the {n} read.");
                arrays.Add(builder.ExportArray());
                foreach (LeafReader r in leaves) r.BatchExported();
                n = 0;
            }
            if (finish)
            {
                foreach (LeafReader r in leaves) r.Finish();
            }
            foreach (LeafReader r in leaves) r.ReleasePages();
            return [.. arrays];
        }
        catch
        {
            foreach (IArrowArray array in arrays) array.Dispose();
            throw;
        }
    }

    /// <summary>Tells each leaf reader its slot rules, whether to keep levels, and its fixed-size list parent.</summary>
    private static void ConfigureLeaves(ReadNode node, Dictionary<ReadNode, LeafReader> readers, bool keepLevels, ReadNode? fixedSizeList)
    {
        if (node.Kind == ReadKind.Leaf)
        {
            readers[node].Configure(node, keepLevels, fixedSizeList);
            return;
        }
        foreach (ReadNode child in node.Children) ConfigureLeaves(child, readers, keepLevels, node.Kind == ReadKind.FixedSizeList ? node : null);
    }

    /// <summary>Where the selected column chunks of a row group lie (checked: in this file, a codec this library reads).</summary>
    private (long Start, long Length)[] ChunkRanges(int g)
    {
        RowGroup group = _file.Thrift.RowGroups[g];
        var ranges = new (long Start, long Length)[_leaves.Length];
        for (int l = 0; l < _leaves.Length; l++)
        {
            SchemaNode leaf = _leaves[l].Leaf!;
            ColumnChunk chunk = group.Columns[leaf.LeafIndex];
            string name = string.Join('.', leaf.Path);
            if (chunk.FilePath is not null) throw new ParquetFormatException($"Column '{name}' of row group {g} is stored in another file ('{chunk.FilePath}').");
            ColumnMetaData meta = chunk.MetaData!;
            if (!PageCodecs.CanDecompress(meta.Codec)) throw new ParquetFormatException($"Column '{name}' is compressed with {PageCodecs.Name(meta.Codec)}, which is not supported.");
            ranges[l] = ChunkRange(meta, _file.Source.Length, name, _oldParquetMr);
        }
        return ranges;
    }

    /// <summary>Reads the selected column chunks of a row group, coalescing nearby ones into one read.</summary>
    internal ValueTask<RowGroupChunks> Fetch(int g, bool useAsync, CancellationToken ct) => Fetch(g, null, useAsync, ct);

    /// <summary>
    /// Reads the selected column chunks of a row group, coalescing nearby ones into one read; where rows are skipped and
    /// a chunk has a usable offset index, only its dictionary page and the pages holding selected rows (pages closer
    /// than <see cref="ParquetReadOptions.FetchGap"/> come in the same read, unused).
    /// </summary>
    private async ValueTask<RowGroupChunks> Fetch(int g, IReadOnlyList<(long Start, long Count)>? rows, bool useAsync, CancellationToken ct)
    {
        (long Start, long Length)[] at = ChunkRanges(g);
        var pieces = new (long Start, long Length)[at.Length][];
        var whole = new bool[at.Length];
        for (int l = 0; l < at.Length; l++)
        {
            ChunkPageIndex? index = rows is null ? null : _file.PageIndexes.Get(g, _leaves[l].Leaf!.LeafIndex, withColumns: false);
            pieces[l] = index is null ? [at[l]] : Pieces(index, at[l], Needed(index, rows!));
            whole[l] = index is null;
        }
        var chunks = new RowGroupChunks(pieces, whole, _options.FetchGap, _buffers);
        try
        {
            foreach (var (start, length, buffer) in chunks.Reads)
                await ParquetFile.ReadExactlyAsync(_file.Source, start, buffer.Memory[..(int)length], useAsync, ct).ConfigureAwait(false);
            return chunks;
        }
        catch
        {
            chunks.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Where a column chunk lies: from its dictionary page when it has one, else its first data page. Offsets
    /// below the file's magic are not trusted (some writers leave data_page_offset 0 after a dictionary page).
    /// </summary>
    internal static (long Start, long Length) ChunkRange(ColumnMetaData meta, long fileLength, string name, bool padDictionaryHeader)
    {
        long start = meta.DataPageOffset;
        if (meta.DictionaryPageOffset is long d && d >= 4 && (d < start || start < 4)) start = d;
        long length = meta.TotalCompressedSize;
        if (length == 0) return (0, 0);
        if (start < 4 || length < 0 || length > int.MaxValue || start > fileLength - length)
            throw new ParquetFormatException($"Column '{name}' gives a chunk of {length} bytes at {start}, outside the file.");
        // PARQUET-816: parquet-mr before 1.2.9 left the dictionary page header out of the chunk size; as the reference
        // readers do, read up to 100 more bytes (the page sizes then decide).
        if (padDictionaryHeader)
            length = Math.Min(length + 100, Math.Min(int.MaxValue, fileLength - start));
        return (start, length);
    }

    /// <summary>Whether the writer is parquet-mr older than 1.2.9 (or of no stated version): see <see cref="ChunkRange"/>.</summary>
    internal static bool IsOldParquetMr(string? createdBy)
    {
        if (createdBy is null || !createdBy.StartsWith("parquet-mr", StringComparison.Ordinal)) return false;
        string[] words = createdBy.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        int at = System.Array.IndexOf(words, "version");
        if (at < 0 || at + 1 >= words.Length) return true;
        string[] parts = words[at + 1].Split('.', '-');
        int Part(int i) => i < parts.Length && int.TryParse(parts[i], System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out int n) ? n : 0;
        return (Part(0), Part(1), Part(2)).CompareTo((1, 2, 9)) < 0;
    }

    /// <summary>The bytes of a row group's column chunks: a few native buffers, each read with one ranged read.</summary>
    /// <summary>Which pages of a chunk hold a row of <paramref name="ranges"/> (the others are skipped whole).</summary>
    private static bool[] Needed(ChunkPageIndex index, IReadOnlyList<(long Start, long Count)> ranges)
    {
        var needed = new bool[index.PageCount];
        int r = 0;
        for (int k = 0; k < needed.Length; k++)
        {
            long start = index.FirstRows[k], end = start + index.RowsOf(k);
            while (r < ranges.Count && ranges[r].Start + ranges[r].Count <= start) r++;
            needed[k] = r < ranges.Count && ranges[r].Start < end;
        }
        return needed;
    }

    /// <summary>The bytes of a chunk to fetch: the dictionary page before the indexed pages, and each run of needed pages.</summary>
    private static (long Start, long Length)[] Pieces(ChunkPageIndex index, (long Start, long Length) chunk, bool[] needed)
    {
        List<(long Start, long Length)> pieces = [];
        if (index.Offsets[0] > chunk.Start) pieces.Add((chunk.Start, index.Offsets[0] - chunk.Start));
        for (int k = 0; k < needed.Length; k++)
        {
            if (!needed[k]) continue;
            long start = index.Offsets[k], end = start + index.Sizes[k];
            if (pieces.Count > 0 && pieces[^1].Start + pieces[^1].Length == start) pieces[^1] = (pieces[^1].Start, end - pieces[^1].Start);
            else pieces.Add((start, end - start));
        }
        return [.. pieces];
    }

    internal sealed class RowGroupChunks : IDisposable
    {
        private readonly List<(long Start, long Length, NativeByteBuffer Buffer)> _reads = [];
        private readonly (long Start, int Read, long Offset, int Length)[][] _pieces; // per leaf: its pieces and where they were read
        private readonly bool[] _whole;
        private readonly NativeBufferPool? _pool;

        public RowGroupChunks((long Start, long Length)[] ranges, NativeBufferPool? pool = null)
            : this([.. ranges.Select(r => ((long Start, long Length)[])[r])], [.. ranges.Select(_ => true)], CoalesceGap, pool)
        {
        }

        /// <summary>
        /// Reads per leaf <paramref name="pieces"/> of its chunk (<paramref name="whole"/>: the whole chunk, in one piece),
        /// coalescing those closer than <paramref name="gap"/>.
        /// </summary>
        public RowGroupChunks((long Start, long Length)[][] pieces, bool[] whole, long gap, NativeBufferPool? pool = null)
        {
            _pool = pool;
            _pieces = [.. pieces.Select(p => new (long, int, long, int)[p.Length])];
            _whole = whole;
            (int Leaf, int Piece, long Start, long Length)[] all = [.. pieces.SelectMany((p, l) => p.Select((r, i) => (l, i, r.Start, r.Length)))];
            System.Array.Sort(all, (a, b) => a.Start.CompareTo(b.Start));
            long readStart = 0, readEnd = -1;
            List<(int Leaf, int Piece, long Start, long Length)> members = [];
            foreach (var piece in all)
            {
                var (_, _, start, length) = piece;
                if (members.Count > 0 && start - readEnd <= gap && Math.Max(readEnd, start + length) - readStart <= CoalesceLimit)
                    readEnd = Math.Max(readEnd, start + length);
                else
                {
                    Close();
                    readStart = start;
                    readEnd = start + length;
                }
                members.Add(piece);
            }
            Close();

            void Close()
            {
                if (members.Count == 0) return;
                long length = readEnd - readStart;
                NativeByteBuffer buffer;
                if (pool is not null) buffer = pool.Rent(length);
                else
                {
                    buffer = new NativeByteBuffer(0, pooled: true);
                    buffer.EnsureCapacity(length);
                }
                _reads.Add((readStart, length, buffer));
                foreach (var (leaf, index, start, size) in members) _pieces[leaf][index] = (start, _reads.Count - 1, start - readStart, (int)size);
                members.Clear();
            }
        }

        public IReadOnlyList<(long Start, long Length, NativeByteBuffer Buffer)> Reads => _reads;

        /// <summary>Whether the leaf's chunk was fetched whole (one piece).</summary>
        public bool IsWhole(int leaf) => _whole[leaf];

        public ReadOnlyMemory<byte> Chunk(int leaf)
        {
            var (_, read, offset, length) = _pieces[leaf][0];
            return _reads[read].Buffer.Memory.Slice((int)offset, length);
        }

        /// <summary>The leaf's pieces, each at its offset in the chunk starting at <paramref name="chunkStart"/>.</summary>
        public (long Start, ReadOnlyMemory<byte> Bytes)[] Pieces(int leaf, long chunkStart) =>
            [.. _pieces[leaf].Select(p => (p.Start - chunkStart, _reads[p.Read].Buffer.Memory.Slice((int)p.Offset, p.Length)))];

        public void Dispose()
        {
            foreach (var (_, _, buffer) in _reads)
            {
                if (_pool is null) buffer.Release();
                else _pool.Return(buffer);
            }
            _reads.Clear();
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        _schedulers.Complete();
        _buffers.Dispose();
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
