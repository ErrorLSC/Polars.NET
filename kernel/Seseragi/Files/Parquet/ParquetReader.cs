using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Network;
using Kaminari.Sazanami.Rows;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet.Codecs;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Reading;
using Kaminari.Seseragi.Files.Parquet.Schema;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Filters;
using Microsoft.FSharp.Core;

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
    private readonly ColumnSet _projected;           // the columns delivered
    private readonly LateFilter? _late;              // with FilterRows: the filter's program and the columns it reads
    private readonly int[] _candidates;              // the row groups to read, in order, after pruning by their statistics
    private readonly Pruning.PruneNode? _pruning;    // the compiled filter, when it can rule something out
    private readonly Pruning.LeafDomain[] _predicates; // the leaves the filter tests
    private readonly int[] _equalities;              // the leaves it tests for equality (Bloom filters answer those)
    private ReadUnit[]? _planned;                    // the units, once planned (on the first read)
    private readonly bool _oldParquetMr;
    private readonly NativeBufferPool _buffers;

    /// <summary>The pool of the chunk, window and scratch buffers (tests).</summary>
    internal NativeBufferPool Buffers => _buffers;
    private readonly ReadOnlyMemory<byte>? _memory;           // the source's bytes, when it lives in memory
    private readonly bool _fetch;                             // whether chunks are fetched before decoding (not a local source)
    private readonly ConcurrentExclusiveSchedulerPair _schedulers;
    private readonly ReadBudget? _shared;                     // the dataset's threads and buffers, when this reader is one of its files'

    /// <summary>
    /// The order of this reader's requests among those of the dataset's other readers (its file's position): requests
    /// beyond <see cref="ReadBudget.Requests"/> wait, the lowest order let in first.
    /// </summary>
    internal long RequestOrder { get; init; }

    /// <summary>
    /// A reader of the file; with <paramref name="shared"/>, one of several readers a dataset drives unit by unit
    /// (<see cref="PlanUnitsAsync"/>, <see cref="DecodeUnitAsync"/>) on its threads and buffers, which it owns.
    /// </summary>
    /// <param name="file">The file read.</param>
    /// <param name="options">The options.</param>
    /// <param name="shared">The dataset's threads and buffers, when one reads the file.</param>
    /// <param name="selection">
    /// What the options select from the file's columns, made before by another reader with the same column lists, row
    /// index column and type options of a file of the same shape (a dataset's readers of one schema); else selected here.
    /// </param>
    internal ParquetReader(ParquetFile file, ParquetReadOptions options, ReadBudget? shared = null, Selection? selection = null)
    {
        options.Validate();
        // The filter in the shapes pruning and lowering read (the LINQ translator gives some in others).
        if (options.Filter is { } given) options = options with { Filter = FilterShapes.normalize(given) };
        _file = file;
        _options = options;
        var (_, all) = file.Mapping(options);
        selection ??= Selection.Of(all, options);
        ColumnSelection = selection;
        _columns = selection.Projected.Columns;
        _projected = selection.Projected;
        _leaves = _projected.Leaves;
        if (options.Reading is ChunkReading.Auto or ChunkReading.Slice && file.Source is ILocalSource local && local.TryGetMemory(out ReadOnlyMemory<byte> memory))
            _memory = memory;
        _fetch = options.Reading switch
        {
            ChunkReading.Fetch => true,
            ChunkReading.Stream => false,
            _ => _memory is null && file.Source is not ILocalSource { SmallReadsAreCheap: true },
        };
        _shared = shared;
        _schedulers = shared?.Schedulers ?? new ConcurrentExclusiveSchedulerPair(TaskScheduler.Default, Math.Max(1, options.MaxDegreeOfParallelism));
        int[] rowGroups = options.RowGroups is { } groups ? [.. groups] : [.. Enumerable.Range(0, file.Thrift.RowGroups.Length)];
        foreach (int g in rowGroups)
        {
            if ((uint)g >= (uint)file.Thrift.RowGroups.Length)
                throw new ArgumentOutOfRangeException(nameof(options), g, $"The file has {file.Thrift.RowGroups.Length} row groups.");
        }
        _predicates = [];
        _equalities = [];
        if (options.Filter is { } filter)
        {
            var compiler = new Pruning.FilterCompiler(file.Tree, file.Thrift, options);
            _pruning = compiler.Compile(filter);
            _predicates = [.. compiler.Leaves];
            _equalities = [.. compiler.EqualityLeaves];
        }
        if (_pruning is { } pruning)
            rowGroups = [.. rowGroups.Where(g => (pruning.Evaluate(new Pruning.RowGroupStatistics(file.Thrift.RowGroups[g])) & Pruning.Outcomes.True) != 0)];
        FiltersExactly = options.FilterRows;
        if (options.FilterRows && options.Filter is { } rowFilter)
        {
            // Lowered over every column as it is read (types as mapped), whether selected or not.
            LoweredFilter lowered = FilterLowering.lower(new Apache.Arrow.Schema([.. all.Select(c => c.Field)], null), rowFilter);
            FiltersExactly = lowered.Exact;
            if (OptionModule.IsSome(lowered.Program))
            {
                FilterProgram program = lowered.Program.Value;
                if (program.Columns.Count > 0) _late = new LateFilter(program, all, _columns);
                else if (!Constant(program)) rowGroups = []; // false or null on every row
            }
        }
        _candidates = rowGroups;
        long total = file.Thrift.RowGroups.Sum(g => Math.Max(0, g.NumRows));
        if (options.Rows is { } rows && rows.End > total)
            throw new ArgumentOutOfRangeException(nameof(options), $"The rows selected reach row {rows.End - 1}; the file has {total} rows.");
        Schema = selection.Schema;
        _oldParquetMr = IsOldParquetMr(file.Thrift.CreatedBy);
        // Per row group in flight: its fetched chunks (a few), and per leaf a decompression scratch and a read window.
        int inFlight = options.MaxDegreeOfParallelism > 1 ? Math.Min(options.MaxDegreeOfParallelism, _candidates.Length) + 1 : 1;
        _buffers = shared?.Buffers ?? new NativeBufferPool((2 * (_leaves.Length + (_late?.Predicates.Leaves.Length ?? 0)) + 4) * inFlight);

        // A program reading no column is true, false or null on every row.
        static bool Constant(FilterProgram program)
        {
            Span<ulong> mask = stackalloc ulong[1];
            return program.Evaluate([], 1, mask) == 1;
        }
    }

    /// <summary>
    /// Whether the batches hold exactly the rows <see cref="ParquetReadOptions.Filter"/> keeps: with
    /// <see cref="ParquetReadOptions.FilterRows"/>, when there is no filter or every conjunct of it is evaluated here
    /// (the caller may then drop its own filter). False without <see cref="ParquetReadOptions.FilterRows"/> (rows are not
    /// filtered) and when some conjunct is not evaluated (the rows delivered are then a superset).
    /// </summary>
    public bool FiltersExactly { get; }

    /// <summary>
    /// From what share of the bytes of the other columns' pages read the pages holding a kept row make the other
    /// columns be read masked (<see cref="LateMaterialization.Auto"/>), below by ranges. Masked reading decodes every
    /// page of the rows read; reading by ranges skips the pages without a kept row, and pays per run and less per value
    /// skipped. Measured (10M rows, one thread and all cores): rows clustered in 5–30 % of a row group's pages, ranges
    /// 2–3 times faster; scattered rows leaving about 5 % of the bytes in pages without one, ranges 3–10 % faster; from
    /// rows in every page on, masked as fast or faster (by 10–30 % from a few percent of the rows kept).
    /// </summary>
    internal const double MaskedPagesFrom = 0.99;

    /// <summary>
    /// Without offset indexes (the pages are not known): from how many runs of kept rows per row read the other
    /// columns are read masked (where reading densely and filtering beat ranges, measured; masked beats dense).
    /// Contiguous selections (a range of a sorted column) are one run however many rows they keep.
    /// </summary>
    internal const double MaskedRunsFrom = 1.0 / 40;

    private int _denseUnits, _sparseUnits, _maskedUnits;
    private long _rowsKept;

    /// <summary>Row groups whose other columns were read densely, sparsely, or masked, after their filter (tests).</summary>
    internal int DenseUnits => Volatile.Read(ref _denseUnits);

    /// <inheritdoc cref="DenseUnits"/>
    internal int SparseUnits => Volatile.Read(ref _sparseUnits);

    /// <inheritdoc cref="DenseUnits"/>
    internal int MaskedUnits => Volatile.Read(ref _maskedUnits);

    /// <summary>Rows the filter kept, over the row groups read (tests).</summary>
    internal long RowsKept => Interlocked.Read(ref _rowsKept);

    /// <summary>Columns of a row group read together: their mappings, their leaves in column order, each column's leaves.</summary>
    /// <summary>The columns a reader selects, and the schema of its batches (the row index column first).</summary>
    /// <summary>What this reader selected (another reader of a file of the same shape and options may take it).</summary>
    internal Selection ColumnSelection { get; }

    internal sealed record Selection(ColumnSet Projected, Apache.Arrow.Schema Schema)
    {
        /// <exception cref="ArgumentException">The options name a column the file lacks, or a row index column of a column's name.</exception>
        public static Selection Of(ColumnMapping[] all, ParquetReadOptions options)
        {
            ColumnMapping[] columns = Select(all, options.Columns, options.ColumnPaths);
            Field[] fields = [.. columns.Select(c => c.Field)];
            if (options.RowIndexColumn is { } rowIndex)
            {
                if (fields.Any(f => f.Name == rowIndex))
                    throw new ArgumentException($"The row index column '{rowIndex}' has the name of a column read.", nameof(options));
                fields = [new Field(rowIndex, Int64Type.Default, nullable: false), .. fields];
            }
            return new Selection(new ColumnSet(columns), new Apache.Arrow.Schema(fields, null));
        }
    }

    internal sealed class ColumnSet
    {
        public ColumnSet(ColumnMapping[] columns)
        {
            Columns = columns;
            Leaves = [.. columns.SelectMany(c => c.Read.Leaves())];
            ColumnLeaves = new (int, int)[columns.Length];
            for (int c = 0, first = 0; c < columns.Length; c++)
            {
                int count = columns[c].Read.Leaves().Count();
                ColumnLeaves[c] = (first, count);
                first += count;
            }
        }

        public ColumnMapping[] Columns { get; }
        public ReadNode[] Leaves { get; }
        public (int First, int Count)[] ColumnLeaves { get; }
    }

    /// <summary>
    /// A filter for <see cref="ParquetReadOptions.FilterRows"/>: its program, the columns it reads (top-level columns
    /// pruned to the struct fields it tests), where each argument of the program lies in them, and how the selected
    /// columns relate to them.
    /// </summary>
    private sealed class LateFilter
    {
        public LateFilter(FilterProgram program, ColumnMapping[] all, ColumnMapping[] projected)
        {
            Program = program;
            // The columns read: whole when the program reads a top-level column, else pruned to its fields; file order.
            List<ColumnMapping> read = [];
            foreach (ColumnMapping column in all)
            {
                string name = column.Field.Name;
                string[][] below = [.. program.Columns.Where(c => c.Path[0] == name).Select(c => c.Path[1..])];
                if (below.Length == 0) continue;
                if (below.Any(b => b.Length == 0)) read.Add(column);
                else
                {
                    ReadNode pruned = ArrowMapping.Prune(column.Read, below, name);
                    read.Add(column with { Field = pruned.Field, Read = pruned });
                }
            }
            Predicates = new ColumnSet([.. read]);
            Arguments = [.. program.Columns.Select(c => (read.FindIndex(r => r.Field.Name == c.Path[0]), c.Path[1..]))];
            // A selected flat column the program reads whole is not decoded twice.
            Reuse = [.. projected.Select(p => p.IsFlat ? read.FindIndex(r => r.IsFlat && r.Field.Name == p.Field.Name) : -1)];
            Remaining = new ColumnSet([.. projected.Where((_, c) => Reuse[c] < 0)]);
            RemainingIndex = new int[projected.Length];
            for (int c = 0, r = 0; c < projected.Length; c++) RemainingIndex[c] = Reuse[c] < 0 ? r++ : -1;
            Nested = projected.Any(p => !p.IsFlat);
        }

        public FilterProgram Program { get; }

        /// <summary>The columns the program reads.</summary>
        public ColumnSet Predicates { get; }

        /// <summary>Per argument of the program: its column in <see cref="Predicates"/> and the struct fields below it.</summary>
        public (int Column, string[] Fields)[] Arguments { get; }

        /// <summary>Per selected column: its column in <see cref="Predicates"/> when it is decoded there already, else -1.</summary>
        public int[] Reuse { get; }

        /// <summary>The selected columns not in <see cref="Predicates"/>, and per selected column its index there (or -1).</summary>
        public ColumnSet Remaining { get; }

        /// <inheritdoc cref="Remaining"/>
        public int[] RemainingIndex { get; }

        /// <summary>Whether a selected column is nested (it cannot be filtered after decoding: those row groups are read sparsely).</summary>
        public bool Nested { get; }
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

        // Bloom filters of the leaves tested for equality, in the row groups statistics kept.
        if (_pruning is { } bloomed && _options.BloomFilters && _equalities.Length > 0)
        {
            BloomFilters blooms = _file.BloomFilters;
            (int, int)[] withBlooms = [.. selected.SelectMany(u => _equalities.Where(l => blooms.Has(u.Group, l)).Select(l => (u.Group, l)))];
            if (withBlooms.Length > 0)
            {
                await blooms.LoadAsync(withBlooms, useAsync, cancellationToken).ConfigureAwait(false);
                selected.RemoveAll(u => (bloomed.Evaluate(new Pruning.RowGroupStatistics(groups[u.Group], l => blooms.Get(u.Group, l))) & Pruning.Outcomes.True) == 0);
            }
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

        // With rows filtered here, MaxRows counts rows delivered (see Limit), not rows read.
        long budget = _late is null ? _options.MaxRows ?? long.MaxValue : long.MaxValue;
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
        // With rows filtered here every unit may be read by ranges: the rows kept are known only when it is decoded.
        ReadNode[] indexed = _late is null ? _leaves : [.. _leaves, .. _late.Predicates.Leaves];
        (int, int)[] chunks = [.. units.Where(u => u.Rows is not null || _late is not null).SelectMany(u => indexed.Select(l => (u.Group, l.Leaf!.LeafIndex)))];
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
        IEnumerable<RecordBatch> batches = InParallel(units) ? ReadParallel(units) : ReadSequential(units);
        if (_late is not null && _options.MaxRows is long most) batches = Limit(batches, most);
        foreach (RecordBatch batch in batches) yield return batch;
    }

    /// <summary>The first <paramref name="most"/> rows of the batches (the last batch cut to fit), then no more.</summary>
    private IEnumerable<RecordBatch> Limit(IEnumerable<RecordBatch> batches, long most)
    {
        if (most == 0) yield break;
        foreach (RecordBatch batch in batches)
        {
            if (batch.Length < most)
            {
                most -= batch.Length;
                yield return batch;
                continue;
            }
            yield return batch.Length == most ? batch : Cut(batch, (int)most);
            yield break;
        }
    }

    /// <summary>A batch's first rows, over its memory (disposing the cut releases it all).</summary>
    private RecordBatch Cut(RecordBatch batch, int rows) =>
        new(Schema, [.. batch.Arrays.Select(a => ArrowArrayFactory.Slice(a, 0, rows))], rows);

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

    internal sealed class OwningBatchStream(Apache.Arrow.Schema schema, IEnumerable<RecordBatch> batches) : Apache.Arrow.Ipc.IArrowArrayStream
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
        long most = _late is not null && _options.MaxRows is long m ? m : long.MaxValue;
        if (most == 0) yield break;
        await foreach (RecordBatch batch in (InParallel(units) ? ReadParallelAsync(units, cancellationToken) : ReadSequentialAsync(units, cancellationToken))
                           .ConfigureAwait(false))
        {
            if (batch.Length < most)
            {
                most -= batch.Length;
                yield return batch;
                continue;
            }
            yield return batch.Length == most ? batch : Cut(batch, (int)most);
            yield break;
        }
    }

    private async IAsyncEnumerable<RecordBatch> ReadSequentialAsync(ReadUnit[] units, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
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
        if (_late is not null) return await DecodeFilteredAsync(_late, unit, useAsync, parallel, cancellationToken).ConfigureAwait(false);
        using Decoded decoded = await DecodeSetAsync(_projected, unit.Group, unit.Rows, useAsync, parallel, cancellationToken).ConfigureAwait(false);
        return Assemble(decoded, unit.FirstRow);
    }

    /// <inheritdoc cref="DecodeRowGroupAsync(ReadUnit, bool, bool, CancellationToken)"/>
    internal Task<RecordBatch[]> DecodeRowGroupAsync(int g, bool useAsync, bool parallel, CancellationToken cancellationToken) =>
        DecodeRowGroupAsync(new ReadUnit(g, 0, null), useAsync, parallel, cancellationToken);

    /// <summary>
    /// Decodes a row group into batches, from fetched <paramref name="chunks"/> or (null) straight from the source;
    /// <paramref name="ranges"/> (internal) selects row ranges.
    /// </summary>
    internal IEnumerable<RecordBatch> Decode(int g, RowGroupChunks? chunks, IReadOnlyList<(long Start, long Count)>? ranges = null)
    {
        using Decoded decoded = DecodeColumnsAsync(_projected, g, chunks, ranges, parallel: false, default).GetAwaiter().GetResult();
        return Hand(Assemble(decoded, 0));
    }

    /// <summary>
    /// Fetches (when the source needs it) and decodes the columns of a set over a row group's <paramref name="ranges"/>
    /// (null: all rows), appending only the rows <paramref name="keeping"/> keeps where a column can.
    /// </summary>
    private async Task<Decoded> DecodeSetAsync(ColumnSet set, int g, IReadOnlyList<(long Start, long Count)>? ranges, bool useAsync, bool parallel,
        CancellationToken cancellationToken, Keeping? keeping = null)
    {
        using RowGroupChunks? chunks = _fetch && set.Leaves.Length > 0 ? await Fetch(set, g, ranges, useAsync, cancellationToken).ConfigureAwait(false) : null;
        // A fetch that waited resumes on the thread pool: decode back on the scheduler, within the threads the options
        // allow (else the units fetched ahead from a store would decode side by side, whatever the threads).
        if (chunks is not null && useAsync && TaskScheduler.Current != _schedulers.ConcurrentScheduler)
        {
            return await Task.Factory.StartNew(() => DecodeColumnsAsync(set, g, chunks, ranges, parallel, cancellationToken, keeping), cancellationToken,
                TaskCreationOptions.DenyChildAttach, _schedulers.ConcurrentScheduler).Unwrap().ConfigureAwait(false);
        }
        return await DecodeColumnsAsync(set, g, chunks, ranges, parallel, cancellationToken, keeping).ConfigureAwait(false);
    }

    /// <summary>The rows a filter kept, batch by batch: a mask over each batch's rows, and how many it keeps.</summary>
    private sealed record Keeping(ulong[][] Masks, int[] Kept);

    /// <summary>
    /// A row group's columns decoded batch by batch: an array per column and batch, each batch's length, and the rows
    /// of each batch (runs of consecutive rows, numbered in the row group); the columns that hold only the rows a
    /// <see cref="Keeping"/> kept. Disposing it disposes the arrays still held.
    /// </summary>
    private sealed class Decoded(IArrowArray[]?[] columns, int[] lengths, (long Start, int Count)[][] rows, bool[] filtered) : IDisposable
    {
        public IArrowArray?[][] Columns { get; } = [.. columns.Select(c => (IArrowArray?[])c!)];
        public int[] Lengths { get; } = lengths;
        public (long Start, int Count)[][] Rows { get; } = rows;
        public bool[] Filtered { get; } = filtered;
        public int Batches => Lengths.Length;

        /// <summary>Takes the array of column <paramref name="c"/> in batch <paramref name="b"/> out (the caller disposes it).</summary>
        public IArrowArray Take(int c, int b)
        {
            IArrowArray array = Columns[c][b]!;
            Columns[c][b] = null;
            return array;
        }

        public void Dispose()
        {
            foreach (IArrowArray?[] column in Columns)
            {
                for (int b = 0; b < column.Length; b++)
                {
                    column[b]?.Dispose();
                    column[b] = null;
                }
            }
        }
    }

    /// <summary>The decoded batches of the selected columns as record batches (with the row index column when asked).</summary>
    private RecordBatch[] Assemble(Decoded decoded, long firstRow)
    {
        var batches = new RecordBatch[decoded.Batches];
        for (int b = 0; b < batches.Length; b++)
        {
            int batch = b;
            IArrowArray[] columns = [.. Enumerable.Range(0, decoded.Columns.Length).Select(c => decoded.Take(c, batch))];
            if (_options.RowIndexColumn is not null) columns = [RowIndex(decoded.Rows[b], firstRow + _options.RowIndexOffset, decoded.Lengths[b]), .. columns];
            batches[b] = new RecordBatch(Schema, columns, decoded.Lengths[b]);
        }
        return batches;
    }

    /// <summary>
    /// A row group read through the filter (<see cref="ParquetReadOptions.FilterRows"/>): the columns it reads first,
    /// the filter evaluated batch by batch, then the selected columns for the rows it keeps, densely (the same batches,
    /// filtered) or sparsely (only those rows, read by ranges).
    /// </summary>
    private async Task<RecordBatch[]> DecodeFilteredAsync(LateFilter late, ReadUnit unit, bool useAsync, bool parallel, CancellationToken cancellationToken)
    {
        using Decoded predicates = await DecodeSetAsync(late.Predicates, unit.Group, unit.Rows, useAsync, parallel, cancellationToken).ConfigureAwait(false);
        var masks = new ulong[predicates.Batches][];
        var kept = new int[predicates.Batches];
        long read = 0, total = 0;
        var arguments = new IArrowArray[late.Arguments.Length];
        for (int b = 0; b < predicates.Batches; b++)
        {
            int length = predicates.Lengths[b];
            for (int a = 0; a < arguments.Length; a++)
                arguments[a] = Argument(predicates.Columns[late.Arguments[a].Column][b]!, late.Arguments[a].Fields);
            masks[b] = new ulong[Masks.Words(length)];
            kept[b] = late.Program.Evaluate(arguments, length, masks[b]);
            read += length;
            total += kept[b];
        }
        Interlocked.Add(ref _rowsKept, total);
        if (total == 0) return [];

        LateMaterialization mode = late.Nested ? LateMaterialization.Sparse : _options.LateMaterialization switch
        {
            LateMaterialization.Auto => ReadMasked(late, unit.Group, predicates.Rows, masks, read) ? LateMaterialization.Masked : LateMaterialization.Sparse,
            var chosen => chosen,
        };
        if (mode == LateMaterialization.Sparse)
        {
            Interlocked.Increment(ref _sparseUnits);
            using Decoded rows = await DecodeSetAsync(_projected, unit.Group, Selected(predicates.Rows, masks), useAsync, parallel, cancellationToken).ConfigureAwait(false);
            return Assemble(rows, unit.FirstRow);
        }

        bool masked = mode == LateMaterialization.Masked;
        Interlocked.Increment(ref masked ? ref _maskedUnits : ref _denseUnits);
        using Decoded others = await DecodeSetAsync(late.Remaining, unit.Group, unit.Rows, useAsync, parallel, cancellationToken,
            masked ? new Keeping(masks, kept) : null).ConfigureAwait(false);
        List<RecordBatch> batches = [];
        try
        {
            for (int b = 0; b < predicates.Batches; b++)
            {
                if (kept[b] == 0) continue;
                bool all = kept[b] == predicates.Lengths[b];
                var columns = new List<IArrowArray>(_columns.Length + 1);
                try
                {
                    // The row index first (as the schema has it).
                    if (_options.RowIndexColumn is not null)
                    {
                        IArrowArray index = RowIndex(predicates.Rows[b], unit.FirstRow + _options.RowIndexOffset, predicates.Lengths[b]);
                        if (all) columns.Add(index);
                        else
                        {
                            using (index) columns.Add(ArrowFilter.Filter(index, masks[b]));
                        }
                    }
                    for (int c = 0; c < _columns.Length; c++)
                    {
                        // The selected columns the filter read come from its batches, the others from theirs (those
                        // decoded masked hold the rows kept already).
                        IArrowArray? source = late.Reuse[c] >= 0 ? predicates.Columns[late.Reuse[c]][b] : others.Columns[late.RemainingIndex[c]][b];
                        columns.Add(all || late.Reuse[c] < 0 && others.Filtered[late.RemainingIndex[c]]
                            ? late.Reuse[c] >= 0 ? predicates.Take(late.Reuse[c], b) : others.Take(late.RemainingIndex[c], b)
                            : ArrowFilter.Filter(source!, masks[b]));
                    }
                }
                catch
                {
                    foreach (IArrowArray column in columns) column.Dispose();
                    throw;
                }
                batches.Add(new RecordBatch(Schema, columns, kept[b]));
                // Filtered copies (and the memory they share) are independent of the batch they came from.
                if (!all)
                {
                    for (int c = 0; c < predicates.Columns.Length; c++) DisposeAt(predicates, c, b);
                    for (int c = 0; c < others.Columns.Length; c++) DisposeAt(others, c, b);
                }
            }
            return [.. batches];
        }
        catch
        {
            foreach (RecordBatch batch in batches) batch.Dispose();
            throw;
        }

        static void DisposeAt(Decoded decoded, int c, int b)
        {
            decoded.Columns[c][b]?.Dispose();
            decoded.Columns[c][b] = null;
        }
    }

    /// <summary>
    /// An argument of the filter's program: a column of the batch, or a struct field below it. A field of a null struct
    /// is null in the arrays read here (a leaf's slot is null wherever a level above it is, required or not), as the IR
    /// reads a field of a null struct.
    /// </summary>
    private static IArrowArray Argument(IArrowArray column, string[] fields)
    {
        IArrowArray current = column;
        foreach (string name in fields)
        {
            var parent = (StructArray)current;
            current = parent.Fields[((StructType)parent.Data.DataType).GetFieldIndex(name)];
        }
        return current;
    }

    /// <summary>
    /// The rows the filter kept, numbered in the row group: each batch's mask over its rows (<paramref name="rows"/>,
    /// the runs of rows of each batch, as <see cref="Decoded.Rows"/>).
    /// </summary>
    internal static RowRanges Selected((long Start, int Count)[][] rows, ulong[][] masks)
    {
        List<(long Start, long Count)> kept = [];
        for (int b = 0; b < rows.Length; b++)
        {
            int at = 0; // the batch's slot of the run's first row
            foreach (var (start, count) in rows[b])
            {
                for (int i = 0; i < count;)
                {
                    int first = NextBit(masks[b], at + i, at + count, set: true);
                    if (first >= at + count) break;
                    int end = NextBit(masks[b], first, at + count, set: false);
                    kept.Add((start + first - at, end - first));
                    i = end - at;
                }
                at += count;
            }
        }
        return RowRanges.Of(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(kept));
    }

    /// <summary>The first slot in [<paramref name="from"/>, <paramref name="end"/>) whose bit is set (or clear), else <paramref name="end"/>.</summary>
    private static int NextBit(ulong[] mask, int from, int end, bool set)
    {
        for (int w = from >> 6; (w << 6) < end; w++)
        {
            ulong word = set ? mask[w] : ~mask[w];
            if (w == from >> 6) word &= ulong.MaxValue << (from & 63);
            if (word != 0) return Math.Min(end, (w << 6) + System.Numerics.BitOperations.TrailingZeroCount(word));
        }
        return end;
    }

    /// <summary>
    /// Whether the other columns of a row group are read masked rather than by ranges (<see cref="LateMaterialization.Auto"/>):
    /// by the bytes of their pages holding a kept row (<see cref="MaskedPagesFrom"/>) when every one has an offset
    /// index, else by the runs of kept rows (<see cref="MaskedRunsFrom"/>). No other column: masked (the filter's
    /// columns are reused).
    /// </summary>
    private bool ReadMasked(LateFilter late, int g, (long Start, int Count)[][] rows, ulong[][] masks, long read)
    {
        long keptBytes = 0, readBytes = 0;
        foreach (ReadNode leaf in late.Remaining.Leaves)
        {
            if (_file.PageIndexes.Get(g, leaf.Leaf!.LeafIndex, withColumns: false) is not { } index)
            {
                long enough = (long)Math.Ceiling(read * MaskedRunsFrom);
                return Runs(rows, masks, enough) >= enough;
            }
            var (kept, all) = PageBytes(index.FirstRows, index.Sizes, rows, masks);
            keptBytes += kept;
            readBytes += all;
        }
        return keptBytes >= readBytes * MaskedPagesFrom;
    }

    /// <summary>
    /// The bytes of a chunk's pages (their first rows, ascending from 0, and sizes) holding a row kept (by the masks
    /// over the batches' <paramref name="rows"/>, as <see cref="Selected"/> reads them), and of its pages holding a row
    /// read; one pass over the pages and the masks.
    /// </summary>
    internal static (long Kept, long Read) PageBytes(long[] firstRows, int[] sizes, (long Start, int Count)[][] rows, ulong[][] masks)
    {
        long kept = 0, read = 0;
        int page = 0, lastKept = -1, lastRead = -1, pages = firstRows.Length;
        for (int b = 0; b < rows.Length; b++)
        {
            int at = 0; // the batch's slot of the run's first row
            foreach (var (start, count) in rows[b])
            {
                if (count == 0) continue;
                long end = start + count;
                while (page + 1 < pages && firstRows[page + 1] <= start) page++;
                for (int p = Math.Max(page, lastRead + 1); p < pages && firstRows[p] < end; p++)
                {
                    read += sizes[p];
                    lastRead = p;
                }
                // Each kept row found marks its page; the search goes on from the next page's first row.
                for (int slot = at; ;)
                {
                    int first = NextBit(masks[b], slot, at + count, set: true);
                    if (first >= at + count) break;
                    long row = start + first - at;
                    while (page + 1 < pages && firstRows[page + 1] <= row) page++;
                    if (page > lastKept)
                    {
                        kept += sizes[page];
                        lastKept = page;
                    }
                    if (page + 1 >= pages || firstRows[page + 1] >= end) break;
                    slot = at + (int)(firstRows[page + 1] - start);
                }
                at += count;
            }
        }
        return (kept, read);
    }

    /// <summary>
    /// The number of ranges <see cref="Selected"/> makes of the same masks (runs of kept rows, those touching across
    /// batches merged), counted up to <paramref name="enough"/> at least: a run starts at each kept row whose row before
    /// it was not kept, found a word of the mask at a time.
    /// </summary>
    internal static long Runs((long Start, int Count)[][] rows, ulong[][] masks, long enough)
    {
        long runs = 0, end = -1; // the row after the last kept row, when it ends a batch's run of rows
        for (int b = 0; b < rows.Length && runs < enough; b++)
        {
            ulong[] mask = masks[b];
            int at = 0; // the batch's slot of the run's first row
            foreach (var (start, count) in rows[b])
            {
                // The bit of the slot before, for the first slot of the run of rows: whether the row before it was kept.
                ulong before = end == start ? 1UL : 0;
                for (int from = at, stop = at + count; from < stop;)
                {
                    int bit = from & 63, take = Math.Min(64 - bit, stop - from);
                    ulong word = mask[from >> 6] >> bit;
                    if (take < 64) word &= (1UL << take) - 1;
                    runs += System.Numerics.BitOperations.PopCount(word & ~(word << 1 | before));
                    before = word >> (take - 1) & 1;
                    from += take;
                }
                if (before != 0) end = start + count;
                at += count;
            }
        }
        return runs;
    }

    /// <summary>
    /// Decodes the columns of <paramref name="set"/> of a row group, each from its first page to its last (side by side
    /// when <paramref name="parallel"/>), batch by batch. Of several failing columns the first one's error is reported,
    /// whatever the order they ran in.
    /// </summary>
    private async Task<Decoded> DecodeColumnsAsync(ColumnSet set, int g, RowGroupChunks? chunks, IReadOnlyList<(long Start, long Count)>? ranges,
        bool parallel, CancellationToken cancellationToken, Keeping? keeping = null)
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
        IEnumerable<Step> plan = Plan(rows, ranges);
        (long Start, long Length)[] at = ChunkRanges(set, g);
        // A hold on the file's mapping for the decode of this row group (the reader may outlive a disposed file), taken
        // only when a chunk of it is read from the mapping: making and dropping a mapping is not free (measured on
        // compressed files read on many threads) and buys nothing for chunks read the ordinary way.
        long mapStart = 0;
        MappedFile? map = null;
        if (chunks is null && _memory is null && _options.MemoryMap != ParquetMemoryMap.Off && _file.Source is ILocalSource local
            && set.Leaves.Any(leaf => FromMapping(group.Columns[leaf.Leaf!.LeafIndex].MetaData!, leaf.Leaf)))
        {
            map = local.Map(out mapStart);
            if (map is not null && !map.TryAcquire()) map = null;
        }
        var readers = new LeafReader[set.Leaves.Length];
        var builders = new NativeColumnBuilder[set.Columns.Length];
        var arrays = new IArrowArray[]?[set.Columns.Length];
        Dictionary<ReadNode, LeafReader> byNode = [];
        try
        {
            for (int l = 0; l < set.Leaves.Length; l++)
            {
                ReadNode leaf = set.Leaves[l];
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
                if (_file.Decryptor?.ForChunk(group.Columns[leaf.Leaf.LeafIndex], group.Ordinal ?? g, leaf.Leaf.LeafIndex, string.Join('.', leaf.Leaf.Path)) is { } crypto)
                    pages.UseDecryption(crypto);
                readers[l] = LeafReaders.Create(pages, leaf.Leaf, leaf.LeafType!, meta.NumValues);
                readers[l].UncompressedBytes = meta.TotalUncompressedSize;
                readers[l].LevelsAsBits = _options.LevelsAsBits;
                // Measured: staged non-temporal writes of strings win when threads compete for memory bandwidth and
                // lose a few percent on one thread.
                if (readers[l].Builder is NativeBinaryViewBuilder views)
                    views.WritesPastCaches = _shared?.WritesPastCaches ?? _options.MaxDegreeOfParallelism > 1 && (_candidates.Length > 1 || set.Columns.Length > 1);
                byNode[leaf] = readers[l];
            }
            foreach (ColumnMapping column in set.Columns) ConfigureLeaves(column.Read, byNode, keepLevels: !column.IsFlat, fixedSizeList: null);
            for (int c = 0; c < set.Columns.Length; c++) builders[c] = NestedAssembler.CreateBuilder(set.Columns[c].Read, byNode);
            // The columns whose leaves all append only the rows kept (as their readers are configured); the others are filtered after.
            var filtered = new bool[set.Columns.Length];
            for (int c = 0; c < filtered.Length; c++)
                filtered[c] = keeping is not null && set.Columns[c].IsFlat && CanKeep(readers.AsSpan(set.ColumnLeaves[c].First, set.ColumnLeaves[c].Count));

            if (parallel && set.Columns.Length > 1)
            {
                var tasks = new Task<IArrowArray[]>[set.Columns.Length];
                for (int c = 0; c < tasks.Length; c++)
                {
                    int column = c;
                    tasks[c] = Task.Factory.StartNew(
                        () =>
                        {
                            _shared?.DecodeStarts();
                            try
                            {
                                return DecodeColumn(set, column, readers, builders[column], byNode, plan, rows, ranges is null, filtered[column] ? keeping : null, cancellationToken);
                            }
                            finally
                            {
                                _shared?.DecodeEnds();
                            }
                        },
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
                _shared?.DecodeStarts();
                try
                {
                    for (int c = 0; c < set.Columns.Length; c++)
                        arrays[c] = DecodeColumn(set, c, readers, builders[c], byNode, plan, rows, ranges is null, filtered[c] ? keeping : null, cancellationToken);
                }
                finally
                {
                    _shared?.DecodeEnds();
                }
            }

            List<int> lengths = [];
            List<(long Start, int Count)[]> runs = [];
            int length = 0;
            long row = 0; // the row group's number of the step's first row
            List<(long Start, int Count)> rowsOfBatch = [];
            foreach (var (read, n, export, _) in plan)
            {
                if (read)
                {
                    rowsOfBatch.Add((row, n));
                    length += n;
                }
                row += n;
                if (!export) continue;
                lengths.Add(length);
                runs.Add([.. rowsOfBatch]);
                rowsOfBatch.Clear();
                length = 0;
            }
            return new Decoded(arrays, [.. lengths], [.. runs], filtered);
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

        static bool CanKeep(ReadOnlySpan<LeafReader> leaves)
        {
            foreach (LeafReader r in leaves)
            {
                if (!r.CanKeep) return false;
            }
            return true;
        }
    }

    /// <summary>The numbers of the rows of a batch (runs of consecutive rows) as an Int64 column.</summary>
    private static IArrowArray RowIndex((long Start, int Count)[] runs, long firstRow, int length) => RowNumbers.Runs(runs, firstRow, length);

    /// <summary>
    /// Whether a column chunk is decoded from the file's mapping: all of them when it is on; when automatic, the
    /// Snappy-compressed ones and those uncompressed and of a fixed-width physical type (measured: the mapping pays
    /// there, and costs on uncompressed strings and Zstandard chunks read on many threads).
    /// </summary>
    private PageReader Mapped(ReadOnlyMemory<byte> chunk, ColumnMetaData meta)
    {
        Interlocked.Increment(ref _mappedChunks);
        return new PageReader(chunk, meta.Codec, _options.VerifyChecksums, _buffers);
    }

    private bool FromMapping(ColumnMetaData meta, SchemaNode leaf) => _options.MemoryMap == ParquetMemoryMap.On
        || meta.Codec == CompressionCodec.Snappy
        || meta.Codec == CompressionCodec.Uncompressed && leaf.Element.Type is ParquetType.Boolean or ParquetType.Int32 or ParquetType.Int64
            or ParquetType.Int96 or ParquetType.Float or ParquetType.Double or ParquetType.FixedLenByteArray;

    /// <summary>
    /// The rows to read and to skip, in order (<paramref name="ranges"/>, all rows when null), and where a batch ends:
    /// a batch gathers the rows read across skips until it holds the batch size, so a scattered selection still gives
    /// full batches.
    /// </summary>
    /// <remarks>
    /// The ranges are checked at once. When they make few steps for their number, the steps are made once, each read
    /// step of a batch after its first then reserving nothing (its first reserves room for the whole batch); else they
    /// are made as they are taken (enumerated once per column), so that a row count the data does not hold (a footer's,
    /// read or skipped) costs no memory before decoding finds that out.
    /// </remarks>
    private IEnumerable<Step> Plan(long rows, IReadOnlyList<(long Start, long Count)>? ranges)
    {
        // At most the steps the ranges make, counted while within the limit: the skips of a range are at most 2^32 + 1,
        // its whole batches are added once below the limit, so no sum overflows.
        long at = 0, steps = 1, limit = 4L * (ranges?.Count ?? 0) + 1024;
        bool few = ranges is not null;
        int batchSize = _options.BatchSize;
        foreach (var (start, count) in ranges ?? [(0, rows)])
        {
            if (start < at || count < 0 || start + count > rows) throw new ArgumentOutOfRangeException(nameof(ranges), "Row ranges must be ordered and inside the row group.");
            long skips = (start - at) / int.MaxValue + 1, reads = count / batchSize;
            few = few && reads <= limit && (steps += skips + reads + 2) <= limit;
            at = start + count;
        }
        if (!few) return Steps(rows, ranges, batchSize);

        Step[] plan = [.. Steps(rows, ranges, batchSize)];
        for (int i = 0, first = -1, batch = 0; i < plan.Length; i++)
        {
            if (plan[i].Read)
            {
                if (first < 0) first = i;
                else plan[i] = plan[i] with { Reserve = 0 };
                batch += plan[i].Count;
            }
            if (!plan[i].Export) continue;
            if (first >= 0) plan[first] = plan[first] with { Reserve = batch };
            first = -1;
            batch = 0;
        }
        return plan;

        static IEnumerable<Step> Steps(long rows, IReadOnlyList<(long Start, long Count)>? ranges, int batchSize)
        {
            long at = 0;
            int pending = 0; // rows read into the batch being gathered
            foreach (var (start, count) in ranges ?? [(0, rows)])
            {
                for (long skip = start - at; skip > 0; skip -= int.MaxValue) yield return new(false, (int)Math.Min(skip, int.MaxValue), false, 0);
                for (long left = count; left > 0;)
                {
                    int n = (int)Math.Min(left, batchSize - pending);
                    left -= n;
                    pending += n;
                    bool full = pending == batchSize;
                    yield return new(true, n, full, n);
                    if (full) pending = 0;
                }
                at = start + count;
            }
            if (pending > 0) yield return new(false, 0, true, 0);
        }
    }

    /// <summary>
    /// A step of a plan: <paramref name="Count"/> rows read or skipped, after room is reserved for
    /// <paramref name="Reserve"/> rows (none: reserved before), and whether a batch ends after it.
    /// </summary>
    private readonly record struct Step(bool Read, int Count, bool Export, int Reserve);

    /// <summary>
    /// Decodes one column of a row group (its leaves side by side, record by record) through the whole
    /// <paramref name="plan"/>: an array per batch, each over memory of its own; with <paramref name="keeping"/>, of
    /// the rows it keeps only.
    /// </summary>
    private static IArrowArray[] DecodeColumn(ColumnSet set, int c, LeafReader[] readers, NativeColumnBuilder builder, Dictionary<ReadNode, LeafReader> byNode,
        IEnumerable<Step> plan, long rows, bool finish, Keeping? keeping, CancellationToken cancellationToken)
    {
        ColumnMapping column = set.Columns[c];
        var (first, count) = set.ColumnLeaves[c];
        ReadOnlySpan<LeafReader> leaves = readers.AsSpan(first, count);
        List<IArrowArray> arrays = [];
        try
        {
            int n = 0, batch = 0; // rows read into the batch, and its number
            foreach (var (read, steps, export, reserve) in plan)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (read)
                {
                    foreach (LeafReader r in leaves)
                    {
                        if (keeping is not null) r.Keep(keeping.Masks[batch], n);
                        if (reserve > 0) r.Reserve(keeping is null ? reserve : Math.Min(reserve, keeping.Kept[batch]), rows);
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
                int expected = keeping is null ? n : keeping.Kept[batch];
                if (builder.Length != expected)
                    throw new ParquetFormatException($"Column '{column.Field.Name}' holds {builder.Length} rows of the {expected} read.");
                batch++;
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
    private (long Start, long Length)[] ChunkRanges(ColumnSet set, int g)
    {
        RowGroup group = _file.Thrift.RowGroups[g];
        var ranges = new (long Start, long Length)[set.Leaves.Length];
        for (int l = 0; l < set.Leaves.Length; l++)
        {
            SchemaNode leaf = set.Leaves[l].Leaf!;
            ColumnChunk chunk = group.Columns[leaf.LeafIndex];
            string name = string.Join('.', leaf.Path);
            if (chunk.FilePath is not null) throw new ParquetFormatException($"Column '{name}' of row group {g} is stored in another file ('{chunk.FilePath}').");
            ColumnMetaData meta = chunk.MetaData ?? throw new ParquetFormatException(
                $"Column '{name}' is encrypted with a key of its own: give it in ParquetDecryptionOptions.ColumnKeys (by its dot-separated path).");
            if (!PageCodecs.CanDecompress(meta.Codec)) throw new ParquetFormatException($"Column '{name}' is compressed with {PageCodecs.Name(meta.Codec)}, which is not supported.");
            ranges[l] = ChunkRange(meta, _file.DataEnd, name, _oldParquetMr);
        }
        return ranges;
    }

    /// <summary>Reads the selected column chunks of a row group, coalescing nearby ones into one read.</summary>
    internal ValueTask<RowGroupChunks> Fetch(int g, bool useAsync, CancellationToken ct) => Fetch(_projected, g, null, useAsync, ct);

    /// <summary>
    /// Reads the selected column chunks of a row group, coalescing nearby ones into one read; where rows are skipped and
    /// a chunk has a usable offset index, only its dictionary page and the pages holding selected rows (pages closer
    /// than <see cref="ParquetReadOptions.FetchGap"/> come in the same read, unused).
    /// </summary>
    private async ValueTask<RowGroupChunks> Fetch(ColumnSet set, int g, IReadOnlyList<(long Start, long Count)>? rows, bool useAsync, CancellationToken ct)
    {
        (long Start, long Length)[] at = ChunkRanges(set, g);
        var pieces = new (long Start, long Length)[at.Length][];
        var whole = new bool[at.Length];
        for (int l = 0; l < at.Length; l++)
        {
            ChunkPageIndex? index = rows is null ? null : _file.PageIndexes.Get(g, set.Leaves[l].Leaf!.LeafIndex, withColumns: false);
            pieces[l] = index is null ? [at[l]] : Pieces(index, at[l], Needed(index, rows!));
            whole[l] = index is null;
        }
        var chunks = new RowGroupChunks(pieces, whole, _options.FetchGap, _buffers);
        try
        {
            RequestGate? gate = useAsync ? _shared?.Requests : null;
            foreach (var (start, length, buffer) in chunks.Reads)
            {
                if (gate is null)
                {
                    await ParquetFile.ReadExactlyAsync(_file.Source, start, buffer.Memory[..(int)length], useAsync, ct).ConfigureAwait(false);
                    continue;
                }
                using RequestGate.Lease lease = await gate.EnterAsync(RequestOrder, ct).ConfigureAwait(false);
                await ParquetFile.ReadExactlyAsync(_file.Source, start, buffer.Memory[..(int)length], useAsync, ct).ConfigureAwait(false);
            }
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
    /// below the file's magic are not trusted (some writers leave data_page_offset 0 after a dictionary page). A chunk
    /// must end before the footer (<paramref name="dataEnd"/>): one reaching into it belongs to a file cut short or
    /// to another file joined before this one.
    /// </summary>
    internal static (long Start, long Length) ChunkRange(ColumnMetaData meta, long dataEnd, string name, bool padDictionaryHeader)
    {
        long start = meta.DataPageOffset;
        if (meta.DictionaryPageOffset is long d && d >= 4 && (d < start || start < 4)) start = d;
        long length = meta.TotalCompressedSize;
        if (length == 0) return (0, 0);
        if (start < 4 || length < 0 || length > int.MaxValue || start > dataEnd - length)
            throw new ParquetFormatException($"Column '{name}' gives a chunk of {length} bytes at {start}, outside the file's data (bytes 4 to {dataEnd}, "
                + "before the footer): the file is cut short or not a single Parquet file.");
        // PARQUET-816: parquet-mr before 1.2.9 left the dictionary page header out of the chunk size; as the reference
        // readers do, read up to 100 more bytes (the page sizes then decide).
        if (padDictionaryHeader)
            length = Math.Min(length + 100, Math.Min(int.MaxValue, dataEnd - start));
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

    // ---- driven by a dataset ---------------------------------------------------------------------------------------

    /// <summary>Plans the units (row groups with their rows) and tells how many there are.</summary>
    internal async ValueTask<int> PlanUnitsAsync(bool useAsync, CancellationToken cancellationToken) =>
        (useAsync ? await UnitsAsync(cancellationToken).ConfigureAwait(false) : Units()).Length;

    /// <summary>The uncompressed bytes of unit <paramref name="index"/>'s row group (what it holds in memory, about).</summary>
    internal long UnitBytes(int index) => Math.Max(0, _file.Thrift.RowGroups[_planned![index].Group].TotalByteSize);

    /// <summary>The rows unit <paramref name="index"/> reads, before any row filter.</summary>
    internal long UnitRows(int index) => _planned![index].Rows?.RowCount ?? Math.Max(0, _file.Thrift.RowGroups[_planned[index].Group].NumRows);

    /// <summary>Decodes unit <paramref name="index"/> (its columns side by side on the scheduler when <paramref name="parallel"/>).</summary>
    internal Task<RecordBatch[]> DecodeUnitAsync(int index, bool parallel, CancellationToken cancellationToken) =>
        DecodeRowGroupAsync(_planned![index], useAsync: true, parallel, cancellationToken);

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_shared is not null) return; // the dataset's threads and buffers
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
