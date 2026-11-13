using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Helpers;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet.Codecs;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Reading;
using Kaminari.Seseragi.Files.Parquet.Schema;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Reads the row groups of a <see cref="ParquetFile"/> as Arrow record batches in native memory, row group by row
/// group (a batch never spans two). Each row group's selected column chunks are fetched with few ranged reads,
/// then decoded page by page.
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
    private readonly int[] _rowGroups;
    private readonly bool _oldParquetMr;
    private readonly NativeBufferPool _buffers;

    internal ParquetReader(ParquetFile file, ParquetReadOptions options)
    {
        options.Validate();
        _file = file;
        _options = options;
        var (_, all) = ArrowMapping.Map(file.Tree, file.Thrift, options);
        _columns = Select(all, options.Columns, options.ColumnPaths);
        _leaves = [.. _columns.SelectMany(c => c.Read.Leaves())];
        _rowGroups = options.RowGroups is { } groups ? [.. groups] : [.. Enumerable.Range(0, file.Thrift.RowGroups.Length)];
        foreach (int g in _rowGroups)
        {
            if ((uint)g >= (uint)file.Thrift.RowGroups.Length)
                throw new ArgumentOutOfRangeException(nameof(options), g, $"The file has {file.Thrift.RowGroups.Length} row groups.");
        }
        Schema = new Apache.Arrow.Schema([.. _columns.Select(c => c.Field)], null);
        _oldParquetMr = IsOldParquetMr(file.Thrift.CreatedBy);
        // Per row group in flight: its fetched chunks (a few) and one decompression scratch per leaf.
        int inFlight = options.MaxDegreeOfParallelism > 1 ? Math.Min(options.MaxDegreeOfParallelism, _rowGroups.Length) + 1 : 1;
        _buffers = new NativeBufferPool((_leaves.Length + 4) * inFlight);
    }

    /// <summary>The Arrow schema of the batches (the selected columns, in file order).</summary>
    public Apache.Arrow.Schema Schema { get; }

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
        if (_options.MaxDegreeOfParallelism > 1 && _rowGroups.Length > 1) return ReadParallel();
        return ReadSequential();
    }

    private IEnumerable<RecordBatch> ReadSequential()
    {
        foreach (int g in _rowGroups)
        {
            using RowGroupChunks chunks = Fetch(g, useAsync: false, default).AsTask().GetAwaiter().GetResult();
            foreach (RecordBatch batch in Decode(g, chunks)) yield return batch;
        }
    }

    /// <summary>
    /// The batches as an Arrow stream that owns them, for handing to another library (Polars' <c>FromArrowStream</c>,
    /// a C stream export): each batch is disposed when the next one is read or the stream is disposed. A consumer
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
        if (_options.MaxDegreeOfParallelism > 1 && _rowGroups.Length > 1)
        {
            await foreach (RecordBatch batch in ReadParallelAsync(cancellationToken).ConfigureAwait(false)) yield return batch;
            yield break;
        }
        foreach (int g in _rowGroups)
        {
            using RowGroupChunks chunks = await Fetch(g, useAsync: true, cancellationToken).ConfigureAwait(false);
            foreach (RecordBatch batch in Decode(g, chunks))
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return batch;
            }
        }
    }

    /// <summary>
    /// Row groups decoded side by side, a window of <see cref="ParquetReadOptions.MaxDegreeOfParallelism"/> of them
    /// ahead of the consumer, delivered in order. A consumer that stops early cancels the rest and frees what they
    /// decoded.
    /// </summary>
    private IEnumerable<RecordBatch> ReadParallel()
    {
        using var window = new RowGroupWindow(this, CancellationToken.None);
        for (int i = 0; i < _rowGroups.Length; i++)
        {
            foreach (RecordBatch batch in window.Take(i).GetAwaiter().GetResult()) yield return batch;
        }
    }

    private async IAsyncEnumerable<RecordBatch> ReadParallelAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var window = new RowGroupWindow(this, cancellationToken);
        for (int i = 0; i < _rowGroups.Length; i++)
        {
            foreach (RecordBatch batch in await window.Take(i).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return batch;
            }
        }
    }

    /// <summary>The row groups being decoded ahead of the consumer, and the batches of the one it is taking.</summary>
    private sealed class RowGroupWindow(ParquetReader reader, CancellationToken cancellationToken) : IDisposable
    {
        private readonly CancellationTokenSource _cancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        private readonly Task<RecordBatch[]>?[] _tasks = new Task<RecordBatch[]>?[reader._rowGroups.Length];
        private RecordBatch[]? _taking;
        private int _taken; // batches of _taking already handed out
        private int _started;

        /// <summary>Starts the row groups up to the window's end and waits for the <paramref name="index"/>th.</summary>
        public async Task<IEnumerable<RecordBatch>> Take(int index)
        {
            for (; _started < _tasks.Length && _started < index + reader._options.MaxDegreeOfParallelism; _started++)
            {
                int g = reader._rowGroups[_started];
                CancellationToken token = _cancel.Token;
                _tasks[_started] = Task.Run(() => reader.DecodeRowGroup(g, token), token);
            }
            RecordBatch[] batches = await _tasks[index]!.ConfigureAwait(false);
            _tasks[index] = null;
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

    /// <summary>Fetches and decodes a whole row group (the unit parallel reading schedules).</summary>
    internal RecordBatch[] DecodeRowGroup(int g, CancellationToken cancellationToken)
    {
        using RowGroupChunks chunks = Fetch(g, useAsync: false, cancellationToken).AsTask().GetAwaiter().GetResult();
        List<RecordBatch> batches = [];
        try
        {
            foreach (RecordBatch batch in Decode(g, chunks))
            {
                batches.Add(batch);
                cancellationToken.ThrowIfCancellationRequested();
            }
            return [.. batches];
        }
        catch
        {
            foreach (RecordBatch batch in batches) batch.Dispose();
            throw;
        }
    }

    /// <summary>Decodes a fetched row group into batches; <paramref name="ranges"/> (internal) selects row ranges.</summary>
    internal IEnumerable<RecordBatch> Decode(int g, RowGroupChunks chunks, IReadOnlyList<(long Start, long Count)>? ranges = null)
    {
        RowGroup group = _file.Thrift.RowGroups[g];
        long rows = group.NumRows;
        if (rows < 0) throw new ParquetFormatException($"Row group {g} has {rows} rows.");
        var readers = new LeafReader[_leaves.Length];
        Dictionary<ReadNode, LeafReader> byNode = [];
        NativeStructBuilder? root = null;
        try
        {
            for (int l = 0; l < _leaves.Length; l++)
            {
                ReadNode leaf = _leaves[l];
                ColumnMetaData meta = group.Columns[leaf.Leaf!.LeafIndex].MetaData!;
                readers[l] = LeafReaders.Create(new PageReader(chunks.Chunk(l), meta.Codec, _options.VerifyChecksums, _buffers), leaf.Leaf, leaf.LeafType!, meta.NumValues);
                readers[l].UncompressedBytes = meta.TotalUncompressedSize;
                byNode[leaf] = readers[l];
            }
            foreach (ColumnMapping column in _columns) ConfigureLeaves(column.Read, byNode, keepLevels: !column.IsFlat, fixedSizeList: null);
            NativeColumnBuilder[] builders = [.. _columns.Select(c => NestedAssembler.CreateBuilder(c.Read, byNode))];
            // Arrow has no struct of no fields: a batch without columns is made directly.
            if (_columns.Length > 0) root = new NativeStructBuilder(new StructType([.. _columns.Select(c => c.Field)]), builders);
            IReadOnlyList<(long Start, long Count)> selected = ranges ?? [(0, rows)];
            long at = 0;
            foreach (var (start, count) in selected)
            {
                if (start < at || count < 0 || start + count > rows) throw new ArgumentOutOfRangeException(nameof(ranges), "Row ranges must be ordered and inside the row group.");
                for (long skip = start - at; skip > 0;)
                {
                    int n = (int)Math.Min(skip, int.MaxValue);
                    foreach (LeafReader r in readers) r.Skip(n);
                    skip -= n;
                }
                for (long left = count; left > 0;)
                {
                    int n = (int)Math.Min(left, _options.BatchSize);
                    if (_options.MaxDegreeOfParallelism > 1 && readers.Length > 1)
                    {
                        // The columns' readers share nothing: read them side by side. Of several failures the first
                        // column's is reported, as reading them in order would.
                        var errors = new Exception?[readers.Length];
                        Parallel.For(0, readers.Length, new ParallelOptions { MaxDegreeOfParallelism = _options.MaxDegreeOfParallelism }, l =>
                        {
                            try
                            {
                                readers[l].Reserve(n, rows);
                                readers[l].Read(n);
                            }
                            catch (Exception e)
                            {
                                errors[l] = e;
                            }
                        });
                        if (System.Array.Find(errors, e => e is not null) is { } error)
                            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(error);
                    }
                    else
                    {
                        foreach (LeafReader r in readers)
                        {
                            r.Reserve(n, rows);
                            r.Read(n);
                        }
                    }
                    for (int c = 0; c < _columns.Length; c++)
                    {
                        try
                        {
                            if (!_columns[c].IsFlat) NestedAssembler.Assemble(_columns[c].Read, builders[c], byNode, _columns[c].Field.Name);
                        }
                        catch (InvalidOperationException e)
                        {
                            throw new ParquetFormatException($"Column '{_columns[c].Field.Name}': {e.Message}", e);
                        }
                        if (builders[c].Length != n)
                            throw new ParquetFormatException($"Column '{_columns[c].Field.Name}' holds {builders[c].Length} rows of the {n} read.");
                    }
                    RecordBatch batch;
                    if (root is null) batch = new RecordBatch(Schema, [], n);
                    else
                    {
                        root.EndStructs(n);
                        batch = root.ExportBatch(Schema);
                    }
                    foreach (LeafReader r in readers) r.BatchExported();
                    left -= n;
                    yield return batch;
                }
                at = start + count;
            }
            if (ranges is null)
            {
                foreach (LeafReader r in readers) r.Finish();
            }
        }
        finally
        {
            foreach (LeafReader? r in readers) r?.Dispose();
            root?.Dispose(); // its children are the readers' builders: disposing twice is harmless
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

    /// <summary>Reads the selected column chunks of a row group, coalescing nearby ones into one read.</summary>
    internal async ValueTask<RowGroupChunks> Fetch(int g, bool useAsync, CancellationToken ct)
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
        var chunks = new RowGroupChunks(ranges, _buffers);
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
    internal sealed class RowGroupChunks : IDisposable
    {
        private readonly List<(long Start, long Length, NativeByteBuffer Buffer)> _reads = [];
        private readonly (int Read, long Offset, int Length)[] _chunks;
        private readonly NativeBufferPool? _pool;

        public RowGroupChunks((long Start, long Length)[] ranges, NativeBufferPool? pool = null)
        {
            _pool = pool;
            _chunks = new (int, long, int)[ranges.Length];
            int[] order = [.. Enumerable.Range(0, ranges.Length).OrderBy(i => ranges[i].Start)];
            long readStart = 0, readEnd = -1;
            List<int> members = [];
            foreach (int i in order)
            {
                var (start, length) = ranges[i];
                if (members.Count > 0 && start - readEnd <= CoalesceGap && Math.Max(readEnd, start + length) - readStart <= CoalesceLimit)
                    readEnd = Math.Max(readEnd, start + length);
                else
                {
                    Close();
                    readStart = start;
                    readEnd = start + length;
                }
                members.Add(i);
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
                    buffer = new NativeByteBuffer(0);
                    buffer.EnsureCapacity(length);
                }
                _reads.Add((readStart, length, buffer));
                foreach (int m in members) _chunks[m] = (_reads.Count - 1, ranges[m].Start - readStart, (int)ranges[m].Length);
                members.Clear();
            }
        }

        public IReadOnlyList<(long Start, long Length, NativeByteBuffer Buffer)> Reads => _reads;

        public ReadOnlyMemory<byte> Chunk(int column)
        {
            var (read, offset, length) = _chunks[column];
            return _reads[read].Buffer.Memory.Slice((int)offset, length);
        }

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
    public void Dispose() => _buffers.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
