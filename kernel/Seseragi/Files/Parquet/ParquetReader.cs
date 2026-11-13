using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Helpers;
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
    private readonly int[] _rowGroups;
    private readonly bool _oldParquetMr;

    internal ParquetReader(ParquetFile file, ParquetReadOptions options)
    {
        options.Validate();
        _file = file;
        _options = options;
        var (_, all) = ArrowMapping.Map(file.Tree, file.Thrift, options);
        _columns = Select(all, options.Columns);
        _rowGroups = options.RowGroups is { } groups ? [.. groups] : [.. Enumerable.Range(0, file.Thrift.RowGroups.Length)];
        foreach (int g in _rowGroups)
        {
            if ((uint)g >= (uint)file.Thrift.RowGroups.Length)
                throw new ArgumentOutOfRangeException(nameof(options), g, $"The file has {file.Thrift.RowGroups.Length} row groups.");
        }
        Schema = new Apache.Arrow.Schema([.. _columns.Select(c => c.Field)], null);
        _oldParquetMr = IsOldParquetMr(file.Thrift.CreatedBy);
    }

    /// <summary>The Arrow schema of the batches (the selected columns, in file order).</summary>
    public Apache.Arrow.Schema Schema { get; }

    private static ColumnMapping[] Select(ColumnMapping[] all, IReadOnlyList<string>? names)
    {
        ColumnMapping[] selected = all;
        if (names is not null)
        {
            HashSet<string> wanted = new(names, StringComparer.Ordinal);
            foreach (string name in wanted)
            {
                if (!all.Any(c => c.Field.Name == name))
                    throw new ArgumentException($"The file has no top-level column '{name}'.", nameof(names));
            }
            selected = [.. all.Where(c => wanted.Contains(c.Field.Name))];
        }
        ColumnMapping[] nested = [.. selected.Where(c => !c.IsFlat)];
        if (nested.Length > 0)
            throw new NotSupportedException($"Reading nested columns is not supported yet ({string.Join(", ", nested.Select(c => $"'{c.Field.Name}'"))}); select flat columns with ParquetReadOptions.Columns.");
        return selected;
    }

    /// <summary>Reads the batches.</summary>
    /// <exception cref="ParquetFormatException">A page is damaged, or uses an encoding or codec this library does not read.</exception>
    public IEnumerable<RecordBatch> ReadBatches()
    {
        foreach (int g in _rowGroups)
        {
            using RowGroupChunks chunks = Fetch(g, useAsync: false, default).AsTask().GetAwaiter().GetResult();
            foreach (RecordBatch batch in Decode(g, chunks)) yield return batch;
        }
    }

    /// <inheritdoc cref="ReadBatches"/>
    public async IAsyncEnumerable<RecordBatch> ReadBatchesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
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

    /// <summary>Decodes a fetched row group into batches; <paramref name="ranges"/> (internal) selects row ranges.</summary>
    internal IEnumerable<RecordBatch> Decode(int g, RowGroupChunks chunks, IReadOnlyList<(long Start, long Count)>? ranges = null)
    {
        RowGroup group = _file.Thrift.RowGroups[g];
        long rows = group.NumRows;
        if (rows < 0) throw new ParquetFormatException($"Row group {g} has {rows} rows.");
        var readers = new LeafReader[_columns.Length];
        NativeStructBuilder? root = null;
        try
        {
            for (int c = 0; c < _columns.Length; c++)
            {
                ColumnMapping column = _columns[c];
                ColumnMetaData meta = group.Columns[column.Node.LeafIndex].MetaData!;
                readers[c] = LeafReaders.Create(new PageReader(chunks.Chunk(c), meta.Codec, _options.VerifyChecksums), column.Node, column.Leaf!, meta.NumValues);
            }
            // Arrow has no struct of no fields: a batch without columns is made directly.
            if (_columns.Length > 0) root = new NativeStructBuilder(new StructType([.. _columns.Select(c => c.Field)]), [.. readers.Select(r => r.Builder)]);
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
                    foreach (LeafReader r in readers) r.Read(n);
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

    /// <summary>Reads the selected column chunks of a row group, coalescing nearby ones into one read.</summary>
    internal async ValueTask<RowGroupChunks> Fetch(int g, bool useAsync, CancellationToken ct)
    {
        RowGroup group = _file.Thrift.RowGroups[g];
        var ranges = new (long Start, long Length)[_columns.Length];
        for (int c = 0; c < _columns.Length; c++)
        {
            ColumnMapping column = _columns[c];
            ColumnChunk chunk = group.Columns[column.Node.LeafIndex];
            string name = string.Join('.', column.Node.Path);
            if (chunk.FilePath is not null) throw new ParquetFormatException($"Column '{name}' of row group {g} is stored in another file ('{chunk.FilePath}').");
            ColumnMetaData meta = chunk.MetaData!;
            if (!PageCodecs.CanDecompress(meta.Codec)) throw new ParquetFormatException($"Column '{name}' is compressed with {PageCodecs.Name(meta.Codec)}, which is not supported.");
            ranges[c] = ChunkRange(meta, _file.Source.Length, name, _oldParquetMr);
        }
        var chunks = new RowGroupChunks(ranges);
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

        public RowGroupChunks((long Start, long Length)[] ranges)
        {
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
                var buffer = new NativeByteBuffer(0);
                buffer.EnsureCapacity(length);
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
            foreach (var (_, _, buffer) in _reads) buffer.Release();
            _reads.Clear();
        }
    }

    /// <inheritdoc/>
    public void Dispose() { }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
