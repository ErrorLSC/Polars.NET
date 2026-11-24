using Apache.Arrow;
using Kaminari.Seseragi.Files.Csv.Writing;
using Kaminari.Seseragi.Files.IO.Writing;

namespace Kaminari.Seseragi.Files.Csv;

/// <summary>
/// CSV files (their header, byte order mark, encoding and compression as <see cref="CsvWriteOptions"/> says, every file
/// its own) for a <see cref="DatasetSink{TPartition}"/>: written by <see cref="CsvWriter"/> with the options given. Rows
/// come as runs of the caller's batches; a run of <see cref="BatchRows"/> rows or more is written as it is (a slice of
/// the batch, encoded before the write returns, on the writer's threads), shorter runs are encoded at once on the calling
/// thread into the file's piece being filled (<see cref="CsvWriter.AppendRows"/>: a partitioned write hands each
/// partition a few rows of each batch, which then cost no write each, nor a copy before encoding). What the encoding
/// reads of a batch (its cells) is made once per batch for every file (<see cref="SourcesOpen"/>). Files written side by
/// side share the dataset's threads (<see cref="EncodeBudget"/>): a batch that feeds one file is encoded on every thread
/// the others leave free.
/// </summary>
internal sealed class CsvFileFormat(CsvWriteOptions options) : DatasetFileFormat
{
    /// <summary>The rows from which a run is written as it is (encoded on the writer's threads).</summary>
    internal const int BatchRows = 64 * 1024;

    private Schema? _schema;
    private CsvWriteOptions? _options;
    // The batches the runs come from, and their cells (made by the first file that reads one: every file's writer has the
    // same schema and options, so reads a batch alike).
    private readonly object _lock = new();
    private RecordBatch[] _sources = [];
    private BatchCells<CsvCell>?[] _cells = [];

    public override string Extension => ".csv" + options.Compression switch
    {
        CsvCompression.Gzip => ".gz",
        CsvCompression.Zstd => ".zst",
        _ => "",
    };

    public override void Prepare(Schema schema, bool sideBySide, int threads, bool measured)
    {
        _schema = schema;
        _options = options with
        {
            LeaveOpen = true,
            BatchOwnership = BatchOwnership.Caller,
            // Files side by side share the dataset's threads: a batch feeding one partition is encoded on every thread
            // the others leave free, and all of them never encode on more.
            MaxDegreeOfParallelism = threads,
            Budget = sideBySide ? new EncodeBudget(threads) : null,
        };
    }

    public override DatasetFileWriter Open(Stream stream) => new Writer(this, CsvWriter.Create(stream, _schema!, _options));

    public override void SourcesOpen(IReadOnlyList<RecordBatch> sources)
    {
        lock (_lock)
        {
            _sources = [.. sources];
            _cells = new BatchCells<CsvCell>?[sources.Count];
        }
    }

    public override void SourcesDone()
    {
        lock (_lock)
        {
            foreach (BatchCells<CsvCell>? cells in _cells) cells?.Dispose();
            _sources = [];
            _cells = [];
        }
    }

    // The cells of a batch given to SourcesOpen (made once, under the lock, by the writer asking first), or null for
    // another batch. The arrays change only between the writes of the batches (SourcesOpen, SourcesDone): read without
    // the lock.
    private BatchCells<CsvCell>? CellsOf(RecordBatch batch, CsvWriter writer)
    {
        int index = System.Array.IndexOf(_sources, batch);
        if (index < 0) return null;
        if (Volatile.Read(ref _cells[index]) is { } made) return made;
        lock (_lock)
        {
            if (_cells[index] is not { } cells) Volatile.Write(ref _cells[index], cells = writer.CellsOf(batch));
            return cells;
        }
    }

    /// <summary>A file's writer.</summary>
    private sealed class Writer(CsvFileFormat format, CsvWriter writer) : DatasetFileWriter
    {
        /// <summary>The bytes written, the rows still on their way written first (a measure that waits).</summary>
        protected override long MeasureBytes()
        {
            writer.Flush();
            return writer.BytesWritten;
        }

        protected override void WriteCore(RecordBatch batch, int start, int count)
        {
            if (count >= BatchRows || writer.EncodesApart(count))
            {
                writer.Write(start == 0 && count == batch.Length ? batch : batch.Slice(start, count));
                return;
            }
            if (format.CellsOf(batch, writer) is { } shared)
            {
                writer.AppendRows(shared, start, count);
                return;
            }
            BatchCells<CsvCell> cells = writer.CellsOf(batch);
            try
            {
                writer.AppendRows(cells, start, count);
            }
            finally
            {
                cells.Dispose();
            }
        }

        /// <summary>The rows encoded, not handed to the writes yet.</summary>
        public override long HeldBytes => writer.OpenBytes;

        public override long BytesWritten => writer.BytesWritten;

        /// <summary>Hands the rows encoded to the writes (to hold less memory).</summary>
        public override void EndChunk() => writer.HandOff();

        public override object? Complete()
        {
            writer.Complete();
            writer.Dispose();
            return null;
        }

        public override void Abort()
        {
            writer.Abort();
            writer.Dispose();
        }
    }
}
