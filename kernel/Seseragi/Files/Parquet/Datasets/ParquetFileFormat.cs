using Apache.Arrow;
using Kaminari.Seseragi.Files.Parquet.Writing;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Parquet files for a <see cref="DatasetSink{TPartition}"/>: written by <see cref="ParquetWriter"/> with the options
/// given. Files written side by side share one write plan and one budget of threads (each file's columns take helpers
/// from it), and encode the caller's rows at once (as ranges, never sliced). A file written alone, not cut by size,
/// takes its rows as batches of its own (references to the caller's memory: the caller may let its batch go) and
/// encodes several row groups side by side (<see cref="BatchOwnership.Writer"/>): one file's columns alone keep few
/// threads busy. Measured (10M rows, 6 columns, 16 threads, 2026-11-21): 270 → 111–126 ms (Polars 133).
/// </summary>
/// <param name="options">The files' writer options (ownership, parallelism and sharing are set here).</param>
/// <param name="takeBatches">Whether a file written alone may take its rows as batches of its own (else it always
/// encodes the caller's rows at once).</param>
internal sealed class ParquetFileFormat(ParquetWriteOptions options, bool takeBatches = true) : DatasetFileFormat
{
    private Apache.Arrow.Schema? _schema;
    private ParquetWriteOptions? _options;
    private ParquetWriter.WritePlan? _plan;
    private bool _takes; // the files take their rows as batches of their own

    public override string Extension => ".parquet";

    public override void Prepare(Apache.Arrow.Schema schema, bool sideBySide, int threads, bool measured)
    {
        _schema = schema;
        // Sizes measured as rows come need every row encoded at once (batches waiting for their turn are not counted).
        _takes = takeBatches && !sideBySide && !measured && threads > 1;
        _options = options with
        {
            LeaveOpen = true,
            BatchOwnership = _takes ? BatchOwnership.Writer : BatchOwnership.Caller,
            MaxDegreeOfParallelism = threads,
            SharedBudget = sideBySide ? new WorkBudget(threads) : null,
        };
        if (sideBySide) _plan = ParquetWriter.WritePlan.Create(schema, _options);
    }

    public override DatasetFileWriter Open(Stream stream) =>
        new Writer(ParquetWriter.Create(stream, _plan ?? ParquetWriter.WritePlan.Create(_schema!, _options!)), _takes);

    private sealed class Writer(ParquetWriter writer, bool takes) : DatasetFileWriter
    {
        protected override long MeasureBytes() => writer.BytesWritten + writer.PendingBytes;

        protected override void WriteCore(RecordBatch batch, int start, int count)
        {
            if (!takes)
            {
                writer.Write(batch, start, count);
                return;
            }
            // A batch of its own over the caller's memory (each buffer's reference counted): the writer disposes it.
            IArrowArray[] columns = new IArrowArray[batch.ColumnCount];
            for (int c = 0; c < columns.Length; c++) columns[c] = ArrowArrayFactory.BuildArray(batch.Column(c).Data.SliceShared(start, count));
            writer.Write(new RecordBatch(batch.Schema, columns, count));
        }

        /// <summary>Its pages not written yet and its column writers' scratch.</summary>
        public override long HeldBytes => writer.PendingBytesBound + writer.ScratchBytes;

        public override long BytesWritten => writer.BytesWritten;

        /// <summary>Ends the open row group (its pages, and those before, go to the stream) and returns the column writers' scratch to the pool.</summary>
        public override void EndChunk()
        {
            writer.EndRowGroup();
            writer.TrimScratch();
        }

        /// <summary>Completes the file; its footer (<see cref="ParquetMetadata"/>).</summary>
        public override object? Complete()
        {
            writer.Complete();
            ParquetMetadata footer = writer.Metadata;
            writer.Dispose();
            return footer;
        }

        public override void Abort()
        {
            writer.Abort();
            writer.Dispose();
        }
    }
}
