using Apache.Arrow;
using Kaminari.Seseragi.Files.Parquet.Writing;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Parquet files for a <see cref="DatasetSink{TPartition}"/>: written by <see cref="ParquetWriter"/> with the options
/// given, the caller's batches encoded at once (rows as ranges, never sliced). Files written side by side share one
/// write plan and one budget of threads (each file's columns take helpers from it).
/// </summary>
internal sealed class ParquetFileFormat(ParquetWriteOptions options) : DatasetFileFormat
{
    private Apache.Arrow.Schema? _schema;
    private ParquetWriteOptions? _options;
    private ParquetWriter.WritePlan? _plan;

    public override string Extension => ".parquet";

    public override void Prepare(Apache.Arrow.Schema schema, bool sideBySide, int threads)
    {
        _schema = schema;
        _options = options with
        {
            LeaveOpen = true,
            BatchOwnership = BatchOwnership.Caller,
            MaxDegreeOfParallelism = threads,
            SharedBudget = sideBySide ? new WorkBudget(threads) : null,
        };
        if (sideBySide) _plan = ParquetWriter.WritePlan.Create(schema, _options);
    }

    public override DatasetFileWriter Open(Stream stream) =>
        new Writer(ParquetWriter.Create(stream, _plan ?? ParquetWriter.WritePlan.Create(_schema!, _options!)));

    private sealed class Writer(ParquetWriter writer) : DatasetFileWriter
    {
        protected override long MeasureBytes() => writer.BytesWritten + writer.PendingBytes;

        protected override void WriteCore(RecordBatch batch, int start, int count) => writer.Write(batch, start, count);

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
