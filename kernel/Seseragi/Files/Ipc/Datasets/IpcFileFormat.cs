using Apache.Arrow;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// Arrow IPC files (the file format, or the stream format: <see cref="IpcWriteOptions.Format"/>) for a
/// <see cref="DatasetSink{TPartition}"/>: written by <see cref="IpcWriter"/> with the options given, the caller's
/// batches written before <c>Write</c> returns. Rows of a batch are written as a record batch of their own (a slice of
/// it, never disposed: it shares the batch's buffers). Files written side by side compress on one thread each.
/// </summary>
internal sealed class IpcFileFormat(IpcWriteOptions options) : DatasetFileFormat
{
    private Schema? _schema;
    private IpcWriteOptions? _options;

    public override string Extension => options.Format == IpcFormat.Stream ? ".arrows" : ".arrow";

    public override void Prepare(Schema schema, bool sideBySide, int threads)
    {
        _schema = schema;
        _options = options with
        {
            LeaveOpen = true,
            BatchOwnership = BatchOwnership.Caller,
            MaxDegreeOfParallelism = sideBySide ? 1 : threads,
        };
    }

    public override DatasetFileWriter Open(Stream stream) => new Writer(IpcWriter.Create(stream, _schema!, _options));

    private sealed class Writer(IpcWriter writer) : DatasetFileWriter
    {
        protected override long MeasureBytes() => writer.BytesWritten;

        protected override void WriteCore(RecordBatch batch, int start, int count) =>
            writer.Write(start == 0 && count == batch.Length ? batch : batch.Slice(start, count));

        /// <summary>Nothing beyond the batch being written (caller-owned batches are written before <c>Write</c> returns).</summary>
        public override long HeldBytes => 0;

        public override long BytesWritten => writer.BytesWritten;

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
