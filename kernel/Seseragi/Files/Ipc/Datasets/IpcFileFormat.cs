using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Builders;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// Arrow IPC files (the file format, or the stream format: <see cref="IpcWriteOptions.Format"/>) for a
/// <see cref="DatasetSink{TPartition}"/>: written by <see cref="IpcWriter"/> with the options given. Rows come as runs
/// of the caller's batches; a run of <see cref="BatchRows"/> rows or more is written as a record batch of its own (a
/// slice of the batch, never disposed: it shares the batch's buffers), shorter runs are copied together
/// (<see cref="ArrowBatchAppender"/>) into record batches of <see cref="BatchRows"/> rows or <see cref="BatchBytes"/>
/// bytes (a partitioned write hands each partition a few rows of each batch: as record batches of their own they would
/// read back slowly). Files written side by side compress on one thread each.
/// </summary>
internal sealed class IpcFileFormat(IpcWriteOptions options) : DatasetFileFormat
{
    /// <summary>The rows from which a run is a record batch of its own, and up to which short runs are gathered.</summary>
    internal const int BatchRows = 64 * 1024;

    /// <summary>The bytes up to which short runs are gathered.</summary>
    internal const long BatchBytes = 8L << 20;

    private Schema? _schema;
    private IpcWriteOptions? _options;
    private bool _gather;

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
        _gather = ArrowBatchAppender.Supports(schema);
    }

    public override DatasetFileWriter Open(Stream stream) =>
        new Writer(IpcWriter.Create(stream, _schema!, _options), _gather ? new ArrowBatchAppender(_schema!) : null);

    /// <summary>A file's writer; <paramref name="pending"/> gathers short runs (null where the schema has a type it cannot copy: runs then go as they are).</summary>
    private sealed class Writer(IpcWriter writer, ArrowBatchAppender? pending) : DatasetFileWriter
    {
        protected override long MeasureBytes() => writer.BytesWritten + (pending?.Bytes ?? 0);

        protected override void WriteCore(RecordBatch batch, int start, int count)
        {
            if (pending is null || count >= BatchRows)
            {
                Flush();
                writer.Write(start == 0 && count == batch.Length ? batch : batch.Slice(start, count));
                return;
            }
            if (!pending.TryAppend(batch, start, count))
            {
                // Rows that cannot join those gathered (another dictionary): the gathered go first.
                Flush();
                if (!pending.TryAppend(batch, start, count))
                {
                    writer.Write(batch.Slice(start, count));
                    return;
                }
            }
            if (pending.Rows >= BatchRows || pending.Bytes >= BatchBytes) Flush();
        }

        /// <summary>Writes the rows gathered as a record batch.</summary>
        private void Flush()
        {
            if (pending is not { Rows: > 0 }) return;
            using RecordBatch gathered = pending.Take();
            writer.Write(gathered);
        }

        /// <summary>The rows gathered, not written yet.</summary>
        public override long HeldBytes => pending?.Bytes ?? 0;

        public override long BytesWritten => writer.BytesWritten;

        /// <summary>Writes the rows gathered (to hold less memory).</summary>
        public override void EndChunk() => Flush();

        public override object? Complete()
        {
            Flush();
            pending?.Dispose();
            writer.Complete();
            writer.Dispose();
            return null;
        }

        public override void Abort()
        {
            pending?.Dispose();
            writer.Abort();
            writer.Dispose();
        }
    }
}
