using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Builders;

namespace Kaminari.Seseragi.Files.Json;

/// <summary>
/// JSON files (lines or documents, compressed or not: <see cref="JsonWriteOptions"/>) for a
/// <see cref="DatasetSink{TPartition}"/>: written by <see cref="JsonWriter"/> with the options given. Rows come as runs
/// of the caller's batches; a run of <see cref="BatchRows"/> rows or more is written as it is (a slice of the batch,
/// encoded before the write returns), shorter runs are copied together (<see cref="ArrowBatchAppender"/>) up to
/// <see cref="BatchRows"/> rows or <see cref="BatchBytes"/> bytes (a partitioned write hands each partition a few rows of
/// each batch: written one by one they would each be a piece of their own). Files written side by side encode on one
/// thread each. The files' columns are the declared schema's fields (their metadata says how some are written: GUIDs,
/// objects), whatever the batches' own fields carry.
/// </summary>
internal sealed class JsonFileFormat(JsonWriteOptions options, Schema declared) : DatasetFileFormat
{
    /// <summary>The rows from which a run is written as it is, and up to which short runs are gathered.</summary>
    internal const int BatchRows = 64 * 1024;

    /// <summary>The bytes up to which short runs are gathered.</summary>
    internal const long BatchBytes = 8L << 20;

    private Schema? _schema;
    private JsonWriteOptions? _options;
    private bool _gather;

    public override string Extension => (options.Format == JsonFormat.Document ? ".json" : ".jsonl") + options.Compression switch
    {
        JsonCompression.Gzip => ".gz",
        JsonCompression.Zstd => ".zst",
        _ => "",
    };

    public override void Prepare(Schema schema, bool sideBySide, int threads, bool measured)
    {
        _schema = new Schema([.. schema.FieldsList.Select(f => declared.GetFieldByName(f.Name) ?? f)], schema.Metadata);
        _options = options with
        {
            LeaveOpen = true,
            BatchOwnership = BatchOwnership.Caller,
            MaxDegreeOfParallelism = sideBySide ? 1 : threads,
        };
        _gather = ArrowBatchAppender.Supports(_schema);
    }

    public override DatasetFileWriter Open(Stream stream) =>
        new Writer(JsonWriter.Create(stream, _schema!, _options), _gather ? new ArrowBatchAppender(_schema!) : null);

    /// <summary>A file's writer; <paramref name="pending"/> gathers short runs (null where the schema has a type it cannot copy: runs then go as they are).</summary>
    private sealed class Writer(JsonWriter writer, ArrowBatchAppender? pending) : DatasetFileWriter
    {
        /// <summary>The bytes written, the rows still on their way written first (a measure that waits).</summary>
        protected override long MeasureBytes()
        {
            writer.Flush();
            return writer.BytesWritten + (pending?.Bytes ?? 0);
        }

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

        /// <summary>Writes the rows gathered.</summary>
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
