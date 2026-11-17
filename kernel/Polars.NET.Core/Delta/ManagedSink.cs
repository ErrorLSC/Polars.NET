using Apache.Arrow;
using Apache.Arrow.Ipc;
using Kaminari.Sazanami.Arrow;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Wrappers;
using ArrowTypes = Apache.Arrow.Types;
using DeltaArray = Kaminari.Seseragi.Lakehouse.Delta.Schema.ArrayType;
using DeltaMap = Kaminari.Seseragi.Lakehouse.Delta.Schema.MapType;
using DeltaStruct = Kaminari.Seseragi.Lakehouse.Delta.Schema.StructType;
using DeltaType = Kaminari.Seseragi.Lakehouse.Delta.Schema.DeltaType;
using StructField = Kaminari.Seseragi.Lakehouse.Delta.Schema.StructField;

namespace Polars.NET.Core.Delta;

/// <summary>
/// Writes a table's data files with the managed Parquet writer instead of the engine's sink: the engine streams the
/// rows (its streaming engine, batch by batch, as Arrow), the writer encodes them and the store takes the bytes as
/// they come (in parts on a cloud store, a temporary file renamed on a local disk). Files are split by size on disk
/// (and by rows), maps are written as Parquet maps, field ids come from the table's column mapping, and each file's
/// footer comes from the writer (nothing read back).
/// </summary>
internal static class ManagedSink
{
    /// <summary>
    /// The environment variable choosing the sink of data files: <c>managed</c>, <c>native</c> (the engine's), or
    /// <c>auto</c> (default: the engine's for now).
    /// </summary>
    public const string Variable = "POLARS_NET_DELTA_SINK";

    /// <summary>The rows of a row group (and of a batch the engine hands over): the engine's own sink's row groups.</summary>
    internal const int DefaultRowGroupRows = 122_880;

    /// <summary>A file written: its path relative to the table, size and footer.</summary>
    public readonly record struct Written(string Relative, long Size, ParquetMetadata Footer);

    /// <summary>
    /// Whether the managed writer writes these files: asked for, without sink keys (partitions and buckets still go
    /// through the engine's sink), into a store that streams (a local disk, or a cloud store served by the managed
    /// clients: the native store would hold every file whole in memory).
    /// </summary>
    public static bool Applies(IWritableFileSystem fileSystem, int sinkKeys) =>
        sinkKeys == 0
        && Environment.GetEnvironmentVariable(Variable)?.Trim().ToLowerInvariant() == "managed"
        && Streams(fileSystem);

    private static bool Streams(IWritableFileSystem fileSystem) => fileSystem switch
    {
        TrackingFileSystem tracking => Streams(tracking.Inner),
        CatalogManagedFileSystem catalog => Streams(catalog.Inner),
        LocalFileSystem => true,
        ObjectStoreFileSystem store => store.StreamsWrites,
        _ => false,
    };

    /// <summary>
    /// Sinks <paramref name="rows"/> (consumed: the data columns under their physical names, in
    /// <paramref name="dataFields"/>' order, then any extra columns) as files at the paths <paramref name="pathFor"/>
    /// names (by file index), each committed before the next is opened. No rows, no file. On a failure the open file is
    /// given up (nothing of it stays) and the error rethrown; files committed before stay for the caller to remove.
    /// </summary>
    /// <param name="approxBytesPerFile">The size a file closes at on disk (the last batch cut to fit); 0 for no limit.</param>
    public static List<Written> Sink(
        IWritableFileSystem fileSystem, LazyFrameHandle rows, StructField[] dataFields, Func<long, string> pathFor,
        KernelWriteOptions options, ulong approxBytesPerFile)
    {
        int rowGroupRows = options.RowGroupSize > 0 ? options.RowGroupSize : DefaultRowGroupRows;
        long target = (long)Math.Min(approxBytesPerFile, long.MaxValue);
        long maxRows = (long)Math.Min(options.MaxRowsPerFile, long.MaxValue);
        List<Written> written = [];
        File? open = null;
        try
        {
            using CollectBatchesHandle batches = PolarsWrapper.LazyCollectBatches(rows, PlEngine.Streaming, options.MaintainOrder, (nuint)rowGroupRows);
            Schema? schema = null;
            IReadOnlySet<string>? maps = null;
            while (true)
            {
                using DataFrameHandle frame = PolarsWrapper.CollectBatchesNext(batches, out bool done);
                if (done) break;
                foreach (RecordBatch chunk in Chunks(frame))
                {
                    using (chunk)
                    {
                        for (int at = 0; at < chunk.Length;)
                        {
                            // The chunk's rows from at: a slice shares the chunk's memory (never disposed; the chunk is).
                            RecordBatch part = at == 0 ? chunk : chunk.Slice(at, chunk.Length - at);
                            if (schema is null) (schema, maps) = WriterSchema(part.Schema, dataFields);
                            open ??= File.Open(fileSystem, pathFor(written.Count), schema, WriterOptions(options, rowGroupRows, maps), options.SyncOnClose != PlSyncOnClose.None);
                            // As many rows as the file takes: up to its row limit, and up to its size, a row's bytes on
                            // disk as the file shows them so far (before its first rows, the Arrow bytes of the chunk's
                            // buffers: no fewer than on disk; the file goes on filling after them).
                            int take = part.Length;
                            if (maxRows > 0) take = (int)Math.Min(take, maxRows - open.Rows);
                            if (target > 0)
                            {
                                double perRow = open.PerRow ?? (double)ParquetWriter.ArrowBytes(part) / part.Length;
                                take = (int)Math.Min(take, Math.Max(1, (long)((target - open.Bytes(target)) / perRow)));
                            }
                            open.Write(take < part.Length ? part.Slice(0, take) : part); // encoded at once
                            at += take;
                            // Closed at its row limit, or when not one more row fits.
                            if ((maxRows > 0 && open.Rows >= maxRows) || (target > 0 && target - open.Bytes(target) < open.PerRow))
                            {
                                written.Add(open.Close());
                                open = null;
                            }
                        }
                    }
                }
            }
            if (open is not null)
            {
                written.Add(open.Close());
                open = null;
            }
            return written;
        }
        catch
        {
            open?.GiveUp();
            throw;
        }
    }

    /// <summary>
    /// An engine batch as Arrow, chunk by chunk as the engine holds it: nothing copied (one Arrow batch for the whole
    /// would concatenate its chunks, and compact a join's gathered strings); each chunk owns its memory.
    /// </summary>
    private static IEnumerable<RecordBatch> Chunks(DataFrameHandle frame)
    {
        using IArrowArrayStream stream = ArrowStreamInterop.ExportToStream(frame);
        while (stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult() is { } chunk) yield return chunk;
    }

    private static ParquetWriteOptions WriterOptions(KernelWriteOptions options, int rowGroupRows, IReadOnlySet<string>? maps) => new()
    {
        Compression = Compression(options.Compression, options.CompressionLevel),
        RowGroupRows = rowGroupRows,
        DataPageBytes = options.DataPageSize > 0 ? options.DataPageSize : new ParquetWriteOptions().DataPageBytes,
        // Whole bounds: the table's statistics are taken from them (and cut to their own rules there).
        StatisticsTruncateLength = null,
        StoreArrowSchema = false,
        MapColumns = maps,
        LeaveOpen = true,
    };

    private static IParquetCompression Compression(PlParquetCompression codec, int level) => codec switch
    {
        PlParquetCompression.Uncompressed => NoCompression.Instance,
        PlParquetCompression.Gzip => level < 0 ? new GzipCompression() : new GzipCompression(level),
        PlParquetCompression.Brotli => level < 0 ? new BrotliCompression() : new BrotliCompression(level),
        PlParquetCompression.ZSTD => level < 0 ? new ZstandardCompression() : new ZstandardCompression(level),
        PlParquetCompression.Lz4Raw => Lz4RawCompression.Instance,
        _ => SnappyCompression.Instance,
    };

    /// <summary>
    /// The schema the files are written with: the engine's, the table's field ids on the data columns' struct fields
    /// at any depth (as Spark writes them), and the paths of the map columns (lists of key/value structs to the
    /// engine) to be written as Parquet maps.
    /// </summary>
    internal static (Schema Schema, IReadOnlySet<string>? Maps) WriterSchema(Schema exported, StructField[] dataFields)
    {
        HashSet<string> maps = [];
        List<Field> fields = [.. exported.FieldsList];
        for (int f = 0; f < dataFields.Length && f < fields.Count; f++)
            fields[f] = With(fields[f], dataFields[f].Type, dataFields[f].FieldId, fields[f].Name);
        return (new Schema(fields, null), maps.Count == 0 ? null : maps);

        // The field with its id, its nested fields with theirs, map paths noted (named as the writer names columns).
        Field With(Field field, DeltaType type, int? id, string where)
        {
            ArrowTypes.IArrowType arrow = (type, field.DataType) switch
            {
                (DeltaStruct s, ArrowTypes.StructType st) => new ArrowTypes.StructType(
                    [.. st.Fields.Select((child, i) => i < s.Fields.Count ? With(child, s.Fields[i].Type, s.Fields[i].FieldId, $"{where}.{child.Name}") : child)]),
                (DeltaArray a, ArrowTypes.ListType l) => new ArrowTypes.ListType(With(l.ValueField, a.ElementType, null, where + "[]")),
                (DeltaArray a, ArrowTypes.LargeListType l) => new ArrowTypes.LargeListType(With(l.ValueField, a.ElementType, null, where + "[]")),
                (DeltaMap m, ArrowTypes.ListType { ValueDataType: ArrowTypes.StructType { Fields.Count: 2 } entry } l) =>
                    new ArrowTypes.ListType(new Field(l.ValueField.Name, Entries(entry, m, where), l.ValueField.IsNullable)),
                (DeltaMap m, ArrowTypes.LargeListType { ValueDataType: ArrowTypes.StructType { Fields.Count: 2 } entry } l) =>
                    new ArrowTypes.LargeListType(new Field(l.ValueField.Name, Entries(entry, m, where), l.ValueField.IsNullable)),
                _ => field.DataType,
            };
            return ArrowFieldIds.With(new Field(field.Name, arrow, field.IsNullable, field.HasMetadata ? field.Metadata : null), id);
        }

        ArrowTypes.StructType Entries(ArrowTypes.StructType entry, DeltaMap map, string where)
        {
            maps.Add(where);
            return new ArrowTypes.StructType(
            [
                With(entry.Fields[0], map.KeyType, null, where + ".key"),
                With(entry.Fields[1], map.ValueType, null, where + ".value"),
            ]);
        }
    }

    /// <summary>A file being written: its stream, writer and rows so far.</summary>
    private sealed class File(string relative, FileWriteStream stream, ParquetWriter writer)
    {
        // The last exact measure of the file's size (it waits for the pages being compressed) and its rows then.
        private long _measuredBytes, _measuredRows;

        public long Rows { get; private set; }

        /// <summary>The bytes of one of its rows, as last measured; null before the first measure.</summary>
        public double? PerRow => _measuredRows > 0 ? (double)_measuredBytes / _measuredRows : null;

        /// <summary>
        /// What the file takes so far on disk (its pages, the open ones as plainly encoded): measured when an estimate
        /// from the last measure comes near <paramref name="target"/> (or there is none), else that estimate, so that
        /// the pages being compressed are waited for only a few times per file.
        /// </summary>
        public long Bytes(long target)
        {
            if (PerRow is double perRow)
            {
                long estimate = _measuredBytes + (long)((Rows - _measuredRows) * perRow);
                if (estimate < target * 9 / 10) return estimate;
            }
            _measuredBytes = writer.BytesWritten + writer.PendingBytes;
            _measuredRows = Rows;
            return _measuredBytes;
        }

        public static File Open(IWritableFileSystem fileSystem, string relative, Schema schema, ParquetWriteOptions options, bool sync)
        {
            FileWriteStream stream = fileSystem.OpenWrite(relative, createOnly: true);
            stream.SyncOnCommit = sync;
            try
            {
                return new File(relative, stream, ParquetWriter.Create(stream, schema, options));
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }

        /// <summary>Encodes a batch (the caller keeps it, and may let it go once this returns).</summary>
        public void Write(RecordBatch batch)
        {
            writer.Write(batch);
            Rows += batch.Length;
        }

        /// <summary>Completes and publishes the file.</summary>
        public Written Close()
        {
            writer.Complete();
            writer.Dispose();
            if (!stream.Commit()) throw new IOException($"The data file {relative} exists already.");
            stream.Dispose();
            return new Written(relative, writer.BytesWritten, writer.Metadata);
        }

        /// <summary>Gives the file up: nothing buffered is written on, nothing of it stays.</summary>
        public void GiveUp()
        {
            writer.Abort();
            try
            {
                writer.Dispose();
            }
            finally
            {
                stream.Dispose();
            }
        }
    }
}
