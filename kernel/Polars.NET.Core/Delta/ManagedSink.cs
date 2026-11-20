using Apache.Arrow;
using Apache.Arrow.Ipc;
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Writing;
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
    /// The environment variable choosing the sink of data files: <c>managed</c> (the default: this one, where the store
    /// streams) or <c>native</c> (the engine's own sink, which writes maps as lists and splits files by their size in
    /// memory).
    /// </summary>
    public const string Variable = "POLARS_NET_DELTA_SINK";

    /// <summary>The rows of a row group (and of a batch the engine hands over): the engine's own sink's row groups.</summary>
    internal const int DefaultRowGroupRows = 122_880;

    /// <summary>A file written: its path relative to the table, size and footer (its bytes in native memory: the caller disposes it).</summary>
    public readonly record struct Written(string Relative, long Size, ParquetFooter Footer);

    /// <summary>
    /// The files written, partition by partition in the order the partitions first came (each partition's in order),
    /// and how many of them were closed before their size to keep the buffered bytes under <see cref="MemoryBudget"/>.
    /// </summary>
    public sealed record SinkResult(List<Written> Files, int ClosedForMemory);

    /// <summary>
    /// The environment variable that sets <see cref="MemoryBudget"/>: a size (<c>2GiB</c>, <c>512MiB</c>) or a share of
    /// the memory the process may use (<c>25%</c>).
    /// </summary>
    public const string MemoryVariable = "POLARS_NET_DELTA_SINK_MEMORY";

    /// <summary>
    /// The bytes the files of a sink may hold in memory together (the open files' pages not written yet, their writers'
    /// scratch, and what the streams buffer: a cloud upload's parts, the files still being published); beyond, the
    /// files being published are waited for, then the largest row groups ended early, then the largest files closed
    /// early. The fixed buffers of each open writer come on top. <see cref="MemoryVariable"/> when set, else an eighth of
    /// the memory the process may use (as the runtime sees it: a container's limit, not the machine's), at least
    /// 256 MiB. Read when a sink starts.
    /// </summary>
    /// <exception cref="ArgumentException"><see cref="MemoryVariable"/> holds neither a size nor a share.</exception>
    internal static long MemoryBudget => MemoryBudgetOverride ?? Budget(Environment.GetEnvironmentVariable(MemoryVariable), GC.GetGCMemoryInfo().TotalAvailableMemoryBytes);

    /// <summary>A budget that takes the place of <see cref="MemoryBudget"/>'s (tests set it).</summary>
    internal static long? MemoryBudgetOverride { get; set; }

    /// <summary>The budget a <paramref name="setting"/> (null or blank: the default) gives with <paramref name="available"/> bytes.</summary>
    internal static long Budget(string? setting, long available) => DatasetSink<object>.Budget(setting, available, MemoryVariable);

    /// <summary>
    /// Whether the managed writer writes these files: unless <see cref="Variable"/> asks for the engine's sink, into a
    /// store that streams (a local disk, or a cloud store served by the managed clients: the native store would hold
    /// every file whole in memory).
    /// </summary>
    /// <exception cref="ArgumentException"><see cref="Variable"/> names neither sink.</exception>
    public static bool Applies(IWritableFileSystem fileSystem) => Chosen(Environment.GetEnvironmentVariable(Variable)) && Streams(fileSystem);

    /// <summary>Whether a <see cref="Variable"/> setting (null: not set) leaves the files to the managed writer.</summary>
    internal static bool Chosen(string? setting) => setting?.Trim().ToLowerInvariant() switch
    {
        null or "" or "managed" => true,
        "native" => false,
        _ => throw new ArgumentException($"{Variable} is '{setting}': 'managed' (the default) or 'native' (the engine's own sink)."),
    };

    private static bool Streams(IWritableFileSystem fileSystem) => fileSystem switch
    {
        TrackingFileSystem tracking => Streams(tracking.Inner),
        CatalogManagedFileSystem catalog => Streams(catalog.Inner),
        LocalFileSystem => true,
        ObjectStoreFileSystem store => store.StreamsWrites,
        _ => false,
    };

    /// <summary>
    /// Sinks <paramref name="rows"/> (consumed: the <paramref name="keys"/> columns, then the data columns under their
    /// physical names in <paramref name="dataFields"/>' order, then any extra columns) as files, split by the keys'
    /// values (each value a partition: rows of one partition only in a file, the key columns left out), by size on disk
    /// and by rows. <paramref name="describe"/> describes a partition from a one-row frame of its rows (null without
    /// keys), once, when its first rows come; <paramref name="pathFor"/> names its file of an index (0, 1, …), when the
    /// file opens (calls are serialized). A file is committed when it is full, or at the end. No rows, no file. On a
    /// failure every open file is given up (nothing of it stays) and the first error, in the order of the rows,
    /// rethrown; files committed before stay for the caller to remove. The files are written by the shared
    /// <see cref="DatasetSink{TPartition}"/> (as the dataset writers write theirs).
    /// </summary>
    /// <param name="keysPreGrouped">Whether the rows come grouped by their keys (each partition's rows together): then
    /// not sorted, and a partition's file is closed once the next partition's rows come.</param>
    /// <param name="approxBytesPerFile">The size a file closes at on disk (the last rows cut to fit); 0 for no limit.</param>
    public static SinkResult Sink<TPartition>(
        IWritableFileSystem fileSystem, LazyFrameHandle rows, string[] keys, bool keysPreGrouped, StructField[] dataFields,
        Func<DataFrameHandle?, TPartition> describe, Func<TPartition, long, string> pathFor, KernelWriteOptions options, ulong approxBytesPerFile)
    {
        int rowGroupRows = options.RowGroupSize > 0 ? options.RowGroupSize : DefaultRowGroupRows;
        long budget = MemoryBudget;
        DatasetSink<TPartition>? sink = null;
        // The batch being written and the rows before each of its chunks: a partition is described from its first row.
        DataFrameHandle? frame = null;
        List<int> rowsBefore = [];
        try
        {
            using CollectBatchesHandle batches = PolarsWrapper.LazyCollectBatches(rows, PlEngine.Streaming, options.MaintainOrder, (nuint)rowGroupRows);
            while (true)
            {
                using DataFrameHandle batch = PolarsWrapper.CollectBatchesNext(batches, out bool done);
                if (done) break;
                if (keys.Length == 0)
                {
                    // Without keys each chunk is written as it comes and let go: a frame's chunks are not held together.
                    foreach (RecordBatch chunk in Chunks(batch))
                    {
                        using (chunk)
                        {
                            sink ??= new DatasetSink<TPartition>(Settings(chunk.ColumnCount), Describe, pathFor);
                            sink.Write([chunk]);
                        }
                    }
                    sink?.HoldMemory();
                    continue;
                }
                List<RecordBatch> chunks = [.. Chunks(batch)];
                try
                {
                    if (chunks.Count == 0) continue;
                    // The columns are known with the first batch: the keys first, every other one written.
                    sink ??= new DatasetSink<TPartition>(Settings(chunks[0].ColumnCount), Describe, pathFor);
                    frame = batch;
                    rowsBefore.Clear();
                    int before = 0;
                    foreach (RecordBatch chunk in chunks)
                    {
                        rowsBefore.Add(before);
                        before += chunk.Length;
                    }
                    sink.Write(chunks, keysPreGrouped);
                }
                finally
                {
                    frame = null;
                    foreach (RecordBatch chunk in chunks) chunk.Dispose();
                }
                sink.HoldMemory();
            }
            if (sink is null) return new SinkResult([], 0);
            return new SinkResult([.. sink.Finish().Select(static f => new Written(f.Path, f.Bytes, (ParquetFooter)f.Footer!))], sink.ClosedForMemory);
        }
        catch
        {
            sink?.GiveUp();
            throw;
        }
        finally
        {
            sink?.Dispose();
        }

        // A partition from its first row, sliced from the batch (the engine renders its values).
        TPartition Describe(PartitionRow? row)
        {
            if (row is not { } first) return describe(null);
            using DataFrameHandle one = PolarsWrapper.Slice(frame!, rowsBefore[first.Chunk] + first.Row, 1);
            return describe(one);
        }

        // Keys as Hive takes them (an empty string is a null), the files written side by side on every processor.
        DatasetSinkSettings Settings(int columns) => new(
            new TableFileStore(fileSystem), new DeltaParquetFormat(dataFields, options, rowGroupRows),
            [.. Enumerable.Range(0, keys.Length)], [.. keys.Select(static _ => true)], [.. Enumerable.Range(keys.Length, columns - keys.Length)],
            (long)Math.Min(options.MaxRowsPerFile, long.MaxValue), (long)Math.Min(approxBytesPerFile, long.MaxValue), budget,
            Environment.ProcessorCount, options.SyncOnClose != PlSyncOnClose.None);
    }

    /// <summary>
    /// Parquet files as Delta writes them: the writer schema (field ids, maps) and options taken from the first batch's
    /// schema, then written as the dataset writers write theirs; the caller's rows always encoded at once.
    /// </summary>
    /// <remarks>
    /// A file written alone could take its rows as batches of its own (row groups encoded side by side), as a dataset
    /// writer's does. Measured on an unpartitioned OPTIMIZE (850 MiB, 2026-11-20): no faster (one file of the whole
    /// table 730–830 ms either way; three files 730–813 vs 815–840 ms), with 25–30 % more CPU and 1.6–1.8× the peak
    /// anonymous memory: the engine reading the rows keeps the processors busy already.
    /// </remarks>
    private sealed class DeltaParquetFormat(StructField[] dataFields, KernelWriteOptions options, int rowGroupRows) : DatasetFileFormat
    {
        private ParquetFileFormat? _parquet;

        public override string Extension => ".parquet";

        public override void Prepare(Schema schema, bool sideBySide, int threads, bool measured)
        {
            (Schema writerSchema, IReadOnlySet<string>? maps) = WriterSchema(schema, dataFields);
            _parquet = new ParquetFileFormat(WriterOptions(options, rowGroupRows, maps), takeBatches: false);
            _parquet.Prepare(writerSchema, sideBySide, threads, measured);
        }

        public override DatasetFileWriter Open(Stream stream) => _parquet!.Open(stream);
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
}
