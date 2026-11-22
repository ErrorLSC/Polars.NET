using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using Apache.Arrow;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta.Log;

/// <summary>What of a checkpoint to read.</summary>
internal enum CheckpointContent
{
    /// <summary>Every action: the table state.</summary>
    All,
    /// <summary>Only the protocol and the metadata (the file actions, nearly all of a checkpoint, are not read).</summary>
    ProtocolAndMetadata,
}

/// <summary>
/// Reads the actions of a Parquet checkpoint file (classic, a part of a multi-part one, a V2 top-level file or a
/// sidecar) straight from its columns: only the action columns needed are read, and of <c>add</c> neither
/// <c>partitionValues_parsed</c> nor, when the text statistics are there, <c>stats_parsed</c>.
/// </summary>
/// <remarks>
/// The file is decoded on the calling thread (<see cref="IFileSystem"/> promises nothing about concurrent ranged
/// reads); the actions of its batches are made side by side, a few batches at a time, and kept in row order.
/// </remarks>
internal static class CheckpointReader
{
    private const int BatchSize = 8_192;

    /// <summary>Checkpoints of at most this many rows (two batches) are read on the calling thread alone.</summary>
    private const int InlineRows = 2 * BatchSize;

    /// <summary>Reads the actions of a checkpoint file, in its row order.</summary>
    /// <param name="fileSystem">The table storage.</param>
    /// <param name="path">The file, relative to the table root or an absolute URI.</param>
    /// <param name="size">The file's size as a listing or the log gives it; 0 or less when unknown.</param>
    /// <param name="content">What to read.</param>
    /// <exception cref="DeltaException">The file is gone, is not a valid Parquet file, or holds invalid actions.</exception>
    public static List<DeltaAction> Read(IFileSystem fileSystem, string path, long size, CheckpointContent content)
    {
        try
        {
            using ParquetFile file = Open(fileSystem, path, size);
            Apache.Arrow.Schema schema = file.Schema;
            List<string[]> columns = Projection(schema, content);
            if (columns.Count == 0) return [];

            IReadOnlySet<string>? instantLeaves = columns.Any(static c => c is ["add", "stats_parsed"]) ? Int96StatsLeaves(file) : null;
            // Utf8, which the actions read.
            using ParquetReader reader = file.OpenReader(new ParquetReadOptions
            {
                ColumnPaths = columns, BatchSize = BatchSize, BinaryAsString = true, ViewTypes = false, MaxDegreeOfParallelism = 1,
            });
            return file.Metadata.RowCount <= InlineRows || Environment.ProcessorCount == 1
                ? ReadInline(reader.ReadBatches(), instantLeaves)
                : ReadSideBySide(reader.ReadBatches(), instantLeaves);
        }
        catch (Exception e) when (e is ParquetFormatException or NotSupportedException)
        {
            throw new DeltaException($"The checkpoint file {path} cannot be read: {e.Message}", e);
        }
        catch (FileNotFoundException e)
        {
            throw new DeltaException($"The checkpoint file {path} disappeared while reading the log.", e);
        }
    }

    private static List<DeltaAction> ReadInline(IEnumerable<RecordBatch> batches, IReadOnlySet<string>? instantLeaves)
    {
        using var converter = new Converter(instantLeaves);
        List<DeltaAction> actions = [];
        foreach (RecordBatch batch in batches)
        {
            using (batch) converter.Read(batch, actions);
        }
        return actions;
    }

    /// <summary>
    /// Decodes the batches here and makes their actions on the thread pool, at most one batch per core in flight. The
    /// first failure in row order is thrown (as reading the batches one by one would throw it), once every batch in
    /// flight is done.
    /// </summary>
    private static List<DeltaAction> ReadSideBySide(IEnumerable<RecordBatch> batches, IReadOnlySet<string>? instantLeaves)
    {
        int width = Environment.ProcessorCount;
        using var slots = new SemaphoreSlim(width);
        var idle = new ConcurrentBag<Converter>();
        List<Converter> made = [];
        List<Part> parts = [];
        ExceptionDispatchInfo? decodeFailure = null;
        int failed = 0;
        IEnumerator<RecordBatch> source = batches.GetEnumerator();
        try
        {
            while (true)
            {
                slots.Wait();
                RecordBatch batch;
                try
                {
                    if (Volatile.Read(ref failed) != 0 || !source.MoveNext())
                    {
                        slots.Release();
                        break;
                    }
                    batch = source.Current;
                }
                catch (Exception e)
                {
                    slots.Release();
                    decodeFailure = ExceptionDispatchInfo.Capture(e);
                    break;
                }
                if (!idle.TryTake(out Converter? converter)) made.Add(converter = new Converter(instantLeaves));
                var part = new Part(batch.Length);
                parts.Add(part);
                part.Done = Task.Run(() =>
                {
                    try
                    {
                        using (batch) converter.Read(batch, part.Actions);
                    }
                    catch (Exception e)
                    {
                        part.Failure = ExceptionDispatchInfo.Capture(e);
                        Volatile.Write(ref failed, 1);
                    }
                    finally
                    {
                        idle.Add(converter);
                        slots.Release();
                    }
                });
            }
        }
        finally
        {
            // Nothing the batches use is freed while one is read; the parts never throw.
            foreach (Part part in parts) part.Done.Wait();
            source.Dispose();
            foreach (Converter converter in made) converter.Dispose();
        }

        foreach (Part part in parts) part.Failure?.Throw();
        decodeFailure?.Throw();
        List<DeltaAction> actions = new(parts.Sum(static p => p.Actions.Count));
        foreach (Part part in parts) actions.AddRange(part.Actions);
        return actions;
    }

    /// <summary>The actions of one batch, or its failure.</summary>
    private sealed class Part(int rows)
    {
        public List<DeltaAction> Actions { get; } = new(rows);

        public Task Done { get; set; } = Task.CompletedTask;

        public ExceptionDispatchInfo? Failure { get; set; }
    }

    /// <summary>
    /// Makes actions from batches on one thread at a time: what the adds share (statistics bytes, partition maps) and
    /// the writer of <c>stats_parsed</c> text are its own.
    /// </summary>
    private sealed class Converter(IReadOnlySet<string>? instantLeaves) : IDisposable
    {
        private readonly ParsedStatsText? _stats = instantLeaves is null ? null : new ParsedStatsText(instantLeaves);
        private readonly CheckpointActions.Shared _shared = new();

        public void Read(RecordBatch batch, List<DeltaAction> actions) => CheckpointActions.Read(batch, _stats, _shared, actions);

        public void Dispose()
        {
            _stats?.Dispose();
            _shared.Dispose();
        }
    }

    private static ParquetFile Open(IFileSystem fileSystem, string path, long size)
    {
        if (size <= 0 && fileSystem is ITailReadable { CanReadTails: true } tails)
            size = (tails.ReadTail(path, 8) ?? throw new FileNotFoundException($"{path} does not exist.", path)).Size;
        if (size > 0) return ParquetFile.Open(new FileSystemRandomAccess(fileSystem, path, size));

        // No size to go by (a sidecar the log gives none for, on a store that cannot read tails): the whole file.
        FileBytes bytes = fileSystem.ReadFile(path) ?? throw new FileNotFoundException($"{path} does not exist.", path);
        return ParquetFile.Open(bytes.Memory, owner: bytes); // the file frees the bytes
    }

    /// <summary>The column paths to read: the action columns the file has, whole, except the typed copies in <c>add</c>.</summary>
    private static List<string[]> Projection(Apache.Arrow.Schema schema, CheckpointContent content)
    {
        List<string[]> paths = [];
        foreach (string column in content == CheckpointContent.All ? CheckpointActions.Columns : ["metaData", "protocol"])
        {
            Field? field = schema.FieldsList.FirstOrDefault(f => f.Name == column);
            if (field is null) continue;
            if (column == "add" && field.DataType is Apache.Arrow.Types.StructType add)
            {
                bool hasText = add.Fields.Any(static f => f.Name == "stats");
                foreach (Field child in add.Fields)
                {
                    if (child.Name == "partitionValues_parsed" || (hasText && child.Name == "stats_parsed")) continue;
                    paths.Add(["add", child.Name]);
                }
            }
            else
            {
                paths.Add([column]);
            }
        }
        return paths;
    }

    /// <summary>The leaves of <c>add.stats_parsed</c> stored as INT96 (Spark's timestamps), by their path inside it joined with <c>\0</c>.</summary>
    private static HashSet<string> Int96StatsLeaves(ParquetFile file)
    {
        HashSet<string> leaves = new(StringComparer.Ordinal);
        foreach (var leaf in file.Tree.Leaves)
        {
            if (leaf.Element.Type == ParquetType.Int96 && leaf.Path is ["add", "stats_parsed", _, ..])
                leaves.Add(string.Join('\0', leaf.Path[2..]));
        }
        return leaves;
    }
}
