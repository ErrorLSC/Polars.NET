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
internal static class CheckpointReader
{
    private const int BatchSize = 8_192;

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
            List<DeltaAction> actions = [];
            if (columns.Count == 0) return actions;

            ParsedStatsText? stats = columns.Any(static c => c is ["add", "stats_parsed"]) ? new ParsedStatsText(Int96StatsLeaves(file)) : null;
            // One thread: IFileSystem promises nothing about concurrent ranged reads. Utf8, which the actions read.
            using ParquetReader reader = file.OpenReader(new ParquetReadOptions
            {
                ColumnPaths = columns, BatchSize = BatchSize, BinaryAsString = true, ViewTypes = false, MaxDegreeOfParallelism = 1,
            });
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch) CheckpointActions.Read(batch, stats, actions);
            }
            return actions;
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

    private static ParquetFile Open(IFileSystem fileSystem, string path, long size)
    {
        if (size <= 0 && fileSystem is ITailReadable { CanReadTails: true } tails)
            size = (tails.ReadTail(path, 8) ?? throw new FileNotFoundException($"{path} does not exist.", path)).Size;
        if (size > 0) return ParquetFile.Open(new FileSystemRandomAccess(fileSystem, path, size));

        // No size to go by (a sidecar the log gives none for, on a store that cannot read tails): the whole file.
        byte[] bytes = fileSystem.ReadAll(path) ?? throw new FileNotFoundException($"{path} does not exist.", path);
        return ParquetFile.Open(new MemoryStream(bytes, writable: false));
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
