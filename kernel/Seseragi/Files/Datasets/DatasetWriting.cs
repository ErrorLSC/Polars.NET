using System.Runtime.ExceptionServices;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files;

/// <summary>A directory of a dataset being written, as a <see cref="DatasetSink{TPartition}"/> partition: its path under the dataset's (ending with '/'), its values.</summary>
internal sealed record DatasetPartition(string Directory, IReadOnlyDictionary<string, string?> Values)
{
    public static readonly DatasetPartition Root = new("", new Dictionary<string, string?>());
}

/// <summary>
/// A dataset write, whatever the format of its files (the writers of each format wrap it): the target checked for
/// files there already, the rows handed to a <see cref="DatasetSink{TPartition}"/>, the files named, the old files
/// deleted once every new one is published (<see cref="ExistingData.ReplacePartitions"/>). A failed write gives its
/// open files up and deletes the files it published (the target is as it was); files it cannot delete are listed by a
/// <see cref="DatasetWriteException"/>. One thread at a time.
/// </summary>
internal sealed class DatasetWriting<TFile> : IDisposable where TFile : DatasetWrittenFile
{
    private const int Open = 0, Completed = 1, Failed = 2;

    private readonly IWritableFileStore _store;
    private readonly string _root;
    private readonly DatasetWriteOptions _options;
    private readonly string _extension;
    private readonly Func<SunkFile<DatasetPartition>, TFile> _file;
    private readonly DatasetSink<DatasetPartition> _sink;
    private readonly string[] _keys;
    private int _state;

    private DatasetWriting(IWritableFileStore store, string root, Schema schema, DatasetWriteOptions options, DatasetFileFormat format,
        Func<SunkFile<DatasetPartition>, TFile> file)
    {
        _store = store;
        _root = root;
        Schema = schema;
        _options = options;
        _extension = format.Extension;
        _file = file;
        _keys = [.. options.PartitionBy ?? []];
        int[] keys = [.. _keys.Select(k => schema.GetFieldIndex(k))];
        int[] data = [.. Enumerable.Range(0, schema.FieldsList.Count).Where(c => options.IncludePartitionColumns || !keys.Contains(c))];
        // An empty string and a null are partitions apart: their directories differ (k= and k=__HIVE_DEFAULT_PARTITION__).
        var settings = new DatasetSinkSettings(store, format, keys, [.. keys.Select(_ => false)], data,
            options.MaxRowsPerFile ?? 0, options.ApproxBytesPerFile ?? 0, options.MemoryBudget ?? DatasetSink<DatasetPartition>.DefaultBudget,
            options.MaxDegreeOfParallelism, options.SyncOnCommit);
        _sink = new DatasetSink<DatasetPartition>(settings, Partition, PathOf);
    }

    /// <summary>The schema of the rows written.</summary>
    public Schema Schema { get; }

    /// <summary>The write's id (in the files' default names).</summary>
    public string WriteId { get; } = Guid.NewGuid().ToString();

    /// <summary>Starts a write to <paramref name="path"/> (its files there checked first, as <see cref="DatasetWriteOptions.ExistingData"/> says).</summary>
    public static async ValueTask<DatasetWriting<TFile>> CreateAsync(string path, Schema schema, DatasetWriteOptions options, DatasetFileFormat format,
        Func<SunkFile<DatasetPartition>, TFile> file, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        IReadOnlyList<string> keys = options.PartitionBy ?? [];
        foreach (string key in keys)
        {
            int index = schema.GetFieldIndex(key);
            if (index < 0) throw new ArgumentException($"The partition column '{key}' is not a column of the schema.", nameof(options));
            if (!HivePaths.Renders(schema.GetFieldByIndex(index).DataType))
                throw new ArgumentException($"The partition column '{key}' is of type {schema.GetFieldByIndex(index).DataType.Name}, which partition paths cannot hold.", nameof(options));
        }
        if (keys.Distinct(StringComparer.Ordinal).Count() != keys.Count) throw new ArgumentException("A partition column is named twice.", nameof(options));
        if (keys.Count == schema.FieldsList.Count && keys.Count > 0 && !options.IncludePartitionColumns)
            throw new ArgumentException("Every column partitions the rows: the files would hold no column (include the partition columns in them).", nameof(options));
        IWritableFileStore store = options.Store ?? new LocalFileStore();
        string root = path.Length > 1 ? path.TrimEnd('/', '\\') : path;
        if (options.ExistingData == ExistingData.Error && await FirstFileAsync(store, root, ct).ConfigureAwait(false) is { } there)
            throw new IOException($"'{path}' holds files already ('{there}'): choose ExistingData.Append or ReplacePartitions to write there.");
        if (options.ExistingData == ExistingData.ReplacePartitions && !store.CanList)
            throw new NotSupportedException($"The store of '{path}' cannot list its files: the old ones cannot be found to replace them.");
        return new DatasetWriting<TFile>(store, root, schema, options, format, file);
    }

    /// <summary>A file under <paramref name="root"/>, at any depth; null when there is none.</summary>
    private static async ValueTask<string?> FirstFileAsync(IWritableFileStore store, string root, CancellationToken ct)
    {
        if (!store.CanList)
            throw new NotSupportedException($"The store of '{root}' cannot list its files: it cannot be checked for files there already (choose ExistingData.Append).");
        await foreach (StoredFile file in store.ListAsync(root + "/", recursive: true, ct).ConfigureAwait(false)) return file.Path;
        return null;
    }

    /// <summary>The partition of a first row (none without keys): its values rendered, its directory.</summary>
    private DatasetPartition Partition(PartitionRow? row)
    {
        if (row is not { } first) return DatasetPartition.Root;
        string?[] values = [.. first.Keys.Select(keys => HivePaths.Render(keys, first.Row))];
        return new DatasetPartition(HivePaths.Directory(_keys, values), _keys.Zip(values).ToDictionary(static p => p.First, static p => p.Second));
    }

    /// <summary>The path of a partition's file of an index.</summary>
    private string PathOf(DatasetPartition partition, long index)
    {
        string name;
        if (_options.FileName is { } naming)
        {
            name = naming(new DatasetFileName(WriteId, index, _extension));
            if (string.IsNullOrEmpty(name) || name is "." or ".." || name.AsSpan().IndexOfAny('/', '\\') >= 0)
                throw new InvalidOperationException($"The file name '{name}' is not a plain file name (no directory).");
        }
        else name = $"part-{index:D5}-{WriteId}{_extension}";
        return $"{_root}/{partition.Directory}{name}";
    }

    /// <summary>Writes a batch's rows (the caller keeps it, and may let it go once this returns).</summary>
    /// <exception cref="ArgumentException">The batch does not match the schema (nothing is written).</exception>
    public void Write(RecordBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        EnsureOpen();
        if (!Matches(batch.Schema))
            throw new ArgumentException($"The batch's columns ({Describe(batch.Schema)}) are not the dataset's ({Describe(Schema)}).", nameof(batch));
        try
        {
            _sink.Write([batch]);
            _sink.HoldMemory();
        }
        catch (Exception e)
        {
            Fail(e);
            throw; // not reached
        }
    }

    /// <summary>
    /// Completes the write: the open files closed and published, then, replacing data, the files that were there
    /// deleted.
    /// </summary>
    public DatasetWriteResult<TFile> Complete()
    {
        EnsureOpen();
        List<SunkFile<DatasetPartition>> files;
        try
        {
            files = _sink.Finish();
        }
        catch (Exception e)
        {
            Fail(e);
            throw; // not reached
        }
        _state = Completed;
        if (_options.ExistingData == ExistingData.ReplacePartitions)
            DeleteOld([.. files.Select(f => Normalized(f.Path))], [.. files.Select(f => f.Partition.Directory).Distinct()]);
        return new DatasetWriteResult<TFile>(WriteId, [.. files.Select(_file)]);
    }

    /// <summary>
    /// Gives the write up: its open files given up, the files it published deleted (those that cannot be are listed by
    /// a <see cref="DatasetWriteException"/>). Nothing once completed or given up.
    /// </summary>
    public void Abort()
    {
        if (_state != Open) return;
        _state = Failed;
        List<string> left = Undo();
        if (left.Count > 0) throw new DatasetWriteException($"The dataset write was given up; {left.Count} of the files it published could not be deleted.", left, null);
    }

    /// <summary>Gives the write up after a failure, and throws: the failure, or a <see cref="DatasetWriteException"/> naming the files left.</summary>
    private void Fail(Exception failure)
    {
        _state = Failed;
        List<string> left;
        try
        {
            left = Undo();
        }
        catch (Exception e)
        {
            throw new AggregateException(failure, e);
        }
        if (left.Count > 0)
            throw new DatasetWriteException($"The dataset write failed ({failure.Message}); {left.Count} of the files it published could not be deleted.", left, failure);
        ExceptionDispatchInfo.Throw(failure);
    }

    /// <summary>Gives the open files up and deletes the files published; returns those that could not be deleted.</summary>
    private List<string> Undo()
    {
        _sink.GiveUp();
        List<string> left = [];
        foreach (SunkFile<DatasetPartition> file in _sink.Landed())
        {
            try
            {
                _store.DeleteAsync(file.Path).AsTask().GetAwaiter().GetResult();
            }
            catch (Exception)
            {
                left.Add(file.Path);
            }
        }
        return left;
    }

    /// <summary>
    /// Deletes the files under the directories of the partitions written (without partitions: under the dataset's path)
    /// that the write did not publish; those it cannot are listed by a <see cref="DatasetWriteException"/>.
    /// </summary>
    private void DeleteOld(HashSet<string> written, List<string> directories)
    {
        List<string> left = [];
        Exception? first = null;
        foreach (string old in directories.SelectMany(ListAll))
        {
            if (written.Contains(Normalized(old))) continue;
            try
            {
                _store.DeleteAsync(old).AsTask().GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                first ??= e;
                left.Add(old);
            }
        }
        if (left.Count > 0)
            throw new DatasetWriteException($"The dataset was written; {left.Count} of the files it replaces could not be deleted.", left, first);

        List<string> ListAll(string directory)
        {
            List<string> paths = [];
            IAsyncEnumerator<StoredFile> files = _store.ListAsync($"{_root}/{directory}", recursive: true).GetAsyncEnumerator();
            try
            {
                while (files.MoveNextAsync().AsTask().GetAwaiter().GetResult()) paths.Add(files.Current.Path);
            }
            finally
            {
                files.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            return paths;
        }
    }

    // A local store lists files below directories with the system's separator.
    private static string Normalized(string path) => Path.DirectorySeparatorChar == '/' ? path : path.Replace(Path.DirectorySeparatorChar, '/');

    private bool Matches(Schema schema) =>
        schema.FieldsList.Count == Schema.FieldsList.Count
        && schema.FieldsList.Zip(Schema.FieldsList).All(static p => p.First.Name == p.Second.Name && ArrowTypes.SameType(p.First.DataType, p.Second.DataType));

    private static string Describe(Schema schema) => string.Join(", ", schema.FieldsList.Select(f => $"{f.Name}: {f.DataType.Name}"));

    private void EnsureOpen()
    {
        if (_state == Completed) throw new InvalidOperationException("The dataset write is complete.");
        if (_state == Failed) throw new InvalidOperationException("The dataset write failed or was given up.");
    }

    /// <summary>Gives the write up unless it completed (files that cannot be deleted are reported as a warning only); frees the sink.</summary>
    public void Dispose()
    {
        try
        {
            Abort();
        }
        catch (Exception e)
        {
            System.Diagnostics.Trace.TraceWarning($"A dataset write given up on dispose left files: {e.Message}");
        }
        finally
        {
            _sink.Dispose();
        }
    }
}
