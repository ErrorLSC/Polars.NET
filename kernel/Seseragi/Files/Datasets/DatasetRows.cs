using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Data.Building;

namespace Kaminari.Seseragi.Files;

/// <summary>
/// The rows of <typeparamref name="T"/> of a dataset write (the typed writers of each format wrap it): gathered into
/// batches (<see cref="RowBatcher{T}"/>: <see cref="ArrowRowBuilder{T}"/>'s layout, in native memory), each written by
/// the dataset write and freed. A row the layout refuses gives the write up (what it published deleted); when files
/// cannot be deleted the refusal comes as the inner exception of a <see cref="DatasetWriteException"/> listing them.
/// </summary>
internal sealed class DatasetRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T, TFile> : IDisposable
    where TFile : DatasetWrittenFile
{
    private readonly DatasetWriting<TFile> _writing;
    private readonly Sink _sink;
    private readonly RowBatcher<T> _rows;

    /// <param name="writing">The dataset write.</param>
    /// <param name="options">Its options (the threads that make batches, the partition columns).</param>
    /// <param name="format">The format's name, for messages.</param>
    /// <param name="batchRows">The rows of a batch (<see cref="DatasetWriting{TFile}.ChunkRows"/>, the rows grouped by
    /// their keys at once, unless the format wants others).</param>
    public DatasetRows(DatasetWriting<TFile> writing, DatasetWriteOptions options, string format, int batchRows = DatasetWriting<TFile>.ChunkRows)
    {
        _writing = writing;
        _sink = new Sink(writing);
        _rows = new RowBatcher<T>(_sink, options.MaxDegreeOfParallelism, format, batchRows);
        PartitionSchema = PartitionSchemaOf(writing.Schema, options.PartitionBy);
    }

    /// <summary>The partition columns' types, for reading the dataset back (null without partitions).</summary>
    public Schema? PartitionSchema { get; }

    /// <summary>The rows taken so far (those still gathered into the open batch included).</summary>
    public long RowCount => _rows.RowCount;

    /// <summary>
    /// The partition columns as the dataset readers can type them from the paths: a dictionary (an enum's member names)
    /// as its values; null without partitions.
    /// </summary>
    internal static Schema? PartitionSchemaOf(Schema schema, IReadOnlyList<string>? keys) => keys is null or { Count: 0 }
        ? null
        : new Schema([.. keys.Select(key => schema.GetFieldByName(key)).Select(static f =>
            new Field(f.Name, f.DataType is DictionaryType d ? d.ValueType : f.DataType, true, f.HasMetadata ? f.Metadata : null))], null);

    /// <summary>Adds a row.</summary>
    public void WriteRow(T row)
    {
        try
        {
            _rows.WriteRow(row);
        }
        catch (Exception e) when (_sink.TakeLeft() is { } left)
        {
            throw Left(e, left);
        }
    }

    /// <summary>Adds rows.</summary>
    public async ValueTask WriteRowsAsync(IEnumerable<T> rows, bool useAsync, CancellationToken ct)
    {
        try
        {
            await _rows.WriteRowsAsync(rows, useAsync, ct).ConfigureAwait(false);
        }
        catch (Exception e) when (_sink.TakeLeft() is { } left)
        {
            throw Left(e, left);
        }
    }

    /// <summary>Adds rows as they come.</summary>
    public async ValueTask WriteRowsAsync(IAsyncEnumerable<T> rows, CancellationToken ct)
    {
        try
        {
            await _rows.WriteRowsAsync(rows, ct).ConfigureAwait(false);
        }
        catch (Exception e) when (_sink.TakeLeft() is { } left)
        {
            throw Left(e, left);
        }
    }

    /// <summary>Writes the open batch, then completes the write.</summary>
    public DatasetWriteResult<TFile> Complete()
    {
        _rows.FlushAsync(false, default).AsTask().GetAwaiter().GetResult();
        return _writing.Complete();
    }

    /// <inheritdoc cref="Complete"/>
    public async Task<DatasetWriteResult<TFile>> CompleteAsync(CancellationToken ct)
    {
        await _rows.FlushAsync(true, ct).ConfigureAwait(false);
        return await Task.Run(_writing.Complete, ct).ConfigureAwait(false);
    }

    /// <summary>Gives the write up (the rows of the open batch dropped).</summary>
    public void Abort() => _writing.Abort();

    /// <summary>Gives the write up unless it completed; frees the batches' builders.</summary>
    public void Dispose()
    {
        try
        {
            _writing.Dispose();
        }
        finally
        {
            _rows.Dispose();
        }
    }

    private static DatasetWriteException Left(Exception refused, IReadOnlyList<string> left) =>
        new($"The dataset write was given up ({refused.Message}); {left.Count} of the files it published could not be deleted.", left, refused);

    /// <summary>The dataset write as the batches' sink (the batches stay the caller's: each is freed once written).</summary>
    /// <remarks>
    /// Measured (10M rows, 2026-11-20): several batches held and written at once (by 1M rows) were slower everywhere
    /// (Parquet 173 → 191 ms, 30 partitions 334 → 374, IPC 164 → 200): batches are made while the one before is
    /// written, and holding them loses that overlap.
    /// </remarks>
    private sealed class Sink(DatasetWriting<TFile> writing) : IBatchSink
    {
        private IReadOnlyList<string>? _left;

        public bool IsWritable => writing.IsOpen;

        /// <summary>Gives the write up after a refused row; the files it could not delete are kept for the refusal to report.</summary>
        public void Abort()
        {
            try
            {
                writing.Abort();
            }
            catch (DatasetWriteException e)
            {
                _left = e.Remaining;
            }
        }

        /// <summary>The files an abort left (once: then forgotten).</summary>
        public IReadOnlyList<string>? TakeLeft()
        {
            IReadOnlyList<string>? left = _left;
            _left = null;
            return left;
        }

        public ValueTask WriteAsync(RecordBatch batch, bool useAsync, CancellationToken ct)
        {
            // Not cancelled once handed over: the batch's rows are counted, and freed after this (a cancelled run
            // would lose them while the write goes on). Cancellation stops the rows at their source.
            if (useAsync) return new ValueTask(Task.Run(() => writing.Write(batch), CancellationToken.None));
            writing.Write(batch);
            return ValueTask.CompletedTask;
        }
    }
}
