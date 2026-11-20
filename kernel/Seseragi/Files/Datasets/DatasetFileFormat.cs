using Apache.Arrow;

namespace Kaminari.Seseragi.Files;

/// <summary>
/// A file format as <see cref="DatasetSink{TPartition}"/> writes it: prepared once with the schema of the files, then
/// a <see cref="DatasetFileWriter"/> per file, over the file's stream.
/// </summary>
internal abstract class DatasetFileFormat
{
    /// <summary>The extension of the files (with its dot).</summary>
    public abstract string Extension { get; }

    /// <summary>
    /// Prepares the files' writing (called once, before the first file opens): <paramref name="schema"/> is the schema of
    /// the rows the files take; with <paramref name="sideBySide"/> several files are written at once (one per partition),
    /// on <paramref name="threads"/> threads together.
    /// </summary>
    public abstract void Prepare(Schema schema, bool sideBySide, int threads);

    /// <summary>A writer of one file into <paramref name="stream"/> (which the sink owns: the writer leaves it open).</summary>
    public abstract DatasetFileWriter Open(Stream stream);
}

/// <summary>
/// One file being written: rows of batches given as ranges (the caller keeps the batch, and may let it go once
/// <see cref="Write"/> returns), the file's size on disk measured or estimated, what it holds in memory.
/// </summary>
internal abstract class DatasetFileWriter
{
    // The last exact measure of the file's size (it may wait for pages being compressed) and its rows then.
    private long _measuredBytes, _measuredRows;

    /// <summary>The rows written.</summary>
    public long Rows { get; private set; }

    /// <summary>The bytes of one of its rows, as last measured; null before the first measure.</summary>
    public double? PerRow => _measuredRows > 0 ? (double)_measuredBytes / _measuredRows : null;

    /// <summary>
    /// What the file takes so far on disk: measured when an estimate from the last measure comes near
    /// <paramref name="target"/> (or there is none), else that estimate, so that a measure that waits (for pages being
    /// compressed) happens only a few times per file.
    /// </summary>
    public long Bytes(long target)
    {
        if (PerRow is double perRow)
        {
            long estimate = _measuredBytes + (long)((Rows - _measuredRows) * perRow);
            if (estimate < target * 9 / 10) return estimate;
        }
        _measuredBytes = MeasureBytes();
        _measuredRows = Rows;
        return _measuredBytes;
    }

    /// <summary>Encodes rows <paramref name="start"/> .. +<paramref name="count"/> of a batch.</summary>
    public void Write(RecordBatch batch, int start, int count)
    {
        WriteCore(batch, start, count);
        Rows += count;
    }

    /// <summary>The file's bytes on disk so far, exactly (its pages pending included, as they will be written).</summary>
    protected abstract long MeasureBytes();

    /// <inheritdoc cref="Write"/>
    protected abstract void WriteCore(RecordBatch batch, int start, int count);

    /// <summary>What the writer holds in memory (at least), its stream aside.</summary>
    public abstract long HeldBytes { get; }

    /// <summary>The bytes written to the stream.</summary>
    public abstract long BytesWritten { get; }

    /// <summary>Writes out what the writer holds that can be (a Parquet row group ended early), to hold less memory.</summary>
    public virtual void EndChunk()
    {
    }

    /// <summary>Completes the file (all of it goes to the stream); returns its footer where the format has one to report.</summary>
    public abstract object? Complete();

    /// <summary>Gives the file up: nothing more is written; the writer's memory is let go.</summary>
    public abstract void Abort();
}
