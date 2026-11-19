namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>The two IPC formats.</summary>
public enum IpcFormat
{
    /// <summary>The file format (<c>.arrow</c>, Feather v2): magic numbers and a footer that indexes the batches, for random access.</summary>
    File,

    /// <summary>The stream format (<c>.arrows</c>): messages one after another, for pipes and sockets; dictionaries may be replaced.</summary>
    Stream,
}

/// <summary>How <see cref="IpcWriter"/> writes Arrow IPC data.</summary>
public sealed record IpcWriteOptions
{
    /// <summary>The format (<see cref="IpcFormat.File"/> by default).</summary>
    public IpcFormat Format { get; init; } = IpcFormat.File;

    /// <summary>
    /// The body compression: <see cref="NoCompression"/> by default (as Polars writes; pyarrow's Feather writes LZ4),
    /// <see cref="Lz4FrameCompression"/> or <see cref="ZstandardCompression"/>. Each buffer is compressed on its own; one
    /// that does not get smaller is stored as it is.
    /// </summary>
    public IIpcCompression Compression { get; init; } = NoCompression.Instance;

    /// <summary>
    /// Custom metadata of the file's footer (the file format only; the schema's own metadata is
    /// <see cref="Apache.Arrow.Schema.Metadata"/>).
    /// </summary>
    public IReadOnlyDictionary<string, string>? FileMetadata { get; init; }

    /// <summary>How many buffers of a batch are compressed at once (all cores by default). The bytes written do not depend on it.</summary>
    public int MaxDegreeOfParallelism { get; init; } = Environment.ProcessorCount;

    /// <summary>
    /// Who owns the batches (<see cref="BatchOwnership.Caller"/> by default: a batch is compressed before Write
    /// returns). With <see cref="BatchOwnership.Writer"/> the writer takes every batch given to it, returns once the
    /// batch is checked, compresses up to <see cref="MaxDegreeOfParallelism"/> batches side by side and writes them in
    /// order, disposing each once written (also when writing fails, or the writer is aborted or disposed). The bytes
    /// written do not depend on it.
    /// </summary>
    public BatchOwnership BatchOwnership { get; init; } = BatchOwnership.Caller;

    /// <summary>
    /// With <see cref="BatchOwnership.Writer"/>, the uncompressed bytes of the batches on their way at most (a batch
    /// goes beyond alone; tests set less).
    /// </summary>
    internal long InFlightBytes { get; init; } = 512L << 20;

    /// <summary>Whether the stream stays open once the writer is disposed (the writer of a path always closes its file).</summary>
    public bool LeaveOpen { get; init; }

    /// <summary>The uncompressed bytes of a Zstandard frame: larger buffers are compressed as several frames side by side (tests lower it).</summary>
    internal int ZstdFrameBytes { get; init; } = Kaminari.Sazanami.Arrow.Ipc.IpcBatchEncoder.ZstdFrameBytes;

    /// <summary>
    /// A compressed record batch of at least this many uncompressed bytes, written to a seekable output, is written as
    /// its buffers are compressed (its metadata, which precedes the body, written over a placeholder once known); the
    /// bytes are the same. Tests set 0 (always) or <see cref="long.MaxValue"/> (never).
    /// </summary>
    internal long StreamBodyFrom { get; init; } = 8L << 20;

    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(Compression, nameof(Compression));
        if (!Enum.IsDefined(BatchOwnership)) throw new ArgumentOutOfRangeException(nameof(BatchOwnership), BatchOwnership, "Unknown batch ownership.");
        if (!Enum.IsDefined(Format)) throw new ArgumentOutOfRangeException(nameof(Format), Format, "Unknown IPC format.");
        if (MaxDegreeOfParallelism <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxDegreeOfParallelism), MaxDegreeOfParallelism, "The degree of parallelism must be positive.");
        if (FileMetadata is not null && Format == IpcFormat.Stream)
            throw new ArgumentException("File metadata goes in the footer, which only the file format has.", nameof(FileMetadata));
    }
}
