namespace Kaminari.Seseragi.Files.Json;

/// <summary>How <see cref="JsonWriter"/> writes JSON.</summary>
public sealed record JsonWriteOptions
{
    /// <summary>The layout: one object per line (the default) or one document (an array of objects).</summary>
    public JsonFormat Format { get; init; } = JsonFormat.Lines;

    /// <summary>
    /// The compression of the bytes written (none by default). The rows are compressed in pieces side by side, each a
    /// gzip member or Zstandard frame of its own: one valid file that every reader decompresses whole. Text outputs
    /// (<see cref="TextWriter"/>, strings) take no compression.
    /// </summary>
    public JsonCompression Compression { get; init; } = JsonCompression.None;

    /// <summary>The compression level (the codec's default when null): gzip 0–9, Zstandard 1–22.</summary>
    public int? CompressionLevel { get; init; }

    /// <summary>
    /// How many pieces of rows are encoded (and compressed) at once: the physical cores by default (a second hardware
    /// thread per core slows this encoding down). The bytes written do not depend on it.
    /// </summary>
    public int MaxDegreeOfParallelism { get; init; } = Kaminari.Sazanami.Platform.CpuTopology.PhysicalCores;

    /// <summary>
    /// Who owns the batches (<see cref="BatchOwnership.Caller"/> by default: a batch is encoded before Write returns).
    /// With <see cref="BatchOwnership.Writer"/> the writer takes every batch given to it, returns once the batch is
    /// checked, encodes several batches side by side and disposes each once encoded (also when writing fails, or the
    /// writer is aborted or disposed). The bytes written do not depend on it.
    /// </summary>
    public BatchOwnership BatchOwnership { get; init; } = BatchOwnership.Caller;

    /// <summary>Whether the stream (or text writer) stays open once the writer is disposed (the writer of a path always closes its file).</summary>
    public bool LeaveOpen { get; init; }

    /// <summary>The bytes of encoded rows in flight at most (a piece of rows goes beyond alone; tests set less).</summary>
    internal long InFlightBytes { get; init; } = 256L << 20;

    /// <summary>The encoded bytes a piece of rows aims at (tests set less, to cut batches finely).</summary>
    internal int PieceBytes { get; init; } = 1 << 20;

    internal void Validate()
    {
        if (!Enum.IsDefined(Format)) throw new ArgumentOutOfRangeException(nameof(Format), Format, "Unknown JSON format.");
        if (!Enum.IsDefined(Compression)) throw new ArgumentOutOfRangeException(nameof(Compression), Compression, "Unknown JSON compression.");
        if (!Enum.IsDefined(BatchOwnership)) throw new ArgumentOutOfRangeException(nameof(BatchOwnership), BatchOwnership, "Unknown batch ownership.");
        if (MaxDegreeOfParallelism <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxDegreeOfParallelism), MaxDegreeOfParallelism, "The degree of parallelism must be positive.");
        if (CompressionLevel is int level && (Compression == JsonCompression.None
            || Compression == JsonCompression.Gzip && level is < 0 or > 9 || Compression == JsonCompression.Zstd && level is < 1 or > 22))
            throw new ArgumentOutOfRangeException(nameof(CompressionLevel), level, "The level is not one of the compression's (gzip 0–9, Zstandard 1–22).");
    }
}

/// <summary>The compression of JSON written.</summary>
public enum JsonCompression
{
    /// <summary>None.</summary>
    None,

    /// <summary>gzip (members side by side).</summary>
    Gzip,

    /// <summary>Zstandard (frames side by side).</summary>
    Zstd,
}
