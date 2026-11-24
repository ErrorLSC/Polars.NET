namespace Kaminari.Seseragi.Files.Csv;

/// <summary>How <see cref="CsvWriter"/> writes delimited text.</summary>
public sealed record CsvWriteOptions
{
    /// <summary>Whether the first line names the columns.</summary>
    public bool IncludeHeader { get; init; } = true;

    /// <summary>Whether the text starts with a byte order mark (spreadsheet programs take it as the sign of UTF-8).</summary>
    public bool IncludeBom { get; init; }

    /// <summary>The byte between fields (ASCII; not the quote, nor CR or LF).</summary>
    public char Separator { get; init; } = ',';

    /// <summary>The quote around fields that need one (ASCII; a quote inside is doubled).</summary>
    public char Quote { get; init; } = '"';

    /// <summary>Which fields are quoted (<see cref="CsvQuoteStyle.Necessary"/>: those that would not read back as they are).</summary>
    public CsvQuoteStyle QuoteStyle { get; init; } = CsvQuoteStyle.Necessary;

    /// <summary>The text of a null (never quoted, except with <see cref="CsvQuoteStyle.Always"/>).</summary>
    public string NullValue { get; init; } = "";

    /// <summary>What ends each line (may be empty).</summary>
    public string LineTerminator { get; init; } = "\n";

    /// <summary>
    /// The compression of the bytes written (none by default). The rows are compressed in pieces side by side, each a
    /// gzip member or Zstandard frame of its own: one valid file that every reader decompresses whole. Text outputs
    /// (<see cref="TextWriter"/>, strings) take no compression.
    /// </summary>
    public CsvCompression Compression { get; init; } = CsvCompression.None;

    /// <summary>The compression level (the codec's default when null): gzip 0–9, Zstandard 1–22.</summary>
    public int? CompressionLevel { get; init; }

    /// <summary>
    /// How many pieces of rows are encoded (and compressed) at once: the physical cores by default. The bytes written do
    /// not depend on it.
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

    /// <summary>
    /// The pieces of rows encoded and not yet written, at most (0: twice <see cref="MaxDegreeOfParallelism"/>; tests set
    /// less): the memory a write holds.
    /// </summary>
    internal int WindowPieces { get; init; }

    internal void Validate()
    {
        if (Separator > 0x7F || Separator is '\r' or '\n')
            throw new ArgumentOutOfRangeException(nameof(Separator), Separator, "The separator must be one ASCII character other than CR and LF.");
        if (Quote > 0x7F || Quote is '\r' or '\n' || Quote == Separator)
            throw new ArgumentOutOfRangeException(nameof(Quote), Quote, "The quote must be one ASCII character other than CR, LF and the separator.");
        ArgumentNullException.ThrowIfNull(NullValue);
        ArgumentNullException.ThrowIfNull(LineTerminator);
        if (!Enum.IsDefined(QuoteStyle)) throw new ArgumentOutOfRangeException(nameof(QuoteStyle), QuoteStyle, "Unknown quote style.");
        if (!Enum.IsDefined(Compression)) throw new ArgumentOutOfRangeException(nameof(Compression), Compression, "Unknown CSV compression.");
        if (!Enum.IsDefined(BatchOwnership)) throw new ArgumentOutOfRangeException(nameof(BatchOwnership), BatchOwnership, "Unknown batch ownership.");
        if (MaxDegreeOfParallelism <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxDegreeOfParallelism), MaxDegreeOfParallelism, "The degree of parallelism must be positive.");
        if (CompressionLevel is int level && (Compression == CsvCompression.None
            || Compression == CsvCompression.Gzip && level is < 0 or > 9 || Compression == CsvCompression.Zstd && level is < 1 or > 22))
            throw new ArgumentOutOfRangeException(nameof(CompressionLevel), level, "The level is not one of the compression's (gzip 0–9, Zstandard 1–22).");
    }
}

/// <summary>Which fields a <see cref="CsvWriter"/> quotes.</summary>
public enum CsvQuoteStyle
{
    /// <summary>
    /// Those that would not read back as they are: a field holding the separator, the quote, CR or LF, and an empty
    /// string (told from a null).
    /// </summary>
    Necessary,

    /// <summary>Every field, nulls included.</summary>
    Always,

    /// <summary>Every field that is not a number (the header too); nulls are not.</summary>
    NonNumeric,

    /// <summary>None (a field holding the separator or a line end does not read back as it is).</summary>
    Never,
}

/// <summary>The compression of CSV written.</summary>
public enum CsvCompression
{
    /// <summary>None.</summary>
    None,

    /// <summary>gzip (members side by side).</summary>
    Gzip,

    /// <summary>Zstandard (frames side by side).</summary>
    Zstd,
}
